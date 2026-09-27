//! Evidence from outside the server, in the system calls `strace` records:
//! hits leave the storage node with `sendfile` and the gateway with
//! `splice`, with no write carrying their bytes, and the node's disk writes
//! keep their order. Needs `strace`.

mod common;

use common::{Cluster, Process, data_dir, start_origin};
use s3_accelerator::disk::decode_record;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::LocalSet;
use xxhash_rust::xxh3::xxh3_64;

/// Three whole 64 KiB blocks.
const OBJECT_SIZE: usize = 3 * 65536;
const HEADER_SIZE: u64 = 4096;
const MIN_SLOT: u64 = 4096;
const WRITES: &str = "write,writev,sendto,sendmsg,pwrite64,pwritev,pwritev2";

/// Reads each object twice through a node and a gateway, each a separate
/// traced process. The second reads are hits: their bytes must leave the
/// node through `sendfile` from the slab file and the gateway through
/// `splice` into the client's socket, and no call that writes may carry
/// any of them.
#[tokio::test(flavor = "current_thread")]
async fn hits_leave_by_sendfile_and_splice() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cache = "block_size = 65536\nextent_size = 1048576\nextents = 4";
            let cluster = Cluster::new(&dir, origin_port, cache);
            let calls = format!("sendfile,splice,{WRITES}");
            let node = Process::traced(&cluster.node, &dir.join("node.trace"), &calls);
            common::listening(cluster.node_port).await;
            let gateway = Process::traced(&cluster.gateway, &dir.join("gateway.trace"), &calls);
            common::listening(cluster.gateway_port).await;

            let keys = ["a", "b", "c"];
            let bodies: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| origin.object(&format!("/bucket/{key}")))
                .collect();
            for (key, body) in keys.iter().zip(&bodies) {
                assert!(cluster.get(key).await == (200, body.clone()), "{key}");
            }
            let hits_from = now();
            for (key, body) in keys.iter().zip(&bodies) {
                assert!(cluster.get(key).await == (200, body.clone()), "{key}");
            }
            let hits_until = now();
            assert_eq!(origin.requests.get(), keys.len() as u64);
            node.stop();
            gateway.stop();

            let total = (keys.len() * OBJECT_SIZE) as i64;
            let windows = windows(&bodies);
            let node = read_trace(&dir.join("node.trace"), hits_from, hits_until);
            let sent: i64 = node
                .iter()
                .filter(|call| {
                    call.name == "sendfile"
                        && call.fds().get(1).is_some_and(|fd| fd.ends_with("/slabs>"))
                })
                .map(|call| call.result)
                .sum();
            assert!(
                sent >= total,
                "the node sent {sent} of {total} hit bytes with sendfile"
            );
            let node_written = check_writes_carry_no_body("node", &node, &windows, total);

            let gateway = read_trace(&dir.join("gateway.trace"), hits_from, hits_until);
            let spliced: i64 = gateway
                .iter()
                .filter(|call| {
                    call.name == "splice" && call.fds().get(1).is_some_and(|fd| fd.contains("<TCP"))
                })
                .map(|call| call.result.max(0))
                .sum();
            assert!(
                spliced >= total,
                "the gateway spliced {spliced} of {total} hit bytes to clients"
            );
            let gateway_written = check_writes_carry_no_body("gateway", &gateway, &windows, total);
            println!(
                "{total} hit bytes: the node sent {sent} with sendfile and wrote {node_written} bytes; \
                 the gateway spliced {spliced} to clients and wrote {gateway_written} bytes"
            );
        })
        .await;
}

/// Reads ten objects through a node whose cache holds sixteen blocks, so
/// it evicts blocks and reuses their slots. Every record must follow an
/// `fdatasync` of its block's bytes, and every write over a cleared slot
/// must follow an `fdatasync` of the slot table.
#[tokio::test(flavor = "current_thread")]
async fn blocks_are_synced_before_their_records_and_clears_before_reuse() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cache = "block_size = 65536\nextent_size = 1048576\nextents = 1";
            let cluster = Cluster::new(&dir, origin_port, cache);
            let node = Process::traced(&cluster.node, &dir.join("node.trace"), "pwrite64,fdatasync");
            common::listening(cluster.node_port).await;
            let _gateway = cluster.start_gateway().await;
            for index in 0..10 {
                let key = format!("k{index}");
                let body = origin.object(&format!("/bucket/{key}"));
                assert!(cluster.get(&key).await == (200, body), "{key}");
            }
            // Writes finish before a clean shutdown ends.
            node.stop();

            let calls = read_trace(&dir.join("node.trace"), 0.0, f64::MAX);
            let file = |call: &Call| call.fds().first().cloned().unwrap_or_default();
            let on = |call: &Call, name: &str| file(call).ends_with(&format!("/{name}>"));
            let syncs = |name: &str| -> Vec<(f64, f64)> {
                calls
                    .iter()
                    .filter(|call| call.name == "fdatasync" && on(call, name))
                    .map(|call| (call.start, call.end))
                    .collect()
            };
            let (slab_syncs, table_syncs) = (syncs("slabs"), syncs("slots"));
            let synced_between = |syncs: &[(f64, f64)], after: f64, before: f64| {
                syncs.iter().any(|&(start, end)| start >= after && end <= before)
            };
            // Block writes by slab offset, and the length each record gave
            // its slot.
            let mut blocks: BTreeMap<u64, Vec<(f64, f64, u64)>> = BTreeMap::new();
            let mut lengths: BTreeMap<u64, u64> = BTreeMap::new();
            let (mut records, mut clears) = (0, 0);
            for call in &calls {
                if call.name != "pwrite64" || call.result < 0 {
                    continue;
                }
                let (offset, len) = call.offset_and_len();
                if on(call, "slabs") {
                    blocks.entry(offset).or_default().push((call.start, call.end, len));
                    // A write over a cleared slot follows a sync of the table.
                    for (&cleared, &(clear_end, slot)) in &cleared_slots(&calls, &lengths, call.start) {
                        if offset < cleared + slot && cleared < offset + len {
                            assert!(
                                synced_between(&table_syncs, clear_end, call.start),
                                "slab offset {offset} was written over a cleared slot before the clear was synced"
                            );
                        }
                    }
                } else if on(call, "slots") && offset >= HEADER_SIZE {
                    let data = call.data();
                    let slot = (offset - HEADER_SIZE) / 64 * MIN_SLOT;
                    if data.iter().all(|&byte| byte == 0) {
                        clears += 1;
                        continue;
                    }
                    records += 1;
                    let (record, _, _) = decode_record(&data).expect("a whole record");
                    lengths.insert(offset, record.len);
                    let written = blocks
                        .get(&slot)
                        .and_then(|writes| writes.iter().rev().find(|write| write.1 <= call.start))
                        .unwrap_or_else(|| panic!("a record for slab offset {slot} with no block written"));
                    assert!(
                        synced_between(&slab_syncs, written.1, call.start),
                        "the record for slab offset {slot} was written before its block was synced"
                    );
                }
            }
            assert!(records >= 16 && clears > 0, "{records} records and {clears} clears");
            println!("{records} records and {clears} clears, each in order");
        })
        .await;
}

/// Slots cleared before `time`, by slab offset: when each clear ended, and
/// the slot's size under its last record.
fn cleared_slots(
    calls: &[Call],
    lengths: &BTreeMap<u64, u64>,
    time: f64,
) -> BTreeMap<u64, (f64, u64)> {
    let mut cleared = BTreeMap::new();
    for call in calls.iter().filter(|call| call.end < time) {
        if call.name != "pwrite64" || !call.fds().first().is_some_and(|fd| fd.ends_with("/slots>"))
        {
            continue;
        }
        let (offset, _) = call.offset_and_len();
        if offset < HEADER_SIZE {
            continue;
        }
        let slot = (offset - HEADER_SIZE) / 64 * MIN_SLOT;
        if call.data().iter().all(|&byte| byte == 0) {
            let len = lengths.get(&offset).copied().unwrap_or(MIN_SLOT);
            cleared.insert(slot, (call.end, len.max(MIN_SLOT).next_power_of_two()));
        } else {
            cleared.remove(&slot);
        }
    }
    cleared
}

/// No call that writes, to a socket, pipe or file, carries 32 bytes in a
/// row of any body, and together they write far less than the bodies.
fn check_writes_carry_no_body(
    process: &str,
    calls: &[Call],
    windows: &HashSet<u64>,
    total: i64,
) -> i64 {
    let writes: Vec<&Call> = calls
        .iter()
        .filter(|call| WRITES.split(',').any(|name| name == call.name))
        .collect();
    for call in &writes {
        let data = call.data();
        let carried = data
            .windows(32)
            .any(|window| windows.contains(&xxh3_64(window)));
        assert!(
            !carried,
            "the {process} wrote body bytes with {}",
            call.name
        );
    }
    let written: i64 = writes.iter().map(|call| call.result.max(0)).sum();
    assert!(written < total / 16, "the {process} wrote {written} bytes");
    written
}

/// Hashes of every 32-byte run in the bodies.
fn windows(bodies: &[Vec<u8>]) -> HashSet<u64> {
    bodies
        .iter()
        .flat_map(|body| body.windows(32).map(xxh3_64))
        .collect()
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// `text` with each `\xHH` escape replaced by its byte.
fn unescape(text: &str) -> String {
    let mut parts = text.split("\\x");
    let mut bytes = parts.next().unwrap_or_default().as_bytes().to_vec();
    for part in parts {
        bytes.push(u8::from_str_radix(&part[..2], 16).expect("strace -xx escapes"));
        bytes.extend_from_slice(&part.as_bytes()[2..]);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// One completed system call from a trace.
struct Call {
    name: String,
    /// When it began and returned, in seconds since the epoch.
    start: f64,
    end: f64,
    /// Its arguments as `strace` printed them.
    args: String,
    result: i64,
}

impl Call {
    /// The descriptor arguments, with the names `strace -yy` gives them,
    /// such as `12</data/slabs>` or `7<TCP:[...]>`.
    fn fds(&self) -> Vec<String> {
        self.args
            .split(", ")
            .filter(|arg| {
                arg.split_once('<')
                    .is_some_and(|(fd, _)| fd.parse::<u32>().is_ok())
            })
            .map(unescape)
            .collect()
    }

    /// The bytes of every string argument.
    fn data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        for (index, part) in self.args.split('"').enumerate() {
            if index % 2 == 1 {
                for hex in part.split("\\x").skip(1) {
                    data.push(u8::from_str_radix(&hex[..2], 16).expect("strace -xx escapes"));
                }
            }
        }
        data
    }

    /// A `pwrite64`'s offset and byte count: its last two arguments.
    fn offset_and_len(&self) -> (u64, u64) {
        let mut args = self.args.rsplit(", ");
        let offset = args
            .next()
            .and_then(|arg| arg.parse().ok())
            .expect("an offset");
        let len = args
            .next()
            .and_then(|arg| arg.parse().ok())
            .expect("a count");
        (offset, len)
    }
}

/// The calls in `path` that returned between `from` and `until`. Lines
/// look like `PID TIME name(args) = result`, and a call another thread
/// interrupted spans an `<unfinished ...>` line and a `<... name resumed>`
/// line.
fn read_trace(path: &Path, from: f64, until: f64) -> Vec<Call> {
    let trace = std::fs::read_to_string(path).unwrap();
    let mut unfinished: BTreeMap<&str, (&str, f64, String)> = BTreeMap::new();
    let mut calls = Vec::new();
    for line in trace.lines() {
        // Some versions pad the PID column.
        let Some((pid, rest)) = line.trim_start().split_once(' ') else {
            continue;
        };
        let Some((time, rest)) = rest.trim_start().split_once(' ') else {
            continue;
        };
        let Ok(time) = time.parse::<f64>() else {
            continue;
        };
        let (name, start, text) = if let Some(rest) = rest.strip_prefix("<... ") {
            let Some((_, rest)) = rest.split_once(" resumed>") else {
                continue;
            };
            let Some((name, start, args)) = unfinished.remove(pid) else {
                continue;
            };
            (name, start, format!("{args}{rest}"))
        } else if let Some(rest) = rest.strip_suffix(" <unfinished ...>") {
            if let Some((name, args)) = rest.split_once('(') {
                unfinished.insert(pid, (name, time, args.to_string()));
            }
            continue;
        } else {
            let Some((name, args)) = rest.split_once('(') else {
                continue;
            };
            (name, time, args.to_string())
        };
        let Some((args, result)) = text.rsplit_once(") = ") else {
            continue;
        };
        let result = result
            .split_whitespace()
            .next()
            .and_then(|result| result.parse().ok())
            .unwrap_or(-1);
        if time >= from && time <= until {
            calls.push(Call {
                name: name.to_string(),
                start,
                end: time,
                args: args.to_string(),
                result,
            });
        }
    }
    calls
}
