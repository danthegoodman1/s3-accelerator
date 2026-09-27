//! Evidence from outside the server, in the system calls `strace` records:
//! hits leave the storage node with `sendfile` and the gateway with
//! `splice`, with no write carrying their bytes, and the node's disk writes
//! keep their order. Needs `strace`.

mod common;

use common::trace::{Call, WRITES, check_writes_carry_no_body, now, read_trace, windows};
use common::{Cluster, Process, data_dir, start_origin};
use s3_accelerator::disk::decode_record;
use std::collections::BTreeMap;
use tokio::task::LocalSet;

/// Three whole 64 KiB blocks.
const OBJECT_SIZE: usize = 3 * 65536;
const HEADER_SIZE: u64 = 4096;
const MIN_SLOT: u64 = 4096;

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
