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

/// A purge frees its blocks' storage and syncs the slab file before the
/// client hears it succeeded, so no crash brings the bytes back.
#[tokio::test(flavor = "current_thread")]
async fn a_purge_syncs_its_erased_bytes_before_it_confirms() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, "block_size = 65536");
            let node = Process::traced(
                &cluster.node,
                &dir.join("node.trace"),
                "fallocate,fdatasync",
            );
            common::listening(cluster.node_port).await;
            let _gateway = cluster.start_gateway().await;
            let body = origin.object("/bucket/k");
            assert!(cluster.get("k").await == (200, body));
            // The blocks' writes finish.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let asked = now();
            let (status, _) = common::send(
                cluster.gateway_port,
                "POST",
                "/bucket/k",
                "x-accel-purge=",
                &[],
                Vec::new(),
            )
            .await;
            let confirmed = now();
            assert_eq!(status, 204);
            node.stop();

            let calls = read_trace(&dir.join("node.trace"), asked, confirmed);
            let on_slabs =
                |call: &&Call| call.fds().first().is_some_and(|fd| fd.ends_with("/slabs>"));
            // Each block's hole, and its space reserved again.
            let allocations: Vec<&Call> = calls
                .iter()
                .filter(on_slabs)
                .filter(|call| call.name == "fallocate" && call.result == 0)
                .collect();
            let holes = allocations
                .iter()
                .filter(|call| call.args.contains("PUNCH_HOLE"))
                .count();
            assert_eq!(
                (holes, allocations.len()),
                (3, 6),
                "a hole and a reservation per block"
            );
            let last = allocations.iter().map(|call| call.end).fold(0.0, f64::max);
            let synced = calls.iter().filter(on_slabs).any(|call| {
                call.name == "fdatasync" && call.start >= last && call.end <= confirmed
            });
            assert!(
                synced,
                "the slab file was not synced after its holes and before the purge confirmed"
            );
        })
        .await;
}

/// A hit whose bytes the page cache holds goes out with `sendfile` from
/// the node's event loop, which it then never blocks; once the pages are
/// gone, the same hit's `sendfile` runs on a worker, which may wait on
/// the drive.
#[tokio::test(flavor = "current_thread")]
async fn a_cached_hit_is_sent_from_the_event_loop() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, "block_size = 65536");
            let node = Process::traced(&cluster.node, &dir.join("node.trace"), "sendfile");
            common::listening(cluster.node_port).await;
            let _gateway = cluster.start_gateway().await;
            let body = origin.object("/bucket/k");
            assert!(cluster.get("k").await == (200, body.clone()));
            // The blocks' writes finish, and their pages stay cached.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let cached = now();
            assert!(cluster.get("k").await == (200, body.clone()));
            let dropped = now();
            let slabs = std::fs::File::open(dir.join("disk-0/slabs")).unwrap();
            rustix::fs::fadvise(&slabs, 0, None, rustix::fs::Advice::DontNeed).unwrap();
            assert!(cluster.get("k").await == (200, body));
            let event_loop = node.server_pid();
            node.stop();

            let threads = |from: f64, until: f64| -> Vec<bool> {
                read_trace(&dir.join("node.trace"), from, until)
                    .iter()
                    .filter(|call| call.name == "sendfile" && call.result > 0)
                    .map(|call| call.thread == event_loop)
                    .collect()
            };
            let hit = threads(cached, dropped);
            assert!(
                !hit.is_empty() && hit.iter().all(|&on_loop| on_loop),
                "{hit:?}"
            );
            let uncached = threads(dropped, f64::MAX);
            assert!(
                !uncached.is_empty() && uncached.iter().all(|&on_loop| !on_loop),
                "{uncached:?}"
            );
        })
        .await;
}

/// A first read's bytes move on worker threads: the node receives S3's
/// body and writes it to the gateway off its event loop, the thread that
/// owns its core, which handles only heads.
#[tokio::test(flavor = "current_thread")]
async fn a_fill_moves_its_bytes_off_the_event_loop() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.size.set(4 << 20);
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, "block_size = 65536");
            let calls = "read,recvfrom,write,writev,sendto";
            let node = Process::traced(&cluster.node, &dir.join("node.trace"), calls);
            common::listening(cluster.node_port).await;
            let _gateway = cluster.start_gateway().await;
            let body = origin.object("/bucket/k");
            assert!(cluster.get("k").await == (200, body));
            let event_loop = node.server_pid();
            node.stop();

            let calls = read_trace(&dir.join("node.trace"), 0.0, f64::MAX);
            let moving: Vec<&Call> = calls
                .iter()
                .filter(|call| call.fds().first().is_some_and(|fd| fd.contains("TCP")))
                .filter(|call| call.result >= 16 << 10)
                .collect();
            let reads = |name: &str| matches!(name, "read" | "recvfrom");
            let received = moving.iter().filter(|call| reads(&call.name)).count();
            let sent = moving.len() - received;
            assert!(
                received > 0 && sent > 0,
                "{received} large reads, {sent} large writes"
            );
            let on_loop = moving
                .iter()
                .filter(|call| call.thread == event_loop)
                .count();
            assert_eq!(on_loop, 0, "large socket calls on the event loop");
            println!("{received} large reads and {sent} large writes, all on workers");
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
