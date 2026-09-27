//! Hot keys: an owner leases a placement read often enough to its next
//! rendezvous candidates, and gateways spread its reads across them.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{IMMUTABLE_BUCKET, Options, Simulator};

/// Four nodes. A placement read 5 times within 1,000 ticks is leased to
/// two more nodes for 2,000 ticks. Blocks are stored on their first read.
fn options() -> Options {
    let mut options = Options::scenario();
    options.nodes = 4;
    options.hot_threshold = 5;
    options.hot_window = 1_000;
    options.hot_replicas = 2;
    options.lease = 2_000;
    options.immutable_admit_on_first_read = true;
    options
}

/// A 100-byte object: two blocks, both on its home.
fn hot_object(sim: &mut Simulator) -> ObjectKey {
    let key = ObjectKey {
        bucket: IMMUTABLE_BUCKET.into(),
        key: "hot".into(),
    };
    sim.put(&key, 100);
    key
}

/// Each node's share of reads since `before`.
fn reads_since(sim: &Simulator, before: &[u64]) -> Vec<u64> {
    let now = sim.summary().node_reads;
    now.iter()
        .zip(before)
        .map(|(now, then)| now - then)
        .collect()
}

/// Once a placement is hot, its reads spread across its owner and the two
/// replicas, which fill from the owner rather than from S3.
#[test]
fn a_hot_placement_spreads_across_its_replicas() {
    let mut sim = Simulator::new(1, options());
    let key = hot_object(&mut sim);
    for _ in 0..6 {
        sim.read(Request::get(key.clone())).unwrap();
    }
    let before = sim.summary().node_reads;
    for _ in 0..30 {
        let (head, body) = sim.read(Request::get(key.clone())).unwrap();
        assert_eq!((head.status, body.len()), (200, 100));
    }
    let reads = reads_since(&sim, &before);
    let serving = reads.iter().filter(|&&count| count > 0).count();
    assert_eq!(serving, 3, "{reads:?}");
    assert!(reads.iter().all(|&count| count <= 12), "{reads:?}");
    assert!(sim.summary().leased_reads > 0);
    // The first read fetched the object; replicas took blocks from the
    // owner.
    assert_eq!(sim.summary().origin_requests, 1);
    assert!(sim.summary().peer_requests > 0);
}

/// Leases on a placement no longer read run out, and its reads go back to
/// its owner alone. The burst that made it hot keeps it busy enough for
/// one renewal, so the leases end with their second term.
#[test]
fn a_lease_runs_out_once_reads_stop() {
    let mut sim = Simulator::new(1, options());
    let key = hot_object(&mut sim);
    for _ in 0..12 {
        sim.read(Request::get(key.clone())).unwrap();
    }
    assert!(sim.summary().leased_reads > 0);
    for _ in 0..5_000 {
        sim.step().unwrap();
    }
    let before = sim.summary().node_reads;
    for _ in 0..3 {
        sim.read(Request::get(key.clone())).unwrap();
    }
    let reads = reads_since(&sim, &before);
    let serving = reads.iter().filter(|&&count| count > 0).count();
    assert_eq!(serving, 1, "{reads:?}");
}
