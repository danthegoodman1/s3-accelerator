//! Hot keys: an owner leases a placement read often enough to its next
//! rendezvous candidates, and gateways spread its reads across them.

use s3_accelerator_core::s3::{ByteRange, ObjectKey, Request};
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

/// Leases renew while reads, spread across the owner and its replicas,
/// stay above half the promotion rate, and end below it. Replicas report
/// their reads halfway through a lease, so the owner counts them over
/// that half.
#[test]
fn leases_renew_above_half_the_promotion_rate() {
    for (percent, renews) in [(55, true), (45, false)] {
        let mut options = options();
        // 40 reads within 1,000 ticks make a placement hot.
        options.hot_threshold = 40;
        let mut sim = Simulator::new(1, options);
        let key = hot_object(&mut sim);
        for _ in 0..41 {
            sim.read(Request::get(key.clone())).unwrap();
        }
        let granted = sim.summary().leases;
        assert!(granted > 0);
        // `percent` of the promotion rate, for two lease terms.
        let gap = 100_000 / (40 * percent);
        let start = sim.now();
        for read in 0.. {
            let due = start + read * gap;
            if due > start + 4_000 {
                break;
            }
            while sim.now() < due {
                sim.step().unwrap();
            }
            sim.read(Request::get(key.clone())).unwrap();
        }
        let renewed = sim.summary().leases > granted;
        assert_eq!(renewed, renews, "reads at {percent}% of the promotion rate");
    }
}

/// Replicas admit a leased placement's blocks without the doorkeeper, even
/// once its owner stops answering and they fill from S3: after the first
/// block's reads find the owner gone, each replica stores the second block
/// on its first read.
#[test]
fn replicas_skip_the_doorkeeper_while_their_owner_is_gone() {
    let mut options = options();
    options.immutable_admit_on_first_read = false;
    let mut sim = Simulator::new(1, options);
    let key = hot_object(&mut sim);
    let range = |first, last| Request {
        range: Some(ByteRange::Inclusive { first, last }),
        ..Request::get(key.clone())
    };
    let (first, second) = (range(0, 9), range(70, 79));
    for _ in 0..6 {
        sim.read(first.clone()).unwrap();
    }
    assert!(sim.summary().leases > 0);
    let owner = sim.home(&key);
    sim.crash(owner).unwrap();
    for _ in 0..6 {
        let (head, _) = sim.read(first.clone()).unwrap();
        assert_eq!(head.status, 206);
    }
    let before = sim.summary().origin_requests;
    for _ in 0..4 {
        let (head, body) = sim.read(second.clone()).unwrap();
        assert_eq!((head.status, body.len()), (206, 10));
    }
    assert_eq!(sim.summary().origin_requests - before, 2);
}
