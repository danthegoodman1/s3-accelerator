//! The cache survives resizing: a node that joins or leaves costs few S3
//! requests, because new owners read from previous owners first.

use s3_accelerator_core::layout::Layout;
use s3_accelerator_core::placement::{Member, NodeId, Ring};
use s3_accelerator_core::s3::{ByteRange, ObjectKey, Request};
use s3_accelerator_sim::{IMMUTABLE_BUCKET, Options, Simulator, Summary, TTL_BUCKET};
use std::num::NonZeroU32;

fn run(sim: &mut Simulator, ticks: u64) {
    for _ in 0..ticks {
        sim.step().unwrap();
    }
}

/// Four gossiping nodes that store blocks on their first read, with a
/// gateway that keeps no metadata, so every read asks the object's home.
/// The seed varies the delays.
fn cluster(seed: u64, fallback_window: u64) -> Options {
    let mut options = Options::scenario();
    options.nodes = 4;
    options.membership = true;
    options.chunk_blocks = 2;
    options.extents = 64;
    options.immutable_admit_on_first_read = true;
    options.ttl_admit_on_first_read = true;
    options.gateway_metadata_capacity = 1;
    options.fallback_window = fallback_window;
    options.delay_max = 1 + seed % 4;
    options.disk_delay_max = seed % 3;
    options.send_delay_max = seed % 5;
    options
}

/// Forty objects of up to three chunks, so some spread across owners.
fn keys(seed: u64, bucket: &str) -> Vec<(ObjectKey, u64)> {
    (0..40u64)
        .map(|index| {
            let key = ObjectKey {
                bucket: bucket.into(),
                key: format!("k{index}"),
            };
            (key, 50 + (index * 37 + seed * 101) % 700)
        })
        .collect()
}

/// S3 requests, and body bytes from previous owners and from S3, while
/// every object is read once after a resize.
struct Reread {
    origin_requests: u64,
    peer_bytes: u64,
    miss_bytes: u64,
}

fn reread(sim: &mut Simulator, keys: &[(ObjectKey, u64)]) -> Reread {
    let before = sim.summary();
    for (key, size) in keys {
        let (head, body) = sim.read(Request::get(key.clone())).unwrap();
        assert_eq!((head.status, body.len() as u64), (200, *size), "{key:?}");
    }
    let after = sim.summary();
    let delta = |field: fn(&Summary) -> u64| field(&after) - field(&before);
    Reread {
        origin_requests: delta(|summary| summary.origin_requests),
        peer_bytes: delta(|summary| summary.peer_bytes),
        miss_bytes: delta(|summary| summary.miss_bytes),
    }
}

/// A cluster warmed with every object read twice: the first read stores
/// the home's blocks, and the second the other chunks' owners'.
fn warmed(seed: u64, fallback_window: u64, keys: &[(ObjectKey, u64)]) -> Simulator {
    let mut sim = Simulator::new(seed, cluster(seed, fallback_window));
    run(&mut sim, 300);
    for (key, size) in keys {
        sim.put(key, *size);
        sim.read(Request::get(key.clone())).unwrap();
        sim.read(Request::get(key.clone())).unwrap();
    }
    sim
}

/// Waits for the nodes to agree on the ring after a change, then makes a
/// read whose answer shows the gateway the new ring's version, so later
/// reads go by it. The gateway routes that read by its old ring.
fn settle_ring(sim: &mut Simulator, key: &ObjectKey) {
    run(sim, 1_000);
    sim.read(Request::get(key.clone())).unwrap();
    run(sim, 50);
}

/// Warms the cache, resizes it, and rereads every object once the nodes and
/// the gateway agree on the new ring.
fn resize(seed: u64, fallback_window: u64, change: impl Fn(&mut Simulator)) -> Reread {
    let keys = keys(seed, IMMUTABLE_BUCKET);
    let mut sim = warmed(seed, fallback_window, &keys);
    change(&mut sim);
    settle_ring(&mut sim, &keys[0].0);
    reread(&mut sim, &keys)
}

fn grow(seed: u64, fallback_window: u64) -> Reread {
    resize(seed, fallback_window, |sim| {
        sim.add_node().unwrap();
    })
}

/// A node that knows one seed at first, as a config naming one running
/// node: its ring grows as it hears of the others, and it must still read
/// from the owners before it arrived.
fn grow_from_one_seed(seed: u64, fallback_window: u64) -> Reread {
    resize(seed, fallback_window, |sim| {
        sim.add_node_knowing(Some(vec![1])).unwrap();
    })
}

fn shrink(seed: u64, fallback_window: u64) -> Reread {
    resize(seed, fallback_window, |sim| sim.remove_node(3).unwrap())
}

/// Against a control run without fallback, fallback cuts S3 requests and
/// bytes read from S3 by at least three quarters.
fn check(name: &str, seed: u64, with: Reread, without: Reread) {
    eprintln!(
        "{name} seed {seed}: with fallback {} S3 requests, {} bytes from previous owners and {} from S3; without, {} S3 requests and {} bytes from S3",
        with.origin_requests,
        with.peer_bytes,
        with.miss_bytes,
        without.origin_requests,
        without.miss_bytes
    );
    assert!(with.origin_requests <= 2, "{name} seed {seed}");
    assert!(
        with.origin_requests * 4 <= without.origin_requests,
        "{name} seed {seed}"
    );
    assert!(
        with.miss_bytes * 4 <= without.miss_bytes,
        "{name} seed {seed}"
    );
    assert!(with.peer_bytes > 0, "{name} seed {seed}");
}

#[test]
fn a_new_node_reads_from_previous_owners() {
    for seed in 0..8 {
        check("grow", seed, grow(seed, 5_000), grow(seed, 0));
    }
}

#[test]
fn a_node_that_knows_one_seed_reads_from_previous_owners() {
    for seed in 0..4 {
        let (with, without) = (grow_from_one_seed(seed, 5_000), grow_from_one_seed(seed, 0));
        check("grow from one seed", seed, with, without);
    }
}

#[test]
fn a_leaving_node_serves_its_blocks_to_their_new_owners() {
    for seed in 0..8 {
        check("shrink", seed, shrink(seed, 5_000), shrink(seed, 0));
    }
}

/// Once the fallback window ends, a new owner reads S3 directly.
#[test]
fn after_the_fallback_window_new_owners_read_s3() {
    let keys = keys(0, IMMUTABLE_BUCKET);
    let mut sim = warmed(0, 500, &keys);
    sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    let reread = reread(&mut sim, &keys);
    assert_eq!(reread.peer_bytes, 0);
    assert_eq!(sim.summary().peer_requests, 0);
}

/// A write through an object's new home makes the metadata its previous
/// home knew useless: the next read returns the written version.
#[test]
fn a_write_since_the_previous_home_knew_the_object_wins() {
    let keys = keys(0, TTL_BUCKET);
    let mut sim = warmed(0, 5_000, &keys);
    let node = sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    let (key, _) = keys
        .iter()
        .find(|(key, _)| sim.home(key) == node)
        .expect("an object whose home moved");
    sim.write_through(key, 999).unwrap();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 999);
}

/// Blocks a new owner reads from a previous owner skip the doorkeeper: the
/// next read finds them on the new owner's disk.
#[test]
fn blocks_from_a_previous_owner_skip_the_doorkeeper() {
    let keys = keys(0, IMMUTABLE_BUCKET);
    let mut options = cluster(0, 5_000);
    options.immutable_admit_on_first_read = false;
    let mut sim = Simulator::new(0, options);
    run(&mut sim, 300);
    // The second read of a block admits it; the third finds it stored.
    for (key, size) in &keys {
        sim.put(key, *size);
        for _ in 0..3 {
            sim.read(Request::get(key.clone())).unwrap();
        }
    }
    sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    let first = reread(&mut sim, &keys);
    assert!(first.peer_bytes > 0);
    let before = sim.summary();
    let second = reread(&mut sim, &keys);
    assert_eq!(second.peer_bytes, 0);
    assert_eq!(sim.summary().peer_requests, before.peer_requests);
    assert_eq!(second.origin_requests, 0);
}

/// A gateway still on the old ring tells the old home of a write to an
/// object whose metadata the new home already copied. The old home passes
/// the write on, and the next read through the new home returns the
/// written version.
#[test]
fn a_write_through_the_old_home_reaches_the_new_one() {
    let keys = keys(0, IMMUTABLE_BUCKET);
    let mut options = cluster(0, 5_000);
    options.gateways = 2;
    let mut sim = Simulator::new(0, options);
    run(&mut sim, 300);
    for (key, size) in &keys {
        sim.put(key, *size);
        sim.read(Request::get(key.clone())).unwrap();
    }
    let node = sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    let (key, _) = keys
        .iter()
        .find(|(key, _)| sim.home(key) == node)
        .expect("an object whose home moved");
    // The new home copies the metadata from the old one. Gateway 0 keeps
    // one object's metadata, so reading another makes it ask the home.
    sim.read(Request::get(key.clone())).unwrap();
    let other = keys.iter().find(|(other, _)| other != key).unwrap();
    sim.read(Request::get(other.0.clone())).unwrap();
    assert_ne!(sim.gateway_ring(1), sim.gateway_ring(0));
    sim.write_through_via(1, key, 999).unwrap();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 999);
}

/// A previous owner that never answers is asked once: after its timeout,
/// the new owner goes straight to S3 until the ring changes again.
#[test]
fn a_previous_owner_that_never_answers_is_skipped() {
    let keys = keys(0, IMMUTABLE_BUCKET);
    let mut options = cluster(0, 50_000);
    // Node 0 stays in every ring while the objects are reread.
    options.down_grace = 100_000;
    let mut sim = Simulator::new(0, options);
    run(&mut sim, 300);
    for (key, size) in &keys {
        sim.put(key, *size);
        sim.read(Request::get(key.clone())).unwrap();
        sim.read(Request::get(key.clone())).unwrap();
    }
    sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    sim.fail(0).unwrap();
    let before = sim.summary();
    reread(&mut sim, &keys);
    let after = sim.summary();
    assert!(after.peer_requests > before.peer_requests + 1);
    assert_eq!(after.peer_timeouts - before.peer_timeouts, 1);
}

/// Metadata a previous home validated long ago is as old at the new home:
/// it has outlived its TTL, so the new home checks S3 and finds the
/// object changed, rather than serving the old version for another TTL.
#[test]
fn metadata_from_a_previous_home_keeps_its_age() {
    let keys = keys(0, TTL_BUCKET);
    let mut options = cluster(0, 50_000);
    options.ttl = 1_000;
    let mut sim = Simulator::new(0, options);
    run(&mut sim, 300);
    for (key, size) in &keys {
        sim.put(key, *size);
        sim.read(Request::get(key.clone())).unwrap();
    }
    // S3 changes behind the cache's back.
    for (key, size) in &keys {
        sim.put(key, size + 1);
    }
    run(&mut sim, 1_000);
    let node = sim.add_node().unwrap();
    settle_ring(&mut sim, &keys[0].0);
    let before = sim.summary().peer_metadata;
    let moved: Vec<&(ObjectKey, u64)> = keys
        .iter()
        .filter(|(key, _)| sim.home(key) == node)
        .collect();
    assert!(!moved.is_empty());
    for (key, size) in moved {
        let (_, body) = sim.read(Request::get(key.clone())).unwrap();
        assert_eq!(body.len() as u64, size + 1);
    }
    assert!(sim.summary().peer_metadata > before);
}

/// A read across two placements that both moved to the new node, from two
/// different nodes, asks each previous owner for its own blocks.
#[test]
fn a_read_across_two_placements_asks_each_previous_owner() {
    let options = cluster(0, 50_000);
    let layout = Layout::new(options.block_size, options.chunk_blocks);
    let block = options.block_size;
    let ring = |nodes: u64| {
        let members = (0..nodes)
            .map(|id| Member {
                id: NodeId(id),
                weight: NonZeroU32::MIN,
            })
            .collect();
        Ring::new(1, members)
    };
    let (before, after) = (ring(4), ring(5));
    let size: u64 = 700;
    let blocks = size.div_ceil(block);
    let owner = |ring: &Ring, key: &ObjectKey, index: u64| {
        ring.owner(layout.placement(key, size, index).hash())
    };
    // Adjacent blocks of two placements the new node will own, which two
    // different nodes owned before.
    let (key, first) = (0..1_000)
        .map(|index| ObjectKey {
            bucket: IMMUTABLE_BUCKET.into(),
            key: format!("wide-{index}"),
        })
        .find_map(|key| {
            let first = (0..blocks - 1).find(|&index| {
                layout.placement(&key, size, index).hash()
                    != layout.placement(&key, size, index + 1).hash()
                    && owner(&after, &key, index) == Some(NodeId(4))
                    && owner(&after, &key, index + 1) == Some(NodeId(4))
                    && owner(&before, &key, index) != owner(&before, &key, index + 1)
            })?;
            Some((key, first))
        })
        .expect("an object with two such placements");
    let mut sim = Simulator::new(0, options);
    run(&mut sim, 300);
    // The first read stores the home's blocks, and the second the others'.
    sim.put(&key, size);
    sim.read(Request::get(key.clone())).unwrap();
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(sim.add_node().unwrap(), 4);
    settle_ring(&mut sim, &key);
    let requests = sim.summary().origin_requests;
    let range = ByteRange::Inclusive {
        first: first * block,
        last: (first + 2) * block - 1,
    };
    let (head, _) = sim
        .read(Request {
            range: Some(range),
            ..Request::get(key)
        })
        .unwrap();
    assert_eq!(head.status, 206);
    assert_eq!(sim.summary().origin_requests, requests);
}
