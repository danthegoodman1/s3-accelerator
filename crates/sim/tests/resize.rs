//! The cache survives resizing: a node that joins or leaves costs few S3
//! requests, because new owners read from previous owners first.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{IMMUTABLE_BUCKET, Options, Simulator, Summary, TTL_BUCKET};

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
