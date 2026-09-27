//! Writes through gateways: a gateway passes each write to the key's home,
//! or to the next candidate while it routes around the home, and the node
//! passes it to S3.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{Options, Simulator, TTL_BUCKET};

fn key(name: &str) -> ObjectKey {
    ObjectKey {
        bucket: TTL_BUCKET.into(),
        key: name.into(),
    }
}

/// Three nodes and two gateways. Metadata stays fresh for the whole run
/// unless a write says otherwise, and a gateway routes around a node that
/// timed out for 2,000 ticks. Blocks are stored on the first read, so a
/// stale read would be a pure hit that no `If-Match` fill could catch.
fn options() -> Options {
    let mut options = Options::scenario();
    options.nodes = 3;
    options.gateways = 2;
    options.ttl = 1_000_000;
    options.ttl_admit_on_first_read = true;
    options.suspect_ttl = 2_000;
    options
}

/// Gateway 0 times out on `key`'s home, which a partition cuts off for
/// `ticks`, reading another key the home holds; it then routes around the
/// home until its suspicion ends. The read is lost as it leaves, so the
/// timeout comes whatever the partition's length.
fn suspect_home(sim: &mut Simulator, key: &ObjectKey, ticks: u64) {
    let home = sim.home(key);
    let neighbor = (0..)
        .map(|index| self::key(&format!("n{index}")))
        .find(|other| sim.home(other) == home)
        .expect("keys spread over every node");
    sim.put(&neighbor, 10);
    sim.partition(home, ticks);
    sim.read_through(0, Request::get(neighbor)).unwrap();
}

/// A write whose home the gateway routes around goes through the next
/// candidate, which passes it to the home, so every gateway's next read
/// sees it.
#[test]
fn a_write_around_a_suspected_home_reaches_the_home() {
    let mut sim = Simulator::new(1, options());
    let key = key("k");
    sim.put(&key, 100);
    sim.read_through(1, Request::get(key.clone())).unwrap();
    // The partition ends long before the write.
    suspect_home(&mut sim, &key, 100);
    sim.write_through_via(0, &key, 120).unwrap();
    assert_eq!(sim.summary().detoured_writes, 1);
    // Gateway 1's entry has expired, so it asks the home.
    let (head, body) = sim.read_through(1, Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
}

/// A write that went around its home while the home was cut off never
/// reaches the home, which keeps the old metadata until its TTL runs out.
/// The gateway that passed the write reads the key directly from S3 until
/// then, so its next read sees the write.
#[test]
fn a_gateway_reads_a_key_it_wrote_around_its_home_from_s3() {
    let mut sim = Simulator::new(1, options());
    let key = key("k");
    sim.put(&key, 100);
    sim.read_through(0, Request::get(key.clone())).unwrap();
    // The partition outlasts the write.
    suspect_home(&mut sim, &key, 1_500);
    sim.write_through_via(0, &key, 120).unwrap();
    assert_eq!(sim.summary().detoured_writes, 1);
    // The partition and the suspicion end, and gateway 0 reads through
    // the home again.
    for _ in 0..2_000 {
        sim.step().unwrap();
    }
    let before = sim.summary().origin_requests;
    let (head, body) = sim.read_through(0, Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
    assert_eq!(sim.summary().origin_requests - before, 1);
}
