//! S3's event notifications: a node takes each event from the queue,
//! passes it to the key's home, and deletes it once the home has it.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{Options, Simulator, TTL_BUCKET};

fn key(name: &str) -> ObjectKey {
    ObjectKey {
        bucket: TTL_BUCKET.into(),
        key: name.into(),
    }
}

/// Three nodes. S3 notifies the TTL bucket's changes, whose metadata
/// otherwise stays fresh for the whole run, and gateway entries last 50
/// ticks. Blocks are stored on their first read, so a stale read would be
/// a pure hit that no `If-Match` fill could catch.
fn options() -> Options {
    let mut options = Options::scenario();
    options.nodes = 3;
    options.ttl = 1_000_000;
    options.ttl_admit_on_first_read = true;
    options.gateway_metadata_ttl = 50;
    options.events = true;
    options.visibility_timeout = 200;
    options
}

/// Creates `key` and reads it, so its home and the gateway know it.
fn cached(sim: &mut Simulator, key: &ObjectKey) {
    sim.put(key, 100);
    sim.deliver_events().unwrap();
    sim.read(Request::get(key.clone())).unwrap();
}

/// Runs until the gateway's entries from before now have expired, and
/// reads `key`, which must be S3's current version.
fn assert_reads_current(sim: &mut Simulator, key: &ObjectKey) {
    for _ in 0..60 {
        sim.step().unwrap();
    }
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len() as u64, current.size);
}

/// A node other than the home takes the event and passes it on, so the
/// home drops the old version's metadata.
#[test]
fn an_event_reaches_the_home_through_another_node() {
    let mut sim = Simulator::new(1, options());
    let key = key("k");
    cached(&mut sim, &key);
    sim.offer_events_to((sim.home(&key) + 1) % 3);
    sim.put(&key, 120);
    sim.deliver_events().unwrap();
    assert_reads_current(&mut sim, &key);
}

/// A home cut off when its event comes never hears of it, so the node
/// that took the event leaves it in the queue. The queue offers it again
/// once the visibility timeout passes, and the home hears then.
#[test]
fn an_event_its_home_missed_comes_back() {
    let mut sim = Simulator::new(1, options());
    let key = key("k");
    cached(&mut sim, &key);
    let home = sim.home(&key);
    sim.offer_events_to((home + 1) % 3);
    sim.partition(home, 100);
    sim.put(&key, 120);
    sim.deliver_events().unwrap();
    assert_eq!(sim.summary().events, 2);
    assert_reads_current(&mut sim, &key);
}

/// An event that comes after the home fetched the version it names, as
/// for a write through the cache, changes nothing: the next read costs no
/// S3 request.
#[test]
fn an_event_for_the_version_the_home_holds_keeps_it() {
    let mut sim = Simulator::new(1, options());
    let key = key("k");
    cached(&mut sim, &key);
    sim.hold_events();
    sim.write_through(&key, 120).unwrap();
    sim.read(Request::get(key.clone())).unwrap();
    sim.release_events();
    sim.deliver_events().unwrap();
    let before = sim.summary().origin_requests;
    assert_reads_current(&mut sim, &key);
    assert_eq!(sim.summary().origin_requests, before);
}
