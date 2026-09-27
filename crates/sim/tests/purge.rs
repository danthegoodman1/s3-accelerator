//! Purges: the home drops an object's metadata and blocks, and has every
//! other node drop theirs, telling each again until it confirms.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{Options, Simulator, TTL_BUCKET};

/// Three nodes, 64-byte blocks in 4-block chunks, blocks stored on their
/// first read: a 1,500-byte object's middle chunks spread across nodes.
fn options() -> Options {
    let mut options = Options::scenario();
    options.nodes = 3;
    options.ttl_admit_on_first_read = true;
    options
}

fn key() -> ObjectKey {
    ObjectKey {
        bucket: TTL_BUCKET.into(),
        key: "k".into(),
    }
}

/// A node other than the home that holds blocks of the object, which two
/// reads spread, the second from the chunks' owners, and then deleted from
/// S3.
fn cached_and_deleted(sim: &mut Simulator) -> usize {
    let key = key();
    sim.put(&key, 1_500);
    for _ in 0..2 {
        sim.read(Request::get(key.clone())).unwrap();
    }
    sim.delete(&key);
    let home = sim.home(&key);
    (0..3)
        .find(|&node| node != home && sim.recorded_blocks_of(node, &key) > 0)
        .expect("a chunk owner holds blocks")
}

fn assert_purged(sim: &Simulator) {
    let key = key();
    for node in 0..3 {
        assert_eq!(sim.recorded_blocks_of(node, &key), 0, "node {node}");
        assert_eq!(sim.pending_purges(node), [], "node {node}");
    }
}

/// Lets a coordinator tell nodes again, a few times over.
fn wait(sim: &mut Simulator) {
    for _ in 0..1_000 {
        sim.step().unwrap();
    }
}

#[test]
fn a_purge_drops_every_block_of_the_object() {
    let mut sim = Simulator::new(1, options());
    cached_and_deleted(&mut sim);
    sim.purge(&key()).unwrap();
    assert_purged(&sim);
    let (head, _) = sim.read(Request::get(key())).unwrap();
    assert_eq!(head.status, 404);
}

/// A chunk owner down during the purge keeps its blocks until it is back;
/// the home tells it again until it confirms.
#[test]
fn an_owner_down_during_a_purge_drops_its_blocks_once_back() {
    let mut sim = Simulator::new(1, options());
    let owner = cached_and_deleted(&mut sim);
    sim.shut_down(owner).unwrap();
    sim.purge(&key()).unwrap();
    let home = sim.home(&key());
    assert_eq!(sim.pending_purges(home), [(key(), vec![owner])]);
    sim.restart(owner).unwrap();
    wait(&mut sim);
    assert_purged(&sim);
}

/// The home records which nodes a purge waits on durably, so a home that
/// crashed and restarted still tells an owner that was down.
#[test]
fn a_home_restarted_during_a_purge_still_tells_the_owners() {
    let mut sim = Simulator::new(1, options());
    let owner = cached_and_deleted(&mut sim);
    sim.shut_down(owner).unwrap();
    sim.purge(&key()).unwrap();
    let home = sim.home(&key());
    sim.crash(home).unwrap();
    sim.restart(home).unwrap();
    sim.restart(owner).unwrap();
    wait(&mut sim);
    assert_purged(&sim);
}
