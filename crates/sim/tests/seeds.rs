use s3_accelerator_sim::Simulator;

#[test]
fn seeds_pass() {
    for seed in 0..200 {
        if let Err(failure) = Simulator::from_seed(seed).run() {
            panic!("{failure}");
        }
    }
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [1, 42, 0xdead_beef] {
        let first = Simulator::from_seed(seed).run().unwrap();
        let second = Simulator::from_seed(seed).run().unwrap();
        assert_eq!(first, second);
    }
}

/// Seed 7859's one node is down or cut off most of the time, so its clients
/// never finish issuing while faults last. Faults stop once their tick
/// budget runs out, and every request is then answered.
#[test]
fn faults_stop_once_their_budget_runs_out() {
    if let Err(failure) = Simulator::from_seed(7859).run() {
        panic!("{failure}");
    }
}

/// Seed 5757: a new home's first fetch admitted a block whose reservation
/// evicted a block of the same version it no longer owned, dropping the
/// version's last reference while the fetch still stored its blocks.
#[test]
fn an_eviction_during_a_first_fetch_keeps_its_version() {
    if let Err(failure) = Simulator::from_seed(5757).run() {
        panic!("{failure}");
    }
}

/// Seed 8590: a new home took a deleted object's metadata from its previous
/// home, S3 answered its revalidation with 404, and the home asked the
/// previous home again, around and around within one tick.
#[test]
fn a_change_s3_reveals_outweighs_a_previous_homes_metadata() {
    if let Err(failure) = Simulator::from_seed(8590).run() {
        panic!("{failure}");
    }
}

/// Seed 2921: a gateway that holds the metadata of three objects wrote a
/// key, and other keys evicted the key's entry, which carried the time of
/// the write; the home's answer to a read sent before the write then
/// restored the old version's metadata.
#[test]
fn a_write_marker_outlasts_cache_eviction() {
    if let Err(failure) = Simulator::from_seed(2921).run() {
        panic!("{failure}");
    }
}

/// Seed 2093: a gateway routing around a home it suspected wrote a key
/// through another node, whose notice to the home a partition dropped. Once
/// the suspicion ended, the gateway read the old version from the home.
#[test]
fn a_write_around_a_suspected_home_is_read_from_s3() {
    if let Err(failure) = Simulator::from_seed(2093).run() {
        panic!("{failure}");
    }
}

/// Seed 426: two nodes split, each alone in its own ring. A gateway routed
/// around one of them to the other, which read its own ring as naming it
/// the home and served metadata an event had made stale.
#[test]
fn a_read_sent_around_the_home_reads_s3() {
    if let Err(failure) = Simulator::from_seed(426).run() {
        panic!("{failure}");
    }
}

/// Seed 947: a warm check found metadata the home had fetched since the
/// write, with requests waiting on its revalidation, and replaced it,
/// dropping the requests.
#[test]
fn a_warm_check_keeps_metadata_the_home_has() {
    if let Err(failure) = Simulator::from_seed(947).run() {
        panic!("{failure}");
    }
}

/// Seed 6265: storing a warmed block evicted another block of the same
/// version, its last reference, and the next block's admission found the
/// version gone.
#[test]
fn warming_keeps_its_version_while_it_stores_blocks() {
    if let Err(failure) = Simulator::from_seed(6265).run() {
        panic!("{failure}");
    }
}
