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
