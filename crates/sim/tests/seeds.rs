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

/// Seed 7596: a range read misrouted to a node that had left was refused,
/// and the gateway blamed the live node it meant, twice, until no
/// candidate was left and the client got a 503. Misroutes stand for a
/// ring naming another owner, so they go only to nodes that are up.
#[test]
fn a_misroute_goes_only_to_a_node_that_is_up() {
    if let Err(failure) = Simulator::from_seed(7596).run() {
        panic!("{failure}");
    }
}

/// Seed 9998: a part S3 refused started its read with S3's answer, but the
/// runs the read-ahead window had yet to ask for stayed pending, and the
/// gateway asked for them after the answer ended.
#[test]
fn a_refused_part_drops_the_runs_still_to_ask_for() {
    if let Err(failure) = Simulator::from_seed(9998).run() {
        panic!("{failure}");
    }
}

/// Seed 566: a read planned again after a node found its version stale
/// kept the runs its first plan had yet to ask for, and forwarded them
/// after the home's whole answer.
#[test]
fn a_replanned_read_drops_its_earlier_runs() {
    if let Err(failure) = Simulator::from_seed(566).run() {
        panic!("{failure}");
    }
}

/// Seed 447: a write landed while a response streamed, and the part the
/// window asked for next found S3 without the version, so the response
/// ended early, as the spec allows.
#[test]
fn a_write_during_a_response_can_end_it_early() {
    if let Err(failure) = Simulator::from_seed(447).run() {
        panic!("{failure}");
    }
}

/// A clean shutdown finished the node's writes, which answered requests
/// waiting on them, after the simulator had read the node's counters, so
/// the counters missed three blocks the node sent.
#[test]
fn a_clean_stop_counts_what_its_last_writes_answer() {
    if let Err(failure) = Simulator::from_seed(16648030974266863723).run() {
        panic!("{failure}");
    }
}

/// With both nodes down and no network delay, every gateway answered 503
/// at once and clients retried within the same tick, forever.
#[test]
fn clients_back_off_from_a_cluster_that_fails_at_once() {
    if let Err(failure) = Simulator::from_seed(16648030974266864628).run() {
        panic!("{failure}");
    }
}

/// A purged block found corrupt went as the last plan reading it was
/// abandoned, and the node then freed its slot a second time.
#[test]
fn a_corrupt_purged_block_is_freed_once() {
    if let Err(failure) = Simulator::from_seed(6707258591206456590).run() {
        panic!("{failure}");
    }
}

/// A restarted node passed an event to the home its first ring named, then
/// adopted the cluster's ring before the event finished, and the home that
/// ring named never heard: its metadata stayed stale past the event.
#[test]
fn an_event_under_way_reaches_a_new_rings_home() {
    if let Err(failure) = Simulator::from_seed(14674409610259123330).run() {
        panic!("{failure}");
    }
}

/// Seed 45747: a warm check sent before a write through the home answered
/// after the node had forgotten the write, which it kept only for a
/// fallback window shorter than S3's timeout, so the home knew the
/// replaced version. The client's back-off moved this seed's run off the
/// bug; `a_warm_check_older_than_a_write_keeps_no_metadata` pins it.
#[test]
fn a_change_outlasts_the_s3_requests_sent_before_it() {
    if let Err(failure) = Simulator::from_seed(45747).run() {
        panic!("{failure}");
    }
}

/// Seed 68495: a fill's body from S3 ended short, and with no network
/// delay a client retried each response that ended early within the same
/// tick, forever.
#[test]
fn clients_back_off_from_a_body_that_ends_early() {
    if let Err(failure) = Simulator::from_seed(68495).run() {
        panic!("{failure}");
    }
}
