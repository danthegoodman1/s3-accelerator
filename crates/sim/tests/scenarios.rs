//! Scripted runs of one node, for behavior that random seeds exercise but
//! cannot pin down.

use s3_accelerator_core::s3::{ByteRange, ETag, ObjectKey, Request};
use s3_accelerator_sim::{IMMUTABLE_BUCKET, Options, Simulator, TTL_BUCKET};

fn key(bucket: &str, name: &str) -> ObjectKey {
    ObjectKey {
        bucket: bucket.into(),
        key: name.into(),
    }
}

#[test]
fn the_doorkeeper_stores_a_block_on_its_second_read() {
    let mut sim = Simulator::new(1, Options::scenario());
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 256);
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(sim.summary().written_bytes, 0);
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(sim.summary().written_bytes, 256);
    let before = sim.summary().hit_bytes;
    sim.read(Request::get(key)).unwrap();
    assert_eq!(sim.summary().hit_bytes - before, 256);
}

/// Capacity is 64 blocks. The hot set is 16 blocks read three times; the
/// scan is 400 blocks read once.
#[test]
fn a_scan_leaves_the_hot_set_cached() {
    for admit_on_first_read in [false, true] {
        let mut options = Options::scenario();
        options.immutable_admit_on_first_read = admit_on_first_read;
        let mut sim = Simulator::new(1, options);
        let hot: Vec<ObjectKey> = (0..8)
            .map(|index| key(IMMUTABLE_BUCKET, &format!("hot-{index}")))
            .collect();
        for key in &hot {
            sim.put(key, 128);
        }
        for _ in 0..3 {
            for key in &hot {
                sim.read(Request::get(key.clone())).unwrap();
            }
        }
        for index in 0..200 {
            let cold = key(IMMUTABLE_BUCKET, &format!("cold-{index}"));
            sim.put(&cold, 128);
            sim.read(Request::get(cold)).unwrap();
        }
        let before = sim.summary().hit_bytes;
        for key in &hot {
            sim.read(Request::get(key.clone())).unwrap();
        }
        let hot_hits = sim.summary().hit_bytes - before;
        assert_eq!(
            hot_hits,
            8 * 128,
            "admit on first read: {admit_on_first_read}"
        );
    }
}

/// S3 checks preconditions before the range, so a cold object's home must
/// answer these itself instead of relaying its first fetch's 416.
#[test]
fn preconditions_come_before_an_unsatisfiable_range() {
    let mut sim = Simulator::new(1, Options::scenario());
    let range = Some(ByteRange::From { first: 500 });
    let stale = key(TTL_BUCKET, "stale");
    sim.put(&stale, 100);
    let request = Request {
        if_match: Some(ETag("\"stale\"".into())),
        range,
        ..Request::get(stale)
    };
    assert_eq!(sim.read(request).unwrap().0.status, 412);
    let current = key(TTL_BUCKET, "current");
    sim.put(&current, 100);
    let etag = sim.read(Request::head(current.clone())).unwrap().0.etag;
    let cold = key(TTL_BUCKET, "cold");
    sim.put(&cold, 100);
    let request = Request {
        range,
        ..Request::get(cold)
    };
    assert_eq!(sim.read(request).unwrap().0.status, 416);
    let request = Request {
        if_none_match: etag,
        range,
        ..Request::get(current)
    };
    assert_eq!(sim.read(request).unwrap().0.status, 304);
}

/// A request that planned two fills, both of which fail `If-Match`, goes
/// back to the gateway once.
#[test]
fn a_request_whose_fills_all_fail_retries_once() {
    let mut options = Options::scenario();
    options.ttl_admit_on_first_read = true;
    options.ttl = 10_000;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 192);
    let middle = Request {
        range: Some(ByteRange::Inclusive {
            first: 64,
            last: 127,
        }),
        ..Request::get(key.clone())
    };
    sim.read(middle).unwrap();
    // Client to gateway to node takes two ticks; the fills reach S3 on the third.
    let whole = sim.start(Request::get(key.clone()));
    for _ in 0..3 {
        sim.step().unwrap();
    }
    sim.put(&key, 192);
    let (head, body) = sim.finish(whole).unwrap();
    assert_eq!((head.status, body.len()), (200, 192));
    assert_eq!(sim.summary().retries, 1);
}

#[test]
fn concurrent_first_reads_share_one_fetch() {
    let mut sim = Simulator::new(1, Options::scenario());
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 256);
    let first = sim.start(Request::get(key.clone()));
    let second = sim.start(Request::get(key));
    sim.finish(first).unwrap();
    sim.finish(second).unwrap();
    assert_eq!(sim.summary().origin_requests, 1);
}

#[test]
fn the_fill_budget_caps_blocks_filling_at_once() {
    let mut options = Options::scenario();
    options.fill_budget_blocks = 1;
    options.immutable_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 256);
    sim.read(Request::get(key)).unwrap();
    assert_eq!(sim.summary().written_bytes, 64);
}

#[test]
fn the_home_forgets_its_least_recently_used_metadata() {
    let mut options = Options::scenario();
    options.metadata_capacity = 2;
    // The gateway remembers only the last object, so the HEADs reach the home.
    options.gateway_metadata_capacity = 1;
    let mut sim = Simulator::new(1, options);
    let [a, b, c] = ["a", "b", "c"].map(|name| key(IMMUTABLE_BUCKET, name));
    for key in [&a, &b, &c] {
        sim.put(key, 100);
    }
    let head = |sim: &mut Simulator, key: &ObjectKey| {
        let before = sim.summary().origin_requests;
        sim.read(Request::head(key.clone())).unwrap();
        sim.summary().origin_requests - before
    };
    assert_eq!(head(&mut sim, &a), 1);
    assert_eq!(head(&mut sim, &b), 1);
    assert_eq!(head(&mut sim, &a), 0);
    // Knowing c makes room by forgetting b, the least recently used.
    assert_eq!(head(&mut sim, &c), 1);
    assert_eq!(head(&mut sim, &a), 0);
    assert_eq!(head(&mut sim, &b), 1);
}

/// A write through the home replaces what it knew, however long the
/// bucket's metadata would otherwise stay fresh. Blocks are stored on the
/// first read, so a stale read would be a pure hit that no `If-Match`
/// fill could catch.
#[test]
fn a_write_through_the_home_drops_its_metadata() {
    let mut options = Options::scenario();
    options.ttl = 1_000_000;
    options.ttl_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 100);
    sim.read(Request::get(key.clone())).unwrap();
    sim.write_through(&key, 120).unwrap();
    let before = sim.summary().origin_requests;
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
    // The old version's blocks remain, but the first fetch still relays
    // the read: one S3 request, not a HEAD and then fills.
    assert_eq!(sim.summary().origin_requests - before, 1);
}

/// A read that races a write may see the old version, but the home must
/// not keep what its first fetch learned before the write.
#[test]
fn a_first_fetch_sent_before_a_write_is_not_kept() {
    let mut options = Options::scenario();
    options.ttl = 1_000_000;
    options.ttl_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 100);
    // The first fetch leaves the node on the third tick and S3 answers it on the fourth.
    let racing = sim.start(Request::get(key.clone()));
    for _ in 0..4 {
        sim.step().unwrap();
    }
    sim.write_at_once(0, &key, 120).unwrap();
    let (_, old) = sim.finish(racing).unwrap();
    assert_eq!(old.len(), 100);
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
}

/// Four nodes; 64-byte blocks, 2-block chunks: a 2 KiB object has 16
/// chunks placed across the cluster.
fn cluster() -> Options {
    let mut options = Options::scenario();
    options.nodes = 4;
    options.chunk_blocks = 2;
    options
}

fn nodes_read(sim: &Simulator) -> usize {
    sim.summary()
        .node_reads
        .iter()
        .filter(|&&reads| reads > 0)
        .count()
}

/// A home that knows the object answers with its metadata, and the
/// gateway reads each chunk from its owner.
#[test]
fn a_large_object_spreads_across_owners() {
    let mut options = cluster();
    // The gateway keeps no metadata, so every read asks the home.
    options.gateway_metadata_ttl = 0;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "large");
    sim.put(&key, 2_048);
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(
        nodes_read(&sim),
        1,
        "the cold read is the home's first fetch"
    );
    let (_, body) = sim.read(Request::get(key)).unwrap();
    assert_eq!(body.len(), 2_048);
    assert!(
        nodes_read(&sim) >= 3,
        "reads per node: {:?}",
        sim.summary().node_reads
    );
}

#[test]
fn the_gateway_answers_heads_and_preconditions_itself() {
    let mut sim = Simulator::new(1, cluster());
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 300);
    let (head, _) = sim.read(Request::get(key.clone())).unwrap();
    let reads = sim.summary().node_reads.iter().sum::<u64>();
    assert_eq!(sim.read(Request::head(key.clone())).unwrap().0.status, 200);
    let unchanged = Request {
        if_none_match: head.etag,
        ..Request::get(key)
    };
    assert_eq!(sim.read(unchanged).unwrap().0.status, 304);
    assert_eq!(sim.summary().node_reads.iter().sum::<u64>(), reads);
}

/// An object changed behind the cache: the owner's `If-Match` fill fails,
/// and the gateway forgets the ETag and reads the new version.
#[test]
fn a_gateway_with_a_stale_etag_reads_the_new_version() {
    let mut options = cluster();
    options.ttl = 1_000_000;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 1_024);
    sim.read(Request::get(key.clone())).unwrap();
    sim.put(&key, 1_000);
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 1_000);
    assert!(sim.summary().retries >= 1);
}

/// A read of middle chunks only reaches their owners. When those find the
/// ETag stale, the home must hear of it and revalidate, so one retry
/// suffices; otherwise it hands out the old ETag again until the read gives
/// up and goes to S3 directly. The simulator found the missing report as a
/// livelock under zero network delay.
#[test]
fn a_stale_report_makes_the_home_revalidate() {
    let mut options = cluster();
    options.ttl = 1_000_000;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 2_048);
    sim.read(Request::get(key.clone())).unwrap();
    sim.put(&key, 2_048);
    // Chunk 4 alone, so one owner answers and one stale message comes back.
    let middle = Request {
        range: Some(ByteRange::Inclusive {
            first: 512,
            last: 639,
        }),
        ..Request::get(key.clone())
    };
    let (head, _) = sim.read(middle).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(sim.summary().retries, 1);
}

/// Every attempt to read a middle chunk finds the object changed: each
/// stale report makes the home revalidate and hand out a newer ETag, which
/// is stale again by the time its owner fills. After a few retries the
/// gateway reads S3 directly, and the read finishes.
#[test]
fn a_read_of_an_object_that_keeps_changing_finishes() {
    let mut options = cluster();
    options.ttl = 1_000_000;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 2_048);
    sim.read(Request::get(key.clone())).unwrap();
    let middle = Request {
        range: Some(ByteRange::Inclusive {
            first: 512,
            last: 639,
        }),
        ..Request::get(key.clone())
    };
    let request = sim.start(middle);
    for _ in 0..1_000 {
        if let Some((head, body)) = sim.take_answer(request) {
            assert_eq!((head.status, body.len()), (206, 128));
            return;
        }
        sim.put(&key, 2_048);
        sim.step().unwrap();
    }
    panic!("the read never finished");
}

/// A home's answer that reaches the gateway after a write through that
/// gateway predates the write, so it must not restore the gateway's entry.
#[test]
fn an_answer_older_than_a_write_is_not_cached() {
    let mut options = Options::scenario();
    options.ttl = 1_000_000;
    options.ttl_admit_on_first_read = true;
    options.gateway_metadata_capacity = 1;
    let mut sim = Simulator::new(1, options);
    let [key, other] = ["k", "other"].map(|name| key(TTL_BUCKET, name));
    sim.put(&key, 100);
    sim.put(&other, 100);
    sim.read(Request::get(key.clone())).unwrap();
    // The gateway forgets `key`; the home still knows it.
    sim.read(Request::get(other)).unwrap();
    let racing = sim.start(Request::get(key.clone()));
    // The home answers on the third tick; the answer reaches the gateway after it.
    for _ in 0..3 {
        sim.step().unwrap();
    }
    sim.write_at_once(0, &key, 120).unwrap();
    sim.finish(racing).unwrap();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
}

/// Requests that queued behind a first fetch share its 404 if they arrived
/// before it left, since S3 checked after they did. Later ones need a fetch
/// of their own, which they share in turn. The simulator found the
/// alternative, one request answered per S3 round trip, as a metastable
/// collapse: client retries outran the queue.
#[test]
fn requests_queued_behind_a_404_share_it() {
    let mut sim = Simulator::new(1, Options::scenario());
    let key = key(TTL_BUCKET, "absent");
    let mut reads: Vec<u64> = (0..5)
        .map(|_| sim.start(Request::get(key.clone())))
        .collect();
    // These five reach the home two ticks after the first fetch leaves.
    for _ in 0..2 {
        sim.step().unwrap();
    }
    reads.extend((0..5).map(|_| sim.start(Request::get(key.clone()))));
    for read in reads {
        assert_eq!(sim.finish(read).unwrap().0.status, 404);
    }
    assert_eq!(sim.summary().origin_requests, 2);
}

/// The owner of a chunk is cut off: the gateway times out, reads the chunk
/// from the next rendezvous candidate, and the client never notices. With no
/// suspect period, only the gateway's record of nodes tried moves it on.
#[test]
fn reads_fail_over_from_a_partitioned_owner() {
    for suspect_ttl in [0, 100] {
        let mut options = cluster();
        options.suspect_ttl = suspect_ttl;
        let mut sim = Simulator::new(1, options);
        let key = key(IMMUTABLE_BUCKET, "k");
        sim.put(&key, 2_048);
        sim.read(Request::get(key.clone())).unwrap();
        for node in 0..4 {
            sim.partition(node, 5_000);
            let started = sim.summary().ticks;
            let (head, body) = sim.read(Request::get(key.clone())).unwrap();
            let context = format!("node {node} cut off, suspect ttl {suspect_ttl}");
            assert_eq!((head.status, body.len()), (200, 2_048), "{context}");
            assert!(sim.summary().ticks - started < 5_000, "{context}");
            assert_eq!(sim.summary().client_retries, 0, "{context}");
            sim.partition(node, 0);
        }
    }
}

/// The home and its next candidate are both cut off, and suspicion lapses
/// at once: only the gateway's record of nodes tried keeps it from going
/// back to one of them.
#[test]
fn failover_moves_past_every_node_tried() {
    let mut options = cluster();
    options.suspect_ttl = 0;
    let mut sim = Simulator::new(1, options);
    let key = key(IMMUTABLE_BUCKET, "small");
    sim.put(&key, 100);
    let candidates = sim.home_candidates(&key);
    sim.partition(candidates[0], 1_000_000);
    sim.partition(candidates[1], 1_000_000);
    let started = sim.summary().ticks;
    let (head, body) = sim.read(Request::get(key)).unwrap();
    assert_eq!((head.status, body.len()), (200, 100));
    assert_eq!(sim.summary().client_retries, 0);
    assert!(sim.summary().ticks - started < 10_000);
}

/// With the home cut off, a candidate answers its reads, but writes reach
/// only the home: the candidate must not keep metadata a write would leave
/// stale. A `HeadObject` shows it, since metadata alone answers it.
#[test]
fn a_candidate_standing_in_for_the_home_keeps_no_metadata() {
    let mut options = cluster();
    options.ttl = 1_000_000;
    options.suspect_ttl = 1_000_000;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 100);
    let home = sim.home(&key);
    sim.partition(home, 1_000_000);
    sim.read(Request::get(key.clone())).unwrap();
    sim.write_through(&key, 120).unwrap();
    let (head, _) = sim.read(Request::head(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(head.content_length, 120);
}

/// A node that stored a 200-byte object in four blocks, then stopped and
/// started again over its slot table. The gateway keeps no metadata, so
/// every read goes through the home.
fn restarted(crash: bool) -> (Simulator, ObjectKey) {
    let mut options = Options::scenario();
    options.ttl_admit_on_first_read = true;
    options.gateway_metadata_ttl = 0;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 200);
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(sim.summary().written_bytes, 200);
    match crash {
        true => sim.crash(0).unwrap(),
        false => sim.shut_down(0).unwrap(),
    }
    sim.restart(0).unwrap();
    (sim, key)
}

#[test]
fn a_clean_restart_keeps_the_cache_warm() {
    let (mut sim, key) = restarted(false);
    let before = sim.summary();
    let (_, body) = sim.read(Request::get(key)).unwrap();
    let after = sim.summary();
    assert_eq!(body.len(), 200);
    assert_eq!(after.hit_bytes - before.hit_bytes, 200);
    // A HEAD for the metadata the restart lost.
    assert_eq!(after.origin_requests - before.origin_requests, 1);
    assert_eq!(after.verified_blocks, 0);
}

#[test]
fn after_a_crash_each_block_is_verified_before_it_is_served() {
    let (mut sim, key) = restarted(true);
    let before = sim.summary();
    sim.read(Request::get(key.clone())).unwrap();
    let after = sim.summary();
    assert_eq!(after.hit_bytes - before.hit_bytes, 200);
    assert_eq!(after.origin_requests - before.origin_requests, 1);
    assert_eq!((after.verified_blocks, after.corrupt_blocks), (4, 0));
    sim.read(Request::get(key)).unwrap();
    assert_eq!(sim.summary().verified_blocks, 4);
}

#[test]
fn a_damaged_block_is_read_again_from_s3() {
    let (mut sim, key) = restarted(true);
    assert!(sim.damage(0, &key, 1));
    let before = sim.summary();
    let (_, body) = sim.read(Request::get(key)).unwrap();
    let after = sim.summary();
    assert_eq!(body.len(), 200);
    assert_eq!(after.corrupt_blocks, 1);
    assert_eq!(after.hit_bytes - before.hit_bytes, 200 - 64);
    // The HEAD, and a fill of the damaged block.
    assert_eq!(after.origin_requests - before.origin_requests, 2);
}

#[test]
fn a_clean_shutdown_vouches_only_for_blocks_it_verified() {
    let (mut sim, key) = restarted(true);
    assert!(sim.damage(0, &key, 1));
    sim.shut_down(0).unwrap();
    sim.restart(0).unwrap();
    let (_, body) = sim.read(Request::get(key)).unwrap();
    assert_eq!(body.len(), 200);
    assert_eq!(sim.summary().corrupt_blocks, 1);
}

#[test]
fn verified_blocks_stay_trusted_across_clean_restarts() {
    let (mut sim, key) = restarted(true);
    sim.read(Request::get(key.clone())).unwrap();
    assert_eq!(sim.summary().verified_blocks, 4);
    for _ in 0..2 {
        sim.shut_down(0).unwrap();
        sim.restart(0).unwrap();
        let before = sim.summary();
        sim.read(Request::get(key.clone())).unwrap();
        let after = sim.summary();
        assert_eq!(after.hit_bytes - before.hit_bytes, 200);
        assert_eq!(after.verified_blocks, 4);
    }
}

#[test]
fn a_crash_during_a_fill_leaves_no_record_of_it() {
    let mut options = Options::scenario();
    options.immutable_admit_on_first_read = true;
    options.disk_delay_max = 50;
    let mut sim = Simulator::new(1, options);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 200);
    let request = sim.start(Request::get(key.clone()));
    while sim.writes_in_progress(0) < 4 {
        sim.step().unwrap();
    }
    sim.crash(0).unwrap();
    sim.restart(0).unwrap();
    assert_eq!(sim.finish(request).unwrap().1.len(), 200);
    // The torn blocks were never recorded, so none needs verifying.
    let (_, body) = sim.read(Request::get(key)).unwrap();
    assert_eq!(body.len(), 200);
    assert_eq!(sim.summary().verified_blocks, 0);
}

/// Each node in turn sends a body that ends partway. The gateway reads the
/// rest from the next candidate, and the client never notices.
#[test]
fn a_body_cut_partway_resumes_from_the_next_candidate() {
    let mut sim = Simulator::new(1, cluster());
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 2_048);
    sim.read(Request::get(key.clone())).unwrap();
    for node in 0..4 {
        sim.cut_response(node, 10);
        let (head, body) = sim.read(Request::get(key.clone())).unwrap();
        assert_eq!((head.status, body.len()), (200, 2_048), "node {node} cut");
    }
    let summary = sim.summary();
    assert_eq!(summary.cut_bodies, 4);
    assert_eq!((summary.early_ends, summary.client_retries), (0, 0));
}

/// The object changes behind the cache, and the home's body ends partway
/// through a response of the old version. The rest of the old version is
/// gone from S3, so the response ends early. The gateway tells the home the
/// old ETag is stale, so the client's retry reads the new version though
/// the home's metadata is still within its TTL.
#[test]
fn a_response_whose_object_changed_after_it_started_ends_early() {
    let mut options = cluster();
    options.ttl_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(TTL_BUCKET, "k");
    sim.put(&key, 2_048);
    sim.read(Request::get(key.clone())).unwrap();
    sim.read(Request::get(key.clone())).unwrap();
    sim.put(&key, 2_048);
    sim.cut_response(sim.home(&key), 10);
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    assert_eq!((head.status, body.len()), (200, 2_048));
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    let summary = sim.summary();
    assert_eq!(summary.cut_bodies, 1);
    assert_eq!((summary.early_ends, summary.client_retries), (1, 1));
}

/// A cold read's body from the home ends partway, and its rest goes to
/// several nodes. The first of them is forwarding when a later one finds
/// the object changed. The response ends early only after that forward
/// finishes, since the driver is still copying its body.
#[test]
fn an_early_end_waits_for_the_forward_in_progress() {
    let mut options = cluster();
    options.gateway_metadata_ttl = 0;
    options.suspect_ttl = 0;
    let sim = Simulator::new(1, options.clone());
    // A key whose second chunk has an owner other than the home and the
    // home's next candidate, which stand in for the first chunk.
    let (key, cut_off) = (0..)
        .map(|index| key(TTL_BUCKET, &format!("k{index}")))
        .find_map(|key| {
            let candidates = sim.home_candidates(&key);
            let owner = sim.owner(&key, 2_048, 2);
            (owner != candidates[0] && owner != candidates[1]).then_some((key, owner))
        })
        .unwrap();
    let mut sim = Simulator::new(1, options);
    sim.put(&key, 2_048);
    sim.partition(cut_off, 1_000_000);
    sim.cut_response(sim.home(&key), 10);
    let request = sim.start(Request::get(key.clone()));
    while sim.summary().cut_bodies == 0 {
        sim.step().unwrap();
    }
    sim.hold_forwards();
    while sim.held_forwards() == 0 {
        sim.step().unwrap();
    }
    // The part sent to the cut-off owner times out; its next candidate
    // finds the object changed.
    sim.put(&key, 2_048);
    for _ in 0..2_000 {
        sim.step().unwrap();
    }
    assert_eq!(sim.summary().early_ends, 0);
    sim.release_forwards();
    sim.partition(cut_off, 0);
    let (head, body) = sim.finish(request).unwrap();
    assert_eq!((head.status, body.len()), (200, 2_048));
    assert_eq!(sim.summary().early_ends, 1);
}

/// S3's body ends partway through the home's first fetch. The home stores
/// none of it, and the gateway reads the rest from the next candidate.
#[test]
fn a_cut_s3_body_stores_nothing_and_the_read_resumes() {
    let mut options = cluster();
    options.immutable_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 100);
    sim.cut_origin_response(sim.home(&key), 10);
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    assert_eq!((head.status, body.len()), (200, 100));
    let summary = sim.summary();
    assert_eq!((summary.early_ends, summary.client_retries), (0, 0));
    assert_eq!(summary.written_bytes, 0);
    sim.read(Request::get(key)).unwrap();
    assert_eq!(sim.summary().written_bytes, 100);
}

/// The home saves an immutable object's metadata, so after a restart, clean
/// or not, a gateway that never saw the object reads it without any S3
/// request.
#[test]
fn an_immutable_object_read_after_a_restart_costs_no_s3_request() {
    for crash in [false, true] {
        let mut options = Options::scenario();
        options.gateways = 2;
        options.immutable_admit_on_first_read = true;
        let mut sim = Simulator::new(1, options);
        let key = key(IMMUTABLE_BUCKET, "k");
        sim.put(&key, 200);
        sim.read(Request::get(key.clone())).unwrap();
        // The metadata file syncs within a disk delay.
        for _ in 0..10 {
            sim.step().unwrap();
        }
        match crash {
            true => sim.crash(0).unwrap(),
            false => sim.shut_down(0).unwrap(),
        }
        sim.restart(0).unwrap();
        let before = sim.summary();
        let (_, body) = sim.read_through(1, Request::get(key)).unwrap();
        let after = sim.summary();
        assert_eq!(body.len(), 200, "crash: {crash}");
        assert_eq!(
            after.origin_requests - before.origin_requests,
            0,
            "crash: {crash}"
        );
        assert_eq!(after.hit_bytes - before.hit_bytes, 200, "crash: {crash}");
    }
}

/// A write through the home drops the metadata it saved, so a restart does
/// not bring the old version back. The old version's blocks stay stored,
/// so no `If-Match` fill would notice.
#[test]
fn a_write_through_the_home_outlasts_a_restart() {
    let mut options = Options::scenario();
    options.immutable_admit_on_first_read = true;
    let mut sim = Simulator::new(1, options);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 100);
    sim.read(Request::get(key.clone())).unwrap();
    sim.write_through(&key, 120).unwrap();
    sim.shut_down(0).unwrap();
    sim.restart(0).unwrap();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
}

/// Three nodes that gossip, with the scenario's timings: probes every 50
/// ticks, suspicion for 100, and a grace period of 500.
fn gossiping() -> Options {
    let mut options = Options::scenario();
    options.nodes = 3;
    options.membership = true;
    options
}

fn run(sim: &mut Simulator, ticks: u64) {
    for _ in 0..ticks {
        sim.step().unwrap();
    }
}

/// A node that stays down past the grace period leaves every ring, and a
/// gateway takes up the new ring once an answer names its version.
#[test]
fn a_node_down_past_its_grace_period_leaves_every_ring() {
    let mut sim = Simulator::new(1, gossiping());
    run(&mut sim, 300);
    for node in 0..3 {
        assert_eq!(sim.node_ring(node), Some(vec![0, 1, 2]), "node {node}");
    }
    sim.crash(2).unwrap();
    // Declared down within about 300 ticks, and kept for 500 more.
    run(&mut sim, 500);
    assert_eq!(sim.node_ring(0), Some(vec![0, 1, 2]));
    run(&mut sim, 600);
    for node in 0..2 {
        assert_eq!(sim.node_ring(node), Some(vec![0, 1]), "node {node}");
    }
    assert_eq!(sim.gateway_ring(0), [0, 1, 2]);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 100);
    let (head, _) = sim.read(Request::get(key)).unwrap();
    assert_eq!(head.status, 200);
    run(&mut sim, 50);
    assert_eq!(sim.gateway_ring(0), [0, 1]);
    assert_eq!(sim.summary().ring_fetches, 1);
}

/// A home rewrites its metadata file, least recently used first, once the
/// file holds twice the metadata capacity and at a clean shutdown: the
/// file stays bounded, and a restart keeps the most recently used entries,
/// such as one served from memory since it was fetched, in the order it
/// used them, so new keys push out the least recently used first.
#[test]
fn the_metadata_file_keeps_the_most_recently_used() {
    let mut options = Options::scenario();
    options.nodes = 1;
    options.metadata_capacity = 4;
    options.gateway_metadata_capacity = 1;
    let mut sim = Simulator::new(1, options);
    let keys: Vec<ObjectKey> = (0..23)
        .map(|index| key(IMMUTABLE_BUCKET, &format!("k{index}")))
        .collect();
    for key in &keys[..20] {
        sim.put(key, 100);
        sim.read(Request::head(key.clone())).unwrap();
        assert!(sim.metadata_file_entries(0) <= 8);
    }
    // The home holds k16 to k19; k16, served again, is used after k19,
    // and k20 then takes k17's place.
    let before = sim.summary().origin_requests;
    sim.read(Request::head(keys[16].clone())).unwrap();
    assert_eq!(sim.summary().origin_requests, before);
    sim.put(&keys[20], 100);
    sim.read(Request::head(keys[20].clone())).unwrap();
    sim.shut_down(0).unwrap();
    sim.restart(0).unwrap();
    // k18 and k19, used longest ago, make room for k21 and k22.
    for key in &keys[21..] {
        sim.put(key, 100);
        sim.read(Request::head(key.clone())).unwrap();
    }
    let before = sim.summary().origin_requests;
    let (head, _) = sim.read(Request::head(keys[16].clone())).unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(sim.summary().origin_requests, before);
}

/// A purge whose forgetting fills the metadata file past twice the
/// capacity rewrites the file without the purged key, so no restart brings
/// its metadata back.
#[test]
fn a_purge_that_rewrites_the_metadata_file_stays_purged() {
    let mut options = Options::scenario();
    options.nodes = 1;
    options.metadata_capacity = 4;
    options.gateway_metadata_capacity = 1;
    let mut sim = Simulator::new(1, options);
    let keys: Vec<ObjectKey> = (0..8)
        .map(|index| key(IMMUTABLE_BUCKET, &format!("k{index}")))
        .collect();
    for key in &keys {
        sim.put(key, 100);
        sim.read(Request::head(key.clone())).unwrap();
    }
    assert_eq!(sim.metadata_file_entries(0), 8);
    sim.purge(&keys[7]).unwrap();
    // The gateway, which keeps one object's metadata, keeps k0's instead.
    sim.read(Request::head(keys[0].clone())).unwrap();
    sim.crash(0).unwrap();
    sim.restart(0).unwrap();
    let before = sim.summary().origin_requests;
    sim.read(Request::head(keys[7].clone())).unwrap();
    assert_eq!(sim.summary().origin_requests, before + 1);
}

/// Runs until `request` is answered, and returns its status and how many
/// ticks the answer took.
fn answer_time(sim: &mut Simulator, request: u64) -> (u16, u64) {
    let started = sim.now();
    loop {
        if let Some((head, _)) = sim.take_answer(request) {
            return (head.status, sim.now() - started);
        }
        assert!(
            sim.now() < started + 100_000,
            "request {request} unanswered"
        );
        sim.step().unwrap();
    }
}

/// A key whose home is `home`.
fn key_homed_on(sim: &Simulator, home: usize) -> ObjectKey {
    (0..)
        .map(|index| key(IMMUTABLE_BUCKET, &format!("k{index}")))
        .find(|key| sim.home(key) == home)
        .unwrap()
}

/// A read whose home has crashed goes to the next candidate as soon as
/// the connection is refused, well within the node timeout.
#[test]
fn a_crashed_home_costs_a_read_no_timeout() {
    let mut sim = Simulator::new(1, cluster());
    let key = key_homed_on(&sim, 1);
    sim.put(&key, 100);
    sim.crash(1).unwrap();
    let request = sim.start(Request::get(key));
    let (status, ticks) = answer_time(&mut sim, request);
    assert_eq!(status, 200);
    assert!(ticks < 100, "the read took {ticks} ticks");
}

/// A gateway routes around a node that refused a connection for the
/// suspect window, so a second read within it goes straight to the next
/// candidate.
#[test]
fn a_refused_node_is_skipped_for_the_suspect_window() {
    let mut sim = Simulator::new(1, cluster());
    let key = key_homed_on(&sim, 1);
    sim.put(&key, 100);
    sim.crash(1).unwrap();
    for _ in 0..2 {
        let (head, _) = sim.read(Request::get(key.clone())).unwrap();
        assert_eq!(head.status, 200);
    }
    assert_eq!(sim.summary().refused, 1);
}

/// Once membership holds a cut-off node down, any node's answer tells the
/// gateway, which then routes the node's reads to the next candidate
/// without waiting for the node timeout.
#[test]
fn a_node_held_down_is_routed_around() {
    let mut sim = Simulator::new(1, gossiping());
    run(&mut sim, 300);
    let cut = key_homed_on(&sim, 2);
    let other = key_homed_on(&sim, 0);
    sim.put(&cut, 100);
    sim.put(&other, 100);
    sim.partition(2, 100_000);
    // Declared down within about 300 ticks, and kept in the ring for 500.
    run(&mut sim, 400);
    let request = sim.start(Request::get(other));
    assert_eq!(answer_time(&mut sim, request).0, 200);
    run(&mut sim, 20);
    let request = sim.start(Request::get(cut));
    let (status, ticks) = answer_time(&mut sim, request);
    assert_eq!(status, 200);
    assert!(ticks < 100, "the read took {ticks} ticks");
}

/// A read waiting on a node when the gateway learns membership holds it
/// down moves to the next candidate at once.
#[test]
fn a_read_waiting_on_a_node_held_down_moves_on() {
    let mut sim = Simulator::new(1, gossiping());
    run(&mut sim, 300);
    let cut = key_homed_on(&sim, 2);
    let other = key_homed_on(&sim, 0);
    sim.put(&cut, 100);
    sim.put(&other, 100);
    sim.partition(2, 100_000);
    let waiting = sim.start(Request::get(cut));
    run(&mut sim, 400);
    let request = sim.start(Request::get(other));
    assert_eq!(answer_time(&mut sim, request).0, 200);
    let (status, ticks) = answer_time(&mut sim, waiting);
    assert_eq!(status, 200);
    // The node timeout is 1,000 ticks from when the read was sent.
    assert!(ticks < 500, "the read took {ticks} more ticks");
}

/// A node that comes back within the grace period moves no ownership.
#[test]
fn a_node_back_within_its_grace_period_changes_no_ring() {
    let mut sim = Simulator::new(1, gossiping());
    run(&mut sim, 300);
    sim.crash(2).unwrap();
    run(&mut sim, 400);
    sim.restart(2).unwrap();
    run(&mut sim, 1_000);
    for node in 0..3 {
        assert_eq!(sim.node_ring(node), Some(vec![0, 1, 2]), "node {node}");
    }
    assert_eq!(sim.summary().ring_changes, 0);
}

/// Every node a gateway's ring names fails for good while a new node
/// serves. The gateway finds the ring through the nodes it knows, and its
/// client's retry succeeds.
#[test]
fn a_gateway_whose_ring_names_only_gone_nodes_finds_the_ring() {
    let mut options = gossiping();
    options.nodes = 2;
    let mut sim = Simulator::new(1, options);
    run(&mut sim, 300);
    sim.add_node().unwrap();
    run(&mut sim, 300);
    sim.fail(0).unwrap();
    sim.fail(1).unwrap();
    run(&mut sim, 1_500);
    assert_eq!(sim.node_ring(2), Some(vec![2]));
    assert_eq!(sim.gateway_ring(0), [0, 1]);
    let key = key(IMMUTABLE_BUCKET, "k");
    sim.put(&key, 100);
    let (head, body) = sim.read(Request::get(key)).unwrap();
    assert_eq!((head.status, body.len()), (200, 100));
    assert_eq!(sim.gateway_ring(0), [2]);
}
