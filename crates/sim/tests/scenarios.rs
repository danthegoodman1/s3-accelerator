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
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
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
    sim.write_through(&key, 120).unwrap();
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
    sim.write_through(&key, 120).unwrap();
    sim.finish(racing).unwrap();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 120);
}
