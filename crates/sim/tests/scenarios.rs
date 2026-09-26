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
