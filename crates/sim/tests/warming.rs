//! Warming on write: a home that passes an upload to S3 keeps its region
//! of the body, checks with a HEAD that S3 still holds the version, and
//! stores the blocks.

use s3_accelerator_core::s3::{ObjectKey, Request};
use s3_accelerator_sim::{Options, Simulator, TTL_BUCKET};

fn key() -> ObjectKey {
    ObjectKey {
        bucket: TTL_BUCKET.into(),
        key: "k".into(),
    }
}

/// One node whose TTL bucket warms on write, with metadata fresh for the
/// whole run: 64-byte blocks in 4-block chunks, so a 600-byte object's
/// home holds its first and last chunks.
fn options() -> Options {
    let mut options = Options::scenario();
    options.ttl = 1_000_000;
    options.ttl_warm_on_write = true;
    options
}

/// The first read after an upload through the home comes from disk: the
/// home stored the region it holds and knows the metadata, so only the
/// block between its chunks comes from S3.
#[test]
fn an_upload_through_its_home_is_read_from_disk() {
    let mut sim = Simulator::new(1, options());
    let key = key();
    sim.write_through(&key, 600).unwrap();
    assert_eq!(sim.summary().warmed_uploads, 1);
    let before = sim.summary();
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 600);
    let after = sim.summary();
    // Chunk 0, bytes 0..256, and the blocks overlapping the last 256
    // bytes, 320..600; block 4 comes from S3.
    assert_eq!(after.hit_bytes - before.hit_bytes, 536);
    assert_eq!(after.origin_requests - before.origin_requests, 1);
}

/// S3 took another write before the home's HEAD reached it, so the HEAD
/// names another version: the home stores nothing, and the read returns
/// the version S3 holds.
#[test]
fn an_upload_replaced_before_its_check_is_not_stored() {
    let mut sim = Simulator::new(1, options());
    let key = key();
    sim.write_before_next_answer(&key, 100);
    sim.write_through(&key, 600).unwrap();
    assert_eq!(sim.summary().warmed_uploads, 0);
    let (head, body) = sim.read(Request::get(key.clone())).unwrap();
    let current = sim.origin().current(&key).unwrap();
    assert_eq!(head.etag.as_ref(), Some(&current.etag));
    assert_eq!(body.len(), 100);
}
