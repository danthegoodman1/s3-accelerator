//! Metadata prefetch: a home that reads a format's spot learns where the
//! object's metadata lies, and fills it before the reader asks.

use s3_accelerator_core::formats::Format;
use s3_accelerator_core::s3::{ByteRange, ObjectKey, Request};
use s3_accelerator_sim::{IMMUTABLE_BUCKET, Options, Simulator};
use std::ops::Range;

fn key(name: &str) -> ObjectKey {
    ObjectKey {
        bucket: IMMUTABLE_BUCKET.into(),
        key: name.into(),
    }
}

/// The span of `key`'s metadata, from S3's bytes.
fn metadata(sim: &Simulator, key: &ObjectKey) -> Range<u64> {
    let object = sim.origin().current(key).unwrap();
    let format = Format::of(key).unwrap();
    let spot = format.spot(object.size).unwrap();
    format.metadata(object.size, &object.bytes(spot)).unwrap()
}

/// A reader of a 3,000-byte object asks for its format's spot, then for
/// the metadata the spot names. One node, with 64-byte blocks and a
/// doorkeeper that keeps blocks off disk until their second read: the
/// second read comes from disk all the same, with no request to S3.
fn check(name: &str, spot: ByteRange) {
    let mut sim = Simulator::new(1, Options::scenario());
    let key = key(name);
    sim.put(&key, 3_000);
    let read = |range| Request {
        range: Some(range),
        ..Request::get(key.clone())
    };
    sim.read(read(spot)).unwrap();
    let span = metadata(&sim, &key);
    let before = sim.summary();
    let footer = ByteRange::Inclusive {
        first: span.start,
        last: span.end - 1,
    };
    let (head, body) = sim.read(read(footer)).unwrap();
    assert_eq!(head.status, 206);
    assert_eq!(body.len() as u64, span.end - span.start);
    let after = sim.summary();
    assert_eq!(after.origin_requests, before.origin_requests);
    assert_eq!(after.hit_bytes - before.hit_bytes, span.end - span.start);
    assert!(after.prefetched_blocks > 0);
}

#[test]
fn a_parquet_footer_is_on_disk_once_its_length_is_read() {
    check("table/part-0.parquet", ByteRange::Suffix { length: 8 });
}

#[test]
fn an_orc_tail_is_on_disk_once_its_postscript_is_read() {
    check("table/part-0.orc", ByteRange::Suffix { length: 256 });
}

#[test]
fn a_safetensors_header_is_on_disk_once_its_length_is_read() {
    check(
        "model.safetensors",
        ByteRange::Inclusive { first: 0, last: 7 },
    );
}

/// A home that already knows the object stores the spot's blocks on their
/// first read, past the doorkeeper, and then prefetches the footer.
#[test]
fn a_known_object_prefetches_from_its_first_spot_read() {
    let mut sim = Simulator::new(1, Options::scenario());
    let key = key("table/part-1.parquet");
    sim.put(&key, 3_000);
    sim.read(Request::head(key.clone())).unwrap();
    let read = |range| Request {
        range: Some(range),
        ..Request::get(key.clone())
    };
    sim.read(read(ByteRange::Suffix { length: 8 })).unwrap();
    let span = metadata(&sim, &key);
    let before = sim.summary();
    let footer = ByteRange::Inclusive {
        first: span.start,
        last: span.end - 1,
    };
    sim.read(read(footer)).unwrap();
    assert_eq!(sim.summary().origin_requests, before.origin_requests);
}
