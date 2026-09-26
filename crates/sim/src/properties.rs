//! What every run must satisfy.

use crate::origin::{Origin, respond};
use s3_accelerator_core::node::StoredBlock;
use s3_accelerator_core::s3::{Method, Request, ResponseHead};

/// A response must equal S3's response to the same request while its key
/// held some state between `from`, the request's issue tick less the
/// bucket's staleness bound, and `to`, the tick it was answered.
pub fn check_response(
    origin: &Origin,
    request: &Request,
    from: u64,
    to: u64,
    head: &ResponseHead,
    body: &[u8],
) -> Result<(), String> {
    if request.method == Method::Get && head.content_length != body.len() as u64 {
        return Err(format!(
            "Content-Length {} with a {}-byte body",
            head.content_length,
            body.len()
        ));
    }
    let states = origin.states_during(&request.key, from, to);
    let matches = |state| {
        let (expected_head, expected_body) = respond(state, request);
        expected_head == *head && expected_body == body
    };
    if states.into_iter().any(matches) {
        return Ok(());
    }
    Err(format!(
        "{} {:?} {:?} ({} bytes) for {request:?} matches no state of the key in ticks {from}..={to}",
        head.status,
        head.etag,
        head.content_range,
        body.len(),
    ))
}

/// A response that ended early must be S3's response to the same request,
/// cut short, while its key held some state between `from` and `to`.
pub fn check_early_end(
    origin: &Origin,
    request: &Request,
    from: u64,
    to: u64,
    head: &ResponseHead,
    body: &[u8],
) -> Result<(), String> {
    let states = origin.states_during(&request.key, from, to);
    let matches = |state| {
        let (expected_head, expected_body) = respond(state, request);
        expected_head == *head && expected_body.starts_with(body)
    };
    if states.into_iter().any(matches) {
        return Ok(());
    }
    Err(format!(
        "{} {:?} {:?} cut at {} bytes for {request:?} begins no state of the key in ticks {from}..={to}",
        head.status,
        head.etag,
        head.content_range,
        body.len(),
    ))
}

/// A stored block must hold exactly the bytes of the version it is keyed
/// by, at its index.
pub fn check_block(
    origin: &Origin,
    block_size: u64,
    block: &StoredBlock,
    bytes: &[u8],
) -> Result<(), String> {
    let Some(object) = origin.version(block.etag) else {
        return Err(format!("stored block of unknown version {:?}", block.etag));
    };
    if object.key != *block.key {
        return Err(format!("{:?} stored under {:?}", block.etag, block.key));
    }
    let start = block.index * block_size;
    let end = (start + block_size).min(object.size);
    if start >= end || end - start != block.len {
        return Err(format!(
            "block {} of {:?} stored as {} bytes",
            block.index, block.etag, block.len
        ));
    }
    if object.bytes(start..end) != bytes {
        return Err(format!(
            "block {} of {:?} at {:?} holds the wrong bytes",
            block.index, block.etag, block.location
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prng::Prng;
    use s3_accelerator_core::s3::{ByteRange, ETag, ObjectKey};
    use s3_accelerator_core::store::Location;

    fn key() -> ObjectKey {
        ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        }
    }

    fn get(range: Option<ByteRange>) -> Request {
        Request {
            range,
            ..Request::get(key())
        }
    }

    /// Version 1 from tick 0, version 2 from tick 10.
    fn origin() -> Origin {
        let mut origin = Origin::default();
        let mut prng = Prng::new(1);
        origin.put(0, &key(), 100, &mut prng);
        origin.put(10, &key(), 80, &mut prng);
        origin
    }

    fn first_version(origin: &Origin, request: &Request) -> (ResponseHead, Vec<u8>) {
        respond(origin.states_during(&key(), 0, 0)[0], request)
    }

    #[test]
    fn accepts_the_current_version() {
        let mut origin = origin();
        let request = get(Some(ByteRange::Suffix { length: 10 }));
        let (head, body) = origin.respond_now(&request);
        assert_eq!(
            check_response(&origin, &request, 12, 15, &head, &body),
            Ok(())
        );
    }

    #[test]
    fn accepts_a_version_overwritten_within_the_window() {
        let origin = origin();
        let request = get(None);
        let (head, body) = first_version(&origin, &request);
        assert_eq!(
            check_response(&origin, &request, 5, 15, &head, &body),
            Ok(())
        );
    }

    #[test]
    fn rejects_a_version_overwritten_before_the_window() {
        let origin = origin();
        let request = get(None);
        let (head, body) = first_version(&origin, &request);
        assert!(check_response(&origin, &request, 11, 15, &head, &body).is_err());
    }

    #[test]
    fn rejects_a_corrupt_byte() {
        let mut origin = origin();
        let request = get(Some(ByteRange::Inclusive { first: 3, last: 9 }));
        let (head, mut body) = origin.respond_now(&request);
        body[4] ^= 1;
        assert!(check_response(&origin, &request, 12, 15, &head, &body).is_err());
    }

    #[test]
    fn checks_stored_blocks_against_their_version() {
        let origin = origin();
        let first = origin.states_during(&key(), 0, 0)[0].unwrap();
        let etag = first.etag.clone();
        let block = |index, len| StoredBlock {
            key: &first.key,
            etag: &etag,
            index,
            location: Location {
                extent: 0,
                offset: 0,
            },
            len,
        };
        let bytes = first.bytes(96..100);
        assert_eq!(check_block(&origin, 32, &block(3, 4), &bytes), Ok(()));
        assert!(check_block(&origin, 32, &block(2, 4), &bytes).is_err());
        let unknown = ETag("\"missing\"".into());
        let orphan = StoredBlock {
            etag: &unknown,
            ..block(3, 4)
        };
        assert!(check_block(&origin, 32, &orphan, &bytes).is_err());
    }
}
