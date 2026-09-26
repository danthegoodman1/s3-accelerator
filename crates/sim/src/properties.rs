//! What every run must satisfy.

use crate::origin::{Origin, respond};
use s3_accelerator_core::s3::{GetObject, ResponseHead};

/// A response must equal S3's response to the same request at some tick
/// while the request was in flight.
pub fn check_response(
    origin: &Origin,
    get: &GetObject,
    issued: u64,
    answered: u64,
    head: &ResponseHead,
    body: &[u8],
) -> Result<(), String> {
    if head.content_length != body.len() as u64 {
        return Err(format!(
            "Content-Length {} with a {}-byte body",
            head.content_length,
            body.len()
        ));
    }
    let states = origin.states_during(&get.key, issued, answered);
    let matches = |state| {
        let (expected_head, expected_body) = respond(state, get);
        expected_head == *head && expected_body == body
    };
    if states.into_iter().any(matches) {
        return Ok(());
    }
    Err(format!(
        "{} {:?} {:?} ({} bytes) for {get:?} matches no state of the key in ticks {issued}..={answered}",
        head.status,
        head.etag,
        head.content_range,
        body.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prng::Prng;
    use s3_accelerator_core::s3::{ByteRange, ObjectKey};

    fn get(range: Option<ByteRange>) -> GetObject {
        GetObject {
            key: ObjectKey {
                bucket: "b".into(),
                key: "k".into(),
            },
            range,
            if_match: None,
        }
    }

    /// Version 1 from tick 0, version 2 from tick 10.
    fn origin() -> Origin {
        let mut origin = Origin::default();
        let mut prng = Prng::new(1);
        origin.put(0, &get(None).key, 100, &mut prng);
        origin.put(10, &get(None).key, 80, &mut prng);
        origin
    }

    #[test]
    fn accepts_the_current_version() {
        let origin = origin();
        let get = get(Some(ByteRange::Suffix { length: 10 }));
        let (head, body) = origin.get(&get);
        assert_eq!(check_response(&origin, &get, 12, 15, &head, &body), Ok(()));
    }

    #[test]
    fn accepts_a_version_overwritten_mid_request() {
        let origin = origin();
        let get = get(None);
        let (head, body) = respond(origin.states_during(&get.key, 0, 0)[0], &get);
        assert_eq!(check_response(&origin, &get, 5, 15, &head, &body), Ok(()));
    }

    #[test]
    fn rejects_a_version_overwritten_before_the_request() {
        let origin = origin();
        let get = get(None);
        let (head, body) = respond(origin.states_during(&get.key, 0, 0)[0], &get);
        assert!(check_response(&origin, &get, 11, 15, &head, &body).is_err());
    }

    #[test]
    fn rejects_a_corrupt_byte() {
        let origin = origin();
        let get = get(Some(ByteRange::Inclusive { first: 3, last: 9 }));
        let (head, mut body) = origin.get(&get);
        body[4] ^= 1;
        assert!(check_response(&origin, &get, 12, 15, &head, &body).is_err());
    }
}
