//! A model of S3 that remembers every state of every key and when it began.
//!
//! An object's bytes are a function of its version's seed and the offset, so
//! the model stores no data and the checker can rebuild any response. An
//! object whose name says it is Parquet, ORC or safetensors starts and ends
//! with that format's framing, which states the span of its metadata.

use crate::prng::{Prng, splitmix64};
use s3_accelerator_core::formats::Format;
use s3_accelerator_core::formats::fixtures::frame;
use s3_accelerator_core::s3::{
    ByteRange, ContentRange, ETag, Method, ObjectKey, Request, ResponseHead,
};
use s3_accelerator_core::store::VersionId;
use std::collections::BTreeMap;
use std::ops::Range;

/// One version of an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: ObjectKey,
    pub etag: ETag,
    pub size: u64,
    pub headers: Vec<(String, String)>,
    seed: u64,
    /// The format's framing at the object's start and end, if any.
    head: Vec<u8>,
    tail: Vec<u8>,
}

impl Object {
    pub fn bytes(&self, span: Range<u64>) -> Vec<u8> {
        let tail_start = self.size - self.tail.len() as u64;
        span.map(|offset| {
            if offset < self.head.len() as u64 {
                return self.head[offset as usize];
            }
            if offset >= tail_start {
                return self.tail[(offset - tail_start) as usize];
            }
            let mut state = self.seed ^ (offset / 8).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            (splitmix64(&mut state) >> (offset % 8 * 8)) as u8
        })
        .collect()
    }
}

/// What a key held from tick `since` until its next state began.
#[derive(Clone, Debug)]
struct State {
    since: u64,
    etag: Option<ETag>,
}

#[derive(Default)]
pub struct Origin {
    history: BTreeMap<ObjectKey, Vec<State>>,
    versions: BTreeMap<ETag, Object>,
    /// Each version's ETag by the hash nodes know it by.
    ids: BTreeMap<VersionId, ETag>,
    requests: u64,
}

impl Origin {
    pub fn put(&mut self, now: u64, key: &ObjectKey, size: u64, prng: &mut Prng) {
        let seed = prng.next_u64();
        let etag = ETag(format!("\"{:016x}{seed:016x}\"", self.versions.len()));
        let headers = vec![
            (
                "content-type".to_string(),
                format!("application/x-seed-{:x}", seed % 97),
            ),
            ("last-modified".to_string(), format!("tick {now}")),
            (
                "x-amz-meta-version".to_string(),
                self.versions.len().to_string(),
            ),
        ];
        // Metadata of up to a third of the object, by the seed.
        let (head, tail) = Format::of(key)
            .and_then(|format| frame(format, size, 1 + seed % (size / 3).max(1)))
            .unwrap_or_default();
        let object = Object {
            key: key.clone(),
            etag: etag.clone(),
            size,
            headers,
            seed,
            head,
            tail,
        };
        self.versions.insert(etag.clone(), object);
        self.ids.insert(VersionId::of(key, &etag), etag.clone());
        self.record(now, key, Some(etag));
    }

    pub fn delete(&mut self, now: u64, key: &ObjectKey) {
        self.record(now, key, None);
    }

    fn record(&mut self, now: u64, key: &ObjectKey, etag: Option<ETag>) {
        let states = self.history.entry(key.clone()).or_default();
        // A later write in the same tick replaces the earlier one.
        if states.last().is_some_and(|state| state.since == now) {
            states.pop();
        }
        states.push(State { since: now, etag });
    }

    pub fn current(&self, key: &ObjectKey) -> Option<&Object> {
        let state = self.history.get(key)?.last()?;
        state.etag.as_ref().map(|etag| &self.versions[etag])
    }

    pub fn version(&self, etag: &ETag) -> Option<&Object> {
        self.versions.get(etag)
    }

    /// The version nodes know by `id`.
    pub fn version_by_id(&self, id: VersionId) -> Option<&Object> {
        self.ids.get(&id).map(|etag| &self.versions[etag])
    }

    /// Requests S3 has answered.
    pub fn requests(&self) -> u64 {
        self.requests
    }

    /// Every state `key` held at some tick in `from..=to`. `None` is absent.
    pub fn states_during(&self, key: &ObjectKey, from: u64, to: u64) -> Vec<Option<&Object>> {
        let states = self.history.get(key).map(Vec::as_slice).unwrap_or_default();
        let started = states.partition_point(|state| state.since <= from);
        let object = |state: &State| state.etag.as_ref().map(|etag| &self.versions[etag]);
        let at_start = match started {
            0 => None,
            _ => object(&states[started - 1]),
        };
        let during = states[started..]
            .iter()
            .take_while(|state| state.since <= to)
            .map(object);
        std::iter::once(at_start).chain(during).collect()
    }

    pub fn respond_now(&mut self, request: &Request) -> (ResponseHead, Vec<u8>) {
        self.requests += 1;
        respond(self.current(&request.key), request)
    }
}

/// S3's response to `request` while its key holds `object`.
pub fn respond(object: Option<&Object>, request: &Request) -> (ResponseHead, Vec<u8>) {
    let Some(object) = object else {
        return (ResponseHead::status(404), Vec::new());
    };
    if request
        .if_match
        .as_ref()
        .is_some_and(|etag| *etag != object.etag)
    {
        return (ResponseHead::status(412), Vec::new());
    }
    if request.if_none_match.as_ref() == Some(&object.etag) {
        let head = ResponseHead {
            status: 304,
            etag: Some(object.etag.clone()),
            ..ResponseHead::status(304)
        };
        return (head, Vec::new());
    }
    // S3 ignores a range whose last byte precedes its first, as RFC 9110
    // requires. s3proxy answers 416 instead, so the conformance suite
    // leaves this case to the model.
    let range = match request.range {
        Some(ByteRange::Inclusive { first, last }) if last < first => None,
        range => range,
    };
    let (status, span, content_range) = match range {
        None => (200, 0..object.size, None),
        Some(range) => match resolve(range, object.size) {
            None => return (ResponseHead::status(416), Vec::new()),
            Some((first, last)) => {
                let content_range = ContentRange {
                    first,
                    last,
                    size: object.size,
                };
                (206, first..last + 1, Some(content_range))
            }
        },
    };
    let head = ResponseHead {
        status,
        etag: Some(object.etag.clone()),
        content_range,
        content_length: span.end - span.start,
        headers: object.headers.clone(),
    };
    let body = match request.method {
        Method::Get => object.bytes(span),
        Method::Head => Vec::new(),
    };
    (head, body)
}

/// The inclusive span `range` selects from `size` bytes, or `None` when it
/// selects nothing.
pub fn resolve(range: ByteRange, size: u64) -> Option<(u64, u64)> {
    match range {
        ByteRange::Inclusive { first, last } if first < size => Some((first, last.min(size - 1))),
        ByteRange::From { first } if first < size => Some((first, size - 1)),
        ByteRange::Suffix { length } if length > 0 && size > 0 => {
            Some((size - length.min(size), size - 1))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ObjectKey {
        ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        }
    }

    #[test]
    fn states_during_covers_the_window() {
        let mut origin = Origin::default();
        let mut prng = Prng::new(1);
        origin.put(10, &key(), 5, &mut prng);
        origin.put(20, &key(), 6, &mut prng);
        origin.delete(30, &key());
        let sizes = |from, to| -> Vec<Option<u64>> {
            let states = origin.states_during(&key(), from, to);
            states
                .iter()
                .map(|state| state.map(|object| object.size))
                .collect()
        };
        assert_eq!(sizes(0, 5), [None]);
        assert_eq!(sizes(0, 10), [None, Some(5)]);
        assert_eq!(sizes(12, 19), [Some(5)]);
        assert_eq!(sizes(12, 20), [Some(5), Some(6)]);
        assert_eq!(sizes(20, 40), [Some(6), None]);
    }

    #[test]
    fn preconditions_come_before_ranges() {
        let mut origin = Origin::default();
        origin.put(0, &key(), 10, &mut Prng::new(1));
        let etag = origin.current(&key()).unwrap().etag.clone();
        let range = Some(ByteRange::From { first: 50 });
        let stale = Request {
            if_match: Some(ETag("\"x\"".into())),
            range,
            ..Request::get(key())
        };
        assert_eq!(origin.respond_now(&stale).0.status, 412);
        let current = Request {
            if_none_match: Some(etag),
            range,
            ..Request::get(key())
        };
        assert_eq!(origin.respond_now(&current).0.status, 304);
        let unsatisfiable = Request {
            range,
            ..Request::get(key())
        };
        assert_eq!(origin.respond_now(&unsatisfiable).0.status, 416);
    }

    #[test]
    fn resolve_follows_rfc_9110() {
        let inclusive = |first, last| ByteRange::Inclusive { first, last };
        assert_eq!(resolve(inclusive(0, 0), 10), Some((0, 0)));
        assert_eq!(resolve(inclusive(5, 100), 10), Some((5, 9)));
        assert_eq!(resolve(inclusive(10, 12), 10), None);
        assert_eq!(resolve(ByteRange::From { first: 9 }, 10), Some((9, 9)));
        assert_eq!(resolve(ByteRange::From { first: 10 }, 10), None);
        assert_eq!(resolve(ByteRange::Suffix { length: 3 }, 10), Some((7, 9)));
        assert_eq!(resolve(ByteRange::Suffix { length: 30 }, 10), Some((0, 9)));
        assert_eq!(resolve(ByteRange::Suffix { length: 0 }, 10), None);
    }
}
