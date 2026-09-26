//! A model of S3 that remembers every state of every key and when it began.
//!
//! An object's bytes are a function of its version's seed and the offset, so
//! the model stores no data and the checker can rebuild any response.

use crate::prng::{Prng, splitmix64};
use s3_accelerator_core::s3::{ByteRange, ContentRange, ETag, GetObject, ObjectKey, ResponseHead};
use std::collections::BTreeMap;
use std::ops::Range;

/// One version of an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub etag: ETag,
    pub size: u64,
    seed: u64,
}

impl Object {
    pub fn bytes(&self, span: Range<u64>) -> Vec<u8> {
        span.map(|offset| {
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
    object: Option<Object>,
}

#[derive(Default)]
pub struct Origin {
    history: BTreeMap<ObjectKey, Vec<State>>,
    versions: u64,
}

impl Origin {
    pub fn put(&mut self, now: u64, key: &ObjectKey, size: u64, prng: &mut Prng) {
        let seed = prng.next_u64();
        self.versions += 1;
        let etag = ETag(format!("\"{:016x}{seed:016x}\"", self.versions));
        let object = Object { etag, size, seed };
        self.record(now, key, Some(object));
    }

    pub fn delete(&mut self, now: u64, key: &ObjectKey) {
        self.record(now, key, None);
    }

    fn record(&mut self, now: u64, key: &ObjectKey, object: Option<Object>) {
        let states = self.history.entry(key.clone()).or_default();
        // A later write in the same tick replaces the earlier one.
        if states.last().is_some_and(|state| state.since == now) {
            states.pop();
        }
        states.push(State { since: now, object });
    }

    pub fn current(&self, key: &ObjectKey) -> Option<&Object> {
        let states = self.history.get(key)?;
        states.last()?.object.as_ref()
    }

    /// Every state `key` held at some tick in `from..=to`. `None` is absent.
    pub fn states_during(&self, key: &ObjectKey, from: u64, to: u64) -> Vec<Option<&Object>> {
        let states = self.history.get(key).map(Vec::as_slice).unwrap_or_default();
        let started = states.partition_point(|state| state.since <= from);
        let during = states[started..]
            .iter()
            .take_while(|state| state.since <= to);
        let at_start = match started {
            0 => None,
            _ => states[started - 1].object.as_ref(),
        };
        std::iter::once(at_start)
            .chain(during.map(|state| state.object.as_ref()))
            .collect()
    }

    pub fn get(&self, get: &GetObject) -> (ResponseHead, Vec<u8>) {
        respond(self.current(&get.key), get)
    }
}

/// S3's response to `get` while its key holds `object`.
pub fn respond(object: Option<&Object>, get: &GetObject) -> (ResponseHead, Vec<u8>) {
    let Some(object) = object else {
        return (ResponseHead::status(404), Vec::new());
    };
    if get
        .if_match
        .as_ref()
        .is_some_and(|etag| *etag != object.etag)
    {
        return (ResponseHead::status(412), Vec::new());
    }
    let Some(range) = get.range else {
        let head = ResponseHead {
            status: 200,
            etag: Some(object.etag.clone()),
            content_range: None,
            content_length: object.size,
        };
        return (head, object.bytes(0..object.size));
    };
    let Some((first, last)) = resolve(range, object.size) else {
        return (ResponseHead::status(416), Vec::new());
    };
    let head = ResponseHead {
        status: 206,
        etag: Some(object.etag.clone()),
        content_range: Some(ContentRange {
            first,
            last,
            size: object.size,
        }),
        content_length: last - first + 1,
    };
    (head, object.bytes(first..last + 1))
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
