//! The parts of S3 requests and responses the core reasons about.

/// A bucket and a key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub bucket: String,
    pub key: String,
}

/// An object version's entity tag, quotes included.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ETag(pub String);

/// The single byte range of a `Range` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=first-last`, both inclusive.
    Inclusive { first: u64, last: u64 },
    /// `bytes=first-`.
    From { first: u64 },
    /// `bytes=-length`: the final `length` bytes.
    Suffix { length: u64 },
}

impl ByteRange {
    /// S3 ignores a range whose last byte precedes its first.
    pub fn is_valid(self) -> bool {
        !matches!(self, ByteRange::Inclusive { first, last } if last < first)
    }

    /// The inclusive span this range selects from `size` bytes, or `None`
    /// when it selects nothing (a 416) or is invalid.
    pub fn resolve(self, size: u64) -> Option<(u64, u64)> {
        let last_byte = size.checked_sub(1)?;
        match self {
            ByteRange::Inclusive { first, last } if first <= last && first <= last_byte => {
                Some((first, last.min(last_byte)))
            }
            ByteRange::From { first } if first <= last_byte => Some((first, last_byte)),
            ByteRange::Suffix { length } if length > 0 => {
                Some((size - length.min(size), last_byte))
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
}

/// A `GetObject` or `HeadObject` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: Method,
    pub key: ObjectKey,
    pub range: Option<ByteRange>,
    pub if_match: Option<ETag>,
    pub if_none_match: Option<ETag>,
}

impl Request {
    pub fn get(key: ObjectKey) -> Request {
        Request {
            method: Method::Get,
            key,
            range: None,
            if_match: None,
            if_none_match: None,
        }
    }

    pub fn head(key: ObjectKey) -> Request {
        Request {
            method: Method::Head,
            ..Request::get(key)
        }
    }
}

/// A response's status and the headers the core reads or sets. For a
/// `HeadObject`, `content_length` describes the body a `GetObject` would
/// return, and the response has no body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead {
    pub status: u16,
    pub etag: Option<ETag>,
    pub content_range: Option<ContentRange>,
    pub content_length: u64,
    /// The headers S3 returns with the object, such as `Content-Type`,
    /// `Last-Modified` and `x-amz-meta-*`, which the cache replays.
    pub headers: Vec<(String, String)>,
}

impl ResponseHead {
    /// A response with no body and no object headers.
    pub fn status(status: u16) -> ResponseHead {
        ResponseHead {
            status,
            etag: None,
            content_range: None,
            content_length: 0,
            headers: Vec::new(),
        }
    }
}

/// How S3 answers a `GetObject` or `HeadObject` for one version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// A response with no body: a failed precondition, an unsatisfiable
    /// range, a `HeadObject`, or an empty object.
    Head(ResponseHead),
    /// A response whose body is bytes `first..=last` of the object.
    Body {
        head: ResponseHead,
        first: u64,
        last: u64,
    },
}

/// S3's answer to `request` for the version `etag`, of `size` bytes, that
/// carries `headers`. The request's range must be valid.
pub fn answer(request: &Request, etag: &ETag, size: u64, headers: &[(String, String)]) -> Answer {
    if let Some(head) = preconditions(request, etag) {
        return Answer::Head(head);
    }
    let span = match request.range {
        None => (size > 0).then(|| (0, size - 1)),
        Some(range) => match range.resolve(size) {
            Some(span) => Some(span),
            None => return Answer::Head(ResponseHead::status(416)),
        },
    };
    let head = ResponseHead {
        status: if request.range.is_some() { 206 } else { 200 },
        etag: Some(etag.clone()),
        content_range: request.range.and(span).map(|(first, last)| ContentRange {
            first,
            last,
            size,
        }),
        content_length: span.map_or(0, |(first, last)| last - first + 1),
        headers: headers.to_vec(),
    };
    match (request.method, span) {
        (Method::Get, Some((first, last))) => Answer::Body { head, first, last },
        _ => Answer::Head(head),
    }
}

/// The answer to a request whose preconditions fail against `etag`: S3
/// checks `If-Match`, then `If-None-Match`, before anything else.
pub fn preconditions(request: &Request, etag: &ETag) -> Option<ResponseHead> {
    if request
        .if_match
        .as_ref()
        .is_some_and(|expected| expected != etag)
    {
        return Some(ResponseHead::status(412));
    }
    if request.if_none_match.as_ref() == Some(etag) {
        return Some(ResponseHead {
            etag: Some(etag.clone()),
            ..ResponseHead::status(304)
        });
    }
    None
}

/// A `Content-Range` header: the body's inclusive span and the object's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentRange {
    pub first: u64,
    pub last: u64,
    pub size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_follows_rfc_9110() {
        let inclusive = |first, last| ByteRange::Inclusive { first, last };
        assert_eq!(inclusive(0, 0).resolve(10), Some((0, 0)));
        assert_eq!(inclusive(5, 100).resolve(10), Some((5, 9)));
        assert_eq!(inclusive(10, 12).resolve(10), None);
        assert_eq!(ByteRange::From { first: 9 }.resolve(10), Some((9, 9)));
        assert_eq!(ByteRange::From { first: 10 }.resolve(10), None);
        assert_eq!(ByteRange::Suffix { length: 3 }.resolve(10), Some((7, 9)));
        assert_eq!(ByteRange::Suffix { length: 30 }.resolve(10), Some((0, 9)));
        assert_eq!(ByteRange::Suffix { length: 0 }.resolve(10), None);
        assert_eq!(ByteRange::From { first: 0 }.resolve(0), None);
        assert_eq!(inclusive(5, 3).resolve(10), None);
        assert!(!inclusive(5, 3).is_valid());
        assert!(inclusive(3, 3).is_valid());
    }
}
