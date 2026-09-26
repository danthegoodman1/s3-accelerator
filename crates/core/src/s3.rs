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

/// A `GetObject` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetObject {
    pub key: ObjectKey,
    pub range: Option<ByteRange>,
    pub if_match: Option<ETag>,
}

/// A response's status and the headers the core reads or sets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseHead {
    pub status: u16,
    pub etag: Option<ETag>,
    pub content_range: Option<ContentRange>,
    pub content_length: u64,
}

impl ResponseHead {
    /// A response with no body and no object headers.
    pub fn status(status: u16) -> ResponseHead {
        ResponseHead {
            status,
            etag: None,
            content_range: None,
            content_length: 0,
        }
    }
}

/// A `Content-Range` header: the body's inclusive span and the object's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentRange {
    pub first: u64,
    pub last: u64,
    pub size: u64,
}
