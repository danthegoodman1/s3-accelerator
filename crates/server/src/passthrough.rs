//! Requests the cache doesn't serve pass through to S3: a gateway streams
//! the client's body to a storage node, which re-signs the request and
//! streams it to S3, and S3's answer comes back the same way. Only storage
//! nodes hold S3 credentials. No body sits whole in memory.

use crate::http::{self, Connection, Framing, header, split_path};
use crate::origin::{self, Origin, RequestBody};
use crate::protocol::{Forward, NodeAnswer};
use crate::zero_copy::Short;
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::channel::{Channel, Sender};
use hyper::body::Incoming;
use s3_accelerator_core::s3::ObjectKey;
use sha2::{Digest, Sha256};
use std::io;
use std::ops::Range;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Where a passing body goes.
pub(crate) trait BodySink {
    /// Passes on the next piece, and whether the far end still takes it.
    async fn send(&mut self, piece: Bytes) -> bool;

    /// Ends the body short, so the far end never sees it whole.
    fn abort(self);
}

impl BodySink for Sender<Bytes, io::Error> {
    async fn send(&mut self, piece: Bytes) -> bool {
        self.send_data(piece).await.is_ok()
    }

    fn abort(self) {
        Sender::abort(self, io::Error::other("the body ended short"));
    }
}

/// A node's socket, which the gateway also watches for an early answer.
/// A body ends short there when the gateway closes the connection.
pub struct ToNode<'a>(pub &'a TcpStream);

impl BodySink for ToNode<'_> {
    async fn send(&mut self, piece: Bytes) -> bool {
        http::write_shared(self.0, &piece).await.is_ok()
    }

    fn abort(self) {}
}

/// A sink that keeps copies of the bytes within `ranges` of the body, in
/// order, as it passes the body on.
struct Keeping<'a, S> {
    sink: S,
    /// The body offset of the next piece.
    offset: u64,
    ranges: &'a [Range<u64>],
    /// Each range's start and the bytes kept of it so far.
    kept: &'a mut Vec<(u64, Vec<u8>)>,
}

impl<S: BodySink> BodySink for Keeping<'_, S> {
    async fn send(&mut self, piece: Bytes) -> bool {
        let (start, end) = (self.offset, self.offset + piece.len() as u64);
        for range in self.ranges {
            let (from, to) = (range.start.max(start), range.end.min(end));
            if from >= to {
                continue;
            }
            let bytes = &piece[(from - start) as usize..(to - start) as usize];
            match self
                .kept
                .iter_mut()
                .find(|(first, _)| *first == range.start)
            {
                Some((_, kept)) => kept.extend_from_slice(bytes),
                None => self.kept.push((range.start, bytes.to_vec())),
            }
        }
        self.offset = end;
        self.sink.send(piece).await
    }

    fn abort(self) {
        self.sink.abort();
    }
}

/// Passes a `len`-byte body from `source` to `sink` as it arrives, and
/// returns whether it matched `digest`, the SHA-256 the client signed. The
/// last bytes wait until the whole body is checked, so the far end never
/// receives a body that fails its hash.
pub(crate) async fn pass_body(
    source: &mut Connection,
    len: u64,
    digest: Option<&str>,
    mut sink: impl BodySink,
) -> Result<bool, Short> {
    let mut hasher = Sha256::new();
    let mut remaining = len;
    let mut held: Option<Bytes> = None;
    let stopped = || Short::Destination(io::Error::other("the far end stopped taking the body"));
    while remaining > 0 {
        let max = usize::try_from(remaining).unwrap_or(usize::MAX);
        let piece = match source.read_some(max).await {
            Ok(piece) => piece,
            Err(error) => {
                sink.abort();
                return Err(Short::Source(error));
            }
        };
        remaining -= piece.len() as u64;
        if digest.is_some() {
            hasher.update(&piece);
        }
        if let Some(previous) = held.replace(piece)
            && !sink.send(previous).await
        {
            return Err(stopped());
        }
    }
    if let Some(digest) = digest
        && hex::encode(hasher.finalize()) != digest
    {
        sink.abort();
        return Ok(false);
    }
    if let Some(last) = held
        && !sink.send(last).await
    {
        return Err(stopped());
    }
    Ok(true)
}

/// How a request passed to S3 went.
pub struct Sent {
    pub response: io::Result<hyper::Response<Incoming>>,
    /// Whether the request's body was read to its end.
    pub body_read: bool,
    /// The bytes kept of each range asked for, by the range's start.
    pub kept: Vec<(u64, Vec<u8>)>,
}

/// Sends a forwarded request to S3, streaming its body from `source` while
/// S3's answer is awaited: S3 may answer before the body ends, such as to
/// refuse it. The gateway checked the body's hash. The bytes within
/// `keep` are kept as they pass.
pub async fn to_s3(
    origin: &Arc<Origin>,
    forward: &Forward,
    source: &mut Connection,
    keep: &[Range<u64>],
) -> Sent {
    let mut kept = Vec::new();
    let (response, body_read) = {
        let (sender, body) = Channel::<Bytes, io::Error>::new(2);
        let body: RequestBody = body.boxed();
        let sent = origin.forward(
            &forward.method,
            &forward.path,
            &forward.query,
            &forward.headers,
            &forward.payload_hash,
            body,
            forward.len,
        );
        let sink = Keeping {
            sink: sender,
            offset: 0,
            ranges: keep,
            kept: &mut kept,
        };
        let mut sent = std::pin::pin!(sent);
        let mut passing = std::pin::pin!(pass_body(source, forward.len, None, sink));
        let mut passed = None;
        let response = loop {
            tokio::select! {
                result = &mut passing, if passed.is_none() => passed = Some(result),
                response = &mut sent => break response,
            }
            if passed.is_some() {
                // The body is sent; S3 has a while to answer.
                break tokio::time::timeout(origin::READ_TIMEOUT, &mut sent)
                    .await
                    .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()));
            }
        };
        (response, matches!(passed, Some(Ok(_))))
    };
    Sent {
        response,
        body_read,
        kept,
    }
}

/// Whether a request uploads a whole object's bytes as its body: a
/// `PutObject`, rather than a part, a copy, a body in aws-chunked encoding,
/// whose bytes on the wire are not the object's, or one S3 encrypts with
/// the client's key, which the cache never holds.
pub fn uploads_object(forward: &Forward) -> bool {
    let part = forward
        .query
        .split('&')
        .any(|pair| pair.starts_with("partNumber=") || pair.starts_with("uploadId="));
    let chunked = forward.payload_hash.starts_with("STREAMING-")
        || header(&forward.headers, "x-amz-decoded-content-length").is_some();
    forward.method == "PUT"
        && forward.len > 0
        && !part
        && !chunked
        && header(&forward.headers, "x-amz-copy-source").is_none()
        && header(
            &forward.headers,
            "x-amz-server-side-encryption-customer-key",
        )
        .is_none()
}

/// Headers that describe a hop rather than the response.
const HOP: [&str; 4] = [
    "connection",
    "content-length",
    "keep-alive",
    "transfer-encoding",
];

/// S3's answer as a node sends it on: its status and headers, and its
/// body's length, which a chunked body lacks. A bodiless answer's length
/// is what a GET's would be.
pub fn forwarded(method: &str, response: &hyper::Response<Incoming>) -> NodeAnswer {
    let status = response.status().as_u16();
    let headers = origin::header_pairs(response.headers());
    let length = header(&headers, "content-length").and_then(|value| value.parse::<u64>().ok());
    let bodiless = bodiless(method, status);
    let headers = headers
        .into_iter()
        .filter(|(name, _)| !HOP.contains(&name.as_str()))
        .collect();
    NodeAnswer::Forwarded {
        status,
        headers,
        length: match bodiless {
            true => Some(length.unwrap_or(0)),
            false => length,
        },
    }
}

/// The object a request changes if it succeeds: the path's object for a
/// `PUT`, `POST` or `DELETE`.
pub fn written_key(method: &str, path: &str) -> Option<ObjectKey> {
    let (bucket, key) = split_path(path);
    let writes = matches!(method, "PUT" | "POST" | "DELETE");
    (writes && !bucket.is_empty() && !key.is_empty()).then_some(ObjectKey { bucket, key })
}

/// Whether a response to `method` with `status` carries no body.
pub fn bodiless(method: &str, status: u16) -> bool {
    method == "HEAD" || status == 204 || status == 304
}

/// Streams S3's response body into `connection`, framed as the answer's
/// head said, and returns whether it arrived in full.
pub async fn stream_body(
    connection: &mut Connection,
    response: hyper::Response<Incoming>,
    framing: Framing,
) -> io::Result<bool> {
    let mut body = response.into_body();
    let mut sent = 0;
    loop {
        match origin::next_frame(&mut body).await {
            Ok(Some(piece)) => {
                match framing {
                    Framing::Length(_) => connection.write_body(&piece).await?,
                    Framing::Chunked => connection.write_chunk(&piece).await?,
                }
                sent += piece.len() as u64;
            }
            Ok(None) => break,
            // The body ends short, and the connection with it.
            Err(_) => return Ok(false),
        }
    }
    match framing {
        Framing::Chunked => {
            connection.finish_chunks().await?;
            Ok(true)
        }
        Framing::Length(length) => Ok(sent == length),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(payload_hash: &str, headers: &[(&str, &str)]) -> Forward {
        Forward {
            method: "PUT".into(),
            path: "/b/k".into(),
            query: String::new(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            payload_hash: payload_hash.into(),
            len: 100,
        }
    }

    #[test]
    fn only_whole_bodies_are_uploads() {
        assert!(uploads_object(&put("UNSIGNED-PAYLOAD", &[])));
        let trailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
        let decoded = [("x-amz-decoded-content-length", "80")];
        assert!(!uploads_object(&put(trailer, &decoded)));
        assert!(!uploads_object(&put("UNSIGNED-PAYLOAD", &decoded)));
        let copy = [("x-amz-copy-source", "b/other")];
        assert!(!uploads_object(&put("UNSIGNED-PAYLOAD", &copy)));
        let mut part = put("UNSIGNED-PAYLOAD", &[]);
        part.query = "partNumber=1&uploadId=x".into();
        assert!(!uploads_object(&part));
        let customer_key = [("x-amz-server-side-encryption-customer-key", "a2V5")];
        assert!(!uploads_object(&put("UNSIGNED-PAYLOAD", &customer_key)));
    }
}
