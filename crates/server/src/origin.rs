//! The S3 origin: signed requests from this node, with the cluster's
//! credentials. Paths go out exactly as written, so keys with `.` and `..`
//! segments reach S3 as the keys they name, and bodies stream both ways.

use crate::http::{format_range, header, parse_content_range};
use crate::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use s3_accelerator_core::s3::{ETag, Method, ObjectKey, Request, ResponseHead};
use std::io;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest S3 may take to answer, or leave a response body idle.
pub const READ_TIMEOUT: Duration = Duration::from_secs(60);
/// The largest body the node holds that it did not size in advance: an
/// error body, or one S3 sent without a `Content-Length`.
const HELD_LIMIT: u64 = 1 << 20;

/// A request body sent to S3.
pub type RequestBody = BoxBody<Bytes, io::Error>;

pub struct Origin {
    client: Client<HttpsConnector<HttpConnector>, RequestBody>,
    /// Scheme and authority, such as `http://127.0.0.1:8080`.
    endpoint: String,
    authority: String,
    signer: Signer,
}

/// S3's response body to a read.
pub enum OriginBody {
    /// Read in full.
    Held(Bytes),
    /// Arriving: `ResponseHead::content_length` bytes follow.
    Arriving(Incoming),
}

/// Request headers that describe this hop, or that signing replaces.
const HOP_HEADERS: [&str; 10] = [
    "authorization",
    "connection",
    "content-length",
    "expect",
    "host",
    "keep-alive",
    "transfer-encoding",
    "x-amz-content-sha256",
    "x-amz-date",
    "x-amz-security-token",
];

/// Whether a request header describes the client's hop to the gateway, or
/// is one signing replaces.
pub fn is_hop_header(name: &str) -> bool {
    HOP_HEADERS.contains(&name.to_ascii_lowercase().as_str())
}

impl Origin {
    pub fn new(endpoint: &str, region: &str, credentials: Credentials) -> Origin {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let authority = endpoint
            .split_once("://")
            .map_or(endpoint.as_str(), |(_, rest)| rest)
            .to_string();
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_connect_timeout(Some(CONNECT_TIMEOUT));
        let https = HttpsConnectorBuilder::new()
            .with_platform_verifier()
            .https_or_http()
            .enable_http1()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new()).build(https);
        Origin {
            client,
            endpoint,
            authority,
            signer: Signer {
                credentials,
                region: region.to_string(),
            },
        }
    }

    /// Sends a request from the core. `hold` is the most body bytes to read
    /// in full before answering; without it, a body of known length is
    /// left arriving. A failure to reach S3 answers 503.
    pub async fn read(&self, request: &Request, hold: Option<u64>) -> (ResponseHead, OriginBody) {
        let failed = || (ResponseHead::status(503), OriginBody::Held(Bytes::new()));
        let mut headers = Vec::new();
        if let Some(range) = request.range {
            headers.push(("range".to_string(), format_range(range)));
        }
        if let Some(etag) = &request.if_match {
            headers.push(("if-match".to_string(), etag.0.clone()));
        }
        if let Some(etag) = &request.if_none_match {
            headers.push(("if-none-match".to_string(), etag.0.clone()));
        }
        let method = match request.method {
            Method::Get => "GET",
            Method::Head => "HEAD",
        };
        let path = object_path(&request.key);
        let sent = self.send(method, &path, "", headers, UNSIGNED_PAYLOAD, empty(), None);
        let response = match tokio::time::timeout(READ_TIMEOUT, sent).await {
            Ok(Ok(response)) => response,
            _ => return failed(),
        };
        let (parts, body) = response.into_parts();
        let headers = header_pairs(&parts.headers);
        let length = header(&headers, "content-length").and_then(|value| value.parse().ok());
        let mut head = response_head(parts.status.as_u16(), &headers, length.unwrap_or(0));
        match (request.method, hold, length) {
            (Method::Head, _, _) => (head, OriginBody::Held(Bytes::new())),
            (Method::Get, None, Some(_)) => (head, OriginBody::Arriving(body)),
            (Method::Get, hold, _) => match collect(body, hold.unwrap_or(HELD_LIMIT)).await {
                Ok(bytes) => {
                    head.content_length = bytes.len() as u64;
                    (head, OriginBody::Held(bytes))
                }
                Err(_) => failed(),
            },
        }
    }

    /// Passes a client's request to S3 under the cluster's signature. The
    /// body, `len` bytes, and its payload hash travel unchanged.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        body: RequestBody,
        len: u64,
    ) -> io::Result<hyper::Response<Incoming>> {
        let headers = headers
            .iter()
            .filter(|(name, _)| !is_hop_header(name))
            .cloned()
            .collect();
        self.send(method, path, query, headers, payload_hash, body, Some(len))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        mut headers: Vec<(String, String)>,
        payload_hash: &str,
        body: RequestBody,
        len: Option<u64>,
    ) -> io::Result<hyper::Response<Incoming>> {
        headers.push(("host".to_string(), self.authority.clone()));
        self.signer.sign(
            method,
            path,
            query,
            &mut headers,
            payload_hash,
            sigv4::unix_now(),
        );
        let uri = match query {
            "" => format!("{}{path}", self.endpoint),
            query => format!("{}{path}?{query}", self.endpoint),
        };
        let mut request = http::Request::builder().method(method).uri(uri);
        for (name, value) in &headers {
            request = request.header(name, value);
        }
        if let Some(len) = len {
            request = request.header("content-length", len);
        }
        let request = request.body(body).map_err(io::Error::other)?;
        self.client.request(request).await.map_err(io::Error::other)
    }
}

/// `/bucket/key`, each segment percent-encoded.
fn object_path(key: &ObjectKey) -> String {
    let segments: Vec<String> = key.key.split('/').map(sigv4::encode).collect();
    format!("/{}/{}", sigv4::encode(&key.bucket), segments.join("/"))
}

fn empty() -> RequestBody {
    Empty::new().map_err(|never| match never {}).boxed()
}

pub fn header_pairs(headers: &http::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            (name.as_str().to_string(), value)
        })
        .collect()
}

/// Reads a whole body of at most `limit` bytes.
async fn collect(mut body: Incoming, limit: u64) -> io::Result<Bytes> {
    let mut bytes = Vec::new();
    while let Some(frame) = next_frame(&mut body).await? {
        if (bytes.len() + frame.len()) as u64 > limit {
            return Err(io::Error::other(format!(
                "S3's body is larger than {limit} bytes"
            )));
        }
        bytes.extend_from_slice(&frame);
    }
    Ok(bytes.into())
}

/// The next piece of a response body, or `None` at its end. S3 has
/// `READ_TIMEOUT` to send each.
pub async fn next_frame(body: &mut Incoming) -> io::Result<Option<Bytes>> {
    loop {
        let frame = tokio::time::timeout(READ_TIMEOUT, body.frame())
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?;
        match frame {
            None => return Ok(None),
            Some(Err(error)) => return Err(io::Error::other(error)),
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    return Ok(Some(data));
                }
            }
        }
    }
}

/// What the core reads from S3's response, whose body is `len` bytes.
fn response_head(status: u16, headers: &[(String, String)], len: u64) -> ResponseHead {
    let header = |name: &str| header(headers, name);
    ResponseHead {
        status,
        etag: header("etag").map(|etag| ETag(etag.to_string())),
        content_range: header("content-range").and_then(parse_content_range),
        content_length: len,
        headers: headers
            .iter()
            .filter(|(name, _)| is_object_header(name))
            .cloned()
            .collect(),
    }
}

/// Headers that describe the object rather than the response, which the
/// cache stores and replays.
pub fn is_object_header(name: &str) -> bool {
    const OBJECT_HEADERS: [&str; 19] = [
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "content-type",
        "expires",
        "last-modified",
        "x-amz-object-lock-legal-hold",
        "x-amz-object-lock-mode",
        "x-amz-object-lock-retain-until-date",
        "x-amz-replication-status",
        "x-amz-restore",
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-bucket-key-enabled",
        "x-amz-storage-class",
        "x-amz-tagging-count",
        "x-amz-version-id",
        "x-amz-website-redirect-location",
    ];
    let name = name.to_ascii_lowercase();
    name.starts_with("x-amz-meta-") || OBJECT_HEADERS.contains(&name.as_str())
}
