//! The S3 origin: signed requests from this node, with the cluster's
//! credentials.

use crate::http::{format_range, header, parse_content_range};
use crate::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use bytes::Bytes;
use s3_accelerator_core::s3::{ETag, Method, Request, ResponseHead};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest S3 may leave a response idle.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Origin {
    client: reqwest::Client,
    /// Scheme and authority, such as `http://127.0.0.1:8080`.
    endpoint: String,
    authority: String,
    signer: Signer,
    /// The largest response body held in memory.
    max_body: u64,
}

/// A response to a forwarded request.
pub struct Forwarded {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
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

impl Origin {
    pub fn new(endpoint: &str, region: &str, credentials: Credentials, max_body: u64) -> Origin {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let authority = endpoint
            .split_once("://")
            .map_or(endpoint.as_str(), |(_, rest)| rest)
            .to_string();
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .expect("an HTTP client");
        Origin {
            client,
            endpoint,
            authority,
            signer: Signer {
                credentials,
                region: region.to_string(),
            },
            max_body,
        }
    }

    /// Sends a request from the core. A failure to reach S3 answers 503.
    pub async fn read(&self, request: &Request) -> (ResponseHead, Bytes) {
        let path = format!(
            "/{}/{}",
            sigv4::encode(&request.key.bucket),
            request
                .key
                .key
                .split('/')
                .map(sigv4::encode)
                .collect::<Vec<_>>()
                .join("/")
        );
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
        match self
            .send(method, &path, "", headers, UNSIGNED_PAYLOAD, Bytes::new())
            .await
        {
            Ok(response) => (response_head(&response, request.method), response.body),
            Err(_) => (ResponseHead::status(503), Bytes::new()),
        }
    }

    /// Passes a client's request to S3 under the cluster's signature. The
    /// body and its payload hash travel unchanged.
    pub async fn forward(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(String, String)],
        payload_hash: &str,
        body: Bytes,
    ) -> Result<Forwarded, String> {
        let headers = headers
            .iter()
            .filter(|(name, _)| !HOP_HEADERS.contains(&name.to_ascii_lowercase().as_str()))
            .cloned()
            .collect();
        self.send(method, path, query, headers, payload_hash, body)
            .await
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        mut headers: Vec<(String, String)>,
        payload_hash: &str,
        body: Bytes,
    ) -> Result<Forwarded, String> {
        headers.push(("host".to_string(), self.authority.clone()));
        self.signer.sign(
            method,
            path,
            query,
            &mut headers,
            payload_hash,
            sigv4::unix_now(),
        );
        let url = match query {
            "" => format!("{}{path}", self.endpoint),
            query => format!("{}{path}?{query}", self.endpoint),
        };
        let method =
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?;
        let mut request = self.client.request(method, url).body(body);
        for (name, value) in &headers {
            request = request.header(name, value);
        }
        let mut response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
                (name.as_str().to_string(), value)
            })
            .collect();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
            if (body.len() + chunk.len()) as u64 > self.max_body {
                return Err(format!(
                    "S3's response is larger than {} bytes",
                    self.max_body
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Forwarded {
            status,
            headers,
            body: body.into(),
        })
    }
}

/// What the core reads from S3's response.
fn response_head(response: &Forwarded, method: Method) -> ResponseHead {
    let header = |name: &str| header(&response.headers, name);
    let content_length = match method {
        Method::Get => response.body.len() as u64,
        Method::Head => header("content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
    };
    ResponseHead {
        status: response.status,
        etag: header("etag").map(|etag| ETag(etag.to_string())),
        content_range: header("content-range").and_then(parse_content_range),
        content_length,
        headers: response
            .headers
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
