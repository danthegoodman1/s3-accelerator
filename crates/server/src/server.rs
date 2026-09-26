//! Accepts S3 clients, authenticates and authorizes each request, and serves
//! `GetObject` and `HeadObject` through the core. Other operations pass
//! through to S3 under the cluster's signature.

use crate::config::{Client, Config};
use crate::engine::{Answer, Engine, Shared};
use crate::http::header;
use crate::http::{Connection, RequestHead, Response};
use crate::origin::Origin;
use crate::sigv4::{self, AuthError, Credentials, Signable};
use bytes::Bytes;
use percent_encoding::percent_decode_str;
use s3_accelerator_core::s3::{ByteRange, ETag, Method, ObjectKey, Request, ResponseHead};
use sha2::{Digest, Sha256};
use std::io;
use std::rc::Rc;
use tokio::net::{TcpListener, TcpStream};

struct Context {
    engine: Shared,
    origin: Rc<Origin>,
    clients: Vec<Client>,
    /// The largest body held in memory, uploaded or fetched.
    max_body: u64,
}

pub async fn serve(config: Config) -> io::Result<()> {
    let listener = TcpListener::bind(&config.listen).await?;
    eprintln!("s3-accelerator listening on {}", listener.local_addr()?);
    run(listener, config).await
}

/// Serves clients on `listener`, ignoring `config.listen`. Runs on a
/// `LocalSet`.
pub async fn run(listener: TcpListener, config: Config) -> io::Result<()> {
    let credentials = Credentials {
        access_key_id: config.origin.access_key_id.clone(),
        secret_access_key: config.origin.secret_access_key.clone(),
    };
    let origin = Rc::new(Origin::new(
        &config.origin.endpoint,
        &config.origin.region,
        credentials,
        config.max_body,
    ));
    let context = Rc::new(Context {
        engine: Engine::new(config.cache.node_config(), origin.clone()),
        origin,
        clients: config.clients,
        max_body: config.max_body,
    });
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let context = context.clone();
        tokio::task::spawn_local(async move {
            if let Err(error) = connection(stream, &context).await {
                eprintln!("connection closed: {error}");
            }
        });
    }
}

async fn connection(stream: TcpStream, context: &Context) -> io::Result<()> {
    let mut connection = Connection::new(stream);
    while let Some(head) = connection.read_head().await? {
        let len = match head.content_length() {
            Ok(len) => len,
            Err(what) => {
                let response = error(501, "NotImplemented", &format!("unsupported {what}"));
                return connection.write_response(&response, false).await;
            }
        };
        // The signature covers the head, so a request is authenticated before
        // its body is read. A rejected request's body is never read, so the
        // connection closes.
        let client = match authenticate(&head, context) {
            Ok(client) => client,
            Err(response) => return connection.write_response(&response, false).await,
        };
        if len > context.max_body {
            let response = error(
                400,
                "EntityTooLarge",
                "the body is larger than this server holds",
            );
            return connection.write_response(&response, false).await;
        }
        let expects_continue = header(&head.headers, "expect")
            .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
        if len > 0 && expects_continue {
            connection.write_continue().await?;
        }
        let body = connection.read_body(len).await?;
        let response = handle(&head, client, Bytes::from(body), context).await;
        connection
            .write_response(&response, head.keep_alive)
            .await?;
        if !head.keep_alive {
            return Ok(());
        }
    }
    Ok(())
}

/// The client that signed the request's head.
fn authenticate<'c>(head: &RequestHead, context: &'c Context) -> Result<&'c Client, Response> {
    if header(&head.headers, "authorization").is_none() {
        return Err(auth_error(AuthError::Missing));
    }
    let Some(payload_hash) = header(&head.headers, "x-amz-content-sha256") else {
        return Err(error(400, "InvalidRequest", "missing x-amz-content-sha256"));
    };
    if payload_hash.starts_with("STREAMING-AWS4-HMAC-SHA256") {
        return Err(error(501, "NotImplemented", "signed streaming uploads"));
    }
    let signable = Signable {
        method: &head.method,
        path: &head.path,
        query: &head.query,
        headers: &head.headers,
        payload_hash,
    };
    let lookup = |id: &str| {
        let client = context
            .clients
            .iter()
            .find(|client| client.access_key_id == id)?;
        Some((client, client.secret_access_key.as_str()))
    };
    sigv4::verify(&signable, sigv4::unix_now(), lookup).map_err(auth_error)
}

async fn handle(head: &RequestHead, client: &Client, body: Bytes, context: &Context) -> Response {
    let payload_hash = header(&head.headers, "x-amz-content-sha256").expect("authenticated");
    let is_digest = payload_hash.len() == 64 && payload_hash.bytes().all(|b| b.is_ascii_hexdigit());
    if is_digest && hex::encode(Sha256::digest(&body)) != payload_hash.to_ascii_lowercase() {
        return error(
            400,
            "XAmzContentSHA256Mismatch",
            "the body does not match its hash",
        );
    }
    let (bucket, key) = split_path(&head.path);
    // reqwest's URL parser collapses `.` and `..` segments, so the request
    // S3 received would name a different key than the one signed.
    if key
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return error(501, "NotImplemented", "keys with . or .. path segments");
    }
    if !client.may_access(&bucket, &key) {
        return error(403, "AccessDenied", "Access Denied");
    }
    // A copy reads its source, so the grants must cover the source too.
    if let Some(source) = header(&head.headers, "x-amz-copy-source") {
        let (source_bucket, source_key) = copy_source(source);
        if !client.may_access(&source_bucket, &source_key) {
            return error(403, "AccessDenied", "Access Denied");
        }
    }
    if let Some(request) = cacheable(head, &bucket, &key) {
        return read(request, context).await;
    }
    forward(head, payload_hash, body, &bucket, &key, context).await
}

/// The core's request, if the core serves this one.
fn cacheable(head: &RequestHead, bucket: &str, key: &str) -> Option<Request> {
    let method = match head.method.as_str() {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        _ => return None,
    };
    let only_operation_id = head
        .query
        .split('&')
        .all(|pair| pair.is_empty() || pair.starts_with("x-id="));
    let unsupported = [
        "if-modified-since",
        "if-unmodified-since",
        "x-amz-server-side-encryption-customer-key",
    ];
    if bucket.is_empty()
        || key.is_empty()
        || !only_operation_id
        || unsupported
            .iter()
            .any(|name| header(&head.headers, name).is_some())
    {
        return None;
    }
    Some(Request {
        method,
        key: ObjectKey {
            bucket: bucket.to_string(),
            key: key.to_string(),
        },
        range: header(&head.headers, "range").and_then(parse_range),
        if_match: etag_condition(header(&head.headers, "if-match"))?,
        if_none_match: etag_condition(header(&head.headers, "if-none-match"))?,
    })
}

async fn read(request: Request, context: &Context) -> Response {
    let method = request.method;
    let Ok(Answer { head, body }) = Engine::read(&context.engine, request).await else {
        return error(500, "InternalError", "the request was dropped");
    };
    let mut headers = vec![("Accept-Ranges".to_string(), "bytes".to_string())];
    headers.extend(head.headers.iter().cloned());
    if let Some(etag) = &head.etag {
        headers.push(("ETag".to_string(), etag.0.clone()));
    }
    if let Some(range) = head.content_range {
        let value = format!("bytes {}-{}/{}", range.first, range.last, range.size);
        headers.push(("Content-Range".to_string(), value));
    }
    if method == Method::Head {
        return Response {
            status: head.status,
            headers,
            content_length: head.content_length,
            body: Bytes::new(),
        };
    }
    if body.is_empty() && head.status >= 400 {
        return error(head.status, error_code(&head), reason_message(head.status));
    }
    if head.status >= 400 {
        headers.push(("Content-Type".to_string(), "application/xml".to_string()));
    }
    Response {
        status: head.status,
        headers,
        content_length: body.len() as u64,
        body,
    }
}

async fn forward(
    head: &RequestHead,
    payload_hash: &str,
    body: Bytes,
    bucket: &str,
    key: &str,
    context: &Context,
) -> Response {
    let forwarded = context
        .origin
        .forward(
            &head.method,
            &head.path,
            &head.query,
            &head.headers,
            payload_hash,
            body.clone(),
        )
        .await;
    let forwarded = match forwarded {
        Ok(forwarded) => forwarded,
        Err(failure) => return error(502, "BadGateway", &failure),
    };
    if forwarded.status < 300 && !bucket.is_empty() {
        for key in written_keys(head, key, &body) {
            let key = ObjectKey {
                bucket: bucket.to_string(),
                key,
            };
            Engine::write_succeeded(&context.engine, &key);
        }
    }
    let content_length = match head.method.as_str() {
        "HEAD" => header(&forwarded.headers, "content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        _ => forwarded.body.len() as u64,
    };
    let hop = [
        "connection",
        "content-length",
        "keep-alive",
        "transfer-encoding",
    ];
    let headers = forwarded
        .headers
        .into_iter()
        .filter(|(name, _)| !hop.contains(&name.as_str()))
        .collect();
    Response {
        status: forwarded.status,
        headers,
        content_length,
        body: forwarded.body,
    }
}

/// The keys a successful request wrote: the path's key for a `PUT`,
/// `POST` or `DELETE` of an object, or every key a `DeleteObjects` lists.
fn written_keys(head: &RequestHead, key: &str, body: &[u8]) -> Vec<String> {
    let deletes_objects = head.method == "POST"
        && key.is_empty()
        && head
            .query
            .split('&')
            .any(|pair| pair == "delete" || pair.starts_with("delete="));
    if deletes_objects {
        return listed_keys(&String::from_utf8_lossy(body));
    }
    match (head.method.as_str(), key.is_empty()) {
        ("PUT" | "POST" | "DELETE", false) => vec![key.to_string()],
        _ => Vec::new(),
    }
}

/// The `<Key>` elements of a `DeleteObjects` request body.
fn listed_keys(xml: &str) -> Vec<String> {
    xml.split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Key>"))
        .map(|(key, _)| {
            key.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&")
        })
        .collect()
}

/// The bucket and key an `x-amz-copy-source` names.
fn copy_source(value: &str) -> (String, String) {
    let value = value.split_once('?').map_or(value, |(source, _)| source);
    split_path(&format!("/{}", value.trim_start_matches('/')))
}

/// A path-style request's bucket and key, decoded.
fn split_path(path: &str) -> (String, String) {
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
    let decode = |part: &str| percent_decode_str(part).decode_utf8_lossy().into_owned();
    (decode(bucket), decode(key))
}

/// A single `Range: bytes=` range. S3 serves the whole object for anything
/// else, so the rest become no range.
fn parse_range(value: &str) -> Option<ByteRange> {
    let spec = value.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (first, last) = spec.split_once('-')?;
    let number = |text: &str| text.trim().parse::<u64>().ok();
    match (first.trim(), last.trim()) {
        ("", length) => Some(ByteRange::Suffix {
            length: number(length)?,
        }),
        (first, "") => Some(ByteRange::From {
            first: number(first)?,
        }),
        (first, last) => Some(ByteRange::Inclusive {
            first: number(first)?,
            last: number(last)?,
        }),
    }
}

/// An `If-Match` or `If-None-Match` the core can evaluate: `Some(None)`
/// when absent, `None` when it lists several tags or `*`, which pass
/// through to S3.
fn etag_condition(value: Option<&str>) -> Option<Option<ETag>> {
    let Some(value) = value.map(str::trim) else {
        return Some(None);
    };
    if value == "*" || value.contains(',') || value.starts_with("W/") {
        return None;
    }
    let quoted = match value.starts_with('"') {
        true => value.to_string(),
        false => format!("\"{value}\""),
    };
    Some(Some(ETag(quoted)))
}

fn auth_error(failure: AuthError) -> Response {
    let code = match failure {
        AuthError::Missing => "AccessDenied",
        AuthError::Malformed(_) => "AuthorizationHeaderMalformed",
        AuthError::UnknownAccessKey => "InvalidAccessKeyId",
        AuthError::Expired => "RequestTimeTooSkewed",
        AuthError::SignatureMismatch => "SignatureDoesNotMatch",
    };
    error(403, code, &failure.to_string())
}

/// An S3 error code for a status the core answered without S3's own
/// error body.
fn error_code(head: &ResponseHead) -> &'static str {
    match head.status {
        400 => "InvalidRequest",
        403 => "AccessDenied",
        404 => "NoSuchKey",
        412 => "PreconditionFailed",
        416 => "InvalidRange",
        501 => "NotImplemented",
        503 => "ServiceUnavailable",
        _ => "InternalError",
    }
}

fn reason_message(status: u16) -> &'static str {
    match status {
        400 => "The request is invalid.",
        403 => "Access Denied",
        404 => "The specified key does not exist.",
        412 => "At least one of the pre-conditions you specified did not hold",
        416 => "The requested range is not satisfiable",
        501 => "A header you provided implies functionality that is not implemented.",
        503 => "Please reduce your request rate.",
        _ => "We encountered an internal error. Please try again.",
    }
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{message}</Message></Error>"
    );
    Response {
        status,
        headers: vec![("Content-Type".to_string(), "application/xml".to_string())],
        content_length: body.len() as u64,
        body: Bytes::from(body),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_path_style_requests() {
        assert_eq!(split_path("/"), (String::new(), String::new()));
        assert_eq!(split_path("/bucket"), ("bucket".into(), String::new()));
        assert_eq!(
            split_path("/bucket/a/b%20c"),
            ("bucket".into(), "a/b c".into())
        );
    }

    #[test]
    fn parses_ranges_as_s3_does() {
        assert_eq!(
            parse_range("bytes=0-9"),
            Some(ByteRange::Inclusive { first: 0, last: 9 })
        );
        assert_eq!(parse_range("bytes=5-"), Some(ByteRange::From { first: 5 }));
        assert_eq!(
            parse_range("bytes=-5"),
            Some(ByteRange::Suffix { length: 5 })
        );
        assert_eq!(parse_range("bytes=0-1,4-5"), None);
        assert_eq!(parse_range("items=0-1"), None);
    }

    #[test]
    fn reads_copy_sources() {
        assert_eq!(
            copy_source("logs/a%20b?versionId=3"),
            ("logs".into(), "a b".into())
        );
        assert_eq!(copy_source("/logs/x/y"), ("logs".into(), "x/y".into()));
    }

    #[test]
    fn lists_deleted_keys() {
        let xml =
            "<Delete><Object><Key>a&amp;b</Key></Object><Object><Key>c/d</Key></Object></Delete>";
        assert_eq!(listed_keys(xml), ["a&b", "c/d"]);
    }

    #[test]
    fn passes_complex_preconditions_to_s3() {
        assert_eq!(etag_condition(None), Some(None));
        assert_eq!(
            etag_condition(Some("\"a\"")),
            Some(Some(ETag("\"a\"".into())))
        );
        assert_eq!(etag_condition(Some("a")), Some(Some(ETag("\"a\"".into()))));
        assert_eq!(etag_condition(Some("*")), None);
        assert_eq!(etag_condition(Some("\"a\", \"b\"")), None);
    }
}
