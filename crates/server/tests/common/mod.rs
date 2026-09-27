//! A fake S3 that counts its requests, and a signed client, for running the
//! server end to end.

#![allow(dead_code, reason = "each test crate uses its own part of the harness")]

pub mod trace;

use s3_accelerator::config::Config;
use s3_accelerator::http::{Connection, Framing, Response};
use s3_accelerator::server::{self, Listeners};
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use s3_accelerator_core::formats::Format;
use s3_accelerator_core::formats::fixtures::frame;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub const ETAG: &str = "\"0123456789abcdef\"";
/// The CRC32 the fake S3 gives every object, when asked.
pub const CHECKSUM: &str = "AAAAAA==";
pub const SIZE: usize = 300_000;
/// The request ID the fake S3 gives every answer.
pub const S3_REQUEST_ID: &str = "FAKES3REQUEST";

pub fn object() -> Vec<u8> {
    object_of(SIZE, "")
}

/// An object of `size` bytes whose content depends on `path`.
pub fn object_of(size: usize, path: &str) -> Vec<u8> {
    let seed = path.bytes().fold(0u32, |seed, byte| {
        seed.wrapping_mul(31).wrapping_add(byte.into())
    });
    (0..size as u32)
        .map(|index| (index.wrapping_add(seed).wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect()
}

/// The fake S3's answer to a bucket listing.
pub const LISTING: &str = "<ListBucketResult><Name>bucket</Name></ListBucketResult>";

/// What the fake S3 has seen, and whether a `DeleteObjects` removed its
/// object.
pub struct Origin {
    pub requests: Cell<u64>,
    /// Response body bytes it sent.
    pub sent: Cell<u64>,
    pub deleted: Cell<bool>,
    /// Each request's path and query, as they arrived.
    pub paths: RefCell<Vec<String>>,
    pub queries: RefCell<Vec<String>>,
    /// How long it waits before each answer.
    pub delay: Cell<std::time::Duration>,
    /// The size of its objects, and whether each path has its own content
    /// (`object_of`) rather than all sharing `object()`'s.
    pub size: Cell<usize>,
    pub distinct: Cell<bool>,
    /// Bodies of the writes it received in full, and the latest by path
    /// with its ETag, which it serves from then on.
    pub uploads: RefCell<Vec<Vec<u8>>>,
    pub written: RefCell<BTreeMap<String, (String, Vec<u8>)>>,
    /// Refuse each `PUT` with 403 from its head, before its body arrives.
    pub refuse_writes: Cell<bool>,
    /// How long it waits before each 64 KiB of a body after the first.
    pub trickle: Cell<Duration>,
    /// Answer bucket listings chunked, with no `Content-Length`.
    pub chunked: Cell<bool>,
}

impl Default for Origin {
    fn default() -> Origin {
        Origin {
            requests: Cell::default(),
            sent: Cell::default(),
            deleted: Cell::default(),
            paths: RefCell::default(),
            queries: RefCell::default(),
            delay: Cell::default(),
            size: Cell::new(SIZE),
            distinct: Cell::default(),
            uploads: RefCell::default(),
            written: RefCell::default(),
            refuse_writes: Cell::default(),
            trickle: Cell::default(),
            chunked: Cell::default(),
        }
    }
}

impl Origin {
    /// The object it serves at `path`, before any write to it. A Parquet
    /// file's footer is its last third.
    pub fn object(&self, path: &str) -> Vec<u8> {
        let mut object = match self.distinct.get() {
            true => object_of(self.size.get(), path),
            false => object_of(self.size.get(), ""),
        };
        if path.ends_with(".parquet") {
            let size = object.len() as u64;
            let (head, tail) = frame(Format::Parquet, size, size / 3).expect("room for a footer");
            object[..head.len()].copy_from_slice(&head);
            let end = object.len() - tail.len();
            object[end..].copy_from_slice(&tail);
        }
        object
    }

    /// The ETag and bytes it serves at `path` now.
    fn current(&self, path: &str) -> (String, Vec<u8>) {
        match self.written.borrow().get(path) {
            Some(version) => version.clone(),
            None => (ETAG.to_string(), self.object(path)),
        }
    }
}

/// Serves an object at any path, honoring `Range` and `If-Match`, until a
/// `DeleteObjects` removes it, and `LISTING` for a bucket. Answers every
/// other request with 200 once its body arrives.
async fn fake_origin(listener: TcpListener, origin: Rc<Origin>) {
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let origin = origin.clone();
        tokio::task::spawn_local(async move {
            let mut connection = Connection::new(stream);
            while let Ok(Some(head)) = connection.read_head().await {
                if head.method == "PUT" && origin.refuse_writes.get() {
                    let response = Response {
                        status: 403,
                        headers: Vec::new(),
                        content_length: 0,
                        body: Vec::new().into(),
                    };
                    let _ = connection.write_response(&response, false).await;
                    connection.linger().await;
                    return;
                }
                let len = head.content_length().unwrap();
                let Ok(upload) = connection.read_body(len).await else {
                    return;
                };
                origin.requests.set(origin.requests.get() + 1);
                origin.paths.borrow_mut().push(head.path.clone());
                origin.queries.borrow_mut().push(head.query.clone());
                let mut headers = vec![("x-amz-request-id".to_string(), S3_REQUEST_ID.to_string())];
                if head.method == "PUT" {
                    let etag = format!("\"written-{}\"", origin.uploads.borrow().len());
                    headers.push(("ETag".to_string(), etag.clone()));
                    let version = (etag, upload.clone());
                    origin
                        .written
                        .borrow_mut()
                        .insert(head.path.clone(), version);
                    origin.uploads.borrow_mut().push(upload);
                }
                let (etag, object) = origin.current(&head.path);
                let (status, body) = match head.method.as_str() {
                    "POST" if head.query.contains("delete") => {
                        origin.deleted.set(true);
                        (200, Vec::new())
                    }
                    "GET" if !head.path.trim_start_matches('/').contains('/') => {
                        (200, LISTING.as_bytes().to_vec())
                    }
                    "GET" | "HEAD" if origin.deleted.get() => (404, Vec::new()),
                    "GET" | "HEAD" => {
                        headers.push(("ETag".to_string(), etag.clone()));
                        let (status, body) = read(&head.headers, &object, &etag, &mut headers);
                        let asked =
                            s3_accelerator::http::header(&head.headers, "x-amz-checksum-mode")
                                .is_some_and(|mode| mode == "ENABLED");
                        if asked && status == 200 {
                            headers.push(("x-amz-checksum-crc32".into(), CHECKSUM.into()));
                            headers.push(("x-amz-checksum-type".into(), "FULL_OBJECT".into()));
                        }
                        (status, body)
                    }
                    _ => (200, Vec::new()),
                };
                tokio::time::sleep(origin.delay.get()).await;
                let listing = head.method == "GET" && body == LISTING.as_bytes();
                if listing && origin.chunked.get() {
                    let written = async {
                        connection
                            .write_response_head(status, &headers, Framing::Chunked, true)
                            .await?;
                        for piece in body.chunks(7) {
                            connection.write_chunk(piece).await?;
                        }
                        connection.finish_chunks().await
                    };
                    if written.await.is_err() {
                        return;
                    }
                    continue;
                }
                let framing = Framing::Length(body.len() as u64);
                if connection
                    .write_response_head(status, &headers, framing, true)
                    .await
                    .is_err()
                {
                    return;
                }
                let body = if head.method == "HEAD" {
                    &[][..]
                } else {
                    &body
                };
                for (index, piece) in body.chunks(64 << 10).enumerate() {
                    if index > 0 {
                        tokio::time::sleep(origin.trickle.get()).await;
                    }
                    if connection.write_all(piece).await.is_err() {
                        return;
                    }
                    origin.sent.set(origin.sent.get() + piece.len() as u64);
                }
            }
        });
    }
}

fn read(
    request: &[(String, String)],
    object: &[u8],
    current: &str,
    headers: &mut Vec<(String, String)>,
) -> (u16, Vec<u8>) {
    let header = |name| s3_accelerator::http::header(request, name);
    if header("if-match").is_some_and(|etag| etag != current) {
        return (412, Vec::new());
    }
    let size = object.len();
    let range = header("range").and_then(|value| {
        let (first, last) = value.strip_prefix("bytes=")?.split_once('-')?;
        if first.is_empty() {
            let length: usize = last.parse().ok()?;
            return Some((size - length.min(size), size - 1));
        }
        let first: usize = first.parse().ok()?;
        let last = last
            .parse()
            .map_or(size - 1, |last: usize| last.min(size - 1));
        Some((first, last))
    });
    match range {
        Some((first, last)) => {
            headers.push((
                "Content-Range".into(),
                format!("bytes {first}-{last}/{size}"),
            ));
            (206, object[first..=last].to_vec())
        }
        None => (200, object.to_vec()),
    }
}

/// Starts the fake S3 and a server in front of it on this `LocalSet`, and
/// returns the server's port. `extra` is appended to the server's config.
pub async fn start(grants: &str, extra: &str) -> (u16, Rc<Origin>) {
    let (origin_port, origin) = start_origin().await;
    let server = Server::start(origin_port, &data_dir(), grants, extra).await;
    (server.port, origin)
}

/// Starts the fake S3 on this `LocalSet`, and returns its port.
pub async fn start_origin() -> (u16, Rc<Origin>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let origin = Rc::new(Origin::default());
    tokio::task::spawn_local(fake_origin(listener, origin.clone()));
    (port, origin)
}

/// A fake SQS queue of S3's event notifications. It offers each message to
/// one poller at a time, and offers it again once its visibility timeout
/// passes without a delete.
#[derive(Default)]
pub struct Queue {
    /// Each message's receipt, body, and when it becomes visible again.
    messages: RefCell<Vec<(String, String, Option<std::time::Instant>)>>,
    sent: Cell<u64>,
}

impl Queue {
    /// Queues S3's event that `bucket`'s `key` now has `etag`, or is gone.
    pub fn send(&self, bucket: &str, key: &str, etag: Option<&str>) {
        let (name, object) = match etag {
            Some(etag) => (
                "ObjectCreated:Put",
                serde_json::json!({ "key": key, "eTag": etag }),
            ),
            None => ("ObjectRemoved:Delete", serde_json::json!({ "key": key })),
        };
        let event = serde_json::json!({ "Records": [{
            "eventName": name,
            "s3": { "bucket": { "name": bucket }, "object": object },
        }]});
        self.sent.set(self.sent.get() + 1);
        let receipt = format!("receipt-{}", self.sent.get());
        self.messages
            .borrow_mut()
            .push((receipt, event.to_string(), None));
    }

    /// Messages not yet deleted.
    pub fn len(&self) -> usize {
        self.messages.borrow().len()
    }

    /// Up to ten visible messages, hidden for `visibility` from now on.
    fn offer(&self, visibility: Duration) -> Vec<(String, String)> {
        let now = std::time::Instant::now();
        let mut messages = self.messages.borrow_mut();
        messages
            .iter_mut()
            .filter(|(_, _, hidden)| hidden.is_none_or(|until| until <= now))
            .take(10)
            .map(|(receipt, body, hidden)| {
                *hidden = Some(now + visibility);
                (receipt.clone(), body.clone())
            })
            .collect()
    }
}

/// Starts the fake SQS on this `LocalSet`, and returns its port. It takes
/// only requests signed for SQS.
pub async fn start_queue() -> (u16, Rc<Queue>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let queue = Rc::new(Queue::default());
    tokio::task::spawn_local(fake_queue(listener, queue.clone()));
    (port, queue)
}

async fn fake_queue(listener: TcpListener, queue: Rc<Queue>) {
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let queue = queue.clone();
        tokio::task::spawn_local(async move {
            let mut connection = Connection::new(stream);
            while let Ok(Some(head)) = connection.read_head().await {
                let len = head.content_length().unwrap();
                let Ok(body) = connection.read_body(len).await else {
                    return;
                };
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let signed_for_sqs = head
                    .header("authorization")
                    .is_some_and(|value| value.contains("/sqs/aws4_request"));
                let answer = match (signed_for_sqs, head.header("x-amz-target")) {
                    (false, _) => None,
                    (true, Some("AmazonSQS.ReceiveMessage")) => {
                        let wait =
                            Duration::from_secs(request["WaitTimeSeconds"].as_u64().unwrap());
                        let visibility =
                            Duration::from_secs(request["VisibilityTimeout"].as_u64().unwrap());
                        let deadline = tokio::time::Instant::now() + wait;
                        let mut offered = queue.offer(visibility);
                        while offered.is_empty() && tokio::time::Instant::now() < deadline {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            offered = queue.offer(visibility);
                        }
                        let messages: Vec<serde_json::Value> = offered
                            .into_iter()
                            .map(|(receipt, body)| {
                                serde_json::json!({ "ReceiptHandle": receipt, "Body": body })
                            })
                            .collect();
                        Some(serde_json::json!({ "Messages": messages }))
                    }
                    (true, Some("AmazonSQS.DeleteMessage")) => {
                        let receipt = request["ReceiptHandle"].as_str().unwrap();
                        queue
                            .messages
                            .borrow_mut()
                            .retain(|(held, _, _)| held != receipt);
                        Some(serde_json::json!({}))
                    }
                    (true, _) => None,
                };
                let (status, body) = match answer {
                    Some(answer) => (200, answer.to_string()),
                    None => (403, String::new()),
                };
                let response = Response {
                    status,
                    headers: vec![("content-type".into(), "application/x-amz-json-1.0".into())],
                    content_length: body.len() as u64,
                    body: body.into(),
                };
                if connection.write_response(&response, true).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// A fresh directory for a server's disk.
pub fn data_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        "server-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The admin listener's answer at `path`: its status and body.
pub async fn admin(port: u16, path: &str) -> (u16, String) {
    let response = reqwest::get(format!("http://127.0.0.1:{port}{path}"))
        .await
        .unwrap();
    let status = response.status().as_u16();
    (status, response.text().await.unwrap())
}

/// The value of the sample `name` with `labels`, as a scrape renders it,
/// such as `s3accel_node_reads_total` and `""`, or 0 if the scrape has
/// none.
pub fn sample(scrape: &str, name: &str, labels: &str) -> f64 {
    let series = match labels {
        "" => format!("{name} "),
        labels => format!("{name}{{{labels}}} "),
    };
    scrape
        .lines()
        .find_map(|line| line.strip_prefix(series.as_str()))
        .map_or(0.0, |value| value.parse().unwrap())
}

/// A server running on this `LocalSet`.
pub struct Server {
    pub port: u16,
    /// Where the server answers scrapes and health checks.
    pub admin_port: u16,
    stop: oneshot::Sender<()>,
    done: tokio::task::JoinHandle<io::Result<()>>,
}

impl Server {
    /// Starts a server in front of the S3 at `origin_port`, keeping its
    /// disk in `dir`. `extra` is appended to its config.
    pub async fn start(origin_port: u16, dir: &Path, grants: &str, extra: &str) -> Server {
        let cache = "block_size = 65536\nextent_size = 1048576\nextents = 8";
        Server::start_with(origin_port, dir, grants, extra, cache).await
    }

    /// Starts a server whose `[cache]` table's own settings are `cache`.
    pub async fn start_with(
        origin_port: u16,
        dir: &Path,
        grants: &str,
        extra: &str,
        cache: &str,
    ) -> Server {
        let gateway = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_address = node.local_addr().unwrap();
        let config: Config = toml::from_str(&format!(
            r#"
            {extra}
            [origin]
            endpoint = "http://127.0.0.1:{origin_port}"
            region = "us-east-1"
            access_key_id = "origin"
            secret_access_key = "origin-secret"
            [[clients]]
            access_key_id = "reader"
            secret_access_key = "reader-secret"
            grants = [{grants}]
            [cache]
            {cache}
            [cache.default_policy]
            ttl_ms = 60000
            [cluster]
            secret = "cluster-secret"
            nodes = [{{ id = 0, address = "{node_address}" }}]
            [gateway]
            listen = "unused"
            domains = ["s3.test"]
            [node]
            id = 0
            data_dir = "{}"
            "#,
            dir.display()
        ))
        .unwrap();
        config.check().unwrap();
        let port = gateway.local_addr().unwrap().port();
        let admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin_port = admin.local_addr().unwrap().port();
        let listeners = Listeners {
            gateway: Some(gateway),
            node: Some(node),
            admin: Some(admin),
        };
        let (stop, stopped) = oneshot::channel::<()>();
        // A server whose handle is dropped runs until the test ends.
        let stopped = async move {
            if stopped.await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        let done = tokio::task::spawn_local(server::run(config, listeners, stopped));
        Server {
            port,
            admin_port,
            stop,
            done,
        }
    }

    /// Shuts the server down cleanly and waits until it has.
    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        self.done.await.unwrap().unwrap();
    }

    /// Stops the server as a crash would, without a clean shutdown.
    pub fn crash(self) {
        self.done.abort();
    }
}

/// The headers of a request `reader` signed, with an unsigned payload.
pub fn signed(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    signed_payload(port, method, path, query, extra, UNSIGNED_PAYLOAD)
}

/// The headers of a request `reader` signed, with `payload_hash`.
pub fn signed_payload(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    payload_hash: &str,
) -> Vec<(String, String)> {
    let signer = Signer {
        credentials: Credentials {
            access_key_id: "reader".into(),
            secret_access_key: "reader-secret".into(),
        },
        region: "us-east-1".into(),
        service: "s3",
    };
    let mut headers = vec![("host".to_string(), format!("127.0.0.1:{port}"))];
    for (name, value) in extra {
        headers.push((name.to_string(), value.to_string()));
    }
    signer.sign(
        method,
        path,
        query,
        &mut headers,
        payload_hash,
        sigv4::unix_now(),
    );
    headers
}

/// A URL `reader` presigned for a request to `host`, which reaches the
/// server at `port`, signed `age` seconds ago and valid for `expires`.
pub fn presigned(
    port: u16,
    host: &str,
    (method, path, query): (&str, &str, &str),
    age: i64,
    expires: i64,
) -> String {
    let signer = Signer {
        credentials: Credentials {
            access_key_id: "reader".into(),
            secret_access_key: "reader-secret".into(),
        },
        region: "us-east-1".into(),
        service: "s3",
    };
    let now = sigv4::unix_now() - age;
    let query = signer.presign((method, path, query), host, expires, now);
    format!("http://127.0.0.1:{port}{path}?{query}")
}

/// Sends a signed request and returns the status and body.
pub async fn send(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: Vec<u8>,
) -> (u16, Vec<u8>) {
    send_payload(port, method, path, query, extra, body, UNSIGNED_PAYLOAD).await
}

/// Sends a request signed with `payload_hash`, and returns the status and
/// body.
pub async fn send_payload(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: Vec<u8>,
    payload_hash: &str,
) -> (u16, Vec<u8>) {
    let url = match query {
        "" => format!("http://127.0.0.1:{port}{path}"),
        query => format!("http://127.0.0.1:{port}{path}?{query}"),
    };
    let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
    let mut request = reqwest::Client::new()
        .request(method.clone(), url)
        .body(body);
    for (name, value) in signed_payload(port, method.as_str(), path, query, extra, payload_hash) {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.bytes().await.unwrap().to_vec())
}

/// Sends a signed request with an unsigned payload, and returns the status
/// and the response's headers.
pub async fn send_for_headers(
    port: u16,
    method: &str,
    path: &str,
    body: Vec<u8>,
) -> (u16, Vec<(String, String)>) {
    let url = format!("http://127.0.0.1:{port}{path}");
    let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
    let mut request = reqwest::Client::new()
        .request(method.clone(), url)
        .body(body);
    for (name, value) in signed(port, method.as_str(), path, "", &[]) {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_string()))
        .collect();
    let _ = response.bytes().await;
    (status, headers)
}

/// Sends a signed GET and returns the status and body, or `None` if the
/// connection failed or the body ended early, as when its server dies.
pub async fn try_get(port: u16, path: &str) -> Option<(u16, Vec<u8>)> {
    let mut request = reqwest::Client::new().get(format!("http://127.0.0.1:{port}{path}"));
    for (name, value) in signed(port, "GET", path, "", &[]) {
        request = request.header(name, value);
    }
    let response = request.send().await.ok()?;
    let status = response.status().as_u16();
    Some((status, response.bytes().await.ok()?.to_vec()))
}

/// A server process, killed if the test ends first.
pub struct Process(Child);

impl Process {
    pub fn start(config: &Path) -> Process {
        let child = Command::new(env!("CARGO_BIN_EXE_s3-accelerator"))
            .arg(config)
            .spawn()
            .unwrap();
        Process(child)
    }

    /// Starts the server with its log lines going to `log`.
    pub fn logged(config: &Path, log: &Path) -> Process {
        let child = Command::new(env!("CARGO_BIN_EXE_s3-accelerator"))
            .arg(config)
            .stderr(std::fs::File::create(log).unwrap())
            .spawn()
            .unwrap();
        Process(child)
    }

    /// Starts the server under `strace`, which writes the system calls
    /// `calls` of every thread to `trace`: a timestamp on each, file and
    /// socket names for descriptors, and data in hex.
    pub fn traced(config: &Path, trace: &Path, calls: &str) -> Process {
        let child = Command::new("strace")
            .args(["-f", "-qq", "-ttt", "-yy", "-xx", "-s", "1048576"])
            .args(["-e", &format!("trace={calls}"), "-e", "signal=none", "-o"])
            .arg(trace)
            .arg(env!("CARGO_BIN_EXE_s3-accelerator"))
            .arg(config)
            .spawn()
            .expect("strace runs; install it to run the zero-copy tests");
        Process(child)
    }

    /// Shuts down cleanly, as a deploy does, and waits. A traced server's
    /// tracer exits once the server has, with its trace written.
    pub fn stop(mut self) {
        let pid = self.server_pid();
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .unwrap();
        assert!(self.0.wait().unwrap().success());
    }

    /// Sends the server `signal`, such as `USR1` to make a node leave.
    pub fn signal(&self, signal: &str) {
        let pid = self.server_pid();
        Command::new("kill")
            .args([&format!("-{signal}"), &pid.to_string()])
            .status()
            .unwrap();
    }

    /// Waits up to `limit` for the server to exit, and whether it did so
    /// cleanly.
    pub async fn exited(&mut self, limit: Duration) -> Option<bool> {
        let deadline = tokio::time::Instant::now() + limit;
        while tokio::time::Instant::now() < deadline {
            if let Some(status) = self.0.try_wait().unwrap() {
                return Some(status.success());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    /// The server's process: this child, or the child `strace` started.
    /// Its main thread, which runs the event loop, has the same ID.
    pub fn server_pid(&self) -> u32 {
        let pid = self.0.id();
        let children = format!("/proc/{pid}/task/{pid}/children");
        std::fs::read_to_string(children)
            .ok()
            .and_then(|children| children.split_whitespace().next()?.parse().ok())
            .filter(|_| {
                std::fs::read_to_string(format!("/proc/{pid}/comm"))
                    .is_ok_and(|name| name.trim() == "strace")
            })
            .unwrap_or(pid)
    }
}

impl Drop for Process {
    /// Kills the server, and the tracer too, which would otherwise leave
    /// the server running once it died.
    fn drop(&mut self) {
        let server = self.server_pid();
        if server != self.0.id() {
            let _ = Command::new("kill")
                .args(["-KILL", &server.to_string()])
                .status();
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A free port on the loopback interface for a server to bind later. It
/// lies below the kernel's ephemeral range, where no connection takes it as
/// its local port, and this process reserves it for as long as it runs.
pub fn port() -> u16 {
    const LOW: u64 = 10_000;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    static RESERVED: Mutex<Vec<OwnedFd>> = Mutex::new(Vec::new());
    let ephemeral = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|range| range.split_whitespace().next()?.parse().ok())
        .unwrap_or(32_768);
    let span = ephemeral.max(LOW + 1_000) - LOW;
    // Each process starts at its own offset, so test binaries running at
    // once seldom try the same ports.
    let start = u64::from(std::process::id()) * 7_919;
    loop {
        let next = NEXT.fetch_add(1, Ordering::Relaxed);
        let port = (LOW + (start + next) % span) as u16;
        if let Some(reservation) = reserve(port) {
            RESERVED.lock().unwrap().push(reservation);
            return port;
        }
    }
}

/// Reserves `port`: a socket bound to it that never listens. It binds as
/// no other socket holds the port, then takes `SO_REUSEADDR`, which lets a
/// server share the port while other processes' reservations fail.
fn reserve(port: u16) -> Option<OwnedFd> {
    use rustix::net::{AddressFamily, SocketType, bind, socket, sockopt};
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let reservation = socket(AddressFamily::INET, SocketType::STREAM, None).ok()?;
    bind(&reservation, &address).ok()?;
    sockopt::set_socket_reuseaddr(&reservation, true).ok()?;
    // Nodes gossip over UDP on their TCP port.
    std::net::UdpSocket::bind(address).ok()?;
    Some(reservation)
}

pub async fn listening(port: u16) {
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("nothing listens on port {port}");
}

/// The `[cache]` settings of `Cluster`s by default.
pub const CLUSTER_CACHE: &str = "block_size = 65536\nextent_size = 1048576\nextents = 32";

/// Configs for storage nodes and a gateway, in `dir`. Bucket `bucket` is
/// immutable, and bucket `changing` keeps metadata for ten minutes; both
/// admit blocks on their first read. Bucket `warm` keeps metadata for ten
/// minutes, and its homes store the uploads that pass through them.
pub struct Cluster {
    pub gateway_port: u16,
    pub gateway: PathBuf,
    /// Node 0's port and config.
    pub node_port: u16,
    pub node: PathBuf,
    /// Every node's port and config, by ID.
    pub nodes: Vec<(u16, PathBuf)>,
}

impl Cluster {
    /// One node. `cache` holds the `[cache]` table's own settings.
    pub fn new(dir: &Path, origin_port: u16, cache: &str) -> Cluster {
        Cluster::with_nodes(dir, origin_port, cache, 1, "", &[])
    }

    /// `count` nodes, each with its own disk. Every config names every node
    /// but those `unnamed`, which only their own configs name, as for nodes
    /// added after the others' configs were written. `cluster` holds more
    /// of the `[cluster]` table's settings.
    pub fn with_nodes(
        dir: &Path,
        origin_port: u16,
        cache: &str,
        count: usize,
        cluster: &str,
        unnamed: &[usize],
    ) -> Cluster {
        std::fs::create_dir_all(dir).unwrap();
        let gateway_port = port();
        let ports: Vec<u16> = (0..count).map(|_| port()).collect();
        let entries: Vec<String> = ports
            .iter()
            .enumerate()
            .map(|(id, port)| format!(r#"{{ id = {id}, address = "127.0.0.1:{port}" }}"#))
            .collect();
        // The nodes a config names: every node for an unnamed node's own,
        // and every node but the unnamed ones for the rest.
        let named = |everyone: bool| {
            let entries: Vec<&str> = entries
                .iter()
                .enumerate()
                .filter(|(id, _)| everyone || !unnamed.contains(id))
                .map(|(_, entry)| entry.as_str())
                .collect();
            entries.join(", ")
        };
        // Only nodes reach S3.
        let origin = format!(
            r#"
            [origin]
            endpoint = "http://127.0.0.1:{origin_port}"
            region = "us-east-1"
            access_key_id = "origin"
            secret_access_key = "origin-secret"
            "#
        );
        let shared = |everyone: bool| {
            let named = named(everyone);
            format!(
                r#"
            [[clients]]
            access_key_id = "reader"
            secret_access_key = "reader-secret"
            grants = [{{ bucket = "bucket" }}, {{ bucket = "changing" }}, {{ bucket = "warm" }}]
            [cache]
            {cache}
            [cache.buckets.bucket]
            immutable = true
            admit_on_first_read = true
            [cache.buckets.changing]
            ttl_ms = 600000
            admit_on_first_read = true
            [cache.buckets.warm]
            ttl_ms = 600000
            warm_on_write = true
            [cluster]
            secret = "cluster-secret"
            nodes = [{named}]
            {cluster}
            "#
            )
        };
        let nodes: Vec<(u16, PathBuf)> = ports
            .iter()
            .enumerate()
            .map(|(id, &port)| {
                let config = dir.join(format!("node-{id}.toml"));
                let role = format!(
                    "[node]\nid = {id}\ndata_dir = \"{}\"\n",
                    dir.join(format!("disk-{id}")).display()
                );
                let shared = shared(unnamed.contains(&id));
                std::fs::write(&config, format!("{origin}\n{shared}\n{role}")).unwrap();
                (port, config)
            })
            .collect();
        let gateway = dir.join("gateway.toml");
        let gateway_role = format!("[gateway]\nlisten = \"127.0.0.1:{gateway_port}\"\n");
        std::fs::write(&gateway, format!("{}\n{gateway_role}", shared(false))).unwrap();
        Cluster {
            gateway_port,
            gateway,
            node_port: nodes[0].0,
            node: nodes[0].1.clone(),
            nodes,
        }
    }

    /// Starts node `id` and waits until it listens.
    pub async fn start(&self, id: usize) -> Process {
        let (port, config) = &self.nodes[id];
        let process = Process::start(config);
        listening(*port).await;
        process
    }

    pub async fn start_node(&self) -> Process {
        let process = Process::start(&self.node);
        listening(self.node_port).await;
        process
    }

    pub async fn start_gateway(&self) -> Process {
        let process = Process::start(&self.gateway);
        listening(self.gateway_port).await;
        process
    }

    pub async fn get(&self, key: &str) -> (u16, Vec<u8>) {
        let path = format!("/bucket/{key}");
        send(self.gateway_port, "GET", &path, "", &[], Vec::new()).await
    }
}
