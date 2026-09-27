//! A fake S3 that counts its requests, and a signed client, for running the
//! server end to end.

#![allow(dead_code, reason = "each test crate uses its own part of the harness")]

use s3_accelerator::config::Config;
use s3_accelerator::http::{Connection, Response};
use s3_accelerator::server::{self, Listeners};
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use std::cell::{Cell, RefCell};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub const ETAG: &str = "\"0123456789abcdef\"";
pub const SIZE: usize = 300_000;

pub fn object() -> Vec<u8> {
    (0..SIZE as u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect()
}

/// What the fake S3 has seen, and whether a `DeleteObjects` removed its
/// object.
#[derive(Default)]
pub struct Origin {
    pub requests: Cell<u64>,
    pub deleted: Cell<bool>,
    /// Each request's path, as it arrived.
    pub paths: RefCell<Vec<String>>,
    /// How long it waits before each answer.
    pub delay: Cell<std::time::Duration>,
}

/// Serves one object at any path, honoring `Range` and `If-Match`, until a
/// `DeleteObjects` removes it. Answers every other request with 200.
async fn fake_origin(listener: TcpListener, origin: Rc<Origin>) {
    let object = object();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let (object, origin) = (object.clone(), origin.clone());
        tokio::task::spawn_local(async move {
            let mut connection = Connection::new(stream);
            while let Ok(Some(head)) = connection.read_head().await {
                origin.requests.set(origin.requests.get() + 1);
                origin.paths.borrow_mut().push(head.path.clone());
                let len = head.content_length().unwrap();
                connection.read_body(len).await.unwrap();
                let mut headers = Vec::new();
                let (status, body) = match head.method.as_str() {
                    "POST" if head.query.contains("delete") => {
                        origin.deleted.set(true);
                        (200, Vec::new())
                    }
                    "GET" | "HEAD" if origin.deleted.get() => (404, Vec::new()),
                    "GET" | "HEAD" => {
                        headers.push(("ETag".to_string(), ETAG.to_string()));
                        read(&head.headers, &object, &mut headers)
                    }
                    _ => (200, Vec::new()),
                };
                tokio::time::sleep(origin.delay.get()).await;
                let response = Response {
                    status,
                    headers,
                    content_length: body.len() as u64,
                    body: if head.method == "HEAD" {
                        Vec::new()
                    } else {
                        body
                    }
                    .into(),
                };
                if connection.write_response(&response, true).await.is_err() {
                    return;
                }
            }
        });
    }
}

fn read(
    request: &[(String, String)],
    object: &[u8],
    headers: &mut Vec<(String, String)>,
) -> (u16, Vec<u8>) {
    let header = |name| s3_accelerator::http::header(request, name);
    if header("if-match").is_some_and(|etag| etag != ETAG) {
        return (412, Vec::new());
    }
    let range = header("range").and_then(|value| {
        let (first, last) = value.strip_prefix("bytes=")?.split_once('-')?;
        let first: usize = first.parse().ok()?;
        let last = last
            .parse()
            .map_or(SIZE - 1, |last: usize| last.min(SIZE - 1));
        Some((first, last))
    });
    match range {
        Some((first, last)) => {
            headers.push((
                "Content-Range".into(),
                format!("bytes {first}-{last}/{SIZE}"),
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

/// A server running on this `LocalSet`.
pub struct Server {
    pub port: u16,
    stop: oneshot::Sender<()>,
    done: tokio::task::JoinHandle<io::Result<()>>,
}

impl Server {
    /// Starts a server in front of the S3 at `origin_port`, keeping its
    /// disk in `dir`. `extra` is appended to its config.
    pub async fn start(origin_port: u16, dir: &Path, grants: &str, extra: &str) -> Server {
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
            block_size = 65536
            extent_size = 1048576
            extents = 8
            [cache.default_policy]
            ttl_ms = 60000
            [cluster]
            secret = "cluster-secret"
            nodes = [{{ id = 0, address = "{node_address}" }}]
            [gateway]
            listen = "unused"
            [node]
            id = 0
            data_dir = "{}"
            "#,
            dir.display()
        ))
        .unwrap();
        config.check().unwrap();
        let port = gateway.local_addr().unwrap().port();
        let listeners = Listeners {
            gateway: Some(gateway),
            node: Some(node),
        };
        let (stop, stopped) = oneshot::channel::<()>();
        // A server whose handle is dropped runs until the test ends.
        let stopped = async move {
            if stopped.await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        let done = tokio::task::spawn_local(server::run(config, listeners, stopped));
        Server { port, stop, done }
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

/// The headers of a request `reader` signed.
pub fn signed(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    let signer = Signer {
        credentials: Credentials {
            access_key_id: "reader".into(),
            secret_access_key: "reader-secret".into(),
        },
        region: "us-east-1".into(),
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
        UNSIGNED_PAYLOAD,
        sigv4::unix_now(),
    );
    headers
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
    let url = match query {
        "" => format!("http://127.0.0.1:{port}{path}"),
        query => format!("http://127.0.0.1:{port}{path}?{query}"),
    };
    let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
    let mut request = reqwest::Client::new()
        .request(method.clone(), url)
        .body(body);
    for (name, value) in signed(port, method.as_str(), path, query, extra) {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.bytes().await.unwrap().to_vec())
}
