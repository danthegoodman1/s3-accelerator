//! A fake S3 that counts its requests, and a signed client, for running the
//! server end to end.

#![allow(dead_code, reason = "each test crate uses its own part of the harness")]

use s3_accelerator::config::Config;
use s3_accelerator::http::{Connection, Framing, Response};
use s3_accelerator::server::{self, Listeners};
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
use std::cell::{Cell, RefCell};
use std::io;
use std::net::TcpListener as StdListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub const ETAG: &str = "\"0123456789abcdef\"";
pub const SIZE: usize = 300_000;

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

/// What the fake S3 has seen, and whether a `DeleteObjects` removed its
/// object.
pub struct Origin {
    pub requests: Cell<u64>,
    pub deleted: Cell<bool>,
    /// Each request's path, as it arrived.
    pub paths: RefCell<Vec<String>>,
    /// How long it waits before each answer.
    pub delay: Cell<std::time::Duration>,
    /// The size of its objects, and whether each path has its own content
    /// (`object_of`) rather than all sharing `object()`'s.
    pub size: Cell<usize>,
    pub distinct: Cell<bool>,
    /// Bodies of the writes it received in full.
    pub uploads: RefCell<Vec<Vec<u8>>>,
    /// Refuse each `PUT` with 403 from its head, before its body arrives.
    pub refuse_writes: Cell<bool>,
    /// How long it waits before each 64 KiB of a body after the first.
    pub trickle: Cell<Duration>,
}

impl Default for Origin {
    fn default() -> Origin {
        Origin {
            requests: Cell::default(),
            deleted: Cell::default(),
            paths: RefCell::default(),
            delay: Cell::default(),
            size: Cell::new(SIZE),
            distinct: Cell::default(),
            uploads: RefCell::default(),
            refuse_writes: Cell::default(),
            trickle: Cell::default(),
        }
    }
}

impl Origin {
    /// The object it serves at `path`.
    pub fn object(&self, path: &str) -> Vec<u8> {
        match self.distinct.get() {
            true => object_of(self.size.get(), path),
            false => object_of(self.size.get(), ""),
        }
    }
}

/// Serves an object at any path, honoring `Range` and `If-Match`, until a
/// `DeleteObjects` removes it. Answers every other request with 200 once
/// its body arrives.
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
                if head.method == "PUT" {
                    origin.uploads.borrow_mut().push(upload);
                }
                let object = origin.object(&head.path);
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
    let size = object.len();
    let range = header("range").and_then(|value| {
        let (first, last) = value.strip_prefix("bytes=")?.split_once('-')?;
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

    /// The server's process: this child, or the child `strace` started.
    fn server_pid(&self) -> u32 {
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

/// A free port on the loopback interface.
pub fn port() -> u16 {
    StdListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

/// Configs for one node and a gateway, in `dir`, whose bucket `bucket` is
/// immutable and admits blocks on their first read.
pub struct Cluster {
    pub gateway_port: u16,
    pub node_port: u16,
    pub node: PathBuf,
    pub gateway: PathBuf,
}

impl Cluster {
    /// `cache` holds the `[cache]` table's own settings.
    pub fn new(dir: &Path, origin_port: u16, cache: &str) -> Cluster {
        std::fs::create_dir_all(dir).unwrap();
        let (gateway_port, node_port) = (port(), port());
        let shared = format!(
            r#"
            [origin]
            endpoint = "http://127.0.0.1:{origin_port}"
            region = "us-east-1"
            access_key_id = "origin"
            secret_access_key = "origin-secret"
            [[clients]]
            access_key_id = "reader"
            secret_access_key = "reader-secret"
            grants = [{{ bucket = "bucket" }}]
            [cache]
            {cache}
            [cache.buckets.bucket]
            immutable = true
            admit_on_first_read = true
            [cluster]
            secret = "cluster-secret"
            nodes = [{{ id = 0, address = "127.0.0.1:{node_port}" }}]
            "#
        );
        let node = dir.join("node.toml");
        let node_role = format!(
            "[node]\nid = 0\ndata_dir = \"{}\"\n",
            dir.join("disk").display()
        );
        std::fs::write(&node, format!("{shared}\n{node_role}")).unwrap();
        let gateway = dir.join("gateway.toml");
        let gateway_role = format!("[gateway]\nlisten = \"127.0.0.1:{gateway_port}\"\n");
        std::fs::write(&gateway, format!("{shared}\n{gateway_role}")).unwrap();
        Cluster {
            gateway_port,
            node_port,
            node,
            gateway,
        }
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
