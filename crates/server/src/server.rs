//! Accepts S3 clients, authenticates and authorizes each request, and serves
//! `GetObject` and `HeadObject` through the core. Other operations pass
//! through to S3 under the cluster's signature.

use crate::config::{Client, Config};
use crate::disk::Disk;
use crate::gateway_engine::{Event, GatewayEngine, SharedGateway};
use crate::http::{Connection, Framing, RequestHead, Response};
use crate::http::{etag_condition, format_content_range, header, parse_range};
use crate::membership_engine;
use crate::node_engine::{self, NodeEngine};
use crate::origin::{self, Origin, RequestBody};
use crate::peers::Peers;
use crate::sigv4::{self, AuthError, Credentials, Signable};
use crate::zero_copy::{self, Short};
use bytes::Bytes;
use http_body_util::channel::{Channel, Sender as BodySender};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use percent_encoding::percent_decode_str;
use s3_accelerator_core::Time;
use s3_accelerator_core::membership::{Membership, Peer};
use s3_accelerator_core::node::Node;
use s3_accelerator_core::placement::NodeId;
use s3_accelerator_core::s3::{Method, ObjectKey, Request, ResponseHead};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

struct Context {
    gateway: SharedGateway,
    origin: Rc<Origin>,
    clients: Vec<Client>,
}

/// The largest `DeleteObjects` body the gateway reads: S3 takes at most
/// 1,000 keys of at most 1,024 bytes each.
const MAX_DELETE_BODY: u64 = 8 << 20;

pub async fn serve(config: Config) -> io::Result<()> {
    let mut listeners = Listeners::default();
    if let Some(gateway) = &config.gateway {
        let listener = TcpListener::bind(&gateway.listen).await?;
        eprintln!("gateway listening on {}", listener.local_addr()?);
        listeners.gateway = Some(listener);
    }
    if let Some(node) = &config.node {
        let address = &config.addresses()[&NodeId(node.id)];
        let listener = TcpListener::bind(address).await?;
        eprintln!("node {} listening on {}", node.id, listener.local_addr()?);
        listeners.node = Some(listener);
    }
    let mut terminate = signal(SignalKind::terminate())?;
    let stop = async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    };
    let mut user1 = signal(SignalKind::user_defined1())?;
    let leave = async move {
        user1.recv().await;
    };
    run_with(config, listeners, stop, leave).await
}

/// Where a process's gateway takes S3 clients and its node takes gateways.
#[derive(Default)]
pub struct Listeners {
    pub gateway: Option<TcpListener>,
    pub node: Option<TcpListener>,
}

/// Serves the gateway and node the config names on `listeners` until `stop`
/// completes; the node then waits for work in progress and shuts its disk
/// down cleanly. Runs on a `LocalSet`.
pub async fn run(
    config: Config,
    listeners: Listeners,
    stop: impl Future<Output = ()> + 'static,
) -> io::Result<()> {
    run_with(config, listeners, stop, std::future::pending()).await
}

/// As `run`, and once `leave` completes, the node leaves the cluster: it
/// drops out of every ring, serves its blocks to their new owners through
/// the fallback window, and then stops.
pub async fn run_with(
    config: Config,
    listeners: Listeners,
    stop: impl Future<Output = ()> + 'static,
    leave: impl Future<Output = ()> + 'static,
) -> io::Result<()> {
    let credentials = Credentials {
        access_key_id: config.origin.access_key_id.clone(),
        secret_access_key: config.origin.secret_access_key.clone(),
    };
    let origin = Rc::new(Origin::new(
        &config.origin.endpoint,
        &config.origin.region,
        credentials,
    ));
    let secret: Rc<str> = config.cluster.secret.as_str().into();
    let peers = Peers::new(config.addresses(), secret.clone());
    let (stopping, stopped) = watch::channel(false);
    let stopping = Rc::new(stopping);
    let stopper = stopping.clone();
    tokio::task::spawn_local(async move {
        stop.await;
        let _ = stopper.send(true);
    });
    let node = match (&config.node, listeners.node) {
        (Some(node), Some(listener)) => {
            let node_config = config.cache.node_config();
            let (disk, recovery) = Disk::open(Path::new(&node.data_dir), node_config.store)?;
            let id = NodeId(node.id);
            let me = config
                .peers()
                .into_iter()
                .find(|peer| peer.id == node.id)
                .map(|peer| Peer {
                    run: disk.run(),
                    ..peer
                })
                .expect("a node is in cluster.nodes");
            let recovered = Node::recover(
                id,
                config.ring(),
                node_config,
                recovery.records,
                recovery.metadata,
            );
            let engine = NodeEngine::new(
                recovered,
                origin.clone(),
                peers.clone(),
                Arc::new(disk),
                config.addresses(),
            );
            let gossip = resolve(&config)?;
            let socket = UdpSocket::bind(gossip[&id]).await?;
            let started = Instant::now();
            let membership = Membership::new(
                Time(0),
                me,
                &config.peers(),
                config.cluster.membership.config(),
                random_seed(),
            );
            let seeds: Vec<NodeId> = gossip.keys().copied().collect();
            let fallback_window = Duration::from_millis(config.cache.fallback_window_ms);
            let (engine_for_membership, peers) = (engine.clone(), peers.clone());
            let stopper = stopping.clone();
            tokio::task::spawn_local(async move {
                let membership = membership_engine::run(
                    started,
                    membership,
                    engine_for_membership,
                    peers,
                    socket,
                    gossip,
                    seeds,
                )
                .await;
                leave.await;
                eprintln!("leaving the cluster");
                membership_engine::start_leaving(&membership);
                tokio::time::sleep(fallback_window).await;
                membership_engine::leave(&membership);
                let _ = stopper.send(true);
            });
            let stop = stopped_signal(stopped.clone());
            Some(tokio::task::spawn_local(node_engine::serve(
                listener,
                engine,
                secret.clone(),
                stop,
            )))
        }
        _ => None,
    };
    if let (Some(_), Some(listener)) = (&config.gateway, listeners.gateway) {
        let gateway = GatewayEngine::new(config.ring(), config.cache.gateway_config(), peers);
        let context = Rc::new(Context {
            gateway,
            origin,
            clients: config.clients,
        });
        serve_clients(listener, context, stopped_signal(stopped)).await?;
    }
    match node {
        Some(node) => node.await.map_err(io::Error::other)?,
        None => Ok(()),
    }
}

/// Each node's cluster address, where it also gossips over UDP.
fn resolve(config: &Config) -> io::Result<BTreeMap<NodeId, SocketAddr>> {
    config
        .addresses()
        .into_iter()
        .map(|(id, address)| {
            let resolved = address.to_socket_addrs()?.next().ok_or_else(|| {
                io::Error::other(format!("node {} has no address: {address}", id.0))
            })?;
            Ok((id, resolved))
        })
        .collect()
}

/// A seed for membership's random choices, which need no secrecy.
fn random_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    nanos ^ (u64::from(std::process::id()) << 32)
}

/// Completes once `run`'s stop has.
async fn stopped_signal(mut stopped: watch::Receiver<bool>) {
    let _ = stopped.wait_for(|stopped| *stopped).await;
}

/// Serves S3 clients on `listener` until `stop` completes.
async fn serve_clients(
    listener: TcpListener,
    context: Rc<Context>,
    stop: impl Future<Output = ()>,
) -> io::Result<()> {
    let ticking = context.gateway.clone();
    let ticker = tokio::task::spawn_local(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            GatewayEngine::tick(&ticking);
        }
    });
    tokio::pin!(stop);
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            () = &mut stop => break,
        };
        stream.set_nodelay(true)?;
        let context = context.clone();
        tokio::task::spawn_local(async move {
            if let Err(error) = connection(stream, &context).await {
                eprintln!("connection closed: {error}");
            }
        });
    }
    ticker.abort();
    Ok(())
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
        // The signature covers the head, so a request is authenticated and
        // authorized before its body is read. A request answered before its
        // body is read closes the connection.
        let refusal = authenticate(&head, context).and_then(|client| authorize(&head, client));
        let reusable = match refusal {
            Err(response) => {
                let keep_alive = head.keep_alive && len == 0;
                connection.write_response(&response, keep_alive).await?;
                keep_alive
            }
            Ok(()) => handle(&mut connection, &head, len, context).await? && head.keep_alive,
        };
        if !reusable {
            // The client may still be sending a body; draining it lets the
            // client read the answer before the socket closes.
            if len > 0 {
                connection.linger().await;
            }
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

/// Whether the client's grants cover the object, and a copy's source.
fn authorize(head: &RequestHead, client: &Client) -> Result<(), Response> {
    let (bucket, key) = split_path(&head.path);
    if !client.may_access(&bucket, &key) {
        return Err(error(403, "AccessDenied", "Access Denied"));
    }
    // A copy reads its source, so the grants must cover the source too.
    if let Some(source) = header(&head.headers, "x-amz-copy-source") {
        let (source_bucket, source_key) = copy_source(source);
        if !client.may_access(&source_bucket, &source_key) {
            return Err(error(403, "AccessDenied", "Access Denied"));
        }
    }
    Ok(())
}

/// Answers an authorized request with a `len`-byte body, and returns
/// whether the connection can take another request.
async fn handle(
    connection: &mut Connection,
    head: &RequestHead,
    len: u64,
    context: &Context,
) -> io::Result<bool> {
    let payload_hash = header(&head.headers, "x-amz-content-sha256").expect("authenticated");
    let is_digest = payload_hash.len() == 64 && payload_hash.bytes().all(|b| b.is_ascii_hexdigit());
    let digest = is_digest.then(|| payload_hash.to_ascii_lowercase());
    let (bucket, key) = split_path(&head.path);
    if len == 0
        && let Some(request) = cacheable(head, &bucket, &key)
    {
        if digest
            .as_deref()
            .is_some_and(|digest| digest != sigv4::EMPTY_SHA256)
        {
            let response = hash_mismatch();
            connection
                .write_response(&response, head.keep_alive)
                .await?;
            return Ok(true);
        }
        return read(connection, request, head.keep_alive, context).await;
    }
    let request = Forward {
        head,
        payload_hash,
        digest,
        len,
        bucket: &bucket,
        key: &key,
    };
    forward(connection, request, context).await
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

/// Serves a read through the gateway: its answer, or a head and then the
/// nodes' bodies, each moved from the node's connection to the client's
/// with `splice`.
async fn read(
    connection: &mut Connection,
    request: Request,
    keep_alive: bool,
    context: &Context,
) -> io::Result<bool> {
    let method = request.method;
    let mut events = GatewayEngine::read(&context.gateway, request);
    // Body bytes the started response still owes.
    let mut remaining = None;
    while let Some(event) = events.recv().await {
        match event {
            Event::Respond(head) => {
                connection
                    .write_response(&answer(&head, method), true)
                    .await?;
                return Ok(true);
            }
            Event::Start(head) => {
                let framing = Framing::Length(head.content_length);
                connection
                    .write_response_head(head.status, &client_headers(&head), framing, keep_alive)
                    .await?;
                remaining = Some(head.content_length);
            }
            Event::Forward { from, body, len } => {
                let want = len.min(body.unread());
                let (copied, relayed) =
                    zero_copy::relay(body.stream(), connection.stream(), want).await;
                if let Err(Short::Destination(error)) = relayed {
                    // The client is gone, and needs none of the rest.
                    GatewayEngine::forwarded(&context.gateway, from, len, None);
                    return Err(error);
                }
                let read_in_full = relayed.is_ok() && copied == body.unread();
                GatewayEngine::forwarded(
                    &context.gateway,
                    from,
                    copied,
                    read_in_full.then_some(body),
                );
                let owed = remaining.unwrap_or(0).saturating_sub(copied);
                remaining = Some(owed);
                if owed == 0 {
                    return Ok(true);
                }
            }
            // The body ends short, and the connection with it.
            Event::Abort => return Ok(false),
        }
    }
    // The gateway dropped the read.
    if remaining.is_none() {
        let response = error(500, "InternalError", "the request was dropped");
        connection.write_response(&response, false).await?;
    }
    Ok(false)
}

/// The headers of a client's response.
fn client_headers(head: &ResponseHead) -> Vec<(String, String)> {
    let mut headers = vec![("Accept-Ranges".to_string(), "bytes".to_string())];
    headers.extend(head.headers.iter().cloned());
    if let Some(etag) = &head.etag {
        headers.push(("ETag".to_string(), etag.0.clone()));
    }
    if let Some(range) = head.content_range {
        headers.push(("Content-Range".to_string(), format_content_range(range)));
    }
    if head.status >= 400 {
        headers.push(("Content-Type".to_string(), "application/xml".to_string()));
    }
    headers
}

/// A response with no body from the nodes: a HEAD's, or an answer the core
/// gave itself, with an S3 error body for a failure.
fn answer(head: &ResponseHead, method: Method) -> Response {
    if method == Method::Get && head.status >= 400 {
        return error(head.status, error_code(head), reason_message(head.status));
    }
    let content_length = match method {
        Method::Head => head.content_length,
        Method::Get => 0,
    };
    Response {
        status: head.status,
        headers: client_headers(head),
        content_length,
        body: Bytes::new(),
    }
}

/// A request the gateway passes to S3.
struct Forward<'a> {
    head: &'a RequestHead,
    payload_hash: &'a str,
    /// The body's SHA-256, when the client signed it.
    digest: Option<String>,
    len: u64,
    bucket: &'a str,
    key: &'a str,
}

/// Passes a request to S3 and its response back. The body streams to S3
/// as it arrives, except a `DeleteObjects` list, which the gateway reads
/// to learn the keys it deletes.
async fn forward(
    connection: &mut Connection,
    request: Forward<'_>,
    context: &Context,
) -> io::Result<bool> {
    let head = request.head;
    let deletes_objects = head.method == "POST"
        && !request.bucket.is_empty()
        && request.key.is_empty()
        && head
            .query
            .split('&')
            .any(|pair| pair == "delete" || pair.starts_with("delete="));
    if deletes_objects && request.len > MAX_DELETE_BODY {
        let response = error(
            400,
            "EntityTooLarge",
            "the key list is larger than S3 takes",
        );
        connection.write_response(&response, false).await?;
        return Ok(false);
    }
    let expects_continue = header(&head.headers, "expect")
        .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
    if request.len > 0 && expects_continue {
        connection.write_continue().await?;
    }
    let (sent, listed) = match deletes_objects {
        true => {
            let body = Bytes::from(connection.read_body(request.len).await?);
            if request
                .digest
                .as_ref()
                .is_some_and(|digest| hex::encode(Sha256::digest(&body)) != *digest)
            {
                connection.write_response(&hash_mismatch(), true).await?;
                return Ok(true);
            }
            let keys = listed_keys(&String::from_utf8_lossy(&body));
            let body = Full::new(body).map_err(|never| match never {}).boxed();
            let sent = send_to_s3(&request, body, context).await;
            (
                Sent {
                    response: sent,
                    body_read: true,
                    matched: true,
                },
                Some(keys),
            )
        }
        false => (upload(connection, &request, context).await, None),
    };
    if !sent.matched {
        connection
            .write_response(&hash_mismatch(), sent.body_read)
            .await?;
        return Ok(sent.body_read);
    }
    let response = match sent.response {
        Ok(response) => response,
        Err(failure) => {
            let response = error(502, "BadGateway", &failure.to_string());
            connection.write_response(&response, false).await?;
            return Ok(false);
        }
    };
    if response.status().as_u16() < 300 && !request.bucket.is_empty() {
        let keys = listed.unwrap_or_else(|| written_keys(head, request.key));
        for key in keys {
            let key = ObjectKey {
                bucket: request.bucket.to_string(),
                key,
            };
            GatewayEngine::written(&context.gateway, &key);
        }
    }
    let passed = pass_response(connection, head, response).await?;
    Ok(passed && sent.body_read)
}

/// How a forwarded request went.
struct Sent {
    response: io::Result<hyper::Response<Incoming>>,
    /// Whether the client's body was read to its end.
    body_read: bool,
    /// Whether the body matched its signed hash.
    matched: bool,
}

async fn send_to_s3(
    request: &Forward<'_>,
    body: RequestBody,
    context: &Context,
) -> io::Result<hyper::Response<Incoming>> {
    let head = request.head;
    let sent = context.origin.forward(
        &head.method,
        &head.path,
        &head.query,
        &head.headers,
        request.payload_hash,
        body,
        request.len,
    );
    tokio::time::timeout(origin::READ_TIMEOUT, sent)
        .await
        .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
}

/// Streams the client's body to S3 while S3's answer is awaited. S3 may
/// answer before the body ends, such as to refuse it.
async fn upload(connection: &mut Connection, request: &Forward<'_>, context: &Context) -> Sent {
    let (sender, body) = Channel::<Bytes, io::Error>::new(2);
    let head = request.head;
    let sent = context.origin.forward(
        &head.method,
        &head.path,
        &head.query,
        &head.headers,
        request.payload_hash,
        body.boxed(),
        request.len,
    );
    let mut sent = std::pin::pin!(sent);
    let mut passing = std::pin::pin!(pass_body(connection, request, sender));
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
    Sent {
        response,
        body_read: matches!(passed, Some(Ok(_))),
        matched: !matches!(passed, Some(Ok(false))),
    }
}

/// Passes the client's body to S3 as it arrives, and returns whether it
/// matched its signed hash. The last bytes wait until the whole body is
/// checked, so S3 never receives a body that fails its hash.
async fn pass_body(
    connection: &mut Connection,
    request: &Forward<'_>,
    mut sender: BodySender<Bytes, io::Error>,
) -> io::Result<bool> {
    let mut hasher = Sha256::new();
    let mut remaining = request.len;
    let mut held: Option<Bytes> = None;
    while remaining > 0 {
        let max = usize::try_from(remaining).unwrap_or(usize::MAX);
        let chunk = match connection.read_some(max).await {
            Ok(chunk) => chunk,
            Err(error) => {
                sender.abort(io::Error::other("the client's body ended early"));
                return Err(error);
            }
        };
        remaining -= chunk.len() as u64;
        if request.digest.is_some() {
            hasher.update(&chunk);
        }
        if let Some(previous) = held.replace(chunk)
            && sender.send_data(previous).await.is_err()
        {
            return Err(io::Error::other("S3 stopped taking the body"));
        }
    }
    if let Some(digest) = &request.digest
        && hex::encode(hasher.finalize()) != *digest
    {
        sender.abort(io::Error::other("the body does not match its hash"));
        return Ok(false);
    }
    if let Some(last) = held
        && sender.send_data(last).await.is_err()
    {
        return Err(io::Error::other("S3 stopped taking the body"));
    }
    Ok(true)
}

/// Sends S3's response on to the client as it arrives, and returns whether
/// it arrived in full.
async fn pass_response(
    connection: &mut Connection,
    head: &RequestHead,
    response: hyper::Response<Incoming>,
) -> io::Result<bool> {
    let status = response.status().as_u16();
    let headers = origin::header_pairs(response.headers());
    let length = header(&headers, "content-length").and_then(|value| value.parse::<u64>().ok());
    let hop = [
        "connection",
        "content-length",
        "keep-alive",
        "transfer-encoding",
    ];
    let headers: Vec<(String, String)> = headers
        .into_iter()
        .filter(|(name, _)| !hop.contains(&name.as_str()))
        .collect();
    let bodiless = head.method == "HEAD" || status == 204 || status == 304;
    let framing = match (bodiless, length) {
        (true, length) => Framing::Length(length.unwrap_or(0)),
        (false, Some(length)) => Framing::Length(length),
        (false, None) => Framing::Chunked,
    };
    connection
        .write_response_head(status, &headers, framing, head.keep_alive)
        .await?;
    if bodiless {
        return Ok(true);
    }
    let mut body = response.into_body();
    let mut sent = 0;
    loop {
        match origin::next_frame(&mut body).await {
            Ok(Some(chunk)) => {
                match framing {
                    Framing::Length(_) => connection.write_all(&chunk).await?,
                    Framing::Chunked => connection.write_chunk(&chunk).await?,
                }
                sent += chunk.len() as u64;
            }
            Ok(None) => break,
            // The client's body ends short, and the connection with it.
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

fn hash_mismatch() -> Response {
    error(
        400,
        "XAmzContentSHA256Mismatch",
        "the body does not match its hash",
    )
}

/// The key a successful request wrote: the path's key for a `PUT`, `POST`
/// or `DELETE` of an object.
fn written_keys(head: &RequestHead, key: &str) -> Vec<String> {
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
}
