//! Accepts S3 clients, authenticates and authorizes each request, and serves
//! `GetObject` and `HeadObject` through the core. Other operations pass
//! through a storage node, which signs them for S3.

use crate::config::{Access, Client, Config};
use crate::disk::Disk;
use crate::gateway_engine::{Event, GatewayEngine, SharedGateway};
use crate::http::{Connection, Framing, RequestHead, Response};
use crate::http::{etag_condition, format_content_range, header, parse_range, split_path};
use crate::membership_engine::{self, GossipKey};
use crate::node_engine::{self, NodeEngine};
use crate::origin::{self, Origin};
use crate::passthrough::{self, ToNode};
use crate::peers::{Exchanged, Peers};
use crate::protocol::{self, NodeAnswer, NodeRequest};
use crate::sigv4::{self, AuthError, Credentials, Signable};
use crate::sqs::Queue;
use crate::tls::{self, Connector, Tls};
use crate::zero_copy::{self, Short};
use bytes::Bytes;
use percent_encoding::percent_decode_str;
use s3_accelerator_core::Time;
use s3_accelerator_core::membership::{Membership, Peer};
use s3_accelerator_core::node::Node;
use s3_accelerator_core::placement::NodeId;
use s3_accelerator_core::s3::{Method, ObjectKey, Request, ResponseHead};
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, UdpSocket};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

struct Context {
    gateway: SharedGateway,
    clients: Vec<Client>,
    /// Domains the gateway takes virtual-hosted-style requests for.
    domains: Vec<String>,
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
    let secret: Rc<str> = config.cluster.secret.as_str().into();
    let client_tls = config
        .gateway
        .as_ref()
        .and_then(|gateway| gateway.tls.as_ref());
    let wants_kernel = client_tls.is_some_and(|tls| tls.kernel)
        || config.cluster.tls.as_ref().is_some_and(|tls| tls.kernel);
    let kernel = tls::kernel(wants_kernel).await;
    let (members, connector) = match &config.cluster.tls {
        Some(tls) => {
            let kernel = kernel.as_ref().filter(|_| tls.kernel);
            let members = Rc::new(Tls::members(tls, kernel)?);
            (Some(members), Some(Connector::new(tls, kernel)?))
        }
        None => (None, None),
    };
    let clients = match client_tls {
        Some(tls) => {
            let kernel = kernel.as_ref().filter(|_| tls.kernel);
            Some(Rc::new(Tls::clients(tls, kernel)?))
        }
        None => None,
    };
    let peers = Peers::new(config.addresses(), secret.clone(), connector);
    let (stopping, stopped) = watch::channel(false);
    let stopping = Rc::new(stopping);
    let stopper = stopping.clone();
    tokio::task::spawn_local(async move {
        stop.await;
        let _ = stopper.send(true);
    });
    let node = match (&config.node, listeners.node) {
        (Some(node), Some(listener)) => {
            let origin = config
                .origin
                .as_ref()
                .expect("checked: a node has an origin");
            let credentials = || Credentials {
                access_key_id: origin.access_key_id.clone(),
                secret_access_key: origin.secret_access_key.clone(),
            };
            let queue = match &config.events {
                Some(events) => {
                    let region = events.region.as_ref().unwrap_or(&origin.region);
                    let queue = Queue::new(&events.queue_url, region, credentials())?;
                    let visibility = Duration::from_secs(events.visibility_timeout_s);
                    Some((Rc::new(queue), visibility))
                }
                None => None,
            };
            let origin = Rc::new(Origin::new(&origin.endpoint, &origin.region, credentials()));
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
                recovery.purges,
            );
            let engine = NodeEngine::new(
                recovered,
                origin,
                peers.clone(),
                Arc::new(disk),
                config.addresses(),
            );
            let address = &config.addresses()[&id];
            let own = tokio::net::lookup_host(address.as_str())
                .await?
                .next()
                .ok_or_else(|| io::Error::other(format!("{address} does not resolve")))?;
            let socket = UdpSocket::bind(own).await?;
            let started = Instant::now();
            let membership = Membership::new(
                Time(0),
                me,
                &config.peers(),
                config.cluster.membership.config(),
                random_seed(),
            );
            let seeds: Vec<NodeId> = config.addresses().into_keys().collect();
            let fallback_window = Duration::from_millis(config.cache.fallback_window_ms);
            let gossip_key = GossipKey::new(&config.cluster.secret);
            let (engine_for_membership, peers) = (engine.clone(), peers.clone());
            let stopper = stopping.clone();
            tokio::task::spawn_local(async move {
                let membership = membership_engine::run(
                    started,
                    membership,
                    engine_for_membership,
                    peers,
                    socket,
                    seeds,
                    gossip_key,
                )
                .await;
                leave.await;
                eprintln!("leaving the cluster");
                membership_engine::start_leaving(&membership);
                tokio::time::sleep(fallback_window).await;
                membership_engine::leave(&membership);
                let _ = stopper.send(true);
            });
            if let Some((queue, visibility)) = queue {
                let engine = engine.clone();
                tokio::task::spawn_local(node_engine::take_events(engine, queue, visibility));
            }
            let stop = stopped_signal(stopped.clone());
            Some(tokio::task::spawn_local(node_engine::serve(
                listener,
                members,
                engine,
                secret.clone(),
                stop,
            )))
        }
        _ => None,
    };
    if let (Some(_), Some(listener)) = (&config.gateway, listeners.gateway) {
        let gateway = GatewayEngine::new(config.ring(), config.cache.gateway_config(), peers);
        let domains = config
            .gateway
            .as_ref()
            .map(|gateway| gateway.domains.clone())
            .unwrap_or_default();
        let context = Rc::new(Context {
            gateway,
            clients: config.clients,
            domains,
        });
        serve_clients(listener, context, clients, stopped_signal(stopped)).await?;
    }
    match node {
        Some(node) => node.await.map_err(io::Error::other)?,
        None => Ok(()),
    }
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
    tls: Option<Rc<Tls>>,
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
        let (context, tls) = (context.clone(), tls.clone());
        tokio::task::spawn_local(async move {
            let Some(connection) = tls::accept(tls.as_deref(), stream).await else {
                return;
            };
            if let Err(error) = serve_connection(connection, &context).await {
                eprintln!("connection closed: {error}");
            }
        });
    }
    ticker.abort();
    Ok(())
}

async fn serve_connection(mut connection: Connection, context: &Context) -> io::Result<()> {
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
        let authorized = authenticate(&head, context).and_then(|client| {
            let head = normalize(&head, &context.domains);
            authorize(&head, client).map(|()| (head, client))
        });
        let reusable = match authorized {
            Err(response) => {
                let keep_alive = head.keep_alive && len == 0;
                connection.write_response(&response, keep_alive).await?;
                keep_alive
            }
            Ok((normal, client)) => {
                handle(&mut connection, &normal, client, len, context).await? && head.keep_alive
            }
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

/// The client that signed the request's head, in its `Authorization`
/// header or, for a presigned URL, its query.
fn authenticate<'c>(head: &RequestHead, context: &'c Context) -> Result<&'c Client, Response> {
    let signed_header = header(&head.headers, "authorization").is_some();
    if !signed_header && !sigv4::is_presigned(&head.query) {
        return Err(auth_error(AuthError::Missing));
    }
    let payload_hash = match header(&head.headers, "x-amz-content-sha256") {
        Some(payload_hash) => payload_hash,
        // A presigned URL signs no body.
        None if !signed_header => sigv4::UNSIGNED_PAYLOAD,
        None => return Err(error(400, "InvalidRequest", "missing x-amz-content-sha256")),
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

/// The request as the gateway serves it once its signature checks out:
/// path-style, with its bucket first in the path; without a presigned
/// URL's signature, which S3 must not see beside the node's; and with a
/// presigned body's payload hash.
fn normalize(head: &RequestHead, domains: &[String]) -> RequestHead {
    let mut normal = head.clone();
    if let Some(bucket) = virtual_bucket(header(&head.headers, "host"), domains) {
        normal.path = format!("/{bucket}{}", head.path);
    }
    if sigv4::is_presigned(&head.query) {
        let kept: Vec<&str> = head
            .query
            .split('&')
            .filter(|pair| {
                !sigv4::PRESIGN_PARAMETERS.contains(&pair.split('=').next().unwrap_or(""))
            })
            .collect();
        normal.query = kept.join("&");
        if header(&head.headers, "x-amz-content-sha256").is_none() {
            let unsigned = (
                "x-amz-content-sha256".to_string(),
                sigv4::UNSIGNED_PAYLOAD.to_string(),
            );
            normal.headers.push(unsigned);
        }
    }
    normal
}

/// The bucket a virtual-hosted-style request names: what precedes one of
/// the gateway's domains in its `Host`.
fn virtual_bucket(host: Option<&str>, domains: &[String]) -> Option<String> {
    let host = host?.to_ascii_lowercase();
    let name = match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|byte| byte.is_ascii_digit()) => name,
        _ => host.as_str(),
    };
    domains.iter().find_map(|domain| {
        let bucket = name.strip_suffix(domain.as_str())?.strip_suffix('.')?;
        (!bucket.is_empty()).then(|| bucket.to_string())
    })
}

/// What a request needs of the client's grants.
enum Need {
    /// Nothing grants it: it is malformed, for the reason given.
    Invalid(&'static str),
    /// Access to its object.
    Object(Access),
    /// Reading the prefix it lists.
    List(String),
    /// Access to part of its bucket: for `DeleteObjects`, whose keys the
    /// grants must each cover, and for requests that reveal only that the
    /// bucket exists.
    Part(Access),
    /// Changing or deleting the bucket.
    Admin,
}

/// Query parameters a listing may carry.
const LISTING: [&str; 17] = [
    "continuation-token",
    "delimiter",
    "encoding-type",
    "fetch-owner",
    "key-marker",
    "list-type",
    "marker",
    "max-keys",
    "max-uploads",
    "optional-object-attributes",
    "prefix",
    "start-after",
    "upload-id-marker",
    "uploads",
    "version-id-marker",
    "versions",
    "x-id",
];

fn need(head: &RequestHead, key: &str) -> Need {
    let reads = head.method == "GET" || head.method == "HEAD";
    if !key.is_empty() {
        return Need::Object(if reads { Access::Read } else { Access::Write });
    }
    let parameters: Vec<(&str, &str)> = head
        .query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect();
    let names: Vec<&str> = parameters.iter().map(|(name, _)| *name).collect();
    match head.method.as_str() {
        "HEAD" if names.is_empty() => Need::Part(Access::Read),
        "GET" if names == ["location"] => Need::Part(Access::Read),
        // Each parameter once, so the prefix checked is the one S3 lists.
        "GET"
            if names
                .iter()
                .enumerate()
                .any(|(at, name)| names[..at].contains(name)) =>
        {
            Need::Invalid("a listing names a parameter twice")
        }
        "GET" if names.iter().all(|name| LISTING.contains(name)) => {
            let prefix = parameters
                .iter()
                .find(|(name, _)| *name == "prefix")
                .map(|(_, prefix)| percent_decode_str(prefix).decode_utf8_lossy().into_owned());
            Need::List(prefix.unwrap_or_default())
        }
        "POST" if names.contains(&"delete") => Need::Part(Access::Write),
        _ => Need::Admin,
    }
}

/// Whether the client's grants cover what the request does, and a copy's
/// source.
fn authorize(head: &RequestHead, client: &Client) -> Result<(), Response> {
    let (bucket, key) = split_path(&head.path);
    let allowed = match need(head, &key) {
        Need::Invalid(reason) => return Err(error(400, "InvalidArgument", reason)),
        Need::Object(access) => client.may(access, &bucket, &key),
        Need::List(prefix) => client.may(Access::Read, &bucket, &prefix),
        Need::Part(access) => client.may_reach(access, &bucket),
        Need::Admin => client.may(Access::Admin, &bucket, ""),
    };
    if !allowed {
        return Err(error(403, "AccessDenied", "Access Denied"));
    }
    // A copy reads its source, so the grants must cover the source too.
    if let Some(source) = header(&head.headers, "x-amz-copy-source") {
        let (source_bucket, source_key) = copy_source(source);
        if !client.may(Access::Read, &source_bucket, &source_key) {
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
    client: &Client,
    len: u64,
    context: &Context,
) -> io::Result<bool> {
    let payload_hash = header(&head.headers, "x-amz-content-sha256").expect("authenticated");
    let is_digest = payload_hash.len() == 64 && payload_hash.bytes().all(|b| b.is_ascii_hexdigit());
    let digest = is_digest.then(|| payload_hash.to_ascii_lowercase());
    let (bucket, key) = split_path(&head.path);
    let purges = head
        .query
        .split('&')
        .any(|pair| pair.split('=').next() == Some(PURGE));
    if head.method == "POST" && purges && !bucket.is_empty() && !key.is_empty() {
        if len > 0 {
            let response = error(400, "InvalidRequest", "a purge has no body");
            connection.write_response(&response, false).await?;
            return Ok(false);
        }
        let key = ObjectKey { bucket, key };
        return purge(connection, head, key, context).await;
    }
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
        let Some(overrides) = overrides(&head.query) else {
            let response = error(
                400,
                "InvalidArgument",
                "a response override holds a control character",
            );
            connection
                .write_response(&response, head.keep_alive)
                .await?;
            return Ok(true);
        };
        let presenting = Presenting {
            overrides,
            checksums: header(&head.headers, "x-amz-checksum-mode")
                .is_some_and(|mode| mode.eq_ignore_ascii_case("enabled")),
        };
        return read(connection, request, &presenting, head.keep_alive, context).await;
    }
    let request = Passing {
        head,
        client,
        payload_hash,
        digest,
        len,
        bucket: &bucket,
        key: &key,
    };
    pass(connection, request, context).await
}

/// The query parameter that makes a `POST` to an object a purge.
const PURGE: &str = "x-accel-purge";

/// Purges an object from the cache: its home, or the next candidate when
/// the home does not answer, drops the object's metadata and blocks and
/// has every other node drop theirs. The client hears once the purge is
/// durable on the node that coordinates it.
async fn purge(
    connection: &mut Connection,
    head: &RequestHead,
    key: ObjectKey,
    context: &Context,
) -> io::Result<bool> {
    let peers = GatewayEngine::peers(&context.gateway);
    for node in GatewayEngine::pass_candidates(&context.gateway, &key) {
        let request = NodeRequest::Purge {
            key: key.clone(),
            passed_on: false,
        };
        match peers.exchange(node, &request).await {
            Ok(Exchanged {
                answer: NodeAnswer::Written,
                body,
                ..
            }) => {
                peers.idle(body);
                GatewayEngine::written_via(&context.gateway, &key, node);
                let response = Response {
                    status: 204,
                    headers: Vec::new(),
                    content_length: 0,
                    body: Bytes::new(),
                };
                connection
                    .write_response(&response, head.keep_alive)
                    .await?;
                return Ok(true);
            }
            Ok(_) => eprintln!("node {} answered a purge out of protocol", node.0),
            Err(failure) => eprintln!("purging through node {}: {failure}", node.0),
        }
    }
    let response = error(503, "ServiceUnavailable", "no storage node took the purge");
    connection
        .write_response(&response, head.keep_alive)
        .await?;
    Ok(true)
}

/// The core's request, if the core serves this one.
fn cacheable(head: &RequestHead, bucket: &str, key: &str) -> Option<Request> {
    let method = match head.method.as_str() {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        _ => return None,
    };
    // The cache serves reads that name only their operation and the
    // response headers they set.
    let served_query = head.query.split('&').all(|pair| {
        let name = pair.split('=').next().unwrap_or("");
        pair.is_empty()
            || name == "x-id"
            || OVERRIDES.iter().any(|(parameter, _)| *parameter == name)
    });
    let unsupported = [
        "if-modified-since",
        "if-unmodified-since",
        "x-amz-server-side-encryption-customer-key",
    ];
    if bucket.is_empty()
        || key.is_empty()
        || !served_query
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
    presenting: &Presenting,
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
                let mut response = answer(&head, method);
                response.headers = presenting.present(response.status, response.headers);
                connection.write_response(&response, true).await?;
                return Ok(true);
            }
            Event::Start(head) => {
                let framing = Framing::Length(head.content_length);
                let headers = presenting.present(head.status, client_headers(&head));
                connection
                    .write_response_head(head.status, &headers, framing, keep_alive)
                    .await?;
                remaining = Some(head.content_length);
            }
            Event::Forward { from, body, len } => {
                let want = len.min(body.unread());
                let kernel_tls = body.kernel_tls() || connection.kernel_tls();
                let (copied, relayed) =
                    zero_copy::relay(body.stream(), connection.stream(), want, kernel_tls).await;
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

/// Query parameters that set a header of a read's response, and the
/// header each sets.
const OVERRIDES: [(&str, &str); 6] = [
    ("response-cache-control", "Cache-Control"),
    ("response-content-disposition", "Content-Disposition"),
    ("response-content-encoding", "Content-Encoding"),
    ("response-content-language", "Content-Language"),
    ("response-content-type", "Content-Type"),
    ("response-expires", "Expires"),
];

/// The headers a read's `response-*` parameters set, or `None` if a value
/// holds a control character, which would end the header early.
fn overrides(query: &str) -> Option<Vec<(String, String)>> {
    let mut overrides = Vec::new();
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let Some((_, header)) = OVERRIDES.iter().find(|(parameter, _)| *parameter == name) else {
            continue;
        };
        let value = percent_decode_str(value).decode_utf8_lossy().into_owned();
        if value.chars().any(char::is_control) {
            return None;
        }
        overrides.push((header.to_string(), value));
    }
    Some(overrides)
}

/// How the gateway presents a read's answer: with the headers its
/// `response-*` parameters set, and with the object's checksums if it
/// asked for them.
struct Presenting {
    overrides: Vec<(String, String)>,
    checksums: bool,
}

impl Presenting {
    /// The headers of an answer with `status`. S3 sends checksums only for
    /// a whole object, and applies overrides only to a success.
    fn present(&self, status: u16, mut headers: Vec<(String, String)>) -> Vec<(String, String)> {
        if !(self.checksums && status == 200) {
            headers.retain(|(name, _)| !origin::is_checksum_header(name));
        }
        if (200..300).contains(&status) {
            let overrides = &self.overrides;
            headers.retain(|(name, _)| {
                !overrides
                    .iter()
                    .any(|(set, _)| set.eq_ignore_ascii_case(name))
            });
            headers.extend(overrides.iter().cloned());
        }
        headers
    }
}

/// A request the gateway passes to S3 through a storage node.
struct Passing<'a> {
    head: &'a RequestHead,
    client: &'a Client,
    payload_hash: &'a str,
    /// The body's SHA-256, when the client signed it.
    digest: Option<String>,
    len: u64,
    bucket: &'a str,
    key: &'a str,
}

/// Passes a request to S3 through a storage node, and S3's answer back.
/// An object's request goes through its home, and a bucket's through a
/// node chosen by the bucket. The body streams to the node as it arrives,
/// except a `DeleteObjects` list, which the gateway reads to learn the keys
/// it deletes.
async fn pass(
    connection: &mut Connection,
    request: Passing<'_>,
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
    let listed = match deletes_objects {
        true => {
            let body = connection.read_body(request.len).await?;
            if request
                .digest
                .as_ref()
                .is_some_and(|digest| hex::encode(Sha256::digest(&body)) != *digest)
            {
                connection
                    .write_response(&hash_mismatch(), head.keep_alive)
                    .await?;
                return Ok(true);
            }
            // The grants must cover every key the list deletes.
            let Some(named) = listed_keys(&String::from_utf8_lossy(&body)) else {
                let response = error(
                    400,
                    "MalformedXML",
                    "the key list is not XML the gateway reads",
                );
                connection
                    .write_response(&response, head.keep_alive)
                    .await?;
                return Ok(true);
            };
            if !named
                .iter()
                .all(|key| request.client.may(Access::Write, request.bucket, key))
            {
                let response = error(403, "AccessDenied", "Access Denied");
                connection
                    .write_response(&response, head.keep_alive)
                    .await?;
                return Ok(true);
            }
            Some((body, named))
        }
        false => None,
    };
    let forward = NodeRequest::Forward(protocol::Forward {
        method: head.method.clone(),
        path: head.path.clone(),
        query: head.query.clone(),
        headers: head
            .headers
            .iter()
            .filter(|(name, _)| !origin::is_hop_header(name))
            .cloned()
            .collect(),
        payload_hash: request.payload_hash.to_string(),
        len: request.len,
    });
    let target = ObjectKey {
        bucket: request.bucket.to_string(),
        key: request.key.to_string(),
    };
    let peers = GatewayEngine::peers(&context.gateway);
    let mut opened = None;
    for node in GatewayEngine::pass_candidates(&context.gateway, &target) {
        match peers.send_head(node, &forward, request.len).await {
            Ok(connection) => {
                opened = Some((node, connection));
                break;
            }
            Err(failure) => eprintln!("passing a request to node {}: {failure}", node.0),
        }
    }
    let Some((node, mut to_node)) = opened else {
        let response = error(503, "ServiceUnavailable", "no storage node answered");
        connection.write_response(&response, false).await?;
        return Ok(listed.is_some());
    };
    let body_read = match &listed {
        Some((body, _)) => to_node.write_all(body).await.is_ok(),
        None => {
            let stream = to_node.stream();
            let digest = request.digest.as_deref();
            let passing = passthrough::pass_body(connection, request.len, digest, ToNode(stream));
            // The node may answer before the body ends, such as when S3
            // refuses it.
            let answered = async {
                let _ = stream.peek(&mut [0; 1]).await;
            };
            let passed = tokio::select! {
                biased;
                passed = passing => Some(passed),
                () = answered => None,
            };
            match passed {
                Some(Ok(true)) => true,
                Some(Ok(false)) => {
                    // Closing the node's connection ends the body short,
                    // so S3 never takes it.
                    connection
                        .write_response(&hash_mismatch(), head.keep_alive)
                        .await?;
                    return Ok(true);
                }
                Some(Err(Short::Source(failure))) => return Err(failure),
                Some(Err(Short::Destination(_))) | None => false,
            }
        }
    };
    let answered = match to_node.read_response_head().await {
        Ok((status, headers)) => protocol::decode_answer(status, &headers)
            .map(|answer| (answer, headers))
            .map_err(io::Error::other),
        Err(failure) => Err(failure),
    };
    let (status, headers, length, node_closes) = match answered {
        Ok((
            NodeAnswer::Forwarded {
                status,
                headers,
                length,
            },
            hop,
        )) => {
            if let Some(versions) = protocol::versions(&hop) {
                GatewayEngine::versions(&context.gateway, node, versions);
            }
            let closes = header(&hop, "connection").is_some_and(|value| value == "close");
            (status, headers, length, closes)
        }
        Ok(_) | Err(_) => {
            if let Err(failure) = answered {
                eprintln!("passing a request through node {}: {failure}", node.0);
            }
            let response = error(502, "BadGateway", "the storage node failed");
            connection.write_response(&response, false).await?;
            return Ok(false);
        }
    };
    if status < 300 {
        match listed {
            Some((_, named)) => {
                let keys = named
                    .into_iter()
                    .map(|key| ObjectKey {
                        bucket: request.bucket.to_string(),
                        key,
                    })
                    .collect();
                GatewayEngine::written(&context.gateway, keys).await;
            }
            None => {
                if let Some(key) = passthrough::written_key(&head.method, &head.path) {
                    GatewayEngine::written_via(&context.gateway, &key, node);
                }
            }
        }
    }
    let bodiless = passthrough::bodiless(&head.method, status);
    let framing = match (bodiless, length) {
        (true, length) => Framing::Length(length.unwrap_or(0)),
        (false, Some(length)) => Framing::Length(length),
        (false, None) => Framing::Chunked,
    };
    let keep_alive = head.keep_alive && body_read;
    connection
        .write_response_head(status, &headers, framing, keep_alive)
        .await?;
    let complete = match (bodiless, framing) {
        (true, _) => true,
        (false, Framing::Length(length)) => {
            let kernel_tls = to_node.kernel_tls() || connection.kernel_tls();
            let (copied, relayed) =
                zero_copy::relay(to_node.stream(), connection.stream(), length, kernel_tls).await;
            if let Err(Short::Destination(failure)) = relayed {
                return Err(failure);
            }
            copied == length
        }
        (false, Framing::Chunked) => relay_chunks(&mut to_node, connection).await?,
    };
    if complete && body_read && !node_closes {
        peers.keep(node, to_node);
    }
    Ok(complete && body_read)
}

/// Relays a chunked body from a node to the client, and returns whether it
/// arrived in full.
async fn relay_chunks(from: &mut Connection, to: &mut Connection) -> io::Result<bool> {
    let mut left = 0;
    loop {
        match from.read_chunked(&mut left).await {
            Ok(Some(piece)) => to.write_chunk(&piece).await?,
            Ok(None) => break,
            // The body ends short, and the connection with it.
            Err(_) => return Ok(false),
        }
    }
    to.finish_chunks().await?;
    Ok(true)
}

fn hash_mismatch() -> Response {
    error(
        400,
        "XAmzContentSHA256Mismatch",
        "the body does not match its hash",
    )
}

/// The `<Key>` elements of a `DeleteObjects` request body.
///
/// Every element named `Key`, in any namespace and however its tag is
/// written, counts, with CDATA and character references in its text: the
/// keys S3 reads. `None` for a body that isn't XML, or that holds a DTD,
/// whose entities could name keys the gateway never sees.
fn listed_keys(xml: &str) -> Option<Vec<String>> {
    use xmlparser::{ElementEnd, Token, Tokenizer};
    let mut keys = Vec::new();
    // Whether each open element is a `Key`, and the text of the one read.
    let mut open: Vec<bool> = Vec::new();
    let mut key: Option<String> = None;
    for token in Tokenizer::from(xml) {
        match token.ok()? {
            Token::ElementStart { local, .. } => {
                // A key holds only text.
                if key.is_some() {
                    return None;
                }
                open.push(local.as_str() == "Key");
            }
            Token::ElementEnd { end, .. } => match end {
                ElementEnd::Open => {
                    if open.last() == Some(&true) {
                        key = Some(String::new());
                    }
                }
                ElementEnd::Empty => {
                    if open.pop()? {
                        keys.push(String::new());
                    }
                }
                ElementEnd::Close(_, local) => {
                    if open.pop()? != (local.as_str() == "Key") {
                        return None;
                    }
                    if let Some(read) = key.take() {
                        keys.push(read);
                    }
                }
            },
            Token::Text { text } => {
                if let Some(key) = key.as_mut() {
                    key.push_str(&unescape(text.as_str())?);
                }
            }
            Token::Cdata { text, .. } => {
                if let Some(key) = key.as_mut() {
                    key.push_str(text.as_str());
                }
            }
            Token::DtdStart { .. }
            | Token::EmptyDtd { .. }
            | Token::EntityDeclaration { .. }
            | Token::DtdEnd { .. } => return None,
            _ => {}
        }
    }
    open.is_empty().then_some(keys)
}

/// `text` with XML's predefined entities and character references
/// replaced; `None` if it names any other entity.
fn unescape(text: &str) -> Option<String> {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let end = rest[at..].find(';')? + at;
        let name = &rest[at + 1..end];
        let decoded = match name {
            "lt" => '<',
            "gt" => '>',
            "amp" => '&',
            "quot" => '"',
            "apos" => '\'',
            _ => {
                let code = match name.strip_prefix("#x").or_else(|| name.strip_prefix("#X")) {
                    Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                    None => name.strip_prefix('#')?.parse().ok()?,
                };
                char::from_u32(code)?
            }
        };
        out.push(decoded);
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    Some(out)
}

/// The bucket and key an `x-amz-copy-source` names.
fn copy_source(value: &str) -> (String, String) {
    let value = value.split_once('?').map_or(value, |(source, _)| source);
    split_path(&format!("/{}", value.trim_start_matches('/')))
}

fn auth_error(failure: AuthError) -> Response {
    let code = match failure {
        AuthError::Missing => "AccessDenied",
        AuthError::Malformed(_) => "AuthorizationHeaderMalformed",
        AuthError::UnknownAccessKey => "InvalidAccessKeyId",
        AuthError::Expired => "RequestTimeTooSkewed",
        AuthError::SignatureMismatch => "SignatureDoesNotMatch",
        AuthError::UnsignedHeader(_) => "AccessDenied",
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
        assert_eq!(listed_keys(xml), Some(vec!["a&b".into(), "c/d".into()]));
    }

    /// However a key is written, the gateway reads the key S3 would.
    #[test]
    fn lists_keys_however_they_are_written() {
        let keys = |xml: &str| listed_keys(xml).map(|keys| keys.join(","));
        let written = [
            "<Delete><Object><Key >s/1</Key ></Object></Delete>",
            r#"<Delete><Object><Key xmlns="">s/1</Key></Object></Delete>"#,
            r#"<d:Delete xmlns:d="x"><d:Object><d:Key>s/1</d:Key></d:Object></d:Delete>"#,
            "<Delete><Object><Key><![CDATA[s/1]]></Key></Object></Delete>",
            "<Delete><Object><Key>s<!-- -->/&#x31;</Key></Object></Delete>",
        ];
        for xml in written {
            assert_eq!(keys(xml), Some("s/1".into()), "{xml}");
        }
        let refused = [
            r#"<!DOCTYPE d [<!ENTITY e "s/1">]><Delete><Object><Key>&e;</Key></Object></Delete>"#,
            "<Delete><Object><Key>s/1</Object></Delete>",
            "<Delete><Object><Key><Key>s/1</Key></Key></Object></Delete>",
        ];
        for xml in refused {
            assert_eq!(keys(xml), None, "{xml}");
        }
    }
}
