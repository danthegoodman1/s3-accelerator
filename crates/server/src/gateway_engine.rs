//! A gateway: runs the core's gateway on this thread, sends its reads to the
//! storage nodes over the cluster protocol, and assembles the answers for
//! S3 clients.

use crate::http::Connection;
use crate::protocol::{self, NodeAnswer, NodeRequest};
use bytes::Bytes;
use s3_accelerator_core::Time;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::node::Read;
use s3_accelerator_core::placement::{NodeId, Placement, Ring};
use s3_accelerator_core::s3::{ObjectKey, Request, ResponseHead};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

pub struct Answer {
    pub head: ResponseHead,
    pub body: Bytes,
}

pub type SharedGateway = Rc<RefCell<GatewayEngine>>;

pub struct GatewayEngine {
    started: Instant,
    gateway: Gateway,
    ring: Ring,
    /// Where each node listens, and the secret the cluster shares.
    addresses: BTreeMap<NodeId, String>,
    secret: Rc<str>,
    next_id: u64,
    clients: BTreeMap<ClientRequestId, oneshot::Sender<Answer>>,
    /// Bodies of nodes' answers, until forwarded or discarded.
    relayed: BTreeMap<NodeRequestId, Bytes>,
    /// Client responses the gateway started, and their bodies so far.
    responses: BTreeMap<ClientRequestId, (ResponseHead, Vec<Bytes>)>,
    /// Reads to send to nodes.
    sends: Vec<(NodeId, NodeRequestId, Read)>,
    /// Idle connections to each node.
    idle: BTreeMap<NodeId, Vec<Connection>>,
}

impl GatewayEngine {
    pub fn new(
        ring: Ring,
        config: gateway::Config,
        addresses: BTreeMap<NodeId, String>,
        secret: Rc<str>,
    ) -> SharedGateway {
        Rc::new(RefCell::new(GatewayEngine {
            started: Instant::now(),
            gateway: Gateway::new(ring.clone(), config),
            ring,
            addresses,
            secret,
            next_id: 0,
            clients: BTreeMap::new(),
            relayed: BTreeMap::new(),
            responses: BTreeMap::new(),
            sends: Vec::new(),
            idle: BTreeMap::new(),
        }))
    }

    /// Serves a `GetObject` or `HeadObject`; the answer arrives on the
    /// receiver.
    pub fn read(engine: &SharedGateway, request: Request) -> oneshot::Receiver<Answer> {
        let (sender, receiver) = oneshot::channel();
        let sends = {
            let mut this = engine.borrow_mut();
            this.next_id += 1;
            let id = ClientRequestId(this.next_id);
            this.clients.insert(id, sender);
            let now = this.now();
            this.gateway.on_request(now, id, request);
            this.pump()
        };
        send(engine, sends);
        receiver
    }

    /// A write to `key` through this gateway succeeded: the gateway forgets
    /// the key's metadata, and tells the key's home.
    pub fn written(engine: &SharedGateway, key: &ObjectKey) {
        let home = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway.on_write(now, key);
            this.ring.owner(Placement::Home(key).hash())
        };
        if let Some(home) = home {
            let engine = engine.clone();
            let request = NodeRequest::Written(key.clone());
            tokio::task::spawn_local(async move {
                if let Err(error) = exchange(&engine, home, &request).await {
                    eprintln!("telling node {} of a write: {error}", home.0);
                }
            });
        }
    }

    /// Lets the gateway fail over from nodes that time out.
    pub fn tick(engine: &SharedGateway) {
        let sends = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway.on_tick(now);
            this.pump()
        };
        send(engine, sends);
    }

    /// Carries out every action until the gateway has none left, and
    /// returns the reads to send.
    fn pump(&mut self) -> Vec<(NodeId, NodeRequestId, Read)> {
        loop {
            let actions = self.gateway.drain();
            if actions.is_empty() {
                return std::mem::take(&mut self.sends);
            }
            for action in actions {
                self.act(action);
            }
        }
    }

    fn act(&mut self, action: gateway::Action) {
        match action {
            gateway::Action::Send { node, id, read } => self.sends.push((node, id, read)),
            gateway::Action::Start { request, head } => {
                self.responses.insert(request, (head, Vec::new()));
            }
            gateway::Action::Forward { request, from, len } => {
                let now = self.now();
                // Every node's body is here before the gateway forwards it;
                // a missing one is a bug, and the client gets an error, not
                // a body from elsewhere.
                let Some(bytes) = self.relayed.remove(&from) else {
                    self.responses.remove(&request);
                    self.answer(request, ResponseHead::status(500), Bytes::new());
                    return self.gateway.on_forwarded(now, from, len);
                };
                let bytes = bytes.slice(..bytes.len().min(len as usize));
                let copied = bytes.len() as u64;
                if let Some((head, parts)) = self.responses.get_mut(&request) {
                    parts.push(bytes);
                    let sent: usize = parts.iter().map(Bytes::len).sum();
                    if sent as u64 == head.content_length {
                        let (head, parts) = self.responses.remove(&request).expect("started");
                        self.answer(request, head, concat(parts));
                    }
                }
                self.gateway.on_forwarded(now, from, copied);
            }
            // The client gets the body so far, and the connection closes.
            gateway::Action::Abort { request } => {
                if let Some((head, parts)) = self.responses.remove(&request) {
                    self.answer(request, head, concat(parts));
                }
            }
            gateway::Action::Respond { request, head } => self.answer(request, head, Bytes::new()),
            gateway::Action::Discard { id } => {
                self.relayed.remove(&id);
            }
        }
    }

    fn answer(&mut self, request: ClientRequestId, head: ResponseHead, body: Bytes) {
        if let Some(client) = self.clients.remove(&request) {
            // The client may have disconnected.
            let _ = client.send(Answer { head, body });
        }
    }

    fn now(&self) -> Time {
        Time(self.started.elapsed().as_millis() as u64)
    }
}

/// Sends each read to its node, and feeds the node's answer back to the
/// gateway. A node that cannot be reached, or answers out of protocol, fails
/// the read over at once, as a 5xx does.
fn send(engine: &SharedGateway, sends: Vec<(NodeId, NodeRequestId, Read)>) {
    for (node, id, read) in sends {
        let engine = engine.clone();
        tokio::task::spawn_local(async move {
            let request = NodeRequest::Read(read);
            let exchanged = exchange(&engine, node, &request).await;
            let sends = {
                let mut this = engine.borrow_mut();
                let now = this.now();
                match exchanged {
                    Ok((NodeAnswer::Respond { head, meta }, body)) => {
                        this.relayed.insert(id, body);
                        this.gateway.on_node_response(now, id, head, meta);
                    }
                    Ok((NodeAnswer::Metadata(meta), _)) => {
                        this.gateway.on_node_metadata(now, id, meta)
                    }
                    Ok((NodeAnswer::Stale, _)) => this.gateway.on_node_stale(now, id),
                    Ok((NodeAnswer::Written, _)) | Err(_) => {
                        if let Err(error) = &exchanged {
                            eprintln!("reading from node {}: {error}", node.0);
                        }
                        this.relayed.insert(id, Bytes::new());
                        this.gateway
                            .on_node_response(now, id, ResponseHead::status(503), None);
                    }
                }
                this.pump()
            };
            send(&engine, sends);
        });
    }
}

/// Sends one request to `node` and reads the answer and its body. An idle
/// connection the node has since closed gets one retry on a new one.
async fn exchange(
    engine: &SharedGateway,
    node: NodeId,
    request: &NodeRequest,
) -> io::Result<(NodeAnswer, Bytes)> {
    let (idle, address, secret) = {
        let mut this = engine.borrow_mut();
        let idle = this.idle.get_mut(&node).and_then(Vec::pop);
        let address = this.addresses.get(&node).cloned();
        (idle, address, this.secret.clone())
    };
    let address = address.ok_or_else(|| io::Error::other("no address"))?;
    let (answer, body, connection) = match idle {
        Some(connection) => match exchange_on(connection, request, &secret).await {
            Ok(exchanged) => exchanged,
            Err(_) => exchange_on(connect(&address).await?, request, &secret).await?,
        },
        None => exchange_on(connect(&address).await?, request, &secret).await?,
    };
    engine
        .borrow_mut()
        .idle
        .entry(node)
        .or_default()
        .push(connection);
    Ok((answer, body))
}

async fn connect(address: &str) -> io::Result<Connection> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    stream.set_nodelay(true)?;
    Ok(Connection::new(stream))
}

async fn exchange_on(
    mut connection: Connection,
    request: &NodeRequest,
    secret: &str,
) -> io::Result<(NodeAnswer, Bytes, Connection)> {
    let (method, target, headers) = protocol::encode_request(request, secret);
    connection
        .write_request(method, &target, &headers, &[])
        .await?;
    let (status, headers) = connection.read_response_head().await?;
    let answer = protocol::decode_answer(status, &headers).map_err(io::Error::other)?;
    let len = crate::http::header(&headers, "content-length")
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    let body = connection.read_body(len).await?;
    Ok((answer, Bytes::from(body), connection))
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// One body from its pieces, copying only when there are several.
fn concat(mut parts: Vec<Bytes>) -> Bytes {
    match parts.len() {
        1 => parts.pop().expect("one part"),
        _ => Bytes::from(parts.concat()),
    }
}
