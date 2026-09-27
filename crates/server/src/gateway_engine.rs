//! A gateway: runs the core's gateway on this thread, sends its reads to the
//! storage nodes over the cluster protocol, and tells each client's
//! connection how to answer. A node's body stays in its connection until
//! the client's connection relays it with `splice`, or the gateway drops it.

use crate::http::Connection;
use crate::protocol::{self, NodeAnswer, NodeRequest};
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
use tokio::sync::mpsc;

/// What a client's connection does next for its read.
pub enum Event {
    /// Answer with `head` and no body.
    Respond(ResponseHead),
    /// Start the response with `head`; forwards supply its body.
    Start(ResponseHead),
    /// Copy the first `len` bytes of the node's body into the response,
    /// then call `GatewayEngine::forwarded`.
    Forward {
        from: NodeRequestId,
        body: NodeBody,
        len: u64,
    },
    /// End the started response early.
    Abort,
}

/// A node's answer whose body is still in its connection.
pub struct NodeBody {
    node: NodeId,
    connection: Connection,
    /// Body bytes still unread.
    len: u64,
}

impl NodeBody {
    pub fn stream(&self) -> &TcpStream {
        self.connection.stream()
    }

    /// Body bytes still unread.
    pub fn unread(&self) -> u64 {
        self.len
    }
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
    clients: BTreeMap<ClientRequestId, mpsc::UnboundedSender<Event>>,
    /// Nodes' answered bodies, until forwarded or discarded.
    relayed: BTreeMap<NodeRequestId, NodeBody>,
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
            sends: Vec::new(),
            idle: BTreeMap::new(),
        }))
    }

    /// Serves a `GetObject` or `HeadObject`; what to answer arrives on the
    /// receiver.
    pub fn read(engine: &SharedGateway, request: Request) -> mpsc::UnboundedReceiver<Event> {
        let (sender, receiver) = mpsc::unbounded_channel();
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

    /// The client's connection copied `copied` bytes of the node's body.
    /// A body read to its end leaves its connection for the next read.
    pub fn forwarded(
        engine: &SharedGateway,
        from: NodeRequestId,
        copied: u64,
        read_in_full: Option<NodeBody>,
    ) {
        let sends = {
            let mut this = engine.borrow_mut();
            if let Some(mut body) = read_in_full {
                body.len = 0;
                this.idle(body);
            }
            let now = this.now();
            this.gateway.on_forwarded(now, from, copied);
            this.pump()
        };
        send(engine, sends);
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
                match exchange(&engine, home, &request).await {
                    Ok((_, body)) => engine.borrow_mut().idle(body),
                    Err(error) => eprintln!("telling node {} of a write: {error}", home.0),
                }
            });
        }
    }

    /// Lets the gateway fail over from nodes that time out.
    pub fn tick(engine: &SharedGateway) {
        let sends = {
            let mut this = engine.borrow_mut();
            this.clients.retain(|_, client| !client.is_closed());
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
                self.tell(request, Event::Start(head));
            }
            gateway::Action::Forward { request, from, len } => {
                let now = self.now();
                // Every answered body is here until forwarded; a missing
                // one counts as ending at once, so the rest comes from
                // elsewhere.
                let Some(body) = self.relayed.remove(&from) else {
                    return self.gateway.on_forwarded(now, from, 0);
                };
                // A client that hung up needs no more of its body.
                if !self.tell(request, Event::Forward { from, body, len }) {
                    self.gateway.on_forwarded(now, from, len);
                }
            }
            gateway::Action::Abort { request } => {
                self.tell(request, Event::Abort);
                self.clients.remove(&request);
            }
            gateway::Action::Respond { request, head } => {
                self.tell(request, Event::Respond(head));
                self.clients.remove(&request);
            }
            gateway::Action::Discard { id } => {
                if let Some(body) = self.relayed.remove(&id) {
                    self.idle(body);
                }
            }
        }
    }

    /// Passes `event` to the client's connection, and whether it was still
    /// open.
    fn tell(&mut self, request: ClientRequestId, event: Event) -> bool {
        self.clients
            .get(&request)
            .is_some_and(|client| client.send(event).is_ok())
    }

    /// Keeps a connection for the next read if its last answer was read in
    /// full, and otherwise closes it.
    fn idle(&mut self, body: NodeBody) {
        if body.len == 0 {
            self.idle
                .entry(body.node)
                .or_default()
                .push(body.connection);
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
                    Ok((NodeAnswer::Metadata(meta), body)) => {
                        this.idle(body);
                        this.gateway.on_node_metadata(now, id, meta);
                    }
                    Ok((NodeAnswer::Stale, body)) => {
                        this.idle(body);
                        this.gateway.on_node_stale(now, id);
                    }
                    Ok((NodeAnswer::Written, _)) | Err(_) => {
                        if let Err(error) = &exchanged {
                            eprintln!("reading from node {}: {error}", node.0);
                        }
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

/// Sends one request to `node` and reads the answer's head, leaving its
/// body in the connection. An idle connection the node has since closed
/// gets one retry on a new one.
async fn exchange(
    engine: &SharedGateway,
    node: NodeId,
    request: &NodeRequest,
) -> io::Result<(NodeAnswer, NodeBody)> {
    let (idle, address, secret) = {
        let mut this = engine.borrow_mut();
        let idle = this.idle.get_mut(&node).and_then(Vec::pop);
        let address = this.addresses.get(&node).cloned();
        (idle, address, this.secret.clone())
    };
    let address = address.ok_or_else(|| io::Error::other("no address"))?;
    let (answer, len, connection) = match idle {
        Some(connection) => match exchange_on(connection, request, &secret).await {
            Ok(exchanged) => exchanged,
            Err(_) => exchange_on(connect(&address).await?, request, &secret).await?,
        },
        None => exchange_on(connect(&address).await?, request, &secret).await?,
    };
    let body = NodeBody {
        node,
        connection,
        len,
    };
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
) -> io::Result<(NodeAnswer, u64, Connection)> {
    let (method, target, headers) = protocol::encode_request(request, secret);
    connection
        .write_request(method, &target, &headers, &[])
        .await?;
    let (status, headers) = connection.read_response_head().await?;
    let answer = protocol::decode_answer(status, &headers).map_err(io::Error::other)?;
    let len = crate::http::header(&headers, "content-length")
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    Ok((answer, len, connection))
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
