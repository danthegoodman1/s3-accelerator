//! Runs the core's gateway and storage node on this thread and carries out
//! their actions: fetches from S3, block writes, and answers to clients.
//! Blocks live in memory; the on-disk store replaces this in phase S2.

use crate::origin::Origin;
use bytes::Bytes;
use s3_accelerator_core::Time;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::node::{self, GatewayRequestId, Node, OriginRequestId, Segment};
use s3_accelerator_core::placement::{Member, NodeId, Ring};
use s3_accelerator_core::s3::{ObjectKey, Request, ResponseHead};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::time::Instant;
use tokio::sync::oneshot;

pub struct Answer {
    pub head: ResponseHead,
    pub body: Bytes,
}

pub type Shared = Rc<RefCell<Engine>>;

pub struct Engine {
    started: Instant,
    origin: Rc<Origin>,
    gateway: Gateway,
    node: Node,
    extents: Vec<Vec<u8>>,
    /// S3 response bodies the node still reads.
    bodies: BTreeMap<OriginRequestId, Bytes>,
    next_id: u64,
    clients: BTreeMap<ClientRequestId, oneshot::Sender<Answer>>,
    node_requests: BTreeMap<GatewayRequestId, NodeRequestId>,
    relayed: BTreeMap<NodeRequestId, Bytes>,
    /// Client responses the gateway started, and their bodies so far.
    responses: BTreeMap<ClientRequestId, (ResponseHead, Vec<u8>)>,
    fetches: Vec<(OriginRequestId, Request)>,
    /// S3 requests in flight, which a cancellation aborts.
    tasks: BTreeMap<OriginRequestId, tokio::task::AbortHandle>,
}

impl Engine {
    pub fn new(config: node::Config, gateway: gateway::Config, origin: Rc<Origin>) -> Shared {
        let member = Member {
            id: NodeId(0),
            weight: NonZeroU32::MIN,
        };
        let ring = Ring::new(1, vec![member]);
        let extents = (0..config.store.extents)
            .map(|_| vec![0; config.store.extent_size as usize])
            .collect();
        Rc::new(RefCell::new(Engine {
            started: Instant::now(),
            origin,
            gateway: Gateway::new(ring.clone(), gateway),
            node: Node::new(NodeId(0), ring, config),
            extents,
            bodies: BTreeMap::new(),
            next_id: 0,
            clients: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            relayed: BTreeMap::new(),
            responses: BTreeMap::new(),
            fetches: Vec::new(),
            tasks: BTreeMap::new(),
        }))
    }

    /// Serves a `GetObject` or `HeadObject`; the answer arrives on the
    /// receiver.
    pub fn read(engine: &Shared, request: Request) -> oneshot::Receiver<Answer> {
        let (sender, receiver) = oneshot::channel();
        let fetches = {
            let mut this = engine.borrow_mut();
            let id = ClientRequestId(this.next_id());
            this.clients.insert(id, sender);
            let now = this.now();
            this.gateway.on_request(now, id, request);
            this.pump()
        };
        start_fetches(engine, fetches);
        receiver
    }

    /// Lets the core's timeouts run: nodes give up on S3 requests, and
    /// gateways fail over from nodes.
    pub fn tick(engine: &Shared) {
        let fetches = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway.on_tick(now);
            this.node.on_tick(now);
            this.pump()
        };
        start_fetches(engine, fetches);
    }

    /// A write to `key` passed through to S3 and succeeded.
    pub fn write_succeeded(engine: &Shared, key: &ObjectKey) {
        let fetches = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_write(now, key);
            this.gateway.on_write(now, key);
            this.pump()
        };
        start_fetches(engine, fetches);
    }

    /// Carries out every action until the core has none left, and returns
    /// the S3 requests to start.
    fn pump(&mut self) -> Vec<(OriginRequestId, Request)> {
        loop {
            let gateway_actions = self.gateway.drain();
            let node_actions = self.node.drain();
            if gateway_actions.is_empty() && node_actions.is_empty() {
                return std::mem::take(&mut self.fetches);
            }
            for action in gateway_actions {
                self.gateway_action(action);
            }
            for action in node_actions {
                self.node_action(action);
            }
        }
    }

    fn gateway_action(&mut self, action: gateway::Action) {
        match action {
            gateway::Action::Send { id, read, .. } => {
                let local = GatewayRequestId(self.next_id());
                self.node_requests.insert(local, id);
                let now = self.now();
                self.node.on_request(now, local, read);
            }
            gateway::Action::Start { request, head } => {
                self.responses.insert(request, (head, Vec::new()));
            }
            gateway::Action::Forward { request, from, len } => {
                let bytes = self.relayed.remove(&from).unwrap_or_default();
                let copied = bytes.len().min(len as usize);
                let (head, body) = self
                    .responses
                    .get_mut(&request)
                    .expect("a forward follows its start");
                body.extend_from_slice(&bytes[..copied]);
                if body.len() as u64 == head.content_length {
                    let (head, body) = self.responses.remove(&request).expect("started");
                    self.answer(request, head, Bytes::from(body));
                }
                let now = self.now();
                self.gateway.on_forwarded(now, from, copied as u64);
            }
            // The client gets the body so far, and the connection closes.
            gateway::Action::Abort { request } => {
                let (head, body) = self.responses.remove(&request).expect("an aborted start");
                self.answer(request, head, Bytes::from(body));
            }
            gateway::Action::Respond { request, head } => self.answer(request, head, Bytes::new()),
            gateway::Action::Discard { id } => {
                self.relayed.remove(&id);
            }
        }
    }

    fn node_action(&mut self, action: node::Action) {
        let now = self.now();
        match action {
            node::Action::Fetch { origin, request } => self.fetches.push((origin, request)),
            node::Action::Respond {
                request,
                head,
                body,
                meta,
            } => {
                let bytes = self.assemble(&body);
                let id = self
                    .node_requests
                    .remove(&request)
                    .expect("a gateway asked");
                self.relayed.insert(id, bytes);
                self.gateway.on_node_response(now, id, head, meta);
                self.node.on_sent(request);
            }
            node::Action::Metadata { request, meta } => {
                let id = self
                    .node_requests
                    .remove(&request)
                    .expect("a gateway asked");
                self.gateway.on_node_metadata(now, id, meta);
            }
            node::Action::Stale { request } => {
                let id = self
                    .node_requests
                    .remove(&request)
                    .expect("a gateway asked");
                self.gateway.on_node_stale(now, id);
            }
            node::Action::Write {
                location,
                origin,
                offset,
                len,
            } => {
                let source = &self.bodies[&origin][offset as usize..(offset + len) as usize];
                let start = location.offset as usize;
                self.extents[location.extent as usize][start..start + len as usize]
                    .copy_from_slice(source);
                self.node.on_written(location);
            }
            // Blocks live in memory and leave with the process, so there is
            // no slot table to keep and nothing recovered to verify.
            node::Action::Record { .. } | node::Action::Clear { .. } => {}
            node::Action::Verify { .. } => unreachable!("a node started empty verifies nothing"),
            node::Action::Release { origin } => {
                self.bodies.remove(&origin);
            }
            node::Action::Cancel { origin } => {
                if let Some(task) = self.tasks.remove(&origin) {
                    task.abort();
                }
            }
        }
    }

    fn assemble(&self, body: &[Segment]) -> Bytes {
        if let [
            Segment::Origin {
                origin,
                offset,
                len,
            },
        ] = body
        {
            return self.bodies[origin].slice(*offset as usize..(offset + len) as usize);
        }
        let mut bytes = Vec::new();
        for segment in body {
            match *segment {
                Segment::Slot {
                    location,
                    offset,
                    len,
                } => {
                    let start = (location.offset + offset) as usize;
                    let extent = &self.extents[location.extent as usize];
                    bytes.extend_from_slice(&extent[start..start + len as usize]);
                }
                Segment::Origin {
                    origin,
                    offset,
                    len,
                } => {
                    let start = offset as usize;
                    bytes.extend_from_slice(&self.bodies[&origin][start..start + len as usize]);
                }
            }
        }
        Bytes::from(bytes)
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

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

fn start_fetches(engine: &Shared, fetches: Vec<(OriginRequestId, Request)>) {
    for (origin, request) in fetches {
        let handle = engine.clone();
        let task = tokio::task::spawn_local(async move {
            let engine = handle;
            let client = engine.borrow().origin.clone();
            let (head, body) = client.read(&request).await;
            let fetches = {
                let mut this = engine.borrow_mut();
                this.tasks.remove(&origin);
                this.bodies.insert(origin, body);
                let now = this.now();
                this.node.on_origin_response(now, origin, head);
                this.pump()
            };
            start_fetches(&engine, fetches);
        });
        engine
            .borrow_mut()
            .tasks
            .insert(origin, task.abort_handle());
    }
}
