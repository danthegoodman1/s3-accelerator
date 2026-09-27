//! Runs the core's gateway and storage node on this thread and carries out
//! their actions: fetches from S3, block reads and writes on the node's
//! disk, and answers to clients. Block writes and verifications run on
//! blocking worker threads.

use crate::disk::Disk;
use crate::origin::Origin;
use bytes::Bytes;
use s3_accelerator_core::Time;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::node::{self, GatewayRequestId, Node, OriginRequestId, Segment};
use s3_accelerator_core::placement::Ring;
use s3_accelerator_core::s3::{ObjectKey, Request, ResponseHead};
use s3_accelerator_core::store::Location;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
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
    disk: Arc<Disk>,
    gateway: Gateway,
    node: Node,
    /// S3 response bodies the node still reads.
    bodies: BTreeMap<OriginRequestId, Bytes>,
    next_id: u64,
    clients: BTreeMap<ClientRequestId, oneshot::Sender<Answer>>,
    node_requests: BTreeMap<GatewayRequestId, NodeRequestId>,
    relayed: BTreeMap<NodeRequestId, Bytes>,
    /// Client responses the gateway started, and their bodies so far.
    responses: BTreeMap<ClientRequestId, (ResponseHead, Vec<Bytes>)>,
    work: Work,
    /// S3 requests in flight, which a cancellation aborts.
    tasks: BTreeMap<OriginRequestId, tokio::task::AbortHandle>,
    /// Entries appended to the metadata file since it was last synced.
    unsynced_metadata: bool,
}

/// What the core's actions left to start off this thread.
#[derive(Default)]
struct Work {
    fetches: Vec<(OriginRequestId, Request)>,
    writes: Vec<(Location, Bytes)>,
    /// Slots to verify: location, length and checksum.
    verifies: Vec<(Location, u64, u64)>,
}

impl Engine {
    pub fn new(
        node: Node,
        ring: Ring,
        gateway: gateway::Config,
        origin: Rc<Origin>,
        disk: Arc<Disk>,
    ) -> Shared {
        let engine = Rc::new(RefCell::new(Engine {
            started: Instant::now(),
            origin,
            disk,
            gateway: Gateway::new(ring, gateway),
            node,
            bodies: BTreeMap::new(),
            next_id: 0,
            clients: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            relayed: BTreeMap::new(),
            responses: BTreeMap::new(),
            work: Work::default(),
            tasks: BTreeMap::new(),
            unsynced_metadata: false,
        }));
        // A recovering node's first actions clear records it cannot use.
        let work = engine.borrow_mut().pump();
        start(&engine, work);
        engine
    }

    /// Serves a `GetObject` or `HeadObject`; the answer arrives on the
    /// receiver.
    pub fn read(engine: &Shared, request: Request) -> oneshot::Receiver<Answer> {
        let (sender, receiver) = oneshot::channel();
        let work = {
            let mut this = engine.borrow_mut();
            let id = ClientRequestId(this.next_id());
            this.clients.insert(id, sender);
            let now = this.now();
            this.gateway.on_request(now, id, request);
            this.pump()
        };
        start(engine, work);
        receiver
    }

    /// Lets the core's timeouts run: nodes give up on S3 requests, and
    /// gateways fail over from nodes.
    pub fn tick(engine: &Shared) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway.on_tick(now);
            this.node.on_tick(now);
            if std::mem::take(&mut this.unsynced_metadata) {
                let disk = this.disk.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) = disk.sync_metadata() {
                        eprintln!("syncing the metadata file: {error}");
                    }
                });
            }
            this.pump()
        };
        start(engine, work);
    }

    /// A write to `key` passed through to S3 and succeeded.
    pub fn write_succeeded(engine: &Shared, key: &ObjectKey) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_write(now, key);
            this.gateway.on_write(now, key);
            this.pump()
        };
        start(engine, work);
    }

    /// True when neither the gateway nor the node has work in progress.
    pub fn is_idle(engine: &Shared) -> bool {
        let this = engine.borrow();
        this.gateway.is_idle() && this.node.is_idle()
    }

    /// Syncs the disk and marks its slot table clean. Call once idle.
    pub fn shut_down(engine: &Shared) -> io::Result<()> {
        engine.borrow().disk.shut_down()
    }

    /// Carries out every action until the core has none left, and returns
    /// the work to start off this thread.
    fn pump(&mut self) -> Work {
        loop {
            let gateway_actions = self.gateway.drain();
            let node_actions = self.node.drain();
            if gateway_actions.is_empty() && node_actions.is_empty() {
                return std::mem::take(&mut self.work);
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

    fn node_action(&mut self, action: node::Action) {
        let now = self.now();
        match action {
            node::Action::Fetch {
                origin, request, ..
            } => self.work.fetches.push((origin, request)),
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
                let body = &self.bodies[&origin];
                let range = offset as usize..(offset + len) as usize;
                match body.get(range.clone()) {
                    Some(_) => self.work.writes.push((location, body.slice(range))),
                    None => self.node.on_write_failed(location),
                }
            }
            node::Action::Record { location, record } => {
                if let Err(error) = self.disk.record(location, record) {
                    eprintln!("recording {location:?}: {error}");
                }
            }
            node::Action::Clear { location } => {
                if let Err(error) = self.disk.clear(location) {
                    eprintln!("clearing {location:?}: {error}");
                }
            }
            node::Action::Remember { key, meta } => self.save(&key, Some(&meta)),
            node::Action::Forget { key } => self.save(&key, None),
            node::Action::Verify {
                location,
                len,
                checksum,
            } => self.work.verifies.push((location, len, checksum)),
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
                } => match self.disk.read(location, offset, len) {
                    Ok(slot) => bytes.extend_from_slice(&slot),
                    // The body ends early, and the gateway reads the rest
                    // from elsewhere.
                    Err(error) => {
                        eprintln!("reading {location:?}: {error}");
                        break;
                    }
                },
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

    fn save(&mut self, key: &ObjectKey, meta: Option<&node::Meta>) {
        match self.disk.append(key, meta) {
            Ok(()) => self.unsynced_metadata = true,
            Err(error) => eprintln!("saving metadata of {key:?}: {error}"),
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

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

/// Starts S3 requests on this thread, and block writes and verifications
/// on worker threads, each feeding its result back to the node.
fn start(engine: &Shared, work: Work) {
    for (origin, request) in work.fetches {
        let handle = engine.clone();
        let task = tokio::task::spawn_local(async move {
            let engine = handle;
            let client = engine.borrow().origin.clone();
            let (head, body) = client.read(&request).await;
            let work = {
                let mut this = engine.borrow_mut();
                this.tasks.remove(&origin);
                this.bodies.insert(origin, body);
                let now = this.now();
                this.node.on_origin_response(now, origin, head);
                this.pump()
            };
            start(&engine, work);
        });
        engine
            .borrow_mut()
            .tasks
            .insert(origin, task.abort_handle());
    }
    for (location, bytes) in work.writes {
        let engine = engine.clone();
        let disk = engine.borrow().disk.clone();
        tokio::task::spawn_local(async move {
            let written = tokio::task::spawn_blocking(move || disk.write(location, &bytes)).await;
            let work = {
                let mut this = engine.borrow_mut();
                match written {
                    Ok(Ok(())) => this.node.on_written(location),
                    Ok(Err(error)) => {
                        eprintln!("writing {location:?}: {error}");
                        this.node.on_write_failed(location);
                    }
                    Err(error) => {
                        eprintln!("writing {location:?}: {error}");
                        this.node.on_write_failed(location);
                    }
                }
                this.pump()
            };
            start(&engine, work);
        });
    }
    for (location, len, checksum) in work.verifies {
        let engine = engine.clone();
        let disk = engine.borrow().disk.clone();
        tokio::task::spawn_local(async move {
            let verified =
                tokio::task::spawn_blocking(move || disk.verify(location, len, checksum)).await;
            let work = {
                let mut this = engine.borrow_mut();
                // A block that cannot be read counts as corrupt.
                let intact = matches!(verified, Ok(Ok(true)));
                let now = this.now();
                this.node.on_verified(now, location, intact);
                this.pump()
            };
            start(&engine, work);
        });
    }
}

/// One body from its pieces, copying only when there are several.
fn concat(mut parts: Vec<Bytes>) -> Bytes {
    match parts.len() {
        1 => parts.pop().expect("one part"),
        _ => Bytes::from(parts.concat()),
    }
}
