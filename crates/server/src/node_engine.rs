//! A storage node: runs the core's node on this thread, serves gateways'
//! reads over the cluster protocol, and carries out the node's actions:
//! fetches from S3, and block reads and writes on its disk. Block writes and
//! verifications run on blocking worker threads.

use crate::disk::Disk;
use crate::http::{Connection, Response, header};
use crate::origin::Origin;
use crate::protocol::{self, NodeAnswer, NodeRequest};
use bytes::Bytes;
use s3_accelerator_core::Time;
use s3_accelerator_core::node::{self, GatewayRequestId, Node, OriginRequestId, Read, Segment};
use s3_accelerator_core::s3::{Method, ObjectKey, Request};
use s3_accelerator_core::store::Location;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

pub type SharedNode = Rc<RefCell<NodeEngine>>;

/// A node's answer to a gateway, with its body.
pub struct Reply {
    pub answer: NodeAnswer,
    pub body: Bytes,
}

pub struct NodeEngine {
    started: Instant,
    origin: Rc<Origin>,
    disk: Arc<Disk>,
    node: Node,
    /// S3 response bodies the node still reads.
    bodies: BTreeMap<OriginRequestId, Bytes>,
    next_id: u64,
    /// Gateways' reads waiting for the node's answer.
    replies: BTreeMap<GatewayRequestId, oneshot::Sender<Reply>>,
    work: Work,
    /// S3 requests in flight, which a cancellation aborts.
    tasks: BTreeMap<OriginRequestId, tokio::task::AbortHandle>,
    /// Entries appended to the metadata file since it was last synced.
    unsynced_metadata: bool,
}

/// What the node's actions left to start off this thread.
#[derive(Default)]
struct Work {
    fetches: Vec<(OriginRequestId, Request)>,
    writes: Vec<(Location, Bytes)>,
    /// Slots to verify: location, length and checksum.
    verifies: Vec<(Location, u64, u64)>,
}

impl NodeEngine {
    pub fn new(node: Node, origin: Rc<Origin>, disk: Arc<Disk>) -> SharedNode {
        let engine = Rc::new(RefCell::new(NodeEngine {
            started: Instant::now(),
            origin,
            disk,
            node,
            bodies: BTreeMap::new(),
            next_id: 0,
            replies: BTreeMap::new(),
            work: Work::default(),
            tasks: BTreeMap::new(),
            unsynced_metadata: false,
        }));
        // A recovering node's first actions clear records it cannot use.
        let work = engine.borrow_mut().pump();
        start(&engine, work);
        engine
    }

    /// Serves a gateway's read; the answer arrives on the receiver.
    pub fn read(engine: &SharedNode, read: Read) -> oneshot::Receiver<Reply> {
        let (sender, receiver) = oneshot::channel();
        let work = {
            let mut this = engine.borrow_mut();
            this.next_id += 1;
            let id = GatewayRequestId(this.next_id);
            this.replies.insert(id, sender);
            let now = this.now();
            this.node.on_request(now, id, read);
            this.pump()
        };
        start(engine, work);
        receiver
    }

    /// A write to `key` passed through a gateway and succeeded.
    pub fn written(engine: &SharedNode, key: &ObjectKey) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_write(now, key);
            this.pump()
        };
        start(engine, work);
    }

    /// Lets the node's timeouts run, and syncs the metadata file.
    pub fn tick(engine: &SharedNode) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
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

    pub fn is_idle(engine: &SharedNode) -> bool {
        engine.borrow().node.is_idle()
    }

    /// Syncs the disk and marks its slot table clean. Call once idle.
    pub fn shut_down(engine: &SharedNode) -> io::Result<()> {
        engine.borrow().disk.shut_down()
    }

    /// Carries out every action until the node has none left, and returns
    /// the work to start off this thread.
    fn pump(&mut self) -> Work {
        loop {
            let actions = self.node.drain();
            if actions.is_empty() {
                return std::mem::take(&mut self.work);
            }
            for action in actions {
                self.act(action);
            }
        }
    }

    fn act(&mut self, action: node::Action) {
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
                let body = self.assemble(&body);
                self.reply(request, NodeAnswer::Respond { head, meta }, body);
                self.node.on_sent(request);
            }
            node::Action::Metadata { request, meta } => {
                self.reply(request, NodeAnswer::Metadata(meta), Bytes::new());
            }
            node::Action::Stale { request } => {
                self.reply(request, NodeAnswer::Stale, Bytes::new());
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

    fn reply(&mut self, request: GatewayRequestId, answer: NodeAnswer, body: Bytes) {
        if let Some(reply) = self.replies.remove(&request) {
            // The gateway may have hung up.
            let _ = reply.send(Reply { answer, body });
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

    fn now(&self) -> Time {
        Time(self.started.elapsed().as_millis() as u64)
    }
}

/// Starts S3 requests on this thread, and block writes and verifications
/// on worker threads, each feeding its result back to the node.
fn start(engine: &SharedNode, work: Work) {
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
                match written
                    .map_err(io::Error::other)
                    .and_then(|written| written)
                {
                    Ok(()) => this.node.on_written(location),
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

/// Serves gateways on `listener` until `stop` completes, then waits for work
/// in progress and shuts the disk down cleanly.
pub async fn serve(
    listener: TcpListener,
    engine: SharedNode,
    secret: Rc<str>,
    stop: impl Future<Output = ()>,
) -> io::Result<()> {
    let ticking = engine.clone();
    let ticker = tokio::task::spawn_local(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            NodeEngine::tick(&ticking);
        }
    });
    tokio::pin!(stop);
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            () = &mut stop => break,
        };
        stream.set_nodelay(true)?;
        let (engine, secret) = (engine.clone(), secret.clone());
        tokio::task::spawn_local(async move {
            if let Err(error) = connection(stream, &engine, &secret).await {
                eprintln!("gateway connection closed: {error}");
            }
        });
    }
    drop(listener);
    let deadline = tokio::time::Instant::now() + SHUTDOWN_WAIT;
    while !NodeEngine::is_idle(&engine) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ticker.abort();
    NodeEngine::shut_down(&engine)
}

/// How long a shutdown waits for work in progress.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(30);

async fn connection(stream: TcpStream, engine: &SharedNode, secret: &str) -> io::Result<()> {
    let mut connection = Connection::new(stream);
    while let Some(head) = connection.read_head().await? {
        let len = head.content_length().map_err(io::Error::other)?;
        connection.read_body(len).await?;
        if header(&head.headers, protocol::SECRET) != Some(secret) {
            let response = Response {
                status: 403,
                headers: Vec::new(),
                content_length: 0,
                body: Bytes::new(),
            };
            return connection.write_response(&response, false).await;
        }
        let (answer, body) = match protocol::decode_request(&head.path, &head.headers) {
            Err(error) => {
                let response = Response {
                    status: 400,
                    headers: Vec::new(),
                    content_length: 0,
                    body: Bytes::new(),
                };
                eprintln!("a gateway's request: {error}");
                return connection.write_response(&response, false).await;
            }
            Ok(NodeRequest::Written(key)) => {
                NodeEngine::written(engine, &key);
                (NodeAnswer::Written, Bytes::new())
            }
            Ok(NodeRequest::Read(read)) => {
                let head_only =
                    matches!(&read, Read::Object { request, .. } if request.method == Method::Head);
                let Ok(reply) = NodeEngine::read(engine, read).await else {
                    return Err(io::Error::other("the node dropped a read"));
                };
                let body = if head_only { Bytes::new() } else { reply.body };
                (reply.answer, body)
            }
        };
        let (status, headers) = protocol::encode_answer(&answer);
        let response = Response {
            status,
            headers,
            content_length: body.len() as u64,
            body,
        };
        connection
            .write_response(&response, head.keep_alive)
            .await?;
        if !head.keep_alive {
            return Ok(());
        }
    }
    Ok(())
}
