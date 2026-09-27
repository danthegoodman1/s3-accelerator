//! A storage node: runs the core's node on this thread, serves gateways'
//! reads over the cluster protocol, and carries out the node's actions:
//! fetches from S3, and block reads and writes on its disk.
//!
//! Stored blocks leave with `sendfile` on worker threads, which also write
//! and verify blocks. A fill's body is held until the node releases it;
//! every other S3 body passes through as it arrives, to the replies and
//! slots that read it, and is never held whole.

use crate::disk::Disk;
use crate::http::{Connection, Framing, Response, header};
use crate::origin::{self, Origin, OriginBody};
use crate::protocol::{self, NodeAnswer, NodeRequest};
use bytes::Bytes;
use hyper::body::Incoming;
use s3_accelerator_core::Time;
use s3_accelerator_core::node::{self, GatewayRequestId, Node, OriginRequestId, Read, Segment};
use s3_accelerator_core::s3::{ByteRange, Method, ObjectKey, Request};
use s3_accelerator_core::store::Location;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::os::fd::AsFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

pub type SharedNode = Rc<RefCell<NodeEngine>>;

/// A node's answer to a gateway, with its body.
pub struct Reply {
    pub answer: NodeAnswer,
    pub body: Vec<Part>,
    /// The body's length.
    pub len: u64,
    /// The response the node holds blocks and bodies for until it is sent.
    sending: Option<GatewayRequestId>,
}

/// A piece of a reply's body.
pub enum Part {
    /// `len` bytes of the slab file from `offset`, sent with `sendfile`.
    File { offset: u64, len: u64 },
    /// `len` bytes of a body the node holds. Fewer bytes end the reply
    /// there.
    Held { bytes: Bytes, len: u64 },
    /// `len` bytes of an S3 body, as they arrive.
    Arriving {
        chunks: mpsc::Receiver<Bytes>,
        len: u64,
    },
}

/// An S3 response body the node reads.
enum Body {
    /// A fill, held whole until the node releases it.
    Held(Bytes),
    /// A body whose head just arrived, gathering its readers.
    Arriving(Vec<Reader>),
    /// A body passing through to the readers it had as its head arrived.
    Passing,
}

/// Where bytes of an arriving body go.
enum Reader {
    Reply {
        offset: u64,
        len: u64,
        chunks: mpsc::Sender<Bytes>,
    },
    Write {
        location: Location,
        offset: u64,
        len: u64,
    },
}

/// Chunks of an arriving body queued for each reply. A reply that falls
/// this far behind holds up the body for every reader.
const QUEUED_CHUNKS: usize = 4;
/// How long a write waits for a slot's old pages to be released, and how
/// long it waits before it first looks again; each wait doubles, up to a
/// second.
const PAGES_WAIT: Duration = Duration::from_secs(30);
const PAGES_RECHECK: Duration = Duration::from_millis(10);

pub struct NodeEngine {
    started: Instant,
    origin: Rc<Origin>,
    disk: Arc<Disk>,
    node: Node,
    /// S3 response bodies the node still reads.
    bodies: BTreeMap<OriginRequestId, Body>,
    next_id: u64,
    /// Gateways' reads waiting for the node's answer.
    replies: BTreeMap<GatewayRequestId, oneshot::Sender<Reply>>,
    work: Work,
    /// S3 requests awaiting their heads, which a cancellation aborts.
    tasks: BTreeMap<OriginRequestId, tokio::task::AbortHandle>,
    /// Entries appended to the metadata file since it was last synced.
    unsynced_metadata: bool,
}

/// What the node's actions left to start off this thread.
#[derive(Default)]
struct Work {
    /// S3 requests, and whether each body streams.
    fetches: Vec<(OriginRequestId, Request, bool)>,
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

    /// A reply's body is sent, or will never be: its blocks and bodies are
    /// free.
    fn sent(engine: &SharedNode, request: GatewayRequestId) {
        let work = {
            let mut this = engine.borrow_mut();
            this.node.on_sent(request);
            this.pump()
        };
        start(engine, work);
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
                origin,
                request,
                streams,
            } => self.work.fetches.push((origin, request, streams)),
            node::Action::Respond {
                request,
                head,
                body,
                meta,
            } => {
                let len = body.iter().map(segment_len).sum();
                let body = self.parts(&body);
                let answer = NodeAnswer::Respond { head, meta };
                if !self.reply(request, answer, body, len, true) {
                    self.node.on_sent(request);
                }
            }
            node::Action::Metadata { request, meta } => {
                self.reply(request, NodeAnswer::Metadata(meta), Vec::new(), 0, false);
            }
            node::Action::Stale { request } => {
                self.reply(request, NodeAnswer::Stale, Vec::new(), 0, false);
            }
            node::Action::Write {
                location,
                origin,
                offset,
                len,
            } => match self.bodies.get_mut(&origin) {
                Some(Body::Held(bytes)) => match slice(bytes, offset, len) {
                    Some(bytes) => self.work.writes.push((location, bytes)),
                    None => self.node.on_write_failed(location),
                },
                Some(Body::Arriving(readers)) => readers.push(Reader::Write {
                    location,
                    offset,
                    len,
                }),
                // Only a body's first readers read it as it passes.
                Some(Body::Passing) | None => self.node.on_write_failed(location),
            },
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

    /// Sends the gateway its answer, and whether it was still waiting. A
    /// `tracked` answer's blocks and bodies stay held until it is sent.
    fn reply(
        &mut self,
        request: GatewayRequestId,
        answer: NodeAnswer,
        body: Vec<Part>,
        len: u64,
        tracked: bool,
    ) -> bool {
        let Some(sender) = self.replies.remove(&request) else {
            return false;
        };
        let reply = Reply {
            answer,
            body,
            len,
            sending: tracked.then_some(request),
        };
        sender.send(reply).is_ok()
    }

    /// The pieces of a response body. An arriving body gains a reader for
    /// each piece of it.
    fn parts(&mut self, segments: &[Segment]) -> Vec<Part> {
        let mut parts = Vec::new();
        for segment in segments {
            let part = match *segment {
                Segment::Slot {
                    location,
                    offset,
                    len,
                } => Part::File {
                    offset: self.disk.offset(location) + offset,
                    len,
                },
                Segment::Origin {
                    origin,
                    offset,
                    len,
                } => match self.bodies.get_mut(&origin) {
                    Some(Body::Held(bytes)) => Part::Held {
                        bytes: slice(bytes, offset, len).unwrap_or_default(),
                        len,
                    },
                    Some(Body::Arriving(readers)) => {
                        let (sender, receiver) = mpsc::channel(QUEUED_CHUNKS);
                        readers.push(Reader::Reply {
                            offset,
                            len,
                            chunks: sender,
                        });
                        Part::Arriving {
                            chunks: receiver,
                            len,
                        }
                    }
                    // The reply ends short, and the gateway reads the rest
                    // from elsewhere.
                    Some(Body::Passing) | None => Part::Held {
                        bytes: Bytes::new(),
                        len,
                    },
                },
            };
            parts.push(part);
        }
        parts
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

fn segment_len(segment: &Segment) -> u64 {
    match *segment {
        Segment::Slot { len, .. } | Segment::Origin { len, .. } => len,
    }
}

fn slice(bytes: &Bytes, offset: u64, len: u64) -> Option<Bytes> {
    let range = usize::try_from(offset).ok()?..usize::try_from(offset + len).ok()?;
    bytes.get(range.clone())?;
    Some(bytes.slice(range))
}

/// Starts S3 requests on this thread, and block writes and verifications
/// on worker threads, each feeding its result back to the node.
fn start(engine: &SharedNode, work: Work) {
    for (origin, request, streams) in work.fetches {
        let task = tokio::task::spawn_local(fetch(engine.clone(), origin, request, streams));
        engine
            .borrow_mut()
            .tasks
            .insert(origin, task.abort_handle());
    }
    for (location, bytes) in work.writes {
        write(engine, location, bytes);
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

/// Sends a request to S3 and gives the node its answer. A body that
/// streams is then passed to the readers the node gave it.
async fn fetch(engine: SharedNode, origin: OriginRequestId, request: Request, streams: bool) {
    let client = engine.borrow().origin.clone();
    let hold = (!streams).then(|| fill_limit(&request));
    let (head, body) = client.read(&request, hold).await;
    let (work, passing) = {
        let mut this = engine.borrow_mut();
        this.tasks.remove(&origin);
        let arriving = match body {
            OriginBody::Held(bytes) => {
                this.bodies.insert(origin, Body::Held(bytes));
                None
            }
            OriginBody::Arriving(body) => {
                this.bodies.insert(origin, Body::Arriving(Vec::new()));
                Some(body)
            }
        };
        let now = this.now();
        this.node.on_origin_response(now, origin, head);
        let work = this.pump();
        let readers = match this.bodies.get_mut(&origin) {
            Some(body) => match std::mem::replace(body, Body::Passing) {
                Body::Arriving(readers) => readers,
                held => {
                    *body = held;
                    Vec::new()
                }
            },
            None => Vec::new(),
        };
        (work, arriving.map(|body| (body, readers)))
    };
    start(&engine, work);
    if let Some((body, readers)) = passing {
        pass_through(&engine, body, readers).await;
    }
}

/// The most a fill's held body may be: the bytes it asked for, or an
/// error body.
fn fill_limit(request: &Request) -> u64 {
    let asked = match request.range {
        Some(ByteRange::Inclusive { first, last }) => last.saturating_sub(first) + 1,
        _ => 0,
    };
    asked.max(1 << 20)
}

/// A slot's bytes, gathered as an arriving body passes.
struct Gathering {
    location: Location,
    start: u64,
    end: u64,
    bytes: Vec<u8>,
}

/// Passes an arriving S3 body to its readers in lockstep: each chunk goes
/// to every reply that reads it before the next chunk is read, so the
/// slowest gateway paces S3. Each slot is written once its bytes are in.
/// Readers of bytes that never arrive get a short reply, or a failed write.
async fn pass_through(engine: &SharedNode, mut body: Incoming, readers: Vec<Reader>) {
    let mut replies = Vec::new();
    let mut gathering = Vec::new();
    for reader in readers {
        match reader {
            Reader::Reply {
                offset,
                len,
                chunks,
            } => replies.push((offset, offset + len, Some(chunks))),
            Reader::Write {
                location,
                offset,
                len,
            } => gathering.push(Gathering {
                location,
                start: offset,
                end: offset + len,
                bytes: Vec::new(),
            }),
        }
    }
    let end = replies
        .iter()
        .map(|(_, end, _)| *end)
        .chain(gathering.iter().map(|slot| slot.end))
        .max()
        .unwrap_or(0);
    let mut position = 0;
    while position < end {
        let chunk = match origin::next_frame(&mut body).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) | Err(_) => break,
        };
        let chunk_end = position + chunk.len() as u64;
        for (start, end, chunks) in &mut replies {
            let (from, to) = (position.max(*start), chunk_end.min(*end));
            if from < to
                && let Some(sender) = chunks
                && sender
                    .send(chunk.slice((from - position) as usize..(to - position) as usize))
                    .await
                    .is_err()
            {
                // The gateway hung up.
                *chunks = None;
            }
        }
        let mut complete = Vec::new();
        gathering.retain_mut(|slot| {
            let (from, to) = (position.max(slot.start), chunk_end.min(slot.end));
            if from < to {
                if slot.bytes.is_empty() {
                    slot.bytes.reserve_exact((slot.end - slot.start) as usize);
                }
                slot.bytes.extend_from_slice(
                    &chunk[(from - position) as usize..(to - position) as usize],
                );
            }
            let done = slot.bytes.len() as u64 == slot.end - slot.start;
            if done {
                complete.push((slot.location, std::mem::take(&mut slot.bytes)));
            }
            !done
        });
        for (location, bytes) in complete {
            write(engine, location, bytes.into());
        }
        position = chunk_end;
    }
    if !gathering.is_empty() {
        let work = {
            let mut this = engine.borrow_mut();
            for slot in gathering {
                this.node.on_write_failed(slot.location);
            }
            this.pump()
        };
        start(engine, work);
    }
}

/// Writes a block into its slot on a worker thread, once no socket or pipe
/// still holds the slot's old pages, and tells the node how it went.
fn write(engine: &SharedNode, location: Location, bytes: Bytes) {
    let engine = engine.clone();
    tokio::task::spawn_local(async move {
        let disk = engine.borrow().disk.clone();
        let deadline = tokio::time::Instant::now() + PAGES_WAIT;
        let mut recheck = PAGES_RECHECK;
        let written = loop {
            let (disk, bytes) = (disk.clone(), bytes.clone());
            let written = tokio::task::spawn_blocking(move || disk.write(location, &bytes))
                .await
                .map_err(io::Error::other)
                .and_then(|written| written);
            match written {
                Ok(false) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(recheck).await;
                    recheck = (recheck * 2).min(Duration::from_secs(1));
                }
                Ok(false) => break Err(io::Error::other("the slot's old pages stayed in use")),
                Ok(true) => break Ok(()),
                Err(error) => break Err(error),
            }
        };
        let work = {
            let mut this = engine.borrow_mut();
            match written {
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
            return refuse(&mut connection, 403).await;
        }
        let reply = match protocol::decode_request(&head.path, &head.headers) {
            Err(error) => {
                eprintln!("a gateway's request: {error}");
                return refuse(&mut connection, 400).await;
            }
            Ok(NodeRequest::Written(key)) => {
                NodeEngine::written(engine, &key);
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            Ok(NodeRequest::Read(read)) => {
                let head_only =
                    matches!(&read, Read::Object { request, .. } if request.method == Method::Head);
                let Ok(mut reply) = NodeEngine::read(engine, read).await else {
                    return Err(io::Error::other("the node dropped a read"));
                };
                if head_only {
                    (reply.body, reply.len) = (Vec::new(), 0);
                }
                reply
            }
        };
        let (status, headers) = protocol::encode_answer(&reply.answer);
        let framing = Framing::Length(reply.len);
        let sent = async {
            connection
                .write_response_head(status, &headers, framing, head.keep_alive)
                .await?;
            send_body(&mut connection, engine, reply.body).await
        }
        .await;
        if let Some(request) = reply.sending {
            NodeEngine::sent(engine, request);
        }
        // A body that ended short ends the connection, which tells the
        // gateway.
        if sent? < reply.len || !head.keep_alive {
            return Ok(());
        }
    }
    Ok(())
}

async fn refuse(connection: &mut Connection, status: u16) -> io::Result<()> {
    let response = Response {
        status,
        headers: Vec::new(),
        content_length: 0,
        body: Bytes::new(),
    };
    connection.write_response(&response, false).await
}

/// Sends a reply's body and returns how many bytes went. Runs of stored
/// blocks go with `sendfile` on a worker thread; held and arriving bytes
/// are written from memory.
async fn send_body(
    connection: &mut Connection,
    engine: &SharedNode,
    parts: Vec<Part>,
) -> io::Result<u64> {
    let mut sent = 0;
    let mut parts = parts.into_iter().peekable();
    while let Some(part) = parts.next() {
        match part {
            Part::File { offset, len } => {
                let mut run = vec![(offset, len)];
                while let Some(&Part::File { offset, len }) = parts.peek() {
                    run.push((offset, len));
                    parts.next();
                }
                let socket = connection.stream().as_fd().try_clone_to_owned()?;
                let disk = engine.borrow().disk.clone();
                let total: u64 = run.iter().map(|(_, len)| len).sum();
                tokio::task::spawn_blocking(move || {
                    run.into_iter()
                        .try_for_each(|(offset, len)| disk.send(&socket, offset, len))
                })
                .await
                .map_err(io::Error::other)??;
                sent += total;
            }
            Part::Held { bytes, len } => {
                connection.write_all(&bytes).await?;
                sent += bytes.len() as u64;
                if (bytes.len() as u64) < len {
                    return Ok(sent);
                }
            }
            Part::Arriving { mut chunks, len } => {
                let mut arrived = 0;
                // The body goes on for other readers after this one's bytes.
                while arrived < len
                    && let Some(chunk) = chunks.recv().await
                {
                    connection.write_all(&chunk).await?;
                    arrived += chunk.len() as u64;
                }
                sent += arrived;
                if arrived < len {
                    return Ok(sent);
                }
            }
        }
    }
    Ok(sent)
}
