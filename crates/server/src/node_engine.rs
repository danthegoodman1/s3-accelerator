//! A storage node: runs the core's node on this thread, serves gateways'
//! reads over the cluster protocol, and carries out the node's actions:
//! fetches from S3, and block reads and writes on its disk.
//!
//! Stored blocks leave with `sendfile` on worker threads, which also write
//! and verify blocks. A fill's body is held until the node releases it;
//! every other S3 body passes through as it arrives, to the replies and
//! slots that read it, and is never held whole. After a ring change, the
//! node reads blocks and metadata from their previous owners over the
//! cluster protocol, and holds those bodies like fills.

use crate::disk::Disk;
use crate::http::{Connection, Framing, Response, header};
use crate::origin::{self, Origin, OriginBody};
use crate::passthrough;
use crate::peers::{Exchanged, Peers};
use crate::protocol::{self, Forward, Hint, NodeAnswer, NodeRequest, Versions};
use crate::sigv4::constant_time_eq;
use crate::sqs::{self, Queue};
use crate::tls::{self, Tls};
use crate::zero_copy;
use bytes::Bytes;
use hyper::body::Incoming;
use s3_accelerator_core::Time;
use s3_accelerator_core::node::{
    self, EventId, GatewayRequestId, HotHint, Node, OriginRequestId, Read, Segment,
};
use s3_accelerator_core::placement::{NodeId, PlacementHash, Ring, down_version};
use s3_accelerator_core::s3::{ByteRange, ETag, Method, ObjectKey, Request, ResponseHead};
use s3_accelerator_core::store::{Location, VersionId};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::os::fd::AsFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
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
    /// The home's region of an upload, by each range's start, which holds
    /// `reserved` bytes of the warming budget.
    Kept {
        ranges: Vec<(u64, Bytes)>,
        reserved: u64,
    },
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
/// The most bytes of uploads a node keeps at once for warming.
const WARM_BUDGET: u64 = 256 << 20;
/// How long a write waits for a slot's old pages to be released, and how
/// long it waits before it first looks again; each wait doubles, up to a
/// second.
const PAGES_WAIT: Duration = Duration::from_secs(30);
const PAGES_RECHECK: Duration = Duration::from_millis(10);

pub struct NodeEngine {
    started: Instant,
    origin: Arc<Origin>,
    peers: Rc<Peers>,
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
    /// A rewrite of the metadata file runs on a blocking thread. Entries
    /// saved meanwhile wait here, and a rewrite asked for meanwhile waits
    /// in `next_rewrite`, in place of those before it.
    rewriting_metadata: bool,
    saved_meanwhile: Vec<(ObjectKey, Option<node::Meta>)>,
    next_rewrite: Option<Vec<(ObjectKey, node::Meta)>>,
    /// Where each node in the ring is reached, for answering ring requests.
    addresses: BTreeMap<NodeId, String>,
    /// The nodes membership holds down, which answers tell gateways of.
    down: Vec<NodeId>,
    events: Events,
    /// Bytes of uploads the node may still keep for warming.
    warm_budget: u64,
    /// An append to the purge log or an erasure failed during the purge
    /// under way, which then fails rather than confirm.
    purge_failed: bool,
    /// A purge's actions are being carried out.
    purging: bool,
}

/// Messages from S3's event queue while the core works through their
/// events.
#[derive(Default)]
struct Events {
    queue: Option<Rc<Queue>>,
    /// How long a message stays hidden once taken; the queue offers it
    /// again after that, so the node forgets it.
    visibility: Duration,
    next_event: u64,
    next_message: u64,
    /// Each event's message.
    events: BTreeMap<EventId, u64>,
    /// Each message's receipt, its events left, and when the node took it.
    messages: BTreeMap<u64, (String, usize, Instant)>,
}

/// Slot bytes to read, each a location, an offset into it and a length.
type SpotParts = Vec<(Location, u64, u64)>;

/// What the node's actions left to start off this thread.
#[derive(Default)]
struct Work {
    /// S3 requests, and whether each body streams.
    fetches: Vec<(OriginRequestId, Request, bool)>,
    /// Reads of previous owners, and writes to pass on.
    peer_fetches: Vec<(OriginRequestId, NodeId, Read)>,
    passed_writes: Vec<(NodeId, ObjectKey)>,
    /// Events to pass on, and receipts of messages to delete.
    passed_events: Vec<(EventId, NodeId, ObjectKey, Option<ETag>)>,
    /// Leases and lease reports to send.
    notices: Vec<(NodeId, NodeRequest)>,
    /// Purges to pass on.
    purges: Vec<(NodeId, ObjectKey)>,
    deletes: Vec<String>,
    writes: Vec<(Location, Bytes)>,
    /// Slots to verify: location, length and checksum.
    verifies: Vec<(Location, u64, u64)>,
    /// Spots to read: a version and the slot bytes that hold its spot.
    spots: Vec<(VersionId, SpotParts)>,
    /// Slots were erased outside a purge, so the slab file needs a sync.
    erased: bool,
    /// A rewrite of the metadata file to start.
    rewrite: Option<Vec<(ObjectKey, node::Meta)>>,
}

impl NodeEngine {
    /// A node started over `disk`. `addresses` say where the nodes of its
    /// first ring are reached.
    pub fn new(
        node: Node,
        origin: Arc<Origin>,
        peers: Rc<Peers>,
        disk: Arc<Disk>,
        addresses: BTreeMap<NodeId, String>,
    ) -> SharedNode {
        let engine = Rc::new(RefCell::new(NodeEngine {
            addresses,
            down: Vec::new(),
            started: Instant::now(),
            origin,
            peers,
            disk,
            node,
            bodies: BTreeMap::new(),
            next_id: 0,
            replies: BTreeMap::new(),
            work: Work::default(),
            tasks: BTreeMap::new(),
            unsynced_metadata: false,
            rewriting_metadata: false,
            saved_meanwhile: Vec::new(),
            next_rewrite: None,
            events: Events::default(),
            warm_budget: WARM_BUDGET,
            purge_failed: false,
            purging: false,
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

    /// The node's ring.
    pub fn ring(engine: &SharedNode) -> Ring {
        engine.borrow().node.ring().clone()
    }

    /// Membership changed the ring, whose nodes are reached at `addresses`.
    pub fn on_ring(engine: &SharedNode, ring: Ring, addresses: BTreeMap<NodeId, String>) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.addresses = addresses;
            this.node.on_ring(now, ring);
            this.pump()
        };
        start(engine, work);
    }

    /// Membership now holds `down` down.
    pub fn on_down(engine: &SharedNode, down: Vec<NodeId>) {
        engine.borrow_mut().down = down;
    }

    /// What the node stamps its answers with: its ring's version, and the
    /// version of the nodes membership holds down.
    pub fn versions(engine: &SharedNode) -> Versions {
        let this = engine.borrow();
        Versions {
            ring: this.node.ring().version(),
            down: down_version(&this.down),
        }
    }

    /// The node is joining a cluster whose ring was `before`.
    pub fn on_joined(engine: &SharedNode, before: Ring) {
        let mut this = engine.borrow_mut();
        let now = this.now();
        this.node.on_joined(now, before);
    }

    /// A write to `key` passed through a gateway and succeeded; another
    /// node passed it on if `passed_on`.
    pub fn written(engine: &SharedNode, key: &ObjectKey, passed_on: bool) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_write_from(now, key, passed_on);
            this.pump()
        };
        start(engine, work);
    }

    /// The node takes S3's events from `queue`, whose messages stay hidden
    /// for `visibility` once taken.
    pub fn take_events_from(engine: &SharedNode, queue: Rc<Queue>, visibility: Duration) {
        let mut this = engine.borrow_mut();
        this.events.queue = Some(queue);
        this.events.visibility = visibility;
    }

    /// The node took a message from the queue, whose receipt deletes it,
    /// naming `changes`: each object and its new ETag, or `None` once gone.
    pub fn take_message(
        engine: &SharedNode,
        receipt: String,
        changes: Vec<(ObjectKey, Option<ETag>)>,
    ) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            let events = &mut this.events;
            let message = events.next_message;
            events.next_message += 1;
            let left = changes.len();
            events
                .messages
                .insert(message, (receipt, left, Instant::now()));
            let mut taken = Vec::new();
            for (key, etag) in changes {
                let event = EventId(events.next_event);
                events.next_event += 1;
                events.events.insert(event, message);
                taken.push((event, key, etag));
            }
            for (event, key, etag) in taken {
                this.node.on_event(now, event, key, etag);
            }
            this.pump()
        };
        start(engine, work);
    }

    /// Purges `key`, coordinating the purge unless another node
    /// `passed_on` it, and returns once the purge is durable.
    pub async fn purge(engine: &SharedNode, key: &ObjectKey, passed_on: bool) -> io::Result<()> {
        let (work, logged) = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.purging = true;
            this.node.on_purge(now, key, passed_on);
            let work = this.pump();
            this.purging = false;
            (work, !std::mem::take(&mut this.purge_failed))
        };
        start(engine, work);
        if !logged {
            return Err(io::Error::other("the purge could not be logged or erased"));
        }
        let disk = engine.borrow().disk.clone();
        tokio::task::spawn_blocking(move || {
            // The erased slots' bytes, then the records that no longer
            // name them.
            disk.sync_slabs()?;
            disk.sync_table()?;
            disk.sync_metadata()?;
            disk.sync_purges()
        })
        .await
        .map_err(io::Error::other)?
    }

    /// `node` confirmed it purged `key`.
    fn purge_confirmed(engine: &SharedNode, key: &ObjectKey, node: NodeId) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_purge_confirmed(now, key, node);
            this.pump()
        };
        start(engine, work);
    }

    /// `owner` leased `placement` to this node for `left` milliseconds.
    pub fn lease(engine: &SharedNode, placement: PlacementHash, owner: NodeId, left: u64) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node
                .on_lease(now, placement, owner, Time(now.0 + left));
            this.pump()
        };
        start(engine, work);
    }

    /// A replica served `reads` reads of `placement` under its lease.
    pub fn lease_report(engine: &SharedNode, placement: PlacementHash, reads: u64) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_lease_report(now, placement, reads);
            this.pump()
        };
        start(engine, work);
    }

    /// The byte ranges of an upload of `key`, `len` bytes long, to keep as
    /// it passes to S3, with warming budget set aside for them; none if the
    /// node does not warm the upload or the budget is spent.
    fn keep_upload(engine: &SharedNode, key: &ObjectKey, len: u64) -> Vec<Range<u64>> {
        let mut this = engine.borrow_mut();
        let region = this.node.warm_region(key, len);
        let bytes: u64 = region.iter().map(|range| range.end - range.start).sum();
        if bytes > this.warm_budget {
            return Vec::new();
        }
        this.warm_budget -= bytes;
        region
    }

    /// S3 took an upload of `key` as version `etag` of `len` bytes, and the
    /// node kept the bytes `keep_upload` named in `kept`, holding
    /// `reserved` of the budget: the core checks the version and stores
    /// them. Kept bytes that fall short, or that the core does not want,
    /// free their budget at once.
    fn uploaded(
        engine: &SharedNode,
        key: ObjectKey,
        etag: Option<ETag>,
        len: u64,
        kept: Vec<(u64, Vec<u8>)>,
        reserved: u64,
    ) {
        let work = {
            let mut this = engine.borrow_mut();
            let whole: u64 = kept.iter().map(|(_, bytes)| bytes.len() as u64).sum();
            let now = this.now();
            let taken = match etag {
                Some(etag) if whole == reserved => this.node.on_uploaded(now, key, etag, len),
                _ => None,
            };
            match taken {
                Some(origin) => {
                    let ranges = kept
                        .into_iter()
                        .map(|(start, bytes)| (start, Bytes::from(bytes)))
                        .collect();
                    this.bodies.insert(origin, Body::Kept { ranges, reserved });
                }
                None => this.warm_budget += reserved,
            }
            this.pump()
        };
        start(engine, work);
    }

    /// Another node passed on S3's event that `key` changed to `etag`.
    pub fn event_notice(engine: &SharedNode, key: &ObjectKey, etag: Option<&ETag>) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_event_notice(now, key, etag);
            this.pump()
        };
        start(engine, work);
    }

    fn event_passed(engine: &SharedNode, event: EventId, node: NodeId, heard: bool) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.node.on_event_passed(now, event, node, heard);
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
            let visibility = this.events.visibility;
            let events = &mut this.events;
            events
                .messages
                .retain(|_, (_, _, taken)| taken.elapsed() < visibility);
            let messages = &events.messages;
            events
                .events
                .retain(|_, message| messages.contains_key(message));
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
        let this = engine.borrow();
        this.node.is_idle() && !this.rewriting_metadata
    }

    /// Rewrites the metadata file least recently used first, syncs the
    /// disk and marks its slot table clean. Call once idle.
    pub fn shut_down(engine: &SharedNode) -> io::Result<()> {
        let this = engine.borrow();
        this.disk.rewrite_metadata(&this.node.saved_metadata())?;
        this.disk.shut_down()
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
                hot,
            } => {
                let len = body.iter().map(segment_len).sum();
                let body = self.parts(&body);
                let hot = self.hints(hot);
                let answer = NodeAnswer::Respond { head, meta, hot };
                if !self.reply(request, answer, body, len, true) {
                    self.node.on_sent(request);
                }
            }
            node::Action::Metadata { request, meta, hot } => {
                let answer = NodeAnswer::Metadata(meta, self.hints(hot));
                self.reply(request, answer, Vec::new(), 0, false);
            }
            node::Action::GrantLease {
                node,
                placement,
                until,
            } => {
                let request = NodeRequest::Lease {
                    placement,
                    owner: self.node.id(),
                    left: until.0.saturating_sub(self.now().0),
                };
                self.work.notices.push((node, request));
            }
            node::Action::ReportLease {
                node,
                placement,
                reads,
            } => {
                let request = NodeRequest::LeaseReport { placement, reads };
                self.work.notices.push((node, request));
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
                Some(Body::Kept { ranges, .. }) => match kept_slice(ranges, offset, len) {
                    Some(bytes) => self.work.writes.push((location, bytes)),
                    None => self.node.on_write_failed(location),
                },
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
            // Rare: once the file holds twice the metadata capacity.
            node::Action::RewriteMetadata { entries } => {
                if self.rewriting_metadata {
                    self.saved_meanwhile.clear();
                    self.next_rewrite = Some(entries);
                } else {
                    self.rewriting_metadata = true;
                    self.work.rewrite = Some(entries);
                }
            }
            node::Action::Verify {
                location,
                len,
                checksum,
            } => self.work.verifies.push((location, len, checksum)),
            node::Action::ReadSpot { version, parts } => self.work.spots.push((version, parts)),
            // Erasing in order with the node's actions finishes before any
            // later write can reuse the slot.
            // A purge syncs its own erasures before it confirms; a slot a
            // purged block held until now is synced on its own.
            node::Action::Erase { location, len } => match self.disk.erase(location, len) {
                Ok(()) => self.work.erased |= !self.purging,
                Err(error) => {
                    eprintln!("erasing {location:?}: {error}");
                    self.purge_failed |= self.purging;
                }
            },
            node::Action::SavePurge { key, nodes } => {
                if let Err(error) = self.disk.save_purge(&key, &nodes) {
                    eprintln!("saving the purge of {key:?}: {error}");
                    self.purge_failed = true;
                }
            }
            node::Action::PassPurge { node, key } => self.work.purges.push((node, key)),
            node::Action::Release { origin } => {
                if let Some(Body::Kept { reserved, .. }) = self.bodies.remove(&origin) {
                    self.warm_budget += reserved;
                }
            }
            node::Action::PeerFetch { origin, peer, read } => {
                self.work.peer_fetches.push((origin, peer, read));
            }
            node::Action::PassWrite { node, key } => self.work.passed_writes.push((node, key)),
            node::Action::PassEvent {
                event,
                node,
                key,
                etag,
            } => self.work.passed_events.push((event, node, key, etag)),
            node::Action::EventDone { event } => {
                let events = &mut self.events;
                let Some(message) = events.events.remove(&event) else {
                    return;
                };
                if let Some((_, left, _)) = events.messages.get_mut(&message) {
                    *left -= 1;
                    if *left == 0 {
                        let (receipt, _, _) = events.messages.remove(&message).expect("present");
                        self.work.deletes.push(receipt);
                    }
                }
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
                    Some(Body::Kept { ranges, .. }) => Part::Held {
                        bytes: kept_slice(ranges, offset, len).unwrap_or_default(),
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

    /// Hints as the protocol carries them, in milliseconds left rather
    /// than this node's clock.
    fn hints(&self, hot: Vec<HotHint>) -> Vec<Hint> {
        let now = self.now();
        hot.into_iter()
            .map(|hint| Hint {
                placement: hint.placement,
                nodes: hint.nodes,
                left: hint.until.0.saturating_sub(now.0),
            })
            .collect()
    }

    fn save(&mut self, key: &ObjectKey, meta: Option<&node::Meta>) {
        if self.rewriting_metadata {
            self.saved_meanwhile.push((key.clone(), meta.cloned()));
            return;
        }
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

/// Bytes `offset..offset + len` of an object, from the kept range that
/// holds them.
fn kept_slice(ranges: &[(u64, Bytes)], offset: u64, len: u64) -> Option<Bytes> {
    let (start, bytes) = ranges
        .iter()
        .find(|(start, bytes)| *start <= offset && offset + len <= start + bytes.len() as u64)?;
    slice(bytes, offset - start, len)
}

fn slice(bytes: &Bytes, offset: u64, len: u64) -> Option<Bytes> {
    let range = usize::try_from(offset).ok()?..usize::try_from(offset + len).ok()?;
    bytes.get(range.clone())?;
    Some(bytes.slice(range))
}

/// Starts S3 requests on this thread, and block writes and verifications
/// on worker threads, each feeding its result back to the node.
/// Rewrites the metadata file on a blocking thread, then any rewrite asked
/// for meanwhile, then appends the entries saved meanwhile.
async fn rewrite_metadata(engine: SharedNode, entries: Vec<(ObjectKey, node::Meta)>) {
    let mut entries = entries;
    loop {
        let disk = engine.borrow().disk.clone();
        let rewritten = tokio::task::spawn_blocking(move || disk.rewrite_metadata(&entries)).await;
        if let Err(error) = rewritten
            .map_err(io::Error::other)
            .and_then(|result| result)
        {
            eprintln!("rewriting the metadata file: {error}");
        }
        let mut this = engine.borrow_mut();
        match this.next_rewrite.take() {
            Some(next) => entries = next,
            None => {
                this.rewriting_metadata = false;
                for (key, meta) in std::mem::take(&mut this.saved_meanwhile) {
                    this.save(&key, meta.as_ref());
                }
                return;
            }
        }
    }
}

fn start(engine: &SharedNode, work: Work) {
    if let Some(entries) = work.rewrite {
        tokio::task::spawn_local(rewrite_metadata(engine.clone(), entries));
    }
    if work.erased {
        let disk = engine.borrow().disk.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) = disk.sync_slabs() {
                eprintln!("syncing erased slots: {error}");
            }
        });
    }
    for (origin, request, streams) in work.fetches {
        let task = tokio::task::spawn_local(fetch(engine.clone(), origin, request, streams));
        engine
            .borrow_mut()
            .tasks
            .insert(origin, task.abort_handle());
    }
    for (origin, peer, read) in work.peer_fetches {
        let task = tokio::task::spawn_local(fetch_from_peer(engine.clone(), origin, peer, read));
        engine
            .borrow_mut()
            .tasks
            .insert(origin, task.abort_handle());
    }
    for (node, key) in work.passed_writes {
        let peers = engine.borrow().peers.clone();
        tokio::task::spawn_local(async move {
            let request = NodeRequest::Written {
                key,
                passed_on: true,
            };
            match peers.exchange(node, &request).await {
                Ok(exchanged) => peers.idle(exchanged.body),
                Err(error) => eprintln!("passing a write to node {}: {error}", node.0),
            }
        });
    }
    for (node, key) in work.purges {
        let engine = engine.clone();
        let peers = engine.borrow().peers.clone();
        tokio::task::spawn_local(async move {
            let request = NodeRequest::Purge {
                key: key.clone(),
                passed_on: true,
            };
            let told = tokio::time::timeout(PASS_WAIT, peers.exchange(node, &request)).await;
            match told {
                Ok(Ok(Exchanged {
                    answer: NodeAnswer::Written,
                    body,
                    ..
                })) => {
                    peers.idle(body);
                    NodeEngine::purge_confirmed(&engine, &key, node);
                }
                Ok(Ok(_)) => eprintln!("node {} answered a purge out of protocol", node.0),
                Ok(Err(error)) => eprintln!("passing a purge to node {}: {error}", node.0),
                Err(_) => eprintln!("passing a purge to node {}: timed out", node.0),
            }
        });
    }
    for (node, request) in work.notices {
        let peers = engine.borrow().peers.clone();
        tokio::task::spawn_local(async move {
            let told = tokio::time::timeout(PASS_WAIT, peers.exchange(node, &request)).await;
            match told {
                Ok(Ok(exchanged)) => peers.idle(exchanged.body),
                Ok(Err(error)) => eprintln!("telling node {} of a lease: {error}", node.0),
                Err(_) => eprintln!("telling node {} of a lease: timed out", node.0),
            }
        });
    }
    for (event, node, key, etag) in work.passed_events {
        let engine = engine.clone();
        let peers = engine.borrow().peers.clone();
        tokio::task::spawn_local(async move {
            let request = NodeRequest::Event { key, etag };
            let told = tokio::time::timeout(PASS_WAIT, peers.exchange(node, &request)).await;
            let heard = match told {
                Ok(Ok(exchanged)) => {
                    peers.idle(exchanged.body);
                    true
                }
                Ok(Err(error)) => {
                    eprintln!("passing an event to node {}: {error}", node.0);
                    false
                }
                Err(_) => false,
            };
            NodeEngine::event_passed(&engine, event, node, heard);
        });
    }
    if !work.deletes.is_empty()
        && let Some(queue) = engine.borrow().events.queue.clone()
    {
        for receipt in work.deletes {
            let queue = queue.clone();
            tokio::task::spawn_local(async move {
                if let Err(error) = queue.delete(&receipt).await {
                    eprintln!("deleting an event message: {error}");
                }
            });
        }
    }
    for (location, bytes) in work.writes {
        write(engine, location, bytes);
    }
    for (version, parts) in work.spots {
        let engine = engine.clone();
        let disk = engine.borrow().disk.clone();
        tokio::task::spawn_local(async move {
            let read = tokio::task::spawn_blocking(move || {
                let mut bytes = Vec::new();
                for (location, offset, len) in parts {
                    bytes.extend(disk.read(location, offset, len)?);
                }
                Ok::<_, io::Error>(bytes)
            })
            .await;
            let work = {
                let mut this = engine.borrow_mut();
                // A spot that cannot be read names no metadata.
                let bytes = match read {
                    Ok(Ok(bytes)) => bytes,
                    _ => Vec::new(),
                };
                let now = this.now();
                this.node.on_spot(now, version, bytes);
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

/// Reads blocks or metadata from a previous owner and gives the node the
/// answer. Blocks are held like a fill's; a peer that fails or answers out
/// of protocol has none.
async fn fetch_from_peer(engine: SharedNode, origin: OriginRequestId, peer: NodeId, read: Read) {
    let peers = engine.borrow().peers.clone();
    let asks_metadata = matches!(read, Read::Known(_));
    let exchanged = peers.exchange(peer, &NodeRequest::Read(read)).await;
    let answer = match exchanged {
        Ok(Exchanged {
            answer: NodeAnswer::Metadata(meta, _),
            body,
            ..
        }) => {
            peers.idle(body);
            Ok(Some(meta))
        }
        Ok(Exchanged {
            answer: NodeAnswer::Respond { head, .. },
            body,
            ..
        }) if !asks_metadata => match peers.read_body(body).await {
            Ok(bytes) => Err((head, bytes)),
            Err(_) => Err((ResponseHead::status(503), Bytes::new())),
        },
        _ => Ok(None),
    };
    let work = {
        let mut this = engine.borrow_mut();
        this.tasks.remove(&origin);
        let now = this.now();
        match (asks_metadata, answer) {
            (true, Ok(meta)) => this.node.on_peer_metadata(now, origin, meta),
            (false, Err((head, bytes))) => {
                this.bodies.insert(origin, Body::Held(bytes));
                this.node.on_origin_response(now, origin, head);
            }
            (_, _) => {
                this.bodies.insert(origin, Body::Held(Bytes::new()));
                this.node
                    .on_origin_response(now, origin, ResponseHead::status(503));
            }
        }
        this.pump()
    };
    start(&engine, work);
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

/// How long each poll of the event queue waits for messages.
const EVENTS_WAIT: Duration = Duration::from_secs(20);

/// Takes S3's events from `queue` into the node for as long as the node
/// runs. A message the node cannot read leaves the queue, since it would
/// fail again; one whose events the node fails to finish returns to the
/// queue once `visibility` passes.
pub async fn take_events(engine: SharedNode, queue: Rc<Queue>, visibility: Duration) {
    NodeEngine::take_events_from(&engine, queue.clone(), visibility);
    loop {
        let offered = match queue.receive(EVENTS_WAIT, visibility).await {
            Ok(offered) => offered,
            Err(error) => {
                eprintln!("polling the event queue: {error}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        for message in offered {
            match sqs::changes(&message.body) {
                Ok(changes) if !changes.is_empty() => {
                    NodeEngine::take_message(&engine, message.receipt, changes);
                }
                read => {
                    if let Err(error) = read {
                        eprintln!("an event message: {error}");
                    }
                    if let Err(error) = queue.delete(&message.receipt).await {
                        eprintln!("deleting an event message: {error}");
                    }
                }
            }
        }
    }
}

/// Serves gateways on `listener` until `stop` completes, then waits for work
/// in progress and shuts the disk down cleanly.
pub async fn serve(
    listener: TcpListener,
    tls: Option<Rc<Tls>>,
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
        let (tls, engine, secret) = (tls.clone(), engine.clone(), secret.clone());
        tokio::task::spawn_local(async move {
            let Some(connection) = tls::accept(tls.as_deref(), stream).await else {
                return;
            };
            if let Err(error) = serve_connection(connection, &engine, &secret).await {
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
/// How long a node waits for another to take an event it passes on.
const PASS_WAIT: Duration = Duration::from_secs(10);

async fn serve_connection(
    mut connection: Connection,
    engine: &SharedNode,
    secret: &str,
) -> io::Result<()> {
    while let Some(head) = connection.read_head().await? {
        let len = head.content_length().map_err(io::Error::other)?;
        let given = header(&head.headers, protocol::SECRET);
        if !given.is_some_and(|given| constant_time_eq(given.as_bytes(), secret.as_bytes())) {
            return refuse(&mut connection, 403, len).await;
        }
        let request = protocol::decode_request(&head.path, &head.query, &head.headers, len);
        let request = match request {
            Err(error) => {
                eprintln!("a gateway's request: {error}");
                return refuse(&mut connection, 400, len).await;
            }
            Ok(NodeRequest::Forward(forward)) => {
                if forward_to_s3(&mut connection, engine, &forward, head.keep_alive).await? {
                    continue;
                }
                return Ok(());
            }
            Ok(request) => request,
        };
        // Only forwarded requests carry a body.
        connection.read_body(len).await?;
        let reply = match request {
            NodeRequest::Forward(_) => unreachable!("forwarded above"),
            NodeRequest::Purge { key, passed_on } => {
                // A purge that is not durable goes unconfirmed, and its
                // coordinator tells the node again.
                NodeEngine::purge(engine, &key, passed_on).await?;
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            NodeRequest::Lease {
                placement,
                owner,
                left,
            } => {
                NodeEngine::lease(engine, placement, owner, left);
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            NodeRequest::LeaseReport { placement, reads } => {
                NodeEngine::lease_report(engine, placement, reads);
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            NodeRequest::Event { key, etag } => {
                NodeEngine::event_notice(engine, &key, etag.as_ref());
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            NodeRequest::Written { key, passed_on } => {
                NodeEngine::written(engine, &key, passed_on);
                Reply {
                    answer: NodeAnswer::Written,
                    body: Vec::new(),
                    len: 0,
                    sending: None,
                }
            }
            NodeRequest::Ring => Reply {
                answer: NodeAnswer::Ring {
                    ring: NodeEngine::ring(engine),
                    addresses: engine.borrow().addresses.clone(),
                    down: engine.borrow().down.clone(),
                },
                body: Vec::new(),
                len: 0,
                sending: None,
            },
            NodeRequest::Read(read) => {
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
        let versions = NodeEngine::versions(engine);
        let (status, headers) = protocol::encode_answer(&reply.answer, versions);
        let framing = Framing::Length(reply.len);
        let sent = async {
            // A small body the node holds goes out with the head.
            if let [Part::Held { bytes, len }] = reply.body.as_slice()
                && bytes.len() as u64 == *len
                && bytes.len() < zero_copy::INLINE_WRITE
            {
                let keep_alive = head.keep_alive;
                connection
                    .write_head_and_body(status, &headers, framing, keep_alive, bytes)
                    .await?;
                return Ok(*len);
            }
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

/// Answers `status` without reading the request's `len`-byte body, and
/// closes the connection.
async fn refuse(connection: &mut Connection, status: u16, len: u64) -> io::Result<()> {
    let response = Response {
        status,
        headers: Vec::new(),
        content_length: 0,
        body: Bytes::new(),
    };
    connection.write_response(&response, false).await?;
    if len > 0 {
        connection.linger().await;
    }
    Ok(())
}

/// Passes a gateway's forwarded request to S3 and S3's answer back, and
/// returns whether the connection can take another request. A write S3
/// accepted changes the node's view of its object before the gateway
/// hears, so the gateway's next read sees the write.
async fn forward_to_s3(
    connection: &mut Connection,
    engine: &SharedNode,
    forward: &Forward,
    keep_alive: bool,
) -> io::Result<bool> {
    let origin = engine.borrow().origin.clone();
    let key = passthrough::written_key(&forward.method, &forward.path);
    // The home keeps the region it holds of an upload, to store once S3
    // takes it.
    let keep = match &key {
        Some(key) if passthrough::uploads_object(forward) => {
            NodeEngine::keep_upload(engine, key, forward.len)
        }
        _ => Vec::new(),
    };
    let reserved: u64 = keep.iter().map(|range| range.end - range.start).sum();
    let sent = passthrough::to_s3(&origin, forward, connection, &keep).await;
    let response = match sent.response {
        Ok(response) => response,
        Err(error) => {
            engine.borrow_mut().warm_budget += reserved;
            eprintln!("forwarding to S3: {error}");
            let unread = if sent.body_read { 0 } else { forward.len };
            refuse(connection, 502, unread).await?;
            return Ok(false);
        }
    };
    let status = response.status().as_u16();
    match key {
        Some(key) if status < 300 => {
            NodeEngine::written(engine, &key, false);
            if !keep.is_empty() {
                let headers = origin::header_pairs(response.headers());
                let etag = header(&headers, "etag").map(|etag| ETag(etag.to_string()));
                let kept = if sent.body_read {
                    sent.kept
                } else {
                    Vec::new()
                };
                NodeEngine::uploaded(engine, key, etag, forward.len, kept, reserved);
            }
        }
        _ => engine.borrow_mut().warm_budget += reserved,
    }
    let answer = passthrough::forwarded(&forward.method, &response);
    let length = match &answer {
        NodeAnswer::Forwarded { length, .. } => *length,
        _ => None,
    };
    let bodiless = passthrough::bodiless(&forward.method, status);
    let framing = match (bodiless, length) {
        (true, _) => Framing::Length(0),
        (false, Some(length)) => Framing::Length(length),
        (false, None) => Framing::Chunked,
    };
    let versions = NodeEngine::versions(engine);
    let (status, headers) = protocol::encode_answer(&answer, versions);
    let keep_alive = keep_alive && sent.body_read;
    connection
        .write_response_head(status, &headers, framing, keep_alive)
        .await?;
    let complete = bodiless || passthrough::stream_body(connection, response, framing).await?;
    if !sent.body_read {
        // S3 answered before the body ended.
        connection.linger().await;
    }
    Ok(complete && keep_alive)
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
                let disk = engine.borrow().disk.clone();
                let total: u64 = run.iter().map(|(_, len)| len).sum();
                // Bytes the page cache holds go out from the event loop.
                // Kernel TLS encrypts as it sends, so its sends stay on
                // workers.
                let inline = !connection.kernel_tls()
                    && total <= zero_copy::INLINE_SEND
                    && run.iter().all(|&(offset, len)| disk.cached(offset, len));
                if inline {
                    for (offset, len) in run {
                        disk.send_cached(connection.stream(), offset, len).await?;
                    }
                    sent += total;
                    continue;
                }
                let socket = connection.stream().as_fd().try_clone_to_owned()?;
                tokio::task::spawn_blocking(move || {
                    run.into_iter()
                        .try_for_each(|(offset, len)| disk.send(&socket, offset, len))
                })
                .await
                .map_err(io::Error::other)??;
                sent += total;
            }
            Part::Held { bytes, len } => {
                let held = bytes.len() as u64;
                match bytes.len() < zero_copy::INLINE_WRITE {
                    true => connection.write_all(&bytes).await?,
                    false => zero_copy::write_on_workers(connection.stream(), bytes).await?,
                }
                sent += held;
                if held < len {
                    return Ok(sent);
                }
            }
            Part::Arriving { chunks, len } => {
                let arrived = zero_copy::write_arriving(connection.stream(), chunks, len).await?;
                sent += arrived;
                if arrived < len {
                    return Ok(sent);
                }
            }
        }
    }
    Ok(sent)
}
