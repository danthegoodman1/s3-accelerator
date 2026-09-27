//! The storage node: serves the blocks it holds and fills misses from S3.
//!
//! As an object's home, a node keeps the object's metadata. Its first fetch
//! of an object is unconditional and sets the metadata; every later fill
//! carries `If-Match` with that ETag, so every stored block belongs to the
//! version it is keyed by. A fill that fails `If-Match` drops the metadata,
//! and the requests waiting on it go back to the gateway to retry.
//!
//! The slot table on disk records each stored block once its bytes are
//! durable, so a restarted node rebuilds its index. After a crash, each
//! recovered block's first read checks its bytes against the recorded
//! checksum; a block that fails is dropped and read again from S3.

use crate::Time;
use crate::doorkeeper::Doorkeeper;
use crate::layout::Layout;
use crate::placement::{NodeId, Placement, PlacementHash, Ring};
use crate::s3::{
    Answer, ByteRange, ContentRange, ETag, Method, ObjectKey, Request, ResponseHead, answer,
    preconditions,
};
use crate::store::{BlockKey, BlockState, Location, Store, StoreConfig, VersionId};
use std::collections::{BTreeMap, BTreeSet};
use xxhash_rust::xxh3::xxh3_64;

/// A request from a gateway, numbered by the node's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GatewayRequestId(pub u64);

/// A request the node sent to S3, numbered by the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginRequestId(pub u64);

/// What a gateway asks a storage node to read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read {
    /// A client's `GetObject` or `HeadObject`, sent to the object's home.
    /// `stale` is an ETag a chunk owner just found out of date: the home
    /// revalidates it instead of trusting it until its age runs out.
    Object {
        request: Request,
        stale: Option<ETag>,
        /// Fetch exactly this request from S3 and relay the answer, leaving
        /// metadata and blocks alone: the gateway's way out of a read that
        /// keeps finding the object changed.
        direct: bool,
    },
    /// Bytes of one version, sent to the node that owns them.
    Range(RangeRead),
    /// Bytes of one version that a node took over from this one: answered
    /// from stored blocks only, and with 404 if any is missing.
    Stored(RangeRead),
    /// The metadata this node knows for an object whose home it was:
    /// answered with `Action::Metadata`, or with 404 if it knows none.
    Known(ObjectKey),
}

/// Bytes `first..=last` of the version `etag` of an object of `size`
/// bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeRead {
    pub key: ObjectKey,
    pub etag: ETag,
    pub size: u64,
    pub first: u64,
    pub last: u64,
}

/// What a home tells gateways about an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub etag: ETag,
    pub size: u64,
    pub headers: Vec<(String, String)>,
    /// Milliseconds since the home last confirmed the metadata with S3.
    pub age: u64,
}

/// How long a bucket's metadata stays fresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    /// Objects never change once written, so metadata never expires.
    Immutable,
    /// Metadata older than this many milliseconds is revalidated with
    /// `If-None-Match` before it is used.
    Ttl(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketPolicy {
    pub freshness: Freshness,
    /// Store blocks on their first read instead of their second.
    pub admit_on_first_read: bool,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub layout: Layout,
    pub store: StoreConfig,
    /// Reads the doorkeeper remembers.
    pub doorkeeper_window: u64,
    /// Bytes of stored blocks that may be filling at once.
    pub fill_budget: u64,
    /// Objects whose metadata the home keeps; the least recently used goes first.
    pub metadata_capacity: usize,
    /// Milliseconds after which an unanswered S3 request is abandoned and
    /// treated as S3 failing with 503.
    pub origin_timeout: u64,
    /// Milliseconds after a ring change during which the node keeps the
    /// previous ring, and asks previous owners for blocks first.
    pub fallback_window: u64,
    /// Milliseconds a previous owner has to answer before the node goes to
    /// S3 instead, and stops asking it until the next ring change.
    pub peer_timeout: u64,
    pub default_policy: BucketPolicy,
    pub buckets: BTreeMap<String, BucketPolicy>,
}

/// What the slot table records about a stored block: enough to put it back
/// in the index after a restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRecord {
    pub version: VersionId,
    pub index: u64,
    pub len: u64,
    pub placement: PlacementHash,
}

/// A slot-table record read back when the node starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovered {
    pub location: Location,
    pub record: SlotRecord,
    pub checksum: u64,
    /// Written or verified in a run that ended with a clean shutdown, so
    /// the bytes need no check.
    pub trusted: bool,
}

/// A piece of a response body. A body is a list of pieces, sent in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Segment {
    /// `len` bytes of a stored block, from `offset` bytes into its slot.
    Slot {
        location: Location,
        offset: u64,
        len: u64,
    },
    /// `len` bytes of S3's response body to `origin`, from `offset`.
    Origin {
        origin: OriginRequestId,
        offset: u64,
        len: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `request` to S3. A streaming body has every reader it will
    /// have by the time its head arrives, so it may pass through without
    /// being held. Otherwise it is a fill of at most one chunk, and the
    /// node may read any of it until it releases it.
    Fetch {
        origin: OriginRequestId,
        request: Request,
        streams: bool,
    },
    /// Send `read`, a `Stored` or `Known` read, to node `peer`. Its answer
    /// to a `Stored` read goes to `on_origin_response`, its body held like
    /// a fill's; its answer to a `Known` read goes to `on_peer_metadata`.
    PeerFetch {
        origin: OriginRequestId,
        peer: NodeId,
        read: Read,
    },
    /// Answer the gateway with `head`, then `body`. Call `on_sent` once the
    /// body is sent. A home that knows the object's metadata includes it.
    Respond {
        request: GatewayRequestId,
        head: ResponseHead,
        body: Vec<Segment>,
        meta: Option<ObjectMeta>,
    },
    /// The request reaches past the blocks the home holds: answer the
    /// gateway with the metadata, and it reads the blocks from their owners.
    Metadata {
        request: GatewayRequestId,
        meta: ObjectMeta,
    },
    /// The object changed while the node served the request; the gateway
    /// retries it.
    Stale { request: GatewayRequestId },
    /// Copy `len` bytes of S3's response body to `origin`, from `offset`,
    /// into the slot at `location`. Call `on_written` once they are
    /// durable, or `on_write_failed` if the body ends before them.
    Write {
        location: Location,
        origin: OriginRequestId,
        offset: u64,
        len: u64,
    },
    /// Record in the slot table that the slot at `location` holds
    /// `record`'s block, with a checksum of its bytes. The node asks once
    /// the bytes are durable, and again once it verifies a recovered block,
    /// so a later clean shutdown vouches for it.
    Record {
        location: Location,
        record: SlotRecord,
    },
    /// Erase the slot table's record for `location`. The erasure must be
    /// durable before a later `Write` over any of the slot's bytes begins.
    Clear { location: Location },
    /// Append `key`'s metadata to the metadata file. A home saves the
    /// metadata of immutable buckets, which stays valid across restarts.
    Remember { key: ObjectKey, meta: Meta },
    /// Append to the metadata file that `key`'s saved metadata no longer
    /// holds.
    Forget { key: ObjectKey },
    /// Check the `len` bytes of the block at `location` against the
    /// checksum its record held, then call `on_verified`.
    Verify {
        location: Location,
        len: u64,
        checksum: u64,
    },
    /// The node needs no more of S3's response body to `origin`.
    Release { origin: OriginRequestId },
    /// The node gave up on S3 request `origin`: drop its response if it
    /// arrives.
    Cancel { origin: OriginRequestId },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Body bytes served from stored blocks.
    pub hit_bytes: u64,
    /// Body bytes served from S3's responses.
    pub miss_bytes: u64,
    pub origin_requests: u64,
    pub written_bytes: u64,
    /// Requests from gateways.
    pub reads: u64,
    pub evicted_blocks: u64,
    /// Recovered blocks checked against their checksums, and those that
    /// failed.
    pub verified_blocks: u64,
    pub corrupt_blocks: u64,
    /// Reads sent to previous owners, objects whose metadata a previous
    /// home supplied, and body bytes served from previous owners' blocks.
    pub peer_requests: u64,
    pub peer_metadata: u64,
    pub peer_bytes: u64,
}

impl std::ops::AddAssign for Stats {
    fn add_assign(&mut self, other: Stats) {
        self.hit_bytes += other.hit_bytes;
        self.miss_bytes += other.miss_bytes;
        self.origin_requests += other.origin_requests;
        self.written_bytes += other.written_bytes;
        self.reads += other.reads;
        self.evicted_blocks += other.evicted_blocks;
        self.verified_blocks += other.verified_blocks;
        self.corrupt_blocks += other.corrupt_blocks;
        self.peer_requests += other.peer_requests;
        self.peer_metadata += other.peer_metadata;
        self.peer_bytes += other.peer_bytes;
    }
}

/// A stored block, as the node's index describes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredBlock {
    pub version: VersionId,
    pub index: u64,
    pub location: Location,
    pub len: u64,
}

pub struct Node {
    id: NodeId,
    /// The latest time an input carried.
    now: Time,
    ring: Ring,
    /// The ring before the last change, until the fallback window ends.
    previous: Option<(Ring, Time)>,
    /// Previous owners that failed to answer since the last ring change.
    unreachable: BTreeSet<NodeId>,
    /// Writes to keys this node knew nothing of, during the fallback
    /// window: a previous home's metadata validated before one is stale.
    written: BTreeMap<ObjectKey, Time>,
    config: Config,
    objects: BTreeMap<ObjectKey, Object>,
    /// Known objects by when they were last used, oldest first.
    recency: BTreeMap<u64, ObjectKey>,
    next_use: u64,
    versions: BTreeMap<VersionId, Version>,
    store: Store,
    doorkeeper: Doorkeeper,
    origins: BTreeMap<OriginRequestId, OriginRequest>,
    next_origin: u64,
    /// Blocks whose bytes are in, or on their way in, an S3 response body.
    in_flight: BTreeMap<BlockKey, OriginRequestId>,
    /// Slots being written, and the response bodies they copy from.
    writes: BTreeMap<Location, OriginRequestId>,
    /// Requests that read slots a first fetch is writing.
    awaiting_writes: BTreeMap<Location, Vec<GatewayRequestId>>,
    /// While the node handles a first fetch's head: its streaming body's
    /// version and span. Requests queued behind the fetch may read the
    /// body only then, since it streams through.
    arriving: Option<(OriginRequestId, VersionId, u64, u64)>,
    /// Hashes of keys whose blocks the node recovered at startup and whose
    /// metadata it has not fetched since.
    recovered: BTreeSet<u64>,
    /// Recovered blocks being verified, and the requests that wait for them.
    verifying: BTreeMap<Location, (BlockKey, Vec<GatewayRequestId>)>,
    /// Bytes of stored blocks that are filling.
    filling_bytes: u64,
    waiting: BTreeMap<GatewayRequestId, Waiting>,
    sending: BTreeMap<GatewayRequestId, Holds>,
    stats: Stats,
    actions: Vec<Action>,
}

/// What a home knows about one of its objects.
enum Object {
    /// The first fetch is in flight; `waiting` requests need its metadata.
    Fetching {
        origin: OriginRequestId,
        waiting: Vec<GatewayRequestId>,
        /// A write succeeded after the fetch was sent, so its answer may
        /// predate the write: it answers its own request and is not kept.
        superseded: bool,
    },
    Known {
        meta: Meta,
        /// When the request that confirmed the metadata was sent.
        validated: Time,
        revalidation: Option<(OriginRequestId, Vec<GatewayRequestId>)>,
        /// Its place in `recency`.
        used: u64,
    },
}

/// An object's metadata, as a home keeps it and saves it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub etag: ETag,
    pub size: u64,
    pub headers: Vec<(String, String)>,
}

struct Version {
    /// Stored and in-flight blocks of this version.
    refs: u64,
    /// Its key and ETag, once a read names it: a version recovered from
    /// the slot table is known only by its hash until then.
    name: Option<(ObjectKey, ETag)>,
}

struct OriginRequest {
    purpose: Purpose,
    method: Method,
    sent: Time,
    /// How long it has to answer.
    timeout: u64,
    /// The node asked, when it is a peer rather than S3.
    peer: Option<NodeId>,
    answered: bool,
    /// Timed out: its response, if it comes, is the owner's to drop.
    cancelled: bool,
    /// The object's offset of the first byte of the response body.
    body_start: u64,
    /// Responses and writes that still read the body.
    readers: u64,
    /// Blocks registered as in flight in this body.
    blocks: Vec<BlockKey>,
    /// Requests that wait for this response before their own starts.
    waiters: Vec<GatewayRequestId>,
}

enum Purpose {
    /// A direct read: S3's answer is the gateway's.
    Direct {
        request: GatewayRequestId,
    },
    First {
        key: ObjectKey,
        request: GatewayRequestId,
        sent: Time,
        /// The fetch asks exactly what the client asked, so its response
        /// is the client's answer.
        relay: bool,
    },
    Revalidate {
        key: ObjectKey,
        sent: Time,
    },
    Fill {
        version: VersionId,
        last_byte: u64,
        stored: Vec<(BlockKey, Location)>,
    },
    /// Metadata asked of the object's previous home, for the request that
    /// found none.
    PeerMeta {
        key: ObjectKey,
        request: GatewayRequestId,
        sent: Time,
    },
}

struct Waiting {
    read: Read,
    arrived: Time,
    /// Set once the metadata is known and the body planned.
    plan: Option<Plan>,
}

struct Plan {
    head: ResponseHead,
    meta: Option<ObjectMeta>,
    body: Vec<Segment>,
    holds: Holds,
    /// Fills and verifications that must finish before the response starts.
    awaiting: BTreeSet<Await>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Await {
    Fill(OriginRequestId),
    Verify(Location),
    /// A first fetch's write into this slot.
    Written(Location),
}

/// What a response keeps in place until its body is sent.
#[derive(Default)]
struct Holds {
    pins: Vec<BlockKey>,
    readers: Vec<OriginRequestId>,
}

impl Node {
    pub fn new(id: NodeId, ring: Ring, config: Config) -> Node {
        assert!(
            config.layout.block_size() <= config.store.max_slot,
            "blocks of {} bytes exceed the largest slot",
            config.layout.block_size()
        );
        assert!(config.metadata_capacity > 0, "no room for metadata");
        Node {
            id,
            now: Time::default(),
            ring,
            previous: None,
            unreachable: BTreeSet::new(),
            written: BTreeMap::new(),
            store: Store::new(config.store),
            doorkeeper: Doorkeeper::new(config.doorkeeper_window),
            config,
            objects: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_use: 0,
            versions: BTreeMap::new(),

            origins: BTreeMap::new(),
            next_origin: 0,
            in_flight: BTreeMap::new(),
            writes: BTreeMap::new(),
            awaiting_writes: BTreeMap::new(),
            arriving: None,
            recovered: BTreeSet::new(),
            verifying: BTreeMap::new(),
            filling_bytes: 0,
            waiting: BTreeMap::new(),
            sending: BTreeMap::new(),
            stats: Stats::default(),
            actions: Vec::new(),
        }
    }

    /// A node restarting over the slot table's records and the metadata
    /// file's entries, in the order they were appended. It serves trusted
    /// blocks as they are and verifies each other block on its first read.
    /// Records that no slot of this store can hold, or that clash with
    /// others, are cleared. The latest saved metadata is kept, up to
    /// capacity.
    pub fn recover(
        id: NodeId,
        ring: Ring,
        config: Config,
        records: impl IntoIterator<Item = Recovered>,
        metadata: impl IntoIterator<Item = (ObjectKey, Option<Meta>)>,
    ) -> Node {
        let mut node = Node::new(id, ring, config);
        for (key, meta) in metadata {
            match meta {
                Some(meta) => node.keep(key, meta, Time::default()),
                None => {
                    node.forget(&key);
                }
            }
        }
        let block_size = node.config.layout.block_size();
        for recovered in records {
            let Recovered {
                location,
                record,
                checksum,
                trusted,
            } = recovered;
            let version = record.version;
            node.versions.entry(version).or_insert(Version {
                refs: 0,
                name: None,
            });
            let block = BlockKey {
                version,
                index: record.index,
            };
            let hash = block_hash(version, block_size, record.index);
            let verify = (!trusted).then_some(checksum);
            let restored = record.len <= block_size
                && node
                    .store
                    .restore(block, location, record.len, hash, record.placement, verify);
            if restored {
                node.refer(version);
                node.recovered.insert(version.key);
            } else {
                node.forget_if_unused(version);
                node.actions.push(Action::Clear { location });
            }
        }
        node
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Every stored block the node would serve without verifying it first.
    pub fn stored_blocks(&self) -> impl Iterator<Item = StoredBlock> {
        self.store
            .blocks()
            .filter(|(_, entry)| entry.state == BlockState::Ready && entry.verify.is_none())
            .map(|(block, entry)| StoredBlock {
                version: block.version,
                index: block.index,
                location: entry.location,
                len: entry.len,
            })
    }

    /// The block stored at `location`, if the node would serve it without
    /// verifying it first.
    pub fn stored_block_at(&self, location: Location) -> Option<StoredBlock> {
        let block = self.store.block_at(location)?;
        let entry = self.store.get(&block)?;
        if entry.state != BlockState::Ready || entry.verify.is_some() {
            return None;
        }
        Some(StoredBlock {
            version: block.version,
            index: block.index,
            location,
            len: entry.len,
        })
    }

    /// A one-line summary of the work in progress, for debugging.
    pub fn describe(&self) -> String {
        let fetching = self
            .objects
            .iter()
            .filter_map(|(key, object)| match object {
                Object::Fetching {
                    origin, waiting, ..
                } => Some(format!(
                    "{}: first fetch {origin:?} with {} waiting",
                    key.key,
                    waiting.len()
                )),
                Object::Known {
                    revalidation: Some((origin, waiting)),
                    ..
                } => Some(format!(
                    "{}: revalidation {origin:?} with {} waiting",
                    key.key,
                    waiting.len()
                )),
                Object::Known { .. } => None,
            })
            .collect::<Vec<_>>();
        let origins = self
            .origins
            .iter()
            .map(|(origin, request)| {
                format!(
                    "{origin:?} sent {} answered {} cancelled {} readers {} waiters {}",
                    request.sent.0,
                    request.answered,
                    request.cancelled,
                    request.readers,
                    request.waiters.len()
                )
            })
            .collect::<Vec<_>>();
        format!(
            "{} waiting, {} sending, objects [{}], origins [{}]",
            self.waiting.len(),
            self.sending.len(),
            fetching.join("; "),
            origins.join("; ")
        )
    }

    /// True when no request, response, fill, write or verification is in
    /// progress.
    pub fn is_idle(&self) -> bool {
        self.waiting.is_empty()
            && self.sending.is_empty()
            && self.origins.is_empty()
            && self.in_flight.is_empty()
            && self.writes.is_empty()
            && self.verifying.is_empty()
    }

    /// The actions since the last drain, in the order the node took them.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    pub fn on_request(&mut self, now: Time, id: GatewayRequestId, read: Read) {
        self.now = self.now.max(now);
        self.stats.reads += 1;
        match read {
            // Only the home keeps an object's metadata, since writes reach
            // only the home; a failover candidate reads S3 directly.
            Read::Object {
                request, direct, ..
            } if direct || !self.is_home(&request.key) => {
                let purpose = Purpose::Direct { request: id };
                self.fetch(purpose, request);
            }
            Read::Object {
                mut request, stale, ..
            } => {
                request.range = request.range.filter(|range| range.is_valid());
                let waiting = Waiting {
                    read: Read::Object {
                        request,
                        stale,
                        direct: false,
                    },
                    arrived: now,
                    plan: None,
                };
                self.waiting.insert(id, waiting);
                self.serve(now, id);
            }
            Read::Range(range) => self.read_range(now, id, range),
            Read::Stored(range) => self.serve_stored(id, range),
            Read::Known(key) => self.serve_known(id, &key),
        }
    }

    /// Answers a new owner's read from stored blocks, or with 404 if any
    /// is missing or unverified. The blocks gain no hits, since their new
    /// owner will hold them.
    fn serve_stored(&mut self, id: GatewayRequestId, range: RangeRead) {
        let layout = self.config.layout;
        let version = VersionId::of(&range.key, &range.etag);
        let mut blocks = Vec::new();
        let valid = range.first <= range.last && range.last < range.size;
        for index in layout
            .blocks_covering(range.first, range.last)
            .filter(|_| valid)
        {
            let block = BlockKey { version, index };
            match self.store.get(&block) {
                Some(entry) if entry.state == BlockState::Ready && entry.verify.is_none() => {
                    blocks.push((block, entry.location, layout.block_span(range.size, index)));
                }
                _ => {
                    blocks.clear();
                    break;
                }
            }
        }
        if blocks.is_empty() {
            let head = ResponseHead::status(404);
            return self.respond(id, head, Vec::new(), Holds::default(), None);
        }
        let mut holds = Holds::default();
        let mut body = Vec::new();
        for (block, location, span) in blocks {
            self.store.pin(block);
            holds.pins.push(block);
            let piece = span.start.max(range.first)..span.end.min(range.last + 1);
            body.push(Segment::Slot {
                location,
                offset: piece.start - span.start,
                len: piece.end - piece.start,
            });
        }
        let head = ResponseHead {
            status: 206,
            etag: Some(range.etag.clone()),
            content_range: Some(ContentRange {
                first: range.first,
                last: range.last,
                size: range.size,
            }),
            content_length: range.last - range.first + 1,
            headers: Vec::new(),
        };
        self.respond(id, head, body, holds, None);
    }

    /// Tells an object's new home the metadata this node knows, however
    /// old, or answers 404.
    fn serve_known(&mut self, id: GatewayRequestId, key: &ObjectKey) {
        match self.objects.get(key) {
            Some(Object::Known {
                meta, validated, ..
            }) => {
                let meta = shared(meta, *validated, self.now);
                self.actions.push(Action::Metadata { request: id, meta });
            }
            _ => self.respond(
                id,
                ResponseHead::status(404),
                Vec::new(),
                Holds::default(),
                None,
            ),
        }
    }

    /// Serves bytes of a version the gateway names. The node fills with
    /// `If-Match`, so the bytes are that version's or the read goes stale.
    fn read_range(&mut self, now: Time, id: GatewayRequestId, range: RangeRead) {
        if range.first > range.last || range.last >= range.size {
            return self.respond(
                id,
                ResponseHead::status(416),
                Vec::new(),
                Holds::default(),
                None,
            );
        }
        let head = ResponseHead {
            status: 206,
            etag: Some(range.etag.clone()),
            content_range: Some(ContentRange {
                first: range.first,
                last: range.last,
                size: range.size,
            }),
            content_length: range.last - range.first + 1,
            headers: Vec::new(),
        };
        let waiting = Waiting {
            read: Read::Range(range.clone()),
            arrived: now,
            plan: None,
        };
        self.waiting.insert(id, waiting);
        let body = BodyPlan {
            head,
            meta: None,
            first: range.first,
            last: range.last,
        };
        self.plan_body(id, &range.key, &range.etag, range.size, body);
    }

    fn slot_record(&self, block: BlockKey) -> SlotRecord {
        let entry = self.store.get(&block).expect("recorded block exists");
        SlotRecord {
            version: block.version,
            index: block.index,
            len: entry.len,
            placement: entry.placement,
        }
    }

    fn holds_blocks_of(&self, key: &ObjectKey) -> bool {
        let key = VersionId::key_hash(key);
        self.versions.range(VersionId::all_of(key)).next().is_some()
    }

    fn is_home(&self, key: &ObjectKey) -> bool {
        self.ring.owner(Placement::Home(key).hash()) == Some(self.id)
    }

    /// The `GetObject` or `HeadObject` a home serves.
    fn object_request(&self, id: GatewayRequestId) -> &Request {
        match &self.waiting[&id].read {
            Read::Object { request, .. } => request,
            _ => unreachable!("only object reads need metadata"),
        }
    }

    pub fn on_origin_response(&mut self, now: Time, origin: OriginRequestId, head: ResponseHead) {
        self.now = self.now.max(now);
        let Some(request) = self.origins.get_mut(&origin) else {
            return;
        };
        request.answered = true;
        match &request.purpose {
            // A previous home that answers without metadata has none.
            Purpose::PeerMeta { .. } => return self.on_peer_metadata(now, origin, None),
            Purpose::First {
                key,
                request,
                sent,
                relay,
            } => {
                let (key, request, sent, relay) = (key.clone(), *request, *sent, *relay);
                self.first_answered(now, origin, key, request, sent, relay, head);
            }
            Purpose::Revalidate { key, sent } => {
                let (key, sent) = (key.clone(), *sent);
                self.revalidated(now, origin, key, sent, head);
            }
            Purpose::Fill { .. } => self.fill_answered(now, origin, head),
            Purpose::Direct { request } => {
                let request = *request;
                self.relay(request, origin, head);
            }
        }
        self.release_if_unread(origin);
    }

    /// A write to `key` passed through this node and succeeded: its
    /// metadata no longer holds.
    pub fn on_write(&mut self, now: Time, key: &ObjectKey) {
        self.now = self.now.max(now);
        if self.previous.is_some() {
            self.written.insert(key.clone(), self.now);
        }
        if self.policy(&key.bucket).freshness == Freshness::Immutable {
            let key = key.clone();
            self.actions.push(Action::Forget { key });
        }
        match self.objects.get_mut(key) {
            Some(Object::Fetching { superseded, .. }) => *superseded = true,
            Some(Object::Known { .. }) => {
                if let Some(Object::Known {
                    revalidation: Some((_, waiters)),
                    ..
                }) = self.forget(key)
                {
                    for waiter in waiters {
                        self.serve(now, waiter);
                    }
                }
            }
            None => {}
        }
    }

    /// The ring the node places blocks by.
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    /// Membership changed the ring. The node keeps the previous one for
    /// the fallback window.
    pub fn on_ring(&mut self, now: Time, ring: Ring) {
        self.now = self.now.max(now);
        if ring == self.ring {
            return;
        }
        let until = Time(self.now.0 + self.config.fallback_window);
        let previous = std::mem::replace(&mut self.ring, ring);
        self.previous = Some((previous, until));
        self.unreachable.clear();
    }

    /// The node is joining a cluster whose ring, before it arrived, was
    /// `before`. If that ring lacks this node, the node took its placements
    /// from others, and asks them first until the fallback window ends.
    pub fn on_joined(&mut self, now: Time, before: Ring) {
        self.now = self.now.max(now);
        if before.members().iter().any(|member| member.id == self.id) {
            return;
        }
        let until = Time(self.now.0 + self.config.fallback_window);
        self.previous = Some((before, until));
        self.unreachable.clear();
    }

    /// The node that owned `placement` before the last ring change, while
    /// the fallback window lasts, unless it is this node or failed to
    /// answer.
    fn previous_owner(&self, placement: PlacementHash) -> Option<NodeId> {
        let (ring, until) = self.previous.as_ref()?;
        if self.now >= *until {
            return None;
        }
        ring.owner(placement)
            .filter(|owner| *owner != self.id && !self.unreachable.contains(owner))
    }

    /// Time passed: S3 requests unanswered past the timeout are abandoned,
    /// and whatever waited on them proceeds as if S3 failed with 503. The
    /// previous ring goes once the fallback window ends.
    pub fn on_tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        if self
            .previous
            .as_ref()
            .is_some_and(|(_, until)| *until <= self.now)
        {
            self.previous = None;
            self.written.clear();
            // Blocks placed elsewhere now go before any this node owns.
            let (ring, id) = (&self.ring, self.id);
            self.store
                .disown(|placement| ring.owner(placement) == Some(id));
        }
        let expired: Vec<OriginRequestId> = self
            .origins
            .iter()
            .filter(|(_, request)| !request.answered && request.sent.0 + request.timeout <= now.0)
            .map(|(&origin, _)| origin)
            .collect();
        for origin in expired {
            let request = self.origins.get_mut(&origin).expect("expired request");
            request.cancelled = true;
            let (peer, asks_metadata) = (
                request.peer,
                matches!(request.purpose, Purpose::PeerMeta { .. }),
            );
            self.actions.push(Action::Cancel { origin });
            self.unreachable.extend(peer);
            match asks_metadata {
                true => self.on_peer_metadata(now, origin, None),
                false => self.on_origin_response(now, origin, ResponseHead::status(503)),
            }
        }
    }

    /// The bytes for the slot at `location` are durable.
    pub fn on_written(&mut self, location: Location) {
        let origin = self
            .writes
            .remove(&location)
            .expect("a write was in progress");
        let block = self
            .store
            .block_at(location)
            .expect("a written slot holds a block");
        self.store.filled(block);
        let record = self.slot_record(block);
        self.filling_bytes -= record.len;
        self.stats.written_bytes += record.len;
        self.actions.push(Action::Record { location, record });
        if self.in_flight.get(&block) == Some(&origin) {
            self.in_flight.remove(&block);
            self.unref(block.version);
        }
        self.stop_reading(origin);
        for waiter in self.awaiting_writes.remove(&location).unwrap_or_default() {
            self.arrived(waiter, Await::Written(location));
        }
    }

    /// S3's response body ended before the bytes for the slot at
    /// `location` arrived: the block is not stored.
    pub fn on_write_failed(&mut self, location: Location) {
        let origin = self
            .writes
            .remove(&location)
            .expect("a write was in progress");
        let block = self
            .store
            .block_at(location)
            .expect("a written slot holds a block");
        let len = self.store.get(&block).expect("written block exists").len;
        let waiters: Vec<GatewayRequestId> = self
            .awaiting_writes
            .remove(&location)
            .unwrap_or_default()
            .into_iter()
            .filter(|&waiter| self.abandon_plan(waiter))
            .collect();
        self.store.remove(block);
        self.filling_bytes -= len;
        self.unref(block.version);
        // Later reads fill the block again instead of reading this body.
        if self.in_flight.get(&block) == Some(&origin) {
            self.in_flight.remove(&block);
            self.unref(block.version);
        }
        self.stop_reading(origin);
        let now = self.now;
        for waiter in waiters {
            self.replan(now, waiter);
        }
    }

    /// The block at `location` was checked against its checksum. An intact
    /// block serves the requests waiting for it; a corrupt one is dropped,
    /// and they plan again as misses.
    pub fn on_verified(&mut self, now: Time, location: Location, intact: bool) {
        self.now = self.now.max(now);
        let (block, waiters) = self
            .verifying
            .remove(&location)
            .expect("a verification was in progress");
        self.store.unpin(block);
        self.stats.verified_blocks += 1;
        if intact {
            self.store.verified(block);
            let record = self.slot_record(block);
            self.actions.push(Action::Record { location, record });
            for waiter in waiters {
                self.arrived(waiter, Await::Verify(location));
            }
            return;
        }
        self.stats.corrupt_blocks += 1;
        let waiters: Vec<GatewayRequestId> = waiters
            .into_iter()
            .filter(|&waiter| self.abandon_plan(waiter))
            .collect();
        self.store.remove(block);
        self.actions.push(Action::Clear { location });
        self.unref(block.version);
        for waiter in waiters {
            self.replan(now, waiter);
        }
    }

    /// The response to `request` is sent: its blocks and bodies are free.
    pub fn on_sent(&mut self, request: GatewayRequestId) {
        let holds = self
            .sending
            .remove(&request)
            .expect("a response was sending");
        self.release(holds);
    }

    fn serve(&mut self, now: Time, id: GatewayRequestId) {
        let key = self.object_request(id).key.clone();
        let arrived = self.waiting[&id].arrived;
        let freshness = self.policy(&key.bucket).freshness;
        if !self.objects.contains_key(&key) {
            // After a restart, a home that still holds blocks of the
            // object fetches only its metadata, then serves the blocks.
            if self.recovered.remove(&VersionId::key_hash(&key)) && self.holds_blocks_of(&key) {
                return self.first_fetch(now, id, Request::head(key), Vec::new(), false);
            }
        }
        let previous_home = self.previous_owner(Placement::Home(&key).hash());
        match self.objects.get_mut(&key) {
            None if let Some(peer) = previous_home => {
                let purpose = Purpose::PeerMeta {
                    key: key.clone(),
                    request: id,
                    sent: now,
                };
                let origin = self.ask_peer(purpose, peer, Read::Known(key.clone()), 0);
                let fetching = Object::Fetching {
                    origin,
                    waiting: Vec::new(),
                    superseded: false,
                };
                self.objects.insert(key, fetching);
            }
            None => {
                let client = self.object_request(id);
                let request = Request {
                    method: client.method,
                    key: key.clone(),
                    range: client.range,
                    if_match: None,
                    if_none_match: None,
                };
                self.first_fetch(now, id, request, Vec::new(), true);
            }
            Some(Object::Fetching { waiting, .. }) => waiting.push(id),
            Some(Object::Known {
                meta,
                validated,
                revalidation,
                ..
            }) => {
                let reported_stale = match &mut self.waiting.get_mut(&id).expect("served").read {
                    // Revalidating answers the report, so it counts once.
                    Read::Object { stale, .. } => {
                        stale.take().is_some_and(|stale| stale == meta.etag)
                    }
                    _ => false,
                };
                let fresh = !reported_stale
                    && match freshness {
                        Freshness::Immutable => true,
                        Freshness::Ttl(ttl) => validated.0 + ttl >= arrived.0,
                    };
                if fresh {
                    let meta = meta.clone();
                    let shared = shared(&meta, *validated, now);
                    self.touch(&key);
                    self.plan(id, &key, &meta, shared);
                } else if let Some((_, waiting)) = revalidation {
                    waiting.push(id);
                } else {
                    let request = Request {
                        if_none_match: Some(meta.etag.clone()),
                        ..Request::head(key.clone())
                    };
                    let purpose = Purpose::Revalidate {
                        key: key.clone(),
                        sent: now,
                    };
                    let origin = self.fetch(purpose, request);
                    let Some(Object::Known { revalidation, .. }) = self.objects.get_mut(&key)
                    else {
                        unreachable!("the object was known a moment ago");
                    };
                    *revalidation = Some((origin, vec![id]));
                }
            }
        }
    }

    /// Fetches an object the home has no metadata for. The fetch is
    /// unconditional, so its response sets the metadata; when `relay` is
    /// set, it asks exactly what the client asked and also answers it.
    fn first_fetch(
        &mut self,
        now: Time,
        id: GatewayRequestId,
        request: Request,
        waiting: Vec<GatewayRequestId>,
        relay: bool,
    ) {
        let key = request.key.clone();
        let purpose = Purpose::First {
            key: key.clone(),
            request: id,
            sent: now,
            relay,
        };
        let origin = self.fetch(purpose, request);
        let fetching = Object::Fetching {
            origin,
            waiting,
            superseded: false,
        };
        self.objects.insert(key, fetching);
    }

    fn fetch(&mut self, purpose: Purpose, request: Request) -> OriginRequestId {
        let origin = OriginRequestId(self.next_origin);
        self.next_origin += 1;
        let (body_start, streams) = match &purpose {
            Purpose::Fill { .. } => match request.range {
                Some(ByteRange::Inclusive { first, .. }) => (first, false),
                _ => unreachable!("fills ask for inclusive ranges"),
            },
            _ => (0, true),
        };
        self.origins.insert(
            origin,
            OriginRequest {
                purpose,
                method: request.method,
                sent: self.now,
                timeout: self.config.origin_timeout,
                peer: None,
                answered: false,
                cancelled: false,
                body_start,
                readers: 0,
                blocks: Vec::new(),
                waiters: Vec::new(),
            },
        );
        self.stats.origin_requests += 1;
        self.actions.push(Action::Fetch {
            origin,
            request,
            streams,
        });
        origin
    }

    /// Sends `read` to `peer`, whose answer's body, if it has one, starts
    /// at the object's byte `body_start`.
    fn ask_peer(
        &mut self,
        purpose: Purpose,
        peer: NodeId,
        read: Read,
        body_start: u64,
    ) -> OriginRequestId {
        let origin = OriginRequestId(self.next_origin);
        self.next_origin += 1;
        self.origins.insert(
            origin,
            OriginRequest {
                purpose,
                method: Method::Get,
                sent: self.now,
                timeout: self.config.peer_timeout,
                peer: Some(peer),
                answered: false,
                cancelled: false,
                body_start,
                readers: 0,
                blocks: Vec::new(),
                waiters: Vec::new(),
            },
        );
        self.stats.peer_requests += 1;
        self.actions.push(Action::PeerFetch { origin, peer, read });
        origin
    }

    /// The object's previous home answered with the metadata it knows, or
    /// with none. Metadata counts as validated when it was asked for, less
    /// its age, and a write since then makes it useless. Without it, the
    /// home fetches the object from S3.
    pub fn on_peer_metadata(
        &mut self,
        now: Time,
        origin: OriginRequestId,
        meta: Option<ObjectMeta>,
    ) {
        self.now = self.now.max(now);
        let Some(request) = self.origins.get(&origin) else {
            return;
        };
        let Purpose::PeerMeta {
            key,
            request: id,
            sent,
        } = &request.purpose
        else {
            return;
        };
        let (key, id, sent) = (key.clone(), *id, *sent);
        self.origins.remove(&origin);
        let (waiters, superseded) = match self.objects.remove(&key) {
            Some(Object::Fetching {
                origin: fetch,
                waiting,
                superseded,
            }) if fetch == origin => (waiting, superseded),
            other => {
                self.objects.extend(other.map(|object| (key, object)));
                return;
            }
        };
        let usable = meta.filter(|_| !superseded).and_then(|meta| {
            let validated = Time(sent.0.saturating_sub(meta.age));
            let written = self
                .written
                .get(&key)
                .is_some_and(|&written| written >= validated);
            (!written).then_some((meta, validated))
        });
        let Some((meta, validated)) = usable else {
            let client = self.object_request(id);
            let request = Request {
                method: client.method,
                key: key.clone(),
                range: client.range,
                if_match: None,
                if_none_match: None,
            };
            return self.first_fetch(now, id, request, waiters, true);
        };
        self.stats.peer_metadata += 1;
        let meta = Meta {
            etag: meta.etag,
            size: meta.size,
            headers: meta.headers,
        };
        self.know(key, meta, validated);
        self.serve(now, id);
        for waiter in waiters {
            self.serve(now, waiter);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn first_answered(
        &mut self,
        now: Time,
        origin: OriginRequestId,
        key: ObjectKey,
        request: GatewayRequestId,
        sent: Time,
        relay: bool,
        head: ResponseHead,
    ) {
        let (waiters, superseded) = match self.objects.remove(&key) {
            Some(Object::Fetching {
                origin: fetch,
                waiting,
                superseded,
            }) if fetch == origin => (waiting, superseded),
            other => unreachable!(
                "first fetch {origin:?} answered while {:?}",
                other.is_some()
            ),
        };
        let client = self.object_request(request);
        let conditional = client.if_match.is_some() || client.if_none_match.is_some();
        if relay && head.status == 416 && conditional {
            // S3 checks preconditions before ranges, so the client's answer
            // may differ: learn the metadata and answer here.
            self.first_fetch(now, request, Request::head(key), waiters, false);
            return;
        }
        let meta = metadata(&head);
        let has_meta = meta.is_some();
        if !relay {
            if superseded {
                self.serve(now, request);
            } else if let Some(meta) = meta {
                self.know(key, meta, sent);
                self.serve(now, request);
            } else {
                self.waiting.remove(&request);
                let head = ResponseHead::status(head.status);
                self.respond(request, head, Vec::new(), Holds::default(), None);
            }
            return self.resume(now, waiters, sent, &head, has_meta);
        }
        if let Some(meta) = meta.as_ref().filter(|_| !superseded) {
            self.know(key.clone(), meta.clone(), sent);
        }
        let Read::Object {
            request: client, ..
        } = self
            .waiting
            .remove(&request)
            .expect("the first request waits")
            .read
        else {
            unreachable!("a first fetch serves an object read");
        };
        let answer = meta
            .as_ref()
            .and_then(|meta| preconditions(&client, &meta.etag));
        let known = meta
            .as_ref()
            .filter(|_| !superseded)
            .map(|meta| shared(meta, sent, now));
        match answer {
            Some(head) => self.respond(request, head, Vec::new(), Holds::default(), known),
            None => {
                let len = match self.origins[&origin].method {
                    Method::Get => head.content_length,
                    Method::Head => 0,
                };
                let mut holds = Holds::default();
                let mut body = Vec::new();
                if len > 0 {
                    body.push(Segment::Origin {
                        origin,
                        offset: 0,
                        len,
                    });
                    self.read(origin, &mut holds);
                }
                self.respond(request, head.clone(), body, holds, known);
            }
        }
        if let Some(meta) = meta
            && !superseded
            && self.origins[&origin].method == Method::Get
            && let Some((first, last)) = body_span(&head, meta.size)
        {
            self.store_first_fetch(origin, &key, &meta, first, last);
            let version = VersionId::of(&key, &meta.etag);
            self.arriving = Some((origin, version, first, last));
        }
        self.resume(now, waiters, sent, &head, has_meta);
        self.arriving = None;
    }

    /// Serves the requests that waited on a first fetch sent at `sent`. A
    /// 404 or a 5xx without metadata answers those that arrived before the
    /// fetch left, since S3 checked after they did; the rest start over.
    fn resume(
        &mut self,
        now: Time,
        waiters: Vec<GatewayRequestId>,
        sent: Time,
        head: &ResponseHead,
        known: bool,
    ) {
        let shared_answer = !known && (head.status == 404 || head.status >= 500);
        for waiter in waiters {
            let arrived = self.waiting.get(&waiter).map(|waiting| waiting.arrived);
            if shared_answer && arrived.is_some_and(|arrived| arrived <= sent) {
                self.waiting.remove(&waiter);
                let head = ResponseHead::status(head.status);
                self.respond(waiter, head, Vec::new(), Holds::default(), None);
            } else {
                self.serve(now, waiter);
            }
        }
    }

    /// Stores the whole blocks in a first fetch's body that the admission
    /// policy accepts.
    fn store_first_fetch(
        &mut self,
        origin: OriginRequestId,
        key: &ObjectKey,
        meta: &Meta,
        first: u64,
        last: u64,
    ) {
        let version = self.version(key, &meta.etag);
        // Admitting a block may evict another of this version, which must
        // not take the version with it.
        self.refer(version);
        let layout = self.config.layout;
        for index in layout.blocks_covering(first, last) {
            let span = layout.block_span(meta.size, index);
            let block = BlockKey { version, index };
            if span.start < first
                || span.end > last + 1
                || self.store.get(&block).is_some()
                || self.in_flight.contains_key(&block)
            {
                continue;
            }
            // The body streams, so later readers wait for the block's write
            // and read its slot instead of joining the body.
            if let Some(location) = self.admit(key, meta.size, block, false) {
                self.write(location, origin, span.start - first, span.end - span.start);
            }
        }
        self.unref(version);
    }

    fn revalidated(
        &mut self,
        now: Time,
        origin: OriginRequestId,
        key: ObjectKey,
        sent: Time,
        head: ResponseHead,
    ) {
        let Some(Object::Known {
            meta,
            validated,
            revalidation,
            ..
        }) = self.objects.get_mut(&key)
        else {
            return;
        };
        if revalidation.as_ref().map(|(pending, _)| *pending) != Some(origin) {
            return;
        }
        let (_, waiters) = revalidation.take().expect("a revalidation was pending");
        match (head.status, metadata(&head)) {
            (304, _) => *validated = sent,
            (200, Some(fresh)) => {
                *meta = fresh;
                *validated = sent;
            }
            _ => {
                self.forget(&key);
            }
        }
        for waiter in waiters {
            self.serve(now, waiter);
        }
    }

    /// Plans the response to a request whose metadata is known and fresh.
    fn plan(&mut self, id: GatewayRequestId, key: &ObjectKey, meta: &Meta, shared: ObjectMeta) {
        let request = self.object_request(id).clone();
        let (head, first, last) = match answer(&request, &meta.etag, meta.size, &meta.headers) {
            Answer::Head(head) => {
                self.waiting.remove(&id);
                return self.respond(id, head, Vec::new(), Holds::default(), Some(shared));
            }
            Answer::Body { head, first, last } => (head, first, last),
        };
        let layout = self.config.layout;
        let beyond_home = layout
            .runs(key, meta.size, first, last)
            .iter()
            .any(|(placement, _, _)| self.ring.owner(placement.hash()) != Some(self.id));
        if beyond_home {
            self.waiting.remove(&id);
            self.actions.push(Action::Metadata {
                request: id,
                meta: shared,
            });
            return;
        }
        let body = BodyPlan {
            head,
            meta: Some(shared),
            first,
            last,
        };
        self.plan_body(id, key, &meta.etag.clone(), meta.size, body);
    }

    /// Plans the body of bytes `first..=last` of a version: stored blocks,
    /// blocks already arriving, and fills for the rest. The response starts
    /// once every fill it reads has answered.
    fn plan_body(
        &mut self,
        id: GatewayRequestId,
        key: &ObjectKey,
        etag: &ETag,
        size: u64,
        plan: BodyPlan,
    ) {
        let BodyPlan {
            head,
            meta,
            first,
            last,
        } = plan;
        let version = self.version(key, etag);
        let mut body = Vec::new();
        let mut holds = Holds::default();
        // A request queued behind a first fetch reads its body while the
        // body arrives, if the body holds every byte it asks for.
        if let Some((origin, arriving, start, end)) = self.arriving
            && arriving == version
            && start <= first
            && last <= end
        {
            body.push(Segment::Origin {
                origin,
                offset: first - start,
                len: last - first + 1,
            });
            self.read(origin, &mut holds);
            self.forget_if_unused(version);
            self.waiting.remove(&id);
            return self.respond(id, head, body, holds, meta);
        }
        let mut awaiting = BTreeSet::new();
        let layout = self.config.layout;
        let chunk_blocks = (layout.chunk_size() / layout.block_size()) as usize;
        let blocks: Vec<u64> = layout.blocks_covering(first, last).collect();
        let mut next = 0;
        while next < blocks.len() {
            let index = blocks[next];
            let block = BlockKey { version, index };
            let span = layout.block_span(size, index);
            let piece = span.start.max(first)..span.end.min(last + 1);
            let len = piece.end - piece.start;
            let stored = self
                .store
                .get(&block)
                .map(|entry| (entry.location, entry.state, entry.verify.is_some()));
            let in_flight = self.in_flight.get(&block).copied();
            // A stored block is read from its slot once it is ready, or,
            // while a first fetch's streaming body writes it, once written.
            let slot = match (stored, in_flight) {
                (Some((location, BlockState::Ready, unverified)), _) => {
                    self.store.hit(block);
                    Some((location, unverified.then_some(Await::Verify(location))))
                }
                (Some((location, BlockState::Filling, _)), None) => {
                    Some((location, Some(Await::Written(location))))
                }
                _ => None,
            };
            if let Some((location, awaited)) = slot {
                self.store.pin(block);
                holds.pins.push(block);
                let offset = piece.start - span.start;
                body.push(Segment::Slot {
                    location,
                    offset,
                    len,
                });
                awaiting.extend(awaited);
                next += 1;
                continue;
            }
            let origin = match in_flight {
                Some(origin) => origin,
                None => {
                    // Fetch this block and every missing block after it,
                    // up to a chunk, in one range GET.
                    let run_end = blocks[next..]
                        .iter()
                        .take_while(|&&index| {
                            let block = BlockKey { version, index };
                            self.store.get(&block).is_none() && !self.in_flight.contains_key(&block)
                        })
                        .take(chunk_blocks)
                        .count();
                    let run = blocks[next]..=blocks[next + run_end - 1];
                    self.fill(key, etag, size, version, run)
                }
            };
            let body_start = self.origins[&origin].body_start;
            body.push(Segment::Origin {
                origin,
                offset: piece.start - body_start,
                len,
            });
            self.read(origin, &mut holds);
            if !self.origins[&origin].answered {
                awaiting.insert(Await::Fill(origin));
            }
            next += 1;
        }
        self.forget_if_unused(version);
        if awaiting.is_empty() {
            self.waiting.remove(&id);
            return self.respond(id, head, body, holds, meta);
        }
        for &awaited in &awaiting {
            match awaited {
                Await::Fill(origin) => {
                    let request = self.origins.get_mut(&origin).expect("awaited fill exists");
                    request.waiters.push(id);
                }
                Await::Verify(location) => self.verify(location, id),
                Await::Written(location) => {
                    self.awaiting_writes.entry(location).or_default().push(id);
                }
            }
        }
        let waiting = self
            .waiting
            .get_mut(&id)
            .expect("the planned request waits");
        waiting.plan = Some(Plan {
            head,
            meta,
            body,
            holds,
            awaiting,
        });
    }

    /// Fetches blocks `run` with `If-Match`, storing those the admission
    /// policy accepts.
    fn fill(
        &mut self,
        key: &ObjectKey,
        etag: &ETag,
        size: u64,
        version: VersionId,
        run: std::ops::RangeInclusive<u64>,
    ) -> OriginRequestId {
        let layout = self.config.layout;
        let first = layout.block_span(size, *run.start()).start;
        let last_byte = layout.block_span(size, *run.end()).end - 1;
        let request = Request {
            method: Method::Get,
            key: key.clone(),
            range: Some(ByteRange::Inclusive {
                first,
                last: last_byte,
            }),
            if_match: Some(etag.clone()),
            if_none_match: None,
        };
        let purpose = Purpose::Fill {
            version,
            last_byte,
            stored: Vec::new(),
        };
        // A run lies within one chunk, so one node owns it, and owned it.
        let placement = layout.placement(key, size, *run.start()).hash();
        let peer = match self.ring.owner(placement) == Some(self.id) {
            true => self.previous_owner(placement),
            false => None,
        };
        let origin = match peer {
            Some(peer) => {
                let read = Read::Stored(RangeRead {
                    key: key.clone(),
                    etag: etag.clone(),
                    size,
                    first,
                    last: last_byte,
                });
                self.ask_peer(purpose, peer, read, first)
            }
            None => self.fetch(purpose, request),
        };
        let mut stored = Vec::new();
        for index in run {
            let block = BlockKey { version, index };
            self.track_in_flight(block, origin);
            if let Some(location) = self.admit(key, size, block, peer.is_some()) {
                stored.push((block, location));
            }
        }
        let request = self.origins.get_mut(&origin).expect("fill just started");
        let Purpose::Fill { stored: slots, .. } = &mut request.purpose else {
            unreachable!("a fill's purpose is Fill");
        };
        *slots = stored;
        origin
    }

    fn fill_answered(&mut self, now: Time, origin: OriginRequestId, head: ResponseHead) {
        let request = &self.origins[&origin];
        let Purpose::Fill {
            version,
            last_byte,
            stored,
        } = &request.purpose
        else {
            unreachable!("fill_answered on a fill");
        };
        let (version, stored) = (*version, stored.clone());
        let (_, etag) = self.name(version);
        let expected = Some((request.body_start, *last_byte));
        let valid = head.status == 206
            && head.etag.as_ref() == Some(etag)
            && head.content_range.map(|range| (range.first, range.last)) == expected;
        if !valid && request.peer.is_some() {
            return self.refill(origin);
        }
        let waiters = std::mem::take(&mut self.origins.get_mut(&origin).expect("fill").waiters);
        if valid {
            let body_start = self.origins[&origin].body_start;
            for (block, location) in stored {
                let entry = self.store.get(&block).expect("stored block reserved");
                let offset = self.config.layout.block_size() * block.index - body_start;
                self.write(location, origin, offset, entry.len);
            }
            for waiter in waiters {
                self.arrived(waiter, Await::Fill(origin));
            }
            return;
        }
        for (block, _) in stored {
            let len = self.store.get(&block).expect("stored block reserved").len;
            self.store.remove(block);
            self.filling_bytes -= len;
            self.unref(block.version);
        }
        // A 412 or 404 means the object changed, and so does a 206 for
        // another version or range. Any other status is S3 failing, which
        // the waiting requests pass on.
        let changed = matches!(head.status, 206 | 404 | 412);
        let mut revalidating = Vec::new();
        let (key, etag) = self.name(version).clone();
        let known =
            matches!(self.objects.get(&key), Some(Object::Known { meta, .. }) if meta.etag == etag);
        if changed
            && known
            && let Some(Object::Known { revalidation, .. }) = self.forget(&key)
        {
            revalidating = revalidation.map(|(_, waiters)| waiters).unwrap_or_default();
        }
        for waiter in waiters {
            if changed {
                self.stale(waiter);
            } else {
                self.fail(waiter, origin, &head);
            }
        }
        for waiter in revalidating {
            self.serve(now, waiter);
        }
    }

    /// A previous owner lacked the blocks of fill `old`, or never answered:
    /// the same bytes come from S3, into the same slots, and the requests
    /// waiting on `old` wait on the new fill instead.
    fn refill(&mut self, old: OriginRequestId) {
        let request = &self.origins[&old];
        let Purpose::Fill {
            version,
            last_byte,
            stored,
        } = &request.purpose
        else {
            unreachable!("only fills are refilled");
        };
        let (version, last_byte, stored) = (*version, *last_byte, stored.clone());
        let first = request.body_start;
        let (key, etag) = self.name(version).clone();
        let s3 = Request {
            method: Method::Get,
            key,
            range: Some(ByteRange::Inclusive {
                first,
                last: last_byte,
            }),
            if_match: Some(etag),
            if_none_match: None,
        };
        let purpose = Purpose::Fill {
            version,
            last_byte,
            stored,
        };
        let new = self.fetch(purpose, s3);
        let request = self.origins.get_mut(&old).expect("refilled fill");
        let blocks = std::mem::take(&mut request.blocks);
        let waiters = std::mem::take(&mut request.waiters);
        let mut readers = 0;
        for &waiter in &waiters {
            let Some(plan) = self
                .waiting
                .get_mut(&waiter)
                .and_then(|waiting| waiting.plan.as_mut())
            else {
                continue;
            };
            if plan.awaiting.remove(&Await::Fill(old)) {
                plan.awaiting.insert(Await::Fill(new));
            }
            for segment in &mut plan.body {
                if let Segment::Origin { origin, .. } = segment
                    && *origin == old
                {
                    *origin = new;
                }
            }
            for reader in plan
                .holds
                .readers
                .iter_mut()
                .filter(|reader| **reader == old)
            {
                *reader = new;
                readers += 1;
            }
        }
        self.origins.get_mut(&old).expect("refilled fill").readers -= readers;
        for &block in &blocks {
            if self.in_flight.get(&block) == Some(&old) {
                self.in_flight.insert(block, new);
            }
        }
        let request = self.origins.get_mut(&new).expect("the new fill");
        request.readers += readers;
        request.waiters = waiters;
        request.blocks = blocks;
    }

    /// A fill or verification a request awaited has finished.
    fn arrived(&mut self, id: GatewayRequestId, awaited: Await) {
        let Some(plan) = self
            .waiting
            .get_mut(&id)
            .and_then(|waiting| waiting.plan.as_mut())
        else {
            return;
        };
        if plan.awaiting.remove(&awaited) && plan.awaiting.is_empty() {
            let plan = self
                .waiting
                .remove(&id)
                .expect("waiting")
                .plan
                .expect("plan");
            self.respond(id, plan.head, plan.body, plan.holds, plan.meta);
        }
    }

    /// Starts verifying the recovered block at `location` for request `id`,
    /// or adds `id` to a verification in progress.
    fn verify(&mut self, location: Location, id: GatewayRequestId) {
        if let Some((_, waiters)) = self.verifying.get_mut(&location) {
            waiters.push(id);
            return;
        }
        let block = self
            .store
            .block_at(location)
            .expect("a verified slot holds a block");
        let entry = self.store.get(&block).expect("verified block exists");
        let (len, checksum) = (entry.len, entry.verify.expect("an unverified block"));
        // The verification holds the slot until it answers, so the slot
        // is never reused under it.
        self.store.pin(block);
        self.verifying.insert(location, (block, vec![id]));
        self.actions.push(Action::Verify {
            location,
            len,
            checksum,
        });
    }

    /// Drops a waiting request's plan, releasing what it held and leaving
    /// the fills and verifications it awaited. Returns false if the
    /// request no longer waits on a plan.
    fn abandon_plan(&mut self, id: GatewayRequestId) -> bool {
        let Some(plan) = self
            .waiting
            .get_mut(&id)
            .and_then(|waiting| waiting.plan.take())
        else {
            return false;
        };
        for awaited in plan.awaiting {
            match awaited {
                Await::Fill(origin) => {
                    if let Some(request) = self.origins.get_mut(&origin) {
                        request.waiters.retain(|&waiter| waiter != id);
                    }
                }
                Await::Verify(location) => {
                    if let Some((_, waiters)) = self.verifying.get_mut(&location) {
                        waiters.retain(|&waiter| waiter != id);
                    }
                }
                Await::Written(location) => {
                    if let Some(waiters) = self.awaiting_writes.get_mut(&location) {
                        waiters.retain(|&waiter| waiter != id);
                    }
                }
            }
        }
        self.release(plan.holds);
        true
    }

    /// Plans a waiting request again from the start.
    fn replan(&mut self, now: Time, id: GatewayRequestId) {
        match self.waiting[&id].read.clone() {
            Read::Object { .. } => self.serve(now, id),
            Read::Range(range) => {
                self.waiting.remove(&id);
                self.read_range(now, id, range);
            }
            Read::Stored(_) | Read::Known(_) => unreachable!("peers' reads never wait"),
        }
    }

    /// Returns the request to the gateway, releasing what its plan held.
    fn stale(&mut self, id: GatewayRequestId) {
        // A request awaiting several failed fills goes back once.
        let Some(waiting) = self.waiting.remove(&id) else {
            return;
        };
        if let Some(plan) = waiting.plan {
            self.release(plan.holds);
        }
        self.actions.push(Action::Stale { request: id });
    }

    /// Answers a direct read with S3's response to `origin`.
    fn relay(&mut self, id: GatewayRequestId, origin: OriginRequestId, head: ResponseHead) {
        let mut holds = Holds::default();
        let mut body = Vec::new();
        let len = match self.origins[&origin].method {
            Method::Get => head.content_length,
            Method::Head => 0,
        };
        if len > 0 {
            self.read(origin, &mut holds);
            body.push(Segment::Origin {
                origin,
                offset: 0,
                len,
            });
        }
        self.respond(id, head, body, holds, None);
    }

    /// Answers a waiting request with S3's error response to `origin`.
    fn fail(&mut self, id: GatewayRequestId, origin: OriginRequestId, error: &ResponseHead) {
        let Some(waiting) = self.waiting.remove(&id) else {
            return;
        };
        if let Some(plan) = waiting.plan {
            self.release(plan.holds);
        }
        let mut holds = Holds::default();
        let mut body = Vec::new();
        if error.content_length > 0 {
            self.read(origin, &mut holds);
            body.push(Segment::Origin {
                origin,
                offset: 0,
                len: error.content_length,
            });
        }
        let head = ResponseHead {
            content_length: error.content_length,
            ..ResponseHead::status(error.status)
        };
        self.respond(id, head, body, holds, None);
    }

    fn respond(
        &mut self,
        id: GatewayRequestId,
        head: ResponseHead,
        body: Vec<Segment>,
        holds: Holds,
        meta: Option<ObjectMeta>,
    ) {
        for segment in &body {
            match segment {
                Segment::Slot { len, .. } => self.stats.hit_bytes += len,
                Segment::Origin { origin, len, .. } => match self.origins[origin].peer {
                    Some(_) => self.stats.peer_bytes += len,
                    None => self.stats.miss_bytes += len,
                },
            }
        }
        self.sending.insert(id, holds);
        self.actions.push(Action::Respond {
            request: id,
            head,
            body,
            meta,
        });
    }

    fn release(&mut self, holds: Holds) {
        for block in holds.pins {
            self.store.unpin(block);
        }
        for origin in holds.readers {
            self.stop_reading(origin);
        }
    }

    fn read(&mut self, origin: OriginRequestId, holds: &mut Holds) {
        self.origins
            .get_mut(&origin)
            .expect("read body exists")
            .readers += 1;
        holds.readers.push(origin);
    }

    fn stop_reading(&mut self, origin: OriginRequestId) {
        let request = self.origins.get_mut(&origin).expect("read body exists");
        request.readers -= 1;
        self.release_if_unread(origin);
    }

    /// Frees an answered response's body once nothing reads it.
    fn release_if_unread(&mut self, origin: OriginRequestId) {
        let Some(request) = self.origins.get(&origin) else {
            return;
        };
        if !request.answered || request.readers > 0 {
            return;
        }
        let request = self.origins.remove(&origin).expect("released body exists");
        for block in request.blocks {
            if self.in_flight.get(&block) == Some(&origin) {
                self.in_flight.remove(&block);
                self.unref(block.version);
            }
        }
        if !request.cancelled {
            self.actions.push(Action::Release { origin });
        }
    }

    fn track_in_flight(&mut self, block: BlockKey, origin: OriginRequestId) {
        self.in_flight.insert(block, origin);
        self.refer(block.version);
        let request = self.origins.get_mut(&origin).expect("tracked body exists");
        request.blocks.push(block);
    }

    /// Reserves a slot if the admission policy stores this block. Blocks
    /// asked of a previous owner skip the doorkeeper: they were read
    /// before, where they were stored.
    fn admit(
        &mut self,
        key: &ObjectKey,
        size: u64,
        block: BlockKey,
        skip_doorkeeper: bool,
    ) -> Option<Location> {
        let layout = self.config.layout;
        let placement = layout.placement(key, size, block.index).hash();
        if self.ring.owner(placement) != Some(self.id) {
            return None;
        }
        let hash = block_hash(block.version, layout.block_size(), block.index);
        if !skip_doorkeeper
            && !self.policy(&key.bucket).admit_on_first_read
            && !self.doorkeeper.contains(hash)
        {
            self.doorkeeper.insert(hash);
            return None;
        }
        let span = layout.block_span(size, block.index);
        let len = span.end - span.start;
        if self.filling_bytes + len > self.config.fill_budget {
            return None;
        }
        let location = self.store.reserve(block, len, hash, placement);
        // The new block refers to its version before evictions may drop
        // their versions' last references, which can include this one.
        if location.is_some() {
            self.refer(block.version);
        }
        for (evicted, location) in self.store.drain_evicted() {
            self.stats.evicted_blocks += 1;
            self.actions.push(Action::Clear { location });
            self.unref(evicted.version);
        }
        let location = location?;
        self.filling_bytes += len;
        Some(location)
    }

    fn write(&mut self, location: Location, origin: OriginRequestId, offset: u64, len: u64) {
        self.origins
            .get_mut(&origin)
            .expect("written body exists")
            .readers += 1;
        self.writes.insert(location, origin);
        self.actions.push(Action::Write {
            location,
            origin,
            offset,
            len,
        });
    }

    fn version(&mut self, key: &ObjectKey, etag: &ETag) -> VersionId {
        let id = VersionId::of(key, etag);
        let version = self.versions.entry(id).or_insert(Version {
            refs: 0,
            name: None,
        });
        version
            .name
            .get_or_insert_with(|| (key.clone(), etag.clone()));
        id
    }

    /// The key and ETag of a version a read named.
    fn name(&self, id: VersionId) -> &(ObjectKey, ETag) {
        self.versions[&id].name.as_ref().expect("a named version")
    }

    fn refer(&mut self, id: VersionId) {
        self.versions.get_mut(&id).expect("a known version").refs += 1;
    }

    fn unref(&mut self, id: VersionId) {
        self.versions.get_mut(&id).expect("a known version").refs -= 1;
        self.forget_if_unused(id);
    }

    fn forget_if_unused(&mut self, id: VersionId) {
        if self
            .versions
            .get(&id)
            .is_some_and(|version| version.refs == 0)
        {
            self.versions.remove(&id);
        }
    }

    /// Keeps metadata the home just validated, and saves it if its bucket
    /// is immutable.
    fn know(&mut self, key: ObjectKey, meta: Meta, validated: Time) {
        if self.policy(&key.bucket).freshness == Freshness::Immutable {
            let (key, meta) = (key.clone(), meta.clone());
            self.actions.push(Action::Remember { key, meta });
        }
        self.keep(key, meta, validated);
    }

    /// Keeps metadata in place of any the key had, then drops the least
    /// recently used past capacity.
    fn keep(&mut self, key: ObjectKey, meta: Meta, validated: Time) {
        self.forget(&key);
        let used = self.next_use;
        self.next_use += 1;
        self.recency.insert(used, key.clone());
        let known = Object::Known {
            meta,
            validated,
            revalidation: None,
            used,
        };
        self.objects.insert(key, known);
        let mut kept = 0;
        while self.recency.len() > self.config.metadata_capacity && kept < self.recency.len() {
            let (_, oldest) = self.recency.first_key_value().expect("over capacity");
            let oldest = oldest.clone();
            match self.objects.get(&oldest) {
                Some(Object::Known {
                    revalidation: None, ..
                }) => {
                    self.forget(&oldest);
                }
                // Requests wait on its revalidation, so it stays.
                _ => {
                    self.touch(&oldest);
                    kept += 1;
                }
            }
        }
    }

    fn touch(&mut self, key: &ObjectKey) {
        let next = self.next_use;
        self.next_use += 1;
        if let Some(Object::Known { used, .. }) = self.objects.get_mut(key) {
            self.recency.remove(used);
            *used = next;
            self.recency.insert(next, key.clone());
        }
    }

    fn forget(&mut self, key: &ObjectKey) -> Option<Object> {
        let object = self.objects.remove(key)?;
        if let Object::Known { used, .. } = &object {
            self.recency.remove(used);
        }
        Some(object)
    }

    fn policy(&self, bucket: &str) -> BucketPolicy {
        self.config
            .buckets
            .get(bucket)
            .copied()
            .unwrap_or(self.config.default_policy)
    }
}

/// A body to plan: its head, the metadata to send with it, and its bytes.
struct BodyPlan {
    head: ResponseHead,
    meta: Option<ObjectMeta>,
    first: u64,
    last: u64,
}

/// Metadata as a home shares it with gateways.
fn shared(meta: &Meta, validated: Time, now: Time) -> ObjectMeta {
    ObjectMeta {
        etag: meta.etag.clone(),
        size: meta.size,
        headers: meta.headers.clone(),
        age: now.0.saturating_sub(validated.0),
    }
}

/// The object's bytes a successful GET's body holds, first and last.
fn body_span(head: &ResponseHead, size: u64) -> Option<(u64, u64)> {
    match head.content_range {
        Some(range) => Some((range.first, range.last)),
        None if size > 0 => Some((0, size - 1)),
        None => None,
    }
}

/// The metadata a successful response carries.
fn metadata(head: &ResponseHead) -> Option<Meta> {
    let size = match head.status {
        200 => head.content_length,
        206 => head.content_range?.size,
        _ => return None,
    };
    let etag = head.etag.clone()?;
    let headers = head.headers.clone();
    Some(Meta {
        etag,
        size,
        headers,
    })
}

/// A block's identity: its version, the block size and its index.
fn block_hash(version: VersionId, block_size: u64, index: u64) -> u64 {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(&version.version.to_le_bytes());
    bytes[16..24].copy_from_slice(&block_size.to_le_bytes());
    bytes[24..].copy_from_slice(&index.to_le_bytes());
    xxh3_64(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::Member;
    use std::num::NonZeroU32;

    fn config(metadata_capacity: usize) -> Config {
        let policy = BucketPolicy {
            freshness: Freshness::Immutable,
            admit_on_first_read: false,
        };
        Config {
            layout: Layout::new(64, 1),
            store: StoreConfig {
                extent_size: 256,
                extents: 4,
                min_slot: 16,
                max_slot: 64,
            },
            doorkeeper_window: 16,
            fill_budget: 1_024,
            metadata_capacity,
            origin_timeout: 1_000,
            fallback_window: 1_000,
            peer_timeout: 100,
            default_policy: policy,
            buckets: BTreeMap::new(),
        }
    }

    fn key(name: &str) -> ObjectKey {
        ObjectKey {
            bucket: "b".into(),
            key: name.into(),
        }
    }

    fn meta(tag: &str) -> Meta {
        Meta {
            etag: ETag(format!("\"{tag}\"")),
            size: 10,
            headers: Vec::new(),
        }
    }

    /// The metadata file holds an entry each time the home learned a key,
    /// so a replay keeps some keys twice. The later entry replaces the
    /// earlier, and both stay within capacity.
    #[test]
    fn a_replayed_key_keeps_one_entry() {
        let member = Member {
            id: NodeId(0),
            weight: NonZeroU32::MIN,
        };
        let ring = Ring::new(1, vec![member]);
        let saved = vec![
            (key("a"), Some(meta("a"))),
            (key("b"), Some(meta("b"))),
            (key("a"), Some(meta("a"))),
        ];
        let mut node = Node::recover(NodeId(0), ring, config(2), Vec::new(), saved);
        for (id, name) in ["a", "b"].into_iter().enumerate() {
            let read = Read::Object {
                request: Request::head(key(name)),
                stale: None,
                direct: false,
            };
            node.on_request(Time(1), GatewayRequestId(id as u64), read);
            let actions = node.drain();
            assert!(
                matches!(actions.as_slice(), [Action::Respond { head, .. }] if head.status == 200),
                "{name}: {actions:?}"
            );
        }
    }
}
