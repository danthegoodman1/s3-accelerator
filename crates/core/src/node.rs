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
    pub default_policy: BucketPolicy,
    pub buckets: BTreeMap<String, BucketPolicy>,
}

/// What the slot table records about a stored block: enough to put it back
/// in the index after a restart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotRecord {
    pub key: ObjectKey,
    pub etag: ETag,
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
    /// Send `request` to S3.
    Fetch {
        origin: OriginRequestId,
        request: Request,
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
    }
}

/// A stored block, as the node's index describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlock<'a> {
    pub key: &'a ObjectKey,
    pub etag: &'a ETag,
    pub index: u64,
    pub location: Location,
    pub len: u64,
}

pub struct Node {
    id: NodeId,
    /// The latest time an input carried.
    now: Time,
    ring: Ring,
    config: Config,
    objects: BTreeMap<ObjectKey, Object>,
    /// Known objects by when they were last used, oldest first.
    recency: BTreeMap<u64, ObjectKey>,
    next_use: u64,
    versions: BTreeMap<(ObjectKey, ETag), Version>,
    version_names: BTreeMap<VersionId, (ObjectKey, ETag)>,
    next_version: u64,
    store: Store,
    doorkeeper: Doorkeeper,
    origins: BTreeMap<OriginRequestId, OriginRequest>,
    next_origin: u64,
    /// Blocks whose bytes are in, or on their way in, an S3 response body.
    in_flight: BTreeMap<BlockKey, OriginRequestId>,
    /// Slots being written, and the response bodies they copy from.
    writes: BTreeMap<Location, OriginRequestId>,
    /// Keys whose blocks the node recovered at startup and whose metadata
    /// it has not fetched since.
    recovered: BTreeSet<ObjectKey>,
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

#[derive(Clone, Debug)]
struct Meta {
    etag: ETag,
    size: u64,
    headers: Vec<(String, String)>,
}

struct Version {
    id: VersionId,
    /// Stored and in-flight blocks that name this version.
    refs: u64,
}

struct OriginRequest {
    purpose: Purpose,
    method: Method,
    sent: Time,
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
            store: Store::new(config.store),
            doorkeeper: Doorkeeper::new(config.doorkeeper_window),
            config,
            objects: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_use: 0,
            versions: BTreeMap::new(),
            version_names: BTreeMap::new(),
            next_version: 0,
            origins: BTreeMap::new(),
            next_origin: 0,
            in_flight: BTreeMap::new(),
            writes: BTreeMap::new(),
            recovered: BTreeSet::new(),
            verifying: BTreeMap::new(),
            filling_bytes: 0,
            waiting: BTreeMap::new(),
            sending: BTreeMap::new(),
            stats: Stats::default(),
            actions: Vec::new(),
        }
    }

    /// A node restarting over the slot table's records. It serves trusted
    /// blocks as they are and verifies each other block on its first read.
    /// Records that no slot of this store can hold, or that clash with
    /// others, are cleared.
    pub fn recover(
        id: NodeId,
        ring: Ring,
        config: Config,
        records: impl IntoIterator<Item = Recovered>,
    ) -> Node {
        let mut node = Node::new(id, ring, config);
        let block_size = node.config.layout.block_size();
        for recovered in records {
            let Recovered {
                location,
                record,
                checksum,
                trusted,
            } = recovered;
            let version = node.version(&record.key, &record.etag);
            let block = BlockKey {
                version,
                index: record.index,
            };
            let hash = block_hash(&record.key, &record.etag, block_size, record.index);
            let verify = (!trusted).then_some(checksum);
            let restored = record.len <= block_size
                && node
                    .store
                    .restore(block, location, record.len, hash, record.placement, verify);
            if restored {
                node.refer(version);
                node.recovered.insert(record.key);
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
    pub fn stored_blocks(&self) -> impl Iterator<Item = StoredBlock<'_>> {
        self.store
            .blocks()
            .filter(|(_, entry)| entry.state == BlockState::Ready && entry.verify.is_none())
            .map(|(block, entry)| {
                let (key, etag) = &self.version_names[&block.version];
                StoredBlock {
                    key,
                    etag,
                    index: block.index,
                    location: entry.location,
                    len: entry.len,
                }
            })
    }

    /// The block stored at `location`, if the node would serve it without
    /// verifying it first.
    pub fn stored_block_at(&self, location: Location) -> Option<StoredBlock<'_>> {
        let block = self.store.block_at(location)?;
        let entry = self.store.get(&block)?;
        if entry.state != BlockState::Ready || entry.verify.is_some() {
            return None;
        }
        let (key, etag) = &self.version_names[&block.version];
        Some(StoredBlock {
            key,
            etag,
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
                    "{origin:?} answered {} cancelled {} readers {} waiters {}",
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
        let (key, etag) = self.version_names[&block.version].clone();
        SlotRecord {
            key,
            etag,
            index: block.index,
            len: entry.len,
            placement: entry.placement,
        }
    }

    fn holds_blocks_of(&self, key: &ObjectKey) -> bool {
        self.versions
            .range((key.clone(), ETag(String::new()))..)
            .next()
            .is_some_and(|((held, _), _)| held == key)
    }

    fn is_home(&self, key: &ObjectKey) -> bool {
        self.ring.owner(Placement::Home(key).hash()) == Some(self.id)
    }

    /// The `GetObject` or `HeadObject` a home serves.
    fn object_request(&self, id: GatewayRequestId) -> &Request {
        match &self.waiting[&id].read {
            Read::Object { request, .. } => request,
            Read::Range(_) => unreachable!("only object reads need metadata"),
        }
    }

    pub fn on_origin_response(&mut self, now: Time, origin: OriginRequestId, head: ResponseHead) {
        self.now = self.now.max(now);
        let Some(request) = self.origins.get_mut(&origin) else {
            return;
        };
        request.answered = true;
        match &request.purpose {
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

    /// Time passed: S3 requests unanswered past the timeout are abandoned,
    /// and whatever waited on them proceeds as if S3 failed with 503.
    pub fn on_tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        let timeout = self.config.origin_timeout;
        let expired: Vec<OriginRequestId> = self
            .origins
            .iter()
            .filter(|(_, request)| !request.answered && request.sent.0 + timeout <= now.0)
            .map(|(&origin, _)| origin)
            .collect();
        for origin in expired {
            self.origins
                .get_mut(&origin)
                .expect("expired request")
                .cancelled = true;
            self.actions.push(Action::Cancel { origin });
            self.on_origin_response(now, origin, ResponseHead::status(503));
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
        self.store.remove(block);
        self.filling_bytes -= len;
        self.unref(block.version);
        // Later reads fill the block again instead of reading this body.
        if self.in_flight.get(&block) == Some(&origin) {
            self.in_flight.remove(&block);
            self.unref(block.version);
        }
        self.stop_reading(origin);
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
            if self.recovered.remove(&key) && self.holds_blocks_of(&key) {
                return self.first_fetch(now, id, Request::head(key), Vec::new(), false);
            }
        }
        match self.objects.get_mut(&key) {
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
                    Read::Range(_) => false,
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
        let body_start = match &purpose {
            Purpose::Fill { .. } => match request.range {
                Some(ByteRange::Inclusive { first, .. }) => first,
                _ => unreachable!("fills ask for inclusive ranges"),
            },
            _ => 0,
        };
        self.origins.insert(
            origin,
            OriginRequest {
                purpose,
                method: request.method,
                sent: self.now,
                answered: false,
                cancelled: false,
                body_start,
                readers: 0,
                blocks: Vec::new(),
                waiters: Vec::new(),
            },
        );
        self.stats.origin_requests += 1;
        self.actions.push(Action::Fetch { origin, request });
        origin
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
        {
            self.store_first_fetch(origin, &key, &meta, &head);
        }
        self.resume(now, waiters, sent, &head, has_meta);
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

    /// Makes the whole blocks in a first fetch's body available to other
    /// readers, and stores those the admission policy accepts.
    fn store_first_fetch(
        &mut self,
        origin: OriginRequestId,
        key: &ObjectKey,
        meta: &Meta,
        head: &ResponseHead,
    ) {
        let (first, last) = match head.content_range {
            Some(range) => (range.first, range.last),
            None if meta.size > 0 => (0, meta.size - 1),
            None => return,
        };
        self.origins
            .get_mut(&origin)
            .expect("answered origin")
            .body_start = first;
        let version = self.version(key, &meta.etag);
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
            self.track_in_flight(block, origin);
            if let Some(location) = self.admit(key, &meta.etag, meta.size, block) {
                self.write(location, origin, span.start - first, span.end - span.start);
            }
        }
        self.forget_if_unused(version);
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
        let mut awaiting = BTreeSet::new();
        let layout = self.config.layout;
        let blocks: Vec<u64> = layout.blocks_covering(first, last).collect();
        let mut next = 0;
        while next < blocks.len() {
            let index = blocks[next];
            let block = BlockKey { version, index };
            let span = layout.block_span(size, index);
            let piece = span.start.max(first)..span.end.min(last + 1);
            let len = piece.end - piece.start;
            let ready = self
                .store
                .get(&block)
                .filter(|entry| entry.state == BlockState::Ready)
                .map(|entry| (entry.location, entry.verify.is_some()));
            if let Some((location, unverified)) = ready {
                self.store.hit(block);
                self.store.pin(block);
                holds.pins.push(block);
                let offset = piece.start - span.start;
                body.push(Segment::Slot {
                    location,
                    offset,
                    len,
                });
                if unverified {
                    awaiting.insert(Await::Verify(location));
                }
                next += 1;
                continue;
            }
            let origin = match self.in_flight.get(&block) {
                Some(&origin) => origin,
                None => {
                    // Fetch this block and every missing block after it in one range GET.
                    let run_end = blocks[next..]
                        .iter()
                        .take_while(|&&index| {
                            let block = BlockKey { version, index };
                            self.store.get(&block).is_none() && !self.in_flight.contains_key(&block)
                        })
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
        let origin = self.fetch(purpose, request);
        let mut stored = Vec::new();
        for index in run {
            let block = BlockKey { version, index };
            self.track_in_flight(block, origin);
            if let Some(location) = self.admit(key, etag, size, block) {
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
        let (_, etag) = &self.version_names[&version];
        let expected = Some((request.body_start, *last_byte));
        let valid = head.status == 206
            && head.etag.as_ref() == Some(etag)
            && head.content_range.map(|range| (range.first, range.last)) == expected;
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
        let (key, etag) = self.version_names[&version].clone();
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
                Segment::Origin { len, .. } => self.stats.miss_bytes += len,
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

    /// Reserves a slot if the admission policy stores this block.
    fn admit(
        &mut self,
        key: &ObjectKey,
        etag: &ETag,
        size: u64,
        block: BlockKey,
    ) -> Option<Location> {
        let layout = self.config.layout;
        let placement = layout.placement(key, size, block.index).hash();
        if self.ring.owner(placement) != Some(self.id) {
            return None;
        }
        let hash = block_hash(key, etag, layout.block_size(), block.index);
        if !self.policy(&key.bucket).admit_on_first_read && !self.doorkeeper.contains(hash) {
            self.doorkeeper.insert(hash);
            return None;
        }
        let span = layout.block_span(size, block.index);
        let len = span.end - span.start;
        if self.filling_bytes + len > self.config.fill_budget {
            return None;
        }
        let location = self.store.reserve(block, len, hash, placement);
        for (evicted, location) in self.store.drain_evicted() {
            self.stats.evicted_blocks += 1;
            self.actions.push(Action::Clear { location });
            self.unref(evicted.version);
        }
        let location = location?;
        self.filling_bytes += len;
        self.refer(block.version);
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
        let name = (key.clone(), etag.clone());
        if let Some(version) = self.versions.get(&name) {
            return version.id;
        }
        let id = VersionId(self.next_version);
        self.next_version += 1;
        self.version_names.insert(id, name.clone());
        self.versions.insert(name, Version { id, refs: 0 });
        id
    }

    fn refer(&mut self, id: VersionId) {
        let name = &self.version_names[&id];
        self.versions.get_mut(name).expect("named version").refs += 1;
    }

    fn unref(&mut self, id: VersionId) {
        let name = &self.version_names[&id];
        let version = self.versions.get_mut(name).expect("named version");
        version.refs -= 1;
        self.forget_if_unused(id);
    }

    fn forget_if_unused(&mut self, id: VersionId) {
        let name = &self.version_names[&id];
        if self.versions[name].refs == 0 {
            let name = self.version_names.remove(&id).expect("named version");
            self.versions.remove(&name);
        }
    }

    /// Records metadata the home just validated, then drops the least
    /// recently used metadata past capacity.
    fn know(&mut self, key: ObjectKey, meta: Meta, validated: Time) {
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

/// A block's identity: bucket, key, ETag, block size and index.
fn block_hash(key: &ObjectKey, etag: &ETag, block_size: u64, index: u64) -> u64 {
    let mut bytes = Vec::with_capacity(key.bucket.len() + key.key.len() + etag.0.len() + 28);
    for part in [&key.bucket, &key.key, &etag.0] {
        bytes.extend_from_slice(&(part.len() as u32).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes.extend_from_slice(&block_size.to_le_bytes());
    bytes.extend_from_slice(&index.to_le_bytes());
    xxh3_64(&bytes)
}
