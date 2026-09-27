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
use crate::formats::Format;
use crate::layout::Layout;
use crate::placement::{NodeId, Placement, PlacementHash, Ring};
use crate::s3::{
    Answer, ByteRange, ContentRange, ETag, Method, ObjectKey, Request, ResponseHead, answer,
    preconditions,
};
use crate::store::{BlockKey, BlockState, ClassUsage, Location, Store, StoreConfig, VersionId};
use std::collections::{BTreeMap, BTreeSet};
use xxhash_rust::xxh3::xxh3_64;

/// A request from a gateway, numbered by the node's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GatewayRequestId(pub u64);

/// A request the node sent to S3, numbered by the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginRequestId(pub u64);

/// A hot placement's owner and replicas, which gateways spread its reads
/// across until `until`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotHint {
    pub placement: PlacementHash,
    pub nodes: Vec<NodeId>,
    pub until: Time,
}

/// A message from S3's event queue, numbered by the node's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(pub u64);

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
    /// Store the home's region of each upload that passes through the
    /// home, so the first read hits.
    pub warm_on_write: bool,
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
    /// A placement its owner reads `hot_threshold` times within
    /// `hot_window` milliseconds is hot: the owner leases it to its next
    /// `hot_replicas` rendezvous candidates for `lease` milliseconds, and
    /// gateways spread its reads across them. A threshold of 0 leases
    /// nothing.
    pub hot_threshold: u64,
    pub hot_window: u64,
    pub hot_replicas: usize,
    pub lease: u64,
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
    /// Hints name the hot placements the request read.
    Respond {
        request: GatewayRequestId,
        head: ResponseHead,
        body: Vec<Segment>,
        meta: Option<ObjectMeta>,
        hot: Vec<HotHint>,
    },
    /// The request reaches past the blocks the home holds: answer the
    /// gateway with the metadata, and it reads the blocks from their owners.
    Metadata {
        request: GatewayRequestId,
        meta: ObjectMeta,
        hot: Vec<HotHint>,
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
    /// Replace the metadata file with `entries`, least recently used first,
    /// so a start keeps the most recently used up to capacity.
    RewriteMetadata { entries: Vec<(ObjectKey, Meta)> },
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
    /// Tell `node` of a write to `key` that passed through a gateway, with
    /// `on_write_from` and `passed_on` set.
    PassWrite { node: NodeId, key: ObjectKey },
    /// Tell `node` of S3's event that `key` changed to `etag`, or went
    /// away for `None`, with `on_event_notice`; then call
    /// `on_event_passed` with whether it heard.
    PassEvent {
        event: EventId,
        node: NodeId,
        key: ObjectKey,
        etag: Option<ETag>,
    },
    /// Every home of the event's key has it: delete its message from the
    /// queue.
    EventDone { event: EventId },
    /// Lease `placement` to `node` until `until`, with `on_lease`.
    GrantLease {
        node: NodeId,
        placement: PlacementHash,
        until: Time,
    },
    /// Tell the owner of a leased placement how many reads the lease
    /// served, with `on_lease_report`.
    ReportLease {
        node: NodeId,
        placement: PlacementHash,
        reads: u64,
    },
    /// Read the bytes of the slots `parts` name, each a location, an offset
    /// into it and a length, and pass them, in order, to `on_spot`; or
    /// pass nothing if the read fails.
    ReadSpot {
        version: VersionId,
        parts: Vec<(Location, u64, u64)>,
    },
    /// Erase the `len` bytes of the slot at `location`, whose block a
    /// purge dropped.
    Erase { location: Location, len: u64 },
    /// Record durably that `key`'s purge waits on `nodes` to confirm, or on
    /// none once all have.
    SavePurge { key: ObjectKey, nodes: Vec<NodeId> },
    /// Tell `node` to purge `key`, with `on_purge` and `passed_on` set, and
    /// call `on_purge_confirmed` once it confirms.
    PassPurge { node: NodeId, key: ObjectKey },
}

/// S3's event that `key` changed to `etag`, as a node passes it to the
/// key's other homes.
struct PassedEvent {
    key: ObjectKey,
    etag: Option<ETag>,
    /// Homes told of it, and those yet to hear.
    told: BTreeSet<NodeId>,
    waiting: BTreeSet<NodeId>,
    /// When the node stops waiting, leaving the queue to offer the event
    /// again.
    until: Time,
}

/// What a node has done since it started. Its owner reads these for
/// metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Reads from gateways, and from nodes that took over placements.
    pub reads: u64,
    /// Blocks the node's responses read from the store, and from S3's or a
    /// previous owner's responses.
    pub block_hits: u64,
    pub blocks_fetched: u64,
    /// Blocks an owner or leased replica fetched for a read and could
    /// store, by what it remembers of them: the ghost queue holds them, the
    /// doorkeeper turned them away, or neither.
    pub misses_evicted: u64,
    pub misses_unadmitted: u64,
    pub misses_new: u64,
    /// The same blocks: stored, or turned away by the doorkeeper, the fill
    /// budget, or a store whose eviction candidates are all in use.
    pub admitted: u64,
    pub refused_doorkeeper: u64,
    pub refused_budget: u64,
    pub refused_full: u64,
    /// Blocks the store let go, each by one cause: evicted cold or
    /// disowned, purged, failing their checksums, or left unfilled by a
    /// fill that ended without them.
    pub evicted_blocks: u64,
    pub disowned_blocks: u64,
    pub purged_blocks: u64,
    pub corrupt_dropped: u64,
    pub unfilled_blocks: u64,
    /// Body bytes served from stored blocks, from S3's responses, and from
    /// previous owners' blocks.
    pub hit_bytes: u64,
    pub miss_bytes: u64,
    pub peer_bytes: u64,
    pub origin_requests: u64,
    pub written_bytes: u64,
    /// Recovered blocks checked against their checksums, and those that
    /// failed.
    pub verified_blocks: u64,
    pub corrupt_blocks: u64,
    /// Reads sent to previous owners, those that went unanswered, and
    /// objects whose metadata a previous home supplied.
    pub peer_requests: u64,
    pub peer_timeouts: u64,
    pub peer_metadata: u64,
    /// Reads served under a lease, and leases granted.
    pub leased_reads: u64,
    pub leases_granted: u64,
    /// Uploads whose blocks the home stored as they passed through.
    pub warmed_uploads: u64,
    /// Blocks of metadata the home filled before a reader asked.
    pub prefetched_blocks: u64,
    /// Purges the node carried out.
    pub purges: u64,
}

/// What a node holds now. Its owner reads these for metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Usage {
    /// Bytes the store holds when full.
    pub capacity: u64,
    /// Bytes of stored blocks that are filling, and the most there may be.
    pub filling_bytes: u64,
    pub fill_budget: u64,
    /// Objects whose metadata the node holds.
    pub objects: u64,
    /// Purges waiting on other nodes to confirm.
    pub pending_purges: u64,
    pub classes: Vec<ClassUsage>,
}

impl std::ops::AddAssign for Stats {
    fn add_assign(&mut self, other: Stats) {
        self.reads += other.reads;
        self.block_hits += other.block_hits;
        self.blocks_fetched += other.blocks_fetched;
        self.misses_evicted += other.misses_evicted;
        self.misses_unadmitted += other.misses_unadmitted;
        self.misses_new += other.misses_new;
        self.admitted += other.admitted;
        self.refused_doorkeeper += other.refused_doorkeeper;
        self.refused_budget += other.refused_budget;
        self.refused_full += other.refused_full;
        self.evicted_blocks += other.evicted_blocks;
        self.disowned_blocks += other.disowned_blocks;
        self.purged_blocks += other.purged_blocks;
        self.corrupt_blocks += other.corrupt_blocks;
        self.corrupt_dropped += other.corrupt_dropped;
        self.unfilled_blocks += other.unfilled_blocks;
        self.hit_bytes += other.hit_bytes;
        self.miss_bytes += other.miss_bytes;
        self.peer_bytes += other.peer_bytes;
        self.origin_requests += other.origin_requests;
        self.written_bytes += other.written_bytes;
        self.verified_blocks += other.verified_blocks;
        self.peer_requests += other.peer_requests;
        self.peer_timeouts += other.peer_timeouts;
        self.peer_metadata += other.peer_metadata;
        self.leased_reads += other.leased_reads;
        self.leases_granted += other.leases_granted;
        self.warmed_uploads += other.warmed_uploads;
        self.prefetched_blocks += other.prefetched_blocks;
        self.purges += other.purges;
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
    /// Changes this node learned of lately, from a gateway's write, S3's
    /// answer or S3's event: metadata validated before one, a previous
    /// home's or S3's, is stale.
    written: BTreeMap<ObjectKey, Time>,
    /// When this node restarted, if it ran before: it forgot the changes
    /// it had learned of, so a previous home's metadata validated before
    /// then may be stale.
    restarted: Option<Time>,
    /// Events passed to other homes, until each hears.
    events: BTreeMap<EventId, PassedEvent>,
    /// Reads of each placement this node owns in its current hot window:
    /// when the window began, and how many.
    read_counts: BTreeMap<PlacementHash, (Time, u64)>,
    /// Hot placements this node owns and leases out, and leases it holds.
    hot: BTreeMap<PlacementHash, Hot>,
    leases: BTreeMap<PlacementHash, Lease>,
    /// Hints for reads in progress, which their answers carry.
    hints: BTreeMap<GatewayRequestId, Vec<HotHint>>,
    /// Versions whose spot the node read, and the blocks it pins while the
    /// read is in progress.
    inspected: BTreeSet<VersionId>,
    spots: BTreeMap<VersionId, Vec<BlockKey>>,
    /// Purges this node coordinates: the nodes yet to confirm each, and
    /// when to tell them again.
    purges: BTreeMap<ObjectKey, (BTreeSet<NodeId>, Time)>,
    config: Config,
    objects: BTreeMap<ObjectKey, Object>,
    /// Known objects by when they were last used, oldest first.
    recency: BTreeMap<u64, ObjectKey>,
    next_use: u64,
    /// Entries the metadata file holds, which the home rewrites past twice
    /// the metadata capacity.
    saved_entries: usize,
    versions: BTreeMap<VersionId, Version>,
    store: Store,
    doorkeeper: Doorkeeper,
    origins: BTreeMap<OriginRequestId, OriginRequest>,
    next_origin: u64,
    /// Blocks whose bytes are in, or on their way in, an S3 response body.
    in_flight: BTreeMap<BlockKey, OriginRequestId>,
    /// Slots being written, and the response bodies they copy from.
    writes: BTreeMap<Location, OriginRequestId>,
    /// Slots being written whose bytes are in the slab file, readable
    /// though not yet durable.
    readable: BTreeSet<Location>,
    /// Requests that read slots a first fetch is writing.
    awaiting_writes: BTreeMap<Location, Vec<GatewayRequestId>>,
    /// Blocks whose write failed after reads of their bytes began: new
    /// reads fetch them again, and the slot is freed once the last read
    /// lets go.
    unwritten: BTreeSet<BlockKey>,
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

/// A hot placement this node owns: the replicas it leased it to, when their
/// leases run out, when it granted them, its own reads since, the reads
/// replicas reported over the lease's first half, and whether it let the
/// leases end.
struct Hot {
    replicas: Vec<NodeId>,
    until: Time,
    granted: Time,
    reads: u64,
    replica_reads: u64,
    ending: bool,
}

/// A lease this node holds: the placement's owner, when the lease runs
/// out, the reads it served since its last report, and whether it
/// reported this term.
struct Lease {
    owner: NodeId,
    until: Time,
    reads: u64,
    reported: bool,
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
        size: u64,
        last_byte: u64,
        stored: Vec<(BlockKey, Location)>,
        /// The blocks skipped the doorkeeper because a previous owner was
        /// asked for them; if the fill goes to S3, they pass it after all.
        provisional: bool,
    },
    /// Metadata asked of the object's previous home, for the request that
    /// found none.
    PeerMeta {
        key: ObjectKey,
        request: GatewayRequestId,
        sent: Time,
    },
    /// The home's region of an upload this node passed to S3, which its
    /// owner kept: version `etag` of `size` bytes, read by offset from the
    /// object's start. S3 never answers it.
    Kept {
        key: ObjectKey,
        etag: ETag,
        size: u64,
    },
    /// A HEAD that checks S3 still holds the version a kept upload wrote.
    WarmCheck {
        kept: OriginRequestId,
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
    /// Blocks the body reads from fetches, which count once it is sent.
    fetched: u64,
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
            restarted: None,
            events: BTreeMap::new(),
            read_counts: BTreeMap::new(),
            hot: BTreeMap::new(),
            leases: BTreeMap::new(),
            hints: BTreeMap::new(),
            inspected: BTreeSet::new(),
            spots: BTreeMap::new(),
            purges: BTreeMap::new(),
            store: Store::new(config.store),
            doorkeeper: Doorkeeper::new(config.doorkeeper_window),
            config,
            objects: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_use: 0,
            saved_entries: 0,
            versions: BTreeMap::new(),

            origins: BTreeMap::new(),
            next_origin: 0,
            in_flight: BTreeMap::new(),
            writes: BTreeMap::new(),
            readable: BTreeSet::new(),
            awaiting_writes: BTreeMap::new(),
            unwritten: BTreeSet::new(),
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
        purges: impl IntoIterator<Item = (ObjectKey, Vec<NodeId>)>,
    ) -> Node {
        let mut node = Node::new(id, ring, config);
        // The last record of each purge holds the nodes it still waits on;
        // the first tick tells them again.
        for (key, nodes) in purges {
            match nodes.is_empty() {
                true => node.purges.remove(&key),
                false => node
                    .purges
                    .insert(key, (nodes.into_iter().collect(), Time::default())),
            };
        }
        for (key, meta) in metadata {
            node.saved_entries += 1;
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

    /// The node ran before and restarted at `now`. It forgot the changes it
    /// had learned of, so it uses no metadata a previous home validated
    /// before now.
    pub fn restarted(&mut self, now: Time) {
        self.now = self.now.max(now);
        self.restarted = Some(now);
    }

    pub fn usage(&self) -> Usage {
        let store = self.config.store;
        Usage {
            capacity: u64::from(store.extents) * store.extent_size,
            filling_bytes: self.filling_bytes,
            fill_budget: self.config.fill_budget,
            objects: self.recency.len() as u64,
            pending_purges: self.purges.len() as u64,
            classes: self.store.usage(),
        }
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
        self.note_reads(now, id, &read);
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

    /// Answers a new owner's read from stored blocks, readable or durable,
    /// or with 404 if any is missing or unverified. The blocks gain no hits,
    /// since their new owner will hold them.
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
                Some(entry)
                    if (entry.state == BlockState::Ready && entry.verify.is_none())
                        || (self.readable.contains(&entry.location)
                            && !self.unwritten.contains(&block)) =>
                {
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
                let hot = self.hints.remove(&id).unwrap_or_default();
                self.actions.push(Action::Metadata {
                    request: id,
                    meta,
                    hot,
                });
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
            Purpose::WarmCheck { kept } => {
                let kept = *kept;
                self.warm_checked(origin, kept, head);
            }
            Purpose::Kept { .. } => unreachable!("S3 never answers a kept upload"),
        }
        self.release_if_unread(origin);
    }

    /// The bytes of an upload of `key`, `size` bytes long, that this node's
    /// owner keeps as it passes the body to S3: the home's region, when the
    /// bucket warms on write and this node is the key's home.
    pub fn warm_region(&self, key: &ObjectKey, size: u64) -> Vec<std::ops::Range<u64>> {
        match self.policy(&key.bucket).warm_on_write && self.is_home(key) {
            true => self.config.layout.home_region(size),
            false => Vec::new(),
        }
    }

    /// S3 took an upload of `key` through this node as version `etag` of
    /// `size` bytes, after `on_write`, and the owner kept the bytes
    /// `warm_region` named. Returns the id to hold them under, as a body
    /// read by offset from the object's start, and released once the node
    /// is done: it checks S3 still holds the version with a HEAD, then
    /// stores the blocks and the metadata.
    pub fn on_uploaded(
        &mut self,
        now: Time,
        key: ObjectKey,
        etag: ETag,
        size: u64,
    ) -> Option<OriginRequestId> {
        self.now = self.now.max(now);
        if self.warm_region(&key, size).is_empty() {
            return None;
        }
        let kept = OriginRequestId(self.next_origin);
        self.next_origin += 1;
        let purpose = Purpose::Kept {
            key: key.clone(),
            etag,
            size,
        };
        // The check holds the body until its answer.
        let request = OriginRequest {
            purpose,
            method: Method::Get,
            sent: now,
            timeout: self.config.origin_timeout,
            peer: None,
            answered: true,
            cancelled: false,
            body_start: 0,
            readers: 1,
            blocks: Vec::new(),
            waiters: Vec::new(),
        };
        self.origins.insert(kept, request);
        self.fetch(Purpose::WarmCheck { kept }, Request::head(key));
        Some(kept)
    }

    /// S3 answered the HEAD for a kept upload. If it still holds the
    /// upload's version, the home stores the region's blocks from the kept
    /// bytes, and keeps the metadata if it has none: any it has came from
    /// S3 since the write, and a change it learned of since the HEAD went
    /// out makes the HEAD's stale.
    fn warm_checked(&mut self, check: OriginRequestId, kept: OriginRequestId, head: ResponseHead) {
        let sent = self.origins[&check].sent;
        let Some(OriginRequest {
            purpose: Purpose::Kept { key, etag, size },
            ..
        }) = self.origins.get(&kept)
        else {
            unreachable!("a check names its kept upload");
        };
        let (key, etag, size) = (key.clone(), etag.clone(), *size);
        let current =
            head.status == 200 && head.etag.as_ref() == Some(&etag) && head.content_length == size;
        if current && self.is_home(&key) {
            let unchanged = self
                .written
                .get(&key)
                .is_none_or(|changed| *changed <= sent);
            if unchanged && !self.objects.contains_key(&key) {
                let meta = Meta {
                    etag: etag.clone(),
                    size,
                    headers: head.headers.clone(),
                };
                self.know(key.clone(), meta, sent);
            }
            // The version stays while the loop runs, though a reservation
            // may evict its other blocks.
            let version = self.version(&key, &etag);
            self.refer(version);
            let layout = self.config.layout;
            for range in layout.home_region(size) {
                for index in layout.blocks_covering(range.start, range.end - 1) {
                    let block = BlockKey { version, index };
                    if self.store.get(&block).is_some() || self.in_flight.contains_key(&block) {
                        continue;
                    }
                    if let Some(location) = self.admit(&key, size, block, true, false) {
                        let span = layout.block_span(size, index);
                        self.track_in_flight(block, kept);
                        self.write(location, kept, span.start, span.end - span.start);
                    }
                }
            }
            self.unref(version);
            self.stats.warmed_uploads += 1;
        }
        self.stop_reading(kept);
    }

    /// A write to `key` passed through this node and succeeded: its
    /// metadata no longer holds.
    pub fn on_write(&mut self, now: Time, key: &ObjectKey) {
        self.on_write_from(now, key, false);
    }

    /// As `on_write`, for a write another node passed on, which goes no
    /// further. Otherwise the node passes the write to the key's home when
    /// that is another node, as when a gateway's ring differs from this
    /// node's, and within the fallback window to the key's home under the
    /// previous ring too, which may hold or ask for the metadata.
    pub fn on_write_from(&mut self, now: Time, key: &ObjectKey, passed_on: bool) {
        self.now = self.now.max(now);
        if !passed_on {
            for home in self.other_homes(key) {
                let key = key.clone();
                self.actions.push(Action::PassWrite { node: home, key });
            }
        }
        self.invalidate(now, key);
    }

    /// S3's event queue delivered `event`: `key` changed to `etag`, or went
    /// away for `None`. The node applies the event, passes it to the key's
    /// other homes, and says once every home has it. A late or repeated
    /// event does no harm: at worst it drops current metadata.
    pub fn on_event(&mut self, now: Time, event: EventId, key: ObjectKey, etag: Option<ETag>) {
        self.now = self.now.max(now);
        self.change_to(now, &key, etag.as_ref());
        let homes = self.other_homes(&key);
        if homes.is_empty() {
            return self.actions.push(Action::EventDone { event });
        }
        let passed = PassedEvent {
            key,
            etag,
            told: BTreeSet::new(),
            waiting: BTreeSet::new(),
            until: Time(now.0 + self.config.peer_timeout),
        };
        self.events.insert(event, passed);
        self.tell_homes(event);
    }

    /// Passes `event` to the key's other homes that have not been told,
    /// such as a home a ring adopted since the event arrived.
    fn tell_homes(&mut self, event: EventId) {
        let homes = self.other_homes(&self.events[&event].key);
        let passed = self.events.get_mut(&event).expect("a passed event");
        let untold: Vec<NodeId> = homes.difference(&passed.told).copied().collect();
        if untold.is_empty() {
            return;
        }
        passed.until = Time(self.now.0 + self.config.peer_timeout);
        for node in untold {
            passed.told.insert(node);
            passed.waiting.insert(node);
            self.actions.push(Action::PassEvent {
                event,
                node,
                key: passed.key.clone(),
                etag: passed.etag.clone(),
            });
        }
    }

    /// Another node passed on S3's event that `key` changed to `etag`, or
    /// went away for `None`.
    pub fn on_event_notice(&mut self, now: Time, key: &ObjectKey, etag: Option<&ETag>) {
        self.now = self.now.max(now);
        self.change_to(now, key, etag);
    }

    /// `node` heard of `event`, or failed to, which leaves the event to
    /// the queue.
    pub fn on_event_passed(&mut self, now: Time, event: EventId, node: NodeId, heard: bool) {
        self.now = self.now.max(now);
        let Some(passed) = self.events.get_mut(&event) else {
            return;
        };
        passed.waiting.remove(&node);
        if !heard {
            self.events.remove(&event);
        } else if passed.waiting.is_empty() {
            self.events.remove(&event);
            self.actions.push(Action::EventDone { event });
        }
    }

    /// Purges `key`: drops its metadata and every block of every version
    /// of it, erasing their slots, and drops blocks being written or read
    /// once free. A node that `passed_on` is unset for coordinates the
    /// purge: it has every other node in its rings purge the key, records
    /// durably which have yet to confirm, and tells them again until each
    /// does. The owner confirms once the actions are durable.
    pub fn on_purge(&mut self, now: Time, key: &ObjectKey, passed_on: bool) {
        self.now = self.now.max(now);
        self.stats.purges += 1;
        self.invalidate(now, key);
        let versions: Vec<VersionId> = self
            .versions
            .range(VersionId::all_of(VersionId::key_hash(key)))
            .map(|(&version, _)| version)
            .collect();
        for version in versions {
            for block in self.store.blocks_of(version) {
                let Some(entry) = self.store.get(&block) else {
                    continue;
                };
                // The record goes now, so the purge is durable once the
                // table is, even for a block that is still being read.
                if entry.state == BlockState::Ready {
                    let location = entry.location;
                    self.actions.push(Action::Clear { location });
                }
                self.store.purge(block);
                self.drop_purged(block);
            }
        }
        if passed_on {
            return;
        }
        let previous = self.previous.iter().flat_map(|(ring, _)| ring.members());
        let nodes: BTreeSet<NodeId> = self
            .ring
            .members()
            .iter()
            .chain(previous)
            .map(|member| member.id)
            .filter(|&node| node != self.id)
            .collect();
        if nodes.is_empty() {
            return;
        }
        let saved = nodes.iter().copied().collect();
        self.actions.push(Action::SavePurge {
            key: key.clone(),
            nodes: saved,
        });
        self.pass_purge(key, &nodes);
        let again = Time(now.0 + self.purge_retry());
        self.purges.insert(key.clone(), (nodes, again));
    }

    /// `node` confirmed it purged `key`, durably.
    pub fn on_purge_confirmed(&mut self, now: Time, key: &ObjectKey, node: NodeId) {
        self.now = self.now.max(now);
        let Some((nodes, _)) = self.purges.get_mut(key) else {
            return;
        };
        if !nodes.remove(&node) {
            return;
        }
        let saved = nodes.iter().copied().collect();
        if nodes.is_empty() {
            self.purges.remove(key);
        }
        self.actions.push(Action::SavePurge {
            key: key.clone(),
            nodes: saved,
        });
    }

    /// Purges this node coordinates, and the nodes each waits on.
    pub fn pending_purges(&self) -> Vec<(ObjectKey, Vec<NodeId>)> {
        self.purges
            .iter()
            .map(|(key, (nodes, _))| (key.clone(), nodes.iter().copied().collect()))
            .collect()
    }

    /// A coordinator tells the nodes of its rings again of each purge they
    /// have yet to confirm, every few peer timeouts. A node out of every
    /// ring waits until it is back, with the blocks it kept.
    fn tick_purges(&mut self, now: Time) {
        let due: Vec<(ObjectKey, BTreeSet<NodeId>)> = self
            .purges
            .iter()
            .filter(|(_, (_, again))| *again <= now)
            .map(|(key, (nodes, _))| {
                let nodes = nodes
                    .iter()
                    .copied()
                    .filter(|&node| self.in_rings(node))
                    .collect();
                (key.clone(), nodes)
            })
            .collect();
        let again = Time(now.0 + self.purge_retry());
        for (key, nodes) in due {
            self.purges.get_mut(&key).expect("due purge").1 = again;
            self.pass_purge(&key, &nodes);
        }
    }

    /// Whether `node` is in this node's ring, or its previous one.
    fn in_rings(&self, node: NodeId) -> bool {
        let previous = self.previous.iter().flat_map(|(ring, _)| ring.members());
        self.ring
            .members()
            .iter()
            .chain(previous)
            .any(|member| member.id == node)
    }

    fn pass_purge(&mut self, key: &ObjectKey, nodes: &BTreeSet<NodeId>) {
        for &node in nodes {
            let key = key.clone();
            self.actions.push(Action::PassPurge { node, key });
        }
    }

    /// How long a coordinator waits before telling nodes of a purge again.
    fn purge_retry(&self) -> u64 {
        4 * self.config.peer_timeout.max(1)
    }

    /// Frees a purged block's slot and erases it, once the block is no
    /// longer being written or read.
    fn drop_purged(&mut self, block: BlockKey) {
        let Some(entry) = self.store.get(&block) else {
            return;
        };
        if !entry.purged() || entry.state != BlockState::Ready || entry.pinned() {
            return;
        }
        let (location, len) = (entry.location, self.config.store.slot_size(entry.len));
        self.store.remove(block);
        self.stats.purged_blocks += 1;
        self.actions.push(Action::Erase { location, len });
        self.unref(block.version);
    }

    /// Unpins a block, and frees it if a purge waited for it.
    fn unpin(&mut self, block: BlockKey) {
        self.store.unpin(block);
        let pinned = self.store.get(&block).is_some_and(|entry| entry.pinned());
        if !pinned && self.unwritten.remove(&block) {
            self.store.remove(block);
            self.unref(block.version);
            return;
        }
        self.drop_purged(block);
    }

    /// The key's home and, while the fallback window lasts, its home under
    /// the previous ring, other than this node.
    fn other_homes(&self, key: &ObjectKey) -> BTreeSet<NodeId> {
        let placement = Placement::Home(key).hash();
        let previous = self.previous.as_ref();
        let homes = [
            self.ring.owner(placement),
            previous.and_then(|(previous, _)| previous.owner(placement)),
        ];
        homes
            .into_iter()
            .flatten()
            .filter(|home| *home != self.id)
            .collect()
    }

    /// S3 says `key` holds `etag`, or nothing for `None`. Metadata of that
    /// version stands; any other goes, as after a write.
    fn change_to(&mut self, now: Time, key: &ObjectKey, etag: Option<&ETag>) {
        if let (Some(etag), Some(Object::Known { meta, .. })) = (etag, self.objects.get(key))
            && meta.etag == *etag
        {
            return;
        }
        self.invalidate(now, key);
    }

    /// `key` changed: its metadata no longer holds, and a first fetch in
    /// flight may predate the change.
    fn invalidate(&mut self, now: Time, key: &ObjectKey) {
        self.changed(key);
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
        // After the key is forgotten, so a rewrite this save starts leaves
        // it out.
        if self.policy(&key.bucket).freshness == Freshness::Immutable {
            let key = key.clone();
            self.save(Action::Forget { key });
        }
    }

    /// `owner` leased `placement` to this node until `until`: the node
    /// admits its blocks, fills them from the owner first, and reports its
    /// reads before the lease runs out.
    pub fn on_lease(&mut self, now: Time, placement: PlacementHash, owner: NodeId, until: Time) {
        self.now = self.now.max(now);
        let lease = self.leases.entry(placement).or_insert(Lease {
            owner,
            until,
            reads: 0,
            reported: false,
        });
        if until > lease.until {
            (lease.owner, lease.until, lease.reported) = (owner, until, false);
        }
    }

    /// A replica served `reads` reads of `placement` under its lease.
    pub fn on_lease_report(&mut self, now: Time, placement: PlacementHash, reads: u64) {
        self.now = self.now.max(now);
        if let Some(hot) = self.hot.get_mut(&placement) {
            hot.replica_reads += reads;
        }
    }

    /// Counts a gateway's read of each placement it touches: a replica's
    /// under its lease, an owner's toward making the placement hot. The
    /// answer carries hints for the hot ones.
    fn note_reads(&mut self, now: Time, id: GatewayRequestId, read: &Read) {
        let placements: BTreeSet<PlacementHash> = match read {
            Read::Object { request, .. } => BTreeSet::from([Placement::Home(&request.key).hash()]),
            Read::Range(range) if range.first <= range.last && range.last < range.size => {
                let layout = self.config.layout;
                layout
                    .blocks_covering(range.first, range.last)
                    .map(|index| layout.placement(&range.key, range.size, index).hash())
                    .collect()
            }
            _ => BTreeSet::new(),
        };
        let mut hints = Vec::new();
        for placement in placements {
            if let Some(lease) = self.leases.get_mut(&placement)
                && lease.until > now
            {
                lease.reads += 1;
                self.stats.leased_reads += 1;
                continue;
            }
            if self.ring.owner(placement) != Some(self.id) {
                continue;
            }
            if let Some(hot) = self.hot.get_mut(&placement) {
                hot.reads += 1;
            } else if self.config.hot_threshold > 0 {
                let window = self.config.hot_window;
                let (start, count) = self.read_counts.entry(placement).or_insert((now, 0));
                if start.0 + window <= now.0 {
                    (*start, *count) = (now, 0);
                }
                *count += 1;
                if *count >= self.config.hot_threshold {
                    self.read_counts.remove(&placement);
                    self.lease_out(now, placement);
                }
            }
            if let Some(hot) = self.hot.get(&placement) {
                let nodes = std::iter::once(self.id)
                    .chain(hot.replicas.iter().copied())
                    .collect();
                hints.push(HotHint {
                    placement,
                    nodes,
                    until: hot.until,
                });
            }
        }
        if !hints.is_empty() {
            self.hints.insert(id, hints);
        }
    }

    /// Leases a placement this node owns to its next rendezvous
    /// candidates, anew.
    fn lease_out(&mut self, now: Time, placement: PlacementHash) {
        let replicas: Vec<NodeId> = self
            .ring
            .candidates(placement)
            .into_iter()
            .filter(|node| *node != self.id)
            .take(self.config.hot_replicas)
            .collect();
        if replicas.is_empty() {
            self.hot.remove(&placement);
            return;
        }
        let until = Time(now.0 + self.config.lease);
        for &node in &replicas {
            self.stats.leases_granted += 1;
            self.actions.push(Action::GrantLease {
                node,
                placement,
                until,
            });
        }
        let hot = Hot {
            replicas,
            until,
            granted: now,
            reads: 0,
            replica_reads: 0,
            ending: false,
        };
        self.hot.insert(placement, hot);
    }

    /// Whether this node holds a lease on `placement` now.
    fn leased(&self, placement: PlacementHash) -> bool {
        self.leases
            .get(&placement)
            .is_some_and(|lease| lease.until > self.now)
    }

    /// The owner that leased `placement` to this node, while the lease
    /// lasts and the owner answers.
    fn lease_owner(&self, placement: PlacementHash) -> Option<NodeId> {
        let lease = self.leases.get(&placement)?;
        let usable = lease.until > self.now
            && lease.owner != self.id
            && !self.unreachable.contains(&lease.owner);
        usable.then_some(lease.owner)
    }

    /// Owners renew leases on placements that stay busy, three quarters
    /// through the lease, and let the rest run out; replicas report their
    /// reads halfway through, in time for the owner's decision.
    fn tick_leases(&mut self, now: Time) {
        let window = self.config.hot_window;
        self.read_counts
            .retain(|_, (start, _)| start.0 + window > now.0);
        let (half, quarter) = (self.config.lease / 2, self.config.lease / 4);
        let due: Vec<PlacementHash> = self
            .hot
            .iter()
            .filter(|(_, hot)| !hot.ending && now.0 + quarter >= hot.until.0)
            .map(|(&placement, _)| placement)
            .collect();
        for placement in due {
            let hot = &self.hot[&placement];
            let period = now.0.saturating_sub(hot.granted.0).max(1);
            // At least half the promotion rate: the owner's reads over the
            // lease so far, and the replicas' over the half they reported.
            let reported = half.max(1);
            let busy = (hot.reads * reported + hot.replica_reads * period) * window * 2
                >= self.config.hot_threshold * period * reported;
            if busy && self.ring.owner(placement) == Some(self.id) {
                self.lease_out(now, placement);
            } else if let Some(hot) = self.hot.get_mut(&placement) {
                hot.ending = true;
            }
        }
        self.hot.retain(|_, hot| hot.until > now);
        for (&placement, lease) in &mut self.leases {
            if !lease.reported && now.0 + half >= lease.until.0 {
                self.actions.push(Action::ReportLease {
                    node: lease.owner,
                    placement,
                    reads: lease.reads,
                });
                (lease.reads, lease.reported) = (0, true);
            }
        }
        self.leases.retain(|_, lease| lease.until > now);
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The ring the node places blocks by.
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    /// Membership changed the ring. The node keeps the previous one for
    /// the fallback window. Changes that follow while the window lasts,
    /// such as while a joining node hears of the others, extend it and
    /// keep the ring from before the first: its owners held the data.
    pub fn on_ring(&mut self, now: Time, ring: Ring) {
        self.now = self.now.max(now);
        if ring == self.ring {
            return;
        }
        let until = Time(self.now.0 + self.config.fallback_window);
        let replaced = std::mem::replace(&mut self.ring, ring);
        let previous = match self.previous.take() {
            Some((previous, _)) => previous,
            None => replaced,
        };
        self.previous = Some((previous, until));
        self.unreachable.clear();
        self.store.clear_disowned();
        // An event under way is done once the new ring's home hears too.
        let events: Vec<EventId> = self.events.keys().copied().collect();
        for event in events {
            self.tell_homes(event);
        }
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

    /// Notes that `key` changed now, so metadata validated earlier, such as
    /// a previous home's or an S3 answer to a request sent before, is
    /// stale. A change is kept while either may still arrive: through the
    /// fallback window, and while an S3 request sent before it may answer.
    fn changed(&mut self, key: &ObjectKey) {
        let kept = self.config.fallback_window.max(self.config.origin_timeout);
        let horizon = self.now.0.saturating_sub(kept);
        self.written.retain(|_, written| written.0 >= horizon);
        self.written.insert(key.clone(), self.now);
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
    /// previous ring goes once the fallback window ends. An event the
    /// other homes have not all heard of within the peer timeout is left
    /// to the queue, which offers it again.
    pub fn on_tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        self.events.retain(|_, passed| passed.until > now);
        self.tick_leases(now);
        if self
            .previous
            .as_ref()
            .is_some_and(|(_, until)| *until <= self.now)
        {
            self.previous = None;
            // Blocks placed elsewhere now go before any this node owns.
            let (ring, id) = (&self.ring, self.id);
            self.store
                .disown(|placement| ring.owner(placement) == Some(id));
        }
        self.tick_purges(now);
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
            if peer.is_some() {
                self.stats.peer_timeouts += 1;
            }
            self.unreachable.extend(peer);
            match asks_metadata {
                true => self.on_peer_metadata(now, origin, None),
                false => self.on_origin_response(now, origin, ResponseHead::status(503)),
            }
        }
    }

    /// The bytes for the slot at `location` are in the slab file, though
    /// not yet durable: reads waiting for them go ahead, and later reads
    /// read the slot. The block is recorded once the bytes are durable.
    pub fn on_readable(&mut self, location: Location) {
        if !self.writes.contains_key(&location) {
            return;
        }
        self.readable.insert(location);
        for waiter in self.awaiting_writes.remove(&location).unwrap_or_default() {
            self.arrived(waiter, Await::Written(location));
        }
    }

    /// The bytes for the slot at `location` are durable.
    pub fn on_written(&mut self, location: Location) {
        let origin = self
            .writes
            .remove(&location)
            .expect("a write was in progress");
        self.readable.remove(&location);
        let block = self
            .store
            .block_at(location)
            .expect("a written slot holds a block");
        self.store.filled(block);
        let record = self.slot_record(block);
        self.filling_bytes -= record.len;
        self.stats.written_bytes += record.len;
        // A purged block is never recorded, so no restart brings it back.
        let purged = self.store.get(&block).is_some_and(|entry| entry.purged());
        if !purged {
            self.actions.push(Action::Record { location, record });
        }
        if self.in_flight.get(&block) == Some(&origin) {
            self.in_flight.remove(&block);
            self.unref(block.version);
        }
        self.stop_reading(origin);
        for waiter in self.awaiting_writes.remove(&location).unwrap_or_default() {
            self.arrived(waiter, Await::Written(location));
        }
        self.drop_purged(block);
        self.inspect(block.version);
    }

    /// Once the blocks at a stored object's spot are all ready, a home
    /// reads the spot to learn where the object's metadata lies, once per
    /// version, pinning the blocks while the read lasts.
    fn inspect(&mut self, version: VersionId) {
        if self.inspected.contains(&version) {
            return;
        }
        let Some((key, etag)) = self
            .versions
            .get(&version)
            .and_then(|version| version.name.clone())
        else {
            return;
        };
        let Some(format) = Format::of(&key) else {
            return;
        };
        let size = match self.objects.get(&key) {
            Some(Object::Known { meta, .. }) if meta.etag == etag => meta.size,
            _ => return,
        };
        let Some(spot) = format.spot(size) else {
            return;
        };
        let layout = self.config.layout;
        let mut parts = Vec::new();
        let mut blocks = Vec::new();
        for index in layout.blocks_covering(spot.start, spot.end - 1) {
            let block = BlockKey { version, index };
            let Some(entry) = self.store.get(&block) else {
                return;
            };
            if entry.state != BlockState::Ready || entry.verify.is_some() {
                return;
            }
            let span = layout.block_span(size, index);
            let (from, to) = (span.start.max(spot.start), span.end.min(spot.end));
            parts.push((entry.location, from - span.start, to - from));
            blocks.push(block);
        }
        for &block in &blocks {
            self.store.pin(block);
        }
        self.refer(version);
        self.inspected.insert(version);
        self.spots.insert(version, blocks);
        self.actions.push(Action::ReadSpot { version, parts });
    }

    /// The bytes at a version's spot, or none if reading them failed. If
    /// they name the span of the object's metadata, and the home still
    /// keeps that version's metadata, it fills every block of the span it
    /// places and lacks, before the reader asks.
    pub fn on_spot(&mut self, now: Time, version: VersionId, bytes: Vec<u8>) {
        self.now = self.now.max(now);
        let Some(blocks) = self.spots.remove(&version) else {
            return;
        };
        for block in blocks {
            self.unpin(block);
        }
        let prefetch = self.versions.get(&version).and_then(|entry| {
            let (key, etag) = entry.name.clone()?;
            let size = match self.objects.get(&key) {
                Some(Object::Known { meta, .. }) if meta.etag == etag => meta.size,
                _ => return None,
            };
            let span = Format::of(&key)?.metadata(size, &bytes)?;
            Some((key, etag, size, span))
        });
        if let Some((key, etag, size, span)) = prefetch {
            self.prefetch(&key, &etag, size, version, span);
        }
        self.unref(version);
    }

    /// Fills the blocks of `span` this node places and lacks, a run per
    /// placement, skipping the doorkeeper.
    fn prefetch(
        &mut self,
        key: &ObjectKey,
        etag: &ETag,
        size: u64,
        version: VersionId,
        span: std::ops::Range<u64>,
    ) {
        let layout = self.config.layout;
        let blocks: Vec<u64> = layout.blocks_covering(span.start, span.end - 1).collect();
        let mut next = 0;
        while next < blocks.len() {
            let first = blocks[next];
            let placement = layout.placement(key, size, first).hash();
            let run = match self.ring.owner(placement) == Some(self.id) {
                true => self.fill_run(key, size, version, &blocks[next..]),
                false => 0,
            };
            if run == 0 {
                next += 1;
                continue;
            }
            let last = blocks[next + run - 1];
            // A fill the budget would not store fetches bytes nobody reads.
            let bytes = layout.block_span(size, last).end - layout.block_span(size, first).start;
            if self.filling_bytes + bytes <= self.config.fill_budget {
                self.stats.prefetched_blocks += last - first + 1;
                self.fill(key, etag, size, version, first..=last, true);
            }
            next += run;
        }
    }

    /// How many of `blocks`, contiguous indices from the first, one range
    /// GET fills: the missing blocks that share the first one's placement,
    /// up to a chunk. One owner holds such a run, and held it before.
    fn fill_run(&self, key: &ObjectKey, size: u64, version: VersionId, blocks: &[u64]) -> usize {
        let layout = self.config.layout;
        let chunk_blocks = usize::try_from(layout.chunk_size() / layout.block_size()).unwrap_or(1);
        let placement = layout.placement(key, size, blocks[0]).hash();
        blocks
            .iter()
            .take_while(|&&index| {
                let block = BlockKey { version, index };
                (self.store.get(&block).is_none() || self.unwritten.contains(&block))
                    && !self.in_flight.contains_key(&block)
                    && layout.placement(key, size, index).hash() == placement
            })
            .take(chunk_blocks)
            .count()
    }

    /// S3's response body ended before the bytes for the slot at
    /// `location` arrived: the block is not stored.
    pub fn on_write_failed(&mut self, location: Location) {
        let origin = self
            .writes
            .remove(&location)
            .expect("a write was in progress");
        self.readable.remove(&location);
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
        // Reads of its readable bytes may still be sending; the slot stays
        // theirs until the last lets go.
        match self.store.get(&block).is_some_and(|entry| entry.pinned()) {
            true => {
                self.unwritten.insert(block);
            }
            false => {
                self.store.remove(block);
                self.unref(block.version);
            }
        }
        self.stats.unfilled_blocks += 1;
        self.filling_bytes -= len;
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
            if !self.store.get(&block).is_some_and(|entry| entry.purged()) {
                let record = self.slot_record(block);
                self.actions.push(Action::Record { location, record });
            }
            for waiter in waiters {
                self.arrived(waiter, Await::Verify(location));
            }
            self.drop_purged(block);
            return;
        }
        self.stats.corrupt_blocks += 1;
        let waiters: Vec<GatewayRequestId> = waiters
            .into_iter()
            .filter(|&waiter| self.abandon_plan(waiter))
            .collect();
        // A purged block goes as its last reader lets go, which may be one
        // of the plans just abandoned.
        if self.store.get(&block).is_some() {
            self.store.remove(block);
            self.stats.corrupt_dropped += 1;
            self.actions.push(Action::Clear { location });
            self.unref(block.version);
        }
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
            // Validated before a restart, which forgot the changes this
            // node had learned of.
            let forgotten = self
                .restarted
                .is_some_and(|restarted| sent.0 < restarted.0 + meta.age);
            (!written && !forgotten).then_some((meta, validated))
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
            match meta {
                // A write overtook the fetch, so the home keeps none of its
                // metadata. The request that started the fetch arrived
                // before the write, so the fetch still answers it: fetching
                // again could starve it while writes keep coming.
                Some(meta) if superseded => {
                    let shared = shared(&meta, sent, now);
                    self.plan(request, &key, &meta, shared);
                }
                Some(meta) => {
                    self.know(key, meta, sent);
                    self.serve(now, request);
                }
                None => {
                    self.waiting.remove(&request);
                    let head = ResponseHead::status(head.status);
                    self.respond(request, head, Vec::new(), Holds::default(), None);
                }
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
                    self.count_relayed(&head);
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
            // A read of a format's spot that stored none of its blocks
            // fills them, so the home can read the spot.
            if let Some(spot) = Format::of(&key).and_then(|format| format.spot(meta.size))
                && first < spot.end
                && spot.start <= last
            {
                let version = self.version(&key, &meta.etag);
                self.refer(version);
                self.prefetch(&key, &meta.etag, meta.size, version, spot);
                self.unref(version);
            }
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
            if let Some(location) = self.admit(key, meta.size, block, false, true) {
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
                self.changed(&key);
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
            let hot = self.hints.remove(&id).unwrap_or_default();
            self.actions.push(Action::Metadata {
                request: id,
                meta: shared,
                hot,
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
            self.stats.blocks_fetched +=
                self.config.layout.blocks_covering(first, last).count() as u64;
            self.read(origin, &mut holds);
            self.forget_if_unused(version);
            self.waiting.remove(&id);
            return self.respond(id, head, body, holds, meta);
        }
        let mut awaiting = BTreeSet::new();
        let mut fetched = 0;
        let layout = self.config.layout;
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
                (Some((location, BlockState::Filling, _)), None)
                    if !self.unwritten.contains(&block) =>
                {
                    let written = self.readable.contains(&location);
                    Some((location, (!written).then_some(Await::Written(location))))
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
                    // Fetch this block and the missing blocks after it
                    // that one range GET can.
                    let run_end = self.fill_run(key, size, version, &blocks[next..]);
                    let run = blocks[next]..=blocks[next + run_end - 1];
                    self.fill(key, etag, size, version, run, false)
                }
            };
            let body_start = self.origins[&origin].body_start;
            body.push(Segment::Origin {
                origin,
                offset: piece.start - body_start,
                len,
            });
            fetched += 1;
            self.read(origin, &mut holds);
            if !self.origins[&origin].answered {
                awaiting.insert(Await::Fill(origin));
            }
            next += 1;
        }
        self.forget_if_unused(version);
        if awaiting.is_empty() {
            self.stats.blocks_fetched += fetched;
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
            fetched,
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
        prefetch: bool,
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
        // A run shares one placement, so one node owns it, and owned it.
        let placement = layout.placement(key, size, *run.start()).hash();
        let owner = self.ring.owner(placement) == Some(self.id);
        let peer = match owner {
            true => self.previous_owner(placement),
            false => self.lease_owner(placement),
        };
        // A replica admits a leased placement's blocks without the
        // doorkeeper; an owner, those a previous owner supplies.
        let leased = !owner && self.leased(placement);
        let skip_doorkeeper = leased || peer.is_some() || prefetch;
        let purpose = Purpose::Fill {
            version,
            size,
            last_byte,
            stored: Vec::new(),
            provisional: owner && peer.is_some() && !prefetch,
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
            if let Some(location) = self.admit(key, size, block, skip_doorkeeper, !prefetch) {
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
            ..
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
            self.stats.unfilled_blocks += 1;
            self.filling_bytes -= len;
            self.unref(block.version);
        }
        // A 412 or 404 means the object changed, and so does a 206 for
        // another version or range. Any other status is S3 failing, which
        // the waiting requests pass on.
        let changed = matches!(head.status, 206 | 404 | 412);
        let mut revalidating = Vec::new();
        let (key, etag) = self.name(version).clone();
        if changed {
            self.changed(&key);
        }
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
            size,
            last_byte,
            stored,
            provisional,
        } = &request.purpose
        else {
            unreachable!("only fills are refilled");
        };
        let (version, size, last_byte, provisional) = (*version, *size, *last_byte, *provisional);
        let stored = stored.clone();
        let first = request.body_start;
        let (key, etag) = self.name(version).clone();
        // The previous owner lacked the blocks, so S3's pass the doorkeeper.
        let stored = match provisional {
            true => {
                let mut kept = Vec::new();
                for (block, location) in stored {
                    if self.passes_doorkeeper(&key, size, block) {
                        kept.push((block, location));
                    } else {
                        let len = self.store.get(&block).expect("stored block reserved").len;
                        self.store.remove(block);
                        self.stats.unfilled_blocks += 1;
                        self.filling_bytes -= len;
                        self.unref(block.version);
                    }
                }
                kept
            }
            false => stored,
        };
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
            size,
            last_byte,
            stored,
            provisional: false,
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
            self.stats.blocks_fetched += plan.fetched;
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
        self.hints.remove(&id);
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
            self.count_relayed(&head);
        }
        self.respond(id, head, body, holds, None);
    }

    /// Counts the blocks an object's bytes in a relayed body span.
    fn count_relayed(&mut self, head: &ResponseHead) {
        if matches!(head.status, 200 | 206)
            && let Some((first, last)) = body_span(head, head.content_length)
        {
            let blocks = self.config.layout.blocks_covering(first, last).count();
            self.stats.blocks_fetched += blocks as u64;
        }
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
                Segment::Slot { len, .. } => {
                    self.stats.block_hits += 1;
                    self.stats.hit_bytes += len;
                }
                Segment::Origin { origin, len, .. } => match self.origins[origin].peer {
                    Some(_) => self.stats.peer_bytes += len,
                    None => self.stats.miss_bytes += len,
                },
            }
        }
        self.sending.insert(id, holds);
        let hot = self.hints.remove(&id).unwrap_or_default();
        self.actions.push(Action::Respond {
            request: id,
            head,
            body,
            meta,
            hot,
        });
    }

    fn release(&mut self, holds: Holds) {
        for block in holds.pins {
            self.unpin(block);
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
    /// before, where they were stored. A block a read fetched is `missed`,
    /// and counts as a miss and by its admission; one warmed on write or
    /// prefetched counts only where those do.
    fn admit(
        &mut self,
        key: &ObjectKey,
        size: u64,
        block: BlockKey,
        skip_doorkeeper: bool,
        missed: bool,
    ) -> Option<Location> {
        let layout = self.config.layout;
        let placement = layout.placement(key, size, block.index).hash();
        let owns = self.ring.owner(placement) == Some(self.id) || self.leased(placement);
        // A block whose failed write reads still hold keeps its entry until
        // they let go, so this fetch serves without storing it.
        if !owns || self.unwritten.contains(&block) {
            return None;
        }
        let hash = block_hash(block.version, layout.block_size(), block.index);
        let counted = u64::from(missed);
        if self.store.remembers(hash) {
            self.stats.misses_evicted += counted;
        } else if self.doorkeeper.contains(hash) {
            self.stats.misses_unadmitted += counted;
        } else {
            self.stats.misses_new += counted;
        }
        if !skip_doorkeeper && !self.passes_doorkeeper(key, size, block) {
            self.stats.refused_doorkeeper += counted;
            return None;
        }
        let span = layout.block_span(size, block.index);
        let len = span.end - span.start;
        if self.filling_bytes + len > self.config.fill_budget {
            self.stats.refused_budget += counted;
            return None;
        }
        let location = self.store.reserve(block, len, hash, placement);
        // The new block refers to its version before evictions may drop
        // their versions' last references, which can include this one.
        if location.is_some() {
            self.refer(block.version);
        }
        for (evicted, location, disowned) in self.store.drain_evicted() {
            match disowned {
                true => self.stats.disowned_blocks += 1,
                false => self.stats.evicted_blocks += 1,
            }
            self.actions.push(Action::Clear { location });
            self.unref(evicted.version);
        }
        let Some(location) = location else {
            self.stats.refused_full += counted;
            return None;
        };
        self.stats.admitted += counted;
        self.filling_bytes += len;
        Some(location)
    }

    /// Whether a block goes to disk by the doorkeeper's rules: it lies at a
    /// format's spot, its bucket admits on first read, or the doorkeeper
    /// saw it lately. A block turned away marks the doorkeeper.
    fn passes_doorkeeper(&mut self, key: &ObjectKey, size: u64, block: BlockKey) -> bool {
        let layout = self.config.layout;
        let hash = block_hash(block.version, layout.block_size(), block.index);
        // A block at a format's spot is metadata every reader asks for.
        let at_spot = Format::of(key)
            .and_then(|format| format.spot(size))
            .is_some_and(|spot| {
                let span = layout.block_span(size, block.index);
                span.start < spot.end && spot.start < span.end
            });
        if at_spot || self.policy(&key.bucket).admit_on_first_read || self.doorkeeper.contains(hash)
        {
            return true;
        }
        self.doorkeeper.insert(hash);
        false
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
            self.inspected.remove(&id);
        }
    }

    /// Keeps metadata the home just validated, and saves it if its bucket
    /// is immutable.
    fn know(&mut self, key: ObjectKey, meta: Meta, validated: Time) {
        self.keep(key.clone(), meta.clone(), validated);
        if self.policy(&key.bucket).freshness == Freshness::Immutable {
            self.save(Action::Remember { key, meta });
        }
    }

    /// Appends an entry to the metadata file, and rewrites the file once it
    /// holds twice the metadata capacity.
    fn save(&mut self, entry: Action) {
        self.actions.push(entry);
        self.saved_entries += 1;
        if self.saved_entries > 2 * self.config.metadata_capacity {
            let entries = self.saved_metadata();
            self.saved_entries = entries.len();
            self.actions.push(Action::RewriteMetadata { entries });
        }
    }

    /// The metadata the home saves, of immutable buckets, least recently
    /// used first: what a rewrite of the metadata file holds.
    pub fn saved_metadata(&self) -> Vec<(ObjectKey, Meta)> {
        self.recency
            .values()
            .filter(|key| self.policy(&key.bucket).freshness == Freshness::Immutable)
            .filter_map(|key| match self.objects.get(key) {
                Some(Object::Known { meta, .. }) => Some((key.clone(), meta.clone())),
                _ => None,
            })
            .collect()
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
            warm_on_write: false,
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
            hot_threshold: 0,
            hot_window: 1_000,
            hot_replicas: 2,
            lease: 10_000,
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

    /// A warm check that went out before a write through the home, and
    /// that S3 answered with the version the write replaced, leaves the
    /// home without that version's metadata, however long S3 took and
    /// however short the fallback window.
    #[test]
    fn a_warm_check_older_than_a_write_keeps_no_metadata() {
        let policy = BucketPolicy {
            freshness: Freshness::Ttl(1_000_000),
            admit_on_first_read: false,
            warm_on_write: true,
        };
        let config = Config {
            fallback_window: 1,
            default_policy: policy,
            ..config(16)
        };
        let member = Member {
            id: NodeId(0),
            weight: NonZeroU32::MIN,
        };
        let mut node = Node::new(NodeId(0), Ring::new(1, vec![member]), config);
        let etag = ETag("\"v1\"".into());
        node.on_uploaded(Time(0), key("k"), etag.clone(), 100)
            .expect("warmed");
        let check = node
            .drain()
            .into_iter()
            .find_map(|action| match action {
                Action::Fetch { origin, .. } => Some(origin),
                _ => None,
            })
            .expect("a check");
        // Another write through the home, then a change to another key
        // long after the fallback window.
        node.on_write(Time(10), &key("k"));
        node.on_write(Time(500), &key("other"));
        node.drain();
        let head = ResponseHead {
            status: 200,
            etag: Some(etag),
            content_range: None,
            content_length: 100,
            headers: Vec::new(),
        };
        node.on_origin_response(Time(600), check, head);
        node.drain();
        let read = Read::Object {
            request: Request::get(key("k")),
            stale: None,
            direct: false,
        };
        node.on_request(Time(700), GatewayRequestId(1), read);
        let fetches = node.drain().into_iter().any(
            |action| matches!(action, Action::Fetch { request, .. } if request.key == key("k")),
        );
        assert!(fetches, "the home kept the replaced version's metadata");
    }

    /// Reads take a block a first fetch is writing once its bytes are in
    /// the slab file, before the sync. When the sync then fails, later
    /// reads fetch the block again without storing it while an earlier
    /// read still sends its bytes, and the slot is freed once that read
    /// ends.
    #[test]
    fn a_block_is_readable_before_it_is_durable() {
        let policy = BucketPolicy {
            freshness: Freshness::Immutable,
            admit_on_first_read: true,
            warm_on_write: false,
        };
        let config = Config {
            default_policy: policy,
            ..config(16)
        };
        let member = Member {
            id: NodeId(0),
            weight: NonZeroU32::MIN,
        };
        let mut node = Node::new(NodeId(0), Ring::new(1, vec![member]), config);
        let read = || Read::Object {
            request: Request::get(key("k")),
            stale: None,
            direct: false,
        };
        let etag = ETag("\"v1\"".into());
        node.on_request(Time(0), GatewayRequestId(1), read());
        let first = node
            .drain()
            .into_iter()
            .find_map(|action| match action {
                Action::Fetch { origin, .. } => Some(origin),
                _ => None,
            })
            .expect("a first fetch");
        let head = ResponseHead {
            status: 200,
            etag: Some(etag),
            content_range: None,
            content_length: 50,
            headers: Vec::new(),
        };
        node.on_origin_response(Time(1), first, head);
        let written = node
            .drain()
            .into_iter()
            .find_map(|action| match action {
                Action::Write { location, .. } => Some(location),
                _ => None,
            })
            .expect("the block is written");
        // Before its bytes are in, a read waits; once they are, it reads
        // the slot.
        node.on_request(Time(2), GatewayRequestId(2), read());
        let answered = |actions: Vec<Action>, id| {
            actions.into_iter().any(|action| {
                matches!(action, Action::Respond { request, ref body, .. }
                    if request == id && matches!(body[..], [Segment::Slot { .. }]))
            })
        };
        assert!(!answered(node.drain(), GatewayRequestId(2)));
        node.on_readable(written);
        assert!(answered(node.drain(), GatewayRequestId(2)));
        node.on_request(Time(3), GatewayRequestId(3), read());
        assert!(answered(node.drain(), GatewayRequestId(3)));
        // The sync fails while both still send.
        node.on_write_failed(written);
        node.drain();
        node.on_request(Time(4), GatewayRequestId(4), read());
        let actions = node.drain();
        let refetched = actions
            .iter()
            .any(|action| matches!(action, Action::Fetch { .. }));
        let stored = actions
            .iter()
            .any(|action| matches!(action, Action::Write { .. }));
        assert!(refetched && !stored, "{actions:?}");
        let blocks =
            |node: &Node| -> u64 { node.usage().classes.iter().map(|class| class.blocks).sum() };
        assert_eq!(blocks(&node), 1);
        node.on_sent(GatewayRequestId(2));
        assert_eq!(blocks(&node), 1);
        // The last read lets go, and the slot is free.
        node.on_sent(GatewayRequestId(3));
        assert_eq!(blocks(&node), 0);
    }

    /// A restarted node forgot the writes it had heard of, so it takes no
    /// metadata from a previous home validated before its restart, and
    /// takes metadata validated since.
    #[test]
    fn a_restarted_node_takes_no_metadata_validated_before_it_restarted() {
        let member = |id| Member {
            id: NodeId(id),
            weight: NonZeroU32::MIN,
        };
        let policy = BucketPolicy {
            freshness: Freshness::Ttl(1_000_000),
            admit_on_first_read: false,
            warm_on_write: false,
        };
        let config = Config {
            default_policy: policy,
            ..config(16)
        };
        let both = Ring::new(1, vec![member(0), member(1)]);
        // A key whose home was node 1, and is node 0 once node 1 leaves.
        let key = (0..)
            .map(|index| key(&format!("k{index}")))
            .find(|key| both.owner(Placement::Home(key).hash()) == Some(NodeId(1)))
            .expect("a key node 1 was home to");
        let asks = |age: u64| {
            let mut node = Node::recover(
                NodeId(0),
                both.clone(),
                config.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            node.restarted(Time(100));
            node.on_ring(Time(100), Ring::new(2, vec![member(0)]));
            let read = Read::Object {
                request: Request::head(key.clone()),
                stale: None,
                direct: false,
            };
            node.on_request(Time(150), GatewayRequestId(1), read);
            let asked = node.drain().into_iter().find_map(|action| match action {
                Action::PeerFetch { origin, .. } => Some(origin),
                _ => None,
            });
            let meta = ObjectMeta {
                etag: ETag("\"v1\"".into()),
                size: 10,
                headers: Vec::new(),
                age,
            };
            node.on_peer_metadata(
                Time(160),
                asked.expect("the previous home is asked"),
                Some(meta),
            );
            node.drain()
                .into_iter()
                .any(|action| matches!(action, Action::Fetch { .. }))
        };
        // Validated at 90, before the restart: S3 is asked instead.
        assert!(asks(60));
        // Validated at 140, since the restart: the metadata answers.
        assert!(!asks(10));
    }

    /// A coordinator tells the nodes of its rings of a purge again until
    /// each confirms, and waits for a node out of every ring to come back
    /// rather than tell it in vain.
    #[test]
    fn a_purge_is_told_again_to_nodes_in_the_ring() {
        let ring = |ids: &[u64]| {
            let members = ids
                .iter()
                .map(|&id| Member {
                    id: NodeId(id),
                    weight: NonZeroU32::MIN,
                })
                .collect();
            Ring::new(ids.len() as u64, members)
        };
        let told = |node: &mut Node| -> Vec<u64> {
            node.drain()
                .into_iter()
                .filter_map(|action| match action {
                    Action::PassPurge { node, .. } => Some(node.0),
                    _ => None,
                })
                .collect()
        };
        let mut node = Node::new(NodeId(0), ring(&[0, 1, 2]), config(4));
        node.on_purge(Time(1), &key("k"), false);
        assert_eq!(told(&mut node), [1, 2]);
        node.on_purge_confirmed(Time(2), &key("k"), NodeId(1));
        node.on_tick(Time(500));
        assert_eq!(told(&mut node), [2]);
        // Node 2 leaves, and once the fallback window ends, no ring holds
        // it: the purge waits on it without telling it.
        node.on_ring(Time(600), ring(&[0, 1]));
        node.on_tick(Time(2_000));
        assert_eq!(told(&mut node), Vec::<u64>::new());
        assert_eq!(node.pending_purges(), [(key("k"), vec![NodeId(2)])]);
        node.on_ring(Time(2_100), ring(&[0, 1, 2]));
        node.on_tick(Time(2_500));
        assert_eq!(told(&mut node), [2]);
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
        let mut node = Node::recover(NodeId(0), ring, config(2), Vec::new(), saved, Vec::new());
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
