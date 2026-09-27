//! Deterministic simulator for the gateway and storage nodes.
//!
//! One thread runs gateways, storage nodes with their disks, clients and a
//! model of S3. Every message crosses a simulated network, disk writes and
//! response bodies take time, writers change objects behind the cache's back,
//! and every response is checked against what S3 held while the request was
//! in flight, give or take the bucket's staleness bound. Every stored block
//! is checked against the version it is keyed by, and so is every record in
//! a node's slot table, through crashes and restarts. The seed determines the
//! whole run, from the cluster's size to each delay, so a seed replays its
//! run exactly.

pub mod disk;
pub mod origin;
pub mod prng;
pub mod properties;
pub mod queue;

use disk::Disk;
use origin::Origin;
use prng::Prng;
use queue::Queue;
use s3_accelerator_core::Time;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::layout::Layout;
use s3_accelerator_core::membership::{self, Membership, MembershipTimer, Peer};
use s3_accelerator_core::node::{
    self, BucketPolicy, EventId, Freshness, GatewayRequestId, HotHint, Node, ObjectMeta,
    OriginRequestId, Read, Segment, StoredBlock,
};
use s3_accelerator_core::placement::{
    Member, NodeId, Placement, PlacementHash, Ring, down_version,
};
use s3_accelerator_core::s3::{ByteRange, ETag, Method, ObjectKey, Request, ResponseHead};
use s3_accelerator_core::store::{Location, StoreConfig, VersionId};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::xxh3_64_with_seed;

fn members(ring: &Ring) -> Vec<usize> {
    ring.members()
        .iter()
        .map(|member| member.id.0 as usize)
        .collect()
}

/// Node `node`'s identity in its run `run`.
fn peer(node: usize, run: u64) -> Peer {
    Peer {
        id: node as u64,
        weight: 1,
        run,
        leaving: false,
        address: format!("node-{node}"),
    }
}

/// Resizes a run makes at most.
pub const MAX_RESIZES: u64 = 6;

/// More events than any tick of a live run delivers: with zero delays, a
/// retry loop would otherwise spin within one tick forever.
const EVENTS_PER_TICK: u64 = 1_000_000;

/// Objects in this bucket are written once and never change.
pub const IMMUTABLE_BUCKET: &str = "immutable";
/// Objects in this bucket are overwritten and deleted; metadata expires.
pub const TTL_BUCKET: &str = "ttl";

/// A run's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub nodes: usize,
    pub gateways: usize,
    pub clients: usize,
    pub block_size: u64,
    pub chunk_blocks: u64,
    pub extent_blocks: u64,
    pub extents: u32,
    pub min_slot: u64,
    pub doorkeeper_window: u64,
    pub fill_budget_blocks: u64,
    /// Objects whose metadata a home keeps.
    pub metadata_capacity: usize,
    /// Objects whose metadata a gateway keeps, and for how many ticks it
    /// keeps metadata of objects that may change.
    pub gateway_metadata_capacity: usize,
    pub gateway_metadata_ttl: u64,
    /// Ticks a gateway keeps metadata of immutable objects.
    pub purge_window: u64,
    /// Chance that a gateway's range read reaches a node that does not own
    /// its blocks, as when gateways and nodes disagree about the ring.
    pub misroute_percent: u64,
    /// Ticks before a node abandons an S3 request, a gateway fails over
    /// from a node, and a client retries; and how long a gateway routes
    /// around a node that timed out.
    pub origin_timeout: u64,
    pub node_timeout: u64,
    pub client_timeout: u64,
    pub suspect_ttl: u64,
    /// Faults while clients are still issuing requests: lost messages,
    /// delayed ones, nodes cut off from everyone for up to
    /// `partition_max` ticks, and S3 answering 503.
    pub loss_percent: u64,
    pub spike_percent: u64,
    pub partition_percent: u64,
    pub partition_max: u64,
    pub origin_error_percent: u64,
    /// Chance per thousand ticks, while faults happen, that a node stops
    /// for up to `down_max` ticks. It shuts down cleanly, finishing its
    /// writes, `clean_percent` of the time; otherwise it crashes, tearing
    /// the writes in progress, and `damage_percent` of crashes also damage
    /// recorded slots, as a drive that loses acknowledged writes would.
    pub crash_permille: u64,
    pub clean_percent: u64,
    pub damage_percent: u64,
    pub down_max: u64,
    /// Chance, while faults happen, that a node's response body or S3's
    /// ends partway, as when a connection drops.
    pub cut_percent: u64,
    pub origin_cut_percent: u64,
    /// Freshness of the TTL bucket's metadata, in ticks.
    pub ttl: u64,
    pub immutable_admit_on_first_read: bool,
    pub ttl_admit_on_first_read: bool,
    pub keys: usize,
    pub object_size_max: u64,
    /// Client requests in the run.
    pub requests: u64,
    /// Requests each client keeps in flight at most.
    pub requests_in_flight: usize,
    /// Chance per tick that a client with room issues a request.
    pub request_percent: u64,
    /// Chance per tick that a writer creates, overwrites or deletes an object.
    pub write_percent: u64,
    /// Share of writes to the TTL bucket that delete.
    pub delete_percent: u64,
    /// Share of writes, while clients issue requests, that pass through a
    /// gateway and a node to S3; the rest reach S3 from elsewhere.
    pub gateway_write_percent: u64,
    /// S3 notifies the cluster of every change to the TTL bucket through a
    /// queue, whose bucket TTL is then a long backstop. The queue offers
    /// each event after up to `event_delay_max` ticks, repeats
    /// `event_repeat_percent` of them, and offers an event again once
    /// `visibility_timeout` ticks pass without a node deleting it.
    pub events: bool,
    pub event_delay_max: u64,
    pub event_repeat_percent: u64,
    pub visibility_timeout: u64,
    /// A placement its owner reads `hot_threshold` times within
    /// `hot_window` ticks is leased to `hot_replicas` more nodes for
    /// `lease` ticks; 0 turns leases off. `hot_percent` of reads go to the
    /// first key.
    pub hot_threshold: u64,
    pub hot_window: u64,
    pub hot_replicas: usize,
    pub lease: u64,
    pub hot_percent: u64,
    /// Whether each bucket's home stores the uploads that pass through it.
    pub immutable_warm_on_write: bool,
    pub ttl_warm_on_write: bool,
    /// Keys are named for formats that state their metadata's span, in
    /// turn: none, Parquet, ORC and safetensors.
    pub formats: bool,
    /// Share of deletes from elsewhere that the key's home then purges.
    pub purge_percent: u64,
    /// One-way network delay, in ticks.
    pub delay_min: u64,
    pub delay_max: u64,
    /// Ticks a disk write or a response body takes, at most.
    pub disk_delay_max: u64,
    pub send_delay_max: u64,
    /// Membership runs SWIM among the nodes, and each node's ring follows
    /// what it hears; otherwise every node shares one fixed ring.
    pub membership: bool,
    /// Membership's timings, in ticks: how often a node probes another,
    /// how long it waits for the answer, how long a suspect has to answer,
    /// and how long a node declared down stays in the ring.
    pub probe_period: u64,
    pub probe_rtt: u64,
    pub suspect_to_down: u64,
    pub down_grace: u64,
    /// Ticks a node keeps the previous ring after a change, and ticks a
    /// previous owner has to answer.
    pub fallback_window: u64,
    pub peer_timeout: u64,
    /// Chance per 10,000 ticks, while faults happen, that the cluster
    /// resizes: a node joins, one leaves, or one fails for good and a new
    /// one replaces it. A run resizes at most `MAX_RESIZES` times.
    pub resize_per_10k: u64,
}

impl Options {
    /// A configuration drawn from `prng`, so many seeds cover many
    /// configurations.
    pub fn swarm(prng: &mut Prng) -> Options {
        let block_size = 1 << prng.range(4..=9);
        let chunk_blocks = prng.range(1..=4);
        let delay_min = prng.range(0..=3);
        let mut options = Options {
            nodes: prng.range(1..=6) as usize,
            gateways: prng.range(1..=3) as usize,
            clients: prng.range(1..=8) as usize,
            block_size,
            chunk_blocks,
            extent_blocks: prng.range(1..=4),
            extents: prng.range(1..=24) as u32,
            min_slot: 1 << prng.range(2..=u64::from(block_size.trailing_zeros())),
            doorkeeper_window: prng.range(1..=512),
            fill_budget_blocks: prng.range(1..=32),
            metadata_capacity: prng.range(1..=32) as usize,
            ttl: prng.range(0..=300),
            immutable_admit_on_first_read: prng.percent(50),
            ttl_admit_on_first_read: prng.percent(50),
            keys: prng.range(1..=24) as usize,
            object_size_max: prng.range(1..=block_size * chunk_blocks * 6),
            requests: prng.range(1..=1_500),
            requests_in_flight: prng.range(1..=4) as usize,
            request_percent: prng.range(10..=100),
            write_percent: prng.range(0..=30),
            delete_percent: prng.range(0..=30),
            delay_min,
            delay_max: delay_min + prng.range(0..=20),
            disk_delay_max: prng.range(0..=6),
            send_delay_max: prng.range(0..=6),
            // Options added since Phase 1 draw last, so earlier ones keep
            // their values for every seed.
            gateway_metadata_capacity: prng.range(1..=32) as usize,
            gateway_metadata_ttl: prng.range(0..=200),
            purge_window: 5_000,
            misroute_percent: prng.range(0..=20),
            origin_timeout: 0,
            node_timeout: 0,
            client_timeout: 0,
            suspect_ttl: 0,
            loss_percent: 0,
            spike_percent: 0,
            partition_percent: 0,
            partition_max: 0,
            origin_error_percent: 0,
            crash_permille: 0,
            clean_percent: 0,
            damage_percent: 0,
            down_max: 0,
            cut_percent: 0,
            origin_cut_percent: 0,
            membership: false,
            probe_period: 0,
            probe_rtt: 0,
            suspect_to_down: 0,
            down_grace: 0,
            fallback_window: 0,
            peer_timeout: 0,
            resize_per_10k: 0,
            gateway_write_percent: 0,
            events: false,
            event_delay_max: 0,
            event_repeat_percent: 0,
            visibility_timeout: 0,
            hot_threshold: 0,
            hot_window: 0,
            hot_replicas: 0,
            lease: 0,
            hot_percent: 0,
            immutable_warm_on_write: false,
            ttl_warm_on_write: false,
            formats: false,
            purge_percent: 0,
        };
        // Timeouts outlast every exchange of a fault-free run, so only
        // faults make them fire: S3 answers within two hops, and a node
        // within eight.
        let hop = options.delay_max + options.disk_delay_max.max(options.send_delay_max) + 1;
        options.origin_timeout = hop * prng.range(3..=6);
        // A node may wait out one S3 timeout and then fetch again.
        options.node_timeout = 2 * options.origin_timeout + hop * prng.range(4..=10);
        options.client_timeout = options.node_timeout * (options.nodes as u64 + 2);
        options.suspect_ttl = prng.range(0..=500);
        options.loss_percent = if prng.percent(50) {
            0
        } else {
            prng.range(1..=10)
        };
        options.spike_percent = prng.range(0..=5);
        options.partition_percent = prng.range(0..=2);
        options.partition_max = prng.range(1..=200);
        options.origin_error_percent = prng.range(0..=5);
        options.crash_permille = if prng.percent(50) {
            0
        } else {
            prng.range(1..=10)
        };
        options.clean_percent = prng.range(0..=100);
        options.damage_percent = prng.range(0..=50);
        options.down_max = prng.range(1..=300);
        options.cut_percent = prng.range(0..=5);
        options.origin_cut_percent = prng.range(0..=5);
        options.membership = prng.percent(80);
        // A probe's answer takes two hops, and its indirect probes four.
        options.probe_rtt = 2 * hop + prng.range(0..=hop);
        options.probe_period = options.probe_rtt * prng.range(2..=4);
        options.suspect_to_down = options.probe_period * prng.range(1..=3);
        options.down_grace = options.probe_period * prng.range(0..=10);
        options.fallback_window = prng.range(0..=2_000);
        // A previous owner answers within two hops unless faults intervene.
        options.peer_timeout = hop * prng.range(2..=6);
        options.resize_per_10k = match options.membership {
            true => prng.range(0..=20),
            false => 0,
        };
        options.gateway_write_percent = match prng.percent(25) {
            true => 0,
            false => prng.range(1..=100),
        };
        options.events = prng.percent(40);
        options.event_delay_max = prng.range(0..=300);
        options.event_repeat_percent = prng.range(0..=20);
        options.visibility_timeout = options.peer_timeout + hop * prng.range(2..=10);
        if options.events {
            options.ttl = prng.range(2_000..=20_000);
        }
        options.hot_threshold = match prng.percent(30) {
            true => 0,
            false => prng.range(3..=30),
        };
        options.hot_window = prng.range(50..=500);
        options.hot_replicas = prng.range(1..=3) as usize;
        options.lease = prng.range(100..=2_000);
        options.hot_percent = match prng.percent(50) {
            true => 0,
            false => prng.range(10..=60),
        };
        options.immutable_warm_on_write = prng.percent(50);
        options.ttl_warm_on_write = prng.percent(50);
        options.formats = prng.percent(50);
        options.purge_percent = match prng.percent(50) {
            true => 0,
            false => prng.range(10..=100),
        };
        options
    }

    /// One node, one gateway, one client and no background workload: a
    /// cluster for scripted scenarios.
    pub fn scenario() -> Options {
        Options {
            nodes: 1,
            gateways: 1,
            clients: 1,
            block_size: 64,
            chunk_blocks: 4,
            extent_blocks: 4,
            extents: 16,
            min_slot: 16,
            doorkeeper_window: 1_024,
            fill_budget_blocks: 64,
            metadata_capacity: 1_024,
            gateway_metadata_capacity: 1_024,
            gateway_metadata_ttl: 1_000,
            purge_window: 5_000,
            misroute_percent: 0,
            origin_timeout: 1_000,
            node_timeout: 1_000,
            client_timeout: 10_000,
            suspect_ttl: 100,
            loss_percent: 0,
            spike_percent: 0,
            partition_percent: 0,
            partition_max: 0,
            origin_error_percent: 0,
            crash_permille: 0,
            clean_percent: 0,
            damage_percent: 0,
            down_max: 0,
            cut_percent: 0,
            origin_cut_percent: 0,
            ttl: 1_000,
            immutable_admit_on_first_read: false,
            ttl_admit_on_first_read: false,
            keys: 0,
            object_size_max: 1,
            requests: 0,
            requests_in_flight: 1,
            request_percent: 0,
            write_percent: 0,
            delete_percent: 0,
            delay_min: 1,
            delay_max: 1,
            disk_delay_max: 1,
            send_delay_max: 1,
            membership: false,
            probe_period: 50,
            probe_rtt: 10,
            suspect_to_down: 100,
            down_grace: 500,
            fallback_window: 1_000,
            peer_timeout: 50,
            resize_per_10k: 0,
            gateway_write_percent: 0,
            events: false,
            event_delay_max: 0,
            event_repeat_percent: 0,
            visibility_timeout: 1_000,
            hot_threshold: 0,
            hot_window: 100,
            hot_replicas: 2,
            lease: 1_000,
            hot_percent: 0,
            immutable_warm_on_write: false,
            ttl_warm_on_write: false,
            formats: false,
            purge_percent: 0,
        }
    }

    fn gateway_config(&self) -> gateway::Config {
        let node = self.node_config();
        gateway::Config {
            layout: node.layout,
            default_policy: node.default_policy,
            buckets: node.buckets,
            metadata_capacity: self.gateway_metadata_capacity,
            metadata_ttl: self.gateway_metadata_ttl,
            purge_window: self.purge_window,
            node_timeout: self.node_timeout,
            suspect_ttl: self.suspect_ttl,
        }
    }

    fn node_config(&self) -> node::Config {
        let policy = |freshness, admit_on_first_read, warm_on_write| BucketPolicy {
            freshness,
            admit_on_first_read,
            warm_on_write,
        };
        let immutable = policy(
            Freshness::Immutable,
            self.immutable_admit_on_first_read,
            self.immutable_warm_on_write,
        );
        let ttl = policy(
            Freshness::Ttl(self.ttl),
            self.ttl_admit_on_first_read,
            self.ttl_warm_on_write,
        );
        node::Config {
            layout: Layout::new(self.block_size, self.chunk_blocks),
            store: StoreConfig {
                extent_size: self.block_size * self.extent_blocks,
                extents: self.extents,
                min_slot: self.min_slot,
                max_slot: self.block_size,
            },
            doorkeeper_window: self.doorkeeper_window,
            fill_budget: self.block_size * self.fill_budget_blocks,
            metadata_capacity: self.metadata_capacity,
            origin_timeout: self.origin_timeout,
            fallback_window: self.fallback_window,
            peer_timeout: self.peer_timeout,
            hot_threshold: self.hot_threshold,
            hot_window: self.hot_window,
            hot_replicas: self.hot_replicas,
            lease: self.lease,
            default_policy: ttl,
            buckets: BTreeMap::from([
                (IMMUTABLE_BUCKET.to_string(), immutable),
                (TTL_BUCKET.to_string(), ttl),
            ]),
        }
    }

    fn membership_config(&self) -> membership::Config {
        membership::Config {
            probe_period: self.probe_period,
            probe_rtt: self.probe_rtt,
            suspect_to_down: self.suspect_to_down,
            down_grace: self.down_grace,
            gossip_period: (self.probe_period / 2).max(1),
            max_packet: 1_400,
        }
    }

    /// How far before a request's issue tick its answer may reflect.
    fn staleness(&self, key: &ObjectKey) -> u64 {
        match key.bucket.as_str() {
            IMMUTABLE_BUCKET => 0,
            _ => self.ttl,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Address {
    Client(usize),
    Gateway(usize),
    Node(usize),
    Origin,
}

/// What crosses the network.
#[derive(Debug)]
enum Message {
    ClientRequest {
        request: u64,
        read: Request,
    },
    NodeRequest {
        gateway: usize,
        id: NodeRequestId,
        read: Read,
    },
    OriginRequest {
        node: usize,
        run: u64,
        origin: OriginRequestId,
        read: Request,
    },
    ClientResponse {
        request: u64,
        head: ResponseHead,
        body: Vec<u8>,
    },
    NodeResponse {
        id: NodeRequestId,
        head: ResponseHead,
        body: Body,
        meta: Option<ObjectMeta>,
        hot: Vec<HotHint>,
        ring: (usize, u64, u64),
    },
    NodeMetadata {
        id: NodeRequestId,
        meta: ObjectMeta,
        hot: Vec<HotHint>,
        ring: (usize, u64, u64),
    },
    /// An owner leases a hot placement to a node, and the node reports the
    /// reads the lease served.
    Lease {
        owner: usize,
        placement: PlacementHash,
        until: u64,
    },
    LeaseReport {
        placement: PlacementHash,
        reads: u64,
    },
    NodeStale {
        id: NodeRequestId,
        ring: (usize, u64, u64),
    },
    /// Membership's packets between nodes.
    Gossip {
        packet: Vec<u8>,
    },
    RingRequest {
        from: Address,
    },
    RingResponse {
        ring: Ring,
        down: Vec<NodeId>,
    },
    /// A ring request that found its node down, as a refused connection.
    RingFailed,
    /// A gateway's node request that found its node down, as a refused
    /// connection.
    NodeRefused {
        id: NodeRequestId,
    },
    /// A write one node passes to another.
    WriteNotice {
        key: ObjectKey,
    },
    /// A client's write through a gateway, which the gateway passes to a
    /// node and the node to S3; `size` is `None` for a delete. Answers
    /// carry when S3 applied it.
    ClientWrite {
        change: u64,
        key: ObjectKey,
    },
    NodeWrite {
        gateway: usize,
        change: u64,
        key: ObjectKey,
        size: Option<u64>,
    },
    /// A node write that found its node down, as a refused connection.
    NodeWriteRefused {
        change: u64,
    },
    /// S3's event queue offers a node message `message`, which says `key`
    /// changed to `etag`; the node passes it to other homes, and deletes it
    /// once they have it.
    QueueEvent {
        message: u64,
        key: ObjectKey,
        etag: Option<ETag>,
    },
    EventNotice {
        node: usize,
        run: u64,
        event: EventId,
        key: ObjectKey,
        etag: Option<ETag>,
    },
    EventHeard {
        run: u64,
        event: EventId,
        from: usize,
    },
    /// A purge a coordinator passes to a node, and the node's confirmation
    /// once the purge is durable.
    PurgeNotice {
        from: usize,
        run: u64,
        key: ObjectKey,
    },
    PurgeConfirmed {
        run: u64,
        key: ObjectKey,
        from: usize,
    },
    /// A node deletes a message it finished, under its ring `ring`.
    DeleteEvent {
        message: u64,
        ring: u64,
    },
    OriginWrite {
        node: usize,
        run: u64,
        gateway: usize,
        change: u64,
        key: ObjectKey,
        size: Option<u64>,
    },
    /// S3's answer to a write, and for an upload, the version it made and
    /// its size.
    OriginWriteResponse {
        run: u64,
        gateway: usize,
        change: u64,
        key: ObjectKey,
        status: u16,
        applied: u64,
        upload: Option<(ETag, u64)>,
    },
    NodeWriteResponse {
        change: u64,
        key: ObjectKey,
        status: u16,
        applied: u64,
        ring: (usize, u64, u64),
    },
    ClientWriteResponse {
        change: u64,
        status: u16,
        applied: u64,
    },
    /// A node's read of a previous owner, which it numbered `origin` in its
    /// run `run`, and the answers.
    PeerRequest {
        node: usize,
        run: u64,
        origin: OriginRequestId,
        read: Read,
    },
    PeerResponse {
        run: u64,
        origin: OriginRequestId,
        head: ResponseHead,
        body: Body,
    },
    PeerMetadata {
        run: u64,
        origin: OriginRequestId,
        meta: ObjectMeta,
    },
    OriginResponse {
        run: u64,
        origin: OriginRequestId,
        head: ResponseHead,
        body: Body,
    },
}

/// Events for a node carry the run that caused them, and a node that
/// restarted since ignores them.
enum Event {
    Deliver {
        to: Address,
        message: Message,
    },
    /// A node's write reached its disk.
    Written {
        node: usize,
        run: u64,
        location: Location,
    },
    /// A node finished sending a response body.
    Sent {
        node: usize,
        run: u64,
        id: GatewayRequestId,
    },
    /// A gateway finished copying a node's response body into a client's
    /// response.
    Forwarded {
        gateway: usize,
        request: ClientRequestId,
        from: NodeRequestId,
        bytes: Vec<u8>,
    },
    /// A node read the bytes at a version's spot.
    SpotRead {
        node: usize,
        run: u64,
        version: VersionId,
        parts: Vec<(Location, u64, u64)>,
    },
    /// A node read a recovered block back to check its checksum.
    Verified {
        node: usize,
        run: u64,
        location: Location,
        len: u64,
        checksum: u64,
    },
    /// A timer a node's membership set.
    MembershipTimer {
        node: usize,
        run: u64,
        timer: MembershipTimer,
    },
    /// A starting node stops waiting for a seed's ring and joins.
    JoinTimeout {
        node: usize,
        run: u64,
    },
}

/// Who sent a node a request, and where the answer goes.
#[derive(Clone, Copy, Debug)]
enum Requester {
    Gateway(usize, NodeRequestId),
    /// A node reading from a previous owner, in its run `run`.
    Peer {
        node: usize,
        run: u64,
        origin: OriginRequestId,
    },
}

/// A node's answer to a request.
enum NodeAnswer {
    Response {
        head: ResponseHead,
        body: Body,
        meta: Option<ObjectMeta>,
        hot: Vec<HotHint>,
    },
    Metadata(ObjectMeta, Vec<HotHint>),
    Stale,
}

impl Event {
    fn is_membership(&self) -> bool {
        matches!(
            self,
            Event::MembershipTimer { .. }
                | Event::JoinTimeout { .. }
                | Event::Deliver {
                    message: Message::Gossip { .. },
                    ..
                }
        )
    }
}

/// A client's request, which it sends again after a 5xx or a timeout.
struct Pending {
    client: usize,
    read: Request,
    issued: u64,
    /// When the latest attempt went out.
    sent: u64,
}

/// An attempt at a client's read: the request, its client, the gateway
/// it went through, and when it went out.
struct Attempt {
    request: u64,
    client: usize,
    gateway: usize,
    sent: u64,
}

/// A client's write through a gateway, until the client hears or gives up.
struct Change {
    client: usize,
    gateway: usize,
    key: ObjectKey,
    size: Option<u64>,
    issued: u64,
    /// The nodes the gateway has yet to try, and the version of the ring
    /// it chose them by.
    candidates: Vec<NodeId>,
    ring: u64,
}

/// A write through a gateway that its client heard succeed: when S3
/// applied it, when the client heard, and the gateway's ring version.
#[derive(Clone, Copy)]
struct Acknowledged {
    applied: u64,
    heard: u64,
    ring: u64,
}

/// A message in S3's event queue: its change to `key`, when S3 applied the
/// change, and when the queue next offers the message.
struct Notification {
    key: ObjectKey,
    etag: Option<ETag>,
    applied: u64,
    visible_at: u64,
}

/// A response body as it arrived: `len` bytes long, unless it ended early.
#[derive(Debug)]
struct Body {
    bytes: Vec<u8>,
    len: u64,
}

impl Body {
    fn whole(bytes: Vec<u8>) -> Body {
        let len = bytes.len() as u64;
        Body { bytes, len }
    }
}

/// A node write in progress: the bytes of an S3 response body to copy.
struct Write {
    origin: OriginRequestId,
    offset: u64,
    len: u64,
}

/// A node response whose body is being sent.
struct Sending {
    head: ResponseHead,
    body: Vec<Segment>,
    meta: Option<ObjectMeta>,
    hot: Vec<HotHint>,
}

pub struct Simulator {
    seed: u64,
    options: Options,
    /// One stream per source of randomness, so drawing more for one
    /// leaves the others' draws unchanged.
    workload: Prng,
    writers: Prng,
    network: Prng,
    misroutes: Prng,
    losses: Prng,
    spikes: Prng,
    partitions: Prng,
    origin_errors: Prng,
    retries: Prng,
    crashes: Prng,
    tears: Prng,
    cuts: Prng,
    resizes: Prng,
    gateway_writes: Prng,
    write_errors: Prng,
    events: Prng,
    hot_reads: Prng,
    purges: Prng,
    /// S3's event queue, and for each key, when S3 applied each change a
    /// node finished the event of, when, and under what ring.
    notifications: BTreeMap<u64, Notification>,
    next_notification: u64,
    /// Scripted: the node the queue offers every event to, and whether it
    /// holds them back.
    events_to: Option<usize>,
    events_held: bool,
    finished_events: BTreeMap<ObjectKey, Vec<(u64, u64, u64)>>,
    /// Whether faults happen: while clients are still issuing requests.
    faulty: bool,
    /// Nodes cut off from everyone, until the tick given.
    partitioned: BTreeMap<usize, u64>,
    /// Nodes that are down, until the tick given.
    down: BTreeMap<usize, u64>,
    /// Nodes leaving the cluster, and the tick each stops for good.
    leaving: BTreeMap<usize, u64>,
    /// Nodes that left the cluster.
    left: BTreeSet<usize>,
    /// Starting nodes waiting for a seed's ring before they join.
    joining: BTreeSet<usize>,
    /// Nodes started knowing only some others.
    given_seeds: BTreeMap<usize, Vec<usize>>,
    /// The node a gateway that lost track of the ring asks next.
    ring_seeds: usize,
    /// Each node's run: how many times it has stopped.
    runs: Vec<u64>,
    /// Stats of each node's runs that ended.
    retired: Vec<node::Stats>,
    disk_delays: Prng,
    send_delays: Prng,
    now: u64,
    queue: Queue<Event>,
    origin: Origin,
    keys: Vec<ObjectKey>,
    ring: Ring,
    gateways: Vec<Gateway>,
    /// Each node, while it is up, and its membership, while it gossips.
    nodes: Vec<Option<Node>>,
    memberships: Vec<Option<Membership>>,
    /// Seeds for each membership's random choices.
    membership_seeds: Prng,
    disks: Vec<Disk>,
    in_flight: Vec<usize>,
    requests: BTreeMap<u64, Pending>,
    issued: u64,
    attempts: BTreeMap<u64, Attempt>,
    /// Each gateway's ring version, and when it took that ring up.
    gateway_rings: Vec<(u64, u64)>,
    /// Writes through gateways in progress, and the latest each gateway
    /// saw succeed for each key.
    changes: BTreeMap<u64, Change>,
    next_change: u64,
    acknowledged: BTreeMap<(usize, ObjectKey), Acknowledged>,
    /// Writes S3 received, which it applies as the next tick starts.
    origin_writes: Vec<Message>,
    /// Scripted writes, and their statuses once they arrive.
    watched_changes: BTreeMap<u64, Option<u16>>,
    /// When faults stopped.
    quiet_since: Option<u64>,
    next_attempt: u64,
    /// Scripted requests, and their answers once they arrive.
    watched: BTreeMap<u64, Option<(ResponseHead, Vec<u8>)>>,
    // What the server keeps per connection: who asked, and the bodies it
    // holds while a node or gateway reads them.
    next_id: u64,
    gateway_requests: BTreeMap<(usize, ClientRequestId), u64>,
    node_requests: BTreeMap<(usize, GatewayRequestId), Requester>,
    /// Reads nodes sent previous owners, and whether each asked for
    /// metadata rather than blocks.
    peer_reads: BTreeMap<(usize, OriginRequestId), bool>,
    gateway_bodies: BTreeMap<(usize, NodeRequestId), Body>,
    origin_bodies: BTreeMap<(usize, OriginRequestId), Body>,
    /// Scripted cuts: the next body a node sends, or S3 sends a node, that
    /// is longer than the given length ends there.
    cut_responses: BTreeMap<usize, u64>,
    cut_origin_responses: BTreeMap<usize, u64>,
    /// Scripted: a write S3 applies just before it answers the next
    /// request from a node.
    write_before_answer: Option<(ObjectKey, u64)>,
    /// Scripted: forwards finish only once released.
    holding_forwards: bool,
    held_forwards: Vec<Event>,
    /// Client responses a gateway started, and the body so far.
    client_responses: BTreeMap<(usize, ClientRequestId), (ResponseHead, Vec<u8>)>,
    writes: BTreeMap<(usize, Location), Write>,
    sending: BTreeMap<(usize, GatewayRequestId), Sending>,
    /// S3 requests a node gave up on, whose responses it drops.
    cancelled: BTreeSet<(usize, OriginRequestId)>,
    /// S3 bodies that stream through a node: those it asked for, and
    /// those whose head has arrived, which only the node's actions on
    /// that arrival may read.
    streaming: BTreeSet<(usize, OriginRequestId)>,
    started: BTreeSet<(usize, OriginRequestId)>,
    /// Print client attempts and answers to stderr.
    trace: bool,
    summary: Summary,
}

impl Simulator {
    /// A run whose configuration the seed also determines.
    pub fn from_seed(seed: u64) -> Simulator {
        let options = Options::swarm(&mut Prng::stream(seed, "options"));
        Simulator::new(seed, options)
    }

    pub fn new(seed: u64, options: Options) -> Simulator {
        let mut setup = Prng::stream(seed, "setup");
        let members = (0..options.nodes)
            .map(|index| Member {
                id: NodeId(index as u64),
                weight: NonZeroU32::MIN,
            })
            .collect();
        let mut membership_seeds = Prng::stream(seed, "membership");
        let known: Vec<Peer> = (0..options.nodes).map(|node| peer(node, 0)).collect();
        let memberships: Vec<Option<Membership>> = known
            .iter()
            .map(|me| {
                let seed = membership_seeds.next_u64();
                options.membership.then(|| {
                    Membership::new(
                        Time(0),
                        me.clone(),
                        &known,
                        options.membership_config(),
                        seed,
                    )
                })
            })
            .collect();
        // Nodes that start together start with the same ring.
        let ring = match memberships.first() {
            Some(Some(membership)) => membership.ring().clone(),
            _ => Ring::new(1, members),
        };
        let keys: Vec<ObjectKey> = (0..options.keys)
            .map(|index| {
                let suffix = match options.formats {
                    true => ["", ".parquet", ".orc", ".safetensors"][index % 4],
                    false => "",
                };
                ObjectKey {
                    bucket: [IMMUTABLE_BUCKET, TTL_BUCKET][index % 2].to_string(),
                    key: format!("key-{index}{suffix}"),
                }
            })
            .collect();
        let mut origin = Origin::default();
        for key in &keys {
            if setup.percent(80) {
                let size = setup.range(1..=options.object_size_max);
                origin.put(0, key, size, &mut setup);
            }
        }
        let config = options.node_config();
        let mut simulator = Simulator {
            seed,
            workload: Prng::stream(seed, "workload"),
            writers: Prng::stream(seed, "writers"),
            network: Prng::stream(seed, "network"),
            misroutes: Prng::stream(seed, "misroutes"),
            losses: Prng::stream(seed, "losses"),
            spikes: Prng::stream(seed, "spikes"),
            partitions: Prng::stream(seed, "partitions"),
            origin_errors: Prng::stream(seed, "origin errors"),
            retries: Prng::stream(seed, "retries"),
            crashes: Prng::stream(seed, "crashes"),
            tears: Prng::stream(seed, "tears"),
            cuts: Prng::stream(seed, "cuts"),
            resizes: Prng::stream(seed, "resizes"),
            gateway_writes: Prng::stream(seed, "gateway writes"),
            write_errors: Prng::stream(seed, "write errors"),
            events: Prng::stream(seed, "events"),
            hot_reads: Prng::stream(seed, "hot reads"),
            purges: Prng::stream(seed, "purges"),
            notifications: BTreeMap::new(),
            next_notification: 0,
            events_to: None,
            events_held: false,
            finished_events: BTreeMap::new(),
            faulty: true,
            partitioned: BTreeMap::new(),
            down: BTreeMap::new(),
            leaving: BTreeMap::new(),
            left: BTreeSet::new(),
            joining: BTreeSet::new(),
            given_seeds: BTreeMap::new(),
            ring_seeds: 0,
            runs: vec![0; options.nodes],
            retired: vec![node::Stats::default(); options.nodes],
            disk_delays: Prng::stream(seed, "disk delays"),
            send_delays: Prng::stream(seed, "send delays"),
            now: 0,
            queue: Queue::default(),
            origin,
            keys,
            ring: ring.clone(),
            gateways: (0..options.gateways)
                .map(|_| Gateway::new(ring.clone(), options.gateway_config()))
                .collect(),
            nodes: (0..options.nodes)
                .map(|index| {
                    Some(Node::new(
                        NodeId(index as u64),
                        ring.clone(),
                        config.clone(),
                    ))
                })
                .collect(),
            memberships,
            membership_seeds,
            disks: (0..options.nodes)
                .map(|_| Disk::new(config.store.extents, config.store.extent_size))
                .collect(),
            in_flight: vec![0; options.clients],
            requests: BTreeMap::new(),
            attempts: BTreeMap::new(),
            gateway_rings: vec![(ring.version(), 0); options.gateways],
            changes: BTreeMap::new(),
            next_change: 0,
            acknowledged: BTreeMap::new(),
            origin_writes: Vec::new(),
            watched_changes: BTreeMap::new(),
            quiet_since: None,
            next_attempt: 0,
            issued: 0,
            watched: BTreeMap::new(),
            next_id: 0,
            gateway_requests: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            peer_reads: BTreeMap::new(),
            gateway_bodies: BTreeMap::new(),
            origin_bodies: BTreeMap::new(),
            client_responses: BTreeMap::new(),
            cut_responses: BTreeMap::new(),
            cut_origin_responses: BTreeMap::new(),
            write_before_answer: None,
            holding_forwards: false,
            held_forwards: Vec::new(),
            writes: BTreeMap::new(),
            sending: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            streaming: BTreeSet::new(),
            started: BTreeSet::new(),
            trace: false,
            summary: Summary {
                seed,
                ..Summary::default()
            },
            options,
        };
        for node in 0..simulator.nodes.len() {
            simulator.start_joining(node);
        }
        simulator
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Prints each client attempt and answer to stderr, for debugging a
    /// seed.
    pub fn trace(mut self) -> Simulator {
        self.trace = true;
        self
    }

    /// Runs the random workload with faults while clients issue requests,
    /// then without: every request must then be answered in time.
    pub fn run(mut self) -> Result<Summary, Failure> {
        let per_hop =
            self.options.delay_max + self.options.disk_delay_max + self.options.send_delay_max;
        // Faults last until clients have issued every request, or until
        // this budget runs out: faults heavy enough to stall a small
        // cluster would otherwise hold the run in its faulty phase forever.
        // The budget also bounds issuing without faults.
        let budget =
            20_000 + self.options.requests * ((per_hop + 1) * 20 + 2 * self.options.client_timeout);
        let mut deadline = None;
        while self.issued < self.options.requests || !self.requests.is_empty() {
            if self.faulty && (self.issued == self.options.requests || self.now > budget) {
                self.stop_faults()?;
            }
            if self.issued == self.options.requests && deadline.is_none() {
                deadline = Some(self.now + 20 * self.options.client_timeout + 10_000);
            }
            let quiet = self.quiet_since.unwrap_or(self.now);
            if self.now > deadline.unwrap_or(quiet + budget) {
                let unanswered = self.requests.len();
                let phase = match deadline {
                    Some(_) => "after every request was issued and faults stopped",
                    None => "while clients were issuing",
                };
                return Err(self.failure(format!("{unanswered} requests unanswered {phase}")));
            }
            self.tick()?;
        }
        self.settle()?;
        Ok(self.summary())
    }

    /// Ends the faulty phase: partitions heal and stopped nodes restart.
    fn stop_faults(&mut self) -> Result<(), Failure> {
        if self.trace {
            eprintln!("{} faults stop", self.now);
        }
        self.faulty = false;
        self.quiet_since = Some(self.now);
        self.partitioned.clear();
        for node in std::mem::take(&mut self.leaving).into_keys() {
            self.leave(node)?;
        }
        for node in std::mem::take(&mut self.down).into_keys() {
            if !self.left.contains(&node) {
                self.restart(node)?;
            }
        }
        Ok(())
    }

    /// Writes an object to the model of S3 now.
    pub fn put(&mut self, key: &ObjectKey, size: u64) {
        self.origin.put(self.now, key, size, &mut self.writers);
        self.notify(key);
    }

    pub fn delete(&mut self, key: &ObjectKey) {
        self.origin.delete(self.now, key);
        self.notify(key);
    }

    /// Runs until S3's event queue is empty and the cluster is idle.
    pub fn deliver_events(&mut self) -> Result<(), Failure> {
        let deadline = self.now + 100_000;
        while !self.notifications.is_empty() {
            if self.now > deadline {
                return Err(self.failure("events stayed in the queue".into()));
            }
            self.tick()?;
        }
        self.settle()
    }

    /// Purges `key` through its home under gateway 0's ring, if the home is
    /// up, and runs until the cluster is idle.
    pub fn purge(&mut self, key: &ObjectKey) -> Result<(), Failure> {
        self.start_purge(key)?;
        self.settle()
    }

    /// Purges `key` through its home under gateway 0's ring, if the home is
    /// up, without waiting.
    pub fn start_purge(&mut self, key: &ObjectKey) -> Result<(), Failure> {
        let home = self.gateways[0]
            .ring()
            .owner(Placement::Home(key).hash())
            .expect("a node")
            .0 as usize;
        self.purge_at(home, key)
    }

    /// Has `node` coordinate a purge of `key`, and syncs its disk.
    fn purge_at(&mut self, node: usize, key: &ObjectKey) -> Result<(), Failure> {
        let now = Time(self.now);
        let Some(up) = self.nodes[node].as_mut() else {
            return Ok(());
        };
        up.on_purge(now, key, false);
        self.drain_node(node)?;
        self.purged(node, key)
    }

    /// A node that purged `key` syncs its disk before it confirms, and then
    /// records no block of it: blocks still being read go once free, but
    /// a restart must not bring them back.
    fn purged(&mut self, node: usize, key: &ObjectKey) -> Result<(), Failure> {
        let unsynced = self.disks[node].unsynced();
        self.disks[node].keep_appended(unsynced);
        let recorded = self.recorded_blocks_of(node, key);
        if recorded > 0 {
            return Err(self.failure(format!(
                "node {node} purged {key:?} but still records {recorded} of its blocks"
            )));
        }
        Ok(())
    }

    /// Responses `node` is still sending.
    pub fn sends_in_progress(&self, node: usize) -> usize {
        self.sending
            .keys()
            .filter(|(owner, _)| *owner == node)
            .count()
    }

    /// Blocks of any version of `key` that `node`'s slot table records.
    pub fn recorded_blocks_of(&self, node: usize, key: &ObjectKey) -> usize {
        let versions = VersionId::all_of(VersionId::key_hash(key));
        self.disks[node]
            .records
            .values()
            .filter(|(record, _, _)| versions.contains(&record.version))
            .count()
    }

    /// The purges `node` coordinates, and the nodes each waits on.
    pub fn pending_purges(&self, node: usize) -> Vec<(ObjectKey, Vec<usize>)> {
        let Some(up) = self.nodes[node].as_ref() else {
            return Vec::new();
        };
        up.pending_purges()
            .into_iter()
            .map(|(key, nodes)| (key, nodes.into_iter().map(|node| node.0 as usize).collect()))
            .collect()
    }

    /// Makes S3 write `size` bytes to `key` just before it answers the next
    /// request from a node.
    pub fn write_before_next_answer(&mut self, key: &ObjectKey, size: u64) {
        self.write_before_answer = Some((key.clone(), size));
    }

    /// Makes the queue offer every event to `node`, whether or not it is up.
    pub fn offer_events_to(&mut self, node: usize) {
        self.events_to = Some(node);
    }

    /// Makes the queue hold its events until `release_events`.
    pub fn hold_events(&mut self) {
        self.events_held = true;
    }

    pub fn release_events(&mut self) {
        self.events_held = false;
    }

    /// S3 queues an event for a change it just applied to `key`, and
    /// sometimes a repeat of it.
    fn notify(&mut self, key: &ObjectKey) {
        if !self.options.events || key.bucket != TTL_BUCKET {
            return;
        }
        let etag = self.origin.current(key).map(|object| object.etag.clone());
        let copies = 1 + u64::from(self.events.percent(self.options.event_repeat_percent));
        for _ in 0..copies {
            let delay = self.events.range(0..=self.options.event_delay_max);
            let message = self.next_notification;
            self.next_notification += 1;
            let notification = Notification {
                key: key.clone(),
                etag: etag.clone(),
                applied: self.now,
                visible_at: self.now + delay,
            };
            self.notifications.insert(message, notification);
        }
    }

    /// The queue offers each visible event to a node that is up, and hides
    /// it until the visibility timeout passes.
    fn offer_events(&mut self) {
        if self.events_held {
            return;
        }
        let visible: Vec<u64> = self
            .notifications
            .iter()
            .filter(|(_, notification)| notification.visible_at <= self.now)
            .map(|(&message, _)| message)
            .collect();
        let up: Vec<usize> = match self.events_to {
            Some(node) => vec![node],
            None => (0..self.nodes.len())
                .filter(|&node| self.nodes[node].is_some())
                .collect(),
        };
        if up.is_empty() {
            return;
        }
        for message in visible {
            let node = up[self.events.index(up.len())];
            let notification = self.notifications.get_mut(&message).expect("visible");
            notification.visible_at = self.now + self.options.visibility_timeout;
            let (key, etag) = (notification.key.clone(), notification.etag.clone());
            if self.trace {
                eprintln!(
                    "{} queue offers node {node} event {message}: {key:?} {etag:?}",
                    self.now
                );
            }
            let offer = Message::QueueEvent { message, key, etag };
            self.send(Address::Origin, Address::Node(node), offer);
        }
    }

    /// Writes an object through gateway 0, which passes it to the key's
    /// home under its ring and the home to S3, and runs until the client
    /// hears that it succeeded and the cluster is idle.
    pub fn write_through(&mut self, key: &ObjectKey, size: u64) -> Result<(), Failure> {
        self.write_through_via(0, key, size)
    }

    /// As `write_through`, through `gateway`.
    pub fn write_through_via(
        &mut self,
        gateway: usize,
        key: &ObjectKey,
        size: u64,
    ) -> Result<(), Failure> {
        let change = self.issue_change(0, gateway, key.clone(), Some(size));
        self.watched_changes.insert(change, None);
        let deadline = self.now + 100_000;
        while self.watched_changes[&change].is_none() {
            if self.now > deadline {
                return Err(self.failure(format!("write {change} unanswered")));
            }
            self.tick()?;
        }
        self.settle()?;
        match self.watched_changes.remove(&change).flatten() {
            Some(status) if status < 300 => Ok(()),
            status => Err(self.failure(format!("write {change} answered {status:?}"))),
        }
    }

    /// Writes an object through `gateway` and the key's home under its
    /// ring within this tick, as if the write's messages took no time, for
    /// scripting a race against the write's answer.
    pub fn write_at_once(
        &mut self,
        gateway: usize,
        key: &ObjectKey,
        size: u64,
    ) -> Result<(), Failure> {
        self.origin.put(self.now, key, size, &mut self.writers);
        self.notify(key);
        let home = self.gateways[gateway]
            .ring()
            .owner(Placement::Home(key).hash())
            .expect("a node");
        let now = Time(self.now);
        if let Some(node) = self.nodes[home.0 as usize].as_mut() {
            node.on_write(now, key);
            self.drain_node(home.0 as usize)?;
        }
        self.gateways[gateway].on_write(now, key, Some(home));
        self.drain_gateway(gateway)
    }

    pub fn origin(&self) -> &Origin {
        &self.origin
    }

    /// Sends one request from client 0 through gateway 0 and runs until it
    /// is answered and the cluster is idle.
    pub fn read(&mut self, read: Request) -> Result<(ResponseHead, Vec<u8>), Failure> {
        let request = self.start(read);
        self.finish(request)
    }

    /// Sends one request from client 0 through `gateway` and runs until it
    /// is answered and the cluster is idle.
    pub fn read_through(
        &mut self,
        gateway: usize,
        read: Request,
    ) -> Result<(ResponseHead, Vec<u8>), Failure> {
        let request = self.issue(0, gateway, read);
        self.watched.insert(request, None);
        self.finish(request)
    }

    /// Sends one request from client 0 through gateway 0 without waiting.
    pub fn start(&mut self, read: Request) -> u64 {
        let request = self.issue(0, 0, read);
        self.watched.insert(request, None);
        request
    }

    /// Runs until a started request is answered and the cluster is idle.
    pub fn finish(&mut self, request: u64) -> Result<(ResponseHead, Vec<u8>), Failure> {
        let deadline = self.now + 100_000;
        while self.watched.get(&request).is_some_and(Option::is_none) {
            if self.now > deadline {
                return Err(self.failure(format!("request {request} unanswered")));
            }
            self.tick()?;
        }
        self.settle()?;
        let answer = self.watched.remove(&request).flatten();
        answer.ok_or_else(|| self.failure(format!("request {request} was never started")))
    }

    /// The members of `node`'s ring, while it is up.
    pub fn node_ring(&self, node: usize) -> Option<Vec<usize>> {
        let up = self.nodes[node].as_ref()?;
        Some(members(up.ring()))
    }

    /// The members of `gateway`'s ring.
    pub fn gateway_ring(&self, gateway: usize) -> Vec<usize> {
        members(self.gateways[gateway].ring())
    }

    /// Starts a new node with an empty disk, which joins through the nodes
    /// it knows. Returns its index.
    pub fn add_node(&mut self) -> Result<usize, Failure> {
        self.add_node_knowing(None)
    }

    /// Starts a new node that knows only `seeds`, as a config that names
    /// only some nodes, or every node if `None`.
    pub fn add_node_knowing(&mut self, seeds: Option<Vec<usize>>) -> Result<usize, Failure> {
        let node = self.nodes.len();
        if let Some(seeds) = seeds {
            self.given_seeds.insert(node, seeds);
        }
        let config = self.options.node_config();
        self.nodes.push(None);
        self.memberships.push(None);
        self.disks
            .push(Disk::new(config.store.extents, config.store.extent_size));
        self.runs.push(0);
        self.retired.push(node::Stats::default());
        if self.trace {
            eprintln!("{} node {node} joins", self.now);
        }
        self.restart(node)?;
        Ok(node)
    }

    /// Starts `node` leaving: it drops out of every ring at once, serves
    /// its blocks to their new owners through the fallback window, and then
    /// stops for good.
    pub fn remove_node(&mut self, node: usize) -> Result<(), Failure> {
        if self.trace {
            eprintln!("{} node {node} starts leaving", self.now);
        }
        let until = self.now + self.options.fallback_window;
        self.leaving.insert(node, until);
        if let Some(membership) = self.memberships[node].as_mut() {
            membership.start_leaving(Time(self.now));
        }
        self.drain_membership(node)
    }

    /// Grows the cluster by a node, lets one leave, or replaces one that
    /// fails for good, within bounds that keep it between one and eight
    /// nodes.
    fn resize(&mut self) -> Result<(), Failure> {
        const MAX_NODES: usize = 8;
        let live: Vec<usize> = self
            .members()
            .filter(|node| self.nodes[*node].is_some() && !self.leaving.contains_key(node))
            .collect();
        let room = self.members().count() < MAX_NODES;
        match self.resizes.below(3) {
            0 if room => {
                self.add_node()?;
                self.summary.resizes += 1;
            }
            1 if live.len() > 1 => {
                let node = live[self.resizes.index(live.len())];
                self.remove_node(node)?;
                self.summary.resizes += 1;
            }
            2 if live.len() > 1 => {
                let node = live[self.resizes.index(live.len())];
                self.fail(node)?;
                self.add_node()?;
                self.summary.resizes += 1;
            }
            _ => {}
        }
        Ok(())
    }

    /// Crashes `node` for good: it never restarts, and leaves the cluster.
    pub fn fail(&mut self, node: usize) -> Result<(), Failure> {
        if self.trace {
            eprintln!("{} node {node} fails for good", self.now);
        }
        self.stop(node, false, true, u64::MAX)?;
        self.down.remove(&node);
        self.left.insert(node);
        Ok(())
    }

    /// Stops a leaving node for good, as a clean shutdown.
    fn leave(&mut self, node: usize) -> Result<(), Failure> {
        if self.trace {
            eprintln!("{} node {node} leaves", self.now);
        }
        if let Some(membership) = self.memberships[node].as_mut() {
            membership.leave(Time(self.now));
        }
        self.drain_membership(node)?;
        if self.nodes[node].is_some() {
            self.stop(node, true, false, u64::MAX)?;
        }
        self.down.remove(&node);
        self.left.insert(node);
        Ok(())
    }

    /// The ring of the lowest node up, or the fixed ring.
    fn current_ring(&self) -> &Ring {
        self.nodes
            .iter()
            .flatten()
            .next()
            .filter(|_| self.options.membership)
            .map_or(&self.ring, |node| node.ring())
    }

    /// The node that is `key`'s home.
    pub fn home(&self, key: &ObjectKey) -> usize {
        let home = self
            .current_ring()
            .owner(Placement::Home(key).hash())
            .expect("a node");
        home.0 as usize
    }

    /// The rendezvous candidates for `key`'s home, best first.
    pub fn home_candidates(&self, key: &ObjectKey) -> Vec<usize> {
        let placement = Placement::Home(key).hash();
        self.current_ring()
            .candidates(placement)
            .into_iter()
            .map(|node| node.0 as usize)
            .collect()
    }

    /// Cuts `node` off from everyone for `ticks` ticks, whatever the options
    /// say about faults.
    pub fn partition(&mut self, node: usize, ticks: u64) {
        self.partitioned.insert(node, self.now + ticks);
    }

    /// Crashes `node`: its memory is lost and its writes in progress tear.
    /// It stays down until `restart`.
    pub fn crash(&mut self, node: usize) -> Result<(), Failure> {
        self.stop(node, false, false, u64::MAX)
    }

    /// Shuts `node` down cleanly: it finishes its writes and marks its slot
    /// table. It stays down until `restart`.
    pub fn shut_down(&mut self, node: usize) -> Result<(), Failure> {
        self.stop(node, true, false, u64::MAX)
    }

    /// Starts a node that is down, over its slot table. With membership,
    /// it starts as a new run, with every node it knows in its ring.
    pub fn restart(&mut self, node: usize) -> Result<(), Failure> {
        self.down.remove(&node);
        let records = self.disks[node].start();
        let metadata = self.disks[node].metadata.clone();
        let purges = self.disks[node].purges.clone();
        let config = self.options.node_config();
        let id = NodeId(node as u64);
        let seed = self.membership_seeds.next_u64();
        let ring = match self.options.membership {
            true => {
                let known: Vec<Peer> = self.seeds(node).map(|node| peer(node, 0)).collect();
                let me = peer(node, self.runs[node]);
                let config = self.options.membership_config();
                let membership = Membership::new(Time(self.now), me, &known, config, seed);
                let ring = membership.ring().clone();
                self.memberships[node] = Some(membership);
                ring
            }
            false => self.ring.clone(),
        };
        self.nodes[node] = Some(Node::recover(id, ring, config, records, metadata, purges));
        self.drain_node(node)?;
        self.start_joining(node);
        Ok(())
    }

    /// A starting node asks the others for their ring before it joins, so
    /// it learns whether it is new to them. It joins once a ring arrives,
    /// or after a few probe periods without one.
    fn start_joining(&mut self, node: usize) {
        if self.memberships[node].is_none() {
            return;
        }
        self.joining.insert(node);
        let seeds: Vec<usize> = self.seeds(node).filter(|&seed| seed != node).collect();
        for seed in seeds {
            let message = Message::RingRequest {
                from: Address::Node(node),
            };
            self.send(Address::Node(node), Address::Node(seed), message);
        }
        let run = self.runs[node];
        let timeout = Event::JoinTimeout { node, run };
        self.queue
            .push(self.now + 4 * self.options.probe_period, timeout);
    }

    /// A starting node joins, having heard the ring `before` it, if any.
    fn finish_joining(&mut self, node: usize, before: Option<Ring>) -> Result<(), Failure> {
        if !self.joining.remove(&node) {
            return Ok(());
        }
        if let (Some(before), Some(up)) = (before, self.nodes[node].as_mut()) {
            up.on_joined(Time(self.now), before);
        }
        self.join(node);
        self.drain_membership(node)
    }

    /// Nodes that have not left the cluster.
    fn members(&self) -> impl Iterator<Item = usize> + use<'_> {
        (0..self.nodes.len()).filter(|node| !self.left.contains(node))
    }

    /// The nodes `node` knows when it starts, as its config names them:
    /// those it was given, or every member.
    fn seeds(&self, node: usize) -> impl Iterator<Item = usize> + use<'_> {
        let given = self.given_seeds.get(&node);
        self.members()
            .filter(move |seed| given.is_none_or(|given| *seed == node || given.contains(seed)))
    }

    /// Announces a node's membership to every node it knows.
    fn join(&mut self, node: usize) {
        let seeds: Vec<NodeId> = self.seeds(node).map(|node| NodeId(node as u64)).collect();
        if let Some(membership) = self.memberships[node].as_mut() {
            membership.join(Time(self.now), &seeds);
        }
    }

    /// Carries out a node's membership actions: packets cross the network,
    /// timers go on the queue, and a new ring reaches the node.
    fn drain_membership(&mut self, node: usize) -> Result<(), Failure> {
        let Some(membership) = self.memberships[node].as_mut() else {
            return Ok(());
        };
        let run = self.runs[node];
        for action in membership.drain() {
            match action {
                membership::Action::Send { to, packet } => {
                    let message = Message::Gossip { packet };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                membership::Action::Schedule { timer, at } => {
                    let event = Event::MembershipTimer { node, run, timer };
                    self.queue.push(at.0.max(self.now), event);
                }
                membership::Action::Ring(ring) => {
                    if self.trace {
                        let members: Vec<u64> =
                            ring.members().iter().map(|member| member.id.0).collect();
                        eprintln!(
                            "{} node {node} ring {:016x} {members:?}",
                            self.now,
                            ring.version()
                        );
                    }
                    self.summary.ring_changes += 1;
                    if let Some(up) = self.nodes[node].as_mut() {
                        up.on_ring(Time(self.now), ring);
                    }
                }
                // Answers read the down nodes from membership as they go.
                membership::Action::Down(_) => {}
            }
        }
        self.drain_node(node)
    }

    /// Ends the next response body `node` sends that is longer than `at`
    /// bytes after `at` bytes.
    pub fn cut_response(&mut self, node: usize, at: u64) {
        self.cut_responses.insert(node, at);
    }

    /// Ends the next response body S3 sends `node` that is longer than
    /// `at` bytes after `at` bytes.
    pub fn cut_origin_response(&mut self, node: usize, at: u64) {
        self.cut_origin_responses.insert(node, at);
    }

    /// Holds every forward that starts from now on until
    /// `release_forwards`.
    pub fn hold_forwards(&mut self) {
        self.holding_forwards = true;
    }

    /// Lets held forwards finish, and later ones run as usual.
    pub fn release_forwards(&mut self) {
        self.holding_forwards = false;
        for event in std::mem::take(&mut self.held_forwards) {
            self.queue.push(self.now, event);
        }
    }

    /// Forwards held since `hold_forwards`.
    pub fn held_forwards(&self) -> usize {
        self.held_forwards.len()
    }

    /// The node that owns block `index` of `key`, an object of `size` bytes.
    pub fn owner(&self, key: &ObjectKey, size: u64, index: u64) -> usize {
        let layout = Layout::new(self.options.block_size, self.options.chunk_blocks);
        let placement = layout.placement(key, size, index).hash();
        self.current_ring().owner(placement).expect("a node").0 as usize
    }

    /// Writes `node` has in progress.
    pub fn writes_in_progress(&self, node: usize) -> usize {
        self.writes
            .keys()
            .filter(|(owner, _)| *owner == node)
            .count()
    }

    /// Damages the slot `node` recorded for block `index` of `key`, as a
    /// lost write would. Returns false if no record names that block.
    pub fn damage(&mut self, node: usize, key: &ObjectKey, index: u64) -> bool {
        let disk = &mut self.disks[node];
        let found = disk
            .records
            .iter()
            .find(|(_, (record, _, _))| {
                record.version.key == VersionId::key_hash(key) && record.index == index
            })
            .map(|(&location, _)| location);
        if let Some(location) = found {
            disk.damage(location, 0);
        }
        found.is_some()
    }

    /// Entries in `node`'s metadata file, durable or not.
    pub fn metadata_file_entries(&self, node: usize) -> usize {
        self.disks[node].metadata.len() + self.disks[node].unsynced()
    }

    /// The simulated time, in ticks.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// The answer to a started request, once it has arrived.
    pub fn take_answer(&mut self, request: u64) -> Option<(ResponseHead, Vec<u8>)> {
        self.watched.get_mut(&request)?.take()
    }

    /// Runs one tick.
    pub fn step(&mut self) -> Result<(), Failure> {
        self.tick()
    }

    pub fn summary(&self) -> Summary {
        let mut summary = self.summary.clone();
        summary.ticks = self.now;
        summary.origin_requests = self.origin.requests();
        for (node, &retired) in self.nodes.iter().zip(&self.retired) {
            let mut stats = node.as_ref().map(Node::stats).unwrap_or_default();
            stats += retired;
            summary.hit_bytes += stats.hit_bytes;
            summary.miss_bytes += stats.miss_bytes;
            summary.written_bytes += stats.written_bytes;
            summary.evicted_blocks += stats.evicted_blocks;
            summary.verified_blocks += stats.verified_blocks;
            summary.corrupt_blocks += stats.corrupt_blocks;
            summary.peer_requests += stats.peer_requests;
            summary.peer_timeouts += stats.peer_timeouts;
            summary.peer_metadata += stats.peer_metadata;
            summary.peer_bytes += stats.peer_bytes;
            summary.leases += stats.leases_granted;
            summary.leased_reads += stats.leased_reads;
            summary.warmed_uploads += stats.warmed_uploads;
            summary.prefetched_blocks += stats.prefetched_blocks;
            summary.node_reads.push(stats.reads);
        }
        summary
    }

    fn tick(&mut self) -> Result<(), Failure> {
        self.tick_faults()?;
        self.apply_origin_writes();
        self.tick_writes()?;
        self.offer_events();
        self.tick_clients();
        let mut events = 0;
        while let Some(event) = self.queue.pop_due(self.now) {
            events += 1;
            if events > EVENTS_PER_TICK {
                return Err(self.failure("livelock: events keep arriving within one tick".into()));
            }
            self.handle(event)?;
        }
        let now = Time(self.now);
        for node in 0..self.nodes.len() {
            if let Some(membership) = self.memberships[node].as_mut() {
                membership.on_tick(now);
                self.drain_membership(node)?;
            }
            if let Some(up) = self.nodes[node].as_mut() {
                up.on_tick(now);
                self.drain_node(node)?;
            }
        }
        for gateway in 0..self.gateways.len() {
            self.gateways[gateway].on_tick(now);
            self.drain_gateway(gateway)?;
        }
        for disk in &mut self.disks {
            disk.sync(self.now);
        }
        if self.now.is_multiple_of(64) {
            self.check_disks()?;
        }
        self.now += 1;
        Ok(())
    }

    /// Heals partitions and restarts nodes whose time is up, and starts new
    /// partitions and stops.
    fn tick_faults(&mut self) -> Result<(), Failure> {
        let now = self.now;
        let gone: Vec<usize> = self
            .leaving
            .iter()
            .filter(|&(_, &until)| until <= now)
            .map(|(&node, _)| node)
            .collect();
        for node in gone {
            self.leaving.remove(&node);
            self.leave(node)?;
        }
        self.partitioned.retain(|_, until| *until > now);
        if self.faulty && self.partitions.percent(self.options.partition_percent) {
            let node = self.partitions.index(self.nodes.len());
            let ticks = self.partitions.range(1..=self.options.partition_max);
            self.partitioned.insert(node, now + ticks);
            if self.trace {
                eprintln!("{now} node {node} partitioned for {ticks} ticks");
            }
        }
        let due: Vec<usize> = self
            .down
            .iter()
            .filter(|&(_, &until)| until <= now)
            .map(|(&node, _)| node)
            .collect();
        for node in due {
            if self.trace {
                eprintln!("{now} node {node} restarts");
            }
            self.restart(node)?;
        }
        if self.faulty
            && self.summary.resizes < MAX_RESIZES
            && self.resizes.below(10_000) < self.options.resize_per_10k
        {
            self.resize()?;
        }
        if self.faulty && self.crashes.below(1_000) < self.options.crash_permille {
            let node = self.crashes.index(self.nodes.len());
            let clean = self.crashes.percent(self.options.clean_percent);
            let ticks = self.crashes.range(1..=self.options.down_max);
            // A leaving node stops only for good.
            if self.nodes[node].is_some() && !self.leaving.contains_key(&node) {
                if self.trace {
                    let how = if clean { "shuts down" } else { "crashes" };
                    eprintln!("{now} node {node} {how} for {ticks} ticks");
                }
                self.stop(node, clean, true, now + ticks)?;
            }
        }
        Ok(())
    }

    /// Takes `node` down until tick `until`, losing its memory. A clean
    /// shutdown first finishes the node's writes and marks its slot table;
    /// a crash tears the writes in progress, and if `damage` allows, may
    /// also damage recorded slots.
    fn stop(&mut self, node: usize, clean: bool, damage: bool, until: u64) -> Result<(), Failure> {
        let Some(stopped) = self.nodes[node].as_ref() else {
            return Err(self.failure(format!("node {node} stopped while down")));
        };
        let stats = stopped.stats();
        // Responses in progress finish on a clean shutdown. A crash cuts
        // them short or loses them, and they read the disk as it was.
        let sending: Vec<GatewayRequestId> = self
            .sending
            .keys()
            .filter(|(owner, _)| *owner == node)
            .map(|&(_, id)| id)
            .collect();
        for id in sending {
            let Sending {
                head,
                body,
                meta,
                hot,
            } = self
                .sending
                .remove(&(node, id))
                .expect("a send in progress");
            let mut body = self.assemble(node, &body)?;
            let requester = self.node_requests[&(node, id)];
            if !clean {
                if self.cuts.percent(50) {
                    continue;
                }
                body.bytes
                    .truncate(self.cuts.below(body.len.max(1)) as usize);
            }
            let answer = NodeAnswer::Response {
                head,
                body,
                meta,
                hot,
            };
            self.answer_request(node, requester, answer);
        }
        let writes: Vec<Location> = self
            .writes
            .keys()
            .filter(|(owner, _)| *owner == node)
            .map(|&(_, location)| location)
            .collect();
        if clean {
            for location in writes {
                self.written(node, location)?;
            }
            let entries = self.node(node).saved_metadata();
            let disk = &mut self.disks[node];
            disk.rewrite_metadata(entries);
            disk.shut_down();
            self.summary.clean_shutdowns += 1;
        } else {
            for location in writes {
                let write = &self.writes[&(node, location)];
                let body = &self.origin_bodies[&(node, write.origin)].bytes;
                let start = (write.offset as usize).min(body.len());
                let end = ((write.offset + write.len) as usize).min(body.len());
                let torn = self.tears.range(0..=(end - start) as u64) as usize;
                self.disks[node].write(location, &body[start..start + torn]);
            }
            let synced = self.tears.range(0..=self.disks[node].unsynced() as u64);
            self.disks[node].keep_appended(synced as usize);
            self.disks[node].lose_unsynced();
            if damage && self.tears.percent(self.options.damage_percent) {
                for _ in 0..self.tears.range(1..=3) {
                    let disk = &self.disks[node];
                    if disk.records.is_empty() {
                        break;
                    }
                    let (&location, (record, _, _)) = disk
                        .records
                        .iter()
                        .nth(self.tears.index(disk.records.len()))
                        .expect("a record");
                    let offset = self.tears.below(record.len);
                    self.disks[node].damage(location, offset);
                }
            }
            self.summary.crashes += 1;
        }
        self.retired[node] += stats;
        self.nodes[node] = None;
        self.memberships[node] = None;
        self.joining.remove(&node);
        self.runs[node] += 1;
        self.down.insert(node, until);
        self.writes.retain(|&(owner, _), _| owner != node);
        self.sending.retain(|&(owner, _), _| owner != node);
        self.origin_bodies.retain(|&(owner, _), _| owner != node);
        self.cancelled.retain(|&(owner, _)| owner != node);
        self.streaming.retain(|&(owner, _)| owner != node);
        self.started.retain(|&(owner, _)| owner != node);
        self.node_requests.retain(|&(owner, _), _| owner != node);
        self.peer_reads.retain(|&(owner, _), _| owner != node);
        self.check_table(node)
    }

    /// Runs until nothing is in flight, then checks that nothing is left
    /// behind.
    fn settle(&mut self) -> Result<(), Failure> {
        let deadline = self.now
            + 4 * (self.options.client_timeout
                + self.options.node_timeout
                + self.options.origin_timeout);
        // Membership gossips forever, so its timers and packets never run
        // out.
        while !(self.queue.all(Event::is_membership)
            && self.origin_writes.is_empty()
            && self.idle())
            && self.now <= deadline
        {
            self.tick()?;
        }
        // Responses to cancelled requests that were lost never arrive.
        self.cancelled.clear();
        self.check_disks()?;
        let stranded = self.gateway_requests.len()
            + self.node_requests.len()
            + self.gateway_bodies.len()
            + self.origin_bodies.len()
            + self.client_responses.len()
            + self.held_forwards.len()
            + self.writes.len()
            + self.sending.len();
        let busy = self.busy();
        if stranded > 0 || busy > 0 {
            return Err(self.failure(format!(
                "{stranded} requests or bodies stranded and {busy} nodes or gateways busy after the last response"
            )));
        }
        Ok(())
    }

    fn idle(&self) -> bool {
        self.busy() == 0
    }

    /// Nodes and gateways with work in progress.
    fn busy(&self) -> usize {
        self.nodes
            .iter()
            .flatten()
            .filter(|node| !node.is_idle())
            .count()
            + self
                .gateways
                .iter()
                .filter(|gateway| !gateway.is_idle())
                .count()
    }

    fn tick_writes(&mut self) -> Result<(), Failure> {
        if self.keys.is_empty() || !self.writers.percent(self.options.write_percent) {
            return Ok(());
        }
        let key = self.keys[self.writers.index(self.keys.len())].clone();
        let size = self.writers.range(1..=self.options.object_size_max);
        let through_gateway = self.issued < self.options.requests
            && self
                .gateway_writes
                .percent(self.options.gateway_write_percent);
        if key.bucket == IMMUTABLE_BUCKET {
            // An immutable object is written once.
            let writing = self.changes.values().any(|change| change.key == key);
            if self.origin.current(&key).is_some() || writing {
                return Ok(());
            }
            match through_gateway {
                true => self.write_through_gateway(key, Some(size)),
                false => self.origin.put(self.now, &key, size, &mut self.writers),
            }
            self.summary.writes += 1;
            return Ok(());
        }
        let delete = self.writers.percent(self.options.delete_percent);
        match (through_gateway, delete) {
            (true, true) => self.write_through_gateway(key.clone(), None),
            (true, false) => self.write_through_gateway(key.clone(), Some(size)),
            (false, true) => self.origin.delete(self.now, &key),
            (false, false) => self.origin.put(self.now, &key, size, &mut self.writers),
        }
        if !through_gateway {
            self.notify(&key);
            if self.trace {
                let etag = self.origin.current(&key).map(|object| &object.etag);
                eprintln!("{} S3 write of {key:?}: {etag:?}", self.now);
            }
        }
        self.summary.writes += 1;
        // A delete may be followed by a purge through the key's home.
        if !through_gateway
            && delete
            && self.purges.percent(self.options.purge_percent)
            && let Some(home) = self.current_ring().owner(Placement::Home(&key).hash())
        {
            self.summary.purges += 1;
            self.purge_at(home.0 as usize, &key)?;
        }
        Ok(())
    }

    /// S3 applies the writes nodes sent it during the last tick. The model
    /// of S3 keeps one state per key per tick, so writes land as a tick
    /// starts, before any read in it.
    fn apply_origin_writes(&mut self) {
        for message in std::mem::take(&mut self.origin_writes) {
            let Message::OriginWrite {
                node,
                run,
                gateway,
                change,
                key,
                size,
            } = message
            else {
                unreachable!("only S3 writes wait for the tick");
            };
            let fails = self.faulty && self.write_errors.percent(self.options.origin_error_percent);
            let status = match (fails, size) {
                (true, _) => 503,
                (false, Some(size)) => {
                    self.origin
                        .put(self.now, &key, size, &mut self.gateway_writes);
                    200
                }
                (false, None) => {
                    self.origin.delete(self.now, &key);
                    204
                }
            };
            if status < 300 {
                self.notify(&key);
            }
            let upload = match (status, size) {
                (200, Some(size)) => {
                    let etag = self
                        .origin
                        .current(&key)
                        .expect("just written")
                        .etag
                        .clone();
                    Some((etag, size))
                }
                _ => None,
            };
            if self.trace {
                eprintln!(
                    "{} write {change} reached S3 via node {node}: {status}",
                    self.now
                );
            }
            let response = Message::OriginWriteResponse {
                run,
                gateway,
                change,
                key,
                status,
                applied: self.now,
                upload,
            };
            self.send(Address::Origin, Address::Node(node), response);
        }
    }

    /// A random client writes `key` through a random gateway: `size` bytes,
    /// or a delete for `None`.
    fn write_through_gateway(&mut self, key: ObjectKey, size: Option<u64>) {
        let client = self.gateway_writes.index(self.options.clients);
        let gateway = self.gateway_writes.index(self.options.gateways);
        self.issue_change(client, gateway, key, size);
    }

    fn issue_change(
        &mut self,
        client: usize,
        gateway: usize,
        key: ObjectKey,
        size: Option<u64>,
    ) -> u64 {
        let change = self.next_change;
        self.next_change += 1;
        if self.trace {
            eprintln!(
                "{} write {change} of {key:?} ({size:?}) via gateway {gateway}",
                self.now
            );
        }
        let entry = Change {
            client,
            gateway,
            key: key.clone(),
            size,
            issued: self.now,
            candidates: Vec::new(),
            ring: 0,
        };
        self.changes.insert(change, entry);
        let message = Message::ClientWrite { change, key };
        self.send(Address::Client(client), Address::Gateway(gateway), message);
        change
    }

    /// Sends a write to the next node the gateway has yet to try, or
    /// answers 503 when none is left.
    fn pass_change(&mut self, gateway: usize, change: u64) {
        let Some(entry) = self.changes.get_mut(&change) else {
            return;
        };
        let (client, key, size) = (entry.client, entry.key.clone(), entry.size);
        if entry.candidates.is_empty() {
            let answer = Message::ClientWriteResponse {
                change,
                status: 503,
                applied: self.now,
            };
            self.send(Address::Gateway(gateway), Address::Client(client), answer);
            return;
        }
        let node = entry.candidates.remove(0).0 as usize;
        let message = Message::NodeWrite {
            gateway,
            change,
            key,
            size,
        };
        self.send(Address::Gateway(gateway), Address::Node(node), message);
    }

    fn tick_clients(&mut self) {
        let timeout = self.options.client_timeout;
        let late: Vec<u64> = self
            .requests
            .iter()
            .filter(|(_, pending)| pending.sent + timeout <= self.now)
            .map(|(&request, _)| request)
            .collect();
        for request in late {
            self.summary.client_retries += 1;
            self.attempt(request);
        }
        // A write whose answer never comes may or may not have happened.
        let now = self.now;
        self.changes
            .retain(|_, change| change.issued + timeout > now);
        for client in 0..self.options.clients {
            if self.issued == self.options.requests
                || self.in_flight[client] == self.options.requests_in_flight
                || !self.workload.percent(self.options.request_percent)
            {
                continue;
            }
            let read = self.random_request();
            let gateway = self.workload.index(self.options.gateways);
            self.issue(client, gateway, read);
        }
    }

    fn issue(&mut self, client: usize, gateway: usize, read: Request) -> u64 {
        let request = self.issued;
        self.issued += 1;
        self.in_flight[client] += 1;
        let pending = Pending {
            client,
            read,
            issued: self.now,
            sent: self.now,
        };
        self.requests.insert(request, pending);
        self.send_attempt(request, gateway);
        request
    }

    /// Sends a request again, through any gateway.
    fn attempt(&mut self, request: u64) {
        let gateway = self.retries.index(self.options.gateways);
        self.send_attempt(request, gateway);
    }

    fn send_attempt(&mut self, request: u64, gateway: usize) {
        let pending = self.requests.get_mut(&request).expect("a pending request");
        pending.sent = self.now;
        let (client, read) = (pending.client, pending.read.clone());
        let attempt = self.next_attempt;
        self.next_attempt += 1;
        let sent = self.now;
        let entry = Attempt {
            request,
            client,
            gateway,
            sent,
        };
        self.attempts.insert(attempt, entry);
        if self.trace {
            eprintln!(
                "{} request {request} attempt {attempt} via gateway {gateway}: {read:?}",
                self.now
            );
        }
        let message = Message::ClientRequest {
            request: attempt,
            read,
        };
        self.send(Address::Client(client), Address::Gateway(gateway), message);
    }

    /// A read of a random key, with ranges and preconditions that S3 may
    /// satisfy or reject.
    fn random_request(&mut self) -> Request {
        let key = match self.hot_reads.percent(self.options.hot_percent) {
            true => self.keys[0].clone(),
            false => self.keys[self.workload.index(self.keys.len())].clone(),
        };
        let span = self.options.object_size_max + 1;
        let range = match self.workload.below(6) {
            0 => {
                let first = self.workload.below(span);
                let last = first + self.workload.below(span);
                Some(ByteRange::Inclusive { first, last })
            }
            // Invalid: S3 ignores it.
            5 if span > 1 => {
                let last = self.workload.below(span - 1);
                let first = last + 1 + self.workload.below(span - last - 1);
                Some(ByteRange::Inclusive { first, last })
            }
            1 => Some(ByteRange::From {
                first: self.workload.below(span),
            }),
            2 => Some(ByteRange::Suffix {
                length: self.workload.range(1..=span),
            }),
            _ => None,
        };
        let current = self.origin.current(&key).map(|object| object.etag.clone());
        let precondition = |prng: &mut Prng| match prng.below(20) {
            0 | 1 => current.clone(),
            2 => Some(ETag("\"stale\"".into())),
            _ => None,
        };
        let if_match = precondition(&mut self.workload);
        let if_none_match = precondition(&mut self.workload);
        let method = if self.workload.percent(15) {
            Method::Head
        } else {
            Method::Get
        };
        Request {
            method,
            key,
            range,
            if_match,
            if_none_match,
        }
    }

    fn handle(&mut self, event: Event) -> Result<(), Failure> {
        match event {
            Event::Deliver { to, message } => self.deliver(to, message),
            Event::Written { node, run, .. }
            | Event::Sent { node, run, .. }
            | Event::Verified { node, run, .. }
            | Event::SpotRead { node, run, .. }
            | Event::MembershipTimer { node, run, .. }
            | Event::JoinTimeout { node, run }
                if run != self.runs[node] =>
            {
                Ok(())
            }
            Event::JoinTimeout { node, .. } => self.finish_joining(node, None),
            Event::MembershipTimer { node, timer, .. } => {
                if let Some(membership) = self.memberships[node].as_mut() {
                    membership.on_timer(Time(self.now), timer);
                }
                self.drain_membership(node)
            }
            Event::Written { node, location, .. } => self.written(node, location),
            Event::Forwarded {
                gateway,
                request,
                from,
                bytes,
            } => self.forwarded(gateway, request, from, bytes),
            Event::Sent { node, id, .. } => self.sent(node, id),
            Event::SpotRead {
                node,
                version,
                parts,
                ..
            } => {
                let bytes = parts
                    .iter()
                    .flat_map(|&(location, offset, len)| {
                        self.disks[node].read(location, offset, len)
                    })
                    .copied()
                    .collect();
                let now = Time(self.now);
                self.node(node).on_spot(now, version, bytes);
                self.drain_node(node)
            }
            Event::Verified {
                node,
                location,
                len,
                checksum,
                ..
            } => {
                let intact = self.disks[node].checksum(location, len) == checksum;
                let now = Time(self.now);
                self.node(node).on_verified(now, location, intact);
                self.drain_node(node)
            }
        }
    }

    fn deliver(&mut self, to: Address, message: Message) -> Result<(), Failure> {
        let now = Time(self.now);
        if let (Address::Node(node), Message::RingRequest { from }) = (to, &message)
            && self.nodes[node].is_none()
            && matches!(from, Address::Gateway(_))
        {
            let from = *from;
            self.send(to, from, Message::RingFailed);
            return Ok(());
        }
        if let (Address::Node(node), Message::NodeRequest { gateway, id, .. }) = (to, &message)
            && self.nodes[node].is_none()
        {
            let refused = Message::NodeRefused { id: *id };
            self.send(to, Address::Gateway(*gateway), refused);
            return Ok(());
        }
        if let (
            Address::Node(node),
            Message::NodeWrite {
                gateway, change, ..
            },
        ) = (to, &message)
            && self.nodes[node].is_none()
        {
            let refused = Message::NodeWriteRefused { change: *change };
            self.send(to, Address::Gateway(*gateway), refused);
            return Ok(());
        }
        if let Address::Node(node) = to {
            let stale = match &message {
                Message::OriginResponse { run, .. }
                | Message::OriginWriteResponse { run, .. }
                | Message::EventHeard { run, .. }
                | Message::PurgeConfirmed { run, .. }
                | Message::PeerResponse { run, .. }
                | Message::PeerMetadata { run, .. } => *run != self.runs[node],
                _ => false,
            };
            if self.nodes[node].is_none() || stale {
                self.summary.lost += 1;
                return Ok(());
            }
        }
        match (to, message) {
            (Address::Gateway(gateway), Message::ClientRequest { request, read }) => {
                let id = ClientRequestId(self.next_id());
                self.gateway_requests.insert((gateway, id), request);
                self.gateways[gateway].on_request(now, id, read);
                self.drain_gateway(gateway)
            }
            (
                Address::Gateway(gateway),
                Message::NodeResponse {
                    id,
                    head,
                    body,
                    meta,
                    hot,
                    ring: (node, version, down),
                },
            ) => {
                self.gateway_bodies.insert((gateway, id), body);
                self.gateways[gateway].on_hot(now, hot);
                self.gateways[gateway].on_node_response(now, id, head, meta);
                self.gateways[gateway].on_ring_version(now, NodeId(node as u64), version, down);
                self.drain_gateway(gateway)
            }
            (
                Address::Gateway(gateway),
                Message::NodeMetadata {
                    id,
                    meta,
                    hot,
                    ring: (node, version, down),
                },
            ) => {
                self.gateways[gateway].on_hot(now, hot);
                self.gateways[gateway].on_node_metadata(now, id, meta);
                self.gateways[gateway].on_ring_version(now, NodeId(node as u64), version, down);
                self.drain_gateway(gateway)
            }
            (
                Address::Gateway(gateway),
                Message::NodeStale {
                    id,
                    ring: (node, version, down),
                },
            ) => {
                self.summary.retries += 1;
                self.gateways[gateway].on_node_stale(now, id);
                self.gateways[gateway].on_ring_version(now, NodeId(node as u64), version, down);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::RingResponse { ring, down }) => {
                self.summary.ring_fetches += 1;
                self.gateways[gateway].on_ring(now, ring, down);
                self.drain_gateway(gateway)
            }
            (Address::Node(node), Message::RingResponse { ring, .. }) => {
                self.finish_joining(node, Some(ring))
            }
            (Address::Gateway(gateway), Message::NodeRefused { id }) => {
                self.summary.refused += 1;
                self.gateways[gateway].on_node_unreachable(now, id);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::RingFailed) => {
                self.gateways[gateway].on_ring_failed(now);
                self.drain_gateway(gateway)
            }
            (Address::Node(node), Message::WriteNotice { key }) => {
                self.node(node).on_write_from(now, &key, true);
                self.drain_node(node)
            }
            (Address::Gateway(gateway), Message::ClientWrite { change, key }) => {
                let candidates = self.gateways[gateway].pass_candidates(&key);
                let ring = self.gateways[gateway].ring().version();
                if self.trace {
                    let home = self.gateways[gateway]
                        .ring()
                        .owner(Placement::Home(&key).hash());
                    eprintln!(
                        "{} write {change} candidates {candidates:?} under ring {ring:x}, home {home:?}",
                        self.now
                    );
                }
                if let Some(entry) = self.changes.get_mut(&change) {
                    (entry.candidates, entry.ring) = (candidates, ring);
                }
                self.pass_change(gateway, change);
                Ok(())
            }
            (
                Address::Node(node),
                Message::Lease {
                    owner,
                    placement,
                    until,
                },
            ) => {
                self.node(node)
                    .on_lease(now, placement, NodeId(owner as u64), Time(until));
                self.drain_node(node)
            }
            (Address::Node(node), Message::LeaseReport { placement, reads }) => {
                self.node(node).on_lease_report(now, placement, reads);
                self.drain_node(node)
            }
            (Address::Node(to), Message::PurgeNotice { from, run, key }) => {
                self.node(to).on_purge(now, &key, true);
                self.drain_node(to)?;
                self.purged(to, &key)?;
                let confirmed = Message::PurgeConfirmed { run, key, from: to };
                self.send(Address::Node(to), Address::Node(from), confirmed);
                Ok(())
            }
            (Address::Node(node), Message::PurgeConfirmed { key, from, .. }) => {
                self.node(node)
                    .on_purge_confirmed(now, &key, NodeId(from as u64));
                self.drain_node(node)
            }
            (Address::Node(node), Message::QueueEvent { message, key, etag }) => {
                self.node(node).on_event(now, EventId(message), key, etag);
                self.drain_node(node)
            }
            (
                Address::Node(to),
                Message::EventNotice {
                    node,
                    run,
                    event,
                    key,
                    etag,
                },
            ) => {
                if self.trace {
                    eprintln!(
                        "{} node {to} hears {event:?} from node {node}: {key:?} {etag:?}",
                        self.now
                    );
                }
                self.node(to).on_event_notice(now, &key, etag.as_ref());
                self.drain_node(to)?;
                let heard = Message::EventHeard {
                    run,
                    event,
                    from: to,
                };
                self.send(Address::Node(to), Address::Node(node), heard);
                Ok(())
            }
            (Address::Node(node), Message::EventHeard { event, from, .. }) => {
                self.node(node)
                    .on_event_passed(now, event, NodeId(from as u64), true);
                self.drain_node(node)
            }
            (Address::Origin, Message::DeleteEvent { message, ring }) => {
                if let Some(notification) = self.notifications.remove(&message) {
                    self.summary.events += 1;
                    if self.trace {
                        eprintln!("{} event {message} finished under ring {ring:x}", self.now);
                    }
                    let finished = (notification.applied, self.now, ring);
                    self.finished_events
                        .entry(notification.key)
                        .or_default()
                        .push(finished);
                }
                Ok(())
            }
            (Address::Gateway(gateway), Message::NodeWriteRefused { change }) => {
                self.pass_change(gateway, change);
                Ok(())
            }
            (
                Address::Node(node),
                Message::NodeWrite {
                    gateway,
                    change,
                    key,
                    size,
                },
            ) => {
                if self.trace {
                    eprintln!("{} node {node} takes write {change}", self.now);
                }
                let run = self.runs[node];
                let message = Message::OriginWrite {
                    node,
                    run,
                    gateway,
                    change,
                    key,
                    size,
                };
                self.send(Address::Node(node), Address::Origin, message);
                Ok(())
            }
            (Address::Origin, message @ Message::OriginWrite { .. }) => {
                self.origin_writes.push(message);
                Ok(())
            }
            (
                Address::Node(node),
                Message::OriginWriteResponse {
                    gateway,
                    change,
                    key,
                    status,
                    applied,
                    upload,
                    ..
                },
            ) => {
                // The home learns of the write before the gateway does, and
                // stores the upload's home region if it warms on write.
                if status < 300 {
                    self.node(node).on_write(now, &key);
                    if let Some((etag, size)) = upload
                        && let Some(kept) =
                            self.node(node)
                                .on_uploaded(now, key.clone(), etag.clone(), size)
                    {
                        let object = self.origin.version(&etag).expect("uploaded");
                        let bytes = Body::whole(object.bytes(0..size));
                        self.origin_bodies.insert((node, kept), bytes);
                    }
                    self.drain_node(node)?;
                }
                let ring = self.stamp(node);
                let response = Message::NodeWriteResponse {
                    change,
                    key,
                    status,
                    applied,
                    ring,
                };
                self.send(Address::Node(node), Address::Gateway(gateway), response);
                Ok(())
            }
            (
                Address::Gateway(gateway),
                Message::NodeWriteResponse {
                    change,
                    key,
                    status,
                    applied,
                    ring: (node, version, down),
                },
            ) => {
                if status < 300 {
                    let via = NodeId(node as u64);
                    let home = self.gateways[gateway]
                        .ring()
                        .owner(Placement::Home(&key).hash());
                    if home != Some(via) {
                        self.summary.detoured_writes += 1;
                    }
                    self.gateways[gateway].on_write(now, &key, Some(via));
                }
                self.gateways[gateway].on_ring_version(now, NodeId(node as u64), version, down);
                self.drain_gateway(gateway)?;
                if let Some(entry) = self.changes.get(&change) {
                    let client = entry.client;
                    let response = Message::ClientWriteResponse {
                        change,
                        status,
                        applied,
                    };
                    self.send(Address::Gateway(gateway), Address::Client(client), response);
                }
                Ok(())
            }
            (
                Address::Client(_),
                Message::ClientWriteResponse {
                    change,
                    status,
                    applied,
                },
            ) => {
                let Some(entry) = self.changes.remove(&change) else {
                    return Ok(());
                };
                if self.trace {
                    eprintln!("{} write {change} answered {status}", self.now);
                }
                if let Some(watched) = self.watched_changes.get_mut(&change) {
                    *watched = Some(status);
                }
                if status < 300 {
                    self.summary.gateway_writes += 1;
                    let heard = Acknowledged {
                        applied,
                        heard: self.now,
                        ring: entry.ring,
                    };
                    let latest = self
                        .acknowledged
                        .entry((entry.gateway, entry.key))
                        .or_insert(heard);
                    if latest.applied <= applied {
                        *latest = heard;
                    }
                }
                Ok(())
            }
            (Address::Node(node), Message::Gossip { packet }) => {
                if let Some(membership) = self.memberships[node].as_mut() {
                    membership.on_packet(now, &packet);
                }
                self.drain_membership(node)
            }
            (Address::Node(node), Message::RingRequest { from }) => {
                let ring = self.node(node).ring().clone();
                let down = self.down_of(node);
                let response = Message::RingResponse { ring, down };
                self.send(Address::Node(node), from, response);
                Ok(())
            }
            (Address::Node(node), Message::NodeRequest { gateway, id, read }) => {
                let local = GatewayRequestId(self.next_id());
                let requester = Requester::Gateway(gateway, id);
                self.node_requests.insert((node, local), requester);
                self.node(node).on_request(now, local, read);
                self.drain_node(node)
            }
            (
                Address::Node(peer),
                Message::PeerRequest {
                    node,
                    run,
                    origin,
                    read,
                },
            ) => {
                let local = GatewayRequestId(self.next_id());
                let requester = Requester::Peer { node, run, origin };
                self.node_requests.insert((peer, local), requester);
                self.node(peer).on_request(now, local, read);
                self.drain_node(peer)
            }
            (
                Address::Node(node),
                Message::PeerResponse {
                    origin, head, body, ..
                },
            ) => {
                let asked_metadata = self.peer_reads.remove(&(node, origin));
                if self.cancelled.remove(&(node, origin)) {
                    return Ok(());
                }
                match asked_metadata {
                    Some(true) => self.node(node).on_peer_metadata(now, origin, None),
                    Some(false) => {
                        self.origin_bodies.insert((node, origin), body);
                        self.node(node).on_origin_response(now, origin, head);
                    }
                    None => {
                        return Err(self.failure(format!(
                            "node {node} got an answer to {origin:?}, which it never sent"
                        )));
                    }
                }
                self.drain_node(node)
            }
            (Address::Node(node), Message::PeerMetadata { origin, meta, .. }) => {
                self.peer_reads.remove(&(node, origin));
                if self.cancelled.remove(&(node, origin)) {
                    return Ok(());
                }
                self.node(node).on_peer_metadata(now, origin, Some(meta));
                self.drain_node(node)
            }
            (
                Address::Node(node),
                Message::OriginResponse {
                    origin, head, body, ..
                },
            ) => {
                if self.cancelled.remove(&(node, origin)) {
                    return Ok(());
                }
                self.origin_bodies.insert((node, origin), body);
                self.node(node).on_origin_response(now, origin, head);
                self.drain_node(node)?;
                if self.streaming.remove(&(node, origin)) {
                    self.started.insert((node, origin));
                }
                Ok(())
            }
            (
                Address::Origin,
                Message::OriginRequest {
                    node,
                    run,
                    origin,
                    read,
                },
            ) => {
                let (head, body) = match self.faulty
                    && self
                        .origin_errors
                        .percent(self.options.origin_error_percent)
                {
                    true => (ResponseHead::status(503), Vec::new()),
                    false => {
                        if let Some((key, size)) = self.write_before_answer.take() {
                            self.put(&key, size);
                        }
                        self.origin.respond_now(&read)
                    }
                };
                let mut body = Body::whole(body);
                if let Some(&at) = self.cut_origin_responses.get(&node)
                    && body.len > at
                {
                    self.cut_origin_responses.remove(&node);
                    body.bytes.truncate(at as usize);
                } else if self.faulty
                    && body.len > 0
                    && self.cuts.percent(self.options.origin_cut_percent)
                {
                    body.bytes.truncate(self.cuts.below(body.len) as usize);
                }
                let response = Message::OriginResponse {
                    run,
                    origin,
                    head,
                    body,
                };
                self.send(Address::Origin, Address::Node(node), response);
                Ok(())
            }
            (
                Address::Client(_),
                Message::ClientResponse {
                    request,
                    head,
                    body,
                },
            ) => self.answer(request, head, body),
            (to, message) => Err(self.failure(format!("{to:?} received {message:?}"))),
        }
    }

    fn drain_gateway(&mut self, gateway: usize) -> Result<(), Failure> {
        let version = self.gateways[gateway].ring().version();
        if self.gateway_rings[gateway].0 != version {
            self.gateway_rings[gateway] = (version, self.now);
            if self.trace {
                let members = members(self.gateways[gateway].ring());
                eprintln!(
                    "{} gateway {gateway} ring {version:016x} {members:?}",
                    self.now
                );
            }
        }
        for action in self.gateways[gateway].drain() {
            match action {
                gateway::Action::Send { node, id, read } => {
                    let node = self.route(node.0 as usize, &read);
                    if self.trace {
                        eprintln!(
                            "{} gateway {gateway} sends node {node} {id:?}: {read:?}",
                            self.now
                        );
                    }
                    let message = Message::NodeRequest { gateway, id, read };
                    self.send(Address::Gateway(gateway), Address::Node(node), message);
                }
                gateway::Action::Start { request, head } => {
                    let started = (head, Vec::new());
                    if self
                        .client_responses
                        .insert((gateway, request), started)
                        .is_some()
                    {
                        return Err(
                            self.failure(format!("gateway {gateway} started {request:?} twice"))
                        );
                    }
                }
                gateway::Action::Forward { request, from, len } => {
                    let Some(body) = self.gateway_bodies.remove(&(gateway, from)) else {
                        return Err(self.failure(format!(
                            "gateway {gateway} forwarded {from:?}, which it no longer holds"
                        )));
                    };
                    if body.len != len {
                        return Err(self.failure(format!(
                            "gateway {gateway} forwarded {len} bytes of a {}-byte body",
                            body.len
                        )));
                    }
                    if (body.bytes.len() as u64) < len {
                        self.summary.cut_bodies += 1;
                    }
                    let delay = self.send_delays.range(0..=self.options.send_delay_max);
                    let forwarded = Event::Forwarded {
                        gateway,
                        request,
                        from,
                        bytes: body.bytes,
                    };
                    if self.holding_forwards {
                        self.held_forwards.push(forwarded);
                    } else {
                        self.queue.push(self.now + delay, forwarded);
                    }
                }
                gateway::Action::Abort { request } => {
                    let Some((head, body)) = self.client_responses.remove(&(gateway, request))
                    else {
                        return Err(self
                            .failure(format!("gateway {gateway} aborted {request:?} unstarted")));
                    };
                    self.respond_to_client(gateway, request, head, body)?;
                }
                gateway::Action::Respond { request, head } => {
                    self.respond_to_client(gateway, request, head, Vec::new())?;
                }
                gateway::Action::Discard { id } => {
                    if self.gateway_bodies.remove(&(gateway, id)).is_none() {
                        return Err(
                            self.failure(format!("gateway {gateway} discarded {id:?} twice"))
                        );
                    }
                }
                // A gateway knows the nodes in the cluster, as if its
                // config were kept current, and asks them in turn.
                gateway::Action::FindRing => {
                    let members: Vec<usize> = self.members().collect();
                    let node = members[self.ring_seeds % members.len()];
                    self.ring_seeds += 1;
                    let message = Message::RingRequest {
                        from: Address::Gateway(gateway),
                    };
                    self.send(Address::Gateway(gateway), Address::Node(node), message);
                }
                gateway::Action::FetchRing { node } => {
                    let message = Message::RingRequest {
                        from: Address::Gateway(gateway),
                    };
                    self.send(
                        Address::Gateway(gateway),
                        Address::Node(node.0 as usize),
                        message,
                    );
                }
            }
        }
        Ok(())
    }

    /// Where a read goes: its node, or sometimes, for a range, another one.
    fn route(&mut self, node: usize, read: &Read) -> usize {
        // Another node that is up: a gateway's ring may name a different
        // owner, and the gateway blames the node it meant for any failure.
        let others: Vec<usize> = (0..self.nodes.len())
            .filter(|&other| other != node && self.nodes[other].is_some())
            .collect();
        let misroute = matches!(read, Read::Range(_))
            && !others.is_empty()
            && self.misroutes.percent(self.options.misroute_percent);
        match misroute {
            true => others[self.misroutes.index(others.len())],
            false => node,
        }
    }

    fn respond_to_client(
        &mut self,
        gateway: usize,
        id: ClientRequestId,
        head: ResponseHead,
        body: Vec<u8>,
    ) -> Result<(), Failure> {
        let Some(request) = self.gateway_requests.remove(&(gateway, id)) else {
            return Err(self.failure(format!("gateway {gateway} answered {id:?} twice")));
        };
        let client = self.attempts[&request].client;
        let response = Message::ClientResponse {
            request,
            head,
            body,
        };
        self.send(Address::Gateway(gateway), Address::Client(client), response);
        Ok(())
    }

    /// A streaming S3 body passes through as it arrives, so once its head's
    /// arrival has been handled, nothing more may read it.
    fn check_streaming(&self, node: usize, origin: OriginRequestId) -> Result<(), Failure> {
        if self.started.contains(&(node, origin)) {
            return Err(self.failure(format!(
                "node {node} read S3's streaming body to {origin:?} after it started"
            )));
        }
        Ok(())
    }

    /// A gateway copied `bytes` of a node's body into a client's response,
    /// which goes to the client once complete.
    fn forwarded(
        &mut self,
        gateway: usize,
        request: ClientRequestId,
        from: NodeRequestId,
        bytes: Vec<u8>,
    ) -> Result<(), Failure> {
        let Some((head, body)) = self.client_responses.get_mut(&(gateway, request)) else {
            return Err(self.failure(format!(
                "gateway {gateway} forwarded into {request:?} unstarted"
            )));
        };
        body.extend_from_slice(&bytes);
        let (sent, expected) = (body.len() as u64, head.content_length);
        if sent > expected {
            return Err(self.failure(format!(
                "gateway {gateway} sent {sent} bytes of a {expected}-byte response"
            )));
        }
        if sent == expected {
            let (head, body) = self
                .client_responses
                .remove(&(gateway, request))
                .expect("a started response");
            self.respond_to_client(gateway, request, head, body)?;
        }
        self.gateways[gateway].on_forwarded(Time(self.now), from, bytes.len() as u64);
        self.drain_gateway(gateway)
    }

    fn node(&mut self, node: usize) -> &mut Node {
        self.nodes[node].as_mut().expect("the node is up")
    }

    fn drain_node(&mut self, node: usize) -> Result<(), Failure> {
        let run = self.runs[node];
        for action in self.node(node).drain() {
            match action {
                node::Action::Fetch {
                    origin,
                    request,
                    streams,
                } => {
                    let chunk = self.options.block_size * self.options.chunk_blocks;
                    match (streams, request.range) {
                        (true, _) => {
                            self.streaming.insert((node, origin));
                        }
                        (false, Some(ByteRange::Inclusive { first, last }))
                            if last - first < chunk => {}
                        (false, range) => {
                            return Err(self.failure(format!(
                                "node {node} held a fill of {range:?}, more than a chunk"
                            )));
                        }
                    }
                    let message = Message::OriginRequest {
                        node,
                        run,
                        origin,
                        read: request,
                    };
                    self.send(Address::Node(node), Address::Origin, message);
                }
                node::Action::Respond {
                    request,
                    head,
                    body,
                    meta,
                    hot,
                } => {
                    if self.trace {
                        let etag = meta.as_ref().map(|meta| &meta.etag);
                        eprintln!(
                            "{} node {node} answers {request:?} {} {:?} with metadata {etag:?}",
                            self.now, head.status, head.etag
                        );
                    }
                    for segment in &body {
                        if let Segment::Origin { origin, .. } = segment {
                            self.check_streaming(node, *origin)?;
                        }
                    }
                    let delay = self.send_delays.range(0..=self.options.send_delay_max);
                    let sending = Sending {
                        head,
                        body,
                        meta,
                        hot,
                    };
                    self.sending.insert((node, request), sending);
                    let sent = Event::Sent {
                        node,
                        run,
                        id: request,
                    };
                    self.queue.push(self.now + delay, sent);
                }
                node::Action::Metadata { request, meta, hot } => {
                    let Some(requester) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    self.answer_request(node, requester, NodeAnswer::Metadata(meta, hot));
                }
                node::Action::GrantLease {
                    node: to,
                    placement,
                    until,
                } => {
                    let message = Message::Lease {
                        owner: node,
                        placement,
                        until: until.0,
                    };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                node::Action::ReportLease {
                    node: to,
                    placement,
                    reads,
                } => {
                    let message = Message::LeaseReport { placement, reads };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                node::Action::Stale { request } => {
                    let Some(requester) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    self.answer_request(node, requester, NodeAnswer::Stale);
                }
                node::Action::PassWrite { node: to, key } => {
                    let message = Message::WriteNotice { key };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                node::Action::PassEvent {
                    event,
                    node: to,
                    key,
                    etag,
                } => {
                    let message = Message::EventNotice {
                        node,
                        run,
                        event,
                        key,
                        etag,
                    };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                node::Action::EventDone { event } => {
                    let ring = self.node(node).ring().version();
                    let message = Message::DeleteEvent {
                        message: event.0,
                        ring,
                    };
                    self.send(Address::Node(node), Address::Origin, message);
                }
                node::Action::PeerFetch { origin, peer, read } => {
                    let asks_metadata = matches!(read, Read::Known(_));
                    self.peer_reads.insert((node, origin), asks_metadata);
                    let message = Message::PeerRequest {
                        node,
                        run,
                        origin,
                        read,
                    };
                    self.send(Address::Node(node), Address::Node(peer.0 as usize), message);
                }
                node::Action::Write {
                    location,
                    origin,
                    offset,
                    len,
                } => {
                    self.check_streaming(node, origin)?;
                    self.check_owned(node, location)?;
                    let write = Write {
                        origin,
                        offset,
                        len,
                    };
                    if self.writes.insert((node, location), write).is_some() {
                        return Err(
                            self.failure(format!("node {node} wrote {location:?} twice at once"))
                        );
                    }
                    let delay = self.disk_delays.range(0..=self.options.disk_delay_max);
                    let written = Event::Written {
                        node,
                        run,
                        location,
                    };
                    self.queue.push(self.now + delay, written);
                }
                node::Action::Record { location, record } => {
                    self.disks[node].record(location, record);
                }
                node::Action::Clear { location } => self.disks[node].clear(location),
                node::Action::Erase { location, len } => self.disks[node].erase(location, len),
                node::Action::SavePurge { key, nodes } => {
                    self.disks[node].purges.push((key, nodes))
                }
                node::Action::PassPurge { node: to, key } => {
                    let message = Message::PurgeNotice {
                        from: node,
                        run,
                        key,
                    };
                    self.send(Address::Node(node), Address::Node(to.0 as usize), message);
                }
                node::Action::Remember { key, meta } => {
                    let delay = self.disk_delays.range(0..=self.options.disk_delay_max);
                    self.disks[node].append(self.now + delay, key, Some(meta));
                }
                node::Action::Forget { key } => {
                    let delay = self.disk_delays.range(0..=self.options.disk_delay_max);
                    self.disks[node].append(self.now + delay, key, None);
                }
                node::Action::RewriteMetadata { entries } => {
                    self.disks[node].rewrite_metadata(entries);
                }
                node::Action::Verify {
                    location,
                    len,
                    checksum,
                } => {
                    let delay = self.disk_delays.range(0..=self.options.disk_delay_max);
                    let verified = Event::Verified {
                        node,
                        run,
                        location,
                        len,
                        checksum,
                    };
                    self.queue.push(self.now + delay, verified);
                }
                node::Action::ReadSpot { version, parts } => {
                    let delay = self.disk_delays.range(0..=self.options.disk_delay_max);
                    let read = Event::SpotRead {
                        node,
                        run,
                        version,
                        parts,
                    };
                    self.queue.push(self.now + delay, read);
                }
                node::Action::Cancel { origin } => {
                    if self.trace {
                        eprintln!("{} node {node} cancelled {origin:?}", self.now);
                    }
                    self.cancelled.insert((node, origin));
                    self.streaming.remove(&(node, origin));
                }
                node::Action::Release { origin } => {
                    self.started.remove(&(node, origin));
                    if self.origin_bodies.remove(&(node, origin)).is_none() {
                        return Err(self.failure(format!("node {node} released {origin:?} twice")));
                    }
                }
            }
        }
        Ok(())
    }

    /// Copies a write's bytes from the body it names, as the server would
    /// once the body arrives, and tells the node they are durable.
    fn written(&mut self, node: usize, location: Location) -> Result<(), Failure> {
        let write = self
            .writes
            .remove(&(node, location))
            .expect("a write was scheduled");
        let Some(body) = self.origin_bodies.get(&(node, write.origin)) else {
            return Err(self.failure(format!(
                "node {node} released {:?} before writing it",
                write.origin
            )));
        };
        if write.offset + write.len > body.len {
            return Err(self.failure(format!(
                "node {node} wrote past the end of {:?}",
                write.origin
            )));
        }
        let Some(bytes) = body
            .bytes
            .get(write.offset as usize..(write.offset + write.len) as usize)
        else {
            self.node(node).on_write_failed(location);
            return self.drain_node(node);
        };
        let bytes = bytes.to_vec();
        self.disks[node].write(location, &bytes);
        self.node(node).on_written(location);
        self.check_disk(node, Some(location))?;
        self.drain_node(node)
    }

    /// Reads a response body as `sendfile` and `splice` would, at the moment
    /// it is sent, and forwards it. While faults happen, the connection may
    /// drop partway.
    fn sent(&mut self, node: usize, id: GatewayRequestId) -> Result<(), Failure> {
        let Sending {
            head,
            body,
            meta,
            hot,
        } = self
            .sending
            .remove(&(node, id))
            .expect("a send was scheduled");
        let mut body = self.assemble(node, &body)?;
        if let Some(&at) = self.cut_responses.get(&node)
            && body.len > at
        {
            self.cut_responses.remove(&node);
            body.bytes.truncate(at as usize);
        } else if self.faulty && body.len > 0 && self.cuts.percent(self.options.cut_percent) {
            body.bytes.truncate(self.cuts.below(body.len) as usize);
        }
        let Some(requester) = self.node_requests.remove(&(node, id)) else {
            return Err(self.failure(format!("node {node} answered {id:?} twice")));
        };
        let answer = NodeAnswer::Response {
            head,
            body,
            meta,
            hot,
        };
        self.answer_request(node, requester, answer);
        self.node(node).on_sent(id);
        self.drain_node(node)
    }

    /// What a node's answers to gateways carry: the node, its ring's
    /// version, and the version of the nodes its membership holds down.
    fn stamp(&self, node: usize) -> (usize, u64, u64) {
        let up = self.nodes[node].as_ref().expect("the node is up");
        (node, up.ring().version(), down_version(&self.down_of(node)))
    }

    /// The nodes `node`'s membership holds down; none without membership.
    fn down_of(&self, node: usize) -> Vec<NodeId> {
        self.memberships[node]
            .as_ref()
            .map(Membership::down)
            .unwrap_or_default()
    }

    /// Sends a node's answer to whoever asked: a gateway, with the node's
    /// ring version, or a node reading from a previous owner.
    fn answer_request(&mut self, node: usize, requester: Requester, answer: NodeAnswer) {
        let from = Address::Node(node);
        match requester {
            Requester::Gateway(gateway, id) => {
                let ring = self.stamp(node);
                let message = match answer {
                    NodeAnswer::Response {
                        head,
                        body,
                        meta,
                        hot,
                    } => Message::NodeResponse {
                        id,
                        head,
                        body,
                        meta,
                        hot,
                        ring,
                    },
                    NodeAnswer::Metadata(meta, hot) => Message::NodeMetadata {
                        id,
                        meta,
                        hot,
                        ring,
                    },
                    NodeAnswer::Stale => Message::NodeStale { id, ring },
                };
                self.send(from, Address::Gateway(gateway), message);
            }
            Requester::Peer {
                node: asker,
                run,
                origin,
            } => {
                let message = match answer {
                    NodeAnswer::Response { head, body, .. } => Message::PeerResponse {
                        run,
                        origin,
                        head,
                        body,
                    },
                    NodeAnswer::Metadata(meta, _) => Message::PeerMetadata { run, origin, meta },
                    NodeAnswer::Stale => Message::PeerResponse {
                        run,
                        origin,
                        head: ResponseHead::status(503),
                        body: Body::whole(Vec::new()),
                    },
                };
                self.send(from, Address::Node(asker), message);
            }
        }
    }

    /// A node response's body from its segments. It ends early where an S3
    /// body it reads ended early.
    fn assemble(&self, node: usize, segments: &[Segment]) -> Result<Body, Failure> {
        let mut bytes = Vec::new();
        let mut cut = false;
        let mut len = 0;
        for &segment in segments {
            match segment {
                Segment::Slot {
                    location,
                    offset,
                    len: piece,
                } => {
                    len += piece;
                    if !cut {
                        bytes.extend_from_slice(self.disks[node].read(location, offset, piece));
                    }
                }
                Segment::Origin {
                    origin,
                    offset,
                    len: piece,
                } => {
                    len += piece;
                    let Some(source) = self.origin_bodies.get(&(node, origin)) else {
                        return Err(
                            self.failure(format!("node {node} sent {origin:?} after releasing it"))
                        );
                    };
                    if offset + piece > source.len {
                        return Err(
                            self.failure(format!("node {node} sent past the end of {origin:?}"))
                        );
                    }
                    if cut {
                        continue;
                    }
                    let start = (offset as usize).min(source.bytes.len());
                    let end = ((offset + piece) as usize).min(source.bytes.len());
                    bytes.extend_from_slice(&source.bytes[start..end]);
                    cut = end < (offset + piece) as usize;
                }
            }
        }
        Ok(Body { bytes, len })
    }

    fn answer(&mut self, attempt: u64, head: ResponseHead, body: Vec<u8>) -> Result<(), Failure> {
        let Attempt {
            request,
            gateway,
            sent,
            ..
        } = self
            .attempts
            .remove(&attempt)
            .expect("one response per attempt");
        if self.trace {
            let (status, len) = (head.status, body.len());
            eprintln!(
                "{} request {request} attempt {attempt} answered {status} ({len} bytes)",
                self.now
            );
        }
        if !self.requests.contains_key(&request) {
            // An earlier attempt already answered it.
            return Ok(());
        }
        // Faults explain a 5xx or a body that ended early until their
        // effects have run their course: S3 and node timeouts, and messages
        // held back by a spike.
        let mut grace = 2 * (self.options.origin_timeout + self.options.node_timeout)
            + 10 * self.options.delay_max
            + 10;
        // Membership forgets a node that stopped for good only after it is
        // declared down and its grace period ends, and a gateway may need
        // a timeout or two to learn the new ring.
        if self.options.membership {
            grace += 3 * self.options.probe_period
                + self.options.suspect_to_down
                + self.options.down_grace
                + 2 * self.options.node_timeout;
        }
        let unexplained = self.quiet_since.is_some_and(|quiet| sent >= quiet + grace);
        if head.status >= 500 {
            if unexplained {
                return Err(self.failure(format!(
                    "{} for {request:?} with no fault to explain it",
                    head.status
                )));
            }
            self.summary.server_errors += 1;
            self.summary.client_retries += 1;
            self.attempt(request);
            return Ok(());
        }
        let pending = &self.requests[&request];
        if pending.read.method == Method::Get && (body.len() as u64) < head.content_length {
            // The client sees the connection close early, and retries.
            let from = pending
                .issued
                .saturating_sub(self.options.staleness(&pending.read.key));
            properties::check_early_end(&self.origin, &pending.read, from, self.now, &head, &body)
                .map_err(|message| self.failure(message))?;
            if unexplained {
                return Err(self.failure(format!(
                    "{request:?} ended early with no fault to explain it"
                )));
            }
            self.summary.early_ends += 1;
            self.summary.client_retries += 1;
            self.attempt(request);
            return Ok(());
        }
        let Pending {
            client,
            read,
            issued,
            ..
        } = self.requests.remove(&request).expect("pending");
        self.in_flight[client] -= 1;
        let mut from = issued.saturating_sub(self.options.staleness(&read.key));
        // The gateway's ring, and when it last changed.
        let (current, since) = self.gateway_rings[gateway];
        // A read through the gateway that passed a write, sent once the
        // write succeeded, sees it while the gateway's ring stays the same.
        // Clients send before a tick's messages arrive, so a read sent in
        // the tick its client heard came first.
        if let Some(write) = self.acknowledged.get(&(gateway, read.key.clone()))
            && write.heard < sent
            && write.ring == current
            && since <= write.applied
        {
            from = from.max(write.applied);
        }
        // Once a node finished a change's event, a read sent after the
        // gateways' entries from before it expired sees the change, while
        // the gateway has routed by the ring that node told the homes under
        // since then.
        if let Some(changes) = self.finished_events.get(&read.key) {
            let settled =
                self.options.gateway_metadata_ttl + 2 * (10 * self.options.delay_max + 10);
            let seen = changes
                .iter()
                .filter(|(_, finished, by)| {
                    finished + settled < sent && *by == current && since <= *finished
                })
                .map(|(applied, _, _)| *applied)
                .max();
            if let Some(applied) = seen {
                from = from.max(applied);
            }
        }
        properties::check_response(&self.origin, &read, from, self.now, &head, &body)
            .map_err(|message| self.failure(message))?;
        *self.summary.statuses.entry(head.status).or_default() += 1;
        let digest = xxh3_64_with_seed(&body, u64::from(head.status));
        self.summary.fingerprint = xxh3_64_with_seed(
            &[request, self.now, digest].map(u64::to_le_bytes).concat(),
            self.summary.fingerprint,
        );
        if let Some(answer) = self.watched.get_mut(&request) {
            *answer = Some((head, body));
        }
        Ok(())
    }

    fn check_disks(&self) -> Result<(), Failure> {
        for node in 0..self.nodes.len() {
            self.check_disk(node, None)?;
            self.check_table(node)?;
            self.check_metadata_file(node)?;
        }
        Ok(())
    }

    /// Every entry a node saved in its metadata file describes a version
    /// its key had.
    fn check_metadata_file(&self, node: usize) -> Result<(), Failure> {
        for (key, meta) in &self.disks[node].metadata {
            let Some(meta) = meta else {
                continue;
            };
            let matches = self.origin.version(&meta.etag).is_some_and(|object| {
                object.key == *key && object.size == meta.size && object.headers == meta.headers
            });
            if !matches {
                return Err(self.failure(format!(
                    "node {node} saved metadata {meta:?} for {key:?}, which no version of it had"
                )));
            }
        }
        Ok(())
    }

    /// Checks the stored blocks a node would serve as they are, or only the
    /// one at `only`.
    fn check_disk(&self, node: usize, only: Option<Location>) -> Result<(), Failure> {
        let Some(up) = &self.nodes[node] else {
            return Ok(());
        };
        let blocks: Vec<_> = match only {
            Some(location) => up.stored_block_at(location).into_iter().collect(),
            None => up.stored_blocks().collect(),
        };
        for block in blocks {
            let bytes = self.disks[node].read(block.location, 0, block.len);
            properties::check_block(&self.origin, self.options.block_size, &block, bytes)
                .map_err(|message| self.failure(format!("node {node}: {message}")))?;
        }
        Ok(())
    }

    /// Every record in a node's slot table describes the bytes its slot
    /// holds, unless a fault damaged them after they were durable.
    fn check_table(&self, node: usize) -> Result<(), Failure> {
        let disk = &self.disks[node];
        for (&location, (record, _, _)) in &disk.records {
            if disk.damaged.contains(&location) {
                continue;
            }
            let block = StoredBlock {
                version: record.version,
                index: record.index,
                location,
                len: record.len,
            };
            let bytes = disk.read(location, 0, record.len);
            properties::check_block(&self.origin, self.options.block_size, &block, bytes)
                .map_err(|message| self.failure(format!("node {node}'s slot table: {message}")))?;
        }
        Ok(())
    }

    /// A node admits only blocks it owns under its ring.
    fn check_owned(&self, node: usize, location: Location) -> Result<(), Failure> {
        let Some(up) = &self.nodes[node] else {
            return Ok(());
        };
        let Some(block) = up.stored_block_at(location) else {
            return Ok(());
        };
        let Some(object) = self.origin.version_by_id(block.version) else {
            return Ok(());
        };
        let layout = Layout::new(self.options.block_size, self.options.chunk_blocks);
        let placement = layout
            .placement(&object.key, object.size, block.index)
            .hash();
        let owner = up.ring().owner(placement).map(|id| id.0 as usize);
        if owner != Some(node) {
            return Err(self.failure(format!(
                "node {node} stored block {} of {:?}, which node {owner:?} owns",
                block.index, object.key
            )));
        }
        Ok(())
    }

    /// Puts a message on the network. While faults happen, it may be
    /// lost, cut off by a partition, or held back far longer than usual.
    fn send(&mut self, from: Address, to: Address, message: Message) {
        let mut delay = self
            .network
            .range(self.options.delay_min..=self.options.delay_max);
        let cut = |address: Address| matches!(address, Address::Node(node) if self.partitioned.contains_key(&node));
        if cut(from) || cut(to) {
            self.summary.lost += 1;
            return;
        }
        if self.faulty {
            if self.losses.percent(self.options.loss_percent) {
                self.summary.lost += 1;
                return;
            }
            if self.spikes.percent(self.options.spike_percent) {
                delay = delay * 10 + 10;
            }
        }
        self.queue
            .push(self.now + delay, Event::Deliver { to, message });
    }

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn failure(&self, message: String) -> Failure {
        if self.trace {
            for (index, node) in self.nodes.iter().enumerate() {
                if let Some(node) = node {
                    eprintln!("node {index}: {}", node.describe());
                }
            }
        }
        Failure {
            seed: self.seed,
            tick: self.now,
            message,
        }
    }
}

/// A run's outcome. Two runs of one seed produce equal summaries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    pub ticks: u64,
    pub statuses: BTreeMap<u16, u64>,
    /// Writes, those that passed through a gateway and succeeded, and
    /// those of them that went through a node other than the key's home.
    pub writes: u64,
    pub gateway_writes: u64,
    pub detoured_writes: u64,
    /// Messages from S3's event queue that nodes finished, and purges
    /// homes coordinated.
    pub events: u64,
    pub purges: u64,
    /// Leases owners granted, and reads replicas served under them.
    pub leases: u64,
    pub leased_reads: u64,
    /// Uploads whose blocks their home stored as they passed through, and
    /// metadata blocks homes filled before a reader asked.
    pub warmed_uploads: u64,
    pub prefetched_blocks: u64,
    /// Requests a node sent back because their object changed.
    pub retries: u64,
    pub origin_requests: u64,
    pub hit_bytes: u64,
    pub miss_bytes: u64,
    pub written_bytes: u64,
    pub evicted_blocks: u64,
    /// Requests each node received from gateways.
    pub node_reads: Vec<u64>,
    /// Messages lost to faults, 5xx answers clients got, and requests
    /// clients sent again after a 5xx or a timeout.
    pub lost: u64,
    pub server_errors: u64,
    pub client_retries: u64,
    /// Gateways' node requests that found their node down.
    pub refused: u64,
    /// Nodes that crashed and that shut down cleanly; recovered blocks
    /// checked against their checksums, and those that failed.
    pub crashes: u64,
    pub clean_shutdowns: u64,
    /// Node response bodies a gateway forwarded that had ended early, and
    /// client responses that ended early.
    pub cut_bodies: u64,
    pub early_ends: u64,
    pub verified_blocks: u64,
    pub corrupt_blocks: u64,
    /// Times the cluster grew, shrank or replaced a node; rings nodes took
    /// up, and rings gateways fetched.
    pub resizes: u64,
    pub ring_changes: u64,
    pub ring_fetches: u64,
    /// Reads nodes sent previous owners, objects whose metadata a previous
    /// home supplied, and body bytes served from previous owners' blocks.
    pub peer_requests: u64,
    pub peer_timeouts: u64,
    pub peer_metadata: u64,
    pub peer_bytes: u64,
    /// A digest of every response: its request, tick, status and body.
    pub fingerprint: u64,
}

impl Summary {
    /// The share of body bytes served from stored blocks, in percent.
    pub fn hit_percent(&self) -> u64 {
        let total = self.hit_bytes + self.miss_bytes;
        (self.hit_bytes * 100).checked_div(total).unwrap_or(0)
    }
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let statuses: Vec<String> = self
            .statuses
            .iter()
            .map(|(status, count)| format!("{status}={count}"))
            .collect();
        write!(
            f,
            "seed {} passed: {} ticks, {} writes ({} through gateways, {} around homes), {} events, {} purges, \
             {} leases serving {} reads, {} uploads warmed, {} blocks prefetched, {} retries, \
             responses {}, \
             {}% of body bytes from disk, {} S3 requests, {} bytes written, \
             {} blocks evicted, node reads {:?}, {} messages lost, {} server errors, \
             {} client retries, {} crashes, {} clean shutdowns, {} blocks verified, \
             {} corrupt, {} cut bodies, {} early ends, {} resizes, {} ring changes, {} ring fetches, \
             {} peer requests, {} peer metadata, {} peer bytes, fingerprint {:016x}",
            self.seed,
            self.ticks,
            self.writes,
            self.gateway_writes,
            self.detoured_writes,
            self.events,
            self.purges,
            self.leases,
            self.leased_reads,
            self.warmed_uploads,
            self.prefetched_blocks,
            self.retries,
            statuses.join(" "),
            self.hit_percent(),
            self.origin_requests,
            self.written_bytes,
            self.evicted_blocks,
            self.node_reads,
            self.lost,
            self.server_errors,
            self.client_retries,
            self.crashes,
            self.clean_shutdowns,
            self.verified_blocks,
            self.corrupt_blocks,
            self.cut_bodies,
            self.early_ends,
            self.resizes,
            self.ring_changes,
            self.ring_fetches,
            self.peer_requests,
            self.peer_metadata,
            self.peer_bytes,
            self.fingerprint
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub seed: u64,
    pub tick: u64,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "seed {} failed at tick {}: {}",
            self.seed, self.tick, self.message
        )
    }
}

impl std::error::Error for Failure {}

/// A seed from a decimal number, or from a git commit hash's first 16 hex
/// digits.
pub fn parse_seed(text: &str) -> Result<u64, String> {
    if let Ok(seed) = text.parse() {
        return Ok(seed);
    }
    match text.get(..16) {
        Some(prefix) if text.chars().all(|c| c.is_ascii_hexdigit()) => {
            u64::from_str_radix(prefix, 16).map_err(|error| error.to_string())
        }
        _ => Err(format!("{text:?} is neither a number nor a commit hash")),
    }
}
