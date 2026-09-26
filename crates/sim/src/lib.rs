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
use s3_accelerator_core::node::{
    self, BucketPolicy, Freshness, GatewayRequestId, Node, ObjectMeta, OriginRequestId, Read,
    Segment, StoredBlock,
};
use s3_accelerator_core::placement::{Member, NodeId, Placement, Ring};
use s3_accelerator_core::s3::{ByteRange, ETag, Method, ObjectKey, Request, ResponseHead};
use s3_accelerator_core::store::{Location, StoreConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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
    /// One-way network delay, in ticks.
    pub delay_min: u64,
    pub delay_max: u64,
    /// Ticks a disk write or a response body takes, at most.
    pub disk_delay_max: u64,
    pub send_delay_max: u64,
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
            node_timeout: self.node_timeout,
            suspect_ttl: self.suspect_ttl,
        }
    }

    fn node_config(&self) -> node::Config {
        let policy = |freshness, admit_on_first_read| BucketPolicy {
            freshness,
            admit_on_first_read,
        };
        let immutable = policy(Freshness::Immutable, self.immutable_admit_on_first_read);
        let ttl = policy(Freshness::Ttl(self.ttl), self.ttl_admit_on_first_read);
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
            default_policy: ttl,
            buckets: BTreeMap::from([
                (IMMUTABLE_BUCKET.to_string(), immutable),
                (TTL_BUCKET.to_string(), ttl),
            ]),
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
    },
    NodeMetadata {
        id: NodeRequestId,
        meta: ObjectMeta,
    },
    NodeStale {
        id: NodeRequestId,
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
    /// A node read a recovered block back to check its checksum.
    Verified {
        node: usize,
        run: u64,
        location: Location,
        len: u64,
        checksum: u64,
    },
}

/// A client's request, which it sends again after a 5xx or a timeout.
struct Pending {
    client: usize,
    read: Request,
    issued: u64,
    /// When the latest attempt went out.
    sent: u64,
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
    /// Whether faults happen: while clients are still issuing requests.
    faulty: bool,
    /// Nodes cut off from everyone, until the tick given.
    partitioned: BTreeMap<usize, u64>,
    /// Nodes that are down, until the tick given.
    down: BTreeMap<usize, u64>,
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
    /// Each node, while it is up.
    nodes: Vec<Option<Node>>,
    disks: Vec<Disk>,
    in_flight: Vec<usize>,
    requests: BTreeMap<u64, Pending>,
    issued: u64,
    /// Each attempt a client sent: its request and its client.
    attempts: BTreeMap<u64, (u64, usize, u64)>,
    /// When faults stopped.
    quiet_since: Option<u64>,
    next_attempt: u64,
    /// Scripted requests, and their answers once they arrive.
    watched: BTreeMap<u64, Option<(ResponseHead, Vec<u8>)>>,
    // What the server keeps per connection: who asked, and the bodies it
    // holds while a node or gateway reads them.
    next_id: u64,
    gateway_requests: BTreeMap<(usize, ClientRequestId), u64>,
    node_requests: BTreeMap<(usize, GatewayRequestId), (usize, NodeRequestId)>,
    gateway_bodies: BTreeMap<(usize, NodeRequestId), Body>,
    origin_bodies: BTreeMap<(usize, OriginRequestId), Body>,
    /// Scripted cuts: the next body a node sends, or S3 sends a node, that
    /// is longer than the given length ends there.
    cut_responses: BTreeMap<usize, u64>,
    cut_origin_responses: BTreeMap<usize, u64>,
    /// Client responses a gateway started, and the body so far.
    client_responses: BTreeMap<(usize, ClientRequestId), (ResponseHead, Vec<u8>)>,
    writes: BTreeMap<(usize, Location), Write>,
    sending: BTreeMap<(usize, GatewayRequestId), Sending>,
    /// S3 requests a node gave up on, whose responses it drops.
    cancelled: BTreeSet<(usize, OriginRequestId)>,
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
        let ring = Ring::new(1, members);
        let keys: Vec<ObjectKey> = (0..options.keys)
            .map(|index| ObjectKey {
                bucket: [IMMUTABLE_BUCKET, TTL_BUCKET][index % 2].to_string(),
                key: format!("key-{index}"),
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
        Simulator {
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
            faulty: true,
            partitioned: BTreeMap::new(),
            down: BTreeMap::new(),
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
            disks: (0..options.nodes)
                .map(|_| Disk::new(config.store.extents, config.store.extent_size))
                .collect(),
            in_flight: vec![0; options.clients],
            requests: BTreeMap::new(),
            attempts: BTreeMap::new(),
            quiet_since: None,
            next_attempt: 0,
            issued: 0,
            watched: BTreeMap::new(),
            next_id: 0,
            gateway_requests: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            gateway_bodies: BTreeMap::new(),
            origin_bodies: BTreeMap::new(),
            client_responses: BTreeMap::new(),
            cut_responses: BTreeMap::new(),
            cut_origin_responses: BTreeMap::new(),
            writes: BTreeMap::new(),
            sending: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            trace: false,
            summary: Summary {
                seed,
                ..Summary::default()
            },
            options,
        }
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
                if self.trace {
                    for (index, node) in self.nodes.iter().enumerate() {
                        if let Some(node) = node {
                            eprintln!("node {index}: {}", node.describe());
                        }
                    }
                }
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
        for node in std::mem::take(&mut self.down).into_keys() {
            self.restart(node)?;
        }
        Ok(())
    }

    /// Writes an object to the model of S3 now.
    pub fn put(&mut self, key: &ObjectKey, size: u64) {
        self.origin.put(self.now, key, size, &mut self.writers);
    }

    pub fn delete(&mut self, key: &ObjectKey) {
        self.origin.delete(self.now, key);
    }

    /// Writes an object through its home: to the model of S3, then to the
    /// home, which learns the write succeeded.
    pub fn write_through(&mut self, key: &ObjectKey, size: u64) -> Result<(), Failure> {
        self.origin.put(self.now, key, size, &mut self.writers);
        let home = self
            .ring
            .owner(Placement::Home(key).hash())
            .expect("a node")
            .0 as usize;
        self.gateways[0].on_write(Time(self.now), key);
        // A home that is down lost its metadata with its memory.
        let Some(node) = self.nodes[home].as_mut() else {
            return Ok(());
        };
        node.on_write(Time(self.now), key);
        self.drain_node(home)
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

    /// The node that is `key`'s home.
    pub fn home(&self, key: &ObjectKey) -> usize {
        let home = self
            .ring
            .owner(Placement::Home(key).hash())
            .expect("a node");
        home.0 as usize
    }

    /// The rendezvous candidates for `key`'s home, best first.
    pub fn home_candidates(&self, key: &ObjectKey) -> Vec<usize> {
        let placement = Placement::Home(key).hash();
        self.ring
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

    /// Starts a node that is down, over its slot table.
    pub fn restart(&mut self, node: usize) -> Result<(), Failure> {
        self.down.remove(&node);
        let records = self.disks[node].start();
        let config = self.options.node_config();
        let id = NodeId(node as u64);
        self.nodes[node] = Some(Node::recover(id, self.ring.clone(), config, records));
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
            .find(|(_, (record, _, _))| record.key == *key && record.index == index)
            .map(|(&location, _)| location);
        if let Some(location) = found {
            disk.damage(location, 0);
        }
        found.is_some()
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
        for (node, retired) in self.nodes.iter().zip(&self.retired) {
            let stats = node.as_ref().map(Node::stats).unwrap_or_default();
            summary.hit_bytes += retired.hit_bytes + stats.hit_bytes;
            summary.miss_bytes += retired.miss_bytes + stats.miss_bytes;
            summary.written_bytes += retired.written_bytes + stats.written_bytes;
            summary.evicted_blocks += retired.evicted_blocks + stats.evicted_blocks;
            summary.verified_blocks += retired.verified_blocks + stats.verified_blocks;
            summary.corrupt_blocks += retired.corrupt_blocks + stats.corrupt_blocks;
            summary.node_reads.push(retired.reads + stats.reads);
        }
        summary
    }

    fn tick(&mut self) -> Result<(), Failure> {
        self.tick_faults()?;
        self.tick_writes();
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
            if let Some(up) = self.nodes[node].as_mut() {
                up.on_tick(now);
                self.drain_node(node)?;
            }
        }
        for gateway in 0..self.gateways.len() {
            self.gateways[gateway].on_tick(now);
            self.drain_gateway(gateway)?;
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
        if self.faulty && self.crashes.below(1_000) < self.options.crash_permille {
            let node = self.crashes.index(self.nodes.len());
            let clean = self.crashes.percent(self.options.clean_percent);
            let ticks = self.crashes.range(1..=self.options.down_max);
            if self.nodes[node].is_some() {
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
            let Sending { head, body, meta } = self
                .sending
                .remove(&(node, id))
                .expect("a send in progress");
            let mut body = self.assemble(node, &body)?;
            let (gateway, gateway_id) = self.node_requests[&(node, id)];
            if !clean {
                if self.cuts.percent(50) {
                    continue;
                }
                body.bytes
                    .truncate(self.cuts.below(body.len.max(1)) as usize);
            }
            let response = Message::NodeResponse {
                id: gateway_id,
                head,
                body,
                meta,
            };
            self.send(Address::Node(node), Address::Gateway(gateway), response);
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
            self.disks[node].shut_down();
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
        let retired = &mut self.retired[node];
        retired.hit_bytes += stats.hit_bytes;
        retired.miss_bytes += stats.miss_bytes;
        retired.written_bytes += stats.written_bytes;
        retired.evicted_blocks += stats.evicted_blocks;
        retired.verified_blocks += stats.verified_blocks;
        retired.corrupt_blocks += stats.corrupt_blocks;
        retired.reads += stats.reads;
        self.nodes[node] = None;
        self.runs[node] += 1;
        self.down.insert(node, until);
        self.writes.retain(|&(owner, _), _| owner != node);
        self.sending.retain(|&(owner, _), _| owner != node);
        self.origin_bodies.retain(|&(owner, _), _| owner != node);
        self.cancelled.retain(|&(owner, _)| owner != node);
        self.node_requests.retain(|&(owner, _), _| owner != node);
        self.check_table(node)
    }

    /// Runs until nothing is in flight, then checks that nothing is left
    /// behind.
    fn settle(&mut self) -> Result<(), Failure> {
        let deadline = self.now
            + 4 * (self.options.client_timeout
                + self.options.node_timeout
                + self.options.origin_timeout);
        while !(self.queue.is_empty() && self.idle()) && self.now <= deadline {
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

    fn tick_writes(&mut self) {
        if self.keys.is_empty() || !self.writers.percent(self.options.write_percent) {
            return;
        }
        let key = self.keys[self.writers.index(self.keys.len())].clone();
        let size = self.writers.range(1..=self.options.object_size_max);
        if key.bucket == IMMUTABLE_BUCKET {
            if self.origin.current(&key).is_none() {
                self.origin.put(self.now, &key, size, &mut self.writers);
                self.summary.writes += 1;
            }
            return;
        }
        if self.writers.percent(self.options.delete_percent) {
            self.origin.delete(self.now, &key);
        } else {
            self.origin.put(self.now, &key, size, &mut self.writers);
        }
        self.summary.writes += 1;
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
        self.attempts.insert(attempt, (request, client, self.now));
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
        let key = self.keys[self.workload.index(self.keys.len())].clone();
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
                if run != self.runs[node] =>
            {
                Ok(())
            }
            Event::Written { node, location, .. } => self.written(node, location),
            Event::Forwarded {
                gateway,
                request,
                from,
                bytes,
            } => self.forwarded(gateway, request, from, bytes),
            Event::Sent { node, id, .. } => self.sent(node, id),
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
        if let Address::Node(node) = to {
            let stale =
                matches!(&message, Message::OriginResponse { run, .. } if *run != self.runs[node]);
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
                },
            ) => {
                self.gateway_bodies.insert((gateway, id), body);
                self.gateways[gateway].on_node_response(now, id, head, meta);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::NodeMetadata { id, meta }) => {
                self.gateways[gateway].on_node_metadata(now, id, meta);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::NodeStale { id }) => {
                self.summary.retries += 1;
                self.gateways[gateway].on_node_stale(now, id);
                self.drain_gateway(gateway)
            }
            (Address::Node(node), Message::NodeRequest { gateway, id, read }) => {
                let local = GatewayRequestId(self.next_id());
                self.node_requests.insert((node, local), (gateway, id));
                self.node(node).on_request(now, local, read);
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
                self.drain_node(node)
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
                    false => self.origin.respond_now(&read),
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
        for action in self.gateways[gateway].drain() {
            match action {
                gateway::Action::Send { node, id, read } => {
                    let node = self.route(node.0 as usize, &read);
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
                    self.queue.push(self.now + delay, forwarded);
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
            }
        }
        Ok(())
    }

    /// Where a read goes: its node, or sometimes, for a range, another one.
    fn route(&mut self, node: usize, read: &Read) -> usize {
        let misroute = matches!(read, Read::Range(_))
            && self.nodes.len() > 1
            && self.misroutes.percent(self.options.misroute_percent);
        match misroute {
            true => (node + 1 + self.misroutes.index(self.nodes.len() - 1)) % self.nodes.len(),
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
        let (_, client, _) = self.attempts[&request];
        let response = Message::ClientResponse {
            request,
            head,
            body,
        };
        self.send(Address::Gateway(gateway), Address::Client(client), response);
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
                node::Action::Fetch { origin, request } => {
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
                } => {
                    let delay = self.send_delays.range(0..=self.options.send_delay_max);
                    self.sending
                        .insert((node, request), Sending { head, body, meta });
                    let sent = Event::Sent {
                        node,
                        run,
                        id: request,
                    };
                    self.queue.push(self.now + delay, sent);
                }
                node::Action::Metadata { request, meta } => {
                    let Some((gateway, id)) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    self.send(
                        Address::Node(node),
                        Address::Gateway(gateway),
                        Message::NodeMetadata { id, meta },
                    );
                }
                node::Action::Stale { request } => {
                    let Some((gateway, id)) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    self.send(
                        Address::Node(node),
                        Address::Gateway(gateway),
                        Message::NodeStale { id },
                    );
                }
                node::Action::Write {
                    location,
                    origin,
                    offset,
                    len,
                } => {
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
                node::Action::Cancel { origin } => {
                    if self.trace {
                        eprintln!("{} node {node} cancelled {origin:?}", self.now);
                    }
                    self.cancelled.insert((node, origin));
                }
                node::Action::Release { origin } => {
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
        self.check_owned(node, location)?;
        self.drain_node(node)
    }

    /// Reads a response body as `sendfile` and `splice` would, at the moment
    /// it is sent, and forwards it. While faults happen, the connection may
    /// drop partway.
    fn sent(&mut self, node: usize, id: GatewayRequestId) -> Result<(), Failure> {
        let Sending { head, body, meta } = self
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
        let Some((gateway, gateway_id)) = self.node_requests.remove(&(node, id)) else {
            return Err(self.failure(format!("node {node} answered {id:?} twice")));
        };
        let response = Message::NodeResponse {
            id: gateway_id,
            head,
            body,
            meta,
        };
        self.send(Address::Node(node), Address::Gateway(gateway), response);
        self.node(node).on_sent(id);
        self.drain_node(node)
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
        let (request, _, sent) = self
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
        let grace = 2 * (self.options.origin_timeout + self.options.node_timeout)
            + 10 * self.options.delay_max
            + 10;
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
        let from = issued.saturating_sub(self.options.staleness(&read.key));
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
                key: &record.key,
                etag: &record.etag,
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

    /// A node stores only blocks it owns.
    fn check_owned(&self, node: usize, location: Location) -> Result<(), Failure> {
        let Some(up) = &self.nodes[node] else {
            return Ok(());
        };
        let Some(block) = up.stored_block_at(location) else {
            return Ok(());
        };
        let Some(object) = self.origin.version(block.etag) else {
            return Ok(());
        };
        let layout = Layout::new(self.options.block_size, self.options.chunk_blocks);
        let placement = layout.placement(block.key, object.size, block.index).hash();
        let owner = self.ring.owner(placement).map(|id| id.0 as usize);
        if owner != Some(node) {
            return Err(self.failure(format!(
                "node {node} stored block {} of {:?}, which node {owner:?} owns",
                block.index, block.key
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
    pub writes: u64,
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
            "seed {} passed: {} ticks, {} writes, {} retries, responses {}, \
             {}% of body bytes from disk, {} S3 requests, {} bytes written, \
             {} blocks evicted, node reads {:?}, {} messages lost, {} server errors, \
             {} client retries, {} crashes, {} clean shutdowns, {} blocks verified, \
             {} corrupt, {} cut bodies, {} early ends, fingerprint {:016x}",
            self.seed,
            self.ticks,
            self.writes,
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
