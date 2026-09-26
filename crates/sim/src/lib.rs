//! Deterministic simulator for the gateway and storage nodes.
//!
//! One thread runs gateways, storage nodes with their disks, clients and a
//! model of S3. Every message crosses a simulated network, disk writes and
//! response bodies take time, writers change objects behind the cache's back,
//! and every response is checked against what S3 held while the request was
//! in flight, give or take the bucket's staleness bound. Every stored block
//! is checked against the version it is keyed by. The seed determines the
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
    self, BucketPolicy, Freshness, GatewayRequestId, Node, OriginRequestId, Segment,
};
use s3_accelerator_core::placement::{Member, NodeId, Ring};
use s3_accelerator_core::s3::{ByteRange, ETag, Method, ObjectKey, Request, ResponseHead};
use s3_accelerator_core::store::{Location, StoreConfig};
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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
        Options {
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
        }
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
        read: Request,
    },
    OriginRequest {
        node: usize,
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
        body: Vec<u8>,
    },
    NodeStale {
        id: NodeRequestId,
    },
    OriginResponse {
        origin: OriginRequestId,
        head: ResponseHead,
        body: Vec<u8>,
    },
}

enum Event {
    Deliver {
        to: Address,
        message: Message,
    },
    /// A node's write reached its disk.
    Written {
        node: usize,
        location: Location,
    },
    /// A node finished sending a response body.
    Sent {
        node: usize,
        id: GatewayRequestId,
    },
}

/// A client request in flight.
struct Pending {
    client: usize,
    read: Request,
    issued: u64,
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
}

pub struct Simulator {
    seed: u64,
    options: Options,
    /// One stream per source of randomness, so drawing more for one
    /// leaves the others' draws unchanged.
    workload: Prng,
    writers: Prng,
    network: Prng,
    disk_delays: Prng,
    send_delays: Prng,
    now: u64,
    queue: Queue<Event>,
    origin: Origin,
    keys: Vec<ObjectKey>,
    gateways: Vec<Gateway>,
    nodes: Vec<Node>,
    disks: Vec<Disk>,
    in_flight: Vec<usize>,
    requests: BTreeMap<u64, Pending>,
    issued: u64,
    /// Scripted requests, and their answers once they arrive.
    watched: BTreeMap<u64, Option<(ResponseHead, Vec<u8>)>>,
    // What the server keeps per connection: who asked, and the bodies it
    // holds while a node or gateway reads them.
    next_id: u64,
    gateway_requests: BTreeMap<(usize, ClientRequestId), u64>,
    node_requests: BTreeMap<(usize, GatewayRequestId), (usize, NodeRequestId)>,
    gateway_bodies: BTreeMap<(usize, NodeRequestId), Vec<u8>>,
    origin_bodies: BTreeMap<(usize, OriginRequestId), Vec<u8>>,
    writes: BTreeMap<(usize, Location), Write>,
    sending: BTreeMap<(usize, GatewayRequestId), Sending>,
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
            disk_delays: Prng::stream(seed, "disk delays"),
            send_delays: Prng::stream(seed, "send delays"),
            now: 0,
            queue: Queue::default(),
            origin,
            keys,
            gateways: (0..options.gateways)
                .map(|_| Gateway::new(ring.clone()))
                .collect(),
            nodes: (0..options.nodes)
                .map(|_| Node::new(config.clone()))
                .collect(),
            disks: (0..options.nodes)
                .map(|_| Disk::new(config.store.extents, config.store.extent_size))
                .collect(),
            in_flight: vec![0; options.clients],
            requests: BTreeMap::new(),
            issued: 0,
            watched: BTreeMap::new(),
            next_id: 0,
            gateway_requests: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            gateway_bodies: BTreeMap::new(),
            origin_bodies: BTreeMap::new(),
            writes: BTreeMap::new(),
            sending: BTreeMap::new(),
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

    /// Runs the random workload until every request is answered.
    pub fn run(mut self) -> Result<Summary, Failure> {
        let per_hop =
            self.options.delay_max + self.options.disk_delay_max + self.options.send_delay_max;
        let tick_limit = 20_000 + self.options.requests * (per_hop + 1) * 20;
        while self.issued < self.options.requests || !self.requests.is_empty() {
            if self.now > tick_limit {
                let unanswered = self.requests.len();
                return Err(self.failure(format!("{unanswered} requests unanswered")));
            }
            self.tick()?;
        }
        self.settle()?;
        Ok(self.summary())
    }

    /// Writes an object to the model of S3 now.
    pub fn put(&mut self, key: &ObjectKey, size: u64) {
        self.origin.put(self.now, key, size, &mut self.writers);
    }

    pub fn delete(&mut self, key: &ObjectKey) {
        self.origin.delete(self.now, key);
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

    /// Runs one tick.
    pub fn step(&mut self) -> Result<(), Failure> {
        self.tick()
    }

    pub fn summary(&self) -> Summary {
        let mut summary = self.summary.clone();
        summary.ticks = self.now;
        summary.origin_requests = self.origin.requests();
        for node in &self.nodes {
            let stats = node.stats();
            summary.hit_bytes += stats.hit_bytes;
            summary.miss_bytes += stats.miss_bytes;
            summary.written_bytes += stats.written_bytes;
            summary.evicted_blocks += stats.evicted_blocks;
        }
        summary
    }

    fn tick(&mut self) -> Result<(), Failure> {
        self.tick_writes();
        self.tick_clients();
        while let Some(event) = self.queue.pop_due(self.now) {
            self.handle(event)?;
        }
        if self.now.is_multiple_of(64) {
            self.check_disks()?;
        }
        self.now += 1;
        Ok(())
    }

    /// Runs until nothing is in flight, then checks that nothing is left
    /// behind.
    fn settle(&mut self) -> Result<(), Failure> {
        while !self.queue.is_empty() {
            self.tick()?;
        }
        self.check_disks()?;
        let stranded = self.gateway_requests.len()
            + self.node_requests.len()
            + self.gateway_bodies.len()
            + self.origin_bodies.len()
            + self.writes.len()
            + self.sending.len();
        let busy = self.nodes.iter().filter(|node| !node.is_idle()).count();
        if stranded > 0 || busy > 0 {
            return Err(self.failure(format!(
                "{stranded} requests or bodies stranded and {busy} nodes busy after the last response"
            )));
        }
        Ok(())
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
            read: read.clone(),
            issued: self.now,
        };
        self.requests.insert(request, pending);
        self.send(
            Address::Gateway(gateway),
            Message::ClientRequest { request, read },
        );
        request
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
            Event::Written { node, location } => self.written(node, location),
            Event::Sent { node, id } => self.sent(node, id),
        }
    }

    fn deliver(&mut self, to: Address, message: Message) -> Result<(), Failure> {
        let now = Time(self.now);
        match (to, message) {
            (Address::Gateway(gateway), Message::ClientRequest { request, read }) => {
                let id = ClientRequestId(self.next_id());
                self.gateway_requests.insert((gateway, id), request);
                self.gateways[gateway].on_request(id, read);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::NodeResponse { id, head, body }) => {
                self.gateway_bodies.insert((gateway, id), body);
                self.gateways[gateway].on_node_response(id, head);
                self.drain_gateway(gateway)
            }
            (Address::Gateway(gateway), Message::NodeStale { id }) => {
                self.summary.retries += 1;
                self.gateways[gateway].on_node_stale(id);
                self.drain_gateway(gateway)
            }
            (Address::Node(node), Message::NodeRequest { gateway, id, read }) => {
                let local = GatewayRequestId(self.next_id());
                self.node_requests.insert((node, local), (gateway, id));
                self.nodes[node].on_request(now, local, read);
                self.drain_node(node)
            }
            (Address::Node(node), Message::OriginResponse { origin, head, body }) => {
                self.origin_bodies.insert((node, origin), body);
                self.nodes[node].on_origin_response(now, origin, head);
                self.drain_node(node)
            }
            (Address::Origin, Message::OriginRequest { node, origin, read }) => {
                let (head, body) = self.origin.respond_now(&read);
                let response = Message::OriginResponse { origin, head, body };
                self.send(Address::Node(node), response);
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
                gateway::Action::Send { node, id, request } => {
                    let message = Message::NodeRequest {
                        gateway,
                        id,
                        read: request,
                    };
                    self.send(Address::Node(node.0 as usize), message);
                }
                gateway::Action::Relay {
                    request,
                    head,
                    from,
                } => {
                    let Some(body) = self.gateway_bodies.remove(&(gateway, from)) else {
                        return Err(
                            self.failure(format!("gateway {gateway} relayed {from:?} twice"))
                        );
                    };
                    self.respond_to_client(gateway, request, head, body)?;
                }
                gateway::Action::Respond { request, head } => {
                    self.respond_to_client(gateway, request, head, Vec::new())?;
                }
            }
        }
        Ok(())
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
        let client = self.requests[&request].client;
        let response = Message::ClientResponse {
            request,
            head,
            body,
        };
        self.send(Address::Client(client), response);
        Ok(())
    }

    fn drain_node(&mut self, node: usize) -> Result<(), Failure> {
        for action in self.nodes[node].drain() {
            match action {
                node::Action::Fetch { origin, request } => {
                    let message = Message::OriginRequest {
                        node,
                        origin,
                        read: request,
                    };
                    self.send(Address::Origin, message);
                }
                node::Action::Respond {
                    request,
                    head,
                    body,
                } => {
                    let delay = self.send_delays.range(0..=self.options.send_delay_max);
                    self.sending.insert((node, request), Sending { head, body });
                    self.queue
                        .push(self.now + delay, Event::Sent { node, id: request });
                }
                node::Action::Stale { request } => {
                    let Some((gateway, id)) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    self.send(Address::Gateway(gateway), Message::NodeStale { id });
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
                    self.queue
                        .push(self.now + delay, Event::Written { node, location });
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
        let Some(bytes) = body.get(write.offset as usize..(write.offset + write.len) as usize)
        else {
            return Err(self.failure(format!(
                "node {node} wrote past the end of {:?}",
                write.origin
            )));
        };
        let bytes = bytes.to_vec();
        self.disks[node].write(location, &bytes);
        self.nodes[node].on_written(location);
        self.check_disk(node, Some(location))?;
        self.drain_node(node)
    }

    /// Reads a response body as `sendfile` and `splice` would, at the moment
    /// it is sent, and forwards it.
    fn sent(&mut self, node: usize, id: GatewayRequestId) -> Result<(), Failure> {
        let Sending { head, body } = self
            .sending
            .remove(&(node, id))
            .expect("a send was scheduled");
        let mut bytes = Vec::new();
        for segment in body {
            match segment {
                Segment::Slot {
                    location,
                    offset,
                    len,
                } => bytes.extend_from_slice(self.disks[node].read(location, offset, len)),
                Segment::Origin {
                    origin,
                    offset,
                    len,
                } => {
                    let Some(source) = self.origin_bodies.get(&(node, origin)) else {
                        return Err(
                            self.failure(format!("node {node} sent {origin:?} after releasing it"))
                        );
                    };
                    let Some(piece) = source.get(offset as usize..(offset + len) as usize) else {
                        return Err(
                            self.failure(format!("node {node} sent past the end of {origin:?}"))
                        );
                    };
                    bytes.extend_from_slice(piece);
                }
            }
        }
        let Some((gateway, gateway_id)) = self.node_requests.remove(&(node, id)) else {
            return Err(self.failure(format!("node {node} answered {id:?} twice")));
        };
        let response = Message::NodeResponse {
            id: gateway_id,
            head,
            body: bytes,
        };
        self.send(Address::Gateway(gateway), response);
        self.nodes[node].on_sent(id);
        self.drain_node(node)
    }

    fn answer(&mut self, request: u64, head: ResponseHead, body: Vec<u8>) -> Result<(), Failure> {
        let Pending {
            client,
            read,
            issued,
        } = self
            .requests
            .remove(&request)
            .expect("one response per request");
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
        (0..self.nodes.len()).try_for_each(|node| self.check_disk(node, None))
    }

    /// Checks a node's stored blocks, or only the one at `only`.
    fn check_disk(&self, node: usize, only: Option<Location>) -> Result<(), Failure> {
        let blocks: Vec<_> = match only {
            Some(location) => self.nodes[node]
                .stored_block_at(location)
                .into_iter()
                .collect(),
            None => self.nodes[node].stored_blocks().collect(),
        };
        for block in blocks {
            let bytes = self.disks[node].read(block.location, 0, block.len);
            properties::check_block(&self.origin, self.options.block_size, &block, bytes)
                .map_err(|message| self.failure(format!("node {node}: {message}")))?;
        }
        Ok(())
    }

    fn send(&mut self, to: Address, message: Message) {
        let delay = self
            .network
            .range(self.options.delay_min..=self.options.delay_max);
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
             {} blocks evicted, fingerprint {:016x}",
            self.seed,
            self.ticks,
            self.writes,
            self.retries,
            statuses.join(" "),
            self.hit_percent(),
            self.origin_requests,
            self.written_bytes,
            self.evicted_blocks,
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
