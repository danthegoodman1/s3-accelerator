//! Deterministic simulator for the gateway and storage nodes.
//!
//! One thread runs gateways, storage nodes, clients and a model of S3. Every
//! message crosses a simulated network, writers change objects behind the
//! cache's back, and every response is checked against what S3 held while
//! the request was in flight. The seed determines the whole run, from the
//! cluster's size to each delay, so a seed replays its run exactly.

pub mod network;
pub mod origin;
pub mod prng;
pub mod properties;

use network::{Address, Network};
use origin::Origin;
use prng::Prng;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::node::{self, GatewayRequestId, Node, OriginRequestId};
use s3_accelerator_core::placement::{Member, NodeId, Ring};
use s3_accelerator_core::s3::{ByteRange, ETag, GetObject, ObjectKey, ResponseHead};
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// A run's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub nodes: usize,
    pub gateways: usize,
    pub clients: usize,
    pub keys: usize,
    pub object_size_max: u64,
    /// Client requests in the run.
    pub requests: u64,
    /// Requests each client keeps in flight at most.
    pub requests_in_flight: usize,
    /// Chance per tick that a client with room issues a request.
    pub request_percent: u64,
    /// Chance per tick that a writer puts or deletes an object.
    pub write_percent: u64,
    /// Share of writes that delete.
    pub delete_percent: u64,
    /// One-way network delay, in ticks.
    pub delay_min: u64,
    pub delay_max: u64,
}

impl Options {
    /// A configuration drawn from `prng`, so many seeds cover many
    /// configurations.
    pub fn swarm(prng: &mut Prng) -> Options {
        let delay_min = prng.range(0..=3);
        Options {
            nodes: prng.range(1..=8) as usize,
            gateways: prng.range(1..=3) as usize,
            clients: prng.range(1..=8) as usize,
            keys: prng.range(1..=32) as usize,
            object_size_max: prng.range(1..=4_096),
            requests: prng.range(1..=2_000),
            requests_in_flight: prng.range(1..=4) as usize,
            request_percent: prng.range(10..=100),
            write_percent: prng.range(0..=30),
            delete_percent: prng.range(0..=30),
            delay_min,
            delay_max: delay_min + prng.range(0..=20),
        }
    }
}

/// What crosses the network.
#[derive(Debug)]
enum Message {
    ClientGet {
        request: u64,
        get: GetObject,
    },
    NodeGet {
        gateway: usize,
        request: NodeRequestId,
        get: GetObject,
    },
    OriginGet {
        node: usize,
        request: OriginRequestId,
        get: GetObject,
    },
    ClientResponse {
        request: u64,
        head: ResponseHead,
        body: Vec<u8>,
    },
    NodeResponse {
        request: NodeRequestId,
        head: ResponseHead,
        body: Vec<u8>,
    },
    OriginResponse {
        request: OriginRequestId,
        head: ResponseHead,
        body: Vec<u8>,
    },
}

/// A client request in flight.
struct Request {
    client: usize,
    get: GetObject,
    issued: u64,
}

pub struct Simulator {
    seed: u64,
    options: Options,
    prng: Prng,
    now: u64,
    network: Network<Message>,
    origin: Origin,
    keys: Vec<ObjectKey>,
    gateways: Vec<Gateway>,
    nodes: Vec<Node>,
    in_flight: Vec<usize>,
    requests: BTreeMap<u64, Request>,
    issued: u64,
    // What the server keeps per connection: who asked, and response bodies
    // waiting to be relayed.
    next_request: u64,
    gateway_requests: BTreeMap<(usize, ClientRequestId), u64>,
    node_requests: BTreeMap<(usize, GatewayRequestId), (usize, NodeRequestId)>,
    gateway_bodies: BTreeMap<(usize, NodeRequestId), Vec<u8>>,
    node_bodies: BTreeMap<(usize, OriginRequestId), Vec<u8>>,
    summary: Summary,
}

impl Simulator {
    /// A run whose configuration the seed also determines.
    pub fn from_seed(seed: u64) -> Simulator {
        let options = Options::swarm(&mut Prng::stream(seed, "options"));
        Simulator::new(seed, options)
    }

    pub fn new(seed: u64, options: Options) -> Simulator {
        let mut prng = Prng::stream(seed, "run");
        let members = (0..options.nodes)
            .map(|index| Member {
                id: NodeId(index as u64),
                weight: NonZeroU32::MIN,
            })
            .collect();
        let ring = Ring::new(1, members);
        let keys: Vec<ObjectKey> = (0..options.keys)
            .map(|index| ObjectKey {
                bucket: "bucket".into(),
                key: format!("key-{index}"),
            })
            .collect();
        let mut origin = Origin::default();
        for key in &keys {
            if prng.percent(90) {
                origin.put(0, key, prng.range(1..=options.object_size_max), &mut prng);
            }
        }
        Simulator {
            seed,
            prng,
            now: 0,
            network: Network::new(options.delay_min, options.delay_max),
            origin,
            keys,
            gateways: (0..options.gateways)
                .map(|_| Gateway::new(ring.clone()))
                .collect(),
            nodes: (0..options.nodes).map(|_| Node::new()).collect(),
            in_flight: vec![0; options.clients],
            requests: BTreeMap::new(),
            issued: 0,
            next_request: 0,
            gateway_requests: BTreeMap::new(),
            node_requests: BTreeMap::new(),
            gateway_bodies: BTreeMap::new(),
            node_bodies: BTreeMap::new(),
            summary: Summary {
                seed,
                ticks: 0,
                statuses: BTreeMap::new(),
                writes: 0,
                fingerprint: 0,
            },
            options,
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Runs until every request is answered.
    pub fn run(mut self) -> Result<Summary, Failure> {
        let tick_limit = 10_000 + self.options.requests * (self.options.delay_max * 6 + 10);
        while self.issued < self.options.requests || !self.requests.is_empty() {
            if self.now > tick_limit {
                let unanswered = self.requests.len();
                return Err(self.failure(format!("{unanswered} requests unanswered")));
            }
            self.tick()?;
        }
        self.check_quiescent()?;
        self.summary.ticks = self.now;
        Ok(self.summary)
    }

    fn tick(&mut self) -> Result<(), Failure> {
        self.tick_writes();
        self.tick_clients();
        while let Some((to, message)) = self.network.deliver(self.now) {
            self.deliver(to, message)?;
        }
        self.now += 1;
        Ok(())
    }

    fn tick_writes(&mut self) {
        if !self.prng.percent(self.options.write_percent) {
            return;
        }
        let key = &self.keys[self.prng.index(self.keys.len())];
        if self.prng.percent(self.options.delete_percent) {
            self.origin.delete(self.now, key);
        } else {
            let size = self.prng.range(1..=self.options.object_size_max);
            self.origin.put(self.now, key, size, &mut self.prng);
        }
        self.summary.writes += 1;
    }

    fn tick_clients(&mut self) {
        for client in 0..self.options.clients {
            if self.issued == self.options.requests
                || self.in_flight[client] == self.options.requests_in_flight
                || !self.prng.percent(self.options.request_percent)
            {
                continue;
            }
            let get = self.random_get();
            let request = self.issued;
            self.issued += 1;
            self.in_flight[client] += 1;
            let issued = self.now;
            let gateway = self.prng.index(self.options.gateways);
            self.send(
                Address::Gateway(gateway),
                Message::ClientGet {
                    request,
                    get: get.clone(),
                },
            );
            self.requests.insert(
                request,
                Request {
                    client,
                    get,
                    issued,
                },
            );
        }
    }

    /// A read of a random key, with ranges and preconditions that S3 may
    /// satisfy or reject.
    fn random_get(&mut self) -> GetObject {
        let key = self.keys[self.prng.index(self.keys.len())].clone();
        let span = self.options.object_size_max + 1;
        let range = match self.prng.below(5) {
            0 => {
                let first = self.prng.below(span);
                let last = first + self.prng.below(span);
                Some(ByteRange::Inclusive { first, last })
            }
            1 => Some(ByteRange::From {
                first: self.prng.below(span),
            }),
            2 => Some(ByteRange::Suffix {
                length: self.prng.range(1..=span),
            }),
            _ => None,
        };
        let if_match = match self.prng.below(10) {
            0 => self.origin.current(&key).map(|object| object.etag.clone()),
            1 => Some(ETag("\"stale\"".into())),
            _ => None,
        };
        GetObject {
            key,
            range,
            if_match,
        }
    }

    fn deliver(&mut self, to: Address, message: Message) -> Result<(), Failure> {
        match (to, message) {
            (Address::Gateway(gateway), Message::ClientGet { request, get }) => {
                let id = ClientRequestId(self.next_id());
                self.gateway_requests.insert((gateway, id), request);
                self.gateways[gateway].on_get(id, get);
                self.drain_gateway(gateway)
            }
            (
                Address::Gateway(gateway),
                Message::NodeResponse {
                    request,
                    head,
                    body,
                },
            ) => {
                self.gateway_bodies.insert((gateway, request), body);
                self.gateways[gateway].on_node_response(request, head);
                self.drain_gateway(gateway)
            }
            (
                Address::Node(node),
                Message::NodeGet {
                    gateway,
                    request,
                    get,
                },
            ) => {
                let id = GatewayRequestId(self.next_id());
                self.node_requests.insert((node, id), (gateway, request));
                self.nodes[node].on_get(id, get);
                self.drain_node(node)
            }
            (
                Address::Node(node),
                Message::OriginResponse {
                    request,
                    head,
                    body,
                },
            ) => {
                self.node_bodies.insert((node, request), body);
                self.nodes[node].on_origin_response(request, head);
                self.drain_node(node)
            }
            (Address::Origin, Message::OriginGet { node, request, get }) => {
                let (head, body) = self.origin.get(&get);
                let response = Message::OriginResponse {
                    request,
                    head,
                    body,
                };
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
                gateway::Action::Send { node, request, get } => {
                    let message = Message::NodeGet {
                        gateway,
                        request,
                        get,
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
                node::Action::Fetch { request, get } => {
                    self.send(Address::Origin, Message::OriginGet { node, request, get });
                }
                node::Action::Relay {
                    request,
                    head,
                    from,
                } => {
                    let Some(body) = self.node_bodies.remove(&(node, from)) else {
                        return Err(self.failure(format!("node {node} relayed {from:?} twice")));
                    };
                    let Some((gateway, id)) = self.node_requests.remove(&(node, request)) else {
                        return Err(self.failure(format!("node {node} answered {request:?} twice")));
                    };
                    let response = Message::NodeResponse {
                        request: id,
                        head,
                        body,
                    };
                    self.send(Address::Gateway(gateway), response);
                }
            }
        }
        Ok(())
    }

    fn answer(&mut self, request: u64, head: ResponseHead, body: Vec<u8>) -> Result<(), Failure> {
        let Request {
            client,
            get,
            issued,
        } = self
            .requests
            .remove(&request)
            .expect("one response per request");
        self.in_flight[client] -= 1;
        properties::check_response(&self.origin, &get, issued, self.now, &head, &body)
            .map_err(|message| self.failure(message))?;
        *self.summary.statuses.entry(head.status).or_default() += 1;
        let digest = xxh3_64_with_seed(&body, u64::from(head.status));
        self.summary.fingerprint = xxh3_64_with_seed(
            &[request, self.now, digest].map(u64::to_le_bytes).concat(),
            self.summary.fingerprint,
        );
        Ok(())
    }

    /// Once every request is answered, nothing may be left waiting.
    fn check_quiescent(&self) -> Result<(), Failure> {
        let stranded = self.gateway_requests.len()
            + self.node_requests.len()
            + self.gateway_bodies.len()
            + self.node_bodies.len();
        if stranded > 0 || !self.network.is_empty() {
            return Err(self.failure(format!(
                "{stranded} requests or bodies stranded after the last response"
            )));
        }
        Ok(())
    }

    fn send(&mut self, to: Address, message: Message) {
        self.network.send(&mut self.prng, self.now, to, message);
    }

    fn next_id(&mut self) -> u64 {
        self.next_request += 1;
        self.next_request
    }

    fn failure(&self, message: String) -> Failure {
        Failure {
            seed: self.seed,
            tick: self.now,
            message,
        }
    }
}

/// A passing run's outcome. Two runs of one seed produce equal summaries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    pub seed: u64,
    pub ticks: u64,
    pub statuses: BTreeMap<u16, u64>,
    pub writes: u64,
    /// A digest of every response: its request, tick, status and body.
    pub fingerprint: u64,
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
            "seed {} passed: {} ticks, {} writes, responses {}, fingerprint {:016x}",
            self.seed,
            self.ticks,
            self.writes,
            statuses.join(" "),
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
