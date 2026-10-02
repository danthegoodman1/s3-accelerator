//! A gateway: runs the core's gateway on this thread, sends its reads to the
//! storage nodes over the cluster protocol, and tells each client's
//! connection how to answer. A node's body stays in its connection until
//! the client's connection relays it with `splice`, or the gateway drops it.
//! Every answer names the version of its node's ring, and the gateway
//! fetches a ring whose version differs from its own.
//!
//! A gateway runs several event loops, each with its own core. A loop
//! tells the others of each write it proxies before its client hears, so a
//! client's next read sees its write through any loop, and of each ring it
//! fetches, so they route by the same ring.

use crate::log;
use crate::metrics::{Metrics, NodeFailure};
use crate::peers::{Exchanged, NodeBody, Peers};
use crate::protocol::{self, Hint, NodeAnswer, NodeRequest, RequestId, Versions};
use s3_accelerator_core::Time;
use s3_accelerator_core::gateway::{self, ClientRequestId, Gateway, NodeRequestId};
use s3_accelerator_core::node::{HotHint, Read};
use s3_accelerator_core::placement::{NodeId, Placement, Ring};
use s3_accelerator_core::s3::{ObjectKey, Request, ResponseHead};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// How long a gateway waits for each node's ring.
const RING_WAIT: Duration = Duration::from_secs(1);
/// How long a gateway waits for homes to hear of writes before it answers.
const WRITTEN_WAIT: Duration = Duration::from_secs(5);

/// What a client's connection does next for its read.
pub enum Event {
    /// Answer with `head` and no body.
    Respond(ResponseHead),
    /// Start the response with `head`; forwards supply its body.
    Start(ResponseHead),
    /// Copy the first `len` bytes of the node's body into the response,
    /// then call `GatewayEngine::forwarded`.
    Forward {
        from: NodeRequestId,
        body: NodeBody,
        len: u64,
    },
    /// End the started response early.
    Abort,
}

pub type SharedGateway = Rc<RefCell<GatewayEngine>>;

/// What a gateway event loop tells the gateway's other loops.
pub enum Shared {
    /// Writes the loop proxied, each key with the node that told its home,
    /// if one did. The other loop answers on `applied` once its core has
    /// them.
    Writes {
        keys: Arc<[(ObjectKey, Option<NodeId>)]>,
        applied: oneshot::Sender<()>,
    },
    /// A ring the loop fetched in place of the ring whose version is
    /// `from`, with the nodes' addresses and those down.
    Ring {
        from: u64,
        ring: Ring,
        addresses: BTreeMap<NodeId, String>,
        down: Vec<NodeId>,
    },
}

pub struct GatewayEngine {
    started: Instant,
    metrics: Arc<Metrics>,
    gateway: Gateway,
    peers: Rc<Peers>,
    next_id: u64,
    clients: BTreeMap<ClientRequestId, mpsc::UnboundedSender<Event>>,
    /// Each client read's ID, which its node requests carry.
    request_ids: BTreeMap<ClientRequestId, RequestId>,
    /// Nodes' answered bodies, until forwarded or discarded.
    relayed: BTreeMap<NodeRequestId, NodeBody>,
    /// Reads to send to nodes, with their client reads' IDs, and nodes to
    /// fetch rings from, or `None` to try every node known.
    sends: Vec<(NodeId, NodeRequestId, Read, Option<RequestId>)>,
    ring_fetches: Vec<Option<NodeId>>,
    /// The gateway's other event loops.
    siblings: Vec<mpsc::UnboundedSender<Shared>>,
}

impl GatewayEngine {
    /// A gateway loop, which tells `siblings` what it learns.
    pub fn new(
        ring: Ring,
        config: gateway::Config,
        peers: Rc<Peers>,
        metrics: Arc<Metrics>,
        siblings: Vec<mpsc::UnboundedSender<Shared>>,
    ) -> SharedGateway {
        Rc::new(RefCell::new(GatewayEngine {
            started: Instant::now(),
            metrics,
            gateway: Gateway::new(ring, config),
            peers,
            next_id: 0,
            clients: BTreeMap::new(),
            request_ids: BTreeMap::new(),
            relayed: BTreeMap::new(),
            sends: Vec::new(),
            ring_fetches: Vec::new(),
            siblings,
        }))
    }

    /// Tells the gateway's other loops of writes this one proxied, and
    /// waits until each has them, so a client's next read sees its write
    /// through whichever loop serves it.
    pub async fn tell_writes(engine: &SharedGateway, keys: Vec<(ObjectKey, Option<NodeId>)>) {
        let keys: Arc<[(ObjectKey, Option<NodeId>)]> = keys.into();
        let siblings = engine.borrow().siblings.clone();
        let mut applied = Vec::new();
        for sibling in siblings {
            let (sender, receiver) = oneshot::channel();
            let writes = Shared::Writes {
                keys: keys.clone(),
                applied: sender,
            };
            if sibling.send(writes).is_ok() {
                applied.push(receiver);
            }
        }
        for receiver in applied {
            // A loop that stopped serves no reads to keep fresh.
            let _ = receiver.await;
        }
    }

    /// Takes what another of the gateway's loops shared.
    pub fn take_shared(engine: &SharedGateway, shared: Shared) {
        let mut this = engine.borrow_mut();
        let now = this.now();
        match shared {
            Shared::Writes { keys, applied } => {
                for (key, via) in keys.iter() {
                    this.gateway.on_write(now, key, *via);
                }
                let _ = applied.send(());
            }
            Shared::Ring {
                from,
                ring,
                addresses,
                down,
            } => {
                this.peers.learn(&addresses);
                // Versions don't say which ring is newer: a loop still on
                // the ring the sharer left takes the new one, a loop on the
                // new one takes its nodes down, and a loop that moved on
                // keeps its own.
                let own = this.gateway.ring().version();
                if own == from || own == ring.version() {
                    this.gateway.on_ring(now, ring, down);
                }
            }
        }
    }

    /// Tells the gateway's other loops of `shared`, which needs no answer.
    fn share(&self, shared: impl Fn() -> Shared) {
        for sibling in &self.siblings {
            let _ = sibling.send(shared());
        }
    }

    /// Serves a `GetObject` or `HeadObject` whose ID is `request_id`; what
    /// to answer arrives on the receiver.
    pub fn read(
        engine: &SharedGateway,
        request: Request,
        request_id: Option<RequestId>,
    ) -> mpsc::UnboundedReceiver<Event> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let work = {
            let mut this = engine.borrow_mut();
            this.next_id += 1;
            let id = ClientRequestId(this.next_id);
            this.clients.insert(id, sender);
            if let Some(request_id) = request_id {
                this.request_ids.insert(id, request_id);
            }
            let now = this.now();
            this.gateway.on_request(now, id, request);
            this.pump()
        };
        start(engine, work);
        receiver
    }

    /// The client's connection copied `copied` bytes of the node's body.
    /// A body read to its end leaves its connection for the next read.
    pub fn forwarded(
        engine: &SharedGateway,
        from: NodeRequestId,
        copied: u64,
        read_in_full: Option<NodeBody>,
    ) {
        let work = {
            let mut this = engine.borrow_mut();
            if let Some(mut body) = read_in_full {
                body.consumed(body.unread());
                this.peers.idle(body);
            }
            let now = this.now();
            this.gateway.on_forwarded(now, from, copied);
            this.pump()
        };
        start(engine, work);
    }

    /// A write through this gateway to `key` succeeded, and `via`, the
    /// node that passed it to S3, has told the key's home: the gateway
    /// forgets the key's metadata.
    pub fn written_via(engine: &SharedGateway, key: &ObjectKey, via: NodeId) {
        let mut this = engine.borrow_mut();
        let now = this.now();
        this.gateway.on_write(now, key, Some(via));
    }

    /// Writes through this gateway to `keys` succeeded, and no node told
    /// their homes: the gateway tells each home, and then forgets the keys'
    /// metadata, so its next read of a key sees its write. Returns each key
    /// with the home that heard of it, if one did.
    pub async fn written(
        engine: &SharedGateway,
        keys: Vec<ObjectKey>,
    ) -> Vec<(ObjectKey, Option<NodeId>)> {
        let (peers, homes) = {
            let this = engine.borrow();
            let ring = this.gateway.ring();
            let homes: Vec<Option<NodeId>> = keys
                .iter()
                .map(|key| ring.owner(Placement::Home(key).hash()))
                .collect();
            (this.peers.clone(), homes)
        };
        let mut by_home: BTreeMap<NodeId, Vec<usize>> = BTreeMap::new();
        for (index, home) in homes.iter().enumerate() {
            if let Some(home) = home {
                by_home.entry(*home).or_default().push(index);
            }
        }
        let keys = Rc::new(keys);
        let heard = Rc::new(RefCell::new(vec![false; keys.len()]));
        let mut telling = tokio::task::JoinSet::new();
        for (home, indexes) in by_home {
            let (peers, keys, heard) = (peers.clone(), keys.clone(), heard.clone());
            telling.spawn_local(async move {
                for index in indexes {
                    let request = NodeRequest::Written {
                        key: keys[index].clone(),
                        passed_on: false,
                    };
                    match peers.exchange(home, &request).await {
                        Ok(exchanged) => {
                            peers.idle(exchanged.body);
                            heard.borrow_mut()[index] = true;
                        }
                        Err(error) => {
                            log!(
                                Warn,
                                "telling a home of a write failed",
                                node = home.0,
                                error = error
                            );
                            return;
                        }
                    }
                }
            });
        }
        let told = async { while telling.join_next().await.is_some() {} };
        if tokio::time::timeout(WRITTEN_WAIT, told).await.is_err() {
            log!(Warn, "homes took too long to hear of writes");
        }
        let heard = heard.borrow();
        let writes: Vec<(ObjectKey, Option<NodeId>)> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (key.clone(), homes[index].filter(|_| heard[index])))
            .collect();
        let mut this = engine.borrow_mut();
        let now = this.now();
        for (key, via) in &writes {
            this.gateway.on_write(now, key, *via);
        }
        writes
    }

    /// The nodes to pass a request for `target` through to S3, best first.
    pub fn pass_candidates(engine: &SharedGateway, target: &ObjectKey) -> Vec<NodeId> {
        engine.borrow().gateway.pass_candidates(target)
    }

    pub fn peers(engine: &SharedGateway) -> Rc<Peers> {
        engine.borrow().peers.clone()
    }

    /// The ring's version with its nodes up and down, and whether a node
    /// has answered the gateway.
    pub fn observe(engine: &SharedGateway) -> ((u64, usize, usize), bool) {
        let this = engine.borrow();
        let ring = this.gateway.ring();
        let members = ring.members();
        let down = this.gateway.down();
        let held = members
            .iter()
            .filter(|member| down.contains(&member.id))
            .count();
        let view = (ring.version(), members.len() - held, held);
        (view, this.gateway.has_heard())
    }

    /// `node` answered stamped with `versions`.
    pub fn versions(engine: &SharedGateway, node: NodeId, versions: Versions) {
        let work = {
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway
                .on_ring_version(now, node, versions.ring, versions.down);
            this.pump()
        };
        start(engine, work);
    }

    /// Lets the gateway fail over from nodes that time out.
    pub fn tick(engine: &SharedGateway) {
        let work = {
            let mut this = engine.borrow_mut();
            let GatewayEngine {
                clients,
                request_ids,
                ..
            } = &mut *this;
            clients.retain(|_, client| !client.is_closed());
            request_ids.retain(|id, _| clients.contains_key(id));
            let now = this.now();
            this.gateway.on_tick(now);
            this.pump()
        };
        start(engine, work);
    }

    /// Carries out every action until the gateway has none left, and
    /// returns the reads to send and the rings to fetch.
    fn pump(&mut self) -> Work {
        loop {
            let actions = self.gateway.drain();
            if actions.is_empty() {
                return Work {
                    sends: std::mem::take(&mut self.sends),
                    ring_fetches: std::mem::take(&mut self.ring_fetches),
                };
            }
            for action in actions {
                self.act(action);
            }
        }
    }

    fn act(&mut self, action: gateway::Action) {
        match action {
            gateway::Action::Send {
                node,
                id,
                read,
                request,
            } => {
                let request_id = self.request_ids.get(&request).copied();
                self.sends.push((node, id, read, request_id));
            }
            gateway::Action::Start { request, head } => {
                self.tell(request, Event::Start(head));
            }
            gateway::Action::Forward { request, from, len } => {
                let now = self.now();
                // Every answered body is here until forwarded; a missing
                // one counts as ending at once, so the rest comes from
                // elsewhere.
                let Some(body) = self.relayed.remove(&from) else {
                    return self.gateway.on_forwarded(now, from, 0);
                };
                // A client that hung up needs no more of its body.
                if !self.tell(request, Event::Forward { from, body, len }) {
                    self.gateway.on_forwarded(now, from, len);
                }
            }
            gateway::Action::Abort { request } => {
                self.tell(request, Event::Abort);
                self.clients.remove(&request);
                self.request_ids.remove(&request);
            }
            gateway::Action::Respond { request, head } => {
                self.tell(request, Event::Respond(head));
                self.clients.remove(&request);
                self.request_ids.remove(&request);
            }
            gateway::Action::Discard { id } => {
                if let Some(body) = self.relayed.remove(&id) {
                    self.peers.idle(body);
                }
            }
            gateway::Action::FetchRing { node } => self.ring_fetches.push(Some(node)),
            gateway::Action::FindRing => self.ring_fetches.push(None),
        }
    }

    /// Passes `event` to the client's connection, and whether it was still
    /// open.
    fn tell(&mut self, request: ClientRequestId, event: Event) -> bool {
        self.clients
            .get(&request)
            .is_some_and(|client| client.send(event).is_ok())
    }

    fn now(&self) -> Time {
        Time(self.started.elapsed().as_millis() as u64)
    }
}

/// Hints on this gateway's clock.
fn hints(now: Time, hot: Vec<Hint>) -> Vec<HotHint> {
    hot.into_iter()
        .map(|hint| HotHint {
            placement: hint.placement,
            nodes: hint.nodes,
            until: Time(now.0 + hint.left),
        })
        .collect()
}

/// What the gateway's actions left to start.
struct Work {
    sends: Vec<(NodeId, NodeRequestId, Read, Option<RequestId>)>,
    ring_fetches: Vec<Option<NodeId>>,
}

/// Sends each read to its node and feeds the node's answer back to the
/// gateway, and fetches each ring asked for. A node that cannot be reached,
/// or answers out of protocol, fails the read over at once, as a 5xx does.
fn start(engine: &SharedGateway, work: Work) {
    for (node, id, read, request_id) in work.sends {
        let engine = engine.clone();
        tokio::task::spawn_local(async move {
            let peers = engine.borrow().peers.clone();
            let request = NodeRequest::Read(read);
            let exchanged = peers.exchange_for(node, &request, request_id).await;
            let work = {
                let mut this = engine.borrow_mut();
                let now = this.now();
                let versions = exchanged
                    .as_ref()
                    .ok()
                    .and_then(|exchanged| exchanged.versions);
                match exchanged {
                    Ok(Exchanged {
                        answer:
                            NodeAnswer::Respond {
                                head,
                                meta,
                                hot,
                                s3_error,
                            },
                        body,
                        ..
                    }) => {
                        this.relayed.insert(id, body);
                        this.gateway.on_hot(now, hints(now, hot));
                        this.gateway.on_node_response(now, id, head, meta, s3_error);
                    }
                    Ok(Exchanged {
                        answer: NodeAnswer::Metadata(meta, hot),
                        body,
                        ..
                    }) => {
                        this.peers.idle(body);
                        this.gateway.on_hot(now, hints(now, hot));
                        this.gateway.on_node_metadata(now, id, meta);
                    }
                    Ok(Exchanged {
                        answer: NodeAnswer::Stale,
                        body,
                        ..
                    }) => {
                        this.peers.idle(body);
                        this.gateway.on_node_stale(now, id);
                    }
                    // The node was reached, but answered out of protocol.
                    Ok(_) => {
                        this.metrics.node_failure(NodeFailure::Error);
                        this.gateway.on_node_response(
                            now,
                            id,
                            ResponseHead::status(503),
                            None,
                            false,
                        );
                    }
                    Err(error) => {
                        this.metrics.node_failure(NodeFailure::of(&error));
                        log!(
                            Warn,
                            "reading from a node failed",
                            request = protocol::logged(request_id),
                            node = node.0,
                            error = error
                        );
                        this.gateway.on_node_unreachable(now, id);
                    }
                }
                if let Some(versions) = versions {
                    this.gateway
                        .on_ring_version(now, node, versions.ring, versions.down);
                }
                this.pump()
            };
            start(&engine, work);
        });
    }
    for node in work.ring_fetches {
        let engine = engine.clone();
        tokio::task::spawn_local(async move {
            let peers = engine.borrow().peers.clone();
            let nodes = match node {
                Some(node) => vec![node],
                None => peers.nodes(),
            };
            for node in nodes {
                let exchanged =
                    tokio::time::timeout(RING_WAIT, peers.exchange(node, &NodeRequest::Ring));
                match exchanged.await {
                    Ok(Ok(Exchanged {
                        answer:
                            NodeAnswer::Ring {
                                ring,
                                addresses,
                                down,
                            },
                        body,
                        ..
                    })) => {
                        peers.idle(body);
                        peers.learn(&addresses);
                        let mut this = engine.borrow_mut();
                        let now = this.now();
                        let from = this.gateway.ring().version();
                        if ring.version() != from {
                            this.metrics.ring_changed(true);
                        }
                        this.share(|| Shared::Ring {
                            from,
                            ring: ring.clone(),
                            addresses: addresses.clone(),
                            down: down.clone(),
                        });
                        this.gateway.on_ring(now, ring, down);
                        return;
                    }
                    Ok(Ok(_)) => {
                        engine.borrow().metrics.node_failure(NodeFailure::Error);
                        log!(
                            Warn,
                            "a node answered a ring request out of protocol",
                            node = node.0
                        )
                    }
                    Ok(Err(error)) => {
                        engine
                            .borrow()
                            .metrics
                            .node_failure(NodeFailure::of(&error));
                        log!(Warn, "fetching a ring failed", node = node.0, error = error)
                    }
                    Err(_) => {
                        engine.borrow().metrics.node_failure(NodeFailure::Timeout);
                        log!(Warn, "fetching a ring timed out", node = node.0)
                    }
                }
            }
            // The next answer with another version, or the next read no
            // node can serve, asks again.
            let mut this = engine.borrow_mut();
            let now = this.now();
            this.gateway.on_ring_failed(now);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3_accelerator_core::placement::Member;
    use std::num::NonZeroU32;

    fn ring(version: u64, nodes: &[u64]) -> Ring {
        let members = nodes
            .iter()
            .map(|&id| Member {
                id: NodeId(id),
                weight: NonZeroU32::MIN,
            })
            .collect();
        Ring::new(version, members)
    }

    fn shared(from: u64, ring: Ring, down: Vec<NodeId>) -> Shared {
        Shared::Ring {
            from,
            ring,
            addresses: BTreeMap::new(),
            down,
        }
    }

    /// A loop takes a sibling's ring only in place of the ring the sibling
    /// left, since versions don't say which ring is newer, and takes the
    /// nodes down that come with the ring it holds.
    #[test]
    fn a_loop_takes_a_siblings_ring_in_place_of_the_one_it_left() {
        let config: crate::config::Config =
            toml::from_str("[cluster]\nsecret = \"s\"\nnodes = []\n[gateway]\nlisten = \"x\"\n")
                .unwrap();
        let peers = Peers::new(BTreeMap::new(), "s".into(), None, Default::default());
        let metrics = Arc::new(Metrics::default());
        let engine = GatewayEngine::new(
            ring(1, &[0, 1]),
            config.cache.gateway_config(),
            peers,
            metrics,
            Vec::new(),
        );
        let version = |engine: &SharedGateway| engine.borrow().gateway.ring().version();
        GatewayEngine::take_shared(&engine, shared(1, ring(2, &[0, 1, 2]), Vec::new()));
        assert_eq!(version(&engine), 2);
        // A sibling still sharing what it fetched in place of ring 1.
        GatewayEngine::take_shared(&engine, shared(1, ring(3, &[0]), Vec::new()));
        assert_eq!(version(&engine), 2);
        GatewayEngine::take_shared(&engine, shared(7, ring(2, &[0, 1, 2]), vec![NodeId(2)]));
        assert_eq!(version(&engine), 2);
        assert!(engine.borrow().gateway.down().contains(&NodeId(2)));
    }
}
