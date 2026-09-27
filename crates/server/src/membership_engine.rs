//! A storage node's membership: foca's SWIM over UDP on the node's
//! address, with its timers on this thread. Each new ring goes to the node.
//!
//! A starting node first asks its seeds for their ring: if the ring lacks
//! the node, the node is new, and reads what it takes over from the owners
//! that held it. A leaving node drops out of every ring at once, serves its
//! blocks to their new owners through the fallback window, and then stops.

use crate::node_engine::{NodeEngine, SharedNode};
use crate::peers::{Exchanged, Peers};
use crate::protocol::{NodeAnswer, NodeRequest};
use s3_accelerator_core::Time;
use s3_accelerator_core::membership::{self, Membership};
use s3_accelerator_core::placement::NodeId;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

/// How long a starting node waits for each seed's ring.
const RING_WAIT: Duration = Duration::from_secs(1);

pub type SharedMembership = Rc<RefCell<MembershipEngine>>;

pub struct MembershipEngine {
    started: Instant,
    membership: Membership,
    socket: Rc<UdpSocket>,
    /// Where each node listens for gossip: its cluster address, from the
    /// config or from gossip.
    addresses: BTreeMap<NodeId, SocketAddr>,
    node: SharedNode,
    peers: Rc<Peers>,
}

impl MembershipEngine {
    fn now(&self) -> Time {
        Time(self.started.elapsed().as_millis() as u64)
    }
}

/// Joins the cluster through `seeds`, then gossips until the node stops.
/// `membership` started at `started`.
pub async fn run(
    started: Instant,
    membership: Membership,
    node: SharedNode,
    peers: Rc<Peers>,
    socket: UdpSocket,
    addresses: BTreeMap<NodeId, SocketAddr>,
    seeds: Vec<NodeId>,
) -> SharedMembership {
    let me = NodeId(membership.me().id);
    for &seed in seeds.iter().filter(|&&seed| seed != me) {
        let exchanged = tokio::time::timeout(RING_WAIT, peers.exchange(seed, &NodeRequest::Ring));
        if let Ok(Ok(Exchanged {
            answer: NodeAnswer::Ring { ring, addresses },
            body,
            ..
        })) = exchanged.await
        {
            peers.idle(body);
            peers.learn(&addresses);
            NodeEngine::on_joined(&node, ring);
            break;
        }
    }
    let engine = Rc::new(RefCell::new(MembershipEngine {
        started,
        membership,
        socket: Rc::new(socket),
        addresses,
        node,
        peers,
    }));
    {
        let mut this = engine.borrow_mut();
        let now = this.now();
        this.membership.join(now, &seeds);
    }
    apply(&engine);
    let receiving = engine.clone();
    tokio::task::spawn_local(async move {
        let socket = receiving.borrow().socket.clone();
        let mut packet = vec![0; 64 << 10];
        loop {
            let Ok((len, _)) = socket.recv_from(&mut packet).await else {
                continue;
            };
            {
                let mut this = receiving.borrow_mut();
                let now = this.now();
                this.membership.on_packet(now, &packet[..len]);
            }
            apply(&receiving);
        }
    });
    let ticking = engine.clone();
    tokio::task::spawn_local(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            {
                let mut this = ticking.borrow_mut();
                let now = this.now();
                this.membership.on_tick(now);
            }
            apply(&ticking);
        }
    });
    engine
}

/// Starts leaving the cluster: the node drops out of every ring and keeps
/// serving. Call `leave` once the fallback window has passed.
pub fn start_leaving(engine: &SharedMembership) {
    {
        let mut this = engine.borrow_mut();
        let now = this.now();
        this.membership.start_leaving(now);
    }
    apply(engine);
}

/// Declares the node down to the others.
pub fn leave(engine: &SharedMembership) {
    {
        let mut this = engine.borrow_mut();
        let now = this.now();
        this.membership.leave(now);
    }
    apply(engine);
}

/// Carries out membership's actions: packets go out over UDP, timers wait
/// on this thread, and a new ring goes to the node.
fn apply(engine: &SharedMembership) {
    let actions = engine.borrow_mut().membership.drain();
    for action in actions {
        match action {
            membership::Action::Send { to, packet } => {
                let this = engine.borrow();
                // Gossip tolerates lost packets, so a full socket drops one.
                if let Some(&address) = this.addresses.get(&to) {
                    let _ = this.socket.try_send_to(&packet, address);
                }
            }
            membership::Action::Schedule { timer, at } => {
                let engine = engine.clone();
                let due = engine.borrow().started + Duration::from_millis(at.0);
                tokio::task::spawn_local(async move {
                    tokio::time::sleep_until(due.into()).await;
                    {
                        let mut this = engine.borrow_mut();
                        let now = this.now();
                        this.membership.on_timer(now, timer);
                    }
                    apply(&engine);
                });
            }
            membership::Action::Ring(ring) => {
                let (node, addresses) = {
                    let mut this = engine.borrow_mut();
                    let addresses = this.membership.addresses();
                    this.peers.learn(&addresses);
                    for (&id, address) in &addresses {
                        let resolved = address
                            .to_socket_addrs()
                            .ok()
                            .and_then(|mut all| all.next());
                        if let Some(resolved) = resolved {
                            this.addresses.insert(id, resolved);
                        }
                    }
                    (this.node.clone(), addresses)
                };
                let members: Vec<u64> = ring.members().iter().map(|member| member.id.0).collect();
                eprintln!("ring {:016x}: nodes {members:?}", ring.version());
                NodeEngine::on_ring(&node, ring, addresses);
            }
        }
    }
}
