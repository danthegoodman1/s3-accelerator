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
use hmac::{Hmac, KeyInit, Mac};
use s3_accelerator_core::Time;
use s3_accelerator_core::membership::{self, Membership};
use s3_accelerator_core::placement::NodeId;
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
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
    /// The key gossip packets carry a tag under, from the cluster's secret.
    key: GossipKey,
    /// Where each node listens for gossip, its cluster address, as last
    /// resolved from the address membership gives, and names resolving.
    resolved: BTreeMap<NodeId, (String, SocketAddr)>,
    resolving: BTreeMap<NodeId, String>,
    node: SharedNode,
    peers: Rc<Peers>,
}

impl MembershipEngine {
    fn now(&self) -> Time {
        Time(self.started.elapsed().as_millis() as u64)
    }
}

/// A key that tags gossip packets, derived from the cluster's secret, so
/// only processes that hold the secret take part in membership.
#[derive(Clone)]
pub struct GossipKey([u8; 32]);

/// Bytes of the tag that ends each gossip packet: an HMAC-SHA256 of the
/// packet under the gossip key.
const TAG: usize = 32;

impl GossipKey {
    pub fn new(secret: &str) -> GossipKey {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
        mac.update(b"s3-accelerator gossip");
        GossipKey(mac.finalize().into_bytes().into())
    }

    fn mac(&self, packet: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("any key length");
        mac.update(packet);
        mac
    }

    /// `packet` followed by its tag.
    pub fn seal(&self, packet: &[u8]) -> Vec<u8> {
        let mut sealed = packet.to_vec();
        sealed.extend_from_slice(&self.mac(packet).finalize().into_bytes());
        sealed
    }

    /// The packet `sealed` carries, if its tag is right.
    pub fn open<'a>(&self, sealed: &'a [u8]) -> Option<&'a [u8]> {
        let (packet, tag) = sealed.split_at(sealed.len().checked_sub(TAG)?);
        self.mac(packet).verify_slice(tag).ok()?;
        Some(packet)
    }
}

/// Joins the cluster through `seeds`, then gossips until the node stops.
/// `membership` started at `started`; `key` tags its packets.
pub async fn run(
    started: Instant,
    membership: Membership,
    node: SharedNode,
    peers: Rc<Peers>,
    socket: UdpSocket,
    seeds: Vec<NodeId>,
    key: GossipKey,
) -> SharedMembership {
    let me = NodeId(membership.me().id);
    for &seed in seeds.iter().filter(|&&seed| seed != me) {
        let exchanged = tokio::time::timeout(RING_WAIT, peers.exchange(seed, &NodeRequest::Ring));
        if let Ok(Ok(Exchanged {
            answer: NodeAnswer::Ring {
                ring, addresses, ..
            },
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
        key,
        resolved: BTreeMap::new(),
        resolving: BTreeMap::new(),
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
                let Some(opened) = this.key.open(&packet[..len]) else {
                    // Only a process without the cluster's secret sends it.
                    continue;
                };
                let opened = opened.to_vec();
                let now = this.now();
                this.membership.on_packet(now, &opened);
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

/// Learns the addresses membership now gives, such as of a node just
/// heard from or restarted elsewhere, before any packet goes to it. A name
/// resolves off this thread's critical path, and packets to a node wait for
/// nothing: until its address resolves, they are lost, as gossip allows.
fn refresh(engine: &SharedMembership) {
    let mut this = engine.borrow_mut();
    let addresses = this.membership.addresses();
    this.peers.learn(&addresses);
    for (id, address) in addresses {
        let known = this
            .resolved
            .get(&id)
            .is_some_and(|(known, _)| *known == address);
        if known || this.resolving.get(&id) == Some(&address) {
            continue;
        }
        if let Ok(resolved) = address.parse::<SocketAddr>() {
            this.resolved.insert(id, (address, resolved));
            continue;
        }
        this.resolving.insert(id, address.clone());
        let engine = engine.clone();
        tokio::task::spawn_local(async move {
            let resolved = tokio::net::lookup_host(address.as_str()).await;
            let mut this = engine.borrow_mut();
            if this.resolving.get(&id) != Some(&address) {
                return;
            }
            this.resolving.remove(&id);
            match resolved.map(|mut all| all.next()) {
                Ok(Some(resolved)) => {
                    this.resolved.insert(id, (address, resolved));
                }
                _ => eprintln!("node {}'s address {address} does not resolve", id.0),
            }
        });
    }
}

/// Carries out membership's actions: packets go out over UDP, timers wait
/// on this thread, and a new ring goes to the node.
fn apply(engine: &SharedMembership) {
    refresh(engine);
    let actions = engine.borrow_mut().membership.drain();
    for action in actions {
        match action {
            membership::Action::Send { to, packet } => {
                let this = engine.borrow();
                // Gossip tolerates lost packets, so a full socket drops one.
                if let Some((_, address)) = this.resolved.get(&to) {
                    let _ = this.socket.try_send_to(&this.key.seal(&packet), *address);
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
                    let this = engine.borrow();
                    (this.node.clone(), this.membership.addresses())
                };
                let members: Vec<u64> = ring.members().iter().map(|member| member.id.0).collect();
                eprintln!("ring {:016x}: nodes {members:?}", ring.version());
                NodeEngine::on_ring(&node, ring, addresses);
            }
            membership::Action::Down(down) => {
                let node = engine.borrow().node.clone();
                NodeEngine::on_down(&node, down);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_clusters_key_opens_a_packet() {
        let key = GossipKey::new("cluster-secret");
        let sealed = key.seal(b"announce");
        assert_eq!(key.open(&sealed), Some(&b"announce"[..]));
        assert_eq!(GossipKey::new("other-secret").open(&sealed), None);
        let mut altered = sealed.clone();
        altered[0] ^= 1;
        assert_eq!(key.open(&altered), None);
        assert_eq!(key.open(b"announce"), None);
        assert_eq!(key.open(b""), None);
    }
}
