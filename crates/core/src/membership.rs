//! Cluster membership: SWIM gossip among storage nodes, through foca, and
//! the ring each node derives from what it hears.
//!
//! A member foca declares down stays in the ring for `down_grace`, so a
//! brief failure moves no ownership; gateways route around it meanwhile. A
//! member that is leaving announces it and drops out of the ring at once,
//! while it keeps serving. Each ring's version is a hash of its members, so
//! nodes that agree on the members agree on the version.

use crate::Time;
use crate::placement::{Member, NodeId, Ring};
use foca::{
    Config as FocaConfig, Foca, Identity, NoCustomBroadcast, Notification, PeriodicParams,
    PostcardCodec, Runtime, Timer,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::num::{NonZeroU8, NonZeroU32, NonZeroUsize};
use std::time::Duration;
use xxhash_rust::xxh3::xxh3_64;

/// A storage node as the cluster knows it. A restarted node takes a later
/// run, which replaces the earlier one wherever the two meet.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Peer {
    pub id: u64,
    /// Its share of placement, typically its disk size.
    pub weight: u32,
    pub run: u64,
    /// The node is leaving the cluster: it serves while others take over
    /// what it owned, but owns nothing.
    pub leaving: bool,
    /// Where other processes reach it, which gossip spreads to those that
    /// were never told.
    pub address: String,
}

impl Identity for Peer {
    type Addr = u64;

    fn renew(&self) -> Option<Peer> {
        Some(Peer {
            run: self.run + 1,
            ..self.clone()
        })
    }

    fn addr(&self) -> u64 {
        self.id
    }

    fn win_addr_conflict(&self, adversary: &Peer) -> bool {
        (self.run, self.leaving) > (adversary.run, adversary.leaving)
    }
}

/// Membership's timings, in the owner's time units.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often a node probes another, and how long it waits for the
    /// answer before asking others to probe it.
    pub probe_period: u64,
    pub probe_rtt: u64,
    /// How long a member stays suspected before it is declared down.
    pub suspect_to_down: u64,
    /// How long a member declared down stays in the ring.
    pub down_grace: u64,
    /// How often a node gossips updates to a few others.
    pub gossip_period: u64,
    /// The largest packet a node sends.
    pub max_packet: usize,
}

/// A timer membership set, to hand back to `on_timer` when it fires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipTimer(Timer<Peer>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `packet` to node `to`.
    Send { to: NodeId, packet: Vec<u8> },
    /// Call `on_timer` with `timer` at `at`.
    Schedule { timer: MembershipTimer, at: Time },
    /// The ring changed.
    Ring(Ring),
}

type Swim = Foca<Peer, PostcardCodec, SplitMix, NoCustomBroadcast>;

/// Probe periods between announcements to seeds a node does not hear from.
const REJOIN_PERIODS: u64 = 10;

pub struct Membership {
    foca: Swim,
    me: Peer,
    config: Config,
    now: Time,
    /// Members foca reports up, by ID.
    up: BTreeMap<u64, Peer>,
    /// Members declared down, or not yet heard from, and since when.
    down: BTreeMap<u64, (Peer, Time)>,
    ring: Ring,
    /// The nodes this one joined through, and when it next announces
    /// itself again to those it does not hear from.
    seeds: Vec<NodeId>,
    next_rejoin: Time,
    actions: Vec<Action>,
}

impl Membership {
    /// A node about to join. `known` are the other nodes it starts with,
    /// such as those its config names: they stay in its ring for
    /// `down_grace` unless it hears from them. `seed` seeds foca's random
    /// choices.
    pub fn new(now: Time, me: Peer, known: &[Peer], config: Config, seed: u64) -> Membership {
        let foca = Foca::new(
            me.clone(),
            foca_config(&config),
            SplitMix(seed),
            PostcardCodec,
        );
        let down = known
            .iter()
            .filter(|peer| peer.id != me.id)
            .map(|peer| (peer.id, (peer.clone(), now)))
            .collect();
        let mut membership = Membership {
            foca,
            me,
            config,
            now,
            up: BTreeMap::new(),
            down,
            ring: Ring::new(0, Vec::new()),
            seeds: Vec::new(),
            next_rejoin: now,
            actions: Vec::new(),
        };
        membership.ring = membership.derive_ring();
        membership
    }

    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    pub fn me(&self) -> &Peer {
        &self.me
    }

    /// Where each node in the ring is reached.
    pub fn addresses(&self) -> BTreeMap<NodeId, String> {
        self.down
            .values()
            .map(|(peer, _)| peer)
            .chain(self.up.values())
            .chain([&self.me])
            .map(|peer| (NodeId(peer.id), peer.address.clone()))
            .collect()
    }

    /// The actions since the last drain, in order.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    /// Announces this node to each of `seeds`, the nodes it knows, and
    /// again every few probe periods to those it does not hear from: an
    /// announcement may be lost, and nodes that each declared the others
    /// down, as a partition makes them, would otherwise stay apart.
    pub fn join(&mut self, now: Time, seeds: &[NodeId]) {
        self.now = self.now.max(now);
        self.seeds = seeds.to_vec();
        self.announce(seeds);
    }

    fn announce(&mut self, seeds: &[NodeId]) {
        self.next_rejoin = Time(self.now.0 + REJOIN_PERIODS * self.config.probe_period);
        let mut runtime = Collected::default();
        for seed in seeds.iter().filter(|seed| seed.0 != self.me.id) {
            // An announcement reaches whichever run of the node is up.
            let dst = Peer {
                id: seed.0,
                weight: 1,
                run: 0,
                leaving: false,
                address: String::new(),
            };
            let _ = self.foca.announce(dst, &mut runtime);
        }
        self.apply(runtime);
    }

    /// Starts leaving: this node drops out of every ring, including its
    /// own, while it goes on serving. `leave` then stops its gossip.
    pub fn start_leaving(&mut self, now: Time) {
        self.now = self.now.max(now);
        self.me.leaving = true;
        let mut runtime = Collected::default();
        let _ = self.foca.change_identity(self.me.clone(), &mut runtime);
        self.apply(runtime);
    }

    /// Declares this node down to the others and stops taking part.
    pub fn leave(&mut self, now: Time) {
        self.now = self.now.max(now);
        let mut runtime = Collected::default();
        let _ = self.foca.leave_cluster(&mut runtime);
        self.apply(runtime);
    }

    /// A packet from another node arrived. A malformed one is dropped.
    pub fn on_packet(&mut self, now: Time, packet: &[u8]) {
        self.now = self.now.max(now);
        let mut runtime = Collected::default();
        let _ = self.foca.handle_data(packet, &mut runtime);
        self.apply(runtime);
    }

    pub fn on_timer(&mut self, now: Time, timer: MembershipTimer) {
        self.now = self.now.max(now);
        let mut runtime = Collected::default();
        let _ = self.foca.handle_timer(timer.0, &mut runtime);
        self.apply(runtime);
    }

    /// Time passed: members down longer than `down_grace` leave the ring,
    /// and seeds not heard from hear from this node again.
    pub fn on_tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        if self.now >= self.next_rejoin && !self.me.leaving {
            let silent: Vec<NodeId> = self
                .seeds
                .iter()
                .filter(|seed| !self.up.contains_key(&seed.0))
                .copied()
                .collect();
            self.announce(&silent);
        }
        self.update_ring();
    }

    /// Carries out what foca asked for, and updates the ring from its
    /// notifications.
    fn apply(&mut self, runtime: Collected) {
        for (to, packet) in runtime.sends {
            self.actions.push(Action::Send {
                to: NodeId(to.id),
                packet,
            });
        }
        for (timer, after) in runtime.timers {
            let at = Time(self.now.0 + after.as_millis() as u64);
            self.actions.push(Action::Schedule {
                timer: MembershipTimer(timer),
                at,
            });
        }
        for note in runtime.notes {
            match note {
                Note::Up(peer) => {
                    self.down.remove(&peer.id);
                    self.up.insert(peer.id, peer);
                }
                Note::Down(peer) => {
                    // A later run of the node may already have replaced it.
                    if self.up.get(&peer.id) == Some(&peer) {
                        self.up.remove(&peer.id);
                        self.down.insert(peer.id, (peer, self.now));
                    }
                }
                Note::Renamed(peer) => {
                    self.down.remove(&peer.id);
                    self.up.insert(peer.id, peer);
                }
            }
        }
        self.update_ring();
    }

    fn update_ring(&mut self) {
        let grace = self.config.down_grace;
        let now = self.now;
        self.down.retain(|_, (_, since)| now.0 < since.0 + grace);
        let ring = self.derive_ring();
        if ring != self.ring {
            self.ring = ring.clone();
            self.actions.push(Action::Ring(ring));
        }
    }

    /// Members up or down within the grace period, this node among them,
    /// less those leaving.
    fn derive_ring(&self) -> Ring {
        let peers = self
            .down
            .values()
            .map(|(peer, _)| peer)
            .chain(self.up.values())
            .chain([&self.me]);
        ring_of(peers)
    }
}

/// The ring of `peers`, less those leaving, versioned by a hash of its
/// members. A later entry for a node replaces an earlier one.
pub fn ring_of<'a>(peers: impl IntoIterator<Item = &'a Peer>) -> Ring {
    let mut weights = BTreeMap::new();
    for peer in peers {
        match peer.leaving {
            true => weights.remove(&peer.id),
            false => weights.insert(peer.id, peer.weight),
        };
    }
    let mut encoded = Vec::with_capacity(weights.len() * 12);
    for (&id, &weight) in &weights {
        encoded.extend_from_slice(&id.to_le_bytes());
        encoded.extend_from_slice(&weight.to_le_bytes());
    }
    let members = weights
        .into_iter()
        .map(|(id, weight)| Member {
            id: NodeId(id),
            weight: NonZeroU32::new(weight).unwrap_or(NonZeroU32::MIN),
        })
        .collect();
    Ring::new(xxh3_64(&encoded), members)
}

fn foca_config(config: &Config) -> FocaConfig {
    let millis = Duration::from_millis;
    FocaConfig {
        probe_period: millis(config.probe_period),
        probe_rtt: millis(config.probe_rtt),
        num_indirect_probes: NonZeroUsize::new(3).expect("nonzero"),
        max_transmissions: NonZeroU8::new(8).expect("nonzero"),
        suspect_to_down_after: millis(config.suspect_to_down),
        remove_down_after: Some(millis(config.down_grace.max(config.suspect_to_down) * 4)),
        max_packet_size: NonZeroUsize::new(config.max_packet).expect("a packet size"),
        notify_down_members: true,
        periodic_announce: Some(PeriodicParams {
            frequency: millis(config.probe_period * 30),
            num_members: NonZeroUsize::MIN,
        }),
        periodic_announce_to_down_members: Some(PeriodicParams {
            frequency: millis(config.probe_period * 60),
            num_members: NonZeroUsize::new(2).expect("nonzero"),
        }),
        periodic_gossip: Some(PeriodicParams {
            frequency: millis(config.gossip_period),
            num_members: NonZeroUsize::new(3).expect("nonzero"),
        }),
    }
}

/// What foca asked for during one call.
#[derive(Default)]
struct Collected {
    sends: Vec<(Peer, Vec<u8>)>,
    timers: Vec<(Timer<Peer>, Duration)>,
    notes: Vec<Note>,
}

enum Note {
    Up(Peer),
    Down(Peer),
    /// A member's identity changed, such as to announce it is leaving.
    Renamed(Peer),
}

impl Runtime<Peer> for Collected {
    fn notify(&mut self, notification: Notification<'_, Peer>) {
        match notification {
            Notification::MemberUp(peer) => self.notes.push(Note::Up(peer.clone())),
            Notification::MemberDown(peer) => self.notes.push(Note::Down(peer.clone())),
            Notification::Rename(_, peer) => self.notes.push(Note::Renamed(peer.clone())),
            _ => {}
        }
    }

    fn send_to(&mut self, to: Peer, data: &[u8]) {
        self.sends.push((to, data.to_vec()));
    }

    fn submit_after(&mut self, event: Timer<Peer>, after: Duration) {
        self.timers.push((event, after));
    }
}

/// SplitMix64: foca's random choices, from a seed the owner passes in.
struct SplitMix(u64);

impl rand_core::TryRng for SplitMix {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        Ok((self.try_next_u64()? >> 32) as u32)
    }

    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        Ok(z ^ (z >> 31))
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
        for chunk in dst.chunks_mut(8) {
            let bytes = self.try_next_u64()?.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn config() -> Config {
        Config {
            probe_period: 100,
            probe_rtt: 40,
            suspect_to_down: 300,
            down_grace: 1_000,
            gossip_period: 50,
            max_packet: 1_400,
        }
    }

    fn peer(id: u64) -> Peer {
        Peer {
            id,
            weight: 1,
            run: 0,
            leaving: false,
            address: format!("node-{id}"),
        }
    }

    /// Nodes wired together with a 5 ms network; `cut` nodes hear nothing
    /// and are heard by no one.
    struct Cluster {
        now: u64,
        nodes: BTreeMap<u64, Membership>,
        timers: Vec<(u64, u64, MembershipTimer)>,
        packets: Vec<(u64, u64, Vec<u8>)>,
        cut: BTreeSet<u64>,
    }

    impl Cluster {
        fn new(ids: &[u64]) -> Cluster {
            let known: Vec<Peer> = ids.iter().map(|&id| peer(id)).collect();
            let mut cluster = Cluster {
                now: 0,
                nodes: BTreeMap::new(),
                timers: Vec::new(),
                packets: Vec::new(),
                cut: BTreeSet::new(),
            };
            for &id in ids {
                cluster.add(peer(id), &known);
            }
            cluster
        }

        fn add(&mut self, me: Peer, known: &[Peer]) {
            let id = me.id;
            let mut membership = Membership::new(Time(self.now), me, known, config(), id);
            let seeds: Vec<NodeId> = known.iter().map(|peer| NodeId(peer.id)).collect();
            membership.join(Time(self.now), &seeds);
            self.nodes.insert(id, membership);
            self.collect(id);
        }

        fn collect(&mut self, id: u64) {
            for action in self.nodes.get_mut(&id).unwrap().drain() {
                match action {
                    // A cut node's packets go nowhere.
                    Action::Send { .. } if self.cut.contains(&id) => {}
                    Action::Send { to, packet } => self.packets.push((self.now + 5, to.0, packet)),
                    Action::Schedule { timer, at } => self.timers.push((at.0, id, timer)),
                    Action::Ring(_) => {}
                }
            }
        }

        fn run(&mut self, until: u64) {
            while self.now < until {
                self.now += 1;
                let now = self.now;
                let (due, later): (Vec<_>, Vec<_>) =
                    self.packets.drain(..).partition(|(at, _, _)| *at <= now);
                self.packets = later;
                for (_, to, packet) in due {
                    if self.cut.contains(&to) || !self.nodes.contains_key(&to) {
                        continue;
                    }
                    self.nodes
                        .get_mut(&to)
                        .unwrap()
                        .on_packet(Time(now), &packet);
                    self.collect(to);
                }
                let (due, later): (Vec<_>, Vec<_>) =
                    self.timers.drain(..).partition(|(at, _, _)| *at <= now);
                self.timers = later;
                for (_, id, timer) in due {
                    if let Some(node) = self.nodes.get_mut(&id) {
                        node.on_timer(Time(now), timer);
                        self.collect(id);
                    }
                }
                let ids: Vec<u64> = self.nodes.keys().copied().collect();
                for id in ids {
                    self.nodes.get_mut(&id).unwrap().on_tick(Time(now));
                    self.collect(id);
                }
            }
        }

        fn members(&self, id: u64) -> Vec<u64> {
            self.nodes[&id]
                .ring()
                .members()
                .iter()
                .map(|member| member.id.0)
                .collect()
        }
    }

    #[test]
    fn nodes_that_start_together_agree_on_the_ring() {
        let mut cluster = Cluster::new(&[1, 2, 3]);
        cluster.run(2_000);
        for id in [1, 2, 3] {
            assert_eq!(cluster.members(id), [1, 2, 3], "node {id}");
        }
        let versions: BTreeSet<u64> = cluster
            .nodes
            .values()
            .map(|node| node.ring().version())
            .collect();
        assert_eq!(versions.len(), 1);
    }

    /// A node cut off from the rest is declared down, but stays in their
    /// rings until the grace period ends.
    #[test]
    fn a_down_node_stays_in_the_ring_for_the_grace_period() {
        let mut cluster = Cluster::new(&[1, 2, 3]);
        cluster.run(1_000);
        cluster.cut.insert(3);
        // Probes fail and suspicion runs out well within 800 ms.
        cluster.run(1_800);
        assert!(!cluster.nodes[&1].up.contains_key(&3), "node 3 is still up");
        assert_eq!(cluster.members(1), [1, 2, 3]);
        cluster.run(3_000);
        assert_eq!(cluster.members(1), [1, 2]);
        assert_eq!(cluster.members(2), [1, 2]);
    }

    /// A leaving node leaves every ring at once, and goes on gossiping.
    #[test]
    fn a_leaving_node_leaves_every_ring_at_once() {
        let mut cluster = Cluster::new(&[1, 2, 3]);
        cluster.run(1_000);
        let now = Time(cluster.now);
        cluster.nodes.get_mut(&3).unwrap().start_leaving(now);
        cluster.collect(3);
        cluster.run(1_300);
        for id in [1, 2, 3] {
            assert_eq!(cluster.members(id), [1, 2], "node {id}");
        }
    }

    /// Nodes cut apart long enough to drop each other find each other
    /// again once the cut heals: each announces itself to the seeds it no
    /// longer hears from.
    #[test]
    fn nodes_cut_apart_past_the_grace_period_merge_again() {
        let mut cluster = Cluster::new(&[1, 2, 3]);
        cluster.run(1_000);
        cluster.cut.insert(3);
        cluster.run(4_000);
        assert_eq!(cluster.members(1), [1, 2]);
        assert_eq!(cluster.members(3), [3]);
        cluster.cut.clear();
        cluster.run(7_000);
        for id in [1, 2, 3] {
            assert_eq!(cluster.members(id), [1, 2, 3], "node {id}");
        }
    }

    #[test]
    fn a_node_that_joins_later_enters_every_ring() {
        let mut cluster = Cluster::new(&[1, 2, 3]);
        cluster.run(1_000);
        cluster.add(peer(4), &[peer(1)]);
        cluster.run(3_000);
        for id in [1, 2, 3, 4] {
            assert_eq!(cluster.members(id), [1, 2, 3, 4], "node {id}");
        }
    }
}
