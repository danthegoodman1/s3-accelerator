//! Weighted rendezvous hashing over stable node IDs.
//!
//! Every gateway and storage node computes placement independently, so the
//! same ring and key must give the same answer on every machine and every
//! build. `placement_is_stable` pins that.

use crate::s3::ObjectKey;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

/// A storage node's stable identity. It survives restarts, so a restarted
/// node keeps the blocks it owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// A storage node and its share of placement, typically its disk size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    pub weight: NonZeroU32,
}

/// What placement assigns to a node.
#[derive(Clone, Copy, Debug)]
pub enum Placement<'a> {
    /// The object's home: its metadata, chunk 0 and its final 16 MiB.
    Home(&'a ObjectKey),
    /// One of the object's other chunks.
    Chunk(&'a ObjectKey, u64),
}

impl Placement<'_> {
    pub fn hash(self) -> PlacementHash {
        let (key, chunk) = match self {
            Placement::Home(key) => (key, None),
            Placement::Chunk(key, index) => (key, Some(index)),
        };
        // Bucket and key are length-prefixed, so no two placements encode alike.
        let mut point = Vec::with_capacity(key.bucket.len() + key.key.len() + 16);
        for part in [&key.bucket, &key.key] {
            point.extend_from_slice(&(part.len() as u32).to_le_bytes());
            point.extend_from_slice(part.as_bytes());
        }
        if let Some(index) = chunk {
            point.extend_from_slice(&index.to_le_bytes());
        }
        PlacementHash(xxh3_64(&point))
    }
}

/// The hash nodes are ranked by. A node stores it with each block, so it can
/// recheck the block's owner after a ring change without the object's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementHash(pub u64);

/// A version of the nodes membership holds down: 0 when none are, and a
/// hash of their IDs otherwise. Nodes send it with every answer, so a
/// gateway learns of a change from any node it asks.
pub fn down_version(down: &[NodeId]) -> u64 {
    if down.is_empty() {
        return 0;
    }
    let bytes: Vec<u8> = down.iter().flat_map(|node| node.0.to_le_bytes()).collect();
    xxh3_64(&bytes)
}

/// An immutable, versioned snapshot of the storage nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ring {
    version: u64,
    members: Vec<Member>,
    /// Whether every member has the same weight. A member's score then rises
    /// with its hash alone, so comparing hashes ranks members as the scores
    /// do, without a logarithm for each.
    uniform: bool,
}

impl Ring {
    pub fn new(version: u64, mut members: Vec<Member>) -> Ring {
        members.sort_by_key(|member| member.id);
        members.dedup_by_key(|member| member.id);
        let uniform = members
            .windows(2)
            .all(|pair| pair[0].weight == pair[1].weight);
        Ring {
            version,
            members,
            uniform,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// The node that owns `placement`, or `None` for an empty ring.
    pub fn owner(&self, placement: PlacementHash) -> Option<NodeId> {
        if self.uniform {
            let ranked = self
                .members
                .iter()
                .map(|member| (draw(placement, member), member.id));
            return ranked.max().map(|(_, id)| id);
        }
        self.members
            .iter()
            .map(|member| (score(placement, member), member.id))
            .max_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, id)| id)
    }

    /// Every node, best candidate first. The first owns `placement`; the next
    /// ones take over when it fails and hold its hot-key replicas.
    pub fn candidates(&self, placement: PlacementHash) -> Vec<NodeId> {
        if self.uniform {
            let mut ranked: Vec<_> = self
                .members
                .iter()
                .map(|member| (draw(placement, member), member.id))
                .collect();
            ranked.sort_unstable_by(|a, b| b.cmp(a));
            return ranked.into_iter().map(|(_, id)| id).collect();
        }
        let mut scored: Vec<_> = self
            .members
            .iter()
            .map(|member| (score(placement, member), member.id))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.cmp(&a.1)));
        scored.into_iter().map(|(_, id)| id).collect()
    }
}

/// A member's draw for `placement`: 53 bits, which a score turns into a
/// point in (0, 1).
fn draw(placement: PlacementHash, member: &Member) -> u64 {
    xxh3_64_with_seed(&placement.0.to_le_bytes(), member.id.0) >> 11
}

/// The logarithmic method: each node wins with probability proportional to
/// its weight, and a membership change moves only the placements it must.
fn score(placement: PlacementHash, member: &Member) -> f64 {
    let unit = (draw(placement, member) as f64 + 0.5) / (1u64 << 53) as f64;
    // libm computes the same logarithm on every platform.
    f64::from(member.weight.get()) / -libm::log(unit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn ring(weights: &[(u64, u32)]) -> Ring {
        let members = weights
            .iter()
            .map(|&(id, weight)| Member {
                id: NodeId(id),
                weight: NonZeroU32::new(weight).unwrap(),
            })
            .collect();
        Ring::new(1, members)
    }

    fn key(index: usize) -> ObjectKey {
        ObjectKey {
            bucket: "bucket".into(),
            key: format!("table/part-{index}.parquet"),
        }
    }

    fn owners(ring: &Ring, keys: usize) -> Vec<NodeId> {
        (0..keys)
            .map(|index| ring.owner(Placement::Home(&key(index)).hash()).unwrap())
            .collect()
    }

    #[test]
    fn placement_is_stable() {
        let ring = ring(&[(1, 1), (2, 1), (3, 2), (4, 1)]);
        let homes: Vec<u64> = owners(&ring, 8).iter().map(|id| id.0).collect();
        let chunks: Vec<u64> = (0..8)
            .map(|index| {
                ring.owner(Placement::Chunk(&key(0), index).hash())
                    .unwrap()
                    .0
            })
            .collect();
        assert_eq!(homes, [1, 3, 2, 3, 1, 1, 3, 3]);
        assert_eq!(chunks, [4, 3, 1, 4, 4, 3, 4, 2]);
    }

    /// A ring whose members share one weight ranks them by their draws, as
    /// the scores would: the same owner and the same candidates, in order.
    #[test]
    fn uniform_rings_rank_as_the_scores_do() {
        for weight in [1, 7, 3_800] {
            let members: Vec<(u64, u32)> = (0..40).map(|id| (id * 37 + 5, weight)).collect();
            let ring = ring(&members);
            for index in 0..2_000 {
                let key = key(index);
                for placement in [
                    Placement::Home(&key).hash(),
                    Placement::Chunk(&key, index as u64).hash(),
                ] {
                    let mut scored: Vec<_> = ring
                        .members()
                        .iter()
                        .map(|member| (score(placement, member), member.id))
                        .collect();
                    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.cmp(&a.1)));
                    let expected: Vec<NodeId> = scored.into_iter().map(|(_, id)| id).collect();
                    assert_eq!(ring.candidates(placement), expected);
                    assert_eq!(ring.owner(placement), Some(expected[0]));
                }
            }
        }
    }

    #[test]
    fn empty_ring_has_no_owner() {
        let ring = Ring::new(1, Vec::new());
        assert_eq!(ring.owner(Placement::Home(&key(0)).hash()), None);
        assert!(ring.candidates(Placement::Home(&key(0)).hash()).is_empty());
    }

    #[test]
    fn owner_is_first_candidate() {
        let ring = ring(&[(1, 1), (2, 3), (3, 1), (4, 2), (5, 1)]);
        for index in 0..1_000 {
            let key = key(index);
            for placement in [
                Placement::Home(&key).hash(),
                Placement::Chunk(&key, 3).hash(),
            ] {
                let candidates = ring.candidates(placement);
                assert_eq!(candidates.len(), 5);
                assert_eq!(Some(candidates[0]), ring.owner(placement));
            }
        }
    }

    #[test]
    fn home_and_chunks_place_independently() {
        let ring = ring(&[(1, 1), (2, 1), (3, 1), (4, 1)]);
        let key = key(0);
        let owners: std::collections::BTreeSet<_> = (0..64)
            .map(|index| ring.owner(Placement::Chunk(&key, index).hash()).unwrap())
            .collect();
        assert_eq!(owners.len(), 4);
    }

    #[test]
    fn adding_a_node_moves_keys_only_to_it() {
        let before = owners(&ring(&[(1, 1), (2, 1), (3, 1)]), 10_000);
        let after = owners(&ring(&[(1, 1), (2, 1), (3, 1), (4, 1)]), 10_000);
        let mut moved = 0;
        for (before, after) in before.iter().zip(&after) {
            if before != after {
                assert_eq!(*after, NodeId(4));
                moved += 1;
            }
        }
        assert!((2_200..2_800).contains(&moved), "moved {moved}");
    }

    #[test]
    fn removing_a_node_moves_only_its_keys() {
        let before = owners(&ring(&[(1, 1), (2, 1), (3, 1), (4, 1)]), 10_000);
        let after = owners(&ring(&[(1, 1), (2, 1), (4, 1)]), 10_000);
        for (before, after) in before.iter().zip(&after) {
            if before != after {
                assert_eq!(*before, NodeId(3));
            }
        }
    }

    #[test]
    fn weights_split_keys_proportionally() {
        let owners = owners(&ring(&[(1, 1), (2, 2), (3, 3)]), 60_000);
        let mut counts = BTreeMap::new();
        for owner in owners {
            *counts.entry(owner.0).or_insert(0u32) += 1;
        }
        for (id, expected) in [(1, 10_000u32), (2, 20_000), (3, 30_000)] {
            let count = counts[&id];
            assert!(
                count.abs_diff(expected) < expected / 20,
                "node {id} owns {count}, expected about {expected}"
            );
        }
    }
}
