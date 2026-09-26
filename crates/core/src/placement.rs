//! Weighted rendezvous hashing over stable node IDs.
//!
//! Every gateway and storage node computes placement independently, so the
//! same ring and key must give the same answer on every machine and every
//! build. `placement_is_stable` pins that.

use crate::s3::ObjectKey;
use std::num::NonZeroU32;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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

/// An immutable, versioned snapshot of the storage nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ring {
    version: u64,
    members: Vec<Member>,
}

impl Ring {
    pub fn new(version: u64, mut members: Vec<Member>) -> Ring {
        members.sort_by_key(|member| member.id);
        members.dedup_by_key(|member| member.id);
        Ring { version, members }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// The node that owns `placement`, or `None` for an empty ring.
    pub fn owner(&self, placement: Placement) -> Option<NodeId> {
        let point = point(placement);
        self.members
            .iter()
            .map(|member| (score(&point, member), member.id))
            .max_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, id)| id)
    }

    /// Every node, best candidate first. The first owns `placement`; the next
    /// ones take over when it fails and hold its hot-key replicas.
    pub fn candidates(&self, placement: Placement) -> Vec<NodeId> {
        let point = point(placement);
        let mut scored: Vec<_> = self
            .members
            .iter()
            .map(|member| (score(&point, member), member.id))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(b.1.cmp(&a.1)));
        scored.into_iter().map(|(_, id)| id).collect()
    }
}

/// An unambiguous encoding of `placement`: bucket and key are length-prefixed.
fn point(placement: Placement) -> Vec<u8> {
    let (key, chunk) = match placement {
        Placement::Home(key) => (key, None),
        Placement::Chunk(key, index) => (key, Some(index)),
    };
    let mut point = Vec::with_capacity(key.bucket.len() + key.key.len() + 17);
    for part in [&key.bucket, &key.key] {
        point.extend_from_slice(&(part.len() as u32).to_le_bytes());
        point.extend_from_slice(part.as_bytes());
    }
    if let Some(index) = chunk {
        point.extend_from_slice(&index.to_le_bytes());
    }
    point
}

/// The logarithmic method: each node wins with probability proportional to
/// its weight, and a membership change moves only the points it must.
fn score(point: &[u8], member: &Member) -> f64 {
    let hash = xxh3_64_with_seed(point, member.id.0);
    let unit = ((hash >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
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
            .map(|index| ring.owner(Placement::Home(&key(index))).unwrap())
            .collect()
    }

    #[test]
    fn placement_is_stable() {
        let ring = ring(&[(1, 1), (2, 1), (3, 2), (4, 1)]);
        let homes: Vec<u64> = owners(&ring, 8).iter().map(|id| id.0).collect();
        let chunks: Vec<u64> = (0..8)
            .map(|index| ring.owner(Placement::Chunk(&key(0), index)).unwrap().0)
            .collect();
        assert_eq!(homes, [4, 3, 3, 3, 4, 4, 1, 3]);
        assert_eq!(chunks, [1, 3, 1, 3, 3, 3, 2, 4]);
    }

    #[test]
    fn empty_ring_has_no_owner() {
        let ring = Ring::new(1, Vec::new());
        assert_eq!(ring.owner(Placement::Home(&key(0))), None);
        assert!(ring.candidates(Placement::Home(&key(0))).is_empty());
    }

    #[test]
    fn owner_is_first_candidate() {
        let ring = ring(&[(1, 1), (2, 3), (3, 1), (4, 2), (5, 1)]);
        for index in 0..1_000 {
            let key = key(index);
            for placement in [Placement::Home(&key), Placement::Chunk(&key, 3)] {
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
            .map(|index| ring.owner(Placement::Chunk(&key, index)).unwrap())
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
