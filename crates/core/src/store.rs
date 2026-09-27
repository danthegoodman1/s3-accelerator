//! The block store's policy: which slot each block fills, and which blocks
//! leave when space runs out. The owner writes and reads the bytes.
//!
//! The disk is a row of equal extents. Each extent in use holds slots of one
//! size class, a power of two, and each block fills the smallest slot that
//! fits it. Eviction is S3-FIFO: new blocks enter a small queue, blocks read
//! again move to the main queue, and a ghost queue remembers recently
//! evicted blocks so they go straight to the main queue when they return.

use crate::placement::PlacementHash;
use crate::s3::{ETag, ObjectKey};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreConfig {
    /// Bytes per extent, a multiple of `max_slot`.
    pub extent_size: u64,
    pub extents: u32,
    /// The smallest and largest size classes, both powers of two.
    pub min_slot: u64,
    pub max_slot: u64,
}

impl StoreConfig {
    /// The size of the slots that hold blocks of `len` bytes: the smallest
    /// size class that fits them.
    pub fn slot_size(&self, len: u64) -> u64 {
        len.max(self.min_slot).next_power_of_two()
    }
}

/// Where a slot's bytes live: an extent, and an offset within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Location {
    pub extent: u32,
    pub offset: u64,
}

/// An object version, named by hashes: one of its bucket and key, which
/// groups an object's versions, and a 128-bit one of its bucket, key and
/// ETag. Records on disk hold it, so they have one size whatever the key.
/// Packed to 8-byte alignment, so it takes 24 bytes rather than 32, in
/// every block key the index holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C, packed(8))]
pub struct VersionId {
    pub key: u64,
    pub version: u128,
}

impl VersionId {
    pub fn of(key: &ObjectKey, etag: &ETag) -> VersionId {
        let mut bytes = named(key);
        let key = xxh3_64(&bytes);
        push(&mut bytes, &etag.0);
        VersionId {
            key,
            version: xxh3_128(&bytes),
        }
    }

    /// The hash of `key` that every version of it shares.
    pub fn key_hash(key: &ObjectKey) -> u64 {
        xxh3_64(&named(key))
    }

    /// Every version of the object whose key hashes to `key`, in order.
    pub fn all_of(key: u64) -> std::ops::RangeInclusive<VersionId> {
        let bound = |version| VersionId { key, version };
        bound(0)..=bound(u128::MAX)
    }
}

/// Bucket and key, each prefixed with its length.
fn named(key: &ObjectKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.bucket.len() + key.key.len() + 8);
    push(&mut bytes, &key.bucket);
    push(&mut bytes, &key.key);
    bytes
}

fn push(bytes: &mut Vec<u8>, part: &str) {
    bytes.extend_from_slice(&(part.len() as u32).to_le_bytes());
    bytes.extend_from_slice(part.as_bytes());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockKey {
    pub version: VersionId,
    pub index: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockState {
    /// The slot is reserved and its bytes are on their way to disk.
    Filling,
    /// The bytes are on disk and may be read.
    Ready,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub location: Location,
    pub len: u64,
    pub state: BlockState,
    /// The block's identity across restarts and versions renumbering: what
    /// the ghost queue remembers.
    pub hash: u64,
    pub placement: PlacementHash,
    /// For a recovered block not yet verified, the checksum its bytes
    /// must match before it is served.
    pub verify: Option<u64>,
    pins: u32,
    /// A purge dropped the block's record; the block goes once free.
    purged: bool,
    freq: u8,
    queue: Queue,
    /// Tells this entry's place in a queue from places a removed block with
    /// the same key left behind.
    seq: u64,
}

impl Entry {
    /// Whether a response or check holds the block in its slot.
    pub fn pinned(&self) -> bool {
        self.pins > 0
    }

    /// Whether a purge dropped the block, which goes once free.
    pub fn purged(&self) -> bool {
        self.purged
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Queue {
    None,
    Small,
    Main,
}

/// Evictions a reservation tries before it empties the extent of the last
/// block evicted.
const EVICTIONS_PER_RESERVE: u32 = 8;
const SMALL_QUEUE_PERCENT: u64 = 10;
const MAX_FREQ: u8 = 3;

pub struct Store {
    config: StoreConfig,
    blocks: BTreeMap<BlockKey, Entry>,
    extents: Vec<Extent>,
    free_extents: Vec<u32>,
    classes: Vec<Class>,
    /// The queues name each block by its slot, which holds its key, and
    /// by the sequence number of its entry, which tells it from a block
    /// that held the slot before.
    small: VecDeque<(Location, u64)>,
    main: VecDeque<(Location, u64)>,
    small_bytes: u64,
    ghost: VecDeque<u64>,
    ghosts: BTreeMap<u64, u32>,
    /// Blocks the node no longer owns, which eviction takes first, with the
    /// sequence number of the entry each was marked in.
    disowned: VecDeque<(Location, u64)>,
    next_seq: u64,
    evicted: Vec<(BlockKey, Location)>,
}

struct Extent {
    class: Option<usize>,
    free: Vec<u64>,
    /// The block in each slot, by slot number.
    slots: Vec<Option<BlockKey>>,
    /// Slots that hold a block.
    held: u32,
    /// Blocks that are filling or pinned, which keep the extent in use.
    busy: u32,
}

struct Class {
    size: u64,
    /// Extents of this class with a free slot.
    with_space: BTreeSet<u32>,
}

impl Store {
    pub fn new(config: StoreConfig) -> Store {
        assert!(config.min_slot.is_power_of_two() && config.max_slot.is_power_of_two());
        assert!(config.min_slot <= config.max_slot);
        assert!(config.extent_size > 0 && config.extent_size.is_multiple_of(config.max_slot));
        let mut classes = Vec::new();
        let mut size = config.min_slot;
        while size <= config.max_slot {
            classes.push(Class {
                size,
                with_space: BTreeSet::new(),
            });
            size *= 2;
        }
        Store {
            config,
            blocks: BTreeMap::new(),
            extents: (0..config.extents)
                .map(|_| Extent {
                    class: None,
                    free: Vec::new(),
                    slots: Vec::new(),
                    held: 0,
                    busy: 0,
                })
                .collect(),
            free_extents: (0..config.extents).rev().collect(),
            classes,
            small: VecDeque::new(),
            main: VecDeque::new(),
            small_bytes: 0,
            ghost: VecDeque::new(),
            ghosts: BTreeMap::new(),
            disowned: VecDeque::new(),
            next_seq: 1,
            evicted: Vec::new(),
        }
    }

    pub fn get(&self, key: &BlockKey) -> Option<&Entry> {
        self.blocks.get(key)
    }

    /// The blocks of `version` the store holds or is filling.
    pub fn blocks_of(&self, version: VersionId) -> Vec<BlockKey> {
        let first = BlockKey { version, index: 0 };
        let last = BlockKey {
            version,
            index: u64::MAX,
        };
        self.blocks
            .range(first..=last)
            .map(|(key, _)| *key)
            .collect()
    }

    pub fn blocks(&self) -> impl Iterator<Item = (&BlockKey, &Entry)> {
        self.blocks.iter()
    }

    pub fn block_at(&self, location: Location) -> Option<BlockKey> {
        let extent = self.extents.get(location.extent as usize)?;
        let size = self.classes[extent.class?].size;
        if !location.offset.is_multiple_of(size) {
            return None;
        }
        *extent.slots.get((location.offset / size) as usize)?
    }

    /// Reserves a slot for a block about to fill, evicting blocks as needed.
    /// Returns `None` when every candidate is pinned or filling; the owner
    /// then serves the block without storing it.
    pub fn reserve(
        &mut self,
        key: BlockKey,
        len: u64,
        hash: u64,
        placement: PlacementHash,
    ) -> Option<Location> {
        assert!(
            len > 0 && len <= self.config.max_slot,
            "block of {len} bytes"
        );
        assert!(!self.blocks.contains_key(&key), "{key:?} reserved twice");
        let class = self.class_of(len);
        let mut evictions = 0;
        // The extents eviction's victims left. Eviction chose those blocks
        // as the coldest, so if evictions free no room, the latest victim's
        // extent is emptied, or failing that another's.
        let mut victims = Vec::new();
        let mut emptied = false;
        let location = loop {
            if let Some(location) = self.take_free(class) {
                break location;
            }
            if let Some(extent) = self.free_extents.pop() {
                self.assign(extent, class);
            } else if evictions < EVICTIONS_PER_RESERVE {
                evictions += 1;
                victims.push(self.evict_one()?);
            } else if emptied {
                return None;
            } else {
                let extent = victims
                    .iter()
                    .rev()
                    .copied()
                    .find(|&extent| self.idle(extent));
                self.evacuate(extent.or_else(|| self.fewest_idle())?);
                emptied = true;
            }
        };
        self.hold(location, key);
        self.extents[location.extent as usize].busy += 1;
        self.blocks.insert(
            key,
            Entry {
                location,
                len,
                state: BlockState::Filling,
                hash,
                placement,
                verify: None,
                pins: 0,
                purged: false,
                freq: 0,
                queue: Queue::None,
                seq: 0,
            },
        );
        Some(location)
    }

    /// Puts back a block the slot table recorded at `location`, readable,
    /// and unverified if `verify` holds a checksum. Returns false for a
    /// record that cannot describe this store.
    pub fn restore(
        &mut self,
        key: BlockKey,
        location: Location,
        len: u64,
        hash: u64,
        placement: PlacementHash,
        verify: Option<u64>,
    ) -> bool {
        if len == 0 || len > self.config.max_slot || self.blocks.contains_key(&key) {
            return false;
        }
        let class = self.class_of(len);
        let size = self.classes[class].size;
        let fits = location.offset.is_multiple_of(size)
            && location.offset + size <= self.config.extent_size;
        let Some(extent) = self.extents.get(location.extent as usize) else {
            return false;
        };
        if !fits || extent.class.is_some_and(|assigned| assigned != class) {
            return false;
        }
        if extent.class.is_none() {
            self.free_extents.retain(|&free| free != location.extent);
            self.assign(location.extent, class);
        }
        let state = &mut self.extents[location.extent as usize];
        let Some(position) = state
            .free
            .iter()
            .position(|&offset| offset == location.offset)
        else {
            return false;
        };
        state.free.swap_remove(position);
        if state.free.is_empty() {
            self.classes[class].with_space.remove(&location.extent);
        }
        self.hold(location, key);
        let seq = self.next_seq;
        self.next_seq += 1;
        self.small.push_back((location, seq));
        self.small_bytes += size;
        self.blocks.insert(
            key,
            Entry {
                location,
                len,
                state: BlockState::Ready,
                hash,
                placement,
                verify,
                pins: 0,
                purged: false,
                freq: 0,
                queue: Queue::Small,
                seq,
            },
        );
        true
    }

    /// A recovered block's bytes matched its checksum.
    pub fn verified(&mut self, key: BlockKey) {
        if let Some(entry) = self.blocks.get_mut(&key) {
            entry.verify = None;
        }
    }

    /// The block's bytes are on disk: it becomes readable and joins the
    /// small queue, or the main queue if the ghost queue remembers it.
    pub fn filled(&mut self, key: BlockKey) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let entry = self.blocks.get_mut(&key).expect("filled block exists");
        assert_eq!(entry.state, BlockState::Filling, "{key:?} filled twice");
        entry.state = BlockState::Ready;
        entry.seq = seq;
        self.extents[entry.location.extent as usize].busy -= 1;
        let size = self.classes[class_index(&self.config, entry.len)].size;
        let location = entry.location;
        if self.ghosts.contains_key(&entry.hash) {
            entry.queue = Queue::Main;
            self.main.push_back((location, seq));
        } else {
            entry.queue = Queue::Small;
            self.small.push_back((location, seq));
            self.small_bytes += size;
        }
    }

    /// Marks a block purged: it is neither recorded nor served once free.
    pub fn purge(&mut self, key: BlockKey) {
        if let Some(entry) = self.blocks.get_mut(&key) {
            entry.purged = true;
        }
    }

    /// Frees a block's slot: a fill that failed, or a purge.
    pub fn remove(&mut self, key: BlockKey) {
        let entry = self.detach(key);
        debug_assert_eq!(entry.pins, 0, "removed pinned {key:?}");
        if entry.state == BlockState::Filling {
            self.extents[entry.location.extent as usize].busy -= 1;
        }
        self.free_slot(entry.location);
    }

    pub fn hit(&mut self, key: BlockKey) {
        if let Some(entry) = self.blocks.get_mut(&key) {
            entry.freq = (entry.freq + 1).min(MAX_FREQ);
        }
    }

    /// Keeps a block in its slot while a response reads it.
    pub fn pin(&mut self, key: BlockKey) {
        let entry = self.blocks.get_mut(&key).expect("pinned block exists");
        if entry.pins == 0 {
            self.extents[entry.location.extent as usize].busy += 1;
        }
        entry.pins += 1;
    }

    pub fn unpin(&mut self, key: BlockKey) {
        let entry = self.blocks.get_mut(&key).expect("unpinned block exists");
        entry.pins = entry
            .pins
            .checked_sub(1)
            .expect("unpinned more than pinned");
        if entry.pins == 0 {
            self.extents[entry.location.extent as usize].busy -= 1;
        }
    }

    /// Blocks evicted since the last drain, and the slots they held.
    pub fn drain_evicted(&mut self) -> Vec<(BlockKey, Location)> {
        std::mem::take(&mut self.evicted)
    }

    fn class_of(&self, len: u64) -> usize {
        class_index(&self.config, len)
    }

    fn take_free(&mut self, class: usize) -> Option<Location> {
        let extent = *self.classes[class].with_space.first()?;
        let slots = &mut self.extents[extent as usize].free;
        let offset = slots.pop().expect("an extent with space has a free slot");
        if slots.is_empty() {
            self.classes[class].with_space.remove(&extent);
        }
        Some(Location { extent, offset })
    }

    fn assign(&mut self, extent: u32, class: usize) {
        let size = self.classes[class].size;
        let slots = self.config.extent_size / size;
        let state = &mut self.extents[extent as usize];
        state.class = Some(class);
        state.free = (0..slots).rev().map(|slot| slot * size).collect();
        state.slots = vec![None; slots as usize];
        self.classes[class].with_space.insert(extent);
    }

    /// Puts `key` in the slot at `location`, which its extent's class sizes.
    fn hold(&mut self, location: Location, key: BlockKey) {
        let state = &mut self.extents[location.extent as usize];
        let size = self.classes[state.class.expect("a used extent has a class")].size;
        state.slots[(location.offset / size) as usize] = Some(key);
        state.held += 1;
    }

    fn free_slot(&mut self, location: Location) {
        let state = &mut self.extents[location.extent as usize];
        let class = state.class.expect("a used extent has a class");
        let size = self.classes[class].size;
        state.slots[(location.offset / size) as usize] = None;
        state.held -= 1;
        if state.held == 0 {
            state.class = None;
            state.free.clear();
            state.slots = Vec::new();
            self.classes[class].with_space.remove(&location.extent);
            self.free_extents.push(location.extent);
        } else {
            state.free.push(location.offset);
            self.classes[class].with_space.insert(location.extent);
        }
    }

    /// Removes a block from the index and from the queue it sits in.
    fn detach(&mut self, key: BlockKey) -> Entry {
        let entry = self.blocks.remove(&key).expect("detached block exists");
        if entry.queue == Queue::Small {
            self.small_bytes -= self.classes[class_index(&self.config, entry.len)].size;
        }
        entry
    }

    /// Marks every readable block whose placement `owned` rejects, so
    /// eviction takes them before any other, oldest mark first.
    pub fn disown(&mut self, owned: impl Fn(PlacementHash) -> bool) {
        self.disowned = self
            .blocks
            .iter()
            .filter(|(_, entry)| entry.queue != Queue::None && !owned(entry.placement))
            .map(|(_, entry)| (entry.location, entry.seq))
            .collect();
    }

    /// Forgets which blocks are disowned, as when the ring changes again.
    pub fn clear_disowned(&mut self) {
        self.disowned.clear();
    }

    /// Evicts one block: a disowned one if any is unpinned, and otherwise
    /// by S3-FIFO. Returns the extent it left, or `None` when every queued
    /// block is pinned.
    fn evict_one(&mut self) -> Option<u32> {
        for _ in 0..self.disowned.len() {
            let Some((location, seq)) = self.disowned.pop_front() else {
                break;
            };
            let Some(key) = self.block_at(location) else {
                continue;
            };
            // A mark counts only for the entry it was made on.
            match self.blocks.get(&key) {
                Some(entry) if entry.seq != seq || entry.queue == Queue::None => {}
                Some(entry) if entry.pins > 0 => self.disowned.push_back((location, seq)),
                Some(_) => return Some(self.evict(key).extent),
                None => {}
            }
        }
        self.evict_by_frequency()
    }

    /// Evicts one block by S3-FIFO. Returns the extent it left, or `None`
    /// when every queued block is pinned.
    fn evict_by_frequency(&mut self) -> Option<u32> {
        let small_target =
            self.config.extent_size * u64::from(self.config.extents) * SMALL_QUEUE_PERCENT / 100;
        // Pinned blocks each queue has rotated past since the last block
        // moved or left. Once a queue has rotated through all of its blocks,
        // only the other queue can yield one.
        let (mut pinned_small, mut pinned_main) = (0, 0);
        loop {
            let small_stuck = pinned_small >= self.small.len();
            let main_stuck = pinned_main >= self.main.len();
            let from_small = match (small_stuck, main_stuck) {
                (true, true) => return None,
                (true, false) => false,
                (false, true) => true,
                (false, false) => self.small_bytes > small_target,
            };
            let (queue, popped) = if from_small {
                (Queue::Small, self.small.pop_front())
            } else {
                (Queue::Main, self.main.pop_front())
            };
            let (location, seq) = popped?;
            let Some(key) = self.block_at(location) else {
                continue;
            };
            let Some(entry) = self.blocks.get_mut(&key) else {
                continue;
            };
            if entry.seq != seq || entry.queue != queue {
                continue;
            }
            if entry.pins > 0 {
                match queue {
                    Queue::Small => {
                        pinned_small += 1;
                        self.small.push_back((location, seq));
                    }
                    _ => {
                        pinned_main += 1;
                        self.main.push_back((location, seq));
                    }
                }
                continue;
            }
            (pinned_small, pinned_main) = (0, 0);
            if queue == Queue::Small {
                self.small_bytes -= self.classes[class_index(&self.config, entry.len)].size;
                if entry.freq > 0 {
                    entry.freq = 0;
                    entry.queue = Queue::Main;
                    self.main.push_back((location, seq));
                    continue;
                }
                let hash = entry.hash;
                entry.queue = Queue::None;
                self.remember(hash);
            } else if entry.freq > 0 {
                entry.freq -= 1;
                self.main.push_back((location, seq));
                continue;
            }
            return Some(self.evict(key).extent);
        }
    }

    fn remember(&mut self, hash: u64) {
        self.ghost.push_back(hash);
        *self.ghosts.entry(hash).or_default() += 1;
        while self.ghost.len() > self.blocks.len().max(1) {
            let old = self.ghost.pop_front().expect("ghost queue is nonempty");
            if let Some(count) = self.ghosts.get_mut(&old) {
                *count -= 1;
                if *count == 0 {
                    self.ghosts.remove(&old);
                }
            }
        }
    }

    fn evict(&mut self, key: BlockKey) -> Location {
        let entry = self.detach(key);
        self.free_slot(entry.location);
        self.evicted.push((key, entry.location));
        entry.location
    }

    /// Whether `extent` holds blocks, none of them filling or pinned.
    fn idle(&self, extent: u32) -> bool {
        let state = &self.extents[extent as usize];
        state.class.is_some() && state.busy == 0
    }

    /// The idle extent with the fewest blocks, for when every victim's
    /// extent is busy.
    fn fewest_idle(&self) -> Option<u32> {
        (0..self.config.extents)
            .filter(|&extent| self.idle(extent))
            .min_by_key(|&extent| (self.extents[extent as usize].held, extent))
    }

    /// Empties an idle `extent`, so its space can change class.
    fn evacuate(&mut self, extent: u32) {
        let keys: Vec<BlockKey> = self.extents[extent as usize]
            .slots
            .iter()
            .flatten()
            .copied()
            .collect();
        for key in keys {
            self.evict(key);
        }
    }
}

fn class_index(config: &StoreConfig, len: u64) -> usize {
    (config.slot_size(len) / config.min_slot).trailing_zeros() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two extents of four 16-byte slots, with classes of 4, 8 and 16 bytes.
    fn store() -> Store {
        Store::new(StoreConfig {
            extent_size: 64,
            extents: 2,
            min_slot: 4,
            max_slot: 16,
        })
    }

    fn key(index: u64) -> BlockKey {
        BlockKey {
            version: VersionId { key: 1, version: 1 },
            index,
        }
    }

    fn evicted(store: &mut Store) -> Vec<BlockKey> {
        store
            .drain_evicted()
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    fn fill(store: &mut Store, index: u64, len: u64) -> Option<Location> {
        let location = store.reserve(key(index), len, index, PlacementHash(0))?;
        store.filled(key(index));
        Some(location)
    }

    #[test]
    fn slots_are_distinct_and_aligned() {
        let mut store = store();
        let mut seen = BTreeSet::new();
        for index in 0..8 {
            let location = fill(&mut store, index, 16).unwrap();
            assert_eq!(location.offset % 16, 0);
            assert!(location.offset < 64);
            assert!(seen.insert(location));
        }
        assert!(evicted(&mut store).is_empty());
    }

    #[test]
    fn small_blocks_take_small_slots() {
        let mut store = store();
        let first = fill(&mut store, 0, 3).unwrap();
        let second = fill(&mut store, 1, 4).unwrap();
        assert_eq!(first.extent, second.extent);
        assert_eq!(second.offset.abs_diff(first.offset), 4);
        assert_eq!(class_index(&store.config, 5), 1);
        assert_eq!(class_index(&store.config, 16), 2);
    }

    #[test]
    fn blocks_read_once_leave_first() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16);
        }
        store.hit(key(0));
        fill(&mut store, 8, 16).unwrap();
        assert_eq!(evicted(&mut store), [key(1)]);
        assert!(store.get(&key(0)).is_some());
    }

    /// Blocks placed elsewhere leave before any the node owns, however
    /// often those were read, and a pinned one waits its turn.
    #[test]
    fn disowned_blocks_leave_first() {
        let mut store = store();
        for index in 0..8 {
            store.reserve(key(index), 16, index, PlacementHash(index));
            store.filled(key(index));
        }
        store.pin(key(3));
        store.disown(|placement| placement.0 % 2 == 0 || placement.0 == 7);
        fill(&mut store, 8, 16).unwrap();
        fill(&mut store, 9, 16).unwrap();
        assert_eq!(evicted(&mut store), [key(1), key(5)]);
        fill(&mut store, 10, 16).unwrap();
        assert_eq!(evicted(&mut store), [key(0)]);
        store.unpin(key(3));
    }

    /// A mark counts only for the block it was made on: a block evicted
    /// and stored again leaves nothing to evict first.
    #[test]
    fn disowned_marks_end_with_their_block() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16).unwrap();
        }
        store.disown(|_| false);
        // Block 0's mark is for the entry it had before it was stored again.
        store.remove(key(0));
        fill(&mut store, 0, 16).unwrap();
        store.hit(key(0));
        store.hit(key(1));
        fill(&mut store, 8, 16).unwrap();
        assert_eq!(evicted(&mut store), [key(1)]);
    }

    /// Marks a ring change clears leave eviction to S3-FIFO.
    #[test]
    fn cleared_marks_leave_eviction_to_s3_fifo() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16).unwrap();
        }
        store.disown(|_| false);
        store.hit(key(0));
        store.clear_disowned();
        fill(&mut store, 8, 16).unwrap();
        assert_eq!(evicted(&mut store), [key(1)]);
    }

    #[test]
    fn a_returning_block_skips_the_small_queue() {
        let mut store = store();
        for index in 0..9 {
            fill(&mut store, index, 16);
        }
        assert_eq!(evicted(&mut store), [key(0)]);
        fill(&mut store, 100, 16);
        let evicted = evicted(&mut store);
        // Block 0 comes back under a new key with the same hash.
        let location = store.reserve(key(200), 16, 0, PlacementHash(0)).unwrap();
        store.filled(key(200));
        assert_eq!(store.get(&key(200)).unwrap().queue, Queue::Main);
        assert_eq!(evicted.len(), 1);
        assert_eq!(store.block_at(location), Some(key(200)));
    }

    #[test]
    fn pinned_and_filling_blocks_stay() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16);
            store.pin(key(index));
        }
        assert_eq!(store.reserve(key(8), 16, 8, PlacementHash(0)), None);
        store.unpin(key(3));
        assert!(store.reserve(key(8), 16, 8, PlacementHash(0)).is_some());
        assert_eq!(evicted(&mut store), [key(3)]);
        // Block 8 is filling, so nothing else can make room.
        assert_eq!(store.reserve(key(9), 16, 9, PlacementHash(0)), None);
    }

    #[test]
    fn a_pinned_queue_yields_to_the_other() {
        let mut store = store();
        // Extent 0 holds four 16-byte blocks; extent 1 holds sixteen 4-byte ones.
        for index in 0..20 {
            fill(&mut store, index, if index < 4 { 16 } else { 4 });
            if index < 19 {
                store.hit(key(index));
            }
        }
        // Evicting for a 16-byte block moves 0 to 18 to the main queue and
        // evicts 0, leaving the small queue with block 19: 4 bytes, under
        // its 12-byte target.
        store.reserve(key(100), 16, 100, PlacementHash(0)).unwrap();
        assert_eq!(evicted(&mut store), [key(0)]);
        for index in 1..19 {
            store.pin(key(index));
        }
        assert!(store.reserve(key(201), 4, 201, PlacementHash(0)).is_some());
        assert_eq!(evicted(&mut store), [key(19)]);
    }

    #[test]
    fn frequent_blocks_still_leave_eventually() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16);
            for _ in 0..10 {
                store.hit(key(index));
            }
        }
        for index in 100..140 {
            assert!(fill(&mut store, index, 16).is_some(), "block {index}");
        }
    }

    #[test]
    fn extents_change_class_as_demand_shifts() {
        let mut store = store();
        for index in 0..8 {
            fill(&mut store, index, 16);
            store.hit(key(index));
        }
        // Every extent holds 16-byte slots; a 4-byte block must empty one.
        let location = fill(&mut store, 100, 4).unwrap();
        let extent = &store.extents[location.extent as usize];
        assert_eq!(extent.class, Some(0));
        assert_eq!(evicted(&mut store).len(), 4);
    }

    /// Room for a class comes from the extent eviction's victims left,
    /// which holds the coldest blocks, rather than the extent with the
    /// fewest.
    #[test]
    fn emptying_an_extent_follows_eviction() {
        let mut store = store();
        // Extent 0 holds sixteen 4-byte blocks, and extent 1 four 16-byte
        // blocks read again.
        for index in 0..16 {
            fill(&mut store, index, 4);
        }
        for index in 16..20 {
            fill(&mut store, index, 16);
            store.hit(key(index));
        }
        // Eight evictions take small blocks and free no 16-byte slot, so
        // the reservation empties the rest of their extent.
        let location = fill(&mut store, 100, 16).unwrap();
        assert_eq!(location.extent, 0);
        let mut gone = evicted(&mut store);
        gone.sort();
        assert_eq!(gone, (0..16).map(key).collect::<Vec<_>>());
        assert!((16..20).all(|index| store.get(&key(index)).is_some()));
    }

    /// When the victims' extent holds a block still filling, the room
    /// comes from another idle extent instead.
    #[test]
    fn a_busy_victim_extent_yields_to_an_idle_one() {
        let mut store = store();
        // Extent 0 holds fifteen 4-byte blocks and one still filling;
        // extent 1 holds four 16-byte blocks.
        for index in 0..15 {
            fill(&mut store, index, 4);
        }
        store.reserve(key(15), 4, 15, PlacementHash(0)).unwrap();
        for index in 16..20 {
            fill(&mut store, index, 16);
        }
        // Eight evictions take small blocks from extent 0, which the
        // filling block keeps from being emptied.
        let location = fill(&mut store, 100, 8).unwrap();
        assert_eq!(location.extent, 1);
        assert!(store.get(&key(15)).is_some());
    }

    #[test]
    fn restored_blocks_take_their_recorded_slots() {
        let mut store = store();
        let at = |extent, offset| Location { extent, offset };
        assert!(store.restore(key(0), at(1, 16), 16, 0, PlacementHash(0), Some(7)));
        assert!(store.restore(key(1), at(0, 8), 5, 1, PlacementHash(0), None));
        // Wrong class for extent 1, misaligned, taken, and out of bounds.
        assert!(!store.restore(key(2), at(1, 32), 4, 2, PlacementHash(0), None));
        assert!(!store.restore(key(3), at(0, 4), 8, 3, PlacementHash(0), None));
        assert!(!store.restore(key(4), at(1, 16), 16, 4, PlacementHash(0), None));
        assert!(!store.restore(key(5), at(9, 0), 16, 5, PlacementHash(0), None));
        assert_eq!(store.get(&key(0)).unwrap().verify, Some(7));
        store.verified(key(0));
        assert_eq!(store.get(&key(0)).unwrap().verify, None);
        // New blocks take the free slots around the restored ones.
        for index in 10..13 {
            let location = fill(&mut store, index, 16).unwrap();
            assert_ne!(location, at(1, 16));
        }
    }

    #[test]
    fn removing_the_last_block_frees_its_extent() {
        let mut store = store();
        store.reserve(key(0), 16, 0, PlacementHash(0)).unwrap();
        store.remove(key(0));
        assert_eq!(store.free_extents.len(), 2);
        assert!(store.blocks().next().is_none());
    }

    /// Hot and cold blocks share extents, admitted in turn as concurrent
    /// fills write them, until the cache is full. Small blocks then need
    /// slots of smaller classes. With extents of one largest slot, each
    /// takes the room eviction's coldest victim left, and the hot set,
    /// read throughout, keeps every block.
    #[test]
    fn a_shift_to_smaller_blocks_keeps_the_hot_set() {
        const LARGEST: u64 = 1024;
        const EXTENTS: u64 = 4096;
        let mut store = Store::new(StoreConfig {
            extent_size: LARGEST,
            extents: EXTENTS as u32,
            min_slot: 4,
            max_slot: LARGEST,
        });
        let block = |object: u64, index: u64| BlockKey {
            version: VersionId {
                key: object,
                version: 1,
            },
            index,
        };
        let admit = |store: &mut Store, key: BlockKey, len: u64| {
            store.reserve(key, len, key.version.key << 8 | key.index, PlacementHash(0))?;
            store.filled(key);
            store.drain_evicted();
            Some(())
        };
        // 96 objects of 64 largest blocks, in groups of 32 whose blocks
        // arrive in turn; every third object is hot and read again.
        let mut hot = Vec::new();
        for group in 0..3 {
            let objects: Vec<u64> = (group * 32..group * 32 + 32).collect();
            for index in 0..64 {
                for &object in &objects {
                    admit(&mut store, block(object, index), LARGEST).unwrap();
                }
            }
            for object in objects.into_iter().filter(|object| object % 3 == 2) {
                for index in 0..64 {
                    store.hit(block(object, index));
                    hot.push(block(object, index));
                }
            }
        }
        assert!(hot.iter().all(|key| store.get(key).is_some()));
        // Small blocks from 4 to 512 bytes filling 30% of the cache, with
        // the hot set read twice among them; a hot block missing is
        // fetched and admitted again.
        let mut small = Vec::new();
        let (mut held, mut next) = (0, 0x9e37_79b9_u64);
        while held < EXTENTS * LARGEST * 3 / 10 {
            next = next.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let len = (4f64 * 2f64.powf((next >> 11) as f64 / (1u64 << 53) as f64 * 7.0)) as u64;
            small.push((block(1_000_000 + small.len() as u64, 0), len));
            held += len;
        }
        let reads: Vec<BlockKey> = hot.iter().chain(&hot).copied().collect();
        let (mut taken, mut misses) = (0, 0);
        for (position, &(key, len)) in small.iter().enumerate() {
            admit(&mut store, key, len).unwrap();
            let due = (position + 1) * reads.len() / small.len();
            for &read in &reads[taken..due] {
                if store.get(&read).is_some() {
                    store.hit(read);
                } else {
                    misses += 1;
                    admit(&mut store, read, LARGEST).unwrap();
                }
            }
            taken = due;
        }
        assert_eq!(misses, 0);
    }
}
