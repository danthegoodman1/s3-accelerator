//! A storage node's disk: extents of bytes that start as filler, so a read
//! of a slot nobody wrote returns bytes no object holds, and the slot
//! table and metadata file that survive the node's restarts.

use s3_accelerator_core::node::{Meta, Recovered, SlotRecord};
use s3_accelerator_core::s3::ObjectKey;
use s3_accelerator_core::store::Location;
use std::collections::{BTreeMap, BTreeSet};
use xxhash_rust::xxh3::xxh3_64;

pub struct Disk {
    extents: Vec<Vec<u8>>,
    /// Each recorded slot's block, the checksum of its bytes, and the run
    /// of the node that wrote or verified it.
    pub records: BTreeMap<Location, (SlotRecord, u64, u64)>,
    /// The node's current run; each start begins a new one.
    run: u64,
    /// The table's clean mark: after a clean shutdown, the earliest run
    /// whose records are all sound. A start takes it.
    trusted_from: Option<u64>,
    /// The mark the current run started from.
    started_from: Option<u64>,
    /// Recorded slots whose bytes a fault damaged after they were durable.
    pub damaged: BTreeSet<Location>,
    /// The metadata file's durable entries, oldest first, and those
    /// appended since, with the tick each becomes durable.
    pub metadata: Vec<(ObjectKey, Option<Meta>)>,
    unsynced: Vec<(u64, ObjectKey, Option<Meta>)>,
}

const FILLER: u8 = 0xa5;

impl Disk {
    pub fn new(extents: u32, extent_size: u64) -> Disk {
        Disk {
            extents: (0..extents)
                .map(|_| vec![FILLER; extent_size as usize])
                .collect(),
            records: BTreeMap::new(),
            run: 0,
            trusted_from: None,
            started_from: None,
            damaged: BTreeSet::new(),
            metadata: Vec::new(),
            unsynced: Vec::new(),
        }
    }

    /// Appends to the metadata file; the entry is durable from tick `due`.
    pub fn append(&mut self, due: u64, key: ObjectKey, meta: Option<Meta>) {
        self.unsynced.push((due, key, meta));
    }

    /// Makes durable the appended entries due by `now`, in order.
    pub fn sync(&mut self, now: u64) {
        let due = self
            .unsynced
            .iter()
            .take_while(|(due, _, _)| *due <= now)
            .count();
        self.keep_appended(due);
    }

    /// Makes durable the first `count` entries appended since the last
    /// sync.
    pub fn keep_appended(&mut self, count: usize) {
        let kept: Vec<_> = self.unsynced.drain(..).collect();
        let (durable, lost) = kept.split_at(count.min(kept.len()));
        self.metadata.extend(
            durable
                .iter()
                .map(|(_, key, meta)| (key.clone(), meta.clone())),
        );
        self.unsynced = lost.to_vec();
    }

    /// Entries appended but not yet durable.
    pub fn unsynced(&self) -> usize {
        self.unsynced.len()
    }

    /// Loses the entries not yet durable, as a crash does.
    pub fn lose_unsynced(&mut self) {
        self.unsynced.clear();
    }

    pub fn read(&self, location: Location, offset: u64, len: u64) -> &[u8] {
        let start = (location.offset + offset) as usize;
        &self.extents[location.extent as usize][start..start + len as usize]
    }

    pub fn write(&mut self, location: Location, bytes: &[u8]) {
        let start = location.offset as usize;
        self.extents[location.extent as usize][start..start + bytes.len()].copy_from_slice(bytes);
    }

    /// The checksum a record keeps of `len` bytes at `location`.
    pub fn checksum(&self, location: Location, len: u64) -> u64 {
        xxh3_64(self.read(location, 0, len))
    }

    pub fn record(&mut self, location: Location, record: SlotRecord) {
        let checksum = self.checksum(location, record.len);
        self.records.insert(location, (record, checksum, self.run));
        self.damaged.remove(&location);
    }

    /// Marks the table after a clean shutdown. This run's records are
    /// sound, and so are those the run trusted when it started.
    pub fn shut_down(&mut self) {
        self.trusted_from = Some(self.started_from.unwrap_or(self.run));
    }

    /// Starts a new run of the node and reads back the table, trusting the
    /// records the clean mark vouches for.
    pub fn start(&mut self) -> Vec<Recovered> {
        let trusted_from = self.trusted_from.take();
        self.started_from = trusted_from;
        self.run += 1;
        self.records
            .iter()
            .map(|(&location, (record, checksum, run))| Recovered {
                location,
                record: *record,
                checksum: *checksum,
                trusted: trusted_from.is_some_and(|from| *run >= from),
            })
            .collect()
    }

    pub fn clear(&mut self, location: Location) {
        self.records.remove(&location);
        self.damaged.remove(&location);
    }

    /// Flips a byte of a recorded slot, as a write the drive acknowledged
    /// and then lost would.
    pub fn damage(&mut self, location: Location, offset: u64) {
        let byte = &mut self.extents[location.extent as usize][(location.offset + offset) as usize];
        *byte = !*byte;
        self.damaged.insert(location);
    }
}
