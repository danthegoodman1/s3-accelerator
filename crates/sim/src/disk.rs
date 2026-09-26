//! A storage node's disk: extents of bytes that start as filler, so a read
//! of a slot nobody wrote returns bytes no object holds, and the slot
//! table that survives the node's restarts.

use s3_accelerator_core::node::{Recovered, SlotRecord};
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
    /// A run that ended with a clean shutdown, until the next start.
    clean_run: Option<u64>,
    /// Recorded slots whose bytes a fault damaged after they were durable.
    pub damaged: BTreeSet<Location>,
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
            clean_run: None,
            damaged: BTreeSet::new(),
        }
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

    /// Marks the table after a clean shutdown: its run's records are sound.
    pub fn shut_down(&mut self) {
        self.clean_run = Some(self.run);
    }

    /// Starts a new run of the node and reads back the table, trusting the
    /// records of a run that shut down cleanly.
    pub fn start(&mut self) -> Vec<Recovered> {
        let clean = self.clean_run.take();
        self.run += 1;
        self.records
            .iter()
            .map(|(&location, (record, checksum, run))| Recovered {
                location,
                record: record.clone(),
                checksum: *checksum,
                trusted: clean == Some(*run),
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
