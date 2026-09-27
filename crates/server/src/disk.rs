//! A storage node's disk: a slab file of extents that holds block bytes, a
//! slot table with one record per slot, and a metadata file of saved object
//! metadata. The core decides what goes where; this module moves the bytes
//! and keeps their order on disk.
//!
//! A block's bytes are synced before its record is written, and a cleared
//! record is synced before any later block write begins, so a record never
//! describes bytes that were not durable. Records carry the run that wrote
//! or verified them, and a clean shutdown marks the table with the earliest
//! run whose records are sound, which the next start trusts.
//!
//! Blocks leave the slab file with `sendfile`, whose sockets hold references
//! to the page cache's pages until the bytes are consumed. A write into a
//! slot waits until none of the slot's old pages are still in use, so a
//! response in flight keeps the bytes it was sent.

use crate::zero_copy::{self, PageCache};
use rustix::fs::{Advice, fadvise};
use s3_accelerator_core::node::{Meta, Recovered, SlotRecord};
use s3_accelerator_core::placement::PlacementHash;
use s3_accelerator_core::s3::{ETag, ObjectKey};
use s3_accelerator_core::store::{Location, StoreConfig, VersionId};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Mutex;
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"S3ACSLOT";
const FORMAT: u64 = 1;
/// The slot table's header, before its records.
const HEADER_SIZE: u64 = 4096;
pub const RECORD_SIZE: u64 = 64;
/// A header's `trusted_from` when no clean shutdown vouches for a run.
const NO_RUN: u64 = u64::MAX;

pub struct Disk {
    slabs: File,
    /// Which of the slab file's pages are cached.
    pages: PageCache,
    table: File,
    metadata: Mutex<File>,
    config: StoreConfig,
    run: u64,
    /// The clean mark this run started from.
    started_from: Option<u64>,
    /// Checksums of blocks written or verified, until their records are.
    checksums: Mutex<BTreeMap<Location, u64>>,
    /// Records cleared since the table was last synced. A block write
    /// syncs them first.
    clears: Mutex<bool>,
}

/// What a start reads back: the slot table's records and the metadata
/// file's entries, oldest first.
pub struct Recovery {
    pub records: Vec<Recovered>,
    pub metadata: Vec<(ObjectKey, Option<Meta>)>,
}

impl Disk {
    /// Opens the store in `dir`, creating it if needed, and starts a new
    /// run. A slot table written for another layout is discarded, with the
    /// metadata file.
    pub fn open(dir: &Path, config: StoreConfig) -> io::Result<(Disk, Recovery)> {
        std::fs::create_dir_all(dir)?;
        let open = |name: &str| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.join(name))
        };
        let (slabs, table, mut metadata) = (open("slabs")?, open("slots")?, open("metadata")?);
        let slots = config.extent_size * u64::from(config.extents) / config.min_slot;
        let table_len = HEADER_SIZE + slots * RECORD_SIZE;
        let header = read_header(&table)?.filter(|header| header.config == config);
        let (records, trusted_from, last_run) = match header {
            Some(header) => (
                read_records(&table, slots)?,
                header.trusted_from,
                header.run,
            ),
            None => {
                table.set_len(0)?;
                metadata.set_len(0)?;
                (Vec::new(), None, 0)
            }
        };
        table.set_len(table_len)?;
        let slabs_len = config.extent_size * u64::from(config.extents);
        slabs.set_len(slabs_len)?;
        refuse_memory_filesystems(&slabs)?;
        let page = rustix::param::page_size() as u64;
        if !config.min_slot.is_multiple_of(page) {
            return Err(io::Error::other(format!(
                "the smallest slot ({} bytes) must be a multiple of the page size ({page} bytes)",
                config.min_slot
            )));
        }
        // Reads fetch just the pages asked for, one page per folio, so
        // no cached folio spans two slots and each slot's pages can be
        // dropped on their own.
        fadvise(&slabs, 0, None, Advice::Random)?;
        let pages = PageCache::new(&slabs, slabs_len)?;
        let run = last_run + 1;
        write_header(
            &table,
            &Header {
                config,
                run,
                trusted_from: None,
            },
        )?;
        table.sync_data()?;
        let records = records
            .into_iter()
            .map(|(index, record, checksum, written)| Recovered {
                location: location(&config, index),
                record,
                checksum,
                trusted: trusted_from.is_some_and(|from| written >= from),
            })
            .collect();
        let metadata_entries = read_metadata(&mut metadata)?;
        let disk = Disk {
            slabs,
            pages,
            table,
            metadata: Mutex::new(metadata),
            config,
            run,
            started_from: trusted_from,
            checksums: Mutex::new(BTreeMap::new()),
            clears: Mutex::new(false),
        };
        let recovery = Recovery {
            records,
            metadata: metadata_entries,
        };
        Ok((disk, recovery))
    }

    /// Writes a block's bytes into its slot and syncs them, after any
    /// cleared records. Returns false, writing nothing, while a socket or
    /// pipe still holds any of the old pages the bytes would overwrite.
    pub fn write(&self, location: Location, bytes: &[u8]) -> io::Result<bool> {
        let offset = self.offset(location);
        let len = bytes.len() as u64;
        let slot = offset..offset + self.config.slot_size(len);
        if self.pages.in_use(&self.slabs, slot, offset..offset + len)? {
            return Ok(false);
        }
        {
            let mut clears = self.clears.lock().expect("clears lock");
            if *clears {
                self.table.sync_data()?;
                *clears = false;
            }
        }
        self.slabs.write_all_at(bytes, offset)?;
        self.slabs.sync_data()?;
        let checksum = xxh3_64(bytes);
        self.checksums
            .lock()
            .expect("checksums lock")
            .insert(location, checksum);
        Ok(true)
    }

    /// Sends `len` bytes of the slab file from `offset` to `socket` with
    /// `sendfile`. Runs on a worker thread.
    pub fn send(&self, socket: &OwnedFd, offset: u64, len: u64) -> io::Result<()> {
        zero_copy::send_file(socket, &self.slabs, offset, len)
    }

    /// Records the block in `location`'s slot, with the checksum of the
    /// bytes last written or verified there.
    pub fn record(&self, location: Location, record: SlotRecord) -> io::Result<()> {
        let checksum = self
            .checksums
            .lock()
            .expect("checksums lock")
            .remove(&location);
        let Some(checksum) = checksum else {
            return Err(io::Error::other(format!("no checksum for {location:?}")));
        };
        let bytes = encode_record(&record, checksum, self.run);
        self.table
            .write_all_at(&bytes, self.record_offset(location))
    }

    /// Erases `location`'s record. The next block write syncs the erasure
    /// first.
    pub fn clear(&self, location: Location) -> io::Result<()> {
        let mut clears = self.clears.lock().expect("clears lock");
        self.table
            .write_all_at(&[0; RECORD_SIZE as usize], self.record_offset(location))?;
        *clears = true;
        Ok(())
    }

    /// Whether the `len` bytes at `location` match `checksum`.
    pub fn verify(&self, location: Location, len: u64, checksum: u64) -> io::Result<bool> {
        let bytes = self.read(location, 0, len)?;
        let intact = xxh3_64(&bytes) == checksum;
        if intact {
            self.checksums
                .lock()
                .expect("checksums lock")
                .insert(location, checksum);
        }
        Ok(intact)
    }

    pub fn read(&self, location: Location, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0; len as usize];
        self.slabs
            .read_exact_at(&mut bytes, self.offset(location) + offset)?;
        Ok(bytes)
    }

    /// Appends an entry to the metadata file: `key`'s metadata, or `None`
    /// once it no longer holds. `sync_metadata` makes entries durable.
    pub fn append(&self, key: &ObjectKey, meta: Option<&Meta>) -> io::Result<()> {
        let entry = encode_entry(key, meta);
        let mut file = self.metadata.lock().expect("metadata lock");
        let end = file.metadata()?.len();
        file.write_all_at(&entry, end)?;
        file.flush()
    }

    pub fn sync_metadata(&self) -> io::Result<()> {
        self.metadata.lock().expect("metadata lock").sync_data()
    }

    /// Syncs everything and marks the table clean: this run's records are
    /// sound, and so are those it trusted when it started.
    pub fn shut_down(&self) -> io::Result<()> {
        self.slabs.sync_data()?;
        self.sync_metadata()?;
        self.table.sync_data()?;
        let trusted_from = Some(self.started_from.unwrap_or(self.run));
        let header = Header {
            config: self.config,
            run: self.run,
            trusted_from,
        };
        write_header(&self.table, &header)?;
        self.table.sync_data()
    }

    /// This run of the node: one more than the last run the slot table
    /// saw.
    pub fn run(&self) -> u64 {
        self.run
    }

    /// The slab file's offset of `location`.
    pub fn offset(&self, location: Location) -> u64 {
        u64::from(location.extent) * self.config.extent_size + location.offset
    }

    fn record_offset(&self, location: Location) -> u64 {
        HEADER_SIZE + self.offset(location) / self.config.min_slot * RECORD_SIZE
    }
}

/// Refuses a slab file in memory: the page cache is its only copy, so its
/// pages never leave, and every write would wait for them.
fn refuse_memory_filesystems(slabs: &File) -> io::Result<()> {
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    const RAMFS_MAGIC: i64 = 0x8584_58f6;
    let kind = rustix::fs::fstatfs(slabs)?.f_type as i64;
    if kind == TMPFS_MAGIC || kind == RAMFS_MAGIC {
        return Err(io::Error::other(
            "the data directory is in memory (tmpfs or ramfs); put it on a disk",
        ));
    }
    Ok(())
}

/// The location of the slot whose record is at `index`.
fn location(config: &StoreConfig, index: u64) -> Location {
    let offset = index * config.min_slot;
    Location {
        extent: (offset / config.extent_size) as u32,
        offset: offset % config.extent_size,
    }
}

struct Header {
    config: StoreConfig,
    run: u64,
    trusted_from: Option<u64>,
}

fn write_header(table: &File, header: &Header) -> io::Result<()> {
    let mut bytes = Vec::with_capacity(88);
    bytes.extend_from_slice(MAGIC);
    let config = header.config;
    for value in [
        FORMAT,
        config.extent_size,
        u64::from(config.extents),
        config.min_slot,
        config.max_slot,
        header.run,
        header.trusted_from.unwrap_or(NO_RUN),
    ] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let check = xxh3_64(&bytes);
    bytes.extend_from_slice(&check.to_le_bytes());
    table.write_all_at(&bytes, 0)
}

/// The table's header, or `None` if the file holds none this format reads.
fn read_header(table: &File) -> io::Result<Option<Header>> {
    let mut bytes = [0; 72];
    if table.metadata()?.len() < HEADER_SIZE {
        return Ok(None);
    }
    table.read_exact_at(&mut bytes, 0)?;
    let word =
        |index: usize| u64::from_le_bytes(bytes[8 + index * 8..16 + index * 8].try_into().unwrap());
    let check = xxh3_64(&bytes[..64]);
    if &bytes[..8] != MAGIC || word(0) != FORMAT || word(7) != check {
        return Ok(None);
    }
    let config = StoreConfig {
        extent_size: word(1),
        extents: word(2) as u32,
        min_slot: word(3),
        max_slot: word(4),
    };
    let trusted_from = Some(word(6)).filter(|&run| run != NO_RUN);
    Ok(Some(Header {
        config,
        run: word(5),
        trusted_from,
    }))
}

/// Every valid record in the table: its index, the record, the checksum of
/// its block's bytes, and the run that wrote it.
fn read_records(table: &File, slots: u64) -> io::Result<Vec<(u64, SlotRecord, u64, u64)>> {
    let mut records = Vec::new();
    let mut chunk = vec![0; (RECORD_SIZE * 1024) as usize];
    let mut index = 0;
    while index < slots {
        let count = (slots - index).min(1024);
        let bytes = &mut chunk[..(count * RECORD_SIZE) as usize];
        table.read_exact_at(bytes, HEADER_SIZE + index * RECORD_SIZE)?;
        let (records_read, _) = bytes.as_chunks::<{ RECORD_SIZE as usize }>();
        for (offset, record) in records_read.iter().enumerate() {
            if let Some((record, checksum, run)) = decode_record(record) {
                records.push((index + offset as u64, record, checksum, run));
            }
        }
        index += count;
    }
    Ok(records)
}

/// A record's 64 bytes: the version (key hash, then the 128-bit version
/// hash), block index, length, placement hash, the block's checksum, the
/// run that wrote it, and the record's own check. A torn or cleared record
/// fails the check.
pub fn encode_record(record: &SlotRecord, checksum: u64, run: u64) -> [u8; RECORD_SIZE as usize] {
    let mut bytes = [0; RECORD_SIZE as usize];
    bytes[0..8].copy_from_slice(&record.version.key.to_le_bytes());
    bytes[8..24].copy_from_slice(&record.version.version.to_le_bytes());
    bytes[24..32].copy_from_slice(&record.index.to_le_bytes());
    bytes[32..40].copy_from_slice(&record.len.to_le_bytes());
    bytes[40..48].copy_from_slice(&record.placement.0.to_le_bytes());
    bytes[48..56].copy_from_slice(&checksum.to_le_bytes());
    bytes[56..60].copy_from_slice(&(run as u32).to_le_bytes());
    let check = xxh3_64(&bytes[..60]) as u32;
    bytes[60..64].copy_from_slice(&check.to_le_bytes());
    bytes
}

pub fn decode_record(bytes: &[u8]) -> Option<(SlotRecord, u64, u64)> {
    let word = |range: std::ops::Range<usize>| u64::from_le_bytes(bytes[range].try_into().unwrap());
    let check = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
    if check != xxh3_64(&bytes[..60]) as u32 {
        return None;
    }
    let record = SlotRecord {
        version: VersionId {
            key: word(0..8),
            version: u128::from_le_bytes(bytes[8..24].try_into().unwrap()),
        },
        index: word(24..32),
        len: word(32..40),
        placement: PlacementHash(word(40..48)),
    };
    let run = u64::from(u32::from_le_bytes(bytes[56..60].try_into().unwrap()));
    Some((record, word(48..56), run))
}

/// An entry: its payload's length and check, then the payload: a kind,
/// the bucket and key, and for metadata its ETag, size and headers.
pub fn encode_entry(key: &ObjectKey, meta: Option<&Meta>) -> Vec<u8> {
    let mut payload = Vec::new();
    let text = |payload: &mut Vec<u8>, text: &str| {
        payload.extend_from_slice(&(text.len() as u32).to_le_bytes());
        payload.extend_from_slice(text.as_bytes());
    };
    payload.push(u8::from(meta.is_some()));
    text(&mut payload, &key.bucket);
    text(&mut payload, &key.key);
    if let Some(meta) = meta {
        text(&mut payload, &meta.etag.0);
        payload.extend_from_slice(&meta.size.to_le_bytes());
        payload.extend_from_slice(&(meta.headers.len() as u32).to_le_bytes());
        for (name, value) in &meta.headers {
            text(&mut payload, name);
            text(&mut payload, value);
        }
    }
    let mut entry = Vec::with_capacity(payload.len() + 12);
    entry.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    entry.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
    entry.extend_from_slice(&payload);
    entry
}

/// The metadata file's entries up to the first torn or unreadable one,
/// which a crash left; the file is cut there.
fn read_metadata(file: &mut File) -> io::Result<Vec<(ObjectKey, Option<Meta>)>> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut entries = Vec::new();
    let mut at = 0;
    while let Some((entry, len)) = decode_entry(&bytes[at..]) {
        entries.push(entry);
        at += len;
    }
    file.set_len(at as u64)?;
    Ok(entries)
}

/// The entry at the start of `bytes`, and its length.
pub fn decode_entry(bytes: &[u8]) -> Option<((ObjectKey, Option<Meta>), usize)> {
    let len = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    let check = u64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?);
    let payload = bytes.get(12..12 + len)?;
    if xxh3_64(payload) != check {
        return None;
    }
    let mut reader = Reader(payload);
    let remember = reader.byte()? == 1;
    let key = ObjectKey {
        bucket: reader.text()?,
        key: reader.text()?,
    };
    let meta = match remember {
        false => None,
        true => {
            let etag = ETag(reader.text()?);
            let size = reader.word()?;
            let count = reader.count()?;
            let headers = (0..count)
                .map(|_| Some((reader.text()?, reader.text()?)))
                .collect::<Option<Vec<_>>>()?;
            Some(Meta {
                etag,
                size,
                headers,
            })
        }
    };
    Some(((key, meta), 12 + len))
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, len: usize) -> Option<&[u8]> {
        let (taken, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(taken)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn count(&mut self) -> Option<usize> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?) as usize)
    }

    fn word(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn text(&mut self) -> Option<String> {
        let len = self.count()?;
        String::from_utf8(self.take(len)?.to_vec()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> StoreConfig {
        StoreConfig {
            extent_size: 16384,
            extents: 4,
            min_slot: 4096,
            max_slot: 8192,
        }
    }

    /// A directory beside the test binary, on a disk-backed filesystem.
    fn dir(name: &str) -> std::path::PathBuf {
        let binary = std::env::current_exe().unwrap();
        let dir = binary
            .parent()
            .unwrap()
            .join(format!("s3-accelerator-disk-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn key() -> ObjectKey {
        ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        }
    }

    fn record() -> SlotRecord {
        SlotRecord {
            version: VersionId::of(&key(), &ETag("\"e\"".into())),
            index: 3,
            len: 1000,
            placement: PlacementHash(7),
        }
    }

    #[test]
    fn records_round_trip_and_torn_ones_fail_their_check() {
        let bytes = encode_record(&record(), 99, 5);
        assert_eq!(decode_record(&bytes), Some((record(), 99, 5)));
        let mut torn = bytes;
        torn[20] ^= 1;
        assert_eq!(decode_record(&torn), None);
        assert_eq!(decode_record(&[0; 64]), None);
    }

    #[test]
    fn metadata_entries_round_trip_up_to_a_torn_tail() {
        let meta = Meta {
            etag: ETag("\"e\"".into()),
            size: 12,
            headers: vec![("content-type".into(), "text/plain".into())],
        };
        let mut bytes = encode_entry(&key(), Some(&meta));
        let first = bytes.len();
        bytes.extend(encode_entry(&key(), None));
        assert_eq!(decode_entry(&bytes), Some(((key(), Some(meta)), first)));
        assert_eq!(
            decode_entry(&bytes[first..]).map(|(entry, _)| entry),
            Some((key(), None))
        );
        assert_eq!(decode_entry(&bytes[first..bytes.len() - 1]), None);
    }

    /// Blocks written, recorded and cleared come back as the table left
    /// them: untrusted after a crash, trusted after a clean shutdown, and
    /// trusted across a second clean restart.
    #[test]
    fn a_restart_reads_back_the_records() {
        let dir = dir("restart");
        let at = |extent, offset| Location { extent, offset };
        {
            let (disk, recovery) = Disk::open(&dir, config()).unwrap();
            assert!(recovery.records.is_empty());
            assert!(disk.write(at(1, 4096), &[1; 1000]).unwrap());
            disk.record(at(1, 4096), record()).unwrap();
            assert!(disk.write(at(2, 0), &[2; 1000]).unwrap());
            disk.record(at(2, 0), record()).unwrap();
            disk.clear(at(2, 0)).unwrap();
            disk.append(&key(), None).unwrap();
        }
        let (disk, recovery) = Disk::open(&dir, config()).unwrap();
        let [recovered] = recovery.records.as_slice() else {
            panic!("{} records", recovery.records.len());
        };
        assert_eq!(recovered.location, at(1, 4096));
        assert_eq!(recovered.checksum, xxh3_64(&[1; 1000]));
        assert!(!recovered.trusted);
        assert_eq!(recovery.metadata, vec![(key(), None)]);
        assert!(disk.verify(at(1, 4096), 1000, recovered.checksum).unwrap());
        disk.record(at(1, 4096), record()).unwrap();
        disk.shut_down().unwrap();
        drop(disk);
        for _ in 0..2 {
            let (disk, recovery) = Disk::open(&dir, config()).unwrap();
            assert!(recovery.records[0].trusted);
            disk.shut_down().unwrap();
        }
        let other = StoreConfig {
            max_slot: 4096,
            ..config()
        };
        let (_, recovery) = Disk::open(&dir, other).unwrap();
        assert!(recovery.records.is_empty() && recovery.metadata.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
