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

use crate::log;
use crate::zero_copy::{self, PageCache};
use rustix::fs::{Advice, FallocateFlags, fadvise, fallocate};
use s3_accelerator_core::node::{Meta, Recovered, SlotRecord};
use s3_accelerator_core::placement::NodeId;
use s3_accelerator_core::placement::PlacementHash;
use s3_accelerator_core::s3::{ETag, ObjectKey};
use s3_accelerator_core::store::{Location, StoreConfig, VersionId};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"S3ACSLOT";
const FORMAT: u64 = 1;
/// The slot table's header, before its records.
const HEADER_SIZE: u64 = 4096;
pub const RECORD_SIZE: u64 = 64;
/// A header's `trusted_from` when no clean shutdown vouches for a run.
const NO_RUN: u64 = u64::MAX;

pub struct Disk {
    /// The directory the node's files live in.
    dir: PathBuf,
    slabs: File,
    /// Held while a block's bytes are copied into the slab file. The file
    /// system takes one write into a file at a time anyway, and threads
    /// that wait here sleep rather than spin on the file's lock.
    writing: Mutex<()>,
    /// For each span a largest slot covers, the largest slot written there
    /// since its pages were last dropped whole: a folio no larger may
    /// remain cached across the span's slots.
    written: Mutex<Vec<u64>>,
    /// Syncs the slab file once for every block write waiting on it.
    slab_sync: GroupSync,
    /// Which of the slab file's pages are cached.
    pages: PageCache,
    table: File,
    metadata: Mutex<File>,
    /// The purge log: each purge the node coordinates, and the nodes it
    /// still waits on.
    purges: Mutex<File>,
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

/// Syncs a file for many writers at once. Syncs run one at a time, each
/// serving every writer waiting when it begins, so concurrent block writes
/// share each flush of the drive. A write is durable once a sync that began
/// after it ended succeeds, unless a sync that could have flushed its pages
/// failed first: the kernel may have dropped those pages, so no later sync
/// makes it durable.
#[derive(Default)]
struct GroupSync {
    state: Mutex<SyncState>,
    done: Condvar,
}

#[derive(Default)]
struct SyncState {
    /// Syncs begun and finished, numbered from 0 in the order they begin.
    begun: u64,
    finished: u64,
    failed: BTreeSet<u64>,
    /// Writes waiting for a sync.
    waiting: u64,
}

impl GroupSync {
    /// The first sync that could flush a write beginning now: the one
    /// running, or else the next.
    fn begin(&self) -> u64 {
        self.state.lock().expect("sync lock").finished
    }

    /// Makes durable a write that began when `begin` returned `first` and
    /// has ended, running `flush` if no sync that began since will. Returns
    /// how long `flush` took, if this call ran it.
    fn sync(
        &self,
        first: u64,
        mut flush: impl FnMut() -> io::Result<()>,
    ) -> io::Result<Option<Duration>> {
        let mut took = None;
        let mut state = self.state.lock().expect("sync lock");
        // The first sync to begin after the write ended.
        let needed = state.begun;
        state.waiting += 1;
        loop {
            if state.finished > needed {
                state.waiting -= 1;
                return match state.failed.range(first..=needed).next() {
                    Some(_) => Err(io::Error::other("a sync of the slab file failed")),
                    None => Ok(took),
                };
            }
            if state.begun > state.finished {
                state = self.done.wait(state).expect("sync lock");
                continue;
            }
            let number = state.begun;
            state.begun += 1;
            drop(state);
            let flushing = Instant::now();
            let synced = flush();
            took = Some(flushing.elapsed());
            state = self.state.lock().expect("sync lock");
            state.finished += 1;
            if synced.is_err() {
                state.failed.insert(number);
            }
            self.done.notify_all();
        }
    }
}

/// A block write that reached the disk.
pub struct Written {
    /// How long the sync this write ran took, if it ran one rather than
    /// sharing another's.
    pub sync: Option<Duration>,
}

/// What a start reads back: the slot table's records and the metadata
/// file's entries, oldest first.
pub struct Recovery {
    pub records: Vec<Recovered>,
    pub metadata: Vec<(ObjectKey, Option<Meta>)>,
    pub purges: Vec<(ObjectKey, Vec<NodeId>)>,
}

impl Disk {
    /// Opens the store in `dir`, creating it if needed, and starts a new
    /// run. A slot table written for other slots is discarded, with the
    /// metadata file; one written for other extents over the same slots is
    /// kept, as records name slots by their offset.
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
        let mut purges = open("purges")?;
        refuse_memory_filesystems(&slabs)?;
        // Every slot's space, so a full filesystem stops the start rather
        // than fills.
        let slabs_len = config.extent_size * u64::from(config.extents);
        fallocate(&slabs, FallocateFlags::empty(), 0, slabs_len).map_err(|error| {
            io::Error::other(format!(
                "reserving {slabs_len} bytes for the slab file in {}: {error}",
                dir.display()
            ))
        })?;
        let slots = config.extent_size * u64::from(config.extents) / config.min_slot;
        let table_len = HEADER_SIZE + slots * RECORD_SIZE;
        let header = read_header(&table)?.filter(|header| same_slots(&header.config, &config));
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
        slabs.set_len(slabs_len)?;
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
        let purge_entries = read_purges(&mut purges)?;
        // The page cache outlives the process, so any span may still hold
        // a largest block's folio from an earlier run.
        let spans = (slabs_len / config.max_slot) as usize;
        let disk = Disk {
            dir: dir.to_path_buf(),
            writing: Mutex::new(()),
            written: Mutex::new(vec![config.max_slot; spans]),
            slabs,
            slab_sync: GroupSync::default(),
            pages,
            table,
            metadata: Mutex::new(metadata),
            purges: Mutex::new(purges),
            config,
            run,
            started_from: trusted_from,
            checksums: Mutex::new(BTreeMap::new()),
            clears: Mutex::new(false),
        };
        let recovery = Recovery {
            records,
            metadata: metadata_entries,
            purges: purge_entries,
        };
        Ok((disk, recovery))
    }

    /// Writes a block's bytes into its slot and syncs them, after any
    /// cleared records. Returns `None`, writing nothing, while a socket or
    /// pipe still holds any of the old pages the bytes would overwrite.
    pub fn write(&self, location: Location, bytes: &[u8]) -> io::Result<Option<Written>> {
        let offset = self.offset(location);
        let len = bytes.len() as u64;
        let range = offset..offset + len;
        let slot_size = self.config.slot_size(len);
        let slot = offset..offset + slot_size;
        // A larger block written into this space may have left its bytes
        // cached in one folio that spans the slot, as the kernel caches a
        // write; only dropping the whole largest slot's span releases it.
        let largest = self.config.max_slot;
        let index = (offset / largest) as usize;
        let span = index as u64 * largest..(index as u64 + 1) * largest;
        if self.pages.in_use(&self.slabs, slot, range.clone())? {
            let written = self.written.lock().expect("written lock")[index];
            if written <= slot_size || self.pages.in_use(&self.slabs, span, range)? {
                return Ok(None);
            }
            self.written.lock().expect("written lock")[index] = slot_size;
        }
        {
            let mut clears = self.clears.lock().expect("clears lock");
            if *clears {
                self.table.sync_data()?;
                *clears = false;
            }
        }
        {
            let mut written = self.written.lock().expect("written lock");
            written[index] = written[index].max(slot_size);
        }
        let first = self.slab_sync.begin();
        {
            let _writing = self.writing.lock().expect("writing lock");
            self.slabs.write_all_at(bytes, offset)?;
        }
        let sync = self.slab_sync.sync(first, || self.slabs.sync_data())?;
        let checksum = xxh3_64(bytes);
        self.checksums
            .lock()
            .expect("checksums lock")
            .insert(location, checksum);
        Ok(Some(Written { sync }))
    }

    /// Sends `len` bytes of the slab file from `offset` to `socket` with
    /// `sendfile`. Runs on a worker thread.
    pub fn send(&self, socket: &OwnedFd, offset: u64, len: u64) -> io::Result<()> {
        zero_copy::send_file(socket, &self.slabs, offset, len)
    }

    /// Whether the page cache holds every byte of the slab file from
    /// `offset`, `len` of them.
    pub fn cached(&self, offset: u64, len: u64) -> bool {
        self.pages.resident(offset..offset + len).unwrap_or(false)
    }

    /// Sends bytes the page cache holds on this thread's event loop.
    pub async fn send_cached(
        &self,
        socket: &tokio::net::TcpStream,
        offset: u64,
        len: u64,
    ) -> io::Result<()> {
        zero_copy::send_cached(socket, &self.slabs, offset, len).await
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

    /// Replaces the metadata file with `entries`, durably: a crash leaves
    /// the old file or the new one.
    pub fn rewrite_metadata(&self, entries: &[(ObjectKey, Meta)]) -> io::Result<()> {
        let bytes: Vec<u8> = entries
            .iter()
            .flat_map(|(key, meta)| encode_entry(key, Some(meta)))
            .collect();
        let mut file = self.metadata.lock().expect("metadata lock");
        let path = self.dir.join("metadata.new");
        let mut new = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        new.write_all(&bytes)?;
        new.sync_data()?;
        std::fs::rename(&path, self.dir.join("metadata"))?;
        File::open(&self.dir)?.sync_all()?;
        *file = new;
        Ok(())
    }

    pub fn sync_metadata(&self) -> io::Result<()> {
        self.metadata.lock().expect("metadata lock").sync_data()
    }

    /// Makes the slot table's cleared records durable.
    pub fn sync_table(&self) -> io::Result<()> {
        let mut clears = self.clears.lock().expect("clears lock");
        self.table.sync_data()?;
        *clears = false;
        Ok(())
    }

    /// Appends to the purge log that `key`'s purge waits on `nodes`.
    /// `sync_purges` makes entries durable.
    pub fn save_purge(&self, key: &ObjectKey, nodes: &[NodeId]) -> io::Result<()> {
        let file = self.purges.lock().expect("purges lock");
        let end = file.metadata()?.len();
        file.write_all_at(&purge_entry(key, nodes), end)
    }

    pub fn sync_purges(&self) -> io::Result<()> {
        self.purges.lock().expect("purges lock").sync_data()
    }

    /// Makes erased slots' holes durable.
    pub fn sync_slabs(&self) -> io::Result<()> {
        self.slabs.sync_data()
    }

    /// Frees the storage behind a slot a purge dropped, so its bytes are
    /// gone from the disk.
    pub fn erase(&self, location: Location, len: u64) -> io::Result<()> {
        let offset = self.offset(location);
        let flags = FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE;
        fallocate(&self.slabs, flags, offset, len)?;
        // The slot's space again, empty, so the file stays preallocated.
        if let Err(error) = fallocate(&self.slabs, FallocateFlags::KEEP_SIZE, offset, len) {
            log!(
                Warn,
                "reserving an erased slot's space failed",
                error = error
            );
        }
        Ok(())
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

/// Whether a table written for `old` describes the slots of `new`: the
/// same slot sizes over the same bytes, whatever the extents. A record
/// whose new extent holds another class goes when the store restores it.
fn same_slots(old: &StoreConfig, new: &StoreConfig) -> bool {
    let bytes = |config: &StoreConfig| config.extent_size * u64::from(config.extents);
    old.min_slot == new.min_slot && old.max_slot == new.max_slot && bytes(old) == bytes(new)
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
    // Threads read and decode parts of the table at once: a large disk's
    // table is a sixty-fourth of its size, and one reader leaves the drive
    // and the cores idle.
    let threads = std::thread::available_parallelism().map_or(1, |threads| threads.get());
    let part = slots.div_ceil(threads.min(8) as u64).max(1);
    let parts: Vec<io::Result<Vec<_>>> = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..slots)
            .step_by(part as usize)
            .map(|first| scope.spawn(move || read_part(table, first, (first + part).min(slots))))
            .collect();
        readers
            .into_iter()
            .map(|reader| reader.join().expect("a table reader finishes"))
            .collect()
    });
    let mut records = Vec::new();
    for part in parts {
        records.extend(part?);
    }
    Ok(records)
}

/// The records of slots `first..end`.
fn read_part(table: &File, first: u64, end: u64) -> io::Result<Vec<(u64, SlotRecord, u64, u64)>> {
    const BATCH: u64 = 16_384;
    let mut records = Vec::new();
    let mut chunk = vec![0; (RECORD_SIZE * BATCH) as usize];
    let mut index = first;
    while index < end {
        let count = (end - index).min(BATCH);
        let bytes = &mut chunk[..(count * RECORD_SIZE) as usize];
        table.read_exact_at(bytes, HEADER_SIZE + index * RECORD_SIZE)?;
        let (records_read, _) = bytes.as_chunks::<{ RECORD_SIZE as usize }>();
        for (offset, record) in records_read.iter().enumerate() {
            // Most of a table is empty slots, which need no check.
            if record.iter().all(|&byte| byte == 0) {
                continue;
            }
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
    framed(&payload)
}

/// A log entry: the payload's length and checksum, then the payload.
fn framed(payload: &[u8]) -> Vec<u8> {
    let mut entry = Vec::with_capacity(payload.len() + 12);
    entry.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    entry.extend_from_slice(&xxh3_64(payload).to_le_bytes());
    entry.extend_from_slice(payload);
    entry
}

/// The payload of the entry at the start of `bytes`, and the entry's
/// length, if it is whole.
fn unframed(bytes: &[u8]) -> Option<(&[u8], usize)> {
    let len = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    let check = u64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?);
    let payload = bytes.get(12..12 + len)?;
    (xxh3_64(payload) == check).then_some((payload, 12 + len))
}

fn purge_entry(key: &ObjectKey, nodes: &[NodeId]) -> Vec<u8> {
    let mut payload = Vec::new();
    for text in [&key.bucket, &key.key] {
        payload.extend_from_slice(&(text.len() as u32).to_le_bytes());
        payload.extend_from_slice(text.as_bytes());
    }
    payload.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
    for node in nodes {
        payload.extend_from_slice(&node.0.to_le_bytes());
    }
    framed(&payload)
}

/// The purges still waiting on nodes, from the purge log's entries up to
/// the first torn one, which a crash left. The log is rewritten to hold
/// just those, so it stays as long as the purges in progress.
fn read_purges(file: &mut File) -> io::Result<Vec<(ObjectKey, Vec<NodeId>)>> {
    let mut latest = BTreeMap::new();
    for (key, nodes) in read_purge_entries(file)? {
        latest.insert(key, nodes);
    }
    latest.retain(|_, nodes: &mut Vec<NodeId>| !nodes.is_empty());
    let mut log = Vec::new();
    for (key, nodes) in &latest {
        log.extend(purge_entry(key, nodes));
    }
    file.set_len(0)?;
    file.write_all_at(&log, 0)?;
    file.sync_data()?;
    Ok(latest.into_iter().collect())
}

fn read_purge_entries(file: &mut File) -> io::Result<Vec<(ObjectKey, Vec<NodeId>)>> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut entries = Vec::new();
    let mut at = 0;
    while let Some((payload, len)) = unframed(&bytes[at..]) {
        let mut reader = Reader(payload);
        let entry = (|| {
            let key = ObjectKey {
                bucket: reader.text()?,
                key: reader.text()?,
            };
            let nodes = (0..reader.count()?)
                .map(|_| reader.word().map(NodeId))
                .collect::<Option<Vec<_>>>()?;
            Some((key, nodes))
        })();
        let Some(entry) = entry else {
            break;
        };
        entries.push(entry);
        at += len;
    }
    Ok(entries)
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

    /// A rewrite replaces every entry appended before it, and later appends
    /// follow the rewritten ones.
    #[test]
    fn a_rewritten_metadata_file_holds_its_entries() {
        let dir = dir("rewrite");
        let entry = |name: &str| {
            let key = ObjectKey {
                bucket: "b".into(),
                key: name.into(),
            };
            let meta = Meta {
                etag: ETag(format!("\"{name}\"")),
                size: 10,
                headers: Vec::new(),
            };
            (key, meta)
        };
        {
            let (disk, _) = Disk::open(&dir, config()).unwrap();
            for name in ["a", "b", "c"] {
                let (key, meta) = entry(name);
                disk.append(&key, Some(&meta)).unwrap();
            }
            disk.rewrite_metadata(&[entry("c"), entry("a")]).unwrap();
            let (key, meta) = entry("d");
            disk.append(&key, Some(&meta)).unwrap();
            disk.sync_metadata().unwrap();
        }
        let (_, recovery) = Disk::open(&dir, config()).unwrap();
        let names: Vec<&str> = recovery
            .metadata
            .iter()
            .map(|(key, _)| key.key.as_str())
            .collect();
        assert_eq!(names, ["c", "a", "d"]);
    }

    /// Erasing a slot frees its bytes and reserves its space again, so the
    /// slab file stays preallocated.
    #[test]
    fn an_erased_slot_is_empty_and_still_reserved() {
        use std::os::unix::fs::MetadataExt;
        let dir = dir("erase");
        let (disk, _) = Disk::open(&dir, config()).unwrap();
        let at = Location {
            extent: 1,
            offset: 0,
        };
        assert!(disk.write(at, &[7; 8192]).unwrap().is_some());
        let blocks = || std::fs::metadata(dir.join("slabs")).unwrap().blocks();
        let reserved = blocks();
        disk.erase(at, 8192).unwrap();
        assert_eq!(blocks(), reserved);
        assert_eq!(disk.read(at, 0, 8192).unwrap(), vec![0; 8192]);
    }

    /// A start whose slab file the filesystem can't hold fails before it
    /// touches anything else. A file past the filesystem's largest fails
    /// without allocating a byte.
    #[test]
    fn a_slab_file_the_filesystem_can_not_hold_stops_the_start() {
        let dir = dir("too-large");
        let config = StoreConfig {
            extent_size: 1 << 60,
            extents: 4,
            min_slot: 4096,
            max_slot: 8192,
        };
        let Err(error) = Disk::open(&dir, config) else {
            panic!("a start with no room for its slab file");
        };
        assert!(error.to_string().contains("reserving"), "{error}");
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
            assert!(disk.write(at(1, 4096), &[1; 1000]).unwrap().is_some());
            disk.record(at(1, 4096), record()).unwrap();
            assert!(disk.write(at(2, 0), &[2; 1000]).unwrap().is_some());
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
        // Extents half as large over the same slots keep the record, which
        // lies in the third of them.
        let halves = StoreConfig {
            extent_size: config().extent_size / 2,
            extents: config().extents * 2,
            ..config()
        };
        let (disk, recovery) = Disk::open(&dir, halves).unwrap();
        let [recovered] = recovery.records.as_slice() else {
            panic!("{} records", recovery.records.len());
        };
        assert_eq!(recovered.location, at(2, 4096));
        assert_eq!(recovery.metadata, vec![(key(), None)]);
        disk.shut_down().unwrap();
        drop(disk);
        let other = StoreConfig {
            max_slot: 4096,
            ..config()
        };
        let (_, recovery) = Disk::open(&dir, other).unwrap();
        assert!(recovery.records.is_empty() && recovery.metadata.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Writes that asked while another sync ran share the next one, and if
    /// it fails, each of them fails, while later writes sync afresh.
    #[test]
    fn a_failed_sync_fails_every_write_it_covered() {
        use std::sync::Arc;
        use std::sync::mpsc::channel;
        let group = Arc::new(GroupSync::default());
        let (started, has_started) = channel();
        let (release, released) = channel::<()>();
        let first = {
            let group = group.clone();
            std::thread::spawn(move || {
                let write = group.begin();
                group.sync(write, || {
                    started.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                })
            })
        };
        has_started.recv().unwrap();
        let flushes = Arc::new(Mutex::new(0));
        let waiting: Vec<_> = (0..2)
            .map(|_| {
                let (group, flushes) = (group.clone(), flushes.clone());
                let write = group.begin();
                std::thread::spawn(move || {
                    group.sync(write, || {
                        *flushes.lock().unwrap() += 1;
                        Err(io::Error::other("the drive failed a flush"))
                    })
                })
            })
            .collect();
        while group.state.lock().unwrap().waiting < 3 {
            std::thread::yield_now();
        }
        release.send(()).unwrap();
        assert!(first.join().unwrap().is_ok());
        for write in waiting {
            assert!(write.join().unwrap().is_err());
        }
        assert_eq!(*flushes.lock().unwrap(), 1, "one sync covers both");
        let write = group.begin();
        assert!(group.sync(write, || Ok(())).is_ok());
    }

    /// A write that ended while a sync that failed was flushing may have
    /// had its pages dropped, so it fails although the next sync succeeds.
    #[test]
    fn a_write_during_a_failed_sync_fails() {
        use std::sync::Arc;
        use std::sync::mpsc::channel;
        let group = Arc::new(GroupSync::default());
        let write = group.begin();
        let (started, has_started) = channel();
        let (release, released) = channel::<()>();
        let failing = {
            let group = group.clone();
            std::thread::spawn(move || {
                let other = group.begin();
                group.sync(other, || {
                    started.send(()).unwrap();
                    released.recv().unwrap();
                    Err(io::Error::other("the drive failed a flush"))
                })
            })
        };
        has_started.recv().unwrap();
        // The write ends and asks while the failing sync flushes.
        let asking = {
            let group = group.clone();
            std::thread::spawn(move || group.sync(write, || Ok(())))
        };
        while group.state.lock().unwrap().waiting < 2 {
            std::thread::yield_now();
        }
        release.send(()).unwrap();
        assert!(failing.join().unwrap().is_err());
        assert!(asking.join().unwrap().is_err());
    }
}
