//! The gateway: plans each client read and sends its parts to the storage
//! nodes that own them.
//!
//! A gateway that knows an object's metadata answers preconditions and
//! `HeadObject` itself, and reads each byte range from the node that owns
//! it, fanning out across nodes. Otherwise it asks the object's home, whose
//! answer carries the metadata. Every range read names the version, so the
//! parts of one response belong to one version; a node that finds the
//! object changed sends the read back, and the gateway forgets the metadata
//! and plans again.

use crate::Time;
use crate::layout::Layout;
use crate::node::{BucketPolicy, Freshness, ObjectMeta, RangeRead, Read};
use crate::placement::{NodeId, Placement, Ring};
use crate::s3::{Answer, ETag, ObjectKey, Request, ResponseHead, answer};
use std::collections::{BTreeMap, BTreeSet};

/// Stale retries after which a read goes to S3 directly, through the home,
/// uncached: a read whose object keeps changing still finishes.
const STALE_RETRIES: u32 = 4;

/// A client request, numbered by the gateway's owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientRequestId(pub u64);

/// A request the gateway sent to a storage node, numbered by the gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeRequestId(pub u64);

#[derive(Clone, Debug)]
pub struct Config {
    pub layout: Layout,
    pub default_policy: BucketPolicy,
    pub buckets: BTreeMap<String, BucketPolicy>,
    /// Objects whose metadata the gateway keeps.
    pub metadata_capacity: usize,
    /// How long the gateway keeps metadata of objects that may change, in
    /// milliseconds, however long the bucket's policy would allow.
    pub metadata_ttl: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send `read` to `node`.
    Send {
        node: NodeId,
        id: NodeRequestId,
        read: Read,
    },
    /// Answer the client with `head`, then the bodies of the nodes'
    /// responses to `from`, in order.
    Relay {
        request: ClientRequestId,
        head: ResponseHead,
        from: Vec<NodeRequestId>,
    },
    /// Answer the client with `head` and no body.
    Respond {
        request: ClientRequestId,
        head: ResponseHead,
    },
    /// Drop the body of the node's response to `id`, whose read was
    /// abandoned.
    Discard { id: NodeRequestId },
}

pub struct Gateway {
    config: Config,
    ring: Ring,
    cache: MetadataCache,
    next_node_request: u64,
    reads: BTreeMap<ClientRequestId, ClientRead>,
    /// The node requests of reads in progress.
    parts: BTreeMap<NodeRequestId, ClientRequestId>,
    actions: Vec<Action>,
}

struct ClientRead {
    request: Request,
    arrived: Time,
    stage: Stage,
    /// An ETag a node found out of date, which the home is told about.
    stale: Option<ETag>,
    /// Times a node found the object changed during this read.
    retries: u32,
}

enum Stage {
    /// Planning has not started or is starting again.
    Planning,
    /// The home was asked, at `sent`.
    Home { sent: Time },
    /// Parts were sent to their owners; the response starts once all
    /// have answered.
    Parts {
        head: ResponseHead,
        parts: Vec<NodeRequestId>,
        answered: BTreeSet<NodeRequestId>,
    },
}

impl Gateway {
    pub fn new(ring: Ring, config: Config) -> Gateway {
        assert!(config.metadata_capacity > 0, "no room for metadata");
        Gateway {
            cache: MetadataCache::new(config.metadata_capacity),
            config,
            ring,
            next_node_request: 0,
            reads: BTreeMap::new(),
            parts: BTreeMap::new(),
            actions: Vec::new(),
        }
    }

    /// True when no read is in progress.
    pub fn is_idle(&self) -> bool {
        self.reads.is_empty() && self.parts.is_empty()
    }

    /// The actions since the last drain, in the order the gateway took them.
    pub fn drain(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    pub fn on_request(&mut self, now: Time, id: ClientRequestId, mut request: Request) {
        request.range = request.range.filter(|range| range.is_valid());
        let read = ClientRead {
            request,
            arrived: now,
            stage: Stage::Planning,
            stale: None,
            retries: 0,
        };
        self.reads.insert(id, read);
        self.plan(now, id);
    }

    /// A write this gateway passed to S3 succeeded at `now`: it forgets
    /// the key's metadata, and ignores answers to reads sent before.
    pub fn on_write(&mut self, now: Time, key: &ObjectKey) {
        self.cache.written(key, now);
    }

    pub fn on_node_response(
        &mut self,
        now: Time,
        from: NodeRequestId,
        head: ResponseHead,
        meta: Option<ObjectMeta>,
    ) {
        let Some(&id) = self.parts.get(&from) else {
            return self.actions.push(Action::Discard { id: from });
        };
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        match &mut read.stage {
            Stage::Home { sent } => {
                if let Some(meta) = meta {
                    let (key, sent) = (read.request.key.clone(), *sent);
                    self.cache
                        .insert(key, CachedMeta::new(meta, sent, now), sent);
                }
                self.finish(id, head, vec![from]);
            }
            Stage::Parts { .. } if head.status != 206 => {
                // S3 failed a fill: the client gets that error.
                self.abandon_parts_except(id, from);
                self.finish(id, head, vec![from]);
            }
            Stage::Parts {
                head: planned,
                parts,
                answered,
            } => {
                answered.insert(from);
                if answered.len() == parts.len() {
                    let (head, parts) = (planned.clone(), parts.clone());
                    self.finish(id, head, parts);
                }
            }
            Stage::Planning => unreachable!("a planning read has no parts"),
        }
    }

    /// The home answered with the object's metadata; the read goes to the
    /// blocks' owners.
    pub fn on_node_metadata(&mut self, now: Time, from: NodeRequestId, meta: ObjectMeta) {
        let Some(id) = self.parts.remove(&from) else {
            return;
        };
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        let Stage::Home { sent } = read.stage else {
            unreachable!("only the home answers with metadata");
        };
        read.stage = Stage::Planning;
        let key = read.request.key.clone();
        let cached = CachedMeta::new(meta, sent, now);
        self.cache.insert(key, cached.clone(), sent);
        self.plan_with(id, &cached.meta);
    }

    /// The object changed while a node served part of the read: forget the
    /// metadata, tell the home, and plan again.
    pub fn on_node_stale(&mut self, now: Time, from: NodeRequestId) {
        let Some(id) = self.parts.remove(&from) else {
            return;
        };
        self.abandon_parts_except(id, from);
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        if let Stage::Parts { head, .. } = &read.stage {
            read.stale = head.etag.clone();
        }
        read.retries += 1;
        read.stage = Stage::Planning;
        let key = read.request.key.clone();
        self.cache.remove(&key);
        self.plan(now, id);
    }

    fn plan(&mut self, now: Time, id: ClientRequestId) {
        let read = &self.reads[&id];
        let key = read.request.key.clone();
        let (arrived, policy) = (read.arrived, self.policy(&key.bucket));
        let direct = read.retries >= STALE_RETRIES;
        let ttl = self.config.metadata_ttl;
        if !direct && let Some(cached) = self.cache.fresh(&key, arrived, policy, ttl) {
            let meta = cached.meta.clone();
            return self.plan_with(id, &meta);
        }
        let Some(home) = self.ring.owner(Placement::Home(&key).hash()) else {
            return self.finish(id, ResponseHead::status(503), Vec::new());
        };
        let read = self.reads.get_mut(&id).expect("planned read exists");
        let (request, stale) = (read.request.clone(), read.stale.take());
        let read = Read::Object {
            request,
            stale,
            direct,
        };
        self.send(home, id, read);
        let read = self.reads.get_mut(&id).expect("planned read exists");
        read.stage = Stage::Home { sent: now };
    }

    /// Plans a read from known metadata: preconditions and heads are
    /// answered here, and each run of blocks with one owner becomes a part.
    fn plan_with(&mut self, id: ClientRequestId, meta: &ObjectMeta) {
        let request = self.reads[&id].request.clone();
        let (head, first, last) = match answer(&request, &meta.etag, meta.size, &meta.headers) {
            Answer::Head(head) => return self.finish(id, head, Vec::new()),
            Answer::Body { head, first, last } => (head, first, last),
        };
        let mut parts = Vec::new();
        for (node, part_first, part_last) in self.owners(&request.key, meta.size, first, last) {
            let range = RangeRead {
                key: request.key.clone(),
                etag: meta.etag.clone(),
                size: meta.size,
                first: part_first,
                last: part_last,
            };
            parts.push(self.send(node, id, Read::Range(range)));
        }
        let read = self.reads.get_mut(&id).expect("planned read exists");
        read.stage = Stage::Parts {
            head,
            parts,
            answered: BTreeSet::new(),
        };
    }

    /// Bytes `first..=last` split into runs with one owner.
    fn owners(&self, key: &ObjectKey, size: u64, first: u64, last: u64) -> Vec<(NodeId, u64, u64)> {
        let mut runs: Vec<(NodeId, u64, u64)> = Vec::new();
        for (placement, start, end) in self.config.layout.runs(key, size, first, last) {
            let owner = self
                .ring
                .owner(placement.hash())
                .expect("a ring with members");
            match runs.last_mut() {
                Some((node, _, run_end)) if *node == owner => *run_end = end,
                _ => runs.push((owner, start, end)),
            }
        }
        runs
    }

    fn send(&mut self, node: NodeId, id: ClientRequestId, read: Read) -> NodeRequestId {
        let node_request = NodeRequestId(self.next_node_request);
        self.next_node_request += 1;
        self.parts.insert(node_request, id);
        self.actions.push(Action::Send {
            node,
            id: node_request,
            read,
        });
        node_request
    }

    /// Answers the client and ends the read.
    fn finish(&mut self, id: ClientRequestId, head: ResponseHead, from: Vec<NodeRequestId>) {
        for part in &from {
            self.parts.remove(part);
        }
        self.reads.remove(&id);
        if from.is_empty() {
            self.actions.push(Action::Respond { request: id, head });
        } else {
            self.actions.push(Action::Relay {
                request: id,
                head,
                from,
            });
        }
    }

    /// Stops waiting for a read's other parts; answered ones are dropped
    /// now, the rest when they arrive.
    fn abandon_parts_except(&mut self, id: ClientRequestId, keep: NodeRequestId) {
        let Some(read) = self.reads.get(&id) else {
            return;
        };
        let Stage::Parts {
            parts, answered, ..
        } = &read.stage
        else {
            return;
        };
        for &part in parts.iter().filter(|&&part| part != keep) {
            self.parts.remove(&part);
            if answered.contains(&part) {
                self.actions.push(Action::Discard { id: part });
            }
        }
    }

    fn policy(&self, bucket: &str) -> BucketPolicy {
        self.config
            .buckets
            .get(bucket)
            .copied()
            .unwrap_or(self.config.default_policy)
    }
}

#[derive(Clone, Debug)]
struct CachedMeta {
    meta: ObjectMeta,
    /// When the home confirmed the metadata with S3, or earlier: the
    /// gateway's request time less the metadata's age.
    validated: Time,
    received: Time,
}

impl CachedMeta {
    fn new(meta: ObjectMeta, sent: Time, now: Time) -> CachedMeta {
        CachedMeta {
            validated: Time(sent.0.saturating_sub(meta.age)),
            meta,
            received: now,
        }
    }
}

/// Object metadata by key, dropping the least recently used past capacity.
/// A key written through this gateway keeps its entry, empty, with the time
/// of the write.
struct MetadataCache {
    capacity: usize,
    entries: BTreeMap<ObjectKey, Entry>,
    recency: BTreeMap<u64, ObjectKey>,
    next_use: u64,
}

struct Entry {
    meta: Option<CachedMeta>,
    /// Answers to reads sent before this were answered before the last
    /// write, and are ignored.
    written: Time,
    used: u64,
}

impl MetadataCache {
    fn new(capacity: usize) -> MetadataCache {
        MetadataCache {
            capacity,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            next_use: 0,
        }
    }

    /// The metadata for `key` if it may answer a request that arrived at
    /// `arrived`.
    fn fresh(
        &mut self,
        key: &ObjectKey,
        arrived: Time,
        policy: BucketPolicy,
        ttl: u64,
    ) -> Option<&CachedMeta> {
        let cached = self.entries.get(key)?.meta.as_ref()?;
        let fresh = match policy.freshness {
            Freshness::Immutable => true,
            Freshness::Ttl(bucket_ttl) => {
                cached.validated.0 + bucket_ttl >= arrived.0 && cached.received.0 + ttl >= arrived.0
            }
        };
        if !fresh {
            return None;
        }
        self.touch(key);
        self.entries.get(key)?.meta.as_ref()
    }

    /// Stores metadata from the answer to a read sent at `sent`, unless a
    /// write or newer metadata superseded it.
    fn insert(&mut self, key: ObjectKey, meta: CachedMeta, sent: Time) {
        let written = match self.entries.get(&key) {
            Some(entry) if sent < entry.written => return,
            Some(Entry {
                meta: Some(current),
                ..
            }) if current.validated > meta.validated => return,
            Some(entry) => entry.written,
            None => Time::default(),
        };
        self.put(key, Some(meta), written);
    }

    fn written(&mut self, key: &ObjectKey, now: Time) {
        self.put(key.clone(), None, now);
    }

    fn remove(&mut self, key: &ObjectKey) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.meta = None;
        }
    }

    fn put(&mut self, key: ObjectKey, meta: Option<CachedMeta>, written: Time) {
        if let Some(entry) = self.entries.remove(&key) {
            self.recency.remove(&entry.used);
        }
        let used = self.next_use;
        self.next_use += 1;
        self.recency.insert(used, key.clone());
        self.entries.insert(
            key,
            Entry {
                meta,
                written,
                used,
            },
        );
        while self.entries.len() > self.capacity {
            let (_, oldest) = self.recency.pop_first().expect("over capacity");
            self.entries.remove(&oldest);
        }
    }

    fn touch(&mut self, key: &ObjectKey) {
        let next = self.next_use;
        self.next_use += 1;
        if let Some(entry) = self.entries.get_mut(key) {
            self.recency.remove(&entry.used);
            entry.used = next;
            self.recency.insert(next, key.clone());
        }
    }
}
