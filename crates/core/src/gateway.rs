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
use crate::placement::{NodeId, Placement, PlacementHash, Ring};
use crate::s3::{Answer, ETag, ObjectKey, Request, ResponseHead, answer};
use std::collections::BTreeMap;

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
    /// Milliseconds a node has to answer before the gateway asks the next
    /// rendezvous candidate.
    pub node_timeout: u64,
    /// Milliseconds the gateway routes around a node that timed out.
    pub suspect_ttl: u64,
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
    now: Time,
    next_node_request: u64,
    reads: BTreeMap<ClientRequestId, ClientRead>,
    /// The node requests of reads in progress.
    parts: BTreeMap<NodeRequestId, Part>,
    /// Nodes that timed out, and until when the gateway routes around them.
    suspects: BTreeMap<NodeId, Time>,
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
    },
}

/// A node request of a read in progress.
struct Part {
    read: ClientRequestId,
    what: What,
    node: NodeId,
    sent: Time,
    /// Nodes asked before this one, which timed out or failed.
    tried: Vec<NodeId>,
    answered: bool,
}

/// Bytes `first..=last` of a version that share a placement.
type Run = (PlacementHash, u64, u64);

/// What a part asks a node for.
#[derive(Clone)]
enum What {
    /// A client's request, from the object's home.
    Object(Read, PlacementHash),
    /// Bytes of one version: runs that went to one node, each with its own
    /// placement, so a failover can send each to its own next candidate.
    Range {
        key: ObjectKey,
        etag: ETag,
        size: u64,
        runs: Vec<Run>,
    },
}

impl What {
    fn read(&self) -> Read {
        match self {
            What::Object(read, _) => read.clone(),
            What::Range {
                key,
                etag,
                size,
                runs,
            } => Read::Range(RangeRead {
                key: key.clone(),
                etag: etag.clone(),
                size: *size,
                first: runs.first().expect("a part has runs").1,
                last: runs.last().expect("a part has runs").2,
            }),
        }
    }
}

impl Gateway {
    pub fn new(ring: Ring, config: Config) -> Gateway {
        assert!(config.metadata_capacity > 0, "no room for metadata");
        Gateway {
            cache: MetadataCache::new(config.metadata_capacity),
            config,
            ring,
            now: Time::default(),
            next_node_request: 0,
            reads: BTreeMap::new(),
            parts: BTreeMap::new(),
            suspects: BTreeMap::new(),
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
        self.now = self.now.max(now);
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
        self.now = self.now.max(now);
        self.cache.written(key, now);
    }

    /// Time passed: a node request unanswered past the timeout goes to the
    /// next rendezvous candidate, and its node is routed around for a while.
    pub fn on_tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        self.suspects.retain(|_, until| *until > now);
        let timeout = self.config.node_timeout;
        let expired: Vec<NodeRequestId> = self
            .parts
            .iter()
            .filter(|(_, part)| !part.answered && part.sent.0 + timeout <= now.0)
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            self.fail_over(now, id, None);
        }
    }

    pub fn on_node_response(
        &mut self,
        now: Time,
        from: NodeRequestId,
        head: ResponseHead,
        meta: Option<ObjectMeta>,
    ) {
        self.now = self.now.max(now);
        let Some(part) = self.parts.get_mut(&from) else {
            return self.actions.push(Action::Discard { id: from });
        };
        if head.status >= 500 {
            // The node could not serve it; the next candidate may.
            self.actions.push(Action::Discard { id: from });
            return self.fail_over(now, from, Some(head));
        }
        part.answered = true;
        let id = part.read;
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        match &read.stage {
            Stage::Home { sent } => {
                if let Some(meta) = meta {
                    let (key, sent) = (read.request.key.clone(), *sent);
                    self.cache
                        .insert(key, CachedMeta::new(meta, sent, now), sent);
                }
                self.finish(id, head, vec![from]);
            }
            Stage::Parts { .. } if head.status != 206 => {
                // S3 refused the part: the client gets its answer.
                self.abandon_parts_except(id, from);
                self.finish(id, head, vec![from]);
            }
            Stage::Parts { head, parts } => {
                if parts.iter().all(|part| self.parts[part].answered) {
                    let (head, parts) = (head.clone(), parts.clone());
                    self.finish(id, head, parts);
                }
            }
            Stage::Planning => unreachable!("a planning read has no parts"),
        }
    }

    /// The home answered with the object's metadata; the read goes to the
    /// blocks' owners.
    pub fn on_node_metadata(&mut self, now: Time, from: NodeRequestId, meta: ObjectMeta) {
        self.now = self.now.max(now);
        let Some(part) = self.parts.remove(&from) else {
            return;
        };
        let id = part.read;
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
        self.now = self.now.max(now);
        let Some(part) = self.parts.remove(&from) else {
            return;
        };
        let id = part.read;
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
        let read = self.reads.get_mut(&id).expect("planned read exists");
        let (request, stale) = (read.request.clone(), read.stale.take());
        read.stage = Stage::Home { sent: now };
        let read = Read::Object {
            request,
            stale,
            direct,
        };
        let placement = Placement::Home(&key).hash();
        match self.target(placement, &[]) {
            Some(node) => {
                self.dispatch(id, What::Object(read, placement), node, Vec::new());
            }
            None => self.finish(id, ResponseHead::status(503), Vec::new()),
        }
    }

    /// Plans a read from known metadata: preconditions and heads are
    /// answered here, and each run of blocks with one placement becomes a
    /// part.
    fn plan_with(&mut self, id: ClientRequestId, meta: &ObjectMeta) {
        let request = self.reads[&id].request.clone();
        let (head, first, last) = match answer(&request, &meta.etag, meta.size, &meta.headers) {
            Answer::Head(head) => return self.finish(id, head, Vec::new()),
            Answer::Body { head, first, last } => (head, first, last),
        };
        let runs: Vec<Run> = self
            .config
            .layout
            .runs(&request.key, meta.size, first, last)
            .into_iter()
            .map(|(placement, start, end)| (placement.hash(), start, end))
            .collect();
        let Some(parts) = self.dispatch_runs(id, &request.key, &meta.etag, meta.size, runs, &[])
        else {
            return self.finish(id, ResponseHead::status(503), Vec::new());
        };
        let read = self.reads.get_mut(&id).expect("planned read exists");
        read.stage = Stage::Parts { head, parts };
    }

    /// Sends runs of a version's bytes to their targets, one part per run
    /// of runs with one target; `None` if some run has no candidate left.
    fn dispatch_runs(
        &mut self,
        id: ClientRequestId,
        key: &ObjectKey,
        etag: &ETag,
        size: u64,
        runs: Vec<Run>,
        tried: &[NodeId],
    ) -> Option<Vec<NodeRequestId>> {
        let mut groups: Vec<(NodeId, Vec<Run>)> = Vec::new();
        for run in runs {
            let node = self.target(run.0, tried)?;
            match groups.last_mut() {
                Some((target, runs)) if *target == node => runs.push(run),
                _ => groups.push((node, vec![run])),
            }
        }
        let parts = groups
            .into_iter()
            .map(|(node, runs)| {
                let what = What::Range {
                    key: key.clone(),
                    etag: etag.clone(),
                    size,
                    runs,
                };
                self.dispatch(id, what, node, tried.to_vec())
            })
            .collect();
        Some(parts)
    }

    /// The best candidate for `placement` not in `tried`, preferring nodes
    /// that are not suspected: usually the owner, found without ranking the
    /// whole ring.
    fn target(&self, placement: PlacementHash, tried: &[NodeId]) -> Option<NodeId> {
        let owner = self.ring.owner(placement)?;
        if !tried.contains(&owner) && !self.suspects.contains_key(&owner) {
            return Some(owner);
        }
        let candidates: Vec<NodeId> = self
            .ring
            .candidates(placement)
            .into_iter()
            .filter(|node| !tried.contains(node))
            .collect();
        let healthy = candidates
            .iter()
            .find(|node| !self.suspects.contains_key(node));
        healthy.or(candidates.first()).copied()
    }

    fn dispatch(
        &mut self,
        id: ClientRequestId,
        what: What,
        node: NodeId,
        tried: Vec<NodeId>,
    ) -> NodeRequestId {
        let node_request = NodeRequestId(self.next_node_request);
        self.next_node_request += 1;
        let read = what.read();
        let part = Part {
            read: id,
            what,
            node,
            sent: self.now,
            tried,
            answered: false,
        };
        self.parts.insert(node_request, part);
        self.actions.push(Action::Send {
            node,
            id: node_request,
            read,
        });
        node_request
    }

    /// Resends a part that timed out, or that its node failed with
    /// `failure`, to the next rendezvous candidates. A node that timed out
    /// is routed around for a while. When no candidate is left, the client
    /// gets the failure, or a 503.
    fn fail_over(&mut self, now: Time, id: NodeRequestId, failure: Option<ResponseHead>) {
        // An earlier failover in the same tick may have ended this read.
        let Some(part) = self.parts.remove(&id) else {
            return;
        };
        if failure.is_none() {
            self.suspects
                .insert(part.node, Time(now.0 + self.config.suspect_ttl));
        }
        let mut tried = part.tried;
        tried.push(part.node);
        let replacements = match part.what {
            What::Object(read, placement) => self.target(placement, &tried).map(|node| {
                vec![self.dispatch(part.read, What::Object(read, placement), node, tried)]
            }),
            What::Range {
                key,
                etag,
                size,
                runs,
            } => self.dispatch_runs(part.read, &key, &etag, size, runs, &tried),
        };
        let Some(replacements) = replacements else {
            self.abandon_parts_except(part.read, id);
            let head = failure.unwrap_or_else(|| ResponseHead::status(503));
            return self.finish(part.read, head, Vec::new());
        };
        let read = self
            .reads
            .get_mut(&part.read)
            .expect("a part's read exists");
        if let Stage::Parts { parts, .. } = &mut read.stage
            && let Some(position) = parts.iter().position(|slot| *slot == id)
        {
            parts.splice(position..=position, replacements);
        }
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
        let Stage::Parts { parts, .. } = &read.stage else {
            return;
        };
        let others: Vec<NodeRequestId> =
            parts.iter().copied().filter(|&part| part != keep).collect();
        self.abandon_parts(&others);
    }

    fn abandon_parts(&mut self, parts: &[NodeRequestId]) {
        for part in parts {
            if let Some(part_state) = self.parts.remove(part)
                && part_state.answered
            {
                self.actions.push(Action::Discard { id: *part });
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
