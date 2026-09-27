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
//!
//! The response starts once every part has answered, and the parts' bodies
//! follow in order. If a body ends early, the gateway asks the next
//! candidates for the rest of that version; if the object changed, or no
//! node can serve the rest, the response ends early and the client retries.

use crate::Time;
use crate::layout::Layout;
use crate::node::{BucketPolicy, Freshness, ObjectMeta, RangeRead, Read};
use crate::placement::{NodeId, Placement, PlacementHash, Ring};
use crate::s3::{Answer, ContentRange, ETag, Method, ObjectKey, Request, ResponseHead, answer};
use std::collections::{BTreeMap, VecDeque};

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
    /// Start the client's response with `head`. `Forward` actions supply
    /// its body, which ends after `head.content_length` bytes or at `Abort`.
    Start {
        request: ClientRequestId,
        head: ResponseHead,
    },
    /// Copy the first `len` bytes of the body of the node's response to
    /// `from` into the client's response, then call `on_forwarded` with the
    /// bytes copied. The body is not needed after.
    Forward {
        request: ClientRequestId,
        from: NodeRequestId,
        len: u64,
    },
    /// End the client's started response before its body is complete: the
    /// object changed, or no node could serve the rest. No `Forward` for it
    /// is in progress.
    Abort { request: ClientRequestId },
    /// Answer the client with `head` and no body.
    Respond {
        request: ClientRequestId,
        head: ResponseHead,
    },
    /// Drop the body of the node's response to `id`, which the client's
    /// response will not use.
    Discard { id: NodeRequestId },
    /// Fetch the ring from `node`, and pass it to `on_ring`.
    FetchRing { node: NodeId },
    /// Every node the ring names for a read failed: fetch the ring from a
    /// node the owner knows of otherwise, and pass it to `on_ring`.
    FindRing,
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
    /// When the gateway last asked a node for its ring, until it arrives.
    fetching_ring: Option<Time>,
    actions: Vec<Action>,
}

struct ClientRead {
    request: Request,
    arrived: Time,
    stage: Stage,
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
        parts: VecDeque<NodeRequestId>,
    },
    /// The response started, and `parts` hold the rest of its body in
    /// order. The first is forwarded once it has answered.
    Streaming {
        parts: VecDeque<NodeRequestId>,
        /// Body bytes not yet forwarded.
        remaining: u64,
        forwarding: bool,
        /// The response ends early once the forward in progress finishes.
        aborted: bool,
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
    /// The node's answer, once it arrives.
    answer: Option<ResponseHead>,
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
    /// Whether a 206 carries exactly the bytes this part asked for.
    fn answered_by(&self, head: &ResponseHead) -> bool {
        match self {
            What::Object(..) => true,
            What::Range {
                etag, size, runs, ..
            } => {
                let (first, last) = (runs[0].1, runs[runs.len() - 1].2);
                let range = ContentRange {
                    first,
                    last,
                    size: *size,
                };
                head.etag.as_ref() == Some(etag)
                    && head.content_range == Some(range)
                    && head.content_length == last - first + 1
            }
        }
    }

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
            fetching_ring: None,
            actions: Vec::new(),
        }
    }

    /// The ring the gateway routes by.
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    /// A node answered with its ring's version. A version other than the
    /// gateway's means the ring changed, so the gateway fetches it from
    /// that node, one fetch at a time.
    pub fn on_ring_version(&mut self, now: Time, from: NodeId, version: u64) {
        self.now = self.now.max(now);
        let fetching = self
            .fetching_ring
            .is_some_and(|since| now.0 < since.0 + self.config.node_timeout);
        if version == self.ring.version() || fetching {
            return;
        }
        self.fetching_ring = Some(now);
        self.actions.push(Action::FetchRing { node: from });
    }

    /// A ring fetch failed: the next answer with another version, or the
    /// next read no node can serve, asks again.
    pub fn on_ring_failed(&mut self, now: Time) {
        self.now = self.now.max(now);
        self.fetching_ring = None;
    }

    /// A node sent its ring. Reads in progress keep the nodes they went
    /// to; later ones go by this ring.
    pub fn on_ring(&mut self, now: Time, ring: Ring) {
        self.now = self.now.max(now);
        self.fetching_ring = None;
        self.ring = ring;
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
            .filter(|(_, part)| part.answer.is_none() && part.sent.0 + timeout <= now.0)
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
        // A driver delivers one answer per request; a repeat is ignored.
        if part.answer.is_some() {
            return;
        }
        if head.status >= 500 {
            // The node could not serve it; the next candidate may.
            self.actions.push(Action::Discard { id: from });
            return self.fail_over(now, from, Some(head));
        }
        if head.status == 206 && !part.what.answered_by(&head) {
            // The node sent other bytes than those asked for.
            self.actions.push(Action::Discard { id: from });
            return self.fail_over(now, from, Some(ResponseHead::status(502)));
        }
        part.answer = Some(head.clone());
        let id = part.read;
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        match &read.stage {
            Stage::Home { sent } => {
                if let Some(meta) = meta {
                    let (key, sent) = (read.request.key.clone(), *sent);
                    self.cache
                        .insert(key, CachedMeta::new(meta, sent, now), sent);
                }
                self.start(id, head, VecDeque::from([from]));
            }
            Stage::Parts { .. } if head.status != 206 => {
                // S3 refused the part: the client gets its answer.
                self.abandon_parts_except(id, from);
                self.start(id, head, VecDeque::from([from]));
            }
            Stage::Parts { head, parts } => {
                if parts.iter().all(|part| self.parts[part].answer.is_some()) {
                    let (head, parts) = (head.clone(), parts.clone());
                    self.start(id, head, parts);
                }
            }
            // The rest of a body that ended early. It must be the bytes
            // asked for, since the response already started.
            Stage::Streaming { .. } if head.status != 206 => self.abort(id),
            Stage::Streaming { .. } => self.forward_next(id),
            Stage::Planning => unreachable!("a planning read has no parts"),
        }
    }

    /// The driver copied `copied` bytes of the body of the node's response
    /// to `from` into the client's response. A body that ended early is
    /// read from the next candidates, from where it stopped.
    pub fn on_forwarded(&mut self, now: Time, from: NodeRequestId, copied: u64) {
        self.now = self.now.max(now);
        let Some(part) = self.parts.remove(&from) else {
            return;
        };
        let id = part.read;
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        let Stage::Streaming {
            parts,
            remaining,
            forwarding,
            aborted,
        } = &mut read.stage
        else {
            unreachable!("only a started response forwards");
        };
        assert_eq!(parts.pop_front(), Some(from), "forwarded out of order");
        *forwarding = false;
        *remaining = remaining.saturating_sub(copied);
        if *aborted {
            return self.abort(id);
        }
        let expected = part
            .answer
            .as_ref()
            .expect("forwarded answer")
            .content_length;
        if copied < expected {
            return self.resume(id, part, copied);
        }
        self.forward_next(id);
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
        // The next read of the key through the home reports the ETag, so
        // the home revalidates it.
        let key = self.reads[&id].request.key.clone();
        match part.what {
            What::Range { etag, .. } => self.cache.stale(&key, etag),
            What::Object(..) => self.cache.remove(&key),
        }
        if matches!(self.reads[&id].stage, Stage::Streaming { .. }) {
            return self.abort(id);
        }
        self.abandon_parts_except(id, from);
        let read = self.reads.get_mut(&id).expect("a part's read exists");
        read.retries += 1;
        read.stage = Stage::Planning;
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
        let stale = self.cache.take_stale(&key);
        let read = self.reads.get_mut(&id).expect("planned read exists");
        let request = read.request.clone();
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
            None => {
                self.find_ring(now);
                self.respond(id, ResponseHead::status(503));
            }
        }
    }

    /// The ring named no node that could serve a read, and may name only
    /// nodes that are gone: the gateway asks for the ring elsewhere.
    fn find_ring(&mut self, now: Time) {
        let fetching = self
            .fetching_ring
            .is_some_and(|since| now.0 < since.0 + self.config.node_timeout);
        if !fetching {
            self.fetching_ring = Some(now);
            self.actions.push(Action::FindRing);
        }
    }

    /// Plans a read from known metadata: preconditions and heads are
    /// answered here, and each run of blocks with one placement becomes a
    /// part.
    fn plan_with(&mut self, id: ClientRequestId, meta: &ObjectMeta) {
        let request = self.reads[&id].request.clone();
        let (head, first, last) = match answer(&request, &meta.etag, meta.size, &meta.headers) {
            Answer::Head(head) => return self.respond(id, head),
            Answer::Body { head, first, last } => (head, first, last),
        };
        let runs = self.runs(&request.key, meta.size, first, last);
        let Some(parts) = self.dispatch_runs(id, &request.key, &meta.etag, meta.size, runs, &[])
        else {
            let now = self.now;
            self.find_ring(now);
            return self.respond(id, ResponseHead::status(503));
        };
        let read = self.reads.get_mut(&id).expect("planned read exists");
        read.stage = Stage::Parts { head, parts };
    }

    /// Bytes `first..=last` of an object of `size` bytes, as runs that
    /// share a placement.
    fn runs(&self, key: &ObjectKey, size: u64, first: u64, last: u64) -> Vec<Run> {
        self.config
            .layout
            .runs(key, size, first, last)
            .into_iter()
            .map(|(placement, start, end)| (placement.hash(), start, end))
            .collect()
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
    ) -> Option<VecDeque<NodeRequestId>> {
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
            answer: None,
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
                VecDeque::from([self.dispatch(
                    part.read,
                    What::Object(read, placement),
                    node,
                    tried,
                )])
            }),
            What::Range {
                key,
                etag,
                size,
                runs,
            } => self.dispatch_runs(part.read, &key, &etag, size, runs, &tried),
        };
        let Some(replacements) = replacements else {
            self.find_ring(now);
            if matches!(self.reads[&part.read].stage, Stage::Streaming { .. }) {
                return self.abort(part.read);
            }
            self.abandon_parts_except(part.read, id);
            let head = failure.unwrap_or_else(|| ResponseHead::status(503));
            return self.respond(part.read, head);
        };
        let read = self
            .reads
            .get_mut(&part.read)
            .expect("a part's read exists");
        if let Stage::Parts { parts, .. } | Stage::Streaming { parts, .. } = &mut read.stage
            && let Some(position) = parts.iter().position(|slot| *slot == id)
        {
            let mut after = parts.split_off(position);
            after.pop_front();
            parts.extend(replacements);
            parts.extend(after);
        }
    }

    /// Answers the client with `head` and no body, and ends the read.
    fn respond(&mut self, id: ClientRequestId, head: ResponseHead) {
        self.reads.remove(&id);
        self.actions.push(Action::Respond { request: id, head });
    }

    /// Starts the client's response with `head`, and a body from `parts`
    /// unless it has none.
    fn start(&mut self, id: ClientRequestId, head: ResponseHead, parts: VecDeque<NodeRequestId>) {
        let head_only = self.reads[&id].request.method == Method::Head;
        if head_only || head.content_length == 0 {
            self.abandon_parts(parts);
            return self.respond(id, head);
        }
        let remaining = head.content_length;
        self.actions.push(Action::Start { request: id, head });
        let read = self.reads.get_mut(&id).expect("a started read exists");
        read.stage = Stage::Streaming {
            parts,
            remaining,
            forwarding: false,
            aborted: false,
        };
        self.forward_next(id);
    }

    /// Forwards the next part's body once it has answered, and ends the
    /// read after the last: early, if the parts fell short of the head.
    fn forward_next(&mut self, id: ClientRequestId) {
        let read = self.reads.get_mut(&id).expect("a streaming read exists");
        let Stage::Streaming {
            parts,
            remaining,
            forwarding,
            ..
        } = &mut read.stage
        else {
            unreachable!("only a started response forwards");
        };
        if *forwarding {
            return;
        }
        let Some(&next) = parts.front() else {
            if *remaining > 0 {
                return self.abort(id);
            }
            self.reads.remove(&id);
            return;
        };
        let Some(answer) = &self.parts[&next].answer else {
            return;
        };
        let len = answer.content_length;
        *forwarding = true;
        self.actions.push(Action::Forward {
            request: id,
            from: next,
            len,
        });
    }

    /// Asks the next candidates for the rest of a part whose body ended
    /// after `copied` bytes, from the same version.
    fn resume(&mut self, id: ClientRequestId, part: Part, copied: u64) {
        let answer = part.answer.expect("a forwarded part answered");
        let rest = match part.what {
            // Only a body of the bytes asked for can be resumed.
            What::Range { .. } if answer.status != 206 => None,
            What::Range {
                key,
                etag,
                size,
                runs,
            } => {
                let first = runs[0].1 + copied;
                let runs = runs
                    .into_iter()
                    .filter(|&(_, _, last)| last >= first)
                    .map(|(placement, start, last)| (placement, start.max(first), last))
                    .collect();
                Some((key, etag, size, runs))
            }
            // The home's answer names the version and where its body
            // starts; the rest comes from the blocks' owners.
            What::Object(read, _) => {
                let Read::Object { request, .. } = read else {
                    unreachable!("a home part reads an object");
                };
                let span = match (answer.status, answer.content_range) {
                    (200, None) => Some((0, answer.content_length)),
                    (206, Some(range)) => Some((range.first, range.size)),
                    _ => None,
                };
                answer.etag.clone().zip(span).map(|(etag, (start, size))| {
                    let first = start + copied;
                    let last = start + answer.content_length - 1;
                    let runs = self.runs(&request.key, size, first, last);
                    (request.key, etag, size, runs)
                })
            }
        };
        let mut tried = part.tried;
        tried.push(part.node);
        let parts = rest.and_then(|(key, etag, size, runs)| {
            self.dispatch_runs(id, &key, &etag, size, runs, &tried)
        });
        let Some(parts) = parts else {
            return self.abort(id);
        };
        let read = self.reads.get_mut(&id).expect("a streaming read exists");
        let Stage::Streaming { parts: queue, .. } = &mut read.stage else {
            unreachable!("only a started response resumes");
        };
        for part in parts.into_iter().rev() {
            queue.push_front(part);
        }
    }

    /// Ends a started response early, once no forward is in progress.
    fn abort(&mut self, id: ClientRequestId) {
        let read = self.reads.get_mut(&id).expect("an aborted read exists");
        let Stage::Streaming {
            parts,
            forwarding,
            aborted,
            ..
        } = &mut read.stage
        else {
            unreachable!("only a started response ends early");
        };
        let in_progress = if *forwarding { parts.pop_front() } else { None };
        let rest = std::mem::take(parts);
        if let Some(part) = in_progress {
            parts.push_back(part);
            *aborted = true;
        }
        self.abandon_parts(rest);
        if in_progress.is_none() {
            self.reads.remove(&id);
            self.actions.push(Action::Abort { request: id });
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
        self.abandon_parts(others);
    }

    fn abandon_parts(&mut self, parts: impl IntoIterator<Item = NodeRequestId>) {
        for part in parts {
            if let Some(part_state) = self.parts.remove(&part)
                && part_state.answer.is_some()
            {
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
    /// An ETag a node found out of date, which the next read through the
    /// home reports.
    stale: Option<ETag>,
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

    /// Forgets the key's metadata, whose version `etag` a node found out
    /// of date.
    fn stale(&mut self, key: &ObjectKey, etag: ETag) {
        let written = self
            .entries
            .get(key)
            .map_or(Time::default(), |entry| entry.written);
        self.put(key.clone(), None, written);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.stale = Some(etag);
        }
    }

    fn take_stale(&mut self, key: &ObjectKey) -> Option<ETag> {
        self.entries.get_mut(key)?.stale.take()
    }

    fn put(&mut self, key: ObjectKey, meta: Option<CachedMeta>, written: Time) {
        let mut stale = None;
        if let Some(entry) = self.entries.remove(&key) {
            self.recency.remove(&entry.used);
            stale = entry.stale;
        }
        let used = self.next_use;
        self.next_use += 1;
        self.recency.insert(used, key.clone());
        self.entries.insert(
            key,
            Entry {
                meta,
                written,
                stale,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Freshness;
    use crate::placement::Member;
    use std::num::NonZeroU32;

    fn gateway() -> Gateway {
        let members = (0..3)
            .map(|id| Member {
                id: NodeId(id),
                weight: NonZeroU32::MIN,
            })
            .collect();
        let policy = BucketPolicy {
            freshness: Freshness::Immutable,
            admit_on_first_read: false,
        };
        let config = Config {
            layout: Layout::new(64, 1),
            default_policy: policy,
            buckets: BTreeMap::new(),
            metadata_capacity: 16,
            metadata_ttl: 1_000,
            node_timeout: 1_000,
            suspect_ttl: 100,
        };
        Gateway::new(Ring::new(1, members), config)
    }

    fn sends(actions: &[Action]) -> Vec<NodeRequestId> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Send { id, .. } => Some(*id),
                _ => None,
            })
            .collect()
    }

    /// A gateway fetches a ring whose version differs, one fetch at a time;
    /// a fetch that fails lets the next answer ask again.
    #[test]
    fn a_failed_ring_fetch_lets_the_next_answer_ask_again() {
        let mut gateway = gateway();
        let fetches = |gateway: &mut Gateway| {
            gateway
                .drain()
                .into_iter()
                .filter(|action| matches!(action, Action::FetchRing { .. }))
                .count()
        };
        gateway.on_ring_version(Time(10), NodeId(1), 7);
        assert_eq!(fetches(&mut gateway), 1);
        gateway.on_ring_version(Time(20), NodeId(2), 7);
        assert_eq!(fetches(&mut gateway), 0);
        gateway.on_ring_failed(Time(30));
        gateway.on_ring_version(Time(40), NodeId(2), 7);
        assert_eq!(fetches(&mut gateway), 1);
        gateway.on_ring_version(Time(50), NodeId(1), 1);
        assert_eq!(fetches(&mut gateway), 0);
    }

    /// A node answers the same request twice while the gateway forwards
    /// the first answer's body: the repeat changes nothing.
    #[test]
    fn a_repeated_answer_is_ignored() {
        let mut gateway = gateway();
        let key = ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        };
        let client = ClientRequestId(1);
        gateway.on_request(Time(0), client, Request::get(key));
        let home = sends(&gateway.drain())[0];
        let head = ResponseHead {
            status: 200,
            etag: Some(ETag("\"v1\"".into())),
            content_range: None,
            content_length: 256,
            headers: Vec::new(),
        };
        gateway.on_node_response(Time(1), home, head.clone(), None);
        let forward = Action::Forward {
            request: client,
            from: home,
            len: 256,
        };
        assert_eq!(
            gateway.drain(),
            vec![
                Action::Start {
                    request: client,
                    head
                },
                forward
            ]
        );
        for status in [200, 503] {
            gateway.on_node_response(Time(2), home, ResponseHead::status(status), None);
            assert_eq!(gateway.drain(), Vec::new());
        }
        gateway.on_forwarded(Time(3), home, 256);
        assert_eq!(gateway.drain(), Vec::new());
        assert!(gateway.is_idle());
    }

    /// A node answers a part with other bytes than it asked for, as a node
    /// on another layout might. The gateway reads the part from the next
    /// candidate instead of forwarding a body of the wrong length.
    #[test]
    fn a_part_answered_with_other_bytes_goes_to_the_next_candidate() {
        let mut gateway = gateway();
        let key = ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        };
        let etag = ETag("\"v1\"".into());
        gateway.on_request(Time(0), ClientRequestId(1), Request::head(key.clone()));
        let home = sends(&gateway.drain())[0];
        let meta = ObjectMeta {
            etag: etag.clone(),
            size: 256,
            headers: Vec::new(),
            age: 0,
        };
        gateway.on_node_metadata(Time(1), home, meta);
        gateway.drain();
        let request = Request {
            range: Some(crate::s3::ByteRange::Inclusive { first: 0, last: 9 }),
            ..Request::get(key)
        };
        gateway.on_request(Time(2), ClientRequestId(2), request);
        let part = sends(&gateway.drain())[0];
        let wrong = ResponseHead {
            status: 206,
            etag: Some(etag),
            content_range: Some(ContentRange {
                first: 0,
                last: 4,
                size: 256,
            }),
            content_length: 5,
            headers: Vec::new(),
        };
        gateway.on_node_response(Time(3), part, wrong, None);
        let actions = gateway.drain();
        assert_eq!(actions[0], Action::Discard { id: part });
        assert!(matches!(actions[1], Action::Send { id, .. } if id != part));
    }
}
