//! Lookups in the metadata service. Nodes look up buckets' origins, and
//! gateways clients' keys. A process keeps the service's answers for the
//! TTL each gives, shares one lookup among the requests waiting on a name,
//! and looks up a name in use again once half its TTL has passed, in the
//! background. While lookups fail, requests go on with the entry they
//! have, until its grace ends.

use crate::config::MetadataConfig;
use crate::log;
use crate::metrics::Metrics;
use crate::origin::{self, HttpClient};
use crate::sigv4;
use crate::zero_copy::workers;
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How long a lookup has to answer.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
/// How long after a failed lookup a process asks again.
const RETRY: Duration = Duration::from_secs(1);
/// The shortest TTL a process keeps an answer for.
const MIN_TTL: Duration = Duration::from_secs(1);
/// The most unknown names of one kind a process remembers at once.
const MAX_UNKNOWN: usize = 10_000;
/// The most lookups of one kind a process has under way for names it holds
/// no record of. Names come from unauthenticated requests, so the cap keeps
/// made-up ones from flooding the service; a name the process knows is
/// looked up whatever the cap.
pub const MAX_IN_FLIGHT: usize = 64;
/// The cache drops spent entries once it holds this many, and again each
/// time it doubles.
const SWEEP_FLOOR: usize = 1_024;
/// How far, in seconds, an invalidation's time may be from the process's.
const INVALIDATION_SKEW: u64 = 300;
/// The longest answer a lookup reads.
const ANSWER_LIMIT: u64 = 64 << 10;

/// What a process looks up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A bucket's origin, which nodes look up.
    Origin,
    /// A client's grants and signing keys, which gateways look up.
    Client,
}

impl Kind {
    /// Where the service answers for a name of this kind.
    fn path(self) -> &'static str {
        match self {
            Kind::Origin => "buckets",
            Kind::Client => "clients",
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Kind::Origin => "a bucket's origin",
            Kind::Client => "a client",
        }
    }
}

/// Why a request has no record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unresolved {
    /// The service doesn't know the name.
    Unknown,
    /// The service hasn't answered, and the process holds no usable entry.
    Unavailable,
    /// The process has as many lookups under way as it starts.
    Busy,
}

/// A lookup's outcome.
#[derive(Clone, Copy, Debug)]
pub enum Lookup {
    Found,
    Unknown,
    Failed,
    /// Past the cap on lookups under way, the process asked nothing.
    Refused,
}

/// What became of an invalidation.
#[derive(Debug, PartialEq, Eq)]
pub enum Invalidated {
    Dropped,
    /// Its signature or time failed.
    Refused,
    /// The process takes these records from its config.
    NoService,
}

/// Reads the service's answer for a name: the record, and how long to
/// keep it.
pub type Parse<T> = fn(&HttpClient, &[u8]) -> Result<(T, Duration), String>;

/// One kind's lookups, and the answers the process keeps.
pub struct Lookups<T> {
    kind: Kind,
    service: Service,
    cache: RefCell<Cache<T>>,
    parse: Parse<T>,
    metrics: Rc<Metrics>,
}

impl<T: Clone + 'static> Lookups<T> {
    pub fn new(
        kind: Kind,
        metadata: &MetadataConfig,
        client: HttpClient,
        parse: Parse<T>,
        metrics: Rc<Metrics>,
    ) -> Rc<Lookups<T>> {
        Rc::new(Lookups {
            kind,
            service: Service {
                client,
                url: metadata.url.trim_end_matches('/').to_string(),
                token: metadata.token.clone(),
            },
            cache: RefCell::new(Cache::new(
                Duration::from_millis(metadata.grace_ms),
                Duration::from_millis(metadata.unknown_ttl_ms),
            )),
            parse,
            metrics,
        })
    }

    /// `name`'s record, and whether its entry is past its TTL.
    pub async fn resolve(self: &Rc<Self>, name: &str) -> Result<(T, bool), Unresolved> {
        let found = self.cache.borrow_mut().find(name, Instant::now());
        match found {
            Found::Ready {
                value,
                stale,
                refresh,
            } => {
                if refresh {
                    self.look_up(name);
                }
                if let Err(Unresolved::Busy) = value {
                    self.metrics.metadata_lookup(self.kind, Lookup::Refused);
                }
                value.map(|value| (value, stale))
            }
            Found::Wait { waiting, start } => {
                if start {
                    self.look_up(name);
                }
                waiting.await.unwrap_or(Err(Unresolved::Unavailable))
            }
        }
    }

    /// Asks the service for `name` until an answer arrives that no
    /// invalidation overtook, and gives it to the requests waiting.
    fn look_up(self: &Rc<Self>, name: &str) {
        let (lookups, name) = (self.clone(), name.to_string());
        tokio::task::spawn_local(async move {
            loop {
                let asked = lookups.service.ask(lookups.kind, &name).await;
                let answer = asked.and_then(|body| match body {
                    None => Ok(Answer::Unknown),
                    Some(body) => (lookups.parse)(&lookups.service.client, &body)
                        .map(|(value, ttl)| Answer::Found(value, ttl))
                        .map_err(|error| format!("the answer is invalid: {error}")),
                });
                let counted = match &answer {
                    Ok(Answer::Found(..)) => Lookup::Found,
                    Ok(Answer::Unknown) => Lookup::Unknown,
                    Ok(Answer::Failed) | Err(_) => Lookup::Failed,
                };
                lookups.metrics.metadata_lookup(lookups.kind, counted);
                let answer = answer.unwrap_or_else(|error| {
                    log!(
                        Warn,
                        "a lookup failed",
                        of = lookups.kind.noun(),
                        name = name,
                        error = error
                    );
                    Answer::Failed
                });
                let looked = lookups
                    .cache
                    .borrow_mut()
                    .looked_up(&name, answer, Instant::now());
                if let Looked::Done {
                    waiters,
                    value,
                    stale,
                } = looked
                {
                    let value = value.map(|value| (value, stale));
                    for waiter in waiters {
                        let _ = waiter.send(value.clone());
                    }
                    return;
                }
            }
        });
    }

    /// Checks an invalidation of `name`, sent to `path` stamped `time` in
    /// Unix seconds, and drops the name's entry. `now` is the process's
    /// Unix time.
    pub fn invalidate(
        &self,
        name: &str,
        path: &str,
        time: &str,
        signature: &str,
        now: i64,
    ) -> Invalidated {
        let Ok(stamped) = time.parse::<i64>() else {
            return Invalidated::Refused;
        };
        let expected = invalidation_signature(&self.service.token, path, stamped);
        let signed = sigv4::constant_time_eq(expected.as_bytes(), signature.as_bytes());
        if !signed || now.abs_diff(stamped) > INVALIDATION_SKEW {
            return Invalidated::Refused;
        }
        self.cache.borrow_mut().invalidate(name);
        self.metrics.metadata_invalidated(self.kind);
        log!(
            Info,
            "took an invalidation",
            of = self.kind.noun(),
            name = name
        );
        Invalidated::Dropped
    }

    /// Keeps `value` for `name` as though the service had answered it.
    #[cfg(test)]
    pub fn preload(&self, name: &str, value: T, ttl: Duration) {
        let mut cache = self.cache.borrow_mut();
        let now = Instant::now();
        let Found::Wait { .. } = cache.find(name, now) else {
            panic!("{name} is already held");
        };
        let _ = cache.looked_up(name, Answer::Found(value, ttl), now);
    }
}

/// The hex HMAC-SHA256 of an invalidation's path and its time, in Unix
/// seconds, under the metadata service's token.
pub fn invalidation_signature(token: &str, path: &str, time: i64) -> String {
    let message = format!("{path}\n{time}");
    hex::encode(sigv4::hmac(token.as_bytes(), message.as_bytes()))
}

/// The metadata service, as a lookup reaches it.
#[derive(Clone)]
struct Service {
    client: HttpClient,
    url: String,
    token: String,
}

impl Service {
    /// Asks, from a worker, for the answer on `name`: its body, or `None`
    /// for a name the service doesn't know.
    async fn ask(&self, kind: Kind, name: &str) -> Result<Option<Bytes>, String> {
        let uri = format!("{}/{}/{}", self.url, kind.path(), sigv4::encode(name));
        let service = self.clone();
        let asking = workers().spawn(async move {
            tokio::time::timeout(LOOKUP_TIMEOUT, service.get(uri))
                .await
                .unwrap_or_else(|_| Err("the service took too long".to_string()))
        });
        asking.await.unwrap_or_else(|error| Err(error.to_string()))
    }

    async fn get(&self, uri: String) -> Result<Option<Bytes>, String> {
        let request = http::Request::get(uri)
            .header("authorization", format!("Bearer {}", self.token))
            .body(origin::empty())
            .map_err(|error| error.to_string())?;
        let response = self
            .client
            .request(request)
            .await
            .map_err(|error| error.to_string())?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            status => return Err(format!("the service answered {status}")),
        }
        origin::collect(response.into_body(), ANSWER_LIMIT)
            .await
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

/// What the service said of a name.
enum Answer<T> {
    /// Its record, and how long to keep it.
    Found(T, Duration),
    Unknown,
    Failed,
}

/// The service's answers, by name, and the lookups under way.
struct Cache<T> {
    entries: BTreeMap<String, Entry<T>>,
    /// Entries remembered as unknown.
    unknown: usize,
    /// Lookups under way for names the cache holds no record of.
    in_flight: usize,
    /// How many entries the cache holds before it next drops spent ones.
    sweep_at: usize,
    grace: Duration,
    unknown_ttl: Duration,
}

struct Entry<T> {
    known: Option<Known<T>>,
    /// The service doesn't know the name, until then.
    unknown_until: Option<Instant>,
    /// When a lookup last failed.
    failed: Option<Instant>,
    lookup: Option<Pending<T>>,
}

impl<T> Default for Entry<T> {
    fn default() -> Entry<T> {
        Entry {
            known: None,
            unknown_until: None,
            failed: None,
            lookup: None,
        }
    }
}

struct Known<T> {
    value: T,
    expires: Instant,
    /// When a request looks the name up again in the background.
    refresh: Instant,
}

/// A request waiting on a lookup, which it hears the record from, with
/// whether the entry is past its TTL.
type Waiter<T> = oneshot::Sender<Result<(T, bool), Unresolved>>;
type Waiting<T> = oneshot::Receiver<Result<(T, bool), Unresolved>>;

struct Pending<T> {
    waiters: Vec<Waiter<T>>,
    /// An invalidation arrived after the lookup went out, so another
    /// follows it.
    again: bool,
    /// The lookup counts toward the cap, since the cache held no record of
    /// its name.
    capped: bool,
}

impl<T> Default for Pending<T> {
    fn default() -> Pending<T> {
        Pending {
            waiters: Vec::new(),
            again: false,
            capped: false,
        }
    }
}

/// What a request finds for its name.
enum Found<T> {
    Ready {
        value: Result<T, Unresolved>,
        /// The entry is past its TTL, and lookups fail.
        stale: bool,
        /// The request starts a lookup in the background.
        refresh: bool,
    },
    Wait {
        waiting: Waiting<T>,
        /// The request starts the lookup it waits on.
        start: bool,
    },
}

/// What a lookup's answer settles.
enum Looked<T> {
    /// An invalidation overtook the lookup: ask again.
    Again,
    Done {
        waiters: Vec<Waiter<T>>,
        value: Result<T, Unresolved>,
        stale: bool,
    },
}

impl<T> Entry<T> {
    fn failed_recently(&self, now: Instant) -> bool {
        self.failed.is_some_and(|at| now < at + RETRY)
    }

    /// Whether the entry holds nothing a request could use: no record
    /// within its grace, no unknown name, no lookup and no recent failure.
    fn is_spent(&self, now: Instant, grace: Duration) -> bool {
        self.known
            .as_ref()
            .is_none_or(|known| now >= known.expires + grace)
            && self.unknown_until.is_none()
            && self.lookup.is_none()
            && !self.failed_recently(now)
    }
}

impl<T: Clone> Entry<T> {
    /// A request's answer from an entry within its TTL, which starts a
    /// lookup in the background past half the TTL.
    fn fresh(&mut self, now: Instant) -> Option<Found<T>> {
        let known = self.known.as_ref().filter(|known| now < known.expires)?;
        let refresh = now >= known.refresh && self.lookup.is_none() && !self.failed_recently(now);
        let value = Ok(known.value.clone());
        if refresh {
            self.lookup = Some(Pending::default());
        }
        Some(Found::Ready {
            value,
            stale: false,
            refresh,
        })
    }
}

impl<T: Clone> Cache<T> {
    fn new(grace: Duration, unknown_ttl: Duration) -> Cache<T> {
        Cache {
            entries: BTreeMap::new(),
            unknown: 0,
            in_flight: 0,
            sweep_at: SWEEP_FLOOR,
            grace,
            unknown_ttl,
        }
    }

    fn find(&mut self, name: &str, now: Instant) -> Found<T> {
        if let Some(entry) = self.entries.get_mut(name)
            && let Some(found) = entry.fresh(now)
        {
            return found;
        }
        if !self.entries.contains_key(name) {
            if self.entries.len() >= self.sweep_at {
                self.forget_expired(now, name);
                self.sweep_at = (2 * self.entries.len()).max(SWEEP_FLOOR);
            }
            self.entries.insert(name.to_string(), Entry::default());
        }
        let entry = self.entries.get_mut(name).expect("inserted");
        if let Some(until) = entry.unknown_until {
            if now < until {
                return Found::Ready {
                    value: Err(Unresolved::Unknown),
                    stale: false,
                    refresh: false,
                };
            }
            entry.unknown_until = None;
            self.unknown -= 1;
        }
        // Past its TTL, an entry whose last lookup failed stays in use
        // through its grace, and the process asks again in the background
        // once a second.
        if let Some(known) = &entry.known
            && entry.failed.is_some()
            && now < known.expires + self.grace
        {
            let value = Ok(known.value.clone());
            let refresh = entry.lookup.is_none() && !entry.failed_recently(now);
            if refresh {
                entry.lookup = Some(Pending::default());
            }
            return Found::Ready {
                value,
                stale: true,
                refresh,
            };
        }
        if entry.failed_recently(now) {
            return Found::Ready {
                value: Err(Unresolved::Unavailable),
                stale: false,
                refresh: false,
            };
        }
        let start = entry.lookup.is_none();
        let capped = start && entry.known.is_none();
        if capped && self.in_flight >= MAX_IN_FLIGHT {
            return Found::Ready {
                value: Err(Unresolved::Busy),
                stale: false,
                refresh: false,
            };
        }
        if capped {
            self.in_flight += 1;
        }
        let (sender, waiting) = oneshot::channel();
        let pending = entry.lookup.get_or_insert_with(|| Pending {
            capped,
            ..Pending::default()
        });
        pending.waiters.push(sender);
        Found::Wait { waiting, start }
    }

    fn looked_up(&mut self, name: &str, answer: Answer<T>, now: Instant) -> Looked<T> {
        let Some(entry) = self.entries.get_mut(name) else {
            return Looked::Done {
                waiters: Vec::new(),
                value: Err(Unresolved::Unavailable),
                stale: false,
            };
        };
        if let Some(pending) = &mut entry.lookup
            && pending.again
        {
            pending.again = false;
            return Looked::Again;
        }
        let pending = entry.lookup.take().unwrap_or_default();
        if pending.capped {
            self.in_flight -= 1;
        }
        let waiters = pending.waiters;
        let (value, stale) = match answer {
            Answer::Found(value, ttl) => {
                let ttl = ttl.max(MIN_TTL);
                entry.known = Some(Known {
                    value: value.clone(),
                    expires: now + ttl,
                    refresh: now + ttl / 2,
                });
                entry.failed = None;
                (Ok(value), false)
            }
            Answer::Unknown => {
                entry.known = None;
                entry.failed = None;
                self.remember_unknown(name, now);
                (Err(Unresolved::Unknown), false)
            }
            Answer::Failed => {
                entry.failed = Some(now);
                match &entry.known {
                    Some(known) if now < known.expires + self.grace => {
                        (Ok(known.value.clone()), now >= known.expires)
                    }
                    _ => (Err(Unresolved::Unavailable), false),
                }
            }
        };
        Looked::Done {
            waiters,
            value,
            stale,
        }
    }

    /// Remembers that the service doesn't know `name`, while fewer than
    /// `MAX_UNKNOWN` names are remembered so.
    fn remember_unknown(&mut self, name: &str, now: Instant) {
        if self.unknown >= MAX_UNKNOWN {
            self.forget_expired(now, name);
        }
        let Some(entry) = self.entries.get_mut(name) else {
            return;
        };
        if self.unknown < MAX_UNKNOWN {
            entry.unknown_until = Some(now + self.unknown_ttl);
            self.unknown += 1;
        } else if entry.is_spent(now, self.grace) {
            self.entries.remove(name);
        }
    }

    /// Drops what has expired, but `name`'s entry: unknown names past
    /// their time, and spent entries.
    fn forget_expired(&mut self, now: Instant, name: &str) {
        let (mut forgotten, grace) = (0, self.grace);
        self.entries.retain(|held, entry| {
            if entry.unknown_until.is_some_and(|until| now >= until) {
                entry.unknown_until = None;
                forgotten += 1;
            }
            held == name || !entry.is_spent(now, grace)
        });
        self.unknown -= forgotten;
    }

    fn invalidate(&mut self, name: &str) {
        let Some(entry) = self.entries.get_mut(name) else {
            return;
        };
        entry.known = None;
        entry.failed = None;
        if entry.unknown_until.take().is_some() {
            self.unknown -= 1;
        }
        match &mut entry.lookup {
            Some(pending) => pending.again = true,
            None => {
                self.entries.remove(name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);
    const GRACE: Duration = Duration::from_secs(600);
    const UNKNOWN_TTL: Duration = Duration::from_secs(10);

    fn cache() -> Cache<u32> {
        Cache::new(GRACE, UNKNOWN_TTL)
    }

    /// A request that finds its answer at once.
    fn ready(found: Found<u32>) -> (Result<u32, Unresolved>, bool, bool) {
        match found {
            Found::Ready {
                value,
                stale,
                refresh,
            } => (value, stale, refresh),
            Found::Wait { .. } => panic!("the request waits"),
        }
    }

    /// A request that waits, and whether it starts the lookup.
    fn waits(found: Found<u32>) -> (Waiting<u32>, bool) {
        match found {
            Found::Wait { waiting, start } => (waiting, start),
            Found::Ready { .. } => panic!("the request goes on"),
        }
    }

    fn done(looked: Looked<u32>) -> (Vec<Waiter<u32>>, Result<u32, Unresolved>, bool) {
        match looked {
            Looked::Done {
                waiters,
                value,
                stale,
            } => (waiters, value, stale),
            Looked::Again => panic!("the lookup goes again"),
        }
    }

    /// Looks `bucket` up at `now` with `answer`, which the cache keeps.
    fn learn(cache: &mut Cache<u32>, bucket: &str, answer: Answer<u32>, now: Instant) {
        let (_, start) = waits(cache.find(bucket, now));
        assert!(start);
        let _ = done(cache.looked_up(bucket, answer, now));
    }

    #[test]
    fn requests_waiting_on_a_bucket_share_one_lookup() {
        let (mut cache, now) = (cache(), Instant::now());
        let (mut first, start) = waits(cache.find("b", now));
        assert!(start);
        let (mut second, start) = waits(cache.find("b", now));
        assert!(!start);
        let (waiters, origin, _) = done(cache.looked_up("b", Answer::Found(7, TTL), now));
        assert_eq!(waiters.len(), 2);
        assert_eq!(origin, Ok(7));
        for waiter in waiters {
            waiter.send(origin.map(|value| (value, false))).unwrap();
        }
        assert_eq!(first.try_recv().unwrap(), Ok((7, false)));
        assert_eq!(second.try_recv().unwrap(), Ok((7, false)));
        assert_eq!(ready(cache.find("b", now)), (Ok(7), false, false));
    }

    #[test]
    fn an_entry_past_half_its_ttl_is_looked_up_once_in_the_background() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let half = now + TTL / 2;
        assert_eq!(ready(cache.find("b", half)), (Ok(7), false, true));
        assert_eq!(ready(cache.find("b", half)), (Ok(7), false, false));
        let (waiters, _, _) = done(cache.looked_up("b", Answer::Found(8, TTL), half));
        assert!(waiters.is_empty());
        assert_eq!(ready(cache.find("b", half)), (Ok(8), false, false));
    }

    #[test]
    fn an_entry_past_its_ttl_waits_for_a_lookup() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let (_, start) = waits(cache.find("b", now + TTL));
        assert!(start);
    }

    #[test]
    fn a_failing_service_leaves_an_entry_in_use_until_its_grace_ends() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let expired = now + TTL;
        let (_, start) = waits(cache.find("b", expired));
        assert!(start);
        let (waiters, origin, stale) = done(cache.looked_up("b", Answer::Failed, expired));
        assert_eq!((waiters.len(), origin, stale), (1, Ok(7), true));
        // Within a second of the failure, requests go on with the entry.
        assert_eq!(ready(cache.find("b", expired)), (Ok(7), true, false));
        // Then they go on with it while one lookup asks again.
        let later = expired + RETRY;
        assert_eq!(ready(cache.find("b", later)), (Ok(7), true, true));
        assert_eq!(ready(cache.find("b", later)), (Ok(7), true, false));
        let (waiters, origin, _) = done(cache.looked_up("b", Answer::Failed, later));
        assert_eq!((waiters.len(), origin), (0, Ok(7)));
        // Past its grace, the entry is gone, and the next request waits.
        let past = now + TTL + GRACE;
        let (_, start) = waits(cache.find("b", past));
        assert!(start);
        let (_, origin, _) = done(cache.looked_up("b", Answer::Failed, past));
        assert_eq!(origin, Err(Unresolved::Unavailable));
        assert_eq!(
            ready(cache.find("b", past)),
            (Err(Unresolved::Unavailable), false, false)
        );
    }

    #[test]
    fn a_failed_refresh_keeps_the_entry_and_waits_a_second_to_ask_again() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let half = now + TTL / 2;
        assert_eq!(ready(cache.find("b", half)), (Ok(7), false, true));
        let (_, origin, stale) = done(cache.looked_up("b", Answer::Failed, half));
        assert_eq!((origin, stale), (Ok(7), false));
        assert_eq!(ready(cache.find("b", half)), (Ok(7), false, false));
        assert_eq!(ready(cache.find("b", half + RETRY)), (Ok(7), false, true));
    }

    #[test]
    fn an_unknown_bucket_is_remembered_for_its_ttl() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Unknown, now);
        assert_eq!(
            ready(cache.find("b", now)),
            (Err(Unresolved::Unknown), false, false)
        );
        let (_, start) = waits(cache.find("b", now + UNKNOWN_TTL));
        assert!(start);
        assert_eq!(cache.unknown, 0);
    }

    #[test]
    fn at_most_so_many_unknown_buckets_are_remembered() {
        let (mut cache, now) = (cache(), Instant::now());
        for index in 0..MAX_UNKNOWN + 1 {
            learn(&mut cache, &index.to_string(), Answer::Unknown, now);
        }
        assert_eq!(cache.unknown, MAX_UNKNOWN);
        assert_eq!(cache.entries.len(), MAX_UNKNOWN);
        // Once the first have expired, their places go to new ones.
        learn(&mut cache, "new", Answer::Unknown, now + UNKNOWN_TTL);
        assert_eq!(cache.unknown, 1);
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn a_lookup_that_answers_again_ends_the_grace() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let expired = now + TTL;
        waits(cache.find("b", expired));
        let _ = done(cache.looked_up("b", Answer::Failed, expired));
        let later = expired + RETRY;
        assert_eq!(ready(cache.find("b", later)), (Ok(7), true, true));
        let _ = done(cache.looked_up("b", Answer::Found(8, TTL), later));
        assert_eq!(ready(cache.find("b", later)), (Ok(8), false, false));
    }

    #[test]
    fn spent_entries_are_dropped_as_the_cache_grows() {
        let (mut cache, now) = (cache(), Instant::now());
        for index in 0..SWEEP_FLOOR {
            learn(
                &mut cache,
                &format!("known-{index}"),
                Answer::Found(7, TTL),
                now,
            );
        }
        // Failed lookups leave entries too. The first finds nothing spent
        // to sweep, so the cache next sweeps at twice its size.
        for index in 0..SWEEP_FLOOR {
            let bucket = format!("failed-{index}");
            waits(cache.find(&bucket, now));
            let _ = done(cache.looked_up(&bucket, Answer::Failed, now));
        }
        assert_eq!(cache.entries.len(), 2 * SWEEP_FLOOR);
        assert_eq!(cache.sweep_at, 2 * SWEEP_FLOOR);
        // Past the entries' grace, and a second past the failures, a new
        // bucket sweeps every spent entry out.
        waits(cache.find("new", now + TTL + GRACE));
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.sweep_at, SWEEP_FLOOR);
    }

    #[test]
    fn a_failed_lookup_without_an_entry_answers_unavailable_for_a_second() {
        let (mut cache, now) = (cache(), Instant::now());
        waits(cache.find("b", now));
        let (_, origin, _) = done(cache.looked_up("b", Answer::Failed, now));
        assert_eq!(origin, Err(Unresolved::Unavailable));
        assert_eq!(
            ready(cache.find("b", now)),
            (Err(Unresolved::Unavailable), false, false)
        );
        let (_, start) = waits(cache.find("b", now + RETRY));
        assert!(start);
    }

    #[test]
    fn an_invalidation_drops_the_entry() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        cache.invalidate("b");
        assert!(cache.entries.is_empty());
        let (_, start) = waits(cache.find("b", now));
        assert!(start);
    }

    #[test]
    fn an_invalidation_forgets_an_unknown_bucket_and_a_failure() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Unknown, now);
        cache.invalidate("b");
        assert_eq!(cache.unknown, 0);
        waits(cache.find("b", now));
        let _ = done(cache.looked_up("b", Answer::Failed, now));
        cache.invalidate("b");
        let (_, start) = waits(cache.find("b", now));
        assert!(start);
    }

    #[test]
    fn a_lookup_an_invalidation_overtakes_is_followed_by_another() {
        let (mut cache, now) = (cache(), Instant::now());
        let (mut waiting, _) = waits(cache.find("b", now));
        cache.invalidate("b");
        assert!(matches!(
            cache.looked_up("b", Answer::Found(7, TTL), now),
            Looked::Again
        ));
        assert!(waiting.try_recv().is_err());
        let (waiters, origin, _) = done(cache.looked_up("b", Answer::Found(8, TTL), now));
        assert_eq!((waiters.len(), origin), (1, Ok(8)));
    }

    #[test]
    fn an_invalidation_during_a_refresh_leaves_requests_waiting_for_the_next_lookup() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, TTL), now);
        let half = now + TTL / 2;
        assert_eq!(ready(cache.find("b", half)), (Ok(7), false, true));
        cache.invalidate("b");
        let (_, start) = waits(cache.find("b", half));
        assert!(!start);
        assert!(matches!(
            cache.looked_up("b", Answer::Found(7, TTL), half),
            Looked::Again
        ));
        let (waiters, origin, _) = done(cache.looked_up("b", Answer::Found(8, TTL), half));
        assert_eq!((waiters.len(), origin), (1, Ok(8)));
    }

    #[test]
    fn a_ttl_under_a_second_counts_as_a_second() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "b", Answer::Found(7, Duration::ZERO), now);
        assert_eq!(ready(cache.find("b", now)), (Ok(7), false, false));
        waits(cache.find("b", now + MIN_TTL));
    }

    #[test]
    fn an_invalidation_signs_its_path_and_time() {
        let token = "token";
        let signature = invalidation_signature(token, "/origins/b/invalidate", 1_000);
        assert_eq!(signature.len(), 64);
        assert_ne!(
            signature,
            invalidation_signature(token, "/clients/b/invalidate", 1_000)
        );
        assert_ne!(
            signature,
            invalidation_signature(token, "/origins/b/invalidate", 1_001)
        );
        assert_ne!(
            signature,
            invalidation_signature("other", "/origins/b/invalidate", 1_000)
        );
    }

    #[test]
    fn lookups_of_unknown_names_stop_at_the_cap() {
        let (mut cache, now) = (cache(), Instant::now());
        learn(&mut cache, "known", Answer::Found(7, TTL), now);
        learn(&mut cache, "expired", Answer::Found(8, TTL), now);
        for index in 0..MAX_IN_FLIGHT {
            let (_, start) = waits(cache.find(&index.to_string(), now));
            assert!(start);
        }
        assert_eq!(
            ready(cache.find("one-more", now)),
            (Err(Unresolved::Busy), false, false)
        );
        // Requests joining a lookup under way still wait on it.
        let (_, start) = waits(cache.find("0", now));
        assert!(!start);
        // Names the cache knows are looked up whatever the cap.
        let half = now + TTL / 2;
        assert_eq!(ready(cache.find("known", half)), (Ok(7), false, true));
        let (_, start) = waits(cache.find("expired", now + TTL));
        assert!(start);
        let _ = done(cache.looked_up("known", Answer::Found(7, TTL), half));
        assert_eq!(cache.in_flight, MAX_IN_FLIGHT);
        // Once a lookup of an unknown name ends, another may start.
        let _ = done(cache.looked_up("0", Answer::Unknown, now));
        let (_, start) = waits(cache.find("one-more", now));
        assert!(start);
    }
}
