//! Each bucket's origin, from the config or from the metadata service. A
//! node keeps the service's answers for the TTL each gives, shares one
//! lookup among the requests waiting on a bucket, and looks up a bucket in
//! use again once half its TTL has passed, in the background. While
//! lookups fail, requests go on with the entry they have, until its grace
//! ends.

use crate::config::{Config, OriginConfig};
use crate::log;
use crate::metrics::Metrics;
use crate::origin::{self, HttpClient, Origin};
use crate::sigv4::{self, Credentials};
use crate::zero_copy::workers;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How long a lookup has to answer.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
/// How long after a failed lookup a node asks again.
const RETRY: Duration = Duration::from_secs(1);
/// The shortest TTL a node keeps an answer for.
const MIN_TTL: Duration = Duration::from_secs(1);
/// The most unknown buckets a node remembers at once.
const MAX_UNKNOWN: usize = 10_000;
/// The cache drops spent entries once it holds this many, and again each
/// time it doubles.
const SWEEP_FLOOR: usize = 1_024;
/// How far, in seconds, an invalidation's time may be from the node's.
const INVALIDATION_SKEW: u64 = 300;
/// The longest answer a lookup reads.
const ANSWER_LIMIT: u64 = 64 << 10;

/// Why a request has no origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unresolved {
    /// No origin serves the bucket: 404.
    Unknown,
    /// The service hasn't answered, and the node holds no usable entry: 503.
    Unavailable,
}

/// A lookup's outcome.
#[derive(Clone, Copy, Debug)]
pub enum Lookup {
    Found,
    Unknown,
    Failed,
}

pub struct Origins {
    /// The config's origins of single buckets.
    buckets: BTreeMap<String, Arc<Origin>>,
    /// The config's origin of every other bucket.
    default: Option<Arc<Origin>>,
    service: Option<Service>,
    cache: RefCell<Cache<Arc<Origin>>>,
    metrics: Rc<Metrics>,
}

impl Origins {
    pub fn new(config: &Config, metrics: Rc<Metrics>) -> Origins {
        let client = origin::client();
        let open = |origin: &OriginConfig| {
            let credentials = Credentials {
                access_key_id: origin.access_key_id.clone(),
                secret_access_key: origin.secret_access_key.clone(),
            };
            Arc::new(Origin::new(
                client.clone(),
                &origin.endpoint,
                &origin.region,
                credentials,
            ))
        };
        let (grace, unknown_ttl) = config.metadata.as_ref().map_or((0, 0), |metadata| {
            (metadata.grace_ms, metadata.unknown_ttl_ms)
        });
        Origins {
            buckets: config
                .origins
                .iter()
                .map(|(bucket, origin)| (bucket.clone(), open(origin)))
                .collect(),
            default: config.origin.as_ref().map(open),
            service: config.metadata.as_ref().map(|metadata| Service {
                client: client.clone(),
                url: metadata.url.trim_end_matches('/').to_string(),
                token: metadata.token.clone(),
            }),
            cache: RefCell::new(Cache::new(
                Duration::from_millis(grace),
                Duration::from_millis(unknown_ttl),
            )),
            metrics,
        }
    }

    /// The origin of requests naming no bucket.
    pub fn default(&self) -> Option<Arc<Origin>> {
        self.default.clone()
    }

    /// The origin a request to `bucket` goes to.
    pub async fn of(self: &Rc<Self>, bucket: &str) -> Result<Arc<Origin>, Unresolved> {
        match self.resolve(bucket).await {
            Ok((origin, stale)) => {
                if stale {
                    self.metrics.origin_stale(1);
                }
                Ok(origin)
            }
            Err(reason) => {
                self.metrics.origin_unresolved(reason);
                Err(reason)
            }
        }
    }

    /// Whether an origin serves `bucket`, which a read of its cached
    /// objects asks before it goes on. A bucket whose origin is unavailable
    /// still serves what the cache holds.
    pub async fn serves(self: &Rc<Self>, bucket: &str) -> bool {
        let unknown = self.resolve(bucket).await.err() == Some(Unresolved::Unknown);
        if unknown {
            self.metrics.origin_unresolved(Unresolved::Unknown);
        }
        !unknown
    }

    /// `bucket`'s origin, and whether its entry is past its TTL.
    async fn resolve(self: &Rc<Self>, bucket: &str) -> Result<(Arc<Origin>, bool), Unresolved> {
        let Some(_) = &self.service else {
            let origin = self.buckets.get(bucket).or(self.default.as_ref());
            return origin
                .map(|origin| (origin.clone(), false))
                .ok_or(Unresolved::Unknown);
        };
        let found = self.cache.borrow_mut().find(bucket, Instant::now());
        match found {
            Found::Ready {
                origin,
                stale,
                refresh,
            } => {
                if refresh {
                    self.look_up(bucket);
                }
                origin.map(|origin| (origin, stale))
            }
            Found::Wait { waiting, start } => {
                if start {
                    self.look_up(bucket);
                }
                waiting.await.unwrap_or(Err(Unresolved::Unavailable))
            }
        }
    }

    /// Asks the service for `bucket`'s origin until an answer arrives that
    /// no invalidation overtook, and gives it to the requests waiting.
    fn look_up(self: &Rc<Self>, bucket: &str) {
        let Some(service) = self.service.clone() else {
            return;
        };
        let (origins, bucket) = (self.clone(), bucket.to_string());
        tokio::task::spawn_local(async move {
            loop {
                let answer = service.look_up(&bucket).await;
                let counted = match &answer {
                    Ok(Answer::Found(..)) => Lookup::Found,
                    Ok(Answer::Unknown) => Lookup::Unknown,
                    Ok(Answer::Failed) | Err(_) => Lookup::Failed,
                };
                origins.metrics.origin_lookup(counted);
                let answer = answer.unwrap_or_else(|error| {
                    log!(
                        Warn,
                        "a lookup of a bucket's origin failed",
                        bucket = bucket,
                        error = error
                    );
                    Answer::Failed
                });
                let looked = origins
                    .cache
                    .borrow_mut()
                    .looked_up(&bucket, answer, Instant::now());
                if let Looked::Done {
                    waiters,
                    origin,
                    stale,
                } = looked
                {
                    let origin = origin.map(|origin| (origin, stale));
                    for waiter in waiters {
                        let _ = waiter.send(origin.clone());
                    }
                    return;
                }
            }
        });
    }

    /// Checks an invalidation of `bucket` stamped `time`, in Unix seconds,
    /// and drops the bucket's entry. `now` is the node's Unix time.
    pub fn invalidate(&self, bucket: &str, time: &str, signature: &str, now: i64) -> Invalidated {
        let Some(service) = &self.service else {
            return Invalidated::NoService;
        };
        let Ok(stamped) = time.parse::<i64>() else {
            return Invalidated::Refused;
        };
        let expected = invalidation_signature(&service.token, bucket, stamped);
        let signed = sigv4::constant_time_eq(expected.as_bytes(), signature.as_bytes());
        if !signed || now.abs_diff(stamped) > INVALIDATION_SKEW {
            return Invalidated::Refused;
        }
        self.cache.borrow_mut().invalidate(bucket);
        self.metrics.origin_invalidated();
        log!(Info, "invalidated a bucket's origin", bucket = bucket);
        Invalidated::Dropped
    }
}

/// What became of an invalidation.
#[derive(Debug, PartialEq, Eq)]
pub enum Invalidated {
    Dropped,
    /// Its signature or time failed.
    Refused,
    /// The node takes origins from its config.
    NoService,
}

/// The hex HMAC-SHA256 of an invalidation of `bucket` at `time`, in Unix
/// seconds, under the metadata service's token.
pub fn invalidation_signature(token: &str, bucket: &str, time: i64) -> String {
    let message = format!("invalidate\n{bucket}\n{time}");
    hex::encode(sigv4::hmac(token.as_bytes(), message.as_bytes()))
}

/// The metadata service, as a lookup reaches it.
#[derive(Clone)]
struct Service {
    client: HttpClient,
    url: String,
    token: String,
}

/// The service's answer for a bucket it serves.
#[derive(Deserialize)]
struct Served {
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    ttl_ms: u64,
}

impl Service {
    /// Asks, from a worker, for `bucket`'s origin.
    async fn look_up(&self, bucket: &str) -> Result<Answer<Arc<Origin>>, String> {
        let (service, bucket) = (self.clone(), bucket.to_string());
        let asking = workers().spawn(async move {
            tokio::time::timeout(LOOKUP_TIMEOUT, service.ask(&bucket))
                .await
                .unwrap_or_else(|_| Err("the service took too long".to_string()))
        });
        asking.await.unwrap_or_else(|error| Err(error.to_string()))
    }

    async fn ask(&self, bucket: &str) -> Result<Answer<Arc<Origin>>, String> {
        let uri = format!("{}/buckets/{}", self.url, sigv4::encode(bucket));
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
            404 => return Ok(Answer::Unknown),
            status => return Err(format!("the service answered {status}")),
        }
        let body = origin::collect(response.into_body(), ANSWER_LIMIT)
            .await
            .map_err(|error| error.to_string())?;
        let served: Served = serde_json::from_slice(&body)
            .map_err(|error| format!("the answer is invalid: {error}"))?;
        let web = served.endpoint.starts_with("http://") || served.endpoint.starts_with("https://");
        if !web || served.region.is_empty() {
            return Err("the answer names no endpoint or region".to_string());
        }
        let credentials = Credentials {
            access_key_id: served.access_key_id,
            secret_access_key: served.secret_access_key,
        };
        let origin = Origin::new(
            self.client.clone(),
            &served.endpoint,
            &served.region,
            credentials,
        );
        Ok(Answer::Found(
            Arc::new(origin),
            Duration::from_millis(served.ttl_ms),
        ))
    }
}

/// What the service said of a bucket.
enum Answer<T> {
    /// Its origin, and how long to keep it.
    Found(T, Duration),
    Unknown,
    Failed,
}

/// The service's answers, by bucket, and the lookups under way.
struct Cache<T> {
    entries: BTreeMap<String, Entry<T>>,
    /// Entries remembered as unknown.
    unknown: usize,
    /// How many entries the cache holds before it next drops spent ones.
    sweep_at: usize,
    grace: Duration,
    unknown_ttl: Duration,
}

struct Entry<T> {
    known: Option<Known<T>>,
    /// The service doesn't serve the bucket, until then.
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
    /// When a request looks the bucket up again in the background.
    refresh: Instant,
}

/// A request waiting on a lookup, which it hears the origin from, with
/// whether the entry is past its TTL.
type Waiter<T> = oneshot::Sender<Result<(T, bool), Unresolved>>;
type Waiting<T> = oneshot::Receiver<Result<(T, bool), Unresolved>>;

struct Pending<T> {
    waiters: Vec<Waiter<T>>,
    /// An invalidation arrived after the lookup went out, so another
    /// follows it.
    again: bool,
}

impl<T> Default for Pending<T> {
    fn default() -> Pending<T> {
        Pending {
            waiters: Vec::new(),
            again: false,
        }
    }
}

/// What a request finds for its bucket.
enum Found<T> {
    Ready {
        origin: Result<T, Unresolved>,
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
        origin: Result<T, Unresolved>,
        stale: bool,
    },
}

impl<T> Entry<T> {
    fn failed_recently(&self, now: Instant) -> bool {
        self.failed.is_some_and(|at| now < at + RETRY)
    }

    /// Whether the entry holds nothing a request could use: no entry
    /// within its grace, no unknown bucket, no lookup and no recent
    /// failure.
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
    /// A request's answer from an entry within its TTL.
    fn fresh(&mut self, now: Instant) -> Option<Found<T>> {
        let known = self.known.as_ref().filter(|known| now < known.expires)?;
        let refresh = now >= known.refresh && self.lookup.is_none() && !self.failed_recently(now);
        let origin = Ok(known.value.clone());
        if refresh {
            self.lookup = Some(Pending::default());
        }
        Some(Found::Ready {
            origin,
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
            sweep_at: SWEEP_FLOOR,
            grace,
            unknown_ttl,
        }
    }

    fn find(&mut self, bucket: &str, now: Instant) -> Found<T> {
        if let Some(entry) = self.entries.get_mut(bucket)
            && let Some(found) = entry.fresh(now)
        {
            return found;
        }
        if !self.entries.contains_key(bucket) {
            if self.entries.len() >= self.sweep_at {
                self.forget_expired(now, bucket);
                self.sweep_at = (2 * self.entries.len()).max(SWEEP_FLOOR);
            }
            self.entries.insert(bucket.to_string(), Entry::default());
        }
        let entry = self.entries.get_mut(bucket).expect("inserted");
        if let Some(until) = entry.unknown_until {
            if now < until {
                return Found::Ready {
                    origin: Err(Unresolved::Unknown),
                    stale: false,
                    refresh: false,
                };
            }
            entry.unknown_until = None;
            self.unknown -= 1;
        }
        // Past its TTL, an entry whose last lookup failed stays in use
        // through its grace, and the node asks again in the background once
        // a second.
        if let Some(known) = &entry.known
            && entry.failed.is_some()
            && now < known.expires + self.grace
        {
            let origin = Ok(known.value.clone());
            let refresh = entry.lookup.is_none() && !entry.failed_recently(now);
            if refresh {
                entry.lookup = Some(Pending::default());
            }
            return Found::Ready {
                origin,
                stale: true,
                refresh,
            };
        }
        if entry.failed_recently(now) {
            return Found::Ready {
                origin: Err(Unresolved::Unavailable),
                stale: false,
                refresh: false,
            };
        }
        let start = entry.lookup.is_none();
        let (sender, waiting) = oneshot::channel();
        entry
            .lookup
            .get_or_insert_with(Pending::default)
            .waiters
            .push(sender);
        Found::Wait { waiting, start }
    }

    fn looked_up(&mut self, bucket: &str, answer: Answer<T>, now: Instant) -> Looked<T> {
        let Some(entry) = self.entries.get_mut(bucket) else {
            return Looked::Done {
                waiters: Vec::new(),
                origin: Err(Unresolved::Unavailable),
                stale: false,
            };
        };
        if let Some(pending) = &mut entry.lookup
            && pending.again
        {
            pending.again = false;
            return Looked::Again;
        }
        let waiters = entry
            .lookup
            .take()
            .map(|pending| pending.waiters)
            .unwrap_or_default();
        let (origin, stale) = match answer {
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
                self.remember_unknown(bucket, now);
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
            origin,
            stale,
        }
    }

    /// Remembers that the service doesn't serve `bucket`, while fewer than
    /// `MAX_UNKNOWN` buckets are remembered so.
    fn remember_unknown(&mut self, bucket: &str, now: Instant) {
        if self.unknown >= MAX_UNKNOWN {
            self.forget_expired(now, bucket);
        }
        let Some(entry) = self.entries.get_mut(bucket) else {
            return;
        };
        if self.unknown < MAX_UNKNOWN {
            entry.unknown_until = Some(now + self.unknown_ttl);
            self.unknown += 1;
        } else if entry.is_spent(now, self.grace) {
            self.entries.remove(bucket);
        }
    }

    /// Drops what has expired, but `bucket`'s entry: unknown buckets past
    /// their time, and spent entries.
    fn forget_expired(&mut self, now: Instant, bucket: &str) {
        let (mut forgotten, grace) = (0, self.grace);
        self.entries.retain(|name, entry| {
            if entry.unknown_until.is_some_and(|until| now >= until) {
                entry.unknown_until = None;
                forgotten += 1;
            }
            name == bucket || !entry.is_spent(now, grace)
        });
        self.unknown -= forgotten;
    }

    fn invalidate(&mut self, bucket: &str) {
        let Some(entry) = self.entries.get_mut(bucket) else {
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
                self.entries.remove(bucket);
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
                origin,
                stale,
                refresh,
            } => (origin, stale, refresh),
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
                origin,
                stale,
            } => (waiters, origin, stale),
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
            waiter.send(origin.map(|origin| (origin, false))).unwrap();
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

    /// What `Origins::of` costs a request whose bucket's entry is fresh,
    /// from the config and from the service: `cargo test --release -p
    /// s3-accelerator --lib cost_of -- --ignored --nocapture`.
    #[test]
    #[ignore = "a measurement, not a check"]
    fn cost_of_finding_an_origin() {
        let config = |origins: &str| -> Config {
            toml::from_str(&format!(
                "{origins}\n[cluster]\nsecret = \"s\"\nnodes = [{{ id = 0, address = \"127.0.0.1:1\" }}]\n"
            ))
            .unwrap()
        };
        let from_config = config(
            "[origin]\nendpoint = \"http://127.0.0.1:1\"\nregion = \"r\"\n\
             access_key_id = \"k\"\nsecret_access_key = \"s\"",
        );
        let from_service = config("[metadata]\nurl = \"http://127.0.0.1:1\"\ntoken = \"t\"");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for (name, config) in [("config", from_config), ("service", from_service)] {
            let origins = Rc::new(Origins::new(&config, Rc::new(Metrics::default())));
            let origin = Arc::new(Origin::new(
                origin::client(),
                "http://127.0.0.1:1",
                "r",
                Credentials {
                    access_key_id: "k".into(),
                    secret_access_key: "s".into(),
                },
            ));
            // A hundred buckets, like a tenant's, each fresh for an hour.
            let now = Instant::now();
            for index in 0..100 {
                let bucket = format!("bucket-{index}");
                let mut cache = origins.cache.borrow_mut();
                let Found::Wait { .. } = cache.find(&bucket, now) else {
                    panic!("an empty cache");
                };
                let answer = Answer::Found(origin.clone(), Duration::from_secs(3600));
                let _ = cache.looked_up(&bucket, answer, now);
            }
            let rounds = 10_000_000;
            let started = Instant::now();
            runtime.block_on(async {
                for round in 0..rounds {
                    let bucket = ["bucket-7", "bucket-42", "bucket-93"][round % 3];
                    std::hint::black_box(origins.of(bucket).await.unwrap());
                }
            });
            let each = started.elapsed().as_nanos() as f64 / rounds as f64;
            println!("from the {name}: {each:.1} ns a request");
        }
    }

    #[test]
    fn an_invalidation_needs_its_signature_and_a_recent_time() {
        let token = "token";
        let signature = invalidation_signature(token, "b", 1_000);
        assert_eq!(signature.len(), 64);
        assert_ne!(signature, invalidation_signature(token, "c", 1_000));
        assert_ne!(signature, invalidation_signature(token, "b", 1_001));
        assert_ne!(signature, invalidation_signature("other", "b", 1_000));
    }
}
