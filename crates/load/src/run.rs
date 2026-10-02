//! Runs one step of a plan from one client host: connections that each
//! send the step's mix of requests, one at a time, until the step's time or
//! its share of requests runs out. Each host writes its own results, which
//! `report` merges.

use crate::client::{Answer, Client, Endpoint, Failure, Range};
use crate::dataset::{Object, mix};
use crate::histogram::Histogram;
use crate::plan::{Keys, Plan, RangeSpec, SizeSpec, Step, Target, Verify};
use rustls::ClientConfig;
use s3_accelerator::sigv4::Signer;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Everything a step's connections share.
pub struct Setup {
    pub plan: Arc<Plan>,
    pub step: Step,
    pub bucket: String,
    pub endpoints: Vec<Arc<Endpoint>>,
    pub tls: Option<Arc<ClientConfig>>,
    pub signer: Arc<Signer>,
    /// This host's place among the client hosts running the step.
    pub host: usize,
    pub hosts: usize,
    /// When every host starts, in Unix milliseconds, so they start together.
    pub start_at: Option<u64>,
    /// Names the keys this run writes.
    pub run: String,
}

/// One host's results for one step.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostResult {
    pub step: String,
    pub target: Target,
    pub host: usize,
    pub hosts: usize,
    pub connections: usize,
    pub rate: f64,
    /// When the measured part of the step began, in Unix milliseconds, and
    /// how long it lasted.
    pub started_unix_ms: u64,
    pub seconds: f64,
    pub classes: Vec<ClassResult>,
    /// Each second of the measured part.
    pub timeline: Vec<Second>,
}

/// The requests of one kind: a read of one set and range, or writes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassResult {
    pub label: String,
    /// Requests answered, by status, and requests that got no answer or a
    /// wrong one, by why.
    pub statuses: BTreeMap<u16, u64>,
    pub failures: BTreeMap<Failure, u64>,
    /// Body bytes of successful answers.
    pub bytes: u64,
    /// Microseconds from when each successful request was due to its
    /// answer's first byte, and to its last.
    pub first_byte: Histogram,
    pub total: Histogram,
}

impl ClassResult {
    fn new(label: String) -> ClassResult {
        ClassResult {
            label,
            statuses: BTreeMap::new(),
            failures: BTreeMap::new(),
            bytes: 0,
            first_byte: Histogram::fine(),
            total: Histogram::fine(),
        }
    }

    pub fn merge(&mut self, other: &ClassResult) {
        for (&status, &count) in &other.statuses {
            *self.statuses.entry(status).or_default() += count;
        }
        for (&failure, &count) in &other.failures {
            *self.failures.entry(failure).or_default() += count;
        }
        self.bytes += other.bytes;
        self.first_byte.merge(&other.first_byte);
        self.total.merge(&other.total);
    }

    pub fn successes(&self) -> u64 {
        self.statuses
            .iter()
            .filter(|(status, _)| (200..300).contains(*status))
            .map(|(_, count)| count)
            .sum()
    }

    /// Answers with any other status, and requests that got none.
    pub fn errors(&self) -> u64 {
        let answered: u64 = self.statuses.values().sum();
        answered - self.successes() + self.failures.values().sum::<u64>()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Second {
    pub requests: u64,
    pub bytes: u64,
    /// Answers other than 2xx, and requests that got none.
    pub errors: u64,
    pub first_byte: Histogram,
}

impl Second {
    pub fn merge(&mut self, other: &Second) {
        self.requests += other.requests;
        self.bytes += other.bytes;
        self.errors += other.errors;
        self.first_byte.merge(&other.first_byte);
    }
}

/// A kind of request in the step's mix.
enum Kind {
    Read {
        set: usize,
        keys: usize,
        draw: Draw,
        range: RangeSpec,
        /// The next key of a sequential read, on this host.
        cursor: AtomicU64,
    },
    Write {
        size: SizeSpec,
    },
}

enum Draw {
    Uniform,
    /// Each window of keys, this many keys long, this many times.
    Sequential {
        window: u64,
        passes: u64,
    },
    /// Ranks by Zipf's law, scattered across the keys by a multiplier
    /// prime to their count.
    Zipf(Zipf, u64),
}

struct Shared {
    setup: Setup,
    kinds: Vec<Kind>,
    /// Cumulative weights of `kinds`.
    weights: Vec<f64>,
    start: Instant,
    measure_from: Instant,
    end: Option<Instant>,
    limit: Option<u64>,
    /// Requests this host has started, and writes, which name their keys.
    issued: AtomicU64,
    written: AtomicU64,
}

pub async fn run(setup: Setup) -> HostResult {
    let step = setup.step.clone();
    let mut kinds = Vec::new();
    let mut weights = Vec::new();
    let mut labels = Vec::new();
    let mut total = 0.0;
    for read in &step.reads {
        let set = setup
            .plan
            .dataset
            .sets
            .iter()
            .position(|set| set.name == read.set)
            .expect("a checked plan names only its sets");
        let keys = read
            .limit
            .unwrap_or(u64::MAX)
            .min(setup.plan.dataset.sets[set].count);
        let draw = match read.keys {
            Keys::Uniform => Draw::Uniform,
            Keys::Sequential => Draw::Sequential {
                window: read.window,
                passes: read.passes,
            },
            Keys::Zipf(exponent) => Draw::Zipf(Zipf::new(keys, exponent), coprime(keys)),
        };
        kinds.push(Kind::Read {
            set,
            keys: keys as usize,
            draw,
            range: read.range,
            cursor: AtomicU64::new(0),
        });
        total += read.weight;
        weights.push(total);
        labels.push(read.label());
    }
    for write in &step.writes {
        kinds.push(Kind::Write { size: write.size });
        total += write.weight;
        weights.push(total);
        labels.push("writes".to_string());
    }
    let start = match setup.start_at {
        Some(at) => {
            let now = unix_ms();
            Instant::now() + Duration::from_millis(at.saturating_sub(now))
        }
        None => Instant::now(),
    };
    let measure_from = start + step.warmup;
    let end = step.duration.map(|duration| start + duration);
    let limit = step.host_requests(setup.host, setup.hosts);
    let connections = step.connections;
    let shared = Arc::new(Shared {
        setup,
        kinds,
        weights,
        start,
        measure_from,
        end,
        limit,
        issued: AtomicU64::new(0),
        written: AtomicU64::new(0),
    });
    tokio::time::sleep_until(start.into()).await;
    let started_unix_ms = unix_ms() + step.warmup.as_millis() as u64;
    let progress = tokio::spawn(report_progress(shared.clone()));
    let workers: Vec<_> = (0..connections)
        .map(|index| tokio::spawn(work(shared.clone(), index, labels.clone())))
        .collect();
    let mut recorded = Recorder::new(&labels);
    let mut last = measure_from;
    for worker in workers {
        let (recorder, finished) = worker.await.expect("a worker finishes");
        recorded.merge(recorder);
        last = last.max(finished);
    }
    progress.abort();
    let Setup { host, hosts, .. } = shared.setup;
    HostResult {
        step: step.name.clone(),
        target: step.target,
        host,
        hosts,
        connections,
        rate: step.rate,
        started_unix_ms,
        seconds: last.saturating_duration_since(measure_from).as_secs_f64(),
        classes: recorded.classes,
        timeline: recorded.timeline,
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970")
        .as_millis() as u64
}

/// What one connection saw, and when its last request ended.
async fn work(shared: Arc<Shared>, index: usize, labels: Vec<String>) -> (Recorder, Instant) {
    let setup = &shared.setup;
    let endpoint = setup.endpoints[index % setup.endpoints.len()].clone();
    let tls = endpoint.tls.then(|| setup.tls.clone()).flatten();
    let mut client = Client::new(endpoint, tls, setup.signer.clone(), setup.step.timeout);
    let seed = mix(setup.plan.dataset.seed ^ ((setup.host as u64) << 32) ^ index as u64);
    let mut rng = Rng(seed ^ unix_ms());
    let mut recorder = Recorder::new(&labels);
    let mut scratch = Vec::new();
    let mut finished = shared.measure_from;
    let interval = (setup.step.rate > 0.0).then(|| Duration::from_secs_f64(1.0 / setup.step.rate));
    loop {
        let number = shared.issued.fetch_add(1, Ordering::Relaxed);
        if shared.limit.is_some_and(|limit| number >= limit) {
            break;
        }
        let due = match interval {
            Some(interval) => shared.start + interval.mul_f64(number as f64),
            None => Instant::now(),
        };
        if shared.end.is_some_and(|end| due >= end) {
            break;
        }
        // A closed loop sends at once: a timer rounds up to the next
        // millisecond, which would count against every request.
        if interval.is_some() {
            tokio::time::sleep_until(due.into()).await;
        }
        let pick = rng.unit() * shared.weights.last().expect("a step sends requests");
        let kind = shared.weights.partition_point(|&weight| weight <= pick);
        let kind = kind.min(shared.kinds.len() - 1);
        let sent = Instant::now();
        let outcome = match &shared.kinds[kind] {
            Kind::Read { .. } => read(&shared, kind, &mut client, &mut rng, &mut scratch).await,
            Kind::Write { size } => write(&shared, *size, &mut client, &mut rng).await,
        };
        let done = Instant::now();
        finished = finished.max(done);
        if due >= shared.measure_from {
            let second = done
                .saturating_duration_since(shared.measure_from)
                .as_secs() as usize;
            recorder.record(kind, second, outcome, sent - due);
        }
    }
    (recorder, finished)
}

async fn read(
    shared: &Shared,
    kind: usize,
    client: &mut Client,
    rng: &mut Rng,
    scratch: &mut Vec<u8>,
) -> Result<Answer, Failure> {
    let Kind::Read {
        set,
        keys,
        draw,
        range,
        cursor,
    } = &shared.kinds[kind]
    else {
        unreachable!("a read's kind is a read");
    };
    let setup = &shared.setup;
    let keys = *keys as u64;
    let index = match draw {
        Draw::Uniform => rng.below(keys),
        Draw::Sequential { window, passes } => {
            let next = cursor.fetch_add(1, Ordering::Relaxed);
            sequential(
                next,
                *window,
                *passes,
                setup.host as u64,
                setup.hosts as u64,
                keys,
            )
        }
        Draw::Zipf(zipf, multiplier) => {
            let rank = zipf.sample(rng) - 1;
            ((u128::from(rank) * u128::from(*multiplier)) % u128::from(keys)) as u64
        }
    };
    let dataset = &setup.plan.dataset;
    let object = dataset.object(&dataset.sets[*set], index);
    let (asked, first, len) = match *range {
        RangeSpec::Random(len) if len < object.size => {
            let first = rng.below(object.size - len + 1);
            (Range::Span(first, first + len - 1), first, len)
        }
        RangeSpec::Suffix(len) if len < object.size => (Range::Suffix(len), object.size - len, len),
        _ => (Range::Whole, 0, object.size),
    };
    let path = format!("/{}/{}", setup.bucket, object.key);
    let verify = setup.step.verify;
    let mut corrupt = false;
    let mut check = |status: u16, offset: u64, bytes: &[u8]| {
        if (200..300).contains(&status) && !corrupt {
            corrupt = !checks(&object, verify, first, len, offset, bytes, scratch);
        }
    };
    let answer = client.get(&path, asked, &mut check).await?;
    let whole = matches!(asked, Range::Whole);
    let expected = if whole { 200 } else { 206 };
    match answer.status {
        200 | 206 if answer.status != expected || answer.bytes != len => Err(Failure::Protocol),
        200 | 206 if corrupt => Err(Failure::Corrupt),
        _ => Ok(answer),
    }
}

/// The key a host's `next`th sequential read takes. A host takes every
/// `hosts`th key from its own, in windows each read `passes` times in turn;
/// its last window ends at its last key, so `passes` reads of each of its
/// keys make a round, and the next round starts over.
fn sequential(next: u64, window: u64, passes: u64, host: u64, hosts: u64, keys: u64) -> u64 {
    let share = keys.saturating_sub(host).div_ceil(hosts);
    if share == 0 {
        return host % keys;
    }
    let within = next % (share * passes);
    let start = within / (window * passes) * window;
    let len = window.min(share - start);
    let position = start + (within - start * passes) % len;
    position * hosts + host
}

/// Whether `bytes`, at `offset` in a body holding the object's bytes from
/// `first` for `len`, are the object's, as far as `verify` checks.
fn checks(
    object: &Object,
    verify: Verify,
    first: u64,
    len: u64,
    offset: u64,
    bytes: &[u8],
    scratch: &mut Vec<u8>,
) -> bool {
    const EDGE: u64 = 4 << 10;
    match verify {
        Verify::None => true,
        Verify::Full => object.matches(first + offset, bytes, scratch),
        Verify::Edges => {
            let end = offset + bytes.len() as u64;
            let mut checked = |from: u64, to: u64| {
                let (from, to) = (from.max(offset), to.min(end));
                from >= to || {
                    let piece = &bytes[(from - offset) as usize..(to - offset) as usize];
                    object.matches(first + from, piece, scratch)
                }
            };
            checked(0, EDGE) && checked(len.saturating_sub(EDGE), len)
        }
    }
}

async fn write(
    shared: &Shared,
    size: SizeSpec,
    client: &mut Client,
    rng: &mut Rng,
) -> Result<Answer, Failure> {
    let setup = &shared.setup;
    let number = shared.written.fetch_add(1, Ordering::Relaxed);
    let unit = rng.unit();
    let size = match size {
        SizeSpec::Fixed(size) => size,
        SizeSpec::Uniform(min, max) => min + ((max - min) as f64 * unit) as u64,
        SizeSpec::LogUniform(min, max) => {
            let (low, high) = ((min as f64).ln(), (max as f64).ln());
            ((low + (high - low) * unit).exp() as u64).clamp(min, max)
        }
    };
    let key = format!(
        "{}/writes/{}/{}/{number:09}",
        setup.plan.dataset.prefix, setup.run, setup.host
    );
    let object = Object::written(key, size);
    let path = format!("/{}/{}", setup.bucket, object.key);
    client
        .put(&path, size, &mut |offset, buffer| {
            object.fill(offset, buffer)
        })
        .await
        .map(|answer| Answer {
            bytes: size,
            ..answer
        })
}

/// Prints the host's progress every ten seconds, for whoever watches.
async fn report_progress(shared: Arc<Shared>) {
    let mut last = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let issued = shared.issued.load(Ordering::Relaxed);
        let elapsed = shared.start.elapsed().as_secs();
        eprintln!(
            "{}: {elapsed} s, {issued} requests started, {} in the last 10 s",
            shared.setup.step.name,
            issued - last
        );
        last = issued;
    }
}

struct Recorder {
    classes: Vec<ClassResult>,
    timeline: Vec<Second>,
}

impl Recorder {
    fn new(labels: &[String]) -> Recorder {
        Recorder {
            classes: labels.iter().cloned().map(ClassResult::new).collect(),
            timeline: Vec::new(),
        }
    }

    /// Records a request of `kind` that ended in `second`, and started
    /// `late` after it was due.
    fn record(
        &mut self,
        kind: usize,
        second: usize,
        outcome: Result<Answer, Failure>,
        late: Duration,
    ) {
        if self.timeline.len() <= second {
            self.timeline.resize_with(second + 1, || Second {
                first_byte: Histogram::coarse(),
                ..Second::default()
            });
        }
        let (class, second) = (&mut self.classes[kind], &mut self.timeline[second]);
        second.requests += 1;
        match outcome {
            Ok(answer) if (200..300).contains(&answer.status) => {
                *class.statuses.entry(answer.status).or_default() += 1;
                let first_byte = (late + answer.first_byte).as_micros() as u64;
                class.first_byte.record(first_byte);
                class.total.record((late + answer.total).as_micros() as u64);
                class.bytes += answer.bytes;
                second.bytes += answer.bytes;
                second.first_byte.record(first_byte);
            }
            Ok(answer) => {
                *class.statuses.entry(answer.status).or_default() += 1;
                second.errors += 1;
            }
            Err(failure) => {
                *class.failures.entry(failure).or_default() += 1;
                second.errors += 1;
            }
        }
    }

    fn merge(&mut self, other: Recorder) {
        for (class, theirs) in self.classes.iter_mut().zip(&other.classes) {
            class.merge(theirs);
        }
        if self.timeline.len() < other.timeline.len() {
            self.timeline.resize_with(other.timeline.len(), || Second {
                first_byte: Histogram::coarse(),
                ..Second::default()
            });
        }
        for (second, theirs) in self.timeline.iter_mut().zip(&other.timeline) {
            second.merge(theirs);
        }
    }
}

/// A SplitMix64 generator.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    /// A number in [0, 1).
    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A number below `bound`.
    pub fn below(&mut self, bound: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(bound)) >> 64) as u64
    }
}

/// A multiplier prime to `count`, which scatters ranks across keys.
fn coprime(count: u64) -> u64 {
    fn gcd(a: u64, b: u64) -> u64 {
        if b == 0 { a } else { gcd(b, a % b) }
    }
    let mut multiplier = (0x9e37_79b9_7f4a_7c15 % count.max(1)) | 1;
    while count > 1 && gcd(multiplier, count) != 1 {
        multiplier += 2;
    }
    multiplier
}

/// Draws ranks 1 to `count` by Zipf's law with exponent `exponent`, by
/// Hörmann and Derflinger's rejection-inversion, in constant memory.
pub struct Zipf {
    exponent: f64,
    count: f64,
    h_x1: f64,
    h_n: f64,
    s: f64,
}

impl Zipf {
    pub fn new(count: u64, exponent: f64) -> Zipf {
        let mut zipf = Zipf {
            exponent,
            count: count as f64,
            h_x1: 0.0,
            h_n: 0.0,
            s: 0.0,
        };
        zipf.h_x1 = zipf.h_integral(1.5) - 1.0;
        zipf.h_n = zipf.h_integral(count as f64 + 0.5);
        zipf.s = 2.0 - zipf.h_integral_inverse(zipf.h_integral(2.5) - zipf.h(2.0));
        zipf
    }

    pub fn sample(&self, rng: &mut Rng) -> u64 {
        loop {
            let u = self.h_n + rng.unit() * (self.h_x1 - self.h_n);
            let x = self.h_integral_inverse(u);
            let k = (x + 0.5).floor().clamp(1.0, self.count);
            if k - x <= self.s || u >= self.h_integral(k + 0.5) - self.h(k) {
                return k as u64;
            }
        }
    }

    fn h(&self, x: f64) -> f64 {
        (-self.exponent * x.ln()).exp()
    }

    fn h_integral(&self, x: f64) -> f64 {
        let log = x.ln();
        helper2((1.0 - self.exponent) * log) * log
    }

    fn h_integral_inverse(&self, x: f64) -> f64 {
        let t = (x * (1.0 - self.exponent)).max(-1.0);
        (helper1(t) * x).exp()
    }
}

/// `ln(1 + x) / x`, accurate near 0.
fn helper1(x: f64) -> f64 {
    if x.abs() > 1e-8 {
        x.ln_1p() / x
    } else {
        1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x))
    }
}

/// `(e^x - 1) / x`, accurate near 0.
fn helper2(x: f64) -> f64 {
    if x.abs() > 1e-8 {
        x.exp_m1() / x
    } else {
        1.0 + x * 0.5 * (1.0 + x / 3.0 * (1.0 + 0.25 * x))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A closed loop's requests to a server that answers at once take a
    /// fraction of a millisecond each, with no timer between them.
    #[tokio::test]
    async fn a_closed_loop_sends_each_request_as_the_last_ends() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut answer = b"HTTP/1.1 200 OK\r\ncontent-length: 1024\r\n\r\n".to_vec();
            answer.resize(answer.len() + 1024, 0);
            let (mut buffer, mut held) = (vec![0; 64 << 10], 0);
            loop {
                match stream.read(&mut buffer[held..]).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => held += read,
                }
                while let Some(end) = buffer[..held].windows(4).position(|w| w == b"\r\n\r\n") {
                    buffer.copy_within(end + 4..held, 0);
                    held -= end + 4;
                    stream.write_all(&answer).await.unwrap();
                }
            }
        });
        let plan = Plan::parse(
            "[dataset]\nprefix = \"t\"\nseed = 1\n\
             [[dataset.sets]]\nname = \"small\"\ncount = 10\nsize = \"1KiB\"\n\
             [[steps]]\nname = \"hits\"\ntarget = \"cache\"\nrequests = 1000\n\
             connections = 1\nverify = \"none\"\n[[steps.reads]]\nset = \"small\"\n",
        )
        .unwrap();
        let step = plan.step("hits").unwrap().clone();
        let endpoint = Endpoint::parse(&format!("http://127.0.0.1:{port}")).unwrap();
        let setup = Setup {
            plan: Arc::new(plan),
            step,
            bucket: "b".into(),
            endpoints: vec![Arc::new(endpoint)],
            tls: None,
            signer: Arc::new(crate::client::signer("key", "secret", "us-east-1")),
            host: 0,
            hosts: 1,
            start_at: None,
            run: "r".into(),
        };
        let started = Instant::now();
        let result = run(setup).await;
        let took = started.elapsed();
        assert_eq!(result.classes[0].statuses.get(&200), Some(&1000));
        // A request over loopback takes about 0.5 ms on a busy CI runner,
        // and a timer would add up to a millisecond to each.
        assert!(
            took < Duration::from_millis(700),
            "1,000 requests took {took:?}"
        );
    }

    #[test]
    fn zipf_draws_ranks_by_their_weight() {
        for exponent in [0.8, 1.0, 1.2] {
            let (count, draws) = (1_000u64, 400_000);
            let zipf = Zipf::new(count, exponent);
            let mut rng = Rng(7);
            let mut seen = vec![0u64; count as usize + 1];
            for _ in 0..draws {
                let rank = zipf.sample(&mut rng);
                assert!((1..=count).contains(&rank));
                seen[rank as usize] += 1;
            }
            let norm: f64 = (1..=count).map(|rank| (rank as f64).powf(-exponent)).sum();
            for rank in [1u64, 2, 10] {
                let expected = draws as f64 * (rank as f64).powf(-exponent) / norm;
                let found = seen[rank as usize] as f64;
                assert!(
                    (found - expected).abs() / expected < 0.05,
                    "{exponent} {rank}: {found} vs {expected}"
                );
            }
        }
    }

    #[test]
    fn sequential_reads_take_each_window_twice_and_split_keys_across_hosts() {
        let order: Vec<u64> = (0..8)
            .map(|next| sequential(next, 2, 2, 0, 1, 100))
            .collect();
        assert_eq!(order, [0, 1, 0, 1, 2, 3, 2, 3]);
        let mut seen = std::collections::BTreeMap::new();
        for host in 0..3 {
            for next in 0..20 {
                *seen.entry(sequential(next, 5, 2, host, 3, 30)).or_insert(0) += 1;
            }
        }
        assert_eq!(seen.len(), 30);
        assert!(seen.values().all(|&count| count == 2));
        // A window that leaves part of a host's keys over: its last window
        // is shorter, and each key is still read twice a round.
        for (keys, hosts, window) in [(20_000u64, 4, 2_000), (31, 3, 4), (7, 2, 10)] {
            let mut seen = std::collections::BTreeMap::new();
            for host in 0..hosts {
                for next in 0..2 * (keys - host).div_ceil(hosts) {
                    let key = sequential(next, window, 2, host, hosts, keys);
                    *seen.entry(key).or_insert(0) += 1;
                }
            }
            let case = format!("{keys} keys, {hosts} hosts, windows of {window}");
            assert!(seen.keys().all(|&key| key < keys), "{case}");
            assert_eq!(seen.len() as u64, keys, "{case}");
            assert!(seen.values().all(|&count| count == 2), "{case}");
        }
    }

    #[test]
    fn scattered_ranks_cover_every_key() {
        for count in [1u64, 2, 10, 1_000, 1_024, 999_983] {
            let multiplier = coprime(count);
            let mut seen = std::collections::BTreeSet::new();
            for rank in 0..count.min(5_000) {
                seen.insert((u128::from(rank) * u128::from(multiplier) % u128::from(count)) as u64);
            }
            assert_eq!(seen.len() as u64, count.min(5_000), "{count}");
        }
    }

    #[test]
    fn a_recorder_counts_answers_failures_and_seconds() {
        let mut recorder = Recorder::new(&["small: whole".to_string()]);
        let answer = |status, micros| Answer {
            status,
            first_byte: Duration::from_micros(micros),
            total: Duration::from_micros(2 * micros),
            bytes: 100,
        };
        recorder.record(0, 0, Ok(answer(200, 500)), Duration::ZERO);
        recorder.record(0, 2, Ok(answer(503, 10)), Duration::ZERO);
        recorder.record(0, 2, Err(Failure::Timeout), Duration::ZERO);
        recorder.record(0, 2, Ok(answer(206, 500)), Duration::from_micros(1_000));
        let class = &recorder.classes[0];
        assert_eq!(
            (class.successes(), class.errors(), class.bytes),
            (2, 2, 200)
        );
        assert_eq!(class.first_byte.max(), 1_500);
        assert_eq!(recorder.timeline.len(), 3);
        assert_eq!(recorder.timeline[2].requests, 3);
        assert_eq!(recorder.timeline[2].errors, 2);
    }
}
