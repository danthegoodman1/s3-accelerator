//! Writes the plan's dataset to the bucket, split across client hosts: host
//! `h` of `H` writes every `H`th object from the `h`th. S3's 503s and
//! failed connections are retried with backoff, since a new bucket answers
//! `SlowDown` until it has split its keys across partitions.

use crate::client::{Client, Endpoint, Failure};
use crate::plan::Plan;
use crate::run::Rng;
use rustls::ClientConfig;
use s3_accelerator::sigv4::Signer;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const ATTEMPTS: u32 = 12;

pub struct Seeding {
    pub plan: Arc<Plan>,
    pub bucket: String,
    pub endpoint: Arc<Endpoint>,
    pub tls: Option<Arc<ClientConfig>>,
    pub signer: Arc<Signer>,
    pub host: usize,
    pub hosts: usize,
    pub connections: usize,
    /// Skips objects S3 already holds at their size, so a seed that
    /// stopped partway picks up where it left off.
    pub resume: bool,
}

struct Progress {
    next: AtomicU64,
    written: AtomicU64,
    skipped: AtomicU64,
    bytes: AtomicU64,
    retries: AtomicU64,
}

/// Writes this host's share of the dataset; fails if an object could not
/// be written after every attempt.
pub async fn seed(seeding: Seeding) -> Result<(), String> {
    let total: u64 = seeding.plan.dataset.sets.iter().map(|set| set.count).sum();
    let share = (total + (seeding.hosts - seeding.host - 1) as u64) / seeding.hosts as u64;
    eprintln!(
        "seeding {share} of {total} objects as host {} of {}",
        seeding.host, seeding.hosts
    );
    let progress = Arc::new(Progress {
        next: AtomicU64::new(0),
        written: AtomicU64::new(0),
        skipped: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        retries: AtomicU64::new(0),
    });
    let seeding = Arc::new(seeding);
    let started = Instant::now();
    let reporter = tokio::spawn(report(progress.clone(), share, started));
    let workers: Vec<_> = (0..seeding.connections)
        .map(|index| tokio::spawn(work(seeding.clone(), progress.clone(), share, index)))
        .collect();
    let mut failed = Vec::new();
    for worker in workers {
        failed.extend(worker.await.expect("a worker finishes"));
    }
    reporter.abort();
    let seconds = started.elapsed().as_secs_f64();
    let gib = progress.bytes.load(Ordering::Relaxed) as f64 / (1u64 << 30) as f64;
    eprintln!(
        "wrote {} objects ({gib:.2} GiB, {:.2} GiB/s) and skipped {} in {seconds:.0} s, with {} retries",
        progress.written.load(Ordering::Relaxed),
        gib / seconds.max(0.001),
        progress.skipped.load(Ordering::Relaxed),
        progress.retries.load(Ordering::Relaxed),
    );
    match failed.as_slice() {
        [] => Ok(()),
        [first, ..] => Err(format!(
            "{} objects failed; the first: {first}",
            failed.len()
        )),
    }
}

/// The objects this worker failed to write.
async fn work(
    seeding: Arc<Seeding>,
    progress: Arc<Progress>,
    share: u64,
    index: usize,
) -> Vec<String> {
    let timeout = Duration::from_secs(300);
    let tls = seeding.endpoint.tls.then(|| seeding.tls.clone()).flatten();
    let mut client = Client::new(
        seeding.endpoint.clone(),
        tls,
        seeding.signer.clone(),
        timeout,
    );
    let mut rng = Rng(index as u64 + 1);
    let dataset = &seeding.plan.dataset;
    let mut failed = Vec::new();
    loop {
        let number = progress.next.fetch_add(1, Ordering::Relaxed);
        if number >= share {
            return failed;
        }
        let mut ordinal = number * seeding.hosts as u64 + seeding.host as u64;
        let Some(set) = dataset.sets.iter().find(|set| {
            let within = ordinal < set.count;
            if !within {
                ordinal -= set.count;
            }
            within
        }) else {
            return failed;
        };
        let object = dataset.object(set, ordinal);
        let path = format!("/{}/{}", seeding.bucket, object.key);
        if seeding.resume
            && let Ok(answer) = client.head(&path).await
            && answer.status == 200
            && answer.bytes == object.size
        {
            progress.skipped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let mut attempt = 0;
        loop {
            let put = client
                .put(&path, object.size, &mut |offset, buffer| {
                    object.fill(offset, buffer)
                })
                .await;
            let retry = match put {
                Ok(answer) if answer.status == 200 => {
                    progress.written.fetch_add(1, Ordering::Relaxed);
                    progress.bytes.fetch_add(object.size, Ordering::Relaxed);
                    break;
                }
                Ok(answer) => answer.status >= 500,
                Err(Failure::Corrupt | Failure::Protocol) => false,
                Err(_) => true,
            };
            attempt += 1;
            if !retry || attempt == ATTEMPTS {
                let why = match put {
                    Ok(answer) => format!("status {}", answer.status),
                    Err(failure) => format!("{failure:?}"),
                };
                failed.push(format!("{} ({why})", object.key));
                break;
            }
            progress.retries.fetch_add(1, Ordering::Relaxed);
            // Full jitter, from 100 ms to 20 s.
            let ceiling = (100u64 << attempt.min(8)).min(20_000);
            tokio::time::sleep(Duration::from_millis(rng.below(ceiling) + 1)).await;
        }
    }
}

async fn report(progress: Arc<Progress>, share: u64, started: Instant) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let done =
            progress.written.load(Ordering::Relaxed) + progress.skipped.load(Ordering::Relaxed);
        let gib = progress.bytes.load(Ordering::Relaxed) as f64 / (1u64 << 30) as f64;
        eprintln!(
            "{:.0} s: {done} of {share} objects, {gib:.1} GiB written, {} retries",
            started.elapsed().as_secs_f64(),
            progress.retries.load(Ordering::Relaxed)
        );
    }
}
