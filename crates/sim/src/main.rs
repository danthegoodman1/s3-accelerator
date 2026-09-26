//! Runs simulations. With no seed it picks one; pass a seed, or a git commit
//! hash, to replay a run. `--seeds N` runs N consecutive seeds on every core.

use s3_accelerator_sim::{Simulator, parse_seed};
use std::process::ExitCode;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

const USAGE: &str = "usage: s3-accelerator-sim [SEED] [--seeds N]";

fn main() -> ExitCode {
    let mut seed = None;
    let mut count = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let parsed = match arg.as_str() {
            "--seeds" => args
                .next()
                .and_then(|n| n.parse().ok())
                .map(|n| count = Some(n)),
            text => parse_seed(text).ok().map(|value| seed = Some(value)),
        };
        if parsed.is_none() {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    }
    let seed = seed.unwrap_or_else(random_seed);
    match count {
        None => run_one(seed),
        Some(count) => sweep(seed, count),
    }
}

fn run_one(seed: u64) -> ExitCode {
    let simulator = Simulator::from_seed(seed);
    println!("seed {seed}: {:?}", simulator.options());
    match simulator.run() {
        Ok(summary) => {
            println!("{summary}");
            ExitCode::SUCCESS
        }
        Err(failure) => {
            eprintln!("{failure}");
            eprintln!("replay: cargo run --release -p s3-accelerator-sim -- {seed}");
            ExitCode::FAILURE
        }
    }
}

/// Runs seeds `start..start + count` in parallel. Each run is deterministic
/// on its own, so the threads change only the order results print in.
#[expect(
    clippy::disallowed_methods,
    reason = "runs are independent, so running them on threads keeps each one deterministic"
)]
fn sweep(start: u64, count: u64) -> ExitCode {
    let next = AtomicU64::new(0);
    let failures = Mutex::new(Vec::new());
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= count {
                        break;
                    }
                    let seed = start.wrapping_add(index);
                    let run = std::panic::catch_unwind(|| Simulator::from_seed(seed).run());
                    let failed = match run {
                        Ok(Ok(_)) => false,
                        Ok(Err(failure)) => {
                            eprintln!("{failure}");
                            true
                        }
                        Err(_) => {
                            eprintln!("seed {seed} panicked");
                            true
                        }
                    };
                    if failed {
                        failures
                            .lock()
                            .expect("no thread panics holding it")
                            .push(seed);
                    }
                }
            });
        }
    });
    let mut failures = failures
        .into_inner()
        .expect("no thread panicked holding it");
    failures.sort_unstable();
    println!("{} of {count} seeds from {start} failed", failures.len());
    for seed in &failures {
        println!("replay: cargo run --release -p s3-accelerator-sim -- {seed}");
    }
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[expect(
    clippy::disallowed_types,
    reason = "the clock is the only entropy a run draws from outside its seed"
)]
fn random_seed() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut state = now.as_nanos() as u64;
    s3_accelerator_sim::prng::splitmix64(&mut state)
}
