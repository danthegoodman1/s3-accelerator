//! Runs one simulation. With no argument it picks a seed; pass a seed, or a
//! git commit hash, to replay a run.

use s3_accelerator_sim::{Simulator, parse_seed};
use std::process::ExitCode;

fn main() -> ExitCode {
    let seed = match std::env::args().nth(1) {
        Some(text) => match parse_seed(&text) {
            Ok(seed) => seed,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::from(2);
            }
        },
        None => random_seed(),
    };
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
