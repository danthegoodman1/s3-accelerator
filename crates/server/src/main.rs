//! The gateway and storage node binary. `s3-accelerator CONFIG` runs the
//! gateway, the storage node, or both, as the config says.

use s3_accelerator::{config, log, server};
use std::process::ExitCode;

/// glibc's allocator took a sixth of a gateway host's CPU under small reads,
/// much of it in locks its threads share; jemalloc gives each thread its own
/// cache, and keeps freed blocks' memory for the next fill.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: s3-accelerator CONFIG");
        return ExitCode::from(2);
    };
    let config = match std::fs::read_to_string(&path)
        .map_err(|error| error.to_string())
        .and_then(|text| toml::from_str::<config::Config>(&text).map_err(|error| error.to_string()))
        .and_then(|config| config.check().map(|()| config))
    {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{path}: {error}");
            return ExitCode::from(2);
        }
    };
    log::set_level(config.log.level);
    raise_open_file_limit();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let local = tokio::task::LocalSet::new();
    match local.block_on(&runtime, server::serve(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log!(Error, "the process stopped", error = error);
            ExitCode::FAILURE
        }
    }
}

/// Raises the soft limit on open files to the hard limit. Every client,
/// peer and S3 connection holds a descriptor, and service managers often
/// start processes with a soft limit of 1,024.
fn raise_open_file_limit() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let Rlimit { current, maximum } = getrlimit(Resource::Nofile);
    if current == maximum {
        return;
    }
    // Linux bounds the hard limit on open files, so neither is unlimited.
    let (from, to) = (current.unwrap_or(u64::MAX), maximum.unwrap_or(u64::MAX));
    let raised = Rlimit {
        current: maximum,
        maximum,
    };
    match setrlimit(Resource::Nofile, raised) {
        Ok(()) => log!(Info, "raised the open file limit", from = from, to = to),
        Err(error) => log!(Warn, "raising the open file limit failed", error = error),
    }
}
