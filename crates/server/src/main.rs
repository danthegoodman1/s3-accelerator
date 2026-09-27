//! The gateway and storage node binary. `s3-accelerator CONFIG` runs the
//! gateway, the storage node, or both, as the config says.

use s3_accelerator::{config, log, server};
use std::process::ExitCode;

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
