//! The reference metadata service. `s3-accelerator-metadata CONFIG` answers
//! nodes' lookups of buckets' origins and gateways' lookups of clients from
//! the config, and on `SIGHUP` reloads it and invalidates each bucket and
//! client that changed.

use s3_accelerator::log;
use s3_accelerator::metadata_service::{self, MetadataService, ServiceConfig};
use std::process::ExitCode;
use tokio::signal::unix::{SignalKind, signal};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: s3-accelerator-metadata CONFIG");
        return ExitCode::from(2);
    };
    let config = match ServiceConfig::load(&path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{path}: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let local = tokio::task::LocalSet::new();
    let served = local.block_on(&runtime, async move {
        let listener = tokio::net::TcpListener::bind(&config.listen).await?;
        let service = MetadataService::new(config);
        tokio::task::spawn_local(metadata_service::serve(listener, service.clone()));
        let mut hangups = signal(SignalKind::hangup())?;
        // Each reload pushes its invalidations on its own task, so one that
        // waits on an unreachable node leaves later reloads unblocked.
        while hangups.recv().await.is_some() {
            let config = ServiceConfig::load(&path);
            let service = service.clone();
            tokio::task::spawn_local(async move {
                let reloaded = match config {
                    Ok(config) => service.reload(config).await,
                    Err(error) => Err(error),
                };
                match reloaded {
                    Ok(changed) => log!(
                        Info,
                        "reloaded",
                        buckets = changed.buckets.len(),
                        clients = changed.clients.len()
                    ),
                    Err(error) => log!(Warn, "the reload failed", error = error),
                }
            });
        }
        Ok::<(), std::io::Error>(())
    });
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log!(Error, "the service stopped", error = error);
            ExitCode::FAILURE
        }
    }
}
