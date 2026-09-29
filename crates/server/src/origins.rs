//! Each bucket's origin, from the config or from the metadata service.

use crate::config::{Config, OriginConfig};
use crate::lookups::{Invalidated, Kind, Lookups, Unresolved};
use crate::metrics::Metrics;
use crate::origin::{HttpClient, Origin};
use crate::sigv4::Credentials;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub struct Origins {
    /// The config's origins of single buckets.
    buckets: BTreeMap<String, Arc<Origin>>,
    /// The config's origin of every other bucket.
    default: Option<Arc<Origin>>,
    /// The metadata service, which names every bucket's origin when set.
    lookups: Option<Rc<Lookups<Arc<Origin>>>>,
    metrics: Rc<Metrics>,
}

impl Origins {
    pub fn new(config: &Config, client: HttpClient, metrics: Rc<Metrics>) -> Origins {
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
        Origins {
            buckets: config
                .origins
                .iter()
                .map(|(bucket, origin)| (bucket.clone(), open(origin)))
                .collect(),
            default: config.origin.as_ref().map(open),
            lookups: config.metadata.as_ref().map(|metadata| {
                let client = client.clone();
                Lookups::new(Kind::Origin, metadata, client, parse, metrics.clone())
            }),
            metrics,
        }
    }

    /// The origin of requests naming no bucket.
    pub fn default(&self) -> Option<Arc<Origin>> {
        self.default.clone()
    }

    /// The origin a request to `bucket` goes to.
    pub async fn of(&self, bucket: &str) -> Result<Arc<Origin>, Unresolved> {
        match self.resolve(bucket).await {
            Ok((origin, stale)) => {
                if stale {
                    self.metrics.metadata_stale(Kind::Origin, 1);
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
    pub async fn serves(&self, bucket: &str) -> bool {
        let unknown = self.resolve(bucket).await.err() == Some(Unresolved::Unknown);
        if unknown {
            self.metrics.origin_unresolved(Unresolved::Unknown);
        }
        !unknown
    }

    /// `bucket`'s origin, and whether its entry is past its TTL.
    async fn resolve(&self, bucket: &str) -> Result<(Arc<Origin>, bool), Unresolved> {
        let Some(lookups) = &self.lookups else {
            let origin = self.buckets.get(bucket).or(self.default.as_ref());
            return origin
                .map(|origin| (origin.clone(), false))
                .ok_or(Unresolved::Unknown);
        };
        lookups.resolve(bucket).await
    }

    /// Checks the service's invalidation of `bucket`, sent to `path`, and
    /// drops the bucket's entry.
    pub fn invalidate(
        &self,
        bucket: &str,
        path: &str,
        time: &str,
        signature: &str,
        now: i64,
    ) -> Invalidated {
        match &self.lookups {
            Some(lookups) => lookups.invalidate(bucket, path, time, signature, now),
            None => Invalidated::NoService,
        }
    }
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

fn parse(client: &HttpClient, body: &[u8]) -> Result<(Arc<Origin>, Duration), String> {
    let served: Served = serde_json::from_slice(body).map_err(|error| error.to_string())?;
    let web = served.endpoint.starts_with("http://") || served.endpoint.starts_with("https://");
    if !web || served.region.is_empty() {
        return Err("it names no endpoint or region".to_string());
    }
    let credentials = Credentials {
        access_key_id: served.access_key_id,
        secret_access_key: served.secret_access_key,
    };
    let origin = Origin::new(
        client.clone(),
        &served.endpoint,
        &served.region,
        credentials,
    );
    Ok((Arc::new(origin), Duration::from_millis(served.ttl_ms)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::origin;
    use std::time::Instant;

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
            let origins = Rc::new(Origins::new(
                &config,
                origin::client(),
                Rc::new(Metrics::default()),
            ));
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
            if let Some(lookups) = &origins.lookups {
                for index in 0..100 {
                    let bucket = format!("bucket-{index}");
                    lookups.preload(&bucket, origin.clone(), Duration::from_secs(3600));
                }
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
}
