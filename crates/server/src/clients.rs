//! Clients' grants and signing keys, from the config or from the metadata
//! service, by access key ID.

use crate::config::{Access, Config, Grant};
use crate::lookups::{Kind, Lookups, Unresolved};
use crate::metrics::Metrics;
use crate::origin::HttpClient;
use crate::sigv4::{self, DateKeys};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

/// What a client may do, and what checks its signatures.
pub struct Credential {
    grants: Vec<Grant>,
    keys: Keys,
}

enum Keys {
    /// A secret the config names, which derives every date's key.
    Secret(String),
    /// Keys of the dates the service sent, by `YYYYMMDD`.
    Dates(BTreeMap<String, Vec<u8>>),
}

impl Credential {
    /// Whether a grant gives `access` to `key` in `bucket`. A listing
    /// passes the prefix it lists, and a request about the bucket itself
    /// an empty key, which only a grant on the whole bucket covers.
    pub fn may(&self, access: Access, bucket: &str, key: &str) -> bool {
        self.grants.iter().any(|grant| {
            grant.access >= access
                && (grant.bucket == "*" || grant.bucket == bucket)
                && key.starts_with(&grant.prefix)
        })
    }

    /// Whether some grant gives `access` to part of `bucket`.
    pub fn may_reach(&self, access: Access, bucket: &str) -> bool {
        self.grants
            .iter()
            .any(|grant| grant.access >= access && (grant.bucket == "*" || grant.bucket == bucket))
    }
}

impl DateKeys for Credential {
    fn date_key(&self, date: &str) -> Option<Vec<u8>> {
        match &self.keys {
            Keys::Secret(secret) => Some(sigv4::date_key(secret, date)),
            Keys::Dates(keys) => keys.get(date).cloned(),
        }
    }
}

pub struct Clients {
    /// The config's clients.
    configured: BTreeMap<String, Rc<Credential>>,
    /// The metadata service, which names every client when set.
    lookups: Option<Rc<Lookups<Rc<Credential>>>>,
    metrics: Arc<Metrics>,
}

impl Clients {
    /// The clients `config` names, or those its metadata service serves,
    /// looked up with at most `MAX_IN_FLIGHT` lookups under way that
    /// `in_flight` counts for the process.
    pub fn new(
        config: &Config,
        client: HttpClient,
        metrics: Arc<Metrics>,
        in_flight: Arc<AtomicUsize>,
    ) -> Clients {
        let configured = config
            .clients
            .iter()
            .map(|client| {
                let credential = Credential {
                    grants: client.grants.clone(),
                    keys: Keys::Secret(client.secret_access_key.clone()),
                };
                (client.access_key_id.clone(), Rc::new(credential))
            })
            .collect();
        Clients {
            configured,
            lookups: config.metadata.as_ref().map(|metadata| {
                Lookups::new(
                    Kind::Client,
                    metadata,
                    client,
                    parse,
                    metrics.clone(),
                    in_flight,
                )
            }),
            metrics,
        }
    }

    /// The client with `access_key_id`.
    pub async fn find(&self, access_key_id: &str) -> Result<Rc<Credential>, Unresolved> {
        let Some(lookups) = &self.lookups else {
            let credential = self.configured.get(access_key_id).cloned();
            return credential.ok_or(Unresolved::Unknown);
        };
        let (credential, stale) = lookups.resolve(access_key_id).await?;
        if stale {
            self.metrics.metadata_stale(Kind::Client, 1);
        }
        Ok(credential)
    }

    /// Drops the client's entry, for an invalidation the admin listener
    /// checked.
    pub fn forget(&self, access_key_id: &str) {
        if let Some(lookups) = &self.lookups {
            lookups.forget(access_key_id);
        }
    }
}

/// The service's answer for a client it knows.
#[derive(Deserialize)]
struct Served {
    grants: Vec<Grant>,
    /// Hex keys by `YYYYMMDD`.
    signing_keys: BTreeMap<String, String>,
    ttl_ms: u64,
}

fn parse(_: &HttpClient, body: &[u8]) -> Result<(Rc<Credential>, Duration), String> {
    let served: Served = serde_json::from_slice(body).map_err(|error| error.to_string())?;
    let mut keys = BTreeMap::new();
    for (date, key) in served.signing_keys {
        let dated = date.len() == 8 && date.bytes().all(|byte| byte.is_ascii_digit());
        let key = hex::decode(&key).ok().filter(|key| key.len() == 32);
        match (dated, key) {
            (true, Some(key)) => keys.insert(date, key),
            _ => return Err(format!("the signing key for {date} is malformed")),
        };
    }
    let credential = Credential {
        grants: served.grants,
        keys: Keys::Dates(keys),
    };
    Ok((Rc::new(credential), Duration::from_millis(served.ttl_ms)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(grants: &str) -> Credential {
        let grants: Vec<Grant> = serde_json::from_str(grants).unwrap();
        Credential {
            grants,
            keys: Keys::Secret("secret".into()),
        }
    }

    #[test]
    fn grants_give_their_access_to_their_prefix() {
        let client = credential(r#"[{ "bucket": "logs", "prefix": "2026/" }]"#);
        assert!(client.may(Access::Write, "logs", "2026/01/a"));
        assert!(!client.may(Access::Admin, "logs", "2026/01/a"));
        assert!(!client.may(Access::Read, "logs", "2025/12/a"));
        assert!(!client.may(Access::Read, "other", "2026/01/a"));
        assert!(client.may_reach(Access::Write, "logs"));
        let reader = credential(r#"[{ "bucket": "*", "access": "read" }]"#);
        assert!(reader.may(Access::Read, "any", "key"));
        assert!(!reader.may(Access::Write, "any", "key"));
    }

    /// What `Clients::find` costs a request whose key's entry is fresh,
    /// from the config and from the service: `cargo test --release -p
    /// s3-accelerator --lib cost_of -- --ignored --nocapture`.
    #[test]
    #[ignore = "a measurement, not a check"]
    fn cost_of_finding_a_client() {
        let config = |tables: &str| -> Config {
            toml::from_str(&format!(
                "{tables}\n[cluster]\nsecret = \"s\"\nnodes = [{{ id = 0, address = \"127.0.0.1:1\" }}]\n"
            ))
            .unwrap()
        };
        let keys: String = (0..100)
            .map(|index| {
                format!(
                    "[[clients]]\naccess_key_id = \"key-{index}\"\nsecret_access_key = \"s\"\n\
                     grants = [{{ bucket = \"*\" }}]\n"
                )
            })
            .collect();
        let from_config = config(&keys);
        let from_service = config("[metadata]\nurl = \"http://127.0.0.1:1\"\ntoken = \"t\"");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for (name, config) in [("config", from_config), ("service", from_service)] {
            let metrics = Arc::new(Metrics::default());
            let clients = Clients::new(&config, crate::origin::client(), metrics, Arc::default());
            if let Some(lookups) = &clients.lookups {
                for index in 0..100 {
                    let credential = Rc::new(Credential {
                        grants: Vec::new(),
                        keys: Keys::Dates(BTreeMap::new()),
                    });
                    let ttl = Duration::from_secs(3600);
                    lookups.preload(&format!("key-{index}"), credential, ttl);
                }
            }
            let rounds = 10_000_000;
            let started = std::time::Instant::now();
            runtime.block_on(async {
                for round in 0..rounds {
                    let id = ["key-7", "key-42", "key-93"][round % 3];
                    std::hint::black_box(clients.find(id).await.ok().unwrap());
                }
            });
            let each = started.elapsed().as_nanos() as f64 / rounds as f64;
            println!("from the {name}: {each:.1} ns a request");
        }
    }

    #[test]
    fn a_services_answer_holds_keys_of_dates() {
        let key = hex::encode(sigv4::date_key("secret", "20260929"));
        let body = format!(
            r#"{{ "grants": [{{ "bucket": "b", "access": "read" }}],
                "signing_keys": {{ "20260929": "{key}" }}, "ttl_ms": 1000 }}"#
        );
        let (credential, ttl) = parse(&crate::origin::client(), body.as_bytes()).unwrap();
        assert_eq!(ttl, Duration::from_secs(1));
        assert_eq!(
            credential.date_key("20260929"),
            Some(sigv4::date_key("secret", "20260929"))
        );
        assert_eq!(credential.date_key("20260928"), None);
        assert!(credential.may(Access::Read, "b", "k"));
        let malformed = body.replace("20260929", "2026-09-29");
        assert!(parse(&crate::origin::client(), malformed.as_bytes()).is_err());
    }
}
