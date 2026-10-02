//! The reference metadata service: buckets' origins and clients' grants
//! and signing keys from a TOML file, answered to nodes' and gateways'
//! lookups. A reload sends an invalidation of each changed bucket to every
//! node the file lists, and of each changed client to every gateway.

use crate::config::{Grant, OriginConfig};
use crate::http::{self, RequestHead, Response, split_path};
use crate::log;
use crate::lookups::invalidation_signature;
use crate::origin::{self, HttpClient};
use crate::sigv4;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpListener;

/// How many times the service sends a process an invalidation, and how
/// long it waits between tries.
const PUSH_TRIES: u32 = 3;
const PUSH_PAUSE: Duration = Duration::from_secs(1);
/// How long a process has to answer an invalidation.
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// How far ahead of the service's clock a client's may run, as SigV4
/// allows.
const SKEW_MS: u64 = 15 * 60 * 1000;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Where nodes and gateways reach the service, such as
    /// `127.0.0.1:9200`.
    pub listen: String,
    /// The token nodes and gateways send, which signs invalidations.
    pub token: String,
    /// Nodes' and gateways' admin listeners, such as `127.0.0.1:9091`.
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub gateways: Vec<String>,
    /// How long a process keeps each answer. A client's also ends by 23:45
    /// UTC the next day, when a clock ahead by the allowed skew could first
    /// sign with a date past its keys.
    #[serde(default = "default_ttl")]
    pub ttl_ms: u64,
    /// The origin of every bucket `buckets` doesn't name.
    pub default: Option<OriginConfig>,
    #[serde(default)]
    pub buckets: BTreeMap<String, OriginConfig>,
    /// Clients by access key ID.
    #[serde(default)]
    pub clients: BTreeMap<String, ClientConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub secret_access_key: String,
    #[serde(default)]
    pub grants: Vec<Grant>,
}

fn default_ttl() -> u64 {
    5 * 60 * 1000
}

impl ServiceConfig {
    pub fn load(path: &str) -> Result<ServiceConfig, String> {
        let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        toml::from_str(&text).map_err(|error| error.to_string())
    }

    fn origin_of(&self, bucket: &str) -> Option<&OriginConfig> {
        self.buckets.get(bucket).or(self.default.as_ref())
    }
}

/// A bucket's answer.
#[derive(Serialize)]
struct ServedOrigin<'a> {
    endpoint: &'a str,
    region: &'a str,
    access_key_id: &'a str,
    secret_access_key: &'a str,
    ttl_ms: u64,
}

/// A client's answer.
#[derive(Serialize)]
struct ServedClient<'a> {
    grants: &'a [Grant],
    /// Hex keys by `YYYYMMDD`.
    signing_keys: BTreeMap<String, String>,
    ttl_ms: u64,
}

/// A client's answer at `now_ms`, Unix milliseconds: the keys of the dates
/// from seven days before today, UTC, to tomorrow, kept at most `ttl_ms`
/// and no later than 23:45 tomorrow.
fn served_client(client: &ClientConfig, ttl_ms: u64, now_ms: u64) -> ServedClient<'_> {
    let now = (now_ms / 1000) as i64;
    let signing_keys = (-7..=1)
        .map(|days: i64| {
            let date = sigv4::format_amz_date(now + days * 86_400)[..8].to_string();
            let key = hex::encode(sigv4::date_key(&client.secret_access_key, &date));
            (date, key)
        })
        .collect();
    ServedClient {
        grants: &client.grants,
        signing_keys,
        ttl_ms: ttl_ms.min(2 * DAY_MS - now_ms % DAY_MS - SKEW_MS),
    }
}

/// What a reload changed of what processes may hold.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Changed {
    pub buckets: Vec<String>,
    pub clients: Vec<String>,
}

pub struct MetadataService {
    config: RefCell<ServiceConfig>,
    /// Buckets and clients the service has served, which processes may
    /// hold. The sets only grow, since a process may hold any of them. A
    /// name the service doesn't serve stays out, since clients' names come
    /// from unauthenticated requests; a process that remembered it as
    /// unknown learns of it once the unknown TTL passes.
    answered_buckets: RefCell<BTreeSet<String>>,
    answered_clients: RefCell<BTreeSet<String>>,
    client: HttpClient,
}

impl MetadataService {
    pub fn new(config: ServiceConfig) -> Rc<MetadataService> {
        Rc::new(MetadataService {
            config: RefCell::new(config),
            answered_buckets: RefCell::new(BTreeSet::new()),
            answered_clients: RefCell::new(BTreeSet::new()),
            client: origin::client(),
        })
    }

    /// `GET /buckets/<bucket>` or `GET /clients/<access key ID>`, with the
    /// token as a bearer.
    fn answer(&self, head: &RequestHead) -> Response {
        let config = self.config.borrow();
        let bearer = head
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        if !sigv4::constant_time_eq(bearer.as_bytes(), config.token.as_bytes()) {
            return Response::text(401, "unauthorized\n");
        }
        let (kind, name) = split_path(&head.path);
        if head.method != "GET" || name.is_empty() || name.contains('/') {
            return Response::text(404, "not found\n");
        }
        let body = match kind.as_str() {
            "buckets" => {
                let Some(origin) = config.origin_of(&name) else {
                    return Response::text(404, "no such bucket\n");
                };
                self.answered_buckets.borrow_mut().insert(name.clone());
                let served = ServedOrigin {
                    endpoint: &origin.endpoint,
                    region: &origin.region,
                    access_key_id: &origin.access_key_id,
                    secret_access_key: &origin.secret_access_key,
                    ttl_ms: config.ttl_ms,
                };
                serde_json::to_vec(&served)
            }
            "clients" => {
                let Some(client) = config.clients.get(&name) else {
                    return Response::text(404, "no such client\n");
                };
                self.answered_clients.borrow_mut().insert(name.clone());
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.as_millis() as u64);
                serde_json::to_vec(&served_client(client, config.ttl_ms, now_ms))
            }
            _ => return Response::text(404, "not found\n"),
        };
        let body = body.expect("an answer serializes");
        Response {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            content_length: body.len() as u64,
            body: Bytes::from(body),
        }
    }

    /// Takes `config` in place of the current one, then invalidates each
    /// bucket the service has answered for whose origin changed, on every
    /// node, and each client whose secret or grants changed, on every
    /// gateway. The token and the listen address take a restart, since
    /// processes check invalidations with the token their own config names.
    pub async fn reload(self: &Rc<Self>, config: ServiceConfig) -> Result<Changed, String> {
        let changed = {
            let old = self.config.borrow();
            if old.token != config.token || old.listen != config.listen {
                return Err("the token and listen address take a restart".to_string());
            }
            let buckets = self.answered_buckets.borrow();
            let clients = self.answered_clients.borrow();
            Changed {
                buckets: buckets
                    .iter()
                    .filter(|bucket| old.origin_of(bucket) != config.origin_of(bucket))
                    .cloned()
                    .collect(),
                clients: clients
                    .iter()
                    .filter(|id| old.clients.get(*id) != config.clients.get(*id))
                    .cloned()
                    .collect(),
            }
        };
        let paths = |kind: &str, names: &[String]| -> Vec<String> {
            names
                .iter()
                .map(|name| format!("/{kind}/{}/invalidate", sigv4::encode(name)))
                .collect()
        };
        let bucket_paths = paths("origins", &changed.buckets);
        let client_paths = paths("clients", &changed.clients);
        let nodes = config
            .nodes
            .iter()
            .map(|node| (node.clone(), bucket_paths.clone()));
        let gateways = config
            .gateways
            .iter()
            .map(|gateway| (gateway.clone(), client_paths.clone()));
        let processes: Vec<(String, Vec<String>)> = nodes
            .chain(gateways)
            .filter(|(_, paths)| !paths.is_empty())
            .collect();
        *self.config.borrow_mut() = config;
        let mut pushes = tokio::task::JoinSet::new();
        for (process, paths) in processes {
            let service = self.clone();
            pushes.spawn_local(async move { service.push(&process, &paths).await });
        }
        pushes.join_all().await;
        Ok(changed)
    }

    /// Sends `process` an invalidation at each of `paths`. A process that
    /// takes none after `PUSH_TRIES` is left to its entries' TTLs.
    async fn push(&self, process: &str, paths: &[String]) {
        let token = self.config.borrow().token.clone();
        for path in paths {
            let mut tries = 0;
            loop {
                tries += 1;
                match self.invalidate(process, &token, path).await {
                    Ok(()) => break,
                    Err(error) if tries == PUSH_TRIES => {
                        log!(
                            Warn,
                            "a process took no invalidation",
                            process = process,
                            path = path,
                            error = error
                        );
                        return;
                    }
                    Err(_) => tokio::time::sleep(PUSH_PAUSE).await,
                }
            }
        }
    }

    async fn invalidate(&self, process: &str, token: &str, path: &str) -> Result<(), String> {
        let time = sigv4::unix_now();
        let request = ::http::Request::post(format!("http://{process}{path}"))
            .header("x-accel-time", time.to_string())
            .header(
                "x-accel-signature",
                invalidation_signature(token, path, time),
            )
            .header("content-length", "0")
            .body(origin::empty())
            .map_err(|error| error.to_string())?;
        let response = tokio::time::timeout(PUSH_TIMEOUT, self.client.request(request))
            .await
            .map_err(|_| "the process took too long".to_string())?
            .map_err(|error| error.to_string())?;
        match response.status().as_u16() {
            204 => Ok(()),
            status => Err(format!("the process answered {status}")),
        }
    }
}

/// Answers lookups for as long as the service runs.
pub async fn serve(listener: TcpListener, service: Rc<MetadataService>) {
    http::serve_heads(
        listener,
        "metadata",
        Rc::new(move |head| Box::pin(std::future::ready(service.answer(head)))),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clients_answer_holds_nine_dates_and_ends_by_midnight() {
        let client = ClientConfig {
            secret_access_key: "secret".into(),
            grants: Vec::new(),
        };
        let now_ms = sigv4::parse_amz_date("20260929T230000Z").unwrap() as u64 * 1000;
        let served = served_client(&client, 5 * 60 * 60 * 1000, now_ms);
        let dates: Vec<&str> = served.signing_keys.keys().map(String::as_str).collect();
        assert_eq!(dates.len(), 9);
        assert_eq!(dates.first(), Some(&"20260922"));
        assert_eq!(dates.last(), Some(&"20260930"));
        assert_eq!(
            served.signing_keys["20260929"],
            hex::encode(sigv4::date_key("secret", "20260929"))
        );
        // Five hours, within the 24 hours and 45 minutes to 23:45 tomorrow.
        assert_eq!(served.ttl_ms, 5 * 60 * 60 * 1000);
        let day = served_client(&client, 48 * 60 * 60 * 1000, now_ms);
        assert_eq!(day.ttl_ms, (24 * 60 + 45) * 60 * 1000);
    }
}
