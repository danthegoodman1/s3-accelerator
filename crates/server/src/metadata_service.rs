//! The reference metadata service: buckets' origins from a TOML file,
//! answered to nodes' lookups. A reload that changes a bucket's origin
//! sends an invalidation of the bucket to every node the file lists.

use crate::config::OriginConfig;
use crate::http::{self, RequestHead, Response, split_path};
use crate::log;
use crate::origin::{self, HttpClient};
use crate::origins::invalidation_signature;
use crate::sigv4;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpListener;

/// How many times the service sends a node an invalidation, and how long
/// it waits between tries.
const PUSH_TRIES: u32 = 3;
const PUSH_PAUSE: Duration = Duration::from_secs(1);
/// How long a node has to answer an invalidation.
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Where nodes reach the service, such as `127.0.0.1:9200`.
    pub listen: String,
    /// The token nodes send, which signs invalidations.
    pub token: String,
    /// Nodes' admin listeners, such as `127.0.0.1:9091`.
    #[serde(default)]
    pub nodes: Vec<String>,
    /// How long a node keeps each answer.
    #[serde(default = "default_ttl")]
    pub ttl_ms: u64,
    /// The origin of every bucket `buckets` doesn't name.
    pub default: Option<OriginConfig>,
    #[serde(default)]
    pub buckets: BTreeMap<String, OriginConfig>,
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

/// A lookup's answer.
#[derive(Serialize)]
struct Served<'a> {
    endpoint: &'a str,
    region: &'a str,
    access_key_id: &'a str,
    secret_access_key: &'a str,
    ttl_ms: u64,
}

pub struct MetadataService {
    config: RefCell<ServiceConfig>,
    /// Buckets the service has answered for, which nodes may hold. The set
    /// only grows, since a node may hold any of them.
    answered: RefCell<BTreeSet<String>>,
    client: HttpClient,
}

impl MetadataService {
    pub fn new(config: ServiceConfig) -> Rc<MetadataService> {
        Rc::new(MetadataService {
            config: RefCell::new(config),
            answered: RefCell::new(BTreeSet::new()),
            client: origin::client(),
        })
    }

    /// `GET /buckets/<bucket>` with the token as a bearer.
    fn answer(&self, head: &RequestHead) -> Response {
        let config = self.config.borrow();
        let bearer = head
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        if !sigv4::constant_time_eq(bearer.as_bytes(), config.token.as_bytes()) {
            return Response::text(401, "unauthorized\n");
        }
        let (prefix, bucket) = split_path(&head.path);
        if head.method != "GET" || prefix != "buckets" || bucket.is_empty() || bucket.contains('/')
        {
            return Response::text(404, "not found\n");
        }
        self.answered.borrow_mut().insert(bucket.clone());
        let Some(origin) = config.origin_of(&bucket) else {
            return Response::text(404, "no such bucket\n");
        };
        let served = Served {
            endpoint: &origin.endpoint,
            region: &origin.region,
            access_key_id: &origin.access_key_id,
            secret_access_key: &origin.secret_access_key,
            ttl_ms: config.ttl_ms,
        };
        let body = serde_json::to_vec(&served).expect("an answer serializes");
        Response {
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            content_length: body.len() as u64,
            body: Bytes::from(body),
        }
    }

    /// Takes `config` in place of the current one, then invalidates, on
    /// every node, each bucket the service has answered for whose origin
    /// changed, and returns those buckets. The token and the listen address
    /// take a restart, since nodes check invalidations with the token their
    /// own config names.
    pub async fn reload(self: &Rc<Self>, config: ServiceConfig) -> Result<Vec<String>, String> {
        let changed: Vec<String> = {
            let old = self.config.borrow();
            if old.token != config.token || old.listen != config.listen {
                return Err("the token and listen address take a restart".to_string());
            }
            self.answered
                .borrow()
                .iter()
                .filter(|bucket| old.origin_of(bucket) != config.origin_of(bucket))
                .cloned()
                .collect()
        };
        let nodes = config.nodes.clone();
        *self.config.borrow_mut() = config;
        let mut pushes = tokio::task::JoinSet::new();
        for node in nodes {
            let (service, buckets) = (self.clone(), changed.clone());
            pushes.spawn_local(async move { service.push(&node, &buckets).await });
        }
        pushes.join_all().await;
        Ok(changed)
    }

    /// Sends `node` an invalidation of each of `buckets`. A node that takes
    /// none after `PUSH_TRIES` is left to its entries' TTLs.
    async fn push(&self, node: &str, buckets: &[String]) {
        let token = self.config.borrow().token.clone();
        for bucket in buckets {
            let mut tries = 0;
            loop {
                tries += 1;
                match self.invalidate(node, &token, bucket).await {
                    Ok(()) => break,
                    Err(error) if tries == PUSH_TRIES => {
                        log!(
                            Warn,
                            "a node took no invalidation",
                            node = node,
                            bucket = bucket,
                            error = error
                        );
                        return;
                    }
                    Err(_) => tokio::time::sleep(PUSH_PAUSE).await,
                }
            }
        }
    }

    async fn invalidate(&self, node: &str, token: &str, bucket: &str) -> Result<(), String> {
        let time = sigv4::unix_now();
        let request = ::http::Request::post(format!("http://{node}/origins/{bucket}/invalidate"))
            .header("x-accel-time", time.to_string())
            .header(
                "x-accel-signature",
                invalidation_signature(token, bucket, time),
            )
            .header("content-length", "0")
            .body(origin::empty())
            .map_err(|error| error.to_string())?;
        let response = tokio::time::timeout(PUSH_TIMEOUT, self.client.request(request))
            .await
            .map_err(|_| "the node took too long".to_string())?
            .map_err(|error| error.to_string())?;
        match response.status().as_u16() {
            204 => Ok(()),
            status => Err(format!("the node answered {status}")),
        }
    }
}

/// Answers lookups for as long as the service runs.
pub async fn serve(listener: TcpListener, service: Rc<MetadataService>) {
    http::serve_heads(
        listener,
        "metadata",
        Rc::new(move |head| service.answer(head)),
    )
    .await;
}
