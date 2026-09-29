//! The server's TOML configuration. A process runs a gateway, a storage
//! node, or both, in a cluster every process describes alike.

use s3_accelerator_core::gateway;
use s3_accelerator_core::layout::Layout;
use s3_accelerator_core::membership::{self, Peer};
use s3_accelerator_core::node::{self, BucketPolicy, Freshness};
use s3_accelerator_core::placement::{NodeId, Ring};
use s3_accelerator_core::store::StoreConfig;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The origin of every bucket `origins` doesn't name, and of requests
    /// naming no bucket. Only storage nodes reach origins.
    pub origin: Option<OriginConfig>,
    /// Origins of single buckets, by bucket name.
    #[serde(default)]
    pub origins: BTreeMap<String, OriginConfig>,
    /// The metadata service, which names each bucket's origin in place of
    /// `origin` and `origins`.
    pub metadata: Option<MetadataConfig>,
    /// The SQS queue S3 sends its event notifications to, which storage
    /// nodes poll.
    pub events: Option<EventsConfig>,
    #[serde(default)]
    pub clients: Vec<Client>,
    #[serde(default)]
    pub cache: CacheConfig,
    pub cluster: ClusterConfig,
    pub gateway: Option<GatewayConfig>,
    pub node: Option<NodeConfig>,
    pub admin: Option<AdminConfig>,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// The least severe level written: `error`, `warn`, `info` or `debug`.
    #[serde(default)]
    pub level: crate::log::Level,
}

/// Where the process serves its metrics, health and readiness.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Such as `127.0.0.1:9090`. The listener checks no credentials.
    pub listen: String,
}

/// The storage nodes, and the secret that gateways and nodes share.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub secret: String,
    /// The nodes a process starts with. Nodes gossip, so one missing here
    /// joins through the others, and gateways learn nodes from rings.
    pub nodes: Vec<ClusterNode>,
    #[serde(default)]
    pub membership: MembershipConfig,
    /// Gateways and nodes reach nodes over mutual TLS when set, and over
    /// plaintext otherwise.
    pub tls: Option<ClusterTlsConfig>,
}

/// Mutual TLS among cluster members. Each presents a certificate the
/// cluster's CA signed, and a node's certificate names the host of its
/// address.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTlsConfig {
    /// PEM files: the CA's certificate, this process's certificate chain,
    /// leaf first, and its private key.
    pub ca: String,
    pub cert: String,
    pub key: String,
    /// As for clients' TLS.
    #[serde(default = "default_kernel")]
    pub kernel: bool,
}

/// SWIM's timings among storage nodes.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MembershipConfig {
    /// How often a node probes another, and how long it waits for the
    /// answer before asking others to probe it.
    pub probe_period_ms: u64,
    pub probe_rtt_ms: u64,
    /// How long a suspected node has to answer before it is declared down,
    /// and how long a node declared down stays in the ring.
    pub suspect_to_down_ms: u64,
    pub down_grace_ms: u64,
    /// How often a node gossips updates to a few others.
    pub gossip_period_ms: u64,
}

impl Default for MembershipConfig {
    fn default() -> MembershipConfig {
        MembershipConfig {
            probe_period_ms: 1_000,
            probe_rtt_ms: 500,
            suspect_to_down_ms: 5_000,
            down_grace_ms: 60_000,
            gossip_period_ms: 200,
        }
    }
}

impl MembershipConfig {
    pub fn config(&self) -> membership::Config {
        membership::Config {
            probe_period: self.probe_period_ms,
            probe_rtt: self.probe_rtt_ms,
            suspect_to_down: self.suspect_to_down_ms,
            down_grace: self.down_grace_ms,
            gossip_period: self.gossip_period_ms,
            max_packet: 1_400,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterNode {
    pub id: u64,
    /// Where gateways reach the node, such as `10.0.0.1:9100`; the node
    /// listens there.
    pub address: String,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// Where S3 clients connect, such as `127.0.0.1:9000`.
    pub listen: String,
    /// Domains the gateway takes virtual-hosted-style requests for: a
    /// request to `bucket.s3.example.com` names `bucket` when this holds
    /// `s3.example.com`. Other requests are path-style.
    #[serde(default)]
    pub domains: Vec<String>,
    /// Clients connect over TLS when set, and over plaintext otherwise.
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM files: the certificate chain, leaf first, and its private key.
    pub cert: String,
    pub key: String,
    /// Sessions move into the kernel when it takes them, so zero-copy works
    /// under TLS; false keeps them in userspace.
    #[serde(default = "default_kernel")]
    pub kernel: bool,
}

fn default_kernel() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// This node's entry in `cluster.nodes`.
    pub id: u64,
    /// Where the node keeps its slab file, slot table and metadata file.
    pub data_dir: String,
}

impl Config {
    /// Settings a process could not run with.
    pub fn check(&self) -> Result<(), String> {
        if self.gateway.is_none() && self.node.is_none() {
            return Err("the config runs neither a gateway nor a node".into());
        }
        if self.cluster.secret.is_empty() {
            return Err("cluster.secret is empty".into());
        }
        let mut ids = BTreeSet::new();
        for node in &self.cluster.nodes {
            if !ids.insert(node.id) {
                return Err(format!("node {} appears twice in cluster.nodes", node.id));
            }
            if node.weight == 0 {
                return Err(format!("node {} has weight 0", node.id));
            }
        }
        if let Some(events) = &self.events {
            let own = events.access_key_id.is_some() && events.secret_access_key.is_some();
            if events.access_key_id.is_some() != events.secret_access_key.is_some() {
                return Err("[events] names half a credential".into());
            }
            if !own && self.origin.is_none() {
                return Err("[events] needs credentials of its own without [origin]".into());
            }
            if events.region.is_none() && self.origin.is_none() {
                return Err("[events] needs a region without [origin]".into());
            }
        }
        if self.metadata.is_some() && (self.origin.is_some() || !self.origins.is_empty()) {
            return Err("[metadata] names origins in place of [origin] and [origins]".into());
        }
        if let Some(node) = &self.node {
            if self.origin.is_none() && self.origins.is_empty() && self.metadata.is_none() {
                return Err("a node needs [origin], [origins] or [metadata]".into());
            }
            if !ids.contains(&node.id) {
                return Err(format!("node {} is not in cluster.nodes", node.id));
            }
            if node.data_dir.is_empty() {
                return Err("node.data_dir names no directory".into());
            }
        }
        if ids.is_empty() {
            return Err("cluster.nodes is empty".into());
        }
        self.cache.check()
    }

    /// The ring of the nodes the config names, which every process that
    /// starts from the same config shares.
    pub fn ring(&self) -> Ring {
        membership::ring_of(&self.peers())
    }

    /// The nodes the config names, as membership first knows them.
    pub fn peers(&self) -> Vec<Peer> {
        self.cluster
            .nodes
            .iter()
            .map(|node| Peer {
                id: node.id,
                weight: node.weight,
                run: 0,
                leaving: false,
                address: node.address.clone(),
            })
            .collect()
    }

    /// Where gateways reach each node.
    pub fn addresses(&self) -> BTreeMap<NodeId, String> {
        self.cluster
            .nodes
            .iter()
            .map(|node| (NodeId(node.id), node.address.clone()))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsConfig {
    /// Such as `https://sqs.us-east-1.amazonaws.com/123456789012/events`.
    pub queue_url: String,
    /// The queue's region, if other than `origin`'s.
    pub region: Option<String>,
    /// Credentials for the queue, if other than `origin`'s.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// How long a message stays hidden from other nodes once one takes it.
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout_s: u64,
}

fn default_visibility_timeout() -> u64 {
    30
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OriginConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Where nodes look up buckets' origins.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataConfig {
    /// Such as `https://metadata.example.com`; a lookup asks
    /// `<url>/buckets/<bucket>`.
    pub url: String,
    /// Sent with each lookup, and the key that signs invalidations.
    pub token: String,
    /// How long past its TTL a node uses an entry while lookups fail.
    #[serde(default = "default_grace")]
    pub grace_ms: u64,
    /// How long a node remembers that the service doesn't serve a bucket.
    #[serde(default = "default_unknown_ttl")]
    pub unknown_ttl_ms: u64,
}

fn default_grace() -> u64 {
    15 * 60 * 1000
}

fn default_unknown_ttl() -> u64 {
    10_000
}

/// A client credential and the buckets and prefixes it may use.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub grants: Vec<Grant>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    /// What the grant allows: `read`, `write` (the default, which reads
    /// too) or `admin`, which also changes and deletes the bucket itself.
    #[serde(default)]
    pub access: Access,
    /// A bucket name, or `*` for every bucket.
    pub bucket: String,
    /// Keys the grant covers start with this.
    #[serde(default)]
    pub prefix: String,
}

/// What a grant allows, each level including the ones before it.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Access {
    /// Reading objects and listing them.
    Read,
    /// Writing and deleting objects.
    #[default]
    Write,
    /// Changing and deleting the bucket: its policy, lifecycle and other
    /// settings.
    Admin,
}

impl Client {
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

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub block_size: u64,
    pub chunk_blocks: u64,
    /// Bytes per extent, the unit of disk a size class takes, a multiple of
    /// the block size: by default one block, so a class that needs room
    /// frees at most a block's worth.
    pub extent_size: Option<u64>,
    /// Extents in the slab file.
    pub extents: u32,
    pub min_slot: u64,
    pub doorkeeper_window: u64,
    pub fill_budget: u64,
    pub metadata_capacity: usize,
    /// Objects whose metadata the gateway keeps, and how long it keeps
    /// metadata of objects that may change, in milliseconds.
    pub gateway_metadata_capacity: usize,
    pub gateway_metadata_ttl_ms: u64,
    /// How long a gateway keeps metadata of immutable buckets, so a purge
    /// reaches every gateway within it.
    pub purge_window_ms: u64,
    /// Bytes of a response's body a gateway asks nodes for ahead of the
    /// part it forwards: a miss holds at most this much of each owner's
    /// fill budget.
    pub read_ahead: u64,
    /// How long a node waits for S3, and a gateway for a node, before
    /// giving up; and how long a gateway routes around a node that timed
    /// out.
    pub origin_timeout_ms: u64,
    pub node_timeout_ms: u64,
    pub suspect_ttl_ms: u64,
    /// How long after a ring change a node asks previous owners for
    /// blocks first, and how long a previous owner has to answer.
    pub fallback_window_ms: u64,
    pub peer_timeout_ms: u64,
    /// A placement its owner reads `hot_threshold` times within
    /// `hot_window_ms` is leased to its next `hot_replicas` candidates for
    /// `lease_ms`, and gateways spread its reads across them; 0 turns
    /// leases off.
    pub hot_threshold: u64,
    pub hot_window_ms: u64,
    pub hot_replicas: usize,
    pub lease_ms: u64,
    pub default_policy: PolicyConfig,
    pub buckets: BTreeMap<String, PolicyConfig>,
}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        CacheConfig {
            block_size: 1 << 20,
            chunk_blocks: 16,
            extent_size: None,
            extents: 256,
            min_slot: 4 << 10,
            doorkeeper_window: 100_000,
            fill_budget: 256 << 20,
            metadata_capacity: 100_000,
            gateway_metadata_capacity: 100_000,
            gateway_metadata_ttl_ms: 1_000,
            purge_window_ms: 3_600_000,
            read_ahead: 64 << 20,
            origin_timeout_ms: 60_000,
            node_timeout_ms: 150_000,
            suspect_ttl_ms: 10_000,
            fallback_window_ms: 600_000,
            peer_timeout_ms: 1_000,
            hot_threshold: 1_000,
            hot_window_ms: 1_000,
            hot_replicas: 2,
            lease_ms: 10_000,
            default_policy: PolicyConfig::default(),
            buckets: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyConfig {
    /// Objects never change once written.
    pub immutable: bool,
    /// Otherwise, how long metadata stays fresh.
    pub ttl_ms: u64,
    pub admit_on_first_read: bool,
    /// The home stores what it holds of each upload that passes through
    /// it, so the first read hits.
    pub warm_on_write: bool,
}

impl Default for PolicyConfig {
    fn default() -> PolicyConfig {
        PolicyConfig {
            immutable: false,
            ttl_ms: 5_000,
            admit_on_first_read: false,
            warm_on_write: false,
        }
    }
}

impl PolicyConfig {
    fn policy(self) -> BucketPolicy {
        BucketPolicy {
            freshness: match self.immutable {
                true => Freshness::Immutable,
                false => Freshness::Ttl(self.ttl_ms),
            },
            admit_on_first_read: self.admit_on_first_read,
            warm_on_write: self.warm_on_write,
        }
    }
}

impl CacheConfig {
    /// Settings the core would misbehave under.
    /// Bytes per extent: one block unless the config names another size.
    pub fn extent_size(&self) -> u64 {
        self.extent_size.unwrap_or(self.block_size)
    }

    pub fn check(&self) -> Result<(), String> {
        for (name, size) in [("block_size", self.block_size), ("min_slot", self.min_slot)] {
            if !size.is_power_of_two() {
                return Err(format!("cache.{name} ({size}) must be a power of two"));
            }
        }
        if self.min_slot > self.block_size {
            return Err(format!(
                "cache.min_slot ({}) must be at most cache.block_size ({})",
                self.min_slot, self.block_size
            ));
        }
        let extent = self.extent_size();
        if extent == 0 || !extent.is_multiple_of(self.block_size) {
            return Err(format!(
                "cache.extent_size ({extent}) must be a multiple of cache.block_size ({})",
                self.block_size
            ));
        }
        if self.node_timeout_ms < 2 * self.origin_timeout_ms {
            return Err(format!(
                "node_timeout_ms ({}) must be at least twice origin_timeout_ms ({}): a node may wait out one S3 timeout and fetch again",
                self.node_timeout_ms, self.origin_timeout_ms
            ));
        }
        Ok(())
    }

    pub fn gateway_config(&self) -> gateway::Config {
        let node = self.node_config();
        gateway::Config {
            layout: node.layout,
            default_policy: node.default_policy,
            buckets: node.buckets,
            metadata_capacity: self.gateway_metadata_capacity,
            metadata_ttl: self.gateway_metadata_ttl_ms,
            purge_window: self.purge_window_ms,
            read_ahead: self.read_ahead,
            node_timeout: self.node_timeout_ms,
            suspect_ttl: self.suspect_ttl_ms,
        }
    }

    pub fn node_config(&self) -> node::Config {
        node::Config {
            layout: Layout::new(self.block_size, self.chunk_blocks),
            store: StoreConfig {
                extent_size: self.extent_size(),
                extents: self.extents,
                min_slot: self.min_slot,
                max_slot: self.block_size,
            },
            doorkeeper_window: self.doorkeeper_window,
            fill_budget: self.fill_budget,
            metadata_capacity: self.metadata_capacity,
            origin_timeout: self.origin_timeout_ms,
            fallback_window: self.fallback_window_ms,
            peer_timeout: self.peer_timeout_ms,
            hot_threshold: self.hot_threshold,
            hot_window: self.hot_window_ms,
            hot_replicas: self.hot_replicas,
            lease: self.lease_ms,
            default_policy: self.default_policy.policy(),
            buckets: self
                .buckets
                .iter()
                .map(|(bucket, policy)| (bucket.clone(), policy.policy()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A size class takes disk one block at a time, so a class that needs
    /// room frees at most a block's worth, whatever the block size.
    #[test]
    fn an_extent_holds_one_block_by_default() {
        let mut cache = CacheConfig::default();
        assert_eq!(cache.node_config().store.extent_size, cache.block_size);
        cache.block_size = 4 << 20;
        assert!(cache.check().is_ok());
        assert_eq!(cache.node_config().store.extent_size, 4 << 20);
    }

    #[test]
    fn the_store_geometry_is_checked() {
        let with = |change: fn(&mut CacheConfig)| {
            let mut cache = CacheConfig::default();
            change(&mut cache);
            cache.check()
        };
        assert!(with(|cache| cache.extent_size = Some(3 << 19)).is_err());
        assert!(with(|cache| cache.block_size = 3 << 20).is_err());
        assert!(with(|cache| cache.min_slot = 2 << 20).is_err());
        assert!(with(|cache| cache.extent_size = Some(64 << 20)).is_ok());
    }

    #[test]
    fn parses_a_minimal_config() {
        let config: Config = toml::from_str(
            r#"
            [origin]
            endpoint = "http://127.0.0.1:8080"
            region = "us-east-1"
            access_key_id = "origin"
            secret_access_key = "secret"

            [[clients]]
            access_key_id = "reader"
            secret_access_key = "secret"
            grants = [{ bucket = "logs", prefix = "2026/" }]

            [cache.buckets.parquet]
            immutable = true

            [cluster]
            secret = "cluster-secret"
            nodes = [{ id = 0, address = "127.0.0.1:9100" }]

            [gateway]
            listen = "127.0.0.1:9000"

            [node]
            id = 0
            data_dir = "/tmp/cache"
            "#,
        )
        .unwrap();
        let client = &config.clients[0];
        assert!(client.may(Access::Write, "logs", "2026/01/a"));
        assert!(!client.may(Access::Admin, "logs", "2026/01/a"));
        assert!(!client.may(Access::Read, "logs", "2025/12/a"));
        assert!(!client.may(Access::Read, "other", "2026/01/a"));
        assert!(client.may_reach(Access::Write, "logs"));
        assert_eq!(config.check(), Ok(()));
        let node = config.cache.node_config();
        assert_eq!(node.buckets["parquet"].freshness, Freshness::Immutable);
        assert_eq!(node.default_policy.freshness, Freshness::Ttl(5_000));
    }
}
