//! The server's TOML configuration.

use s3_accelerator_core::layout::Layout;
use s3_accelerator_core::node::{self, BucketPolicy, Freshness};
use s3_accelerator_core::store::StoreConfig;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where clients connect, such as `127.0.0.1:9000`.
    pub listen: String,
    pub origin: OriginConfig,
    pub clients: Vec<Client>,
    /// The largest body the server holds in memory, uploaded or fetched.
    #[serde(default = "default_max_body")]
    pub max_body: u64,
    #[serde(default)]
    pub cache: CacheConfig,
}

fn default_max_body() -> u64 {
    1 << 30
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
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
    /// A bucket name, or `*` for every bucket.
    pub bucket: String,
    /// Keys the grant covers start with this.
    #[serde(default)]
    pub prefix: String,
}

impl Client {
    /// Whether a grant covers `key` in `bucket`. Requests about a bucket
    /// rather than an object pass an empty key.
    pub fn may_access(&self, bucket: &str, key: &str) -> bool {
        self.grants.iter().any(|grant| {
            (grant.bucket == "*" || grant.bucket == bucket) && key.starts_with(&grant.prefix)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub block_size: u64,
    pub chunk_blocks: u64,
    pub extent_size: u64,
    /// Extents of memory this node caches in.
    pub extents: u32,
    pub min_slot: u64,
    pub doorkeeper_window: u64,
    pub fill_budget: u64,
    pub metadata_capacity: usize,
    pub default_policy: PolicyConfig,
    pub buckets: BTreeMap<String, PolicyConfig>,
}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        CacheConfig {
            block_size: 1 << 20,
            chunk_blocks: 16,
            extent_size: 64 << 20,
            extents: 4,
            min_slot: 4 << 10,
            doorkeeper_window: 100_000,
            fill_budget: 64 << 20,
            metadata_capacity: 100_000,
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
}

impl Default for PolicyConfig {
    fn default() -> PolicyConfig {
        PolicyConfig {
            immutable: false,
            ttl_ms: 5_000,
            admit_on_first_read: false,
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
        }
    }
}

impl CacheConfig {
    pub fn node_config(&self) -> node::Config {
        node::Config {
            layout: Layout::new(self.block_size, self.chunk_blocks),
            store: StoreConfig {
                extent_size: self.extent_size,
                extents: self.extents,
                min_slot: self.min_slot,
                max_slot: self.block_size,
            },
            doorkeeper_window: self.doorkeeper_window,
            fill_budget: self.fill_budget,
            metadata_capacity: self.metadata_capacity,
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

    #[test]
    fn parses_a_minimal_config() {
        let config: Config = toml::from_str(
            r#"
            listen = "127.0.0.1:9000"

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
            "#,
        )
        .unwrap();
        let client = &config.clients[0];
        assert!(client.may_access("logs", "2026/01/a"));
        assert!(!client.may_access("logs", "2025/12/a"));
        assert!(!client.may_access("other", "2026/01/a"));
        let node = config.cache.node_config();
        assert_eq!(node.buckets["parquet"].freshness, Freshness::Immutable);
        assert_eq!(node.default_policy.freshness, Freshness::Ttl(5_000));
    }
}
