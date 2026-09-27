//! The admin listener: metrics that agree with what the fake S3 logged and
//! what clients received, and readiness that waits for the node to join.

mod common;

use common::{S3_REQUEST_ID, SIZE, Server, admin, data_dir, sample, send, send_for_headers};
use common::{start, start_origin};
use s3_accelerator::config::Config;
use s3_accelerator::http::header;
use s3_accelerator::server::{self, Listeners};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::LocalSet;

#[tokio::test(flavor = "current_thread")]
async fn metrics_agree_with_s3s_log_and_the_clients_bytes() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let dir = data_dir();
            let server = Server::start(origin_port, &dir, r#"{ bucket = "bucket" }"#, "").await;
            let port = server.port;
            // A first fetch, a fill the doorkeeper now admits, and, once
            // the fill's blocks are on disk, a hit.
            let mut received = 0;
            for read in 0..3 {
                if read == 2 {
                    written_bytes(server.admin_port, SIZE as u64).await;
                }
                let (status, body) = send(port, "GET", "/bucket/k", "", &[], Vec::new()).await;
                assert_eq!(status, 200);
                received += body.len() as u64;
            }
            let written = send(port, "PUT", "/bucket/new", "", &[], b"new".to_vec()).await;
            assert_eq!(written.0, 200);
            let refused = send(port, "GET", "/other/k", "", &[], Vec::new()).await;
            assert_eq!(refused.0, 403);
            let (status, scrape) = admin(server.admin_port, "/metrics").await;
            assert_eq!(status, 200);
            let value = |name: &str, labels: &str| sample(&scrape, name, labels) as u64;
            let requests = "s3accel_gateway_requests_total";
            assert_eq!(value(requests, r#"operation="GetObject",code="200""#), 3);
            assert_eq!(value(requests, r#"operation="PutObject",code="200""#), 1);
            assert_eq!(value(requests, r#"operation="GetObject",code="403""#), 1);
            let bytes = "s3accel_gateway_response_bytes_total";
            let refusal = refused.1.len() as u64;
            assert_eq!(value(bytes, r#"operation="GetObject""#), received + refusal);
            let first_bytes = "s3accel_gateway_first_byte_seconds_count";
            assert_eq!(value(first_bytes, r#"operation="GetObject""#), 4);
            // S3 saw the first fetch, the fill and the write, and sent the
            // bodies the node relayed.
            let s3 = "s3accel_s3_requests_total";
            let reads =
                value(s3, r#"kind="read",code="200""#) + value(s3, r#"kind="read",code="206""#);
            assert_eq!(reads, 2);
            assert_eq!(value(s3, r#"kind="forward",code="200""#), 1);
            assert_eq!(origin.requests.get(), 3);
            let body = "s3accel_node_body_bytes_total";
            assert_eq!(value(body, r#"source="s3""#), origin.sent.get());
            assert_eq!(value(body, r#"source="cache""#), received / 3);
            assert_eq!(
                value(body, r#"source="s3""#) + value(body, r#"source="cache""#),
                received
            );
            let blocks = "s3accel_node_block_reads_total";
            assert_eq!(value(blocks, r#"result="hit""#), 5);
            assert_eq!(value("s3accel_node_store_capacity_bytes", ""), 8 << 20);
            assert!(value("s3accel_node_sync_seconds_count", "") > 0);
            assert!(scrape.contains("s3accel_ring_info{version=\""));
            assert_eq!(value("s3accel_ring_nodes", r#"state="up""#), 1);
            assert!(value("process_resident_memory_bytes", "") > 0);
            server.stop().await;
        })
        .await;
}

/// Waits until the node has written `bytes` of blocks.
async fn written_bytes(admin_port: u16, bytes: u64) {
    let started = Instant::now();
    loop {
        let (_, scrape) = admin(admin_port, "/metrics").await;
        if sample(&scrape, "s3accel_node_written_bytes_total", "") as u64 >= bytes {
            return;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A seed that takes connections and never answers holds a starting node
/// for the ring wait, and the node is unready until it joins.
#[tokio::test(flavor = "current_thread")]
async fn readiness_waits_for_the_node_to_join() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _) = start_origin().await;
            let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let silent_address = silent.local_addr().unwrap();
            let gateway = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let admin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let admin_port = admin_listener.local_addr().unwrap().port();
            let node_address = node.local_addr().unwrap();
            let config: Config = toml::from_str(&format!(
                r#"
                [origin]
                endpoint = "http://127.0.0.1:{origin_port}"
                region = "us-east-1"
                access_key_id = "origin"
                secret_access_key = "origin-secret"
                [cache]
                block_size = 65536
                extent_size = 1048576
                extents = 8
                [cluster]
                secret = "cluster-secret"
                nodes = [
                    {{ id = 0, address = "{node_address}" }},
                    {{ id = 1, address = "{silent_address}" }},
                ]
                [gateway]
                listen = "unused"
                [node]
                id = 0
                data_dir = "{}"
                "#,
                data_dir().display()
            ))
            .unwrap();
            let listeners = Listeners {
                gateway: Some(gateway),
                node: Some(node),
                admin: Some(admin_listener),
            };
            let started = Instant::now();
            tokio::task::spawn_local(server::run(config, listeners, std::future::pending()));
            assert_eq!(admin(admin_port, "/healthz").await, (200, "ok\n".into()));
            // The node recovers, then waits on the seed.
            let mut joining = false;
            loop {
                let (status, body) = admin(admin_port, "/readyz").await;
                if status == 200 {
                    assert_eq!(body, "ready\n");
                    break;
                }
                assert!(
                    body.contains("recovering") || body.contains("joining"),
                    "{body}"
                );
                joining |= body.contains("joining");
                assert!(started.elapsed() < Duration::from_secs(10), "{body}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(joining);
            // The seed held the node for the ring wait.
            assert!(started.elapsed() >= Duration::from_millis(900));
            drop(silent);
        })
        .await;
}

/// Every response names its request's ID; one the cache served names it
/// as S3's request ID too, and one S3 answered keeps S3's.
#[tokio::test(flavor = "current_thread")]
async fn responses_name_their_request_ids() {
    LocalSet::new()
        .run_until(async {
            let (port, _) = start(r#"{ bucket = "bucket" }"#, "").await;
            let (status, cached) = send_for_headers(port, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 200);
            let id = header(&cached, "x-accel-request-id").unwrap();
            assert_eq!(id.len(), 16);
            assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(header(&cached, "x-amz-request-id"), Some(id));
            let (_, again) = send_for_headers(port, "GET", "/bucket/k", Vec::new()).await;
            assert_ne!(header(&again, "x-accel-request-id"), Some(id));
            let (status, passed) = send_for_headers(port, "PUT", "/bucket/n", b"n".to_vec()).await;
            assert_eq!(status, 200);
            assert!(header(&passed, "x-accel-request-id").is_some());
            assert_eq!(header(&passed, "x-amz-request-id"), Some(S3_REQUEST_ID));
            let (status, refused) = send_for_headers(port, "GET", "/other/k", Vec::new()).await;
            assert_eq!(status, 403);
            assert!(header(&refused, "x-amz-request-id").is_some());
        })
        .await;
}
