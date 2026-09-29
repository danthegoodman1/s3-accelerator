//! Each bucket reaches its own origin: one the config names, or one the
//! metadata service names, which the node keeps for the service's TTL.

mod common;

use common::{
    Metadata, Server, admin, data_dir, invalidate, post_invalidation, sample, send, start_metadata,
    start_origin, start_queue,
};
use s3_accelerator::lookups::invalidation_signature;
use s3_accelerator::metadata_service::{self, MetadataService, ServiceConfig};
use s3_accelerator::sigv4;
use std::time::{Duration, Instant};
use tokio::task::LocalSet;

const CACHE: &str = "block_size = 65536\nextent_size = 1048576\nextents = 8";
const EVERY_BUCKET: &str = r#"{ bucket = "*" }"#;

async fn get(server: &Server, path: &str) -> (u16, Vec<u8>) {
    send(server.port, "GET", path, "", &[], Vec::new()).await
}

/// Starts a server that asks the fake metadata service at `port`, whose
/// `[metadata]` table also holds `settings`.
async fn start_asking(port: u16, settings: &str) -> Server {
    let tables = Metadata::table(port, settings);
    Server::start_config(&tables, &data_dir(), "", CACHE).await
}

async fn scrape(server: &Server, name: &str, labels: &str) -> f64 {
    sample(&admin(server.admin_port, "/metrics").await.1, name, labels)
}

#[tokio::test(flavor = "current_thread")]
async fn buckets_reach_their_own_origins_with_their_own_keys() {
    LocalSet::new()
        .run_until(async {
            let (default_port, default) = start_origin().await;
            let (other_port, other) = start_origin().await;
            let origins = format!(
                r#"
                [origin]
                endpoint = "http://127.0.0.1:{default_port}"
                region = "us-east-1"
                access_key_id = "default-key"
                secret_access_key = "default-secret"
                [origins.other]
                endpoint = "http://127.0.0.1:{other_port}"
                region = "us-east-1"
                access_key_id = "other-key"
                secret_access_key = "other-secret"
                "#
            );
            let server =
                Server::start_origins(&origins, &data_dir(), EVERY_BUCKET, "", CACHE).await;
            assert_eq!(get(&server, "/bucket/k").await.0, 200);
            assert_eq!(get(&server, "/other/k").await.0, 200);
            let (status, _) = send(server.port, "PUT", "/other/new", "", &[], b"x".to_vec()).await;
            assert_eq!(status, 200);
            // Requests naming no bucket go to the default origin.
            assert_eq!(get(&server, "/").await.0, 200);
            assert_eq!(*default.paths.borrow(), ["/bucket/k", "/"]);
            assert!(default.keys.borrow().iter().all(|key| key == "default-key"));
            assert_eq!(*other.paths.borrow(), ["/other/k", "/other/new"]);
            assert!(other.keys.borrow().iter().all(|key| key == "other-key"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_bucket_no_origin_serves_gets_no_such_bucket() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start_origin().await;
            let origins = format!(
                r#"
                [origins.bucket]
                endpoint = "http://127.0.0.1:{port}"
                region = "us-east-1"
                access_key_id = "origin"
                secret_access_key = "origin-secret"
                "#
            );
            let server =
                Server::start_origins(&origins, &data_dir(), EVERY_BUCKET, "", CACHE).await;
            let (status, body) = get(&server, "/missing/k").await;
            assert_eq!(status, 404);
            assert!(String::from_utf8_lossy(&body).contains("NoSuchBucket"));
            let (status, body) =
                send(server.port, "PUT", "/missing/k", "", &[], b"x".to_vec()).await;
            assert_eq!(status, 404);
            assert!(String::from_utf8_lossy(&body).contains("NoSuchBucket"));
            // Without a default origin, a request naming no bucket has none.
            let (status, body) = get(&server, "/").await;
            assert_eq!(status, 501);
            assert!(String::from_utf8_lossy(&body).contains("NotImplemented"));
            assert_eq!(origin.requests.get(), 0);
            let unknown = scrape(
                &server,
                "s3accel_origin_unresolved_total",
                "reason=\"unknown\"",
            );
            assert_eq!(unknown.await, 2.0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn requests_waiting_on_a_bucket_share_one_lookup() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            metadata.delay.set(Duration::from_millis(300));
            let server = start_asking(port, "").await;
            let reads = (0..10).map(|index| {
                let port = server.port;
                tokio::task::spawn_local(async move {
                    let path = format!("/bucket/k{index}");
                    send(port, "GET", &path, "", &[], Vec::new()).await.0
                })
            });
            for read in reads.collect::<Vec<_>>() {
                assert_eq!(read.await.unwrap(), 200);
            }
            assert_eq!(metadata.lookups.get(), 1);
            assert_eq!(origin.requests.get(), 10);
            assert!(origin.keys.borrow().iter().all(|key| key == "looked-up"));
            let found = scrape(
                &server,
                "s3accel_metadata_lookups_total",
                "kind=\"origin\",result=\"found\"",
            );
            assert_eq!(found.await, 1.0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_entry_past_half_its_ttl_refreshes_without_holding_up_requests() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            metadata.ttl_ms.set(2_000);
            let server = start_asking(port, "").await;
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            // Within half the TTL, requests use the entry as it is.
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            assert_eq!(metadata.lookups.get(), 1);
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            metadata.delay.set(Duration::from_secs(1));
            let reading = Instant::now();
            assert_eq!(get(&server, "/bucket/k2").await.0, 200);
            assert!(reading.elapsed() < Duration::from_millis(500));
            common::eventually(|| metadata.lookups.get() == 2).await;
            assert_eq!(get(&server, "/bucket/k3").await.0, 200);
            assert_eq!(metadata.lookups.get(), 2);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_service_leaves_requests_on_the_old_entry_until_the_grace_ends() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            metadata.ttl_ms.set(1_000);
            let server = start_asking(port, "grace_ms = 2000").await;
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            metadata.failing.set(true);
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            // Within a second of the failure, requests ask no lookup.
            let lookups = metadata.lookups.get();
            assert_eq!(get(&server, "/bucket/k2").await.0, 200);
            assert_eq!(metadata.lookups.get(), lookups);
            let stale = scrape(&server, "s3accel_metadata_stale_total", "kind=\"origin\"");
            assert_eq!(stale.await, 2.0);
            // Past the TTL and the grace, the bucket has no usable entry.
            tokio::time::sleep(Duration::from_millis(2_000)).await;
            assert_eq!(get(&server, "/bucket/k3").await.0, 503);
            let (status, _) = send(server.port, "PUT", "/bucket/k", "", &[], b"x".to_vec()).await;
            assert_eq!(status, 503);
            let failed = scrape(
                &server,
                "s3accel_metadata_lookups_total",
                "kind=\"origin\",result=\"failed\"",
            );
            assert!(failed.await >= 2.0);
            // Once the service answers again, so does the bucket.
            metadata.failing.set(false);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(get(&server, "/bucket/k4").await.0, 200);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn reads_past_the_cap_on_origin_lookups_get_503() {
    LocalSet::new()
        .run_until(async {
            let (port, metadata) = start_metadata().await;
            metadata.delay.set(Duration::from_millis(1_000));
            let server = start_asking(port, "").await;
            let over = 4;
            let reads = (0..s3_accelerator::lookups::MAX_IN_FLIGHT + over).map(|index| {
                let port = server.port;
                tokio::task::spawn_local(async move {
                    let path = format!("/missing-{index}/k");
                    send(port, "GET", &path, "", &[], Vec::new()).await.0
                })
            });
            let mut answers = Vec::new();
            for read in reads.collect::<Vec<_>>() {
                answers.push(read.await.unwrap());
            }
            let refused = answers.iter().filter(|status| **status == 503);
            assert_eq!(refused.count(), over);
            let unknown = answers.iter().filter(|status| **status == 404);
            assert_eq!(unknown.count(), s3_accelerator::lookups::MAX_IN_FLIGHT);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_unknown_bucket_gets_404_and_is_remembered() {
    LocalSet::new()
        .run_until(async {
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, "unknown_ttl_ms = 1000").await;
            let (status, body) = get(&server, "/missing/k0").await;
            assert_eq!(status, 404);
            assert!(String::from_utf8_lossy(&body).contains("NoSuchBucket"));
            assert_eq!(get(&server, "/missing/k1").await.0, 404);
            assert_eq!(metadata.lookups.get(), 1);
            // Once the service serves it and the node forgets, it's found.
            let (origin_port, _origin) = start_origin().await;
            metadata.serve("missing", origin_port, "looked-up");
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            assert_eq!(get(&server, "/missing/k2").await.0, 200);
            assert_eq!(metadata.lookups.get(), 2);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_bucket_the_service_stops_serving_stops_serving_its_cached_objects() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            let origins = Metadata::table(port, "");
            let admit = "[cache.buckets.bucket]\nadmit_on_first_read = true";
            let dir = data_dir();
            let server = Server::start_config(&origins, &dir, admit, CACHE).await;
            assert_eq!(get(&server, "/bucket/k").await.0, 200);
            assert_eq!(get(&server, "/bucket/k").await.0, 200);
            assert_eq!(origin.requests.get(), 1);
            metadata.buckets.borrow_mut().clear();
            assert_eq!(invalidate(server.admin_port, "bucket").await, 204);
            let (status, body) = get(&server, "/bucket/k").await;
            assert_eq!(status, 404);
            assert!(String::from_utf8_lossy(&body).contains("NoSuchBucket"));
            // The gateway answers a HEAD from its own metadata until the
            // entry expires, a second on.
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            let head = send(server.port, "HEAD", "/bucket/k", "", &[], Vec::new()).await;
            assert_eq!(head.0, 404);
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_reads_of_an_unknown_bucket_each_get_no_such_bucket() {
    LocalSet::new()
        .run_until(async {
            let (port, metadata) = start_metadata().await;
            metadata.delay.set(Duration::from_millis(200));
            let server = start_asking(port, "").await;
            let reads = (0..5).map(|_| {
                let port = server.port;
                tokio::task::spawn_local(async move {
                    send(port, "GET", "/missing/k", "", &[], Vec::new()).await
                })
            });
            for read in reads.collect::<Vec<_>>() {
                let (status, body) = read.await.unwrap();
                assert_eq!(status, 404);
                assert!(String::from_utf8_lossy(&body).contains("NoSuchBucket"));
            }
            assert_eq!(metadata.lookups.get(), 1);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_service_holds_up_no_request_after_the_first_failure() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            metadata.ttl_ms.set(1_000);
            let server = start_asking(port, "").await;
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            metadata.failing.set(true);
            metadata.delay.set(Duration::from_millis(1_500));
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            // The first request past the TTL waits for the lookup, which
            // fails, and goes on with the entry.
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            // Later ones go on at once while lookups keep failing.
            for index in 2..6 {
                tokio::time::sleep(Duration::from_millis(600)).await;
                let reading = Instant::now();
                assert_eq!(get(&server, &format!("/bucket/k{index}")).await.0, 200);
                assert!(reading.elapsed() < Duration::from_millis(500));
            }
            assert!(metadata.lookups.get() >= 3);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_invalidation_moves_the_next_request_to_the_new_origin() {
    LocalSet::new()
        .run_until(async {
            let (first_port, first) = start_origin().await;
            let (second_port, second) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", first_port, "first");
            let server = start_asking(port, "").await;
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            metadata.serve("bucket", second_port, "second");
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            assert_eq!(first.requests.get(), 2);
            assert_eq!(invalidate(server.admin_port, "bucket").await, 204);
            assert_eq!(get(&server, "/bucket/k2").await.0, 200);
            assert_eq!(second.requests.get(), 1);
            assert_eq!(*second.keys.borrow(), ["second"]);
            assert_eq!(metadata.lookups.get(), 2);
            let taken = scrape(
                &server,
                "s3accel_metadata_invalidations_total",
                "kind=\"origin\"",
            );
            assert_eq!(taken.await, 1.0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_lookup_an_invalidation_overtakes_is_followed_by_another() {
    LocalSet::new()
        .run_until(async {
            let (first_port, first) = start_origin().await;
            let (second_port, second) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", first_port, "first");
            metadata.delay.set(Duration::from_millis(500));
            let server = start_asking(port, "").await;
            let server_port = server.port;
            let read = tokio::task::spawn_local(async move {
                send(server_port, "GET", "/bucket/k", "", &[], Vec::new())
                    .await
                    .0
            });
            common::eventually(|| metadata.lookups.get() == 1).await;
            // The lookup under way settled on the first origin.
            metadata.serve("bucket", second_port, "second");
            assert_eq!(invalidate(server.admin_port, "bucket").await, 204);
            assert_eq!(read.await.unwrap(), 200);
            assert_eq!(metadata.lookups.get(), 2);
            assert_eq!(first.requests.get(), 0);
            assert_eq!(second.requests.get(), 1);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_forged_or_stale_invalidation_changes_nothing() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve("bucket", origin_port, "looked-up");
            let server = start_asking(port, "").await;
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            let now = sigv4::unix_now();
            let path = "/origins/bucket/invalidate";
            let forged = invalidation_signature("another-token", path, now);
            assert_eq!(
                post_invalidation(server.admin_port, path, now, &forged).await,
                403
            );
            let old = now - 301;
            let stale = invalidation_signature(common::METADATA_TOKEN, path, old);
            assert_eq!(
                post_invalidation(server.admin_port, path, old, &stale).await,
                403
            );
            let other = "/origins/other/invalidate";
            let other = invalidation_signature(common::METADATA_TOKEN, other, now);
            assert_eq!(
                post_invalidation(server.admin_port, path, now, &other).await,
                403
            );
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            assert_eq!(metadata.lookups.get(), 1);
            // A node whose config names its origins takes no invalidation.
            let (origin_port, _origin) = start_origin().await;
            let static_server = Server::start(origin_port, &data_dir(), EVERY_BUCKET, "").await;
            assert_eq!(invalidate(static_server.admin_port, "bucket").await, 404);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_asking_the_service_has_no_origin_for_requests_naming_no_bucket() {
    LocalSet::new()
        .run_until(async {
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, "").await;
            assert_eq!(get(&server, "/").await.0, 501);
            assert_eq!(metadata.lookups.get(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_event_queue_signs_with_its_own_credentials() {
    LocalSet::new()
        .run_until(async {
            let (queue_port, queue) = start_queue().await;
            let (port, _metadata) = start_metadata().await;
            let origins = format!(
                r#"
                [events]
                queue_url = "http://127.0.0.1:{queue_port}/000000000000/events"
                region = "us-east-1"
                access_key_id = "queue-key"
                secret_access_key = "queue-secret"
                {}
                "#,
                Metadata::table(port, "")
            );
            let _server = Server::start_config(&origins, &data_dir(), "", CACHE).await;
            common::eventually(|| !queue.keys.borrow().is_empty()).await;
            assert!(queue.keys.borrow().iter().all(|key| key == "queue-key"));
        })
        .await;
}

/// A node that takes no invalidation holds up a reload for its three
/// tries, once, and not for each bucket.
#[tokio::test(flavor = "current_thread")]
async fn a_reload_leaves_a_node_that_takes_no_invalidation_after_three_tries() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            // Nothing listens on the node's admin address.
            let dead = common::port();
            let config = |bucket_port: u16| -> ServiceConfig {
                toml::from_str(&format!(
                    r#"
                    listen = "127.0.0.1:{port}"
                    token = "{}"
                    nodes = ["127.0.0.1:{dead}"]
                    [default]
                    endpoint = "http://127.0.0.1:{bucket_port}"
                    region = "us-east-1"
                    access_key_id = "default"
                    secret_access_key = "default-secret"
                    [clients.reader]
                    secret_access_key = "reader-secret"
                    grants = [{{ bucket = "*" }}]
                    "#,
                    common::METADATA_TOKEN
                ))
                .unwrap()
            };
            let server = start_asking(port, "").await;
            let service = MetadataService::new(config(origin_port));
            tokio::task::spawn_local(metadata_service::serve(listener, service.clone()));
            for bucket in ["a", "b", "c"] {
                assert_eq!(get(&server, &format!("/{bucket}/k")).await.0, 200);
            }
            let (other_port, _other) = start_origin().await;
            let reloading = Instant::now();
            let changed = service.reload(config(other_port)).await.unwrap();
            assert_eq!(changed.buckets, ["a", "b", "c"]);
            // Two pauses between three tries, for the first bucket alone.
            assert!(reloading.elapsed() < Duration::from_secs(3));
        })
        .await;
}

/// The reference service answers from its file, and a reload that moves a
/// bucket to another origin reaches the node before the TTL ends.
#[tokio::test(flavor = "current_thread")]
async fn a_reload_moves_a_bucket_to_another_origin_before_its_ttl_ends() {
    LocalSet::new()
        .run_until(async {
            let (first_port, first) = start_origin().await;
            let (second_port, second) = start_origin().await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let config = |origin_port: u16, admin_port: u16| -> ServiceConfig {
                toml::from_str(&format!(
                    r#"
                    listen = "127.0.0.1:{port}"
                    token = "{}"
                    nodes = ["127.0.0.1:{admin_port}"]
                    [default]
                    endpoint = "http://127.0.0.1:{first_port}"
                    region = "us-east-1"
                    access_key_id = "default"
                    secret_access_key = "default-secret"
                    [buckets.bucket]
                    endpoint = "http://127.0.0.1:{origin_port}"
                    region = "us-east-1"
                    access_key_id = "bucket"
                    secret_access_key = "bucket-secret"
                    [clients.reader]
                    secret_access_key = "reader-secret"
                    grants = [{{ bucket = "*" }}]
                    "#,
                    common::METADATA_TOKEN
                ))
                .unwrap()
            };
            let server = start_asking(port, "").await;
            let service = MetadataService::new(config(first_port, server.admin_port));
            tokio::task::spawn_local(metadata_service::serve(listener, service.clone()));
            assert_eq!(get(&server, "/bucket/k0").await.0, 200);
            assert_eq!(get(&server, "/elsewhere/k0").await.0, 200);
            assert_eq!(*first.keys.borrow(), ["bucket", "default"]);
            let changed = service
                .reload(config(second_port, server.admin_port))
                .await
                .unwrap();
            assert_eq!(changed.buckets, ["bucket"]);
            assert_eq!(get(&server, "/bucket/k1").await.0, 200);
            assert_eq!(*second.keys.borrow(), ["bucket"]);
            assert_eq!(get(&server, "/elsewhere/k1").await.0, 200);
            assert_eq!(first.requests.get(), 3);
            // Nodes check invalidations with their own token, so a new one
            // takes a restart.
            let mut rotated = config(second_port, server.admin_port);
            rotated.token = "another-token".to_string();
            assert!(service.reload(rotated).await.is_err());
        })
        .await;
}
