//! Gateways look up each request's access key in the metadata service and
//! check its signature with the keys of its dates, holding no secret.

mod common;

use common::{
    Metadata, Server, admin, data_dir, invalidate_client, post_invalidation, presigned, sample,
    send, send_as, start_metadata, start_origin,
};
use s3_accelerator::lookups::{MAX_IN_FLIGHT, invalidation_signature};
use s3_accelerator::metadata_service::{self, MetadataService, ServiceConfig};
use s3_accelerator::sigv4;
use serde_json::json;
use std::time::Duration;
use tokio::task::LocalSet;

const CACHE: &str = "block_size = 65536\nextent_size = 1048576\nextents = 8";

/// Starts a server that asks the fake service at `port`, which serves
/// `bucket` from the S3 at `origin_port`.
async fn start_asking(port: u16, metadata: &Metadata, origin_port: u16, settings: &str) -> Server {
    metadata.serve("bucket", origin_port, "origin");
    let tables = Metadata::table(port, settings);
    Server::start_config(&tables, &data_dir(), "", CACHE).await
}

async fn get(server: &Server, path: &str) -> u16 {
    send(server.port, "GET", path, "", &[], Vec::new()).await.0
}

async fn scrape(server: &Server, name: &str, labels: &str) -> f64 {
    sample(&admin(server.admin_port, "/metrics").await.1, name, labels)
}

#[tokio::test(flavor = "current_thread")]
async fn a_key_found_once_serves_concurrent_requests_with_one_lookup() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.client_delay.set(Duration::from_millis(200));
            let server = start_asking(port, &metadata, origin_port, "").await;
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
            assert_eq!(get(&server, "/bucket/k").await, 200);
            assert_eq!(metadata.client_lookups.get(), 1);
            let found = "kind=\"client\",result=\"found\"";
            assert_eq!(
                scrape(&server, "s3accel_metadata_lookups_total", found).await,
                1.0
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_unknown_key_gets_403_and_is_remembered() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, &metadata, origin_port, "").await;
            let stranger = ("stranger", "stranger-secret");
            let (status, body) =
                send_as(server.port, stranger, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 403);
            assert!(String::from_utf8_lossy(&body).contains("InvalidAccessKeyId"));
            let (status, _) = send_as(server.port, stranger, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 403);
            assert_eq!(metadata.client_lookups.get(), 1);
            assert_eq!(origin.requests.get(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_key_named_with_any_characters_can_be_revoked() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve_client("team+ci=1", "team-secret", json!([{ "bucket": "*" }]));
            let server = start_asking(port, &metadata, origin_port, "").await;
            let team = ("team+ci=1", "team-secret");
            let read = || send_as(server.port, team, "GET", "/bucket/k", Vec::new());
            assert_eq!(read().await.0, 200);
            metadata.clients.borrow_mut().remove("team+ci=1");
            assert_eq!(invalidate_client(server.admin_port, "team+ci=1").await, 204);
            assert_eq!(read().await.0, 403);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_request_that_fails_its_time_costs_no_lookup() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, &metadata, origin_port, "").await;
            let host = format!("127.0.0.1:{}", server.port);
            let expired = presigned(server.port, &host, ("GET", "/bucket/k", ""), 7_200, 3_600);
            let response = reqwest::get(expired).await.unwrap();
            assert_eq!(response.status().as_u16(), 403);
            assert!(
                response
                    .text()
                    .await
                    .unwrap()
                    .contains("RequestTimeTooSkewed")
            );
            assert_eq!(metadata.client_lookups.get(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_key_the_service_knows_signs_with_its_secret_alone() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.serve_client("writer", "writer-secret", json!([{ "bucket": "*" }]));
            let server = start_asking(port, &metadata, origin_port, "").await;
            let writer = ("writer", "writer-secret");
            let sent = send_as(server.port, writer, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(sent.0, 200);
            let forged = ("writer", "another-secret");
            let (status, body) = send_as(server.port, forged, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 403);
            assert!(String::from_utf8_lossy(&body).contains("SignatureDoesNotMatch"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_revoked_key_fails_the_next_request_after_its_invalidation() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, &metadata, origin_port, "").await;
            assert_eq!(get(&server, "/bucket/k").await, 200);
            metadata.clients.borrow_mut().remove("reader");
            // Until the service pushes the change, the gateway keeps the key.
            assert_eq!(get(&server, "/bucket/k").await, 200);
            assert_eq!(invalidate_client(server.admin_port, "reader").await, 204);
            let (status, body) = send(server.port, "GET", "/bucket/k", "", &[], Vec::new()).await;
            assert_eq!(status, 403);
            assert!(String::from_utf8_lossy(&body).contains("InvalidAccessKeyId"));
            assert_eq!(metadata.client_lookups.get(), 2);
            let taken = scrape(
                &server,
                "s3accel_metadata_invalidations_total",
                "kind=\"client\"",
            );
            assert_eq!(taken.await, 1.0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn grants_from_the_service_limit_what_a_key_reaches() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let grants = json!([{ "bucket": "bucket", "prefix": "public/", "access": "read" }]);
            metadata.serve_client("limited", "limited-secret", grants);
            let server = start_asking(port, &metadata, origin_port, "").await;
            let limited = ("limited", "limited-secret");
            let read = |path: &'static str| send_as(server.port, limited, "GET", path, Vec::new());
            assert_eq!(read("/bucket/public/a").await.0, 200);
            assert_eq!(read("/bucket/private/a").await.0, 403);
            let body = b"x".to_vec();
            let written = send_as(server.port, limited, "PUT", "/bucket/public/a", body).await;
            assert_eq!(written.0, 403);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_presigned_url_six_days_old_checks_out_with_the_services_keys() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, &metadata, origin_port, "").await;
            let host = format!("127.0.0.1:{}", server.port);
            let week = 7 * 24 * 60 * 60;
            let url = presigned(
                server.port,
                &host,
                ("GET", "/bucket/k", ""),
                6 * 86_400,
                week,
            );
            assert_eq!(reqwest::get(url).await.unwrap().status().as_u16(), 200);
            // Signed with another secret, the same URL fails.
            let url = presigned(server.port, &host, ("GET", "/bucket/k", ""), 60, 3_600);
            let forged = url.replace("X-Amz-Signature=", "X-Amz-Signature=0");
            assert_eq!(reqwest::get(forged).await.unwrap().status().as_u16(), 403);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_failing_service_leaves_known_keys_working_through_the_grace() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.client_ttl_ms.set(1_000);
            let server = start_asking(port, &metadata, origin_port, "grace_ms = 2000").await;
            assert_eq!(get(&server, "/bucket/k").await, 200);
            metadata.clients_failing.set(true);
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            assert_eq!(get(&server, "/bucket/k").await, 200);
            let stale = scrape(&server, "s3accel_metadata_stale_total", "kind=\"client\"");
            assert_eq!(stale.await, 1.0);
            tokio::time::sleep(Duration::from_millis(2_000)).await;
            let (status, body) = send(server.port, "GET", "/bucket/k", "", &[], Vec::new()).await;
            assert_eq!(status, 503);
            assert!(String::from_utf8_lossy(&body).contains("ServiceUnavailable"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lookups_past_the_cap_get_slow_down() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            metadata.client_ttl_ms.set(1_000);
            let server = start_asking(port, &metadata, origin_port, "").await;
            // The gateway knows the reader's key, past its TTL by the time
            // made-up keys fill the cap.
            assert_eq!(get(&server, "/bucket/k").await, 200);
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            metadata.client_delay.set(Duration::from_millis(1_000));
            let over = 6;
            let requests = (0..MAX_IN_FLIGHT + over).map(|index| {
                let port = server.port;
                tokio::task::spawn_local(async move {
                    let (id, secret) = (format!("made-up-{index}"), "secret");
                    let sent = send_as(port, (&id, secret), "GET", "/bucket/k", Vec::new());
                    let (status, body) = sent.await;
                    (status, String::from_utf8_lossy(&body).contains("SlowDown"))
                })
            });
            let requests: Vec<_> = requests.collect();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let known = get(&server, "/bucket/k");
            assert_eq!(known.await, 200);
            let mut answers = Vec::new();
            for request in requests {
                answers.push(request.await.unwrap());
            }
            let slowed = answers
                .iter()
                .filter(|(status, slow)| *status == 503 && *slow);
            assert_eq!(slowed.count(), over);
            let refused = answers.iter().filter(|(status, _)| *status == 403);
            assert_eq!(refused.count(), MAX_IN_FLIGHT);
            assert_eq!(metadata.client_lookups.get(), MAX_IN_FLIGHT as u64 + 2);
            let labels = "kind=\"client\",result=\"refused\"";
            let counted = scrape(&server, "s3accel_metadata_lookups_total", labels).await;
            assert_eq!(counted, over as f64);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_forged_client_invalidation_changes_nothing() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let (port, metadata) = start_metadata().await;
            let server = start_asking(port, &metadata, origin_port, "").await;
            assert_eq!(get(&server, "/bucket/k").await, 200);
            let (path, now) = ("/clients/reader/invalidate", sigv4::unix_now());
            let forged = invalidation_signature("another-token", path, now);
            let status = post_invalidation(server.admin_port, path, now, &forged).await;
            assert_eq!(status, 403);
            // A bucket's signature takes no client's invalidation.
            let other =
                invalidation_signature(common::METADATA_TOKEN, "/origins/reader/invalidate", now);
            let status = post_invalidation(server.admin_port, path, now, &other).await;
            assert_eq!(status, 403);
            assert_eq!(get(&server, "/bucket/k").await, 200);
            assert_eq!(metadata.client_lookups.get(), 1);
            // A gateway whose config names its clients takes no invalidation.
            let static_server =
                Server::start(origin_port, &data_dir(), r#"{ bucket = "*" }"#, "").await;
            assert_eq!(
                invalidate_client(static_server.admin_port, "reader").await,
                404
            );
        })
        .await;
}

/// A reload that changes a key's grants, or removes the key, reaches the
/// gateway before its TTL ends.
#[tokio::test(flavor = "current_thread")]
async fn a_reload_that_changes_or_revokes_a_key_reaches_the_gateway() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let config = |admin_port: u16, reader: &str| -> ServiceConfig {
                toml::from_str(&format!(
                    r#"
                    listen = "127.0.0.1:{port}"
                    token = "{}"
                    gateways = ["127.0.0.1:{admin_port}"]
                    [default]
                    endpoint = "http://127.0.0.1:{origin_port}"
                    region = "us-east-1"
                    access_key_id = "origin"
                    secret_access_key = "origin-secret"
                    {reader}
                    "#,
                    common::METADATA_TOKEN
                ))
                .unwrap()
            };
            let writer = "[clients.reader]\nsecret_access_key = \"reader-secret\"\n\
                          grants = [{ bucket = \"*\" }]";
            let reader_only = "[clients.reader]\nsecret_access_key = \"reader-secret\"\n\
                               grants = [{ bucket = \"*\", access = \"read\" }]";
            let server =
                Server::start_config(&Metadata::table(port, ""), &data_dir(), "", CACHE).await;
            let service = MetadataService::new(config(server.admin_port, writer));
            tokio::task::spawn_local(metadata_service::serve(listener, service.clone()));
            let put = || send(server.port, "PUT", "/bucket/k", "", &[], b"x".to_vec());
            assert_eq!(put().await.0, 200);
            let changed = service.reload(config(server.admin_port, reader_only)).await;
            assert_eq!(changed.unwrap().clients, ["reader"]);
            assert_eq!(put().await.0, 403);
            assert_eq!(get(&server, "/bucket/k").await, 200);
            let changed = service.reload(config(server.admin_port, "")).await;
            assert_eq!(changed.unwrap().clients, ["reader"]);
            assert_eq!(get(&server, "/bucket/k").await, 403);
            // The service pushes only names it has served: a key it didn't
            // know when asked is left to the gateway's unknown TTL.
            let stranger = ("stranger", "stranger-secret");
            let (status, _) = send_as(server.port, stranger, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 403);
            let added = "[clients.stranger]\nsecret_access_key = \"stranger-secret\"\n\
                         grants = [{ bucket = \"*\" }]";
            let changed = service.reload(config(server.admin_port, added)).await;
            assert_eq!(changed.unwrap().clients, Vec::<String>::new());
        })
        .await;
}
