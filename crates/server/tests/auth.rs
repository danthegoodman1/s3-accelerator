//! Authentication and grants, end to end.

mod common;

use common::{send, signed, start};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::LocalSet;

/// Sends a request head and no body, and reads until the server closes.
/// A server that waits for the body fails the test instead of hanging it.
async fn head_only(port: u16, head: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let read = stream.read_to_end(&mut response);
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("the server answers from the head alone")
        .unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test(flavor = "current_thread")]
async fn a_copy_needs_a_grant_on_its_source() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket", prefix = "public/" }"#, "").await;
            let copy = |source: &'static str| {
                let extra = [("x-amz-copy-source", source)];
                async move {
                    send(port, "PUT", "/bucket/public/copy", "", &extra, Vec::new())
                        .await
                        .0
                }
            };
            assert_eq!(copy("bucket/secret/payroll.csv").await, 403);
            assert_eq!(copy("/other/public/a").await, 403);
            assert_eq!(origin.requests.get(), 0);
            assert_eq!(copy("bucket/public/a").await, 200);
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

/// An unsigned request that claims a 1 TiB body is refused from its head,
/// and the server goes on serving.
#[tokio::test(flavor = "current_thread")]
async fn an_unauthenticated_body_is_never_read() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let head = "PUT /bucket/k HTTP/1.1\r\nHost: x\r\nContent-Length: 1099511627776\r\n\r\n";
            let response = head_only(port, head).await;
            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
            assert_eq!(origin.requests.get(), 0);
            assert_eq!(
                send(port, "GET", "/bucket/k", "", &[], Vec::new()).await.0,
                200
            );
        })
        .await;
}

/// A signed head sent over a raw socket, so no client rewrites its path.
fn raw_head(port: u16, method: &str, path: &str, extra: &[(&str, &str)]) -> String {
    let mut head = format!("{method} {path} HTTP/1.1\r\n");
    for (name, value) in signed(port, method, path, "", extra) {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");
    head
}

/// Keys with `.` or `..` segments are refused until the origin client sends
/// paths as written, so they never reach S3 as some other key.
#[tokio::test(flavor = "current_thread")]
async fn dot_segment_keys_are_refused() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            for path in ["/bucket/a/../b", "/bucket/a/%2E%2E/b", "/bucket/./c"] {
                let response = head_only(port, &raw_head(port, "GET", path, &[])).await;
                assert!(response.starts_with("HTTP/1.1 501"), "{path}: {response}");
            }
            assert_eq!(origin.requests.get(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_oversized_body_is_refused_before_it_is_read() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "max_body = 1024").await;
            let head = raw_head(port, "PUT", "/bucket/k", &[("content-length", "4096")]);
            let response = head_only(port, &head).await;
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(response.contains("EntityTooLarge"), "{response}");
            assert_eq!(origin.requests.get(), 0);
        })
        .await;
}
