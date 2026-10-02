//! Bodies stream through the gateway and node without being held whole,
//! and a slot's old pages outlive the responses still sending them.

mod common;

use common::{Server, data_dir, object_of, send, send_payload, signed, start, start_origin};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::LocalSet;

#[tokio::test(flavor = "current_thread")]
async fn a_large_upload_streams_through_to_s3() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let body = object_of(40 << 20, "upload");
            let (status, _) = send(port, "PUT", "/bucket/big", "", &[], body.clone()).await;
            assert_eq!(status, 200);
            assert!(*origin.uploads.borrow() == [body]);
        })
        .await;
}

/// S3 may refuse a write from its head. The gateway passes S3's answer on
/// and drains the rest of the body before it closes, so the client sends
/// its whole body and then reads the answer.
#[tokio::test(flavor = "current_thread")]
async fn a_write_s3_refuses_early_gets_s3s_answer() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            origin.refuse_writes.set(true);
            let body = object_of(32 << 20, "upload");
            let length = body.len().to_string();
            let mut request = "PUT /bucket/k HTTP/1.1\r\n".to_string();
            for (name, value) in
                signed(port, "PUT", "/bucket/k", "", &[("content-length", &length)])
            {
                request.push_str(&format!("{name}: {value}\r\n"));
            }
            request.push_str("\r\n");
            let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            stream
                .write_all(&body)
                .await
                .expect("the gateway takes the whole body");
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8_lossy(&response);
            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        })
        .await;
}

/// A reader queued behind a first fetch that needs only the first bytes is
/// done once they pass, and its connection to the node takes the next read
/// while the body goes on to the other reader.
#[tokio::test(flavor = "current_thread")]
async fn a_prefix_reader_frees_its_connection_before_the_body_ends() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            origin.size.set(4 << 20);
            // The body takes about 3.2 s, 64 pieces 50 ms apart, so a read
            // that waited for it takes twice the bound below, and a busy
            // machine has twice as long as it needs.
            origin.trickle.set(Duration::from_millis(50));
            let whole =
                tokio::task::spawn_local(send(port, "GET", "/bucket/big", "", &[], Vec::new()));
            tokio::time::sleep(Duration::from_millis(10)).await;
            let prefix = [("range", "bytes=0-999")];
            let (status, body) = send(port, "GET", "/bucket/big", "", &prefix, Vec::new()).await;
            assert_eq!((status, body.len()), (206, 1000));
            // The next read goes over the connection the prefix read used.
            let started = std::time::Instant::now();
            assert_eq!(
                send(port, "HEAD", "/bucket/small", "", &[], Vec::new())
                    .await
                    .0,
                200
            );
            assert!(
                !whole.is_finished(),
                "the whole body arrived too soon to tell"
            );
            let waited = started.elapsed();
            assert!(
                waited < Duration::from_millis(1_500),
                "the next read waited {waited:?}"
            );
            let (status, body) = whole.await.unwrap();
            assert_eq!(status, 200);
            assert!(body == origin.object("/bucket/big"), "the body differs");
        })
        .await;
}

/// The gateway checks a signed body's hash before its last bytes go, so S3
/// never receives the whole of a body that fails it.
#[tokio::test(flavor = "current_thread")]
async fn a_body_that_fails_its_hash_never_reaches_s3() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let body = object_of(3 << 20, "upload");
            let other = hex::encode(Sha256::digest(b"other"));
            let (status, answer) =
                send_payload(port, "PUT", "/bucket/k", "", &[], body.clone(), &other).await;
            assert_eq!(status, 400);
            let answer = String::from_utf8_lossy(&answer);
            assert!(answer.contains("XAmzContentSHA256Mismatch"), "{answer}");
            assert!(origin.uploads.borrow().is_empty());
            let digest = hex::encode(Sha256::digest(&body));
            let sent = send_payload(port, "PUT", "/bucket/k", "", &[], body.clone(), &digest);
            assert_eq!(sent.await.0, 200);
            assert!(*origin.uploads.borrow() == [body]);
        })
        .await;
}

/// An object larger than the cache streams through on its first read, and
/// reads back correctly from blocks and fills.
#[tokio::test(flavor = "current_thread")]
async fn a_large_object_streams_through_the_cache() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.size.set(24 << 20);
            let policy = "[cache.buckets.bucket]\nadmit_on_first_read = true";
            let server =
                Server::start(origin_port, &data_dir(), r#"{ bucket = "bucket" }"#, policy).await;
            let object = origin.object("/bucket/big");
            let get = || send(server.port, "GET", "/bucket/big", "", &[], Vec::new());
            for _ in 0..3 {
                let (status, body) = get().await;
                assert_eq!(status, 200);
                assert!(body == object, "the body differs");
            }
            let range = [("range", "bytes=5000000-9000000")];
            let (status, body) =
                send(server.port, "GET", "/bucket/big", "", &range, Vec::new()).await;
            assert_eq!(status, 206);
            assert!(body == object[5_000_000..=9_000_000], "the range differs");
        })
        .await;
}

/// A response's pages stay in socket queues after `sendfile` returns, until
/// the client reads them. The cache holds one block, so the next object's
/// block goes into the same slot; that write waits until the stalled client
/// has read the old bytes.
#[tokio::test(flavor = "current_thread")]
async fn sent_pages_survive_a_rewrite_of_their_slot() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(40_000);
            let one_slot = "block_size = 65536\nextent_size = 65536\nextents = 1";
            let policy = "[cache.buckets.bucket]\nimmutable = true\nadmit_on_first_read = true";
            let grants = r#"{ bucket = "bucket" }"#;
            let server =
                Server::start_with(origin_port, &data_dir(), grants, policy, one_slot).await;
            let port = server.port;
            let get = |key: &'static str| send(port, "GET", key, "", &[], Vec::new());
            let (a, b) = (origin.object("/bucket/a"), origin.object("/bucket/b"));
            assert_eq!(get("/bucket/a").await, (200, a.clone()));
            assert_eq!(get("/bucket/a").await, (200, a.clone()));
            assert_eq!(origin.requests.get(), 1);

            // A client asks for `a` again, a hit, and reads none of it yet.
            let mut stalled = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let mut head = "GET /bucket/a HTTP/1.1\r\n".to_string();
            for (name, value) in signed(port, "GET", "/bucket/a", "", &[]) {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str("Connection: close\r\n\r\n");
            stalled.write_all(head.as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;

            // `b` evicts `a`, and its block is due in `a`'s slot.
            assert_eq!(get("/bucket/b").await, (200, b.clone()));
            tokio::time::sleep(Duration::from_millis(200)).await;

            let mut response = Vec::new();
            stalled.read_to_end(&mut response).await.unwrap();
            let body = &response[response.len() - a.len()..];
            assert!(body == a, "the stalled client read bytes of another block");

            // Once the old pages are free, `b`'s block is written.
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(get("/bucket/b").await, (200, b));
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}
