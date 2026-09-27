//! Authentication and grants, end to end.

mod common;

use common::{object, presigned, send, send_payload, signed, start};
use s3_accelerator::sigv4::{self, Credentials, Signer, UNSIGNED_PAYLOAD};
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
fn raw_head(port: u16, method: &str, path: &str, query: &str, extra: &[(&str, &str)]) -> String {
    let target = match query {
        "" => path.to_string(),
        query => format!("{path}?{query}"),
    };
    let mut head = format!("{method} {target} HTTP/1.1\r\n");
    for (name, value) in signed(port, method, path, query, extra) {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");
    head
}

/// Keys with `.` and `..` segments reach S3 as written, so S3 serves the
/// key the client signed.
#[tokio::test(flavor = "current_thread")]
async fn dot_segment_keys_reach_s3_as_written() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            for path in ["/bucket/a/../b", "/bucket/./c", "/bucket/d/%2E%2E/e"] {
                let response = head_only(port, &raw_head(port, "GET", path, "", &[])).await;
                assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            }
            let paths = origin.paths.borrow();
            assert_eq!(*paths, ["/bucket/a/../b", "/bucket/./c", "/bucket/d/../e"]);
        })
        .await;
}

/// A client that asks to close after a read is told the server closes.
#[tokio::test(flavor = "current_thread")]
async fn a_read_tells_a_closing_client_it_closes() {
    LocalSet::new()
        .run_until(async {
            let (port, _) = start(r#"{ bucket = "bucket" }"#, "").await;
            for _ in 0..2 {
                let response = head_only(port, &raw_head(port, "GET", "/bucket/k", "", &[])).await;
                assert!(response.starts_with("HTTP/1.1 200"), "{response}");
                assert!(response.contains("Connection: close\r\n"), "{response}");
            }
        })
        .await;
}

/// The gateway reads a `DeleteObjects` key list to learn what it deletes,
/// so it refuses one larger than S3 takes before reading it.
#[tokio::test(flavor = "current_thread")]
async fn an_oversized_key_list_is_refused_before_it_is_read() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let length = [("content-length", "16777216")];
            let head = raw_head(port, "POST", "/bucket", "delete", &length);
            let response = head_only(port, &head).await;
            assert!(response.starts_with("HTTP/1.1 400"), "{response}");
            assert!(response.contains("EntityTooLarge"), "{response}");
            assert_eq!(origin.requests.get(), 0);
        })
        .await;
}

/// A bucket whose blocks go to disk on their first read, so the second
/// read of an object is a hit.
const FIRST_READ: &str = "[cache.buckets.bucket]\nimmutable = true\nadmit_on_first_read = true";

/// A presigned URL reads and writes without credentials: its GETs come
/// from the cache after the first, its PUT reaches S3 without the
/// signature's parameters, and an altered or expired URL gets 403.
#[tokio::test(flavor = "current_thread")]
async fn a_presigned_url_reads_and_writes_without_credentials() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, FIRST_READ).await;
            let host = format!("127.0.0.1:{port}");
            let client = reqwest::Client::new();
            let status = |url: String, method: reqwest::Method| {
                let request = client.request(method, url);
                async move { request.send().await.unwrap().status().as_u16() }
            };
            let get = presigned(port, &host, ("GET", "/bucket/k", ""), 0, 300);
            for _ in 0..2 {
                let response = client.get(&get).send().await.unwrap();
                assert_eq!(response.status().as_u16(), 200);
                assert!(response.bytes().await.unwrap() == object());
            }
            assert_eq!(origin.requests.get(), 1);
            let head = presigned(port, &host, ("HEAD", "/bucket/k", ""), 0, 300);
            assert_eq!(status(head, reqwest::Method::HEAD).await, 200);
            let put = presigned(port, &host, ("PUT", "/bucket/new", ""), 0, 300);
            let response = client
                .put(&put)
                .body(b"written".to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 200);
            assert_eq!(origin.written.borrow()["/bucket/new"].1, b"written");
            assert!(
                origin
                    .queries
                    .borrow()
                    .iter()
                    .all(|query| !query.contains("X-Amz"))
            );
            let altered = get.replace("/bucket/k?", "/bucket/other?");
            assert_eq!(status(altered, reqwest::Method::GET).await, 403);
            let expired = presigned(port, &host, ("GET", "/bucket/k", ""), 120, 60);
            assert_eq!(status(expired, reqwest::Method::GET).await, 403);
        })
        .await;
}

/// `response-*` parameters set headers of a read's response, on a hit as
/// on the miss before it.
#[tokio::test(flavor = "current_thread")]
async fn response_overrides_set_a_reads_headers() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, FIRST_READ).await;
            let query = "response-content-type=text%2Fplain\
                &response-content-disposition=attachment%3B%20filename%3D%22k.txt%22";
            for _ in 0..2 {
                let url = format!("http://127.0.0.1:{port}/bucket/k?{query}");
                let mut request = reqwest::Client::new().get(url);
                for (name, value) in signed(port, "GET", "/bucket/k", query, &[]) {
                    request = request.header(name, value);
                }
                let response = request.send().await.unwrap();
                assert_eq!(response.status().as_u16(), 200);
                let header = |name: &str| response.headers()[name].to_str().unwrap().to_string();
                assert_eq!(header("content-type"), "text/plain");
                assert_eq!(
                    header("content-disposition"),
                    r#"attachment; filename="k.txt""#
                );
            }
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

/// A request to a subdomain of one of the gateway's domains names its
/// bucket there, signed or presigned.
#[tokio::test(flavor = "current_thread")]
async fn a_virtual_hosted_request_names_its_bucket_in_its_host() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let host = format!("bucket.s3.test:{port}");
            let signer = Signer {
                credentials: Credentials {
                    access_key_id: "reader".into(),
                    secret_access_key: "reader-secret".into(),
                },
                region: "us-east-1".into(),
                service: "s3",
            };
            let mut headers = vec![("host".to_string(), host.clone())];
            signer.sign(
                "GET",
                "/k",
                "",
                &mut headers,
                UNSIGNED_PAYLOAD,
                sigv4::unix_now(),
            );
            let mut request = reqwest::Client::new().get(format!("http://127.0.0.1:{port}/k"));
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status().as_u16(), 200);
            assert!(response.bytes().await.unwrap() == object());
            assert_eq!(origin.paths.borrow().last().unwrap(), "/bucket/k");
            let url = presigned(port, &host, ("HEAD", "/k", ""), 0, 300);
            let response = reqwest::Client::new()
                .head(url)
                .header("host", &host)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 200);
        })
        .await;
}

/// Grants give read, write or admin access to a prefix. A read grant
/// reads its prefix and writes nothing; a listing is checked against the
/// prefix it lists; every key a `DeleteObjects` names needs a write grant;
/// and only an admin grant on the whole bucket changes the bucket.
#[tokio::test(flavor = "current_thread")]
async fn grants_give_each_level_of_access_to_their_prefix() {
    LocalSet::new()
        .run_until(async {
            let grants = r#"{ bucket = "bucket", prefix = "public/", access = "read" },
                            { bucket = "bucket", prefix = "shared/" }"#;
            let (port, _origin) = start(grants, "").await;
            let status = |method: &'static str, path: &'static str, query: &'static str| async move {
                send(port, method, path, query, &[], Vec::new()).await.0
            };
            assert_eq!(status("GET", "/bucket/public/a", "").await, 200);
            assert_eq!(status("PUT", "/bucket/public/a", "").await, 403);
            assert_eq!(status("PUT", "/bucket/shared/a", "").await, 200);
            assert_eq!(status("GET", "/bucket", "list-type=2&prefix=public%2F").await, 200);
            assert_eq!(status("GET", "/bucket", "list-type=2&prefix=shared%2Fx").await, 200);
            assert_eq!(status("GET", "/bucket", "list-type=2").await, 403);
            assert_eq!(status("GET", "/bucket", "list-type=2&prefix=secret%2F").await, 403);
            assert_eq!(status("HEAD", "/bucket", "").await, 200);
            assert_eq!(status("PUT", "/bucket", "policy").await, 403);
            assert_eq!(status("DELETE", "/bucket", "").await, 403);
            let delete = |keys: &[&str]| {
                let objects: String = keys
                    .iter()
                    .map(|key| format!("<Object><Key>{key}</Key></Object>"))
                    .collect();
                let body = format!("<Delete>{objects}</Delete>").into_bytes();
                async move { send(port, "POST", "/bucket", "delete", &[], body).await.0 }
            };
            assert_eq!(delete(&["shared/a", "secret/b"]).await, 403);
            assert_eq!(delete(&["shared/a", "public/a"]).await, 403);
            assert_eq!(delete(&["shared/a"]).await, 200);
            // Changing the bucket takes an admin grant, even beside a write
            // grant on the whole bucket.
            for (access, allowed) in [("write", 403), ("admin", 200)] {
                let grant = format!(r#"{{ bucket = "bucket", access = "{access}" }}"#);
                let (port, _) = start(&grant, "").await;
                let policy = send(port, "PUT", "/bucket", "policy", &[], b"{}".to_vec()).await;
                assert_eq!(policy.0, allowed, "{access}");
            }
        })
        .await;
}

/// A body signed chunk by chunk gets 501 from its head, with or without a
/// trailer, since re-signing would break its chunk signatures; an
/// unsigned body with trailing checksums passes through.
#[tokio::test(flavor = "current_thread")]
async fn signed_streaming_uploads_get_501() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            for payload in [
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            ] {
                let body = b"0;chunk-signature=0\r\n\r\n".to_vec();
                let sent = send_payload(port, "PUT", "/bucket/k", "", &[], body, payload).await;
                assert_eq!(sent.0, 501, "{payload}");
            }
            assert_eq!(origin.requests.get(), 0);
            let trailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
            let headers = [("x-amz-decoded-content-length", "0")];
            let body = b"0\r\n\r\n".to_vec();
            let sent = send_payload(port, "PUT", "/bucket/k", "", &headers, body, trailer).await;
            assert_eq!(sent.0, 200);
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

/// Reads with a customer-provided encryption key pass through to S3, even
/// of a bucket whose blocks go to disk on their first read.
#[tokio::test(flavor = "current_thread")]
async fn reads_with_a_customer_key_bypass_the_cache() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, FIRST_READ).await;
            let key = [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                ("x-amz-server-side-encryption-customer-key", "a2V5"),
            ];
            for _ in 0..2 {
                let read = send(port, "GET", "/bucket/k", "", &key, Vec::new()).await;
                assert_eq!(read.0, 200);
            }
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}
