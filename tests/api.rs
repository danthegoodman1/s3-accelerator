//! S3 behavior beyond plain reads that the cache serves or passes on:
//! presigned URLs, response overrides, virtual-hosted-style addressing,
//! `DeleteObjects` and checksums.

use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, Delete, ObjectIdentifier};
use s3_accelerator_conformance::{bucket, client, pattern, put, status, virtual_hosted_client};
use std::time::{Duration, SystemTime};

/// Sends a presigned request with `body`, as a browser or `curl` would,
/// holding no credentials, and returns its status and body.
async fn send(presigned: &PresignedRequest, body: Vec<u8>) -> (u16, Vec<u8>) {
    let method = reqwest::Method::from_bytes(presigned.method().as_bytes()).unwrap();
    let mut request = reqwest::Client::new()
        .request(method, presigned.uri())
        .body(body);
    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.bytes().await.unwrap().to_vec())
}

fn for_five_minutes() -> PresigningConfig {
    PresigningConfig::expires_in(Duration::from_secs(300)).unwrap()
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn presigned_urls_read_and_write_without_credentials() {
    let client = client();
    let bucket = bucket(&client, "presigned").await;
    let body = pattern(100_000, 3);
    put(&client, &bucket, "k", &body).await;
    let get = client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .presigned(for_five_minutes())
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(send(&get, Vec::new()).await == (200, body.clone()));
    }
    let head = client
        .head_object()
        .bucket(&bucket)
        .key("k")
        .presigned(for_five_minutes())
        .await
        .unwrap();
    assert_eq!(send(&head, Vec::new()).await.0, 200);
    let written = pattern(5_000, 4);
    let put = client
        .put_object()
        .bucket(&bucket)
        .key("uploaded")
        .presigned(for_five_minutes())
        .await
        .unwrap();
    assert_eq!(send(&put, written.clone()).await.0, 200);
    let read = client
        .get_object()
        .bucket(&bucket)
        .key("uploaded")
        .send()
        .await
        .unwrap();
    assert!(read.body.collect().await.unwrap().to_vec() == written);
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn an_expired_or_altered_presigned_url_is_refused() {
    let client = client();
    let bucket = bucket(&client, "presigned-refused").await;
    put(&client, &bucket, "k", b"secret").await;
    let expired = PresigningConfig::builder()
        .start_time(SystemTime::now() - Duration::from_secs(3_600))
        .expires_in(Duration::from_secs(60))
        .build()
        .unwrap();
    let get = client.get_object().bucket(&bucket).key("k");
    let presigned = get.clone().presigned(expired).await.unwrap();
    assert_eq!(send(&presigned, Vec::new()).await.0, 403);
    let presigned = get.presigned(for_five_minutes()).await.unwrap();
    let altered = presigned.uri().replace("/k?", "/other?");
    let response = reqwest::Client::new().get(altered).send().await.unwrap();
    assert_eq!(response.status().as_u16(), 403);
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn response_overrides_set_a_reads_headers() {
    let client = client();
    let bucket = bucket(&client, "overrides").await;
    put(&client, &bucket, "k", &pattern(10_000, 5)).await;
    for _ in 0..2 {
        let output = client
            .get_object()
            .bucket(&bucket)
            .key("k")
            .response_content_type("text/plain")
            .response_content_disposition(r#"attachment; filename="k.txt""#)
            .response_cache_control("no-cache")
            .send()
            .await
            .unwrap();
        assert_eq!(output.content_type(), Some("text/plain"));
        assert_eq!(
            output.content_disposition(),
            Some(r#"attachment; filename="k.txt""#)
        );
        assert_eq!(output.cache_control(), Some("no-cache"));
    }
    let plain = client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_ne!(plain.content_type(), Some("text/plain"));
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn a_virtual_hosted_request_names_its_bucket_in_the_host() {
    let bucket = bucket(&client(), "virtual-hosted").await;
    let client = virtual_hosted_client();
    let body = pattern(50_000, 6);
    client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    for _ in 0..2 {
        let output = client
            .get_object()
            .bucket(&bucket)
            .key("k")
            .send()
            .await
            .unwrap();
        assert!(output.body.collect().await.unwrap().to_vec() == body);
    }
    let range = client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .range("bytes=100-199")
        .send()
        .await
        .unwrap();
    assert!(range.body.collect().await.unwrap().to_vec() == body[100..200]);
    let listed = client
        .list_objects_v2()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(keys, ["k"]);
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn delete_objects_removes_every_key_it_names() {
    let client = client();
    let bucket = bucket(&client, "delete-objects").await;
    for key in ["a", "b", "c"] {
        put(&client, &bucket, key, &pattern(1_000, 7)).await;
        // Read, so the cache holds the object.
        client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
    }
    let objects = ["a", "b"].map(|key| ObjectIdentifier::builder().key(key).build().unwrap());
    let delete = Delete::builder()
        .set_objects(Some(objects.to_vec()))
        .build()
        .unwrap();
    client
        .delete_objects()
        .bucket(&bucket)
        .delete(delete)
        .send()
        .await
        .unwrap();
    for key in ["a", "b"] {
        let error = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap_err();
        assert_eq!(status(&error), Some(404), "{key}");
    }
    assert!(
        client
            .get_object()
            .bucket(&bucket)
            .key("c")
            .send()
            .await
            .is_ok()
    );
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn a_hit_carries_the_checksums_a_miss_did() {
    let client = client();
    let bucket = bucket(&client, "checksums").await;
    put(&client, &bucket, "k", &pattern(20_000, 8)).await;
    let mut checksums = Vec::new();
    for _ in 0..2 {
        let output = client
            .get_object()
            .bucket(&bucket)
            .key("k")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        checksums.push(output.checksum_crc32().map(str::to_string));
        output.body.collect().await.unwrap();
    }
    assert!(checksums[0].is_some(), "the upload's checksum comes back");
    assert_eq!(checksums[0], checksums[1]);
}
