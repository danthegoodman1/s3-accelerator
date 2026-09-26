//! `GetObject` and `HeadObject`: the requests the cache serves.

use s3_accelerator_conformance::{bucket, client, pattern, put, status};

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn get_whole_object() {
    let client = client();
    let bucket = bucket(&client, "get-whole-object").await;
    let body = pattern(1_000, 1);
    let etag = put(&client, &bucket, "k", &body).await;
    let output = client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(output.e_tag(), Some(etag.as_str()));
    assert_eq!(output.content_length(), Some(1_000));
    assert_eq!(output.content_range(), None);
    assert_eq!(output.body.collect().await.unwrap().to_vec(), body);
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn get_ranges() {
    let client = client();
    let bucket = bucket(&client, "get-ranges").await;
    let body = pattern(1_000, 2);
    let etag = put(&client, &bucket, "k", &body).await;
    let cases = [
        ("bytes=0-0", 0, 0),
        ("bytes=100-199", 100, 199),
        ("bytes=990-5000", 990, 999),
        ("bytes=900-", 900, 999),
        ("bytes=-10", 990, 999),
        ("bytes=-5000", 0, 999),
    ];
    for (range, first, last) in cases {
        let output = client
            .get_object()
            .bucket(&bucket)
            .key("k")
            .range(range)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{range}: {error:?}"));
        assert_eq!(output.e_tag(), Some(etag.as_str()), "{range}");
        let content_range = format!("bytes {first}-{last}/1000");
        assert_eq!(
            output.content_range(),
            Some(content_range.as_str()),
            "{range}"
        );
        let expected = &body[first..=last];
        assert_eq!(
            output.content_length(),
            Some(expected.len() as i64),
            "{range}"
        );
        assert_eq!(
            output.body.collect().await.unwrap().to_vec(),
            expected,
            "{range}"
        );
    }
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn unsatisfiable_range_is_416() {
    let client = client();
    let bucket = bucket(&client, "unsatisfiable-range").await;
    put(&client, &bucket, "k", &pattern(100, 3)).await;
    for range in ["bytes=100-200", "bytes=100-"] {
        let request = client.get_object().bucket(&bucket).key("k").range(range);
        let error = request.send().await.expect_err(range);
        assert_eq!(status(&error), Some(416), "{range}");
    }
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn missing_key_is_404() {
    let client = client();
    let bucket = bucket(&client, "missing-key").await;
    let error = client
        .get_object()
        .bucket(&bucket)
        .key("absent")
        .send()
        .await
        .expect_err("absent key");
    assert_eq!(status(&error), Some(404));
    assert!(error.into_service_error().is_no_such_key());
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn if_match_selects_a_version() {
    let client = client();
    let bucket = bucket(&client, "if-match").await;
    let etag = put(&client, &bucket, "k", &pattern(100, 4)).await;
    let get = || client.get_object().bucket(&bucket).key("k");
    get()
        .if_match(&etag)
        .send()
        .await
        .expect("current ETag matches");
    let error = get()
        .if_match("\"stale\"")
        .send()
        .await
        .expect_err("stale ETag");
    assert_eq!(status(&error), Some(412));
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn if_none_match_is_304_for_the_current_version() {
    let client = client();
    let bucket = bucket(&client, "if-none-match").await;
    let etag = put(&client, &bucket, "k", &pattern(100, 5)).await;
    let get = || client.get_object().bucket(&bucket).key("k");
    let error = get()
        .if_none_match(&etag)
        .send()
        .await
        .expect_err("current ETag");
    assert_eq!(status(&error), Some(304));
    get()
        .if_none_match("\"stale\"")
        .send()
        .await
        .expect("stale ETag");
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn overwrite_changes_etag_and_body() {
    let client = client();
    let bucket = bucket(&client, "overwrite").await;
    let first = put(&client, &bucket, "k", &pattern(100, 6)).await;
    let body = pattern(50, 7);
    let second = put(&client, &bucket, "k", &body).await;
    assert_ne!(first, second);
    let output = client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(output.e_tag(), Some(second.as_str()));
    assert_eq!(output.body.collect().await.unwrap().to_vec(), body);
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn head_object_reports_size_and_etag() {
    let client = client();
    let bucket = bucket(&client, "head-object").await;
    let etag = put(&client, &bucket, "k", &pattern(321, 8)).await;
    let output = client
        .head_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(output.e_tag(), Some(etag.as_str()));
    assert_eq!(output.content_length(), Some(321));
}

#[tokio::test]
#[ignore = "needs an S3 endpoint: scripts/s3proxy start"]
async fn object_headers_come_back_with_every_read() {
    let client = client();
    let bucket = bucket(&client, "object-headers").await;
    client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .content_type("application/x-parquet")
        .metadata("writer", "conformance")
        .body(pattern(1_000, 11).into())
        .send()
        .await
        .unwrap();
    for range in [None, Some("bytes=10-19")] {
        let output = client
            .get_object()
            .bucket(&bucket)
            .key("k")
            .set_range(range.map(String::from))
            .send()
            .await
            .unwrap();
        assert_eq!(
            output.content_type(),
            Some("application/x-parquet"),
            "{range:?}"
        );
        assert_eq!(
            output.metadata().unwrap()["writer"],
            "conformance",
            "{range:?}"
        );
        assert!(output.last_modified().is_some(), "{range:?}");
    }
    let output = client
        .head_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(output.content_type(), Some("application/x-parquet"));
    assert_eq!(output.metadata().unwrap()["writer"], "conformance");
}
