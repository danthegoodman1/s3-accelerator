//! The server end to end, in front of a fake S3 that counts its requests.

mod common;

use common::{object, send, start};
use tokio::task::LocalSet;

#[tokio::test(flavor = "current_thread")]
async fn a_third_read_comes_from_the_cache() {
    LocalSet::new()
        .run_until(async {
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, "").await;
            let object = object();
            let get = |range: Option<&'static str>| {
                let extra: Vec<(&str, &str)> =
                    range.map(|range| ("range", range)).into_iter().collect();
                async move { send(port, "GET", "/bucket/k", "", &extra, Vec::new()).await }
            };
            // The first read fetches the object; the doorkeeper remembers its blocks.
            assert_eq!(get(None).await, (200, object.clone()));
            assert_eq!(origin.requests.get(), 1);
            // The second read fills and stores them with one range GET.
            assert_eq!(get(None).await, (200, object.clone()));
            assert_eq!(origin.requests.get(), 2);
            // The third, and a range, come from the cache.
            assert_eq!(get(None).await, (200, object.clone()));
            let (status, body) = get(Some("bytes=70000-200000")).await;
            assert_eq!((status, body.as_slice()), (206, &object[70_000..=200_000]));
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}

/// `DeleteObjects` names its keys in its body; each one's cached metadata
/// must go. Blocks are stored on the first read, so a stale answer would be
/// a pure hit that no `If-Match` fill could catch.
#[tokio::test(flavor = "current_thread")]
async fn delete_objects_drops_cached_metadata() {
    LocalSet::new()
        .run_until(async {
            let policy = "[cache.buckets.bucket]\nttl_ms = 60000\nadmit_on_first_read = true";
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, policy).await;
            let get = || send(port, "GET", "/bucket/k", "", &[], Vec::new());
            assert_eq!(get().await.0, 200);
            assert_eq!(get().await.0, 200);
            assert_eq!(origin.requests.get(), 1);
            let delete = b"<Delete><Object><Key>k</Key></Object></Delete>".to_vec();
            let (status, _) = send(port, "POST", "/bucket", "delete", &[], delete).await;
            assert_eq!(status, 200);
            assert!(origin.deleted.get());
            assert_eq!(get().await.0, 404);
        })
        .await;
}
