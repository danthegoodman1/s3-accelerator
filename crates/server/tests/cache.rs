//! The server end to end, in front of a fake S3 that counts its requests.

mod common;

use common::{CHECKSUM, Server, data_dir, object, send, signed, start, start_origin};
use s3_accelerator::disk::decode_record;
use std::time::Duration;
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

/// A cache full of 1 MiB blocks makes room for 4 KiB ones, whose slots lie
/// where a larger block's pages were cached, in one folio larger than the
/// new slot on kernels that cache writes in large folios. Each small block
/// must still reach the disk: its record appears in the slot table.
#[tokio::test(flavor = "current_thread")]
async fn small_blocks_take_space_that_held_larger_ones() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            let block = 1 << 20;
            let cache = format!(
                "block_size = {block}\nextent_size = {block}\nextents = 4\nmin_slot = 4096"
            );
            let dir = data_dir();
            let server =
                Server::start_with(origin_port, &dir, r#"{ bucket = "bucket" }"#, "", &cache).await;
            let get = |path: String| async move {
                let answer = send(server.port, "GET", &path, "", &[], Vec::new());
                tokio::time::timeout(Duration::from_secs(10), answer)
                    .await
                    .unwrap_or_else(|_| panic!("{path} took over ten seconds"))
            };
            // Four blocks fill the cache: the doorkeeper admits them on the
            // second read, and the third reads their cached pages.
            origin.size.set(4 * block);
            for _ in 0..3 {
                assert_eq!(get("/bucket/large".into()).await.0, 200);
            }
            // Small objects, each admitted on its second read, then hit.
            origin.size.set(4096);
            let keys: Vec<String> = (0..8)
                .map(|index| format!("/bucket/small-{index}"))
                .collect();
            for _ in 0..2 {
                for key in &keys {
                    assert_eq!(get(key.clone()).await, (200, origin.object(key)));
                }
            }
            let recorded = || {
                let table = std::fs::read(dir.join("slots")).unwrap();
                table[4096..]
                    .chunks(64)
                    .filter_map(decode_record)
                    .filter(|(record, _, _)| record.len == 4096)
                    .count()
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while recorded() < keys.len() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(recorded(), keys.len(), "small blocks recorded");
        })
        .await;
}

/// The home keeps an object's checksums with its metadata: a whole-object
/// read that asks for them gets them, from the cache as from S3, and a
/// range or a read that doesn't ask gets none.
#[tokio::test(flavor = "current_thread")]
async fn checksums_come_back_on_whole_object_reads_that_ask() {
    LocalSet::new()
        .run_until(async {
            let first_read = "[cache.buckets.bucket]\nimmutable = true\nadmit_on_first_read = true";
            let (port, origin) = start(r#"{ bucket = "bucket" }"#, first_read).await;
            let get = |extra: Vec<(&'static str, &'static str)>| async move {
                let url = format!("http://127.0.0.1:{port}/bucket/k");
                let mut request = reqwest::Client::new().get(url);
                for (name, value) in signed(port, "GET", "/bucket/k", "", &extra) {
                    request = request.header(name, value);
                }
                let response = request.send().await.unwrap();
                let checksum = response
                    .headers()
                    .get("x-amz-checksum-crc32")
                    .map(|value| value.to_str().unwrap().to_string());
                (response.status().as_u16(), checksum)
            };
            let asking = ("x-amz-checksum-mode", "ENABLED");
            for _ in 0..2 {
                assert_eq!(get(vec![asking]).await, (200, Some(CHECKSUM.to_string())));
            }
            assert_eq!(origin.requests.get(), 1);
            assert_eq!(get(Vec::new()).await, (200, None));
            assert_eq!(get(vec![asking, ("range", "bytes=0-9")]).await, (206, None));
        })
        .await;
}
