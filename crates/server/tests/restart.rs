//! A node keeps its blocks and immutable-bucket metadata across restarts.

mod common;

use common::{Server, data_dir, object, send, start_origin};
use s3_accelerator::disk::{RECORD_SIZE, decode_record};
use std::os::unix::fs::FileExt;
use std::path::Path;
use tokio::task::LocalSet;

const IMMUTABLE: &str = "[cache.buckets.bucket]\nimmutable = true\nadmit_on_first_read = true";
const GRANTS: &str = r#"{ bucket = "bucket" }"#;

async fn get(server: &Server) -> (u16, Vec<u8>) {
    send(server.port, "GET", "/bucket/k", "", &[], Vec::new()).await
}

/// Reads the object twice, the second time from the cache.
async fn warm(server: &Server, origin: &common::Origin) {
    assert_eq!(get(server).await, (200, object()));
    assert_eq!(get(server).await, (200, object()));
    assert_eq!(origin.requests.get(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn a_clean_restart_keeps_the_cache_warm() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let dir = data_dir();
            let server = Server::start(origin_port, &dir, GRANTS, IMMUTABLE).await;
            warm(&server, &origin).await;
            server.stop().await;
            for _ in 0..2 {
                let server = Server::start(origin_port, &dir, GRANTS, IMMUTABLE).await;
                assert_eq!(get(&server).await, (200, object()));
                assert_eq!(origin.requests.get(), 1);
                server.stop().await;
            }
        })
        .await;
}

/// After a crash the node verifies each block before serving it. Blocks
/// that pass come from the disk; one damaged block comes from S3 again.
#[tokio::test(flavor = "current_thread")]
async fn a_crash_restart_verifies_blocks_before_serving_them() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let dir = data_dir();
            let server = Server::start(origin_port, &dir, GRANTS, IMMUTABLE).await;
            warm(&server, &origin).await;
            server.crash();
            let server = Server::start(origin_port, &dir, GRANTS, IMMUTABLE).await;
            assert_eq!(get(&server).await, (200, object()));
            assert_eq!(origin.requests.get(), 1);
            server.crash();
            damage_a_block(&dir);
            let server = Server::start(origin_port, &dir, GRANTS, IMMUTABLE).await;
            assert_eq!(get(&server).await, (200, object()));
            assert_eq!(origin.requests.get(), 2);
            assert_eq!(get(&server).await, (200, object()));
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}

/// Flips a byte of the block the slot table records first.
fn damage_a_block(dir: &Path) {
    let table = std::fs::read(dir.join("slots")).unwrap();
    let (records, _) = table[4096..].as_chunks::<{ RECORD_SIZE as usize }>();
    let index = records
        .iter()
        .enumerate()
        .find_map(|(index, record)| decode_record(record).map(|_| index))
        .expect("a recorded block");
    // Slots are 4 KiB apart, the default smallest size.
    let offset = index as u64 * 4096;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(dir.join("slabs"))
        .unwrap();
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset).unwrap();
    file.write_all_at(&[!byte[0]], offset).unwrap();
}
