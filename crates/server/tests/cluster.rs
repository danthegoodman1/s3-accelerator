//! Gateways and storage nodes as separate processes, in front of a fake S3
//! in this test's process.

mod common;

use common::{CLUSTER_CACHE, Cluster, data_dir, object, send, start_origin};
use std::time::Duration;
use tokio::task::LocalSet;

/// A node restarted for a deploy keeps serving its blocks, and the
/// metadata it saved, without asking S3 again.
#[tokio::test(flavor = "current_thread")]
async fn a_node_restart_keeps_the_cache_warm() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::new(&data_dir(), origin_port, CLUSTER_CACHE);
            let node = cluster.start_node().await;
            let _gateway = cluster.start_gateway().await;
            assert_eq!(cluster.get("k").await, (200, object()));
            assert_eq!(cluster.get("k").await, (200, object()));
            assert_eq!(origin.requests.get(), 1);
            node.stop();
            let _node = cluster.start_node().await;
            assert_eq!(cluster.get("k").await, (200, object()));
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

/// A node killed while it fills many objects comes back serving every one
/// correctly: blocks whose writes it finished, verified first, and the rest
/// from S3. Each round kills it at a different moment, so some land while
/// block writes are in progress.
#[tokio::test(flavor = "current_thread")]
async fn a_node_killed_during_fills_serves_correct_bytes_after_it_restarts() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::new(&data_dir(), origin_port, CLUSTER_CACHE);
            let _gateway = cluster.start_gateway().await;
            for round in 0..6 {
                let node = cluster.start_node().await;
                origin.delay.set(Duration::from_millis(20));
                let keys: Vec<String> = (0..16).map(|index| format!("r{round}-{index}")).collect();
                let reads: Vec<_> = keys
                    .iter()
                    .map(|key| {
                        let path = format!("/bucket/{key}");
                        let port = cluster.gateway_port;
                        tokio::task::spawn_local(async move {
                            send(port, "GET", &path, "", &[], Vec::new()).await
                        })
                    })
                    .collect();
                tokio::time::sleep(Duration::from_millis(30 + 15 * round)).await;
                drop(node);
                for read in reads {
                    let (status, body) = read.await.unwrap();
                    assert!(
                        status >= 500 || (status, body) == (200, object()),
                        "{status}"
                    );
                }
                origin.delay.set(Duration::ZERO);
                let node = cluster.start_node().await;
                for key in &keys {
                    assert_eq!(cluster.get(key).await, (200, object()), "{key}");
                    assert_eq!(cluster.get(key).await, (200, object()), "{key}");
                }
                drop(node);
            }
        })
        .await;
}
