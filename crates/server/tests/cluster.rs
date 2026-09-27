//! Gateways and storage nodes as separate processes, in front of a fake S3
//! in this test's process.

mod common;

use common::{CLUSTER_CACHE, Cluster, data_dir, object, start_origin, try_get};
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
                        tokio::task::spawn_local(async move { try_get(port, &path).await })
                    })
                    .collect();
                tokio::time::sleep(Duration::from_millis(30 + 15 * round)).await;
                drop(node);
                // A read the kill cuts short fails or ends early, and its
                // client would retry.
                for read in reads {
                    if let Some((status, body)) = read.await.unwrap() {
                        assert!(
                            status >= 500 || (status, body) == (200, object()),
                            "{status}"
                        );
                    }
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

/// Fast membership: a node is declared down within half a second, and
/// leaves the ring a second after that.
const GOSSIP: &str = "[cluster.membership]\nprobe_period_ms = 100\nprobe_rtt_ms = 40\nsuspect_to_down_ms = 300\ndown_grace_ms = 1000\ngossip_period_ms = 50";

/// Two nodes serve and fill the cache; then a third, which no other
/// config names, joins and takes over some objects' homes. The others and
/// the gateway learn where it is from gossip and rings, and it reads what
/// it took over from the nodes that held it, so every object reads back
/// without S3.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_joins_reads_from_previous_owners() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            let cluster =
                Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 3, GOSSIP, &[2]);
            let _nodes = [cluster.start(0).await, cluster.start(1).await];
            let _gateway = cluster.start_gateway().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let keys: Vec<String> = (0..12).map(|index| format!("k{index}")).collect();
            for key in &keys {
                let object = origin.object(&format!("/bucket/{key}"));
                assert!(cluster.get(key).await == (200, object.clone()), "{key}");
                assert!(cluster.get(key).await == (200, object), "{key}");
            }
            let warm = origin.requests.get();
            let _joined = cluster.start(2).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            // An answer shows the gateway the new ring's version.
            cluster.get(&keys[0]).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            let before = origin.requests.get();
            assert!(
                before <= warm + 1,
                "{} S3 requests before the reread",
                before - warm
            );
            for key in &keys {
                let object = origin.object(&format!("/bucket/{key}"));
                assert!(cluster.get(key).await == (200, object), "{key}");
            }
            assert_eq!(origin.requests.get(), before, "the reread asked S3");
        })
        .await;
}

/// A node told to leave drops out of every ring, serves the objects it
/// held to their new owners, and exits once the fallback window ends.
#[tokio::test(flavor = "current_thread")]
async fn a_leaving_node_hands_over_its_objects_and_exits() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            let cache = format!("{CLUSTER_CACHE}\nfallback_window_ms = 4000");
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, &cache, 3, GOSSIP, &[]);
            let _kept = [cluster.start(0).await, cluster.start(1).await];
            let mut leaving = cluster.start(2).await;
            let _gateway = cluster.start_gateway().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let keys: Vec<String> = (0..12).map(|index| format!("k{index}")).collect();
            for key in &keys {
                let object = origin.object(&format!("/bucket/{key}"));
                assert!(cluster.get(key).await == (200, object.clone()), "{key}");
                assert!(cluster.get(key).await == (200, object), "{key}");
            }
            leaving.signal("USR1");
            tokio::time::sleep(Duration::from_secs(1)).await;
            cluster.get(&keys[0]).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let before = origin.requests.get();
            for key in &keys {
                let object = origin.object(&format!("/bucket/{key}"));
                assert!(cluster.get(key).await == (200, object), "{key}");
            }
            assert_eq!(origin.requests.get(), before, "the reread asked S3");
            let exited = leaving.exited(Duration::from_secs(10)).await;
            assert_eq!(exited, Some(true), "the node did not exit cleanly");
        })
        .await;
}

/// A gateway reaches a node its config never named, at the address the
/// ring gives: once the only node its config names stops, the node that
/// joined serves every read.
#[tokio::test(flavor = "current_thread")]
async fn a_gateway_reaches_nodes_its_config_never_named() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            let cluster =
                Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, GOSSIP, &[1]);
            let named = cluster.start(0).await;
            let _gateway = cluster.start_gateway().await;
            let _joined = cluster.start(1).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            // An answer shows the gateway the new ring, and where node 1 is.
            cluster.get("k0").await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            named.stop();
            tokio::time::sleep(Duration::from_secs(2)).await;
            for index in 0..6 {
                let key = format!("k{index}");
                let object = origin.object(&format!("/bucket/{key}"));
                // A read may find the gateway still routing to node 0.
                let mut answer = cluster.get(&key).await;
                if answer.0 >= 500 {
                    answer = cluster.get(&key).await;
                }
                assert!(answer == (200, object), "{key}: {}", answer.0);
            }
        })
        .await;
}
