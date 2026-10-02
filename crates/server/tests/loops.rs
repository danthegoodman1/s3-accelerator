//! A gateway runs an event loop on each of several threads: new
//! connections spread across the loops, a scrape counts every loop's
//! requests, and a read through one loop sees a write through another.

mod common;

use common::{CLUSTER_CACHE, Cluster, Pinned, Server, admin, data_dir, sample, start_origin};
use tokio::task::LocalSet;

/// Each `gateway-N` thread of process `pid`, with the nanoseconds the
/// kernel has run it.
fn loop_run_times(pid: u32) -> Vec<(String, u64)> {
    let mut times = Vec::new();
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let task = task.unwrap().path();
        let name = std::fs::read_to_string(task.join("comm")).unwrap();
        let name = name.trim();
        if name.starts_with("gateway-") {
            let schedstat = std::fs::read_to_string(task.join("schedstat")).unwrap();
            let ran = schedstat
                .split_whitespace()
                .next()
                .unwrap()
                .parse()
                .unwrap();
            times.push((name.to_string(), ran));
        }
    }
    times.sort();
    times
}

/// Eight clients connected at once keep a gateway's four loops about
/// equally busy, as the kernel's count of each thread's run time shows.
#[tokio::test(flavor = "current_thread")]
async fn connections_spread_across_every_loop() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 1, "", &[]);
            let _node = cluster.start(0).await;
            let gateway = cluster.start_gateway().await;
            let clients: Vec<Pinned> = (0..8).map(|_| Pinned::new(cluster.gateway_port)).collect();
            for client in &clients {
                assert_eq!(client.send("GET", "/bucket/k", Vec::new()).await.0, 200);
            }
            let before = loop_run_times(gateway.pid());
            assert_eq!(before.len(), 4, "{before:?}");
            for _ in 0..100 {
                for client in &clients {
                    assert_eq!(client.send("GET", "/bucket/k", Vec::new()).await.0, 200);
                }
            }
            let after = loop_run_times(gateway.pid());
            let ran: Vec<u64> = before
                .iter()
                .zip(&after)
                .map(|((_, before), (_, after))| after - before)
                .collect();
            let (least, most) = (ran.iter().min().unwrap(), ran.iter().max().unwrap());
            assert!(least * 3 > *most, "the loops ran {ran:?} ns");
        })
        .await;
}

/// A scrape sums every loop's counts: five reads through each of two loops
/// count ten.
#[tokio::test(flavor = "current_thread")]
async fn a_scrape_counts_the_requests_of_every_loop() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let server = Server::start(origin_port, &data_dir(), r#"{ bucket = "*" }"#, "").await;
            let clients = [Pinned::new(server.port), Pinned::new(server.port)];
            for _ in 0..5 {
                for client in &clients {
                    assert_eq!(client.send("GET", "/bucket/k", Vec::new()).await.0, 200);
                }
            }
            let scrape = admin(server.admin_port, "/metrics").await.1;
            let labels = "operation=\"GetObject\",code=\"200\"";
            assert_eq!(
                sample(&scrape, "s3accel_gateway_requests_total", labels),
                10.0
            );
        })
        .await;
}

/// A client that reads a key on one connection, writes it on another, and
/// reads it again on the first sees its write, though another loop holds
/// the first connection and had the key's metadata.
#[tokio::test(flavor = "current_thread")]
async fn a_read_through_one_loop_sees_a_write_through_another() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let server = Server::start(origin_port, &data_dir(), r#"{ bucket = "*" }"#, "").await;
            let (reader, writer) = (Pinned::new(server.port), Pinned::new(server.port));
            let path = "/bucket/k";
            let first = origin.object(path);
            assert_eq!(
                reader.send("GET", path, Vec::new()).await,
                (200, first.clone())
            );
            assert_eq!(writer.send("GET", path, Vec::new()).await, (200, first));
            let written = b"the new version".to_vec();
            assert_eq!(writer.send("PUT", path, written.clone()).await.0, 200);
            assert_eq!(reader.send("GET", path, Vec::new()).await, (200, written));
        })
        .await;
}
