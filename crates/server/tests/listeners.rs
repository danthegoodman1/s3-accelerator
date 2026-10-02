//! Listeners hold a long queue of connections waiting to be accepted, a
//! process raises its limit on open files, and a listener that runs out of
//! descriptors serves again once they free up.

mod common;

use common::{
    CLUSTER_CACHE, Cluster, Process, admin, data_dir, eventually, listening, object, port, sample,
    start_origin,
};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use tokio::task::LocalSet;

/// The soft and hard limits on open files of process `pid`.
fn open_file_limits(pid: u32) -> (u64, u64) {
    let limits = std::fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
    let line = limits
        .lines()
        .find(|line| line.starts_with("Max open files"))
        .unwrap();
    let fields: Vec<&str> = line.split_whitespace().collect();
    (fields[3].parse().unwrap(), fields[4].parse().unwrap())
}

/// The queue length of the socket listening on `port`, as `ss` shows it.
fn backlog(port: u16) -> u64 {
    let filter = format!("sport = :{port}");
    let output = Command::new("ss")
        .args(["-Hltn", &filter])
        .output()
        .expect("ss runs");
    let listing = String::from_utf8(output.stdout).unwrap();
    let fields: Vec<&str> = listing.split_whitespace().collect();
    fields[2].parse().unwrap()
}

fn logged(log: &Path, text: &str) -> bool {
    std::fs::read_to_string(log).is_ok_and(|lines| lines.contains(text))
}

/// A gateway's and a node's listeners each queue 4,096 connections, or as
/// many as the kernel allows, for a burst of new clients.
#[tokio::test(flavor = "current_thread")]
async fn listeners_queue_a_burst_of_connections() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _) = start_origin().await;
            let cluster = Cluster::new(&data_dir(), origin_port, CLUSTER_CACHE);
            let _node = cluster.start_node().await;
            let _gateway = cluster.start_gateway().await;
            let most = std::fs::read_to_string("/proc/sys/net/core/somaxconn").unwrap();
            let expected = most.trim().parse::<u64>().unwrap().min(4_096);
            assert_eq!(backlog(cluster.gateway_port), expected);
            assert_eq!(backlog(cluster.node_port), expected);
        })
        .await;
}

/// A process started with a soft limit below its hard limit raises the soft
/// limit to the hard one, and reports it.
#[tokio::test(flavor = "current_thread")]
async fn a_process_raises_its_open_file_limit() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _) = start_origin().await;
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, CLUSTER_CACHE);
            let admin_port = port();
            let mut config = std::fs::read_to_string(&cluster.node).unwrap();
            config.push_str(&format!("\n[admin]\nlisten = \"127.0.0.1:{admin_port}\"\n"));
            std::fs::write(&cluster.node, config).unwrap();
            let node = Process::limited(&cluster.node, &dir.join("node.log"), 256, 4_096);
            listening(cluster.node_port).await;
            assert_eq!(open_file_limits(node.server_pid()), (4_096, 4_096));
            let (_, scrape) = admin(admin_port, "/metrics").await;
            assert_eq!(sample(&scrape, "process_max_fds", ""), 4_096.0);
        })
        .await;
}

/// A gateway and a node, each allowed 128 open files, get 128 connections
/// each. Each listener fails to accept until the connections close, and
/// then serves again.
#[tokio::test(flavor = "current_thread")]
async fn listeners_serve_again_once_descriptors_free_up() {
    const LIMIT: u64 = 128;
    LocalSet::new()
        .run_until(async {
            let (origin_port, _) = start_origin().await;
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, CLUSTER_CACHE);
            let (node_log, gateway_log) = (dir.join("node.log"), dir.join("gateway.log"));
            let mut node = Process::limited(&cluster.node, &node_log, LIMIT, LIMIT);
            listening(cluster.node_port).await;
            let mut gateway = Process::limited(&cluster.gateway, &gateway_log, LIMIT, LIMIT);
            listening(cluster.gateway_port).await;
            assert_eq!(cluster.get("k").await, (200, object()));
            let connect = |port: u16| {
                let address = ([127, 0, 0, 1], port).into();
                TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap()
            };
            let held: Vec<TcpStream> = [cluster.node_port, cluster.gateway_port]
                .into_iter()
                .flat_map(|port| (0..LIMIT).map(move |_| connect(port)))
                .collect();
            let failed = "msg=\"a listener failed to accept\"";
            eventually(|| logged(&node_log, failed) && logged(&gateway_log, failed)).await;
            drop(held);
            assert_eq!(node.exited(Duration::from_millis(200)).await, None);
            assert_eq!(gateway.exited(Duration::from_millis(200)).await, None);
            assert_eq!(cluster.get("k").await, (200, object()));
        })
        .await;
}
