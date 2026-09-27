//! Gateways and storage nodes as separate processes, in front of a fake S3
//! in this test's process.

mod common;

use common::{data_dir, object, send, start_origin};
use std::net::TcpListener as StdListener;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::task::LocalSet;

/// A server process, killed if the test ends first.
struct Process(Child);

impl Process {
    fn start(config: &Path) -> Process {
        let child = Command::new(env!("CARGO_BIN_EXE_s3-accelerator"))
            .arg(config)
            .spawn()
            .unwrap();
        Process(child)
    }

    /// Shuts down cleanly, as a deploy does, and waits.
    fn stop(mut self) {
        let pid = self.0.id().to_string();
        Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        assert!(self.0.wait().unwrap().success());
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A free port on the loopback interface.
fn port() -> u16 {
    StdListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn listening(port: u16) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("nothing listens on port {port}");
}

/// Configs for one node and a gateway, in `dir`.
struct Cluster {
    gateway_port: u16,
    node_port: u16,
    node: std::path::PathBuf,
    gateway: std::path::PathBuf,
}

impl Cluster {
    fn new(dir: &Path, origin_port: u16) -> Cluster {
        std::fs::create_dir_all(dir).unwrap();
        let (gateway_port, node_port) = (port(), port());
        let shared = format!(
            r#"
            [origin]
            endpoint = "http://127.0.0.1:{origin_port}"
            region = "us-east-1"
            access_key_id = "origin"
            secret_access_key = "origin-secret"
            [[clients]]
            access_key_id = "reader"
            secret_access_key = "reader-secret"
            grants = [{{ bucket = "bucket" }}]
            [cache]
            block_size = 65536
            extent_size = 1048576
            extents = 32
            [cache.buckets.bucket]
            immutable = true
            admit_on_first_read = true
            [cluster]
            secret = "cluster-secret"
            nodes = [{{ id = 0, address = "127.0.0.1:{node_port}" }}]
            "#
        );
        let node = dir.join("node.toml");
        let node_role = format!(
            "[node]\nid = 0\ndata_dir = \"{}\"\n",
            dir.join("disk").display()
        );
        std::fs::write(&node, format!("{shared}\n{node_role}")).unwrap();
        let gateway = dir.join("gateway.toml");
        let gateway_role = format!("[gateway]\nlisten = \"127.0.0.1:{gateway_port}\"\n");
        std::fs::write(&gateway, format!("{shared}\n{gateway_role}")).unwrap();
        Cluster {
            gateway_port,
            node_port,
            node,
            gateway,
        }
    }

    async fn start_node(&self) -> Process {
        let process = Process::start(&self.node);
        listening(self.node_port).await;
        process
    }

    async fn start_gateway(&self) -> Process {
        let process = Process::start(&self.gateway);
        listening(self.gateway_port).await;
        process
    }

    async fn get(&self, key: &str) -> (u16, Vec<u8>) {
        let path = format!("/bucket/{key}");
        send(self.gateway_port, "GET", &path, "", &[], Vec::new()).await
    }
}

/// A node restarted for a deploy keeps serving its blocks, and the
/// metadata it saved, without asking S3 again.
#[tokio::test(flavor = "current_thread")]
async fn a_node_restart_keeps_the_cache_warm() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::new(&data_dir(), origin_port);
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
            let cluster = Cluster::new(&data_dir(), origin_port);
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
                    assert!(status >= 500 || (status, body) == (200, object()), "{status}");
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
