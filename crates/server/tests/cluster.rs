//! Gateways and storage nodes as separate processes, in front of a fake S3
//! in this test's process.

mod common;

use common::{
    CLUSTER_CACHE, Cluster, LISTING, Process, data_dir, listening, object, object_of, send,
    send_for_headers, start_origin, start_queue, try_get,
};
use s3_accelerator::http::header;
use s3_accelerator::peers::Peers;
use s3_accelerator::protocol::{NodeAnswer, NodeRequest};
use s3_accelerator_core::layout::Layout;
use s3_accelerator_core::placement::{Member, NodeId, Placement, Ring};
use s3_accelerator_core::s3::ObjectKey;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::task::LocalSet;

/// A read's ID follows it from the gateway to the node that serves it: both
/// log their answers to it under the ID the client got.
#[tokio::test(flavor = "current_thread")]
async fn a_reads_id_follows_it_to_the_node() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _) = start_origin().await;
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, CLUSTER_CACHE);
            for config in [&cluster.node, &cluster.gateway] {
                let mut text = std::fs::read_to_string(config).unwrap();
                text.push_str("\n[log]\nlevel = \"debug\"\n");
                std::fs::write(config, text).unwrap();
            }
            let (node_log, gateway_log) = (dir.join("node.log"), dir.join("gateway.log"));
            let node = Process::logged(&cluster.node, &node_log);
            listening(cluster.node_port).await;
            let gateway = Process::logged(&cluster.gateway, &gateway_log);
            listening(cluster.gateway_port).await;
            let port = cluster.gateway_port;
            let (status, headers) = send_for_headers(port, "GET", "/bucket/k", Vec::new()).await;
            assert_eq!(status, 200);
            let id = header(&headers, "x-accel-request-id").unwrap().to_string();
            gateway.stop();
            node.stop();
            let answered = |log: &std::path::Path, msg: &str| {
                let text = std::fs::read_to_string(log).unwrap();
                let line = text
                    .lines()
                    .find(|line| line.contains(&format!("request={id}")))
                    .unwrap_or_else(|| panic!("no line names {id} in {text}"));
                assert!(line.contains(&format!("msg=\"{msg}\"")), "{line}");
                assert!(line.contains(" level=debug "), "{line}");
            };
            answered(&gateway_log, "answered a client");
            answered(&node_log, "answered a read");
        })
        .await;
}

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

/// A process without the cluster's secret gets 403 from a node, and its
/// gossip is dropped: a node that names it nowhere never takes it into
/// its ring. The same process with the secret joins.
#[tokio::test(flavor = "current_thread")]
async fn a_process_without_the_clusters_secret_can_not_join() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let cluster =
                Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, GOSSIP, &[1]);
            let config = std::fs::read_to_string(&cluster.nodes[1].1).unwrap();
            let impostor = config.replace(r#""cluster-secret""#, r#""impostor-secret""#);
            std::fs::write(&cluster.nodes[1].1, impostor).unwrap();
            let _node = cluster.start(0).await;
            let ring_of_node_0 = |secret: &str| {
                let address = format!("127.0.0.1:{}", cluster.nodes[0].0);
                let peers = Peers::new(BTreeMap::from([(NodeId(0), address)]), secret.into(), None);
                async move {
                    match peers.exchange(NodeId(0), &NodeRequest::Ring).await?.answer {
                        NodeAnswer::Ring { ring, .. } => Ok(members(&ring)),
                        other => Err(std::io::Error::other(format!("{other:?}"))),
                    }
                }
            };
            assert!(ring_of_node_0("impostor-secret").await.is_err());
            let impostor = cluster.start(1).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(ring_of_node_0("cluster-secret").await.unwrap(), [0]);
            impostor.stop();
            std::fs::write(&cluster.nodes[1].1, config).unwrap();
            let _member = cluster.start(1).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(ring_of_node_0("cluster-secret").await.unwrap(), [0, 1]);
        })
        .await;
}

fn members(ring: &Ring) -> Vec<u64> {
    ring.members().iter().map(|member| member.id.0).collect()
}

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

/// A node starts and serves while its config names another node whose
/// address does not resolve, as when that node's DNS name is gone.
#[tokio::test(flavor = "current_thread")]
async fn a_node_starts_while_another_address_does_not_resolve() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster =
                Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 1, GOSSIP, &[]);
            let gone = r#"{ id = 1, address = "no-such-node.invalid:9100" }"#;
            for config in [&cluster.node, &cluster.gateway] {
                let text = std::fs::read_to_string(config).unwrap();
                let text: Vec<String> = text
                    .lines()
                    .map(|line| match line.trim_start().starts_with("nodes = [") {
                        true => line.replace(" }]", &format!(" }}, {gone}]")),
                        false => line.to_string(),
                    })
                    .collect();
                std::fs::write(config, text.join("\n")).unwrap();
            }
            let _node = cluster.start_node().await;
            let _gateway = cluster.start_gateway().await;
            let mut answer = cluster.get("k").await;
            // The gateway may first try the node it cannot reach.
            if answer.0 >= 500 {
                answer = cluster.get("k").await;
            }
            assert_eq!(answer, (200, origin.object("/bucket/k")));
        })
        .await;
}

/// A gateway holds no S3 credentials: writes, deletes and listings pass
/// through storage nodes, and a listing S3 sends chunked reaches the client
/// whole.
#[tokio::test(flavor = "current_thread")]
async fn requests_the_cache_does_not_serve_pass_through_nodes() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.chunked.set(true);
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, "", &[]);
            let gateway = std::fs::read_to_string(&cluster.gateway).unwrap();
            assert!(!gateway.contains("[origin]"));
            let _nodes = [cluster.start(0).await, cluster.start(1).await];
            let _gateway = cluster.start_gateway().await;
            let port = cluster.gateway_port;
            let body = b"written through a node".to_vec();
            let (status, _) = send(port, "PUT", "/bucket/new", "", &[], body.clone()).await;
            assert_eq!(status, 200);
            assert_eq!(*origin.uploads.borrow(), [body]);
            // The second listing reuses the node connection the first left.
            for _ in 0..2 {
                let listed = send(port, "GET", "/bucket", "list-type=2", &[], Vec::new()).await;
                assert_eq!(listed, (200, LISTING.as_bytes().to_vec()));
            }
            let (status, _) = send(port, "DELETE", "/bucket/new", "", &[], Vec::new()).await;
            assert_eq!(status, 200);
        })
        .await;
}

/// A write through a gateway reaches the key's home before the client
/// hears, so the gateway's next read returns what was written, though the
/// gateway and the home both held the old version's metadata and the home
/// its blocks.
#[tokio::test(flavor = "current_thread")]
async fn a_read_after_a_write_through_the_gateway_sees_the_write() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, "", &[]);
            let _nodes = [cluster.start(0).await, cluster.start(1).await];
            let _gateway = cluster.start_gateway().await;
            let port = cluster.gateway_port;
            let path = "/changing/k";
            let first = origin.object(path);
            for _ in 0..2 {
                let read = send(port, "GET", path, "", &[], Vec::new()).await;
                assert!(
                    read == (200, first.clone()),
                    "{} {:?}",
                    read.0,
                    String::from_utf8_lossy(&read.1[..read.1.len().min(300)])
                );
            }
            let written = b"the new version".to_vec();
            let (status, _) = send(port, "PUT", path, "", &[], written.clone()).await;
            assert_eq!(status, 200);
            let read = send(port, "GET", path, "", &[], Vec::new()).await;
            assert_eq!(read, (200, written));
        })
        .await;
}

/// S3 tells the cluster of a change made elsewhere through its event
/// queue. Only node 0 polls the queue, and the key's home is node 1, so
/// node 0 passes the event on; the home drops the old version, and the
/// next read returns the new one. An event for the version the cache
/// holds changes nothing, so the read after it costs no S3 request.
#[tokio::test(flavor = "current_thread")]
async fn an_event_from_the_queue_reaches_the_home() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let (queue_port, queue) = start_queue().await;
            let cluster =
                Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, "", &[]);
            let events = format!(
                "[events]\nqueue_url = \"http://127.0.0.1:{queue_port}/000000000000/events\"\nvisibility_timeout_s = 2\n"
            );
            let config = &cluster.nodes[0].1;
            let text = std::fs::read_to_string(config).unwrap();
            std::fs::write(config, format!("{events}{text}")).unwrap();
            let _nodes = [cluster.start(0).await, cluster.start(1).await];
            let _gateway = cluster.start_gateway().await;
            let homed_on_1 = |name: &String| {
                let members = [0, 1].map(|id| Member {
                    id: NodeId(id),
                    weight: NonZeroU32::MIN,
                });
                let key = ObjectKey {
                    bucket: "changing".into(),
                    key: name.clone(),
                };
                Ring::new(0, members.to_vec()).owner(Placement::Home(&key).hash()) == Some(NodeId(1))
            };
            let name = (0..).map(|index| format!("k{index}")).find(homed_on_1).unwrap();
            let path = format!("/changing/{name}");
            let port = cluster.gateway_port;
            let first = origin.object(&path);
            for _ in 0..2 {
                let read = send(port, "GET", &path, "", &[], Vec::new()).await;
                assert!(read == (200, first.clone()), "{}", read.0);
            }
            let drained = || async {
                while queue.len() > 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // The gateway's entries expire after a second.
                tokio::time::sleep(Duration::from_millis(1_100)).await;
            };
            let changed = b"changed elsewhere".to_vec();
            let version = ("\"elsewhere\"".to_string(), changed.clone());
            origin.written.borrow_mut().insert(path.clone(), version);
            queue.send("changing", &name, Some("elsewhere"));
            tokio::time::timeout(Duration::from_secs(10), drained())
                .await
                .expect("a node deletes the message once the home has the event");
            let read = send(port, "GET", &path, "", &[], Vec::new()).await;
            assert_eq!(read, (200, changed.clone()));
            let before = origin.requests.get();
            queue.send("changing", &name, Some("elsewhere"));
            tokio::time::timeout(Duration::from_secs(10), drained())
                .await
                .expect("a node deletes the message");
            let read = send(port, "GET", &path, "", &[], Vec::new()).await;
            assert_eq!(read, (200, changed));
            assert_eq!(origin.requests.get(), before);
        })
        .await;
}

/// A key read often enough is leased to the home's next two candidates,
/// which fill from the home and store its blocks, and the gateway spreads
/// reads across all three. With the home stopped, reads still come from
/// the replicas' blocks, with no request to S3.
#[tokio::test(flavor = "current_thread")]
async fn a_hot_key_is_read_from_its_replicas() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cache = format!(
                "{CLUSTER_CACHE}\nhot_threshold = 5\nhot_window_ms = 10000\nlease_ms = 60000"
            );
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, &cache, 3, "", &[]);
            let mut nodes: Vec<_> = Vec::new();
            for id in 0..3 {
                nodes.push(Some(cluster.start(id).await));
            }
            let _gateway = cluster.start_gateway().await;
            for _ in 0..30 {
                assert_eq!(cluster.get("hot").await, (200, object()));
            }
            assert_eq!(origin.requests.get(), 1);
            let members = [0, 1, 2].map(|id| Member {
                id: NodeId(id),
                weight: NonZeroU32::MIN,
            });
            let key = ObjectKey {
                bucket: "bucket".into(),
                key: "hot".into(),
            };
            let home = Ring::new(0, members.to_vec())
                .owner(Placement::Home(&key).hash())
                .unwrap();
            nodes[home.0 as usize].take().unwrap().stop();
            for _ in 0..6 {
                assert_eq!(cluster.get("hot").await, (200, object()));
            }
            assert_eq!(origin.requests.get(), 1);
        })
        .await;
}

/// An upload passes through its home, which keeps what it holds of the
/// body and checks with a HEAD that S3 holds the version: the first read
/// after the check comes from the home's disk, with no further request to
/// S3.
#[tokio::test(flavor = "current_thread")]
async fn an_upload_warms_its_home() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, CLUSTER_CACHE, 2, "", &[]);
            let _nodes = [cluster.start(0).await, cluster.start(1).await];
            let _gateway = cluster.start_gateway().await;
            let port = cluster.gateway_port;
            let body = object_of(200_000, "warm");
            let (status, _) = send(port, "PUT", "/warm/k", "", &[], body.clone()).await;
            assert_eq!(status, 200);
            // The home checks the version after the client hears, and a
            // read before it finishes would miss.
            let checked = async {
                while origin.requests.get() < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            tokio::time::timeout(Duration::from_secs(5), checked)
                .await
                .expect("the home sends a HEAD");
            assert_eq!(origin.requests.get(), 2);
            let read = send(port, "GET", "/warm/k", "", &[], Vec::new()).await;
            assert!(read == (200, body), "{}", read.0);
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}

/// A reader asks for a Parquet file's last 8 bytes, which state its
/// footer's length. The home fills the block they lie in, reads the
/// length, and fills the footer's other block: the reader's next read, of
/// the footer, comes from disk.
#[tokio::test(flavor = "current_thread")]
async fn a_parquet_footer_is_prefetched() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let cluster = Cluster::new(&data_dir(), origin_port, CLUSTER_CACHE);
            let _node = cluster.start_node().await;
            let _gateway = cluster.start_gateway().await;
            let port = cluster.gateway_port;
            let path = "/bucket/table.parquet";
            let object = origin.object(path);
            let size = object.len();
            let (status, trailer) =
                send(port, "GET", path, "", &[("range", "bytes=-8")], Vec::new()).await;
            assert_eq!((status, &trailer[4..]), (206, &b"PAR1"[..]));
            let footer = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as usize;
            // The read, the block holding the trailer, and the footer's
            // other block.
            let prefetched = async {
                while origin.requests.get() < 3 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            tokio::time::timeout(Duration::from_secs(5), prefetched)
                .await
                .expect("the home prefetches the footer");
            let (first, last) = (size - 8 - footer, size - 9);
            let range = format!("bytes={first}-{last}");
            let read = send(port, "GET", path, "", &[("range", &range)], Vec::new()).await;
            assert!(read == (206, object[first..=last].to_vec()), "{}", read.0);
            assert_eq!(origin.requests.get(), 3);
        })
        .await;
}

/// A purge through the gateway drops an object from the cache. A node that
/// holds some of the object's chunks is down during the purge; its home
/// tells it again once it is back. S3's bytes change under the same ETag,
/// so a read after the purge returns the new bytes only if no node kept
/// the old ones.
#[tokio::test(flavor = "current_thread")]
async fn a_purge_reaches_a_node_that_was_down() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.size.set(3_500_000);
            let cache = format!("{CLUSTER_CACHE}\npeer_timeout_ms = 100");
            let cluster = Cluster::with_nodes(&data_dir(), origin_port, &cache, 2, "", &[]);
            let mut nodes: Vec<_> = Vec::new();
            for id in 0..2 {
                nodes.push(Some(cluster.start(id).await));
            }
            let _gateway = cluster.start_gateway().await;
            // A key whose chunks are not all on its home.
            let members = [0, 1].map(|id| Member {
                id: NodeId(id),
                weight: NonZeroU32::MIN,
            });
            let ring = Ring::new(0, members.to_vec());
            let layout = Layout::new(65_536, 16);
            let (name, home, other) = (0..)
                .find_map(|index| {
                    let key = ObjectKey {
                        bucket: "bucket".into(),
                        key: format!("big-{index}"),
                    };
                    let home = ring.owner(Placement::Home(&key).hash())?;
                    let other = (0..layout.block_count(3_500_000))
                        .filter_map(|block| {
                            ring.owner(layout.placement(&key, 3_500_000, block).hash())
                        })
                        .find(|owner| *owner != home)?;
                    Some((key.key, home, other))
                })
                .unwrap();
            assert_ne!(home, other);
            for _ in 0..2 {
                assert_eq!(cluster.get(&name).await, (200, object_of(3_500_000, "")));
            }
            let path = format!("/bucket/{name}");
            let changed = object_of(3_500_000, "changed");
            origin
                .written
                .borrow_mut()
                .insert(path.clone(), (common::ETAG.to_string(), changed.clone()));
            nodes[other.0 as usize].take().unwrap().stop();
            let (status, _) = send(
                cluster.gateway_port,
                "POST",
                &path,
                "x-accel-purge=",
                &[],
                Vec::new(),
            )
            .await;
            assert_eq!(status, 204);
            nodes[other.0 as usize] = Some(cluster.start(other.0 as usize).await);
            // The home tells the node again every 400 ms.
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let read = cluster.get(&name).await;
            assert!(read == (200, changed), "{}", read.0);
        })
        .await;
}
