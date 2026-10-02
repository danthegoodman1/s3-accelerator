//! TLS between S3 clients and gateways, and mutual TLS among cluster
//! members: rustls runs the handshake, the kernel carries the session, and
//! hits still leave nodes through `sendfile` and gateways through `splice`.
//! Evidence comes from outside the server: the kernel's TLS counters, `ss`'s
//! view of each socket, and the system calls `strace` records.

mod common;

use common::trace::{
    Call, WRITES, check_writes_carry_at_most, check_writes_carry_no_body, now, read_trace, windows,
};
use common::{Cluster, Process, Server, data_dir, object, signed, start_origin};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use s3_accelerator::http;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio_rustls::TlsConnector;
use xxhash_rust::xxh3::xxh3_64;

/// Three whole 64 KiB blocks.
const OBJECT_SIZE: usize = 3 * 65536;

/// A self-signed certificate for 127.0.0.1, written to `dir`: the paths of
/// its chain and key, and the PEM a client trusts.
fn certificate(dir: &Path) -> (String, String, Vec<u8>) {
    let names = vec!["127.0.0.1".to_string(), "localhost".to_string()];
    let certified = rcgen::generate_simple_self_signed(names).unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    let pem = certified.cert.pem();
    std::fs::write(&cert, &pem).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    let path = |path: &Path| path.display().to_string();
    (path(&cert), path(&key), pem.into_bytes())
}

/// The `[gateway.tls]` table for a certificate in `dir`.
fn tls_table(dir: &Path, kernel: bool) -> (String, Vec<u8>) {
    let (cert, key, trusted) = certificate(&dir.join("tls"));
    let table = format!("[gateway.tls]\ncert = \"{cert}\"\nkey = \"{key}\"\nkernel = {kernel}\n");
    (table, trusted)
}

/// A cluster's CA, which signs its members' certificates.
struct CertificateAuthority(rcgen::Issuer<'static, rcgen::KeyPair>, String);

impl CertificateAuthority {
    fn new() -> CertificateAuthority {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "cluster CA");
        let pem = params.self_signed(&key).unwrap().pem();
        CertificateAuthority(rcgen::Issuer::new(params, key), pem)
    }

    /// The `[cluster.tls]` table for a member whose certificate this CA
    /// signs for 127.0.0.1, written to a new directory in `dir`.
    fn table(&self, dir: &Path, kernel: bool) -> String {
        self.table_trusting(self, dir, kernel)
    }

    /// As `table`, for a member that trusts the members `trusted` signs.
    fn table_trusting(&self, trusted: &CertificateAuthority, dir: &Path, kernel: bool) -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let cert = params.signed_by(&key, &self.0).unwrap();
        let dir = (0..)
            .map(|index| dir.join(format!("member-{index}")))
            .find(|dir| !dir.exists())
            .unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        let path = |name: &str, pem: &str| {
            let path = dir.join(name);
            std::fs::write(&path, pem).unwrap();
            path.display().to_string()
        };
        let (ca, cert, key) = (
            path("ca.pem", &trusted.1),
            path("cert.pem", &cert.pem()),
            path("key.pem", &key.serialize_pem()),
        );
        format!(
            "[cluster.tls]\nca = \"{ca}\"\ncert = \"{cert}\"\nkey = \"{key}\"\nkernel = {kernel}\n"
        )
    }
}

/// A client that trusts `trusted` and keeps its connection between
/// requests.
fn client(trusted: &[u8]) -> reqwest::Client {
    let certificate = reqwest::Certificate::from_pem(trusted).unwrap();
    reqwest::Client::builder()
        .add_root_certificate(certificate)
        .build()
        .unwrap()
}

/// A signed GET over TLS.
async fn get(client: &reqwest::Client, port: u16, path: &str) -> (u16, Vec<u8>) {
    get_over(client, "https", port, path).await
}

/// A signed GET over `scheme`.
async fn get_over(client: &reqwest::Client, scheme: &str, port: u16, path: &str) -> (u16, Vec<u8>) {
    let mut request = client.get(format!("{scheme}://127.0.0.1:{port}{path}"));
    for (name, value) in signed(port, "GET", path, "", &[]) {
        request = request.header(name, value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.bytes().await.unwrap().to_vec())
}

/// A client reads through userspace TLS, first from S3 and then from the
/// cache, every byte decrypted correctly.
#[tokio::test(flavor = "current_thread")]
async fn a_client_reads_over_userspace_tls() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let dir = data_dir();
            let (tls, trusted) = tls_table(&dir, false);
            let server = Server::start(origin_port, &dir, r#"{ bucket = "bucket" }"#, &tls).await;
            let client = client(&trusted);
            // The first read fetches the object, the second stores its
            // blocks past the doorkeeper, and the third hits.
            for _ in 0..3 {
                assert_eq!(
                    get(&client, server.port, "/bucket/k").await,
                    (200, object())
                );
            }
            assert_eq!(origin.requests.get(), 2);
        })
        .await;
}

/// A request that asks the gateway to close the connection after its
/// answer, read to the end of the connection. Reading fails if the session
/// ends without a `close_notify`, as a cut-off connection does.
async fn read_to_close(port: u16, path: &str, trusted: &[u8]) -> io::Result<Vec<u8>> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(trusted) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let socket = TcpStream::connect(("127.0.0.1", port)).await?;
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let mut session = TlsConnector::from(Arc::new(config))
        .connect(name, socket)
        .await?;
    let mut request = format!("GET {path} HTTP/1.1\r\nconnection: close\r\n");
    for (name, value) in signed(port, "GET", path, "", &[]) {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    session.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    session.read_to_end(&mut response).await?;
    Ok(response)
}

/// A session ends with a `close_notify` once the gateway closes the
/// connection.
async fn check_close_notify(kernel: bool) {
    LocalSet::new()
        .run_until(async {
            let (origin_port, _origin) = start_origin().await;
            let dir = data_dir();
            let (tls, trusted) = tls_table(&dir, kernel);
            let server = Server::start(origin_port, &dir, r#"{ bucket = "bucket" }"#, &tls).await;
            for _ in 0..3 {
                let response = read_to_close(server.port, "/bucket/k", &trusted)
                    .await
                    .expect("the session ends with a close_notify");
                assert!(response.starts_with(b"HTTP/1.1 200"));
                assert!(response.ends_with(&object()));
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_userspace_tls_session_ends_with_close_notify() {
    check_close_notify(false).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls)"]
async fn a_kernel_tls_session_ends_with_close_notify() {
    let _counters = COUNTERS.lock().await;
    tls_stat();
    check_close_notify(true).await;
}

/// A node takes only members whose certificates the cluster's CA signed,
/// and a gateway reaches only such nodes.
#[tokio::test(flavor = "current_thread")]
async fn members_prove_themselves_with_the_clusters_certificates() {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            let dir = data_dir();
            let cluster = Cluster::new(&dir, origin_port, common::CLUSTER_CACHE);
            let (cluster_ca, other_ca) = (CertificateAuthority::new(), CertificateAuthority::new());
            let gateway_config = std::fs::read_to_string(&cluster.gateway).unwrap();
            let node_config = std::fs::read_to_string(&cluster.node).unwrap();
            let configure = |node: &str, gateway: &str| {
                std::fs::write(&cluster.node, format!("{node_config}\n{node}")).unwrap();
                std::fs::write(&cluster.gateway, format!("{gateway_config}\n{gateway}")).unwrap();
            };
            let member = || cluster_ca.table(&dir, false);
            let impostor = || other_ca.table_trusting(&cluster_ca, &dir, false);
            let cases = [
                (
                    "a gateway with another CA's certificate",
                    member(),
                    impostor(),
                ),
                ("a gateway without TLS", member(), String::new()),
                ("a node with another CA's certificate", impostor(), member()),
                ("a node without TLS", String::new(), member()),
            ];
            for (case, node, gateway) in cases {
                configure(&node, &gateway);
                let node = cluster.start_node().await;
                let gateway = cluster.start_gateway().await;
                let (status, _) = cluster.get("k").await;
                assert!(status >= 500, "{case}: {status}");
                gateway.stop();
                node.stop();
            }
            assert_eq!(origin.requests.get(), 0);
            configure(&member(), &member());
            let _node = cluster.start_node().await;
            let _gateway = cluster.start_gateway().await;
            assert_eq!(cluster.get("k").await, (200, origin.object("/bucket/k")));
        })
        .await;
}

/// Reads each object twice through a node and a gateway, with TLS between
/// the client and the gateway and mutual TLS between the gateway and the
/// node, all carried by the kernel; then again with both forced onto
/// userspace TLS. The kernel run must show every sign of kTLS on both
/// links, and the userspace run none.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls), strace, and sudo for ss"]
async fn hits_reach_kernel_tls_sessions_through_sendfile_and_splice() {
    let _counters = COUNTERS.lock().await;
    tls_stat();
    let total = 3 * OBJECT_SIZE as i64;
    // Bytes a gateway may read with the answers' heads, which reach the
    // client by a write rather than `splice`.
    let ahead = (3 * http::READ_AHEAD) as i64;

    let kernel = evidence(true, true, true, OBJECT_SIZE).await;
    println!("kernel TLS: {kernel:#?}");
    for (name, link) in [("client", &kernel.clients), ("peer", &kernel.peers)] {
        assert!(link.sockets > 0, "ss found no {name} connection");
        assert_eq!(
            link.ulp_sockets, link.sockets,
            "ss shows the tls ULP on {} of {} {name} sockets",
            link.ulp_sockets, link.sockets
        );
        assert!(
            link.ulp_calls >= link.sockets,
            "{} calls gave {} {name} sockets the tls ULP",
            link.ulp_calls,
            link.sockets
        );
        assert!(
            link.zero_copy >= total - ahead,
            "{} of {total} hit bytes reached {name} sockets inside the kernel",
            link.zero_copy
        );
        assert!(
            link.written < total / 16 + ahead,
            "{} bytes were written to {name} sockets",
            link.written
        );
        assert_eq!(
            link.on_event_loop, 0,
            "the event loop moved {} hit bytes into {name} sockets, encrypting them",
            link.on_event_loop
        );
    }
    let sockets = (kernel.clients.sockets + kernel.peers.sockets) as u64;
    assert!(
        kernel.tx_sessions >= sockets && kernel.rx_sessions >= sockets,
        "the kernel set up {} sending and {} receiving TLS sessions for {sockets} sockets",
        kernel.tx_sessions,
        kernel.rx_sessions
    );

    let userspace = evidence(false, true, true, OBJECT_SIZE).await;
    println!("userspace TLS: {userspace:#?}");
    for (name, link) in [("client", &userspace.clients), ("peer", &userspace.peers)] {
        assert!(link.sockets > 0, "ss found no {name} connection");
        assert_eq!(
            (link.ulp_sockets, link.ulp_calls, link.zero_copy),
            (0, 0, 0)
        );
        assert!(
            link.written >= total,
            "the relay wrote {} bytes to {name} sockets",
            link.written
        );
    }
    assert_eq!((userspace.tx_sessions, userspace.rx_sessions), (0, 0));
}

/// Over plaintext links to the node, a gateway reads each answer's first
/// bytes with its head. A kernel TLS client still gets them from a worker,
/// which encrypts them as it writes, and none from an event loop.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls), strace, and sudo for ss"]
async fn a_kernel_tls_client_takes_held_bytes_from_a_worker() {
    let _counters = COUNTERS.lock().await;
    let evidence = evidence(true, true, false, OBJECT_SIZE).await;
    assert!(
        evidence.held > 0,
        "the gateway held none of the answers' bytes"
    );
    assert_eq!(
        evidence.held_on_event_loop, 0,
        "an event loop wrote held bytes to a kernel TLS client"
    );
    assert_eq!(evidence.clients.on_event_loop, 0);
}

/// Behind a plaintext client, as a gateway on the client's host serves it,
/// workers splice and so decrypt the body of a kernel TLS node answer,
/// past the bytes read with its head.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls), strace, and sudo for ss"]
async fn kernel_tls_node_answers_are_decrypted_on_workers() {
    let _counters = COUNTERS.lock().await;
    let evidence = evidence(true, false, true, OBJECT_SIZE).await;
    println!("{evidence:#?}");
    let ahead = (3 * http::READ_AHEAD) as i64;
    let total = 3 * OBJECT_SIZE as i64;
    assert!(
        evidence.from_peers >= total - ahead,
        "{} of {total} hit bytes left the node's links by splice",
        evidence.from_peers
    );
    assert_eq!(
        evidence.from_peers_on_event_loop, 0,
        "an event loop spliced, and so decrypted, a node's answer"
    );
}

/// A hit within one TLS record crosses kernel TLS links on event loops: the
/// node sends it with `sendfile` from its event loop, and the gateway
/// writes it with its head from one of its loops, each encrypting in the
/// kernel, with no worker between them.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls), strace, and sudo for ss"]
async fn a_small_hit_crosses_kernel_tls_links_on_event_loops() {
    let _counters = COUNTERS.lock().await;
    let size = 4096;
    let evidence = evidence(true, true, true, size).await;
    println!("{evidence:#?}");
    let total = 3 * size as i64;
    assert!(
        evidence.held_on_event_loop >= total && evidence.held_on_event_loop == evidence.held,
        "the gateway's event loops wrote {} of the {} bytes of writes carrying {total} body bytes",
        evidence.held_on_event_loop,
        evidence.held
    );
    assert_eq!(
        evidence.peers.on_event_loop, total,
        "the node's event loop sent {} of {total} hit bytes",
        evidence.peers.on_event_loop
    );
}

/// The threads of process `pid` that run event loops: its main thread, and
/// a gateway's loops.
fn event_loops(pid: u32) -> Vec<u32> {
    let mut loops = vec![pid];
    for task in std::fs::read_dir(format!("/proc/{pid}/task")).unwrap() {
        let task = task.unwrap();
        let name = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
        if name.trim().starts_with("gateway-") {
            loops.push(task.file_name().to_string_lossy().parse().unwrap());
        }
    }
    loops
}

/// Tests that set up kernel TLS sessions or count them take turns.
static COUNTERS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What `ss`, `strace` and the kernel's counters show of TLS in a cluster.
#[derive(Debug)]
struct Evidence {
    /// Clients' connections to the gateway, and the gateway's to the node.
    clients: Link,
    peers: Link,
    /// Sessions the kernel set up to encrypt and to decrypt.
    tx_sessions: u64,
    rx_sessions: u64,
    /// Body bytes the gateway wrote to clients from userspace, and those
    /// its event loops wrote.
    held: i64,
    held_on_event_loop: i64,
    /// Body bytes the gateway spliced out of its links to the node, and
    /// those its event loops spliced.
    from_peers: i64,
    from_peers_on_event_loop: i64,
}

/// One kind of connection.
#[derive(Debug)]
struct Link {
    /// Sockets `ss` lists, and those with the tls ULP: the gateway's ends
    /// of client connections, and both ends of peer connections.
    sockets: usize,
    ulp_sockets: usize,
    /// Calls that gave those sockets the tls ULP.
    ulp_calls: usize,
    /// Hit bytes the server moved into its ends of these connections inside
    /// the kernel: the gateway's `splice` to clients, and the node's
    /// `sendfile` to the gateway.
    zero_copy: i64,
    /// Those bytes the process's event loops moved.
    on_event_loop: i64,
    /// Bytes written to the same ends from userspace during the hits.
    written: i64,
}

/// Reads three objects of `size` bytes twice through a traced node and
/// gateway, with every
/// session in the kernel or in userspace, the client's link to the gateway
/// over TLS when `clients_tls`, and the gateway's links to the node over
/// TLS when `peers_tls`; each is plaintext otherwise.
async fn evidence(kernel: bool, clients_tls: bool, peers_tls: bool, size: usize) -> Evidence {
    LocalSet::new()
        .run_until(async {
            let before = tls_stat();
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(size);
            let dir = data_dir();
            let cache = "block_size = 65536\nextent_size = 1048576\nextents = 4";
            let cluster = Cluster::new(&dir, origin_port, cache);
            let (tls, trusted) = tls_table(&dir, kernel);
            if clients_tls {
                append(&cluster.gateway, &tls);
            }
            if peers_tls {
                let members = CertificateAuthority::new();
                for config in [&cluster.node, &cluster.gateway] {
                    append(config, &members.table(&dir, kernel));
                }
            }
            let (node_trace, gateway_trace) = (dir.join("node.trace"), dir.join("gateway.trace"));
            let calls = format!("sendfile,splice,setsockopt,{WRITES}");
            let node = Process::traced(&cluster.node, &node_trace, &calls);
            common::listening(cluster.node_port).await;
            let gateway = Process::traced(&cluster.gateway, &gateway_trace, &calls);
            let port = cluster.gateway_port;
            common::listening(port).await;
            let (node_loop, gateway_loop) = (node.server_pid(), gateway.server_pid());

            let client = client(&trusted);
            let scheme = if clients_tls { "https" } else { "http" };
            let keys = ["a", "b", "c"];
            let bodies: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| origin.object(&format!("/bucket/{key}")))
                .collect();
            for (key, body) in keys.iter().zip(&bodies) {
                let path = format!("/bucket/{key}");
                assert!(
                    get_over(&client, scheme, port, &path).await == (200, body.clone()),
                    "{key}"
                );
            }
            let hits_from = now();
            for (key, body) in keys.iter().zip(&bodies) {
                let path = format!("/bucket/{key}");
                assert!(
                    get_over(&client, scheme, port, &path).await == (200, body.clone()),
                    "{key}"
                );
            }
            let hits_until = now();
            assert_eq!(origin.requests.get(), keys.len() as u64);
            let client_sockets = sockets(&format!("( sport = :{port} )"));
            let node_port = cluster.node_port;
            let peer_sockets =
                sockets(&format!("( sport = :{node_port} or dport = :{node_port} )"));
            let after = tls_stat();
            // The gateway's event loops: its main thread, which accepts
            // clients, and a thread per loop.
            let gateway_loops = event_loops(gateway_loop);
            drop(client);
            gateway.stop();
            node.stop();

            let total = (keys.len() * size) as i64;
            let windows = windows(&bodies);
            let gateway_calls = read_trace(&gateway_trace, 0.0, f64::MAX);
            let node_calls = read_trace(&node_trace, 0.0, f64::MAX);
            // `strace -yy` names a socket by its ends: the gateway's end of a
            // client connection starts at its port, and the node's end of a
            // peer connection at the node's.
            let gateway_end = format!(":{port}->");
            let node_end = format!(":{node_port}->");
            let peer_end = format!("->127.0.0.1:{node_port}]");
            let ulp_calls = |calls: &[Call], end: &str| {
                calls
                    .iter()
                    .filter(|call| call.name == "setsockopt" && call.result == 0)
                    .filter(|call| call.args.contains("TCP_ULP") && call.data() == b"tls")
                    .filter(|call| on(call.fds().first(), end))
                    .count()
            };
            let during_hits = |calls: &[Call]| -> Vec<Call> {
                calls
                    .iter()
                    .filter(|call| call.end >= hits_from && call.end <= hits_until)
                    .cloned()
                    .collect()
            };
            let (gateway_hits, node_hits) = (during_hits(&gateway_calls), during_hits(&node_calls));
            // Bytes moved into a link's ends by `name` calls, whose
            // destination is argument `at`, on any thread or on `thread`.
            let moved =
                |calls: &[Call], name: &str, at: usize, end: &str, threads: Option<&[u32]>| {
                    calls
                        .iter()
                        .filter(|call| call.name == name && on(call.fds().get(at), end))
                        .filter(|call| threads.is_none_or(|threads| threads.contains(&call.thread)))
                        .map(|call| call.result.max(0))
                        .sum::<i64>()
                };
            let written = |calls: &[Call], end: &str| -> i64 {
                calls
                    .iter()
                    .filter(|call| WRITES.split(',').any(|name| name == call.name))
                    .filter(|call| on(call.fds().first(), end))
                    .map(|call| call.result.max(0))
                    .sum()
            };
            if kernel {
                // No write carries body bytes anywhere, to a socket or to a
                // pipe, but those a gateway read with each answer's head,
                // which `held` counts.
                let ahead = (keys.len() * http::READ_AHEAD) as i64;
                check_writes_carry_at_most("gateway", &gateway_hits, &windows, total, ahead);
                check_writes_carry_no_body("node", &node_hits, &windows, total);
            }
            // Body bytes the gateway wrote to clients from userspace, from
            // any thread or from its event loops.
            let held = |threads: Option<&[u32]>| -> i64 {
                gateway_hits
                    .iter()
                    .filter(|call| WRITES.split(',').any(|name| name == call.name))
                    .filter(|call| on(call.fds().first(), &gateway_end))
                    .filter(|call| threads.is_none_or(|threads| threads.contains(&call.thread)))
                    .filter(|call| {
                        call.data()
                            .windows(32)
                            .any(|window| windows.contains(&xxh3_64(window)))
                    })
                    .map(|call| call.result.max(0))
                    .sum()
            };
            let rose = |names: [&str; 2]| -> u64 {
                names.iter().map(|name| after[*name] - before[*name]).sum()
            };
            Evidence {
                clients: Link {
                    sockets: client_sockets.0,
                    ulp_sockets: client_sockets.1,
                    ulp_calls: ulp_calls(&gateway_calls, &gateway_end),
                    zero_copy: moved(&gateway_hits, "splice", 1, &gateway_end, None),
                    on_event_loop: moved(
                        &gateway_hits,
                        "splice",
                        1,
                        &gateway_end,
                        Some(&gateway_loops),
                    ),
                    written: written(&gateway_hits, &gateway_end),
                },
                peers: Link {
                    sockets: peer_sockets.0,
                    ulp_sockets: peer_sockets.1,
                    ulp_calls: ulp_calls(&node_calls, &node_end)
                        + ulp_calls(&gateway_calls, &peer_end),
                    zero_copy: moved(&node_hits, "sendfile", 0, &node_end, None),
                    on_event_loop: moved(&node_hits, "sendfile", 0, &node_end, Some(&[node_loop])),
                    written: written(&node_hits, &node_end),
                },
                tx_sessions: rose(["TlsTxSw", "TlsTxDevice"]),
                rx_sessions: rose(["TlsRxSw", "TlsRxDevice"]),
                held: held(None),
                held_on_event_loop: held(Some(&gateway_loops)),
                from_peers: moved(&gateway_hits, "splice", 0, &peer_end, None),
                from_peers_on_event_loop: moved(
                    &gateway_hits,
                    "splice",
                    0,
                    &peer_end,
                    Some(&gateway_loops),
                ),
            }
        })
        .await
}

/// Whether a descriptor names a socket with the end `end`.
fn on(fd: Option<&String>, end: &str) -> bool {
    fd.is_some_and(|fd| fd.contains(end))
}

fn append(config: &Path, table: &str) {
    let text = std::fs::read_to_string(config).unwrap();
    std::fs::write(config, format!("{text}\n{table}")).unwrap();
}

/// The kernel's TLS counters, which count sessions it set up since boot.
fn tls_stat() -> BTreeMap<String, u64> {
    let stat = std::fs::read_to_string("/proc/net/tls_stat")
        .expect("/proc/net/tls_stat is missing: load the tls module with `sudo modprobe tls`");
    stat.lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(char::is_whitespace)?;
            Some((name.to_string(), value.trim().parse().ok()?))
        })
        .collect()
}

/// The established sockets `ss` lists with `filter`, and how many of them
/// have the tls ULP. Only CAP_NET_ADMIN sees a socket's ULP, so `ss` runs
/// under `sudo` unless the test runs as root.
fn sockets(filter: &str) -> (usize, usize) {
    // SAFETY: geteuid only reads the process's credentials.
    let root = unsafe { libc::geteuid() } == 0;
    let mut command = if root {
        Command::new("ss")
    } else {
        let mut sudo = Command::new("sudo");
        sudo.args(["-n", "ss"]);
        sudo
    };
    let output = command
        .args(["-tieH", "state", "established", filter])
        .output()
        .expect("ss runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "ss failed: {stderr}");
    let listing = String::from_utf8(output.stdout).unwrap();
    // Each socket's first line starts in the first column, and its details
    // follow on indented lines.
    let sockets = listing
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with(char::is_whitespace))
        .count();
    (sockets, listing.matches("tcp-ulp-tls").count())
}
