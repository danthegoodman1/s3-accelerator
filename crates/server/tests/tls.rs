//! TLS between S3 clients and gateways, and mutual TLS among cluster
//! members: rustls runs the handshake, the kernel carries the session, and
//! hits still leave nodes through `sendfile` and gateways through `splice`.
//! Evidence comes from outside the server: the kernel's TLS counters, `ss`'s
//! view of each socket, and the system calls `strace` records.

mod common;

use common::trace::{Call, WRITES, check_writes_carry_no_body, now, read_trace, windows};
use common::{Cluster, Process, Server, data_dir, object, signed, start_origin};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio_rustls::TlsConnector;

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
    let mut request = client.get(format!("https://127.0.0.1:{port}{path}"));
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

    let kernel = evidence(true).await;
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
            link.zero_copy >= total,
            "{} of {total} hit bytes reached {name} sockets inside the kernel",
            link.zero_copy
        );
        assert!(
            link.written < total / 16,
            "{} bytes were written to {name} sockets",
            link.written
        );
    }
    let sockets = (kernel.clients.sockets + kernel.peers.sockets) as u64;
    assert!(
        kernel.tx_sessions >= sockets && kernel.rx_sessions >= sockets,
        "the kernel set up {} sending and {} receiving TLS sessions for {sockets} sockets",
        kernel.tx_sessions,
        kernel.rx_sessions
    );

    let userspace = evidence(false).await;
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
    /// Bytes written to the same ends from userspace during the hits.
    written: i64,
}

/// Reads three objects twice through a traced node and gateway, with every
/// session in the kernel or in userspace.
async fn evidence(kernel: bool) -> Evidence {
    LocalSet::new()
        .run_until(async {
            let before = tls_stat();
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cache = "block_size = 65536\nextent_size = 1048576\nextents = 4";
            let cluster = Cluster::new(&dir, origin_port, cache);
            let (tls, trusted) = tls_table(&dir, kernel);
            append(&cluster.gateway, &tls);
            let members = CertificateAuthority::new();
            for config in [&cluster.node, &cluster.gateway] {
                append(config, &members.table(&dir, kernel));
            }
            let (node_trace, gateway_trace) = (dir.join("node.trace"), dir.join("gateway.trace"));
            let calls = format!("sendfile,splice,setsockopt,{WRITES}");
            let node = Process::traced(&cluster.node, &node_trace, &calls);
            common::listening(cluster.node_port).await;
            let gateway = Process::traced(&cluster.gateway, &gateway_trace, &calls);
            let port = cluster.gateway_port;
            common::listening(port).await;

            let client = client(&trusted);
            let keys = ["a", "b", "c"];
            let bodies: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| origin.object(&format!("/bucket/{key}")))
                .collect();
            for (key, body) in keys.iter().zip(&bodies) {
                let path = format!("/bucket/{key}");
                assert!(
                    get(&client, port, &path).await == (200, body.clone()),
                    "{key}"
                );
            }
            let hits_from = now();
            for (key, body) in keys.iter().zip(&bodies) {
                let path = format!("/bucket/{key}");
                assert!(
                    get(&client, port, &path).await == (200, body.clone()),
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
            drop(client);
            gateway.stop();
            node.stop();

            let total = (keys.len() * OBJECT_SIZE) as i64;
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
            let spliced = gateway_hits
                .iter()
                .filter(|call| call.name == "splice" && on(call.fds().get(1), &gateway_end))
                .map(|call| call.result.max(0))
                .sum();
            let sent = node_hits
                .iter()
                .filter(|call| call.name == "sendfile" && on(call.fds().first(), &node_end))
                .map(|call| call.result.max(0))
                .sum();
            let written = |process: &str, calls: &[Call], end: &str| -> i64 {
                let to_end: Vec<Call> = calls
                    .iter()
                    .filter(|call| on(call.fds().first(), end))
                    .cloned()
                    .collect();
                if kernel {
                    check_writes_carry_no_body(process, &to_end, &windows, total)
                } else {
                    to_end
                        .iter()
                        .filter(|call| WRITES.split(',').any(|name| name == call.name))
                        .map(|call| call.result.max(0))
                        .sum()
                }
            };
            let rose = |names: [&str; 2]| -> u64 {
                names.iter().map(|name| after[*name] - before[*name]).sum()
            };
            Evidence {
                clients: Link {
                    sockets: client_sockets.0,
                    ulp_sockets: client_sockets.1,
                    ulp_calls: ulp_calls(&gateway_calls, &gateway_end),
                    zero_copy: spliced,
                    written: written("gateway", &gateway_hits, &gateway_end),
                },
                peers: Link {
                    sockets: peer_sockets.0,
                    ulp_sockets: peer_sockets.1,
                    ulp_calls: ulp_calls(&node_calls, &node_end)
                        + ulp_calls(&gateway_calls, &peer_end),
                    zero_copy: sent,
                    written: written("node", &node_hits, &node_end),
                },
                tx_sessions: rose(["TlsTxSw", "TlsTxDevice"]),
                rx_sessions: rose(["TlsRxSw", "TlsRxDevice"]),
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
