//! S3 clients over TLS: the gateway runs the handshake, the kernel carries
//! the session, and hits still reach clients through `splice`. Evidence
//! comes from outside the server: the kernel's TLS counters, `ss`'s view
//! of each socket, and the system calls `strace` records.

mod common;

use common::trace::{WRITES, check_writes_carry_no_body, now, read_trace, windows};
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
    tls_stat();
    check_close_notify(true).await;
}

/// Reads each object twice through a node and a gateway whose clients
/// connect over TLS the kernel carries, then again with the gateway forced
/// onto userspace TLS. The kernel run must show every sign of kTLS, and
/// the userspace run none.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs the tls module (sudo modprobe tls), strace, and sudo for ss"]
async fn hits_reach_kernel_tls_sessions_through_splice() {
    tls_stat();
    let total = 3 * OBJECT_SIZE as i64;

    let kernel = evidence(true).await;
    println!("kernel TLS: {kernel:?}");
    assert!(kernel.sockets > 0, "ss found no client connection");
    assert_eq!(
        kernel.ulp_sockets, kernel.sockets,
        "ss shows the tls ULP on {} of {} client sockets",
        kernel.ulp_sockets, kernel.sockets
    );
    assert!(
        kernel.ulp_calls > 0,
        "the gateway gave no client socket the tls ULP"
    );
    let sockets = kernel.sockets as u64;
    assert!(
        kernel.tx_sessions >= sockets && kernel.rx_sessions >= sockets,
        "the kernel set up {} sending and {} receiving TLS sessions for {sockets} connections",
        kernel.tx_sessions,
        kernel.rx_sessions
    );
    assert!(
        kernel.spliced >= total,
        "the gateway spliced {} of {total} hit bytes into TLS sockets",
        kernel.spliced
    );
    assert!(
        kernel.written < total / 16,
        "the gateway wrote {} bytes to clients",
        kernel.written
    );

    let userspace = evidence(false).await;
    println!("userspace TLS: {userspace:?}");
    assert!(userspace.sockets > 0, "ss found no client connection");
    assert_eq!(userspace.ulp_sockets, 0);
    assert_eq!(userspace.ulp_calls, 0);
    assert_eq!((userspace.tx_sessions, userspace.rx_sessions), (0, 0));
    assert_eq!(userspace.spliced, 0);
    assert!(
        userspace.written >= total,
        "the relay wrote {} bytes to clients",
        userspace.written
    );
}

/// What the kernel, `ss` and `strace` show of a gateway serving TLS.
#[derive(Debug)]
struct Evidence {
    /// The gateway's client sockets, and those with the tls ULP.
    sockets: usize,
    ulp_sockets: usize,
    /// The gateway's calls that gave client sockets the tls ULP.
    ulp_calls: usize,
    /// Sessions the kernel set up to encrypt and to decrypt.
    tx_sessions: u64,
    rx_sessions: u64,
    /// Bytes the gateway spliced into client sockets while serving hits,
    /// and wrote to them from userspace.
    spliced: i64,
    written: i64,
}

/// Reads three objects twice through a node and a traced gateway, with the
/// session in the kernel or in userspace.
async fn evidence(kernel: bool) -> Evidence {
    LocalSet::new()
        .run_until(async {
            let (origin_port, origin) = start_origin().await;
            origin.distinct.set(true);
            origin.size.set(OBJECT_SIZE);
            let dir = data_dir();
            let cache = "block_size = 65536\nextent_size = 1048576\nextents = 4";
            let cluster = Cluster::new(&dir, origin_port, cache);
            let (tls, trusted) = tls_table(&dir, kernel);
            let config = std::fs::read_to_string(&cluster.gateway).unwrap();
            std::fs::write(&cluster.gateway, format!("{config}{tls}")).unwrap();
            let node = cluster.start_node().await;
            let trace = dir.join("gateway.trace");
            let calls = format!("splice,setsockopt,{WRITES}");
            let gateway = Process::traced(&cluster.gateway, &trace, &calls);
            let port = cluster.gateway_port;
            common::listening(port).await;
            // The gateway checks which ciphers the kernel takes before it
            // answers, with sessions of its own; one answered request
            // leaves them out of the counts.
            let ready = client(&trusted).get(format!("https://127.0.0.1:{port}/"));
            ready.send().await.unwrap();

            let before = tls_stat();
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
            let (sockets, ulp_sockets) = client_sockets(port);
            let after = tls_stat();
            drop(client);
            gateway.stop();
            node.stop();

            let rose = |names: [&str; 2]| -> u64 {
                names.iter().map(|name| after[*name] - before[*name]).sum()
            };
            let to_client = format!(":{port}->");
            let on_client = |fd: Option<&String>| fd.is_some_and(|fd| fd.contains(&to_client));
            let ulp_calls = read_trace(&trace, 0.0, f64::MAX)
                .iter()
                .filter(|call| call.name == "setsockopt" && call.result == 0)
                .filter(|call| call.args.contains("TCP_ULP") && call.data() == b"tls")
                .filter(|call| on_client(call.fds().first()))
                .count();
            let hits = read_trace(&trace, hits_from, hits_until);
            let spliced = hits
                .iter()
                .filter(|call| call.name == "splice" && on_client(call.fds().get(1)))
                .map(|call| call.result.max(0))
                .sum();
            let to_clients: Vec<_> = hits
                .into_iter()
                .filter(|call| on_client(call.fds().first()))
                .collect();
            let written = if kernel {
                let total = (keys.len() * OBJECT_SIZE) as i64;
                check_writes_carry_no_body("gateway", &to_clients, &windows(&bodies), total)
            } else {
                to_clients
                    .iter()
                    .filter(|call| WRITES.split(',').any(|name| name == call.name))
                    .map(|call| call.result.max(0))
                    .sum()
            };
            Evidence {
                sockets,
                ulp_sockets,
                ulp_calls,
                tx_sessions: rose(["TlsTxSw", "TlsTxDevice"]),
                rx_sessions: rose(["TlsRxSw", "TlsRxDevice"]),
                spliced,
                written,
            }
        })
        .await
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

/// The established sockets on the gateway's `port`, and how many of them
/// `ss` shows with the tls ULP. Only CAP_NET_ADMIN sees a socket's ULP,
/// so `ss` runs under `sudo` unless the test runs as root.
fn client_sockets(port: u16) -> (usize, usize) {
    // SAFETY: geteuid only reads the process's credentials.
    let root = unsafe { libc::geteuid() } == 0;
    let mut command = if root {
        Command::new("ss")
    } else {
        let mut sudo = Command::new("sudo");
        sudo.args(["-n", "ss"]);
        sudo
    };
    let filter = format!("( sport = :{port} )");
    let output = command
        .args(["-tieH", "state", "established", &filter])
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
