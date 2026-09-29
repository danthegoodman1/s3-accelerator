//! A node and a gateway, each its own process, and what the kernel says of
//! them: CPU time, bytes they wrote to the drive, and the drive's reads.

use crate::client::{ACCESS_KEY_ID, SECRET_ACCESS_KEY};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How the gateway's clients and the cluster's members connect.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Transport {
    Plaintext,
    KernelTls,
    UserspaceTls,
}

impl Transport {
    pub fn name(self) -> &'static str {
        match self {
            Transport::Plaintext => "plaintext",
            Transport::KernelTls => "kernel TLS",
            Transport::UserspaceTls => "userspace TLS",
        }
    }
}

/// The `[cache]` table and the bucket's policy.
pub struct Setup<'a> {
    /// The cache's bytes, in extents of `extent` bytes.
    pub cache: u64,
    pub extent: u64,
    pub policy: &'a str,
    pub transport: Transport,
    /// How often to scrape each process's metrics during runs, if at all.
    pub scrape: Option<Duration>,
    /// The node looks up the bucket's origin in the reference metadata
    /// service, in place of its config's `[origin]`.
    pub metadata: bool,
}

pub struct Cluster {
    node: Child,
    gateway: Child,
    metadata: Option<Child>,
    pub gateway_port: u16,
    /// Where the node serves its metrics.
    node_admin_port: u16,
    pub slabs: PathBuf,
    /// A client config that trusts the gateway, when it serves TLS.
    pub tls: Option<Arc<ClientConfig>>,
    /// Stops the scraper, when one runs.
    scraping: Option<Arc<AtomicBool>>,
    _ports: Vec<OwnedFd>,
}

/// Counters the kernel keeps for the node and the gateway, and for the
/// drive that holds the node's data.
#[derive(Clone, Copy, Default)]
pub struct Counters {
    pub node_cpu: f64,
    pub gateway_cpu: f64,
    /// Bytes the node caused to be written to storage.
    pub node_written: u64,
    /// Bytes read from the drive, by any process.
    pub drive_read: u64,
}

impl Counters {
    pub fn since(self, before: Counters) -> Counters {
        Counters {
            node_cpu: self.node_cpu - before.node_cpu,
            gateway_cpu: self.gateway_cpu - before.gateway_cpu,
            node_written: self.node_written - before.node_written,
            drive_read: self.drive_read - before.drive_read,
        }
    }
}

impl Cluster {
    /// Starts a node with its data in `dir` and a gateway, in front of the
    /// stand-in for S3 on `origin_port`.
    pub fn start(binary: &Path, dir: &Path, origin_port: u16, setup: &Setup) -> Cluster {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (node_port, node_reservation) = reserve_port();
        let (gateway_port, gateway_reservation) = reserve_port();
        let (client_tls, member_tls, tls) = match setup.transport {
            Transport::Plaintext => (String::new(), String::new(), None),
            transport => {
                let kernel = transport == Transport::KernelTls;
                certificates(&dir.join("tls"), kernel)
            }
        };
        let Setup {
            cache,
            extent,
            policy,
            scrape,
            ..
        } = setup;
        // Admin listeners: settling reads the node's, and the scraper both.
        let (
            (node_admin_port, node_admin_reservation),
            (gateway_admin_port, gateway_admin_reservation),
        ) = (reserve_port(), reserve_port());
        let admin_table = |port: u16| format!("\n[admin]\nlisten = \"127.0.0.1:{port}\"\n");
        let node_admin = admin_table(node_admin_port);
        let gateway_admin = admin_table(gateway_admin_port);
        let extents = cache / extent;
        let shared = format!(
            r#"
[[clients]]
access_key_id = "{ACCESS_KEY_ID}"
secret_access_key = "{SECRET_ACCESS_KEY}"
grants = [{{ bucket = "bench" }}]

[cache]
block_size = 1048576
chunk_blocks = 16
extent_size = {extent}
extents = {extents}
fill_budget = 4294967296

[cache.buckets.bench]
{policy}

[cluster]
secret = "bench-secret"
nodes = [{{ id = 0, address = "127.0.0.1:{node_port}" }}]
"#
        );
        let origin = format!(
            "endpoint = \"http://127.0.0.1:{origin_port}\"\nregion = \"us-east-1\"\n\
             access_key_id = \"origin\"\nsecret_access_key = \"origin-secret\"\n"
        );
        let (metadata_port, metadata_reservation) = reserve_port();
        let origins = match setup.metadata {
            true => format!(
                "[metadata]\nurl = \"http://127.0.0.1:{metadata_port}\"\ntoken = \"bench-token\"\n"
            ),
            false => format!("[origin]\n{origin}"),
        };
        let metadata = setup.metadata.then(|| {
            let config = dir.join("metadata.toml");
            std::fs::write(
                &config,
                format!(
                    "listen = \"127.0.0.1:{metadata_port}\"\ntoken = \"bench-token\"\n\
                     nodes = [\"127.0.0.1:{node_admin_port}\"]\n[default]\n{origin}"
                ),
            )
            .unwrap();
            let log = std::fs::File::create(dir.join("metadata.log")).unwrap();
            let service = binary.with_file_name("s3-accelerator-metadata");
            Command::new(&service)
                .arg(&config)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap_or_else(|error| panic!("{}: {error}", service.display()))
        });
        if metadata.is_some() {
            listening(metadata_port);
        }
        let node_config = dir.join("node.toml");
        let data = dir.join("node");
        std::fs::write(
            &node_config,
            format!(
                r#"{origins}
{shared}
[node]
id = 0
data_dir = "{}"
{member_tls}{node_admin}"#,
                data.display()
            ),
        )
        .unwrap();
        let gateway_config = dir.join("gateway.toml");
        std::fs::write(
            &gateway_config,
            format!(
                "{shared}\n[gateway]\nlisten = \"127.0.0.1:{gateway_port}\"\n{client_tls}{member_tls}{gateway_admin}"
            ),
        )
        .unwrap();
        let spawn = |config: &Path, log: &str| {
            let log = std::fs::File::create(dir.join(log)).unwrap();
            Command::new(binary)
                .arg(config)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap_or_else(|error| panic!("{}: {error}", binary.display()))
        };
        let node = spawn(&node_config, "node.log");
        listening(node_port);
        let gateway = spawn(&gateway_config, "gateway.log");
        listening(gateway_port);
        // For profilers to attach to.
        eprintln!("node pid {}, gateway pid {}", node.id(), gateway.id());
        listening(node_admin_port);
        listening(gateway_admin_port);
        let scraping = scrape.map(|every| {
            let files = [
                (node_admin_port, dir.join("node.metrics")),
                (gateway_admin_port, dir.join("gateway.metrics")),
            ];
            scrape_every(every, files)
        });
        Cluster {
            node,
            gateway,
            metadata,
            gateway_port,
            node_admin_port,
            slabs: data.join("slabs"),
            tls,
            scraping,
            _ports: vec![
                node_reservation,
                gateway_reservation,
                node_admin_reservation,
                gateway_admin_reservation,
                metadata_reservation,
            ],
        }
    }

    pub fn counters(&self) -> Counters {
        Counters {
            node_cpu: cpu_seconds(self.node.id()),
            gateway_cpu: cpu_seconds(self.gateway.id()),
            node_written: proc_io(self.node.id(), "write_bytes"),
            drive_read: drive_read(&self.slabs),
        }
    }

    /// Waits until the node has no fills in progress and has written
    /// nothing for half a second, so a run starts after the last one's
    /// blocks reach the drive.
    pub async fn settle(&self) {
        let mut written = proc_io(self.node.id(), "write_bytes");
        let mut quiet = 0;
        while quiet < 5 || self.filling() != Some(0) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = proc_io(self.node.id(), "write_bytes");
            quiet = if now == written { quiet + 1 } else { 0 };
            written = now;
        }
    }

    /// Bytes of blocks the node is filling, which it has yet to make
    /// durable, as its metrics say.
    fn filling(&self) -> Option<u64> {
        self.node_sample("s3accel_node_fill_bytes ")
    }

    /// Lookups of the bucket's origin the node has made, when it asks the
    /// metadata service.
    pub fn lookups(&self) -> Option<u64> {
        self.metadata.as_ref()?;
        self.node_sample("s3accel_origin_lookups_total{result=\"found\"} ")
    }

    /// The value of the node's metric sample that starts `series`.
    fn node_sample(&self, series: &str) -> Option<u64> {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", self.node_admin_port)).ok()?;
        let request = "GET /metrics HTTP/1.1\r\nhost: bench\r\nconnection: close\r\n\r\n";
        stream.write_all(request.as_bytes()).ok()?;
        let mut answer = String::new();
        stream.read_to_string(&mut answer).ok()?;
        answer
            .lines()
            .find_map(|line| line.strip_prefix(series))?
            .trim()
            .parse()
            .ok()
    }

    /// Drops the slab file's pages from the page cache, so the next hits
    /// read the drive.
    pub fn drop_cached_blocks(&self) {
        let file = std::fs::File::open(&self.slabs).unwrap();
        file.sync_all().unwrap();
        rustix::fs::fadvise(&file, 0, None, rustix::fs::Advice::DontNeed).unwrap();
    }

    /// Stops every process cleanly.
    pub fn stop(mut self) {
        if let Some(lookups) = self.lookups() {
            eprintln!("the node looked up the bucket's origin {lookups} times");
        }
        let metadata = self.metadata.as_mut();
        for child in [&mut self.gateway, &mut self.node]
            .into_iter()
            .chain(metadata)
        {
            let _ = Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status();
            let _ = child.wait();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some(scraping) = &self.scraping {
            scraping.store(false, Ordering::Relaxed);
        }
        let _ = self.gateway.kill();
        let _ = self.node.kill();
        if let Some(metadata) = &mut self.metadata {
            let _ = metadata.kill();
        }
    }
}

/// Reads `/metrics` from each admin port every `every`, on a thread of its
/// own, until the returned flag clears, and keeps each port's latest scrape
/// in its file.
fn scrape_every(every: Duration, ports: [(u16, PathBuf); 2]) -> Arc<AtomicBool> {
    use std::io::{Read, Write};
    let running = Arc::new(AtomicBool::new(true));
    let flag = running.clone();
    std::thread::spawn(move || {
        while flag.load(Ordering::Relaxed) {
            for (port, file) in &ports {
                let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", *port)) else {
                    continue;
                };
                let request = "GET /metrics HTTP/1.1\r\nhost: bench\r\nconnection: close\r\n\r\n";
                let mut answer = Vec::new();
                if stream.write_all(request.as_bytes()).is_ok()
                    && stream.read_to_end(&mut answer).is_ok()
                {
                    let _ = std::fs::write(file, answer);
                }
            }
            std::thread::sleep(every);
        }
    });
    running
}

/// Writes a CA, a certificate it signs for 127.0.0.1 that every process
/// presents, and the `[gateway.tls]` and `[cluster.tls]` tables that name
/// them; and a client config that trusts the CA.
fn certificates(dir: &Path, kernel: bool) -> (String, String, Option<Arc<ClientConfig>>) {
    std::fs::create_dir_all(dir).unwrap();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_pem = ca_params.self_signed(&ca_key).unwrap().pem();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();
    let write = |name: &str, pem: &str| {
        let path = dir.join(name);
        std::fs::write(&path, pem).unwrap();
        path.display().to_string()
    };
    let ca = write("ca.pem", &ca_pem);
    let cert_path = write("cert.pem", &cert.pem());
    let key_path = write("key.pem", &key.serialize_pem());
    let client =
        format!("[gateway.tls]\ncert = \"{cert_path}\"\nkey = \"{key_path}\"\nkernel = {kernel}\n");
    let members = format!(
        "[cluster.tls]\nca = \"{ca}\"\ncert = \"{cert_path}\"\nkey = \"{key_path}\"\nkernel = {kernel}\n"
    );
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (client, members, Some(Arc::new(config)))
}

/// A port below the kernel's ephemeral range, held by a socket that never
/// listens: the server's own bind shares it, and no one else takes it.
fn reserve_port() -> (u16, OwnedFd) {
    use rustix::net::{AddressFamily, SocketType, bind, socket, sockopt};
    let start = u64::from(std::process::id()) * 7_919;
    (0..)
        .find_map(|next: u64| {
            let port = (20_000 + (start + next) % 12_000) as u16;
            let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
            let reservation = socket(AddressFamily::INET, SocketType::STREAM, None).ok()?;
            bind(&reservation, &address).ok()?;
            sockopt::set_socket_reuseaddr(&reservation, true).ok()?;
            std::net::UdpSocket::bind(address).ok()?;
            Some((port, reservation))
        })
        .unwrap()
}

fn listening(port: u16) {
    for _ in 0..600 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("nothing listens on port {port}");
}

/// A process's user and system CPU time.
fn cpu_seconds(pid: u32) -> f64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // Fields after the command name, which ends at the last ')'.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks: u64 = [11, 12]
        .iter()
        .filter_map(|&index| fields.get(index)?.parse::<u64>().ok())
        .sum();
    ticks as f64 / rustix::param::clock_ticks_per_second() as f64
}

fn proc_io(pid: u32, field: &str) -> u64 {
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    io.lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(": ")?.parse().ok())
        .unwrap_or(0)
}

/// Bytes read from the block device that holds `path` since boot.
fn drive_read(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let Some(device) = path
        .ancestors()
        .find_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.dev())
    else {
        return 0;
    };
    let (major, minor) = (rustix::fs::major(device), rustix::fs::minor(device));
    let stats = std::fs::read_to_string("/proc/diskstats").unwrap_or_default();
    stats
        .lines()
        .find_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let matches = fields.first()?.parse::<u32>().ok()? == major
                && fields.get(1)?.parse::<u32>().ok()? == minor;
            // Sectors read, in 512-byte units.
            matches.then(|| {
                fields
                    .get(5)?
                    .parse::<u64>()
                    .ok()
                    .map(|sectors| sectors * 512)
            })?
        })
        .unwrap_or(0)
}
