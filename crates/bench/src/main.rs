//! Benchmarks a storage node and a gateway, each its own process, on this
//! machine's disk, in front of an in-process stand-in for S3 fast enough
//! never to limit them. Prints a Markdown report:
//!
//! - Hits and fills: throughput of first reads and of hits from the page
//!   cache and from the drive, and time to first byte of range reads.
//! - Scan and reread: a cache smaller than the data, objects of mixed
//!   sizes, and the doorkeeper: what a scan costs a hot set, and the byte
//!   hit ratio and drive writes of Zipf-distributed rereads.
//! - Transports: hit throughput and CPU over plaintext, kernel TLS and
//!   userspace TLS.
//!
//! Every figure comes from outside the server: the clients' clocks, the
//! stand-in's count of bytes it sent, and the kernel's counters of each
//! process's CPU time and writes and of the drive's reads.
//!
//! ```console
//! cargo build --release -p s3-accelerator -p s3-accelerator-bench
//! target/release/s3-accelerator-bench [--scale X] [--clients N] [--origin-latency-ms MS] [--dir DIR]
//! ```
//!
//! With `--scrape-ms MS`, both processes serve their admin listener, and a
//! thread scrapes each one's `/metrics` every `MS` milliseconds.

mod client;
mod cluster;
mod origin;

use client::{Get, Outcome, Target};
use cluster::{Cluster, Counters, Setup, Transport};
use origin::Origin;
use std::fmt::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const MIB: u64 = 1 << 20;
const GIB: f64 = (1u64 << 30) as f64;
/// The smallest cache a section runs with.
const MIN_CACHE: u64 = 512 * MIB;

struct Args {
    dir: PathBuf,
    server: PathBuf,
    /// Multiplies every data size.
    scale: f64,
    clients: usize,
    latency: Duration,
    /// The slab file's extent size.
    extent: u64,
    /// The one section to run: hits, scan, shift or transports.
    only: Option<String>,
    /// How often to scrape each process's metrics, if at all.
    scrape: Option<Duration>,
}

fn args() -> Args {
    let exe = std::env::current_exe().unwrap();
    let mut args = Args {
        dir: PathBuf::from("target/bench"),
        server: exe.with_file_name("s3-accelerator"),
        scale: 1.0,
        clients: 32,
        latency: Duration::from_millis(20),
        extent: MIB,
        only: None,
        scrape: None,
    };
    let mut given = std::env::args().skip(1);
    while let Some(flag) = given.next() {
        let value = given
            .next()
            .unwrap_or_else(|| panic!("{flag} takes a value"));
        match flag.as_str() {
            "--dir" => args.dir = value.into(),
            "--server" => args.server = value.into(),
            "--scale" => args.scale = value.parse().unwrap(),
            "--clients" => args.clients = value.parse().unwrap(),
            "--extent-mib" => args.extent = value.parse::<u64>().unwrap() * MIB,
            "--only" => args.only = Some(value),
            "--scrape-ms" => {
                args.scrape = Some(Duration::from_millis(value.parse().unwrap()));
            }
            "--origin-latency-ms" => {
                args.latency = Duration::from_millis(value.parse().unwrap());
            }
            _ => panic!("unknown flag {flag}"),
        }
    }
    args
}

/// A run's figures, for one row of the report.
struct Row {
    workload: String,
    outcome: Outcome,
    from_s3: u64,
    counters: Counters,
}

struct Bench {
    args: Args,
    origin: Arc<Origin>,
    origin_port: u16,
    report: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = args();
    let origin = Origin::new(args.latency);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_port = listener.local_addr().unwrap().port();
    tokio::spawn(origin::serve(listener, origin.clone()));
    let mut bench = Bench {
        args,
        origin,
        origin_port,
        report: String::new(),
    };
    bench.machine();
    let runs = |section: &str| {
        bench
            .args
            .only
            .as_deref()
            .is_none_or(|only| only == section)
    };
    let (hits, scan, shift, transports) = (
        runs("hits"),
        runs("scan"),
        runs("shift"),
        runs("transports"),
    );
    if hits {
        bench.hits_and_fills().await;
    }
    if scan {
        bench.scan_and_reread().await;
    }
    if shift {
        bench.size_shift().await;
    }
    if transports {
        bench.transports().await;
    }
    println!("{}", bench.report);
}

impl Bench {
    fn machine(&mut self) {
        let read = |path: &str| std::fs::read_to_string(path).unwrap_or_default();
        let cpu = read("/proc/cpuinfo")
            .lines()
            .find_map(|line| line.strip_prefix("model name")?.split_once(':'))
            .map(|(_, name)| name.trim().to_string())
            .unwrap_or_default();
        let cores = std::thread::available_parallelism().map_or(0, |cores| cores.get());
        let memory = read("/proc/meminfo")
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))
            .and_then(|kib| kib.trim().trim_end_matches(" kB").parse::<f64>().ok())
            .map_or(0.0, |kib| kib / (1 << 20) as f64);
        let drives: Vec<String> = std::fs::read_dir("/sys/block")
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("nvme"))
            .map(|entry| read(&format!("{}/device/model", entry.path().display())))
            .map(|model| model.trim().to_string())
            .collect();
        let kernel = read("/proc/sys/kernel/osrelease");
        let _ = writeln!(
            self.report,
            "Machine: {cpu}, {cores} threads, {memory:.0} GiB of memory, {}, Linux {}. \
             S3's stand-in waits {} ms before each answer. {} clients unless noted.\n",
            drives.join(", "),
            kernel.trim(),
            self.args.latency.as_millis(),
            self.args.clients,
        );
    }

    fn cluster(&self, name: &str, cache: u64, policy: &str, transport: Transport) -> Cluster {
        let setup = Setup {
            cache,
            extent: self.args.extent,
            policy,
            transport,
            scrape: self.args.scrape,
        };
        Cluster::start(
            &self.args.server,
            &self.args.dir.join(name),
            self.origin_port,
            &setup,
        )
    }

    async fn measure(
        &self,
        cluster: &Cluster,
        workload: &str,
        clients: usize,
        gets: Vec<Get>,
    ) -> Row {
        eprintln!("{workload}");
        cluster.settle().await;
        let target = Target {
            port: cluster.gateway_port,
            tls: cluster.tls.clone(),
        };
        let (before, from_s3) = (cluster.counters(), self.s3_bytes());
        let started = monotonic();
        let outcome = client::run(&target, clients, gets)
            .await
            .unwrap_or_else(|error| panic!("{workload}: {error}"));
        // CLOCK_MONOTONIC seconds, as `perf record -k CLOCK_MONOTONIC`
        // stamps samples, so `perf report --time` can take one workload.
        eprintln!("{workload}: from {started:.6} to {:.6}", monotonic());
        Row {
            workload: workload.to_string(),
            outcome,
            from_s3: self.s3_bytes() - from_s3,
            counters: cluster.counters().since(before),
        }
    }

    fn s3_bytes(&self) -> u64 {
        self.origin.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn table(&mut self, title: &str, rows: &[Row], notes: &str) {
        let _ = writeln!(self.report, "### {title}\n");
        let _ = writeln!(
            self.report,
            "| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | \
             GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | \
             Gateway CPU s/GiB |"
        );
        let _ = writeln!(self.report, "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
        for row in rows {
            let served = row.outcome.bytes as f64 / GIB;
            let per_gib = |cpu: f64| cpu / served.max(f64::MIN_POSITIVE);
            let _ = writeln!(
                self.report,
                "| {} | {} | {:.2} | {:.2} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
                row.workload,
                row.outcome.requests,
                served,
                served / row.outcome.elapsed.as_secs_f64(),
                millis(row.outcome.first_byte(0.5)),
                millis(row.outcome.first_byte(0.99)),
                row.from_s3 as f64 / GIB,
                row.counters.node_written as f64 / GIB,
                row.counters.drive_read as f64 / GIB,
                per_gib(row.counters.node_cpu),
                per_gib(row.counters.gateway_cpu),
            );
        }
        let _ = writeln!(self.report, "\n{notes}\n");
    }

    /// First reads of 64 MiB objects into a cache that holds them all,
    /// then hits from the page cache and from the drive, and range reads.
    async fn hits_and_fills(&mut self) {
        let count = ((256.0 * self.args.scale) as u64).max(4);
        let size = 64 * MIB;
        let cache =
            (count * size * 5 / 4).next_multiple_of(self.args.extent) + 8 * self.args.extent;
        let policy = "immutable = true\nadmit_on_first_read = true";
        let cluster = self.cluster("hits", cache, policy, Transport::Plaintext);
        let clients = self.args.clients;
        let objects: Vec<Get> = (0..count)
            .map(|index| whole(&format!("/bench/fill/obj-{index}-{size}")))
            .collect();
        let mut rows = Vec::new();
        rows.push(
            self.measure(&cluster, "First reads (fills)", clients, objects.clone())
                .await,
        );
        rows.push(
            self.measure(&cluster, "Hits", clients, objects.clone())
                .await,
        );
        // A subset the page cache holds: read once to cache it, then again.
        let some: Vec<Get> = objects.iter().take(32).cloned().collect();
        self.measure(&cluster, "Caching a subset", clients, some.clone())
            .await;
        let row = self.measure(&cluster, "Hits from the page cache", clients, some.clone());
        rows.push(row.await);
        let row = self.measure(&cluster, "Hits from the page cache, 1 client", 1, some);
        rows.push(row.await);
        cluster.drop_cached_blocks();
        let row = self.measure(&cluster, "Hits from the drive", clients, objects.clone());
        rows.push(row.await);
        let mut random = Random(7);
        let ranges = |random: &mut Random, requests: u64, class: &str| -> Vec<Get> {
            (0..requests)
                .map(|_| {
                    let index = random.below(count);
                    let first = random.below(size - 64 * 1024);
                    Get {
                        path: format!("/bench/{class}/obj-{index}-{size}"),
                        range: Some((first, first + 64 * 1024 - 1)),
                    }
                })
                .collect()
        };
        let requests = ((20_000.0 * self.args.scale) as u64).max(100);
        let many = ranges(&mut random, requests, "fill");
        rows.push(
            self.measure(&cluster, "64 KiB range hits", clients, many)
                .await,
        );
        let one = ranges(&mut random, 1_000, "fill");
        rows.push(
            self.measure(&cluster, "64 KiB range hits, 1 client", 1, one)
                .await,
        );
        let misses = (0..200)
            .map(|index| {
                let first = random.below(size - 64 * 1024);
                Get {
                    path: format!("/bench/miss/obj-{index}-{size}"),
                    range: Some((first, first + 64 * 1024 - 1)),
                }
            })
            .collect();
        rows.push(
            self.measure(&cluster, "64 KiB range misses, 1 client", 1, misses)
                .await,
        );
        cluster.stop();
        let notes = format!(
            "{count} objects of 64 MiB, which the cache admits on their first read. The node's \
             fill budget is 4 GiB: a gateway asks for all of an object's chunks at once, so \
             each client's miss holds 64 MiB of it, and misses past the budget stream from S3 \
             without admission. \"Hits\" reads every object again, more than the page cache \
             keeps of blocks read once; \"Hits from the page cache\" rereads 2 GiB of them, \
             and \"Hits from the drive\" reads them all after the slab file leaves the page \
             cache. A miss's first byte includes S3's {} ms.",
            self.args.latency.as_millis()
        );
        self.table("Hits and fills", &rows, &notes);
    }

    /// A cache smaller than the data, objects from 64 KiB to 64 MiB, and
    /// the doorkeeper admitting a block on its second read.
    async fn scan_and_reread(&mut self) {
        let cache = ((4.0 * self.args.scale * GIB) as u64).max(MIN_CACHE);
        let cluster = self.cluster("scan", cache, "immutable = true", Transport::Plaintext);
        let clients = self.args.clients;
        let mut random = Random(11);
        let hot = objects(&mut random, "hot", cache / 2);
        let scan = objects(&mut random, "scan", cache * 3);
        let zipf = objects(&mut random, "zipf", cache * 2);
        let mut rows = Vec::new();

        let thrice: Vec<Get> = (0..3).flat_map(|_| hot.clone()).collect();
        rows.push(
            self.measure(&cluster, "Hot set, read three times", clients, thrice)
                .await,
        );

        // Each scan object once, with a hot read between each.
        let mixed: Vec<Get> = scan
            .iter()
            .zip(hot.iter().cycle())
            .flat_map(|(scan, hot)| [scan.clone(), hot.clone()])
            .collect();
        let (hot_before, scan_before) = (self.origin.class("hot"), self.origin.class("scan"));
        rows.push(
            self.measure(
                &cluster,
                "Scan of 3× the cache, with hot reads",
                clients,
                mixed,
            )
            .await,
        );
        let hot_after = self.origin.class("hot");
        let scan_after = self.origin.class("scan");
        let hot_refetched = (hot_after.1 - hot_before.1) as f64 / GIB;
        let scan_fetched = (scan_after.1 - scan_before.1) as f64 / GIB;

        let requests = ((3_000.0 * self.args.scale) as usize).max(100);
        let weights: Vec<f64> = (1..=zipf.len()).map(|rank| 1.0 / rank as f64).collect();
        let reads: Vec<Get> = (0..requests)
            .map(|_| zipf[random.weighted(&weights)].clone())
            .collect();
        rows.push(
            self.measure(&cluster, "Zipf rereads of 2× the cache", clients, reads)
                .await,
        );
        cluster.stop();

        let zipf_row = &rows[2];
        let hit_ratio = 1.0 - zipf_row.from_s3 as f64 / zipf_row.outcome.bytes as f64;
        let written_per_gib = zipf_row.counters.node_written as f64 / zipf_row.outcome.bytes as f64;
        let notes = format!(
            "A {:.1} GiB cache; a hot set of {} objects ({:.1} GiB), a scan of {} objects \
             ({:.1} GiB) and a Zipf (s = 1) set of {} objects ({:.1} GiB), sizes log-uniform \
             from 64 KiB to 64 MiB. The doorkeeper admits a block on its second read. During \
             the scan, S3 sent {scan_fetched:.2} GiB of scan objects and {hot_refetched:.2} GiB \
             of hot ones. Zipf rereads: byte hit ratio {:.1}%, and the node wrote {:.3} bytes \
             per byte served.",
            cache as f64 / GIB,
            hot.len(),
            total(&hot) as f64 / GIB,
            scan.len(),
            total(&scan) as f64 / GIB,
            zipf.len(),
            total(&zipf) as f64 / GIB,
            hit_ratio * 100.0,
            written_per_gib,
        );
        self.table("Scan and reread", &rows, &notes);
    }

    /// A full cache of 1 MiB blocks that must make room for new objects,
    /// with a hot set read throughout. Small objects need slots of smaller
    /// classes, and a class that needs room empties an extent of another,
    /// hot blocks included; large objects, the control, take 1 MiB slots
    /// like the blocks they replace. Hot bytes S3 sends again beyond the
    /// control's are the size classes' cost.
    async fn size_shift(&mut self) {
        let cache = ((4.0 * self.args.scale * GIB) as u64).max(MIN_CACHE);
        let mut random = Random(13);
        let mut small = Vec::new();
        let mut held = 0;
        while held < cache * 3 / 10 {
            let size = 2f64.powf(12.0 + random.unit() * 7.0) as u64;
            small.push(whole(&format!("/bench/small/obj-{}-{size}", small.len())));
            held += size;
        }
        let large_size = 16 * MIB;
        let large: Vec<Get> = (0..total(&small).div_ceil(large_size))
            .map(|index| whole(&format!("/bench/large/obj-{index}-{large_size}")))
            .collect();
        let mut rows = Vec::new();
        let mut refetched = Vec::new();
        for (name, objects) in [("Small", &small), ("Large", &large)] {
            let (row, again) = self.shift(cache, name, objects).await;
            rows.push(row);
            refetched.push(again);
        }
        let notes = format!(
            "A {:.1} GiB cache, filled by reading twice a cold set of 64 MiB objects \
             ({:.1} GiB), then a hot set of them ({:.1} GiB) three times. Then new objects, each \
             read twice so the doorkeeper admits them, with the hot set read twice more among \
             them: {} small objects ({:.2} GiB, log-uniform from 4 KiB to 512 KiB), or, as the \
             control, {} of 16 MiB ({:.2} GiB). S3 sent {:.2} GiB of hot objects again among the \
             small objects, and {:.2} GiB among the large.",
            cache as f64 / GIB,
            cache as f64 / GIB,
            cache as f64 / 2.0 / GIB,
            small.len(),
            total(&small) as f64 / GIB,
            large.len(),
            total(&large) as f64 / GIB,
            refetched[0],
            refetched[1],
        );
        self.table("Size shift", &rows, &notes);
    }

    /// One run of `size_shift` with `objects` as the new objects: the row
    /// for their reads, and the GiB of hot objects S3 sent again meanwhile.
    async fn shift(&mut self, cache: u64, name: &str, objects: &[Get]) -> (Row, f64) {
        let directory = format!("shift-{}", name.to_lowercase());
        let cluster = self.cluster(&directory, cache, "immutable = true", Transport::Plaintext);
        let clients = self.args.clients;
        let size = 64 * MIB;
        let set = |class: &str, bytes: u64| -> Vec<Get> {
            (0..bytes.div_ceil(size))
                .map(|index| whole(&format!("/bench/{class}/obj-{index}-{size}")))
                .collect()
        };
        let lower = name.to_lowercase();
        let hot_class = format!("hot-{lower}");
        let (cold, hot) = (
            set(&format!("cold-{lower}"), cache),
            set(&hot_class, cache / 2),
        );
        let twice =
            |objects: &[Get]| -> Vec<Get> { objects.iter().chain(objects).cloned().collect() };
        self.measure(&cluster, "Cold set, read twice", clients, twice(&cold))
            .await;
        let thrice: Vec<Get> = (0..3).flat_map(|_| hot.clone()).collect();
        self.measure(&cluster, "Hot set, read three times", clients, thrice)
            .await;
        let mixed = spread(twice(objects), twice(&hot));
        let before = self.origin.class(&hot_class);
        let workload = format!("{name} objects, with hot reads");
        let row = self.measure(&cluster, &workload, clients, mixed).await;
        let again = (self.origin.class(&hot_class).1 - before.1) as f64 / GIB;
        cluster.stop();
        (row, again)
    }

    /// Hits over each transport, on both the clients' link and the peers'.
    async fn transports(&mut self) {
        let count = ((64.0 * self.args.scale) as u64).max(4);
        let size = 64 * MIB;
        let cache =
            (count * size * 5 / 4).next_multiple_of(self.args.extent) + 8 * self.args.extent;
        let policy = "immutable = true\nadmit_on_first_read = true";
        let kernel = std::path::Path::new("/proc/net/tls_stat").exists();
        let mut rows = Vec::new();
        for transport in [
            Transport::Plaintext,
            Transport::KernelTls,
            Transport::UserspaceTls,
        ] {
            if transport == Transport::KernelTls && !kernel {
                eprintln!("skipping kernel TLS: load the tls module with `sudo modprobe tls`");
                continue;
            }
            let name = transport.name();
            let cluster = self.cluster(&name.replace(' ', "-"), cache, policy, transport);
            let objects: Vec<Get> = (0..count)
                .map(|index| {
                    whole(&format!(
                        "/bench/{}/obj-{index}-{size}",
                        name.replace(' ', "-")
                    ))
                })
                .collect();
            let clients = self.args.clients;
            self.measure(
                &cluster,
                &format!("First reads, {name}"),
                clients,
                objects.clone(),
            )
            .await;
            rows.push(
                self.measure(&cluster, &format!("Hits, {name}"), clients, objects)
                    .await,
            );
            cluster.stop();
        }
        let notes = format!(
            "{count} objects of 64 MiB, read once to fill and again to measure. Both the \
             clients' link and the gateway's link to the node use the transport. The clients \
             decrypt in userspace, on the same machine."
        );
        self.table("Transports", &rows, &notes);
    }
}

/// `among` spread evenly through `gets`.
fn spread(gets: Vec<Get>, among: Vec<Get>) -> Vec<Get> {
    let mut spread = Vec::with_capacity(gets.len() + among.len());
    let mut taken = 0;
    for (index, get) in gets.iter().enumerate() {
        spread.push(get.clone());
        let due = (index + 1) * among.len() / gets.len();
        spread.extend(among[taken..due].iter().cloned());
        taken = due;
    }
    spread
}

fn whole(path: &str) -> Get {
    Get {
        path: path.to_string(),
        range: None,
    }
}

/// Objects of `class` with sizes log-uniform from 64 KiB to 64 MiB, until
/// they hold `bytes`.
fn objects(random: &mut Random, class: &str, bytes: u64) -> Vec<Get> {
    let mut objects = Vec::new();
    let mut held = 0;
    while held < bytes {
        let exponent = 16.0 + random.unit() * 10.0;
        let size = 2f64.powf(exponent) as u64;
        objects.push(whole(&format!(
            "/bench/{class}/obj-{}-{size}",
            objects.len()
        )));
        held += size;
    }
    objects
}

fn total(objects: &[Get]) -> u64 {
    objects
        .iter()
        .filter_map(|get| origin::object(&get.path))
        .map(|(_, size)| size)
        .sum()
}

fn millis(duration: Duration) -> String {
    format!("{:.2} ms", duration.as_secs_f64() * 1e3)
}

/// splitmix64, so every run reads the same keys in the same order.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// An index drawn with probability proportional to its weight.
    fn weighted(&mut self, weights: &[f64]) -> usize {
        let mut target = self.unit() * weights.iter().sum::<f64>();
        for (index, weight) in weights.iter().enumerate() {
            if target < *weight {
                return index;
            }
            target -= weight;
        }
        weights.len() - 1
    }
}

/// CLOCK_MONOTONIC now, in seconds.
fn monotonic() -> f64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    now.tv_sec as f64 + now.tv_nsec as f64 / 1e9
}
