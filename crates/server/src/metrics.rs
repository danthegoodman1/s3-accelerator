//! What the server measures, and every count rendered in Prometheus's text
//! format. The core counts its own decisions; this module counts what the
//! server measures on its event loop, where every worker's result arrives,
//! so no count is shared between threads.

use crate::http::RequestHead;
use s3_accelerator_core::node::{Stats, Usage};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::time::Duration;

/// Upper bounds of the latency histograms' buckets, in seconds.
const BUCKETS: [f64; 16] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];

#[derive(Clone, Default)]
struct Histogram {
    /// Observations in each bucket, the last above every bound.
    counts: [u64; BUCKETS.len() + 1],
    sum: f64,
}

impl Histogram {
    fn observe(&mut self, value: Duration) {
        let seconds = value.as_secs_f64();
        self.counts[BUCKETS.partition_point(|&bound| bound < seconds)] += 1;
        self.sum += seconds;
    }
}

/// A client request's S3 operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    GetObject,
    HeadObject,
    PutObject,
    CopyObject,
    DeleteObject,
    DeleteObjects,
    ListObjects,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    Purge,
    Other,
}

/// Query parameters a read of an object may carry.
const READ_PARAMETERS: [&str; 9] = [
    "partNumber",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "versionId",
    "x-id",
];

/// Query parameters a listing of objects may carry.
const LIST_PARAMETERS: [&str; 11] = [
    "continuation-token",
    "delimiter",
    "encoding-type",
    "fetch-owner",
    "list-type",
    "marker",
    "max-keys",
    "optional-object-attributes",
    "prefix",
    "start-after",
    "x-id",
];

impl Operation {
    /// The operation a request asks for. A virtual-hosted request's path
    /// holds only its key.
    pub fn of(head: &RequestHead, virtual_hosted: bool) -> Operation {
        let path = head.path.trim_start_matches('/');
        let (names_bucket, has_key) = match virtual_hosted {
            true => (true, !path.is_empty()),
            false => (
                !path.is_empty(),
                path.split_once('/').is_some_and(|(_, key)| !key.is_empty()),
            ),
        };
        let names = || {
            head.query
                .split('&')
                .filter(|pair| !pair.is_empty())
                .map(|pair| pair.split('=').next().unwrap_or(""))
        };
        let has = |name: &str| names().any(|named| named == name);
        let only = |allowed: &[&str]| names().all(|name| allowed.contains(&name));
        let copies = head.header("x-amz-copy-source").is_some();
        match (head.method.as_str(), has_key) {
            ("GET", true) if only(&READ_PARAMETERS) => Operation::GetObject,
            ("HEAD", true) if only(&READ_PARAMETERS) => Operation::HeadObject,
            ("PUT", true) if has("uploadId") => Operation::UploadPart,
            ("PUT", true) if only(&["x-id"]) && copies => Operation::CopyObject,
            ("PUT", true) if only(&["x-id"]) => Operation::PutObject,
            ("DELETE", true) if has("uploadId") => Operation::AbortMultipartUpload,
            ("DELETE", true) if only(&["x-id", "versionId"]) => Operation::DeleteObject,
            ("POST", true) if has("uploads") => Operation::CreateMultipartUpload,
            ("POST", true) if has("uploadId") => Operation::CompleteMultipartUpload,
            ("POST", true) if has("x-accel-purge") => Operation::Purge,
            ("POST", false) if names_bucket && has("delete") => Operation::DeleteObjects,
            ("GET", false) if names_bucket && only(&LIST_PARAMETERS) => Operation::ListObjects,
            _ => Operation::Other,
        }
    }
}

/// Why a gateway's request to a node got no usable answer.
#[derive(Clone, Copy, Debug)]
pub enum NodeFailure {
    Refused,
    Timeout,
    Error,
}

impl NodeFailure {
    pub fn of(error: &std::io::Error) -> NodeFailure {
        match error.kind() {
            std::io::ErrorKind::ConnectionRefused => NodeFailure::Refused,
            std::io::ErrorKind::TimedOut => NodeFailure::Timeout,
            _ => NodeFailure::Error,
        }
    }
}

/// The side that cut a relayed response short.
#[derive(Clone, Copy, Debug)]
pub enum Side {
    Node,
    Client,
}

/// Why a node sent S3 a request: the core's read, or a client's request
/// passed through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum S3Kind {
    Read,
    Forward,
}

/// A TLS link: clients to a gateway, or members to a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Link {
    Client,
    Cluster,
}

#[derive(Default)]
struct Counts {
    requests: BTreeMap<(Operation, u16), u64>,
    first_byte: BTreeMap<Operation, Histogram>,
    response_bytes: BTreeMap<Operation, u64>,
    node_failures: [u64; 3],
    relays_cut: [u64; 2],
    s3_requests: BTreeMap<(S3Kind, Option<u16>), u64>,
    s3_first_byte: BTreeMap<S3Kind, Histogram>,
    syncs: Histogram,
    events: u64,
    events_lag: Histogram,
    /// Rings the gateway adopted, and the node.
    ring_changes: [u64; 2],
    /// By link and whether the kernel carries the session.
    tls_sessions: BTreeMap<(Link, bool), u64>,
    tls_failures: BTreeMap<Link, u64>,
    loop_delay: Histogram,
}

/// What the server measured. It lives on the event loop and is shared as
/// an `Rc`.
#[derive(Default)]
pub struct Metrics {
    counts: RefCell<Counts>,
}

/// The rest of what a scrape renders: the core's counts and the rings.
pub struct View {
    pub gateway: bool,
    /// The node's counts and usage.
    pub node: Option<(Stats, Usage)>,
    /// The current ring's version, and its nodes up and down.
    pub ring: Option<(u64, usize, usize)>,
}

impl Metrics {
    /// A client request answered with `status`, `first_byte` after its head
    /// arrived, with `bytes` of body.
    pub fn request(&self, operation: Operation, status: u16, first_byte: Duration, bytes: u64) {
        let mut counts = self.counts.borrow_mut();
        *counts.requests.entry((operation, status)).or_default() += 1;
        counts
            .first_byte
            .entry(operation)
            .or_default()
            .observe(first_byte);
        *counts.response_bytes.entry(operation).or_default() += bytes;
    }

    pub fn node_failure(&self, failure: NodeFailure) {
        self.counts.borrow_mut().node_failures[failure as usize] += 1;
    }

    pub fn relay_cut(&self, side: Side) {
        self.counts.borrow_mut().relays_cut[side as usize] += 1;
    }

    /// A request to S3, answered with `status` after `first_byte`, or never
    /// answered.
    pub fn s3_request(&self, kind: S3Kind, answer: Option<(u16, Duration)>) {
        let mut counts = self.counts.borrow_mut();
        let status = answer.map(|(status, _)| status);
        *counts.s3_requests.entry((kind, status)).or_default() += 1;
        if let Some((_, first_byte)) = answer {
            counts
                .s3_first_byte
                .entry(kind)
                .or_default()
                .observe(first_byte);
        }
    }

    pub fn sync(&self, took: Duration) {
        self.counts.borrow_mut().syncs.observe(took);
    }

    /// An event message taken from the queue, `lag` after S3's event.
    pub fn event(&self, lag: Option<Duration>) {
        let mut counts = self.counts.borrow_mut();
        counts.events += 1;
        if let Some(lag) = lag {
            counts.events_lag.observe(lag);
        }
    }

    /// The gateway, or the node, adopted a ring.
    pub fn ring_changed(&self, gateway: bool) {
        self.counts.borrow_mut().ring_changes[usize::from(!gateway)] += 1;
    }

    pub fn tls_session(&self, link: Link, kernel: bool) {
        *self
            .counts
            .borrow_mut()
            .tls_sessions
            .entry((link, kernel))
            .or_default() += 1;
    }

    pub fn tls_failure(&self, link: Link) {
        *self
            .counts
            .borrow_mut()
            .tls_failures
            .entry(link)
            .or_default() += 1;
    }

    /// The event loop ran a timer `late`.
    pub fn loop_delay(&self, late: Duration) {
        self.counts.borrow_mut().loop_delay.observe(late);
    }

    /// Every metric in Prometheus's text format.
    pub fn render(&self, view: &View) -> String {
        let counts = self.counts.borrow();
        let mut out = Out(String::new());
        if view.gateway {
            render_gateway(&mut out, &counts);
        }
        if let Some((stats, usage)) = &view.node {
            render_core(&mut out, stats, usage);
            render_node(&mut out, &counts);
        }
        render_process(&mut out, &counts, view);
        out.0
    }
}

fn render_gateway(out: &mut Out, counts: &Counts) {
    out.family(
        "gateway_requests_total",
        "counter",
        "Client requests, by S3 operation and response status.",
    );
    for (&(operation, status), &count) in &counts.requests {
        let labels = format!("operation=\"{operation:?}\",code=\"{status}\"");
        out.sample("gateway_requests_total", &labels, count);
    }
    out.family(
        "gateway_first_byte_seconds",
        "histogram",
        "From a request's head arriving to its response's head leaving.",
    );
    for (operation, histogram) in &counts.first_byte {
        let labels = format!("operation=\"{operation:?}\"");
        out.histogram("gateway_first_byte_seconds", &labels, histogram);
    }
    out.family(
        "gateway_response_bytes_total",
        "counter",
        "Body bytes sent to clients.",
    );
    for (operation, &bytes) in &counts.response_bytes {
        let labels = format!("operation=\"{operation:?}\"");
        out.sample("gateway_response_bytes_total", &labels, bytes);
    }
    out.family(
        "gateway_node_failures_total",
        "counter",
        "Requests to nodes that got no usable answer.",
    );
    for (reason, count) in ["refused", "timeout", "error"]
        .iter()
        .zip(counts.node_failures)
    {
        let labels = format!("reason=\"{reason}\"");
        out.sample("gateway_node_failures_total", &labels, count);
    }
    out.family(
        "gateway_relays_cut_total",
        "counter",
        "Responses cut short after their head, by the side that ended them.",
    );
    for (side, count) in ["node", "client"].iter().zip(counts.relays_cut) {
        let labels = format!("side=\"{side}\"");
        out.sample("gateway_relays_cut_total", &labels, count);
    }
}

fn render_core(out: &mut Out, stats: &Stats, usage: &Usage) {
    let counter = |out: &mut Out, name: &str, help: &str, samples: &[(&str, u64)]| {
        out.family(name, "counter", help);
        for (labels, value) in samples {
            out.sample(name, labels, *value);
        }
    };
    let gauge = |out: &mut Out, name: &str, help: &str, value: u64| {
        out.family(name, "gauge", help);
        out.sample(name, "", value);
    };
    counter(
        out,
        "node_reads_total",
        "Reads from gateways, and from nodes that took over placements.",
        &[("", stats.reads)],
    );
    counter(
        out,
        "node_block_reads_total",
        "Blocks the node's responses read: from the store, or from S3's or a previous owner's response.",
        &[
            ("result=\"hit\"", stats.block_hits),
            ("result=\"fetched\"", stats.blocks_fetched),
        ],
    );
    counter(
        out,
        "node_block_misses_total",
        "Blocks an owner or leased replica fetched and could store, by what it remembers of them.",
        &[
            ("reason=\"evicted\"", stats.misses_evicted),
            ("reason=\"unadmitted\"", stats.misses_unadmitted),
            ("reason=\"new\"", stats.misses_new),
        ],
    );
    counter(
        out,
        "node_body_bytes_total",
        "Body bytes the node sent, by where they came from.",
        &[
            ("source=\"cache\"", stats.hit_bytes),
            ("source=\"s3\"", stats.miss_bytes),
            ("source=\"previous_owner\"", stats.peer_bytes),
        ],
    );
    counter(
        out,
        "node_admissions_total",
        "Blocks an owner or leased replica could store: stored, or what turned them away.",
        &[
            ("result=\"stored\"", stats.admitted),
            ("result=\"doorkeeper\"", stats.refused_doorkeeper),
            ("result=\"budget\"", stats.refused_budget),
            ("result=\"full\"", stats.refused_full),
        ],
    );
    counter(
        out,
        "node_blocks_dropped_total",
        "Blocks the store let go, by cause.",
        &[
            ("cause=\"evicted\"", stats.evicted_blocks),
            ("cause=\"disowned\"", stats.disowned_blocks),
            ("cause=\"purged\"", stats.purged_blocks),
            ("cause=\"corrupt\"", stats.corrupt_dropped),
            ("cause=\"unfilled\"", stats.unfilled_blocks),
        ],
    );
    gauge(
        out,
        "node_fill_bytes",
        "Bytes of blocks filling, which the fill budget caps.",
        usage.filling_bytes,
    );
    gauge(
        out,
        "node_fill_budget_bytes",
        "The fill budget.",
        usage.fill_budget,
    );
    out.family(
        "node_store_blocks",
        "gauge",
        "Blocks in each size class, by slot size in bytes.",
    );
    for class in &usage.classes {
        let labels = format!("class=\"{}\"", class.size);
        out.sample("node_store_blocks", &labels, class.blocks);
    }
    out.family(
        "node_store_extents",
        "gauge",
        "Extents of each size class, by slot size in bytes.",
    );
    for class in &usage.classes {
        let labels = format!("class=\"{}\"", class.size);
        out.sample("node_store_extents", &labels, u64::from(class.extents));
    }
    gauge(
        out,
        "node_store_capacity_bytes",
        "The slab file's size.",
        usage.capacity,
    );
    gauge(
        out,
        "node_objects",
        "Objects whose metadata the node holds.",
        usage.objects,
    );
    counter(
        out,
        "node_written_bytes_total",
        "Block bytes written to the slab file.",
        &[("", stats.written_bytes)],
    );
    counter(
        out,
        "node_verified_blocks_total",
        "Recovered blocks checked against their checksums.",
        &[
            (
                "result=\"intact\"",
                stats.verified_blocks - stats.corrupt_blocks,
            ),
            ("result=\"corrupt\"", stats.corrupt_blocks),
        ],
    );
    counter(
        out,
        "node_fallback_reads_total",
        "Reads asked of previous owners.",
        &[("", stats.peer_requests)],
    );
    counter(
        out,
        "node_fallback_timeouts_total",
        "Reads asked of previous owners that went unanswered.",
        &[("", stats.peer_timeouts)],
    );
    counter(
        out,
        "node_fallback_metadata_total",
        "Objects whose metadata a previous home supplied.",
        &[("", stats.peer_metadata)],
    );
    counter(
        out,
        "node_leases_granted_total",
        "Leases the node granted as a placement's owner.",
        &[("", stats.leases_granted)],
    );
    counter(
        out,
        "node_leased_reads_total",
        "Reads the node served as a leased replica.",
        &[("", stats.leased_reads)],
    );
    counter(
        out,
        "node_warmed_uploads_total",
        "Uploads the node warmed as they passed.",
        &[("", stats.warmed_uploads)],
    );
    counter(
        out,
        "node_prefetched_blocks_total",
        "Blocks of format metadata filled before a reader asked.",
        &[("", stats.prefetched_blocks)],
    );
    counter(
        out,
        "node_purges_total",
        "Purges the node carried out.",
        &[("", stats.purges)],
    );
    gauge(
        out,
        "node_purges_pending",
        "Purges waiting on other nodes to confirm.",
        usage.pending_purges,
    );
}

fn render_node(out: &mut Out, counts: &Counts) {
    out.family(
        "s3_requests_total",
        "counter",
        "Requests to S3, by why the node sent them and S3's status.",
    );
    for (&(kind, status), &count) in &counts.s3_requests {
        let kind = s3_kind(kind);
        let status = status.map_or("none".to_string(), |status| status.to_string());
        let labels = format!("kind=\"{kind}\",code=\"{status}\"");
        out.sample("s3_requests_total", &labels, count);
    }
    out.family(
        "s3_first_byte_seconds",
        "histogram",
        "From sending a request to S3 to its response's head.",
    );
    for (&kind, histogram) in &counts.s3_first_byte {
        let labels = format!("kind=\"{}\"", s3_kind(kind));
        out.histogram("s3_first_byte_seconds", &labels, histogram);
    }
    out.family(
        "node_sync_seconds",
        "histogram",
        "Each sync of the slab file, shared by the blocks it makes durable.",
    );
    out.histogram("node_sync_seconds", "", &counts.syncs);
    out.family(
        "events_received_total",
        "counter",
        "Event messages taken from the queue.",
    );
    out.sample("events_received_total", "", counts.events);
    out.family(
        "events_lag_seconds",
        "histogram",
        "From S3's event time to the node taking the message.",
    );
    out.histogram("events_lag_seconds", "", &counts.events_lag);
}

fn render_process(out: &mut Out, counts: &Counts, view: &View) {
    out.family(
        "ring_changes_total",
        "counter",
        "Rings the process adopted, by the role that adopted them.",
    );
    for (role, count) in ["gateway", "node"].iter().zip(counts.ring_changes) {
        out.sample("ring_changes_total", &format!("role=\"{role}\""), count);
    }
    if let Some((version, up, down)) = view.ring {
        out.family(
            "ring_info",
            "gauge",
            "1, labeled with the current ring's version.",
        );
        out.sample("ring_info", &format!("version=\"{version:016x}\""), 1);
        out.family(
            "ring_nodes",
            "gauge",
            "Nodes in the current ring, by whether the process routes around them.",
        );
        out.sample("ring_nodes", "state=\"up\"", up as u64);
        out.sample("ring_nodes", "state=\"down\"", down as u64);
    }
    out.family(
        "tls_sessions_total",
        "counter",
        "TLS sessions established, and whether the kernel carries them.",
    );
    for link in [Link::Client, Link::Cluster] {
        for kernel in [true, false] {
            let count = counts
                .tls_sessions
                .get(&(link, kernel))
                .copied()
                .unwrap_or(0);
            let mode = if kernel { "kernel" } else { "userspace" };
            let labels = format!("link=\"{}\",mode=\"{mode}\"", link_name(link));
            out.sample("tls_sessions_total", &labels, count);
        }
    }
    out.family(
        "tls_handshake_failures_total",
        "counter",
        "TLS handshakes that failed or timed out.",
    );
    for link in [Link::Client, Link::Cluster] {
        let count = counts.tls_failures.get(&link).copied().unwrap_or(0);
        let labels = format!("link=\"{}\"", link_name(link));
        out.sample("tls_handshake_failures_total", &labels, count);
    }
    out.family(
        "event_loop_delay_seconds",
        "histogram",
        "How late the event loop runs a 100 ms timer.",
    );
    out.histogram("event_loop_delay_seconds", "", &counts.loop_delay);
    out.family(
        "build_info",
        "gauge",
        "1, labeled with the binary's version.",
    );
    let version = format!("version=\"{}\"", env!("CARGO_PKG_VERSION"));
    out.sample("build_info", &version, 1);
    if let Some(process) = Process::read() {
        process.render(out);
    }
}

fn s3_kind(kind: S3Kind) -> &'static str {
    match kind {
        S3Kind::Read => "read",
        S3Kind::Forward => "forward",
    }
}

fn link_name(link: Link) -> &'static str {
    match link {
        Link::Client => "client",
        Link::Cluster => "cluster",
    }
}

/// The text of a scrape, each name prefixed `s3accel_`.
struct Out(String);

impl Out {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP s3accel_{name} {help}");
        let _ = writeln!(self.0, "# TYPE s3accel_{name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &str, value: impl std::fmt::Display) {
        match labels {
            "" => {
                let _ = writeln!(self.0, "s3accel_{name} {value}");
            }
            labels => {
                let _ = writeln!(self.0, "s3accel_{name}{{{labels}}} {value}");
            }
        }
    }

    fn histogram(&mut self, name: &str, labels: &str, histogram: &Histogram) {
        let join = |extra: &str| match labels {
            "" => extra.to_string(),
            labels => format!("{labels},{extra}"),
        };
        let mut cumulative = 0;
        for (bound, count) in BUCKETS.iter().zip(histogram.counts) {
            cumulative += count;
            let labels = join(&format!("le=\"{bound}\""));
            self.sample(&format!("{name}_bucket"), &labels, cumulative);
        }
        cumulative += histogram.counts[BUCKETS.len()];
        self.sample(&format!("{name}_bucket"), &join("le=\"+Inf\""), cumulative);
        self.sample(&format!("{name}_sum"), labels, histogram.sum);
        self.sample(&format!("{name}_count"), labels, cumulative);
    }
}

/// This process's resources, from `/proc/self`.
struct Process {
    cpu_seconds: f64,
    resident_bytes: u64,
    open_fds: u64,
    start_time: f64,
}

impl Process {
    fn read() -> Option<Process> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Fields after the command, which may hold spaces, start at the
        // state, field 3.
        let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
        let field = |number: usize| fields.get(number - 3)?.parse::<u64>().ok();
        let ticks = rustix::param::clock_ticks_per_second() as f64;
        let boot = std::fs::read_to_string("/proc/stat")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("btime ")?.trim().parse::<u64>().ok())?;
        Some(Process {
            cpu_seconds: (field(14)? + field(15)?) as f64 / ticks,
            resident_bytes: field(24)? * rustix::param::page_size() as u64,
            open_fds: std::fs::read_dir("/proc/self/fd").ok()?.count() as u64,
            start_time: boot as f64 + field(22)? as f64 / ticks,
        })
    }

    fn render(&self, out: &mut Out) {
        let mut plain = |name: &str, kind: &str, help: &str, value: String| {
            let _ = writeln!(out.0, "# HELP {name} {help}");
            let _ = writeln!(out.0, "# TYPE {name} {kind}");
            let _ = writeln!(out.0, "{name} {value}");
        };
        plain(
            "process_cpu_seconds_total",
            "counter",
            "User and system CPU time.",
            self.cpu_seconds.to_string(),
        );
        plain(
            "process_resident_memory_bytes",
            "gauge",
            "Resident memory.",
            self.resident_bytes.to_string(),
        );
        plain(
            "process_open_fds",
            "gauge",
            "Open file descriptors.",
            self.open_fds.to_string(),
        );
        plain(
            "process_start_time_seconds",
            "gauge",
            "When the process started, in seconds since the Unix epoch.",
            self.start_time.to_string(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(method: &str, path: &str, query: &str) -> RequestHead {
        RequestHead {
            method: method.to_string(),
            path: path.to_string(),
            query: query.to_string(),
            headers: Vec::new(),
            keep_alive: true,
        }
    }

    #[test]
    fn requests_name_their_operations() {
        let of = |method, path, query| Operation::of(&head(method, path, query), false);
        assert_eq!(of("GET", "/b/k", ""), Operation::GetObject);
        assert_eq!(
            of("GET", "/b/k", "x-id=GetObject&versionId=1"),
            Operation::GetObject
        );
        assert_eq!(of("GET", "/b/k", "tagging"), Operation::Other);
        assert_eq!(of("HEAD", "/b/k", ""), Operation::HeadObject);
        assert_eq!(of("PUT", "/b/k", ""), Operation::PutObject);
        assert_eq!(
            of("PUT", "/b/k", "partNumber=1&uploadId=u"),
            Operation::UploadPart
        );
        assert_eq!(of("PUT", "/b", ""), Operation::Other);
        assert_eq!(of("DELETE", "/b/k", ""), Operation::DeleteObject);
        assert_eq!(
            of("DELETE", "/b/k", "uploadId=u"),
            Operation::AbortMultipartUpload
        );
        assert_eq!(
            of("POST", "/b/k", "uploads"),
            Operation::CreateMultipartUpload
        );
        assert_eq!(
            of("POST", "/b/k", "uploadId=u"),
            Operation::CompleteMultipartUpload
        );
        assert_eq!(of("POST", "/b/k", "x-accel-purge"), Operation::Purge);
        assert_eq!(of("POST", "/b", "delete"), Operation::DeleteObjects);
        assert_eq!(
            of("GET", "/b", "list-type=2&prefix=a"),
            Operation::ListObjects
        );
        assert_eq!(of("GET", "/b", "uploads"), Operation::Other);
        assert_eq!(of("GET", "/", ""), Operation::Other);
        let mut copy = head("PUT", "/b/k", "");
        copy.headers
            .push(("x-amz-copy-source".into(), "/b/j".into()));
        assert_eq!(Operation::of(&copy, false), Operation::CopyObject);
        assert_eq!(
            Operation::of(&head("GET", "/k", ""), true),
            Operation::GetObject
        );
        assert_eq!(
            Operation::of(&head("GET", "/", ""), true),
            Operation::ListObjects
        );
    }

    #[test]
    fn a_histogram_counts_each_observation_under_every_bound_above_it() {
        let metrics = Metrics::default();
        metrics.sync(Duration::from_micros(300));
        metrics.sync(Duration::from_secs(20));
        let view = View {
            gateway: false,
            node: None,
            ring: None,
        };
        let text = metrics.render(&view);
        let bucket = |bound: &str| {
            let prefix = format!("s3accel_node_sync_seconds_bucket{{le=\"{bound}\"}} ");
            text.lines()
                .find_map(|line| line.strip_prefix(prefix.as_str()))
                .map(|count| count.to_string())
        };
        // Node metrics render only for a node.
        assert_eq!(bucket("0.0005"), None);
        let text = metrics.render(&View {
            node: Some((Stats::default(), usage())),
            ..view
        });
        let bucket = |bound: &str| {
            let prefix = format!("s3accel_node_sync_seconds_bucket{{le=\"{bound}\"}} ");
            text.lines()
                .find_map(|line| line.strip_prefix(prefix.as_str()))
                .map(|count| count.to_string())
        };
        assert_eq!(bucket("0.00025").as_deref(), Some("0"));
        assert_eq!(bucket("0.0005").as_deref(), Some("1"));
        assert_eq!(bucket("10").as_deref(), Some("1"));
        assert_eq!(bucket("+Inf").as_deref(), Some("2"));
        assert!(text.contains("s3accel_node_sync_seconds_count 2\n"));
    }

    fn usage() -> Usage {
        Usage {
            capacity: 0,
            filling_bytes: 0,
            fill_budget: 0,
            objects: 0,
            pending_purges: 0,
            classes: Vec::new(),
        }
    }
}
