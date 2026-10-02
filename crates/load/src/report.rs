//! Turns a run's directory into a Markdown report and a CSV timeline per
//! step. The driver fills the directory:
//!
//! - `run.json`: the run's name, plan and settings, and each step's window
//!   and the faults it injected.
//! - `results/<step>/<client>.json`: each client host's `HostResult`.
//! - `metrics/<step>/<process>.{before,after}.prom`: each process's
//!   `/metrics` around the step.
//! - `hosts/<step>/<host>.{before,after}.txt`: each host's CPU, network and
//!   disk counters from `/proc` around the step.
//! - `cloudwatch/<step>.json`: S3's request metrics over the step, when the
//!   driver could fetch them.
//!
//! Client figures come from the clients' clocks, host figures from the
//! kernel, and S3's from CloudWatch; the processes' own counts appear
//! beside them, labeled as such.

use crate::histogram::Histogram;
use crate::run::{ClassResult, HostResult, Second};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

const GIB: f64 = (1u64 << 30) as f64;

#[derive(Debug, Default, Deserialize)]
struct RunInfo {
    #[serde(default)]
    name: String,
    #[serde(default)]
    plan: String,
    #[serde(default)]
    summary: Vec<String>,
    #[serde(default)]
    steps: Vec<StepInfo>,
}

#[derive(Debug, Default, Deserialize)]
struct StepInfo {
    name: String,
    #[serde(default)]
    faults: Vec<FaultInfo>,
}

#[derive(Debug, Deserialize)]
struct FaultInfo {
    at_seconds: f64,
    action: String,
    node: String,
    #[serde(default)]
    outcome: String,
}

/// One step's results from every client host, merged.
struct Merged {
    target: String,
    hosts: usize,
    connections: usize,
    rate: f64,
    seconds: f64,
    classes: Vec<ClassResult>,
    timeline: Vec<Second>,
}

pub fn report(run: &Path) -> Result<String, String> {
    let info: RunInfo = match fs::read_to_string(run.join("run.json")) {
        Ok(text) => serde_json::from_str(&text).map_err(|error| format!("run.json: {error}"))?,
        Err(_) => RunInfo::default(),
    };
    let mut steps: Vec<String> = info.steps.iter().map(|step| step.name.clone()).collect();
    for name in listed(&run.join("results")) {
        if !steps.contains(&name) {
            steps.push(name);
        }
    }
    let mut out = String::new();
    let title = if info.name.is_empty() {
        run.display().to_string()
    } else {
        info.name.clone()
    };
    let _ = writeln!(out, "# Load test {title}\n");
    if !info.plan.is_empty() {
        let _ = writeln!(out, "Plan: `{}`.\n", info.plan);
    }
    for line in &info.summary {
        let _ = writeln!(out, "- {line}");
    }
    if !info.summary.is_empty() {
        out.push('\n');
    }
    let mut sections = String::new();
    let mut summary = String::from(
        "| Step | Target | Requests | Errors | GiB/s | Requests/s | First byte p50 | p99 | p99.9 | S3 reads (nodes) | Block hits (nodes) |\n\
         |---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n",
    );
    fs::create_dir_all(run.join("timelines")).map_err(|error| error.to_string())?;
    for step in &steps {
        let Some(merged) = merge(&run.join("results").join(step))? else {
            continue;
        };
        let metrics = Metrics::read(&run.join("metrics").join(step));
        let mut all = ClassResult::merged("All", &merged.classes);
        all.label = "All".into();
        let requests = all.successes() + all.errors();
        let _ = writeln!(
            summary,
            "| {step} | {} | {requests} | {} | {:.2} | {:.0} | {} | {} | {} | {} | {} |",
            merged.target,
            all.errors(),
            all.bytes as f64 / GIB / merged.seconds.max(1e-9),
            all.successes() as f64 / merged.seconds.max(1e-9),
            millis(all.first_byte.percentile(0.5)),
            millis(all.first_byte.percentile(0.99)),
            millis(all.first_byte.percentile(0.999)),
            metrics.as_ref().map_or("".into(), |m| format!(
                "{:.0}",
                m.node_sum("s3accel_s3_requests_total", &[("kind", "read")])
            )),
            metrics.as_ref().map_or("".into(), |m| m.block_hits()),
        );
        let faults = info
            .steps
            .iter()
            .find(|info| &info.name == step)
            .map(|info| info.faults.as_slice())
            .unwrap_or_default();
        section(
            &mut sections,
            run,
            step,
            &merged,
            &all,
            metrics.as_ref(),
            faults,
        )?;
        write_timeline(
            &run.join("timelines").join(format!("{step}.csv")),
            &merged.timeline,
        )?;
    }
    let _ = writeln!(out, "## Summary\n\n{summary}");
    out.push_str(&sections);
    fs::write(run.join("report.md"), &out).map_err(|error| error.to_string())?;
    Ok(out)
}

fn listed(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    names.sort();
    names
}

fn merge(dir: &Path) -> Result<Option<Merged>, String> {
    let mut merged: Option<Merged> = None;
    for name in listed(dir)
        .into_iter()
        .filter(|name| name.ends_with(".json"))
    {
        let text = fs::read_to_string(dir.join(&name)).map_err(|error| error.to_string())?;
        let result: HostResult = serde_json::from_str(&text)
            .map_err(|error| format!("{}: {error}", dir.join(&name).display()))?;
        let into = merged.get_or_insert_with(|| Merged {
            target: format!("{:?}", result.target).to_lowercase(),
            hosts: 0,
            connections: result.connections,
            rate: result.rate,
            seconds: 0.0,
            classes: result
                .classes
                .iter()
                .map(|class| ClassResult::merged(&class.label, &[]))
                .collect(),
            timeline: Vec::new(),
        });
        into.hosts += 1;
        into.seconds = into.seconds.max(result.seconds);
        for (class, theirs) in into.classes.iter_mut().zip(&result.classes) {
            class.merge(theirs);
        }
        if into.timeline.len() < result.timeline.len() {
            into.timeline.resize_with(result.timeline.len(), || Second {
                first_byte: Histogram::coarse(),
                ..Second::default()
            });
        }
        for (second, theirs) in into.timeline.iter_mut().zip(&result.timeline) {
            second.merge(theirs);
        }
    }
    Ok(merged)
}

impl ClassResult {
    fn merged(label: &str, classes: &[ClassResult]) -> ClassResult {
        let mut all = ClassResult {
            label: label.to_string(),
            statuses: BTreeMap::new(),
            failures: BTreeMap::new(),
            bytes: 0,
            first_byte: Histogram::default(),
            total: Histogram::default(),
        };
        for class in classes {
            all.merge(class);
        }
        all
    }
}

fn millis(micros: u64) -> String {
    let ms = micros as f64 / 1_000.0;
    if ms < 10.0 {
        format!("{ms:.2} ms")
    } else {
        format!("{ms:.0} ms")
    }
}

/// Percentiles the report's distribution table shows.
const SHOWN: [(&str, f64); 7] = [
    ("p50", 0.5),
    ("p75", 0.75),
    ("p90", 0.9),
    ("p95", 0.95),
    ("p99", 0.99),
    ("p99.9", 0.999),
    ("p99.99", 0.9999),
];

/// Writes a step's latency distribution, over every host and class: a table
/// in the report, and `latency/<step>.csv` with the first byte's and the
/// last byte's microseconds at each percentile from 1 to 99, then 99.5,
/// 99.9, 99.95, 99.99 and the maximum, for charts.
fn latency(out: &mut String, run: &Path, step: &str, all: &ClassResult) -> Result<(), String> {
    if all.successes() == 0 {
        return Ok(());
    }
    let header: Vec<&str> = SHOWN.iter().map(|(name, _)| *name).collect();
    let _ = writeln!(
        out,
        "\nLatency of answered requests:\n\n| | {} | max |\n|---|{}--:|",
        header.join(" | "),
        "--:|".repeat(SHOWN.len())
    );
    for (name, histogram) in [("First byte", &all.first_byte), ("Last byte", &all.total)] {
        let shown: Vec<String> = SHOWN
            .iter()
            .map(|(_, share)| millis(histogram.percentile(*share)))
            .collect();
        let max = millis(histogram.max());
        let _ = writeln!(out, "| {name} | {} | {max} |", shown.join(" | "));
    }
    let mut csv = String::from("percentile,first_byte_us,last_byte_us\n");
    let percents = (1..100)
        .map(|percent| percent.to_string())
        .chain(["99.5", "99.9", "99.95", "99.99"].map(String::from));
    for percent in percents {
        let share = percent.parse::<f64>().unwrap_or_default() / 100.0;
        let _ = writeln!(
            csv,
            "{percent},{},{}",
            all.first_byte.percentile(share),
            all.total.percentile(share)
        );
    }
    let _ = writeln!(csv, "100,{},{}", all.first_byte.max(), all.total.max());
    let dir = run.join("latency");
    fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
    fs::write(dir.join(format!("{step}.csv")), csv).map_err(|error| error.to_string())
}

fn section(
    out: &mut String,
    run: &Path,
    step: &str,
    merged: &Merged,
    all: &ClassResult,
    metrics: Option<&Metrics>,
    faults: &[FaultInfo],
) -> Result<(), String> {
    let pacing = match merged.rate {
        rate if rate > 0.0 => format!("{rate:.0} requests/s each"),
        _ => "as fast as they answer".into(),
    };
    let _ = writeln!(
        out,
        "## {step}\n\nTarget: {}. {} client hosts × {} connections, {pacing}, over {:.0} s measured.\n",
        merged.target, merged.hosts, merged.connections, merged.seconds
    );
    let _ = writeln!(
        out,
        "| Requests of | Answered | Errors | GiB | GiB/s | Requests/s | First byte p50 | p90 | p99 | p99.9 | max | Last byte p50 | p99 |\n\
         |---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"
    );
    let seconds = merged.seconds.max(1e-9);
    for class in merged.classes.iter().chain(std::iter::once(all)) {
        if class.successes() + class.errors() == 0 {
            continue;
        }
        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.2} | {:.2} | {:.0} | {} | {} | {} | {} | {} | {} | {} |",
            class.label,
            class.successes(),
            class.errors(),
            class.bytes as f64 / GIB,
            class.bytes as f64 / GIB / seconds,
            class.successes() as f64 / seconds,
            millis(class.first_byte.percentile(0.5)),
            millis(class.first_byte.percentile(0.9)),
            millis(class.first_byte.percentile(0.99)),
            millis(class.first_byte.percentile(0.999)),
            millis(class.first_byte.max()),
            millis(class.total.percentile(0.5)),
            millis(class.total.percentile(0.99)),
        );
    }
    let errors: Vec<String> = all
        .statuses
        .iter()
        .filter(|(status, _)| !(200..300).contains(*status))
        .map(|(status, count)| format!("{count} × {status}"))
        .chain(
            all.failures
                .iter()
                .map(|(failure, count)| format!("{count} × {failure:?}")),
        )
        .collect();
    if !errors.is_empty() {
        let _ = writeln!(out, "\nErrors: {}.", errors.join(", "));
    }
    latency(out, run, step, all)?;
    if let Some(cloudwatch) = CloudWatch::read(&run.join("cloudwatch").join(format!("{step}.json")))
    {
        let _ = writeln!(
            out,
            "\nS3, from its CloudWatch request metrics over the step's minutes: {:.0} GETs, {:.0} PUTs, {:.2} GiB downloaded, {:.2} GiB uploaded, {:.0} 4xx, {:.0} 5xx.",
            cloudwatch.get("GetRequests"),
            cloudwatch.get("PutRequests"),
            cloudwatch.get("BytesDownloaded") / GIB,
            cloudwatch.get("BytesUploaded") / GIB,
            cloudwatch.get("4xxErrors"),
            cloudwatch.get("5xxErrors"),
        );
    }
    if let Some(metrics) = metrics {
        metrics.render(out, seconds);
    }
    hosts(out, &run.join("hosts").join(step), seconds);
    if !faults.is_empty() {
        let _ = writeln!(out, "\nFaults:\n");
        for fault in faults {
            let outcome = if fault.outcome.is_empty() {
                String::new()
            } else {
                format!(": {}", fault.outcome)
            };
            let _ = writeln!(
                out,
                "- {:.0} s: {} {}{outcome}",
                fault.at_seconds, fault.action, fault.node
            );
        }
    }
    timeline(out, &merged.timeline);
    out.push('\n');
    Ok(())
}

/// Ten-second windows of the step's timeline.
fn timeline(out: &mut String, seconds: &[Second]) {
    if seconds.is_empty() {
        return;
    }
    let _ = writeln!(
        out,
        "\nTimeline, by 10 s windows:\n\n| From | Requests/s | GiB/s | Errors | First byte p50 | p99 |\n|--:|--:|--:|--:|--:|--:|"
    );
    for (window, chunk) in seconds.chunks(10).enumerate() {
        let mut merged = Second {
            first_byte: Histogram::coarse(),
            ..Second::default()
        };
        for second in chunk {
            merged.merge(second);
        }
        let len = chunk.len() as f64;
        let _ = writeln!(
            out,
            "| {} s | {:.0} | {:.2} | {} | {} | {} |",
            window * 10,
            merged.requests as f64 / len,
            merged.bytes as f64 / GIB / len,
            merged.errors,
            millis(merged.first_byte.percentile(0.5)),
            millis(merged.first_byte.percentile(0.99)),
        );
    }
}

fn write_timeline(path: &Path, seconds: &[Second]) -> Result<(), String> {
    let mut csv = String::from(
        "second,requests,bytes,errors,first_byte_p50_us,first_byte_p99_us,first_byte_max_us\n",
    );
    for (index, second) in seconds.iter().enumerate() {
        let _ = writeln!(
            csv,
            "{index},{},{},{},{},{},{}",
            second.requests,
            second.bytes,
            second.errors,
            second.first_byte.percentile(0.5),
            second.first_byte.percentile(0.99),
            second.first_byte.max(),
        );
    }
    fs::write(path, csv).map_err(|error| error.to_string())
}

/// Samples of one scrape: name, labels and value.
type Scrape = Vec<(String, BTreeMap<String, String>, f64)>;

fn parse_scrape(text: &str) -> Scrape {
    let mut samples = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let (name, labels) = match series.split_once('{') {
            Some((name, labels)) => (name, labels.trim_end_matches('}')),
            None => (series, ""),
        };
        let labels = labels
            .split(',')
            .filter_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                Some((key.to_string(), value.trim_matches('"').to_string()))
            })
            .collect();
        samples.push((name.to_string(), labels, value));
    }
    samples
}

fn sum(scrape: &Scrape, name: &str, labels: &[(&str, &str)]) -> f64 {
    scrape
        .iter()
        .filter(|(sample, found, _)| {
            sample == name
                && labels.iter().all(|(key, value)| {
                    found
                        .get(*key)
                        .is_some_and(|found| match value.strip_suffix('*') {
                            Some(prefix) => found.starts_with(prefix),
                            None => found == value,
                        })
                })
        })
        .map(|(_, _, value)| value)
        .fold(0.0, |total, value| total + value)
}

/// Each process's scrapes from before and after a step. A process that
/// started during the step has no scrape from before, so its counts begin
/// at its start.
struct Metrics {
    processes: Vec<(String, Scrape, Scrape)>,
    /// Processes that started during the step.
    started: Vec<String>,
}

impl Metrics {
    fn read(dir: &Path) -> Option<Metrics> {
        let (mut processes, mut started) = (Vec::new(), Vec::new());
        for name in listed(dir) {
            let Some(process) = name.strip_suffix(".after.prom") else {
                continue;
            };
            let before =
                fs::read_to_string(dir.join(format!("{process}.before.prom"))).unwrap_or_default();
            let after = fs::read_to_string(dir.join(&name)).unwrap_or_default();
            let (mut before, after) = (parse_scrape(&before), parse_scrape(&after));
            let start = |scrape: &Scrape| sum(scrape, "process_start_time_seconds", &[]);
            if !after.is_empty() && start(&before) != start(&after) {
                if !before.is_empty() {
                    started.push(process.to_string());
                }
                before.clear();
            }
            processes.push((process.to_string(), before, after));
        }
        (!processes.is_empty()).then_some(Metrics { processes, started })
    }

    fn is_node(scrape: &Scrape) -> bool {
        scrape
            .iter()
            .any(|(name, _, _)| name == "s3accel_node_reads_total")
    }

    /// A counter's growth over the step, summed over storage nodes.
    fn node_sum(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.processes
            .iter()
            .filter(|(_, _, after)| Metrics::is_node(after))
            .map(|(_, before, after)| sum(after, name, labels) - sum(before, name, labels))
            .sum()
    }

    fn all_sum(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.processes
            .iter()
            .map(|(_, before, after)| sum(after, name, labels) - sum(before, name, labels))
            .sum()
    }

    fn block_hits(&self) -> String {
        let hits = self.node_sum("s3accel_node_block_reads_total", &[("result", "hit")]);
        let fetched = self.node_sum("s3accel_node_block_reads_total", &[("result", "fetched")]);
        match hits + fetched {
            0.0 => String::new(),
            total => format!("{:.1}%", 100.0 * hits / total),
        }
    }

    fn render(&self, out: &mut String, seconds: f64) {
        let node = |name: &str, labels: &[(&str, &str)]| self.node_sum(name, labels);
        let bytes =
            |source: &str| node("s3accel_node_body_bytes_total", &[("source", source)]) / GIB;
        let (cache, s3, previous) = (bytes("cache"), bytes("s3"), bytes("previous_owner"));
        let served = cache + s3 + previous;
        let _ = writeln!(
            out,
            "\nThe processes' own counts, from `/metrics` before and after the step:\n\n\
             | S3 reads | S3 reads 5xx | S3 reads unanswered | Forwarded to S3 | Block hits | GiB from cache | GiB from S3 | GiB from previous owners | Byte hit % | Blocks stored | Refused: doorkeeper | budget | full | GiB written | Blocks evicted | Leases granted | Leased reads | Gateway 5xx | Gateway node failures |\n\
             |--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"
        );
        let _ = writeln!(
            out,
            "| {:.0} | {:.0} | {:.0} | {:.0} | {} | {cache:.2} | {s3:.2} | {previous:.2} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.2} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} |",
            node("s3accel_s3_requests_total", &[("kind", "read")]),
            node(
                "s3accel_s3_requests_total",
                &[("kind", "read"), ("code", "5*")]
            ),
            node(
                "s3accel_s3_requests_total",
                &[("kind", "read"), ("code", "none")]
            ),
            node("s3accel_s3_requests_total", &[("kind", "forward")]),
            self.block_hits(),
            match served {
                0.0 => String::new(),
                served => format!("{:.1}%", 100.0 * cache / served),
            },
            node("s3accel_node_admissions_total", &[("result", "stored")]),
            node("s3accel_node_admissions_total", &[("result", "doorkeeper")]),
            node("s3accel_node_admissions_total", &[("result", "budget")]),
            node("s3accel_node_admissions_total", &[("result", "full")]),
            node("s3accel_node_written_bytes_total", &[]) / GIB,
            node("s3accel_node_blocks_dropped_total", &[("cause", "evicted")]),
            node("s3accel_node_leases_granted_total", &[]),
            node("s3accel_node_leased_reads_total", &[]),
            self.all_sum("s3accel_gateway_requests_total", &[("code", "5*")]),
            self.all_sum("s3accel_gateway_node_failures_total", &[]),
        );
        let _ = writeln!(
            out,
            "\n| Process | CPU cores | Resident GiB | Open files | Event loop late p99 |\n|---|--:|--:|--:|--:|"
        );
        for (process, before, after) in &self.processes {
            if after.is_empty() {
                let _ = writeln!(out, "| {process} | stopped | | | |");
                continue;
            }
            let cpu = sum(after, "process_cpu_seconds_total", &[])
                - sum(before, "process_cpu_seconds_total", &[]);
            let _ = writeln!(
                out,
                "| {process} | {:.2} | {:.2} | {:.0} of {:.0} | {} |",
                cpu / seconds,
                sum(after, "process_resident_memory_bytes", &[]) / GIB,
                sum(after, "process_open_fds", &[]),
                sum(after, "process_max_fds", &[]),
                loop_delay(before, after),
            );
        }
        if !self.started.is_empty() {
            let _ = writeln!(
                out,
                "\n{} started during the step, so these counts take each from its start, \
                 and a stopped process's counts are left out.",
                self.started.join(", ")
            );
        }
    }
}

/// The 99th percentile of how late the event loop ran its timer over the
/// step, as the histogram's bucket bound.
fn loop_delay(before: &Scrape, after: &Scrape) -> String {
    let name = "s3accel_event_loop_delay_seconds_bucket";
    let mut buckets: Vec<(f64, f64)> = after
        .iter()
        .filter(|(sample, _, _)| sample == name)
        .filter_map(|(_, labels, value)| {
            let bound = labels.get("le")?;
            let bound = if bound == "+Inf" {
                f64::INFINITY
            } else {
                bound.parse().ok()?
            };
            let earlier = sum(before, name, &[("le", labels.get("le")?.as_str())]);
            Some((bound, value - earlier))
        })
        .collect();
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    let Some(&(_, total)) = buckets.last() else {
        return String::new();
    };
    if total == 0.0 {
        return String::new();
    }
    buckets
        .iter()
        .find(|(_, count)| *count >= 0.99 * total)
        .map(|(bound, _)| match bound.is_finite() {
            true => format!("≤ {} ms", bound * 1_000.0),
            false => "over 10 s".into(),
        })
        .unwrap_or_default()
}

/// S3's CloudWatch request metrics over a step, summed by name.
struct CloudWatch(BTreeMap<String, f64>);

impl CloudWatch {
    fn read(path: &Path) -> Option<CloudWatch> {
        let text = fs::read_to_string(path).ok()?;
        serde_json::from_str(&text).ok().map(CloudWatch)
    }

    fn get(&self, name: &str) -> f64 {
        self.0.get(name).copied().unwrap_or(0.0)
    }
}

/// One host's kernel counters.
#[derive(Default)]
struct HostCounters {
    /// When the counters were read, in Unix seconds, if the snapshot says.
    time: Option<f64>,
    cpu_busy: f64,
    cpu_total: f64,
    received: f64,
    sent: f64,
    read: f64,
    written: f64,
    /// The kernel's TCP counters (`/proc/net/snmp` and `/proc/net/netstat`),
    /// as `Tcp:RetransSegs` or `TcpExt:TCPTimeouts`.
    tcp: BTreeMap<String, f64>,
    /// The network cards' drop counters (`ethtool -S`), summed over cards.
    nic: BTreeMap<String, f64>,
}

/// TCP counters the report shows, and their columns.
const TCP_SHOWN: [(&str, &str); 7] = [
    ("Tcp:RetransSegs", "Segments retransmitted"),
    ("TcpExt:TCPTimeouts", "Retransmission timeouts"),
    ("TcpExt:TCPLossProbes", "Tail loss probes"),
    ("TcpExt:TCPRcvQDrop", "Receive queue drops"),
    ("TcpExt:TCPMemoryPressures", "Memory pressure"),
    ("TcpExt:DelayedACKs", "Delayed ACKs"),
    ("TcpExt:TCPBacklogDrop", "Backlog drops"),
];

fn host_counters(text: &str) -> HostCounters {
    let mut counters = HostCounters::default();
    let mut section = "";
    let mut devices: Vec<String> = Vec::new();
    let mut disks: Vec<(String, f64, f64)> = Vec::new();
    // `/proc/net/snmp` and `/proc/net/netstat` give each table as a line
    // of names, then a line of values, under one prefix.
    let mut names: Option<(String, Vec<String>)> = None;
    for line in text.lines() {
        if let Some(name) = line.strip_prefix("== ") {
            section = name.trim();
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        match section {
            "time" => counters.time = fields.first().and_then(|time| time.parse().ok()),
            "cpu" if fields.first() == Some(&"cpu") => {
                let jiffies: Vec<f64> = fields[1..]
                    .iter()
                    .filter_map(|field| field.parse().ok())
                    .collect();
                // user nice system idle iowait irq softirq steal: guests
                // count within user and nice.
                let total: f64 = jiffies.iter().take(8).sum();
                let idle =
                    jiffies.get(3).copied().unwrap_or(0.0) + jiffies.get(4).copied().unwrap_or(0.0);
                counters.cpu_total = total;
                counters.cpu_busy = total - idle;
            }
            "net" => {
                if let Some((name, rest)) = line.split_once(':') {
                    let name = name.trim();
                    let values: Vec<f64> = rest
                        .split_whitespace()
                        .filter_map(|field| field.parse().ok())
                        .collect();
                    if name != "lo" && values.len() >= 9 {
                        counters.received += values[0];
                        counters.sent += values[8];
                    }
                }
            }
            "tcp" => {
                let Some(prefix) = fields.first().filter(|field| field.ends_with(':')) else {
                    continue;
                };
                match names.take() {
                    Some((named, keys)) if named == *prefix && fields[1].parse::<f64>().is_ok() => {
                        for (key, value) in keys.iter().zip(&fields[1..]) {
                            if let Ok(value) = value.parse::<f64>() {
                                counters.tcp.insert(format!("{prefix}{key}"), value);
                            }
                        }
                    }
                    _ => {
                        let keys = fields[1..].iter().map(|key| key.to_string()).collect();
                        names = Some((prefix.to_string(), keys));
                    }
                }
            }
            "nic" => {
                if let Some((name, value)) = line.split_once(':')
                    && let Ok(value) = value.trim().parse::<f64>()
                {
                    *counters.nic.entry(name.trim().to_string()).or_default() += value;
                }
            }
            "devices" => devices.extend(fields.iter().map(|field| field.to_string())),
            "disk" if fields.len() >= 10 => {
                let sectors = |index: usize| fields[index].parse::<f64>().unwrap_or(0.0) * 512.0;
                disks.push((fields[2].to_string(), sectors(5), sectors(9)));
            }
            _ => {}
        }
    }
    for (name, read, written) in disks {
        let counted = match devices.is_empty() {
            true => name.starts_with("nvme") && !name.contains('p'),
            false => devices.contains(&name),
        };
        if counted {
            counters.read += read;
            counters.written += written;
        }
    }
    counters
}

/// Each host's CPU, network and cache drives over the step, from the
/// kernel's counters read just before it and just after. Rates are over the
/// time between the reads, which takes in the step's start and warmup; a
/// snapshot without its time counts the measured `seconds`.
fn hosts(out: &mut String, dir: &Path, seconds: f64) {
    let mut rows = Vec::new();
    let mut tcp_rows = Vec::new();
    for name in listed(dir) {
        let Some(host) = name.strip_suffix(".after.txt") else {
            continue;
        };
        let (Ok(before), Ok(after)) = (
            fs::read_to_string(dir.join(format!("{host}.before.txt"))),
            fs::read_to_string(dir.join(&name)),
        ) else {
            continue;
        };
        let (before, after) = (host_counters(&before), host_counters(&after));
        let seconds = match (before.time, after.time) {
            (Some(before), Some(after)) if after > before => after - before,
            _ => seconds,
        };
        if !after.tcp.is_empty() {
            let delta = |map: &BTreeMap<String, f64>, old: &BTreeMap<String, f64>, key: &str| {
                map.get(key).copied().unwrap_or(0.0) - old.get(key).copied().unwrap_or(0.0)
            };
            let tcp: Vec<String> = TCP_SHOWN
                .iter()
                .map(|(key, _)| format!("{:.0}", delta(&after.tcp, &before.tcp, key)))
                .collect();
            let sent = delta(&after.tcp, &before.tcp, "Tcp:OutSegs").max(1.0);
            let retransmitted = delta(&after.tcp, &before.tcp, "Tcp:RetransSegs");
            let nic: f64 = after
                .nic
                .keys()
                .filter(|key| key.contains("allowance_exceeded"))
                .map(|key| delta(&after.nic, &before.nic, key))
                .sum();
            tcp_rows.push(format!(
                "| {host} | {} | {:.3}% | {nic:.0} |",
                tcp.join(" | "),
                100.0 * retransmitted / sent
            ));
        }
        let busy = after.cpu_busy - before.cpu_busy;
        let total = (after.cpu_total - before.cpu_total).max(1.0);
        rows.push(format!(
            "| {host} | {:.0}% | {:.2} | {:.2} | {:.2} | {:.2} |",
            100.0 * busy / total,
            (after.received - before.received) / GIB / seconds,
            (after.sent - before.sent) / GIB / seconds,
            (after.read - before.read) / GIB,
            (after.written - before.written) / GIB,
        ));
    }
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(
        out,
        "\nHosts, from the kernel's counters just before the step and just after, so averaged over its start and warmup too:\n\n| Host | CPU busy | Received GiB/s | Sent GiB/s | Cache disk read GiB | Cache disk written GiB |\n|---|--:|--:|--:|--:|--:|"
    );
    for row in rows {
        let _ = writeln!(out, "{row}");
    }
    if tcp_rows.is_empty() {
        return;
    }
    let columns: Vec<&str> = TCP_SHOWN.iter().map(|(_, column)| *column).collect();
    let _ = writeln!(
        out,
        "\nTCP and network cards over the same time: the kernel's counts, the share of segments sent again, and packets the network card held back past its allowances:\n\n| Host | {} | Retransmitted | Card allowance drops |\n|---|{}--:|--:|",
        columns.join(" | "),
        "--:|".repeat(columns.len())
    );
    for row in tcp_rows {
        let _ = writeln!(out, "{row}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_gives_tcp_and_network_card_counters() {
        let text = "== tcp\nTcp: RtoAlgorithm OutSegs RetransSegs\nTcp: 1 1000 7\n\
                    TcpExt: DelayedACKs TCPTimeouts\nTcpExt: 40 3\n\
                    == nic\n     bw_in_allowance_exceeded: 5\n     bw_in_allowance_exceeded: 2\n";
        let counters = host_counters(text);
        assert_eq!(counters.tcp["Tcp:RetransSegs"], 7.0);
        assert_eq!(counters.tcp["Tcp:OutSegs"], 1000.0);
        assert_eq!(counters.tcp["TcpExt:TCPTimeouts"], 3.0);
        assert_eq!(counters.nic["bw_in_allowance_exceeded"], 7.0);
    }

    #[test]
    fn a_scrape_sums_by_labels() {
        let scrape = parse_scrape(
            "# HELP x\ns3accel_s3_requests_total{kind=\"read\",code=\"200\"} 10\n\
             s3accel_s3_requests_total{kind=\"read\",code=\"503\"} 2\n\
             s3accel_s3_requests_total{kind=\"forward\",code=\"200\"} 5\n\
             process_open_fds 40\n",
        );
        assert_eq!(
            sum(&scrape, "s3accel_s3_requests_total", &[("kind", "read")]),
            12.0
        );
        assert_eq!(
            sum(&scrape, "s3accel_s3_requests_total", &[("code", "5*")]),
            2.0
        );
        assert_eq!(sum(&scrape, "process_open_fds", &[]), 40.0);
    }

    #[test]
    fn a_process_that_starts_during_a_step_counts_from_its_start() {
        let dir = std::env::temp_dir().join(format!("load-restart-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let scrape = |start: u64, reads: u64| {
            format!(
                "process_start_time_seconds {start}\ns3accel_node_reads_total 1\n\
                 s3accel_s3_requests_total{{kind=\"read\",code=\"200\"}} {reads}\n"
            )
        };
        let scrapes = [
            ("node-0", scrape(100, 50), scrape(100, 80)),
            ("node-1", scrape(100, 50), scrape(160, 7)),
            ("node-2", String::new(), String::new()),
        ];
        for (process, before, after) in scrapes {
            fs::write(dir.join(format!("{process}.before.prom")), before).unwrap();
            fs::write(dir.join(format!("{process}.after.prom")), after).unwrap();
        }
        let metrics = Metrics::read(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();
        let reads = metrics.node_sum("s3accel_s3_requests_total", &[("kind", "read")]);
        assert_eq!(reads, 30.0 + 7.0);
        assert_eq!(metrics.started, ["node-1"]);
        let mut out = String::new();
        metrics.render(&mut out, 10.0);
        assert!(out.contains("| node-2 | stopped |"), "{out}");
        assert!(!out.contains(" -0"), "{out}");
    }

    /// A host's rates are over the time between its snapshots, which the
    /// measured seconds leave short.
    #[test]
    fn host_rates_are_over_the_time_between_snapshots() {
        let dir = std::env::temp_dir().join(format!("load-hosts-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let snapshot = |time: f64, sent: u64| {
            format!(
                "== time\n{time}\n== cpu\ncpu  0 0 0 0 0 0 0 0 0 0\n== net\nInter-|\n face |\n  \
                 ens5: 0 0 0 0 0 0 0 0 {sent} 0 0 0 0 0 0 0\n"
            )
        };
        fs::write(dir.join("node-0.before.txt"), snapshot(1_000.0, 0)).unwrap();
        fs::write(dir.join("node-0.after.txt"), snapshot(1_120.0, 120 << 30)).unwrap();
        let mut out = String::new();
        hosts(&mut out, &dir, 100.0);
        fs::remove_dir_all(&dir).unwrap();
        assert!(out.contains("| node-0 | 0% | 0.00 | 1.00 |"), "{out}");
    }

    #[test]
    fn host_counters_read_proc() {
        let text = "== cpu\ncpu  100 0 50 800 50 0 0 0 0 0\n== net\nInter-|   Receive\n face |bytes\n    lo: 999 1 0 0 0 0 0 0 999 1 0 0 0 0 0 0\n  ens5: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0\n== devices\nnvme1n1\n== disk\n 259 0 nvme0n1 1 0 8 0 1 0 8 0 0 0 0\n 259 1 nvme1n1 10 0 100 0 20 0 200 0 0 0 0\n";
        let counters = host_counters(text);
        assert_eq!((counters.cpu_busy, counters.cpu_total), (150.0, 1_000.0));
        assert_eq!((counters.received, counters.sent), (1_000.0, 2_000.0));
        assert_eq!((counters.read, counters.written), (51_200.0, 102_400.0));
    }

    #[test]
    fn a_run_reports_every_hosts_results() {
        let dir = std::env::temp_dir().join(format!("load-report-{}", std::process::id()));
        let results = dir.join("results").join("hits");
        fs::create_dir_all(&results).unwrap();
        for host in 0..2 {
            let mut class = ClassResult::merged("small: whole", &[]);
            class.first_byte = Histogram::fine();
            class.total = Histogram::fine();
            class.statuses.insert(200, 100);
            class.bytes = 1 << 30;
            for micros in 0..100 {
                class.first_byte.record(1_000 + micros);
                class.total.record(2_000 + micros);
            }
            let mut second = Second {
                first_byte: Histogram::coarse(),
                ..Second::default()
            };
            second.requests = 100;
            let result = HostResult {
                step: "hits".into(),
                target: crate::plan::Target::Cache,
                host,
                hosts: 2,
                connections: 4,
                rate: 0.0,
                started_unix_ms: 0,
                seconds: 2.0,
                classes: vec![class],
                timeline: vec![second],
            };
            let path = results.join(format!("client-{host}.json"));
            fs::write(path, serde_json::to_string(&result).unwrap()).unwrap();
        }
        let report = report(&dir).unwrap();
        assert!(
            report.contains("| hits | cache | 200 | 0 | 1.00 | 100 |"),
            "{report}"
        );
        assert!(dir.join("timelines").join("hits.csv").exists());
        fs::remove_dir_all(&dir).unwrap();
    }
}
