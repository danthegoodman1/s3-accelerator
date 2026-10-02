//! A load test's plan: the dataset `seed` writes to the bucket, and the
//! steps `run` sends against the cache or S3, each a mix of reads and
//! writes for a while, with faults the driver injects as it goes.

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub dataset: Dataset,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    /// Every key starts with this.
    pub prefix: String,
    /// Keys, sizes and contents follow from it, so every host agrees on
    /// them without asking S3.
    #[serde(default)]
    pub seed: u64,
    pub sets: Vec<Set>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Set {
    pub name: String,
    pub count: u64,
    pub size: SizeSpec,
    /// `parquet` names keys `.parquet` and frames each object as a Parquet
    /// file whose footer is `footer` bytes, or half the object if smaller,
    /// so homes prefetch the footer as they would a real one.
    pub format: Option<SetFormat>,
    #[serde(default, deserialize_with = "bytes")]
    pub footer: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SetFormat {
    Parquet,
}

/// An object's size: fixed, or drawn per key between two sizes, uniformly
/// or log-uniformly.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SizeSpec {
    Fixed(u64),
    Uniform(u64, u64),
    LogUniform(u64, u64),
}

impl<'de> Deserialize<'de> for SizeSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<SizeSpec, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Fixed(ByteSize),
            Drawn {
                uniform: Option<[ByteSize; 2]>,
                log_uniform: Option<[ByteSize; 2]>,
            },
        }
        let spec = match Written::deserialize(deserializer)? {
            Written::Fixed(size) => SizeSpec::Fixed(size.0),
            Written::Drawn {
                uniform: Some([min, max]),
                log_uniform: None,
            } => SizeSpec::Uniform(min.0, max.0),
            Written::Drawn {
                uniform: None,
                log_uniform: Some([min, max]),
            } => SizeSpec::LogUniform(min.0, max.0),
            Written::Drawn { .. } => {
                return Err(serde::de::Error::custom(
                    "a size is a size, { uniform = [min, max] } or { log_uniform = [min, max] }",
                ));
            }
        };
        match spec {
            SizeSpec::Fixed(0) => Err(serde::de::Error::custom("sizes start at 1 byte")),
            SizeSpec::Uniform(min, max) | SizeSpec::LogUniform(min, max)
                if min == 0 || min > max =>
            {
                Err(serde::de::Error::custom("a size range runs from 1 byte up"))
            }
            spec => Ok(spec),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub name: String,
    pub target: Target,
    /// How long the step sends requests, counting `warmup`; and the most
    /// it sends across every client host. It ends at whichever comes first.
    #[serde(default, deserialize_with = "optional_duration")]
    pub duration: Option<Duration>,
    pub requests: Option<u64>,
    /// Requests that start within this long of the step's start are left
    /// out of its results.
    #[serde(default, deserialize_with = "duration")]
    pub warmup: Duration,
    /// Connections each client host keeps open, each with one request in
    /// flight.
    #[serde(default = "default_connections")]
    pub connections: usize,
    /// Requests per second each client host starts, each timed from when it
    /// was due, so a stall counts against every request it delays; 0 sends
    /// the next request as soon as a connection is free.
    #[serde(default)]
    pub rate: f64,
    #[serde(default)]
    pub verify: Verify,
    /// How long a request may take before it counts as timed out.
    #[serde(default = "default_timeout", deserialize_with = "duration")]
    pub timeout: Duration,
    /// The driver drops the storage nodes' page caches before the step, so
    /// hits read the drive.
    #[serde(default)]
    pub drop_page_cache: bool,
    #[serde(default)]
    pub reads: Vec<Read>,
    #[serde(default)]
    pub writes: Vec<Write>,
    #[serde(default)]
    pub faults: Vec<Fault>,
}

fn default_connections() -> usize {
    32
}

fn default_timeout() -> Duration {
    Duration::from_secs(60)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    /// The cluster's gateways.
    Cache,
    /// S3 itself, for a baseline.
    S3,
}

/// How much of each body the client checks against the object's bytes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Verify {
    /// Every byte.
    #[default]
    Full,
    /// The first and last 4 KiB of each body, and its length.
    Edges,
    /// Only the length.
    None,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Read {
    pub set: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default)]
    pub keys: Keys,
    /// Reads only the set's first `limit` keys: a smaller working set, or
    /// one hot key.
    pub limit: Option<u64>,
    #[serde(default)]
    pub range: RangeSpec,
    /// Sequential reads take each `window` keys `passes` times before the
    /// next, so the doorkeeper, which stores a block on its second read
    /// among its recent ones, sees both reads of a fill.
    #[serde(default = "default_passes")]
    pub passes: u64,
    #[serde(default = "default_window")]
    pub window: u64,
}

fn default_weight() -> f64 {
    1.0
}

fn default_passes() -> u64 {
    1
}

fn default_window() -> u64 {
    10_000
}

/// Which key each read takes.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Keys {
    #[default]
    Uniform,
    /// Each key once, in order, spread across client hosts, then again.
    Sequential,
    /// Key ranks drawn by Zipf's law with this exponent, the ranks
    /// scattered across the set.
    Zipf(f64),
}

impl<'de> Deserialize<'de> for Keys {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Keys, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Named(String),
            Zipf { zipf: f64 },
        }
        match Written::deserialize(deserializer)? {
            Written::Named(name) if name == "uniform" => Ok(Keys::Uniform),
            Written::Named(name) if name == "sequential" => Ok(Keys::Sequential),
            Written::Zipf { zipf } if zipf > 0.0 => Ok(Keys::Zipf(zipf)),
            _ => Err(serde::de::Error::custom(
                "keys are \"uniform\", \"sequential\" or { zipf = exponent above 0 }",
            )),
        }
    }
}

/// What part of an object a read asks for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RangeSpec {
    #[default]
    Whole,
    /// `size` bytes from an offset drawn uniformly.
    Random(u64),
    /// The last `size` bytes, as a Parquet reader reads its footer.
    Suffix(u64),
}

impl<'de> Deserialize<'de> for RangeSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<RangeSpec, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Named(String),
            Random { size: ByteSize },
            Suffix { suffix: ByteSize },
        }
        match Written::deserialize(deserializer)? {
            Written::Named(name) if name == "whole" => Ok(RangeSpec::Whole),
            Written::Random { size } if size.0 > 0 => Ok(RangeSpec::Random(size.0)),
            Written::Suffix { suffix } if suffix.0 > 0 => Ok(RangeSpec::Suffix(suffix.0)),
            _ => Err(serde::de::Error::custom(
                "a range is \"whole\", { size = bytes } or { suffix = bytes }",
            )),
        }
    }
}

/// `PutObject`s of new keys, each written once.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
    #[serde(default = "default_weight")]
    pub weight: f64,
    pub size: SizeSpec,
}

/// Something the driver does to a storage node partway through a step.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fault {
    /// Seconds after the step starts.
    #[serde(deserialize_with = "duration", serialize_with = "seconds")]
    pub at: Duration,
    pub action: FaultAction,
    /// The node's index among the inventory's storage nodes.
    pub node: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FaultAction {
    /// `SIGKILL`: a crash.
    Kill,
    /// `SIGTERM`: a clean stop, as for a deploy.
    Stop,
    /// Starts a stopped node, or a standby node, which joins the cluster.
    Start,
    /// A clean stop, then a start.
    Restart,
    /// `SIGUSR1`: the node leaves the cluster, hands its blocks over for
    /// the fallback window, and exits.
    Leave,
}

impl Read {
    /// How results name this read's requests, such as `tables: 8 MiB range`.
    pub fn label(&self) -> String {
        let range = match self.range {
            RangeSpec::Whole => "whole".to_string(),
            RangeSpec::Random(size) => format!("{} range", size_name(size)),
            RangeSpec::Suffix(size) => format!("{} suffix", size_name(size)),
        };
        format!("{}: {range}", self.set)
    }
}

impl Plan {
    pub fn parse(text: &str) -> Result<Plan, String> {
        let plan: Plan = toml::from_str(text).map_err(|error| error.to_string())?;
        plan.check()?;
        Ok(plan)
    }

    fn check(&self) -> Result<(), String> {
        let mut names = std::collections::BTreeSet::new();
        for set in &self.dataset.sets {
            if !names.insert(set.name.as_str()) {
                return Err(format!("set {} appears twice", set.name));
            }
            if set.count == 0 {
                return Err(format!("set {} has no objects", set.name));
            }
            if !set
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(format!(
                    "set {}'s name needs only letters, digits, - and _",
                    set.name
                ));
            }
        }
        let prefix = &self.dataset.prefix;
        if prefix.is_empty() || prefix.starts_with('/') || prefix.ends_with('/') {
            return Err("the dataset's prefix is a path without a leading or trailing /".into());
        }
        let mut steps = std::collections::BTreeSet::new();
        for step in &self.steps {
            let name = &step.name;
            if !steps.insert(name.as_str()) {
                return Err(format!("step {name} appears twice"));
            }
            if !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(format!(
                    "step {name}'s name needs only letters, digits, - and _"
                ));
            }
            if step.duration.is_none() && step.requests.is_none() {
                return Err(format!("step {name} needs a duration, requests, or both"));
            }
            if step.reads.is_empty() && step.writes.is_empty() {
                return Err(format!("step {name} sends no requests"));
            }
            if step.connections == 0 || step.rate < 0.0 {
                return Err(format!(
                    "step {name} needs a connection and a rate of at least 0"
                ));
            }
            for read in &step.reads {
                if !names.contains(read.set.as_str()) {
                    return Err(format!(
                        "step {name} reads set {}, which the dataset lacks",
                        read.set
                    ));
                }
                if read.limit == Some(0) {
                    return Err(format!("step {name} limits a read to no keys"));
                }
                if read.passes == 0 || read.window == 0 {
                    return Err(format!("step {name} reads in no passes or windows"));
                }
            }
            let weights = step.reads.iter().map(|read| read.weight);
            let weights = weights.chain(step.writes.iter().map(|write| write.weight));
            if weights
                .clone()
                .any(|weight| weight.is_nan() || weight < 0.0)
                || weights.sum::<f64>() <= 0.0
            {
                return Err(format!(
                    "step {name}'s weights are at least 0 and not all 0"
                ));
            }
        }
        Ok(())
    }

    pub fn step(&self, name: &str) -> Result<&Step, String> {
        self.steps
            .iter()
            .find(|step| step.name == name)
            .ok_or_else(|| format!("the plan has no step {name}"))
    }
}

impl Step {
    /// The requests `host` of `hosts` sends: its share of `requests`.
    pub fn host_requests(&self, host: usize, hosts: usize) -> Option<u64> {
        self.requests.map(|total| {
            let (share, extra) = (total / hosts as u64, total % hosts as u64);
            share + u64::from((host as u64) < extra)
        })
    }
}

/// A size in bytes, written as a number or with a unit: `4KiB`, `1MiB`,
/// `2GiB`, `1TiB`, `500KB` or `1GB`.
#[derive(Clone, Copy, Debug)]
struct ByteSize(u64);

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Number(u64),
            Text(String),
        }
        match Written::deserialize(deserializer)? {
            Written::Number(bytes) => Ok(ByteSize(bytes)),
            Written::Text(text) => parse_bytes(&text)
                .map(ByteSize)
                .map_err(serde::de::Error::custom),
        }
    }
}

fn bytes<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    ByteSize::deserialize(deserializer).map(|size| size.0)
}

pub fn parse_bytes(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '_')
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: u64 = number
        .replace('_', "")
        .parse()
        .map_err(|_| format!("{text} is no size"))?;
    let scale: u64 = match unit.trim() {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        _ => return Err(format!("{text} has an unknown unit")),
    };
    number
        .checked_mul(scale)
        .ok_or_else(|| format!("{text} is too large"))
}

/// A size as the report names it, such as `64 KiB`.
pub fn size_name(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ];
    for (unit, scale) in UNITS {
        if bytes >= scale && bytes.is_multiple_of(scale) {
            return format!("{} {unit}", bytes / scale);
        }
    }
    format!("{bytes} B")
}

/// A duration, written as seconds or with a unit: `500ms`, `30s`, `5m`,
/// `2h`.
fn duration<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Written {
        Seconds(u64),
        Text(String),
    }
    match Written::deserialize(deserializer)? {
        Written::Seconds(seconds) => Ok(Duration::from_secs(seconds)),
        Written::Text(text) => parse_duration(&text).map_err(serde::de::Error::custom),
    }
}

fn optional_duration<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    duration(deserializer).map(Some)
}

fn seconds<S: serde::Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64(duration.as_secs_f64())
}

pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("{text} is no duration"))?;
    let seconds = match unit.trim() {
        "ms" => number / 1_000.0,
        "" | "s" => number,
        "m" => number * 60.0,
        "h" => number * 3_600.0,
        _ => return Err(format!("{text} has an unknown unit")),
    };
    Duration::try_from_secs_f64(seconds).map_err(|_| format!("{text} is no duration"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAN: &str = r#"
        [dataset]
        prefix = "load/v1"
        seed = 7

        [[dataset.sets]]
        name = "small"
        count = 1_000
        size = { log_uniform = ["4KiB", "256KiB"] }

        [[dataset.sets]]
        name = "tables"
        count = 10
        size = "64MiB"
        format = "parquet"
        footer = "1MiB"

        [[steps]]
        name = "hits"
        target = "cache"
        duration = "5m"
        warmup = "30s"
        connections = 64
        rate = 500
        verify = "edges"
        drop_page_cache = true

        [[steps.reads]]
        set = "small"
        keys = { zipf = 1.1 }
        weight = 3

        [[steps.reads]]
        set = "tables"
        range = { suffix = "1MiB" }
        limit = 5

        [[steps.reads]]
        set = "tables"
        range = { size = "8MiB" }
        keys = "sequential"
        passes = 2
        window = 3

        [[steps.writes]]
        size = 1048576

        [[steps.faults]]
        at = "2m"
        action = "kill"
        node = 1

        [[steps]]
        name = "fill"
        target = "s3"
        requests = 1_000
        [[steps.reads]]
        set = "small"
        keys = "sequential"
    "#;

    #[test]
    fn a_plan_reads_its_sizes_durations_and_mixes() {
        let plan = Plan::parse(PLAN).unwrap();
        assert_eq!(
            plan.dataset.sets[0].size,
            SizeSpec::LogUniform(4 << 10, 256 << 10)
        );
        assert_eq!(plan.dataset.sets[1].format, Some(SetFormat::Parquet));
        assert_eq!(plan.dataset.sets[1].footer, 1 << 20);
        let hits = plan.step("hits").unwrap();
        assert_eq!(hits.duration, Some(Duration::from_secs(300)));
        assert_eq!(hits.warmup, Duration::from_secs(30));
        assert_eq!(hits.reads[0].keys, Keys::Zipf(1.1));
        assert_eq!(hits.reads[1].range, RangeSpec::Suffix(1 << 20));
        assert_eq!(hits.reads[2].range, RangeSpec::Random(8 << 20));
        assert_eq!(hits.reads[2].keys, Keys::Sequential);
        assert_eq!((hits.reads[2].passes, hits.reads[2].window), (2, 3));
        assert_eq!((hits.reads[0].passes, hits.reads[0].window), (1, 10_000));
        assert_eq!(hits.writes[0].size, SizeSpec::Fixed(1 << 20));
        assert_eq!(hits.faults[0].at, Duration::from_secs(120));
        assert_eq!(hits.faults[0].action, FaultAction::Kill);
        let fill = plan.step("fill").unwrap();
        assert_eq!(
            (fill.target, fill.connections, fill.verify),
            (Target::S3, 32, Verify::Full)
        );
        assert_eq!(fill.host_requests(0, 3), Some(334));
        assert_eq!(fill.host_requests(2, 3), Some(333));
    }

    #[test]
    fn a_plan_that_names_a_missing_set_is_refused() {
        let plan = PLAN.replace(
            "set = \"small\"\n        keys = \"sequential\"",
            "set = \"large\"",
        );
        assert!(Plan::parse(&plan).unwrap_err().contains("large"));
    }

    #[test]
    fn sizes_and_durations_take_units() {
        assert_eq!(parse_bytes("64KiB"), Ok(65_536));
        assert_eq!(parse_bytes("1_000"), Ok(1_000));
        assert_eq!(parse_bytes("2GB"), Ok(2_000_000_000));
        assert!(parse_bytes("2 parsecs").is_err());
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5h"), Ok(Duration::from_secs(5_400)));
        assert_eq!(size_name(64 << 10), "64 KiB");
        assert_eq!(size_name(1_000), "1000 B");
    }
}
