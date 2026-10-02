//! Load-tests a cluster, or S3 itself for a baseline, from many client
//! hosts at once. `loadtest/loadtest` drives it on each host; see
//! `loadtest/README.md`.
//!
//! ```console
//! s3-accelerator-load check PLAN
//! s3-accelerator-load steps PLAN
//! s3-accelerator-load seed PLAN --bucket B --endpoint URL [--host I --hosts N] [--connections C] [--resume]
//! s3-accelerator-load run PLAN STEP --bucket B --endpoint URL... [--host I --hosts N] [--start-at UNIX_MS] [--run NAME] --out FILE
//! s3-accelerator-load report RUN_DIR
//! ```
//!
//! `seed` and `run` sign requests with `AWS_ACCESS_KEY_ID` and
//! `AWS_SECRET_ACCESS_KEY` for `AWS_REGION` (us-east-1 by default), and
//! trust the system's certificate authorities, or only `--ca FILE`'s.

mod client;
mod dataset;
mod histogram;
mod plan;
mod report;
mod run;
mod seed;

use client::Endpoint;
use plan::{Plan, SetFormat};
use serde::Serialize;
use std::process::ExitCode;
use std::sync::Arc;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match command(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("s3-accelerator-load: {error}");
            ExitCode::FAILURE
        }
    }
}

fn command(args: &[String]) -> Result<(), String> {
    let (command, rest) = args.split_first().ok_or(USAGE)?;
    match command.as_str() {
        "check" => check(&load(rest.first().ok_or(USAGE)?)?),
        "steps" => steps(&load(rest.first().ok_or(USAGE)?)?),
        "seed" => {
            let options = Options::parse(rest, 1)?;
            let plan = Arc::new(load(&options.positional[0])?);
            let endpoint = options
                .endpoints
                .first()
                .ok_or("seed needs --endpoint")?
                .clone();
            let seeding = seed::Seeding {
                plan,
                bucket: options.bucket()?,
                tls: options.tls()?,
                endpoint,
                signer: options.signer()?,
                host: options.host,
                hosts: options.hosts,
                connections: options.connections.unwrap_or(64),
                resume: options.resume,
            };
            runtime().block_on(seed::seed(seeding))
        }
        "run" => {
            let options = Options::parse(rest, 2)?;
            let plan = Arc::new(load(&options.positional[0])?);
            let step = plan.step(&options.positional[1])?.clone();
            if options.endpoints.is_empty() {
                return Err("run needs an --endpoint".into());
            }
            let out = options.out.clone().ok_or("run needs --out")?;
            let setup = run::Setup {
                bucket: options.bucket()?,
                tls: options.tls()?,
                signer: options.signer()?,
                endpoints: options.endpoints.clone(),
                host: options.host,
                hosts: options.hosts,
                start_at: options.start_at,
                run: options.run.clone().unwrap_or_else(|| "run".into()),
                plan,
                step,
            };
            let result = runtime().block_on(run::run(setup));
            let text = serde_json::to_string(&result).map_err(|error| error.to_string())?;
            if let Some(parent) = std::path::Path::new(&out).parent() {
                std::fs::create_dir_all(parent).map_err(|error| format!("{out}: {error}"))?;
            }
            std::fs::write(&out, text).map_err(|error| format!("{out}: {error}"))
        }
        "report" => {
            let dir = rest.first().ok_or(USAGE)?;
            print!("{}", report::report(std::path::Path::new(dir))?);
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

const USAGE: &str =
    "usage: s3-accelerator-load check|steps|seed|run|report ...; see crates/load/src/main.rs";

fn load(path: &str) -> Result<Plan, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
    Plan::parse(&text).map_err(|error| format!("{path}: {error}"))
}

fn runtime() -> tokio::runtime::Runtime {
    raise_open_file_limit();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// Raises the soft limit on open files to the hard limit: each connection
/// holds a descriptor, and a login's soft limit is often 1,024.
fn raise_open_file_limit() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let Rlimit { current, maximum } = getrlimit(Resource::Nofile);
    if current != maximum {
        let raised = Rlimit {
            current: maximum,
            maximum,
        };
        if let Err(error) = setrlimit(Resource::Nofile, raised) {
            eprintln!("raising the open file limit failed: {error}");
        }
    }
}

/// Prints the dataset's size and each step, so an operator sees what a
/// plan costs before seeding it.
fn check(plan: &Plan) -> Result<(), String> {
    let dataset = &plan.dataset;
    let mut total = 0;
    for set in &dataset.sets {
        let bytes: u64 = (0..set.count)
            .map(|index| dataset.object(set, index).size)
            .sum();
        total += bytes;
        let format = match set.format {
            Some(SetFormat::Parquet) => ", Parquet",
            None => "",
        };
        println!(
            "set {}: {} objects, {:.2} GiB{format}",
            set.name,
            set.count,
            bytes as f64 / (1u64 << 30) as f64
        );
    }
    println!(
        "dataset: {:.2} GiB under {}/",
        total as f64 / (1u64 << 30) as f64,
        dataset.prefix
    );
    for step in &plan.steps {
        let length = match (step.duration, step.requests) {
            (Some(duration), Some(requests)) => {
                format!("{} s or {requests} requests", duration.as_secs())
            }
            (Some(duration), None) => format!("{} s", duration.as_secs()),
            (None, Some(requests)) => format!("{requests} requests"),
            (None, None) => unreachable!("a checked step has a length"),
        };
        let mut mix: Vec<String> = step
            .reads
            .iter()
            .map(|read| format!("reads {}", read.label()))
            .collect();
        if !step.writes.is_empty() {
            mix.push("writes".into());
        }
        println!(
            "step {}: {:?}, {length}, {} connections per host; {}",
            step.name,
            step.target,
            step.connections,
            mix.join(", ")
        );
    }
    Ok(())
}

/// What the driver needs of each step, as JSON.
fn steps(plan: &Plan) -> Result<(), String> {
    #[derive(Serialize)]
    struct Summary<'a> {
        name: &'a str,
        target: plan::Target,
        seconds: Option<f64>,
        warmup: f64,
        requests: Option<u64>,
        drop_page_cache: bool,
        faults: &'a [plan::Fault],
    }
    let steps: Vec<Summary> = plan
        .steps
        .iter()
        .map(|step| Summary {
            name: &step.name,
            target: step.target,
            seconds: step.duration.map(|duration| duration.as_secs_f64()),
            warmup: step.warmup.as_secs_f64(),
            requests: step.requests,
            drop_page_cache: step.drop_page_cache,
            faults: &step.faults,
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&steps).map_err(|error| error.to_string())?
    );
    Ok(())
}

struct Options {
    positional: Vec<String>,
    bucket: Option<String>,
    endpoints: Vec<Arc<Endpoint>>,
    ca: Option<String>,
    host: usize,
    hosts: usize,
    connections: Option<usize>,
    start_at: Option<u64>,
    run: Option<String>,
    out: Option<String>,
    resume: bool,
}

impl Options {
    fn parse(args: &[String], positional: usize) -> Result<Options, String> {
        let mut options = Options {
            positional: Vec::new(),
            bucket: None,
            endpoints: Vec::new(),
            ca: None,
            host: 0,
            hosts: 1,
            connections: None,
            start_at: None,
            run: None,
            out: None,
            resume: false,
        };
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            let mut value = || args.next().cloned().ok_or(format!("{arg} needs a value"));
            let number = |text: String| {
                text.parse::<u64>()
                    .map_err(|_| format!("{arg} takes a number"))
            };
            match arg.as_str() {
                "--bucket" => options.bucket = Some(value()?),
                "--endpoint" => options
                    .endpoints
                    .push(Arc::new(Endpoint::parse(&value()?)?)),
                "--ca" => options.ca = Some(value()?),
                "--host" => options.host = number(value()?)? as usize,
                "--hosts" => options.hosts = number(value()?)? as usize,
                "--connections" => options.connections = Some(number(value()?)? as usize),
                "--start-at" => options.start_at = Some(number(value()?)?),
                "--run" => options.run = Some(value()?),
                "--out" => options.out = Some(value()?),
                "--resume" => options.resume = true,
                flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
                _ => options.positional.push(arg.clone()),
            }
        }
        if options.positional.len() != positional {
            return Err(USAGE.into());
        }
        if options.hosts == 0 || options.host >= options.hosts {
            return Err("--host is below --hosts".into());
        }
        Ok(options)
    }

    fn bucket(&self) -> Result<String, String> {
        self.bucket.clone().ok_or_else(|| "needs --bucket".into())
    }

    fn tls(&self) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
        if !self.endpoints.iter().any(|endpoint| endpoint.tls) {
            return Ok(None);
        }
        client::tls_config(self.ca.as_deref())
            .map(Some)
            .map_err(|error| format!("TLS: {error}"))
    }

    fn signer(&self) -> Result<Arc<s3_accelerator::sigv4::Signer>, String> {
        let var = |name: &str| std::env::var(name).map_err(|_| format!("{name} is unset"));
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        Ok(Arc::new(client::signer(
            &var("AWS_ACCESS_KEY_ID")?,
            &var("AWS_SECRET_ACCESS_KEY")?,
            &region,
        )))
    }
}
