//! Reading `strace` output: system calls, their file descriptors and the
//! bytes they carried, for evidence from outside the server.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64;

/// The calls that write.
pub const WRITES: &str = "write,writev,sendto,sendmsg,pwrite64,pwritev,pwritev2";

/// No call that writes to a socket or pipe carries 32 bytes in a row of
/// any body, and together they write far less than the bodies. Writes to
/// files are the node storing blocks, which a slow disk may still be doing
/// as the hits begin.
pub fn check_writes_carry_no_body(
    process: &str,
    calls: &[Call],
    windows: &HashSet<u64>,
    total: i64,
) -> i64 {
    let to_file = |call: &Call| {
        call.fds()
            .first()
            .and_then(|fd| fd.split_once('<'))
            .is_some_and(|(_, name)| name.starts_with('/'))
    };
    let writes: Vec<&Call> = calls
        .iter()
        .filter(|call| WRITES.split(',').any(|name| name == call.name))
        .filter(|call| !to_file(call))
        .collect();
    for call in &writes {
        let data = call.data();
        let carried = data
            .windows(32)
            .any(|window| windows.contains(&xxh3_64(window)));
        assert!(
            !carried,
            "the {process} wrote body bytes with {}",
            call.name
        );
    }
    let written: i64 = writes.iter().map(|call| call.result.max(0)).sum();
    assert!(written < total / 16, "the {process} wrote {written} bytes");
    written
}

/// Hashes of every 32-byte run in the bodies.
pub fn windows(bodies: &[Vec<u8>]) -> HashSet<u64> {
    bodies
        .iter()
        .flat_map(|body| body.windows(32).map(xxh3_64))
        .collect()
}

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// `text` with each `\xHH` escape replaced by its byte.
pub fn unescape(text: &str) -> String {
    let mut parts = text.split("\\x");
    let mut bytes = parts.next().unwrap_or_default().as_bytes().to_vec();
    for part in parts {
        bytes.push(u8::from_str_radix(&part[..2], 16).expect("strace -xx escapes"));
        bytes.extend_from_slice(&part.as_bytes()[2..]);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// One completed system call from a trace.
pub struct Call {
    pub name: String,
    /// When it began and returned, in seconds since the epoch.
    pub start: f64,
    pub end: f64,
    /// Its arguments as `strace` printed them.
    pub args: String,
    pub result: i64,
}

impl Call {
    /// The descriptor arguments, with the names `strace -yy` gives them,
    /// such as `12</data/slabs>` or `7<TCP:[...]>`.
    pub fn fds(&self) -> Vec<String> {
        self.args
            .split(", ")
            .filter(|arg| {
                arg.split_once('<')
                    .is_some_and(|(fd, _)| fd.parse::<u32>().is_ok())
            })
            .map(unescape)
            .collect()
    }

    /// The bytes of every string argument.
    pub fn data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        for (index, part) in self.args.split('"').enumerate() {
            if index % 2 == 1 {
                for hex in part.split("\\x").skip(1) {
                    data.push(u8::from_str_radix(&hex[..2], 16).expect("strace -xx escapes"));
                }
            }
        }
        data
    }

    /// A `pwrite64`'s offset and byte count: its last two arguments.
    pub fn offset_and_len(&self) -> (u64, u64) {
        let mut args = self.args.rsplit(", ");
        let offset = args
            .next()
            .and_then(|arg| arg.parse().ok())
            .expect("an offset");
        let len = args
            .next()
            .and_then(|arg| arg.parse().ok())
            .expect("a count");
        (offset, len)
    }
}

/// The calls in `path` that returned between `from` and `until`. Lines
/// look like `PID TIME name(args) = result`, and a call another thread
/// interrupted spans an `<unfinished ...>` line and a `<... name resumed>`
/// line.
pub fn read_trace(path: &Path, from: f64, until: f64) -> Vec<Call> {
    let trace = std::fs::read_to_string(path).unwrap();
    let mut unfinished: BTreeMap<&str, (&str, f64, String)> = BTreeMap::new();
    let mut calls = Vec::new();
    for line in trace.lines() {
        // Some versions pad the PID column.
        let Some((pid, rest)) = line.trim_start().split_once(' ') else {
            continue;
        };
        let Some((time, rest)) = rest.trim_start().split_once(' ') else {
            continue;
        };
        let Ok(time) = time.parse::<f64>() else {
            continue;
        };
        let (name, start, text) = if let Some(rest) = rest.strip_prefix("<... ") {
            let Some((_, rest)) = rest.split_once(" resumed>") else {
                continue;
            };
            let Some((name, start, args)) = unfinished.remove(pid) else {
                continue;
            };
            (name, start, format!("{args}{rest}"))
        } else if let Some(rest) = rest.strip_suffix(" <unfinished ...>") {
            if let Some((name, args)) = rest.split_once('(') {
                unfinished.insert(pid, (name, time, args.to_string()));
            }
            continue;
        } else {
            let Some((name, args)) = rest.split_once('(') else {
                continue;
            };
            (name, time, args.to_string())
        };
        let Some((args, result)) = text.rsplit_once(") = ") else {
            continue;
        };
        let result = result
            .split_whitespace()
            .next()
            .and_then(|result| result.parse().ok())
            .unwrap_or(-1);
        if time >= from && time <= until {
            calls.push(Call {
                name: name.to_string(),
                start,
                end: time,
                args: args.to_string(),
                result,
            });
        }
    }
    calls
}
