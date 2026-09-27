//! Log lines in logfmt on standard error: `ts`, `level` and `msg`, then
//! fields. `[log] level` sets the least severe level written.

use crate::sigv4::civil_from_days;
use serde::Deserialize;
use std::fmt::{Display, Write as _};
use std::io::Write as _;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
        }
    }
}

/// The least severe level written, for the whole process.
static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

pub fn set_level(level: Level) {
    LEVEL.store(level as u8, Ordering::Relaxed);
}

pub fn enabled(level: Level) -> bool {
    level as u8 <= LEVEL.load(Ordering::Relaxed)
}

/// Writes one line, in one write, so lines from different threads never
/// interleave.
pub fn write(level: Level, msg: &str, fields: &[(&str, &dyn Display)]) {
    let line = format_line(SystemTime::now(), level, msg, fields);
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

fn format_line(
    now: SystemTime,
    level: Level,
    msg: &str,
    fields: &[(&str, &dyn Display)],
) -> String {
    let mut line = String::with_capacity(128);
    line.push_str("ts=");
    timestamp(&mut line, now);
    line.push_str(" level=");
    line.push_str(level.name());
    field(&mut line, "msg", &msg);
    for (key, value) in fields {
        field(&mut line, key, value);
    }
    line.push('\n');
    line
}

/// `key=value`, quoted when the value holds a space, `=`, `"` or a control
/// character, or is empty.
fn field(line: &mut String, key: &str, value: &dyn Display) {
    let value = value.to_string();
    let _ = write!(line, " {key}=");
    let plain = !value.is_empty()
        && !value
            .chars()
            .any(|char| char == ' ' || char == '=' || char == '"' || char.is_control());
    if plain {
        line.push_str(&value);
        return;
    }
    line.push('"');
    for char in value.chars() {
        match char {
            '"' => line.push_str("\\\""),
            '\\' => line.push_str("\\\\"),
            '\n' => line.push_str("\\n"),
            char if char.is_control() => {
                let _ = write!(line, "\\u{{{:x}}}", char as u32);
            }
            char => line.push(char),
        }
    }
    line.push('"');
}

/// RFC 3339 in UTC, to the millisecond.
fn timestamp(line: &mut String, now: SystemTime) {
    let since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = since.as_secs() as i64;
    let (days, of_day) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    let _ = write!(
        line,
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        of_day / 3_600,
        of_day / 60 % 60,
        of_day % 60,
        since.subsec_millis()
    );
}

/// Logs `msg` at `level` with fields: `log!(Warn, "reading from node",
/// node = 3, error = error)`.
#[macro_export]
macro_rules! log {
    ($level:ident, $msg:expr $(, $key:ident = $value:expr)* $(,)?) => {
        if $crate::log::enabled($crate::log::Level::$level) {
            $crate::log::write(
                $crate::log::Level::$level,
                &$msg,
                &[$((stringify!($key), &$value as &dyn std::fmt::Display)),*],
            );
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_line_is_logfmt() {
        let now = UNIX_EPOCH + Duration::from_millis(1_790_512_496_789);
        let error = "connection refused";
        let fields: [(&str, &dyn Display); 3] = [("node", &3), ("error", &error), ("empty", &"")];
        let line = format_line(now, Level::Warn, "reading from node", &fields);
        assert_eq!(
            line,
            "ts=2026-09-27T12:34:56.789Z level=warn msg=\"reading from node\" node=3 \
             error=\"connection refused\" empty=\"\"\n"
        );
        let quoted = format_line(now, Level::Info, "a \"quote\"\nand a line", &[]);
        assert!(quoted.ends_with("msg=\"a \\\"quote\\\"\\nand a line\"\n"));
    }

    #[test]
    fn levels_below_the_setting_are_skipped() {
        assert!(enabled(Level::Warn));
        assert!(!enabled(Level::Debug));
    }
}
