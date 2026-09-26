//! The storage node and gateway binary.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!(
        "s3-accelerator {}: the server has no modes yet",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::FAILURE
}
