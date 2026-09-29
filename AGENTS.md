# s3-accelerator

`spec.md` is the design contract. Read it before changing behavior, and when code and spec disagree, fix one of them in the same change. `PLAN.md` is the build plan: work in its order and update its ledgers with evidence.

## Core rules

- `crates/core` does no I/O, reads no clocks and starts no threads. It takes requests, responses and the current time as inputs and returns actions. The server carries them out over sockets and disks; the simulator carries them out over its models.
- The core handles block locations and response heads. The server moves the bytes with `sendfile` and `splice`.
- One thread owns each node's core state. Worker threads only move bytes.
- Randomness reaches the core as a seeded RNG passed in.
- `clippy.toml` in `crates/core` and `crates/sim` forbids unordered collections, clocks and threads.

## Testing

- Every behavior the spec names needs a test and a planted bug in `scripts/mutants` that the test catches. A behavior the core decides needs a simulator property or a scripted simulator scenario. A behavior only the server carries out, such as auth, origins or metrics, needs a server test or a unit test in `crates/server`.
- The simulator checks responses against its model of S3, never against the core's own state.
- A seed replays its run exactly. The simulator draws from its own PRNG; give each new source of randomness its own `Prng::stream`, so it leaves existing draws unchanged.
- A bug the simulator finds becomes a regression test in `crates/sim/tests` that runs its seed. The commit message records the seed and the commit that failed.
- Every S3 behavior the cache serves needs a conformance test in `tests/`. The suite passes against s3proxy and through the accelerator; CI runs both.
- Verify zero-copy and kTLS from outside the server (`strace`, `/proc/net/tls_stat`, socket state). The server's own counters are not evidence.

## Commands

```console
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/s3proxy start && sudo modprobe tls && cargo test --workspace -- --include-ignored
cargo run -p s3-accelerator -- config/local.toml &   # then CONFORMANCE_ENDPOINT=http://127.0.0.1:9000
scripts/cluster start 3 [--tls] [--metadata] && scripts/cluster stop   # the same, as separate processes
cargo run --release -p s3-accelerator-sim [-- SEED] [--seeds N]
scripts/mutants
```
