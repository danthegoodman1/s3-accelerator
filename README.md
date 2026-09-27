# s3-accelerator

A distributed NVMe read cache in front of S3. [spec.md](spec.md) describes the design.

The core serves reads from each object's home node, which keeps the object's metadata and caches its blocks under the block store policy. The server runs gateways and storage nodes, separately or together in one process: gateways serve S3 clients over plaintext HTTP/1.1 and send reads to nodes over the cluster protocol, and nodes keep blocks and immutable-bucket metadata on disk. Every operation other than `GetObject` and `HeadObject` passes through to S3. [PLAN.md](PLAN.md) tracks the remaining work.

## Layout

- `crates/core`: gateway and storage-node logic as deterministic state machines that do no I/O.
- `crates/server`: the `s3-accelerator` binary, which runs the core over sockets and disks: HTTP/1.1, SigV4 validation and grants for clients, the cluster protocol between gateways and nodes, the node's slab file, slot table and metadata file, and signed requests to S3. Bodies stream: nodes send stored blocks with `sendfile`, and gateways relay them with `splice`.
- `crates/sim`: a deterministic simulator that runs gateways, storage nodes, clients and a model of S3 on one thread.
- `tests`: the S3 conformance suite, which runs against s3proxy and through the accelerator.

## Testing

```console
cargo test --workspace                        # unit, server and simulator tests
scripts/s3proxy start                         # in-memory s3proxy in Docker, on 127.0.0.1:8080
cargo test --workspace -- --include-ignored   # adds the conformance suite
scripts/s3proxy stop
```

The server's tests need Linux and `strace`: `crates/server/tests/zero_copy.rs` traces a node and a gateway to show that hits leave through `sendfile` and `splice`, and that the node syncs each block before its record and each cleared record before the slot is rewritten. The data directory must be on a disk-backed filesystem, and the tests keep theirs under `target/`.

The conformance suite reads `CONFORMANCE_ENDPOINT`, `CONFORMANCE_ACCESS_KEY_ID` and `CONFORMANCE_SECRET_ACCESS_KEY`, which default to the local s3proxy. To run it through the accelerator, which `config/local.toml` points at that s3proxy with a gateway and a node in one process:

```console
cargo run -p s3-accelerator -- config/local.toml &
CONFORMANCE_ENDPOINT=http://127.0.0.1:9000 cargo test -p s3-accelerator-conformance -- --ignored
```

`scripts/cluster start [NODES]` runs a gateway and nodes as separate processes in front of the same s3proxy, with the gateway on the same port; `scripts/cluster stop` shuts them down.

### Simulator

The seed determines the whole run: the cluster's size, block and slot sizes, disk capacity, admission policy, the workload, and every network, disk and send delay. Writers create, overwrite and delete objects in the model of S3 while clients read. Every response must equal what S3 would have returned for a state its key held between the request's issue, less the bucket's staleness bound, and its answer. Every stored block must hold the bytes of the version it is keyed by.

Every run prints its seed. Pass it back to replay the run exactly. A git commit hash also works as a seed, and CI uses the commit being tested.

```console
cargo run --release -p s3-accelerator-sim                      # a random seed
cargo run --release -p s3-accelerator-sim -- 42                # replay seed 42
cargo run --release -p s3-accelerator-sim -- 0 --seeds 10000   # seeds 0 to 9,999 on every core
```

`scripts/mutants` plants known bugs, one at a time, in a scratch copy and reports whether the scenarios in `crates/sim/tests/scenarios.rs` or a seed sweep catch each one.
