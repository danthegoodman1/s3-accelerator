# s3-accelerator

A distributed NVMe read cache in front of S3. [spec.md](spec.md) describes the design.

The core routes each read to its object's home node, which fetches it from S3. The server binary has no modes yet.

## Layout

- `crates/core`: gateway and storage-node logic as deterministic state machines that do no I/O.
- `crates/server`: the `s3-accelerator` binary, which runs the core over sockets and disks.
- `crates/sim`: a deterministic simulator that runs gateways, storage nodes, clients and a model of S3 on one thread.
- `tests`: the S3 conformance suite, which runs against s3proxy and through the accelerator.

## Testing

```console
cargo test --workspace                        # unit tests and 200 simulator seeds
scripts/s3proxy start                         # in-memory s3proxy in Docker, on 127.0.0.1:8080
cargo test --workspace -- --include-ignored   # adds the conformance suite
scripts/s3proxy stop
```

The conformance suite reads `CONFORMANCE_ENDPOINT`, `CONFORMANCE_ACCESS_KEY_ID` and `CONFORMANCE_SECRET_ACCESS_KEY`, which default to the local s3proxy.

### Simulator

The seed determines the whole run: the cluster's size, the workload and every network delay. Writers overwrite and delete objects in the model of S3 while clients read, and every response must equal what S3 would have returned at some tick while the request was in flight.

Every run prints its seed. Pass it back to replay the run exactly. A git commit hash also works as a seed, and CI uses the commit being tested.

```console
cargo run --release -p s3-accelerator-sim         # a random seed
cargo run --release -p s3-accelerator-sim -- 42   # replay seed 42
```
