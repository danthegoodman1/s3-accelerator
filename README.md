# s3-accelerator

A distributed NVMe read cache in front of S3. [spec.md](spec.md) describes the design.

The core serves reads from each object's home node, which keeps the object's metadata and caches its blocks under the block store policy. The server runs gateways and storage nodes, separately or together in one process: gateways serve S3 clients over plaintext HTTP/1.1 and send reads to nodes over the cluster protocol, and nodes keep blocks and immutable-bucket metadata on disk. Nodes find each other by SWIM gossip, and after the cluster grows or shrinks, new owners read what they took over from the nodes that held it. Every operation other than `GetObject` and `HeadObject` passes through a storage node to S3: an object's through its home, which learns of each write before the client does. Only nodes hold S3 credentials, so a gateway's config names no origins. [PLAN.md](PLAN.md) tracks the remaining work.

## Running a cluster

A config's `[cluster]` table names the storage nodes a process starts with, their addresses and weights, and the secret they share; `[cluster.membership]` sets gossip's timings. Nodes gossip over UDP on their cluster addresses, so a node that no other config names joins through the ones its own config names, and gateways learn its address from the ring. To grow the cluster, start a node whose config names at least one running node. To shrink it, send a node `SIGUSR1`: it leaves every ring at once, serves its blocks to their new owners for `cache.fallback_window_ms`, and then exits. `SIGTERM` stops a node for a restart; back within `cluster.membership.down_grace_ms`, it keeps its place in the ring.

Each bucket has an origin: an S3-compatible endpoint, its region and an access key. `[origin]` names every bucket's, and `[origins.<bucket>]` one bucket's:

```toml
[origin]
endpoint = "https://s3.us-east-1.amazonaws.com"
region = "us-east-1"
access_key_id = "AKIA..."
secret_access_key = "..."

[origins.assets]
endpoint = "https://0123456789abcdef.r2.cloudflarestorage.com"
region = "auto"
access_key_id = "..."
secret_access_key = "..."
```

Or nodes look up each bucket's origin in a metadata service, which answers `GET /buckets/<bucket>` with the origin and how long to keep it, and pushes changes to each node's admin listener:

```toml
[metadata]
url = "https://metadata.example.com"
token = "..."
```

`s3-accelerator-metadata CONFIG` runs a reference service that serves origins from a TOML file and sends nodes the buckets whose origins change when it reloads the file on `SIGHUP`; `scripts/cluster start --metadata` runs one. [spec.md](spec.md) describes the service's API.

To have S3 tell the cache of changes made elsewhere, send the bucket's event notifications to an SQS queue, directly or through SNS, and name the queue in each node's `[events]` table:

```toml
[events]
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/bucket-events"
visibility_timeout_s = 30
```

Nodes poll the queue with `[origin]`'s credentials, or with the `access_key_id` and `secret_access_key` the table names. Set the bucket's `ttl_ms` long, such as an hour: events keep its metadata fresh, and the TTL covers an event that goes missing.

To remove an object from the cache, as a retention rule may require after it is deleted, send `POST /bucket/key?x-accel-purge` with a credential whose grants cover the key. Every node drops and erases the object's blocks; a node that is down drops them once it is back.

To watch a process, name an address for its admin listener, which serves Prometheus metrics at `/metrics`, `/healthz`, and `/readyz` for load balancers, and takes a metadata service's invalidations. Only invalidations carry credentials, so keep it on a private address:

```toml
[admin]
listen = "10.0.0.1:9090"
```

## Layout

- `crates/core`: gateway and storage-node logic as deterministic state machines that do no I/O.
- `crates/server`: the `s3-accelerator` binary, which runs the core over sockets and disks: HTTP/1.1, SigV4 validation and grants for clients, the cluster protocol between gateways and nodes, the node's slab file, slot table and metadata file, and signed requests to each bucket's origin. Bodies stream: nodes send stored blocks with `sendfile`, and gateways relay them with `splice`. It also builds `s3-accelerator-metadata`, the reference metadata service.
- `crates/sim`: a deterministic simulator that runs gateways, storage nodes, clients and a model of S3 on one thread.
- `tests`: the S3 conformance suite, which runs against s3proxy and through the accelerator.

## Testing

```console
cargo test --workspace                        # unit, server and simulator tests
scripts/s3proxy start                         # in-memory s3proxy in Docker, on localhost:8080
sudo modprobe tls                             # kernel TLS, for the kTLS tests
cargo test --workspace -- --include-ignored   # adds the conformance suite and the kTLS tests
scripts/s3proxy stop
```

The server's tests need Linux and `strace`: `crates/server/tests/zero_copy.rs` traces a node and a gateway to show that hits leave through `sendfile` and `splice`, and that the node syncs each block before its record and each cleared record before the slot is rewritten. The data directory must be on a disk-backed filesystem, and the tests keep theirs under `target/`.

`crates/server/tests/tls.rs` checks kernel TLS from outside the server, on clients' connections to the gateway and the gateway's mutual-TLS connections to the node: `ss` shows the `tls` ULP on each socket (it runs under `sudo -n`, since only CAP_NET_ADMIN sees a socket's ULP), `/proc/net/tls_stat` counts the kernel's sessions, and `strace` shows hits leaving the node through `sendfile` and the gateway through `splice` into TLS sockets, with no write carrying their bytes. The same run on userspace TLS must show none of this.

Kernel TLS needs Linux 7.0 or later (see Kernel in `spec.md`). Linux 6.17 stalls kernel TLS links over loopback, so CI's test job runs on Ubuntu 26.04.

The conformance suite reads `CONFORMANCE_ENDPOINT`, `CONFORMANCE_ACCESS_KEY_ID` and `CONFORMANCE_SECRET_ACCESS_KEY`, which default to the local s3proxy. To run it through the accelerator, which `config/local.toml` points at that s3proxy with a gateway and a node in one process:

```console
cargo run -p s3-accelerator -- config/local.toml &
CONFORMANCE_ENDPOINT=http://127.0.0.1:9000 cargo test -p s3-accelerator-conformance -- --ignored
```

`scripts/cluster start [NODES]` runs a gateway and nodes as separate processes in front of the same s3proxy, with the gateway on the same port; `scripts/cluster stop` shuts them down. With `--metadata`, nodes look up every bucket's origin in the reference metadata service. With `--tls`, the gateway serves HTTPS with a self-signed certificate, and every process reaches nodes over mutual TLS:

```console
scripts/cluster start 3 --tls
CONFORMANCE_ENDPOINT=https://127.0.0.1:9000 SSL_CERT_FILE=$PWD/target/cluster/tls/cert.pem \
  cargo test -p s3-accelerator-conformance -- --ignored
```

### Benchmarks

`crates/bench` runs a node and a gateway, each its own process, on this machine's disk, in front of an in-process stand-in for S3 that answers after a fixed delay and never limits them. It prints a Markdown report of hit and fill throughput, time to first byte, scan and reread workloads against a cache smaller than the data, a shift in object sizes, and plaintext against kernel and userspace TLS. Every figure comes from outside the server: the clients' clocks, the bytes the stand-in sent, and the kernel's counters of each process's CPU time and writes and of the drive's reads. `BENCHMARKS.md` records a run and what it decided.

```console
cargo build --release -p s3-accelerator -p s3-accelerator-bench
target/release/s3-accelerator-bench [--scale X] [--clients N] [--only hits|scan|shift|transports] [--extent-mib N]
```

### Simulator

The seed determines the whole run: the cluster's size, block and slot sizes, disk capacity, admission policy, the workload, and every network, disk and send delay. Writers create, overwrite and delete objects in the model of S3 while clients read. Every response must equal what S3 would have returned for a state its key held between the request's issue, less the bucket's staleness bound, and its answer. Every stored block must hold the bytes of the version it is keyed by.

Every run prints its seed. Pass it back to replay the run exactly. A git commit hash also works as a seed, and CI uses the commit being tested.

```console
cargo run --release -p s3-accelerator-sim                      # a random seed
cargo run --release -p s3-accelerator-sim -- 42                # replay seed 42
cargo run --release -p s3-accelerator-sim -- 0 --seeds 10000   # seeds 0 to 9,999 on every core
```

`scripts/mutants` plants known bugs, one at a time, in a scratch copy and reports whether the scenarios in `crates/sim/tests/scenarios.rs` or a seed sweep catch each one.
