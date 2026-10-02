# Load test

Runs the cluster on EC2 hosts with local NVMe, in front of a real S3 bucket, and drives it from several client hosts at once. Each run reports what clients saw, what S3 billed, what each process counted, and what each host's kernel counted, step by step.

- `terraform/` makes the hosts, the bucket and the origin's key in one availability zone.
- `hosts/prepare.sh` prepares each host: packages, sysctls, kernel TLS, the NVMe filesystem, node_exporter, and the systemd unit `s3accel@NAME`.
- `loadtest` (Python 3.11+, standard library only) drives everything over SSH.
- `crates/load` builds `s3-accelerator-load`, which seeds the dataset, runs a step from one client host, and writes the report.
- `plans/` holds the plans: `smoke.toml` checks every part in 12 minutes, `full.toml` characterizes the cluster in 2 hours, and `soak.toml` runs 4 hours of mixed traffic with rolling restarts. `rate.toml` measures request rate with small and 4 KiB reads, best with `nodes_per_host` of 4 to 8; `bandwidth.toml` drives large hits to the nodes' line rate, for nodes such as `m8idn.32xlarge` (`-var node_type=m8idn.32xlarge -var node_count=2 -var client_type=c6in.16xlarge -var client_count=5`); `profile.toml` holds load steady for `perf`. All four read `full.toml`'s dataset.
- `settings.toml` sets the topology, TLS and cache.

## What you need

- AWS credentials for the staging account, 399785866736, that can create a VPC, EC2 instances, an S3 bucket, and an IAM user with an access key, for Terraform and for `loadtest cloudwatch`. Terraform's provider names the account in `allowed_account_ids` and refuses any other. The region is `us-east-1` unless you pass `-var region=...`.
- Quota for the instances: by default 5 `i4i.4xlarge` (4 nodes and 1 on standby) and 4 `c6in.8xlarge`, 244 vCPUs of on-demand standard instances in all.
- Terraform 1.6 or later, the AWS CLI, OpenSSH, OpenSSL, Python 3.11 or later, and Rust.
- A Linux x86_64 machine to build on, since `deploy` copies the binaries it builds. Elsewhere, build on a client host and pass `deploy --binaries DIR`.

At on-demand prices in us-east-1, the default hosts cost about $14 an hour. The full plan's 2.3 TiB dataset costs about $10 in PUT requests to seed and about $2 a day to store; its baselines and fills cost a few dollars in GETs. `terraform destroy` deletes everything, the bucket's objects included.

## Run it

```console
cd loadtest/terraform
terraform init
terraform apply -var operator_cidr=$(curl -s https://checkip.amazonaws.com)/32
cd ../..

loadtest/loadtest up                                  # inventory, prepare, render, deploy
loadtest/loadtest status
loadtest/loadtest seed loadtest/plans/smoke.toml
loadtest/loadtest run loadtest/plans/smoke.toml
loadtest/loadtest cloudwatch smoke-YYYYMMDD-HHMMSS    # a few minutes later
```

`run` writes `loadtest/runs/NAME/report.md`, and `cloudwatch` adds S3's request counts to it. For the full plan, set `cache_gib = 256` in `settings.toml` first, so the medium and table sets exceed the cluster's 1 TiB of cache, then `loadtest/loadtest render && loadtest/loadtest deploy`.

Watch a run in Prometheus on the first client host, which scrapes every process and every host every 5 seconds:

```console
loadtest/loadtest ssh client-0 -L 9090:localhost:9090 -N
# then open http://localhost:9090
```

Finish with `terraform destroy` in `loadtest/terraform`.

## What each command does

| Command | Effect |
|---|---|
| `inventory` | Reads Terraform's outputs into `out/inventory.json`, and the origin's key into `out/secrets.json` (mode 600). `--file FILE` takes a hand-written inventory of the same shape, with the key from `ORIGIN_ACCESS_KEY_ID` and `ORIGIN_SECRET_ACCESS_KEY`. |
| `prepare` | Runs `hosts/prepare.sh` on every host, and saves each host's memory, drive space and kernel in `out/facts.json`. |
| `render` | Writes each process's config to `out/configs`, clients' credentials to `out/env`, certificates to `out/tls` when TLS is on, and Prometheus's config. It generates the cluster secret and the client key once. |
| `deploy` | Builds `s3-accelerator` and `s3-accelerator-load` in release mode, stops every process, installs the binaries and configs, then starts the storage nodes, waits until each answers `/readyz`, and starts the gateways. |
| `seed PLAN` | Writes the plan's dataset to the bucket, each client host a share. `--resume` skips objects S3 already holds. |
| `run PLAN` | Runs the plan's steps in order; `--steps a,b` runs some. |
| `fault ACTION NODE` | `kill` (SIGKILL), `stop` (SIGTERM), `start`, `restart` or `leave` (SIGUSR1) a storage node by hand. |
| `status`, `start`, `stop` | Each process's state, readiness and ring; start or stop them all. |
| `logs RUN` | Saves every process's journal since the run began into the run's `logs/`. |
| `ssh HOST [ARGS]` | Logs in to a host, or runs a command there. |

`--dry-run` before any command prints the remote commands instead of running them.

## How a step runs

For each step, `run`:

1. Starts any storage node a fault left stopped, other than standby nodes, and waits until it is ready.
2. Drops the storage nodes' page caches if the step asks, so hits read the drives.
3. Saves each process's `/metrics` and each host's CPU, network and disk counters.
4. Starts `s3-accelerator-load run` on every client host, all timed to begin 15 seconds later, and injects the step's faults on schedule.
5. Waits for every client, saves the counters again, and fetches each client's results.

Each client host keeps the step's connections open, each with one request in flight. With `rate`, each host starts that many requests a second, and a request's latency counts from when it was due, so a stall counts against every request it delays. Every response's length is checked, and its bytes too unless the step says `verify = "edges"` or `"none"`: each object's bytes follow from its key and the dataset's seed.

Sequential reads spread keys across client hosts. A fill reads each window of keys twice (`passes = 2`), since the doorkeeper stores a block on its second read among its recent first reads.

## The layout

| Port | Process |
|---|---|
| 9000 | Gateways: on `127.0.0.1` of each client host, or on every storage node's address when `gateways = "nodes"` |
| 9400 | Storage nodes, TCP for gateways and peers and UDP for gossip; with `nodes_per_host` above 1, a host's further nodes take 9410, 9420 and on |
| 9401, 9402 | Admin listeners of storage nodes (9411, 9421 and on for further nodes) and client gateways: `/metrics`, `/healthz`, `/readyz` |
| 9100 | node_exporter on every host |
| 9090 | Prometheus on the first client host |

Storage nodes stripe their instance-store drives into one ext4 filesystem at `/mnt/s3accel`. With `nodes_per_host` above 1 in `settings.toml`, each node host runs that many storage nodes, `node-0.0`, `node-0.1` and on, each a ring member with its own data directory and an even share of the host's cache; a fault on a host acts on all of them. `render` sizes each node's slab file from its free space, less the slot table, and weights each node by its cache. The systemd unit starts processes with a soft limit of 1,024 open files and a hard limit of 1,048,576, and the server raises the soft limit when it starts.

## The report

`report.md` opens with a row per step, then a section per step:

- **Clients:** requests, errors by status or failure, GiB/s, requests/s, and time to first and last byte, from the clients' clocks.
- **S3:** GETs, PUTs, bytes and errors from the bucket's CloudWatch request metrics, once `cloudwatch` has run. CloudWatch counts by the minute, so a step's figures include its first and last partial minutes.
- **The processes' own counts:** S3 requests, bytes by source, block hits, admissions, evictions, leases, gateway errors, and each process's CPU, memory, open files and event loop delay.
- **Hosts:** CPU, network and cache-drive traffic from `/proc`.
- **Faults**, with when each took effect and how long a started node took to become ready.
- **A timeline** by 10-second windows; `timelines/STEP.csv` has it by the second.

## Choosing targets

The spec leaves the workload targets open: object sizes, request rate, working-set size, and hit-rate and latency goals. The full plan measures the cluster across them. Its results, set against the S3 baselines, are where those targets come from.
