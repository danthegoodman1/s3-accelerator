# Benchmarks

`crates/bench` runs a storage node and a gateway, each its own process, on one machine's NVMe drive, in front of an in-process stand-in for S3 that answers after 20 ms and never limits them. Every figure comes from outside the server: the clients' clocks, the bytes the stand-in sent, the kernel's counters of each process's CPU time and writes (`/proc/<pid>/stat`, `/proc/<pid>/io`), and the drive's reads (`/proc/diskstats`). The clients run on the same machine and share its CPU with the servers.

```console
cargo build --release -p s3-accelerator -p s3-accelerator-bench
target/release/s3-accelerator-bench
```

With `--metadata on`, the node looks up the bucket's origin, and the gateway the client, in the reference metadata service rather than their configs.

Both processes serve their admin listener. Between workloads, the benchmark waits until the node's metrics show no fills in progress and it has written nothing for half a second, so each workload starts after the last one's blocks are durable. `--scrape-ms MS` also scrapes each process's `/metrics` every `MS` milliseconds during the workloads.

## Results

A run at commit time. Runs on this machine vary widely from one to the next: it hosts other services, the page cache keeps different blocks from run to run, and the drive syncs more slowly after hundreds of GiB of writes. A workload that should hit the page cache sometimes reads the drive, and single rows move by a factor of two or more. The next section gives the range over several runs.

Machine: Intel(R) Core(TM) Ultra 9 285, 24 threads, 125 GiB of memory, Micron MTFDKBA2T0TGD-2BK15ABLT, Linux 7.0.0-30-generic. S3's stand-in waits 20 ms before each answer. 32 clients unless noted.

### Hits and fills

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| First reads (fills) | 256 | 16.00 | 3.62 | 34.38 ms | 56.51 ms | 16.00 | 9.00 | 0.00 | 1.92 | 0.14 |
| Hits | 256 | 16.00 | 1.33 | 1.25 ms | 117.28 ms | 3.24 | 3.25 | 12.64 | 0.97 | 0.21 |
| Hits from the page cache | 32 | 2.00 | 15.84 | 12.29 ms | 34.36 ms | 0.00 | 0.00 | 0.00 | 0.09 | 0.06 |
| Hits from the page cache, 1 client | 32 | 2.00 | 8.07 | 0.41 ms | 0.93 ms | 0.00 | 0.00 | 0.00 | 0.09 | 0.09 |
| Hits from the drive | 256 | 16.00 | 2.35 | 0.68 ms | 7.47 ms | 0.00 | 0.00 | 15.99 | 0.53 | 0.28 |
| 64 KiB range hits | 20000 | 1.22 | 2.34 | 0.77 ms | 1.65 ms | 0.00 | 0.00 | 0.00 | 0.30 | 0.42 |
| 64 KiB range hits, 1 client | 1000 | 0.06 | 0.85 | 0.03 ms | 0.48 ms | 0.00 | 0.00 | 0.00 | 0.33 | 0.66 |
| 64 KiB range misses, 1 client | 200 | 0.01 | 0.00 | 22.61 ms | 83.40 ms | 0.01 | 0.00 | 0.00 | 12.29 | 4.10 |

256 objects of 64 MiB, which the cache admits on their first read. The node's fill budget is 4 GiB, and each block holds its share from its fill's start until it is durable; the drive's writes trail S3's bodies, so misses past the budget stream from S3 without admission. "Hits" reads every object again, more than the page cache keeps of blocks read once; "Hits from the page cache" rereads 2 GiB of them, and "Hits from the drive" reads them all after the slab file leaves the page cache. A miss's first byte includes S3's 20 ms.

### Scan and reread

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Hot set, read three times | 606 | 6.05 | 2.68 | 28.71 ms | 98.25 ms | 4.03 | 2.02 | 0.00 | 1.49 | 0.10 |
| Scan of 3× the cache, with hot reads | 2802 | 26.00 | 7.25 | 25.53 ms | 82.08 ms | 12.01 | 0.00 | 0.00 | 0.54 | 0.09 |
| Zipf rereads of 2× the cache | 3000 | 26.33 | 8.94 | 9.67 ms | 86.52 ms | 8.32 | 3.07 | 0.00 | 0.69 | 0.09 |

A 4.0 GiB cache; a hot set of 202 objects (2.0 GiB), a scan of 1401 objects (12.0 GiB) and a Zipf (s = 1) set of 929 objects (8.0 GiB), sizes log-uniform from 64 KiB to 64 MiB. The doorkeeper admits a block on its second read. During the scan, S3 sent 12.01 GiB of scan objects and 0.00 GiB of hot ones. Zipf rereads: byte hit ratio 68.4%, and the node wrote 0.117 bytes per byte served.

### Size shift

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Small objects, with hot reads | 24230 | 6.40 | 0.22 | 23.05 ms | 63.59 ms | 2.40 | 1.21 | 0.04 | 1.48 | 0.49 |
| Large objects, with hot reads | 218 | 6.41 | 3.80 | 48.55 ms | 696.71 ms | 2.41 | 1.18 | 0.00 | 0.78 | 0.09 |

A 4.0 GiB cache, filled by reading twice a cold set of 64 MiB objects (4.0 GiB), then a hot set of them (2.0 GiB) three times. Then new objects, each read twice so the doorkeeper admits them, with the hot set read twice more among them: 12083 small objects (1.20 GiB, log-uniform from 4 KiB to 512 KiB), or, as the control, 77 of 16 MiB (1.20 GiB). S3 sent 0.00 GiB of hot objects again among the small objects, and 0.00 GiB among the large.

### Transports

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Hits, plaintext | 64 | 4.00 | 20.93 | 12.46 ms | 20.65 ms | 0.00 | 0.00 | 0.00 | 0.07 | 0.04 |
| Hits, kernel TLS | 64 | 4.00 | 7.90 | 8.94 ms | 21.61 ms | 0.00 | 0.00 | 0.00 | 0.66 | 1.25 |
| Hits, userspace TLS | 64 | 4.00 | 0.90 | 63.35 ms | 73.40 ms | 0.00 | 0.00 | 0.00 | 0.58 | 1.11 |

64 objects of 64 MiB, read once to fill and again to measure. Both the clients' link and the gateway's link to the node use the transport. The clients decrypt in userspace, on the same machine.

## Phase 7 before and after

Three runs each of the hits section on the same day, of the code before Phase 7 (`f5e7389^`) and at the end of it; ranges are the lowest and highest.

| Workload | Before: GiB/s | After: GiB/s | Before: first byte p99 | After: first byte p99 | Before: node CPU s/GiB | After: node CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|
| First reads (fills) | 1.63–2.26 | 3.42–4.38 | 60–1403 ms | 57–267 ms | 0.82–2.00 | 1.33–1.92 |
| Hits, while unstored objects fill | 0.84–3.92 | 1.33–6.17 | 967–5502 ms | 117–2462 ms | 1.20–1.31 | 0.92–1.80 |
| Hits from the page cache, 1 client | 7.55–8.59 | 8.07–8.34 | 0.78–1.09 ms | 0.73–0.93 ms | 0.07–0.10 | 0.09 |
| 64 KiB range hits | 2.34–2.53 | 0.83–2.40 | 1.38–1.98 ms | 1.65–23.03 ms | 0.54–0.56 | 0.30–1.89 |
| 64 KiB range hits, 1 client | 0.15–0.21 | 0.05–0.85 | 0.74–0.80 ms | 0.44–22.70 ms | 1.64–1.80 | 0.33–2.13 |
| 64 KiB range misses, 1 client | | | 27–64 ms | 66–83 ms | 5.73–7.37 | 10.65–12.29 |

The low ends after Phase 7 of the range hits and the high end of "Hits" come from one run, whose drive synced slowly: its range hits read S3 and the drive rather than the page cache. In the other two, 64 KiB range hits ran at 2.34 and 2.40 GiB/s, 0.80 and 0.85 from one client, and "Hits" reached a p99 of 117 and 366 ms.

- **Fills** run 1.6 to 2 times as fast: S3's bodies arrive on worker threads (7C), off the event loop that used to receive and relay every byte.
- **Hits while objects fill** keep their first byte: p99 fell from about a second to a few hundred milliseconds, since fills no longer occupy the event loop.
- **Small range hits** cost the node half the CPU and reach four times the throughput from one client: a run the page cache holds goes out with `sendfile` from the event loop, and a small reply leaves in one write (7E).
- **Range misses** cost the node about twice the CPU, as each S3 request now crosses to a worker thread and back. Their p99 reached 64 ms in one run of three before Phase 7 and in every run at its end, with some misses waiting 25 to 80 ms longer than S3 took to answer. Once the journal thread (7I) and reads before the sync (7J) landed, four runs put the p99 at 23.2 to 23.7 ms, S3's 20 ms and a few more.

## Origins and clients from the metadata service

Six runs each of the hits section, with the bucket's origin in the node's config and in the reference metadata service: first alternating, then two of one and two of the other. Every range overlaps. The node looked the bucket up once per run with the service, and finding a fresh entry costs a request 46 ns on the node's thread, against 15 ns from the config (`origins::tests::cost_of_finding_an_origin`).

| Workload | Config: GiB/s | Service: GiB/s | Config: first byte p99 | Service: first byte p99 | Config: node CPU s/GiB | Service: node CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|
| First reads (fills) | 3.52–4.16 | 3.49–4.22 | 58–161 ms | 55–197 ms | 1.44–1.92 | 1.34–1.95 |
| Hits | 2.62–7.53 | 3.68–7.26 | 189–349 ms | 229–357 ms | 0.55–2.19 | 0.59–2.17 |
| Hits from the page cache, 1 client | 7.97–9.28 | 8.23–8.91 | 0.60–0.89 ms | 0.70–1.73 ms | 0.07–0.11 | 0.07–0.09 |
| 64 KiB range hits | 2.30–2.46 | 2.18–2.49 | 1.28–1.81 ms | 1.45–21.50 ms | 0.24–0.31 | 0.22–0.48 |
| 64 KiB range hits, 1 client | 0.67–1.03 | 0.27–1.19 | 0.37–0.60 ms | 0.27–0.79 ms | 0.16–0.66 | 0.33–0.49 |
| 64 KiB range misses, 1 client | | | 23.2–23.8 ms | 23.2–23.7 ms | 11.47–14.75 | 10.65–13.11 |

With the gateway's client from the service too, four more runs, with the service, without, without and with, looked the bucket and the key up once each per run, and finding a fresh key costs a request 38 ns on the gateway's thread, against 29 ns from the config (`clients::tests::cost_of_finding_a_client`). Single-client page-cache hits ran at 8.50 and 8.94 GiB/s with the service and 9.73 and 8.16 without, and 64 KiB range misses' p99 at 23.6 and 23.9 ms against 23.5 and 23.3. The drive turned away most fills in three of the four, which then read S3 and the drive on hits.

The wide ranges come from the drive. The node wrote 7.4 to 9.4 GiB while filling in eight runs, and 0.6 to 3.2 GiB in the other four, two of each mode. In those four, the drive's writes trailed S3's bodies so far that the fill budget turned most blocks away (13,706 and 15,401 refusals in the last run of each mode, against 2,999 in a run that stored 9.3 GiB), so fills ran faster with longer tails, and later hits went to S3. The alternating runs made it look like the service's doing: three service runs in a row wrote less. With two of each in a row, the second run of each mode wrote less.

## Where the time goes

`perf` sampled the node and the gateway through the "Hits and fills" section, both built with frame pointers (`RUSTFLAGS="-C force-frame-pointers=yes"`). The benchmark prints each process's PID and each workload's window in `CLOCK_MONOTONIC` seconds, so `perf record -k CLOCK_MONOTONIC` samples split by workload with `perf report --time`:

```console
sudo perf record -k CLOCK_MONOTONIC -F 299 -g -p <pid> -o node.data
sudo perf report -i node.data --time <from>,<to> --sort comm --no-children
```

Before Phase 7, as shares of each process's CPU in each workload:

- **Fills:** the node's event loop, its one thread, took 73%. It received S3's bodies into memory through the S3 client (28%, in `recvfrom`) and wrote them to the gateway from memory (30%, in `sendto`), zeroing fresh pages and copying buffers besides; worker threads wrote the blocks. Every byte crossed the event loop twice.
- **Hits from the page cache and the drive:** the node's workers spent 98% in `sendfile` and the gateway 89% in `splice`: the bytes themselves.
- **64 KiB range hits:** the cost was per request. The node's event loop wrote each response head (22%) and a small body from memory (17%), and handed each `sendfile` to a worker, whose wakeups cost 5 to 16% in `futex`. The gateway spent 28% reading and writing heads and 15 to 18% in `splice`.

After Phase 7's first changes, fills moved to worker threads and ran faster, and 74% of the node's CPU during fills went to spinning in the kernel (`osq_lock`, `rwsem_spin_on_owner`): many threads wrote blocks into the one slab file at once, and ext4 takes one buffered write into a file at a time. Writes now copy their bytes into the file under a mutex, so the threads waiting sleep.

## The storage layout

The spec left the layout open until benchmarks tested its three risks. It stays: slots of power-of-two size classes in extents, served with `sendfile`, with two changes.

- **Class shifts.** An extent larger than a block gives up hot blocks when a class needs room: emptying it evicts every block it holds. The size shift fills a 4 GiB cache with 1 MiB blocks and a hot set, then admits 1.2 GiB of small objects that need smaller classes. With 64 MiB extents, S3 sent 0.14 GiB of the hot set again; with one-block extents, none. The benchmark's clients admit hot and cold objects in separate batches, so its extents mix them little. `a_shift_to_smaller_blocks_keeps_the_hot_set` replays the shift against the store with hot and cold blocks sharing extents, as concurrent fills leave them: with one-block extents the hot set keeps every block, and with extents of 64 largest slots the same scenario misses 752 of its 4,096 hot reads. Extents now default to one block. A class that needs room empties the extent eviction's last victim left, which holds the coldest blocks and needs no scan of millions of extents; the extent with the fewest blocks, the earlier choice, can hold only hot ones (`emptying_an_extent_follows_eviction`).
- **The kernel's memory.** Rereads of blocks the page cache holds reached 16.8 GiB/s, and the same hits after the slab file left the page cache read the drive at 4.9 GiB/s. A pass over 16 GiB of blocks read once kept few of them cached, as the kernel ranks pages read twice above pages read once, and a scan of three times the cache sent none of the hot set back to S3.
- **Random writes.** The node writes what it admits and little more: 2.02 GiB for a 2 GiB hot set, and 0.13 bytes per byte served under Zipf rereads. A sync per block holds the drive to a fraction of its write bandwidth: 1 MiB writes each followed by a sync reached 219 MB/s into preallocated space and 422 MB/s over written space, and the same writes from 32 threads sharing each sync reached 2.2 GiB/s. Block writes now share syncs. What small random writes cost a drive's wear these benchmarks cannot see; a log-structured store would turn fills into sequential writes if a drive's wear counters show the need.

## Defects the benchmarks found

- **Large folios.** On Linux 7.0, ext4 caches a large write in one large folio. A small slot carved from space a larger block held lies inside that folio, which dropping the slot's pages leaves cached, so the node waited 30 seconds for the pages to go and then gave the write up: no small block reached the disk once extents changed class. The node now drops the whole largest slot's span when a slot's pages stay (`small_blocks_take_space_that_held_larger_ones`).
- **Crypto on the gateway's event loop.** Kernel TLS hits reached 1.79 GiB/s, with the gateway's one event-loop thread splicing, and so encrypting and decrypting, every byte. Relays over kernel TLS now run on worker threads: 7.67 GiB/s.
- **A sync per block.** Concurrent block writes each synced the slab file; they now share each sync.
- **Spinning on the slab file's lock.** With fills on worker threads, three quarters of the node's CPU during fills went to threads spinning on the slab file's lock. A mutex around each write's copy into the file brought fills from 3.2 to 4.0 node CPU seconds per GiB down to 1.3 to 1.9, below the 2.0 before Phase 7.
- **Splitting what the window holds.** A read's parts to one owner were split at half the read-ahead window even when the window held the whole body, which gained nothing and cost a single client's throughput. Parts split only while runs remain beyond the window.
- **Settling before fills end.** The benchmark waited for half a second without writes before each workload. A sync that ran longer, with writers throttled behind it, ended the wait early, and a hit workload then waited on the last one's writes: plaintext hits ran at 0.62 GiB/s. The benchmark now waits for the node's metrics to show no fills in progress: 20.9 to 21.9 GiB/s.

## Load tests on AWS

`loadtest/` runs the cluster on EC2 in us-east-1a in front of a real S3 bucket, and checks every byte of every response against the object's known contents. Clients' figures come from the load generator's clocks over each step's measured window, processes' from their `/metrics`, and hosts' from `/proc` and the network card's `ethtool` counters. Runs of 2026-10-01; their reports stay in `loadtest/runs/`, out of the repository. Gb/s below are decimal gigabits: a GiB/s is 8.59 Gb/s.

### Phase 11: one gateway thread

`full-20261001-152652`: 4 i4i.4xlarge nodes (16 vCPUs, one 3.75 TB drive, 25 Gb/s on burst credits and 9.375 Gb/s without) with 256 GiB of cache each, and 4 c6in.8xlarge clients (32 vCPUs, 50 Gb/s), each running a gateway; 2.3 TiB of objects. Of 65 million requests through the cache, two failed: S3's own 500 to a write, passed through, and one read that timed out during a rolling restart. Latencies here include a millisecond the load generator waited before each request, which Phase 12 removed.

| Workload | Requests/s | GiB/s | First byte p50 | p99 | What limited it |
|---|--:|--:|--:|--:|---|
| Small hits, 4-256 KiB, 64 connections per client | 66,562 | 3.85 | 3.79 ms | 5.05 ms | Each gateway's one thread, at a core: about 14,600 requests/s per client |
| 256 MiB hits, 64 connections per client | 45 | 11.23 | 4.93 ms | 45 ms | The nodes' network on burst credits; after an hour, 1.1 GiB/s each without them |
| S3 directly, small objects | 18,327 | 1.06 | 32 ms | 146 ms | S3 answered 30% with 503 on a new bucket |
| S3 directly, 256 MiB objects | 84 | 20.91 | 102 ms | 198 ms | |

### Phase 12: every core

`rate-20261001-211850` and `rate8`: Phase 11's hosts with four, then eight, node processes per host, and gateways of 32 loops. 373 million requests, every one answered.

| Workload | Requests/s | GiB/s | First byte p50 | p99 | What limited it |
|---|--:|--:|--:|--:|---|
| One connection per client | 12,813 | 0.74 | 0.33 ms | 0.46 ms | The round trip |
| Small hits, 64 connections per client | 195,660 | 11.30 | 0.55 ms | 4.29 ms | The nodes' network: 25 Gb/s each, at 15-18% CPU |
| Small hits, 1,024 connections per client | 196,886 | 11.37 | 1.19 ms | 59 ms | The same |
| 4 KiB range hits, 64 connections, 4 nodes per host | 566,777 | 2.16 | 0.43 ms | 0.88 ms | The busiest node processes' threads, at a core, on hosts at 20% |
| 4 KiB range hits, 256 connections, 8 nodes per host | 813,561 | 3.10 | 1.06 ms | 3.68 ms | The client hosts' CPU, 79-85%: each gateway took about 19 cores and the load generator most of the rest |

`bandwidth-20261001-215017`: 2 m8idn.32xlarge nodes (128 vCPUs, 496 GiB of memory, two 3.8 TB drives, 200 Gb/s), one process each with 1 TiB of cache, and 5 c6in.16xlarge clients (64 vCPUs, 100 Gb/s) with gateways of 64 loops, over 512 large objects, 128 GiB, warmed into the cache. Every request answered.

| Workload | GiB/s | Each node | Node CPU | Note |
|---|--:|--:|--:|---|
| S3 directly, 64 connections per client | 27.39 | | | About 88 MiB/s per connection |
| 256 MiB hits, 16 connections per client | 42.73 | 21.37 GiB/s | 3% | |
| 256 MiB hits, 64 connections per client | 45.56 | 22.78 GiB/s, 195.7 Gb/s | 2% | The network card held back 126 million packets (`bw_out_allowance_exceeded`) |
| 256 MiB hits, 128 connections per client | 45.29 | 22.65 GiB/s | 1-2% | |
| 8 MiB range hits, 128 connections per client, 5,895 a second | 46.05 | 23.03 GiB/s, 197.8 Gb/s | 2% | |
| 256 MiB hits after dropping the page cache | 39.98 | 19.99 GiB/s | 13-20% | Each node read its 64 GiB from its drives once, then from the page cache |

Each node's figure counts object bytes; TCP, IP and Ethernet headers add about 0.7% on 9,001-byte frames, which brings the nodes to 197-199 Gb/s of their 200.

### Phase 13: a cheaper request path

`rate13`: Phase 12's `rate8` hardware and layout (4 i4i.4xlarge nodes with eight node processes each, 4 c6in.8xlarge clients with gateways of 32 loops), after the read-ahead, the allocation cuts and the cached signing key. 276 million requests; S3 answered 25 of the million cold reads in `warm-small` with a 5xx, which the gateways passed on, and every other request was answered.

| Workload | Requests/s | GiB/s | First byte p50 | p90 | p99 | p99.9 | Gateway CPU per request |
|---|--:|--:|--:|--:|--:|--:|--:|
| One connection per client | 13,654 | 0.79 | 0.28 ms | 0.33 ms | 0.38 ms | 0.44 ms | |
| Small hits, 256 connections per client | 196,148 | 11.33 | 0.80 ms | 4.93 ms | 29 ms | 55 ms | |
| 4 KiB range hits, 64 connections per client | 691,839 | 2.64 | 0.35 ms | 0.45 ms | 0.59 ms | 0.81 ms | |
| 4 KiB range hits, 256 connections per client | 911,555 | 3.48 | 0.56 ms | 1.11 ms | 16 ms | 24 ms | 86 µs of a vCPU |
| 4 KiB range hits, 1,024 connections per client | 944,692 | 3.60 | 0.83 ms | 3.58 ms | 104 ms | 125 ms | 86 µs of a vCPU |

4 KiB range hits rose 12% at 256 connections, from 813,561 a second to 911,555, and reached 944,692 at 1,024. Each gateway still took about 20 of its host's 32 vCPUs, and the client hosts ran at 72-76%, so the client hosts' CPU still set the limit; the node hosts ran at 32-41%. A request cost the gateway 86 µs of a vCPU against `rate8`'s 93 µs, 8% less, where one loop on a workstation gained 40%: the trims cut the gateway's own work, and on c6in most of a request's cost lies elsewhere, likely in the kernel's TCP and the network driver, which a profile on AWS would confirm. Small hits stayed at the nodes' network limit, and one connection's first byte fell from 0.33 ms to 0.28 ms. Past 256 connections, the client hosts queue: p99 rose to 16 ms at 256 and 104 ms at 1,024.

### Scale test: the cache against S3

`scale`, `scale-2` and `scale-1proc` (`loadtest/plans/scale.toml`): 10 c8in.16xlarge clients (64 vCPUs, 100 Gb/s), each running its gateway, read one dataset from S3 directly and through 6 m8idn.32xlarge nodes (128 vCPUs, 200 Gb/s, two 3.8 TB drives) with 1 TiB of cache each. Small objects and ranges ran 32 node processes per host; `scale-1proc` ran large objects with one. c6in.16xlarge and m6in.16xlarge clients were out of capacity in the zone. Connections are per client; rates are totals.

| Workload | Target | Requests/s | GiB/s | First byte p50 | p90 | p99 | p99.9 |
|---|---|--:|--:|--:|--:|--:|--:|
| 4-256 KiB objects, 1 connection | S3 | 218 | 0.01 | 35 ms | 73 ms | 120 ms | 220 ms |
| | Cache | 13,591 | 0.79 | 0.66 ms | 0.93 ms | 1.20 ms | 1.43 ms |
| 4-256 KiB objects, 64 connections | S3 | 17,595 | 1.02 | 25 ms | 60 ms | 110 ms | 225 ms |
| | Cache | 373,341 | 21.56 | 0.49 ms | 1.38 ms | 20 ms | 26 ms |
| 4-256 KiB objects, 256 connections | Cache | 521,240 | 30.11 | 0.52 ms | 2.25 ms | 47 ms | 147 ms |
| 4 KiB ranges, 64 connections | S3 | 20,694 | 0.08 | 26 ms | 48 ms | 98 ms | 215 ms |
| | Cache | 1,642,343 | 6.27 | 0.38 ms | 0.51 ms | 0.65 ms | 0.79 ms |
| 4 KiB ranges, 256 connections | Cache | 2,721,604 | 10.38 | 0.55 ms | 1.34 ms | 10 ms | 13 ms |
| 4 KiB ranges, 1,024 connections | Cache | 3,696,502 | 14.10 | 1.00 ms | 7.23 ms | 22 ms | 42 ms |
| 256 MiB objects, 16 connections | Cache | 416 | 103.93 | 1.22 ms | 1.98 ms | 3.26 ms | 5.12 ms |
| 256 MiB objects, 64 connections | S3 | 217 | 54.33 | 87 ms | 130 ms | 176 ms | 229 ms |
| | Cache | 459 | 114.73 | 2.78 ms | 5.76 ms | 11 ms | 211 ms |
| 256 MiB objects, 256 connections | S3 | 438 | 109.45 | 34 ms | 97 ms | 143 ms | 190 ms |
| | Cache | 452 | 112.93 | 211 ms | 219 ms | 420 ms | 745 ms |

- **Small objects and ranges:** 21 times S3's request rate for small objects and 79 times for 4 KiB ranges at 64 connections, first byte under a millisecond. Nothing ran out: at 3.7 million ranges a second the client hosts were 42-44% busy and the node hosts 12%, while p99 rose to 22 ms. Small hits at 256 connections climbed from 407,000 to 632,000 a second over the step without a host near its limit. Both point at queueing in the request path, not at a resource.
- **Large objects:** with one node process per host, the cache gave the clients 104 GiB/s at 16 connections and 115 GiB/s at 64, the clients' 1 Tb/s, each node sending about 170 Gb/s at 4% CPU; S3 gave 54 GiB/s at 64 connections and 109 at 256. At 256 connections the cache's reads queue at the clients' network cards, so its first byte waits 211 ms.
- **Large objects across 192 ring members:** with 32 node processes per host, 256 MiB hits gave 43 GiB/s at 16 connections, 58 at 64, 50 at 128 and 13.5 at 256, where 667 reads timed out, with clients at 5-6% CPU and nodes at 1-2%. A gateway held 35,744 connections to nodes, most with receive windows near 250 KB. A rerun on fresh hosts with the same layout did not collapse; see the next section.
- **A cold set read three times** (`cold-medium`, about 870 GiB a pass): 20.5 GiB/s over the three passes; the block hit rate stays at zero while the first pass streams and the second admits, then holds at 100%.
- **Errors:** S3 answered 67 of the 4.0 million direct requests with a 500, and one timed out; the cache passed on S3's 500s for 14 fills.

`docs/scale-test.png` draws `ab-cluster`'s `hits-large-c64` and `ranges-4k-c64` over their measured windows: `loadtest/chart docs/scale-test.png "loadtest/runs/ab-cluster:hits-large-c64:TITLE" "loadtest/runs/ab-cluster:ranges-4k-c64:TITLE"`.

### Cluster TCP timers, and a rerun on fresh hosts

`ab-linux`, `ab-cluster` and `ab-20ms`: the scale test's hardware again, on new instances, with 32 node processes per host for every workload. The three runs share one deploy and differ only in the timers on links among gateways and nodes: Linux's (a 200 ms minimum retransmission timeout, and loss probes that allow for a 200 ms delayed ACK), the cluster's 5 ms for both, and a 20 ms floor with the 5 ms delayed ACK.

| Workload | Timers | GiB/s | First byte p50 | p99 | p99.9 | Segments resent | Retransmission timeouts |
|---|---|--:|--:|--:|--:|--:|--:|
| 256 MiB objects, 16 connections | Linux | 110.63 | 1.40 ms | 3.57 ms | 7.17 ms | | |
| | 5 ms | 115.84 | 1.81 ms | 6.01 ms | 9.79 ms | | |
| | 20 ms floor | 115.79 | 1.72 ms | 5.47 ms | 24 ms | | |
| 256 MiB objects, 64 connections | Linux | 115.22 | 2.90 ms | 15 ms | 213 ms | 14.8 million | 6,057 |
| | 5 ms | 115.23 | 3.10 ms | 13 ms | 19 ms | 24.0 million | 13,562 |
| | 20 ms floor | 115.30 | 3.01 ms | 18 ms | 31 ms | 25.4 million | 15,674 |
| 256 MiB objects, 256 connections | Linux | 112.57 | 2.45 ms | 209 ms | 229 ms | 47.6 million | 176,917 |
| | 5 ms | 105.99 | 3.52 ms | 24 ms | 42 ms | 85.9 million | 1,526,360 |
| | 20 ms floor | 109.19 | 3.42 ms | 36 ms | 1,044 ms | 87.3 million | 1,101,104 |

- **The tail at full network cards:** at 64 and 256 connections per client, the clients' cards drop packets past their allowance, and with Linux's timers each loss that a timeout recovers costs 200 ms: p99.9 of 213 ms at 64 connections, and p99 of 209 ms at 256. The 5 ms timers bring those to 19 ms and 24 ms. They resend about 1.8 times the segments, 4.7-5.6% of what each node sent at 256 connections against 2.7-2.9%, which costs 6% of throughput there.
- **A 20 ms floor** resends as much as 5 ms but recovers more slowly: backed-off timeouts start from 20 ms, and p99.9 reached a second at 256 connections. The cluster keeps 5 ms.
- **Small objects and ranges** lose nothing and see no change: no allowance drops, a few hundred resent segments a step, and rates within 6% across the three runs, which is the runs' own spread.

With the 5 ms timers, the current defaults:

| Workload | Requests/s | GiB/s | First byte p50 | p90 | p99 | p99.9 |
|---|--:|--:|--:|--:|--:|--:|
| 4-256 KiB objects, 64 connections | 1,383,218 | 79.90 | 0.39 ms | 0.53 ms | 0.68 ms | 0.88 ms |
| 4-256 KiB objects, 256 connections | 1,977,807 | 114.25 | 0.80 ms | 1.94 ms | 4.00 ms | 6.82 ms |
| 4 KiB ranges, 64 connections | 1,836,670 | 7.01 | 0.34 ms | 0.46 ms | 0.59 ms | 0.71 ms |
| 4 KiB ranges, 256 connections | 4,621,169 | 17.63 | 0.52 ms | 0.74 ms | 1.07 ms | 1.62 ms |
| 4 KiB ranges, 1,024 connections | 6,053,848 | 23.09 | 1.43 ms | 2.85 ms | 4.77 ms | 6.62 ms |
| 256 MiB objects, 16 connections | 463 | 115.84 | 1.81 ms | 3.26 ms | 6.01 ms | 9.79 ms |
| 256 MiB objects, 64 connections | 461 | 115.23 | 3.10 ms | 7.10 ms | 13 ms | 19 ms |
| 256 MiB objects, 256 connections | 424 | 105.99 | 3.52 ms | 11 ms | 24 ms | 42 ms |

**The first scale run's slowness did not recur.** With the same layout and Linux's timers, `ab-linux` served 3.9 times the small hits (1,472,009 a second against 373,341) and 1.7 times the 4 KiB ranges at 1,024 connections, with p99 under a millisecond at 64 connections where the first run saw 20 ms. Its large reads held 112.6 GiB/s at 256 connections, where the first run fell to 13.5 GiB/s with 667 timeouts. Every process's event loop ran on time in both runs, and the code differed only in the timers, which `ab-linux` left at Linux's. The first run's hosts are the likeliest difference, through the network between them, but nothing measured shows it: that run predates the report's TCP and network card counters.

### What limits each workload

- **Large objects and small ones of tens of KiB:** the nodes' network cards, at line rate with a core or two to spare on a 200 Gb/s node. More throughput needs more network per node, or more nodes.
- **Requests of a few KiB:** on c6in.8xlarge clients, the client hosts' CPU, where a gateway took 86 µs of a vCPU per request after Phase 13 and shared the host with the load generator. On the scale test's c8in.16xlarge clients, queueing set the limit before any CPU ran out. A node runs its core on one thread, so a host needs several node processes for small requests.
- **Data the page cache doesn't hold:** not measured at line rate. Two drives per m8idn.32xlarge node likely read below its network's rate.
- **Instances on burst credits:** an i4i.4xlarge sends 25 Gb/s for about an hour, then 9.375 Gb/s.

## Limits

- **Fills.** A node fills at 3.4 to 4.4 GiB/s, with S3's bodies received on worker threads. The drive's writes trail S3's bodies, so under 32 clients' sustained first reads a 4 GiB fill budget still fills up, and the rest streams from S3 without admission, as the fill budget intends.
- **Fill budget.** A gateway asks for a response's chunks up to `read_ahead` bytes ahead of the part it forwards (64 MiB by default), so a concurrent miss holds at most that much of its owners' budgets, and the default budget, 256 MiB, admits four such misses at once.
- **Slow drive syncs.** The drive takes seconds to sync after heavy writes. A journal thread writes slot records and metadata entries (7I), and readers take a block once its bytes reach the slab file, before the sync (7J), so neither the event loop nor a block's readers wait on a sync. Before them, about a third of scan runs read the hot set at 0.7 to 0.9 GiB/s; in six runs since, it held 2.1 to 4.7 GiB/s.
- **TLS.** Kernel TLS served hits at 7.9 to 8.1 GiB/s against 20.9 to 21.9 in plaintext. The node encrypts toward the gateway, and the gateway decrypts and encrypts again toward the client: 0.66 to 0.71 and 1.22 to 1.25 CPU seconds per GiB. Userspace TLS served 0.9 GiB/s.
