# Benchmarks

`crates/bench` runs a storage node and a gateway, each its own process, on one machine's NVMe drive, in front of an in-process stand-in for S3 that answers after 20 ms and never limits them. Every figure comes from outside the server: the clients' clocks, the bytes the stand-in sent, the kernel's counters of each process's CPU time and writes (`/proc/<pid>/stat`, `/proc/<pid>/io`), and the drive's reads (`/proc/diskstats`). The clients run on the same machine and share its CPU with the servers.

```console
cargo build --release -p s3-accelerator -p s3-accelerator-bench
target/release/s3-accelerator-bench
```

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
- **Range misses** cost the node about twice the CPU, as each S3 request now crosses to a worker thread and back. Their p99 reached 64 ms in one run of three before Phase 7 and in every run after: some misses wait 25 to 80 ms longer than S3 took to answer, though S3 answered each within 25 ms and the event loops show no long stalls then. 7I, below, may explain it.

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

## Limits

- **Fills.** A node fills at 3.4 to 4.4 GiB/s, with S3's bodies received on worker threads. The drive's writes trail S3's bodies, so under 32 clients' sustained first reads a 4 GiB fill budget still fills up, and the rest streams from S3 without admission, as the fill budget intends.
- **Fill budget.** A gateway asks for a response's chunks up to `read_ahead` bytes ahead of the part it forwards (64 MiB by default), so a concurrent miss holds at most that much of its owners' budgets, and the default budget, 256 MiB, admits four such misses at once.
- **Stalls when the drive syncs slowly (7I).** The node's event loop writes each slot record and metadata entry itself. When the drive takes seconds to sync, the kernel throttles those small writes, and the loop, which owns the core, stalls with every read it serves: in about a third of scan runs, the hot set's reads ran at 0.7 to 0.9 GiB/s against 4.3 to 4.9, as the loop ran its 100 ms timer up to a second late and slab-file syncs took up to 5 seconds. Moving those writes to a thread of their own is 7I.
- **TLS.** Kernel TLS served hits at 7.9 to 8.1 GiB/s against 20.9 to 21.9 in plaintext. The node encrypts toward the gateway, and the gateway decrypts and encrypts again toward the client: 0.66 to 0.71 and 1.22 to 1.25 CPU seconds per GiB. Userspace TLS served 0.9 GiB/s.
