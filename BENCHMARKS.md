# Benchmarks

`crates/bench` runs a storage node and a gateway, each its own process, on one machine's NVMe drive, in front of an in-process stand-in for S3 that answers after 20 ms and never limits them. Every figure comes from outside the server: the clients' clocks, the bytes the stand-in sent, the kernel's counters of each process's CPU time and writes (`/proc/<pid>/stat`, `/proc/<pid>/io`), and the drive's reads (`/proc/diskstats`). The clients run on the same machine and share its CPU with the servers.

```console
cargo build --release -p s3-accelerator -p s3-accelerator-bench
target/release/s3-accelerator-bench
```

## Results

A run at commit time, with one-block extents (the default):

Machine: Intel(R) Core(TM) Ultra 9 285, 24 threads, 125 GiB of memory, Micron MTFDKBA2T0TGD-2BK15ABLT, Linux 7.0.0-30-generic. S3's stand-in waits 20 ms before each answer. 32 clients unless noted.

### Hits and fills

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| First reads (fills) | 256 | 16.00 | 2.13 | 38.50 ms | 64.81 ms | 16.00 | 11.15 | 0.00 | 1.52 | 0.10 |
| Hits | 256 | 16.00 | 3.23 | 14.33 ms | 1192.58 ms | 4.86 | 4.63 | 0.00 | 1.12 | 0.07 |
| Hits from the page cache | 32 | 2.00 | 16.83 | 14.45 ms | 21.80 ms | 0.00 | 0.00 | 0.00 | 0.07 | 0.06 |
| Hits from the page cache, 1 client | 32 | 2.00 | 8.90 | 0.32 ms | 0.93 ms | 0.00 | 0.00 | 0.00 | 0.08 | 0.07 |
| Hits from the drive | 256 | 16.00 | 4.90 | 0.11 ms | 7.37 ms | 0.00 | 0.00 | 16.00 | 0.43 | 0.15 |
| 64 KiB range hits | 20000 | 1.22 | 2.51 | 0.71 ms | 1.78 ms | 0.00 | 0.00 | 0.00 | 0.55 | 0.39 |
| 64 KiB range hits, 1 client | 1000 | 0.06 | 0.24 | 0.09 ms | 0.68 ms | 0.00 | 0.00 | 0.00 | 1.31 | 1.47 |
| 64 KiB range misses, 1 client | 200 | 0.01 | 0.00 | 22.21 ms | 25.92 ms | 0.01 | 0.00 | 0.00 | 6.55 | 4.10 |

256 objects of 64 MiB, which the cache admits on their first read. The node's fill budget is 4 GiB: a gateway asks for all of an object's chunks at once, so each client's miss holds 64 MiB of it, and misses past the budget stream from S3 without admission. "Hits" reads every object again, more than the page cache keeps of blocks read once; "Hits from the page cache" rereads 2 GiB of them, and "Hits from the drive" reads them all after the slab file leaves the page cache. A miss's first byte includes S3's 20 ms.

### Scan and reread

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Hot set, read three times | 606 | 6.05 | 2.38 | 28.07 ms | 661.07 ms | 4.03 | 2.02 | 0.00 | 1.25 | 0.09 |
| Scan of 3× the cache, with hot reads | 2802 | 26.00 | 7.69 | 22.95 ms | 35.02 ms | 12.01 | 0.00 | 0.00 | 0.17 | 0.10 |
| Zipf rereads of 2× the cache | 3000 | 26.33 | 6.98 | 6.93 ms | 222.38 ms | 8.37 | 3.39 | 0.00 | 0.46 | 0.08 |

A 4.0 GiB cache; a hot set of 202 objects (2.0 GiB), a scan of 1401 objects (12.0 GiB) and a Zipf (s = 1) set of 929 objects (8.0 GiB), sizes log-uniform from 64 KiB to 64 MiB. The doorkeeper admits a block on its second read. During the scan, S3 sent 12.01 GiB of scan objects and 0.00 GiB of hot ones. Zipf rereads: byte hit ratio 68.2%, and the node wrote 0.129 bytes per byte served.

### Size shift

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Small objects, with hot reads | 24230 | 6.40 | 0.23 | 22.69 ms | 37.68 ms | 2.40 | 1.21 | 0.04 | 1.00 | 0.41 |
| Large objects, with hot reads | 218 | 6.41 | 5.57 | 31.60 ms | 349.25 ms | 2.41 | 1.21 | 0.00 | 0.69 | 0.08 |

A 4.0 GiB cache, filled by reading twice a cold set of 64 MiB objects (4.0 GiB), then a hot set of them (2.0 GiB) three times. Then new objects, each read twice so the doorkeeper admits them, with the hot set read twice more among them: 12083 small objects (1.20 GiB, log-uniform from 4 KiB to 512 KiB), or, as the control, 77 of 16 MiB (1.20 GiB). S3 sent 0.00 GiB of hot objects again among the small objects, and 0.00 GiB among the large.

### Transports

| Workload | Requests | GiB served | GiB/s | First byte p50 | First byte p99 | GiB from S3 | GiB the node wrote | GiB read from the drive | Node CPU s/GiB | Gateway CPU s/GiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Hits, plaintext | 64 | 4.00 | 19.06 | 14.02 ms | 21.48 ms | 0.00 | 0.00 | 0.00 | 0.06 | 0.05 |
| Hits, kernel TLS | 64 | 4.00 | 7.67 | 8.41 ms | 23.87 ms | 0.00 | 0.00 | 0.00 | 0.66 | 1.20 |
| Hits, userspace TLS | 64 | 4.00 | 0.92 | 80.64 ms | 93.28 ms | 0.00 | 0.00 | 0.00 | 0.59 | 1.09 |

64 objects of 64 MiB, read once to fill and again to measure. Both the clients' link and the gateway's link to the node use the transport. The clients decrypt in userspace, on the same machine.

With 64 MiB extents, the size shift sent 0.14 GiB of hot objects again among the small objects (`--only shift --extent-mib 64`).

## The storage layout

The spec left the layout open until benchmarks tested its three risks. It stays: slots of power-of-two size classes in extents, served with `sendfile`, with two changes.

- **Class shifts.** An extent larger than a block gives up hot blocks when a class needs room: emptying it evicts every block it holds. The size shift fills a 4 GiB cache with 1 MiB blocks and a hot set, then admits 1.2 GiB of small objects that need smaller classes. With 64 MiB extents, S3 sent 0.14 GiB of the hot set again; with one-block extents, none. The benchmark's clients admit hot and cold objects in separate batches, so its extents mix them little. `a_shift_to_smaller_blocks_keeps_the_hot_set` replays the shift against the store with hot and cold blocks sharing extents, as concurrent fills leave them: with one-block extents the hot set keeps every block, and with extents of 64 largest slots the same scenario misses 752 of its 4,096 hot reads. Extents now default to one block. A class that needs room empties the extent eviction's last victim left, which holds the coldest blocks and needs no scan of millions of extents; the extent with the fewest blocks, the earlier choice, can hold only hot ones (`emptying_an_extent_follows_eviction`).
- **The kernel's memory.** Rereads of blocks the page cache holds reached 16.8 GiB/s, and the same hits after the slab file left the page cache read the drive at 4.9 GiB/s. A pass over 16 GiB of blocks read once kept few of them cached, as the kernel ranks pages read twice above pages read once, and a scan of three times the cache sent none of the hot set back to S3.
- **Random writes.** The node writes what it admits and little more: 2.02 GiB for a 2 GiB hot set, and 0.13 bytes per byte served under Zipf rereads. A sync per block holds the drive to a fraction of its write bandwidth: 1 MiB writes each followed by a sync reached 219 MB/s into preallocated space and 422 MB/s over written space, and the same writes from 32 threads sharing each sync reached 2.2 GiB/s. Block writes now share syncs. What small random writes cost a drive's wear these benchmarks cannot see; a log-structured store would turn fills into sequential writes if a drive's wear counters show the need.

## Defects the benchmarks found

- **Large folios.** On Linux 7.0, ext4 caches a large write in one large folio. A small slot carved from space a larger block held lies inside that folio, which dropping the slot's pages leaves cached, so the node waited 30 seconds for the pages to go and then gave the write up: no small block reached the disk once extents changed class. The node now drops the whole largest slot's span when a slot's pages stay (`small_blocks_take_space_that_held_larger_ones`).
- **Crypto on the gateway's event loop.** Kernel TLS hits reached 1.79 GiB/s, with the gateway's one event-loop thread splicing, and so encrypting and decrypting, every byte. Relays over kernel TLS now run on worker threads: 7.67 GiB/s.
- **A sync per block.** Concurrent block writes each synced the slab file; they now share each sync.

## Limits

- **Fills.** A node fills at about 2.1 GiB/s. Its event loop, one thread, receives S3's bodies and relays them to the gateway, and ran at 86% of a core. Its writes fall behind the fetches, so under 32 clients' sustained first reads the 4 GiB fill budget ran out, and 11.15 of 16 GiB reached the disk; the rest streamed from S3 without admission, as the fill budget intends.
- **Fill budget.** A gateway asks for all of an object's chunks at once, so each concurrent miss of a whole object holds its object's size of the budget until its blocks are durable. The default, 64 MiB, admits one 64 MiB miss at a time.
- **TLS.** Kernel TLS served hits at 7.67 GiB/s against 19.06 in plaintext. The node encrypts toward the gateway, and the gateway decrypts and encrypts again toward the client: 0.66 and 1.20 CPU seconds per GiB. Userspace TLS served 0.92 GiB/s.
