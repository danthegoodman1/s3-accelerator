# S3 Accelerator

A distributed NVMe read cache in front of S3. S3 remains the source of truth, and cache nodes hold only disposable copies. The cluster can scale, restart and lose nodes without moving or recovering data. Writes pass through to S3.

**Best fit:** compute in the same region reading immutable or version-addressed objects, such as ClickHouse parts, Parquet and Iceberg tables, ML datasets and checkpoints, and build artifacts.

**What it buys:**

- **Latency:** sub-millisecond time to first byte on a hit, versus 20–100 ms from S3.
- **Throughput:** the cluster's aggregate bandwidth, well past S3's per-prefix limit of 5,500 GET/s.
- **Request cost:** fewer S3 GETs.
- **Egress:** savings only when the cache runs outside the origin's cloud or region. S3-to-compute transfer within a region is already free, and internet egress costs the same from a VM as from S3.

## Alternatives

| Option | Choose it when |
|---|---|
| **Tigris TAG** (Apache-2.0) | The origin is Tigris or all clients can share one credential, the cluster rarely resizes, and no single key outgrows a node. It already implements versioned block caching, request coalescing and SigV4 validation. |
| **Alluxio Enterprise AI** | You need POSIX (FUSE) or fsspec access alongside S3 and want a vendor. |
| **S3 Express One Zone** | You run in AWS, latency is the only goal, and you want no cluster to operate. |
| **CDN** (CloudFront, Cloudflare) | You serve web assets or media to the internet. |
| **Peer-to-peer distribution** (Dragonfly, Kraken) | Many hosts fetch the same large objects at once, as with model weights or container images. Capacity grows with each reader. |
| **Per-host caches** (ClickHouse filesystem cache, Mountpoint cache) | Each host rereads only its own data. |

## Unique advantages

**1. The cache survives resizing.** Ring ownership decides what a node writes to disk, never what it may serve. After a ring change, the new owner fetches missing blocks from the previous owner before going to S3. A node being removed keeps serving those fetches for a grace window.

- *Matters when* the cluster autoscales, runs on spot instances or deploys often, and the working set is large or slow to refill from S3.
- *Matters little when* the cluster size stays fixed.

**2. Hot keys are replicated.** An owner spreads a key that exceeds one node's request rate across a small set of replicas.

- *Matters when* every client reads the same small objects, such as table manifests, index files or shared lookup data.
- *Matters little when* load spreads across many keys. Chunk placement already spreads large objects.

**3. Disk admission is filtered.** By default, a block reaches NVMe on its second read.

- *Matters when* scans read a lot of data once, as in ad hoc analytics. Admitting everything would evict the hot set and wear out the drives.
- *Matters little when* nearly everything is reread, as in multi-epoch training. Enable admit-on-first-read for those workloads.

**4. Multi-tenant auth for any S3 origin.** Each credential carries its own grants, and the gateway enforces them on every cache hit.

- *Matters when* several teams or services with different permissions share one cache in front of AWS S3.
- *Matters little when* one service owns both the cache and the bucket.

## Architecture

### Topology

- **One cluster per availability zone.** Clients use their own zone's cluster, and each cluster fills from S3 independently. At $0.01/GB each way for traffic between zones, a cross-zone hit costs more than the S3 GET it replaces once the response exceeds about 20 KB.
- **Identical storage nodes.** Every node runs one binary with three modules: Membership, Storage and Gateway.
- **Gateway placement.** The Gateway is stateless and runs in three places:
  - **Client host** (sidecar, DaemonSet or library): routes straight to the owner, so each byte crosses the network once. Use it wherever you control the host.
  - **Standalone tier** (a gateway-only fleet behind a load balancer): serves apps that can't run a sidecar, such as apps on Cloudflare Workers or other serverless platforms. It adds one hop, scales separately from storage, and keeps proxy traffic off storage nodes' NICs.
  - **Storage nodes:** the simplest setup. It adds one hop and shares storage nodes' NICs.
- **Clients outside the cache's cloud.** Apps on Cloudflare Workers, for example, pull every byte out of the cache's cloud as internet egress. Run the cluster where egress is cheap, or count only latency and S3 request savings.

### Membership and placement

- **Membership** runs SWIM gossip among storage nodes only and publishes an immutable, versioned ring snapshot. Nodes also keep the previous snapshot for the grace window.
- **Gateways fetch the ring** over HTTP from any storage node. Every storage response carries the ring version, and a gateway refetches when it sees a newer one. Only storage nodes gossip, so adding gateways adds no membership traffic.
- **Placement** uses weighted rendezvous hashing over stable node IDs. It moves few keys when membership changes, weights nodes by disk size, and gives each key an ordered candidate list that doubles as its replica set. Each home or chunk reduces to a 64-bit placement hash, and a node's score mixes that hash with the node's ID.
- **Blocks and chunks:**
  - A **block** (1 MiB) is the unit of fill, storage and eviction.
  - A **chunk** (16 MiB) is the unit of placement.
- **Object home** = `rendezvous(bucket, key)`. The home holds the object's metadata, chunk 0 and every block overlapping the object's final 16 MiB. Most file formats keep their metadata at the head or tail (Parquet and ORC footers, safetensors headers), so the home serves those reads in one hop. Objects up to 32 MiB live entirely on their home.
- **Other chunks** belong to `rendezvous(bucket, key, chunk_index)`. Large objects spread across the cluster, and a large read fans out to several owners in parallel.
- **Ownership costs disk only for blocks readers touch.** Blocks fill on read, so a large tail region reserves no space.
- **Unresponsive nodes** stay in the ring for a grace period while gateways route around them. A brief failure therefore doesn't reshuffle ownership. A gateway fails over from a node that times out, answers 5xx or ends a body early to the next rendezvous candidate. Only the home keeps an object's metadata, since writes reach only the home, so a candidate standing in for it reads S3 directly and caches nothing.
- **Disagreement about the ring** costs duplicate fills, never wrong data. A node asked for a chunk it doesn't own serves its own copy if it has one; otherwise it fetches the data without admitting it to disk.

### Read path

1. The Gateway authenticates and authorizes the request.
2. The Gateway looks up the object's metadata (size, ETag and response headers) in its cache.
   - On a hit, it sends each range straight to the node that owns it.
   - On a miss, it sends the request to the object's home, which returns the metadata with any requested bytes from the head or tail. Suffix ranges (`bytes=-N`) resolve there. Ranges in the middle of the object take a second hop, unless the home has no metadata yet. In that case, the home's first fetch from S3 requests exactly those bytes and streams them back without admitting them.
3. The Gateway fetches ranges that span several chunks from their owners in parallel. Every request carries the object's ETag.
4. When an owner misses a block, it:
   - merges concurrent misses for that block into one fetch;
   - asks the previous owner first, if the ring changed within the grace window;
   - otherwise fetches from S3 with `If-Match: <etag>`, combining adjacent missing blocks into one range GET.
5. Readers can stream a block while it is still filling.
6. The response starts once every part has answered, and the parts' bodies follow in order. If an owner fails, times out, or its body ends early, even partway through a response, the Gateway fetches the rest through the next rendezvous candidate, which reads from S3 with `Range` and `If-Match`. If S3 no longer holds that version, the response ends early and the client retries. A fill whose S3 body ends early stores nothing.

### Consistency

- **No mixed versions.** Blocks are keyed by `(bucket, key, etag, block_size, block_index)`, so a response never combines bytes from two versions of an object.
- **Validated fills.** The home's first fetch of an object is unconditional, and its response sets the metadata: ETag, headers, and size from `Content-Range`. The home merges concurrent first fetches for a key into one. After a restart, a home that still holds some of an object's blocks fetches only the metadata with a HEAD, then serves the blocks. S3 checks preconditions before ranges, so when a first fetch returns 416 for a request with preconditions, the home fetches the metadata with a HEAD and answers the request itself. Every later fill carries `If-Match` with that ETag. A 412 or 404 drops the metadata; any other S3 error passes to the client and leaves the metadata in place. If the response hasn't started, the Gateway restarts the read; otherwise it ends the response early, and the client's retry reads the new version.
- **Freshness mode**, set per bucket or prefix:
  - `immutable`: never revalidate. Use for content-addressed or never-overwritten keys.
  - `ttl`: after a set age, revalidate metadata with a HEAD carrying `If-None-Match`.
  - `events`: S3 Event Notifications invalidate metadata.
- **Gateway metadata cache.** Gateways keep object metadata in a bounded LRU and answer `HeadObject` and preconditions from it. Entries for `immutable` objects last until evicted; others expire after a short TTL, and never outlive the bucket's TTL counted from when the home last confirmed them. A stale entry is safe: an owner serves the old version consistently, or its fill fails `If-Match`. Then the Gateway drops the entry and retries through the home, telling it the ETag failed, so the home revalidates instead of handing the ETag out again. After a few such retries, the home reads S3 directly and relays the answer uncached, so a read of an object that keeps changing still finishes. Answers to reads sent before a write through the Gateway never restore its entry.
- **Writes go through the object's home.** Once the write succeeds, the home drops the metadata, or replaces it when warming on write. It discards any first fetch that started before then, so a read racing the write can't pin the old version. It forwards the invalidation to replica holders. The Gateway that proxied the write drops its own cached entry; other gateways catch up when their entries expire. Clusters in other zones see the change through their freshness mode.
- **Versioned reads.** Requests with a `versionId` are immutable and are cached without revalidation.
- **No negative caching.** Misses (404s) aren't cached, which preserves S3's read-after-write guarantee for new keys. Requests queued at the home behind a first fetch share its 404 only if they arrived before the fetch was sent, since S3 checked after they did.

### Storage

The core decides what a node admits and evicts and which slot each block fills. The server writes blocks and serves them with `sendfile`. The simulator therefore tests the cache policy and measures its hit rates.

- **Layout:** preallocated files divided into 64 MiB extents. Each extent holds fixed-size slots of one size class, in powers of two from 4 KiB to 1 MiB. A block fills one slot, so it is a `(file, offset, length)` that `sendfile` serves directly. Freeing a block frees its slot, and the store never compacts or rewrites data. Rounding costs space: a 2 KiB manifest fills a 4 KiB slot.
- **Size classes share the disk.** Extents move between classes as demand shifts. To give a class more room, the store empties an extent from another class by evicting its blocks.
- **Object metadata** (size, ETag and headers) lives in the home's memory, keyed by bucket and key. A home keeps metadata for a bounded number of objects and drops the least recently used; a dropped entry costs one first fetch.
- **Index:** an in-memory map from block to slot, at about 100 bytes per block, so 4 TB of 1 MiB blocks needs about 400 MB. A slot table on disk, with one fixed-size record per slot, persists it. The store writes a block, syncs it, then writes its record, and it clears a slot's record before reusing the slot.
- **Restarts and crashes:** the index survives restarts, so rolling deploys keep the cache warm. Each record names the run of the node that wrote or last verified its block. A clean shutdown marks the table with the earliest run whose records are all sound: its own, or the mark it started from. The next start trusts records from that run on. Every other block's first hit reads it and verifies its checksum before `sendfile` serves it, then records it under the new run; a mismatch drops the block and counts as a miss.
- **Memory:** the OS page cache holds hot blocks, and `sendfile` serves them from it. The kernel ranks pages read twice above pages read once, which shields hot blocks from scans.
- **Admission:** owners and hot-key replicas admit blocks to disk.
  - **Doorkeeper** (default): a Bloom filter whose entries age out each window. A block's first read streams to the reader without touching disk and marks the filter. A second read within the window admits the block. Each admitted block costs a second S3 GET, and blocks read once stay off the drive.
  - **Admit on first read** (per bucket or prefix): new blocks go straight to disk. Use it when nearly everything is reread.
  - Blocks fetched from a previous owner, warmed on write or prefetched skip the doorkeeper.
  - **Fill budget:** a node caps the bytes it fills at once. Past the cap, misses stream from S3 without admission.
- **Warming on write** (per bucket or prefix): a `PutObject` passes through the object's home, which keeps chunk 0 and the final 16 MiB as it forwards the body. After S3 accepts the write, the home reads the object's metadata with a HEAD. If the HEAD's ETag matches the write's, the home indexes the blocks under it, so the first read hits; otherwise it discards them.
- **Metadata prefetch:** some formats state their metadata's length at a fixed spot: Parquet and ORC in their trailers, safetensors in its first 8 bytes. When a read touches that spot, the home fills every block the metadata spans with one range GET, before the reader asks for it.
- **Eviction:** S3-FIFO over blocks: a small FIFO holding about 10% of capacity, a main FIFO, and a ghost queue of recently evicted keys. A hit bumps a 2-bit counter and moves nothing. Once a ring change's grace window ends, blocks the node no longer owns go first. Each block stores its placement hash, so the node rechecks ownership without the object's key.
- **Blocks need no TTL.** They are keyed by ETag, so they never go stale: a changed object gets new blocks, and the old ones stop being read and age out. Freshness applies to metadata only. To meet a retention rule, such as removing deleted data within a set time, a purge drops an object's blocks. The home knows the object's size and ETag, so it can reach every chunk owner.

### Hot keys

1. The owner tracks each key's request rate. Above a threshold, it grants time-limited leases to the key's next K rendezvous candidates.
2. Replicas fill from the owner.
3. Responses carry a hot hint with K and an expiry. Gateways cache the hint and spread reads across the owner and its replicas.
4. Replicas report their read counts to the owner when leases renew. The owner renews while the combined rate stays above half the promotion threshold.
5. Leases expire on their own, and losing one costs only hit rate.

### Auth

- **Client credentials:** the Gateway validates SigV4 headers and presigned URLs against its own credential store. Each credential maps to bucket and prefix grants.
- **Every read is authorized, hits included,** because S3 never sees a cache hit.
- **Access to S3:** only storage nodes hold S3 credentials, under the cluster's own role. The Gateway validates a write, decodes streaming uploads (aws-chunked bodies with trailing checksums) and sends the write to the object's home. The home checks the gateway's grants, re-signs the write and forwards it to S3 with its checksums intact.
- **Gateway identity:** each gateway authenticates to storage nodes with its own identity (mTLS or signed request tokens), and storage nodes enforce that gateway's grants. A compromised gateway can reach only what its grants allow.
- **SSE-C:** requests using customer-provided encryption keys bypass the cache.

### S3 API

- **Served from cache:** `GetObject` (including ranges, conditionals and `versionId`) and `HeadObject`.
- **Proxied to S3:** every other operation.
- **Response overrides** (`response-content-*`) are applied per request.
- **Gateways route requests themselves,** so clients never see redirects.

## Implementation

The system is written in Rust.

- **HTTP/1.1 server:** a purpose-built server, starting from [rust_http_router_template](https://github.com/danthegoodman1/rust_http_router_template), parses requests and writes response headers, then hands the body to the kernel. S3 clients speak HTTP/1.1 and every response has a known `Content-Length`, so bodies need no framing.
- **Zero-copy:** storage nodes serve blocks with `sendfile`, and gateways relay peer responses to clients with `splice`. `sendfile` blocks when a block isn't in the page cache, so it runs on worker threads or io_uring, off the async event loop.
- **TLS:** rustls runs the handshake, and the `ktls` crate moves the session into the kernel, so zero-copy works under TLS. A peer's TLS 1.3 KeyUpdate ends the connection, and the client reconnects.
- **Membership:** a Rust gossip library, such as foca (SWIM) or chitchat.
- **Peer links** run on a private network: plaintext with signed request tokens, or mTLS over kTLS where policy requires encryption.
- **Reference designs:** TAG and ocache (Go) implement versioned block caching, request coalescing, SigV4 validation, warming on write and Parquet footer prefetch. Read them before building those parts.

## Open questions

- **Workload targets:** object-size mix, request rate, working-set size, and hit-rate and latency goals. These set the chunk size, block size and hot-key thresholds.
- **Chunk size:** larger chunks mean fewer hops per read; smaller chunks spread load more evenly.
- **Grace window:** how long to keep previous-owner fallback after a ring change.
- **Storage layout:** revisit once the simulator and NVMe benchmarks produce numbers. It carries three risks: rebalancing size classes evicts every block in an extent, hot ones included, and a reservation first evicts up to eight blocks of any class before it empties an extent; the kernel decides what stays in memory; and fills land as random writes, which wear flash faster in small slots. A log-structured store is the fallback if these bite.
