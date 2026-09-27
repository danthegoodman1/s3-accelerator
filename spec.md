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

**1. The cache survives resizing.** Ring ownership decides what a node writes to disk, never what it may serve. After a ring change, the new owner fetches missing blocks, and a new home the metadata, from the previous owner before going to S3. A node being removed keeps serving those fetches for a fallback window.

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

- **Membership** runs SWIM gossip among storage nodes only. Each node derives an immutable ring snapshot from what it hears: the nodes up, and those declared down within the down grace period, less any that are leaving. A ring's version is a hash of its members and their weights, so nodes that agree on the members agree on the version. A node keeps its previous ring for the fallback window after a change; changes that follow while the window lasts, such as those a joining node sees as it hears of the others, extend the window and keep the ring from before the first. Every ten probe periods, a node announces itself again to the seeds it doesn't hear from, so a lost announcement or a healed partition doesn't leave the cluster split.
- **Joining:** a starting node asks its seeds for their ring before it announces itself. A ring that lacks the node means the node is new, and that ring becomes its previous one, so it reads what it takes over from the nodes that held it. A restarted node finds itself in the ring and reads nothing from others.
- **Leaving:** a node told to leave drops out of every ring at once, keeps serving its blocks to their new owners through the fallback window, and then stops.
- **Gateways fetch the ring** over HTTP from storage nodes. Every storage response carries the version of its node's ring, and a gateway fetches the ring from a node whose version differs from its own, one fetch at a time; a fetch that fails lets the next answer ask again. A ring names each node's address, so gateways and nodes reach nodes their configs never named. A gateway whose ring names no node that answers asks the nodes it knows of for a ring. Only storage nodes gossip, so adding gateways adds no membership traffic.
- **Placement** uses weighted rendezvous hashing over stable node IDs. It moves few keys when membership changes, weights nodes by disk size, and gives each key an ordered candidate list that doubles as its replica set. Each home or chunk reduces to a 64-bit placement hash, and a node's score mixes that hash with the node's ID.
- **Blocks and chunks:**
  - A **block** (1 MiB) is the unit of fill, storage and eviction.
  - A **chunk** (16 MiB) is the unit of placement.
- **Object home** = `rendezvous(bucket, key)`. The home holds the object's metadata, chunk 0 and every block overlapping the object's final 16 MiB. Most file formats keep their metadata at the head or tail (Parquet and ORC footers, safetensors headers), so the home serves those reads in one hop. Objects up to 32 MiB live entirely on their home.
- **Other chunks** belong to `rendezvous(bucket, key, chunk_index)`. Large objects spread across the cluster, and a large read fans out to several owners in parallel.
- **Ownership costs disk only for blocks readers touch.** Blocks fill on read, so a large tail region reserves no space.
- **Unresponsive nodes** stay in the ring for the down grace period while gateways route around them. A brief failure therefore doesn't reshuffle ownership. A gateway fails over from a node that times out, answers 5xx or ends a body early to the next rendezvous candidate. Only the home keeps an object's metadata, since writes reach only the home, so a candidate standing in for it reads S3 directly and caches nothing. The gateway marks such a read, so the candidate stands in even when its own ring names it the home.
- **Disagreement about the ring** costs duplicate fills, never wrong data. A node asked for a chunk it doesn't own serves its own copy if it has one; otherwise it fetches the data without admitting it to disk.

### Read path

1. The Gateway authenticates and authorizes the request.
2. The Gateway looks up the object's metadata (size, ETag and response headers) in its cache.
   - On a hit, it sends each range straight to the node that owns it.
   - On a miss, it sends the request to the object's home, which returns the metadata with any requested bytes from the head or tail. Suffix ranges (`bytes=-N`) resolve there. Ranges in the middle of the object take a second hop, unless the home has no metadata yet. In that case, the home's first fetch from S3 requests exactly those bytes and streams them back, storing the whole blocks the admission policy accepts.
   - A home without the metadata asks the object's previous home first, within the fallback window. The metadata counts as validated when the home asked for it, less its age, and a change the new home learned of since then, from a write or from S3's answer, makes it useless.
3. The Gateway fetches ranges that span several chunks from their owners in parallel. Every request carries the object's ETag.
4. When an owner misses a block, it:
   - merges concurrent misses for that block into one fetch;
   - within the fallback window, asks the block's previous owner first, which answers only from blocks it holds; a previous owner that lacks them, or doesn't answer within the peer timeout, sends the fill to S3, and the node stops asking it until the next ring change;
   - otherwise fetches from S3 with `If-Match: <etag>`, combining adjacent missing blocks, up to a chunk, into one range GET.
5. Readers that arrive while a fill is in flight share its body, which the owner holds until its readers finish; a chunk bounds it. A first fetch's body streams through the home without being held, so only the requests queued behind the first fetch share it, as its head arrives. A later reader waits for the block to be written and reads its slot, or fetches the block again.
6. The response starts once every part has answered, and the parts' bodies follow in order. If an owner fails, times out, or its body ends early, even partway through a response, the Gateway fetches the rest through the next rendezvous candidate, which reads from S3 with `Range` and `If-Match`. If S3 no longer holds that version, the response ends early and the client retries. A fill whose S3 body ends early stores nothing.

### Consistency

- **No mixed versions.** Blocks are keyed by `(bucket, key, etag, block_size, block_index)`, so a response never combines bytes from two versions of an object.
- **Validated fills.** The home's first fetch of an object is unconditional, and its response sets the metadata: ETag, headers, and size from `Content-Range`. The home merges concurrent first fetches for a key into one. After a restart, a home that still holds some of an object's blocks fetches only the metadata with a HEAD, then serves the blocks. S3 checks preconditions before ranges, so when a first fetch returns 416 for a request with preconditions, the home fetches the metadata with a HEAD and answers the request itself. Every later fill carries `If-Match` with that ETag. A 412 or 404 drops the metadata; any other S3 error passes to the client and leaves the metadata in place. If the response hasn't started, the Gateway restarts the read; otherwise it ends the response early, and the client's retry reads the new version.
- **Freshness mode**, set per bucket or prefix:
  - `immutable`: never revalidate. Use for content-addressed or never-overwritten keys.
  - `ttl`: after a set age, revalidate metadata with a HEAD carrying `If-None-Match`.
  - `events`: S3 Event Notifications invalidate metadata as they arrive, and the bucket's TTL, set long, revalidates metadata should an event go missing.
- **Gateway metadata cache.** Gateways keep object metadata in a bounded LRU and answer `HeadObject` and preconditions from it. Entries for `immutable` objects last until evicted; others expire after a short TTL, and never outlive the bucket's TTL counted from when the home last confirmed them. A stale entry is safe: an owner serves the old version consistently, or its fill fails `If-Match`. Then the Gateway drops the entry and retries through the home, telling it the ETag failed, so the home revalidates instead of handing the ETag out again. After a few such retries, the home reads S3 directly and relays the answer uncached, so a read of an object that keeps changing still finishes.
- **Writes go through the object's home.**
  - A gateway passes every request the cache doesn't serve through a storage node, which re-signs it for S3: an object's request through its home, and a bucket's through the node rendezvous picks for the bucket. While the gateway routes around a home, it sends the home's writes to the next candidate, which passes each one on to the home.
  - Once S3 accepts a write, and before the gateway hears, the home drops the metadata, or replaces it when warming on write. It discards any first fetch that started before then, so a read racing the write can't pin the old version. It forwards the invalidation to replica holders.
  - Within the fallback window after a ring change, a gateway on either ring tells only the home that ring names, so each home passes the write to the home under its other ring.
  - The Gateway that proxied the write drops its own cached entry, and ignores answers to reads it sent before the write, however full its cache. A write that reached a node other than the home may never reach the home, so the Gateway reads that key directly from S3 until the bucket's TTL has passed. A read through the Gateway after its write succeeds therefore sees the write, as long as the Gateway's ring stays the same.
  - Other gateways catch up when their entries expire. Clusters in other zones see the change through their freshness mode.
- **Event notifications.** S3 sends a bucket's notifications to an SQS queue, which every storage node long-polls. A node passes each event to the key's home, and within the fallback window to its previous home too, and deletes the message once each home has it. A message a node fails to finish returns to the queue after its visibility timeout, and a repeat changes nothing. A home keeps metadata whose ETag the event names, and otherwise drops it as it would for a write. Gateways hear of no events, so a read sees a change once its event is handled and the gateway's entries from before have expired, as long as the gateway routes by the ring of the node that handled it.
- **Versioned reads.** Requests with a `versionId` are immutable and are cached without revalidation.
- **No negative caching.** Misses (404s) aren't cached, which preserves S3's read-after-write guarantee for new keys. Requests queued at the home behind a first fetch share its 404 only if they arrived before the fetch was sent, since S3 checked after they did.

### Storage

The core decides what a node admits and evicts and which slot each block fills. The server writes blocks and serves them with `sendfile`. The simulator therefore tests the cache policy and measures its hit rates.

- **Layout:** preallocated files divided into 64 MiB extents. Each extent holds fixed-size slots of one size class, in powers of two from 4 KiB to 1 MiB. A block fills one slot, so it is a `(file, offset, length)` that `sendfile` serves directly. Freeing a block frees its slot, and the store never compacts or rewrites data. Rounding costs space: a 2 KiB manifest fills a 4 KiB slot.
- **Size classes share the disk.** Extents move between classes as demand shifts. To give a class more room, the store empties an extent from another class by evicting its blocks.
- **Object metadata** (size, ETag and headers) lives in the home's memory, keyed by bucket and key. A home keeps metadata for a bounded number of objects and drops the least recently used; a dropped entry costs one first fetch. Homes also append immutable buckets' metadata to a metadata file, and a start loads the most recently used entries up to capacity, so a restart costs no S3 request for those objects. Other buckets' metadata would be past its TTL after a restart, so it stays in memory.
- **Index:** an in-memory map from block to slot, at about 100 bytes per block, so 4 TB of 1 MiB blocks needs about 400 MB. A slot table on disk, with one fixed-size record per slot, persists it. A record names the block's version by a 128-bit hash of bucket, key and ETag, so every record has the same size whatever the key's length. The store writes a block, syncs it, then writes its record, and it clears a slot's record before reusing the slot.
- **Restarts and crashes:** the index survives restarts, so rolling deploys keep the cache warm. Each record names the run of the node that wrote or last verified its block. A clean shutdown marks the table with the earliest run whose records are all sound: its own, or the mark it started from. The next start trusts records from that run on. Every other block's first hit reads it and verifies its checksum before `sendfile` serves it, then records it under the new run; a mismatch drops the block and counts as a miss.
- **Memory:** the OS page cache holds hot blocks, and `sendfile` serves them from it. The kernel ranks pages read twice above pages read once, which shields hot blocks from scans.
- **Admission:** owners and hot-key replicas admit blocks to disk.
  - **Doorkeeper** (default): a Bloom filter whose entries age out each window. A block's first read streams to the reader without touching disk and marks the filter. A second read within the window admits the block. Each admitted block costs a second S3 GET, and blocks read once stay off the drive.
  - **Admit on first read** (per bucket or prefix): new blocks go straight to disk. Use it when nearly everything is reread.
  - Blocks fetched from a previous owner, warmed on write or prefetched skip the doorkeeper.
  - **Fill budget:** a node caps the bytes it fills at once. Past the cap, misses stream from S3 without admission.
- **Warming on write** (per bucket): a `PutObject` passes through the object's home, which keeps chunk 0 and the final 16 MiB as it forwards the body, up to 256 MiB of uploads at once. After S3 accepts the write, the home reads the object's metadata with a HEAD. If the HEAD's ETag matches the write's, the home stores the blocks under it and keeps the metadata, unless it fetched some since the write, so the first read hits; otherwise it discards them.
- **Metadata prefetch:** some formats state their metadata's length at a fixed spot: Parquet and ORC in their trailers, safetensors in its first 8 bytes. When a read touches that spot, the home fills every block the metadata spans with one range GET, before the reader asks for it.
- **Eviction:** S3-FIFO over blocks: a small FIFO holding about 10% of capacity, a main FIFO, and a ghost queue of recently evicted keys. A hit bumps a 2-bit counter and moves nothing. Once the fallback window after a ring change ends, blocks the node no longer owns go first. Each block stores its placement hash, so the node rechecks ownership without the object's key.
- **Blocks need no TTL.** They are keyed by ETag, so they never go stale: a changed object gets new blocks, and the old ones stop being read and age out. Freshness applies to metadata only. To meet a retention rule, such as removing deleted data within a set time, a purge drops an object's blocks and metadata. The home knows the object's size and ETag, so it can reach every chunk owner. Each node makes the purge durable before it confirms, and the home resends it to owners that were down until each confirms.

### Hot keys

1. The owner of a placement counts its reads over a short window. Above a threshold, it grants time-limited leases to the placement's next K rendezvous candidates.
2. Replicas fill from the owner before S3, and admit the placement's blocks while leased, without the doorkeeper.
3. Answers carry a hot hint naming the owner, the replicas and the leases' end. Gateways spread a hot placement's range reads across them in turn; reads that need the home's metadata stay on the home.
4. Replicas report their read counts to the owner halfway through a lease. Three quarters through, the owner renews while the combined rate stays above half the promotion threshold.
5. Leases expire on their own, and losing one costs only hit rate.

### Auth

- **Client credentials:** the Gateway validates SigV4 headers and presigned URLs against its own credential store. Each credential maps to bucket and prefix grants.
- **Every read is authorized, hits included,** because S3 never sees a cache hit.
- **Access to S3:** only storage nodes hold S3 credentials, under the cluster's own role. The Gateway authenticates and authorizes each request the cache doesn't serve, decodes streaming uploads (aws-chunked bodies with trailing checksums), and passes it through the object's home, or the node a bucket's request goes through. The node checks the gateway's grants, re-signs the request and forwards it to S3 with its checksums intact.
- **Gateway identity:** each gateway authenticates to storage nodes with its own identity (mTLS or signed request tokens), and storage nodes enforce that gateway's grants. A compromised gateway can reach only what its grants allow.
- **SSE-C:** requests using customer-provided encryption keys bypass the cache.

### S3 API

- **Served from cache:** `GetObject` (including ranges, conditionals and `versionId`) and `HeadObject`.
- **Proxied to S3** through storage nodes: every other operation.
- **Response overrides** (`response-content-*`) are applied per request.
- **Gateways route requests themselves,** so clients never see redirects.

## Implementation

The system is written in Rust.

- **HTTP/1.1 server:** a purpose-built server, starting from [rust_http_router_template](https://github.com/danthegoodman1/rust_http_router_template), parses requests and writes response headers, then hands the body to the kernel. S3 clients speak HTTP/1.1, and every response the cache serves has a known `Content-Length`, so its body needs no framing. A passed-through S3 response without one goes out chunked.
- **Streaming bodies:** memory never grows with object size. Uploads pass through a storage node to S3 as they arrive; when the client signed the payload's SHA-256, the gateway holds back the last bytes until the whole body matches, so neither the node nor S3 receives a body that fails its hash. S3's responses pass back the same way, and a gateway relays a chunked one chunk by chunk. A first fetch's body passes through the home in lockstep: each chunk reaches every reader before the next is read, so the slowest reader paces S3, and each admitted block is written once its bytes are in. Fills, at most a chunk, are held until their readers finish. The one body a gateway reads whole is a `DeleteObjects` key list, to learn the keys it deletes; it refuses one over 8 MiB.
- **Event queue:** nodes speak SQS's JSON protocol over hyper, signed for `sqs` with the cluster's credentials. A message holds S3's event, or SNS's envelope around it.
- **S3 client:** hyper, with each request's path sent as written. S3 treats `.` and `..` as ordinary key segments, so a client that normalized them would read or write another key.
- **Zero-copy:** storage nodes serve blocks with `sendfile`, and gateways relay peer responses to clients with `splice`. `sendfile` blocks when a block isn't in the page cache, so it runs on worker threads or io_uring, off the async event loop. `splice` between sockets never blocks, so it runs on the event loop through a pipe. A gateway reads a peer response's head without reading past it, so the body stays in the socket for `splice`.
- **Pages in flight:** `sendfile` and `splice` pass references to page-cache pages. A page stays in use after the call returns, until the peer acknowledges it or, over loopback, until the reader reads it, and a write into it would change bytes already sent. Before a write overwrites a slot, the node asks the kernel to drop the slot's pages; the kernel keeps only pages that a socket or pipe still references, so a page still cached means the write waits. After 30 seconds it gives up, and the block counts as unwritten. The node reads the slab file with `FADV_RANDOM`, so every cached folio lies within one slot, and slots are multiples of the page size. The data directory must be on a disk-backed filesystem: tmpfs pages never leave the page cache.
- **TLS:** rustls runs the handshake, and the `ktls` crate moves the session into the kernel, so zero-copy works under TLS. A peer's TLS 1.3 KeyUpdate ends the connection, and the client reconnects.
- **Membership:** foca (SWIM) in the core, fed packets and timer events by its owner. Nodes gossip over UDP on their cluster addresses, and carry their addresses in their identities. A node resolves each address as it learns it, off its event loop; one that doesn't resolve costs only that node's gossip.
- **Peer links** run on a private network: plaintext with signed request tokens, or mTLS over kTLS where policy requires encryption.
- **Reference designs:** TAG and ocache (Go) implement versioned block caching, request coalescing, SigV4 validation, warming on write and Parquet footer prefetch. Read them before building those parts.

## Open questions

- **Workload targets:** object-size mix, request rate, working-set size, and hit-rate and latency goals. These set the chunk size, block size and hot-key thresholds.
- **Chunk size:** larger chunks mean fewer hops per read; smaller chunks spread load more evenly.
- **Fallback window:** how long to keep previous-owner fallback after a ring change. The default is ten minutes.
- **Storage layout:** revisit once the simulator and NVMe benchmarks produce numbers. It carries three risks: rebalancing size classes evicts every block in an extent, hot ones included, and a reservation first evicts up to eight blocks of any class before it empties an extent; the kernel decides what stays in memory; and fills land as random writes, which wear flash faster in small slots. A log-structured store is the fallback if these bite.
