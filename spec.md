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

- **Membership** runs SWIM gossip among storage nodes only. Each node derives an immutable ring snapshot from what it hears: the nodes up, and those declared down within the down grace period, less any that are leaving. A ring's version is a hash of its members and their weights, so nodes that agree on the members agree on the version. A node keeps its previous ring for the fallback window after a change; changes that follow while the window lasts, such as those a joining node sees as it hears of the others, extend the window and keep the ring from before the first. A starting node counts the nodes its config names as down until it hears from them, so each holds its placements for the down grace period. Every ten probe periods, a node announces itself again to the seeds it doesn't hear from, so a lost announcement or a healed partition doesn't leave the cluster split.
- **Joining:** a starting node asks its seeds for their ring before it announces itself. A ring that lacks the node means the node is new, and that ring becomes its previous one, so it reads what it takes over from the nodes that held it. A restarted node finds itself in the ring and reads nothing from others.
- **Leaving:** a node told to leave, by `SIGUSR1`, drops out of every ring at once, keeps serving its blocks to their new owners through the fallback window, and then stops.
- **Gateways fetch the ring** over HTTP from storage nodes. Every storage response carries the version of its node's ring, and a gateway fetches the ring from a node whose version differs from its own, one fetch at a time; a fetch that fails lets the next answer ask again. A ring names each node's address, so gateways and nodes reach nodes their configs never named. A gateway whose ring names no node that answers asks the nodes it knows of for a ring, and so does a starting gateway, once a suspect window, until a node answers it. Only storage nodes gossip, so adding gateways adds no membership traffic.
- **Placement** uses weighted rendezvous hashing over stable node IDs. It moves few keys when membership changes, weights each node by the `weight` its config gives, typically its disk size, and gives each key an ordered candidate list that doubles as its replica set. Each home or chunk reduces to a 64-bit placement hash, and a node's score mixes that hash with the node's ID.
- **Blocks and chunks:**
  - A **block** (1 MiB) is the unit of fill, storage and eviction.
  - A **chunk** (16 MiB) is the unit of placement.
- **Object home** = `rendezvous(bucket, key)`. The home holds the object's metadata, chunk 0 and every block overlapping the object's final 16 MiB. Most file formats keep their metadata at the head or tail (Parquet and ORC footers, safetensors headers), so the home serves those reads in one hop. Objects up to 32 MiB live entirely on their home.
- **Other chunks** belong to `rendezvous(bucket, key, chunk_index)`. Large objects spread across the cluster, and a large read fans out to several owners in parallel.
- **Ownership costs disk only for blocks readers touch.** Blocks fill on read, so a large tail region reserves no space.
- **Unresponsive nodes** stay in the ring for the down grace period while gateways route around them. A brief failure therefore doesn't reshuffle ownership. A gateway fails over from a node that refuses a connection, times out, answers 5xx or ends a body early to the next rendezvous candidate. It routes around a node for `suspect_ttl_ms` (10 seconds by default) after a connection to it fails or a request waits `node_timeout_ms` (150 seconds by default, time for a node to wait out one S3 timeout and fetch again), and for as long as membership holds the node down. Every node's answer carries a version of the nodes its membership declared down, leaving out those it has yet to hear from, since a node that just started has heard from none; a gateway whose version differs fetches the ring, which lists them, and sends requests waiting on a newly down node to the next candidate. While nodes' views differ, a change in them alone fetches the ring at most once a suspect window, and a node that answers is up, whatever a ring said. Only the home keeps an object's metadata, since writes reach only the home, so a candidate standing in for it reads S3 directly and caches nothing. The gateway marks such a read, so the candidate stands in even when its own ring names it the home.
- **Disagreement about the ring** costs duplicate fills, never wrong data. A node asked for a chunk it doesn't own serves its own copy if it has one; otherwise it fetches the data without admitting it to disk.

### Read path

1. The Gateway authenticates and authorizes the request.
2. The Gateway looks up the object's metadata (size, ETag and response headers) in its cache.
   - On a hit, it sends each range straight to the node that owns it.
   - On a miss, it sends the request to the object's home, which returns the metadata with any requested bytes from the head or tail. Suffix ranges (`bytes=-N`) resolve there. Ranges in the middle of the object take a second hop, unless the home has no metadata yet. In that case, the home's first fetch from S3 requests exactly those bytes and streams them back, storing the whole blocks the admission policy accepts.
   - A home without the metadata asks the object's previous home first, within the fallback window. The metadata counts as validated when the home asked for it, less its age, and a change the new home learned of since then, from a write or from S3's answer, makes it useless. A home that ran before and restarted forgot the changes it had learned of, so it takes none validated before its restart.
3. The Gateway fetches ranges that span several chunks from their owners in parallel, up to `read_ahead` bytes (64 MiB by default) ahead of the part it forwards, asking for more as parts finish, so a miss holds at most that much of each owner's fill budget. Consecutive chunks with one owner share a request; while more remains beyond the window, each request takes at most half of it, so the next is asked for while one streams. Every request carries the object's ETag.
4. When an owner misses a block, it:
   - merges concurrent misses for that block into one fetch;
   - within the fallback window, asks the block's previous owner first, which answers only from blocks it holds; a previous owner that lacks them sends the fill to S3; so does one that doesn't answer within the peer timeout, and the node stops asking that one until the next ring change;
   - otherwise fetches from S3 with `If-Match: <etag>`, combining adjacent missing blocks, up to a chunk, into one range GET.
5. Readers that arrive while a fill is in flight share its body, which the owner holds until its readers finish; a chunk bounds it. A first fetch's body streams through the home without being held, so only the requests queued behind the first fetch share it, as its head arrives. A later reader waits for the block's bytes to reach the slab file and reads its slot, or fetches the block again; it reads before the sync that makes the bytes durable, which a drive slow to flush can take seconds over, and the block is recorded once they are. A block whose sync fails is fetched again by later reads, and its slot is freed once the reads already sending it end.
6. The response starts once every part asked for so far has answered, and the parts' bodies follow in order. If an owner fails, times out, or its body ends early, even partway through a response, the Gateway fetches the rest through the next rendezvous candidate, which reads from S3 with `Range` and `If-Match`. If S3 no longer holds that version, as after a write that lands while the response streams, or one before it that the Gateway's metadata, still fresh enough to use, predates, the response ends early and the client retries. A fill whose S3 body ends early stores nothing.

### Consistency

- **No mixed versions.** Blocks are keyed by `(bucket, key, etag, block_size, block_index)`, so a response never combines bytes from two versions of an object.
- **Validated fills.** The home's first fetch of an object is unconditional, and its response sets the metadata: ETag, headers, and size from `Content-Range`. The home merges concurrent first fetches for a key into one. After a restart, a home that still holds some of an object's blocks fetches only the metadata with a HEAD, then serves the blocks. S3 checks preconditions before ranges, so when a first fetch returns 416 for a request with preconditions, the home fetches the metadata with a HEAD and answers the request itself. Every later fill carries `If-Match` with that ETag. A 412 or 404 drops the metadata; any other S3 error passes to the client and leaves the metadata in place. If the response hasn't started, the Gateway restarts the read; otherwise it ends the response early, and the client's retry reads the new version.
- **Freshness**, set per bucket in `[cache.buckets.<name>]`, or for every other bucket in `[cache.default_policy]`:
  - `immutable = true`: never revalidate. Use for content-addressed or never-overwritten keys.
  - `ttl_ms` (5 seconds by default): after that age, revalidate metadata with a HEAD carrying `If-None-Match`.
  - With `[events]`, S3 Event Notifications invalidate metadata as they arrive, and a bucket's TTL, set long, revalidates metadata should an event go missing.
- **Gateway metadata cache.** Gateways keep object metadata in a bounded LRU and answer `HeadObject` and preconditions from it. Entries for `immutable` objects last up to the purge window (`purge_window_ms`, an hour by default), so a purge reaches every gateway within it; others expire after a short TTL, and never outlive the bucket's TTL counted from when the home last confirmed them. A stale entry is safe: an owner serves the old version consistently, or its fill fails `If-Match`. Then the Gateway drops the entry and retries through the home, telling it the ETag failed, so the home revalidates instead of handing the ETag out again. After a few such retries, the home reads S3 directly and relays the answer uncached, so a read of an object that keeps changing still finishes.
- **Writes go through the object's home.**
  - A gateway passes every request the cache doesn't serve through a storage node, which re-signs it for S3: an object's request through its home, and a bucket's through the node rendezvous picks for the bucket. While the gateway routes around a home, it sends the home's writes to the next candidate, which passes each one on to the home.
  - Once S3 accepts a write, and before the gateway hears, the home drops the metadata and discards any first fetch that started before then, so a read racing the write can't pin the old version. A discarded fetch still answers the read that started it, which arrived before the write, so writes that keep coming can't starve it; reads that joined the fetch are served again. When warming on write, the home stores new metadata once a HEAD confirms the write's ETag.
  - Within the fallback window after a ring change, a gateway on either ring tells only the home that ring names, so each home passes the write to the home under its other ring.
  - The Gateway that proxied the write drops its own cached entry, and ignores answers to reads it sent before the write, however full its cache. A write that reached a node other than the home may never reach the home, so the Gateway reads that key directly from S3 until the bucket's TTL has passed. A read through the Gateway after its write succeeds therefore sees the write, as long as the Gateway's ring stays the same.
  - Other gateways catch up when their entries expire. Clusters in other zones see the change through their freshness mode.
- **Event notifications.** S3 sends buckets' notifications to the SQS queue that `[events]` names, which every storage node long-polls. A node passes each event to the key's home, and within the fallback window to its previous home too, and deletes the message once each home has it; a ring the node adopts while an event is under way adds its home to those that must hear. A message a node fails to finish returns to the queue after its visibility timeout, and a repeat changes nothing. A home keeps metadata whose ETag the event names, and otherwise drops it as it would for a write. Gateways hear of no events, so a read sees a change once its event is handled and the gateway's entries from before have expired, as long as the gateway routes by the ring of the node that handled it.
- **Versioned reads.** Requests with a `versionId` pass through to S3.
- **No negative caching.** Misses (404s) aren't cached, which preserves S3's read-after-write guarantee for new keys. Requests queued at the home behind a first fetch share its 404 or 5xx only if they arrived before the fetch was sent, since S3 answered after they did.

### Storage

The core decides what a node admits and evicts and which slot each block fills. The server writes blocks and serves them with `sendfile`. The simulator therefore tests the cache policy and measures its hit rates.

- **Layout:** a slab file preallocated at start, so a filesystem without the room stops the start rather than a fill, divided into extents, by default one block (1 MiB) each. Each extent holds fixed-size slots of one size class, in powers of two from 4 KiB to 1 MiB. A block fills one slot, so it is a `(file, offset, length)` that `sendfile` serves directly. Freeing a block frees its slot, and the store never compacts or rewrites data. Rounding costs space: a 2 KiB manifest fills a 4 KiB slot.
- **Size classes share the disk.** Extents move between classes as demand shifts. To give a class more room, the store takes a free extent, or evicts blocks by S3-FIFO, up to eight, until one frees a slot of the class or a whole extent. If none does, it empties the extent the last of them left: eviction chose that block as the coldest, and a one-block extent holds at most a block's worth of others. While a block in that extent is filling or pinned, it empties another victim's extent, or else the idle extent that holds the fewest blocks. Extents larger than a block give up hot blocks when classes shift: a hot set that shares extents with cold blocks loses blocks as small objects move in (see `BENCHMARKS.md`).
- **Why this layout:** NVMe benchmarks tested its risks (`BENCHMARKS.md`). With one-block extents, a shift to small objects costs a hot set no blocks; the page cache keeps rereads, and scans leave it; and a node writes about what it admits, with block writes sharing each sync. What small random writes cost a drive's wear is unmeasured; a log-structured store would make fills sequential if a drive's wear counters call for it.
- **Object metadata** (size, ETag and headers) lives in the home's memory, keyed by bucket and key. A home keeps metadata for a bounded number of objects and drops the least recently used; a dropped entry costs one first fetch. Homes also append immutable buckets' metadata to a metadata file, which they rewrite least recently used first, on a blocking thread, once it holds twice the capacity, and at a clean shutdown, so the file stays bounded and a start loads the most recently used entries up to capacity: a restart costs no S3 request for those objects. Other buckets' metadata would be past its TTL after a restart, so it stays in memory.
- **Index:** an in-memory map from block to slot. With the store full and evicting, it holds about 390 bytes per 1 MiB block in a one-block extent and about 310 per 4 KiB block (`crates/core/tests/memory.rs`), so 4 TB of 1 MiB blocks needs about 1.6 GB, and a disk full of 4 KiB blocks needs about 8% of its size. A slot table on disk persists it, with a 64-byte record for each 4 KiB of disk, the smallest slot. A start reads the whole table, 1/64 of the disk's size, with a thread per core and without checking empty records: 64 GB for a 4 TB disk, about 11 seconds on a drive that reads 6 GB/s. A record names the block's version by a 128-bit hash of bucket, key and ETag, so every record has the same size whatever the key's length. The store writes a block, syncs it, then writes its record, and it clears a slot's record before reusing the slot. Concurrent block writes share each sync: a write waits for a sync that began after it ended, and one sync serves every write waiting when it begins, since a sync per block holds a drive to a fraction of its write bandwidth. A write that a failed sync covered fails, whatever later syncs do.
- **Restarts and crashes:** the index survives restarts, so rolling deploys keep the cache warm. Records name slots by their offset, so when the slot sizes and the disk's size stay the same, shrinking the extent size keeps every block, and growing it keeps, in each merged extent, the blocks of the class restored there first; any other change to the slots starts the node cold. Each record names the run of the node that wrote or last verified its block. A clean shutdown marks the table with the earliest run whose records are all sound: its own, or the mark it started from. The next start trusts records from that run on. Every other block's first hit reads it and verifies its checksum before `sendfile` serves it, then records it under the new run; a mismatch drops the block and counts as a miss.
- **Memory:** the OS page cache holds hot blocks, and `sendfile` serves them from it. The kernel ranks pages read twice above pages read once, which shields hot blocks from scans.
- **Admission:** owners and hot-key replicas admit blocks to disk.
  - **Doorkeeper** (default): a pair of Bloom filters that remembers the blocks of the most recent first reads, at least `doorkeeper_window` of them (100,000 by default) and at most twice that. A block's first read streams to the reader without touching disk and marks the filter. A second read within the window admits the block. Each admitted block costs a second S3 GET, and blocks read once stay off the drive.
  - **Admit on first read** (per bucket): new blocks go straight to disk. Use it when nearly everything is reread.
  - Blocks a previous owner supplies, blocks of a placement leased to the node, and blocks warmed on write, at a format's spot or prefetched skip the doorkeeper. A fill whose previous owner lacks its blocks goes to S3 and passes the doorkeeper.
  - **Fill budget:** a node caps the bytes it fills at once, from a fill's start until its blocks are durable: 256 MiB by default. Past the cap, misses stream from S3 without admission. A gateway asks for a response's chunks a read-ahead window at a time, so each concurrent miss holds at most the window of the budget, and the default budget takes four misses of a full window at once.
- **Warming on write** (per bucket): a `PutObject` passes through the object's home, which keeps chunk 0 and the final 16 MiB as it forwards the body, up to 256 MiB of uploads at once. After S3 accepts the write, the home reads the object's metadata with a HEAD. If the HEAD's ETag matches the write's, the home stores the blocks under it and keeps the metadata, unless it fetched some since the write, so the first read hits; otherwise it discards them.
- **Metadata prefetch:** some formats state their metadata's length at a fixed spot: Parquet and ORC in their trailers, safetensors in its first 8 bytes. The home knows the format by the key's extension. When a read touches that spot, the home stores the spot's blocks. Once they are all stored, it reads the spot from them and fills every block the metadata spans, a range GET per placement it holds, before the reader asks for it, as far as the fill budget allows.
- **Eviction:** S3-FIFO over blocks: a small FIFO holding about 10% of capacity, a main FIFO, and a ghost queue of recently evicted keys. A hit bumps a 2-bit counter and moves nothing. Once the fallback window after a ring change ends, blocks the node no longer owns go first. Each block stores its placement hash, so the node rechecks ownership without the object's key.
- **Blocks need no TTL.** They are keyed by ETag, so they never go stale: a changed object gets new blocks, and the old ones stop being read and age out. Freshness applies to metadata only. To meet a retention rule, such as removing deleted data within a set time, a purge drops an object's blocks and metadata: a client sends `POST /bucket/key?x-accel-purge`. The object's home drops every version's blocks and erases their slots, and passes the purge to every other node in its rings, since any may hold blocks from earlier versions, ring changes or leases. A block being written or read loses its record at once, so no restart brings it back, and its slot goes once free. Each node makes the purge durable before it confirms, syncing the freed space, which it reserves again so the slab file stays preallocated, the slot table, the metadata file and the purge log. Gateways drop the object's cached metadata within the purge window. The home records durably which nodes have yet to confirm, and tells those in its rings again until each does, across its own restarts; a node that left the rings is told once it is back.

### Hot keys

1. The owner of a placement counts its reads over a short window. Above a threshold, it grants time-limited leases to the placement's next K rendezvous candidates.
2. Replicas fill from the owner before S3, and admit the placement's blocks while leased, without the doorkeeper.
3. Answers carry a hot hint naming the owner, the replicas and the leases' end. Gateways spread a hot placement's range reads across them in turn; reads that need the home's metadata stay on the home.
4. Replicas report their read counts to the owner halfway through a lease. Three quarters through, the owner renews while the combined rate stays above half the promotion threshold, counting its own reads over the lease so far and the replicas' over the half they reported.
5. Leases expire on their own, and losing one costs only hit rate.

### Auth

- **Client credentials:** the Gateway checks SigV4 signatures against its own credential store, in the `Authorization` header or in a presigned URL's query. A presigned URL lasts up to seven days, signs no body, and works for any method; anyone holding one needs no credentials. Each credential maps to grants of a bucket and a key prefix, each at a level: `read` reads objects and lists them, `write` (the default) also writes and deletes them, and `admin` also changes and deletes the bucket, which takes a grant on the whole bucket. A listing needs a grant on the prefix it lists, and names each parameter once. `DeleteObjects` needs a write grant on every key it names, read as S3 reads its XML, and a body with a DTD gets 400. Every `x-amz-*` header must be signed, as S3 requires, since the node signs whatever it forwards.
- **Every read is authorized, hits included,** because S3 never sees a cache hit.
- **Access to S3:** only storage nodes hold S3 credentials: each bucket's origin names an access key (see Origins). The Gateway authenticates and authorizes each request the cache doesn't serve and passes it through the object's home, or the node a bucket's request goes through, which re-signs it and forwards it to S3 with any checksums intact. A body signed chunk by chunk (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, with or without a trailer) gets 501, since re-signing would break its chunk signatures; an unsigned body with trailing checksums (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`) passes through.
- **Gateway identity:** every request to a node carries the cluster's shared secret, which the node checks in constant time, and with `[cluster.tls]` each member also presents a certificate the cluster's CA signed. A node serves any member that holds the secret, so a compromised gateway reaches whatever the cluster's S3 credentials reach.
- **SSE-C:** requests using customer-provided encryption keys bypass the cache, and uploads using them are never warmed.

### Origins

Each bucket has an origin: an S3-compatible endpoint, its region and a static access key. Elsewhere this spec calls a bucket's origin S3.

- **Compatibility:** an origin speaks S3's API, signed with SigV4 and addressed path-style, and honors `If-Match` and `If-None-Match` on whole and ranged reads. Fills rely on `If-Match` to keep one version's bytes under one ETag.
- **From the config:** `[origins.<bucket>]` names one bucket's origin, and `[origin]` every other bucket's. A bucket neither names gets 404 `NoSuchBucket`.
- **A bucket no origin serves** gets 404 `NoSuchBucket` for reads too, whatever the cache holds of it: a node checks the bucket's origin before it reads. A gateway answers `HeadObject` from its own metadata until the entry expires, so a `HEAD` sees the bucket gone within a second, or within the purge window for an immutable bucket.
- **A bucket that moves to another origin** keeps what the cache holds of it, since blocks are keyed by ETag; its metadata revalidates against the new origin as its freshness mode says.
- **From the metadata service:** with `[metadata]` in place of both, a node asks the service for each bucket's origin: `GET <url>/buckets/<bucket>` with `Authorization: Bearer <token>`. The service answers 200 with a JSON object holding `endpoint`, `region`, `access_key_id`, `secret_access_key` and `ttl_ms`, or 404 for a bucket it doesn't serve. A lookup has five seconds to answer.
- **Cache:** each node keeps the answers in memory for their TTL, at least a second. Concurrent requests for a bucket share one lookup. A request that finds its bucket's entry past half its TTL starts a lookup in the background and goes on with the entry, so a bucket in steady use waits on the service only when a lookup takes longer than half the TTL.
- **Unknown buckets:** a 404 from the service answers the request 404 `NoSuchBucket`, and the node remembers it for `unknown_ttl_ms` (10 seconds by default), for up to 10,000 buckets at a time. The cache drops entries past their grace and failures past their second as it grows.
- **Failures:** a lookup that times out, fails or gets any other answer changes no entry. While lookups fail, a node goes on with an entry for up to `grace_ms` (15 minutes by default) past its TTL, and asks again in the background at most once a second, so only the request that meets the first failure waits for it; a bucket with no usable entry gets 503 on requests that reach its origin, and its reads serve what the cache holds.
- **Invalidation:** the service pushes a change by sending `POST /origins/<bucket>/invalidate` to each node's admin listener, with `x-accel-time`, the Unix time in seconds, and `x-accel-signature`, the hex HMAC-SHA256 of `invalidate\n<bucket>\n<time>` under the service's token. A node takes an invalidation stamped within five minutes of its own clock: it drops the bucket's entry and answers 204. A lookup under way when an invalidation arrives is followed by another, so the next request gets an answer the service gave after the invalidation. A replayed invalidation costs one lookup. A node the push misses uses its entry until the TTL ends, so the TTL bounds how long a change takes to reach every node.
- **Requests naming no bucket,** such as `ListBuckets`, go to `[origin]`, and get 501 without one.
- **Copies** go to the destination bucket's origin, which must reach the source.
- **Reference service:** `s3-accelerator-metadata` serves buckets from a TOML file, with an optional default origin for buckets it doesn't list. On `SIGHUP` it reloads the file and sends each bucket whose origin changed to every node's admin listener it lists, trying each node three times before leaving it to its TTLs. The token and listen address take a restart, since nodes check invalidations with the token their own config names.

### S3 API

- **Served from cache:** `GetObject` (including ranges and conditionals on one strong ETag) and `HeadObject`. Reads with `versionId`, `partNumber` or another query parameter besides `x-id` and the response overrides, with `If-Modified-Since` or `If-Unmodified-Since`, or with a wildcard, list or weak ETag in `If-Match` or `If-None-Match` pass through to S3.
- **Proxied to S3** through storage nodes: every other operation.
- **Response overrides:** a read's `response-*` parameters set its response's `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type` and `Expires`. The gateway applies them to reads the cache serves, and S3 to the rest; a value holding a control character gets 400.
- **Addressing:** path-style, and virtual-hosted-style for the domains `[gateway] domains` lists: a request to `bucket.s3.example.com` names `bucket` when the list holds `s3.example.com`.
- **Checksums:** homes ask S3 for full-object checksums on every read and keep them with the metadata. A whole-object read that asks for them (`x-amz-checksum-mode: ENABLED`) gets them, from the cache as from S3. A range gets none, as from S3, and so does a read of metadata that a ranged first fetch set.
- **Gateways route requests themselves,** so clients never see redirects.

## Implementation

The system is written in Rust.

- **HTTP/1.1 server:** a purpose-built server, starting from [rust_http_router_template](https://github.com/danthegoodman1/rust_http_router_template), parses requests and writes response headers, then hands the body to the kernel. S3 clients speak HTTP/1.1, and every response the cache serves has a known `Content-Length`, so its body needs no framing. A passed-through S3 response without one goes out chunked.
- **Streaming bodies:** memory never grows with object size. Uploads pass through a storage node to S3 as they arrive; when the client signed the payload's SHA-256, the gateway holds back the last bytes until the whole body matches, so neither the node nor S3 receives a body that fails its hash. S3's responses pass back the same way, and a gateway relays a chunked one chunk by chunk. A first fetch's body passes through the home in lockstep: each chunk reaches every reader before the next is read, so the slowest reader paces S3, and each admitted block is written once its bytes are in. Fills, at most a chunk, are held until their readers finish. The one body a gateway reads whole is a `DeleteObjects` key list, to learn the keys it deletes; it refuses one over 8 MiB.
- **Event queue:** nodes speak SQS's JSON protocol over hyper, signed for `sqs` with `[events]`' credentials, or `[origin]`'s when it names none. A message holds S3's event, or SNS's envelope around it.
- **S3 client:** hyper, with each request's path sent as written. S3 treats `.` and `..` as ordinary key segments, so a client that normalized them would read or write another key.
- **Zero-copy:** storage nodes serve blocks with `sendfile`, and gateways relay peer responses to clients with `splice`. `sendfile` blocks when a block isn't in the page cache, so it runs on worker threads, off the async event loop; a run of up to 1 MiB whose pages the page cache holds goes out from the event loop over plaintext, since `sendfile` then never blocks and waking a worker costs more than the send. `splice` between sockets never blocks, so it runs on the event loop through a pipe, unless either socket carries a kernel TLS session: then each `splice` encrypts or decrypts, and the relay runs on a worker thread so the crypto leaves the event loop free. A gateway reads a peer response's head without reading past it, so the body stays in the socket for `splice`. A node's S3 requests run on worker threads too, which receive S3's bodies, and workers write the bodies a node holds or passes on to their readers; bodies under 256 KiB go out from the event loop, where waking a worker costs more than the copy, and a small one in the same write as its head. The event loop, which owns the core, handles heads and the core's actions.
- **Journal:** the node's thread hands slot records, clears, erasures and metadata entries to a journal thread, which applies them in order, so a drive slow to sync stalls the journal and never the thread that owns the core. A block write waits for the entries issued before it, so a slot's clear is durable before the slot's new bytes reach the slab file, and a purge's confirmation, a metadata sync and a clean shutdown each wait for the entries before them.
- **Pages in flight:** `sendfile` and `splice` pass references to page-cache pages. A page stays in use after the call returns, until the peer acknowledges it or, over loopback, until the reader reads it, and a write into it would change bytes already sent. Before a write overwrites a slot, the node asks the kernel to drop the slot's pages; the kernel keeps only pages that a socket or pipe still references, so a page still cached means the write waits. After 30 seconds it gives up, and the block counts as unwritten. The node reads the slab file with `FADV_RANDOM`, so a read caches pages within one slot, and slots are multiples of the page size. A write may cache a block in folios as large as its slot, and a smaller slot later carved from that space lies inside one, which dropping the slot's pages leaves in place; so when a slot's pages stay, the node drops those of the whole largest-slot span around it. The data directory must be on a disk-backed filesystem: tmpfs pages never leave the page cache.
- **TLS:** a gateway serves HTTPS when `[gateway.tls]` names a certificate chain and key. rustls runs the handshake, and the `ktls` crate moves the session into the kernel, which encrypts what `splice` moves into the socket, so zero-copy works under TLS. The gateway sends no session tickets, so rustls has nothing left to write once the handshake ends, and it ends each session with a `close_notify`. Any record but application data ends the connection: a client's `close_notify`, or a TLS 1.3 KeyUpdate, after which the client reconnects. Without the kernel's `tls` module, or with `kernel = false`, a task relays each session between rustls and a loopback socket, and zero-copy ends at that socket.
- **Membership:** foca (SWIM) in the core, fed packets and timer events by its owner. Nodes gossip over UDP on their cluster addresses, and carry their addresses in their identities. A node resolves each address as it learns it, off its event loop; one that doesn't resolve costs only that node's gossip.
- **Peer links:** every request a gateway or node sends a node carries the cluster's shared secret. On a private network the links run plaintext. With `[cluster.tls]`, members reach nodes over mutual TLS, which the kernel carries as it does clients' TLS, so blocks still leave nodes through `sendfile`: each member presents a certificate the cluster's CA signed, and a node's certificate names the host of its address. Gossip runs over UDP, outside TLS: each packet ends with an HMAC-SHA256 tag under a key derived from the cluster's secret, and nodes drop packets without a valid one, so only processes that hold the secret take part in membership. A tag stops forged packets but lets a recorded packet be replayed.
- **Kernel:** kernel TLS needs Linux 7.0 or later, or a 6.18 or 6.19 release with the fix for 6.17's receive-buffer checks. Those checks drop any segment that would push a receive queue past its buffer, and a kernel TLS socket leaves a partial record in the queue until the rest arrives, so the link stalls. Kernels before 7.1 also skip bytes when a `splice` from a kernel TLS socket fills its pipe partway through a record. A relay's 256 KiB pipe takes any record whole, but once a user's pipes pass `fs.pipe-user-pages-soft`, new pipes get two pages, so raise that limit on those kernels.
- **Reference designs:** TAG and ocache (Go) implement versioned block caching, request coalescing, SigV4 validation, warming on write and Parquet footer prefetch. Read them before building those parts.

## Observability

Gateways and nodes report what they do through metrics, health checks and logs.

- **Admin listener:** `[admin] listen` names an address where the process serves plaintext HTTP/1.1: `GET /metrics`, `/healthz` and `/readyz`, and on a node, the metadata service's invalidations. Only invalidations carry credentials, so bind it to a private address.
- **Health:** `/healthz` answers 200 while the process's event loop runs.
- **Readiness:** `/readyz` answers 200 once each role the process runs is ready, and otherwise 503 with a body naming what it waits for. A node is ready once it has recovered its store, off its event loop, and joined the cluster, having taken the ring from a seed or found none answering; a gateway, once a node has answered it. Both turn unready when the process starts to stop or its node starts to leave, so load balancers drain them first.
- **Metrics:** Prometheus's text format, each name prefixed `s3accel_`. Counters end in `_total`; latencies are histograms in seconds, with buckets from 100 µs to 10 s. Label values come from fixed sets or HTTP status codes, and none names a bucket, key or client.
- **Where counts live:** the core counts what it decides in its own state, and its owner reads the counts at each scrape. The server counts what it measures on the event loop, where every worker's result arrives: latencies, transport outcomes and failures. No count is shared between threads, so counting takes no lock and no atomic instruction, and a scrape renders on the event loop between events.
- **Cost:** a hit pays a few increments on its own thread. Benchmarks scraped every second stay within 2% of the same runs without the admin listener.

Gateway metrics:

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `gateway_requests_total` | counter | `operation`, `code` | Client requests, by S3 operation and response status. |
| `gateway_first_byte_seconds` | histogram | `operation` | From a request's head arriving to its response's head leaving. |
| `gateway_response_bytes_total` | counter | `operation` | Body bytes sent to clients. |
| `gateway_node_failures_total` | counter | `reason`: `refused`, `timeout`, `error` | Requests to nodes that got no usable answer. |
| `gateway_relays_cut_total` | counter | `side`: `node`, `client` | Responses cut short after their head: the node's body ended early, or the client left. |

`operation` is one of `GetObject`, `HeadObject`, `PutObject`, `CopyObject`, `DeleteObject`, `DeleteObjects`, `ListObjects`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `Purge` or `Other`.

Node metrics the core counts:

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `node_reads_total` | counter | | Reads from gateways, and from nodes that took over placements. |
| `node_block_reads_total` | counter | `result`: `hit`, `fetched` | Blocks the node's responses read: from the store, or from S3's or a previous owner's response. |
| `node_block_misses_total` | counter | `reason`: `evicted`, `unadmitted`, `new` | Blocks an owner or leased replica fetched for a read and could store, by what it remembers of them: the ghost queue holds them, the doorkeeper turned them away, or neither. Blocks warmed on write or prefetched count only as such. |
| `node_body_bytes_total` | counter | `source`: `cache`, `s3`, `previous_owner` | Body bytes the node sent, by where they came from. |
| `node_admissions_total` | counter | `result`: `stored`, `doorkeeper`, `budget`, `full` | The same blocks: stored, or turned away by the doorkeeper, the fill budget, or a store whose eviction candidates are all in use. |
| `node_blocks_dropped_total` | counter | `cause`: `evicted`, `disowned`, `purged`, `corrupt`, `unfilled` | Blocks the store let go: to make room, cold or no longer owned; by purge; failing their checksums; or a fill that ended without them. |
| `node_fill_bytes`, `node_fill_budget_bytes` | gauge | | Fill budget in use, and the budget. |
| `node_store_blocks`, `node_store_extents` | gauge | `class`: slot size in bytes | Blocks and extents of each size class. |
| `node_store_capacity_bytes` | gauge | | Bytes the store holds when full. |
| `node_objects` | gauge | | Objects whose metadata the node holds. |
| `node_written_bytes_total` | counter | | Block bytes written to the slab file. |
| `node_verified_blocks_total` | counter | `result`: `intact`, `corrupt` | Recovered blocks checked against their checksums. |
| `node_fallback_reads_total`, `node_fallback_timeouts_total` | counter | | Reads asked of previous owners, and those left unanswered. |
| `node_fallback_metadata_total` | counter | | Objects whose metadata a previous home supplied. |
| `node_leases_granted_total`, `node_leased_reads_total` | counter | | Leases the node granted as owner, and reads it served as a replica. |
| `node_warmed_uploads_total`, `node_prefetched_blocks_total` | counter | | Uploads warmed as they passed, and blocks of format metadata filled before a reader asked. |
| `node_purges_total`, `node_purges_pending` | counter, gauge | | Purges carried out, and purges waiting on other nodes to confirm. |

Node metrics the server measures:

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `s3_requests_total` | counter | `kind`: `read`, `forward`; `code`: status, or `none` | Requests to S3: the core's reads and passed-through requests, by status, or `none` when S3 sent no answer. |
| `s3_first_byte_seconds` | histogram | `kind` | From sending a request to S3 to its response's head. |
| `node_sync_seconds` | histogram | | Each sync of the slab file, shared by the blocks it makes durable. |
| `events_received_total` | counter | | Event messages taken from the queue. |
| `events_lag_seconds` | histogram | | From S3's event time to the node taking the message. |
| `origin_lookups_total` | counter | `result`: `found`, `unknown`, `failed` | Lookups of buckets' origins in the metadata service. |
| `origin_invalidations_total` | counter | | Invalidations the node took. |
| `origin_stale_total` | counter | | Requests sent with an entry past its TTL while lookups failed. |
| `origin_unresolved_total` | counter | `reason`: `unknown`, `unavailable` | Requests answered without an origin: 404 for an unknown bucket, 503 for one with no usable entry. |

Metrics of every process:

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `ring_changes_total` | counter | `role`: `gateway`, `node` | Rings the process adopted, by the role that adopted them. |
| `ring_info` | gauge | `version` | 1, labeled with the current ring's version in hex. |
| `ring_nodes` | gauge | `state`: `up`, `down` | Nodes in the current ring, by whether the process routes around them. |
| `tls_sessions_total` | counter | `link`: `client`, `cluster`; `mode`: `kernel`, `userspace` | TLS sessions the process accepted, and whether the kernel carries them. |
| `tls_handshake_failures_total` | counter | `link` | Handshakes that failed or timed out. |
| `event_loop_delay_seconds` | histogram | | How late the event loop runs a 100 ms timer. |
| `build_info` | gauge | `version` | 1, labeled with the binary's version. |

Each process also exports `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds` and `process_start_time_seconds`, read from `/proc/self` at each scrape.

- **Logs:** one line per event on standard error, in logfmt: `ts`, `level` and `msg`, then fields. `[log] level` sets the least severe level written: `error`, `warn`, `info` (the default) or `debug`. Failures log at `warn`; starts, recoveries, ring changes and departures at `info`; at `debug`, gateways and nodes also log each request they answer.
- **Request IDs:** a gateway gives each client request a random 64-bit ID and sends it to nodes in `x-accel-request-id`. Every response carries it in `x-accel-request-id`, and a response S3 did not give also carries it as `x-amz-request-id`, the header S3 clients print; a passed-through response keeps S3's. Log lines about a request carry its ID as `request`. When S3 fails a request, the node logs S3's `x-amz-request-id` and `x-amz-id-2`, beside the client's request ID when it passed the client's request through.

## Open questions

- **Workload targets:** object-size mix, request rate, working-set size, and hit-rate and latency goals. These set the chunk size, block size and hot-key thresholds.
- **Chunk size:** larger chunks mean fewer hops per read; smaller chunks spread load more evenly.
- **Fallback window:** how long to keep previous-owner fallback after a ring change. The default is ten minutes.
