# Development Plan

## Overarching Goal

Build the S3 accelerator that `spec.md` describes: a distributed NVMe read cache in front of S3 whose cache logic runs as deterministic state machines, proven in a simulator before it runs on real sockets and disks. Each core feature lands with the simulator models and properties that test it. The server track runs the same core over real I/O and passes the S3 conformance suite through the accelerator.

Non-goals until a phase names them: POSIX access, multipart-upload warming, SSE-C caching, caching versioned reads, presigned POST uploads, cross-zone clusters.

## Implementation Principles

- `spec.md` is the design contract. A change that alters the design updates the spec, and the page that renders it, in the same commit.
- The core does no I/O, reads no clocks and starts no threads (`AGENTS.md`). It handles block locations and response heads; the server and simulator move bytes.
- Every core feature ships with its simulator model, a property that fails on a wrong answer, and a planted bug that the simulator catches.
- Build the smallest implementation that meets the phase gate. Add abstraction when a later phase needs it.
- Execution order: Phase 1, S1, Phase 2, Phase 3, S2, Phase 4, Phase 5, S3, Phase 6, Phase 7, Phase 8, Phase 9.

## Testing Strategy

- **Unit tests** for pure logic: layout math, placement, eviction and admission policy.
- **Simulator:** `cargo test` runs a fixed seed range; CI runs the commit hash as a seed; a phase ends with a local sweep of at least 10,000 seeds and no failures.
- **Planted bugs:** `scripts/mutants` applies known bugs to a scratch copy and reports how many seeds catch each. Every phase adds its own and must catch all of them.
- **Regression tests:** a bug the simulator finds becomes a test in `crates/sim/tests` that runs its seed, with the seed and failing commit in the commit message.
- **Conformance:** the suite in `tests/` passes against s3proxy, and against the accelerator once S1 lands.
- **Code review:** each phase ends with `/code-review`. Findings are fixed or recorded in the phase ledger before the phase closes.

## Phase 0: Scaffold

Goal:
A workspace with a deterministic core, a simulator, a conformance suite and CI.

Scope:
- Cargo workspace with `core`, `server`, `sim` and `tests`.
- Placement by weighted rendezvous hashing over placement hashes.
- Simulator with a versioned model of S3, a delayed network and a response property.
- Conformance suite against s3proxy, pinned by digest.
- CI: fmt, clippy, tests with s3proxy, and a simulator run seeded by the commit hash.

Completion gate:
CI passes on `main`.

Testing plan:
- Core unit tests, simulator seed tests, conformance tests against s3proxy.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Workspace and crates | Commit `0b68204`; `Cargo.toml`, `crates/*`, `tests/`. |
| Complete | Scope | Rendezvous placement over placement hashes | `crates/core/src/placement.rs`; tests `placement_is_stable`, `adding_a_node_moves_keys_only_to_it`, `weights_split_keys_proportionally`; commit `4b92bd8`. |
| Complete | Scope | Simulator with S3 model and response property | `crates/sim/src/{lib,origin,properties}.rs`; `crates/sim/tests/seeds.rs`; planted `If-Match` drop caught by seed 1 at tick 56. |
| Complete | Scope | Conformance suite against s3proxy | `tests/get_object.rs` (8 tests); `scripts/s3proxy`. |
| Complete | Gate | CI passes on `main` | GitHub Actions run `36257988788` (`0b68204`) and the run for `4b92bd8`: `test` and `simulate` succeeded. |

## Phase 1: Single-Node Read Path

Goal:
A storage node caches the objects it is home to, block by block, and every response stays correct while writers overwrite and delete objects. The simulator reports hit rates.

Scope:
- 1A Layout: block and chunk sizes as configuration; range to blocks; chunk 0 and every block overlapping the final chunk-sized region belong to the home; objects up to two chunks live on the home. In this phase the home serves every block of its objects.
- 1B Object metadata at the home: the first fetch is unconditional and merged across concurrent readers; size comes from `Content-Range` or `Content-Length`; later fills carry `If-Match`; a 412 or 404 on a fill drops the metadata, and the gateway restarts a read whose response has not started.
- 1C Freshness and requests: `immutable` and `ttl` modes per bucket (`ttl` revalidates with `If-None-Match` after its age); no negative caching; `HeadObject` from metadata; client `If-Match` and `If-None-Match` evaluated against metadata.
- 1D Fills: concurrent misses for a block merge into one fill; adjacent missing blocks combine into one range GET; a response streams from blocks that are still filling.
- 1E Block store policy: extents of power-of-two slot classes, extent moves between classes, S3-FIFO with small, main and ghost queues, a doorkeeper with aging, admit on first read per bucket, and a fill budget.
- 1F Simulator: a disk model that holds bytes at `(file, offset)`; block and chunk sizes drawn from the seed; objects spanning several chunks; disk capacity small enough to force eviction; hit rate, S3 GETs and drive writes in the summary; buckets with freshness modes.
- 1G Properties: a response equals S3's response for a state the key held within its staleness bound (`ttl` buckets: from `ttl` before the request to its answer; `immutable` buckets are written once); every indexed block's bytes equal the model's bytes for its key.
- 1H Policy scenarios: a scan leaves a hot set cached; a block read once stays off disk under the doorkeeper.

Out of scope:
- Chunks on other nodes, gateway metadata cache (Phase 2).
- Crashes, message loss, persistence (Phase 3).
- Membership changes (Phase 4).
- Writes through the home, `events` mode (Phase 5).

Completion gate:
All scope items have tests; a 10,000-seed sweep passes; `scripts/mutants` catches every Phase 1 planted bug; `/code-review` findings are resolved.

Testing plan:
- Unit tests for layout math, the slot allocator, S3-FIFO and the doorkeeper.
- Simulator seeds with overwrites, deletes, both freshness modes, and eviction pressure.
- Scenario tests for scan resistance and doorkeeper admission.
- Planted bugs: a fill without `If-Match`; a block indexed under the wrong ETag; a wrong slot offset; `ttl` metadata served past its age; a block admitted on its first read under the doorkeeper.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Layout math | `crates/core/src/layout.rs`; tests `spans_cover_the_object`, `objects_up_to_two_chunks_live_on_their_home`, `home_holds_chunk_0_and_the_final_chunk_sized_region`. The home serves every block this phase; stored blocks record their placement hash. |
| Complete | Work | 1B: Home metadata and validated fills | `Node::first_fetch`, `first_answered`, `fill`, `fill_answered`, `stale` in `crates/core/src/node.rs`; `Gateway::on_node_stale`; scenario `a_request_whose_fills_all_fail_retries_once`; mutants "fill accepts any version", "stale request sent back twice". |
| Complete | Work | 1C: Freshness modes, `HeadObject`, client conditionals | `Node::serve`, `revalidated`, `conditional_answer`; a 416 first fetch with preconditions falls back to a HEAD (`spec.md` Consistency); scenario `preconditions_come_before_an_unsatisfiable_range`; mutants "ttl metadata served past its age", "revalidation keeps the old metadata", "416 relayed despite preconditions". |
| Complete | Work | 1D: Fill merging, range GET coalescing, streaming from filling blocks | `Node::plan` joins in-flight bodies and fetches each run of missing blocks with one range GET; responses start once every fill they read has a validated head and read its body through `Segment::Origin`; mutants "first fetch body read from the wrong offset", "write leaves its body unheld". |
| Complete | Work | 1E: Block store policy | `crates/core/src/store.rs` (extents, size classes, evacuation, S3-FIFO, pins) with 7 unit tests; `crates/core/src/doorkeeper.rs` with 2; fill budget in `Node::admit`; mutants "doorkeeper admits on the first read", "response leaves its blocks unpinned". |
| Complete | Work | 1F: Simulator disk model, sizes from seed, summary metrics | `crates/sim/src/{disk,queue}.rs`; `Options::swarm` draws block, chunk, extent and slot sizes, capacity, policies and delays; `Summary` reports hit percentage, S3 requests, bytes written and evictions (seeds 1 to 8 range from 5% to 99% of body bytes from disk). |
| Complete | Test | 1G: Staleness-bounded response property and disk-content property | `properties::check_response` and `check_block` with 5 unit tests; checked on every answer, every write, and every 64 ticks. |
| Complete | Test | 1H: Scan resistance and doorkeeper scenarios | `crates/sim/tests/scenarios.rs`: `the_doorkeeper_stores_a_block_on_its_second_read`, `a_scan_leaves_the_hot_set_cached` (both admission modes). |
| Complete | Test | Planted bugs for Phase 1 | `scripts/mutants`: 15 of 15 caught (2 by core unit tests, 8 by scenarios, 5 by 300-seed sweeps). Every workspace crate rebuilds for each mutant; before S1 the script could reuse a stale build across crates, which affected no Phase 1 result because every Phase 1 mutant was in one crate. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p s3-accelerator-sim -- 0 --seeds 10000`: 0 of 10,000 failed, after the review fixes. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: mutants now match ignoring whitespace; eviction falls back to the other queue when one is fully pinned (`a_pinned_queue_yields_to_the_other`, `frequent_blocks_still_leave_eventually`); fill errors other than 412 and 404 pass to the client; inverted ranges are ignored, as S3 does; home metadata has an LRU capacity (`the_home_forgets_its_least_recently_used_metadata`); each random source has its own stream; planted bugs for first-fetch merging, the ghost queue and the fill budget; evacuation checks a per-extent busy count; `Node::stored_block_at`; `Node::new` checks block and slot sizes. Wrong-class evictions before an evacuation are recorded under the storage-layout open question in `spec.md`. |

## Phase S1: Pass-Through Server

Goal:
The `s3-accelerator` binary serves S3 over plaintext HTTP/1.1 by running the core, and the conformance suite passes through it.

Scope:
- S1A HTTP/1.1 request parsing and response writing, adapted from `rust_http_router_template`.
- S1B SigV4 header validation against a static credential file, with bucket and prefix grants.
- S1C S3 client that signs requests to the origin with the cluster's credentials.
- S1D The core's gateway and node run in one process; `GetObject` and `HeadObject` follow the core's actions, with blocks held in memory; other operations pass through.
- S1E CI runs the conformance suite against s3proxy and through the accelerator.

Out of scope:
- Disk storage, `sendfile`, `splice`, kTLS (S2 and S3).
- Presigned URLs, signed streaming uploads (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, answered 501), chunked transfer encoding, and virtual-hosted-style addressing.
- Response headers beyond ETag, length and range (2F).

Completion gate:
The conformance suite passes through the accelerator in CI; `/code-review` findings are resolved.

Testing plan:
- Unit tests for request parsing and SigV4 validation, using the AWS test vectors.
- Conformance suite through the accelerator.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S1A: HTTP/1.1 parsing and responses | `crates/server/src/http.rs` (keep-alive, `Content-Length` bodies, `Expect: 100-continue`) with 3 unit tests. |
| Complete | Work | S1B: SigV4 validation and grants | `crates/server/src/sigv4.rs`: AWS's GET Object and List Objects signing examples, sign-then-verify, tampering, expiry and unknown keys (4 tests); body hashes checked against `x-amz-content-sha256`; grants in `config.rs` (`parses_a_minimal_config`); unsigned requests get 403 without reaching S3 (`a_third_read_comes_from_the_cache`). |
| Complete | Work | S1C: Origin signing client | `crates/server/src/origin.rs` signs with `sigv4::Signer`; s3proxy accepts its requests in the conformance run through the accelerator. |
| Complete | Work | S1F: Review fixes | Grants cover `x-amz-copy-source` (`a_copy_needs_a_grant_on_its_source`); heads are authenticated before bodies are read and bodies are capped by `max_body` and read as they arrive (`an_unauthenticated_body_is_never_read`, `an_oversized_body_is_refused_before_it_is_read`); a first fetch sent before a write answers its own request and is not kept (`Node::on_write`, scenario `a_first_fetch_sent_before_a_write_is_not_kept`); `DeleteObjects` drops each listed key's metadata (`delete_objects_drops_cached_metadata`); S3 connect and read timeouts; keys with `.` or `..` segments are refused with 501, because reqwest's URL parser would collapse them after signing (`dot_segment_keys_are_refused`); non-ASCII `x-amz-date` values are rejected; fill errors relay S3's error body; header and clock helpers are shared. S3 bodies are held whole in memory up to `max_body` until S2 streams them. |
| Complete | Work | S1D: Core-driven `GetObject` and `HeadObject` | `crates/server/src/engine.rs` runs the gateway and node on one thread; `crates/server/tests/cache.rs` `a_third_read_comes_from_the_cache` shows the third read and a range read cost no S3 request; writes passed to S3 drop the home's metadata (`Node::on_write`, scenario `a_write_through_the_home_drops_its_metadata`, mutant "a write leaves the home's metadata in place"). |
| Complete | Test | S1E: Conformance through the accelerator in CI | CI run `36266117433` (`46579e2`), step "Conformance through the accelerator": 8 of 8 pass. |
| Complete | Test | Planted bugs for S1 | `scripts/mutants`: 23 of 23 caught, 8 of them S1's (server tests catch the copy-source, body-size, `DeleteObjects`, dot-segment and write-invalidation bugs). |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved in S1F; whole-body buffering moves to S2C and dot-segment keys to S2's origin client. |

## Phase 2: Chunks Across Nodes

Goal:
Large objects spread across the cluster, and gateways assemble responses from several owners.

Scope:
- 2A Gateway read planning: with metadata, send each range to its owner; without it, ask the home, which returns metadata with head and tail bytes; the home's first fetch serves middle ranges without admitting them.
- 2B Fan-out and ordered assembly of responses from several owners.
- 2C Gateway metadata cache: bounded LRU, `immutable` entries until evicted, a short TTL for others; a stale entry makes the owner's fill fail `If-Match`, and the gateway drops it and retries.
- 2D Chunk owners fill with the ETag the gateway sends; a node asked for a chunk it does not own serves its copy or fetches without admitting.
- 2E Simulator: gateways with independent caches; per-node hit rates and load.
- 2F Response headers: metadata carries the headers S3 returns with an object (`Content-Type`, `Last-Modified`, `Cache-Control`, `Content-Encoding`, `Content-Disposition`, `x-amz-meta-*`), the model of S3 sets them, and the server writes them.

Out of scope:
- Ring changes (Phase 4).

Completion gate:
A 10,000-seed sweep passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Unit tests for read planning over many sizes and ranges.
- Simulator seeds with multi-chunk objects and several gateways.
- Planted bugs: segments assembled out of order; a stale gateway ETag served without retry; a middle range admitted from a first fetch.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Gateway read planning | `Gateway::plan`, `plan_with` and `owners` over `Layout::runs` in `crates/core/src/gateway.rs`; the home answers `Action::Metadata` when a read reaches blocks it does not own (`Node::plan`); scenario `a_large_object_spreads_across_owners`; mutant "the home serves blocks it does not hold". |
| Complete | Work | 2B: Multi-owner response assembly | `gateway::Action::Relay` lists parts in order; the simulator and the server's engine concatenate them, and the engine answers 500 if a part is missing; mutant "parts assembled out of order". |
| Complete | Work | 2C: Gateway metadata cache | LRU `MetadataCache` with write markers; freshness bounded by the bucket TTL from the home's confirmation and by `metadata_ttl`; stale parts drop the entry and report the ETag to the home; after `STALE_RETRIES` the home reads S3 directly (`Read::Object { direct }`). Scenarios `the_gateway_answers_heads_and_preconditions_itself`, `a_gateway_with_a_stale_etag_reads_the_new_version`, `a_stale_report_makes_the_home_revalidate`, `an_answer_older_than_a_write_is_not_cached`, `a_read_of_an_object_that_keeps_changing_finishes`; mutants for each. |
| Complete | Work | 2D: Chunk owner fills and non-owner behavior | `Node::read_range` fills with the ETag the gateway names; only owners admit blocks (`Node::admit`); the simulator misroutes a seed-drawn share of range reads and checks on every write that a node stores only blocks it owns (`check_owned`); mutant "nodes store blocks they do not own". |
| Complete | Work | 2E: Simulator gateways and per-node metrics | `Options` draws gateway cache size and TTL and a misroute share; `Summary::node_reads` (seed 2: reads per node 62, 557, 44, 1,102, 68, 63). |
| Complete | Work | 2F: Response headers in metadata | Commit `2F: carry object headers in metadata`; conformance `object_headers_come_back_with_every_read` passes against s3proxy and through the accelerator. |
| Complete | Test | Planted bugs for Phase 2 | `scripts/mutants`: 35 of 35 caught, 11 of them Phase 2's. The direct-read mutant was first missed because its scenario read blocks the home owns, whose failed fill makes the retry a first fetch; the scenario now reads one middle chunk. |
| Complete | Gate | 10,000-seed sweep | `cargo run --release -p s3-accelerator-sim -- 0 --seeds 10000`: 0 of 10,000 failed, after the review fixes. A draft of this phase livelocked under zero network delay (the home was not told of stale ETags); the simulator now fails any tick with over a million events. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved: write markers and newest-wins in the gateway cache; a stale-retry cap ending in a direct read; misroutes and new options on their own draws; ownership, not placement kind, decides the home's redirect; `--check` exits non-zero; mutants for the gateway's write invalidation and local preconditions; one `s3::answer` shared by gateway and node; placement computed per run; a missing part answers 500; `CachedMeta` wraps `ObjectMeta`. |

## Phase 3: Faults

Goal:
Correct answers survive lost messages, node crashes and restarts, and the cluster converges once faults stop.

Scope:
- 3A Time in the core: ticks as input, request timeouts and retries.
- 3B Network faults drawn from the seed: loss, delay spikes, partitions; a safety phase with faults and a liveness phase without. Messages travel over TCP connections, so none arrive twice.
- 3C Failover partway through a response through the next rendezvous candidate, with `Range` and `If-Match`; a response ends early when the version changes after it started, and the client retries.
- 3D Crash and restart: memory lost; the disk model keeps synced writes, tears unsynced ones, and may damage synced ones; the slot table persists the index; write, sync, then record; clear a record before reusing its slot; records name the run that wrote or verified them, and a clean shutdown vouches for its run's records; every other recovered block's first hit verifies its checksum.
- 3E Properties: an early-ended response is a client error followed by a correct retry; every request completes in the liveness phase; the disk-content property holds after restarts.

Out of scope:
- Membership changes (Phase 4).

Completion gate:
A 10,000-seed sweep with faults passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator seeds with every fault type.
- Scripted fault scenarios for crash during a fill and failover mid-response.
- Planted bugs: a record written before its data is durable; eviction that leaves a slot's record; a restart after a crash that trusts its blocks; a corrupt block served anyway; failover without `If-Match`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Ticks, timeouts, retries | `Node::on_tick` cancels S3 requests past `origin_timeout` (`Action::Cancel`) and proceeds as if S3 answered 503; `Gateway::on_tick` fails a part over to the next rendezvous candidate past `node_timeout` and routes around the node for `suspect_ttl`; simulated clients retry after a 5xx or `client_timeout`; the server's engine ticks every 100 ms. |
| Complete | Work | 3B: Network fault models and liveness phase | While clients issue requests the simulator loses messages (`loss_percent`), delays them tenfold (`spike_percent`), cuts nodes off (`partition_percent`, `partition_max`) and has S3 answer 503 (`origin_error_percent`), each from its own `Prng::stream`, with client retries on another; then faults stop and every request must be answered within 20 client timeouts. Faults also stop once a budget of ticks runs out, since heavy enough faults stall a one-node cluster indefinitely (seeds 4010, 4584, 6276, 6384 and 7859 once did). Seeds 1 to 5: 62 to 1,365 messages lost, all answers correct. |
| Complete | Work | 3C: Mid-response failover and early-ended responses | The gateway starts a response once every part has answered (`Action::Start`), then forwards the parts' bodies one at a time, in order (`Action::Forward`, `on_forwarded`). A body that ends early is read from the next candidates from where it stopped, as a range of the same version, so they fill with `If-Match`; the rest of a home's body comes from the blocks' owners. If S3 no longer holds the version, or no candidate is left, the response ends early (`Action::Abort`). A fill whose S3 body ends early stores nothing (`Node::on_write_failed`). The simulator cuts node and S3 bodies (`cut_percent`, `origin_cut_percent`), and a crashing node's responses arrive cut short or not at all. Scenarios: `a_body_cut_partway_resumes_from_the_next_candidate`, `a_response_whose_object_changed_after_it_started_ends_early`, `a_cut_s3_body_stores_nothing_and_the_read_resumes`, with the earlier part-level failover scenarios. Seed 1: 16 bodies cut, all resumed without a client retry. |
| Complete | Gate | Review of 3.1 | `/code-review high` found 10 issues, all resolved: unexplained 5xx after faults end fail the run; a 5xx part fails over without suspecting its node; only the home keeps metadata, and candidates standing in for it read S3 directly (mutant "failover candidates cache metadata the home's writes never reach"); the server aborts cancelled S3 fetches; client retries and each fault kind draw from their own streams; parts merge runs with one target and split them on failover; owner-first targeting; `node_timeout_ms` must be at least twice `origin_timeout_ms`; a stale doc line. |
| Complete | Work | 3D: Crash, restart, slot table, post-crash verification | The node records a block (`Action::Record`) once its bytes are durable and clears the record (`Action::Clear`) when the block leaves. `Node::recover` rebuilds the index from the records, trusting those of a run that shut down cleanly; it verifies every other block on its first read (`Action::Verify`, `on_verified`) and records it again. A corrupt block is dropped, and its readers plan again as misses. A home that lacks an object's metadata but holds its blocks fetches the metadata with a HEAD. The simulator crashes nodes (`crash_permille`, `down_max`), shuts them down cleanly (`clean_percent`), tears writes in progress, damages recorded slots (`damage_percent`), drops events and S3 responses from a node's earlier runs, and checks every slot-table record against the model of S3. Scenarios: `a_clean_restart_keeps_the_cache_warm`, `after_a_crash_each_block_is_verified_before_it_is_served`, `a_damaged_block_is_read_again_from_s3`, `a_clean_shutdown_vouches_only_for_blocks_it_verified`, `verified_blocks_stay_trusted_across_a_clean_restart`, `a_crash_during_a_fill_leaves_no_record_of_it`. Seed 1: 10 crashes, 4 clean shutdowns, 121 blocks verified, 2 corrupt. 20,000 seeds pass. |
| Complete | Test | 3E: Fault properties and liveness | Every request is answered after faults stop, and every accepted answer passes the response property; 5xx answers are retried, as SDKs do, and one sent after faults have had time to play out fails the run (mutant "the gateway fails one read in ten"). The simulator found a metastable collapse: requests queued behind a first fetch that found no object were answered one per S3 round trip, so retries outran them (scenario `requests_queued_behind_a_404_share_it`, mutant "requests queued behind a 404 each wait for their own fetch"; seed 7602 found it, and after the review's changes to timeouts and random streams no seed in 10,000 reproduces it, so the scenario is the regression test). Every slot-table record describes the bytes its slot holds, across crashes and restarts, unless a fault damaged them. A client whose response ends early checks the bytes it got against a state of the key (`check_early_end`) and retries; an early end sent after faults have had time to play out fails the run. Seed 5: 51 bodies cut, 35 responses ended early, all retried correctly. |
| Complete | Test | Planted bugs for Phase 3 | The simulator's tests catch every Phase 3 mutant. 3D: a block recorded before its bytes are durable, eviction that leaves the slot's record, a restart after a crash that trusts its blocks, a corrupt block served anyway, a verified block not recorded again, and a restarted home that fetches whole objects it holds blocks of. 3C: a cut body never resumed, a cut range body and a cut home body each resumed from their start, a changed object restarting a response that already started, a write from a cut S3 body kept, and an early end that cuts off the forward in progress (scenario `an_early_end_waits_for_the_forward_in_progress` holds a forward while a later part goes stale). From the review: a home that holds old blocks fetches a HEAD before every first read, and an early end that forgets the stale ETag. `scripts/mutants`: 51 of 51 caught, 40 by the simulator's tests, 9 by server tests and 2 by core unit tests (one by hanging). |
| Complete | Gate | 10,000-seed sweep with faults | `cargo run --release -p s3-accelerator-sim -- 0 --seeds 20000`: 0 of 20,000 failed, after the review fixes. Seed 7859 once stalled with faults that never ended; regression test `faults_stop_once_their_budget_runs_out`. |
| Complete | Gate | Code review | `/code-review high` of 3.2 and 3.3 found 10 issues, all resolved: an early end keeps the stale ETag in the gateway's cache, so the retry tells the home (the changed-object scenario now checks for the new version); the HEAD-first path runs only for keys recovered at startup (`a_write_through_the_home_drops_its_metadata` counts S3 requests); a clean mark names the earliest sound run, so trust carries across clean restarts (`verified_blocks_stay_trusted_across_clean_restarts`); the gateway ignores a repeated answer (`a_repeated_answer_is_ignored`), sends a part answered with other bytes to the next candidate (`a_part_answered_with_other_bytes_goes_to_the_next_candidate`), and ends a response early if its parts fall short; the engine passes a single part through uncopied and answers 500 for a missing body; one helper builds runs; one collection type and one helper replace parts; `Stats` adds with `+=`; the forward-in-progress mutant has a scenario; seed 7859 has a regression test. |

## Phase S2: Real Block Store and Zero-Copy

Goal:
Storage nodes keep blocks and immutable-bucket metadata on disk and serve hits with `sendfile`, and gateways relay with `splice`.

Scope:
- S2A Core changes, through the simulator first. Slot records have a fixed size: each names its block's version by a 128-bit hash of bucket, key and ETag. Homes save immutable-bucket metadata to a metadata file and load it at startup; the simulated disk models the file, and a crash loses entries not yet synced. A reader joins an S3 body only before the body's response arrives; a later reader waits for the block's write and reads its slot, so the server can stream bodies.
- S2B On disk: slab files, extents, the slot table and the metadata file, carrying out the core's storage actions. `fdatasync` ordering: a block's bytes before its record, and a cleared record before any write over its slot. Restart recovery through `Node::recover`, runs in records, and the clean-shutdown mark; the table's header names the layout, and a changed layout discards the table. Checksums of block bytes for records and `Verify`.
- S2C Separate gateway and storage-node processes that speak HTTP/1.1 to each other; a restart test that keeps the cache warm, and a crash test that kills a node during fills.
- S2D `sendfile` for hits, on worker threads off the event loop, and `splice` for relays, on the event loop. Bodies stream end to end: S3 responses, relays and uploads never sit whole in memory, and object size stops bounding memory. A slot is overwritten only once no socket or pipe holds its old pages. The origin client sends paths as written, so keys with `.` and `..` segments work.
- S2E Zero-copy and sync order verified from outside the server.

Out of scope:
- kTLS (S3).

Completion gate:
Conformance passes through a multi-process cluster; a restart test shows hits after restart; zero-copy is verified from outside the server; `/code-review` findings are resolved.

Testing plan:
- S2A: simulator scenarios for an immutable object read after a restart with no S3 request, and for a write through the home that a restart must not undo; a simulator property that a streaming body is read only as its head arrives; planted bugs for metadata the home never saves, a restart that brings back metadata a write dropped, a replayed key with a stale entry, a late reader that joins a streaming body, a fill larger than a chunk, and queued readers that never share an arriving body.
- Conformance through the accelerator; restart integration test; crash test that kills the process during fills.
- S2D: integration tests for a large upload and a large object streaming through, an upload whose body fails its hash never reaching S3, dot-segment keys reaching S3 as written, and a stalled client keeping the bytes it was sent while their slot is rewritten; planted bugs for each, and for a gateway that reads past a node's response head.
- Zero-copy verification: an integration test runs the storage node and gateway under `strace` and asserts that hit bodies leave through `sendfile` (storage node) and `splice` (gateway) system calls covering at least the body's bytes, and that no `write`, `writev` or `sendmsg` carries body bytes. The server's own counters are not evidence. A planted bug that forces the copying path must fail the test. CI installs `strace` and runs it.
- Sync-order verification: the same `strace` run asserts that each block's `pwrite` to the slab file is followed by an `fdatasync` of it before the block's record is written, and that a cleared record is synced before the next block write. Planted bugs that record before the sync, or write a block before syncing a clear, must fail it.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S2A: Hashed slot records, saved metadata, late readers | `VersionId` is a hash of bucket and key plus a 128-bit hash of bucket, key and ETag, so `SlotRecord` has a fixed size; a version recovered from the slot table is known by its hash until a read names it. Homes save immutable-bucket metadata (`Action::Remember`, `Action::Forget`), and `Node::recover` replays the metadata file, keeping the latest entries up to capacity (unit test `a_replayed_key_keeps_one_entry`). `Action::Fetch` says whether a body streams: fills span at most a chunk and may be read until released; a first fetch's body is read only by the requests queued behind it as its head arrives, and a later reader waits for the block's write (`Await::Written`) or fetches it again. The simulator's disk models the metadata file, whose unsynced tail a crash may lose, and checks every saved entry against the model of S3; it fails a run in which a node reads a streaming body after its head's arrival or holds a fill of more than a chunk. Scenarios: `an_immutable_object_read_after_a_restart_costs_no_s3_request`, `a_write_through_the_home_outlasts_a_restart`, `concurrent_first_reads_share_one_fetch`. Seed 59 found a replayed key whose stale recency entry later evicted its own metadata, so a read fetched it forever; `seeds_pass` runs it. |
| Complete | Test | Planted bugs for S2A | Caught by the simulator's tests: immutable metadata never saved, a restart that brings back metadata a write dropped (first missed: the scenario's old blocks were not stored, so an `If-Match` fill caught the change; they now are), a late reader that joins a streaming body, a fill larger than a chunk, a reader waiting on a first fetch's write never told, queued readers that never share an arriving body, and a queued reader that shares a body lacking its bytes. A replayed key with a stale entry is caught by the unit test `a_replayed_key_keeps_one_entry`, and by seed 59 in `seeds_pass`. |
| Complete | Work | S2B: On-disk store, sync ordering and recovery | `crates/server/src/disk.rs`: a slab file, a slot table of 64-byte records behind a header that names the layout, the run and the clean mark, and a metadata file of checksummed entries whose torn tail a start cuts off. A block's bytes are synced before its record is written, and cleared records are synced before any later block write; writes and verifications run on blocking worker threads. SIGTERM waits for work in progress, syncs, and marks the table (after conformance through the accelerator, the header read run 1, trusted from run 1). Unit tests: `records_round_trip_and_torn_ones_fail_their_check`, `metadata_entries_round_trip_up_to_a_torn_tail`, `a_restart_reads_back_the_records` (untrusted after a crash, trusted across two clean restarts, discarded for another layout). Integration tests: `a_clean_restart_keeps_the_cache_warm` (no S3 request after two restarts) and `a_crash_restart_verifies_blocks_before_serving_them` (a damaged block comes from S3 again, the rest from disk). Conformance passes through the accelerator with blocks on disk. The sync order is verified from outside the server in S2E. |
| Complete | Work | S2C: Multi-process cluster and restart test | `crates/server/src/protocol.rs` carries reads, answers and write notices over HTTP/1.1 with the cluster's secret (`requests_round_trip`, `answers_round_trip`). `gateway_engine.rs` sends reads over pooled connections, retries once on a fresh connection when an idle one has closed, and fails a read over at once when a node cannot be reached; `node_engine.rs` serves gateways and shuts down cleanly on SIGTERM. The config names the cluster (`[cluster]` with its secret and nodes) and a process's roles (`[gateway]`, `[node]`). `scripts/cluster start 3` runs three node processes and a gateway process; conformance passes 9 of 9 through it, and CI runs it after the single-process run. Tests: `a_node_restart_keeps_the_cache_warm` (no S3 request after the node process restarts) and `a_node_killed_during_fills_serves_correct_bytes_after_it_restarts` (six rounds of 16 concurrent fills at 20 ms S3 latency, the node killed 30 to 105 ms in, then every object read correctly twice). |
| Complete | Work | S2D: Streaming bodies, `sendfile` and `splice` | `zero_copy.rs`: `sendfile` from the slab file on blocking worker threads, polling a full socket; `splice` between sockets through a pooled pipe on the event loop; and `PageCache`, which drops a slot's unreferenced pages (`FADV_DONTNEED`) and reports with `mincore` whether any remain. A node's reply is a list of parts: runs of stored blocks go with `sendfile`, held fills and arriving bodies from memory. A first fetch's body passes through the home in lockstep to the replies and slots that read it as its head arrived; fills are held, capped at the bytes asked for. A gateway reads a node's response head with `peek` and consumes only the head, and the client's connection relays the body with `splice` (`Event::Forward`); node connections return to the pool only once read in full. Uploads stream to S3 over a body channel, with a signed SHA-256 checked before the last bytes go; S3's responses stream back, chunked when S3 sent no length; only a `DeleteObjects` key list is read whole, up to 8 MiB. `max_body` is gone. The S3 client is hyper with hyper-rustls, so paths go out as written. Before a slot write, the disk drops the slot's pages and waits while any stay cached: a check on this kernel showed a receiver reading the new bytes after `sendfile` returned, and `FADV_DONTNEED` keeping the pages a socket or pipe held until they were read. The slab file is read with `FADV_RANDOM`, slots must be multiples of the page size, and a data directory on tmpfs is refused. Tests: `a_large_upload_streams_through_to_s3` (40 MiB), `a_body_that_fails_its_hash_never_reaches_s3`, `a_large_object_streams_through_the_cache` (24 MiB through an 8 MiB cache), `sent_pages_survive_a_rewrite_of_their_slot` (fails with the page check removed), `dot_segment_keys_reach_s3_as_written`, `an_oversized_key_list_is_refused_before_it_is_read`; conformance adds `large_objects_read_whole_and_in_ranges` (20 MiB) and passes 10 of 10 against s3proxy, through the accelerator, and through a three-node cluster. s3proxy answers 500 for keys with `..` segments, so that case has no conformance test. On this machine the user's pipes exceed `pipe-user-pages-soft`, so the kernel caps new pipes at 8 KiB; benchmarks in S3 tune pipe sizes. |
| Complete | Test | Planted bugs for S2D | Caught by the server's tests: a key list of any size is read, `DeleteObjects` leaves cached metadata, the S3 client drops `.` segments, S3 receives a body that fails its hash, a slot is overwritten while its pages are in flight, an arriving body's blocks are never written, and a gateway reads past a node's response head. |
| Complete | Test | S2E: `sendfile`, `splice` and sync order verified under `strace` | `crates/server/tests/zero_copy.rs` runs a node and a gateway as separate processes under `strace -f -ttt -yy -xx` and parses every call. `hits_leave_by_sendfile_and_splice`: three objects of three 64 KiB blocks, each read twice; during the hits the node sent all 589,824 bytes with `sendfile` from the slab file, the gateway spliced all of them into client sockets, and no write, `writev`, `sendto` or `sendmsg` to a socket or pipe in either process carried 32 bytes in a row of any body (they wrote 579 and 863 bytes, the heads); writes to files are the node storing blocks, which on a slow runner may still be going as the hits begin. `blocks_are_synced_before_their_records_and_clears_before_reuse`: ten objects through a sixteen-slot cache; each of 30 records follows an `fdatasync` of the slab file that began after its block's `pwrite64`, and each write over any of 14 cleared slots follows an `fdatasync` of the slot table that began after the clear. Planted bugs, all caught by these tests: a hit copied with `read` and `write`, a relay copied through a buffer into the pipe (first missed: the check looked only at socket writes; it now checks sockets and pipes), a record written before its block's sync, and a block write begun before a clear's sync. CI installs `strace`. |
| Complete | Gate | Code review | `/code-review high` on the S2D and S2E commit found 9 issues, all resolved. A node's reply waited for the whole arriving body, so a reader of a prefix held its connection, and the gateway's next read over it, until the body ended; a reply now ends once its bytes have passed (`a_prefix_reader_frees_its_connection_before_the_body_ends`, over a 4 MiB body trickled at 25 ms per 64 KiB). A held part shorter than its segment let later parts go out at the wrong offsets; the reply now ends there. A response sent before the request's body was read closed with the body unread; the connection now stops writing and drains the body for up to two seconds, so the client sends its whole body and reads the answer (`a_write_s3_refuses_early_gets_s3s_answer`, which fails without the drain). Reads ignored the client's `Connection: close` (`a_read_tells_a_closing_client_it_closes`). Writes timed out after 60 s in total rather than 60 s without progress. The test harness left a traced server running when a test panicked. A write waiting on busy pages rechecked every 10 ms; it now backs off to a second. Chunked writes took three calls per frame; they take one. An unused method went, and the empty payload hash is one constant. Planted bugs, both caught: a reply waits for the whole arriving body, and a body answered before it was read is never drained. |

## Phase 4: Membership and Ring Changes

Goal:
The cluster resizes without losing its cache: new owners fill from previous owners, and the simulator measures the hit rate through a resize.

Scope:
- 4A foca in the core, fed packets and timer events; ring snapshots versioned by a hash of their members; the previous ring kept for the fallback window; nodes declared down stay in the ring for the down grace period; leaving nodes drop out at once; nodes announce themselves again to seeds they don't hear from.
- 4B Gateways fetch the ring from storage nodes; answers carry the node's ring version; a gateway fetches a ring whose version differs from its own, and asks the nodes it knows of when its ring names none that answer.
- 4C Within the fallback window, owners read blocks from their previous owner first, and new homes read metadata from the previous home; those blocks skip the doorkeeper. A starting node asks its seeds for their ring, so a new node knows whom it took over from.
- 4D Blocks a node no longer owns are evicted first once the fallback window ends, using stored placement hashes.
- 4E Simulator: nodes join, leave and are replaced; a property bounds the hit-rate drop and S3 GETs through a one-node resize, against a control run without fallback.
- 4F Server: gossip over UDP, peer reads over the cluster protocol, rings that carry addresses, joining through seeds, and leaving on `SIGUSR1`.

Completion gate:
A 10,000-seed sweep with resizes passes; the resize property holds; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator seeds with membership changes alongside Phase 3 faults.
- Planted bugs: fallback outside the fallback window; owned blocks evicted before non-owned ones; and one for each behavior above.
- Server integration tests: a node that joins, named in no other config, reads from previous owners; a node that leaves hands over its objects and exits.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: foca membership and ring snapshots | `crates/core/src/membership.rs` runs foca 2.0 with its postcard codec and a SplitMix RNG seeded by the owner. A node's identity holds its ID, weight, run, whether it is leaving, and its address; a later run wins an address conflict. Its ring is the nodes up and those down within `down_grace`, less those leaving, versioned by a hash of IDs and weights. A node announces itself again every ten probe periods to seeds it doesn't have up. `Node::on_ring` keeps the previous ring for `fallback_window`. Unit tests: `nodes_that_start_together_agree_on_the_ring`, `a_node_that_joins_later_enters_every_ring`, `a_down_node_stays_in_the_ring_for_the_grace_period`, `a_leaving_node_leaves_every_ring_at_once`, `nodes_cut_apart_past_the_grace_period_merge_again`. In the simulator, 80% of swarm seeds gossip over the simulated network, with its losses, partitions and delays, and timers on the event queue; scenarios `a_node_down_past_its_grace_period_leaves_every_ring` and `a_node_back_within_its_grace_period_changes_no_ring`. |
| Complete | Work | 4B: Gateway ring fetch and versioning | `Gateway::on_ring_version` fetches the ring from a node whose version differs, one fetch at a time; `Action::FindRing` asks the nodes the owner knows when no node in the ring can serve a read (scenario `a_gateway_whose_ring_names_only_gone_nodes_finds_the_ring`). Every node answer carries `x-accel-ring`, and a ring answer names each node's address. |
| Complete | Work | 4C: Previous-owner fallback | `Read::Stored` asks a previous owner for bytes it holds, answered from stored blocks or with 404; `Read::Known` asks a previous home for its metadata. `Action::PeerFetch` has its own `peer_timeout`, after which the node skips that peer until the next ring change. A fill whose previous owner lacks the blocks goes to S3 with the same slots and its waiting requests. Metadata from a previous home counts as validated when asked for, less its age, and a write the new home learned of since makes it useless. `Node::on_joined` gives a new node the ring its seeds had. Scenarios in `crates/sim/tests/resize.rs`, over eight seeds of varied delays: after a node joins, rereading forty objects takes at most one S3 request with fallback against 14 to 20 without; after one leaves, none against 18 to 27; bytes read from S3 fall from 3.7 to 6.6 KB to at most 128 bytes. The test asserts at most two S3 requests and a fourfold cut in both requests and bytes. Also `after_the_fallback_window_new_owners_read_s3`, `a_write_since_the_previous_home_knew_the_object_wins`, `blocks_from_a_previous_owner_skip_the_doorkeeper`. |
| Complete | Work | 4D: Eviction of non-owned blocks | When the fallback window ends, `Store::disown` marks every stored block its ring places elsewhere, and eviction takes those first (`disowned_blocks_leave_first`). |
| Complete | Test | 4E: Resize scenarios and hit-rate property | `Simulator::add_node`, `remove_node` and `fail`; swarm seeds resize at random while faults happen, at most six times: a node joins, one leaves, or one fails for good and another replaces it. The resize property is the pair of resize scenarios above, each against a control run with no fallback window. |
| Complete | Work | 4F: Server membership | `membership_engine.rs` gossips over UDP on the node's cluster address and drives foca's timers; `peers.rs` is the cluster protocol's client, shared by gateways and nodes, and learns addresses from rings. A starting node asks its seeds for their ring before it joins; `SIGUSR1` makes a node leave, serve through the fallback window, and exit. Integration tests: `a_node_that_joins_reads_from_previous_owners` (the joining node is named in no other config; rereading twelve objects takes no S3 request, against six with the fallback window at zero), `a_leaving_node_hands_over_its_objects_and_exits`, and `a_gateway_reaches_nodes_its_config_never_named`. Conformance passes 10 of 10 through one process and through a three-node cluster. |
| Complete | Gate | 10,000-seed sweep with resizes | `s3-accelerator-sim 0 --seeds 10000`: 0 failed, in 34 s, after the review's fixes. Earlier sweeps found five problems, fixed before this commit: settling dropped memberships, so scripted reads stopped gossip; a gateway whose ring named only departed nodes never learned another (now `FindRing`); unbounded churn left search walking thousands of departed nodes (resizes capped); partitioned groups never merged (announcements to silent seeds); and seed 5757, where a new home's first fetch evicted a block of its own version and dropped the version while still storing its blocks (the fetch now holds the version; `an_eviction_during_a_first_fetch_keeps_its_version`). |
| Complete | Test | Planted bugs for Phase 4 | Caught by the core's unit tests: a down node leaves the ring at once, a leaving node stays in the ring, a restarted node loses to its earlier run, nodes never announce again to silent seeds, and owned blocks are evicted before disowned ones. By the simulator's tests: gateways ignore ring versions, a gateway never asks for the ring elsewhere, fallback outside the fallback window, a joining node takes no previous ring, new owners never ask previous owners for blocks, a new home never asks the previous home, a previous home's metadata outlives a write, blocks a previous owner lacks are not read from S3, blocks from a previous owner wait for the doorkeeper, and a first fetch lets an eviction drop its version. By the server's tests: a starting node takes no ring from its seeds, a leaving node never stops, and gateways learn no addresses from rings (first missed: failover reached the previous owner, which served its own copy; `a_gateway_reaches_nodes_its_config_never_named` now leaves the gateway only a node its config never named). Four earlier planted bugs were updated to the changed code. |
| Complete | Gate | Code review | `/code-review high` on the Phase 4 commit found 10 issues, all resolved. A joining node's first ring change replaced the seeds' ring with its own startup ring; changes within an open fallback window now extend it and keep the ring from before the first (`a_node_that_knows_one_seed_reads_from_previous_owners`, where the new node knows one seed). A failed ring fetch blocked the next for a node timeout; `Gateway::on_ring_failed` clears it (`a_failed_ring_fetch_lets_the_next_answer_ask_again`). A gateway on the old ring told only the old home of a write; each home now passes a write to the home under its other ring within the window (`a_write_through_the_old_home_reaches_the_new_one`), and changes are recorded for a window whatever the ring state. Gossip addresses were learned only with a ring change, after replies to a new node were dropped; the membership engine now learns them before carrying out any action, resolving names asynchronously and only when an address changes, and a node starts when another node's name does not resolve (`a_node_starts_while_another_address_does_not_resolve`). Disowned marks were bare keys; they now carry the entry's sequence number and a ring change clears them (`disowned_marks_end_with_their_block`, `cleared_marks_leave_eviction_to_s3_fifo`). Fill runs could cross placements; they now stop where the placement changes (`a_read_across_two_placements_asks_each_previous_owner`). New tests cover skipping a previous owner that never answers (`a_previous_owner_that_never_answers_is_skipped`) and metadata keeping its age (`metadata_from_a_previous_home_keeps_its_age`). One finding, a leaving identity built from a stale run, turned out harmless: foca renews a stale identity itself, as `a_node_that_rejoined_can_still_leave` shows; the node now reads foca's identity. The sweep after these fixes found seed 8590: a new home took a deleted object's metadata from its previous home, S3 answered the revalidation with 404, and the home asked again within one tick; a change S3 reveals now counts as a change (`a_change_s3_reveals_outweighs_a_previous_homes_metadata`). Planted bugs, all caught: a joining node's previous ring gives way to its startup ring, a failed ring fetch holds up the next, a write through the old home stays there, disowned marks outlive their blocks, a ring change leaves disowned marks, fill runs cross placements (first missed: nothing read across two placements), an unanswered previous owner is asked again, a previous home's metadata ages from when it arrived, gossip addresses are learned only with a new ring, an unresolvable address stops the node, and a change S3 reveals leaves a previous home's metadata usable. |

## Phase 5: Writes, Hot Keys and Warming

Goal:
Writes pass through the home without exposing stale data, hot keys spread across replicas, and warming cuts first-read misses.

Scope:
- 5A `PutObject` through the gateway and the home: the home drops or replaces metadata, discards first fetches that started before the write, and forwards invalidations; the proxying gateway drops its entry.
- 5B `events` freshness mode with a model of S3 event notifications that delays and duplicates them.
- 5C Hot-key leases: rate tracking, leases to the next K candidates, hot hints, renewal above half the promotion threshold, expiry.
- 5D Warming on write with the HEAD check, and metadata prefetch for Parquet, ORC and safetensors; the simulator's model generates objects with valid trailers and headers.
- 5E Properties: read-after-write through the writing gateway; hot-key load spread; prefetch removes the second miss.
- 5F Purge: the home drops an object's metadata and has every chunk owner drop its blocks. Each node makes the purge durable before it confirms, and the home keeps unconfirmed purges on disk and resends them to owners that were down.

Completion gate:
A 10,000-seed sweep with writes passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator seeds with writes through the cache, event delivery faults and hot keys.
- Scripted scenario: a chunk owner is down during a purge and restarts afterward.
- Planted bugs: a first fetch that started before a write still indexed; warmed blocks indexed without the ETag check; a lease that never expires; a purge that an owner missed while down.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Writes through the home | Gateways pass every request the cache doesn't serve through a storage node, which re-signs it, and need no `[origin]` (`crates/server/src/passthrough.rs`, `NodeRequest::Forward`; `requests_the_cache_does_not_serve_pass_through_nodes`, including a chunked listing; `scripts/cluster`'s gateway has no credentials). The node drops the home's metadata before the gateway hears (`a_read_after_a_write_through_the_gateway_sees_the_write`); a node other than the home passes the write on (`a_write_around_a_suspected_home_reaches_the_home`); a gateway reads a key it wrote around the home directly from S3 until the bucket's TTL passes (`a_gateway_reads_a_key_it_wrote_around_its_home_from_s3`; seed 2093); a gateway's write markers outlast cache eviction (`a_write_outlasts_its_key_leaving_the_cache`; seed 2921). The simulator passes writes through gateways and nodes as messages in three seeds of four. Conformance `reads_after_an_overwrite_and_a_delete_see_them` passes against s3proxy, one process and a three-node cluster (11 of 11). `scripts/mutants` catches the new planted bugs (a write marker leaving with its cache entry, a gateway trusting a home it wrote around, a node keeping a write from the home, a gateway or node skipping its half of a write, a chunked answer relayed unframed) and the seven earlier write and pass-through bugs, four of them repointed at code that moved. |
| Complete | Work | 5B: `events` freshness mode | Nodes long-poll an SQS queue of S3's event notifications, bare or in SNS's envelope (`crates/server/src/sqs.rs`, signed for `sqs`; unit tests `reads_s3_events`, `reads_events_inside_sns_envelopes`, `a_test_event_names_no_change`). A node passes each event to the key's homes and deletes the message once each has it (`Node::on_event`, `on_event_notice`, `on_event_passed`; server test `an_event_from_the_queue_reaches_the_home` against a fake SQS). A home keeps metadata of the version an event names. The simulator's queue, in two seeds of five, offers events after a delay, repeats some, and offers again those no node finished within the visibility timeout; a read sent after a change's event finished, and the gateway's older entries expired, must see the change while the gateway routes by that node's ring (scenarios in `crates/sim/tests/events.rs`). Seed 426 found stand-in reads trusting a candidate's own ring during a split; the gateway now marks reads to any node but the home under its ring as direct (`a_read_sent_around_the_home_goes_direct`). `scripts/mutants` catches every planted bug: an event kept from the home, an event no home heard leaving the queue, an event dropping the version it names, a home ignoring a passed event (core and server), a finished message left in the queue, queue requests signed for S3, `+` kept in event keys, and a stand-in trusting its own ring. |
| Complete | Work | 5C: Hot-key leases | The owner of a placement read `hot_threshold` times in a window leases it to its next `hot_replicas` candidates (`Node::note_reads`, `lease_out`); replicas fill from the owner before S3 and admit its blocks without the doorkeeper; answers carry hints that gateways spread range reads by (`Gateway::on_hot`, `spread`); replicas report reads halfway through a lease, and the owner renews three quarters through while the placement stays at half the promotion rate (`tick_leases`). Scenarios in `crates/sim/tests/hot.rs`: `a_hot_placement_spreads_across_its_replicas` (reads split across three nodes, no more than 12 of 30 on any, replicas fill from the owner with no S3 request) and `a_lease_runs_out_once_reads_stop`. Server test `a_hot_key_is_read_from_its_replicas`: with the home stopped, a hot key's reads come from the replicas' blocks with no S3 request. The simulator draws thresholds, windows, replica counts and leases per seed, and sends a share of reads to one key. Planted bugs: a lease that never runs out, a gateway ignoring hints, a replica that never fills from the owner, a replica that never stores leased blocks. |
| Complete | Work | 5D: Warming on write and metadata prefetch | Warming on write: A home keeps its region of an upload as the body passes to S3, within a 256 MiB budget (`passthrough::to_s3`, `NodeEngine::keep_upload`), and once S3 takes it, checks the version with a HEAD and stores the blocks and metadata (`Node::warm_region`, `on_uploaded`, `warm_checked`). Scenarios `an_upload_through_its_home_is_read_from_disk` and `an_upload_replaced_before_its_check_is_not_stored`; server test `an_upload_warms_its_home` (the first read after an upload costs no S3 request). The simulator warms each bucket in half its seeds; seeds 947 and 6265 found a warm check dropping requests that waited on a revalidation and a warm store losing its version to an eviction. Planted bugs: a kept upload stored without the ETag check; a home that keeps no upload. Metadata prefetch: `crates/core/src/formats.rs` finds the metadata span at Parquet's and ORC's trailers and safetensors' header (4 unit tests); a home stores a spot's blocks past the doorkeeper, reads the spot once they are ready (`Action::ReadSpot`, `Node::on_spot`), and fills the span (`Node::prefetch`), also after a first fetch too small to store a block. The simulator's model frames objects named for these formats, in half its seeds. Scenarios in `crates/sim/tests/prefetch.rs`; server test `a_parquet_footer_is_prefetched`. Planted bugs: a home that never prefetches, a spot's blocks waiting for the doorkeeper, a first fetch of a spot storing none of it, spot reads coming back empty. |
| Complete | Test | 5E: Write, hot-key and prefetch properties | A read through the gateway that passed a write, sent after the write succeeded, sees it while the gateway's ring stays the same (`Simulator::answer`); 10,000 seeds pass. Hot-key load spread: `a_hot_placement_spreads_across_its_replicas`. Prefetch removes the second miss: the scenarios in `crates/sim/tests/prefetch.rs` read each format's metadata from disk after its spot. |
| Complete | Work | 5F: Durable purge | `POST /bucket/key?x-accel-purge` goes to the key's home (`server::purge`), which drops every version's metadata and blocks, erasing their slots with a punched hole (`Node::on_purge`, `Disk::erase`), and passes the purge to every other node in its rings; blocks being written or read go once free (`drop_purged`). Each node syncs before it confirms (`NodeEngine::purge`). The home records which nodes have yet to confirm in a purge log (`Disk::save_purge`), tells them again every four peer timeouts, and restores the log on restart (`Node::recover`). Scenarios in `crates/sim/tests/purge.rs`, among them an owner down during the purge and a home that crashes after it; server test `a_purge_reaches_a_node_that_was_down`. The simulator purges after a share of deletes in half its seeds. Planted bugs: a purge an owner missed while down never told again; a restarted coordinator forgetting its purges. |
| Complete | Gate | 10,000-seed sweep with writes | `cargo run --release -p s3-accelerator-sim -- 0 --seeds 10000`: 0 of 10,000 failed after the review fixes. Seeds pass writes through gateways and nodes in three of four, S3 events in two of five, and draw leases, warming, format-named keys and purges after deletes. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved in `acc6a7f`: a purge confirmed while records of blocks being read or verified survived (records now go at purge time and a purged block is never recorded; the simulator checks each node that purged records none of the key, `a_purge_during_a_read_leaves_nothing_a_crash_restores`); a purge mark leaked from a failed fill (the mark lives on the store entry); purges told nodes out of every ring forever (they wait for the node to return, `a_purge_is_told_again_to_nodes_in_the_ring`, and the purge log is compacted on open); the purge log synced on the event loop and a failed append still confirmed (it syncs with the purge's durability and a failed append fails the purge); aws-chunked uploads were warmed by their wire bytes (`only_whole_bodies_are_uploads`); lifecycle expirations kept metadata (only creations do, `an_expiration_drops_metadata`); the spec claimed node-side grant checks and aws-chunked decoding (corrected; per-gateway identity stays a design goal, as nodes check the cluster's shared secret); prefetch fetched blocks the fill budget would not store (`a_prefetch_past_the_fill_budget_sends_nothing`); reads and prefetch split fill runs in two loops (`Node::fill_run`); `x-accel-purge=` passed through to S3 (`a_purge_reaches_a_node_that_was_down` sends it). |

## Phase S3: TLS and Benchmarks

Goal:
Clients and peers connect over TLS with zero-copy intact, and benchmarks answer the storage-layout open question.

Scope:
- S3A rustls handshake and the `ktls` crate for client and peer listeners.
- S3B Benchmarks on NVMe: hit throughput, time to first byte, fill throughput, and drive writes under scan and reread workloads.
- S3C Record the storage-layout decision in `spec.md`.

Completion gate:
TLS conformance passes; kTLS and zero-copy under TLS are verified from outside the server; benchmark results are recorded; `/code-review` findings are resolved.

Testing plan:
- Conformance over TLS; benchmark harness with recorded results.
- kTLS verification: after a TLS handshake, the socket reports the `tls` ULP (`ss -tie` shows it), `/proc/net/tls_stat` transmit counters (`TlsTxSw` or `TlsTxDevice`) rise for each connection, `strace` shows hit bodies spliced into the TLS socket, and the TLS client decrypts every body byte correctly. A run forced onto userspace TLS must fail the kTLS assertions. CI loads the `tls` kernel module and runs it.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | S3A: rustls and kTLS listeners | Clients: `[gateway.tls]` runs rustls's handshake with no session tickets, then moves the session into the kernel with the `ktls` crate (`crates/server/src/tls.rs`); without the `tls` module, or with `kernel = false`, a task relays through a loopback socket. Sessions end with a `close_notify` (`a_kernel_tls_session_ends_with_close_notify`, `a_userspace_tls_session_ends_with_close_notify`); any other record ends the connection. Peers: `[cluster.tls]` gives every member a certificate the cluster's CA signed; nodes require one (`Tls::members`), and members verify that a node's names the host of its address (`Connector`), over kTLS the same way. `members_prove_themselves_with_the_clusters_certificates` refuses a gateway or node with another CA's certificate or without TLS. Conformance passes 11 of 11 over kernel TLS on both links through a three-node cluster (`scripts/cluster start 3 --tls`; `ss` shows the `tls` ULP on 54 node-side peer sockets), and CI runs it. Planted bugs: sessions kept in userspace, member sessions kept in userspace, a node taking members without the cluster's certificates, a member reaching nodes without TLS, a kernel session ending without `close_notify`. |
| Complete | Test | S3D: kTLS verified through kernel state and `strace` | `hits_reach_kernel_tls_sessions_through_sendfile_and_splice` (`crates/server/tests/tls.rs`) reads three 192 KiB objects twice through a traced node and gateway, with TLS from the client and mutual TLS between gateway and node. With kernel TLS: `ss -tie` shows `tcp-ulp-tls` on the gateway's client socket and both ends of the peer connection; `/proc/net/tls_stat` counts a receiving session for each of those three sockets; `strace` shows each process setting the `tls` ULP, the node sending 589,824 of 589,824 hit bytes with `sendfile` into its TLS socket while writing 627 bytes of heads, and the gateway splicing 589,824 into the client's TLS socket while writing 273; no write carries body bytes; every body decrypts correctly. The same run forced onto userspace TLS shows no ULP, no kernel sessions, nothing moved inside the kernel into either link's sockets, and the relays writing 591,747 and 592,101 bytes from userspace. CI loads the `tls` module and runs it. `scripts/mutants` runs the kernel TLS tests as a layer. |
| Complete | Test | S3B: NVMe benchmarks | `crates/bench` runs a node and a gateway as processes on the NVMe drive against an in-process stand-in for S3, and measures from outside the server (clients' clocks, the stand-in's bytes, `/proc` CPU, write and drive counters). `BENCHMARKS.md` records a run: fills 2.13 GiB/s; hits 16.8 GiB/s from the page cache and 4.9 GiB/s from the drive; 64 KiB range hits 0.09 ms to first byte from one client; a scan of three times the cache sends none of the hot set back to S3 and writes nothing; Zipf rereads over twice the cache hit 68% of bytes at 0.13 bytes written per byte served; hits over kernel TLS 7.67 GiB/s against 19.06 plaintext and 0.92 userspace. The runs found three defects, now fixed: small blocks never written where a larger block's large folio stayed cached (the node waited 30 s and gave up; `small_blocks_take_space_that_held_larger_ones`); kernel-TLS relays encrypting on the gateway's event loop (1.79 to 7.67 GiB/s on worker threads; the kTLS test checks no hit byte is spliced from the event loop); a sync per block write (writes now share syncs; `a_failed_sync_fails_every_write_it_covered`). Limits recorded: a node's event loop bounds its fills near 2.1 GiB/s, and each concurrent whole-object miss holds its object's size of the fill budget. Planted bugs: a write dropping only its own slot's pages, relays under kernel TLS on the event loop or through userspace, a write sharing a sync that began before it ended, a write a failed sync covered counting as durable. |
| Complete | Doc | S3C: Storage-layout decision | `spec.md` Storage: slots of size classes in extents stay, with extents of one block by default, and a class that needs room empties the extent eviction's last victim left. The size shift cost a hot set 0.14 GiB with 64 MiB extents and none with one-block extents; `a_shift_to_smaller_blocks_keeps_the_hot_set` (store alone, hot and cold blocks sharing extents: 0 misses with one-block extents, 752 of 4,096 with 64-slot ones) and `emptying_an_extent_follows_eviction` hold the store to it. The open question is closed; unmeasured wear from small random writes is recorded. Planted bugs: a class's room from the extent with the fewest blocks, a reservation never emptying an extent, 64-block default extents. 10,000 seeds pass. |
| Complete | Gate | Code review | `/code-review high` found 10 issues, all resolved in the commit after `1f764f0`: a reservation refused when the last victim's extent held a filling block (it tries each victim's extent, then the idle extent with the fewest blocks; `a_busy_victim_extent_yields_to_an_idle_one`); a later failed sync could fail a durable write, and a write that ended during a failed flush could count as durable (syncs are numbered, and a write fails only if a sync that overlapped it or followed it first failed; `a_write_during_a_failed_sync_fails`); the userspace-TLS loopback took whatever connection arrived first, so a local process could race in (it takes only its own; `a_loopback_pair_takes_only_its_own_connection`); raising `block_size` alone panicked (extents default to one block of whatever size, and `check` validates the store's geometry; `the_store_geometry_is_checked`); the new default extent discarded every node's cache on upgrade (a table for other extents over the same slots is kept); kTLS relays parked blocking-pool threads that disk I/O shares (they run on a relay runtime, one thread per core); every retry dropped a whole span's pages (only spans a larger block was written into since are dropped whole); the spec understated extent memory for small classes; `scripts/cluster` took `--tls` only third. Planted bugs for each code fix, checked by hand; 10,000 seeds pass. |
| Complete | Test | Planted-bug sweep | `scripts/mutants` at `1f764f0` caught 133 of 139, with the kernel TLS tests as a layer. Of the six missed, two pointed at code that had moved: "nodes store blocks they do not own" matched a copy of the ownership check in hot-key counting, and "a kept upload is stored without the ETag check" left the size check that the Phase 5 review added, which the replacement's other size tripped; both are repointed, and `an_upload_replaced_before_its_check_is_not_stored` now also replaces the upload with one of the same size. Three had lost their scenarios since Phase 4, as writes now reach previous homes and previous owners now hold every block: new scenarios in `crates/sim/tests/resize.rs` catch them (`a_write_the_previous_home_missed_still_wins`, `a_change_the_new_home_saw_outweighs_the_previous_homes_metadata`, `blocks_a_previous_owner_lacks_come_from_s3`). "A first fetch lets an eviction drop its version" is retired: `admit` refers to the version before evictions run, so the fetch's own reference matters only if a reservation evicts the version's blocks and then finds every extent busy, which no scenario or seed reaches. The review fixes' planted bugs were checked by hand. At `fd2fdd3`, the full sweep catches all 141: 23 by the core's unit tests, 64 by the server's tests, 50 by the simulator's tests and 4 by the kernel TLS tests. |

## Phase 6: Spec Reconciliation

Goal:
`spec.md` and the code agree. The code fixes the defects that an audit of the spec against the code found, the gateway accepts presigned URLs and virtual-hosted addressing, and the spec describes what the code does.

Scope:
- 6A Spec corrections where the code's behavior stands: reads with `versionId` pass through to S3; freshness and admission policies are set per bucket; the `events` mode is the cluster's queue over each bucket's TTL, under the config's real keys and defaults; a node's weight is configured, typically from its disk size; replicas hold nothing an invalidation must reach; the doorkeeper's window counts reads; growing the extent size keeps only the class that claims each merged extent first; the index's memory per block and the slot table's size; only a timeout stops a node asking a previous owner; waiters share 5xx answers as well as 404s; warming on write replaces metadata after its HEAD check; config-named nodes join the ring as down; `SIGUSR1` makes a node leave; which streaming payloads get 501; prefetch starts when a block of the version is written; where the store makes room when a victim's extent is busy. `config.rs`'s comment on which nodes a gateway reaches is corrected too.
- 6B Dead nodes (core, simulator first): a gateway routes around a node whose connection fails or times out, and around nodes its ring reports down. The spec names both timeouts.
- 6C Cluster secret: every gossip packet carries an HMAC under the cluster secret, and nodes drop packets without a valid one. Every check of the secret runs in constant time.
- 6D Store and purge:
  - A purge syncs the slab file before the node confirms it. A gateway keeps an `immutable` bucket's metadata at most a configured purge window, so a purge reaches every gateway within it.
  - A node rewrites its metadata file in LRU order once the file passes twice the cache's capacity, so the file stays bounded and a restart loads the most recently used entries.
  - A node preallocates its slab file at start, and a start without the space fails with a clear error.
  - Blocks skip the doorkeeper only when a previous owner or lease owner supplies them, or when their placement is leased; a refill from S3 passes the doorkeeper.
  - Lease renewal scales each replica's reported reads by the time they cover, so renewal takes half the promotion rate, as the spec says.
  - SSE-C uploads skip warming.
- 6E Gateway auth and the S3 API:
  - Presigned URLs: SigV4 in the query string for any method, with `X-Amz-Expires` up to seven days and the same clock skew as signed headers. The gateway strips the auth parameters before it looks up the cache, so presigned GETs hit.
  - `response-content-*` overrides, applied per request to hits and misses.
  - Virtual-hosted-style addressing for the gateway's configured domains, alongside path style.
  - Grants gain an access level: `read`, `write` (which includes read) or `admin` (which includes write and allows a bucket's configuration and deletion). Listings are authorized against their `prefix` parameter.
  - Full-object checksums: the home asks S3 for them on its first fetch, keeps them in the object's metadata, and returns them on whole-object reads that ask for them.
- 6F Conformance tests for each behavior the gateway serves: presigned GET, HEAD and PUT; expired and altered URLs; response overrides; virtual-hosted requests; grant levels and listing prefixes; copy-source grants; the SSE-C bypass; the streaming 501; the `DeleteObjects` size limit; checksums on hits.

Out of scope:
- Presigned POST uploads and caching versioned reads.
- Per-gateway identity on nodes, and S3 credentials from IAM roles (Phase 9).
- Kernel version checks: the spec states the kernel requirement.

Completion gate:
Each audit finding is resolved in the ledger; conformance passes against s3proxy and through the accelerator in each CI configuration; a 10,000-seed sweep passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator: after a node dies, a gateway's reads pay at most one timeout per suspect window; a scan inside the fallback window gets past the doorkeeper no more than outside it; leases renew with reads spread across replicas at 55% of the promotion rate.
- Server tests, with evidence from outside the server: a forged gossip packet leaves the ring unchanged; `strace` shows the slab's sync before a purge's confirmation; the metadata file stays bounded across restarts; a start without space fails.
- The conformance tests in 6F.
- Planted bugs, one per fix: a failed connection that leaves the node in rotation; unsigned gossip accepted; a purge confirmed before the slab sync; a metadata file never rewritten; a refill from S3 that skips the doorkeeper; an SSE-C upload warmed; auth parameters left in the cache key; an expired URL accepted; a `read` grant that writes; and the rest in kind.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: Spec corrections | `spec.md` now says: `versionId`, date conditionals, ETag lists, wildcards and weak ETags, `partNumber` and other query parameters pass through; freshness and admission are per bucket, under the config's keys; `[events]` is one queue over each bucket's TTL; node weights are configured; no replica invalidation; warming replaces metadata after its HEAD; only a timeout stops asking a previous owner; waiters share 404s and 5xx; config-named nodes start down; `SIGUSR1` leaves; the doorkeeper's window counts first reads; extent-size changes; where the store makes room past a busy extent; prefetch within the fill budget; the S3 access key; the streaming 501s; the shared cluster secret. `crates/core/tests/memory.rs` measures the index with a counting allocator: 495 bytes per 1 MiB block and 417 per 4 KiB block, against the 100 the spec stated. `config.rs`'s comment on which nodes gateways reach is corrected. |

## Phase 7: Bottlenecks

Goal:
Fills run as fast as S3, the network or the drive allow, a miss holds a bounded share of the fill budget, hits keep their first-byte latency while fills run, and a node with a large drive starts quickly and keeps its index in memory.

Scope:
- 7A Profiles first: CPU profiles (`perf`) of the node and the gateway under the benchmark's fill, hit and 64 KiB range workloads, recorded in `BENCHMARKS.md`. Each later item cites the profile it answers.
- 7B Read-ahead window (core, simulator first): a gateway asks for a response's chunks a configured number of bytes ahead of the part it relays, so a miss holds at most that much of each owner's fill budget. Failover and early ends keep their behavior. The default fill budget is revisited with the window in place.
- 7C Fills on worker threads: the node's S3 client runs on worker threads, which receive S3's bodies, decrypt S3's TLS, relay bytes to readers and write blocks. The event loop handles heads, completions and the core's actions.
- 7D Hit latency under fills: hits' first byte reached p99 1.19 s while fills ran. Find the queue they wait in and remove it.
- 7E Per-request cost: a 64 KiB range hit costs 5 to 8 times the CPU per byte of a large hit. Cut the per-request work the profiles show.
- 7F Start time and index memory at scale: the slot table holds a record for each 4 KiB of disk, and a start reads all of it, 64 GB at 4 TB; the index costs about 250 bytes per block. Measure both for a 4 TB node, set targets, and shrink them, for example with records per slot of the extent's class, a table that marks empty regions, or a smaller `BlockKey`.
- 7G Rerun the benchmarks and record before and after in `BENCHMARKS.md`.

Completion gate:
The rerun shows a node filling past 2.1 GiB/s until the drive, the network or the stand-in limits it, with every admitted byte written; a miss holds at most the window of the fill budget; hits' p99 first byte under concurrent fills improves; a 4 TB node meets 7F's targets for start time and index memory; a 10,000-seed sweep passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator: a property that a gateway's outstanding chunks per response stay within the window; a scenario where a large miss under a small fill budget is admitted in full.
- Server, with evidence from outside the server: per-thread CPU from `/proc/<pid>/task/*/stat` shows S3's bodies received off the event loop; a restart over a large sparse slab file measures start time.
- Planted bugs: a gateway that asks for every chunk at once; a window that ignores its bound; a restart that skips part of the slot table.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |

## Phase 8: Observability

Goal:
An operator sees what each gateway and node does: traffic by outcome, cache efficiency, fill and store pressure, the health of S3 and peers, membership, and whether zero-copy and kernel TLS are in use.

Scope:
- 8A Spec section: the metrics with their names, types and labels; the health and readiness endpoints; the log format; and the rule that metrics cost the hit path nothing measurable.
- 8B Core counters: the core counts what it decides (hits, misses by reason, admissions and doorkeeper rejections, evictions by cause, fill budget in use, leases, ring changes, fallback reads, purges) in its state, and its owner reads them. The server measures latency, so the core still reads no clocks.
- 8C Server metrics in Prometheus's text format on an admin listener: requests and latency histograms by operation and outcome; bytes by source (cache, S3, previous owner); S3 latency and errors; peer failures; store occupancy by size class; sync latency; relay errors; TLS sessions by kind; event queue lag; membership and ring version. Each thread keeps its own counters and histograms, merged on scrape.
- 8D `/healthz` and `/readyz` on the admin listener; a node is ready once its store has recovered and it knows the ring.
- 8E Structured logs with levels set in config, and a request ID that follows a read from the gateway through the nodes to S3.

Out of scope:
- Distributed tracing.

Completion gate:
Metrics agree with outside observations in server tests; benchmarks with metrics on stay within 2% of Phase 7's results; a 10,000-seed sweep with the counter property passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator property: the core's hit and miss counts agree with the model's record of which reads reached S3.
- Server tests: after a scripted workload, `/metrics` matches the S3 stand-in's request log and the clients' byte counts; readiness waits for recovery and the ring.
- Planted bugs: a miss counted as a hit; S3's bytes counted as cache bytes; a node ready before recovery.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |

## Phase 9: Metadata Service

Goal:
An external metadata service holds the authoritative auth state: client credentials and grants, gateway identities and grants, and the nodes' S3 credentials. Gateways and nodes stream that state from it, cache it in memory, and keep serving through the service's outages.

Scope:
- 9A Spec section for the resources and the protocol, modeled on Envoy's xDS, as netfence's control-plane client uses it, plus the versions and nonces netfence leaves out:
  - One gRPC bidirectional stream per gateway or node, over mutual TLS, carrying typed resources: credentials, grants, gateway identities and S3 credentials.
  - State of the world per resource type, each response versioned and carrying a nonce. A client validates a set before applying it, then ACKs its version, or NACKs with the error and keeps its last good set. On reconnect the client names the versions it holds, and the service sends what changed since.
  - Connection epochs drop stale messages; jittered backoff resets only after a connection stays up; keepalives find dead links.
  - Gateways receive derived SigV4 signing keys for each credential, per date, region and service, covering presigned URLs' seven days, so no gateway holds a client's secret key.
  - How long a client serves from its last good set after it loses the service, and that it fails closed after that.
- 9B The service: a Rust binary in this workspace (tonic), an authoritative store (SQLite first), an admin API that creates, rotates and revokes credentials and grants, and watches that push each change to subscribed streams.
- 9C Clients in gateways and nodes: the stream, the in-memory cache, ACKs and NACKs, reconnect and resume. The config file remains a static source for single-process development.
- 9D Per-gateway identity: each gateway presents its own certificate to nodes, and nodes enforce that gateway's grants on every request, in place of the shared secret.
- 9E S3 credentials for nodes, from the service or from AWS's credential chain (instance role, web identity), with session tokens and refresh before expiry.

Out of scope:
- A replicated service, and tenants.

Completion gate:
A revoked credential stops working at every gateway within the bound the spec sets; gateways and nodes keep serving through an outage of the service and fail closed after the bound; a gateway reaches only what its grants allow; conformance passes with credentials from the service; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Integration tests over loopback, after netfence's: reconnect and resume; a delayed ACK from an older stream; a NACKed set left unapplied; a proxy that blackholes the stream, which keepalives detect; mutual TLS failures.
- Revocation time, measured from the admin call to the first refused request.
- Conformance through the accelerator with credentials from the service.
- Planted bugs: a NACKed set applied; a revoked key accepted after its push; a reconnect that keeps a stale version; nodes that ignore a gateway's identity; an expired session token used for S3.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
