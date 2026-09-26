# Development Plan

## Overarching Goal

Build the S3 accelerator that `spec.md` describes: a distributed NVMe read cache in front of S3 whose cache logic runs as deterministic state machines, proven in a simulator before it runs on real sockets and disks. Each core feature lands with the simulator models and properties that test it. The server track runs the same core over real I/O and passes the S3 conformance suite through the accelerator.

Non-goals until a phase names them: POSIX access, multipart-upload warming, SSE-C caching, cross-zone clusters.

## Implementation Principles

- `spec.md` is the design contract. A change that alters the design updates the spec, and the page that renders it, in the same commit.
- The core does no I/O, reads no clocks and starts no threads (`AGENTS.md`). It handles block locations and response heads; the server and simulator move bytes.
- Every core feature ships with its simulator model, a property that fails on a wrong answer, and a planted bug that the simulator catches.
- Build the smallest implementation that meets the phase gate. Add abstraction when a later phase needs it.
- Execution order: Phase 1, S1, Phase 2, Phase 3, S2, Phase 4, Phase 5, S3.

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
- 3B Network faults drawn from the seed: loss, duplication, delay spikes, partitions; a safety phase with faults and a liveness phase without.
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
| In Progress | Test | Planted bugs for Phase 3 | 3D's six mutants are caught by the simulator's tests: a block recorded before its bytes are durable, eviction that leaves the slot's record, a restart after a crash that trusts its blocks, a corrupt block served anyway, a verified block not recorded again, and a restarted home that fetches whole objects it holds blocks of. So are five of 3C's six: a cut body never resumed, a cut range body and a cut home body each resumed from their start, a changed object restarting a response that already started, and a write from a cut S3 body kept. The sixth, an early end that cuts off the forward in progress, needs a later part to fail while an earlier one is forwarded, which random faults rarely line up; the gateway unit test `an_early_end_waits_for_the_forward_in_progress` catches it. Missing: the full report. |
| Incomplete | Gate | 10,000-seed sweep with faults | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` run and resolved findings. |

## Phase S2: Real Block Store and Zero-Copy

Goal:
Storage nodes keep blocks on disk and serve hits with `sendfile`, and gateways relay with `splice`.

Scope:
- S2A Slab files, extents and the slot table on disk, executing the core's storage actions.
- S2B `fdatasync` ordering: a block's bytes before its record, and a cleared record before any write over its slot. Restart recovery through `Node::recover`, runs in records, and the clean-shutdown mark; the table's header names the layout, and a changed layout discards the table. Checksums of block bytes for records and `Verify`.
- S2C `sendfile` for hits and `splice` for relays, on worker threads off the event loop. Bodies stream end to end: S3 responses, relays and uploads never sit whole in memory, and `max_body` stops limiting object size. The origin client sends paths as written, so keys with `.` and `..` segments work.
- S2D Separate gateway and storage-node processes, and a restart test that keeps the cache warm.

Out of scope:
- kTLS (S3).

Completion gate:
Conformance passes through a multi-process cluster; a restart test shows hits after restart; zero-copy is verified from outside the server; `/code-review` findings are resolved.

Testing plan:
- Conformance through the accelerator; restart integration test; crash test that kills the process during fills.
- Zero-copy verification: an integration test runs the storage node and gateway under `strace` and asserts that hit bodies leave through `sendfile` (storage node) and `splice` (gateway) system calls covering at least the body's bytes, and that no `write`, `writev` or `sendmsg` carries body bytes. The server's own counters are not evidence. A planted bug that forces the copying path must fail the test. CI installs `strace` and runs it.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | S2A: On-disk slab files and slot table | Missing: implementation. |
| Incomplete | Work | S2B: Sync ordering and recovery | Missing: implementation and crash test. |
| Incomplete | Work | S2C: `sendfile` and `splice` | Missing: implementation. |
| Incomplete | Work | S2D: Multi-process cluster and restart test | Missing: test. |
| Incomplete | Test | S2E: `sendfile` and `splice` verified under `strace` | Missing: integration test, planted copying-path bug, CI job. |
| Incomplete | Gate | Code review | Missing: `/code-review` run and resolved findings. |

## Phase 4: Membership and Ring Changes

Goal:
The cluster resizes without losing its cache: new owners fill from previous owners, and the simulator measures the hit rate through a resize.

Scope:
- 4A foca in the core, fed packets and timer events; versioned ring snapshots; the previous snapshot kept for the grace window; unresponsive nodes stay in the ring for a grace period.
- 4B Gateways fetch the ring from storage nodes; responses carry the ring version; gateways refetch on a newer one.
- 4C On a miss within the grace window, owners fetch from the previous owner first; those blocks skip the doorkeeper.
- 4D Blocks a node no longer owns are evicted first once the grace window ends, using stored placement hashes.
- 4E Simulator: nodes join, leave and are replaced; a property bounds the hit-rate drop and S3 GETs through a one-node resize, against a control run without fallback.

Completion gate:
A 10,000-seed sweep with resizes passes; the resize property holds; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator seeds with membership changes alongside Phase 3 faults.
- Planted bugs: fallback outside the grace window; owned blocks evicted before non-owned ones.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: foca membership and ring snapshots | Missing: implementation. |
| Incomplete | Work | 4B: Gateway ring fetch and versioning | Missing: implementation. |
| Incomplete | Work | 4C: Previous-owner fallback | Missing: implementation. |
| Incomplete | Work | 4D: Eviction of non-owned blocks | Missing: implementation. |
| Incomplete | Test | 4E: Resize scenarios and hit-rate property | Missing: property and control run. |
| Incomplete | Gate | 10,000-seed sweep with resizes | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` run and resolved findings. |

## Phase 5: Writes, Hot Keys and Warming

Goal:
Writes pass through the home without exposing stale data, hot keys spread across replicas, and warming cuts first-read misses.

Scope:
- 5A `PutObject` through the gateway and the home: the home drops or replaces metadata, discards first fetches that started before the write, and forwards invalidations; the proxying gateway drops its entry.
- 5B `events` freshness mode with a model of S3 event notifications that delays and duplicates them.
- 5C Hot-key leases: rate tracking, leases to the next K candidates, hot hints, renewal above half the promotion threshold, expiry.
- 5D Warming on write with the HEAD check, and metadata prefetch for Parquet, ORC and safetensors; the simulator's model generates objects with valid trailers and headers.
- 5E Properties: read-after-write through the writing gateway; hot-key load spread; prefetch removes the second miss.

Completion gate:
A 10,000-seed sweep with writes passes; planted bugs are caught; `/code-review` findings are resolved.

Testing plan:
- Simulator seeds with writes through the cache, event delivery faults and hot keys.
- Planted bugs: a first fetch that started before a write still indexed; warmed blocks indexed without the ETag check; a lease that never expires.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Writes through the home | Missing: implementation. |
| Incomplete | Work | 5B: `events` freshness mode | Missing: implementation and event model. |
| Incomplete | Work | 5C: Hot-key leases | Missing: implementation. |
| Incomplete | Work | 5D: Warming on write and metadata prefetch | Missing: implementation and format-aware object model. |
| Incomplete | Test | 5E: Write, hot-key and prefetch properties | Missing: properties. |
| Incomplete | Gate | 10,000-seed sweep with writes | Missing: sweep output. |
| Incomplete | Gate | Code review | Missing: `/code-review` run and resolved findings. |

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
- kTLS verification: after a TLS handshake, the socket reports the `tls` ULP (`ss -tie` shows it), `/proc/net/tls_stat` transmit counters (`TlsTxSw` or `TlsTxDevice`) rise for each connection, `strace` shows hit bodies leaving through `sendfile` on the TLS socket, and the TLS client decrypts every body byte correctly. A run forced onto userspace TLS must fail the kTLS assertions. CI loads the `tls` kernel module and runs it.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | S3A: rustls and kTLS listeners | Missing: implementation. |
| Incomplete | Test | S3D: kTLS verified through kernel state and `strace` | Missing: integration test, forced userspace-TLS control, CI job. |
| Incomplete | Test | S3B: NVMe benchmarks | Missing: harness and results. |
| Incomplete | Doc | S3C: Storage-layout decision | Missing: spec update. |
| Incomplete | Gate | Code review | Missing: `/code-review` run and resolved findings. |
