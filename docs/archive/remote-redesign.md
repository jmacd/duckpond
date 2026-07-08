# Remote redesign: integrating the sandbox prototype into duckpond

## Status

All planned phases (D1-D9) have landed; the branch is functionally
ready for production cutover.  The P1 correctness blocker closed in
D5.9 (`d5bf4b68`); the diagnostic/recovery CLI surface and operator
guide landed in D6; producer-side compaction and bootstrap-bundle
replication landed in D7/D7b; D8/D9 closed host-Parquet read and
testsuite-coverage gaps.  Remaining work is a handful of genuinely
deferred carry-forwards (see § "Open items (post-D9)" below), not
blockers.

| Phase | Status | Commit |
|---|---|---|
| Plan | done | `c2b8cc68` (this doc) |
| D1: relocate sandbox crates | done | `bd965792` |
| D2: prep (sync-* deps wired) | done | `6c75e025` |
| D2: substantive refactor | done | `85eba5c4` |
| D3: deferred — folded into D4 | — | — |
| D4: replace legacy remote with `sync_remote::Remote` | done | `abf0d5c0` .. `5cd1609b` (D4.1-D4.6) |
| D4.7: D4 documentation update | done | `a9bbf6eb` |
| D4.8: auto-init remote Delta table in `pond remote add` | done | `9f3944d6` |
| D5.1: partition tlogfs by `(pond_id, part_id)` | done | `1137d81d` |
| D5.2: pond identity owned by data-table bootstrap row | done | `2d03c946` |
| D5.3: `Remote::push` filters by local `pond_id` partition | done | `50956974` |
| D5.4: `bootstrap_consumer` + `Ship::create_replica` (first-pull) | done | `2f1ee2ab` |
| D5.5: `compute_live_checksums` for tlogfs row schema | done | `ccecc54a` |
| D5.6: drop `data_delta_version=0` clamp from `Remote::push` | done | `1f2e2811` |
| D5.7a: snapshot partition checksums in `record_data_committed` | done | `a1367d90` |
| D5.7a.1: process write lock + drop read-tx control records | done | `c55d9bd3` |
| D5.7a.2: thread `FinalizedCommit` version through commit chain | done | `fbc33b43` |
| D5.7b.1: split `remote add` (pull) vs `backup add` (push) | done | `4cb992d4` |
| D5.7b.2: first-pull cross-pond mount materialization | done | `7e763a42` |
| D5.7b.3: read-only foreign mounts + scoped auto-exec | done | `6632f1ea` |
| D5.7b.4: `remote remove` detach (default) and `--purge` | done | `2f95f99b` |
| D5.7b.5: attach-time mount/store_id conflict checks | done | `bfcfe76e` |
| D5.7b docs (cli-reference.md update for D5.7b verbs) | done | `3914cdd1` |
| D5.8: revive 13 disabled testsuite scripts | done | `01987ebb` .. `75eb4139` (D5.8.1-D5.8.9) |
| D5.9: fix P1-BUG-LF-REPLICATION (large-file blob replication) | done | `d5bf4b68` |
| D6: diagnostic/recovery CLI verbs + operator-guide rewrite | done | verify/status/rebuild-control/restart-from-compact shipped (D6.1-D6.4); operator-guide rewritten |
| D7: producer-side compaction (close P2-PRODUCER-COMPACT-BUNDLES) | done | `Ship::compact` records a pushable `CommitKind::Compact` txn; `pond maintain --compact` routes through it; mirror restart loop now reachable end-to-end |
| D7b: replicate the `pond_init` bundle (close P2-VERIFY-BOOTSTRAP-DRIFT) | done | `366c46b9` -- producer- and replica-side `pond verify` are now symmetric |
| D8: read `host+table:///` / `host+series:///` Parquet directly | done | `f0a38111` -- fixes "not queryable" on host-file URLs |
| D9: `file://` testsuite coverage for D6/D7 remote CLI verbs | done | `e57c357c`; plus follow-on fixes `42c65540` (require `${env:VAR}` credential refs), `078b9f25`/`7232d254` (`last_pushed_seq` watermark correctness), `f0ac0002` (propagate snapshot errors in `apply_pulled_bundle`) |

Active branch: `jmacd/52` (109 commits ahead of `main` as of `f0ac0002`).
The D5 row in earlier revisions of this doc was a single "pending" line;
it expanded into 15 sub-phases (D5.1 through D5.9) during execution.
The original D5 design (§ D5 below) accurately predicted the technical
shape of the work; the sub-phasing is a record of how it actually landed.

## Open items (post-D9)

D5.9 closed the correctness blocker for cutover.  Since then D6 (CLI
verbs + operator-guide rewrite), D7/D7b (producer compaction +
bootstrap-bundle replication), and D8/D9 have all landed.  What
remains is a set of genuinely deferred carry-forwards -- none of them
blockers for replacing the old duckpond in production:

### Correctness blocker (P1) -- CLOSED

- **P1-BUG-LF-REPLICATION** (closed by D5.9, `d5bf4b68`): files larger
  than `LARGE_FILE_THRESHOLD` (64 KiB) are externalized to
  `_large_files/blake3_16=<pfx>/blake3=<hash>.parquet` blobs that live
  outside the `pond_id=<u>/part_id=<v>/` partition tree.  Before the
  fix, `actions_at_version` enumerated only the partition parquet, so
  the receiver got the OpLog row (with correct metadata, size, blake3)
  but never the underlying blob.  Receive-side `pond pull` succeeded
  silently; subsequent `pond cat` of the file on the replica failed
  hard at read time with exit code 1 and
  `TLogFSError::LargeFileNotFound` (no silent data loss -- the bug was
  observable on first read).  Fixed by adding an
  `external_blobs_referenced_by` hook on the `RemoteSteward` trait;
  the duckpond adapter overrides it to scan each partition parquet for
  rows with `content IS NULL`, resolves their `blake3` to on-disk
  paths, and `Remote::push` emits the blobs as additional dedup'd
  `DataAdd` rows.  `apply_pulled_bundle` splits incoming adds by
  `_large_files/` prefix and writes blobs without recording a Delta
  `Add` action.  Regression coverage: new Rust integration test
  `pond_remote_push_pull_large_file_roundtrip`, reinstated 80 KiB
  `big.bin` case in `tests/530-cross-pond-import-minio.sh`, and bumped
  `INITIAL_ROWS` to 6000 in `tests/510-synth-logs-replication-cycle.sh`.

### Resolved production bugs (May-10 cross-pond staleness) -- CLOSED

The cross-pond redesign (A1, row-level `pond_id`) was motivated by a
real production outage: caspar.water's `site-prod` pond served stale
water data for ~6 days in May 2026.  The diagnosis lived in an
untracked `may-10-bugs.md` at the repo root; that file is removed now
that the bugs are fixed and the resolution is recorded here.

- **Bug 1 -- global `max()` import watermark (High; the outage).**  The
  old `crates/remote` import factory took a single `max()` watermark
  across *all* imported partitions, then dropped every foreign txn at
  `txn_seq <= watermark`.  A fast producer pushed the watermark to its
  frontier `F`, so any slower producer whose new transactions landed at
  `txn_seq < F` was masked forever.  **Fixed by the post-D5 model**: the
  old factory is deleted (D4.5) and each producer is attached as its own
  remote with its own `last_pulled_seq:<url>` watermark
  (`crates/sync-remote/src/remote.rs:120` key, `:480` `txn_seq >
  last_pulled` filter, `:501` per-bundle advance).  Divergent producer
  frontiers can no longer mask one another -- the masking is structurally
  impossible, since there is no longer any cross-producer `max()`.
  Regression guard: `testsuite/tests/715-cross-pond-divergent-frontier.sh`
  reproduces the exact condition (fast frontier 15, slow producer's new
  txn at seq 5 < 15) and asserts the slow file still imports.
- **Bug 2 -- phantom partitions persist in S3 across a producer reset
  (Medium).**  A wiped+reinitialized producer left old-epoch partition
  bundles in its bucket, which the old recursive S3 discovery
  (`collect_partitions_recursive`) re-ingested, poisoning the `max()` in
  Bug 1.  **Fixed structurally**: there is no recursive partition cache
  anymore.  Each remote is bound to the producer's `pond_id`/`store_id`;
  a reset producer has a new `pond_id`, surfaced as
  `RemoteError::StoreIdMismatch` on push (`remote.rs:329`) rather than
  silently merged, and `apply_pulled_bundle` validates that every
  imported path's `pond_id` matches the bundle's
  (`crates/steward/src/remote_adapter.rs:541,560`).  Operator note: a
  producer reset now requires wiping its remote bucket (or re-pointing
  the consumer); the consumer no longer auto-discards the old epoch, it
  refuses the mismatch.
- **Bug 3 -- `factory_key` cache collision (needs-verification).**  Moot:
  the import-partition cache and the `entry.id().part_id()` keying it
  questioned are gone with the old factory (D4.5).
- **Bug 4 -- post-reset terraform skips `pond apply` (Operational).**
  Not a duckpond bug; it lives in the separate caspar.water deployment
  repo (`terraform/station/watershop/watershop.tf`,
  `README.operations.md`), which is not part of this workspace.  Tracked
  there, not here.

### Deferred carry-forwards from D4/D5

- **`Remote::restart_from_compact` (mirror) is not generic over
  `RemoteSteward`** -- but this is no longer a blocker.  The per-pond
  consumer recovery path
  `Remote::restart_pond_from_compact<S: RemoteSteward + ?Sized>`
  (`crates/sync-remote/src/remote.rs:792`) IS generic and is what the
  operator-facing `pond restart-from-compact` (D6.4) drives.  The
  remaining non-generic `restart_from_compact` (`remote.rs:618`) is
  only the mirror-mode fresh-directory bootstrap, covered by the next
  bullet.
- **Mirror-restart bootstrap is Rust-only** via
  `ShipContext::create_pond_for_restoration`; cross-pond first-pull
  works through the CLI, but mirror-restart from a fresh local
  directory does not.
- **Physical vacuum of purged foreign `pond_id` rows from the Delta
  log** (D5.7b.4 deferral). Correctness-safe (rows are unreachable
  by path after `--purge`); they remain in the log until a future
  partitioned-delete compaction.

### Per-pond_id seq allocator -- CLOSED (D10)

- **Shared global data-table seq allocator** (closed by D10): previously
  `OpLogPersistence` held a single global `last_txn_seq`, recovered on
  `open` as the global MAX `pond_txn.txn_seq` across ALL commits, and
  `apply_pulled_bundle` bumped it to the foreign bundle's seq.  Importing
  a fast foreign producer therefore inflated the LOCAL pond's own next
  seq and left gaps in its history (e.g. local `1,2,3` then import
  foreign `50` -> local's next own write was `51`).  This realized the
  control table's per-pond_id seq spaces but NOT the data table's, so the
  design promise (§A2) was only half met.  It was never a correctness bug
  (the allocator was monotonic; push/pull iterate actual records), only
  an operability + design-promise gap.  **Fixed** by replacing the scalar
  with a per-pond_id map `seqs: HashMap<String, i64>`
  (`crates/tlogfs/src/persistence.rs`): `last_txn_seq()` returns the LOCAL
  pond's seq; `open` recovery buckets each `pond_txn` commit by its
  `pond_id` and takes the per-pond max; `sync_last_txn_seq(pond_id, seq)`
  is now pond_id-scoped; and `apply_pulled_bundle`
  (`crates/steward/src/remote_adapter.rs`) advances only the foreign
  pond's entry, bumping the local Ship allocator (`sync_last_write_seq`)
  only for a mirror restart (`bundle.pond_id == local`).  Cross-pond
  import now leaves the local seq space contiguous and decoupled from
  foreign frontiers.  Regression: `crates/cmd/tests/test_remote_cli.rs::
  cross_pond_pull_does_not_inflate_local_seq`.

### D6 surface gaps

The D6 CLI plan in § D6 below enumerates verbs that
have not all shipped:

| Verb | Status | Notes |
|------|--------|-------|
| `pond init` | shipped | |
| `pond init --from-remote` | not shipped | Replaced by automatic bootstrap on `pond remote add NAME URL /` (mirror-restart pull mode). |
| `pond remote attach` | shipped as `pond remote add` | D5.7b.1 names. |
| `pond remote detach` | shipped as `pond remote remove` | Default = detach; `--purge` = unlink mount entry. |
| `pond push` / `pond pull` | shipped | |
| `pond maintain` | shipped | |
| `pond status` | **shipped (D6.2, 2026-06-05)** | Operator-facing aggregate of pond identity + local commit state + recovery health + per-remote sync watermarks.  Offline (no network); push lag from local watermarks. |
| `pond log` | shipped | |
| `pond verify` | **shipped (D6.1, 2026-06-05)** | Operator-facing wrapper around `verify_against_remote`.  Symmetric: producer-side and replica-side verify both pass (the `pond_init` bundle is replicated, P2-VERIFY-BOOTSTRAP-DRIFT fixed in D7). |
| `pond recover` | shipped | Plus `pond emergency` for destructive recovery. |
| `pond restart-from-compact` | **shipped (D6.4, 2026-06-05)** | Delegates to the generic `Remote::restart_pond_from_compact` (works on duckpond's `ShipRemoteSteward`; the carry-forward "blocker" was already resolved by that generic path).  Mirror restart re-persists the dropped attachment.  Requires a compact baseline on the remote -- duckpond producers now create one via `pond maintain --compact` + `pond push` (D7, P2-PRODUCER-COMPACT-BUNDLES closed). |
| `pond maintain --compact` | **shipped (D7, 2026-06-06)** | Records the data-table Delta optimize as a `CommitKind::Compact` transaction (`Ship::compact`), so `Remote::push` emits a Compact bundle (restart baseline).  Asserts per-partition checksum invariance; stamps `pond_txn` on the optimize commit so reopen recovers `last_txn_seq`. |
| `pond rebuild-control` | **shipped (D6.3, 2026-06-05)** | Reconstructs the control table from the data Delta table's `pond_txn` commit history.  Recovers pond identity + transaction-log skeleton; settings/watermarks and per-txn checksums are not recoverable (re-attach remotes, re-baseline verify). |

### Documentation reconciliation

- **`docs/operator-guide.md`** was rewritten for the post-D6 CLI
  (2026-06-05): backup/remote attach, push/pull, maintain, status,
  verify, log, recover, rebuild-control, restart-from-compact, plus
  transaction lifecycle-state vocabulary and a runbook.  The pre-D6
  factory-based version was archived to
  `docs/archive/operator-guide-pre-d6.md`.
- **`docs/operator-interface-plan.md`** (the superseded aspirational
  interface plan) was archived to
  `docs/archive/operator-interface-plan.md`.
- **This document's** phase table was updated in this round to
  reflect actual D5 sub-phasing; the design narrative in §§ D5-D6
  below is still accurate as a record of intent.

### Checksum strategy: Merkle is the shipped default

The prototype left "which checksum strategy wins?" open, gated on a
`benchmarks` todo that never ran.  duckpond ships on the prototype's
inherited default: `StewardOptions::default()` constructs
`sync_store::checksum::Merkle`
(`crates/sync-steward/src/steward.rs:136`).  Merkle is the
cryptographically conservative choice; the `Homomorphic` strategy
remains available behind the same `PartitionChecksum` trait for a
future swap if O(1) incremental update cost is ever measured to
matter.  Recorded here as a decision, not an accident: no benchmark
gate blocks cutover.

## Future direction: content-addressed replication (incremental coexistence)

> **Decision (2026-06-23):** the next replication arc migrates this
> subsystem onto the content-addressed object model in
> `docs/content-addressed-pond-design.md`, via **incremental coexistence** --
> tree/commit objects are computed *alongside* today's bundle/frontier
> checksums, sync moves to commit-DAG fetch over phases, and the
> bundle/frontier apparatus is retired **last**, only after the
> object-graph `verify` reaches parity with the shipped one.

> **Decision (2026-06-24): pure reset, no legacy migration.** No deployed
> pond needs preservation, so this arc does **not** convert historical
> ponds onto the object model. New ponds use the content-tree machinery from
> genesis; CA4 hard-removes the bundle path with **zero on-disk migration**.
> The verify-parity gate is retained for correctness confidence on `main`,
> not for data migration.

> **Decision (2026-06-24): checksum/tree state is per-pond.** The commit
> stream is per-`pond_id` and linear (single-writer-per-pond; per-pond seq
> spaces; pond-pure partitions). Touched-partition `tree_hash` updates feed a
> **per-pond accumulator** (a persisted, node-keyed tree-hash cache updated
> only for the partitions a commit touched), replacing today's global flat
> `fsck` Merkle over all partitions. See
> `docs/content-addressed-pond-design.md` Sections 5 and 6. Build it as new
> CA1 machinery, not a retrofit of the bundle's `partition_checksums` list.

The shipped subsystem (D1-D10) is the **bundle/frontier** generation:
3-partition bundles (manifest, checksum, data), per-partition checksums,
`(pond_id, seq)` frontier sequencing, and `push`/`pull`/`verify`/`restart`.
The content-addressed model is its **successor**, not a competing design:
`docs/content-addressed-pond-design.md` Sections 8 and 10 list the bundle
format, the `(pond_id, seq)` frontier, and the compaction-invariance
assertions as apparatus the object model **retires**. Incremental coexistence
is the path that gets there without a big-bang rewrite or stranding either
doc.

### Coexistence invariant (the safety property)

Throughout this arc, the bundle/frontier path remains the **production**
replication path. The content graph is built and verified **in parallel**
but is not load-bearing for sync until the final phase. Cutover of each
capability is gated on **verify parity**: the object-graph answer must agree
with the bundle answer before the bundle path is removed for that capability.
This is what makes the migration safe to land incrementally on `main`.

### Phases

These elaborate the migration described in
`content-addressed-pond-design.md` (Sections 5-8). Prefix `CA`
(content-addressed) to keep them distinct from the D-series.

| Phase | Goal | Touchpoints | Coexistence |
|---|---|---|---|
| **CA1** | Compute **blob/tree/commit objects at commit time**, BLAKE3-addressed, name == hash (design Sections 4-5). Recursive, name-keyed, content-only trees, with a **per-pond accumulator** (node-keyed `tree_hash` cache updated only for touched partitions). | new object store under `data/` (or reuse `_large_files` CAS shape); commit-time fold bounded by txn working set | additive: existing per-partition checksums still computed and authoritative |
| **CA2** | **Move provenance into the commit object** (`pond_id`, `seq`, `time`, author, CLI args) -- the `pond_txn` blob already stamped in Delta commitInfo, now attached to a commit. | `crates/tlogfs/src/txn_metadata.rs`, control-table reconstruction (`persistence.rs` reconstruct path) | commit object mirrors `pond_txn`; control table unchanged |
| **CA3** | **Switch sync to commit-DAG fetch**: compare root tree hashes, descend by child hash, transfer objects the peer lacks; frontier becomes a single commit hash. | `crates/sync-remote/src/remote.rs` push/pull; `remote_adapter.rs` `apply_pulled_bundle` | run object-graph `verify` next to `verify_against_remote`; require parity |
| **CA4** | **Retire bundle/frontier** once parity holds: drop `partition_checksums` from the bundle, the per-pond `seq` allocator's frontier role, and `Ship::compact`'s checksum-invariance assertions; `fsck` becomes the CAS-invariant scrub (design Section 6.1). | `remote_adapter.rs:536,738`; `persistence.rs` seq allocator; `ship.rs:920,989`; `fsck.rs` | bundle path removed only after CA3 parity is demonstrated |

### Design decisions carried into these phases

These are settled in `docs/content-addressed-pond-design.md` Section 11 and
are assumed by the phase detail below:

- **D1** -- the tree-hash table is keyed `(pond_id, node_id, version)` at rest
  (a per-node index, lifecycle identical to existing versioning, no GC);
  content-addressing happens **on the wire** because the row values are
  hashes. True at-rest CAS is deferred.
- **D2** -- the tree wire format and the dynamic-node config bytes start with
  the simplest reasonable encoding and are **not frozen**: a clean reset is
  allowed (no legacy ponds), so formats can be improved by a coordinated reset
  of all syncing ponds. Config bytes are hashed **as-is**, no canonicalization.
- **D3** -- tree hashes are **persisted at commit**, not recomputed at compare.
  This is forced by the log unification: the commit's `root_tree_hash` is the
  transparency-log leaf and must exist at commit time.
- **D4** -- dynamic / read-time-computed nodes hash `blake3(stored config
  bytes)` (the recipe); tree equality means recipe equality, not output
  equality; generated children are not folded.
- **D5** (open, deferred) -- checkpoint cadence and the SHA-256 publish format,
  deferred with signing. Not on the CA1-CA4 path.

The **commit record is simultaneously the top of the content tree and the leaf
of the transparency log** (design Sections 5.3, 7). CA1-CA4 deliver the SPACE
layer and content-addressed sync; the TIME layer (the SHA-256 tile log over
the same commit spine) is a later, separate arc that needs no change to the
spine CA1 writes.

### Phase detail

**CA1 -- content objects at commit time.** Compute, during each write
transaction: `blob = blake3(version bytes)`; `tree = blake3(sorted (name,
entry_type, child_hash))` with the D4 `child_hash` rule table; and a `commit =
(root_tree_hash, parent_commit_hash, provenance)`. Persist tree hashes into the
single `pond_id`-partitioned tree-hash table, **one parquet file per
transaction**, folding only the touched ancestor chain (D1, D3). The object
layer is **additive**: existing per-partition checksums stay authoritative and
nothing in sync depends on the objects yet.
*Acceptance:* (a) recomputing `root_tree_hash` from scratch over a corpus
equals the incrementally persisted value; (b) two ponds that ingest identical
content produce identical `root_tree_hash` regardless of `pond_id`; (c) a deep
write adds exactly one file to the tree-hash table and rewrites no ancestor
partition.

**CA2 -- provenance into the commit object.** Attach `pond_id, seq, time,
author, CLI args` to the commit object, mirroring today's `pond_txn` blob in
Delta `commitInfo`. The control table is unchanged this phase.
*Touchpoints:* `crates/tlogfs/src/txn_metadata.rs`, the `persistence.rs`
reconstruct path. *Acceptance:* commit provenance reconstructs byte-equal to
the existing `pond_txn`-derived control records.

**CA3 -- sync becomes commit-DAG fetch.** Compare tip commit hashes; equal
`root_tree_hash` means identical, else descend by `child_hash` to the divergent
subtrees in O(difference). Transfer the objects the peer lacks by hash (blobs
directly; trees reconstructed from the tree-hash table joined with the
directory snapshot), deduped on the wire (D1). The frontier becomes a **single
commit hash per ref**; the per-pond `seq` frontier role and the per-partition
checksum list are no longer needed for sync but remain until CA4.
*Touchpoints:* `crates/sync-remote/src/remote.rs` push/pull,
`remote_adapter.rs` `apply_pulled_bundle`. *Parity gate (the safety
property):* run object-graph `verify` next to `verify_against_remote` and
require agreement on **both** the identical/divergent verdict **and** the set
of transferred objects, across the full replication testsuite, before any CA4
removal. *Acceptance:* object-graph sync yields byte-identical replicas to the
bundle path on every existing replication test, and its divergence
localization matches the bundle path's divergence boundary.

**CA4 -- retire bundle/frontier.** Once CA3 parity holds: drop
`partition_checksums` from the bundle, retire the per-pond `seq` allocator's
frontier role, remove `Ship::compact`'s checksum-invariance assertions, and
make `fsck` the **CAS-invariant scrub** -- re-hash each object reachable from a
ref, verify `name == hash`, verify large-blob byte-range chains, and check
reachability (design Section 6.1). Compaction becomes an honestly recorded
**rewrite commit** with no invariance assertion. Per the pure-reset decision
there is **no on-disk migration**: new ponds use the object model from genesis
and the bundle path is hard-removed. *Touchpoints:* `remote_adapter.rs`,
`persistence.rs` seq allocator, `ship.rs`, `fsck.rs`. *Acceptance:* the bundle
path is gone, all replication/verify tests pass on the object path alone, and
the `fsck` scrub detects injected single-byte corruption.

### Relationship to "CRDT-style symmetric ponds" (Out of scope, below)

The commit DAG delivered in CA3 is the prerequisite that item was waiting
on. Because DuckPond is **single-writer-per-pond**, each ref is linear, so
push-pull is **fast-forward** and cross-pond import is **grafting** a
distinct ref -- a full 3-way merge engine is needed only if concurrent
writers ever share one ref (foundation §9.5). So the symmetric-sync goal is
reached by the fast-forward/grafting model, not by CRDTs; the
out-of-scope note below is superseded by this arc once CA3 lands.

## Background

The `duckpond/sandbox/` prototype (Cargo workspace separate from
`duckpond/crates/`) was built to validate a redesign of duckpond's
remote-sync layer.  Phase A of that prototype is complete (see
commits `c40737c2 .. d6b79e62` on branch `jmacd/50`), with 232
tests + 2 ignored S3 tests against MinIO.  The sandbox now models:

- Per-pond_id namespaced seq spaces in the control table.
- Pond_id as a Delta partition column (each parquet file is
  pond-pure, so push filters at file granularity).
- Cross-pond import (a consumer can attach a foreign remote and
  pull foreign data into its local table tagged with the foreign
  pond_id; downstream consumers attach the original remote
  directly).
- `verify_against_remote` with `divergence_boundary`.
- `restart_from_compact` (mirror) and `restart_pond_from_compact`
  (per-pond_id, in-place on an existing consumer).
- Bounded-growth maintenance across all three legs (local data,
  control table, remote bundles).
- S3 backend support (verified end-to-end against MinIO).

This document captures the architectural decisions for integrating
the sandbox concepts into duckpond proper, replacing the existing
`crates/remote/` and `crates/steward/` layers.  No backward
compatibility is required (no deployed ponds need preservation
through the migration).

## Glossary

- **Pond**: a transactional filesystem backed by Delta Lake, with
  TinyFS abstractions over the top.
- **Pond identity (`pond_id`)**: a UUID identifying the pond
  family.  All replicas of a pond share its `pond_id`.
- **Replica**: a clone of a pond.  Multiple replicas of one
  `pond_id` exist; they share data but have independent
  per-replica state.
- **Cross-pond import**: pond A consumes data from pond B by
  attaching B's remote.  B's data lives in A's local table tagged
  with B's `pond_id`.  A's own `pond_id` is unchanged.
- **Bundle**: an atomic unit of remote sync.  One bundle per
  source `txn_seq`.  Each bundle is one Delta commit on the
  remote table containing manifest + per-partition checksums +
  chunked file content.
- **Control table**: per-replica Delta table that tracks
  transient state — lifecycle records (Begin/DataCommitted/
  Failed/Completed), per-remote sync watermarks, push/pull
  modes, pond-instance settings.  Treated as erasable cache;
  recoverable from data + filesystem + remotes.
- **Data table**: the tlogfs Delta table holding `OplogEntry`
  rows.  Source of truth for pond content.

## Architectural decisions

### A1. Cross-pond model: row-level pond_id (sandbox model)

Adopt the sandbox's row-level pond_id model in tlogfs:

- `OplogEntry` already has a `pond_id` column (existing, see
  `crates/tlogfs/src/schema.rs:284`); make it a Delta **partition
  column** alongside `part_id`.  Partitioning becomes
  `(pond_id, part_id)`.
- Cross-pond imports tagged with foreign `pond_id`; live in the
  consumer's data table alongside own data.
- Push filters by `pond_id` at file granularity (each parquet
  file is pond-pure).
- Replaces the current factory-mode-based segregation (separate
  factories for backup vs import).
- Downstream consumers wanting a foreign pond's data attach that
  foreign remote directly, not the intermediary pond's remote.

Migration: existing `OplogEntry` rows acquire a `pond_id` column
(default to the local pond's id; no other pond_ids in pre-existing
ponds).  Schema evolution on tlogfs Delta table.

### A2. Crate layout: single workspace

Move the sandbox crates into `duckpond/crates/`:

- `duckpond/sandbox/store/` → `duckpond/crates/sync-store/`
- `duckpond/sandbox/steward/` → `duckpond/crates/sync-steward/`
- `duckpond/sandbox/remote/` → `duckpond/crates/sync-remote/`
- `duckpond/sandbox/tests/` → integrated into existing test harness
  or kept as `duckpond/crates/sync-tests/`

Package names rename `sandbox-*` → `sync-*`.  Single Cargo
workspace; one `cargo build` builds everything.  `duckpond/sandbox/`
directory removed at the end of D1 once the migration is complete.

### A3. Steward layer: merged

`crates/steward/Ship` is replaced by a new `crates/steward/Steward`
built from scratch, using:

- `sync_steward::ControlTable` (per-pond_id seq spaces) as the
  internal control table, REPLACING the existing
  `crates/steward/control_table.rs`.
- `tlogfs::OpLogPersistence` as the data substrate (unchanged).
- TinyFS-based `write_transaction(closure)` as the user-facing
  API (kept from `Ship` — call sites need not change shape).
- `sync_steward::Steward` primitives where they fit (lifecycle
  recording, `apply_pulled_bundle`, `compute_live_checksums`,
  control-table compact/vacuum) called as library functions, not
  inherited as a struct.

`crates/steward/control_table.rs` deleted.  `crates/steward/ship.rs`
renamed to `crates/steward/steward.rs` (or similar).

Sandbox's `WriteGuard` (k/v puts/deletes) doesn't transfer; the
new Steward's writes happen via tinyfs.

### A4. Remote layer: replaced

`crates/remote/{table.rs, schema.rs, chunking.rs}` deleted.  All
remote functionality flows through `sync_remote::Remote` and the
sandbox's bundle format (3-partition: manifest, checksum, data).

`crates/remote/factory.rs` (~2881 lines) significantly slimmed:

- Remote-specific logic (push, pull, replicate, verify, show,
  list-files, import) replaced by direct calls to
  `sync_remote::Remote` methods.
- Factory dispatch infrastructure for non-remote factories
  (hydrovu, sitegen, sql-derived, etc.) preserved.
- Existing `pond run /sys/run/<remote-config> push` continues to
  work — internally calls `sync_remote::Remote::push`.

CLI: BOTH the existing factory invocation AND new top-level verbs
(`pond push [<name>]`, `pond pull [<name>]`, `pond verify
[<name>]`, `pond restart <name>`) are supported.  All read remote
configs from the filesystem.

### A5. Configuration principle

- **Replicated, per-pond-family configuration → filesystem
  (TinyFS)**.  This includes pond identity (via bootstrap row),
  factory definitions, remote attachment configs (URL +
  `${env:VAR}` credential references — text replicates, env
  resolves locally per replica).
- **Per-replica state → control table**.  This includes
  lifecycle records, `last_pulled_seq:<remote>`,
  `last_pushed_seq:<remote>`, push/pull mode per remote,
  pond-instance settings.
- The control table is treated as **transient cache**: erasable
  by an explicit operator action, requiring no concurrent
  writers.
- Recovery from control-table loss reconstructs state from
  data + filesystem + remotes:
  - Pond identity: read from bootstrap row in data.
  - Factory configs: replicated; read from FS.
  - Remote configs: replicated; read from FS.
  - Push/pull mode per remote: re-specified by operator.
  - Sync watermarks: walk own remote (`last_pushed_seq` =
    max txn_seq present there); walk pulled rows (`last_pulled_seq`
    = max foreign-pond_id txn_seq in data).
  - Lifecycle records: lost (audit trail not reconstructible);
    pond functional state is intact.

Future direction (not in scope now): a CRDT-style symmetric
model where every replica can both push and pull, removing the
push/pull mode distinction entirely (git-clone-like).

### A6. Pond identity location

`pond_id` lives as a **bootstrap row in the data Delta table** —
specifically, the root directory's `OplogEntry` carries `pond_id`
(consistent with how every `OplogEntry` row already has a
`pond_id` column).

To recover pond identity from a control-table-erased pond:
read the root directory's OplogEntry from the data Delta table.

The sandbox's current control-table-Setting-based store_id
(`STORE_ID_KEY` setting under `BOOTSTRAP_POND_ID = Uuid::nil()`)
is replaced by this convention in the merged Steward.

### A7. FS conventions

- **Default path** for remote attachment configs:
  `/sys/remotes/<name>` (replaces today's
  `/sys/run/N-remote` for the SPECIFIC purpose of remote configs;
  data factories continue to live at their existing paths).
- The convention itself is a **control-table setting** (operator
  can override the path prefix per-replica).
- Future: `pond init` could read a configuration file to
  initialize FS conventions and other per-replica defaults.
  Out of scope for the initial integration.

### A8. CLI ergonomics

Both the existing factory invocation pattern AND first-class
top-level verbs are supported:

```
# Existing pattern (continues to work):
pond run /sys/remotes/backup push
pond run /sys/remotes/backup pull
pond run /sys/remotes/backup verify

# New top-level verbs (read FS config; dispatch to Remote):
pond push [<name>]
pond pull [<name>]
pond maintain [--scope=local|control|remote|all] [<name>]
pond verify [--local | --remote=<name> | --all]
pond restart-from-compact <name>
pond recover

# New management verbs:
pond remote attach <url> --name <name> [--credentials <ref>] [--mount <path>]
pond remote detach <name>
pond remote list
pond remote list-bundles <name>
```

`pond remote attach <url> --mount <path>` creates a filesystem
node at `<convention-prefix>/<name>` with the appropriate config,
then sets the per-replica push/pull mode in the control table to
`pull` (since `--mount` implies cross-pond import).  Without
`--mount`, default mode is `push` (own backup).

## Phased execution plan

The integration is decomposed into independently committable
phases.  Each phase leaves the system buildable and tested.

### D1: Move sandbox crates into duckpond workspace

**Goal**: physical relocation only; no semantic change.

**Steps**:
- Move `duckpond/sandbox/{store,steward,remote,tests}/` to
  `duckpond/crates/sync-{store,steward,remote,sync-tests}/`.
  Package names renamed `sandbox-*` → `sync-*`.
- Update `duckpond/Cargo.toml` workspace `members` to include
  the new crates.
- Update `duckpond/crates/sync-*/Cargo.toml` to use
  `workspace.dependencies` from the duckpond root.
- Adjust internal imports across `sync-*` crates to use the
  new package names.
- Remove the standalone `duckpond/sandbox/` directory.
- Archive `duckpond/sandbox/DESIGN.md` to `docs/archive/` (done:
  it now lives at `docs/archive/sandbox-design.md`).
- CI gate: workspace builds, all `sync-*` tests pass (existing
  227 + 5 + 3 + 1 + 1 = 237 tests; numbers may shift slightly).

**No functional change**.  Just relocation + rename.

### D2: Adopt sync_steward::ControlTable as duckpond's control table

**Goal**: replace `crates/steward/control_table.rs` with
`sync_steward::ControlTable`; introduce per-pond_id seq spaces.

**Steps**:
- `crates/steward/Ship` retains its API; internally swap its
  `control_table: ControlTable` field for `sync_steward::ControlTable`.
- All Ship methods that wrote/read the old control table get
  updated to use sync_steward's API (with `self.pond_id` as the
  default pond_id).
- Existing tests rewritten to use the new API.  Many tests that
  asserted control-table specifics get rewritten.
- Pond identity (currently in old control table's metadata) is
  STILL stored in the control table during D2, but as a
  transitional measure — D5 moves it to the bootstrap row.
- `crates/steward/maintenance.rs` updated to use
  `sync_steward::ControlTable::compact + vacuum` for control
  table.
- CLI: `pond log` continues to work; new pond_id-aware queries
  available.
- CI gate: full duckpond test suite passes; new per-pond_id
  control table behavior verified.

**Risk**: control-table schema migration on existing local ponds
(should be acceptable — test suite uses fresh ponds; user
deployments treated as fresh).

#### D2 implementation notes (decisions made during initial work)

These decisions emerged from the interactive design session that
produced this document but were not initially captured in the steps
above.  Recorded here so a fresh session can resume D2 cleanly.

**Lean schema decision**: drop the rich captures from the duckpond
ControlTable schema entirely.  Specifically, do NOT carry these
columns over into the sync_steward-backed wrapper:

- `cli_args`, `environment` (CLI invocation context)
- `factory_modes` column-denormalization (modes go in
  `config_set("factory_mode:<name>", ...)` per-pond_id)
- `factory_node_id`, `foreign_part_id`, `foreign_pond_id`,
  `watermark_txn_seq` (import state — gone in D5)
- `parent_txn_seq`, `execution_seq`, `factory_name`,
  `config_path` (post-commit task tracking)

The sync_steward lean schema (`pond_id`, `txn_seq`, `txn_id`,
`record_kind`, `commit_kind`, `parent_seq`, `duration_ms`,
`ts_micros`, `metadata_json`) is sufficient.  Anything that
genuinely needs to travel goes in `metadata_json`; reads that
required dedicated columns get rewritten or dropped.

**Display drops**: `cmd/control.rs` and `cmd/show.rs` currently
display a "Command:" line showing `cli_args`.  Drop those displays
during D2 (or replace with "(not captured)" placeholder) since the
data is no longer in the schema.

**Drop entirely**: import-state methods on `ControlTable`.  D5's
cross-pond model uses row-level pond_id on tlogfs OplogEntry rows
and does not need per-import control-table records.

**Pond identity** stays in the control table during D2 as a
sync_steward `config_set` entry under
`BOOTSTRAP_POND_ID = Uuid::nil()` (the convention the sandbox
prototype already uses).  D5 moves it to the bootstrap row in the
data Delta table.

**File-by-file checklist** for D2 substantive work:

- `crates/steward/src/control_table.rs`: rewrite as a thin wrapper
  around `sync_steward::ControlTable` (~1639 lines → likely ~400
  lines).  Drop `TransactionRecord` struct, drop the rich Arrow
  schema, drop import-state methods.
- `crates/steward/src/ship.rs`: simplify all `record_*` call sites
  (~10 sites).  Methods like `record_begin/data_committed/failed/
  completed` no longer need the rich `PondTxnMetadata`; just
  txn_seq + duration_ms + (for committed) data_fs_version.
- `crates/steward/src/guard.rs`: simplify `record_failed`,
  `record_data_committed`, `record_completed` call sites (~5).
- `crates/cmd/src/commands/control.rs`: drop `cli_args` display,
  drop the per-transaction `Command:` line.
- `crates/cmd/src/commands/show.rs`: drop `cli_args` display.
- `crates/cmd/src/commands/run.rs`: update `factory_modes` /
  `pond_metadata` accessors.
- `crates/cmd/src/commands/init.rs`: update `set_factory_mode` /
  `get_factory_mode` calls.
- `crates/cmd/src/commands/replicate_test_simple.rs`: update
  `get_pond_metadata`, `get_last_write_sequence` calls.
- `crates/cmd/src/commands/control_test.rs`: many test calls;
  update or rewrite.
- `crates/steward/tests/test_post_commit_factory.rs`: update.

**Method name mapping** (duckpond → sync_steward):

| duckpond ControlTable | sync_steward equivalent |
|---|---|
| `record_begin(txn_meta, …)` | `write_record(ControlRecord { kind: Begin, pond_id: local, … })` |
| `record_data_committed(…, data_fs_version, …)` | `write_record(ControlRecord { kind: DataCommitted, …, metadata_json: data_delta_version=… })` |
| `record_failed(…, error, …)` | `write_record(ControlRecord { kind: Failed, metadata_json: reason=… })` |
| `record_completed(…)` | `write_record(ControlRecord { kind: Completed, … })` |
| `record_post_commit_pending` | `write_record(ControlRecord { kind: PostPushPending, … })` (note rename) |
| `record_post_commit_started` | `write_record(ControlRecord { kind: PostPushStarted, … })` |
| `record_post_commit_completed` | `write_record(ControlRecord { kind: PostPushCompleted, … })` |
| `record_post_commit_failed` | `write_record(ControlRecord { kind: PostPushFailed, … })` |
| `set_factory_mode(name, mode)` | `config_set(local_pond_id, "factory_mode:" + name, mode)` |
| `get_factory_mode(name)` | `config_get(local_pond_id, "factory_mode:" + name)` |
| `factory_modes()` (HashMap) | iterate `config_list(local_pond_id)` filtering keys with `"factory_mode:"` prefix |
| `set_setting(k, v)` | `config_set(local_pond_id, k, v)` |
| `get_setting(k)` | `config_get(local_pond_id, k)` |
| `settings()` | `config_list(local_pond_id)` |
| `get_last_write_sequence()` | `last_txn_seq(local_pond_id)` |
| `find_incomplete_transactions()` | `incomplete_transactions(local_pond_id)` |
| `record_import_*` | DROP (D5 handles imports differently) |

**Acceptance criteria** for D2 complete:
- `crates/steward/src/control_table.rs` is a thin wrapper around
  `sync_steward::ControlTable`.
- The full duckpond test suite passes (1192+ tests at start of
  D2; expect a slightly different count after rewrites).
- `cargo fmt`, `cargo clippy --workspace --all-features -- -D warnings`
  clean.
- `pond log` and `pond control` commands still work; the
  "Command: <args>" display is gone or shows a placeholder.
- New per-pond_id behavior is reachable via `last_txn_seq(pond_id)`
  and similar — but no callers need it yet (cross-pond import
  arrives in D5).

#### D2 substantive completion notes

- New `crates/steward/src/control_table.rs` is a ~600 line wrapper
  (vs the 1639-line pre-D2 file) over `sync_steward::ControlTable`.
  `PondMetadata` (pond_id, birth_*) lives under `Uuid::nil()` config
  rows; factory modes under `factory_mode:<name>`, settings under
  `setting:<key>`, both keyed by the local pond_id.
- `record_import_partition`, `update_import_watermark`,
  `query_import_partitions` removed entirely.  Their callers in
  `crates/steward/src/{guard,dispatch}.rs` and
  `crates/cmd/src/commands/run.rs` are now no-ops; cross-pond
  watermarking returns in D5 via row-level pond_id partitioning.
- `record_post_commit_*` methods preserved at the wrapper API but
  internally write `PostPush*` lean records; post-commit attributes
  (`execution_seq`, `factory_name`, `config_path`, `error_message`)
  packed into `metadata_json`.  D6 may rename the wrapper methods.
- `record_failed` records' error text moved to `metadata_json.reason`;
  `record_data_committed` records' `data_fs_version` moved to
  `metadata_json.data_delta_version` (matches sync_steward's
  `DataCommittedMetadata`).
- CLI SQL queries in `cmd/control.rs`, `cmd/show.rs`,
  `cmd/replicate_test_simple.rs`, `cmd/control_test.rs` rewritten
  against the lean schema (`FROM control`, columns `record_kind`,
  `ts_micros`, `has_parent_seq`, `metadata_json`, ...).  Post-commit
  attribute columns extracted via `json_get_str(metadata_json, ...)`.
- `pond control recent` and `pond control detail` no longer render
  the `Command:` line; the original CLI args live in data Delta
  commit metadata (`pond_txn`).
- `pond control show-config` / `set-config` work unchanged against
  the new cache-backed wrapper; settings round-trip across reopen.
- Sync_steward gained three public accessors used by the wrapper:
  `pub const TABLE_NAME`, `pub fn session_ctx()`, `pub fn
  delta_table()`, `pub fn set_delta_table()`.
- Pre-existing `useless-conversion` clippy lints in
  `crates/sync-store/src/store.rs:438` and
  `crates/sync-steward/src/steward.rs:488` cleaned up as part of
  the same CI green-gate pass.
- Final state: 1387 unit tests pass (identical to pre-D2 baseline),
  9 ignored (S3/MinIO require runtime), `cargo fmt --check` clean,
  `cargo clippy --workspace --all-features -- -D warnings` clean.

### D3: Add `verify` and `restart` operations

**Goal**: expose sync_remote's `verify_against_remote` and
`restart_*_from_compact` to operators via new CLI verbs.

**Steps**:
- Implement adapter on top of duckpond's tlogfs that satisfies
  sync_remote's interface for `verify_against_remote`:
  - Compute live partition checksums over OplogEntry rows
    matching `(pond_id, part_id)`.
  - Match against remote-recorded checksums.
- Add CLI: `pond verify [--local | --remote=<name> | --all]`,
  `pond restart-from-compact <name>`.
- Initially these may use a TRANSITIONAL bundle format compatible
  with the existing `RemoteTable` (since D4 hasn't happened yet);
  OR D3 may be deferred until after D4.
- (Re-evaluate D3 vs D4 ordering at start of D3.)

### D4: Replace RemoteTable with sync_remote::Remote

**Goal**: bundle format change; old remote layer deleted.

**Steps**:
- Delete `crates/remote/{table.rs, schema.rs, chunking.rs}`.
- Rewire `crates/remote/factory.rs` to call `sync_remote::Remote`.
  Significant slimming; remove parallel push/pull/import logic.
- Rewire `crates/cmd/src/main.rs` and command modules to call
  the new sync APIs (or keep `pond run` factory dispatch as the
  thin wrapper).
- Add direct top-level CLI verbs: `pond push`, `pond pull`,
  `pond maintain`, `pond restart-from-compact`,
  `pond remote attach/detach/list`.
- Update `crates/remote/s3_registration.rs` — the same pattern
  as sync_remote's `register_s3_handlers`; consolidate to one
  location.
- All existing remote tests get rewritten.
- CI gate: full integration tests pass; S3 tests pass against
  MinIO.

**Risk**: any remote backups in the old `ChunkedFileRecord`
format are no longer readable.  Per "no compatibility issue"
constraint, this is acceptable; existing user backups are
treated as disposable for the migration.

#### D4 implementation notes (as executed)

D3 was folded into D4: rather than expose `verify_against_remote` /
`restart_*_from_compact` against the TRANSITIONAL chunked-parquet
bundle format, we went straight to the sync_remote Delta-bundle
format under `sync_remote::Remote`.  Outcome split across six
sub-commits, all on branch `jmacd/52`:

| Phase | Commit | Summary |
|---|---|---|
| D4.1 | `abf0d5c0` | foundation: `crates/sync-remote` wired into workspace, factory shim removed |
| D4.2 | `cb41d8b2` | `ShipRemoteSteward` adapter + tests |
| D4.3 | `bfc77e32` | CLI verbs `pond remote add/remove/list`, `pond push`, `pond pull` |
| D4.4 | `d6766353` | post-commit auto-push for `/sys/remotes/*` |
| D4.5 | `e62083d8` | delete legacy `crates/remote/` entirely (5092 LOC), prune callers, drop orphan import scaffolding |
| D4.6 | `5cd1609b` | testsuite cleanup — rewrite 500/501 on the new CLI; skip-mark 510/520-523/530-542 |

**Final CLI surface delivered (vs original plan)**:

- `pond remote add <name> <url> --mode {push,pull,both} [--region | --endpoint | --access-key-id | --secret-access-key | --allow-http]`
- `pond remote remove <name>`
- `pond remote list`
- `pond push [<name>]` — pushes one or all `push|both`-mode remotes
- `pond pull [<name>]` — pulls one or all `pull|both`-mode remotes
- Post-commit hook: every write-tx auto-pushes all `/sys/remotes/*`
  configured as `push` or `both`.

Plan called for `pond remote attach <url> --name <name> [--mount]` and
`pond restart-from-compact`.  We renamed the management verb to `add`
(simpler than the path-mount machinery, which is gone in D4 — `--mount`
is D5 cross-pond import territory).  `restart-from-compact` is not
exposed as a CLI yet — the operator-facing version is deferred until
`Remote::restart_from_compact` is generic over `RemoteSteward`.

**Surface removed in D4.5**:

- `pond init --from-backup <path>` and `pond init --config <b64>` —
  bootstrap from a backup no longer exists at the CLI; the in-Rust
  path lives in `ShipContext::create_pond_for_restoration` and is
  only used by tests.
- `pond sync [<name>]` (top-level) and `pond control sync` — replaced
  by `pond push` + `pond pull`.
- `pond run /system/run/N-remote push|pull|show|verify|replicate` —
  the `remote` factory itself is gone; no `pond mknod remote ...` or
  `pond apply` of legacy remote-factory YAML.

**Orphan scaffolding removed in D4.5** (deleted to keep the workspace
clean under `-D warnings`):

- `tinyfs::ProviderContext::import_partitions` field + setter.
- `tlogfs::OpLogPersistence::{pending_import_metadata, add_import_metadata,
  import_metadata}`.
- `tlogfs::ImportPartitionRecord` struct + re-export.
- `steward::dispatch::query_import_partitions`.

Cross-pond import returns in D5 via row-level `pond_id` partitioning;
the scaffolding above was a transitional shim that the rest of D4
made unreachable.

**Carry-forwards into D5**:

- `Remote::restart_from_compact` is not yet generic over `RemoteSteward`,
  so destination ponds cannot bootstrap a first-pull from shell.  The
  integration tests in `crates/cmd/tests/test_remote_cli.rs` seed the
  replica's `last_pulled_seq:<url>=1` manually via
  `ShipRemoteSteward::config_set` + `steward::Steward::create_pond_for_restoration`.
  This blocks the pull halves of `tests/500-s3-replication-minio.sh`
  and `tests/510-synth-logs-replication-cycle.sh`.
- `Remote::push` currently rejects `data_delta_version=0`; the
  driver-side `max(lower+1, 2)` clamp could move into push itself
  (treating 0 as a clean no-op instead of a Schema error).
- 13 testsuite scripts are `# DISABLED-D4:` skip-marked (510, 520-523,
  530-533, 540-recursive-sitegen, 540-import-watermark-incremental,
  541, 542); they need rewriting either as part of D5 (cross-pond
  import) or once first-pull bootstrap is exposed.

### D5: Migrate tlogfs to partition by `(pond_id, part_id)`

**Goal**: make the data Delta table pond-pure at file
granularity; move pond identity to bootstrap row.

**Steps**:
- Update `crates/tlogfs/src/persistence.rs` `with_partition_columns`
  to include `pond_id`.
- Schema migration on existing local ponds: probably "create
  fresh from data" rather than in-place, given no compat
  constraint.
- Pond identity: move from control-table Setting to root
  directory's OplogEntry's `pond_id` column.  Bootstrap row
  becomes the canonical source.
- Adjust `Steward::open` to read `pond_id` from the root
  directory's OplogEntry, not from the control-table Setting.
- File-granularity push filter becomes possible
  (`Remote::push` reads files for own pond_id only via
  partition pruning).
- CLI: cross-pond import now end-to-end via `pond remote attach
  <url> --mount <path>` flow.
- CI gate: cross-pond integration test (mirror
  sandbox/tests/cross_pond.rs for duckpond).

#### D5 implementation notes (as executed)

D5 expanded into 15 sub-phases on `jmacd/52` (every row is in the Status
table above with its commit).  The arc:

- **D5.1-D5.3** (`1137d81d`, `2d03c946`, `50956974`): partition tlogfs by
  `(pond_id, part_id)` via fresh-start migration; move pond identity to
  the data-table bootstrap row; filter `Remote::push` by `pond_id`
  partition.
- **D5.4-D5.6** (`2f1ee2ab`, `ccecc54a`, `1f2e2811`): generic first-pull
  (`bootstrap_consumer` + `Ship::create_replica`); `compute_live_checksums`
  for the tlogfs row schema; lift the `data_delta_version=0` clamp into
  `Remote::push`.
- **D5.7a** (`a1367d90`, `c55d9bd3`, `fbc33b43`): snapshot per-partition
  checksums in `record_data_committed`; process write lock + drop
  read-transaction control records; thread `FinalizedCommit` version
  through the commit chain.
- **D5.7b** (`4cb992d4`, `7e763a42`, `6632f1ea`, `2f95f99b`, `bfcfe76e`):
  split `remote add` (pull) vs `backup add` (push); materialize the
  cross-pond mount on first pull; read-only foreign mounts + scoped
  auto-exec; `remote remove` detach (default) and `--purge`; attach-time
  mount/store_id conflict checks.
- **D5.8** (`01987ebb` .. `75eb4139`): revive 13 testsuite scripts
  disabled in D4.6; D5.8.1 also fixed a silent bundle drop for
  post-commit factory writes (`NoSuchCommit`).
- **D5.9** (`d5bf4b68`): close **P1-BUG-LF-REPLICATION**, the cutover
  correctness blocker -- large-file blobs in `_large_files/` were never
  enumerated by `actions_at_version`, so files >64 KiB failed on first
  read after replication.  See § "Open items (post-D9)" for the full
  write-up.

### D6: Cross-pond model migration + operator-guide rewrite

**Goal**: deprecate factory-mode-based import; document the
new operator surface.

**Steps**:
- Remove `import:` section handling in remote configs (replaced
  by `--mount` on attach).
- Existing duckpond docs (`operator-guide.md`,
  `operator-interface-plan.md`, `cli-command-structure.md`):
  - Move to `docs/archive/` once their replacements land.
  - New `docs/operator-guide.md` written based on the post-
    integration CLI and lifecycle states.
- New CLI commands documented:
  - Setup: `pond init`, `pond init --from-remote`,
    `pond remote attach`, `pond remote detach`.
  - Routine: `pond push`, `pond pull`, `pond maintain`.
  - Diagnostics: `pond status`, `pond log`, `pond verify`.
  - Recovery: `pond recover`, `pond restart-from-compact`,
    `pond rebuild-control` (operator-driven control-table
    reset).
- Lifecycle-state vocabulary documented for diagnostics
  (PreCommit / DataCommitted / PostPushOk / etc.).

#### D6 implementation notes (as executed)

The phase that actually shipped under the "D6" label was the
**diagnostic/recovery CLI surface + operator-guide rewrite**, not the
cross-pond model migration described above.  Cross-pond import had
already landed in D5.7b (row-level `pond_id` mounts via `pond remote add
NAME URL /mount`), so the "remove `import:` section handling" and
"`--mount` on attach" goals were satisfied before D6 began.  D6 therefore
delivered the operator verbs the original plan had grouped under
"Diagnostics" and "Recovery", in four sub-commits on `jmacd/52`:

| Phase | Commit | Summary |
|---|---|---|
| D6.1 | `46a31668` | `pond verify [<name>]` over `verify_against_remote`; per-partition mismatch + divergence-boundary diagnostics |
| D6.2 | `53bb0c1f` | `pond status`: offline aggregate of identity + local commit state + recovery health + per-remote watermarks |
| D6.3 | `2b327aae` | `pond rebuild-control [--force]`: reconstruct the control table from the data table's `pond_txn` commit history |
| D6.4 | `fd15b773` | `pond restart-from-compact <name>`: delegates to the generic `Remote::restart_pond_from_compact`; mirror restart re-persists the dropped attachment |

The operator-guide rewrite (`8a7b9de4`) replaced the factory-based pre-D6
guide (archived to `docs/archive/operator-guide-pre-d6.md`).

D6.1 surfaced **P2-VERIFY-BOOTSTRAP-DRIFT** (replica-side verify
mismatched on the root partition because the producer's `pond_init`
bundle was not replicated) and D6.4 surfaced
**P2-PRODUCER-COMPACT-BUNDLES** (producers hard-coded `CommitKind::Write`,
so no Compact baseline ever reached the remote, leaving the mirror
retention-recovery loop unreachable end-to-end).  Both are closed in
D7/D7b below.

### D7: Producer-side compaction (close P2-PRODUCER-COMPACT-BUNDLES)

**Goal**: duckpond producers create pushable Compact bundles, making the
mirror retention-recovery loop (`pond restart-from-compact`) reachable
end-to-end from duckpond alone.

**Problem**: `pond maintain --compact` ran a best-effort Delta optimize
that was never recorded in the control table, so `Remote::push` (which
iterates `DataCommitted` records) only ever saw `CommitKind::Write` and
never emitted a Compact bundle.

**Steps (as executed, `2df6ea7d`)**:
- `Ship::compact()` runs a recorded, pushable compaction: allocate seq +
  write lock + Begin, snapshot pre-checksums, Delta optimize scoped to
  own `pond_id` partitions, assert per-partition checksum invariance
  (content unchanged), then record `DataCommitted(commit_kind=Compact)`
  at the new Delta version.  A no-op (nothing to merge) writes Completed
  without advancing the committed sequence.
- `ControlTable::record_compact_committed` factors out the shared body
  of `record_data_committed`.
- `maintenance::compact_pond_partitions` runs the `pond_id`-scoped
  optimize and stamps the compaction's `txn_seq` into the commit's
  `pond_txn` metadata.
- `Ship::maintain(force, compact)` routes data-table compaction through
  the recorded `Ship::compact`; checkpoint/vacuum stay best-effort; the
  control table (never pushed) keeps its best-effort optimize.
- Also fixed a latent reopen bug surfaced by the above:
  `OpLogPersistence::open` recovered `last_txn_seq` from only the newest
  Delta commit, but vacuum (VACUUM START/END), optimize, and checkpoint
  commits carry no `pond_txn`; the trailing vacuum after `--compact`
  reset `last_txn_seq` to 0 on reopen, so the compaction never pushed.
  `open` now scans all retained history for the max `pond_txn` seq.
- Tests (`crates/steward/tests/remote_adapter_test.rs`):
  `ship_compact_records_pushable_compact_transaction`,
  `ship_compact_bundle_drives_restart_from_compact` (full mirror loop),
  `ship_compact_survives_reopen`,
  `ship_maintain_compact_survives_reopen` (the reopen regression).

### D7b: Replicate the `pond_init` bundle (close P2-VERIFY-BOOTSTRAP-DRIFT)

**Goal**: a replica bootstrapped from a remote verifies clean (no
root-partition mismatch) with no local writes; producer- and
replica-side `pond verify` become symmetric.

**Root cause** (confirmed at the Delta-log level): the producer's
`pond_init` root-directory row lands at Delta version 1 (version 0 is the
empty CREATE TABLE), but `Ship::create_pond` hard-coded
`data_delta_version=0` + empty checksums for seq=1.  `Remote::push`'s
`data_delta_version==0` bootstrap-skip then never replicated the root
row, so the replica was missing the root-dir v1 leaf the producer's
recorded checksum includes -> root-partition Merkle differs -> mismatch.

**Steps (as executed, `366c46b9`)**:
- `Ship::create_pond` records seq=1's real `data_delta_version`
  (`table.version()` after root init) and real partition checksums, so
  push builds a normal bundle for it.
- `Remote::push`'s `data_delta_version==0` skip remains only as a
  legacy/defensive guard (current ponds no longer hit it).
- `Remote::bootstrap_consumer` and `pond pull` seed `last_pulled_seq` to
  `oldest_available_seq - 1` instead of hard-coding 1: current producers
  (oldest=1) seed 0 and pull seq 1+, receiving the identity-bearing root
  rows; legacy producers (oldest=2) seed 1, preserving old behavior.
- Tests: `ship_replica_verify_matches_after_bootstrap` (the regression),
  `ship_remote_push_replicates_pond_init`.

### D8: Read `host+table:///` / `host+series:///` Parquet directly

**Goal**: `pond cat host+table:///file.parquet` (and `host+series://`)
queries valid Parquet host files instead of erroring `File '...' is not
queryable (type: FilePhysicalVersion)`.

**Root cause**: `host+table://` parses as builtin entry-type `table`, so
`Provider::create_table_provider` routed it through the tinyfs builtin
path, where the host file resolves to `FilePhysicalVersion` (raw bytes)
and `as_queryable()` rejects it -- the Parquet format was never
considered.

**Steps (as executed, `f0a38111`)**:
- When `url.is_host()` and the entry type is `table`/`series`, read the
  host file directly as Parquet (`open_host_url` +
  `ParquetRecordBatchReaderBuilder` -> `MemTable`) instead of routing
  through tinyfs.  A non-Parquet file now yields a clear "not a valid
  Parquet file" error rather than the opaque "not queryable" one.
- Tests (`crates/provider/src/provider_api.rs`):
  `test_host_table_reads_parquet_directly`,
  `test_host_table_rejects_non_parquet`.
- Sibling fix (`212e3dd2`): `host+csv+series://` must use the CSV
  provider, not the Parquet path.

### D9: `file://` testsuite coverage for the D6/D7 remote CLI verbs

**Goal**: cover the D4..D7 replica push/pull CLI verbs in the base
container (no docker-compose / MinIO) via a local `file:///` remote.

**Steps (as executed, `e57c357c`)** -- five self-contained tests, 69
checks total:
- `710-remote-backup-lifecycle.sh`: backup add/list/remove (push side),
  auto-push on attach, push no-op, verify, status, `--bidirectional`,
  unknown-remote negative paths.
- `711-cross-pond-file-import.sh`: `remote add NAME URL /imports/X`,
  pull, content match across the import boundary, watermark
  advance/idempotence, detach vs `--purge`, self-mount refusal.
- `712-maintain-compact-push.sh`: `maintain --compact` records a Compact
  txn, push emits the Compact bundle, verify clean, fresh consumer
  reproduces content.
- `713-rebuild-control.sh`: `rebuild-control --force` gating,
  reconstructs txns, preserves `pond_id`, leaves `control.bak.*`, pond
  stays usable.
- `714-restart-from-compact.sh`: cross-pond rebuild from the compact
  baseline, catch up, imported content intact, own data untouched.

**Follow-on hardening** (post-D9, same cutover sweep):
- `42c65540`: require `${env:VAR}` credential references for remote
  attachments (literal `secret_access_key` rejected at attach time).
- `078b9f25` / `7232d254`: `last_pushed_seq` watermark correctness
  (advance on idempotent re-push; surface corrupt watermarks).
- `f0ac0002`: propagate snapshot errors in `apply_pulled_bundle`.

## Out of scope (for now)

- CRDT-style symmetric ponds (future direction; mode becomes
  non-issue).  **Superseded** by the content-addressed arc above: the
  CA3 commit DAG plus single-writer linear refs gives symmetric sync via
  fast-forward + grafting, without CRDTs (see "Future direction:
  content-addressed replication").
- `pond init`-time configuration of FS conventions and per-replica
  defaults (currently via control-table settings).
- Old-format bundle migration tooling (no compat needed).
- Multi-process locking on local pond directories (single-writer
  model assumed; control-table reset requires operator to
  guarantee no concurrency).

## References

- Sandbox prototype design: `docs/archive/sandbox-design.md` (the
  sandbox-side design that this integration adopts; archived from
  `duckpond/sandbox/DESIGN.md` at the end of D1).
- Sandbox commits adopting the model:
  - `c40737c2` A1 (pond_id partitioning)
  - `97dd8f5d` A2 (control table per-pond seqs)
  - `974c7694` A2 (PulledBundle struct)
  - `149af878` A3 (push filter + cross-pond pull)
  - `bc99c321` A4 (verify scope)
  - `a13419fe` A5 (per-pond restart)
  - `d6b79e62` A6 (S3 backend)
- Integration analysis (this redesign rationale):
  `~/.copilot/session-state/<session>/files/integration-analysis.md`
  (session-local; copy to `docs/` if needed for reference).
- Decisions made via interactive Q&A with the user during
  the planning session (see commit message of D1 for an
  enumeration).
