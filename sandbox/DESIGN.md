# Sandbox: Design and Implementation Status

A throwaway prototype validating a remote-sync redesign before committing
to changes in the duckpond runtime. Lives at `duckpond/sandbox/` as its
own Cargo workspace (disjoint from `duckpond/Cargo.toml`).

> This document is the canonical reference for the sandbox.  It covers
> the design we are validating, what has been built so far, what tests
> cover it, and what remains.

---

## 1. Why we are doing this

DuckPond's current remote-backup design (`crates/remote`) has three
unbounded growth axes:

1. **Bucket size grows monotonically.**  Every commit pushes a new
   bundle.  Nothing is ever deleted.
2. **Push cost grows with version count.**  `execute_push`
   walks `1..=current_version`, opening the local Delta table once
   per missing version.
3. **Restore cost grows with version count.**  `execute_pull` and
   `execute_import` replay every transaction from version 1.

A first design pass introduced a separate "snapshot" artifact in the
bucket as a restart point.  A subsequent rethink (in chat) produced a
much cleaner model:

- **Compaction commits ARE restart points.**  They contain merged
  parquet content for everything before them.  No separate snapshot
  artifact needed.
- **Source and consumers compact independently.**  The remote bucket is
  the canonical append-only log of original writes plus periodic
  source-side compactions.  Each side bounds its own size.
- **Per-partition content checksums on every commit verify equivalence.**
  Two stores reaching the same logical state must produce the same
  checksums regardless of physical layout.

The design is sophisticated enough that landing it directly in
duckpond risked tangling with tinyfs/tlogfs/provider/factory machinery
in ways that would obscure the design's correctness.  So we built a
sandbox: a minimal Delta-Lake-backed key/value store with the same
sync semantics, no other duckpond layers, heavily tested.

The prototype's value is **the design**.  The code is disposable.

---

## 2. The design

### 2.1 Storage model (sandbox)

A single Delta Lake table per "store", partitioned by `partition_key`.
Each row is one version of one item:

| column         | type      |
|----------------|-----------|
| partition_key  | Utf8      |
| item_key       | Utf8      |
| txn_seq        | Int64     |
| deleted        | Boolean   |
| value          | Binary    |
| value_blake3   | Binary (32 bytes) |
| ts_micros      | Int64     |

For each `(partition_key, item_key)`, the row with the largest
`txn_seq` is the **live** version.  If that row has `deleted = true`,
the item is absent.  Tombstones survive forever in storage but never
contribute to the partition's logical state.

The schema is intentionally narrow.  No nested types, no large-file
out-of-band store, no entry types, no factories.  The intent is to
exercise sync semantics, not to model duckpond's data layer.

### 2.2 Per-partition content checksums

A *partition checksum* is a fingerprint of the live (post-tombstone)
key-value content of one partition.  It must satisfy:

- **Deterministic**: same logical content -> same bytes.
- **Layout-independent**: independent of write order, parquet file
  count, compaction history.
- **Sensitive**: any change to a key, a value, or membership flips it.

The `PartitionChecksum` trait has two implementations behind the same
interface:

| Strategy | Compute | Update | Storage | Notes |
|---|---|---|---|---|
| `Merkle` | O(N) | O(K log N) (planned, not yet incremental) | 32 bytes | BLAKE3 binary tree over sorted leaves with leaf/node/empty domain separation tags. Cryptographically conservative. |
| `Homomorphic` | O(N) | O(1) per item touched | 32 bytes | Sum of per-leaf BLAKE3 digests in `Z/2^256 Z` (modular u256 byte-level addition). Order-independent by construction. ~2^128 collision resistance, sufficient for accidental-divergence detection. |

Both produce 32-byte outputs tagged with `ChecksumKind` so two
checksums of the same logical state but from different strategies are
never `==`.

The benchmarks todo will pick a default once measured.  The trait
exists permanently so duckpond can swap if needed.

### 2.3 Transaction lifecycle

```text
begin_write -> WriteGuard
              put / delete buffered in memory
WriteGuard.commit
  -> store.apply_batch (single Delta commit, all ops together)
  -> recompute partition checksums for all known partitions
     (touched partitions get a fresh checksum; unchanged partitions
      carry forward from the previous commit so the snapshot is complete)
  -> control_table.write (DataCommitted with commit_kind, parent_seq,
     duration, partition_checksums in metadata_json)

WriteGuard.abort(reason) -> control_table.write (Failed, reason)
WriteGuard.commit (no ops) -> control_table.write (Completed)

begin_read -> ReadGuard.get / list / partitions
              (NO control-table writes)
```

Read transactions write nothing to the control table.  This is the
selfmon-driven design choice: a 1-min ping reading data should not
produce a control-table commit.

Aborted writes consume a `txn_seq` (so `last_write_seq` advances) but
do NOT advance `last_committed_seq`.  Empty/no-op writes get a
`Completed` row -- terminal but not committed.  Any write that
*panics* leaves an orphan `Begin` row; on re-open the steward loads
`last_write_seq` from `MAX(any record kind)` so the next allocation
skips orphans.

### 2.4 Compaction

A compaction runs Delta `optimize().with_type(Compact)` to merge
small parquets in a partition.  The Delta commit contains `Add` actions
for the new big parquet and `Remove` actions for the small ones.

The steward's `compact(filter)` method:
1. Allocates a new `txn_seq`.
2. Records `Begin`.
3. Snapshots every known partition's checksum **before** the optimize.
4. Runs `Store::compact(filter)` (a thin wrapper over delta-rs's
   `optimize().with_type(Compact)`, optionally restricted to one
   partition_key value).  On error: writes `Failed`, returns the error.
5. Inspects the metrics:
   - **No-op** (zero files added or removed -- empty pond, nonexistent
     filter, or already-optimal layout): writes `Completed`.
     `last_committed_seq` does NOT advance.  No new Delta commit was
     produced, so there is no new restart point either.
   - **Real work**: snapshots every partition's checksum **after**,
     asserts pre and post are equal (per-partition AND set-equality of
     keys).  An inequality is a bug; surface loudly via
     `StewardError::Invariant`.  Writes `DataCommitted` with
     `commit_kind = Compact` and the post snapshot.

Real-work compaction commits are normal Delta commits and live alongside
Write commits in the same `txn_seq` sequence.  Both kinds of commits
qualify as restart points for downstream consumers.

### 2.5 Remote sync model

The "remote" is another local Delta Lake table at a different
filesystem path.  Same shape as an S3 bucket, accessed via
`object_store::LocalFileSystem`.  When the design maps back to
duckpond, swapping for `S3` is a config change; the mechanics here do
not change.

Bucket layout:

```text
<remote-root>/
  _delta_log/                                Delta log of the remote table
  bundle=<store_id>/txn=<seq>/
    manifest.json                            kind, parent_seq, partition_checksums, file_list
    data/part-*.parquet                      copies of the source's Delta data files
    log/<seq>.json                           copy of the source's Delta commit log
```

`manifest.json` is a small JSON sidecar with everything needed for
retention walking and pull dispatch without reading the bundle's data:

```json
{
  "store_id": "...",
  "txn_seq": 42,
  "parent_seq": 41,
  "kind": "Compact",
  "partition_checksums": {
    "p1": {"kind": "merkle", "hex": "..."},
    "p2": {"kind": "merkle", "hex": "..."}
  },
  "data_files": [{"path": "data/part-001.parquet", "size": 12345}],
  "log_file": "log/42.json"
}
```

#### 2.5.1 Push

After each successful commit, push backs up the new bundle:

1. Read the latest local `DataCommitted` record (kind, parent_seq,
   partition_checksums).
2. Build the manifest.
3. Upload manifest, data files, and commit log to
   `bundle=<id>/txn=<seq>/`.
4. Write a Delta commit on the remote table referencing the new bundle.

`last_pushed_seq` (per remote) is tracked as a setting in the local
control table so subsequent pushes are O(M) where M is the number of
unpushed transactions (usually 1).

#### 2.5.2 Pull

A consumer pulls bundles in `txn_seq` order from `last_pulled_seq + 1`
to the remote's current `txn_seq`.  For each bundle, dispatch on
`kind`:

- **Write bundle**: download data files + log, apply to local Delta
  table, recompute affected partition checksums, compare to manifest.
  Mismatch -> abort.
- **Compact bundle, mirror mode** (default): download merged parquets,
  apply Add/Remove, recompute checksums, compare.  Local layout
  matches source.
- **Compact bundle, independent mode** (opt-in via `pull_mode` config):
  skip data download, advance `last_pulled_seq`, recompute checksum
  from local content (which already represents the same logical state),
  compare to manifest.  Consumer keeps its own physical layout and is
  responsible for its own compaction schedule.
- **Compact bundle, consumer behind retention**: this commit IS the
  starting point.  Download merged parquets, set
  `last_pulled_seq = txn_seq`, do NOT replay missing prior bundles.
  Used for fresh restore or when retention pruned earlier bundles.

#### 2.5.3 Retention

`Remote::maintain` with `vacuum_pre_compaction = true`:

1. Find the last N Compact bundles (default N=2 from
   `remote_compaction_retention` setting).
2. Horizon = oldest of those N's `txn_seq`.
3. Delete bundles with `txn_seq < horizon`.
4. Refuse if N < 1 or fewer than N Compact bundles exist (would leave
   no restart point).

Once a consumer falls below the horizon, its next pull returns an
explicit error directing the operator to run `restore_reinit` (wipe
local + bootstrap from oldest available compact bundle).

#### 2.5.4 Verification

`verify_local`: re-derive each partition's checksum from current data
and compare against the `partition_checksums` recorded on the latest
`DataCommitted` record.  Implemented and tested.

`verify_against_remote`: pull the latest manifest's checksums per
partition and compare to local recomputation.  Walks back to identify
the `txn_seq` at which divergence first appeared.  Planned, not yet
implemented.

### 2.6 Coupling and safety rules

Carrying forward from the design discussions:

1. **Compaction is a no-op for partition checksums.**  Strictly
   asserted at compaction time; a violation indicates a bug.
2. **`txn_seq` is monotonic across writes, aborts, no-ops, and
   compactions.**  Allocated at `begin_write` regardless of outcome.
3. **Read transactions never write control-table records.**  Selfmon-
   visible cost rule.
4. **Push runs after commit, before any retention or vacuum.**  The
   bundle for txn N must exist on the remote before the source can
   safely vacuum data files referenced by txn N.
5. **Retention never deletes below the oldest retained Compact
   bundle.**  If a consumer's `last_pulled_seq` is below that horizon,
   it must restart.
6. **Consumer mirror mode is the default.**  Independent mode is
   opt-in and demands the consumer compact its own copy.

### 2.7 Mapping back to duckpond (after the prototype proves out)

What flows back:

- The bundle manifest format and `kind`-tagged push: extends
  `crates/remote`.
- Per-partition checksum primitive: new module in `crates/tlogfs`
  (where `OplogEntry` rows live; "partition" = `part_id`).
- Per-commit checksum recording: extends control table schema in
  `crates/steward`.
- Pull dispatch and retention: rewrites paths in
  `crates/remote/factory.rs`.
- Mirror vs independent mode and the verify command: new operator
  surface.

The simpler kv schema does NOT map back; duckpond keeps `OplogEntry`.
The checksum and sync changes layer on top.

---

## 3. The prototype: what's been built

### 3.1 Workspace layout

```text
duckpond/sandbox/
  Cargo.toml              own workspace root (NOT a member of duckpond/)
  Cargo.lock
  README.md               brief overview
  DESIGN.md               this document
  rust-toolchain.toml
  .gitignore

  store/                  Delta-Lake-backed kv store + checksum primitives
    Cargo.toml
    src/
      lib.rs
      error.rs
      schema.rs
      store.rs
      checksum/
        mod.rs            trait + ChecksumKind + Checksum + Leaf
        merkle.rs         Merkle impl
        homomorphic.rs    Homomorphic impl
    tests/
      store.rs            19 store integration tests
      checksum_integration.rs   11 checksum-x-store tests

  steward/                Transaction lifecycle + control table + verify
    Cargo.toml
    src/
      lib.rs
      error.rs
      control_table.rs
      guard.rs
      steward.rs
    tests/
      steward.rs          18 lifecycle tests

  remote/                 Bundle manifests, push, pull (PLACEHOLDER ONLY)
    Cargo.toml
    src/
      lib.rs              just a smoke test today

  tests/                  Cross-crate integration tests (PLACEHOLDER)
    Cargo.toml
    src/
      lib.rs              just a smoke test today
```

Total Rust source so far: ~2900 lines (including tests).

### 3.2 What works today

#### 3.2.1 `sandbox-store`

Public API:

```rust
Store::create(path) / Store::open(path)
Store::apply_batch(txn_seq, ts_micros, ops)   // transactional
Store::put(partition, key, value)             // single-op convenience
Store::delete(partition, key)
Store::get(partition, key) -> Option<Vec<u8>>
Store::list(partition) -> Vec<(String, Vec<u8>)>
Store::partitions() -> Vec<String>
Store::partition_leaves(partition) -> Vec<(String, [u8; 32])>
Store::compute_partition_checksum(partition, &strategy) -> Checksum
Store::compact(filter: Option<&str>) -> CompactMetrics  // delta optimize(Compact)
Store::data_files() -> Vec<String>            // current Delta version's files
Store::last_txn_seq() -> i64
Store::delta_version() -> i64
```

Checksum trait:

```rust
trait PartitionChecksum: Send + Sync + 'static {
    fn kind(&self) -> ChecksumKind;
    fn compute(&self, leaves: &[Leaf<'_>]) -> Checksum;
}

struct Checksum { kind: ChecksumKind, bytes: Vec<u8> }
enum ChecksumKind { Merkle, Homomorphic }

struct Merkle;       // tag-prefixed BLAKE3 binary tree
struct Homomorphic;  // sum of per-leaf BLAKE3 digests in Z/2^256 Z
```

#### 3.2.2 `sandbox-steward`

Public API:

```rust
Steward::create(path) / Steward::open(path)
Steward::create_with_options(path, opts: StewardOptions)
Steward::open_with_options(path, opts: StewardOptions)

Steward::begin_write() -> WriteGuard
WriteGuard::put / delete / commit -> CommitOutcome / abort

Steward::begin_read() -> ReadGuard
ReadGuard::get / list / partitions

Steward::compact(filter: Option<&str>) -> CommitOutcome  // Compact lifecycle

Steward::log(limit) -> Vec<ControlRecord>
Steward::last_write_seq() / last_committed_seq()
Steward::partition_checksums_at(txn_seq) -> Option<PartitionChecksums>
Steward::incomplete_transactions() -> Vec<ControlRecord>

Steward::config_set / config_get / config_list

verify_local(&Steward) -> VerifyReport
```

`StewardOptions` carries the checksum strategy (boxed `dyn`) so a
single `Steward` is parameterized at construction.  Default is
`Merkle`.

The control-table schema is intentionally narrow: 11 columns, with
nullable Int64 fields encoded as paired `has_*` Boolean columns to
avoid Delta nullable-primitive quirks.  Free-form payloads (partition
checksums, abort reasons, settings) go through a single
`metadata_json` Utf8 column.

#### 3.2.3 `sandbox-remote` and `sandbox-tests`

Skeleton crates with smoke tests only.  Real work in upcoming todos.

### 3.3 Test inventory

84 tests total across all sandbox crates (was 72 before `compact-op`;
+11 new compact tests, +1 net other counts adjustment).  All passing,
all under both checksum strategies where applicable.

| Crate / file | Count | What it covers |
|---|---|---|
| store/src/checksum/mod.rs (unit) | 14 | trait determinism, layout independence, empty-input stability, change sensitivity (run against Merkle and Homomorphic), Checksum hex format, kind serialization, array_from_slice |
| store/src/checksum/merkle.rs (unit) | 4 | empty-tree distinct from single-leaf, duplicate-key behavior, odd-count tree pairing, output is 32 bytes |
| store/src/checksum/homomorphic.rs (unit) | 6 | u256 add/sub roundtrip, carry, borrow, commutativity, order independence, output is 32 bytes |
| store/src/schema.rs (unit) | 3 | Arrow and Delta schemas agree, partition column constant, BLAKE3 length |
| store/tests/store.rs (integration) | 19 | put/get/delete roundtrip, multi-version semantics, tombstones, list filtering, partition discovery, apply_batch coalescing, empty-batch no-op, cross-partition independence, SQL metacharacter safety |
| store/tests/checksum_integration.rs (integration) | 11 | order-independent across stores (Merkle, Homomorphic), tombstone-neutral, cross-partition isolation, value-change detection, empty-partition stability, two strategies disagree on same state |
| steward/src/control_table.rs (unit) | 3 | Arrow vs Delta schema agreement, RecordKind serialization roundtrip, ChecksumValue serialization roundtrip |
| steward/tests/steward.rs (integration) | 18 | lifecycle records (Begin/DataCommitted/Failed/Completed), parent_seq tracking, no-op->Completed, abort, read guards write nothing, txn_seq monotonicity across all outcomes, partition checksums carry forward, orphan-Begin recovery, config_set/get/list latest-wins, persistence across re-open, log limit, verify_local under both strategies, verify on empty pond, put-after-delete-then-verify |
| steward/tests/compact.rs (integration) | 11 | get/list unchanged after compact, real-work compact -> DataCommitted(Compact), per-partition checksum invariance, partition filter touches only that partition (verified via Store::data_files), empty-pond compact -> Completed (no-op), nonexistent-filter compact -> Completed, parent_seq points at last write, Homomorphic strategy invariance, tombstones survive compaction, interleaved write/compact/write/compact lifecycle, persistence across re-open |
| remote/src/lib.rs | 1 | smoke (placeholder) |
| tests/src/lib.rs | 1 | smoke (placeholder) |
| **Total** | **84** | |

### 3.4 CI integration

`.github/workflows/rust-ci.yml` has a parallel `sandbox` job that runs:

```bash
cd sandbox
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-features -- -D warnings
```

`Makefile` has `make sandbox-check` (and `sandbox-test`, `sandbox-fmt`,
`sandbox-clippy`) for local convenience.

The duckpond workspace at `duckpond/Cargo.toml` does NOT include
sandbox crates as members; cargo commands in `duckpond/` ignore the
sandbox entirely.

---

## 4. Status of the 16 todos

| Todo | Status | Notes |
|---|---|---|
| sandbox-skeleton | done | workspace + 4 empty crates + CI smoke |
| sandbox-ci | done | CI job + Makefile target |
| store-schema | done | Store + put/delete/get/list/apply_batch + 19 integration tests |
| checksum-trait | done | trait + Merkle + Homomorphic + 11 store-bridging tests |
| steward-lifecycle | done | Steward + control table + WriteGuard/ReadGuard + verify_local + 18 tests |
| compact-op | done | Store::compact + Store::data_files + Steward::compact (no-op vs real-work lifecycle, partition filter, error -> Failed) + 11 tests |
| remote-push | pending | next on the critical path |
| remote-pull | pending | depends on remote-push |
| restart-from-compact | pending | depends on remote-pull |
| remote-retention | pending | depends on remote-push |
| verify-cmd | pending | depends on remote-push (verify_against_remote half) |
| library-api-coverage | pending | depends on remote-pull, remote-retention, verify-cmd |
| integration-tests | pending | depends on restart-from-compact, library-api-coverage |
| property-tests | pending | depends on remote-pull, remote-retention |
| benchmarks | pending | depends on checksum-trait (UNBLOCKED, can run any time) |
| sandbox-design-doc | pending | depends on integration-tests, property-tests, benchmarks |

6 of 16 done.

---

## 5. Next steps in detail

### 5.1 `compact-op` (done)

Implementation summary:

- `Store::compact(filter: Option<&str>) -> CompactMetrics` wraps
  delta-rs `optimize().with_type(Compact)` with an optional
  `PartitionFilter` on `partition_key`.  Returns the file
  add/remove counts so callers can detect no-ops.
- `Store::data_files() -> Vec<String>` enumerates the parquet files
  referenced by the current Delta version (used by tests to verify
  partition-filtered compaction only touches the requested partition).
- `Steward::compact(filter: Option<&str>) -> CommitOutcome`:
  allocates a `txn_seq`, writes `Begin`, snapshots pre-checksums,
  runs `Store::compact`.  Three terminal branches:
  - Error -> `Failed` record, error returned.
  - No-op (zero files added or removed) -> `Completed` record;
    `last_committed_seq` does not advance.
  - Real work -> snapshot post-checksums, assert `pre == post`
    (per-partition AND set-equality), write `DataCommitted` with
    `commit_kind = Compact`.

The `CommitOutcome` always carries `commit_kind = Compact`; callers
distinguish the no-op case via `had_data = false`.

11 tests in `steward/tests/compact.rs` cover happy paths under both
checksum strategies, partition-filter scoping, both no-op branches,
tombstone preservation, interleaved writes/compactions, and re-open
persistence.

### 5.2 `remote-push` (next on critical path)

**Goal**: implement `Remote::create / open / push`.  The push runs
manually (operator-triggered); auto-push-on-commit is a separate later
todo.  After push, the remote bucket directory contains a Delta table
plus per-bundle subdirectories.

**Files to write**:

- `sandbox/remote/src/error.rs`: error type.
- `sandbox/remote/src/manifest.rs`: `BundleManifest` struct,
  serde-JSON encoding, schema for the remote Delta table (one row per
  bundle).
- `sandbox/remote/src/remote.rs`: `Remote` type, `push` method,
  `list_bundles`, `latest_seq`, `oldest_available_seq`.
- `sandbox/remote/tests/push.rs`: tests covering happy-path bundle
  upload, manifest format, idempotent re-push, push of empty pond.

**Schema for the remote Delta table** (mirrors duckpond's chunked
record schema in spirit, simpler in detail):

| column | type | notes |
|---|---|---|
| store_id | Utf8 | partition column |
| txn_seq | Int64 | bundle id within a store |
| kind | Utf8 | "write" or "compact" |
| parent_seq | Int64 | predecessor or 0 |
| ts_micros | Int64 | when pushed |
| manifest_path | Utf8 | relative path to manifest.json in the bucket |

The actual data files live as plain parquet objects under
`bundle=<store_id>/txn=<seq>/data/`.  The Delta table is just an
index for fast listing.

**Tracking last_pushed_seq**: stored as a steward setting,
`last_pushed_seq:<remote_id>`.  Remote_id is the `store_id` of the
target remote (one steward can push to multiple remotes).

### 5.3 `remote-pull`

**Goal**: implement `Remote::pull(steward, mode)` that walks bundles
in order and dispatches on kind.  Verifies checksums after each apply.

**The dispatch logic** is the heart of the design and deserves
careful test coverage.  Each branch (Write, Compact-mirror,
Compact-independent, Compact-restart) gets at least one focused test
plus one end-to-end test that drives a multi-bundle scenario.

### 5.4 `restart-from-compact`

**Goal**: handle the consumer-behind-retention case.  When a
consumer's `last_pulled_seq` is below the oldest available remote
bundle, refuse the pull with a specific error and provide
`Remote::restore_reinit(steward)` as the recovery path.

### 5.5 `remote-retention`

**Goal**: implement `Remote::maintain(opts)` with `vacuum_pre_compaction`.

### 5.6 `verify-cmd`

**Goal**: implement `Remote::verify_against_remote(steward)` to
complement the existing `verify_local`.  Walks back through bundle
manifests to identify the `txn_seq` at which divergence first appeared.

### 5.7 `library-api-coverage`

**Goal**: round out per-method tests for happy-path AND error-path on
every Steward and Remote method.  Some are already covered (every
Steward method has at least one test).  Audit and fill gaps after
remote work lands.

### 5.8 `integration-tests`, `property-tests`, `benchmarks`

These run against the full library API.  Detailed in the plan; not
elaborated further here.

### 5.9 `sandbox-design-doc`

After all of the above, distill THIS document into a final form
covering only the validated design (no "planned" sections), the
chosen default checksum strategy, and the mapping back to duckpond.
That final form will live at `duckpond/docs/sandbox-design.md`.

---

## 6. Open design questions (to resolve before mapping back)

These are noted in the plan and worth restating:

1. **Which checksum strategy wins?**  Decide based on the benchmarks
   todo: per-commit update cost, full-recompute cost, storage
   overhead.  Both are correct; the tradeoff is performance vs
   security.

2. **Bundle manifest as JSON sidecar vs Delta record?**  Currently
   plan uses both: a small Delta table for indexing + a JSON file per
   bundle for the full manifest payload.  This may be simplifiable
   once we see how queries against it actually look.

3. **Independent-mode pull and physical layout drift.**  A consumer
   in independent mode will accumulate small parquets until it
   compacts itself.  Verify the prototype demonstrates this is
   operationally sane before recommending the mode for any real
   consumer.

4. **Crash recovery in the prototype.**  Steward records the
   lifecycle to detect incomplete transactions.  Recovery currently
   just *reports* them; it does not auto-roll-back or auto-replay.
   For the prototype this is fine; for duckpond we'd need a deliberate
   recovery story.

5. **Concurrency.**  Multiple processes against the same store
   should serialize via filesystem lock; not yet implemented.  Pull
   from one remote by multiple consumers should not block; should
   work today since pulls are read-only against the remote.

---

## 7. How to work on the sandbox

```bash
# Build everything
cd duckpond/sandbox
cargo build --workspace

# Run all tests (fast: ~5s)
cargo test --workspace

# Just one crate
cargo test -p sandbox-store
cargo test -p sandbox-steward

# Match the CI gates locally
cargo fmt --all -- --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace

# Or use the duckpond Makefile target from duckpond/
cd duckpond
make sandbox-check
```

When adding a new feature:

1. **Write the test first** if possible.  The sandbox is small enough
   that test-driven development is genuinely productive.
2. **Run all three CI gates** locally before committing.  Clippy is
   `-D warnings` so even one warning blocks CI.
3. **Keep the API minimal.**  Each method on `Steward`, `Store`, or
   `Remote` should exist because it appears in the design or because
   a test needs it -- not "just in case."
4. **Commit per todo.**  One conceptual change per commit.  Commit
   messages should describe what the design now does, not how the
   code does it.
5. **Update this document** when a todo is finished.  Move the row in
   the table, update the test count, note any design decisions made
   along the way.

---

## 8. References

- `duckpond/docs/efficiency-priorities.md`: the broader efficiency
  redesign this prototype is part of (sections 8 and 9 will be
  rewritten once the prototype validates the model).
- `duckpond/docs/archive/deltalake-efficiency.md`: the prior plan,
  superseded.
- `caspar.water/docs/selfmon-design.md`: the measurement infrastructure
  that will eventually quantify the duckpond-side improvements.
- Session plan file (in `~/.copilot/session-state/`): ephemeral
  per-session notes; this DESIGN.md is the canonical reference.
