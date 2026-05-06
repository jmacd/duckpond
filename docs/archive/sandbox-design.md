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

The "remote" is a single Delta Lake table hosting one pond family.
**One remote = one pond.**  Operators who want consolidation use
multiple buckets; the schema does not multiplex stores.  Production
deployments swap `LocalFileSystem` for `S3` as a config change; the
schema is identical.

The remote's `store_id` -- a UUIDv4 minted at `Steward::create` and
persisted in the source pond's local control table as a setting --
lives in Delta table properties under the `sandbox.store_id` key, set
once at `Remote::create`.  A consumer reads it on `Remote::open` and
either verifies it matches its own `store_id` or uses it to bootstrap
a fresh replica.

Bandwidth is not the primary motivation for the remote format.
Atomicity is: when bundle bytes live IN the remote Delta table's
rows, the Delta commit IS the atomic boundary for the whole bundle.
A push either makes everything visible or nothing; there are no
torn-bundle states to clean up, and Delta's own vacuum/checkpoint
machinery is the only retention mechanism we need.

#### 2.5.1 Schema

The remote table has ONE partition column, `partition_kind`, taking
three values.  All other columns are Parquet-native scalars; no JSON,
no Map, no nested Struct.

| `partition_kind` | Holds | Rows per push |
|---|---|---|
| `manifest` | bundle header (commit_kind, parent_seq, ts_micros) | 1 |
| `checksum` | per-source-partition content checksum | N (= source pond partition count) |
| `data` | per-chunk file content + add/remove markers | C (= sum of chunks across all files in the bundle) |

Constants:

- `CHUNK_SIZE_BYTES = 16 * 1024 * 1024` (16 MB; only the last chunk
  of a file is short)
- `BLAKE3_LEN = 32`

Full Arrow schema (all variant-specific columns nullable):

| column | type | populated on |
|---|---|---|
| `partition_kind` | Utf8 | every row -- partition column |
| `txn_seq` | Int64 | every row |
| `ts_micros` | Int64 | every row |
| `commit_kind` | Utf8 | `manifest` (`"write"` / `"compact"`) |
| `parent_seq` | Int64 | `manifest` (0 if root) |
| `partition_key` | Utf8 | `checksum` |
| `checksum_kind` | Utf8 | `checksum` (`"merkle"` / `"homomorphic"`) |
| `checksum_bytes` | Binary | `checksum` (32 bytes) |
| `file_path` | Utf8 | `data` |
| `file_action` | Utf8 | `data` (`"add"` / `"remove"`) |
| `file_size` | Int64 | `data` (0 for remove markers) |
| `file_blake3` | Binary | `data` (32 bytes; zero for remove) |
| `chunk_count` | Int64 | `data` (0 for remove markers) |
| `chunk_id` | Int64 | `data` (0..chunk_count-1; 0 for remove) |
| `chunk_data` | Binary | `data` (<= 16 MB; empty for remove) |
| `chunk_blake3` | Binary | `data` (32 bytes; zero for remove) |

In-memory representation makes the union shape unrepresentable-when-
invalid via an enum:

```rust
struct RemoteRow {
    txn_seq: i64,
    ts_micros: i64,
    body: RowBody,
}

enum RowBody {
    Manifest {
        commit_kind: CommitKind,
        parent_seq: i64,
    },
    Checksum {
        partition_key: String,
        checksum_kind: ChecksumKind,
        checksum_bytes: [u8; BLAKE3_LEN],
    },
    DataAdd {
        file_path: String,
        file_size: i64,
        file_blake3: [u8; BLAKE3_LEN],
        chunk_count: i64,
        chunk_id: i64,
        chunk_data: Vec<u8>,
        chunk_blake3: [u8; BLAKE3_LEN],
    },
    DataRemove {
        file_path: String,
    },
}
```

Serialization to Arrow `RecordBatch`: match on `body`, populate that
variant's columns, null the rest.  Deserialization from Arrow: read
`partition_kind` (and `file_action` for data rows), dispatch to the
variant constructor, validate that expected columns are non-null and
irrelevant ones are null.  An unexpected combination is a loud
`RemoteError::Schema` -- it would indicate a foreign writer.

#### 2.5.2 Standard queries

| Operation | Plan |
|---|---|
| List bundles | `WHERE partition_kind = 'manifest' ORDER BY txn_seq` |
| Latest seq | `MAX(txn_seq) WHERE partition_kind = 'manifest'` |
| Verify bundle N's checksums | `WHERE partition_kind = 'checksum' AND txn_seq = N` |
| List files in bundle N | `WHERE partition_kind = 'data' AND txn_seq = N AND chunk_id = 0`, project away `chunk_data` |
| Pull a single file | `WHERE partition_kind = 'data' AND txn_seq = N AND file_path = X ORDER BY chunk_id` |
| Push | one Delta commit: 1 manifest + N checksum + C data rows across three partitions atomically |
| Retention | `DELETE WHERE txn_seq < horizon`, then vacuum |

#### 2.5.3 Push

For each new locally-committed bundle (Write or Compact):

1. Read the source's Delta data files added by this `txn_seq`.
2. Stream each file in 16 MB chunks.  For each chunk: BLAKE3-hash the
   chunk, accumulate a file-level BLAKE3, emit one `DataAdd` row.
3. (Compact only) For each removed file: emit one `DataRemove` row,
   binary fields empty.
4. Snapshot partition_checksums from the local control table's
   `DataCommitted` record for this `txn_seq` -> emit N `Checksum`
   rows.
5. Emit one `Manifest` row with `commit_kind`, `parent_seq`,
   `ts_micros`.
6. Single Delta commit on the remote inserting all rows together.
7. Update source steward's `last_pushed_seq:<remote_url>` setting.

If step 6 fails, no rows appear on the remote; retry pushes the same
content (idempotent, because the bundle's content is deterministic
from `txn_seq`).  Local control-table records
`PostPushPending` -> `PostPushStarted` -> `PostPushCompleted` /
`PostPushFailed` make resumption deterministic across crashes.

`last_pushed_seq:<remote_url>` is keyed by remote URL so a single
source can push to multiple remotes (e.g., primary R2 + offsite
archive) and track each independently.

#### 2.5.4 Pull

Consumer walks bundles in `txn_seq` order from `last_pulled_seq + 1`
to the remote's MAX manifest seq.  For each bundle, dispatch on
`commit_kind`:

- **Write bundle**: for each `DataAdd` row, reassemble chunks
  (verify per-chunk BLAKE3 incrementally, then file-level BLAKE3 at
  end) -> apply to local Delta.  Recompute affected partition
  checksums; compare to bundle's checksum rows.  Mismatch -> abort.
- **Compact bundle, mirror mode** (default): apply `DataAdd` and
  `DataRemove` actions via `CommitBuilder` on the local Delta table.
  Recompute checksums; compare.  Mismatch -> abort.
- **Compact bundle, independent mode** (opt-in via `pull_mode`
  setting): skip data rows; read only checksum rows.  Recompute local
  checksums; compare.  Advance `last_pulled_seq`.  Consumer keeps its
  own physical layout and is responsible for its own compaction.
- **Compact bundle, restart from compact**: when consumer's
  `last_pulled_seq` is below the retention horizon AND this is the
  oldest available compact bundle.  Apply `DataAdd` rows wholesale
  (no preceding state to merge with), set
  `last_pulled_seq = txn_seq`.  Used for fresh restore or when
  retention pruned earlier bundles.

#### 2.5.5 Retention

`Remote::maintain` with `vacuum_pre_compaction = true`:

1. Find the last N compact bundles (default N=2 from
   `remote_compaction_retention` setting).
2. Horizon = oldest of those N's `txn_seq`.
3. `DELETE FROM remote WHERE txn_seq < horizon` -- single Delta
   commit, atomically removes manifest + checksum + data rows for all
   older bundles.
4. delta-rs `vacuum` prunes the now-unreferenced parquet row groups.
5. Refuse if N < 1 or fewer than N compact bundles exist (would
   leave no restart point).

Once a consumer falls below the horizon, its next pull returns an
explicit error directing the operator to run `restore_reinit` (wipe
local + bootstrap from oldest available compact bundle, the
restart-from-compact path above).

#### 2.5.6 Verification

`verify_local`: re-derive each partition's checksum from current data
and compare against the `partition_checksums` recorded on the latest
`DataCommitted` record.  Implemented and tested.

`verify_against_remote`: for each `partition_key`, fetch latest
bundle's checksum row from the remote
(`WHERE partition_kind = 'checksum' AND partition_key = X ORDER BY txn_seq DESC LIMIT 1`),
compare to local recomputation.  To find the divergence point, walk
back through manifest rows in descending `txn_seq` order; the first
bundle whose checksums all match local recomputation is the boundary.
Planned, not yet implemented.

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

### 2.7 Bounded growth: the headline correctness property

Every component must reach a bounded steady-state on disk under
periodic maintenance.  No ever-increasing parquet counts, no
ever-increasing data sizes, no commit log that grows without
limit.  This is the most important property of the design.

Three components, three maintenance loops:

| Component | Compact | Vacuum | API |
|---|---|---|---|
| Source data store | merges small parquets into bigger ones | reclaims tombstoned parquet files | `Steward::compact()` + `Steward::vacuum()` |
| Source control table | merges small parquets (audit history is unchanged) | reclaims tombstoned parquet files | `Steward::compact_control()` + `Steward::vacuum_control()` |
| Remote | DELETE rows below retention horizon | reclaims tombstoned parquets | `Remote::maintain(MaintainOptions { vacuum_after: true, .. })` |

Per-component invariants verified by tests in
`sandbox-tests/tests/bounded.rs`:

- The on-disk size of each component reaches steady state when its
  maintenance loop runs periodically (late-cycle size at most 4-5x
  the early-cycle size, typically much less).
- Without maintenance, size grows roughly linearly with cycle count
  (>4x ratio over 25 cycles).
- The active parquet file count stays bounded under maintenance
  (under 30 files for runs that would accumulate 60+ without).
- An end-to-end long run with all maintenance loops applied keeps
  source, remote, and consumer all bounded simultaneously, while
  the consumer remains correct (every key returns its expected
  value at every cycle).

The ordering constraint between push and vacuum (§2.6 rule #4) is
the critical correctness rule that makes this work: vacuuming a
file before push has captured it would lose data.  Tests enforce
"push immediately after every commit, vacuum only after pushes
have captured the data."

### 2.8 Mapping back to duckpond (after the prototype proves out)

What flows back:

- The three-partition (`manifest` / `checksum` / `data`) remote
  schema: replaces or augments duckpond's `ChunkedFileRecord`
  schema in `crates/remote/src/schema.rs`.  The chunked-row idea is
  already there in duckpond; the sandbox refines it with explicit
  `partition_kind` separation, atomic-Delta-commit push, and
  in-Delta retention.
- Chunk size: sandbox uses a 16 MB constant; duckpond's existing
  range is 4-64 MB.  Sandbox skips BLAKE3 outboard data (per-chunk
  hash + per-file root hash only); duckpond keeps outboard for
  cryptographically-verifiable chunk-streaming.  Adding
  `chunk_outboard: Binary` is a one-column extension if needed.
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
Store::actions_at_version(version) -> (Vec<AddPath>, Vec<RemovePath>)  // Add/Remove for one Delta commit
Store::commit_actions(actions, op) -> i64                              // explicit Add/Remove commit (used by remote-pull)
Store::vacuum() -> usize                                               // delta-rs vacuum on the data store
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
Steward::vacuum() -> usize                               // delta-rs vacuum on the data store
Steward::compact_control() -> (u64, u64)                 // optimize on the control table
Steward::vacuum_control() -> usize                       // delta-rs vacuum on the control table

Steward::log(limit) -> Vec<ControlRecord>
Steward::last_write_seq() / last_committed_seq()
Steward::partition_checksums_at(txn_seq) -> Option<PartitionChecksums>
Steward::data_delta_version_at(txn_seq) -> Option<i64>
Steward::incomplete_transactions() -> Vec<ControlRecord>

Steward::store_id() -> Uuid

// remote-push helpers
Steward::data_committed_record(txn_seq) -> Option<ControlRecord>
Steward::actions_at_version(version) -> (Vec<AddPath>, Vec<RemovePath>)
Steward::read_data_file(rel_path) -> Vec<u8>
Steward::record_post_push_pending/completed/failed(...)

// remote-pull helper
Steward::apply_pulled_bundle(txn_seq, commit_kind, parent_seq, adds, removes, partition_checksums)

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

#### 3.2.3 `sandbox-remote`

Public API:

```rust
Remote::create(path, store_id) / Remote::open(path)
Remote::store_id() -> Uuid
Remote::push(&mut self, &mut Steward, txn_seq) -> ()       // remote-push
Remote::pull(&self, &mut Steward) -> PullReport            // remote-pull (mirror mode)
Remote::maintain(&mut self, MaintainOptions) -> MaintainReport  // remote-retention
Remote::restart_from_compact(&self, consumer_path) -> Steward   // BehindRetention recovery
Remote::list_bundles() -> Vec<BundleHeader>
Remote::latest_seq() / oldest_available_seq() -> Option<i64>

verify_against_remote(&Remote, &Steward) -> RemoteVerifyReport  // verify-cmd

PullReport      { bundles_applied: Vec<BundleHeader>, last_pulled_seq: i64 }
MaintainOptions { keep_compact_bundles: usize (>=1), vacuum_after: bool }
MaintainReport  { horizon: i64, rows_deleted: i64, files_vacuumed: usize }
RemoteVerifyReport { ok: bool, remote_latest_seq: Option<i64>,
                     mismatches: Vec<RemoteVerifyMismatch>,
                     divergence_boundary: Option<i64> }
```

The `sandbox-tests` crate is still a smoke-only placeholder.

### 3.3 Test inventory

218 tests total across all sandbox crates.  All passing, all under
both checksum strategies where applicable.

| Crate / file | Count | What it covers |
|---|---|---|
| store/src/checksum/mod.rs (unit) | 14 | trait determinism, layout independence, empty-input stability, change sensitivity (run against Merkle and Homomorphic), Checksum hex format, kind serialization, array_from_slice |
| store/src/checksum/merkle.rs (unit) | 4 | empty-tree distinct from single-leaf, duplicate-key behavior, odd-count tree pairing, output is 32 bytes |
| store/src/checksum/homomorphic.rs (unit) | 6 | u256 add/sub roundtrip, carry, borrow, commutativity, order independence, output is 32 bytes |
| store/src/schema.rs (unit) | 3 | Arrow and Delta schemas agree, partition column constant, BLAKE3 length |
| store/tests/store.rs (integration) | 23 | put/get/delete roundtrip, multi-version semantics, tombstones, list filtering, partition discovery, apply_batch coalescing, empty-batch no-op, cross-partition independence, SQL metacharacter safety, actions_at_version for Write/Compact/missing, commit_actions direct test |
| store/tests/checksum_integration.rs (integration) | 11 | order-independent across stores (Merkle, Homomorphic), tombstone-neutral, cross-partition isolation, value-change detection, empty-partition stability, two strategies disagree on same state |
| steward/src/control_table.rs (unit) | 3 | Arrow vs Delta schema agreement, RecordKind serialization roundtrip, ChecksumValue serialization roundtrip |
| steward/tests/steward.rs (integration) | 29 | lifecycle records (Begin/DataCommitted/Failed/Completed), parent_seq tracking, no-op->Completed, abort, read guards write nothing, txn_seq monotonicity across all outcomes, partition checksums carry forward, orphan-Begin recovery, config_set/get/list latest-wins, persistence across re-open, log limit, verify_local under both strategies + drift detection, verify on empty pond, put-after-delete-then-verify, store_id minted/persisted/overridable/legacy-error, data_delta_version recorded for write/compact, log_with_limit returns most recent N |
| steward/tests/compact.rs (integration) | 11 | get/list unchanged after compact, real-work compact -> DataCommitted(Compact), per-partition checksum invariance, partition filter touches only that partition (verified via Store::data_files), empty-pond compact -> Completed (no-op), nonexistent-filter compact -> Completed, parent_seq points at last write, Homomorphic strategy invariance, tombstones survive compaction, interleaved write/compact/write/compact lifecycle, persistence across re-open |
| steward/tests/apply_pulled.rs (integration) | 10 | apply_pulled_bundle writes DataCommitted record on consumer, idempotent re-apply, last_write_seq advances, parent_seq chain across bundles, path validation rejects absolute / dotdot / missing-partition_key, partition_checksums recorded byte-exact, with-removes drops files from active set, read_data_file errors on missing path |
| steward/src/steward.rs (unit) | 8 | parse_partition_values: happy path, percent-encoded, absolute, dotdot, missing partition_key, multiple partition_key, HIVE null sentinel, empty path |
| remote/src/schema.rs (unit) | 5 | arrow/Delta schemas agree, partition_kind constants distinct, CHUNK_SIZE_BYTES = 16 MiB, BLAKE3_LEN matches blake3 crate, RowBody partition_kind/file_action mapping |
| remote/src/chunking.rs (unit) | 12 | empty input -> 1 empty chunk, file <= == > N*chunk_size boundaries, per-chunk and per-file BLAKE3 correctness, assemble round-trip and detection of bad-hash/out-of-order/wrong-size, chunk_file via temp file |
| remote/tests/schema.rs (integration) | 9 | round-trip for every variant, full bundle, schema rejection of unknown partition_kind / manifest missing commit_kind / data unknown action, MILESTONE: multi-partition single-commit is one Delta version |
| remote/tests/remote.rs (integration) | 11 | create+open store_id roundtrip, open of missing path errors, list/latest_seq/oldest_available_seq on empty, happy-path push, PostPushPending+Completed lifecycle, idempotent re-push, store_id mismatch error, NoSuchCommit on empty pond push, push errors on Failed / Completed / no-op-compact seqs |
| remote/tests/push.rs (integration) | 6 | compact bundle has DataAdd + DataRemove rows, partition_checksums round-trip byte-exact, data rows reassemble to original parquet bytes (file_blake3 verified), multi-bundle ordering with parent_seq chain, open errors on corrupted UUID property, post-remote-commit crash recovery writes Completed |
| remote/tests/pull.rs (integration) | 9 | happy-path round-trip, multi-bundle pull, idempotent re-pull when caught up, compact bundle pull (adds + removes both apply), per-bundle progress simulating partial-pull resumption, store_id mismatch error, behind-retention check exercised in no-gap path, DataCommitted records mirrored on consumer, state persists across consumer reopen |
| remote/tests/maintain.rs (integration) | 8 | refuses keep=0, refuses when fewer compact bundles than keep, keep=1 prunes writes leaves compact, keep=2 keeps two compacts, idempotent re-maintain, vacuum disabled reports 0 files, vacuum enabled reclaims files, INTEGRATION: consumer below new horizon gets BehindRetention on pull |
| remote/tests/restart.rs (integration) | 7 | refuses NoRestartPoint, bootstraps fresh consumer, wipes same-family pond, refuses different-family (StoreIdMismatch), refuses non-pond directory, INTEGRATION: BehindRetention -> restart recovery, idempotent re-pull after restart |
| remote/tests/verify.rs (integration) | 8 | synced consumer ok, empty remote + empty consumer vacuous ok, empty remote + nonempty consumer not ok, store_id mismatch error, tampered consumer detected on partition, INTEGRATION: divergence_boundary walk identifies seq where consumer last agreed, no agreeing bundle returns None, compute_live_checksums consistency with partition_checksums_at |
| tests/src/lib.rs | 1 | smoke (placeholder) |
| tests/tests/integration.rs | 18 | 5 base scenarios (roundtrip, two-consumer parity, long mixed sequence, multi-remote bug catch, full retention+restart loop) plus 13 systematic tests across 7 categories: scaling (5+ consumers, 30 writes), operation-order equivalence (push/pull timing), retention+restart cycles (3 rounds, restart twice, continue after restart), multi-remote (divergent retention with restart recovery, cross-remote pull idempotence), edge mixtures (abort/noop interleaved, source verify_local after every op), per-checkpoint invariants, and a 25-step scripted stress sequence with invariant checks at every meaningful checkpoint |
| tests/tests/bounded.rs | 8 | bounded-growth invariant for all three components: source data dir steady state under periodic compact+vacuum (with control test showing unbounded growth without it), parquet count bounded, remote dir steady state under periodic maintain (with the push-before-vacuum ordering rule enforced), end-to-end long run keeps source+remote+consumer all bounded while consumer remains correct, control table same property under compact_control+vacuum_control (with control test) |
| tests/tests/properties.rs | 5 | proptest sweeps over random op sequences (Write/AbortWrite/NoopWrite/Compact, 16 cases each): consumer matches source after push+pull for every key, compact preserves logical content, only DataCommitted seqs are pushable (aborts/noops rejected), pull is idempotent (second pull applies no bundles), 15-25 step random sequence keeps source verify_local valid throughout and consumer matches after final push+pull. Each case prepends a fixed seed Write so seq 1 is always pushable (works around the BehindRetention check's inability to distinguish "seq N never had a bundle" from "seq N's bundle was pruned") |
| **Total** | **223** | |

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
| remote-push | done | Steward store_id, data_delta_version, Store::actions_at_version, sandbox-remote schema + chunking + Remote::create/open/push with PostPush* lifecycle + 14 push-related tests |
| remote-pull | done | Steward::apply_pulled_bundle (idempotent via consumer DataCommitted mirror, path validation, BLAKE3 chain trust eliminates post-commit recompute) + Remote::pull mirror-mode lifecycle (per-bundle progress, retention horizon check) + 17 pull-related tests |
| remote-retention | done | Remote::maintain (DeleteBuilder predicate `txn_seq < horizon`, optional VacuumBuilder) + MaintainOptions/MaintainReport + InvalidRetention/InsufficientCompactBundles errors + 8 tests |
| restart-from-compact | done | Remote::restart_from_compact (safety wipe + apply oldest compact baseline with empty Removes + catch-up pull) + NoRestartPoint/RestartPathNotPond errors + 7 tests including BehindRetention->recovery flow |
| verify-cmd | done | verify_against_remote (compare consumer's live checksums to remote.latest_seq's recorded checksums; on mismatch walk back to find divergence_boundary) + Steward::compute_live_checksums + 8 tests |
| library-api-coverage | done | audit + 8 gap-filling tests (verify_local drift detection, log limit, apply_pulled_bundle removes path direct test, read_data_file error path, Store::commit_actions direct test, push errors for Failed/Completed/no-op-compact seqs).  Skipped (documented): compact_failure_records_failed (delta-rs cannot inject optimize failure) |
| integration-tests | done | 18 cross-component scenarios in sandbox-tests crate covering: 5 base scenarios; scaling (multi-consumer, many-write); operation-order equivalence (push timing, pull timing); retention/restart cycles; multi-remote with divergent retention; edge mixtures (abort/noop interleaved); per-checkpoint invariant assertions; 25-step deterministic stress.  Caught a real bug: push was not recording last_pushed_seq |
| property-tests | done | 5 proptest sweeps over random op sequences (Write/AbortWrite/NoopWrite/Compact, 16 cases each): consumer matches source after push+pull, compact preserves logical content, only DataCommitted seqs are pushable, pull is idempotent, long random sequence keeps source verify_local valid. Each case prepends a fixed seed Write so seq 1 is always pushable (works around BehindRetention's inability to distinguish "seq N never had a bundle" from "seq N's bundle was pruned") |
| benchmarks | pending | depends on checksum-trait (UNBLOCKED, can run any time) |
| sandbox-design-doc | pending | depends on integration-tests (UNBLOCKED), property-tests (UNBLOCKED), benchmarks |

14 of 16 done.

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

**Goal**: implement `Remote::create / open / push(steward)`.  Push
runs manually (operator-triggered); auto-push-on-commit is a separate
later todo.  After push, the remote bucket contains exactly one Delta
table whose rows ARE the bundle content (see §2.5).

**Files to write**:

- `sandbox/remote/src/error.rs`: `RemoteError` enum.
- `sandbox/remote/src/schema.rs`: Arrow + Delta schemas for the
  three-partition `manifest` / `checksum` / `data` table; the
  `RowBody` enum and converters to/from `RecordBatch`.
- `sandbox/remote/src/chunking.rs`: `CHUNK_SIZE_BYTES = 16 * 1024 * 1024`,
  streaming chunker that takes a parquet file path and yields
  `(chunk_id, chunk_count, chunk_data, chunk_blake3,
  file_blake3_running)` tuples.
- `sandbox/remote/src/remote.rs`: `Remote` type with `create` /
  `open` / `push` / `list_bundles` / `latest_seq` /
  `oldest_available_seq` / `store_id`.
- `sandbox/remote/tests/push.rs`: happy path, idempotent re-push,
  push of empty pond, partition-kind row-shape invariants, schema
  round-trip, chunking boundary cases (file exactly 16 MB, file
  smaller than 16 MB, file spanning multiple chunks).

**`store_id` storage**: `store_id` is minted at `Steward::create` as
a fresh UUIDv4 and persisted via `config_set("store_id", ...)` (or
supplied via a new `StewardOptions::store_id: Option<Uuid>` for the
restore-from-compact path).  `Steward::store_id()` exposes it.  At
`Remote::create`, the source's `store_id` is written as a Delta
table property `sandbox.store_id = <uuid>` via
`CreateBuilder::with_configuration`.  At `Remote::open`, read it
from `DeltaTable::metadata().configuration` and require non-empty;
expose via `Remote::store_id()`.  A consumer opening a remote whose
`store_id` differs from its own local `store_id` is an explicit
`RemoteError::StoreIdMismatch` (catches "wrong bucket configured").

**Tracking `last_pushed_seq`**: stored as a steward setting,
`last_pushed_seq:<remote_url>`.  One source can push to multiple
remotes (e.g., R2 + offsite archive); the URL discriminates.

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

2. **Independent-mode pull and physical layout drift.**  A consumer
   in independent mode will accumulate small parquets until it
   compacts itself.  Verify the prototype demonstrates this is
   operationally sane before recommending the mode for any real
   consumer.

3. **Crash recovery in the prototype.**  Steward records the
   lifecycle to detect incomplete transactions.  Recovery currently
   just *reports* them; it does not auto-roll-back or auto-replay.
   For the prototype this is fine; for duckpond we'd need a deliberate
   recovery story.

4. **Concurrency.**  Multiple processes against the same store
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
