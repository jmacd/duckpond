# Sandbox: Implementation Milestone 2 -- Compaction

This document is a snapshot of the sandbox prototype after the second
implementation milestone landed.  It complements `DESIGN.md` (which is
the canonical *design* reference) by recording what was actually built,
which design decisions were ratified along the way, and what remains.

> Read `DESIGN.md` first if you have not seen this prototype before.
> This document assumes that context.

---

## 1. Where we are

| | Phase 1 (foundations) | Phase 2 (this milestone) |
|---|---|---|
| Todos done | 5 | 6 |
| Test count | 73 | 84 |
| Sandbox crates with real code | 2 (`store`, `steward`) | 2 (`store`, `steward`) |
| Sandbox crates as placeholders | 2 (`remote`, `tests`) | 2 (unchanged) |
| Critical-path next item | `compact-op` | `remote-push` |

Phase 1 delivered the persistence layer: a Delta-Lake-backed key/value
store, two interchangeable per-partition checksum strategies, and the
steward's transaction lifecycle / control table / `verify_local`.

Phase 2 delivers Delta-level compaction with a strict invariant that
compaction is logically transparent.  Compaction is the precondition
for everything still pending: it produces the restart points that
`remote-push`, `remote-pull`, and `restart-from-compact` depend on.

---

## 2. Phase 2 deliverables

### 2.1 New `sandbox-store` API

```rust
Store::compact(filter: Option<&str>) -> Result<CompactMetrics>
Store::data_files() -> Result<Vec<String>>
```

`Store::compact` is a thin wrapper over delta-rs's
`optimize().with_type(Compact)`, with optional restriction to a single
`partition_key` value via `PartitionFilter`.  It returns
`CompactMetrics { num_files_added, num_files_removed }` so callers can
distinguish "Delta merged some files" from "nothing to do".

`Store::data_files` enumerates the parquet files referenced by the
current Delta version.  It exists to let tests verify that a
partition-filtered compaction touched only the requested partition's
files; production code does not need this primitive.

### 2.2 New `sandbox-steward` API

```rust
Steward::compact(filter: Option<&str>) -> Result<CommitOutcome>
```

The lifecycle has three terminal branches, each emitting a different
control-table record:

```
            +-- error from Store::compact -> Failed
Begin ------+-- metrics.is_noop()         -> Completed
            +-- otherwise (real work)     -> DataCommitted(commit_kind=Compact)
```

- **error**: catch the error, write `Failed` with the error text in
  `metadata_json.reason`, propagate the error to the caller.
  Symmetric with `WriteGuard::abort`.
- **no-op** (zero files added or removed): write `Completed`.
  `last_committed_seq` does NOT advance, because no new Delta commit
  was produced.  Covers empty pond, nonexistent filter, and
  already-optimal layout.
- **real work**: snapshot every partition's checksum after the optimize,
  assert `pre == post` (per partition AND set-equality of partition
  keys), write `DataCommitted` with `commit_kind = Compact` and the
  post-compaction snapshot.  An assertion failure surfaces as
  `StewardError::Invariant` -- it would indicate a delta-rs bug or a
  schema bug in this prototype.

The returned `CommitOutcome` always carries `commit_kind = Compact`.
Callers distinguish the no-op case via `had_data = false`.

### 2.3 Internal refactor

Both `finish_commit` (the Write path) and `compact` (the Compact path)
need a "snapshot all partitions' checksums" helper.  Phase 2 extracts
this into a private `Steward::snapshot_all_partition_checksums()` and
both call sites use it.  This keeps the carry-forward semantics
identical between Write and Compact commits.

### 2.4 Test coverage (`steward/tests/compact.rs`, 11 tests)

Each test runs against the steward's configured strategy; a few are
duplicated under both Merkle and Homomorphic.

| Test | What it pins down |
|---|---|
| `compact_does_not_change_get_or_list` | Logical equivalence: `get`/`list`/`verify_local` all unchanged after compact. |
| `compact_records_data_committed_with_compact_kind_when_files_merge` | Real-work path emits exactly one `Begin` and one `DataCommitted(Compact)`; `parent_seq` points at the prior write. |
| `compact_preserves_partition_checksums` | The recorded `partition_checksums_at(N)` after compact equals the recorded snapshot before compact. |
| `compact_with_partition_filter_only_touches_that_partition` | Setting up p1 and p2 each with 3 files, compacting only p1, verifies via `Store::data_files()` that p2's file set is byte-identical and p1's shrunk. |
| `compact_on_empty_pond_is_noop_completed` | Empty pond compact -> `Completed`, NOT `DataCommitted`; `last_committed_seq` does not advance. |
| `compact_on_nonexistent_filter_is_noop_completed` | Same no-op semantic when the filter matches no partition. |
| `compact_advances_parent_seq` | `parent_seq` on the compact's `DataCommitted` is the most recent prior commit, regardless of kind. |
| `compact_under_homomorphic_strategy` | Invariance holds under the alternative checksum. |
| `tombstones_survive_compaction` | A deleted key remains absent post-compact (`get` returns `None`, `list` excludes it, `verify_local` passes). |
| `interleaved_writes_and_compactions` | Five writes interleaved with two compacts produce the expected `parent_seq` chain across mixed `commit_kind` values. |
| `compact_persists_across_reopen` | Compact's `DataCommitted` row survives a steward reopen; `last_write_seq` and `last_committed_seq` are reloaded correctly. |

Two scenarios were intentionally omitted:

- **A defensive "checksums actually mismatch" test.**  The mismatch
  branch is unreachable without monkey-patching the Delta table
  mid-transaction, which the prototype does not expose hooks for.
  The assertion code is mechanically obvious; a test that exercised it
  would also have to manufacture the precondition by violating an
  invariant elsewhere.
- **A `compact_failure_records_failed` test.**  delta-rs does not
  expose a clean way to inject an optimize failure deterministically.
  The Failed branch is structurally symmetric with `WriteGuard::abort`,
  which IS tested.

---

## 3. Design decisions ratified in Phase 2

### 3.1 No-op compact => `Completed`, not `DataCommitted`

Originally I planned to always emit `DataCommitted(Compact)` on compact,
even when delta-rs found nothing to merge.  A rubber-duck pass surfaced
the consequence: every `DataCommitted` seq is supposed to correspond to
a real Delta commit / restart point, and `remote-push` will rely on
that correspondence to emit one bundle per `DataCommitted`.  A
`DataCommitted` row that has no matching Delta commit would
desynchronize the control-table history from the Delta history -- not
visibly broken in Phase 2, but a latent landmine for Phase 3.

The fix: detect "did delta-rs actually commit a new version?" via
`CompactMetrics::is_noop()` (`num_files_added == 0 && num_files_removed == 0`)
and emit `Completed` in that case.  `last_committed_seq` is left
unchanged.  The control table now invariantly maintains: every
`DataCommitted` row corresponds 1-to-1 with an underlying Delta commit.

### 3.2 Partition-set equality, not just per-partition checksum equality

The original assertion was per-partition: "for every `p` in `pre`,
`post[p]` exists and equals `pre[p]`".  This catches value drift but
misses two failure modes:

- A partition appearing post-compaction that did not exist pre-compaction.
- A partition disappearing during compaction.

The strengthened assertion checks `pre.keys() == post.keys()` first
and then per-partition equality.  It is strictly stronger and the same
code complexity.

### 3.3 Error handling symmetric with `WriteGuard::abort`

Phase 2 originally would have left an orphaned `Begin` row when
`Store::compact` returned an error -- the operator would only see it
via `incomplete_transactions()`, indistinguishable from a crash.

The corrected design catches the error, writes `Failed` with the
error reason, then propagates.  This matches `WriteGuard::abort` and
makes "a normal returned error" distinguishable from "the process
crashed mid-transaction".

### 3.4 `had_data` reused for "transaction was not a no-op"

For Write commits, `had_data` already meant "this transaction wrote
data" (which is "data_committed != completed").  Compact reuses the
same field with the same semantic: `had_data = true` iff
`DataCommitted` was recorded.  No new field, no overloading.

### 3.5 Partition filter as `Option<&str>`, not a builder

The DESIGN.md called this "an optional partition filter", and the
delta-rs API takes `&[PartitionFilter]`.  An `Option<&str>` argument
is a one-line wrapper around the delta-rs API that exposes only what
the prototype needs (single-value equality on the partition column).
A builder or options struct would be premature given the current
single-knob surface; we can grow into one if `compact-op` ever needs
more configuration in production.

---

## 4. CI gate state

All three gates clean as of this milestone:

```
cd duckpond/sandbox
cargo fmt --all -- --check                        # 0 issues
cargo clippy --workspace --all-features -- -D warnings  # 0 issues
cargo test --workspace                            # 84/84 passing
```

The duckpond workspace at `duckpond/Cargo.toml` does not include the
sandbox crates.  Cargo commands in `duckpond/` ignore the sandbox.

---

## 5. What Phase 3 unblocks

`compact-op` was the bottleneck for the entire remote-sync chain.  Now
unblocked:

- **`remote-push`** (next critical-path item).  Each `DataCommitted`
  row -- Write or Compact -- becomes a bundle on the remote.  Compact
  bundles carry their merged parquets and serve as restart points.
- **`remote-pull`**.  Dispatches on `commit_kind`: Write bundles
  replay incrementally; Compact bundles in mirror mode replay the
  merged parquets; Compact bundles in independent mode advance the
  pulled-seq without downloading data; Compact bundles below the
  consumer's last-pulled seq become the restart point.
- **`restart-from-compact`**.  Falls out naturally once Compact bundles
  are pushable -- the recovery path bootstraps from the oldest
  available Compact bundle.
- **`remote-retention`**.  Walks back to the Nth oldest Compact bundle
  and prunes everything before it.

`benchmarks` remains unblocked-by-checksum-trait and could run in
parallel with any of the above.

---

## 6. Suggested next session

1. Pick `remote-push` (DESIGN.md sec 5.2) and follow the same loop:
   plan, rubber-duck the plan, implement, test, run all three CI
   gates locally, commit, update DESIGN.md status table.
2. Or pick `benchmarks` (DESIGN.md sec 4) if performance numbers for
   the two checksum strategies would help prioritize the default
   choice for the eventual map-back to duckpond.

The control-table schema already has reserved record kinds
(`PostPushPending`, `PostPushStarted`, `PostPushCompleted`,
`PostPushFailed`) for `remote-push`; no schema change should be needed
to land the next milestone.

---

## 7. Files changed in Phase 2

```
sandbox/DESIGN.md                    | 112 +++++++++++++++--------
sandbox/steward/src/control_table.rs |   4 +-   (cargo fmt)
sandbox/steward/src/steward.rs       | 203 +++++++++++++++++++++++++++++-
sandbox/steward/tests/compact.rs     | 425 +++++++++++++++++++++++++++++++++++++   (NEW)
sandbox/store/src/lib.rs             |   2 +-
sandbox/store/src/store.rs           |  74 +++++++++++++
6 files changed, 755 insertions(+), 65 deletions(-)
```

Single commit: `sandbox: implement compact-op (todo 6/16)`
(`a76d7e76`).
