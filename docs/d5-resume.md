# D5 Resumption Notes (Remote Redesign)

> **Status as of 2026-05-25**: D4 is **complete and ready for PR**.
> **D5.1 is complete** (commit `1137d81d` on `jmacd/52`): tlogfs
> data tables are now partitioned by `(pond_id, part_id)`; on-disk
> layout is `data/pond_id=<uuid>/part_id=<uuid>/part-...parquet`;
> legacy single-`part_id` ponds are refused at open time with an
> error directing the operator to re-init from remote (fresh-start
> migration policy, no in-place migration tool).
> **D5.2 is complete** (commit `2d03c946`): pond identity is
> canonically owned by the data Delta table's bootstrap row;
> `OpLogPersistence::peek_pond_id` reads it from Delta partition
> values without a DataFusion session; `Ship::create_infrastructure`
> prefers the data-table value over the control-table cache and
> auto-heals the cache on disagreement.  Restoration scaffolds (empty
> data table) still fall back to the control cache.
> **D5.3 is complete** (commit pending on top of D5.2):
> `RemoteSteward::actions_at_version` now takes a `pond_id` parameter
> and implementations filter Add/Remove file actions by partition
> value (duckpond) or path prefix (sandbox sync_store).  Today a no-op
> (single-pond commits), but establishes the per-pond bundle contract
> required for D5.7 cross-pond import.
> **D5.4 is complete** (commit pending on top of D5.3):
> `Remote::bootstrap_consumer<S: RemoteSteward>` is the new public,
> generic first-pull API that handles both the compact-baseline path
> (delegates to `restart_pond_from_compact`) and the writes-only path
> (seeds `last_pulled_seq=1` then pulls).  `Ship::create_replica(path,
> uuid::Uuid)` is the duckpond constructor for a fresh replica with a
> caller-supplied `pond_id` (synthesizes default birth metadata).
> The sync_steward `Remote::restart_from_compact(&Path)` is refactored
> to share the apply-baseline/set-seq/pull code via
> `restart_pond_from_compact`.  Manual workarounds in
> `crates/cmd/tests/test_remote_cli.rs` removed in favor of the new
> APIs; remote attachments are now actually replicated to dst on
> first-pull.
> **D5.5 is complete** (commit pending on top of D5.4):
> `ShipRemoteSteward::compute_live_checksums` now reads live OplogEntry
> rows for the given `pond_id` through a fresh DataFusion
> `SessionContext` over a clone of the data table's current snapshot
> (works whether or not a transaction is active), groups by `part_id`,
> hashes each row as `blake3(serde_json::to_vec(row))` keyed on
> `"<node_id>/<version>"`, and folds the per-partition leaves through
> the existing `Merkle` strategy.  This gives the verify path
> end-to-end coverage of the LIVE side for duckpond's row schema.  The
> RECORDED snapshot in `record_data_committed` for native writes is
> still `HashMap::new()` (D5.6/D5.7 scope) so a verify pass after a
> native write today would mis-fire on drift; D5.5 tests deliberately
> exercise `compute_live_checksums` directly.
> This document is a working scratchpad for the next session; it is
> intentionally checked in as a `docs/` file so it survives session
> boundaries.  Delete (or move to `docs/archive/`) once D5 lands.

---

## Where we are right now

### Branch and recent commits

Working branch: **`jmacd/52`** (formerly `jmacd/sandbox_sync`).

The D4 series is **8 sub-commits**, `abf0d5c0..9f3944d6`:

```
9f3944d6 D4.8: auto-initialize remote Delta table in `pond remote add`
a9bbf6eb D4.7: documentation update for D4 remote-redesign
5cd1609b D4.6: testsuite cleanup for legacy remote-factory removal
e62083d8 D4.5: hard-delete legacy crates/remote + import-partition scaffolding
d6766353 D4.4: post-commit auto-push for /sys/remotes/*
bfc77e32 D4.3: CLI verbs `pond remote add/remove/list`, `pond push`, `pond pull`
cb41d8b2 D4.2: ShipRemoteSteward adapter + integration tests
abf0d5c0 D4 foundation: RemoteSteward trait + duckpond ControlTable adapter
```

### Presubmits are clean

As of `9f3944d6`:

```bash
cargo fmt --all -- --check                                # clean
cargo clippy --workspace --all-features -- -D warnings    # clean
cargo test --workspace                                    # all green
```

Test counts (last full workspace run):

- `cmd` integration tests: `test_remote_cli.rs` has 9 tests, all pass.
- `tlogfs` integration tests: 195, 133, 130, etc.
- Total workspace: 1400+ unit tests + integration tests, 0 failures.

### What D4 delivered

D3 was folded into D4 (no point exposing transitional chunked-parquet
bundles through the new CLI when the sync_remote Delta-bundle format
was already ready in the sandbox).  D4 replaced the legacy `crates/remote`
chunked-parquet factory with the `sync_remote::Remote` pipeline driven
by `ShipRemoteSteward`.  Result:

| Old surface                                       | New surface                            |
|---------------------------------------------------|----------------------------------------|
| `pond mknod remote /system/run/N-backup`          | `pond remote add <name> <url>`         |
| `pond run N-backup push`                          | `pond push [<name>]`                   |
| `pond run N-backup pull`                          | `pond pull [<name>]`                   |
| `pond run N-backup show`                          | (no equivalent yet; D5)                |
| `pond run N-backup verify`                        | (no equivalent yet; D5)                |
| `pond init --from-backup` / `pond sync`           | gone — replaced by add+push or add+pull|
| Post-commit auto-execution via `/system/run/*`    | post-commit auto-push for `/sys/remotes/*` |

The `remote` factory is gone.  `crates/remote/` was hard-deleted in D4.5
(5092 LOC).  Cross-pond import was scaffolded but never reachable from
the CLI; the scaffolding (`ProviderContext::import_partitions`,
`OpLogPersistence::pending_import_metadata`, etc.) was also deleted in
D4.5.  Cross-pond import returns in D5 via row-level `pond_id`
partitioning.

The `pond remote add` verb auto-initializes the destination Delta table
(D4.8), so `pond init` → `pond remote add` → `pond push` is a complete
operator sequence with no manual `Remote::create_at_url` shim.

### Documentation state

- `docs/remote-redesign.md` — **current**, has the full D4
  implementation-notes section at line ~480 documenting all six D4
  sub-commits plus carry-forwards.
- `docs/cli-reference.md` — **current** for D4 CLI surface.  Quick
  Reference, mode-comparison table, `pond mknod`/`pond run`/`pond
  remote`/`pond push`/`pond pull` sections, Filesystem Conventions
  section, and the deprecated `### remote` factory section all
  reflect the post-D4 reality.
- `docs/operator-guide.md` — has a top-of-file D4 warning banner only;
  body is otherwise pre-D4.  Full rewrite is D6 scope.
- `docs/d4-resumption.md` — historical; was the original D4 plan.
- **This file** (`docs/d5-resume.md`) — the new working doc.

Docs that still reference legacy patterns and are explicitly **deferred
to D6** per the redesign plan:
- `docs/operator-interface-plan.md`
- `docs/efficiency-priorities.md`
- `docs/cli-command-structure.md`
- `docs/design-cross-pond-import.md`
- `docs/design-security-and-credentials.md`
- `docs/delta-cleanup-synchronization.md`
- `docs/d4-resumption.md` (will become archive)
- `docs/remote-bandwidth-bug.md`
- Several files under `docs/archive/`

Do **not** rewrite these as part of D5 — they go away in D6 when the
operator-guide is rewritten.

---

## What is D5?

From `docs/remote-redesign.md` § D5:

> **Goal**: make the data Delta table pond-pure at file granularity;
> move pond identity to bootstrap row.

D5 has two interleaved threads:

1. **Schema/storage change**: promote `pond_id` from a regular
   OplogEntry column to a Delta table **partition column**.  This
   enables file-granularity push filters (`Remote::push` reads files
   for own `pond_id` only via partition pruning) and `O(1)` drop of
   foreign pond data on `restart_pond_from_compact`.

2. **Cross-pond import end-to-end**: with row-level `pond_id`
   partitioning in place, expose `pond remote attach <url> --mount
   <path>` (or whatever the final UX is) so a consumer can mirror or
   import a foreign pond's content into its own filesystem.

In sandbox terms, D5 is the duckpond port of sandbox commit `c40737c2`
(A1 — pond_id partitioning) and the cross-pond test coverage from
`149af878` (A3 — push filter + cross-pond pull).

### Why D5 matters operationally

Right now (post-D4):

- The data Delta table is partitioned by `part_id` only.
- `pond_id` is a regular OplogEntry column (see
  `crates/tlogfs/src/schema.rs:284`).
- `ShipRemoteSteward::drop_pond_data` (in
  `crates/steward/src/remote_adapter.rs:477-499`) issues a
  `DELETE WHERE pond_id = '<uuid>'` which **rewrites partitions** —
  expensive at scale, and requires reading every file in every
  partition.
- `Remote::push` currently filters at row level (post-read).  A
  partition column would let DataFusion prune at scan time.
- `compute_live_checksums` (same file, line 501-511) is **stubbed
  out** with an `Err(...)` returning "deferred to D3 (pond verify)";
  fixing it cleanly requires the `pond_id` partition to make per-pond
  checksum streams cheap to compute.

### What the cross-pond UX should look like (working theory)

```bash
# On the consumer (single pond), mount a foreign pond's content as a
# subtree of the consumer's filesystem:
pond remote add upstream s3://upstream-bucket --mode pull --mount /imports/upstream

# Triggers a real first-pull bootstrap via `Remote::restart_from_compact`
pond pull upstream

# Result: the consumer can now read /imports/upstream/* as if those
# files were native, but the OplogEntry rows under that subtree carry
# the FOREIGN pond's pond_id.  Drop without disturbing local data:
pond remote remove upstream    # implicitly calls restart_pond_from_compact
                                # with --drop to release the foreign rows
```

This is intentionally vague; the actual UX is a D5 design discussion.
The sandbox prototype's `cross_pond.rs` integration test is the model
to mirror.

---

## D5 Phase Plan (proposed)

This is a starting point, not a final plan.  Adjust during a planning
discussion with the user before starting.

### D5.1 — Partition migration in tlogfs (schema change) — **DONE**

**Status**: completed 2026-05-25 (commit on top of `jmacd/52`).

**What changed**:

- `crates/tlogfs/src/persistence.rs`: both `create_empty` and
  `open_or_create` paths now call
  `with_partition_columns(["pond_id", "part_id"])` and drop
  `pond_id` from `delta.dataSkippingStatsColumns` (partition columns
  don't need stats).
- The manual `CommitBuilder + Action::Add + ArrowWriter` parquet
  writer now groups rows by `(pond_id, part_id)` tuples, strips both
  columns before writing the parquet file, emits paths of the form
  `pond_id=<uuid>/part_id=<uuid>/part-00000-<uuid>-c000.snappy.parquet`,
  and populates `partition_values` for both columns.
  `DeltaOperation::Write { partition_by, .. }` lists both.
- `ExternalAddAction` (Phase 2 of bulk commit) gained a `pond_id`
  field; not currently called from production but kept ready for
  D5.7 cross-pond import.
- `crates/tlogfs/src/error.rs`: new `LegacyPartitionLayout { path,
  found }` variant.  After `deltalake::open_table()` succeeds we
  fetch `snapshot()?.metadata().partition_columns()` and refuse to
  open if it isn't exactly `["pond_id", "part_id"]`.  Error message
  directs the operator to `pond init` a fresh directory and restore
  via `pond remote add` + `pond pull`.
- `crates/steward/src/remote_adapter.rs`: renamed
  `parse_part_id_path` -> `parse_pond_part_path`, returns
  `(pond_id, part_id)`, validates a 3+-segment path and rejects
  duplicate `pond_id=` or `part_id=` segments.  `validate_local_data_path`
  now also asserts the parsed `pond_id` equals `self.store_id()`
  ("no foreign data leaks on push" guard, tightened until D5.7
  enables cross-pond push).  `apply_pulled_bundle` asserts the
  parsed `pond_id` matches the bundle's `pond_id` and populates both
  columns in `partition_values`.  6 old parser tests replaced with
  10 new ones exercising both prefixes and edge cases.

**Migration policy chosen**: fresh-start.  No in-place migration
tool; legacy ponds are refused at open time.  In practice the local
pond is rebuilt by `pond init` + `pond remote add <existing-remote>`
+ `pond pull` once D5 wires that up; for pre-D5 testing operators
simply re-init.

**Validation**:

- `cargo fmt --all -- --check` clean
- `cargo clippy --workspace --all-features -- -D warnings` clean
- `cargo test --workspace`: all green (9 remote-CLI integration
  tests + full unit suite, zero failures across the workspace)
- Manual smoke test confirms on-disk layout
  (`./data/pond_id=<uuid>/part_id=<uuid>/part-...parquet`) and that
  the legacy guard refuses to open a hand-mocked `partitionColumns =
  ["part_id"]` table with the expected error.

**SQL touch-up deferred**: queries in `persistence.rs` (lines 1178,
2976, 3381) still filter only by `part_id` without scoping to
`pond_id`.  Harmless today (one pond_id per local pond, so partition
pruning over a single pond_id is transparent), but must be scoped
by pond_id once D5.7 imports foreign rows.  Tracked as a D5.7
checklist item below.

### D5.1 — (original plan, kept for reference)

**File**: `crates/tlogfs/src/persistence.rs`

Two `create` paths (lines ~262-291 and ~339-371) both write the
configuration property:

```rust
.with_configuration([(
    "delta.checkpoint.writeStatsAsStruct".to_string(),
    Some("true".to_string()),
)])
.with_partition_columns(["part_id"])
```

Change to:

```rust
.with_partition_columns(["pond_id", "part_id"])
```

Same in `open_or_create` paths.  The column ordering matters
(outer-most first): `pond_id` first lets pond-scoped filters prune
entire pond directories before descending into `part_id`.

**Migration policy**: per `docs/remote-redesign.md` § D5, "schema
migration on existing local ponds: probably 'create fresh from data'
rather than in-place, given no compat constraint."  In practice:

- For a single-pond local pond (the only case today, pre-cross-pond),
  the new partition layout is `pond_id=<the-one-pond>/part_id=<uuid>/`.
  Old ponds with no `pond_id` partition just need a one-time
  rewrite.  Provide a one-shot CLI `pond migrate-partitions` or
  similar; refuse to open un-migrated ponds with a clear error.
- For testing, just `pond init` fresh ponds — the migration tool is
  for production operators, not the test suite.

**Touch points outside `persistence.rs`**:

- Any code that constructs paths under `part_id=<uuid>/` directly
  (search for `part_id=` in `crates/tlogfs/`, `crates/steward/`,
  `crates/sync-remote/`).
- The bootstrap that creates the root directory partition needs to
  know the pond_id before the first write (it already does — see
  `Ship::create_pond_with_metadata` calling
  `OpLogPersistence::create_with_metadata`).
- Anywhere that lists partitions (`list_part_ids()` etc.) needs to
  return either `(pond_id, part_id)` tuples or a flat list filtered
  by the local pond_id.

### D5.2 — Bootstrap row as canonical pond identity — **DONE**

**Status**: completed 2026-05-25 (commit pending on top of D5.1).

Per the redesign plan:

> Pond identity: move from control-table Setting to root directory's
> OplogEntry's `pond_id` column.  Bootstrap row becomes the canonical
> source.  Adjust `Steward::open` to read `pond_id` from the root
> directory's OplogEntry, not from the control-table Setting.

**What changed**:

- `crates/tlogfs/src/persistence.rs`: new
  `OpLogPersistence::peek_pond_id(path)` static method.  Opens the
  Delta table read-only via `deltalake::open_table`, reads
  `snapshot()?.add_actions_table(true)` (no DataFusion session
  required), scans the flattened `partition.pond_id` String column,
  collects unique non-null values via a `BTreeSet`.  Returns
  `Ok(None)` if the path doesn't exist or the table has zero rows
  (restoration scaffold), `Ok(Some(uuid_str))` for the unique
  pond_id, or `Err(Internal)` if more than one is found (deferred
  to D5.7 cross-pond import, which will refine this to identify
  the *local* pond_id specifically).
- `crates/steward/src/ship.rs` `create_infrastructure`: the
  `create_new=false` branch now calls `peek_pond_id` first, then
  opens the control table, and on disagreement (a) logs a
  `log::warn!`, (b) keeps the data-table value, and (c) auto-heals
  the control cache by rebuilding `PondMetadata` with the data-table
  `pond_id` plus the existing `birth_*` fields and calling
  `ct.set_pond_metadata(&healed)`.  If `peek_pond_id` returns `None`
  (restoration scaffold awaiting bundles), the control cache is
  used as a fallback.  `create_new=true` is unchanged - fresh ponds
  write the same pond_id to both tables synchronously.

**Why birth fields stay in control**: timestamp/hostname/username are
informational only; if the control table is lost, those are
acceptably lost.  Only `pond_id` is replicated property that must
survive control-table loss.

**Tests added**:

- `crates/tlogfs/src/tests/mod.rs`: 3 `peek_pond_id_*` unit tests:
  missing path returns `None`; freshly-created empty Delta table
  returns `None`; a real pond returns the local pond_id matching the
  control-table value.
- `crates/steward/src/ship.rs`
  `ship_open_prefers_data_pond_id_over_control_cache`: end-to-end
  test that creates a pond, records the canonical pond_id, tampers
  the control table with a different pond_id via
  `set_pond_metadata`, reopens, and asserts the ship sees the
  canonical data-table value.  A third open verifies the heal was
  persisted.

**Validation**:

- `cargo fmt --all -- --check` clean
- `cargo clippy --workspace --all-features -- -D warnings` clean
- `cargo test --workspace`: all green

**Sandbox divergence**: `crates/sync-steward/src/steward.rs:185-219`
still uses the control-table Setting for store_id (matching
duckpond's pre-D5.2 behavior).  Duckpond's D5.2 intentionally goes
beyond the sandbox prototype because of remote replication: a
duckpond replica is created by `pond init` + `pond remote add` +
`pond pull`, during which the pond_id needs to come from the
remote-shipped data (control table is locally minted).  Sandbox
sync_steward has no remote/replication path so it never needs this.

**Deferred** (out of D5.2 scope):

- Full operator workflow for control-table loss recovery
  ("rebuild control from data alone").  Today's `pond remote add` +
  `pond pull` path indirectly covers this for replicas; a dedicated
  recovery command can come later if needed.
- The `peek_pond_id` "multiple pond_ids" error case lands fully
  with D5.7 cross-pond import.

### D5.2 — (original plan, kept for reference)

### D5.3 — Filter Remote::push by pond_id partition — **DONE**

**Status**: completed 2026-05-25 (commit pending on top of D5.2).

**What changed**:

- `crates/sync-remote/src/steward_trait.rs`:
  `RemoteSteward::actions_at_version` signature gained a
  `pond_id: Uuid` parameter.  Doc-comment establishes the contract:
  implementations MUST return only Add/Remove file actions that
  belong to the requested pond.  Default impl (for
  `sync_steward::Steward`) delegates to the public
  `Steward::actions_at_version(version)` and filters by path prefix
  `pond_id={uuid}/` (sync_store's native layout).
- `crates/steward/src/remote_adapter.rs`:
  `ShipRemoteSteward::actions_at_version` re-reads the Delta commit
  log entry, then filters Add actions by
  `partition_values["pond_id"] == pond_id` (semantic, partition-aware
  source of truth).  Remove actions fall back to a path-prefix check
  when `partition_values` is absent (older Delta protocol versions).
- `crates/sync-remote/src/remote.rs` (`Remote::push`):
  now calls `steward.actions_at_version(steward.store_id(), version)`.
  The `validate_local_data_path` defense-in-depth loop is unchanged
  (its comment was updated to reflect that filtering moved upstream).

**Why this is forward-looking, not a behavior change today**: every
`pond run` is one transaction = one Delta commit, and
`apply_pulled_bundle` is a separate transaction.  So today no
single commit can hold rows for multiple pond_ids -- the filter is
a no-op.  But once D5.7 cross-pond import lands and commits can be
multi-pond, push must enumerate only the local pond's files; this
patch establishes that contract at the trait layer so D5.7 doesn't
have to retroactively refactor `Remote::push`.

**Tests added**:

- `crates/steward/tests/remote_adapter_test.rs`:
  `ship_remote_actions_at_version_filters_by_pond_id` creates a
  pond, commits one write, looks up the corresponding Delta version
  via `data_committed_record`, then asserts that
  `actions_at_version(local_pond_id, v)` returns the file (with the
  expected `pond_id=<uuid>/` prefix) while
  `actions_at_version(foreign_pond_id, v)` returns empty
  adds/removes -- the D5.3 invariant.

**Validation**:

- `cargo fmt --all -- --check` clean
- `cargo clippy --workspace --all-features -- -D warnings` clean
- `cargo test --workspace` all green

**Sandbox divergence**: sandbox commit `149af878` references
DataFusion partition-pruning via SQL.  Duckpond's push uses
`read_commit_entry` (raw Delta commit log) rather than a SQL query,
so we filter at the action-iteration step rather than at scan
time.  Same outcome (bundle contains only local-pond files), but
no DataFusion session is opened during push.

### D5.3 — (original plan, kept for reference)

**File**: `crates/sync-remote/src/remote.rs` around `Remote::push`
(line ~285).

Today `Remote::push` reads all OplogEntry rows for the requested
`txn_seq` and pushes them all (the duckpond adapter
`ShipRemoteSteward::commit_to_data_persistence` already filters by
pond_id elsewhere, but the bundle itself doesn't have a
partition-level filter).

After D5.1, partition pruning in DataFusion can do this for free:
the SQL query that builds bundles adds `WHERE pond_id = '<local>'`,
and Delta scans only the relevant partitions.

This is what unblocks **efficient** cross-pond push (and pull) in
sandbox commit `149af878`.

### D5.4 — restart_from_compact made generic over RemoteSteward [DONE]

**Files**: `crates/sync-remote/src/remote.rs` (added
`Remote::bootstrap_consumer<S: RemoteSteward>`, refactored
`restart_from_compact` to share apply/set/pull code via
`restart_pond_from_compact`); `crates/steward/src/ship.rs` (added
`Ship::create_replica(path, uuid::Uuid)` convenience constructor);
`crates/cmd/tests/test_remote_cli.rs` (removed manual workarounds).

The carry-forward landed as a hybrid of the spec's two options:
- The generic API is `Remote::bootstrap_consumer<S>(consumer: &mut S)`:
  caller supplies a fresh consumer at matching `store_id`; the
  method handles both compact-baseline and writes-only first-pull.
- The duckpond-side constructor for a fresh consumer is
  `Ship::create_replica(path, pond_id)` (synthesizes default birth
  metadata; bytes-preserved conversion from `uuid::Uuid` to
  `uuid7::Uuid`).
- The sync_steward concrete API `restart_from_compact(&Path)` stays
  as a convenience wrapper that wipes/creates + delegates to
  `restart_pond_from_compact` (the in-place generic path).  The two
  paths now share apply-baseline/set-seq/catch-up code; the only
  difference is path setup.

New tests:
- `ship_create_replica_yields_matching_store_id`
- `ship_remote_bootstrap_consumer_no_compact`
- `ship_remote_bootstrap_consumer_rejects_store_id_mismatch`
- `test_remote_cli::pond_remote_push_pull_roundtrip` and
  `post_commit_auto_push_publishes_to_file_remote` rewritten to use
  `Ship::create_replica` + `bootstrap_consumer`.

After D5.4, dst pond inherits the source's `/sys/remotes/origin`
attachment on first-pull; tests re-attach with `--overwrite` to
change the mode to `pull`.

Still open (deferred):
- `pond restart-from-compact` CLI verb (would call
  `Ship::create_replica` + `Remote::bootstrap_consumer` from a
  command handler).
- Testsuite revival (`tests/500/510/530-542`) — depends on D5.8.

### D5.5 — compute_live_checksums for duckpond — **[DONE]**

**File**: `crates/steward/src/remote_adapter.rs` (≈85 lines replacing the old stub).
**Tests**: `crates/steward/tests/remote_adapter_test.rs::ship_compute_live_checksums_*` (3 tests).

Strategy chosen: blake3-per-row over `serde_json::to_vec(&OplogEntry)`,
leaf key `"<node_id>/<version>"`, group rows by `part_id`, fold each
partition through the existing `Merkle` strategy from
`sync_store::checksum`.

Why blake3-of-serde_json (not parquet bytes):
- Canonicalization is stable (serde_json uses the struct's declared
  field order; OplogEntry derives Serialize).
- Survives Delta optimize / compaction (rewriting parquet files
  without changing logical rows must not shift the checksum).
- ALL OplogEntry fields contribute, including `pond_id`, `part_id`,
  and `txn_seq`, so foreign-pond rows applied via `apply_pulled_bundle`
  produce the SAME checksums on the consumer as the producer would for
  the same `pond_id` (replication-invariant by construction).

Why a fresh SessionContext (not the persistence layer's State):
- `RemoteSteward::compute_live_checksums` takes `&self` and is invoked
  by the verify path, which is read-only and not gated on an active
  transaction.  `OpLogPersistence::state()` panics outside a
  transaction, so the implementation cannot rely on it.
- `build_session_ctx`-style pattern (mirrored from
  `sync-remote/src/remote.rs`): create a one-shot `SessionContext`,
  `register_table("delta_table_live", Arc::new(table.clone()))`, run
  one SQL query, drop the context.  Cheap and correct in all states.

tlogfs has no row-level tombstones — entries are append-only at the
row level (versions are new rows), so the SQL is just
`SELECT * FROM delta_table_live WHERE pond_id = '...' ORDER BY
part_id, node_id, version` with no ROW_NUMBER dedup (unlike
sync_store's k/v `partition_leaves`).

After D5.5 the verify path now has the LIVE side working
end-to-end for duckpond.  The RECORDED side in
`crates/sync-steward/src/control_table.rs::record_data_committed`
still writes `partition_checksums: HashMap::new()` for native writes
— D5.6/D5.7 will close that, and only then is `pond verify` safe to
run on a fresh native commit.

### D5.6 — Lift the data_delta_version=0 clamp

**File**: `crates/steward/src/remote_adapter.rs:573`.

Today:

```rust
let start = std::cmp::max(previous_last_pushed + 1, 2);
```

Pushes start at txn_seq 2 because the bootstrap pond_init txn has
`data_delta_version=0` and `Remote::push` rejects that with a
`Schema` error.  The carry-forward: move the `version=0` handling
into `Remote::push` itself (treat 0 as a clean skip, return Ok(())
with no work done) and remove the driver-side clamp.

This is a small, isolated change in `sync-remote` and removes a
duckpond-specific workaround.

### D5.7 — Cross-pond import UX surface

This is the design discussion that produces the `pond remote attach
... --mount <path>` flow (or whatever it ends up being called).
Concrete decisions needed:

- Is `--mount` an attribute of `pond remote add`, or a separate verb
  (`pond mount <name> <path>`)?
- What happens at `pond remote remove` if mounts exist?  Auto-unmount
  with `restart_pond_from_compact(drop=true)`, or refuse and require
  explicit `pond unmount`?
- How does the filesystem read path know it's reading a foreign
  pond's content?  Probably via OplogEntry's `pond_id` field plus a
  mount-table lookup ("anything under `/imports/upstream/` is
  expected to have `pond_id=<upstream's-uuid>`").
- How do `pond cat` / `pond list` show foreign content?  Same as
  native?  Marked specially?

The sandbox prototype `duckpond/sandbox/DESIGN.md` and the cross_pond
test in `crates/sync-tests/tests/cross_pond.rs` are the reference.

### D5.8 — Testsuite revival

Once D5.1–D5.7 land, work through the 13 currently-skipped scripts:

**Cross-pond import (D5 core)**:
- `tests/530-cross-pond-import-minio.sh`
- `tests/531-recursive-cross-pond-import.sh`
- `tests/532-cross-pond-path-boundaries.sh`
- `tests/533-cross-pond-factory-resolution.sh`
- `tests/540-import-watermark-incremental.sh`
- `tests/540-recursive-sitegen.sh`
- `tests/541-import-watermark-unrelated-txn.sh`
- `tests/542-import-watermark-restore.sh`

**First-pull bootstrap (unblocked by D5.4)**:
- `tests/510-synth-logs-replication-cycle.sh`
- `tests/523-emergency-erase-and-auto-bucket.sh`
- Plus: extend `tests/500-s3-replication-minio.sh` with a Pond2
  pull-back half.

**`pond run remote show/verify` equivalents (need new CLI verbs)**:
- `tests/520-remote-show-verification.sh` — needs `pond remote show`
  / `pond remote verify` or similar.
- `tests/521-external-tool-verification.sh` — same; also references a
  Rust binary in `crates/remote-verify-tool/` that no longer exists.
- `tests/522-emergency-recovery-tool.sh` — references
  `duckpond-emergency` standalone binary; may stay as a separate
  end-to-end test outside the D4/D5 CLI path.

Each disabled script today has a `# DISABLED-D4: <reason>` comment
header and a `SKIP: ...` early-exit block after `set -e`.  The CI
filter (`.github/workflows/integration-tests.yml:68-76`) greps for
`# REQUIRES: compose` in the first 25 lines, so only 500 and 501
currently match.

---

## Carry-forwards inherited from D4 (a checklist)

These are noted in `docs/remote-redesign.md` § D4 implementation notes
and in the `crates/cmd/tests/test_remote_cli.rs` module doc.  Repeating
here for easy reference:

| Item | Where | What |
|------|-------|------|
| `restart_from_compact` generic + bootstrap_consumer | `crates/sync-remote/src/remote.rs`, `crates/steward/src/ship.rs` | D5.4 — **DONE** |
| `compute_live_checksums` stubbed | `crates/steward/src/remote_adapter.rs` (was 501-511) | D5.5 — **DONE** (blake3-per-row Merkle) |
| `data_delta_version=0` clamp | `crates/steward/src/remote_adapter.rs:573` | D5.6 above |
| 13 disabled testsuite scripts | `testsuite/tests/{510,520-523,530-542}-*.sh` | D5.8 above |
| `pond_id` not a partition column | `crates/tlogfs/src/persistence.rs:276,348` | D5.1 — **DONE** |
| Pond identity from control-table Setting, not bootstrap row | `crates/steward/src/control_table.rs:456` | D5.2 — **DONE** |
| `actions_at_version` not pond-filtered | `crates/sync-remote/src/steward_trait.rs` | D5.3 — **DONE** |

---

## Architectural reminders (so you don't re-derive these)

### Layering (bottom → top)

```
cmd          CLI commands
sync-remote  Remote (Delta-bundle pipeline) + RemoteSteward trait
steward      Ship (transaction orchestration) + ControlTable +
             ShipRemoteSteward (adapter implementing RemoteSteward)
tlogfs       Delta Lake persistence, OpLog, DataFusion sessions
tinyfs       Pure filesystem abstractions
```

`sync-remote` is **agnostic** of duckpond's specific schemas.  It
operates against the `RemoteSteward` trait.  `ShipRemoteSteward` (in
`crates/steward/src/remote_adapter.rs`) is the duckpond
implementation; sandbox has its own.

### Where pond_id lives today (pre-D5)

- **Bootstrap source**: control-table Setting `pond_id` (set by
  `Ship::create_pond_with_metadata`, read by `Steward::open`).
- **Per-row**: regular column on every `OplogEntry`, stamped by
  `OpLogPersistence` at commit time from `self.pond_id`.
- **Partition layout**: NOT yet — `part_id` is the only partition
  column.

After D5: bootstrap row is canonical; partition layout adds `pond_id`
as the outer-most partition; control-table Setting becomes redundant.

### Where transactions live

- `pond` CLI = one process = one transaction (enforced by panic if a
  second `TransactionGuard` is created).
- Inside that transaction: pass `&mut tx` to helpers.  **Never** call
  `OpLogPersistence::open` or `persistence.begin()` again.
- Auto-push (D4.4): post-commit hook fires inside the steward after
  the transaction commits.  It scans `/sys/remotes/*` and pushes to
  each `push|both`-mode remote.  Failures log but don't fail the
  user's write.

### Where remotes live

- **Config**: `/sys/remotes/<name>` as YAML files (in the filesystem,
  inside the steward transaction).
- **Mode**: `remote_mode:<name>` in the control table's raw_config map.
- **Per-remote watermarks**: `last_pushed_seq:<url>` and
  `last_pulled_seq:<url>` in the control table's raw_config map (keyed
  by URL, not name, so remote rename works).

The path constants `SYS_DIR = "/sys"`, `SYS_REMOTES_DIR = "/sys/remotes"`,
and the key prefix `REMOTE_MODE_PREFIX = "remote_mode:"` are exported
from `crates/steward/src/lib.rs`.

---

## First-day workflow when you resume

1. **Re-read this doc** + `docs/remote-redesign.md` § D5.
2. **Verify baseline**: `cargo fmt --all -- --check && cargo clippy
   --workspace --all-features -- -D warnings && cargo test
   --workspace` should be clean on `jmacd/52`.
3. **Decide whether to PR D4 first** or accumulate D5 commits on top.
   Recommended: PR D4 first (it's a coherent, large change at a
   natural boundary) so D5 lands on a known-good main.  The plan.md
   in your session folder explicitly says "ready for PR".
4. **Plan D5.1** carefully before starting — the partition column
   change is a schema migration with operator impact.  Discuss with
   the user:
   - Migration policy: in-place rewrite, or "init fresh from data"?
   - Refuse-to-open old ponds vs. silent upgrade on first write?
   - Should there be a `pond migrate-partitions` CLI, or is this
     internal-only?
5. **Open the sandbox reference**: `duckpond/sandbox/` has working
   prototypes of all D5 features.  Cherry-pick design, not code:
   - `sandbox/A1-partition-by-pond-id.md` (if it exists; otherwise
     read `sandbox/DESIGN.md`).
   - `crates/sync-tests/tests/cross_pond.rs` — the cross-pond
     integration test we need to mirror in duckpond.

## Bookkeeping

- **Untracked file `may-10-bugs.md`**: predates this session;
  unrelated to D4/D5.  Leave alone unless the user asks.
- **Session state on this machine**:
  `~/.copilot/session-state/f5d4ca95-863b-4a92-bb51-56a07c994933/plan.md`
  has the D4 progress log including all 8 commit SHAs.  That session
  is ending; a new session should reference this file
  (`docs/d5-resume.md`) rather than dig into the old plan.md.
- **CI integration filter**:
  `.github/workflows/integration-tests.yml:68-76` greps for
  `# REQUIRES: compose` in the first 25 lines of each test script.
  Today only 500 and 501 match.  When you re-enable a disabled
  script, restore that line.

## Memory cues (stored in repo memory)

These were stored during D4 and should resurface in the next session:

- "`pond remote add` auto-inits the remote: push/both creates Delta
  table with local pond_id (or refuses foreign); pull requires
  existing remote."
- (See `subject: remote attach`, `subject: transactions`,
  `subject: factory init`, etc. in stored memories.)

---

**Bottom line**: D4 is done and clean.  D5 is the partition-column
migration plus cross-pond UX.  Start with D5.1 (partition migration);
D5.2-D5.6 follow naturally; D5.7-D5.8 close out the cross-pond import
flow and revive the disabled tests.  D6 is the operator-guide rewrite
and goes after all of D5.
