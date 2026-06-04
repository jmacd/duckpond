# Remote redesign: integrating the sandbox prototype into duckpond

## Status

Architectural goals achieved; the P1 correctness blocker is now closed
(D5.9, `d5bf4b68`).  Documentation reconciliation and D6 CLI verb gaps
remain (see § "Open after D5.8" below), but the branch is functionally
ready for production cutover.

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
| D6: cross-pond model migration + operator-guide rewrite | partial | see § "Open after D5.8" |

Active branch: `jmacd/52` (89 commits ahead of `main` as of `d5bf4b68`).
The D5 row in earlier revisions of this doc was a single "pending" line;
it expanded into 15 sub-phases (D5.1 through D5.9) during execution.
The original D5 design (lines 556-578) accurately predicted the technical
shape of the work; the sub-phasing is a record of how it actually landed.

## Open after D5.8

D5.9 closed the correctness blocker for cutover.  Two categories of
work remain before this branch can fully replace the old duckpond in
production:

### Correctness blocker (P1) -- CLOSED

- **P1-BUG-LF-REPLICATION** (closed by D5.9, `d5bf4b68`): files larger
  than `LARGE_FILE_THRESHOLD` (64 KiB) are externalized to
  `_large_files/blake3_16=<pfx>/blake3=<hash>.parquet` blobs that live
  outside the `pond_id=<u>/part_id=<v>/` partition tree.  Before the
  fix, `actions_at_version` enumerated only the partition parquet, so
  the receiver got the OpLog row (with correct metadata, size, blake3)
  but never the underlying blob.  Fixed by adding an
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

### Deferred carry-forwards from D4/D5

- **`Remote::restart_from_compact` not generic over `RemoteSteward`**
  (originally noted as a D4 carry-forward; still pending). Currently
  mirror-only; cross-pond consumers cannot restart from a compacted
  remote without manual Rust glue.
- **Mirror-restart bootstrap is Rust-only** via
  `ShipContext::create_pond_for_restoration`; cross-pond first-pull
  works through the CLI, but mirror-restart from a fresh local
  directory does not.
- **Physical vacuum of purged foreign `pond_id` rows from the Delta
  log** (D5.7b.4 deferral). Correctness-safe (rows are unreachable
  by path after `--purge`); they remain in the log until a future
  partitioned-delete compaction.

### D6 surface gaps

The D6 CLI plan at lines 593-602 of this doc enumerates verbs that
have not all shipped:

| Verb | Status | Notes |
|------|--------|-------|
| `pond init` | shipped | |
| `pond init --from-remote` | not shipped | Replaced by automatic bootstrap on `pond remote add NAME URL /` (mirror-restart pull mode). |
| `pond remote attach` | shipped as `pond remote add` | D5.7b.1 names. |
| `pond remote detach` | shipped as `pond remote remove` | Default = detach; `--purge` = unlink mount entry. |
| `pond push` / `pond pull` | shipped | |
| `pond maintain` | shipped | |
| `pond status` | **not shipped** | Operator-facing aggregate of pond + remotes + lifecycle states. |
| `pond log` | shipped | |
| `pond verify` | **not shipped** | Operator-facing wrapper around `verify_against_remote`. |
| `pond recover` | shipped | Plus `pond emergency` for destructive recovery. |
| `pond restart-from-compact` | **not shipped** | Blocked by the carry-forward above. |
| `pond rebuild-control` | **not shipped** | Operator-driven control-table reset. |

### Documentation reconciliation

- **`docs/operator-guide.md`** still carries the "D4 update"
  warning at lines 3-10 noting that examples no longer work and
  that "a full rewrite of this guide is tracked under D6." Either
  rewrite it on the D5.7b/D5.8 surface, or move it to
  `docs/archive/` and point operators at `docs/cli-reference.md`.
- **`docs/operator-interface-plan.md`** likewise predates D4 and
  needs the same disposition.
- **This document's** phase table was updated in this round to
  reflect actual D5 sub-phasing; the design narrative at lines
  556-602 is still accurate as a record of intent.

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
- Update `duckpond/sandbox/DESIGN.md` references — likely
  archive the design doc to `docs/archive/` or merge relevant
  parts into this redesign doc.
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

## Out of scope (for now)

- CRDT-style symmetric ponds (future direction; mode becomes
  non-issue).
- `pond init`-time configuration of FS conventions and per-replica
  defaults (currently via control-table settings).
- Old-format bundle migration tooling (no compat needed).
- Multi-process locking on local pond directories (single-writer
  model assumed; control-table reset requires operator to
  guarantee no concurrency).

## References

- Sandbox prototype design: `duckpond/sandbox/DESIGN.md` (the
  sandbox-side design that this integration adopts).  Will be
  archived after D1.
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
