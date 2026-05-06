# Remote redesign: integrating the sandbox prototype into duckpond

## Status

Plan agreed; phased execution to begin with D1.

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
