# Test Backlog

> Priority: P0 (blocking) → P1 (high) → P2 (medium) → P3 (low)
> Status: 🔴 Open | 🟡 In Progress | 🟢 Done | ⚪ Deferred

---

## 🔴 Open Items

### P1-BUG-LF-REPLICATION: `_large_files/blake3=<hash>.parquet` blobs are not replicated by sync_remote push/pull
- **Type**: BUG (discovered via D5.8.5 testsuite revival)
- **Symptom**: After `pond push origin` + `pond pull` to a fresh pond, files
  >64KiB (i.e., those that took the externalization path on the source) can
  be `pond list`-ed (correct metadata, size, blake3) but `pond cat` returns
  zero bytes.
- **Root cause**: `Steward::actions_at_version` (in `crates/steward/src/remote_adapter.rs`)
  enumerates Delta `Add` actions for the local pond. These are paths of the
  form `pond_id=<u>/part_id=<v>/part-N-N.parquet` — the partition parquet that
  contains the OpLog *row* describing the file.  For a large file, that row
  carries a `blake3` reference into `_large_files/blake3=<hash>.parquet`,
  which lives outside the partition tree and is therefore never enumerated,
  read, chunked, or sent in the bundle.  The receiving side's
  `apply_pulled_bundle` writes the partition parquet correctly but the
  blob it points at never exists on the receiver.
- **Test exposure**:
    - `testsuite/tests/510-synth-logs-replication-cycle.sh` uses
      `INITIAL_ROWS=500` (~10KB) — stays under the threshold, so does not
      trip the bug.  Bump past 5000 to reproduce.
    - `testsuite/tests/530-cross-pond-import-minio.sh` originally included
      an 80KiB random `big.bin` to exercise this path; the case was removed
      pending the fix so that 530 stays green while the bug is open.
      Reinstate it (and add an analogous case to 510) once fixed.
- **Fix sketch**: `actions_at_version` (or a parallel "side-content" pass)
  must, for each Add path, open the parquet and emit any
  `_large_files/blake3=<hash>` references it carries; those external blobs
  then need to be (a) included in the push bundle and (b) materialized by
  `apply_pulled_bundle`.  An alternative is for sync_remote to *inline*
  the external bytes when chunking on push, then re-externalize on the
  receiver — at the cost of duplicating the externalization decision
  across writers.
- **Priority**: P1 — silently corrupts data across the replication
  boundary for files >64KiB.  Small ponds are unaffected.

### P3-001: Document factory configuration examples
- **Type**: DOCS
- **Description**: Factory YAML configs need more complete examples
- **Next Step**: Run factory tests to discover actual syntax

---

## 🟢 Done

### ✅ D5.8.7: Revive the cross-pond import watermark group (540 + 541 + 542) and renumber 540-recursive-sitegen → 550
- **Completed**: 2026-06-03
- **Type**: REVIVAL (three scripts + one rename)
- **Description**: Replaced the three `DISABLED-D4` stubs that drove the
  deleted `remote` factory (`pond mknod remote /system/run/...` +
  `pond run ... pull` + `Watermark: N` log grep) with shell tests that
  exercise the D5.7b watermark surface: the persisted
  `last_pulled_seq:<url>` key in the steward control table, surfaced via
  `pond remote list`'s `LAST_PULLED_SEQ` column, and the
  `[OK] pull NAME: applied N bundle(s) from URL` log line emitted by
  `crates/cmd/src/commands/pull.rs:114`.
- **540 (incremental watermark — bandwidth-bug regression guard)**:
  10 checks.  Three pulls: initial (applied > 0, watermark advances
  above bootstrap), no-op (`applied 0 bundle(s)`, watermark unchanged
  — the regression guard against the production "re-walk 679 txns
  every tick" bug), and one-new-commit (applied >= 1, watermark
  advances by the delta, new file visible).  Captures
  `last_pulled_seq` between pulls and asserts monotonicity + zero
  reset to the bootstrap floor.
- **541 (watermark advances on ANY foreign txn shape)**:
  12 checks.  After initial pull, producer issues two mkdir-only
  commits (no file blobs) and a single file-content commit.  Asserts
  each produces a bundle, each advances the watermark by exactly one,
  the no-op pull between them still reports `applied 0 bundle(s)`,
  and the foreign directories are visible under the mount even
  though they carry no data payload.  Recast from the legacy
  filter-induced-skip bug (filter no longer exists in D5.7b) into a
  generic "bundle shape doesn't affect watermark persistence" guard.
- **542 (fresh-replica bootstrap reconstruction)**:
  11 checks.  Original consumer pulls and captures W_initial; pond
  directory is `rm -rf`'d entirely (the D5.7b analog of "terraform
  destroy + recreate" now that `pond init --from-backup` is removed,
  see `crates/cmd/src/commands/init.rs:15`); fresh consumer with same
  URL + mount path re-attaches and pulls.  Asserts: pre-pull
  `LAST_PULLED_SEQ` is "-" (unset); first pull reaches the SAME
  watermark and SAME applied count as the original; second pull is a
  no-op (regression guard that the rebuilt control table genuinely
  persisted the reconstructed watermark).
- **Numbering collision fix**: `git mv 540-recursive-sitegen.sh →
  550-recursive-sitegen.sh` (the sitegen-over-mount test was
  numerically colliding with the new watermark cluster; `run-test.sh`
  resolves numeric prefixes with `find -name 'N-*.sh' | head -1`,
  which would otherwise have aliased to the first alphabetical
  match).  550 still exits SKIP and is the next revival target.
- **Watermark values observed (for documentation)**: in a 3-file
  producer + 0-extra-history setup, pull #1 → watermark 4–7 (varies
  by mkdir count), pull #2 (no-op) → unchanged, pull #3 (one new
  commit) → watermark +1.  These numbers correspond directly to
  producer-side Delta versions; the bootstrap seed of "1" (`pull.rs:
  140-151`) skips the producer's pond_init txn at version 1.
- **Pre-existing tests still green**: 500, 501, 510, 520, 521, 522,
  523, 530, 532, 533 (full sweep run after this batch).

### ✅ D5.8.6: Revive `532-cross-pond-path-boundaries.sh` and `533-cross-pond-factory-resolution.sh`
- **Completed**: 2026-06-03
- **Type**: REVIVAL (both scripts)
- **Description**: Replaced the `DISABLED-D4` stubs (which mknod'd the
  long-removed `remote` factory with `import:` YAML blocks) with shell
  CLI versions of the two cross-pond invariants from `docs/d5.8-resume.md`
  § 3 that already had Rust integration coverage in
  `crates/cmd/tests/test_remote_cli.rs`.
- **532 (invariant 1: foreign mount is strictly read-only)**: 15 checks.
  After a cross-pond pull, every writing CLI verb that targets a path
  inside `/imports/A/*` must exit non-zero with a "read-only"-flavored
  error.  Verbs exercised: `pond copy host://... /imports/A/x`,
  `pond mkdir /imports/A/newdir`, `pond mkdir /imports/A/data/newsub`,
  `pond mknod sql-derived-table /imports/A/factory ...`.  Coexistence:
  writes to local (non-mount) paths still succeed.  Path-namespace
  isolation: B's local `/data/foo.txt` and `/imports/A/data/foo.txt`
  coexist with distinct content.
- **533 (invariant 2: foreign factories are NOT auto-run on consumer)**:
  12 checks.  Producer installs both kinds of factory entries
  (`/system/run/scratch` via `pond mknod sql-derived-table` and
  `/sys/remotes/origin` via `pond backup add`) and pushes.  Consumer
  cross-pond-imports, then performs a local write — bucket-A object
  count stays unchanged (both `discover_post_commit_factories` pond_id
  filter at `crates/steward/src/guard.rs:699` and the root-anchored
  `/sys/remotes/*` path pattern at `discover_sys_remotes` correctly
  skip the foreign entries).  Consumer then installs its own
  `/sys/remotes/origin-b` → bucket-B; next local write grows bucket-B,
  confirming local factories DO still auto-run.  Foreign entries
  remain visible by explicit traversal under the mount, decorated
  with A's pond_id tail — proof that filtering is per-entry skip at
  scan time, not entry deletion.
- **Findings during revival**:
    - There are TWO post-commit auto-exec mechanisms, with two distinct
      filter strategies for invariant 2:
        (a) `/sys/remotes/*` (D4+ `pond backup add` entries) is scanned
            with a root-anchored glob, so foreign mount entries simply
            do not match the pattern.
        (b) `/system/run/*` (legacy/manual `pond mknod` entries) is
            scanned with a glob that *can* cross mount boundaries, so
            an explicit `node.id().pond_id() != local_pond_id` filter
            skips foreign matches.
      The shell test exercises both by installing one of each on the
      producer side.
    - `pond list` does NOT truncate path components.  `/imports/A/sys`
      really is the directory name (the steward uses `/sys/` for
      its metadata; only the legacy/dynamic factory entries live under
      `/system/`).  These are two separate trees and both need to be
      consulted for cross-pond invariant testing.
    - `pond mkdir -p` is supported (long form `--parents`).

### ✅ D5.8.5: Revive `530-cross-pond-import-minio.sh` against the D5.7b cross-pond CLI
- **Completed**: 2026-06-03
- **Type**: REVIVAL (+ exposed P1-BUG-LF-REPLICATION above)
- **Description**: Replaced the `DISABLED-D4` stub — which mknod'd the
  long-removed `remote` factory with a YAML carrying an `import:` block —
  with a 13-check end-to-end test on top of the D5.7b CLI:
    1. Pond A: `init`, write a small tree under `/data` (csv, second csv,
       nested file), `pond backup add origin s3://BUCKET`, `pond push origin`.
    2. Pond B: `init` (distinct `pond_id`), seed a local `/local/note.txt`,
       `pond remote add upstream s3://BUCKET /imports/A`, `pond pull upstream`.
    3. Verify: every imported file is byte-faithful via the mount;
       Pond B's local data is unaffected; the `[<pond_id-tail>]` tag on
       the `/imports/A` entry shows Pond A's pond_id (provenance);
       a second pull is idempotent.
- **Findings during revival**:
    - `pond list PATH` returns the entry itself; `pond list PATH/` (trailing
      slash) returns its children.  Tests must use the trailing slash form
      to list directory contents.
    - `pond list` decorates every entry with `[<last-12-hex-of-pond_id>]`,
      which is the user-visible signal for cross-pond provenance.  No need
      to query the control table for this -- it shows up on the mount root.
    - `pond config` does not surface the foreign pond_id of an attached
      cross-pond remote in its human output (only the local pond_id).
      If a stable machine-readable view is wanted later, add it there.
    - Large-file replication is broken (see P1-BUG-LF-REPLICATION).  The
      80KiB `big.bin` case originally written into 530 was the smoking gun
      and has been removed from the green path; restore once the bug is
      fixed.
- **Regression**: 500 / 501 / 510 / 520 / 521 / 522 / 523 all clean.

### ✅ D5.8.4: Rewrite `crates/cmd/scripts/duckpond-emergency` for current Delta schema + revive `522-emergency-recovery-tool.sh`
- **Completed**: 2026-06-03
- **Type**: BUG (out-of-date disaster-recovery tool) + REVIVAL
- **Root Cause**: `duckpond-emergency` (v1.0.0) was written for the long-removed
  chunked-parquet remote schema (`bundle_id`, `path`, `root_hash`, `total_size`,
  `pond_txn_id`, `chunk_*` packed into a single parquet per bundle).  After
  D4.5 + D5.x replaced that with a Delta-Lake-native backup table
  (`partition_kind` in {manifest, checksum, data}, with explicit
  `file_path`/`file_action`/`file_blake3`/`chunk_data`/`chunk_blake3` columns
  and source-pond storage paths), the legacy script's SQL referenced
  non-existent columns and would have failed on every command.
- **Fix**: Rewrote the script (now v2.0.0) for the current schema:
  - `list` / `info` / `extract` / `verify` / `export-all` user-facing surface
    preserved.
  - All SQL targets `read_parquet('s3://BUCKET/partition_kind=*/*.parquet',
    hive_partitioning=true)` (works with configured `s3_*` settings, unlike
    `delta_scan()` which forces EC2 IMDS).
  - "Current snapshot" semantics implemented via a
    `ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY txn_seq DESC)` CTE
    that picks the latest `add` per `file_path` (so removed files are
    excluded from list/extract/verify).
  - BLOB extraction via `hex(chunk_data)` + `xxd -r -p` (portable across
    DuckDB versions, same trick as 521).
- **Tests**: `522-emergency-recovery-tool.sh` (21/21 passes) covers info /
  list / extract / verify / export-all + a BLAKE3 round-trip and
  parquet-readability spot check.  500 / 501 / 510 / 520 / 521 / 523
  regression-clean.
- **Notes for future work**: The remote backup only replicates the
  source-pond's parquet files (`pond_id=<u>/part_id=<v>/part-*.parquet`)
  and `_large_files/<hash>` blobs.  The source pond's own `_delta_log/`
  is *not* replicated (it's regenerable from the parquet files); the
  S3 bucket has its own separate `_delta_log/` describing the remote
  table itself.  Document this somewhere user-facing if/when we write
  a public disaster-recovery guide.

### ✅ D5.8.3: Revive `520-remote-show-verification.sh` + `521-external-tool-verification.sh`
- **Completed**: 2026-06-04
- **Type**: REVIVAL
- **Description**: Replaced the disabled (`DISABLED-D4`) scripts — both of which
  drove the removed `pond run /system/run/10-remote show` flow against a long-gone
  chunked-parquet schema (`bundle_id`, `path`, `root_hash`, `total_size`,
  `pond_txn_id`) — with D5.7b-shaped tests built on top of `pond backup add`,
  `pond push`, `pond log`, `pond config`, `mc ls`, and DuckDB over the current
  Delta-Lake-native backup schema (`partition_kind`, `txn_seq`, `file_path`,
  `file_blake3`, `chunk_data`, …).
  - **520**: 14/14 — verifies pond setup, push, transaction log shape, pond id
    config, `mc ls` of bucket layout (`_delta_log/`, `partition_kind=manifest/`,
    `partition_kind=checksum/`, `partition_kind=data/`), and DuckDB queries over
    `read_parquet('s3://.../partition_kind=*/*.parquet', hive_partitioning=true)`
    against partition_kind/file_path/manifest. Confirms the storage-level layout
    — distinct source `pond_id`, manifest `txn_seq` count, data partition with
    `pond_id=<uuid>/part_id=<uuid>/part-*.parquet` references — i.e. the backup
    replicates the *underlying storage*, not just the logical filesystem.
  - **521**: 5/5 — end-to-end "extract using only standard tools": push a tiny
    `hello.txt`, DuckDB picks the single-chunk Add row, `hex(chunk_data)` →
    `xxd -r -p` extracts bytes verbatim, `b3sum` of the extracted bytes equals
    both the stored `chunk_blake3` and `file_blake3` (single-chunk identity).
- **Notes**:
  - DuckDB's `delta_scan()` ignores configured `s3_*` settings and tries EC2
    IMDS (`169.254.169.254`); switched to `read_parquet(..., hive_partitioning
    =true)` over `s3://BUCKET/partition_kind=*/*.parquet`. For freshly-pushed
    tables (no vacuum / no removes) the row set is equivalent to `delta_scan`.
  - DuckDB binary BLOB extraction: `hex(...)` + `xxd -r -p` is the portable
    cross-version path (avoids `COPY (...) TO ... (FORMAT BLOB)` quirks).
- **Tests**: 520 (14/14), 521 (5/5); 500/501/510/523 regression-clean.

### ✅ D5.8.2: Revive `523-emergency-erase-and-auto-bucket.sh`
- **Completed**: 2026-06-03
- **Type**: REVIVAL
- **Description**: Rewrote the disabled (`DISABLED-D4`) script on top of the
  D5.7b backup CLI (`pond backup add`, `pond push`, `pond emergency
  erase-bucket`) after the legacy `remote` factory + `pond apply`
  configuration flow was removed.  Test now exercises four phases:
  Pond1 attach + push (auto-inits bucket Delta table with Pond1 id) →
  `pond emergency erase-bucket --dangerous` (with safety-flag refusal
  check) → Pond2 reattach (empty bucket auto-inits with Pond2 id) +
  push → Pond3 attach to occupied bucket must be refused with a
  `does not match` / foreign-pond error.
- **Test**: `523-emergency-erase-and-auto-bucket.sh` (11/11 passes);
  500, 501, 510 regression-clean.

### ✅ D5.8.1: Revive `510-synth-logs-replication-cycle.sh` + fix post-commit factory replication
- **Completed**: 2026-06-02
- **Type**: BUG + REVIVAL
- **Root Cause**: `StewardTransactionGuard::execute_post_commit_factory` opened a
  fresh `OpLogPersistence::begin_write`, wrote a real parquet, and committed it,
  but discarded the returned Delta version with `_ =` and never wrote any
  `Begin`/`DataCommitted`/`Completed` rows on the control table for that
  factory-allocated `txn_seq`. Replication's `Remote::push` then saw
  `NoSuchCommit(seq)` for the factory's commit and silently skipped the bundle
  — so logfile-ingest (and any other post-commit factory) data never replicated.
- **Fix**: `crates/steward/src/guard.rs` — read `last_txn_seq + 1` from the
  freshly-opened persistence (handles multi-factory case correctly), record
  `Begin` before `begin_write`, and after `factory_tx.commit()` record
  `DataCommitted` (with partition checksums snapshot) + `Completed`, or
  `Completed` for write-no-op, or `Failed` on commit error.
- **Test**: `510-synth-logs-replication-cycle.sh` (10/10 passes); 030-033
  logfile-ingest tests and 500 baseline replication continue to pass.

### ✅ D-007: Improved `pond list /` behavior (P2-002)
- **Completed**: 2026-02-03
- **Resolution**: `/` now lists root entries, trailing slash lists directory contents, updated cli-reference.md
- **Test**: `003-list-patterns.sh` - comprehensive pattern behavior verification

### ✅ D-006: Add `--sql` alias for `--query`
- **Completed**: 2026-02-03
- **Resolution**: Added `--sql` as primary flag with `--query` as visible alias, updated cli-reference.md

### ✅ D-001: Document glob patterns for `pond list`
- **Completed**: 2026-02-02
- **Resolution**: Added to cli-reference.md

### ✅ D-002: Document `host://` prefix for `pond copy`  
- **Completed**: 2026-02-02
- **Resolution**: Added to cli-reference.md

### ✅ D-003: Fix `--sql` → `--query` in docs
- **Completed**: 2026-02-02
- **Resolution**: Updated cli-reference.md and tests

### ✅ D-004: `--format=table` corrupts CSV files (P0 BUG)
- **Completed**: 2026-02-02
- **Root Cause**: `--format=table` should only accept Parquet files, not convert
- **Resolution**: Added PAR1 magic byte validation in copy.rs, helpful error message

### ✅ D-005: Table name inconsistency (`series` vs `source`)
- **Completed**: 2026-02-02
- **Resolution**: Changed code to use `source` to match documentation

---

## ⚪ Deferred

(none yet)

---

## Test Queue

Tests waiting to be run:

1. [ ] `400-dual-pond-observability.sh` - Multi-pond with log ingestion
2. [ ] `500-s3-replication-minio.sh` - S3 backup/restore cycle
3. [ ] `510-synth-logs-replication-cycle.sh` - **NEW** Full pipeline: synth-logs → logfile-ingest → S3 → replicate → verify (multi-round)
4. [ ] Factory node creation (`sql-derived-table`)
5. [ ] Control table queries
6. [ ] Time-series operations

---

## Quick Commands

```bash
# Run next test
cd /Volumes/sourcecode/src/duckpond/tests
./run-test.sh --save-result tests/XXX.sh

# Interactive exploration
./run-test.sh --interactive

# With S3/MinIO
docker-compose up -d minio
docker-compose run --rm duckpond
```

## BUG: `host+table:///path.parquet` fails with "not queryable"

`pond cat host+table:///path/to/file.parquet` errors:
```
Invalid URL: File '...' is not queryable (type: FilePhysicalVersion)
```

**Root cause:** `host+table://` parses as builtin scheme `table`, which routes
to `create_builtin_table_provider()`. That resolves the path through `HostmountPersistence`,
which maps all regular files as `FilePhysicalVersion` (raw data). The queryable check
fails because the entry type metadata isn't set — it's just a file on disk.

**Fix:** When `url.is_host()` and scheme is `table`/`series`, bypass the tinyfs
metadata path and read the file directly as Parquet (validate PAR1 magic, create
a DataFusion `ParquetExec` or `MemTable` from the bytes).
