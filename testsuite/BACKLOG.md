# Test Backlog

> Priority: P0 (blocking) → P1 (high) → P2 (medium) → P3 (low)
> Status: 🔴 Open | 🟡 In Progress | 🟢 Done | ⚪ Deferred

---

## 🔴 Open Items

### P3-001: Document factory configuration examples
- **Type**: DOCS
- **Description**: Factory YAML configs need more complete examples
- **Next Step**: Run factory tests to discover actual syntax

---

## 🟢 Done

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
