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
