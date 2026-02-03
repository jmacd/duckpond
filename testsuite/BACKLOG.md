# Test Backlog

> Priority: P0 (blocking) → P1 (high) → P2 (medium) → P3 (low)
> Status: 🔴 Open | 🟡 In Progress | 🟢 Done | ⚪ Deferred

---

## 🔴 Open Items

### P2-001: Rename `--query` to `--sql`
- **Type**: UX
- **Found**: 2026-02-02
- **Description**: Users expect `--sql` when writing SQL queries. Existing docs showed `--sql`, causing confusion.
- **Proposal**: Add `--sql` as alias for `--query`
- **File**: `crates/cmd/src/commands/cat.rs` (likely)

---

### P2-002: Better error for `pond list /`
- **Type**: UX  
- **Found**: 2026-02-02
- **Description**: `pond list /` returns cryptic "EmptyPath" error
- **Proposal**: Change error to suggest `/*` or `**/*`
- **Workaround**: Documented in cli-reference.md

---

### P3-001: Document factory configuration examples
- **Type**: DOCS
- **Description**: Factory YAML configs need more complete examples
- **Next Step**: Run factory tests to discover actual syntax

---

## 🟢 Done

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
