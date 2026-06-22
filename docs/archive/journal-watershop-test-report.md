# Journal Log Collection: Watershop Real-World Test Report

**Date:** 2026-03-20
**Machine:** watershop.local (Debian 12.13, aarch64, CIX P1)
**Duration:** ~24 hours continuous operation

> **Historical note (2026-04):** the `linux/` configs referenced
> throughout this report were retired after this test concluded.
> The successor production deployment lives in
> [`caspar.water`](https://github.com/jmacd/caspar.water) under
> `config/watershop-selfmon.yaml` (selfmon ingest pipeline) and
> `terraform/station/watershop/` (provisioning).  The pond binary
> is now distributed as a `.deb` (`cargo deb -p cmd`) built natively
> on watershop, replacing the podman-image-extract path.

## Summary

First real-world deployment of the `journal-ingest` factory and `jsonlogs`
format provider on the target machine described in
`docs/journal-log-collection-design.md`.  All phases except remote
replication to an offsite S3 (Phase 3) were exercised end-to-end using a
local MinIO instance as the S3 backend.

**Result:** Collection and backup ran flawlessly for 24 hours.  One
performance issue was discovered in `pond list`.

---

## What Was Deployed

| Component | Detail |
|---|---|
| Binary | `pond 0.30.0`, native ARM64 release build (~181 MB) |
| Rust toolchain | 1.94.0 stable (aarch64) |
| Pond location | `/home/jmacd/pond` |
| Collection interval | Every 10 minutes via systemd user timer |
| Backup target | MinIO on localhost:9000, bucket `duckpond-linux` |
| Config scripts | `linux/` directory in the duckpond repo |

### Pond Structure

```
/logs/watershop/            # 124 files (one per systemd unit)
  ssh.service.jsonl
  NetworkManager.service.jsonl
  kernel.jsonl
  minio.service.jsonl       # MinIO's own logs, collected by pond
  ...
  .journal-cursor           # journalctl cursor for incremental collection
/system/etc/journal         # journal-ingest factory
/system/run/20-backup       # remote factory → MinIO
```

---

## 24-Hour Results

| Metric | Value |
|---|---|
| Timer runs | 138 successful, 0 failed |
| Transactions committed | 147 |
| Journal files | 124 (one per systemd unit) |
| Initial collection | 137,116 entries across 121 files |
| Incremental runs | ~40-70 entries per 10-minute interval |
| Pond disk usage | 32 MB |
| MinIO backup size | 4.3 MB (566 objects) |
| Peak memory (initial) | 350 MB |
| Peak memory (incremental) | 28-37 MB |
| Runtime per collection | ~3-4 seconds (incremental) |

### Key Observations

- **Zero errors** in 24 hours of continuous operation.
- **Incremental collection works correctly:** cursor is persisted in the
  pond, each run collects only new entries since last cursor.
- **Post-commit auto-backup works:** every journal collection transaction
  automatically triggers the remote factory push to MinIO.
- **Self-referential logging:** the pond collects MinIO's own journal logs,
  which are generated in part by the pond backing up to MinIO.
- **Version accumulation:** active files like `CRON.jsonl` reached v11+ and
  `user@1001.service.jsonl` reached v136 over 24 hours (one version per
  10-minute collection interval that includes entries for that unit).

---

## Performance Issue: `pond list` Scales Poorly

### Symptom

`pond list '/logs/watershop/'` takes **~63 seconds** to list 124 files
(day 1, 148 delta log versions).  By day 2 with 267 versions: **~114
seconds**.

### Measurements (day 2, 267 versions, before fix)

| Command | Time |
|---|---|
| `pond list /` (2 entries) | 0.8s |
| `pond list /logs/watershop/ssh.service.jsonl` (1 file) | 1.8s |
| `pond list '/logs/watershop/'` (124 files) | 114s |

### Root Cause

Two layered problems:

**Problem 1 (fixed):** `pond list` ran a **separate DataFusion SQL query
per file** to fetch metadata.  124 files = 124 queries, each triggering
the full DataFusion optimizer pipeline.

**Problem 2 (remaining):** The `/logs/watershop/` partition has accumulated
**261 parquet files** from 267 delta log versions.  Even a single SQL
query scanning this partition is expensive (~60s) because DataFusion must
open and read all 261 files.

### Fix Applied: Partition Records Cache

Added a transparent cache in `InnerState` (`persistence.rs`).  On first
`query_records()` for a given `part_id`, all records for that partition
are loaded in a single SQL query and cached in a
`HashMap<PartID, HashMap<NodeID, Vec<OplogEntry>>>`.  Subsequent calls
for the same partition are HashMap lookups.

**Result:** 124 SQL queries → 1 per partition.

| Metric | Before | After |
|---|---|---|
| `pond list '/logs/watershop/'` | 114s | **60s** |
| SQL queries per listing | 124 | 1 |

The remaining 60 seconds is the cost of scanning 261 parquet files in
one partition.  This will be addressed by Delta Lake compaction (merging
many small parquet files into fewer large ones), which is a separate
concern from the query batching fixed here.

### Possible Further Improvements

1. **Delta Lake compaction:** Periodically compact the data partition
   to merge 261 small parquet files into a few large ones.
2. **Metadata-only query:** Instead of `SELECT *`, query only the
   columns needed for metadata (entry_type, version, timestamp, size,
   blake3) to reduce I/O.
3. **Delta Lake checkpointing:** Ensure Delta checkpoints are written
   regularly to avoid replaying 267 transaction log entries on open.

---

## Setup Artifacts

All deployment scripts are in `linux/` in the duckpond repo:

| File | Purpose |
|---|---|
| `env.sh` | Shared environment (POND path, MinIO credentials) |
| `journal-ingest.yaml` | Journal-ingest factory config |
| `backup.yaml` | Remote factory config (MinIO S3) |
| `setup.sh` | Tear down + initialize + first collection |
| `teardown.sh` | Destroy pond, bucket, and timer |
| `run.sh` | Collect journal + auto-backup (called by timer) |
| `install-timer.sh` | Install systemd user timer (10-minute interval) |
| `setup-mc.sh` | Install MinIO client to `~/.local/bin` |

### Reproducing

```bash
cd /home/jmacd/src/duckpond
./linux/setup-mc.sh          # one-time: install MinIO client
./linux/setup.sh             # init pond, collect, backup
./linux/install-timer.sh     # start 10-minute timer
```

### Monitoring

```bash
# Watch collection runs
journalctl --user -u pond-journal.service -f

# Check timer
systemctl --user status pond-journal.timer

# Query collected logs
export POND=/home/jmacd/pond MINIO_ACCESS_KEY=caspar MINIO_SECRET_KEY=watertown
pond cat jsonlogs:///logs/watershop/ssh.service.jsonl --format=table \
  --sql 'SELECT "MESSAGE", "PRIORITY" FROM source ORDER BY "__REALTIME_TIMESTAMP" DESC LIMIT 10'
```
