# Journal Log Collection: Watershop Real-World Test Report

**Date:** 2026-03-20
**Machine:** watershop.local (Debian 12.13, aarch64, CIX P1)
**Duration:** ~24 hours continuous operation

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

`pond list '/logs/watershop/'` takes **~63 seconds** to list 124 files.

### Measurements

| Command | Time |
|---|---|
| `pond list /` (2 entries) | 0.8s |
| `pond list /logs/watershop/ssh.service.jsonl` (1 file) | 1.8s |
| `pond list '/logs/watershop/'` (124 files) | 63s |

Baseline is ~1-2 seconds (loading the 148-version Delta log).  The
remaining ~60 seconds scales linearly with the number of files listed.
This is approximately **0.5 seconds per file**.

### Root Cause

`pond list` fetches file metadata via an **individual DataFusion SQL query
per file**.  The call chain is:

```
list_command()
  → FileInfoVisitor::visit()         # called once per matched file
    → file_handle.metadata()         # crates/cmd/src/common.rs:381
      → OpLogFile::metadata()        # crates/tlogfs/src/file.rs
        → State::metadata(id)        # crates/tlogfs/src/persistence.rs:909
          → State::query_records(id) # crates/tlogfs/src/persistence.rs:2770
            → session_context.sql(   # ONE SQL QUERY PER FILE
                "SELECT * FROM delta_table
                 WHERE part_id = '...' AND node_id = '...'
                 ORDER BY timestamp DESC"
              )
```

Each `session_context.sql()` call invokes DataFusion's full query
optimization pipeline.  With 124 files, this produces **263 optimizer
passes** and **193,057 debug log lines** — overwhelmingly from DataFusion
internals (`Use filter`, `Plan unchanged`, `Optimized physical`, SQL
parsing).

### Possible Fixes

1. **Batch metadata query:** a single SQL query per directory partition
   retrieving all files in one pass, rather than one query per file.
2. **Cache the oplog scan:** the Delta table is loaded once at startup;
   the metadata for all files could be materialized in a single scan
   rather than re-queried per file.
3. **Skip metadata for non-long listings:** if file size is not needed,
   avoid the query entirely.

This is a `tlogfs` layer issue.  The fix does not affect correctness,
only performance of listing directories with many entries.

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
