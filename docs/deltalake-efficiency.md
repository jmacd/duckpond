# Delta Lake Efficiency Plan

Summary of findings and next steps from the April 4, 2026 session.

## Context

The MS-R1 (CIX P1 ARM workstation) runs Debian 12 Bookworm with systemd
252 and a vendor kernel (6.6.10-cix).  Upgrading the OS to Trixie is
blocked by 48 vendor-specific CIX packages.  We wanted systemd 254+ for
automatic `MEMORY_PEAK` journal logging to track pond memory growth over
time.

## What we did

### Memory peak logging workaround (committed to linux/)

Since systemd 252 logs `CPU_USAGE_NSEC` but not `MEMORY_PEAK` on unit
stop, and the kernel already tracks `memory.peak` via cgroups v2, we
added a workaround:

- **`linux/run.sh`** — after `pond run` exits, reads
  `/sys/fs/cgroup/.../memory.peak` from its own cgroup and writes a
  structured journal entry via `logger --journald` with a `MEMORY_PEAK`
  field.  This is collected by the next journal-ingest run.

- **`linux/install-timer.sh`** — now accepts `--interval` (default
  `1min`, was hardcoded `10min`), adds `MemoryAccounting=yes` to the
  service unit.

The timer is installed and running at 1-minute intervals.

### Baseline measurements (10 days at 10-min intervals)

| Metric | Value |
|--------|-------|
| Files in pond | 138 JSONL files under `/logs/watershop/` |
| Versions per file | ~1,200 |
| Peak memory (first measurement) | **569 MB** |
| CPU per run (day 1 → day 10) | 26s → 17s (initial drop, then +2.5s/day growth) |
| Total transactions | 1,078 writes, 5,384 delta log entries |

CPU growth is linear with version count, confirming the hypothesis in
`journal-memory-monitor-ingest.md`.

## Current problems

### 1. Data filesystem: no checkpoints, many small files

The data Delta table has:
- **1,390 delta log JSON entries** with **zero checkpoints**.  Every
  `pond run` must parse all 1,390 JSON files to open the table.
- **1,395 parquet files** in one partition, averaging ~61 KB each.
  DataFusion scans all of them.
- **~1,200 versions per JSONL file**.  Reading any file concatenates all
  versions (the `async_file_reader_series` path).

At 1-minute intervals this grows ~1,440 files/day.

### 2. Control table: audit log bloat

The control table is 238 MB (2/3 of the pond), mostly from:
- **5,384 delta log JSON entries** (195 MB) — reads are logged too.
- **5,380 transaction parquet files** (43 MB), ~7 KB each.
- 53 checkpoints exist (better than data table's zero).

Read transactions were added for cron-era debugging and are no longer
needed once pond runs as a proper systemd unit with journal logging.
However, removing them is a symptom fix — the real issue is lack of
compaction.

### 3. Version accumulation (memory growth driver)

The oplog is append-only.  `ensure_partition_cached` loads ALL committed
records.  `async_file_reader_series` concatenates ALL versions.  This is
the root cause of linear memory and CPU growth.

## Recommended fix: deltalake 0.30 upgrade + optimize/vacuum

### Library upgrade (separate session)

deltalake 0.30 requires:

| Dependency | Current | Required |
|-----------|---------|----------|
| arrow | 56 | 57 |
| parquet | 56 | 57 |
| datafusion | 50 | 51 |
| serde_arrow | 0.13 (arrow-56) | needs arrow-57 compat |
| thiserror | 1.x | 2.x (breaking) |
| Rust edition | 2021 | 2024 (rust 1.88+) |

Key new features in 0.30:
- **Log compaction** — collapses delta log JSON files into checkpoints.
- **Vacuum lite mode** — fast orphan file cleanup without storage listing.
- **Snappy compression on checkpoints**.
- **Parallel partition writers**.

### After upgrade: implement `pond optimize`

A new CLI command (or automatic post-transaction hook) that:

```rust
// 1. Checkpoint the delta log
table.create_checkpoint().await?;

// 2. Compact small parquets into large ones
let table = DeltaOps(table)
    .optimize()
    .with_type(OptimizeType::Compact)
    .with_target_size(128 * 1024 * 1024)
    .await?;

// 3. Remove orphaned files
let table = DeltaOps(table)
    .vacuum()
    .with_retention_period(Duration::hours(0))
    .with_enforce_retention_duration(false)
    .await?;
```

Apply to both the data table and the control table.

### Then: remove read transaction logging

Once pond has optimize/vacuum and runs as a systemd unit with journal
collection, the read transaction audit trail becomes redundant.  Remove
it from `Ship::begin_txn()` to stop the control table growth.

### Future: version compaction for the oplog

The Delta Lake optimize/vacuum addresses the small-file and delta-log
problems but does NOT address version accumulation within the oplog
(the tlogfs layer).  That requires oplog-level compaction: collapsing
N versions of a file into 1 by reading through DataFusion and writing
a fresh single version.  This is the long-term fix for the memory
growth described in `journal-memory-monitor-ingest.md`.

## Files changed this session

- `linux/run.sh` — added memory peak logging after pond run
- `linux/install-timer.sh` — parameterized interval, added MemoryAccounting
- Timer reinstalled at 1-minute intervals

## Related documents

- `journal-memory-monitor-ingest.md` — original plan for memory benchmarking
- `docs/cli-reference.md` — CLI command reference
- `crates/steward/src/control_table.rs` — control table implementation
- `crates/steward/src/ship.rs` — transaction lifecycle (read vs write)
- `crates/tlogfs/src/persistence.rs` — oplog version accumulation hotspots
