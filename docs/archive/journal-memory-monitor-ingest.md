# Journal Memory Monitor Ingest

A plan for using DuckPond's linux demo as a long-running memory benchmark,
observing how peak RSS grows as version history accumulates.

## Motivation

DuckPond's oplog is append-only.  Each time journal-ingest runs, every
JSONL file in `/logs/watershop/` gains one new version.  Two code paths
cause memory usage to grow with version count:

1. **Partition cache** (`ensure_partition_cached` in tlogfs): on first
   access to a partition, ALL committed oplog records are loaded into
   memory.  More versions means a larger cache.

2. **Series file reads** (`async_file_reader_series`): for
   `FilePhysicalSeries`, ALL non-empty versions are concatenated on read.
   Journal-ingest writes `FilePhysicalSeries`, so every file's read cost
   grows linearly with ingest count.

We need to see this quantitatively before designing the right optimization
(oplog checkpointing, Delta Lake vacuum, version rewriting, etc.).  The
hydrovu-style compaction is too manual -- it's effectively "read
everything through DataFusion, rewrite from scratch."  Delta Lake 0.29's
compaction and vacuum APIs are available in our dependency tree but
completely unused today.  Optimizing blind would be premature.

## Approach: self-referential dataset

The linux demo already collects journal logs into the pond.  With
systemd 254+ and `MemoryAccounting=yes`, the kernel automatically logs
peak RSS for the service cgroup on every exit:

    systemd[1234]: pond-journal.service: Consumed 16.2s CPU time, 84.5M memory peak.

This message is tagged `_SYSTEMD_UNIT=pond-journal.service`.  The
journal-ingest factory collects entries matching that unit, so the
peak-memory metric gets ingested on the **next** run and stored in
`/logs/watershop/pond-journal.service.jsonl`.  The dataset is
self-referential: pond's own kernel-level memory metrics become part of
the collected dataset, queryable from within the pond.

## Data flow

```
Run N (systemd service)            journald              Run N+1
──────────────────────           ──────────              ───────
pond run /system/etc/journal push                         pond run
  |-- reads journal <------------ entries since cursor       |
  |-- writes JSONL to pond                                   |
  |-- saves cursor                                           |
  +-- exits                                                  |
                                                             |
systemd logs (automatic):                                    |
"Consumed Xs CPU, YM memory peak" --> journal --> collected--+
```

After N ingest runs: each JSONL file in `/logs/watershop/` has ~N
versions.  Peak memory per run is queryable from
`pond-journal.service.jsonl`.  Time is a proxy for version count (one
version per file per run).

## Prerequisites

- Debian 13 (trixie) or equivalent with systemd 254+ for automatic
  peak-memory logging on service exit.
- `jq` for the analysis script.
- MinIO running for the backup factory (existing demo requirement).

## Changes

### 1. `linux/install-timer.sh` -- parameterize interval, enable accounting

- Accept `--interval` parameter (default: `1min`, was hardcoded `10min`).
- Add `MemoryAccounting=yes` to the generated `[Service]` section.
- Adjust `OnBootSec` to match the shorter interval.

### 2. `linux/analyze-memory.sh` -- new analysis script

- Reads `/logs/watershop/pond-journal.service.jsonl` via `pond cat`.
- Filters for systemd's "memory peak" messages (kernel RSS, logged
  automatically by systemd 254+ on service exit).
- Parses timestamp and peak memory using `jq`.
- Displays a time-series table to stdout.
- Also shows file count and approximate version count.

### No changes needed

- `linux/run.sh` -- no wrapper needed; systemd logs peak memory natively.
- `linux/setup.sh` -- demo directory structure is correct as-is.
- `linux/teardown.sh` -- cleanup logic unchanged.
- `linux/env.sh` -- no new environment variables needed.
- Factory configs (`journal-ingest.yaml`, `backup.yaml`, `site.yaml`).
- Site templates.

## Benchmark workflow

```bash
# 1. Fresh start
./linux/setup.sh

# 2. Install high-frequency timer (default 1 minute)
./linux/install-timer.sh
# or for a different interval:
./linux/install-timer.sh --interval 30s

# 3. Let it run for hours/days

# 4. Check memory trends
./linux/analyze-memory.sh

# 5. Cleanup when done
./linux/teardown.sh
```

## Expected observations

- After ~1 hour at 1-min intervals: ~60 versions per file, ~30 files.
- After ~24 hours: ~1440 versions per file.
- Peak memory should rise measurably as version count grows, because
  the current implementation reads all oplog versions.
- This data directly motivates checkpointing/compaction optimization.

## Where memory grows (code references)

| Hotspot | File | Behavior |
|---------|------|----------|
| Partition cache | `crates/tlogfs/src/persistence.rs` (`ensure_partition_cached`) | Loads ALL committed records for a partition into memory on first access |
| Series reader | `crates/tlogfs/src/persistence.rs` (`async_file_reader_series`) | Concatenates ALL non-empty versions oldest-to-newest |
| Version allocation | `crates/tlogfs/src/persistence.rs` (`get_next_version_for_node`) | Strictly append-only: "ALWAYS get next version - never reuse (immutable log)" |

## Future optimization directions (out of scope)

- **Delta Lake vacuum**: remove old parquet files after compaction
  (deltalake 0.29 has the APIs, currently unused).
- **Delta Lake optimize/compaction**: merge small parquet files within a
  partition into larger ones.
- **Oplog version rewriting**: collapse N versions of a file into 1,
  preserving the latest content (analogous to git gc).
- **Checkpoint frequency tuning**: periodic compaction triggered by
  version count threshold or memory pressure.
