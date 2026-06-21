# Data-flow efficiency: write and read paths

Status: analysis. Code-grounded as of 2026-06-20 on branch `jmacd/57`.
Citations use `crate/file.rs:line` and were verified against the working tree
when written.

This document is a holistic, per-tick cost analysis of duckpond's two dominant
data-flow paths:

- the **write path** -- how HydroVu and logfile/journald ingest land new data,
  and the shared commit footprint underneath them; and
- the **read / materialize path** -- how sitegen turns landed data into the
  exported parquet that the static site serves, including the incremental
  rollup cache.

It is complementary to two existing documents and does not repeat them:

- `efficiency-priorities.md` covers maintenance, sync, compaction, and database
  I/O hotspots (checkpoint, vacuum, oplog compaction, format-cache pruning,
  remote backup). Read it for the steady-state maintenance design.
- `incremental-rollup-implementation.md` is the landed design for the rollup
  partial-aggregate cache (phases 1-4). This document summarizes its net effect
  and what it does *not* cover.

## 1. Cost vocabulary

Throughout, the unit of work is **one tick**: a single ingest-plus-build cycle
on a deployed pond. The question for every stage is whether its per-tick cost is:

- `O(delta)` -- proportional to the new data that arrived this tick; bounded and
  flat as the pond ages. This is the goal for every steady-state stage.
- `O(history)` -- proportional to all data ever landed; grows without bound as
  the pond ages. Acceptable for one-time or operator-triggered operations
  (archive, rebuild, restore), unacceptable in the per-tick hot path.
- `O(versions)` -- proportional to the number of immutable versions of a node;
  grows with history but far more slowly than `O(history)` bytes, and is the
  target of version compaction (`efficiency-priorities.md` section 6).

## 2. Shared write substrate: the commit is O(delta)

Every writer below ultimately commits through the tlogfs persistence layer, and
that layer is append-only per transaction.

- A commit serializes only **this transaction's** pending oplog records to a new
  parquet object and adds a single Delta `Add` action, partitioned by
  `(pond_id, part_id)`. Prior history is never rewritten.
  `crates/tlogfs/src/persistence.rs:2613-2700`.
- A read-only transaction with no records short-circuits before writing.
  `crates/tlogfs/src/persistence.rs:2578-2581`.
- File content above `LARGE_FILE_THRESHOLD` is stored as an external object keyed
  by its blake3 content hash, so identical content is deduplicated and large
  payloads never bloat the oplog parquet.
  `crates/tlogfs/src/persistence.rs:1191-1230`, `crates/tlogfs/src/persistence.rs:2170-2216`.

So the *commit* footprint is `O(delta)`. What can still be `O(history)` is the
work a writer does **before** it commits -- reconstructing or re-hashing prior
content to decide what the new delta is. The next two sections examine exactly
that.

## 3. Write path: HydroVu ingest

End-to-end: `collect_data` refreshes the parameter/unit dictionaries, then for
each active device `collect_device_data_internal` finds a resume timestamp,
pages the HydroVu API from that point, converts the readings to a `WideRecord`
batch, and writes one series batch to
`{hydrovu_path}/devices/{device_id}/{name}_active.series` as a
`TablePhysicalSeries`.
`crates/hydrovu/src/lib.rs:324-340`, `crates/hydrovu/src/lib.rs:794-879`.

### 3.1 What is O(delta)

- **API fetch is incremental.** `find_youngest_timestamp` resumes from the
  newest event time already landed, so each run pages only the new window.
  `crates/hydrovu/src/lib.rs:515-523`.
- **Write granularity is append-only.** Each run writes only the newly fetched
  batch as a new immutable series version via `async_writer_path_with_type(...,
  TablePhysicalSeries)`; prior versions are not re-read or rewritten.
  `crates/hydrovu/src/lib.rs:871-879`, `crates/tinyfs/src/arrow/parquet.rs:486-504`.
- **No global sort/merge in the hot path.** Only the newly fetched window is
  converted and written; versions are self-contained.
  `crates/hydrovu/src/lib.rs:808-879`.

### 3.2 What is O(versions) or O(history)

- **Resume scan is O(versions).** `find_youngest_timestamp` enumerates every
  `.series` file in the device directory and iterates all versions, reading each
  version's `min_event_time`/`max_event_time` extended metadata.
  `crates/hydrovu/src/lib.rs:462-510`. This reads metadata, not row data, so it
  is cheap per version, but it grows with version count and is a direct
  beneficiary of series version compaction (`efficiency-priorities.md` section 6).
- **Archive compaction is O(history) by design, but not per-tick.** The archive
  command registers the full active series and runs
  `SELECT * FROM _compact ORDER BY timestamp`, writing a single-version snapshot
  `{name}_archive_YYYYMMDD.series` and deleting the old active entry.
  `crates/hydrovu/src/lib.rs:258-288`, `crates/hydrovu/src/lib.rs:137-171`. This
  is an operator/periodic seed operation, not part of every ingest tick, so its
  `O(history)` cost is acceptable.
- **Archive-fallback resume is O(archive files).** When the writeable directory
  is empty, `find_archive_youngest_micros` runs `SELECT max(timestamp)` on each
  archive file. `crates/hydrovu/src/lib.rs:525-538`. This only happens on a cold
  device, not steady state.

Net: the HydroVu hot path is `O(delta)` for both fetch and write. The only
growth term in steady state is the `O(versions)` metadata resume scan, mitigated
by compaction.

## 4. Write path: logfile / journald ingest

The canonical implementation is `crates/provider/src/factory/logfile_ingest.rs`
(the `tlogfs` copy only re-exports config for tests; see the code-structure
memory and `crates/tlogfs/src/factories/logfile_ingest.rs:7-14`). It mirrors host
log files matched by glob patterns into pond paths, writing each as a
`FilePhysicalSeries` whose versions a `ChainedReader` concatenates on read.
`crates/provider/src/factory/logfile_ingest.rs:187-198`, `758-767`, `841-851`;
`crates/tlogfs/src/persistence.rs:724-734`; `crates/tinyfs/src/chained_reader.rs:5-27`.

### 4.1 What is O(delta)

- **Append reads only the new bytes.** For an active file that has grown, ingest
  seeks to the prior `cumulative_size` and reads exactly the new tail, writing
  it as a new `FilePhysicalSeries` version.
  `crates/provider/src/factory/logfile_ingest.rs:795-807`, `841-851`.
- **Write granularity is append-only.** Both the new-file and append cases issue
  a single `write_all` of just the new content into a new version; accumulated
  content is never rewritten. `crates/provider/src/factory/logfile_ingest.rs:758-767`,
  `841-851`.

### 4.2 What is O(history) -- the prefix re-hash

There is one genuine `O(history)` cost in the steady-state append path:

- **Prefix verification re-reads and re-hashes the entire prior file content on
  every append.** Before appending, ingest reads `cumulative_size` bytes of the
  host file -- the whole file-so-far -- and recomputes its blake3 to confirm the
  prefix has not changed (rotation guard).
  `crates/provider/src/factory/logfile_ingest.rs:820-827`.

  ```rust
  let mut prefix_content = vec![0u8; pond_state.cumulative_size as usize];
  prefix_file.read_exact(&mut prefix_content)?;
  let mut hasher = IncrementalHashState::new();
  hasher.ingest(&prefix_content);
  let prefix_blake3 = hasher.root_hash().to_hex().to_string();
  ```

  For a long-lived, continuously-appending active log this re-reads and re-hashes
  all previously-ingested bytes every tick: per-tick cost grows with file size.
  It is correctness-motivated (detect mid-run rotation), but it is the clearest
  `O(history)` term on the ingest write path and a candidate for optimization
  (section 6).

### 4.3 Other growth terms

- **Directory/pond scan is O(files).** Each run enumerates all matching host
  files and all pond files to diff state.
  `crates/provider/src/factory/logfile_ingest.rs:187-198`, `464-505`.
- **Archived-file integrity check is O(archived bytes), once per archived file.**
  Reads a whole rotated host file to compare blake3, only to detect unexpected
  mutation. `crates/provider/src/factory/logfile_ingest.rs:698-719`.
- **Hash resume is bounded.** `read_pending_bytes` walks prior versions backward
  only far enough to gather the tail bytes needed to resume the incremental hash,
  not the whole history. `crates/tlogfs/src/file.rs:160-260`.
- **Read-time concatenation is O(versions).** `FilePhysicalSeries` reads chain
  all versions; this is a read cost, addressed by series compaction, not an
  ingest cost. `crates/tinyfs/src/chained_reader.rs:15-18`.

### 4.4 journald and weblog

- **journald ingest is cursor-based and O(delta).** It reads `.journal-cursor`,
  runs `journalctl --after-cursor=...`, groups lines by unit, writes each unit as
  a new `FilePhysicalSeries` version, and stores the new cursor.
  `crates/provider/src/factory/journal_ingest.rs:115-184`,
  `crates/provider/src/factory/journal_ingest.rs:368-401`; design in
  `docs/journal-log-collection-design.md:35-53`. It has no prefix re-hash, so it
  avoids the section 4.2 cost.
- **weblog has no write path.** `weblog://` is a read-time format provider that
  parses an input stream into one batch; ingestion of web logs reuses the
  logfile-ingest `FilePhysicalSeries` machinery above.
  `crates/provider/src/format/weblog.rs:112-208`; `docs/weblog-ingestion-design.md:72-108`.

## 5. Read / materialize path: sitegen export

Sitegen materializes each series/table node to static, Hive-partitioned parquet
via `provider::export::export_series_to_parquet`, which registers the node as a
DataFusion table and runs `COPY (...) TO ... PARTITIONED BY (...)`.
`crates/sitegen/src/factory.rs:1088`, `crates/provider/src/export.rs:196-353`.

The expensive nodes are the `temporal-reduce` rollups: for Caspar Water that is
roughly 5 metrics x 5 resolutions = 25 materialized exports per site.
`docs/incremental-rollup-implementation.md:20-22`.

### 5.1 What is now O(delta) (the landed rollup cache)

The rollup cache (phases 1-4) made the **aggregation** incremental:

- **Leaf parse is incremental.** The format cache parses each ingested version
  exactly once, ever (`docs/format-cache-design.md`,
  `crates/provider/src/format_cache.rs`).
- **Partial aggregation is incremental.** Each source version's decomposable
  partials (`Sum`, `Count`, `Min`, `Max`) are computed once and cached per
  version; only uncached versions are aggregated.
  `crates/provider/src/factory/temporal_reduce.rs` (try_rollup_table_provider),
  `crates/provider/src/rollup_cache.rs`.
- **Finest partials are shared across resolutions (phase 3).** Raw is aggregated
  into finest-resolution partials once per version regardless of how many
  resolutions a site defines; coarser resolutions re-bin those partials rather
  than rescanning raw.
- **Non-sequential input is a hard error, not a silent miscount (phase 4).** A
  per-source-node frontier sidecar rejects a version that backfills sealed
  buckets, recovered with `export --rebuild`.

So the dominant historical cost -- scanning, parsing, and aggregating the full
raw history every build -- is now `O(delta)`.

### 5.2 What is still O(history) every build

Two downstream tiers were never part of the incremental plan and still
reconstruct the whole history on every tick:

1. **The merge / reconstruction is O(history / finest_interval) per resolution.**
   `merge_sql` is a `GROUP BY date_bin(...)` over *all* cached finest partials,
   emitting every output bucket for all time.
   `crates/provider/src/factory/temporal_reduce.rs:1127-1148`. The finest
   partials are therefore re-scanned **once per resolution** per build (N
   resolutions => N scans of the partials set). Phase 3 deliberately traded this
   N-times partial re-scan for not duplicating coarse-resolution partial storage.

2. **The export write is a full O(history) rewrite.** `export_series_to_parquet`
   does `remove_dir_all(export_dir)` and then `COPY (SELECT * ...) TO ...
   PARTITIONED BY (...)`, rewriting the entire partitioned parquet tree for all
   history every build -- even temporal partitions that did not change.
   `crates/provider/src/export.rs:319-353`. The `remove_dir_all` exists to avoid
   a duplicate-file explosion from DataFusion's UUID-named COPY output
   (`crates/provider/src/export.rs:319-325`), so incrementalizing it requires
   per-partition reconciliation, not just dropping the wipe.

These are `O(history)` but on a much smaller constant than the raw scan: the
merge operates on aggregated buckets (history / finest_interval rows), and the
export writes aggregated output, not raw rows.

## 6. Summary and recommended next optimizations

| Stage | Per-tick cost | Status |
|-------|---------------|--------|
| Core commit (oplog write) | `O(delta)` | Done; append-only per txn |
| HydroVu API fetch | `O(delta)` | Done; watermark resume |
| HydroVu series write | `O(delta)` | Done; append-only versions |
| HydroVu resume scan | `O(versions)` metadata | Mitigated by compaction |
| HydroVu archive compaction | `O(history)` | Acceptable; not per-tick |
| logfile-ingest append write | `O(delta)` | Done; append-only versions |
| **logfile-ingest prefix re-hash** | **`O(history)`** | **Candidate (6.1)** |
| journald ingest | `O(delta)` | Done; cursor-based |
| Raw leaf parse | `O(delta)` | Done; format cache |
| Rollup partial aggregation | `O(delta)` | Done; rollup cache |
| **Rollup merge reconstruction** | **`O(history / interval)` x N res** | **Candidate (6.2)** |
| **sitegen export write** | **`O(history)` full rewrite** | **Candidate (6.3)** |

Prioritized follow-ups, highest expected payoff first:

1. **Incrementalize the logfile-ingest prefix verification (section 4.2).** The
   incremental hash state and bao outboard already let TinyFS resume a hash from
   a stored tail (`crates/tlogfs/src/file.rs:160-260`). Verifying only a bounded
   suffix of the prefix, or trusting the stored incremental hash plus a small
   sentinel, would drop this from `O(file size)` to `O(delta)` per append while
   keeping the rotation guard. This is the only `O(history)` term on the steady
   ingest write path.

2. **Cache the per-resolution merged output (section 5.2, item 1).** The merge
   emits every bucket for all time on every build. Because partials are sealed
   and immutable except at the open frontier bucket, the merged output for sealed
   buckets is also stable; only the frontier bucket and any newly-added buckets
   change. A per-resolution merged-output cache keyed like the partial cache
   would make the merge `O(delta)` and remove the N-times partial re-scan.

3. **Incrementalize the export write (section 5.2, item 2).** Replace the
   `remove_dir_all` + full `COPY` with per-temporal-partition reconciliation:
   only rewrite the open partition (and any partition whose buckets changed),
   leaving sealed historical partitions untouched. This requires deterministic
   per-partition file naming to replace DataFusion's UUID output, which is why
   the current code wipes instead.

Items 2 and 3 compose: with a merged-output cache, the export already knows which
partitions changed, so per-partition reconciliation becomes straightforward.

## 7. Related documents

- `efficiency-priorities.md` -- maintenance, sync, compaction, DB I/O hotspots.
- `incremental-rollup-implementation.md` -- the landed rollup cache (phases 1-4).
- `incremental-timeseries-rollup.md` -- the original rollup architecture sketch.
- `format-cache-design.md` -- the leaf-parse cache that makes parsing incremental.
- `weblog-ingestion-design.md`, `journal-log-collection-design.md` -- ingest
  designs for the log sources discussed in section 4.
