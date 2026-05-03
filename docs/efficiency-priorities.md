# Maintenance, Sync, and I/O Efficiency: a Design

A single design covering how every piece of pond state is created, read, kept
small, replicated to a remote backup, and eventually pruned.  Read top to
bottom; each section assumes the previous ones.  Code citations use the form
`crate/file.rs:line` and were verified against the working tree at the time
this was written.

The design is motivated by what `selfmon` measures on `watershop` today: per-tick
`pond run` cost grows with pond age, the control table is two thirds of the
on-disk pond, and the dashboard query against a 32 KB JSONL log takes 7 to 34
seconds.  None of those are individual bugs; they are all visible consequences
of one architectural choice -- everything is append-only and nothing is
collapsed in steady state.

The design's job is to introduce **bounded steady state**: per-tick cost,
per-query cost, push cost, restore cost, and on-disk footprint must each be
constant (or at worst proportional to retained-window size), independent of
pond age.

---

## 1. Where state lives

Every piece of pond state lives in exactly one of these places.  The whole
design is organized around them.

```
{POND}/
  data/         Delta Lake table of OplogEntry rows (the "tlogfs oplog")
    _delta_log/      JSON commits + checkpoint parquets
    _large_files/    Content-addressed blobs >64 KB (out-of-band from oplog rows)
    part_id=*/       Per-directory partitions, parquets containing OplogEntry rows
  control/      Delta Lake table of TransactionRecord rows
    _delta_log/
    record_category=metadata/
    record_category=transaction/
  cache/        Format-cache materializations (oteljson, csv, jsonlogs -> parquet)
    {scheme}_{node_id}/v{version}_{blake3}.parquet
    {scheme}_glob_{pattern_hash}/  (symlinks to the above)
```

And in the remote bucket (one bucket per pond):

```
s3://<bucket>/
  _delta_log/                       Delta log of the remote backup table
  bundle=<pond_id>:txn=<seq>/       Per-transaction chunked record + parquet copies
  _large_files/                     Mirrored large blobs (cumulative)
```

Four storage layers, each with its own accumulation pattern:

| Layer | Unit that accumulates | Today's compaction | Today's vacuum |
|---|---|---|---|
| Delta log (data table) | one JSON file per commit | checkpoint every 10 commits | log cleanup after checkpoint |
| Delta data files (data table) | one parquet per commit per partition | none (manual `pond maintain --compact`) | every commit |
| Tlogfs oplog versions | one row per file write | none (no primitive exists) | none |
| Format cache | one parquet per source version | none | none |
| Delta log (control table) | one JSON file per record write | checkpoint every 10 commits | log cleanup after checkpoint |
| Delta data files (control table) | one parquet per record write | none | every commit |
| Remote: Delta log | one JSON file per push commit | none | none |
| Remote: per-txn bundles | one bundle per pushed transaction | none | none |
| Remote: large_files | content-addressed blob | n/a | none |

Every "today's compaction = none" or "vacuum = none" cell is a place where
unbounded growth happens.  Every cell with "every commit" is a place that
might be doing more work than it needs to.

---

## 2. Current per-tick lifecycle

A single `pond run` invocation against a pond with a remote `push`-mode
factory in `/system/run/`:

```
ship.begin_txn(write)
  control_table.record_begin                          # 1 control commit
  data_persistence.begin_write
factory does its work
  -> appends OplogEntry rows in memory (self.records)
guard.commit
  data_tx.commit
    flush_directory_operations
    Delta commit (Delta version N+1)                  # 1 data commit
  control_table.record_data_committed                 # 1 control commit
  run_post_commit_factories
    discover /system/run/* factories
    for each factory in "push" mode:
      control_table.record_post_commit_pending         # 1 control commit
      control_table.record_post_commit_started         # 1 control commit
      execute_push:
        list_transaction_numbers  (remote)
        for version in 1..=current_version:            # O(N) scan
          if not backed_up: open_table; load_version(N); copy parquets
        copy any new _large_files
      control_table.record_post_commit_completed       # 1 control commit
ship.commit_transaction (after guard.commit returns)
  if write && not no-op:
    maintain(force=false, compact=false):
      data table:
        if version % 10 == 0: checkpoint + cleanup_metadata
        vacuum (always; retention=0h)
      control table:
        if version % 10 == 0: checkpoint + cleanup_metadata
        vacuum (always; retention=0h)
```

Per write tick: **6 control-table commits**, **2 Delta vacuums**, **0 to 2
checkpoints**, **0 compactions**.  Push is **O(N) in current Delta version**
plus **one Delta `open_table` per missing version**.

A read tick (`pond list`, `pond cat`):

```
ship.begin_txn(read)
  control_table.record_begin                          # 1 control commit
  data_persistence.begin_read
guard returns data
guard.commit
  data_tx.commit -> Ok(None) (no data written)
  control_table.record_completed                      # 1 control commit
ship.commit_transaction
  is_write is false -> NO maintenance
```

Per read tick: **2 control-table commits**, no maintenance.

`watershop` runs 9 systemd timers (1 selfmon every 1 min, 8 `pond@*` between
10 and 30 min).  Each tick is a write transaction (factories ingest data on
every run).  Selfmon also explicitly invokes `pond maintain` once per tick on
its own pond (`config/scripts/run-selfmon.sh:146`), which forces a checkpoint
regardless of the modulo gate.

---

## 3. Goal: bounded steady state

For each cost dimension, the target after this design is in place:

| Cost | Today | Goal |
|---|---|---|
| Per-tick control-table commits (write) | 6 | 4 (pending+started+completed merged where possible, no read records) |
| Per-tick control-table commits (read) | 2 | 0 |
| Per-tick push CPU | O(N) Delta versions | O(M) where M = txns since last push |
| Per-tick push table re-opens | M (one per missing version) | 0 (use the already-open table) |
| Per-partition parquet count | unbounded | bounded by compact threshold |
| Per-file tlogfs version count | unbounded | bounded by collapse threshold |
| Per-file format-cache parquet count | unbounded | follows tlogfs version count |
| Cold restore | replay all N commits from v1 | fast-forward to snapshot, replay tail |
| Remote bucket size | unbounded | bounded by snapshot retention |
| Partition cache RAM | grows with version count | bounded by LRU |

These five lines tell you whether the design has succeeded:

- selfmon's `parquet.files` line per pond stops growing.
- selfmon's `committed.txn_ids` divided by `size.bytes` stays roughly constant.
- selfmon's `peak_rss.bytes` stops growing with pond age.
- `pond cat /logs/journal/<unit>.jsonl --sql 'SELECT COUNT(*)'` time stops
  growing with version count.
- A fresh restore on a five-year-old pond completes in minutes, not days.

---

## 4. The local data table

State: one Delta table at `{POND}/data/`, partitioned by `part_id` (one
partition per directory in the pond).  Rows are `OplogEntry` records (the
tlogfs oplog).

### 4.1 Checkpoint (existing, keep)

`maintenance::maintain_table` (`crates/steward/src/maintenance.rs:120-158`)
calls `deltalake::checkpoints::create_checkpoint` when
`version > 0 && (force || version % 10 == 0)`.  `CHECKPOINT_INTERVAL = 10` is
deltalake-default and adequate.

No change.

### 4.2 Log cleanup (existing, keep)

`cleanup_metadata` runs only when a checkpoint was just created
(`maintenance.rs:160-176`).  This is correct; we cannot drop log files until
a checkpoint exists to replay them from.

No change to the gate.  One safety adjustment is needed once snapshots and
remote vacuum exist (section 8.4): on the *data* table the JSON commit log is
also the source of truth for `execute_push`'s "what changed in version N" query
(`crates/remote/src/factory.rs:get_delta_commit_files`).  If log cleanup runs
before push has consumed those commits, push cannot back up the new files.
Today this is benign because push runs *before* maintenance in every commit.
The rule we will codify: log cleanup on the data table is gated on
`push_succeeded` or "no remote factory configured".  Already prefigured in
`docs/archive/deltalake-efficiency.md`.

### 4.3 Vacuum (existing, change the gate)

Today vacuum runs **on every commit**, with `VACUUM_RETENTION_HOURS = 0` and
`with_enforce_retention_duration(false)` (`maintenance.rs:178-200`).  It scans
the table to find unreferenced parquets and deletes them.  On a partition
with thousands of small files this scan is not free.

Most ticks add files but remove nothing (only writes that change a directory
or supersede a file produce orphans, and then only if those files are not
referenced by an active version).  Vacuum's value is bounded by the amount
of orphan-creating activity.

Change: vacuum runs only when one of these is true:

- `force == true` (manual `pond maintain`).
- The just-completed Delta commit's action list contained a `Remove` action
  (i.e., something actually superseded a file).
- A configurable interval elapsed (`vacuum_interval`, default = 10 commits =
  same as checkpoint, so vacuum runs when checkpoint runs and otherwise only
  when there's something to do).

Implementation: extend `MaintenanceResult` with a `vacuum_skipped` field for
selfmon visibility.  The "Remove action present" check requires reading the
actions of the last commit; this is cheap because the actions are already in
the open Delta table state.

### 4.4 Compact (new auto behavior)

`maintain_table` already supports compaction via `OptimizeType::Compact`
(`maintenance.rs:202-226`) but auto-maintain calls it with `do_compact=false`
(`crates/steward/src/ship.rs:510`).  Manual `pond maintain --compact` is the
only path today.

Change: introduce **per-partition file-count gating** instead of a
version-modulo trigger.  Gating on Delta version is wrong because partitions
accumulate files at very different rates: `/measure/<pond>.jsonl`'s partition
gains a file every selfmon tick, while a quiet partition might not change
for days.

Trigger: at the end of `maintain_table`, count parquet files per partition
in the table's add list (or via a cached counter).  For any partition with
file count above `COMPACT_PARTITION_THRESHOLD` (default 32), schedule a
compaction targeting that partition.

Compaction call: `table.optimize().with_type(OptimizeType::Compact)
  .with_target_size(COMPACT_TARGET_SIZE)
  .with_filters(&[col("part_id").eq(lit(target)))`.  delta-rs supports
partition filters on optimize; if not, a per-partition optimize is acceptable
because each partition is independent.

Default: ON, gated by `pond config get auto_compact`.  This is safe in the
auto-maintain path **only after** push tolerates missing files (section
8.5), because compaction creates `Remove` actions that vacuum will execute,
and any backup of those parquets must already have happened or be
unreachable.

In the auto-maintain path push has already run (it lives in
`run_post_commit_factories`, which is called from `guard.commit` *before*
`ship.commit_transaction` runs `self.maintain`).  So the safety property
"no parquet is vacuumed before it has been backed up at least once" is
preserved by the existing ordering.  The fragile case is: push *failed* this
tick, then auto-compact runs and vacuum deletes parquets that the next push
will need to reference for older commits.  Section 4.5 addresses this.

### 4.5 Auto-compact safety: skip when push failed

Change: `Ship::commit_transaction` records whether the post-commit `push`
factory succeeded.  Auto-maintain receives that flag and skips both compact
and the vacuum-of-removes when `push_succeeded == false`.  Manual `pond
maintain` is unaffected; the operator is presumed to have decided.

This makes one new control-table column or extends the post-commit
`Completed` record with a `push_succeeded` boolean accessible at maintain
time.

### 4.6 Concurrency

In-process auto-maintain runs sequentially after the commit it followed; no
other process is in the same Delta table for that pond at that moment, so
no Delta `Txn` action is required.  This is the *existing* invariant
documented in `docs/archive/deltalake-efficiency.md` ("Auto-Maintain is
Exempt").

Manual `pond maintain` still uses the lock design from
`docs/archive/deltalake-efficiency.md` (Delta `Txn` action via
`with_commit_properties` plus a control-table `MaintenanceStarted` record
for fast-fail of concurrent `pond run`).  That work is independent of this
design.

---

## 5. The local control table

State: one Delta table at `{POND}/control/`, partitioned by `record_category`.
Rows are `TransactionRecord` (lifecycle), settings, and import partitions.

The control table is, today, two thirds of the on-disk pond
(`docs/archive/deltalake-efficiency.md` baseline: 238 MB of 357 MB).  This
is dominated by per-transaction lifecycle records, which were originally
added to support cron-era debugging and incomplete-transaction recovery.

### 5.1 Stop logging read transactions

Per-read footprint today: `record_begin` at `Ship::begin_txn`
(`crates/steward/src/ship.rs:390`) plus `record_completed` at
`StewardTransactionGuard::commit` (`crates/steward/src/guard.rs:404`) -- two
control-table commits per `pond list`, `pond cat`, `pond log`.

Change:

- Add a constant `LOG_READ_TRANSACTIONS = false`.
- `Ship::begin_txn`: when `is_write == false && !LOG_READ_TRANSACTIONS`, skip
  the `record_begin` call.
- `StewardTransactionGuard::commit`: when `transaction_type ==
  TransactionType::Read && !LOG_READ_TRANSACTIONS`, skip `record_completed`.
- `StewardTransactionGuard::abort`: when read, skip `record_failed` (or
  emit a warning log line instead).
- `StewardTransactionGuard::Drop`: must not panic for read txns that drop
  without commit/abort.  Today the `committed` flag covers this; verify
  that the read path doesn't trip a drop assertion.

Side effect: `pond log` (`crates/cmd/src/commands/control.rs:122-138`)
explicitly filters `WHERE transaction_type IN ('read', 'write')`.  After
this change, no new read records will exist; old read records (from before
the change) will still appear.  Two options:

- **Option A** (recommended): change the filter to `transaction_type =
  'write'` and accept that `pond log` no longer shows reads.  Read history is
  available from journald (`pond run` emits a `Run summary` log line per
  invocation per `selfmon-design.md` section "Pond CLI exit log").
- **Option B**: keep the filter; old read records persist and create an
  inconsistent picture (some windows have reads, later windows do not).

Pick A.  Update `pond log` and `docs/cli-reference.md`.

### 5.2 Per-write footprint

Today: a write transaction with one post-commit `push` factory writes 6
control-table records (Begin, DataCommitted, PostCommitPending,
PostCommitStarted, PostCommitCompleted, plus the optional Failed paths).
Each is its own Delta commit.

Two of these can be merged without changing semantics:

- `record_post_commit_pending` and `record_post_commit_started` are written
  back-to-back synchronously inside `run_post_commit_factories`
  (`crates/steward/src/guard.rs`).  Merge into a single `Started` write.
- `record_completed` for the outer transaction can be folded into the last
  `record_post_commit_*` call when there is exactly one post-commit factory
  (the common case).

These are micro-optimizations; defer until after section 5.1 lands and
selfmon shows the actual per-tick control-table commit rate.

### 5.3 Settings used by the design

The control table already has `set_setting` / `get_setting`
(`crates/steward/src/control_table.rs:1072,1116`).  New keys this design
introduces:

| Key | Type | Default | Used by |
|---|---|---|---|
| `auto_compact` | bool | `true` (after 4.5 lands) | section 4.4 |
| `vacuum_interval` | int | 10 | section 4.3 |
| `compact_partition_threshold` | int | 32 | section 4.4 |
| `collapse_version_threshold` | int | 100 | section 6 |
| `last_pushed_seq:<factory_node_id>` | int | 0 | section 8.2 |
| `snapshot_interval_commits` | int | 0 = disabled | section 9 |

`pond config set <key> <value>` already covers writing them.  Add `pond
config get <key>` if not already present, plus `pond config list` to dump
all settings for operator visibility.

---

## 6. Tlogfs oplog version compaction

The deepest new primitive in this design.

### 6.1 What accumulates

A `FilePhysicalSeries` file (entry type `data:series`, used by
`journal-ingest`, `logfile-ingest`, `weblog-ingest`) gains one new
`OplogEntry` row per write.  Rows live in the data table partition for the
file's parent directory.  At read time,
`OpLogPersistence::async_file_reader_series`
(`crates/tlogfs/src/persistence.rs:2023-2092`) filters non-empty versions
(`record.size.unwrap_or(0) > 0`), reverses to oldest-first, and chains them
through `tinyfs::chained_reader::ChainedReader`.

After 1,440 ticks (one day at 1-min cadence) a single 32 KB JSONL becomes
1,441 OplogEntry rows.  Read CPU is O(N).

Delta-level compaction (section 4.4) does NOT help.  Delta sees one row of
parquet per version; the rows are in different parquets for different commits,
so Delta optimize will merge the parquets but the row count stays the same.
What's needed is a tlogfs-level operation that collapses N versions of one
file into 1.

### 6.2 The collapse operation

For a file with `N` non-empty versions `1..N`:

1. Open the file via the existing read path (table provider for table:series
   types, or `async_file_reader_series` for data:series types).  The read
   already merges all versions correctly.
2. Stream the merged content into a new parquet (for table:series) or new
   raw bytes (for data:series), via `ArrowWriter::write` / direct copy.  Do
   not collect into a `Vec<RecordBatch>`.  Stream batch-by-batch.
3. Allocate a new version `M = get_next_version_for_node(id)`
   (`persistence.rs:1724`).  This preserves the append-only invariant
   ("ALWAYS get next version - never reuse").
4. Write the merged content as version `M` using the existing write path
   (`create_file_path_streaming_with_type` for new content; or for series,
   the same path `journal-ingest` uses).  Set temporal metadata via
   `set_temporal_metadata` if the file is a series.
5. In the same Delta commit, write `OplogEntry` rows for versions
   `1..M-1` with `size = 0`.  These are *marker rows*: they tell future
   reads (which use the existing `r.size.unwrap_or(0) > 0` filter at
   `persistence.rs:2032`) to skip those versions.  No data is duplicated;
   the marker rows are tiny.
6. Single Delta commit means atomicity: if the commit fails, no marker rows
   are written, the existing versions remain readable, and the new merged
   version simply does not exist.  No special rollback logic.

### 6.3 What "version M" looks like physically

For `table:series` (`TablePhysicalSeries`): version M is a single parquet
covering the union of all input versions, written via the same code path as
`write_series_from_batch`.  Reads via the table provider see one large
parquet instead of N small ones.

For `data:series` (`FilePhysicalSeries`, the journal-ingest case): version
M is a single byte stream containing the concatenation of all input version
bytes (which is what `async_file_reader_series` was producing on every read
anyway).  The read path then produces a single-element `ChainedReader` for
version M.

### 6.4 Marker rows and compaction interactions

The marker rows are `OplogEntry` rows with `size = 0` and the original
`version` numbers `1..M-1`.  They exist only to make the existing read-time
filter skip those versions cheaply.

Cleanup of the original (pre-collapse) parquets follows the existing Delta
flow: collapse writes new rows in a new parquet; the old parquets containing
the original-version rows for this file may also contain rows for OTHER
files in the same partition, so they are not freeable.  Delta-level optimize
(section 4.4) eventually re-packs the partition into larger parquets;
individual rows for collapsed versions become storage-overhead-only.

If we want to also free the *space* the original rows occupy, we need a
separate "delete the original-version rows" step.  Decision deferred --
marker rows are tiny enough that "row count grows but byte count is bounded"
is acceptable.  Re-evaluate after measurement.

### 6.5 Triggering

Three trigger options:

- **Manual**: `pond maintain --collapse-versions [path]`.  Operator runs
  this on a known-noisy file.  Always available.
- **Threshold-based auto**: at the end of auto-maintain on the data table,
  for each `(file_id, version_count)` pair where `version_count >
  collapse_version_threshold`, schedule a collapse.  Default threshold:
  100 (one collapse per ~100 minutes of selfmon activity per file).
- **Idle-based**: a separate timer that walks the pond looking for high-
  version-count files and collapses them.  More complex, less timely.

Pick threshold-based auto, gated by `pond config get auto_collapse_versions`
(default OFF until the collapse code is well-tested in manual mode).

Collapse runs as its own write transaction, separate from the user
transaction that triggered it.  This keeps user ticks fast and lets a
collapse failure not poison user data.

### 6.6 Audit and pond log

A collapse is a normal write transaction with `args = ["maintain",
"--collapse-versions", path]` (or the auto-equivalent
`["auto-collapse-versions", path]`).  It writes the same lifecycle records
as any write txn.  `pond log` shows it.  No special audit story is needed;
the lifecycle records already capture "what happened, when, by whom".

### 6.7 Coupling with push and remote

Collapse creates a Delta commit (let's call it Delta version `V`).  This
commit:

- Adds the new merged parquet (for the merged content).
- Adds the marker rows (small parquet).
- (Eventually, after Delta optimize) removes the small parquets that held
  the original rows.

`execute_push` (`crates/remote/src/factory.rs:1119`) backs up the new commit
as a normal Delta version.  Restore replays it as a normal Delta version.
The marker rows survive restore.  The original-version data is also still
present in the backup (it was pushed when those original commits happened).

Cross-pond import (`execute_import`, `factory.rs:1469`) reads
`list_transaction_numbers` from the remote and replays commits in order.
After collapse, an import consumer that was already past version `V-1`
sees a new commit `V` containing the merged content + marker rows;
behaviorally identical to the source pond reading after collapse.  A *fresh*
import consumer starting at watermark 0 replays all commits including the
pre-collapse ones, then sees the collapse commit.  Result: consistent.

The 1:1 relationship between *steward* `txn_seq` and *Delta* version is
preserved because collapse is a normal pond transaction.  Push's watermark
(section 8.2) keys off `txn_seq`, which is correct.

### 6.8 What happens to the format cache

`{POND}/cache/{scheme}_{node_id}/v{version}_{blake3}.parquet`
(`crates/provider/src/format_cache.rs:55`) caches one parquet per source
version.  After collapse, versions `1..M-1` exist as marker rows
(size = 0), so `find_uncached_versions` (`format_cache.rs:75`) does not
return them as needing a re-cache; but the OLD cache files for those
versions still exist on disk.

Cache prune step (section 7) is the cleanup.  Functionally the cache is
correct after collapse -- queries against the file see version `M` only --
but the cache directory grows until pruned.

---

## 7. Format cache pruning

The format cache is a peer of `data/` and `control/` at
`{POND}/cache/`, populated lazily by format providers (oteljson, csv,
jsonlogs, excelhtml).  It is throwaway; `rm -rf cache/` is always safe.
It is never backed up.

### 7.1 What accumulates

For each `(scheme, node_id)`, one parquet per source version.  The cache
backs `jsonlogs://`, `oteljson:///`, etc.  Reads return a `ListingTable`
over the cache directory, which DataFusion scans by listing all files.

`selfmon-design.md` limitation #1: a 32 KB JSONL with 101 source versions
becomes 101 cache parquets (~325 bytes payload each), and a `COUNT(*)`
query takes 7-34 seconds because DataFusion opens 101 file footers per
query.

### 7.2 Two distinct fixes

**Pruning** (cache hygiene): delete cache files whose source version no
longer exists or is now a marker row.  Triggered by tlogfs version-collapse
(section 6).  Implementation: when collapse runs on file `id`, after the
collapse commits, list `cache/{scheme}_{id}/v{V}_*.parquet` for `V in
1..M-1` and delete.  The cache `find_uncached_versions` will rebuild for
version `M` on next read.

Standalone `pond maintain --prune-cache` for orphan recovery: walks
`cache/`, lists active versions per file via `query_records`, deletes any
cache file whose `(node_id, version)` no longer exists.

**File-count reduction** (query cost): even without pruning, the query cost
problem is "open N file footers per query".  This is solved by section 6
(collapse to 1 version) plus pruning.  A separate cache-level "merge all
version parquets into one" optimization is possible but redundant once
collapse runs -- defer.

### 7.3 What about the glob cache?

`cache/{scheme}_glob_{pattern_hash}/` (`format_cache.rs:177`) holds symlinks
into per-file caches for multi-file globs.  After pruning a file's cache,
the corresponding symlinks dangle.  Add a `reset_glob_dir`
(`format_cache.rs:239`) call at the end of any prune that touched files
under the glob, or just call it unconditionally; rebuilding symlinks is
cheap (`ensure_glob_symlinks`, `format_cache.rs:197`).

---

## 8. Remote backup: sync model and maintenance

State: one Delta table per pond per remote, in an S3-compatible bucket.
Schema: chunked-parquet records keyed by `(pond_id, transaction_seq)` plus
a cumulative `_large_files/` set.

### 8.1 Current sync model

Push (`crates/remote/src/factory.rs:1119`) is invoked as a post-commit
factory on every write transaction (`run_post_commit_factories` in
`crates/steward/src/guard.rs`).  It:

1. Calls `remote_table.list_transaction_numbers()` to get all backed-up
   txn numbers (returns a `Vec<i64>` of every previously pushed version).
2. Loops `for version in 1..=current_version` to find the missing ones.
3. For each missing version, opens the local Delta table at
   `pond_path` and `load_version(N)` to read the action list.
4. Copies the new parquets and the commit log JSON for that version to the
   remote.
5. Copies any new large files (cumulative; based on remote `list_files` set
   diff).

Pull / restore (`execute_pull`, `factory.rs:1349`) is the inverse: list
remote files, download anything not present locally.  There is no notion
of "start from a snapshot"; restore is always a full replay from version 1.

The chunked-parquet schema (`crates/remote/src/schema.rs`) does not
distinguish snapshot bundles from per-txn deltas; everything is a per-txn
chunk.

### 8.2 Push watermark

Step 2 of push above is O(N) in `current_version`.  Step 3 re-opens the
Delta table per missing version, which is O(M * log_count) where M is the
number of missing versions.  In steady state, M = 1 per tick, but the O(N)
list and O(N) "is N in the set" check happen every tick.

Change: store `last_pushed_seq:<factory_node_id>` in the control table.
Push reads it, walks `last_pushed_seq + 1 ..= current_version`, and writes
the new value at the end.

`<factory_node_id>` is the `FileID` of the `/system/run/<remote-name>` node;
this scopes the watermark to a specific remote, so a pond with multiple
remotes (e.g., one push to MinIO, one to R2) tracks them independently.

The per-version `open_table().load_version(N)` is still needed to read the
add-action list for that version, but only for the small `M = 1..few`
versions actually missing.  In steady state this is one re-open per tick.

Optional follow-up: avoid the re-open entirely by reading the commit log
JSON directly (`_delta_log/{N:020}.json`) without going through `open_table`.
The action list is in there; we don't need a fully-loaded `DeltaTable`
state to extract it.

This change is independent of every other section.  Land first.

### 8.3 Push tolerates missing source files

Step 4 of push reads each parquet from the local `object_store::get`.  If a
prior auto-compact ran and vacuum deleted the small parquets that an
older-but-still-unbacked-up commit referenced, push fails with NotFound.

Today this race is closed by ordering (push runs before
auto-maintain-with-compact, both within the same `pond run` invocation).
After auto-compact lands (section 4.4), the only way to hit it is "push
failed this tick, then auto-maintain ran, then next tick push tries to
re-back-up the version that failed".  Section 4.5 prevents this: skip
auto-compact when push failed.

But section 4.5 protects only the **next** tick, not arbitrary future
ticks where push might still need pre-compact data.  Belt-and-suspenders
fix: when push encounters NotFound on a parquet referenced by version N,
check whether a later commit's compaction superseded that file (i.e., the
parquet is in a `Remove` action of some commit `> N`).  If yes, skip the
file silently; the data has been backed up via the superseding commit's
add list.  If no, error.

Implementation: read the action histories of commits `N..=current_version`
and build a "removed" set; intersect with the missing set.

This guarantees that auto-compact + auto-vacuum + delayed push never lose
data on the backup side.

### 8.4 Remote checkpoint, log cleanup, vacuum, compact

The remote Delta table has had **zero maintenance** since inception.  This
is the largest single pile of unbounded growth in the design.

Change: invoke `maintain_table` against the remote table.  Two contexts:

- **Per-tick from `pond run`**: after a successful push, run a lightweight
  remote maintenance: checkpoint if interval, vacuum if it's been a while.
  This is cheap because it operates on the remote's `_delta_log/`, not on
  bulk data.  Skip compact in the per-tick path.
- **Operator: `pond maintain --remote`**: full maintenance including
  compact.  Uses Delta `Txn` action for cross-process coordination
  (multiple ponds may push to the same bucket; per
  `docs/archive/deltalake-efficiency.md`).

The remote Delta `Txn` action approach already designed in
`docs/archive/deltalake-efficiency.md` carries over verbatim; it solves the
coordination problem at the protocol level.

### 8.5 Remote large_files cleanup

`_large_files/blake3=<hash>` blobs accumulate cumulatively (push only ever
adds, never removes).  Today there is no removal mechanism.  After local
vacuum/compact deletes a large-file blob locally (because no remaining
oplog version references it), the remote copy lingers forever.

Change: when push computes its diff between local and remote large-files,
also identify remote blobs whose blake3 is no longer referenced by any
backed-up commit's action list.  This requires knowing, for each remote
commit, which large-file blakes it references -- which we already encode
in the chunked-record schema (`crates/remote/src/schema.rs`).  Building the
"all currently-referenced blakes" set is a single pass over all backed-up
commits' record bundles; cache the result in the control table or a small
side-state file in the bucket.

For the per-tick path, defer this to the operator command (`pond maintain
--remote --vacuum-blobs`) to keep per-tick push cheap.

---

## 9. Snapshots and fast restore

After section 8 lands, the remote backup has bounded log/data growth via
maintenance, and per-tick push is O(M).  But cold restore still walks
`commits 1..=N` -- and N may be six months of one-minute ticks.

### 9.1 Snapshot bundle

A *snapshot* is a single artifact in the remote bucket containing:

- A Delta checkpoint parquet for the *local* data table at version `S`
  (the snapshot version, equal to a specific `txn_seq`).
- The complete `_large_files/` blob set referenced as of version `S`.
- The control-table state at version `S` (settings, factory modes, import
  partitions, post-commit history through `S`).
- A manifest file naming the above.

Bundle layout:

```
s3://<bucket>/snapshot=<txn_seq>/
  manifest.json
  data/_delta_log/00000000000000000000.checkpoint.parquet
  data/_delta_log/_last_checkpoint
  control/_delta_log/00000000000000000000.checkpoint.parquet
  control/_delta_log/_last_checkpoint
  large_files/                   (or symlinks back to bucket-level _large_files/)
```

### 9.2 Producing a snapshot

Triggered by:

- `pond maintain --snapshot` (operator).
- Automatic after every `snapshot_interval_commits` commits (control-table
  setting, default 0 = disabled).

Work: build the data and control checkpoint parquets locally (we already
have these from section 4.1), enumerate the `_large_files/` set, write the
manifest, and upload to `snapshot=<seq>/` in the bucket.  Use a Delta `Txn`
action on the remote table so two ponds cannot simultaneously produce a
snapshot at the same `seq`.

### 9.3 Restoring from a snapshot

`pond restore` (today: replay all commits) gains a new mode:

1. Look for the highest `snapshot=<seq>/` in the bucket.  If none exists
   (or `--no-snapshot` was passed), fall back to today's full-replay
   restore.
2. Initialize a fresh local pond by downloading the snapshot's data and
   control checkpoints, the referenced large files, and the manifest.
3. Set `last_pushed_seq:<factory_node_id> = seq` (no need to re-push what
   we just downloaded).
4. Replay only commits `seq+1 ..= current_remote_version` from the remote.

Restore time: one snapshot download + tail replay.  For a six-month-old
pond with a daily snapshot, this is hours of data instead of months.

### 9.4 Cross-pond import bootstrap

`execute_import` (`factory.rs:1469`) currently iterates
`remote_table.list_transaction_numbers()` and replays everything past its
watermark.  After remote vacuum (section 9.5) drops pre-snapshot per-txn
chunks, a fresh import consumer (watermark = 0) cannot find commits
1..snapshot_seq.

Change: import bootstrap consults `snapshot=<seq>/` first.  If a snapshot
exists and the consumer's watermark is below `seq`, the consumer:

1. Downloads the snapshot's per-partition records relevant to the imported
   path.
2. Sets watermark = `seq`.
3. Continues with the normal "replay commits past watermark" loop.

This requires the snapshot manifest to identify which partition records
exist under each foreign top-level partition, so an import consumer can
extract just the slice it cares about.  Not free, but bounded.

### 9.5 Remote vacuum (drop pre-snapshot chunks)

Once snapshots exist, the per-txn bundles below the *oldest* snapshot can
be dropped.  This is the only mechanism that keeps the remote bucket
bounded.

`pond maintain --remote --vacuum-snapshots` (or the per-tick equivalent if
desired): walks `bundle=<pond_id>:txn=<seq>/`, deletes any bundle with
`seq < min(snapshot_seq)`.  Also walks `_large_files/` and deletes blobs
no longer referenced by any retained snapshot or post-snapshot commit
(section 8.5 generalized).

Snapshot retention: keep at least `snapshot_retention_count` snapshots
(default 2) so that an import consumer that's slow to update can still
bootstrap.  Configurable per-remote via control-table settings.

---

## 10. Database I/O hotspots

These are independent of the maintenance model but interact with it.

### 10.1 Partition cache

`OpLogPersistence::ensure_partition_cached`
(`crates/tlogfs/src/persistence.rs:2989`) runs `SELECT * FROM delta_table
WHERE part_id = '<id>' ORDER BY timestamp DESC` on the first access to any
partition, deserializes every row into `Vec<OplogEntry>`, and stuffs
everything into `partition_records_cache: HashMap<PartID, HashMap<NodeID,
Vec<OplogEntry>>>`.  Subsequent calls hit the cache.

Memory grows with version count (`docs/archive/journal-memory-monitor-ingest.md`
records this as a primary memory growth driver).

Change: replace the unconditional whole-partition load with per-(part_id,
node_id) lookups, backed by a bounded LRU.

Implementation:

- Replace `partition_records_cache` with `node_records_cache: LruCache<
  (PartID, NodeID), Vec<OplogEntry>>` keyed by node-not-partition.
- New per-node SQL: `SELECT * FROM delta_table WHERE part_id = ? AND
  node_id = ? ORDER BY version DESC` -- predicates are partition + filter,
  so DataFusion uses partition pruning + parquet row-group skipping.
- LRU bound: cache size in entries (not bytes); each entry is a small
  `Vec<OplogEntry>`.  Default 1024 entries.
- Audit every call site of `ensure_partition_cached`:
  `query_records`, `lookup_directory_node`, `query_directory_entries_by_id`,
  `get_factory_for_node`, the `flush_directory_operations` directory snapshot
  reads.  Each site must continue to merge committed-cache results with
  pending in-memory `self.records`.

The merge pattern (committed + pending, pending wins on tie) is already
established (e.g., `lookup_directory_node` at `persistence.rs:2950-2984`).
Generalize and apply uniformly.

This is the single largest RAM win in the design.  It also unlocks
section 6: tlogfs version-collapse becomes cheap because we don't load N
versions just to find them; we look up version M-1, M-2, ... as needed.

### 10.2 Push: O(N) list and per-version table re-open

Covered by section 8.2.  Cross-listed here so the I/O hotspot inventory is
complete.

### 10.3 Restore: replay from version 1

Covered by section 9.3.

### 10.4 Read-tx control writes

Covered by section 5.1.

### 10.5 Vacuum every commit

Covered by section 4.3.

### 10.6 Format cache: open N footers per query

Covered indirectly by section 6 (collapse reduces version count) plus
section 7 (prune removes orphans).  No standalone fix needed.

---

## 11. Per-tick lifecycle, after the design

Write tick (the dominant case on selfmon's 1-min cadence):

```
ship.begin_txn(write)
  control_table.record_begin                                 # 1 control commit
factory work
guard.commit
  Delta data commit                                          # 1 data commit
  control_table.record_data_committed                        # 1 control commit
  run_post_commit_factories
    record_post_commit_started (merged pending+started)      # 1 control commit
    execute_push:
      read last_pushed_seq from control                      # 0 commits (read)
      if current_version > last_pushed_seq:
        for v in last_pushed_seq+1 ..= current_version:      # M iterations, M=1 in steady state
          read commit log JSON, copy parquets, copy large files
        write last_pushed_seq                                # included in control_completed
    record_post_commit_completed (write last_pushed_seq too) # 1 control commit
ship.commit_transaction
  if write && push_succeeded:
    maintain_data:
      checkpoint (gated by version % 10)                     # 0 or 1 Delta commits
      log cleanup (gated by checkpoint just ran)
      vacuum (gated by Remove-action present OR interval)    # 0 or 1 Delta commits
      compact (gated by partition file count > 32)           # 0 or 1 Delta commits
      collapse-versions (gated by per-file version count)    # 0 or 1 separate write txn
    maintain_control:
      same shape, no compact gating needed
    maintain_remote (per-tick lightweight):
      checkpoint (gated by remote version % 10)              # 0 or 1 remote commits
```

Per write tick, steady state: **4 control-table commits** (down from 6),
**1 push iteration** (down from O(N)), **0-2 Delta data commits** for
maintenance (vs 1 vacuum every tick).

Read tick:

```
ship.begin_txn(read)
  (no control-table write)
data read
guard.commit
  data_tx.commit -> Ok(None)
  (no control-table write)
```

Per read tick: **0 control-table commits** (down from 2).

---

## 12. Coupling and safety rules

Everything that must hold for the design to be correct:

1. **In-process auto-maintain is safe without locks.** It runs sequentially
   after the commit it followed, in the same process, on a Delta table no
   other process is currently modifying for that pond.  No Delta `Txn`
   action needed.  Manual `pond maintain` is the only path that needs
   coordination, and the Delta `Txn` design from
   `docs/archive/deltalake-efficiency.md` covers it.

2. **Push happens before auto-maintain in every commit.** In
   `Ship::commit_transaction`, `guard.commit()` (which calls
   `run_post_commit_factories` -> push) returns *before* `self.maintain()`
   runs.  Vacuum cannot delete a parquet that the same tick's push has not
   referenced.  This is the foundational invariant; do not change the
   ordering.

3. **Auto-compact is skipped when push fails this tick.** Otherwise,
   compaction's Remove actions plus vacuum could delete parquets that the
   *next* tick's push will need to re-back-up.  Belt: push tolerates
   missing files for parquets superseded by a later compaction (section
   8.3).

4. **Vacuum on the data table cleans logs only when push succeeded.**
   Inherited from `docs/archive/deltalake-efficiency.md`; `do_cleanup`
   parameter on `maintain_table`.  Already correctly gated for in-process
   auto-maintain (push always runs first).  Manual maintain needs the
   gate explicitly.

5. **Tlogfs version-collapse allocates a new version M.** The append-only
   invariant (`get_next_version_for_node` "ALWAYS get next version - never
   reuse") is not optional; it is the basis of read consistency.  Collapse
   adds version M, marks `1..M-1` with `size=0` markers in the same
   atomic Delta commit.

6. **Marker rows survive forever (until the file is deleted).** They are
   the mechanism by which old code paths see the merged result.  Do not
   garbage-collect marker rows independently of file deletion.

7. **Push watermark keys off `txn_seq`, not Delta version.** Today they
   are 1:1 (`crates/remote/src/factory.rs:1168`), but tlogfs
   version-collapse will create Delta commits that are still part of the
   same pond `txn_seq` (or arguably allocate their own; either way, key
   off the steward's `txn_seq`, not the Delta version).

8. **`pond log` filters out reads after section 5.1.**  The filter at
   `crates/cmd/src/commands/control.rs:122-138` becomes
   `transaction_type = 'write'`.  Read history lives in journald, surfaced
   via `pond run`'s `Run summary` log line per `selfmon-design.md`.

9. **Format-cache pruning fires when tlogfs version-collapse fires.** Same
   transaction or immediately after.  Cache is throwaway, so a brief
   inconsistency is acceptable, but pruning should be coupled to collapse
   so the operator does not have to remember.

10. **Snapshot bundles use Delta `Txn` for two-pond contention.** Same
    pattern as remote-table maintenance; one bucket can hold backups for
    only one pond_id (this is enforced by `extract_pond_id` at
    `crates/remote/src/factory.rs:1145`), so the contention is between
    two replicas of the same pond, which is rare but possible.

11. **Remote vacuum requires a snapshot newer than the deletion target.**
    Never delete a per-txn bundle below the oldest retained snapshot.
    Snapshot retention count is the only safety knob between operator
    intent and accidental data loss.

---

## 13. Operator surface

CLI commands changed or added:

| Command | Status | Notes |
|---|---|---|
| `pond maintain` | existing | Now also runs auto-compact and per-file collapse if thresholds exceeded; preserves `--compact` for "compact regardless of threshold" |
| `pond maintain --compact` | existing | Unchanged |
| `pond maintain --collapse-versions [path]` | new | Tlogfs version-collapse, single file or all |
| `pond maintain --prune-cache` | new | Format cache hygiene |
| `pond maintain --remote` | new | Run maintenance against the remote backup table |
| `pond maintain --snapshot` | new | Produce and upload a snapshot bundle |
| `pond maintain --remote --vacuum-snapshots` | new | Drop pre-snapshot remote bundles + unreferenced large files |
| `pond restore` | existing, mode added | New: snapshot-aware fast restore; old: full replay |
| `pond config set <key> <value>` | existing | New keys per section 5.3 |
| `pond config get <key>` | new | Read a single setting |
| `pond config list` | new | Dump all settings |
| `pond log` | existing, filter changed | `transaction_type = 'write'` only |

Selfmon additions (per `selfmon-design.md`):

- `maintain.duration_seconds` (per-table, per-step)
- `push.duration_seconds`, `push.bytes`, `push.skipped_for_failure`
- `compact.partition_count`, `compact.files_added`, `compact.files_removed`
- `collapse.file_count`, `collapse.versions_collapsed`
- `cache.parquet_files_pruned`
- `oplog.versions_per_file_p99`
- `remote.bucket_size_bytes`
- `remote.snapshot_count`

---

## 14. Implementation sequencing

Each section is independently shippable.  The sequence below minimizes
operator risk:

1. **Section 5.1** (stop logging read transactions).  Touches `Ship` and
   `StewardTransactionGuard` only; instant impact on control-table size.
2. **Section 4.3** (gate vacuum).  Pure measurement + a knob.
3. **Section 8.2** (push watermark).  Independent value; makes per-tick
   push O(1) in the common case.
4. **Section 10.1** (partition cache LRU).  Largest RAM win; touches many
   call sites; needs careful test coverage.
5. **Section 8.3** (push tolerates missing files).  Required before
   section 4.4 in production.
6. **Section 4.4 + 4.5** (auto-compact, gated by push success).  Default
   ON only after section 8.3 lands.
7. **Section 8.4** (remote per-tick maintenance).  Bounds remote log
   growth; independent of everything else after section 8.2.
8. **Section 6** (tlogfs version-collapse).  Manual command first; auto
   trigger after measurement.
9. **Section 7** (format cache prune).  Coupled to section 6's manual
   command; auto-prune coupled to auto-collapse.
10. **Section 9.1, 9.2** (snapshot produce).  No restore path yet; just
    upload.
11. **Section 9.3** (snapshot-aware restore).  Reads what step 10
    produces.
12. **Section 9.5** (remote vacuum, including blob cleanup section 8.5).
    The point at which the remote becomes truly bounded.
13. **Section 9.4** (cross-pond import snapshot bootstrap).  Required if
    remote vacuum runs on buckets that import consumers depend on.

Steps 1-3 are days of work each.  Steps 4-6 are weeks.  Steps 8-9 are
the architectural longpole and depend on the rest.

Selfmon must show, after each step, that the targeted cost dimension
moved.  If it does not, do not proceed -- diagnose first.

---

## 15. Open questions and decisions

These are the points where the design stops and the operator (or a
follow-up design discussion) decides:

1. **Default for `auto_compact`**: ON or OFF on first rollout?
   Recommendation: OFF until section 8.3 lands in production and one full
   selfmon week shows no push-failure cascade; then ON.

2. **`compact_partition_threshold` default**: 32 chosen by analogy to
   typical small-file thresholds; the right number depends on partition
   read access patterns.  Tunable per-pond via control-table settings.

3. **`collapse_version_threshold` default**: 100 is a guess.  A
   selfmon-driven study could pick the version count where read latency
   crosses, e.g., 100 ms.

4. **Snapshot frequency**: per-N-commits, per-time-interval, or operator-
   triggered only?  Recommend operator-triggered for the first version of
   the feature; auto-snapshot once the cost is measured.

5. **Snapshot retention**: keep last K snapshots vs keep snapshots newer
   than D days.  Both are simple; defer the decision to first deployment.

6. **`pond log` reads**: drop entirely from the filter (section 5.1
   Option A) or keep with awareness that old records will appear and new
   ones won't?  Recommend Option A; document the source-of-truth for
   read history (journald + `Run summary` log line).

7. **Cache-level merge optimization**: after sections 6 and 7, is the
   format cache "open N footers per query" cost still meaningful?  If yes,
   consider building a multi-version-merge step at the cache layer.  If
   no, this design is complete.

8. **Marker-row footprint**: at very high version counts (millions),
   marker rows themselves become a non-trivial cost.  At what version
   count does it matter, and is a "delete the original-version rows
   entirely" follow-up needed?  Measure first.

9. **Remote table compaction in the per-tick path**: the remote is also
   subject to small-files accumulation.  This design defers remote
   compaction to operator commands.  If multi-pond buckets become
   contended, revisit and add per-tick remote compaction with Delta
   `Txn` coordination.

10. **The tlogfs version-collapse code path for `data:series` types**:
    the collapse reads via `async_file_reader_series` (which does the
    chained-reader merge already).  This is a self-referential read --
    the collapse must read all N versions one last time to produce
    version M.  For files with thousands of versions and gigabytes of
    content, that one last read is expensive.  Acceptable cost for a
    one-time collapse, but operator must be aware.

---

## Related documents

- `docs/cli-reference.md` -- operator-facing command details
- `docs/duckpond-system-patterns.md` -- transaction and persistence
  patterns this design builds on
- `docs/archive/deltalake-efficiency.md` -- prior plan, superseded by
  this one
- `docs/archive/journal-memory-monitor-ingest.md` -- the original
  identification of the version-accumulation problem
- `docs/format-cache-design.md` -- format cache architecture
- `docs/format-cache-implementation.md` -- implementation status
- `../docs/selfmon-design.md` (repo root) -- the measurement infrastructure
  that makes any of this verifiable
