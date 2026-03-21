# Cross-Pond Import: Implementation Status

## What Works

Cross-pond import is functional for **flat directory structures** (test
530 passes 6/6). The full pipeline:

1. **`pond run host+remote:///config.yaml list-ponds`** -- scans an S3
   bucket for `pond-{uuid}/` prefixes, reports pond IDs and commit counts
2. **`pond run host+remote:///config.yaml show`** -- reads the foreign
   OpLog directly from backup via `ChunkedAsyncFileReader`, displays
   the full directory tree with entry types, sizes, and versions
3. **`pond mknod remote /system/etc/10-import --config-path import.yaml`**
   -- reads foreign backup, navigates directory tree to find `source_path`,
   creates local directory at `local_path` with the foreign `FileID`
4. **`pond run 10-import pull`** -- downloads foreign partition parquet
   files, registers them as Delta Add actions at commit time
5. **`pond list`, `pond cat`** -- imported data is queryable at the
   local path, provenance preserved via `pond_id` column

## What Is In Progress

### Recursive import (`source_path: "/logs/**"`)

The `/**` pattern triggers recursive discovery of child physical
directories during mknod. This works:
- mknod correctly discovers all child directories in the foreign pond
- Creates local directory entries for all of them with foreign FileIDs
- `register_empty_directory()` makes them immediately usable as
  parents within the same transaction
- Pull collects all partition IDs from local directory entries
- Downloads parquet files for all partitions
- Registers external Add actions via `State::add_external_parquet()`

**The bug:** after pull completes, the imported data is not visible
via `pond list` or `pond cat`. The root cause is in the interaction
between the external Add commit and the tlogfs transaction lifecycle:

The external Add actions are committed as a **follow-up Delta commit**
after the normal OpLog record write, within `commit_impl()`. This
creates Delta version N+1 (or just N if there are no OpLog records).
When `OpLogPersistence::commit()` reloads the table, it should see
both commits. But the `last_txn_seq` tracking and partition cache
may not account for the extra Delta version correctly.

The specific failure mode in test 531:
- `pond list '/imported/recursive/**'` shows directories (sensors,
  logs) but not files (temps.csv, events.csv)
- The directories exist because they were created at mknod time
  (in the local partition)
- The files should exist in the foreign partition data (committed
  as external Add actions) but are not visible

### Debugging leads

1. The external commit happens (confirmed by log output):
   "Committing 7 external Add action(s) for imported files"
2. But `pond list` opens a fresh transaction afterward and
   doesn't see the imported partition data
3. Possible causes:
   - The partition cache in the new transaction doesn't load the
     externally-committed data because `ensure_partition_cached()`
     queries the Delta table which may not reflect the latest version
   - The table reload in `OpLogPersistence::commit()` picks up the
     wrong version (it reads `last_txn_seq` from commit metadata,
     but the external commit has no `PondTxnMetadata`)
   - The two-commit approach (normal write + external Add) may
     confuse the version tracking

## Architecture Decisions Made

### External Add Actions in tlogfs

To maintain the single-transaction invariant, imported parquet files
are registered via `State::add_external_parquet()` during factory
execution, then committed as Delta Add actions at transaction commit
time in `commit_impl()`. This replaces the earlier design of using
`CommitBuilder` directly in `execute_import()`, which violated the
one-transaction rule.

The current implementation uses a **follow-up commit** within
`commit_impl()` -- after the normal `DeltaOps::write()` for OpLog
records, a `CommitBuilder` commit adds the external files. This is
not ideal (two Delta versions per transaction) but preserves the
invariant that all changes happen within `commit_impl()`.

The ideal fix: use `CommitBuilder` for EVERYTHING -- serialize OpLog
records to parquet manually, write them to the object store, and
create Add actions for both OpLog files and imported files in a
single `CommitBuilder` commit. This would be a larger refactor of
`commit_impl()` but would produce exactly one Delta version per
transaction.

### Partition ID Discovery

At pull time, partition IDs are discovered from **local directory
entries** (created at mknod time), not by re-reading the foreign
OpLog. This means:
- Pull is fast (no foreign backup read needed)
- New subdirectories added to the foreign pond after mknod require
  re-running mknod (with `--overwrite`)
- The `collect_child_part_ids_from_entries()` function reads
  directory entries from the local OpLog via
  `State::query_directory_entries_by_id()`

### New tlogfs APIs

Added to support cross-pond import:

| API | Purpose |
|-----|---------|
| `State::register_empty_directory(id)` | Make new directory immediately usable for child insertion within same transaction |
| `State::add_external_parquet(action)` | Register imported parquet file for Delta commit at transaction end |
| `State::query_directory_entries_by_id(id)` | Read directory entries from OpLog without opening the directory node |
| `ExternalAddAction` struct | Describes an external parquet file (path, size, part_id) |
| `PartitionNotFound` error variant | Clear error when accessing unimported foreign partition, suggests `/**` pattern |
| `FS::wd()` made public | External directory traversal |
| `WD::insert_node()` | Insert pre-created node with foreign FileID |

## Files Changed

### Core changes (Phase 1 + Phase 2)

| File | Changes |
|------|---------|
| `crates/tlogfs/src/schema.rs` | `pond_id` field on OplogEntry |
| `crates/tlogfs/src/persistence.rs` | pond_id stamping, ExternalAddAction, register_empty_directory, partition cache integration |
| `crates/tlogfs/src/txn_metadata.rs` | pond_id in commit metadata |
| `crates/tlogfs/src/error.rs` | PartitionNotFound variant |
| `crates/tlogfs/src/large_files.rs` | Column index updates for pond_id |
| `crates/tinyfs/src/wd.rs` | insert_node() |
| `crates/tinyfs/src/fs.rs` | wd() made public |
| `crates/remote/src/factory.rs` | ImportConfig, initialize_remote, execute_import, list-ponds, show, recursive import |
| `crates/remote/src/chunked_async_reader.rs` | New file: AsyncFileReader over chunked backup |
| `crates/remote/src/schema.rs` | pond_id in ChunkedFileRecord, bundle_id format |
| `crates/remote/src/table.rs` | pond_id on RemoteTable |
| `crates/remote/src/writer.rs` | pond_id passthrough |
| `crates/utilities/src/chunked_files.rs` | pond_id in Arrow schema |
| `crates/steward/src/ship.rs` | pond_id threading |
| `crates/steward/src/guard.rs` | pond_id in factory execution |

### Tests

| File | Description |
|------|-------------|
| `testsuite/tests/530-cross-pond-import-minio.sh` | Flat import: passes 6/6 |
| `testsuite/tests/531-recursive-cross-pond-import.sh` | Recursive import: 2/11 (directories created, data not visible) |

## Next Steps

1. **Fix the data visibility bug** -- investigate why externally-committed
   partition data isn't visible after table reload. Likely a version
   tracking or partition cache issue.

2. **Consider single-commit approach** -- refactor `commit_impl()` to
   use `CommitBuilder` for all writes (OpLog records + external files)
   in one Delta commit.

3. **Flat import error test** -- test 531's flat import checks fail
   because the pull succeeds (downloads data) before the list check.
   The error only appears on subsequent access. Adjust test flow.

4. **mDNS issue** -- Rust HTTP client doesn't support `.local` domains.
   Causes silent 57s hangs. Separate issue from import.
