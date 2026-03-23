# Cross-Pond Import: Implementation Status

## Status: Working

Both flat and recursive cross-pond import are functional and tested.

- Test 530 (flat import): **6/6 checks pass**
- Test 531 (recursive import): **11/11 checks pass**

## How It Works

### Exploration

```bash
# List ponds available in a remote bucket
pond run host+remote:///config.yaml list-ponds

# Show the full directory tree of a remote pond
pond run host+remote:///config.yaml show
```

### Import Setup

```bash
# Create import factory (discovers foreign partition structure)
pond mknod remote /system/etc/10-import --config-path import.yaml

# Pull data from foreign pond
pond run 10-import pull
```

### Config Format

```yaml
url: "s3://bucket/pond-{foreign-uuid}"
endpoint: "http://host:9000"
region: "us-east-1"
access_key: "..."
secret_key: "..."
allow_http: true
import:
  source_path: "/logs/**"           # /** for recursive
  local_path: "/sources/workshophost"
```

## Architecture

### Data Flow

1. **mknod** reads the foreign OpLog via `ChunkedAsyncFileReader`,
   navigates the directory tree to find `source_path`, and creates
   a local directory at `local_path` with the foreign `FileID`
   (same `part_id` and `node_id` as the foreign directory).

2. **pull** discovers all partition IDs to import:
   - Top-level `part_id` from the local parent directory entry
   - Child `part_id` values from the foreign OpLog (for `/**` imports)
   - Downloads matching parquet files from the backup
   - Writes to local object store
   - Registers as Delta Add actions via `State::add_external_parquet()`
   - Committed at transaction end in `commit_impl()`

3. **query** works immediately -- the local directory references the
   foreign partition, and the imported parquet files are in the Delta
   table. `pond list`, `pond cat`, and SQL queries all work.

### Key Design Points

**Foreign FileIDs.** The import directory is created with
`FileID::new_from_ids()` using the foreign pond's `part_id` and
`node_id`. This means imported parquet files (which contain the
foreign `part_id` in their partition path) land in the correct
partition automatically.

**No local empty records.** `register_empty_directory()` caches the
directory in memory (for child insertions during mknod) but does NOT
persist an empty OpLog record. The real directory content comes from
the imported foreign parquet files. This avoids shadowing foreign
records with local empty ones.

**External Add actions.** Imported parquet files are registered via
`State::add_external_parquet()` and committed as Delta Add actions
in `commit_impl()` via `CommitBuilder`. This happens as a follow-up
commit after the normal OpLog write, with the same `PondTxnMetadata`
for correct version tracking.

**Partition discovery.** On each pull, `execute_import` discovers
partition IDs by reading the parent directory entry (top-level) and
the foreign OpLog (child partitions for recursive imports). This
avoids the need for two pulls. Future optimization: cache the
partition list in the control table.

## New tlogfs APIs

| API | Purpose |
|-----|---------|
| `State::register_empty_directory(id)` | Cache directory for child insertion without persisting empty record |
| `State::add_external_parquet(action)` | Register imported parquet file for Delta commit at transaction end |
| `State::query_directory_entries_by_id(id)` | Read directory entries from OpLog without opening directory node |
| `ExternalAddAction` struct | Describes an external parquet file (path, size, part_id) |
| `PartitionNotFound` error variant | Clear error for unimported foreign partition, suggests `/**` pattern |
| `FS::wd()` made public | External directory traversal |
| `WD::insert_node()` | Insert pre-created node with foreign FileID |

## Known Issues

1. **mDNS (.local) resolution.** Rust HTTP client doesn't support
   mDNS. Hostnames like `workshophost.local` cause silent 57-second
   hangs. Use IP addresses or `/etc/hosts` entries instead.

2. **Two Delta versions per import.** The external Add commit creates
   a separate Delta version from the OpLog record write. Ideally
   these would be a single commit. Requires refactoring `commit_impl`
   to use `CommitBuilder` for all writes.

3. **Partition discovery on every pull.** Recursive imports re-read
   the foreign OpLog on each pull to discover partition IDs. Should
   cache the partition list in the control table for efficiency.

4. **No incremental sync.** Pull downloads all matching partition
   files, skipping only those already present in the local object
   store (via `head()` check). No transaction-level tracking of
   what has been imported.

## Files Changed

| File | Changes |
|------|---------|
| `crates/tlogfs/src/schema.rs` | `pond_id` field on OplogEntry |
| `crates/tlogfs/src/persistence.rs` | pond_id stamping, ExternalAddAction, register_empty_directory, partition cache |
| `crates/tlogfs/src/txn_metadata.rs` | pond_id in commit metadata |
| `crates/tlogfs/src/error.rs` | PartitionNotFound variant |
| `crates/tlogfs/src/large_files.rs` | Column index updates for pond_id |
| `crates/tinyfs/src/wd.rs` | insert_node() |
| `crates/tinyfs/src/fs.rs` | wd() made public |
| `crates/remote/src/factory.rs` | ImportConfig, initialize_remote, execute_import, list-ponds, show, recursive import |
| `crates/remote/src/chunked_async_reader.rs` | New: AsyncFileReader over chunked backup |
| `crates/remote/src/schema.rs` | pond_id in ChunkedFileRecord, bundle_id format |
| `crates/remote/src/table.rs` | pond_id on RemoteTable |
| `crates/remote/src/writer.rs` | pond_id passthrough |
| `crates/utilities/src/chunked_files.rs` | pond_id in Arrow schema |
| `crates/steward/src/ship.rs` | pond_id threading |
| `crates/steward/src/guard.rs` | pond_id in factory execution |
| `testsuite/tests/530-cross-pond-import-minio.sh` | Flat import test: 6/6 |
| `testsuite/tests/531-recursive-cross-pond-import.sh` | Recursive import test: 11/11 |
