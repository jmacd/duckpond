# Cross-Pond Import: Implementation Status

## Summary

Cross-pond import is functionally complete through Phase 2.5.
A producer pond's data and dynamic factories (timeseries-join,
temporal-reduce, etc.) work correctly when imported into a consumer
pond. All 51 integration tests and 848+ unit tests pass.

## What Was Done (prior sessions)

### 1. pond_id scoping in the persistence layer

The persistence layer (`tlogfs/persistence.rs`) now filters committed
records by `pond_id` when looking up nodes. This is the foundation for
cross-pond import: two ponds can have records in the same partition
(e.g., the well-known root partition `00000000-0000-7100-8000-000000000000`)
and queries will return only the records belonging to the correct pond.

**Key changes:**

- `PersistenceLayer` trait gained a `pond_uuid()` method (default returns
  `local_pond_uuid()` for backward compatibility with memory/hostmount)
- `State` implements `pond_uuid()` using `self.pond_id` (the real UUID
  from `OpLogPersistence`)
- `CachingPersistence` forwards `pond_uuid()` to inner persistence
- `FS::root()` now calls `FileID::root_for(self.persistence.pond_uuid())`
  instead of `FileID::root()` (which used the placeholder `LOCAL_POND_ID`)
- `initialize_root_directory(&mut self, pond_id)` takes the real pond UUID
- Three query functions filter committed records by `pond_id`:
  - `query_latest_record`
  - `query_latest_directory_record`
  - `query_records`
- Pending (uncommitted) records are NOT filtered by `pond_id` because
  their `pond_id` field is empty until commit-time stamping. This is
  correct: pending records are always from the local pond's current
  transaction.

### 2. Simplified import architecture

The import creates ONE directory entry per mount point. Previously,
`create_child_dirs_recursive` walked the foreign OpLog and created
individual local directory entries for each child physical directory.
This was wrong -- the foreign partition data already contains the full
directory tree. When the persistence layer loads a foreign directory's
content, the IPC-encoded entries list all children (physical dirs,
dynamic dirs, files). Traversal into child physical directories works
because `execute_import` downloaded those child partitions' parquet
files too.

**Removed:** `create_child_dirs_recursive` (was ~60 lines, filtered
only `dir:physical`, dropped dynamic-dirs and files)

**Kept:** `collect_partitions_recursive` -- this reads the foreign
directory tree to discover partition IDs for the pull step. It does
NOT create local entries.

### 3. Foreign pond_id extraction

`extract_foreign_pond_id()` queries the foreign OpLog for its `pond_id`
value. This UUID is used when creating the mount point `FileID` via
`create_foreign_dir()`, replacing the incorrect `local_pond_uuid()`.

### 4. Fixed Ship::create_pond double-UUID bug

`Ship::create_pond` was calling `PondMetadata::default()` twice: once
in `create_infrastructure` (line 208, used for data persistence) and
once in `create_pond` (line 90, overwrote the control table). These
generated different UUIDs. Fixed by reusing the metadata already set
by `create_infrastructure`.

### 5. Cross-pond example (`cross/`)

A new example directory that imports noyo, water, and septic from
their S3 backups into a fourth pond with a combined sitegen. Scripts:
`setup.sh`, `import.sh`, `generate.sh`.

### 6. Fixed endpoint hostname

All `deploy.env.example` files: `workshophost.casparwater.us` ->
`watershop.casparwater.us` (DNS-resolvable name).

---

## What Is Broken -- FIXED

### The root partition directory listing returns local content

After importing three ponds with `source_path: "/**"`, listing
`/sources/noyo/*` shows the LOCAL root's children (`/sources`,
`/system`) instead of the foreign noyo root's children (`/combined`,
`/hydrovu`, `/laketech`, `/reduced`, `/singled`, `/system`).

All three imports show identical entries with the same node_ids --
confirming these are the local root's entries leaking through.

### Root Cause

The `DirectoryEntry` struct did not carry the child's `pond_id`.
When `OpLogDirectory::insert()` added a foreign node to a local
directory (e.g., inserting the `noyo` mount point under `/sources`),
the foreign `pond_id` was discarded. When `OpLogDirectory::get()`
later reconstructed the child `FileID`, it used the parent's
`pond_id` (local), producing `FileID(root_uuid, root_uuid,
LOCAL_pond_id)` instead of `FileID(root_uuid, root_uuid,
NOYO_pond_id)`. The query then filtered by the wrong `pond_id`
and found the local root's directory record.

The earlier hypothesis about `register_empty_directory` was wrong.
That function correctly marks the directory as `modified: false`,
so it is NOT flushed as a local record. The real issue was one
layer higher: the directory entry metadata itself didn't preserve
cross-pond identity.

### Fix

Added `pond_id: Option<String>` to `DirectoryEntry`:
- `None` = child belongs to the same pond as parent (default)
- `Some(uuid)` = child is from a foreign pond

Changes:
- `crates/tinyfs/src/dir.rs`: Added `pond_id` field with
  `#[serde(default)]` for backward compatibility
- `crates/tlogfs/src/schema.rs`: Added nullable `pond_id` Utf8
  column to the `ForArrow` schema
- `crates/tlogfs/src/directory.rs`: `insert()` compares child vs
  parent `pond_id` and stores it when they differ; `get()` and
  `remove()` use the stored `pond_id` for child FileID construction
- `crates/tlogfs/src/persistence.rs`: `flush_directory_operations()`
  preserves `pond_id` when recreating entries at flush time

Backward compatibility: existing serialized directories (Arrow IPC)
without the `pond_id` column deserialize with `None` via
`#[serde(default)]`. All 848+ unit tests pass.

### 8. Factory path resolution uses context.root()

Read-path factories (`sql_derived`, `temporal_reduce`, `timeseries_pivot`)
were calling `fs.root()` to resolve absolute paths in their configs.
This always returns the global pond root, so patterns like
`/sensors/station_a` in an imported factory would resolve in the
consumer's tree instead of the foreign tree.

Changed all read-path factories to use `self.context.root()`, which
returns a WD scoped to the `effective_root` when set. This is the
same API that `sitegen` already uses.

Write-path factories (`remote`, `hydrovu`, `logfile_ingest`,
`journal_ingest`) are not changed -- they legitimately need the
global root to write to the local pond.

**Files changed:**
- `crates/provider/src/factory/sql_derived.rs` -- 2 call sites
  (pattern resolution and transform path resolution)
- `crates/provider/src/factory/temporal_reduce.rs` -- 2 call sites
  (discover_source_files and get_source_node_by_path)
- `crates/provider/src/factory/timeseries_pivot.rs` -- 1 call site
  (resolve_pattern)

### 9. effective_root threaded into dynamic factory contexts

When `create_dynamic_node_from_oplog_entry` loads a dynamic node
(e.g., a `dynamic-dir` containing `timeseries-join` entries), it
creates a `FactoryContext` for the factory. Previously this context
always had `effective_root: None`, so factories inside an imported
tree couldn't resolve absolute paths within the foreign tree.

Now: if the node's `pond_id` differs from the persistence layer's
`pond_uuid()`, the node is foreign. The code constructs a synthetic
`NodePath` for the foreign root and sets it as `effective_root` on
the context. This avoids deadlock (we're inside the inner lock, so
can't call `load_node`; instead we use `create_directory_node`
directly to build a lightweight handle).

`DynamicDirDirectory::create_child_context()` also propagates
`effective_root` to child factory contexts, so nested factories
(e.g., `timeseries-join` inside a `dynamic-dir`) inherit the scope.

`FactoryContext::effective_root()` getter added for propagation.

**Files changed:**
- `crates/tlogfs/src/persistence.rs` -- foreign node detection
  in `create_dynamic_node_from_oplog_entry`
- `crates/provider/src/factory/dynamic_dir.rs` -- propagate
  `effective_root` in `create_child_context`
- `crates/tinyfs/src/context.rs` -- `effective_root()` getter

### 10. Integration test: cross-pond factory resolution (533)

New test `testsuite/tests/533-cross-pond-factory-resolution.sh`:
- Producer pond with `dynamic-dir` containing `synthetic-timeseries`
  entries and a `timeseries-join` using absolute paths
- Consumer imports producer's entire tree via S3/MinIO
- Consumer reads the imported `timeseries-join` output
- Verifies row counts and data match byte-for-byte
- Verifies individual series accessible through import mount

---

## Next Steps

1. **`cross/` example**: Rebuild and test the cross-pond example
   (`cross/setup.sh` + `cross/import.sh` + `cross/generate.sh`)
   against the real S3 backups now that directory listings and
   factory resolution work.

2. **Provenance display**: `pond list --long` and `pond describe`
   should show foreign `pond_id` for imported files/directories
   (Decision Q2: always show).

3. **Phase 3: Incremental sync**: Track which foreign transactions
   have been imported. Currently uses `object_store.head()` for
   existence checks. Full control table tracking would enable
   efficient re-sync.

4. **Audit factory path usage**: Remaining factories that call
   `fs.root()` (`journal_ingest`, `logfile_ingest`, `hydrovu`)
   are write-path factories that need the global root. Document
   the convention: read-path = `context.root()`, write-path =
   `fs.root()` or `context.context.filesystem().root()`.

### S3 setup

The three source ponds are backed up to MinIO at
`watershop.casparwater.us:9000`:
- `s3://duckpond-dev` -- noyo (pond_id: 019d379c-a10c-7fdc-b2c3-414ccbe41eb0)
- `s3://septic-dev` -- septic (pond_id: 019d377d-b65f-7b9d-b729-07037e3f0925)
- `s3://water-dev` -- water (pond_id: 019d3793-9cb8-7c05-8ef8-132418027919)

Credentials: caspar/watertown (same as in deploy.env files).

### Test suite

All 51 integration tests and 848+ unit tests pass.

---

## Architecture Notes (Learned During This Session)

### FileID has three components

`FileID = (node_id, part_id, pond_id)`. The `pond_id` field was added
to the `FileID` struct alongside the cross-pond import design, but the
actual pond UUID was never threaded through -- everything used a
placeholder constant `LOCAL_POND_ID`. This session fixed that by
adding `PersistenceLayer::pond_uuid()`.

### Directory traversal inherits pond_id from parent

In `OpLogDirectory::get()` (`crates/tlogfs/src/directory.rs`), child
FileIDs are constructed using the entry's stored `pond_id` if present,
falling back to `self.id.pond_id()`. This means once you enter a
foreign directory (with a foreign `pond_id` on its FileID), all
traversal within that tree uses the foreign `pond_id`. Cross-pond
directory entries (where the child's pond differs from the parent)
store the child's `pond_id` explicitly in the `DirectoryEntry`.

### Pending records don't have pond_id

OpLog records get their `pond_id` stamped at commit time (line ~2089
of `persistence.rs`). During the transaction, pending records in
`self.records` have empty `pond_id`. This is why the query functions
don't filter pending records by `pond_id`.

### The root partition is special

The root partition has the well-known UUID `00000000-0000-7100-8000-000000000000`.
ALL ponds share this UUID for their root. Cross-pond import of root
partitions means the local Delta table has records from multiple ponds
in the same partition. The `pond_id` scoping in queries is what makes
this safe.

### One directory entry per import

Import = copy foreign parquet files + create one directory entry
linking the local mount path to the foreign partition. No recursion
into children. The foreign directory listing handles child references.
`collect_partitions_recursive` only discovers partition IDs for the
download step.

### Factory path resolution convention

Read-path factories (sql_derived, temporal_reduce, timeseries_pivot,
sitegen) must use `context.root()` for absolute path resolution.
This respects `effective_root` set by cross-pond boundary detection,
so `/sensors/station_a` resolves within the imported tree.

Write-path factories (remote, hydrovu, logfile_ingest, journal_ingest)
use `fs.root()` because they need the global root to write to the
local pond.

`DynamicDirDirectory` propagates `effective_root` to child contexts.
`create_dynamic_node_from_oplog_entry` auto-detects foreign nodes
and sets `effective_root` to the foreign root.

### effective_root is derived, not stored

The `effective_root` on a `FactoryContext` is computed at node-load
time by comparing `node.pond_id()` vs `persistence.pond_uuid()`.
It is NOT stored in the oplog or directory entries. The `pond_id`
on `DirectoryEntry` and `FileID` is the source of truth; the
effective_root is derived from it.
