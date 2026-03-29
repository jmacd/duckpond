# Cross-Pond Import: Implementation Status

Commit: 0102ebb (jmacd/33)
Date: 2026-03-29

## What Was Done

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

## What Is Broken

### The root partition directory listing returns local content

After importing three ponds with `source_path: "/**"`, listing
`/sources/noyo/*` shows the LOCAL root's children (`/sources`,
`/system`) instead of the foreign noyo root's children (`/combined`,
`/hydrovu`, `/laketech`, `/reduced`, `/singled`, `/system`).

All three imports show identical entries with the same node_ids --
confirming these are the local root's entries leaking through.

### Why

The mount point at `/sources/noyo` has:
```
FileID(part_id=root_uuid, node_id=root_uuid, pond_id=noyo_uuid)
```

When we traverse into it, `query_latest_directory_record` should
filter by `pond_id=noyo_uuid` and find the foreign root's directory
record. Instead, it appears to find the local root's record.

### Where to look

1. **`ensure_partition_cached`** loads ALL records for a partition
   into `HashMap<NodeID, Vec<OplogEntry>>`. For the root partition,
   this cache entry has records from BOTH the local and foreign ponds.
   The records are sorted by `timestamp DESC`.

2. **`query_latest_directory_record`** filters the cache by:
   ```rust
   records.iter()
       .find(|r| r.pond_id == pond_id_str && r.file_type.is_directory())
   ```
   Since records are sorted by timestamp DESC and `find()` returns
   the first match, it should find the latest record with the matching
   pond_id. This logic looks correct on paper.

3. **Possible issues to investigate:**
   - Is the foreign root record actually present in the imported
     parquet files? Check: are the foreign root partition's files
     being downloaded by `execute_import`?
   - Is the `pond_id` on the foreign records correct? The records
     were committed by the foreign pond with its real UUID. Verify
     this matches what `extract_foreign_pond_id()` returns.
   - Is `OpLogDirectory::get()` in `crates/tlogfs/src/directory.rs`
     constructing child FileIDs with the parent's `pond_id`? (Yes,
     it does: `self.id.pond_id()`. But verify the `self.id` is the
     foreign FileID, not the local one.)
   - Is the `register_empty_directory` call in `create_foreign_dir`
     interfering? It registers an EMPTY directory state for the
     foreign partition. If this empty state takes precedence over
     the imported data, we'd see an empty listing.

4. **Most likely cause:** `register_empty_directory` in
   `create_foreign_dir` (line ~444 of `factory.rs`):
   ```rust
   state.register_empty_directory(file_id).await;
   ```
   This inserts an empty `DirectoryState` into the in-memory
   `directories` HashMap. During `flush_directory_operations` at
   commit time, this gets written as a directory record with EMPTY
   content. The imported foreign data has the REAL directory listing,
   but the locally-created empty record has a higher version number
   (it was created in the current transaction). So
   `query_latest_directory_record` finds the empty local record
   first.

   **FIX:** `register_empty_directory` should NOT be called for
   import mount points. It was needed by `create_child_dirs_recursive`
   (now removed) to allow inserting child entries in the same
   transaction. With the simplified architecture (no child entry
   creation), this registration is unnecessary and harmful.

---

## Debugging Instructions

### Quick test

```bash
cd /Volumes/sourcecode/src/duckpond
rm -rf cross/pond
bash cross/setup.sh    # Creates pond, imports 3 sources
bash cross/import.sh   # Pulls data from S3

# This should show noyo's content (combined, hydrovu, etc.)
# but currently shows local content (sources, system):
POND=cross/pond cargo run --release -p cmd -- list -l '/sources/noyo/*'
```

### Verify the hypothesis

1. Comment out `state.register_empty_directory(file_id).await;` in
   `create_foreign_dir` (factory.rs ~line 444)
2. Rebuild and re-run the test
3. If the listing now shows foreign content, the hypothesis is
   confirmed

### If that doesn't fix it

Add debug logging to `query_latest_directory_record`:
```rust
log::debug!(
    "query_latest_directory_record: id={:?}, pond_id_str={}, found={:?}",
    id, pond_id_str,
    committed_record.as_ref().map(|r| &r.pond_id)
);
```

Also log what `ensure_partition_cached` loads for the root partition:
```rust
log::debug!(
    "ensure_partition_cached: part_id={}, loaded {} nodes, pond_ids={:?}",
    part_id, node_count,
    by_node.values().flat_map(|v| v.iter().map(|r| &r.pond_id)).collect::<HashSet<_>>()
);
```

### S3 setup

The three source ponds are backed up to MinIO at
`watershop.casparwater.us:9000`:
- `s3://duckpond-dev` -- noyo (pond_id: 019d379c-a10c-7fdc-b2c3-414ccbe41eb0)
- `s3://septic-dev` -- septic (pond_id: 019d377d-b65f-7b9d-b729-07037e3f0925)
- `s3://water-dev` -- water (pond_id: 019d3793-9cb8-7c05-8ef8-132418027919)

Credentials: caspar/watertown (same as in deploy.env files).

### Test suite

All 848 unit tests pass as of this commit. Run `cargo test` to verify.

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
FileIDs are constructed using `self.id.pond_id()`. This means once
you enter a foreign directory (with a foreign `pond_id` on its FileID),
all traversal within that tree uses the foreign `pond_id`. This is
correct behavior for cross-pond isolation.

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
