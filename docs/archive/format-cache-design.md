# Format Provider Cache -- Design Plan

## Problem

Format providers (`oteljson`, `csv`, `excelhtml`) parse raw file bytes into
Arrow RecordBatches on every read.  The parsed output is collected into a
`MemTable` because DataFusion's `TableProvider` trait requires re-scannability
and the format provider's `open_stream()` is one-shot.

For the Caspar Water sitegen export pipeline, 89 OtelJSON files (~85 MB
each, ~7.5 GB total) are parsed and materialized in memory up to 25
times per run.  Peak memory reaches ~39 GB.

For the Noyo Harbor configuration, excelhtml files go through the same
`MemTable` path.  The factory DAG has a diamond shape -- `/combined/*`
feeds both `single_site` (temporal-reduce) and `single_param`
(timeseries-pivot into temporal-reduce) -- so dynamic nodes that read
format providers are evaluated multiple times.

## Solution

Cache the parsed output of each **individual file version** as a Parquet
file on disk in `{POND}/cache/`.  On subsequent reads, return a
`ListingTable` over the cached Parquet files instead of a `MemTable`
from re-parsing.

### Key insight: per-version, not per-file

Every version in tinyfs is independently immutable.  Once version N is
written, its blake3 hash never changes.  **There is nothing to
invalidate.**  Cache entries accumulate exactly like source versions do.

When a new version is ingested (e.g., logfile-ingest appends version 90),
only that one new version needs parsing.  Versions 1--89 are already
cached.

This gives:

- **Zero re-parsing** -- each version parsed once, ever
- **No invalidation** -- version blake3 is immutable; cache entries are
  permanent until the source file is deleted
- **Incremental cost** -- new ingest caches one version, not all
- **`ListingTable` instead of `MemTable`** -- enables projection pushdown,
  predicate pushdown, and lazy evaluation across all cached versions
- **Throwaway** -- `rm -rf cache/` is always safe; data is recomputed on
  next read

## Pond directory structure

```
{POND}/
  data/       Source of truth (Delta Lake, backed up)
  control/    Local transaction coordinator (not backed up)
  cache/      Format provider materialization (not backed up, throwaway)
```

`cache/` is a peer of `data/` and `control/`.  It is never backed up by
the `remote` factory.  It is created lazily on first cache write.

## Cache layout

### Per-version Parquet files

Each physical file version is cached as a separate Parquet file, keyed
by the version's already-stored blake3 hash:

```
cache/{scheme}_{node_id}/
  v{version}_{blake3}.parquet
  v{version}_{blake3}.parquet
  ...
```

Where:
- `scheme` = URL scheme (`oteljson`, `csv`, `excelhtml`)
- `node_id` = the file's `NodeID` (UUID, from path resolution)
- `version` = the version number (from `FileVersionInfo`)
- `blake3` = that version's blake3 hash (from `FileVersionInfo`,
  already computed and stored by tinyfs at write time)

All values are already known -- no new content hashing is performed.

### Examples

Single-version file (`data`, e.g., laketech `.htm`):
```
cache/excelhtml_a1b2c3d4/
  v1_deadbeef....parquet
```

Multi-version series (`data:series`, e.g., 89 oteljson rotations):
```
cache/oteljson_e5f6a7b8/
  v1_aaa111....parquet
  v2_bbb222....parquet
  ...
  v89_zzz999....parquet
```

After one new ingest:
```
cache/oteljson_e5f6a7b8/
  v1_aaa111....parquet     (already existed, untouched)
  ...
  v89_zzz999....parquet    (already existed, untouched)
  v90_fff000....parquet    (newly written, only parse cost)
```

### Reading: ListingTable over cached versions

When the provider layer needs a table for a format URL, it:

1. Calls `list_file_versions(file_id)` to get all versions with blake3
2. For each version, checks if the corresponding cache Parquet exists
3. Any missing: parse that version via format provider, write to cache
4. Returns a `ListingTable` over the `cache/{scheme}_{node_id}/` directory

DataFusion's `ListingTable` handles multi-file scans with schema merging
automatically.  It treats the directory of per-version Parquet files
exactly like it treats multi-version `table:series` files through the
`TinyFsObjectStore` today.  Projection pushdown, predicate pushdown, and
row group pruning all work across the version files.

### Multi-file globs

A glob like `oteljson:///ingest/casparwater*.json` matching multiple
physical files resolves to multiple `node_id` values.  Each node has its
own `cache/{scheme}_{node_id}/` directory.

At the `sql_derived.rs` UNION ALL level, the current code creates one
`MemTable` per file then unions them.  With the cache, each file's
`create_table_provider()` call returns a `ListingTable` over that file's
cached versions.  The UNION ALL then operates over `ListingTable`
providers -- all lazy, all supporting pushdown.

No glob-level combined cache key is needed.  The per-file per-version
cache is the only primitive.

## Invalidation

**There is none.**  Versions are immutable.  A cache entry for version N
with blake3 hash H is valid forever (or until the source file is deleted
from the pond entirely).

This is the fundamental advantage of per-version caching over per-file
caching: no combined hash, no staleness check, no invalidation protocol.

### Cleanup

Cache entries for deleted files become orphans.  Cleanup options:

- **Explicit**: `pond gc` sweeps `cache/`, lists all `{scheme}_{node_id}`
  directories, checks if each `node_id` still exists in the pond.
  Removes orphans.
- **Nuclear**: `rm -rf {POND}/cache/` (always safe, rebuilds on next read)

No lazy cleanup is needed on the write path because writes never
invalidate existing entries.

## Architecture

### Layer placement

```
provider layer       Calls create_table_for_format_url() instead of
                     create_memtable_from_url().  Receives ListingTable
                     or MemTable depending on cache availability.

tlogfs layer         Owns {POND}/cache/ directory.  Exposes cache_dir()
                     method on OpLogPersistence.

tinyfs layer         No changes.  PersistenceLayer trait unchanged.
                     MemoryPersistence returns None for cache_dir().
```

### Key principle: tlogfs-specific, not on PersistenceLayer

The cache is an optimization, not a correctness requirement.  It belongs
in `OpLogPersistence` (tlogfs), not in the `PersistenceLayer` trait
(tinyfs).  When `MemoryPersistence` is used (tests), format providers
fall back to the existing `MemTable` path.

### Preserving ViewTable pushdown

The cache operates at the **physical file boundary only**.  Dynamic nodes
(temporal-reduce, timeseries-join, timeseries-pivot) are NOT cached.
Their `ViewTable` wrapping a `LogicalPlan` is preserved, so DataFusion's
optimizer can push predicates and projections through the full plan:

```
COPY TO (time-scoped)
  -> temporal-reduce ViewTable (date_bin GROUP BY)
    -> timeseries-join ViewTable (FULL OUTER JOIN)
      -> ListingTable over cache/{scheme}_{node_id}/*.parquet
         (predicate pushdown into Parquet row group statistics)
```

This is critical for incremental publishing.  When the hourly publish
passes `WHERE timestamp >= recent_cutoff`, the predicate propagates
through the ViewTable chain down to the cached Parquet files, which skip
historical row groups via column statistics.  Only the recently-written
cache files contain matching rows.

If dynamic nodes were cached instead, the full join would be
re-evaluated on every source change, defeating incremental updates.

## Implementation plan

### Step 1: Cache directory support in tlogfs

**File: `crates/tlogfs/src/persistence.rs`**

Add to `OpLogPersistence`:

```rust
/// Return the format provider cache directory, if available.
///
/// Returns {pond_root}/cache/ where pond_root is the parent of self.path
/// (which is {pond_root}/data/).  Returns None if the path cannot be
/// determined (e.g., in-memory persistence).
pub fn cache_dir(&self) -> Option<PathBuf> {
    self.path.parent().map(|pond_root| pond_root.join("cache"))
}
```

The `self.path` field stores `{POND}/data`.  Its parent is `{POND}`.

### Step 2: Expose cache_dir through ProviderContext

**File: `crates/tinyfs/src/context.rs`**

Add an optional cache directory to `ProviderContext`:

```rust
#[derive(Clone)]
pub struct ProviderContext {
    pub datafusion_session: Arc<SessionContext>,
    pub table_provider_cache: Arc<std::sync::Mutex<HashMap<String, Arc<dyn TableProvider>>>>,
    pub persistence: Arc<dyn PersistenceLayer>,
    pub cache_dir: Option<PathBuf>,  // NEW
}
```

When constructing `ProviderContext` from `State` (in tlogfs), populate
`cache_dir` from `OpLogPersistence::cache_dir()`.  When constructing from
`MemoryPersistence` (tests), set to `None`.

### Step 3: Cache read/write helpers

**New file: `crates/provider/src/format_cache.rs`**

```rust
use std::path::{Path, PathBuf};
use tinyfs::FileVersionInfo;

/// Directory for a file's cached format conversions.
pub fn cache_node_dir(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
) -> PathBuf {
    cache_dir.join(format!("{}_{}", scheme, node_id))
}

/// Path for a single version's cached Parquet.
pub fn cache_version_path(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
) -> PathBuf {
    let blake3 = version.blake3.as_deref()
        .expect("blake3 must be Some for file data versions");
    cache_node_dir(cache_dir, scheme, node_id)
        .join(format!("v{}_{}.parquet", version.version, blake3))
}

/// Check which versions are missing from the cache.
/// Returns the subset of versions that need parsing.
pub fn find_uncached_versions(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    versions: &[FileVersionInfo],
) -> Vec<FileVersionInfo> {
    versions
        .iter()
        .filter(|v| {
            let path = cache_version_path(cache_dir, scheme, node_id, v);
            !path.exists()
        })
        .cloned()
        .collect()
}

/// Write a single version's format output to cache as Parquet.
///
/// Streams batches from the format provider directly through an
/// ArrowWriter to disk -- does NOT collect into memory.
pub async fn cache_write_version(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    version: &FileVersionInfo,
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
) -> Result<PathBuf> {
    let dir = cache_node_dir(cache_dir, scheme, node_id);
    tokio::fs::create_dir_all(&dir).await?;

    let path = cache_version_path(cache_dir, scheme, node_id, version);
    let file = std::fs::File::create(&path)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    pin_mut!(stream);
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
    }
    writer.close()?;

    Ok(path)
}
```

### Step 4: Create table provider from cache

**File: `crates/provider/src/format_cache.rs` (continued)**

```rust
/// Build a ListingTable over all cached version Parquet files for a node.
///
/// Assumes all versions are already cached (call cache_write_version
/// for any uncached versions first).
pub async fn listing_table_from_cache(
    cache_dir: &Path,
    scheme: &str,
    node_id: &tinyfs::NodeID,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let dir = cache_node_dir(cache_dir, scheme, node_id);
    let dir_url = format!("file://{}/", dir.to_string_lossy());

    let table_url = ListingTableUrl::parse(&dir_url)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}
```

### Step 5: Replace create_memtable_from_url

**File: `crates/provider/src/provider_api.rs`**

Replace the existing `create_memtable_from_url` call with a cache-aware
version.  The new function:

1. Lists all versions of the source file
2. Finds which versions are not yet cached
3. For uncached versions only: opens the version bytes, runs the format
   provider, streams output to a cached Parquet file
4. Returns a `ListingTable` over the cache directory

```rust
async fn create_table_for_format_url(
    &self,
    url: &Url,
    format_provider: &dyn FormatProvider,
    file_id: FileID,
    cache_dir: Option<&Path>,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let scheme = url.scheme();
    let node_id = file_id.node_id();

    // Try cache path
    if let Some(cache_dir) = cache_dir {
        let versions = self.persistence
            .list_file_versions(file_id)
            .await?;

        let uncached = format_cache::find_uncached_versions(
            cache_dir, scheme, &node_id, &versions,
        );

        if uncached.is_empty() {
            log::debug!(
                "[GO] Format cache: all {} versions cached for {}",
                versions.len(), node_id,
            );
        } else {
            log::info!(
                "[SAVE] Format cache: writing {} of {} versions for {}",
                uncached.len(), versions.len(), node_id,
            );

            for version in &uncached {
                // Read this specific version's bytes
                let reader = self.persistence
                    .read_file_version(file_id, version.version)
                    .await?;

                // Parse through format provider
                let reader = format::compression::decompress(
                    reader, url.compression(),
                )?;
                let (schema, stream) = format_provider
                    .open_stream(reader, url)
                    .await?;

                // Stream to cache Parquet -- not collected into memory
                format_cache::cache_write_version(
                    cache_dir, scheme, &node_id, version,
                    schema, stream,
                ).await?;
            }
        }

        // Return ListingTable over all cached version Parquet files
        return format_cache::listing_table_from_cache(
            cache_dir, scheme, &node_id, ctx,
        ).await;
    }

    // No cache dir (MemoryPersistence / tests) -- fall back to MemTable
    self.create_memtable_from_url(url, format_provider).await
}
```

Key properties:
- Each version is parsed independently via the format provider, then
  streamed directly to Parquet on disk via `ArrowWriter`.  No
  `Vec<RecordBatch>` collection.
- Only uncached versions are parsed.  On an incremental ingest that
  adds 1 version, only 1 file is parsed.
- The returned `ListingTable` scans all `*.parquet` files in the cache
  directory.  DataFusion handles schema merging, projection pushdown,
  and predicate pushdown across the version files.

### Step 6: Wire into create_table_provider

**File: `crates/provider/src/provider_api.rs`**

In `create_table_provider()`, where the format provider branch currently
calls `create_memtable_from_url()`, change to:

1. Resolve the URL path to a `FileID`
2. Get `cache_dir` from context
3. Call `create_table_for_format_url()` with the file ID and cache dir

### Step 7: Eliminate UNION ALL for multi-file globs

**File: `crates/provider/src/factory/sql_derived.rs`**

The multi-file format provider path (lines 920-1024) currently:
1. For each of N matched files, calls `create_table_provider()` which
   produces a `MemTable` (today) or `ListingTable` (with cache)
2. Registers each as `t0`, `t1`, ..., `tN` in a throwaway SessionContext
3. Builds SQL: `SELECT * FROM t0 UNION ALL BY NAME ... SELECT * FROM tN`
4. Executes `df.collect()` -- **materializes the entire union in memory**
5. Wraps result in a new `MemTable`

Even with the per-version cache (turning step 1 into ListingTable),
steps 3-5 still fully materialize everything.  The `UNION ALL BY NAME`
result is collected into `Vec<RecordBatch>`, losing all lazy benefits.

#### Replacement: single ListingTable over flat glob directory

Instead of N separate cache directories with their own ListingTables,
write all cached Parquet files into a single directory scoped to the
glob pattern:

```
cache/{scheme}_glob_{pattern_hash}/
  {node1_id}_v1_{blake3}.parquet
  {node1_id}_v2_{blake3}.parquet
  ...
  {nodeN_id}_v1_{blake3}.parquet
```

Then create **one** `ListingTable` over that directory.  This:

- **Eliminates the UNION ALL plan node** -- no N-child union tree in the
  logical plan
- **Eliminates `df.collect()` materialization** -- the ListingTable is
  lazy; rows are pulled only when the downstream query executes
- **Enables projection pushdown** -- DataFusion reads only the columns
  the downstream SQL uses (often timestamp + 1 metric, not all 500+)
- **Enables predicate pushdown** -- `WHERE timestamp >= cutoff` prunes
  row groups via Parquet column statistics in every file
- **Enables `split_file_groups_by_statistics`** -- since cache files have
  non-overlapping time ranges, bin-packing groups them into ordered
  streams, eliminating the merge-sort step

#### Schema evolution across files

`UNION ALL BY NAME` exists because glob-matched files can have different
schemas.  An OtelJSON file from January may have columns `{timestamp,
ph, temperature}` while a file from March adds `{dissolved_oxygen}`.
`UNION ALL BY NAME` aligns by column name and fills missing values with
nulls.

`ListingTable` handles this natively via `SchemaAdapterFactory`.  The
default `DefaultSchemaAdapterFactory` does exactly the same thing: maps
each file's schema to the table schema and fills missing columns with
nulls.  The table schema is inferred from all files during
`ListingTableConfig::infer_schema()`, which reads all file footers and
merges their schemas.

So schema evolution is handled -- `ListingTable` supports heterogeneous
schemas across files, just like `UNION ALL BY NAME`.

#### Cache directory structure

The per-node cache directories still exist for single-file access:

```
cache/oteljson_{node1_id}/
  v1_{blake3}.parquet
  v2_{blake3}.parquet
```

The glob directory is an additional index -- symlinks (or hard links) to
the per-node files:

```
cache/oteljson_glob_{pattern_hash}/
  {node1_id}_v1_{blake3}.parquet -> ../oteljson_{node1_id}/v1_{blake3}.parquet
  {node1_id}_v2_{blake3}.parquet -> ../oteljson_{node1_id}/v2_{blake3}.parquet
  ...
```

This avoids duplicating cached data.  When a new version is cached for
any node, its symlink is added to the glob directory.  When the glob
pattern is first evaluated, missing symlinks are created for all
existing cached versions.

#### Incremental glob directory maintenance

The glob directory is rebuilt lazily:
1. Evaluate the glob pattern to get the set of `(node_id, versions)`
2. For each version, ensure the per-node cache file exists (parse if not)
3. For each version, ensure a symlink exists in the glob directory
4. Return `ListingTable` over the glob directory

Step 3 is cheap -- just `symlink()` calls for any new versions.  On a
steady-state run with no new data, all symlinks already exist.

#### Comparison of approaches

| Approach | Plan tree | Materialization | Pushdown |
|----------|-----------|----------------|----------|
| Current (MemTable) | N-child UNION → collect → MemTable | Full (all files in RAM) | None |
| Cache + lazy UNION | N-child UNION of ListingTables | None (lazy) | Yes, per child |
| Cache + single ListingTable | 1 ListingTable over all files | None (lazy) | Yes, unified |

The single-ListingTable approach produces the simplest plan tree (one
scan node), gives the optimizer the most freedom (all files as one
source), and enables `split_file_groups_by_statistics` to partition
them optimally.

## DataFusion session tuning

Audit of `datafusion-rs/` reveals several configuration opportunities
that are independent of the format cache but compound with it.

### Current session configuration

`InnerState::new()` in [persistence.rs](../crates/tlogfs/src/persistence.rs):

| Setting | Current value | Source |
|---------|---------------|--------|
| `target_partitions` | 2 | Explicit |
| `information_schema` | true | Explicit |
| Memory pool | `GreedyMemoryPool` 512 MiB | `with_memory_limit(512M, 1.0)` |
| File statistics cache | Enabled (default size) | Explicit |
| List files cache | Enabled (default size) | Explicit |
| File metadata cache | Enabled (50 MB LRU, default) | DataFusion default |
| `pushdown_filters` | **false** (default) | Not set |
| `reorder_filters` | **false** (default) | Not set |
| `split_file_groups_by_statistics` | **false** (default) | Not set |
| `metadata_size_hint` | None (default, two I/O reads) | Not set |

### Recommended changes

#### 1. Enable `pushdown_filters` and `reorder_filters`

```rust
let session_config = SessionConfig::default()
    .with_target_partitions(2)
    .with_information_schema(true);

// Enable late-materialization filter pushdown in Parquet reader.
// Filters are evaluated during decode: columns referenced only by
// the filter are decoded first, non-matching rows skipped before
// decoding remaining columns.  Critical for time-scoped queries
// like WHERE timestamp >= cutoff.
session_config
    .options_mut()
    .execution
    .parquet
    .pushdown_filters = true;
session_config
    .options_mut()
    .execution
    .parquet
    .reorder_filters = true;
```

**Why:** Every COPY TO export in the sitegen pipeline applies a time
predicate.  Without `pushdown_filters`, DataFusion reads all columns
from every row group that passes statistics pruning, then filters in a
separate `FilterExec`.  With `pushdown_filters`, the Parquet reader
decodes the timestamp column first, builds a selection mask, and only
decodes remaining columns for matching rows.  This is a multiplicative
win on wide tables (many metrics columns, few selected by time).

Currently only enabled in test code (`null_padding.rs`,
`scope_prefix.rs`).  Should be the production default.

#### 2. Enable `split_file_groups_by_statistics`

```rust
session_config
    .options_mut()
    .execution
    .split_file_groups_by_statistics = true;
```

**Why:** Per-version cache files have non-overlapping time ranges by
construction (each logfile rotation covers a distinct time period).
This setting enables a bin-packing algorithm that groups files with
non-overlapping statistics into separate file groups, preserving sort
order without a merge step.

The algorithm (Dilworth's theorem, first-fit decreasing) reads per-file
min/max column statistics from Parquet metadata, sorts files by min
value, and packs non-overlapping files into groups.  When the number of
groups exceeds `target_partitions`, it falls back to unordered.

With `target_partitions=2`, this means up to 2 ordered file groups.
Since our cache files are temporally non-overlapping, the two groups
partition the timeline in half and each can be scanned in order without
sorting.

#### 3. Set `metadata_size_hint`

```rust
session_config
    .options_mut()
    .execution
    .parquet
    .metadata_size_hint = Some(64 * 1024); // 64 KB
```

**Why:** Without this, the Parquet reader performs two I/O operations
per file: one to read the 8-byte footer length, then one to read the
metadata.  With `metadata_size_hint`, it reads the last N bytes in a
single operation.  Since our cached Parquet files are small (a few MB
each, written with ZSTD), their footers are well under 64 KB.

#### 4. Switch to `FairSpillPool`

```rust
let pool = Arc::new(FairSpillPool::new(512 * 1024 * 1024));
let runtime_env = RuntimeEnvBuilder::new()
    .with_cache_manager(cache_config)
    .with_memory_pool(Arc::new(TrackConsumersPool::new(
        pool,
        NonZeroUsize::new(5).unwrap(),
    )))
    .build_arc()?;
```

**Why:** The current `GreedyMemoryPool` (created by `with_memory_limit`)
is first-come-first-served.  A single aggressive operator (e.g., a hash
aggregate for temporal-reduce) can consume all 512 MiB before other
spillable operators get any memory, causing an `OOM` error rather than
graceful spilling.

`FairSpillPool` divides available memory fairly among all spillable
consumers.  When any consumer exceeds its fair share, it gets a
`ResourcesExhausted` error, which triggers spill-to-disk.  Both hash
aggregation (`GroupedHashAggregateStream`) and sort (`ExternalSorter`)
handle this by writing intermediate results to disk in IPC format and
later doing a streaming merge.

This matters for the Noyo Harbor pipeline where temporal-reduce and
timeseries-join may run concurrently within a single plan.

### Other observations from DataFusion source

#### ViewTable pushdown is complete

`ViewTable::scan()` reports `FilterPushDown::Exact` for all filters,
then composes them into the underlying `LogicalPlan` via
`LogicalPlanBuilder::filter()`.  The combined plan goes through the full
optimizer pipeline, including predicate pushdown into `ListingTable`
leaf nodes.  `get_logical_plan()` returns `Some(Borrowed)`, enabling
optimizer inlining across ViewTable boundaries.

No changes needed -- our dynamic factory ViewTable chain is already
optimal for predicate propagation.

#### UNION ALL pushdown is complete

The `PushDownFilter` optimizer rule explicitly handles
`LogicalPlan::Union`: predicates are cloned and pushed into every child
with column name mapping.  At the physical level, `UnionExec` supports
`try_swapping_with_projection()` to push projections below the union.

This means `WHERE timestamp >= cutoff` on a UNION ALL of ListingTables
pushes into every child ListingTable, enabling row group pruning per
file.  No changes needed.

#### No scan sharing across COPY TO queries

Each `COPY TO` is an independent SQL statement.  DataFusion has no
multi-query optimization or common subexpression elimination across
statements.  Each execution triggers a fresh `scan()` on the
registered `TableProvider`.

For `ListingTable` over cached Parquet this is acceptable: the OS page
cache keeps hot Parquet pages in memory, and DataFusion's file metadata
cache avoids re-reading footers.  The actual data re-scan is cheap
because Parquet's columnar format + row group pruning already minimizes
I/O.

The only way to share scans across exports would be to compose all
exports into a single plan (e.g., one UNION ALL of all COPY TO outputs).
This is architecturally complex and the gain is small given the
per-query cost is dominated by aggregation, not scanning.

#### Consolidating UNION ALL into single ListingTable

See Step 7 in the implementation plan for the full design.  In brief:
replace the N-child UNION ALL BY NAME with a single `ListingTable` over
a flat glob directory containing symlinks to per-node cache files.
`ListingTable`'s `SchemaAdapterFactory` handles schema evolution across
files (fills missing columns with nulls), matching `UNION ALL BY NAME`
semantics.  This eliminates both the union plan node and the
`df.collect()` materialization, enabling full projection and predicate
pushdown as a single scan.

## Prerequisites

Before the cache implementation, three things in the existing code
need attention.

### P1: Format provider path lacks FileID and ProviderContext

`Provider::create_table_provider()` dispatches format providers
(oteljson, csv, excelhtml) through `create_memtable_from_url()`, which
only receives a URL and a `FormatProvider`.  It calls
`self.fs.open_url(url)` to get a byte stream of the latest version.
It has no `FileID`, no version list, and no per-version access.

The cache needs to:
1. Resolve the URL path to a `FileID` (same as the builtin path does)
2. Call `list_file_versions(file_id)` to enumerate versions
3. Call `read_file_version(file_id, version)` per uncached version

This requires wiring `ProviderContext` into the format provider path.
Currently `Provider.provider_context` is `Option<Arc<ProviderContext>>`
and is `None` when format providers are invoked from `sql_derived.rs`
(line 932 creates `Provider::new(fs_arc)` without `with_context()`).

**Fix:** Require `ProviderContext` for the format provider path when
caching is enabled.  Pass it through from `sql_derived.rs` where it is
already available via `self.context.context`.

### P2: `read_file_version` returns `Vec<u8>`, not a stream

The `PersistenceLayer` trait defines:

```rust
async fn read_file_version(&self, id: FileID, version: u64) -> Result<Vec<u8>>;
```

This loads the entire version into memory.  For an 85 MB OtelJSON file,
that is 85 MB of raw bytes held in a `Vec<u8>` before the format
provider even starts parsing.

The format provider then does its own pass over those bytes (OtelJSON
does two-pass parsing, both in memory).  So the peak for caching one
version is ~85 MB raw bytes + format provider working set.

This is acceptable for the initial implementation because:
- Only one version is cached at a time (sequential loop)
- 85 MB raw bytes is transient and small compared to the ~39 GB peak
  being eliminated
- The bottleneck is the parsed MemTable (hundreds of MB per file), not
  the raw bytes

However, `read_file_version` should eventually return
`AsyncRead` / a streaming reader to avoid the full-file allocation.
This is a separate improvement to `PersistenceLayer` -- not a blocker
for the cache work, but should be tracked.

### P3: `blake3` must be `Some` for cached versions

`FileVersionInfo.blake3` is `Option<String>`.  It is `Some` for all
physical file data (inline and large files) and `None` for directories,
symlinks, dynamic nodes, and pending files.

Format providers only operate on file data, so `blake3` should always
be `Some` for any version that reaches the cache path.  If it is
`None`, that is a bug -- the cache code must treat it as an error,
not silently fall back.

## Sequencing

| Phase | What | Effort | Effect |
|-------|------|--------|--------|
| 0 | Session config tuning (above) | Small | Immediate: pushdown, spill |
| P1 | Wire ProviderContext into format path | Small | Prerequisite for cache |
| P2 | (Tracked) streaming read_file_version | Future | Not blocking |
| 1 | Steps 1-2: cache_dir plumbing | Small | Infrastructure only |
| 2 | Step 3: cache read/write helpers | Medium | Standalone, testable |
| 3 | Steps 4-6: single-file integration | Medium | Per-version caching |
| 4 | Step 7: multi-file UNION lazification | Medium | Eliminate df.collect() |

Phase 0 is independent of the cache and can land immediately.  It
improves every existing query.  Phase 3 alone solves the format cache
problem.  Phase 4 eliminates the last `MemTable` materialization.

## What this does NOT change

- **tinyfs `PersistenceLayer` trait** -- no new methods
- **`MemoryPersistence`** -- continues to work (returns None for cache_dir,
  falls back to MemTable)
- **Format provider interface** -- `FormatProvider` trait unchanged
- **Source data** -- raw oteljson/csv/excelhtml files preserved as-is
- **Dynamic factories** -- temporal-reduce, pivot, join are NOT cached;
  their ViewTable plans are preserved for end-to-end optimizer pushdown
- **Backup/remote** -- `{POND}/cache/` is not in `{POND}/data/`, so the
  remote factory ignores it

## Expected impact

### Caspar Water sitegen (25 exports, 89 oteljson versions)

| Scenario | JSON parsed | Peak memory |
|----------|-------------|-------------|
| Before (current) | 25 x 89 x 85 MB = 189 GB | ~39 GB |
| After, first run | 89 x 85 MB = 7.5 GB (once) | ~1 GB streaming |
| After, subsequent | 0 (all cache hits) | ~hundreds of MB |
| After, +1 ingest | 1 x 85 MB | ~hundreds of MB |

### Noyo Harbor (40 exports, excelhtml + hydrovu)

| Scenario | Impact |
|----------|--------|
| First run | Each excelhtml `.htm` file parsed once, cached |
| Subsequent | Cache hits; ViewTable pushdown through join/reduce |
| +1 HydroVu | 1 new Parquet version cached; ViewTable evaluates |
|            | join over cached Parquet with time predicate pushdown |

### Session tuning (independent of cache)

| Change | Effect |
|--------|--------|
| `pushdown_filters = true` | Late materialization: skip decoding non-matching rows |
| `reorder_filters = true` | Cheapest filter evaluated first |
| `split_file_groups_by_statistics` | Ordered scan without merge for non-overlapping files |
| `metadata_size_hint = 64KB` | Single I/O for Parquet footer instead of two |
| `FairSpillPool` | Graceful spill instead of OOM on memory pressure |

### Incremental hourly publish

The per-version cache composes with ViewTable pushdown:

1. New data arrives as version N+1 of an instrument series
2. For format sources: only version N+1 is parsed and cached (~seconds)
3. For Parquet sources: no caching needed (already Parquet)
4. sitegen export with `WHERE timestamp >= cutoff` pushes through:
   - temporal-reduce ViewTable (date_bin GROUP BY)
   - timeseries-join ViewTable (FULL OUTER JOIN)
   - ListingTable over cached Parquet (row group pruning)
5. Only recent row groups are scanned; historical data skipped
6. `pushdown_filters` further reduces I/O: within matching row groups,
   only rows passing the time predicate are fully decoded
