# Format Cache Implementation

This document describes the implementation of the format provider Parquet cache
for DuckPond, as designed in `format-cache-design.md`. The work was completed
across five phases, touching four crates and introducing one new module.

## Table of Contents

- [Problem Statement](#problem-statement)
- [Architecture Overview](#architecture-overview)
- [Phase 0: DataFusion Session Config Tuning](#phase-0-datafusion-session-config-tuning)
- [Phase 1: cache\_dir Plumbing](#phase-1-cache_dir-plumbing)
- [Phase 2: Cache Read/Write Helpers](#phase-2-cache-readwrite-helpers)
- [Phase 3: Single-File Cache Integration](#phase-3-single-file-cache-integration)
- [Phase 4: Multi-File Glob Cache (UNION ALL Replacement)](#phase-4-multi-file-glob-cache-union-all-replacement)
- [On-Disk Layout](#on-disk-layout)
- [Data Flow Diagrams](#data-flow-diagrams)
- [Test Coverage](#test-coverage)
- [Safety Properties](#safety-properties)
- [Files Changed](#files-changed)

---

## Problem Statement

Format providers (`csv`, `oteljson`, `excelhtml`) parse raw file bytes into Arrow
RecordBatches **on every read**. For files with many versions, or queries spanning
many files, this means:

1. **Repeated parsing** -- the same immutable file version is re-parsed every
   time a SQL query touches it.
2. **Full materialization** -- the old multi-file path used `UNION ALL BY NAME`
   via SQL, then `df.collect()` to materialize all results into a `MemTable`.
   This loads all data into memory before the outer query even begins, defeating
   predicate pushdown and projection pruning.
3. **No column/row pruning** -- raw format parsing cannot skip columns or rows;
   every byte is decoded.

The cache converts each file version into Parquet **once**, then serves all
subsequent reads via DataFusion's `ListingTable`, which supports:

- Parquet predicate pushdown (row group skipping via statistics)
- Projection pruning (only requested columns decoded)
- `split_file_groups_by_statistics` (non-overlapping time ranges avoid merge-sort)
- `metadata_size_hint` (single I/O for footer reads)

---

## Architecture Overview

```
                       sql_derived.rs
                     (multi-file query)
                            |
               +-----------+-----------+
               |                       |
         cache_dir set?          no cache_dir
               |                       |
     glob cache path          old UNION ALL path
               |                  (MemTable fallback)
               v
      ensure_url_cached()     <-- per-file cache population
               |
               v
      ensure_glob_symlinks()  <-- flat dir of symlinks
               |
               v
   listing_table_from_glob_cache()
               |
               v
        single ListingTable   <-- DataFusion pushdown
```

The implementation is layered across crates:

| Crate | Module | Role |
|-------|--------|------|
| `tlogfs` | `persistence.rs` | Session config, `cache_dir` computation |
| `tinyfs` | `context.rs` | `ProviderContext.cache_dir` field & accessor |
| `provider` | `format_cache.rs` | **New** -- all cache I/O helpers |
| `provider` | `provider_api.rs` | `ensure_url_cached()`, `create_cached_table_from_url()` |
| `provider` | `factory/sql_derived.rs` | Multi-file glob cache integration |

---

## Phase 0: DataFusion Session Config Tuning

**File:** `crates/tlogfs/src/persistence.rs` (InnerState::new)

Before any cache code, we tuned the DataFusion session to exploit Parquet
properties that the cache produces.

### Memory Pool: GreedyMemoryPool -> FairSpillPool

```rust
let pool = Arc::new(FairSpillPool::new(512 * 1024 * 1024));
```

`GreedyMemoryPool` gives all memory to the first consumer that asks, causing OOM
when multiple operators run concurrently. `FairSpillPool` divides the 512 MiB
budget fairly among all spillable consumers, triggering spill-to-disk instead of
OOM when any consumer exceeds its fair share.

### Parquet Filter Pushdown

```rust
session_config.options_mut().execution.parquet.pushdown_filters = true;
session_config.options_mut().execution.parquet.reorder_filters = true;
```

With `pushdown_filters`, predicates like `WHERE timestamp >= cutoff` are
evaluated **during Parquet decode**: the timestamp column is decoded first, and
non-matching rows are skipped before decoding remaining columns. `reorder_filters`
lets DataFusion reorder filter predicates by selectivity (cheapest first).

### File Group Splitting by Statistics

```rust
session_config.options_mut().execution.split_file_groups_by_statistics = true;
```

Each cached version file covers a non-overlapping time range. This setting
enables bin-packing that groups files with non-overlapping row-group statistics
into ordered streams, eliminating the merge-sort that would otherwise be needed
to produce ordered output.

### Parquet Metadata Size Hint

```rust
session_config.options_mut().execution.parquet.metadata_size_hint = Some(64 * 1024);
```

Reads the Parquet footer in a single 64 KB I/O instead of two separate reads
(8-byte footer length, then metadata). Since our cached Parquet files are
typically small (each is one version of one file), the footer is almost always
within 64 KB of the end.

---

## Phase 1: cache_dir Plumbing

Three changes thread the cache directory path from `tlogfs` down to `provider`:

### 1. ProviderContext gains `cache_dir` field

**File:** `crates/tinyfs/src/context.rs`

```rust
pub struct ProviderContext {
    // ...existing fields...
    pub cache_dir: Option<PathBuf>,
}
```

With builder method `with_cache_dir(mut self, cache_dir: PathBuf) -> Self` and
accessor `cache_dir(&self) -> Option<&Path>`.

For in-memory persistence (tests), `cache_dir` is `None`. For real ponds,
it's `{POND}/cache/`.

### 2. State gains `cache_dir` field

**File:** `crates/tlogfs/src/persistence.rs`

```rust
pub struct State {
    // ...existing fields...
    cache_dir: Option<PathBuf>,
}
```

Computed during transaction begin:

```rust
let cache_dir = self.path.parent().map(|pond_root| pond_root.join("cache"));
```

`self.path` is the `{POND}/data` directory, so `parent()` gives the pond root,
and we append `cache/`.

### 3. as_provider_context() propagates cache_dir

**File:** `crates/tlogfs/src/persistence.rs`

```rust
pub fn as_provider_context(&self) -> provider::ProviderContext {
    let ctx = provider::ProviderContext::new(
        self.session_context.clone(),
        Arc::new(self.clone()) as Arc<dyn PersistenceLayer>,
    );
    if let Some(ref cache_dir) = self.cache_dir {
        ctx.with_cache_dir(cache_dir.clone())
    } else {
        ctx
    }
}
```

### Prerequisite: Wire ProviderContext into Format Provider Path

**File:** `crates/provider/src/factory/sql_derived.rs`

The format provider path in the SQL-derived factory previously created a bare
`Provider::new(fs_arc)` without a `ProviderContext`. Changed to:

```rust
let provider_api = crate::Provider::with_context(
    fs_arc,
    Arc::new(self.context.context.clone()),
);
```

This ensures format providers receive `ProviderContext` (with `cache_dir`) when
invoked from SQL-derived factories.

---

## Phase 2: Cache Read/Write Helpers

**File:** `crates/provider/src/format_cache.rs` (new)

This is the core caching module. All functions are pure helpers with no side
effects beyond filesystem I/O. The module has no dependencies on transaction
state, `State`, or `FS` -- it operates entirely on `Path` values and version
metadata.

### Directory Layout Functions

```rust
pub fn cache_node_dir(cache_dir: &Path, scheme: &str, node_id: &NodeID) -> PathBuf
// -> {cache_dir}/{scheme}_{node_id}/

pub fn cache_version_path(cache_dir: &Path, scheme: &str, node_id: &NodeID, version: &FileVersionInfo) -> PathBuf
// -> {cache_dir}/{scheme}_{node_id}/v{version}_{blake3}.parquet
```

The blake3 hash in the filename is the file version's content hash (computed at
ingest time and stored in the version metadata). Since file versions are
immutable and content-addressed, the cache key is inherently stable -- there is
nothing to invalidate.

### Cache Miss Detection

```rust
pub fn find_uncached_versions(
    cache_dir: &Path,
    scheme: &str,
    node_id: &NodeID,
    versions: &[FileVersionInfo],
) -> Vec<FileVersionInfo>
```

Simple `path.exists()` check for each version. Returns only versions whose
cached Parquet file does not exist on disk.

### Streaming Cache Writer

```rust
pub async fn cache_write_version(
    cache_dir: &Path,
    scheme: &str,
    node_id: &NodeID,
    version: &FileVersionInfo,
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
) -> Result<PathBuf>
```

Key properties:

- **Streaming:** Batches flow from the format provider through `ArrowWriter`
  to disk one at a time. No `Vec<RecordBatch>` collection.
- **ZSTD compression:** Parquet is written with ZSTD default level for good
  compression ratio with fast read speed.
- **Atomic writes:** Data is written to `{path}.parquet.tmp`, then atomically
  renamed to `{path}.parquet`. Prevents partial files on crash.

### ListingTable Builder

```rust
pub async fn listing_table_from_cache(
    cache_dir: &Path, scheme: &str, node_id: &NodeID, ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>>
```

Returns a `ListingTable` pointing at `{cache_dir}/{scheme}_{node_id}/*.parquet`.
DataFusion lazily scans this directory and applies full pushdown optimization.

### Glob Directory Functions (Multi-File Support)

For the multi-file path, additional helpers unify per-node cache directories
into a single flat directory of symlinks:

```rust
pub fn cache_glob_dir(cache_dir: &Path, scheme: &str, pattern_hash: &str) -> PathBuf
// -> {cache_dir}/{scheme}_glob_{pattern_hash}/

pub fn pattern_hash(pattern: &str) -> String
// Deterministic 16-hex-char hash of the glob pattern string

pub fn reset_glob_dir(glob_dir: &Path) -> Result<()>
// rm -rf then mkdir -- ensures no stale symlinks

pub fn ensure_glob_symlinks(
    cache_dir: &Path, scheme: &str, node_id: &NodeID,
    versions: &[FileVersionInfo], glob_dir: &Path,
) -> Result<usize>
// Creates symlinks from glob dir to per-node cache files

pub async fn listing_table_from_glob_cache(
    glob_dir: &Path, ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>>
// Single ListingTable over the glob dir
```

Symlink creation uses `std::os::unix::fs::symlink` on Unix with a
`#[cfg(not(unix))]` fallback to `hard_link` or `copy`.

---

## Phase 3: Single-File Cache Integration

**File:** `crates/provider/src/provider_api.rs`

The `Provider::create_table_provider()` method now checks for a cache path:

```rust
pub async fn create_table_provider(&self, url_str: &str, _ctx: &SessionContext)
    -> Result<Arc<dyn TableProvider>>
{
    // ...parse URL, handle builtins...

    let format_provider = FormatRegistry::get_provider(scheme)?;

    // Try cache path if ProviderContext with cache_dir is available
    if let Some(ref provider_context) = self.provider_context {
        if let Some(cache_dir) = provider_context.cache_dir() {
            return self.create_cached_table_from_url(
                &url, format_provider.as_ref(), cache_dir,
                &provider_context.datafusion_session,
            ).await;
        }
    }

    // No cache available -- fall back to MemTable
    self.create_memtable_from_url(&url, format_provider.as_ref()).await
}
```

### ensure_url_cached()

The cache population logic was extracted into a reusable `pub(crate)` method so
both the single-file path and the multi-file glob path can share it:

```rust
pub(crate) async fn ensure_url_cached(
    &self,
    url: &Url,
    format_provider: &dyn FormatProvider,
    cache_dir: &Path,
) -> Result<(NodeID, Vec<FileVersionInfo>)>
```

This method:

1. Resolves the URL path to a `FileID` via `root.resolve_path()`
2. Lists all versions via `persistence.list_file_versions(file_id)`
3. Checks which versions are uncached via `find_uncached_versions()`
4. For each uncached version:
   - Reads raw bytes via `persistence.read_file_version(file_id, version)`
   - Wraps bytes in `Cursor` as `AsyncRead`
   - Applies decompression if needed (`url.compression()`)
   - Parses through the format provider's `open_stream()`
   - Streams output to cache via `cache_write_version()`
5. Returns `(NodeID, Vec<FileVersionInfo>)` for the caller to build a
   `ListingTable` or glob symlinks

### create_cached_table_from_url()

Thin wrapper that calls `ensure_url_cached()` then returns
`listing_table_from_cache()` -- a `ListingTable` over the single node's cache
directory.

---

## Phase 4: Multi-File Glob Cache (UNION ALL Replacement)

**File:** `crates/provider/src/factory/sql_derived.rs`

### The Problem

When a SQL-derived factory pattern matches multiple files (e.g.,
`oteljson:///sensors/**/*.json`), the old code:

1. Created a `MemTable` per file (parse + collect all batches)
2. Registered each as `t0`, `t1`, ..., `tN`
3. Built SQL: `SELECT * FROM t0 UNION ALL BY NAME SELECT * FROM t1 ...`
4. Executed `df.collect()` to materialize the entire union into memory
5. Wrapped the result in a final `MemTable`

This defeated all DataFusion optimizations: no predicate pushdown, no projection
pruning, complete materialization before the outer query begins.

### The Solution

When `cache_dir` is available, the new code:

1. Computes a deterministic `pattern_hash` from the URL pattern string
2. Creates a glob directory: `{cache_dir}/{scheme}_glob_{hash}/`
3. Resets the glob directory (removes stale symlinks from previous queries)
4. For each matched file:
   - Calls `ensure_url_cached()` to populate the per-node cache
   - Calls `ensure_glob_symlinks()` to create symlinks from the glob dir
     to the per-node cache files
5. Returns a single `ListingTable` over the glob directory

```
{POND}/cache/
  oteljson_{node_a}/
    v1_abc123.parquet      <-- cached version files
    v2_def456.parquet
  oteljson_{node_b}/
    v1_789ghi.parquet
  oteljson_glob_e8a3f1b2/
    {node_a}_v1_abc123.parquet  -> ../../oteljson_{node_a}/v1_abc123.parquet
    {node_a}_v2_def456.parquet  -> ../../oteljson_{node_a}/v2_def456.parquet
    {node_b}_v1_789ghi.parquet  -> ../../oteljson_{node_b}/v1_789ghi.parquet
```

The single `ListingTable` over the glob directory gives DataFusion a flat set of
Parquet files to scan. This enables:

- **Predicate pushdown:** row groups with non-matching statistics are skipped
- **Projection pruning:** only requested columns are decoded
- **File-level pruning:** `split_file_groups_by_statistics` skips entire files
  whose statistics don't match the predicate
- **No materialization:** the outer query's plan connects directly to the
  Parquet scan; rows stream through without intermediate collection

### Fallback

When `cache_dir` is `None` (no real filesystem, e.g., tests with
`MemoryPersistence`), the old `UNION ALL BY NAME` + `MemTable` path is
preserved unchanged.

---

## On-Disk Layout

```
{POND}/
  data/
    _delta_log/
    part_id=.../
  control/
    _delta_log/
  cache/                                  <-- NEW
    {scheme}_{node_id}/                   <-- per-node cache dir
      v{version}_{blake3}.parquet         <-- cached Parquet (ZSTD)
    {scheme}_glob_{pattern_hash}/         <-- glob cache dir
      {node_id}_v{version}_{blake3}.parquet -> symlink to per-node file
```

The entire `cache/` directory is safe to delete at any time (`rm -rf
{POND}/cache/`). It will be transparently repopulated on next query. Cache files
are never modified after creation (write-once, content-addressed by blake3 hash).

---

## Data Flow Diagrams

### Single-File Cache Hit (Subsequent Reads)

```
create_table_provider("csv:///data/file.csv")
  -> cache_dir is set
  -> ensure_url_cached()
     -> find_uncached_versions() -> [] (all cached)
  -> listing_table_from_cache()
     -> ListingTable over {cache}/csv_{node_id}/*.parquet
        -> DataFusion: pushdown filters, prune columns, stream rows
```

### Single-File Cache Miss (First Read)

```
create_table_provider("oteljson:///sensors/probe.json")
  -> cache_dir is set
  -> ensure_url_cached()
     -> find_uncached_versions() -> [v1, v2]
     -> for each uncached version:
        -> persistence.read_file_version() -> raw bytes
        -> Cursor::new(bytes) -> AsyncRead
        -> decompress() if needed
        -> format_provider.open_stream() -> (schema, Stream<RecordBatch>)
        -> cache_write_version() -> stream to Parquet via ArrowWriter
  -> listing_table_from_cache()
```

### Multi-File Glob Cache (SQL-Derived Factory)

```
sql_derived factory pattern "oteljson:///sensors/**/*.json" matches [A, B, C]
  -> cache_dir is set
  -> reset_glob_dir({cache}/oteljson_glob_{hash}/)
  -> for each file [A, B, C]:
     -> ensure_url_cached() -> (node_id, versions)
     -> ensure_glob_symlinks() -> create symlinks in glob dir
  -> listing_table_from_glob_cache({cache}/oteljson_glob_{hash}/)
     -> single ListingTable over all symlinked Parquet files
```

---

## Test Coverage

All tests live in `crates/provider/src/format_cache.rs` (11 tests):

| Test | What it verifies |
|------|-----------------|
| `test_cache_node_dir` | Directory path construction |
| `test_cache_version_path` | Version file path with blake3 |
| `test_find_uncached_versions_all_missing` | All versions reported as uncached |
| `test_find_uncached_versions_some_cached` | Partial cache correctly identified |
| `test_cache_write_version` | Streaming write + atomic rename + ZSTD Parquet |
| `test_listing_table_from_cache` | ListingTable schema inference over cached files |
| `test_pattern_hash_deterministic` | Same input -> same hash, different input -> different |
| `test_cache_glob_dir` | Glob directory path construction |
| `test_reset_glob_dir` | Dir cleanup + recreation |
| `test_ensure_glob_symlinks` | Symlink creation + idempotency (second call creates 0) |
| `test_listing_table_from_glob_cache` | ListingTable over multi-node glob dir, SQL COUNT verification |

Plus 141 existing provider tests continue to pass (152 total).

---

## Safety Properties

### Crash Safety

- **Atomic writes:** Cache files are written to `.parquet.tmp` then renamed.
  A crash during write leaves a `.tmp` file that is never read (cache keys
  check for `.parquet` extension).
- **No state corruption:** The cache is derived data only. The source of truth
  is always the pond's Delta Lake tables.
- **Safe deletion:** `rm -rf {POND}/cache/` is always valid. Next query
  repopulates transparently.

### Correctness

- **Content-addressed:** Cache keys include the blake3 hash of the file
  version's content. If content changes (impossible for existing versions, but
  defensive), the cache key changes.
- **Version-complete:** `find_uncached_versions` checks every version. New
  versions (from `pond copy` appending) are detected and cached automatically.
- **Glob staleness:** `reset_glob_dir()` removes all symlinks before
  repopulating. This ensures deleted files or changed glob patterns don't
  produce stale results.

### Compatibility

- **Graceful degradation:** When `cache_dir` is `None` (in-memory persistence,
  tests), all paths fall back to the original `MemTable` behavior. No
  behavioral change for existing code that doesn't set a cache directory.
- **Non-Unix:** `ensure_glob_symlinks` uses `#[cfg(unix)]` for symlinks with
  a `#[cfg(not(unix))]` fallback to hard links or file copies.

---

## Files Changed

| File | Type | Lines | Description |
|------|------|-------|-------------|
| `crates/tlogfs/src/persistence.rs` | Modified | ~30 | FairSpillPool, session config flags, `cache_dir` field, `as_provider_context` wiring |
| `crates/tinyfs/src/context.rs` | Modified | ~15 | `cache_dir` field, `with_cache_dir()` builder, `cache_dir()` accessor |
| `crates/provider/src/format_cache.rs` | **New** | ~570 | All cache helpers + 11 tests |
| `crates/provider/src/provider_api.rs` | Modified | ~90 | `ensure_url_cached()`, `create_cached_table_from_url()`, dispatch in `create_table_provider()` |
| `crates/provider/src/factory/sql_derived.rs` | Modified | ~80 | Glob cache path for multi-file query, `Provider::with_context()` |
| `crates/provider/src/lib.rs` | Modified | 1 | `pub mod format_cache;` |
