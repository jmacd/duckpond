# Query Repetition Analysis - Investigation Summary

**Date**: November 18, 2025  
**Branch**: jmacd/fifteen  
**Issue**: Export operation performs 3,042 query_records calls for single export (1-2 orders of magnitude more than necessary)

## Problem Statement

Export of `/reduced/single_param/DO/res=12h.series` with `--temporal year` performs massive repetitive database queries:
- **3,042 total query_records calls**
- **Only 7 ObjectStore list() calls from DataFusion**
- Files with 24 versions → queried 24-25 times consecutively
- Files with 21 versions → queried 21-22 times consecutively
- **Pattern**: O(N) queries where N = number of versions per file

## Root Cause: Temporal Operation Layering

### Architecture Overview

The export stack has 4 layers of temporal operations:
```
TemporalReduce (top)
  ↓ calls discover_source_columns()
  ↓ calls as_table_provider()
TimeseriesPivot
  ↓ calls as_table_provider()
TimeseriesJoin  
  ↓ calls as_table_provider()
OpLogFile (bottom - multi-version parquet)
  ↓ triggers DataFusion schema inference
  ↓ calls ObjectStore.list()
  ↓ yields N version files (version/1.parquet, version/2.parquet, ...)
```

### The O(N) Behavior

**Stack trace evidence** (see OUT_STACK:326-386):
```
Frame 29: TemporalReduceSqlFile::as_table_provider
Frame 26: TemporalReduceSqlFile::discover_source_columns  <-- Triggers cascade
Frame 24: TimeseriesPivotFile::as_table_provider
Frame 20: TimeseriesJoinFile::as_table_provider
Frame 16: OpLogFile::as_table_provider
Frame 11: datafusion::ListingTableUrl::list_all_files     <-- Lists 24 versions
Frame 6:  TinyFsObjectStore::list                          <-- Yields 24 files
```

**What happens**:
1. Export calls `TemporalReduceSqlFile::as_table_provider()` (line 1462 in export.rs for schema validation)
2. This calls `ensure_inner()` → `discover_source_columns()` (line 202 in temporal_reduce.rs)
3. `discover_source_columns()` calls `as_table_provider()` on the source (the next layer down)
4. This cascades through all 4 temporal layers
5. At the bottom, DataFusion's schema inference calls `list_all_files()`
6. ObjectStore.list() yields 24 individual version files
7. **Something in the stack then calls `list_file_versions()` 24 times** - once per version file discovered

### Query Breakdown

From analyze_query_patterns.sh on OUT file:

**Top Callers**:
- `list_file_versions`: 959 calls (31.5%)
- `read_file_version`: 952 calls (31.3%)
- `load_node`: 499 calls (16.4%)
- `query_single_directory_entry`: 434 calls (14.3%)
- `query_directory_entries`: 176 calls (5.8%)

**Repetition Pattern**:
- 56.6% sequential duplicates (same query back-to-back)
- 82.6% temporal clustering (same query within 10 calls)
- Only 10 unique part_id values, but 304 queries per part_id on average

**Most repeated query**:
- Same part_id+node_id: queried 707 times
- File with 24 versions: 25 consecutive `list_file_versions`, then 24 consecutive `read_file_version`, then repeats

## Instrumentation Added

### 1. Query Tracing in TLogFS

**File**: `crates/tlogfs/src/persistence.rs:2337-2418`

Added to `InnerState::query_records()`:
```rust
static CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
let call_num = CALL_COUNTER.fetch_add(1, Ordering::Relaxed);

// Extract caller function name from backtrace
let backtrace = std::backtrace::Backtrace::capture();
// ... parse to find tlogfs::persistence::InnerState::<function_name>

eprintln!("QUERY_TRACE|{}|{}|{}|{}", call_num, caller, part_id, node_id);

// ... execute query ...

eprintln!("QUERY_PERF|{}|{}|{}|{}|{}|{}", 
    call_num, committed_count, pending_count, total_count, query_ms, memory_us);
```

**Output formats**:
- `QUERY_TRACE|<call_num>|<caller>|<part_id>|<node_id>`
- `QUERY_PERF|<call_num>|<committed_count>|<pending_count>|<total_count>|<query_ms>|<memory_us>`

### 2. Stack Trace Logging

**File**: `crates/tlogfs/src/persistence.rs:2873-2881`

Added to `InnerState::list_file_versions()`:
```rust
let bt = std::backtrace::Backtrace::capture();
eprintln!("DEBUG_STACK list_file_versions called from:\n{}", bt);
```

### 3. Analysis Script

**File**: `analyze_query_patterns.sh`

Analyzes query patterns:
- Top callers by frequency
- Most queried part_id values
- Duplicate query detection (same part_id+node_id)
- Sequential duplicate detection (cache misses)
- Temporal clustering analysis
- Performance metrics aggregation

## How to Run This Analysis

### Step 1: Build with instrumentation
```bash
cargo build --package tlogfs
```

### Step 2: Run export with full output capture
```bash
# CRITICAL: Redirect BOTH stdout and stderr to same file
# Use RUST_BACKTRACE=1 to capture stack traces
RUST_BACKTRACE=1 POND=noyo/pond cargo run export \
  --pattern /reduced/single_param/DO/res=12h.series \
  --dir OUTDIR \
  --temporal year \
  1> OUT 2> OUT
```

### Step 3: Analyze query patterns
```bash
./analyze_query_patterns.sh OUT | head -100
```

### Step 4: Find sequential repetition
```bash
# Find sequences where same caller+part_id+node_id repeats >5 times
grep '^QUERY_TRACE' OUT | \
  awk -F'|' '{
    key=$3"|"$4"|"$5; 
    if (key == prev) {dup++} 
    else {
      if (dup > 5) {print dup+1, "consecutive calls:", prev} 
      dup=0; prev=key
    }
  } 
  END {if (dup > 5) print dup+1, "consecutive calls:", prev}' | head -20
```

### Step 5: Examine stack traces
```bash
# Find who's calling list_file_versions repeatedly
grep_search tool on OUT file:
  query: "DEBUG_STACK list_file_versions called from"
  
# Then read the stack trace context
read_file tool: offset=326, limit=60
```

## Key Findings

### 1. Layered Temporal Operations Create Cascade
Each temporal layer (`TemporalReduce` → `TimeseriesPivot` → `TimeseriesJoin` → `OpLogFile`) calls `as_table_provider()` which triggers full schema discovery down the stack.

### 2. DataFusion Schema Inference is Repeated
DataFusion's `ListingTableUrl::list_all_files()` is called once per layer, each time discovering the same version files.

### 3. Version Files Trigger Individual Queries
ObjectStore.list() correctly returns 24 version files, but then something calls `list_file_versions()` 24 times - once per discovered file instead of once for the logical multi-version file.

### 4. Export Schema Validation Triggers Full Stack
Export code calls `as_table_provider()` at line 1462 (`crates/cmd/src/commands/export.rs`) for schema validation, which triggers the entire cascade for each temporal partition being exported.

### 5. No Caching at Layer Boundaries
Each temporal layer's `ensure_inner()` checks if inner is `Some` and caches the SqlDerivedFile, BUT the schema discovery still happens every time because `discover_source_columns()` calls `as_table_provider()` on the source.

## What We DON'T Know Yet

1. **Exact count of export calls**: How many times does export call `as_table_provider()` at the top level?
   - Is it once per temporal partition (year)?
   - Is it once per file being exported?
   - Need to check temporal_parts iteration

2. **Why 167 calls to TemporalReduceSqlFile::as_table_provider**: 
   - Found via: `grep -c 'TemporalReduceSqlFile as.*QueryableFile>::as_table_provider' OUT_STACK`
   - What's calling it 167 times?

3. **Why ObjectStore list yields individual version files**: 
   - Is this intentional for DataFusion's parallel parquet reading?
   - Should we be returning a different abstraction?

4. **Where the 24 consecutive list_file_versions come from**:
   - After ObjectStore yields 24 files
   - Before read_file_version starts
   - What code iterates over those 24 files and calls list_file_versions for each?

## Next Steps to Eliminate Repetition

### Option 1: Cache Table Providers at Each Layer
Add `Arc<dyn TableProvider>` cache in each temporal file type:
```rust
pub struct TemporalReduceSqlFile {
    // ... existing fields ...
    cached_provider: Arc<tokio::sync::Mutex<Option<Arc<dyn TableProvider>>>>,
}
```

**Pros**: Simple, works with existing architecture  
**Cons**: Doesn't fix the O(N) version discovery issue

### Option 2: Batch Version Queries
In `tinyfs_object_store.rs`, when ObjectStore.list() yields N version files, call `list_file_versions()` once and cache the results, then return metadata for all N files without re-querying.

**Pros**: Eliminates the O(N) repetition at the source  
**Cons**: Requires restructuring ObjectStore implementation

### Option 3: Pass Discovered Schema Down Stack
Instead of each layer calling `as_table_provider()` independently:
- Top layer discovers schema once
- Pass discovered schema as parameter to lower layers
- Lower layers use cached schema instead of re-discovering

**Pros**: Prevents cascade re-discovery  
**Cons**: Major API change, breaks encapsulation

### Option 4: Defer Schema Discovery Until Query Execution
Change export to NOT call `as_table_provider()` for validation:
- Skip schema validation in export.rs:1462
- Let DataFusion discover schema lazily during query execution
- Schema errors fail at query time instead of validation time

**Pros**: Quick fix, eliminates redundant validation calls  
**Cons**: Loses fail-fast validation, errors happen later

## How to Restart in New Session

1. **Read this document** to understand what we found
2. **Check if instrumentation is still present**:
   ```bash
   grep "QUERY_TRACE" crates/tlogfs/src/persistence.rs
   grep "DEBUG_STACK" crates/tlogfs/src/persistence.rs
   ```
3. **Re-run analysis if needed** (Steps in "How to Run This Analysis" section)
4. **Examine specific code paths**:
   - `crates/cmd/src/commands/export.rs:1462` - export schema validation
   - `crates/tlogfs/src/temporal_reduce.rs:202` - discover_source_columns
   - `crates/tlogfs/src/temporal_reduce.rs:401` - as_table_provider delegation
   - `crates/tlogfs/src/tinyfs_object_store.rs:659` - list_file_versions call in list()
5. **Pick optimization strategy** from "Next Steps" above
6. **Implement with fail-fast principles**: No silent fallbacks, explicit errors

## Related Documentation

- `docs/query-records-optimization.md` - Query performance analysis protocol
- `docs/large-output-debugging.md` - How to capture full output without truncation
- `docs/duckpond-system-patterns.md` - Single transaction rule, architectural patterns
- `docs/anti-duplication.md` - Refactoring patterns to avoid code duplication

## Instrumentation Artifacts

**Files created during analysis**:
- `OUT` - Original export run with query tracing
- `OUT_STACK` - Export run with full backtraces (198,457 lines)
- `analyze_query_patterns.sh` - Analysis script

**To clean up instrumentation**:
```bash
# Remove debug logging from persistence.rs
# Remove stack trace logging from list_file_versions
# Keep QUERY_TRACE infrastructure for future debugging
```
