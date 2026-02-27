# Sitegen Memory Analysis — 2026-02-23

## Context

The sitegen export pipeline for Caspar Water produces 25 exports (5 metrics ×
5 resolutions).  After applying the schema-representative fix (use newest file
for schema discovery, adding pH data), peak memory was measured at **39.4 GB**.
A prior run without pH was **23.6 GB**.  An attempted fix — creating a fresh
`ProviderContext` per export — made things worse, not better, and was reverted.

This document records the findings from a detailed code-level investigation of
where memory is allocated and retained.

## Architecture of one export

Each of 25 exports follows this call chain:

```
export_series_to_parquet(provider_ctx)
  |
  +-- queryable.as_table_provider(node_id, provider_ctx)
  |     |
  |     +-- TemporalReduceSqlFile::as_table_provider
  |           |
  |           +-- ensure_inner()
  |           |     +-- discover_source_columns()   [parses 1 file for schema]
  |           |     +-- SqlDerivedFile::new(...)     [stores SQL + pattern info]
  |           |
  |           +-- inner.as_table_provider(id, ctx)  [SqlDerivedFile]
  |                 |
  |                 +-- Cache check: id-based key -> MISS (unique per node)
  |                 +-- resolve_pattern -> 89 files
  |                 +-- is_format_provider("oteljson") -> true
  |                 +-- FOR EACH of 89 files:
  |                 |     create_memtable_from_url() -> parse oteljson -> MemTable
  |                 +-- UNION ALL BY NAME (temp_ctx) -> df.collect() -> MemTable
  |                 +-- table_exists check on shared SessionContext:
  |                 |     Call  1: false -> register source MemTable
  |                 |     Calls 2-25: true -> DROP new MemTable
  |                 +-- Create ViewTable -> cache -> return
  |
  +-- ctx.register_table(unique_name, view_table)   [NEVER deregistered]
  +-- COPY (SELECT ... FROM view_table) TO ... PARTITIONED BY (year)
```

## Three memory problems identified

### Problem 1: Redundant 89-file MemTable construction (24 of 25 wasted)

All five metrics share the identical source glob
`oteljson:///ingest/casparwater*.json`.  The `SqlDerivedFile` creates the same
deterministic table name for all 25 nodes.  But:

- The MemTable is built **before** the `table_exists` check
  (`sql_derived.rs` lines 920-1024 build the MemTable; line 1150 checks).
- Each build parses all 89 OTelJSON files (~85 MB decompressed each),
  collects them into individual MemTables, runs UNION ALL, calls
  `df.collect()`, and builds the final MemTable.
- Only the 1st call registers; calls 2-25 build then immediately drop.

**Per-cycle peak estimate:**

| Component | Size |
|-----------|------|
| 89 individual per-file MemTables (simultaneous) | ~445 MB |
| Per-file parsing intermediates (sequential) | ~500 MB transient |
| UNION ALL `df.collect()` output | ~440 MB |
| **Total per-cycle peak** | **~1.4 GB** |

### Problem 2: 25 never-deregistered export tables in SessionContext

`export_series_to_parquet` (`export.rs` line 282) calls `register_table` with
unique PID+nanosecond names but **never** calls `deregister_table`.  Compare
with `cmd/export.rs` line 870-872, which does deregister.

Each registered table is an `Arc<ViewTable>` whose `LogicalPlan` holds `Arc`
references to the source MemTable via the `TableScan -> DefaultTableSource`
chain.  After all 25 exports the SessionContext's `MemorySchemaProvider`
holds 25 `Arc<ViewTable>` entries plus the source MemTable itself, all pinned
alive by reference counting.

### Problem 3: 25 schema-discovery file parses

Each `TemporalReduceSqlFile::ensure_inner()` calls `discover_source_columns()`
which parses ONE oteljson file (~85 MB -> ~500 MB transient).  This runs 25
times with no cross-node caching.

## Why the fresh-ProviderContext fix didn't help

The fix created a fresh `ProviderContext` (with empty `table_provider_cache`)
per export.  But:

1. All ProviderContexts share the same `Arc<SessionContext>` — registered
   tables and their Arc'd TableProviders persist regardless.
2. The real memory hog is the 89-file MemTable construction inside
   `SqlDerivedFile::as_table_provider`, which happens before any caching.
3. The fresh cache means the ViewTable cache check (line 828-833, keyed by
   FileID) **always** misses, so all 25 ViewTables are created instead of
   potentially fewer.

## Why it got worse (39 GB vs 23.6 GB)

Two contributing factors:

1. **Schema fix adds pH data**: The newer representative file has 12 columns
   instead of 7.  The pivoted MemTable is ~70% wider.
2. **Fresh ProviderContext prevents ViewTable cache hits**: The ViewTable
   cache check always misses, and the full pattern resolution + MemTable
   build runs every time.

## Root cause: unnecessary MemTable materialization

### Why MemTables exist

DataFusion's `TableProvider` trait requires re-scannability — a provider must
support creating a new scan every time a query references it.
`FormatProvider::open_stream()` returns a one-shot `Stream<Item = RecordBatch>`;
once consumed, it's gone.  So the code collects the stream into a `MemTable`
to make it re-scannable.

### Two layers of materialization

**Layer 1** — `provider_api.rs` line 221 (`create_memtable_from_url`):
Parses each OTelJSON file and collects into a per-file MemTable.  Called 89
times per cycle.  All 89 MemTables alive simultaneously.

**Layer 2** — `sql_derived.rs` line 1003 (`df.collect().await`):
Materializes the UNION ALL of those 89 MemTables into yet another MemTable.
Both the 89 individual MemTables AND the combined result coexist in memory.

### Neither layer needs to exist

The temporal_reduce SQL is a GROUP BY aggregation over the source data.  The
ideal pipeline is:

```
disk -> parse file 1 -> stream batches -+
disk -> parse file 2 -> stream batches -+
...                                     +-> UNION ALL -> GROUP BY (streaming) -> parquet
disk -> parse file 89 -> stream batches +
```

DataFusion can execute this entirely streaming — `AggregateExec` reads from
the UNION, maintains only accumulator state (a few MB for GROUP BY buckets),
and streams output.  At no point would more than one file's parsed output
need to be in memory.

The reason the code materializes defensively is that
`SqlDerivedFile::as_table_provider` returns a `TableProvider` that might be
rescanned by arbitrary future queries.  It doesn't know that the consumer is
a one-shot COPY TO.

For the sitegen export path, the full plan is known: parse -> aggregate ->
COPY TO parquet.  The entire pipeline could be expressed as a single
DataFusion `LogicalPlan` that streams from source to sink with no
intermediate MemTable.

## Potential fix directions

| Approach | Addresses | Notes |
|----------|-----------|-------|
| A. Move `table_exists` check before MemTable construction | Problem 1 (wasted rebuilds) | Eliminates 24 of 25 full parses |
| B. Deregister export tables after COPY | Problem 2 (retained tables) | ViewTable still cached; LogicalPlan still holds Arc |
| C. Fresh SessionContext per export | Problems 1+2 | Loses all session state; source tables re-registered each time |
| D. Pre-build source MemTable once, share | Problem 1 | Requires SqlDerivedFile arch change |
| E. Streaming custom TableProvider for format providers | Root cause | Implements a `TableProvider` that re-runs format parsing on each scan instead of materializing |
| F. Single streaming LogicalPlan for the full pipeline | Root cause | Express parse+aggregate+COPY as one DataFusion plan; zero MemTables |
| G. Pre-convert oteljson to Parquet during ingest | Root cause (at ingest time) | Store as `table:series` instead of `data:series`; all downstream is lazy ListingTable |

### Analysis

**Approaches A-D** are incremental mitigations.  A is the cheapest win
(eliminate 24 redundant 89-file parse cycles).  B is a correctness fix
(other callers always deregister).

**Approaches E-G** address the root cause.  G is the simplest — convert
once at ingest time, then everything downstream uses lazy ListingTables
with no MemTables at all.  F is the most principled DataFusion-native
solution but requires significant refactoring.  E is a middle ground.

## Open questions

1. **Actual per-export memory**: Need instrumentation (`current_usage_as_mb()`
   at key points) to confirm estimates vs the observed 39 GB.
2. **How many times is the 89-file parse actually invoked?** A log counter
   would confirm whether it's really 25.
3. **How large is each MemTable?** Log
   `batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>()` after
   collect to get real numbers.
4. **DataFusion internal caching?** Does the SessionContext retain execution
   artifacts beyond registered tables?
5. **Allocator fragmentation**: Is PeakAlloc measuring true concurrent live
   allocation, or is jemalloc arena fragmentation inflating the number?
