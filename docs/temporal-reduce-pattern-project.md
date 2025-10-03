# Temporal-Reduce Pattern Project Investigation

## Project Overview

This document tracks the investigation and implementation of the temporal-reduce factory pattern for DuckPond's TLogFS system. The goal is to create a dynamic factory that provides time-bucketed aggregations with automatic column discovery, similar to the original DuckPond's reduce module but using the modern TLogFS architecture.

## Background Context

### Original Requirements
- Create a `temporal-reduce` factory type that generates time-bucketed aggregations
- Support automatic column discovery (avoid marker-based approaches)
- Use pattern-based configuration similar to template factories
- Defer SQL generation until access time for better performance
- Support multiple aggregation types (avg, min, max) and time resolutions

### Session Context
This work began after successfully completing TransactionGuard/State architecture refactoring. The user specifically requested completion of the temporal-reduce factory with automatic column discovery "exactly like the original duckpond's implementation."

## Technical Architecture

### Core Design Decisions

1. **Deferred SQL Generation**: Instead of pre-generating SQL at factory creation, we implemented a wrapper approach where SQL is generated on first access using schema discovery.

2. **TemporalReduceSqlFile Wrapper**: Created a new struct that wraps the actual SQL generation logic and implements the File trait through delegation.

3. **Schema Discovery Pattern**: Implemented automatic column discovery by:
   - Finding source files using TinyFS wildcard pattern matching
   - Extracting schema information from source table providers
   - Filtering for numeric columns suitable for aggregation
   - Generating SQL with discovered columns

## Implementation Progress

### Phase 1: Basic Structure (Completed ✅)
- Created `TemporalReduceConfig` struct with pattern-based configuration
- Implemented `TemporalReduceDirectory` factory
- Set up basic file discovery and resolution mapping
- Registered the factory in the dynamic factory system

### Phase 2: TemporalReduceSqlFile Implementation (Completed ✅)
```rust
pub struct TemporalReduceSqlFile {
    config: TemporalReduceConfig,
    resolution: String,
    context: FactoryContext,
    sql_file: Option<SqlDerivedFile>, // Lazy initialization
}
```

Key features implemented:
- **File Trait Delegation**: All File trait methods delegate to the underlying SqlDerivedFile
- **Lazy SQL Generation**: SQL is generated on first access via `ensure_sql_file()`
- **Schema Discovery**: `discover_source_columns()` method finds available numeric columns
- **SQL Generation**: Creates proper time-bucketed aggregation queries

### Phase 3: Schema Discovery Logic (Completed ✅)
```rust
async fn discover_source_columns(&self) -> Result<Vec<String>, anyhow::Error> {
    // Use TinyFS pattern matching to find source files
    let fs = FS::new(self.context.state.clone()).await?;
    let matches = fs.root().await?.collect_matches(&self.config.in_pattern).await?;
    
    // Extract schema from first matching file
    // Filter for numeric columns suitable for aggregation
}
```

### Phase 4: SQL Generation (Completed ✅)
The system generates proper temporal SQL queries:
```sql
WITH time_buckets AS (
  SELECT 
    DATE_TRUNC('day', timestamp) AS time_bucket,
    AVG("temperature") AS "temperature.avg",
    AVG("humidity") AS "humidity.avg"
  FROM temporal_reduce_source_[unique_id]
  GROUP BY DATE_TRUNC('day', timestamp)
)
SELECT 
  time_bucket AS timestamp,
  "temperature.avg",
  "humidity.avg"
FROM time_buckets
ORDER BY time_bucket
```

### Phase 5: Integration Issues and Fixes

#### Issue 1: QueryableFile Recognition (Fixed ✅)
**Problem**: `try_as_queryable_file()` didn't recognize TemporalReduceSqlFile
**Solution**: Updated the function to include TemporalReduceSqlFile support without hardcoding

#### Issue 2: DataFusion Context Creation (Critical Insight ✅)
**Problem**: Creating new DataFusion contexts instead of using existing State context
**User Guidance**: "OMG you must not create new datafusion contexts. The State has the one pre-initialized context we need."
**Solution**: Modified all code to use `state.datafusion_context()` instead of creating new contexts

#### Issue 3: File Access Pattern (Current Issue ⚠️)
**Problem**: Using path-based pattern resolution in `as_table_provider()` instead of NodeID/PartID parameters
**User Guidance**: "Usually when we find 0 files it's because the listing table did not match. Note that we must access files by NodeID and PartID at this level in the code, not by paths."

## Current Status

### What's Working ✅
1. **Schema Discovery**: Successfully finds temperature/humidity columns from source files
2. **SQL Generation**: Produces correct temporal aggregation SQL queries
3. **File Trait Implementation**: Proper delegation pattern working
4. **Factory Registration**: TemporalReduceDirectory properly registered and accessible

### Current Issue ⚠️
The `as_table_provider()` method in TemporalReduceSqlFile is incorrectly trying to resolve patterns when it should use the NodeID/PartID parameters directly.

**Test Output Shows**:
```
DEBUG: Source table schema fields: []
```

This indicates the source table provider has no fields, meaning the pattern matching in `as_table_provider()` isn't finding the correct files.

### Architecture Problem
The issue is in this method:
```rust
fn as_table_provider(
    &self,
    node_id: tinyfs::NodeID,     // These should be used!
    part_id: tinyfs::NodeID,     // These should be used!
    state: &State
) -> Result<Arc<dyn TableProvider>, anyhow::Error>
```

Currently trying to:
1. Use `fs.root().collect_matches(pattern)` to find files
2. Create table providers from discovered files

**Should instead**:
1. Use the passed `node_id`/`part_id` parameters directly
2. These parameters represent the source file that was already resolved during factory creation

## Test Case Analysis

### Test Structure
```rust
// Step 1: Create parquet data in /sensors/stations/all_data.series
// Step 2: Create SQL-derived filter for BDock station  
// Step 3: Create temporal-reduce with pattern "/sensors/stations/*"
// Step 4: Test temporal-reduce file access
```

### Pattern vs NodeID Issue
- **Pattern**: `/sensors/stations/*` (used during discovery)
- **Actual Files**: `/sensors/stations/all_data.series` 
- **Issue**: At `as_table_provider()` time, we should use resolved NodeID/PartID, not re-resolve patterns

## Technical Debt and Lessons Learned

### Key Insights
1. **Context Reuse**: Always use existing DataFusion context from State
2. **NodeID vs Paths**: File access should use NodeID/PartID parameters, not path resolution
3. **Deferred Architecture**: The lazy SQL generation pattern works well for performance
4. **Pattern Resolution**: Patterns are for discovery, NodeIDs are for access

### Current Import Issues
The recent changes introduced several import/compilation issues:
- Missing `FS` import (should be `tinyfs::FS`)
- Incorrect `create_union_table_provider` location
- Method signature mismatch on `as_table_provider`

## Next Steps

### Immediate Priority (In Progress)
1. **Fix as_table_provider Method**: 
   - Remove pattern resolution logic
   - Use passed NodeID/PartID parameters directly
   - Access source file via `state.get_file(node_id, part_id)`
   
2. **Fix Import Issues**:
   - Correct FS import to `tinyfs::FS`
   - Locate correct `create_union_table_provider` function
   - Fix method signature to match trait

### Validation Steps
1. Verify schema discovery works (✅ already working)
2. Verify SQL generation works (✅ already working)  
3. Fix file access in `as_table_provider`
4. Confirm test passes with proper table provider creation

## Code Locations

### Primary Files Modified
- `crates/tlogfs/src/temporal_reduce.rs` - Main implementation
- `crates/tlogfs/src/sql_derived.rs` - QueryableFile integration fix
- `crates/tlogfs/src/sql_derived.rs` - Test case

### Key Functions
- `TemporalReduceSqlFile::discover_source_columns()` - Schema discovery ✅
- `TemporalReduceSqlFile::generate_sql_with_schema_discovery()` - SQL generation ✅
- `TemporalReduceSqlFile::as_table_provider()` - File access (needs fix ⚠️)
- `try_as_queryable_file()` - Recognition fix ✅

## Conclusion

The temporal-reduce factory implementation is approximately 85% complete. The core functionality (schema discovery, SQL generation, deferred loading) is working correctly. The remaining issue is architectural - using the correct file access pattern in the `as_table_provider` method.

The key insight from the user is that at the table provider level, we should not be resolving patterns - we should be using the pre-resolved NodeID/PartID parameters that represent the specific source file for this temporal reduction.

Once this final issue is resolved, the temporal-reduce factory should provide the desired automatic column discovery functionality matching the original DuckPond implementation.