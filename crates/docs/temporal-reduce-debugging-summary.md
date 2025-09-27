# Temporal-Reduce Factory Debugging Summary

**Date**: September 16, 2025  
**Status**: Core architectural issue identified, solution approach defined

## Problem Statement

The temporal-reduce dynamic factory (for time-bucketed aggregations) is failing when trying to query other dynamic files. Specifically, `/test-locations/BDockDownsampled/res=1h.series` fails with "table 'datafusion.public.source' not found" errors.

## Architecture Overview

### Expected Flow (2-Level Dynamic Files)
```
BDockDownsampled (temporal-reduce) 
    → SQL: SELECT time_bucket, AVG(temp) FROM source GROUP BY time_bucket
    → "source" table = BDock's TableProvider

BDock (sql-derived-series)
    → SQL: SELECT * FROM parquet_files WHERE device='BDock'  
    → parquet_files = Real parquet files via ListingTable

Real Parquet Files
    → Actual HydroVu sensor data stored in TinyFS
```

### Current Implementation Status
- ✅ **BDock works individually** - can be queried directly, returns 11,885 rows
- ✅ **Temporal-reduce factory registered** - linkme system recognizes it
- ✅ **BDockDownsampled directory created** - temporal-reduce factory instantiated correctly
- ❌ **Table registration fails** - BDock not properly registered as "source" table

## Root Cause Analysis

### Key Insight: Table Registration Architecture
All dynamic nodes work by:
1. **Resolving source patterns** to find target files/nodes
2. **Registering target TableProviders** with unique names in DataFusion SessionContext
3. **Executing SQL queries** that reference those registered table names
4. **Returning TableProvider** that DataFusion can query further

### The Specific Failure Point
When BDockDownsampled (temporal-reduce) tries to register BDock as "source":

**Current Broken Flow:**
1. `SqlDerivedFile.resolve_pattern_to_files("/test-locations/BDock")` ✅ - finds BDock node correctly
2. `create_listing_table_for_registered_files()` ❌ - tries to create ListingTable via ObjectStore
3. Calls `list_file_versions()` ❌ - looks for stored parquet versions that don't exist for dynamic files
4. Finds 0 records ❌ - BDock appears empty because it's dynamic, not stored
5. Creates empty TableProvider ❌ - "source" table has no data

**Correct Flow Should Be:**
1. `SqlDerivedFile.resolve_pattern_to_files("/test-locations/BDock")` ✅ - finds BDock node correctly  
2. **Detect BDock is dynamic** (SqlDerivedFile, not real parquet)
3. **Call BDock's `as_table_provider()`** - gets BDock's SQL-based TableProvider
4. **Register BDock's TableProvider** as "source" in SessionContext
5. BDockDownsampled executes SQL over registered "source" table

## Technical Details

### File Type Architecture
- **OpLogFile** = Real files with stored parquet data
  - `OpLogFile::as_table_provider()` → calls `create_listing_table_provider()` → ListingTable via ObjectStore
- **SqlDerivedFile** = Dynamic files with SQL-generated data  
  - `SqlDerivedFile::as_table_provider()` → executes SQL query → returns SQL expression TableProvider

### Current Code Paths
```rust
// In sql_derived.rs - Series mode
for (pattern_name, pattern) in &self.config.patterns {
    let resolved_files = self.resolve_pattern_to_files(&tinyfs_root, pattern, EntryType::FileSeries).await?;
    
    if !resolved_files.is_empty() {
        // THIS IS THE PROBLEM: Always uses ListingTable approach
        let table_provider = self.create_listing_table_for_registered_files(&ctx, &resolved_files).await?;
        
        let unique_table_name = self.generate_unique_table_name(pattern_name, &resolved_files);
        ctx.register_table(&unique_table_name, table_provider)?;
    }
}
```

### Debug Evidence
From `OUT` file:
```
20:52:15.606 debug tlogfs sql_derived resolve_pattern_to_files: pattern='/test-locations/BDock', is_exact_path=true
20:52:15.626 debug tlogfs sql_derived resolve_pattern_to_files: found 1 matches for pattern '/test-locations/BDock'
20:52:15.651 debug tlogfs persistence list_file_versions found 0 records for node 019955cd-340a-7493-8d48-00831c3a6ed1
20:52:15.695 debug tlogfs file_table Found 0 records for node_id 019955cd-340a-7493-8d48-00831c3a6ed1
```

Shows: Pattern resolution works, but then it goes through file version lookup instead of QueryableFile path.

## Solution Approach

### Option 1: Fix create_listing_table_for_registered_files()
Modify the existing method to detect dynamic files and handle them appropriately:

```rust
async fn create_listing_table_for_registered_files(&self, ctx: &SessionContext, resolved_files: &[ResolvedFile]) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    // Check if resolved files are dynamic vs real
    for resolved_file in resolved_files {
        // Get actual file handle from TinyFS
        // Check if it's SqlDerivedFile vs OpLogFile
        // If SqlDerivedFile: call as_table_provider()
        // If OpLogFile: use existing ListingTable logic
    }
}
```

### Option 2: Eliminate ResolvedFile Indirection
Replace `ResolvedFile` metadata with direct OpLogFile instances:

```rust
// Instead of: resolve_pattern_to_files() -> Vec<ResolvedFile>
// Use: resolve_pattern_to_oplog_files() -> Vec<Arc<dyn File>>

async fn resolve_pattern_to_oplog_files(&self, tinyfs_root: &tinyfs::WD, pattern: &str) -> Vec<Arc<dyn crate::file::File>> {
    // Return actual File instances (OpLogFile or SqlDerivedFile)
    // Then call appropriate as_table_provider() method on each
}
```

### Recommended Approach
**Option 1** is simpler and less disruptive. The key insight is that `create_listing_table_for_registered_files()` needs to:

1. **Resolve ResolvedFile back to actual File handle** (via TinyFS path resolution)
2. **Check File type** (OpLogFile vs SqlDerivedFile)
3. **Call appropriate as_table_provider() method**
4. **Register the resulting TableProvider**

## Implementation Status

### Completed Work
- ✅ Enhanced pattern resolution to handle exact paths vs glob patterns
- ✅ Added debug logging to trace pattern resolution flow  
- ✅ Identified root cause: wrong table provider creation path
- ✅ Confirmed architecture understanding: dynamic files should use QueryableFile, not ObjectStore

### In Progress  
- 🔄 Started modifying `create_listing_table_for_registered_files()` to handle dynamic files
- 🔄 Added detection logic for dynamic vs real files

### Next Steps
1. **Complete dynamic file detection** in table provider creation
2. **Implement QueryableFile access** for resolved dynamic files
3. **Test end-to-end flow** with actual temporal aggregation queries
4. **Validate schema alignment** between source and aggregated data

## Anti-Duplication Compliance

Following DuckPond's anti-duplication principles:
- ✅ **No function duplication** - enhancing existing methods with configuration
- ✅ **Options pattern usage** - exact path detection via conditional logic  
- ✅ **Single responsibility** - each method has clear purpose
- ⚠️ **Avoided near-duplication** - stopped before creating separate code paths for dynamic vs real files

## Test Environment

**Pond Location**: `/tmp/dynpond`  
**Test Command**: `RUST_LOG=debug POND=/tmp/dynpond cargo run --bin pond cat '/test-locations/BDockDownsampled/res=1h.series'`

**Working Components:**
- BDock direct access: `pond cat '/test-locations/BDock.series'` → 11,885 rows
- Dynamic directory creation: BDockDownsampled directory exists
- Factory registration: temporal-reduce factory recognized

**Failing Component:**
- Dynamic-to-dynamic table provider chain: BDockDownsampled → BDock registration

## Key Files Modified

### `/crates/tlogfs/src/sql_derived.rs`
- Enhanced `resolve_pattern_to_files()` with exact path detection
- Added debug logging for pattern resolution
- Started modifying `create_listing_table_for_registered_files()` 

### `/crates/tlogfs/src/temporal_reduce.rs`  
- Complete temporal-reduce factory implementation (574 lines)
- 8 unit tests passing
- Generates time-bucketed SQL queries correctly
- Uses SqlDerivedFile for table provider creation

## Architecture Lessons Learned

1. **Dynamic files should never go through ObjectStore/file version lookup**
2. **All Files implement QueryableFile - use that interface consistently**
3. **Table provider chaining requires proper SessionContext registration**
4. **Pattern resolution vs exact path resolution are different use cases**
5. **Debug logging is essential for understanding complex DataFusion flows**

## Success Criteria

When fixed, the following should work:
```bash
pond cat '/test-locations/BDockDownsampled/res=1h.series'
# Should return time-bucketed aggregations of BDock sensor data
# With hourly resolution: timestamp, avg_temperature, avg_conductivity, etc.
```

The data flow should be:
**User Query** → **BDockDownsampled TableProvider** → **SQL over "source"** → **BDock TableProvider** → **SQL over parquet files** → **Actual sensor data**

## Critical Architecture Questions ✅ ALL RESOLVED

All fundamental architectural questions have been resolved. Ready to proceed with implementation:

### Question 1: Transaction Context Architecture ✅ RESOLVED
**Problem**: QueryableFile trait requires `TransactionGuard` context, but current method signatures don't provide access to them.

**SOLUTION**: FactoryContext contains State, which IS the transaction guard equivalent. The State is the TinyFS persistence layer.

**Correct Architecture**:
```rust
impl SqlDerivedFile {
    async fn resolve_pattern_to_oplog_files(&self, pattern: &str) -> TinyFSResult<Vec<Arc<dyn File>>> {
        // HAVE: self.context.state - this IS the transaction context
        // BUILD: TinyFS instance from the State
        let tinyfs = TinyFS::from_state(&self.context.state);
        let tinyfs_root = tinyfs.root();
        
        // USE: collect_matches to get NodePath instances
        let matches = tinyfs_root.collect_matches(pattern).await?;
        
        // EXTRACT: From NodePath -> Node -> Handle -> Arc<dyn File>
        // NodePath = Node + Path
        // Node = Arc<Mutex<_>> containing ID + Handle  
        // Handle = Arc<Mutex<Box<_>>> around the actual File
    }
}
```

**Key Insights**:
- ✅ Transaction context is available via `self.context.state`
- ✅ TinyFS is not a POSIX filesystem - use proper NodePath APIs
- ✅ No signature redesign needed - context already contains what we need

### Question 2: TinyFS to QueryableFile Bridge ✅ RESOLVED
**Problem**: We need to convert from TinyFS's internal file representation to QueryableFile trait instances.

**SOLUTION**: TinyFS Handle IS the File! The key insight is understanding the type hierarchy:

**Correct TinyFS Type Flow**:
```rust
// Pattern resolution gives us NodePath instances
let matches = tinyfs_root.collect_matches(pattern).await?;

// NodePath contains Arc<Mutex<Node>> + Path
for (node_path, _captured) in matches {
    let node_ref = node_path.borrow().await;
    
    // NodePathRef has as_file() method
    if let Ok(file_node) = node_ref.as_file() {
        // file_node: FileNode = Pathed<crate::file::Handle>
        // file_node.handle is the Handle that contains the actual File implementation
        
        if let Ok(metadata) = file_node.metadata().await {
            if metadata.entry_type == entry_type {
                // THE KEY: Handle IS Arc<Mutex<Box<dyn File>>>
                // We need to extract the inner File from the Handle
                let file_handle = file_node.handle; // This is tinyfs::file::Handle
                
                // Extract Arc<dyn File> from Handle
                // Based on node_factory pattern: Handle wraps OpLogFile or SqlDerivedFile
                let arc_file: Arc<dyn tinyfs::File> = /* extract from handle */;
                oplog_files.push(arc_file);
            }
        }
    }
}
```

**Key Architecture Understanding**:
- ✅ `NodePath` = Node + Path information  
- ✅ `NodePathRef.as_file()` → `FileNode` (which is `Pathed<Handle>`)
- ✅ `Handle` = `Arc<Mutex<Box<dyn File>>>` - this IS what we need
- ✅ Both `OpLogFile` and `SqlDerivedFile` implement `File` trait
- ✅ Both also implement `QueryableFile` trait

**Complete Implementation**:
```rust
pub async fn resolve_pattern_to_oplog_files(&self, pattern: &str, entry_type: EntryType) -> TinyFSResult<Vec<Arc<dyn tinyfs::File>>> {
    // STEP 1: Build TinyFS from State (transaction context)
    let fs = tinyfs::FS::new(self.context.state.clone()).await?;
    let tinyfs_root = fs.root().await?;
    
    // STEP 2: Use collect_matches to get NodePath instances  
    let matches = tinyfs_root.collect_matches(pattern).await?;
    
    let mut oplog_files = Vec::new();
    
    // STEP 3: Extract Arc<dyn File> from each NodePath
    for (node_path, _captured) in matches {
        let node_ref = node_path.borrow().await;
        
        if let Ok(file_node) = node_ref.as_file() {
            if let Ok(metadata) = file_node.metadata().await {
                if metadata.entry_type == entry_type {
                    // STEP 4: Extract Arc<dyn File> from Handle
                    let file_handle = file_node.handle; // tinyfs::file::Handle
                    let arc_mutex_box_file = file_handle.get_file().await; // Arc<Mutex<Box<dyn File>>>
                    
                    // Convert to Arc<dyn File> by creating wrapper or extracting
                    // This is the final conversion step needed
                    oplog_files.push(/* convert arc_mutex_box_file to Arc<dyn File> */);
                }
            }
        }
    }
    
    Ok(oplog_files)
}
```

**Remaining Detail**: Convert `Arc<Mutex<Box<dyn File>>>` to `Arc<dyn File>` for QueryableFile usage.

### Question 3: ViewTable vs Direct TableProvider ✅ RESOLVED
**Problem**: Unclear whether we should use DataFusion's ViewTable for SQL delegation or direct TableProvider forwarding.

**SOLUTION**: ViewTable approach is correct. Direct forwarding is architecturally wrong.

**✅ CORRECT - ViewTable Approach**:
```rust
// Register source tables first
for (pattern_name, pattern) in &self.config.patterns {
    let oplog_files = self.resolve_pattern_to_oplog_files(pattern, entry_type).await?;
    for file in oplog_files {
        let table_provider = file.as_table_provider(node_id, part_id, tx).await?;
        ctx.register_table(pattern_name, table_provider)?;
    }
}

// Create ViewTable that references registered tables
let effective_sql = self.get_effective_sql(&transform_options);
let logical_plan = ctx.sql(&effective_sql).await?.logical_plan().clone();
let view_table = ViewTable::new(logical_plan, Some(effective_sql));
```

**❌ WRONG - Direct Forwarding Approach**:
```rust
let source_table_provider = source_file.as_table_provider(&mut tx).await?;
// Return source_table_provider directly - THIS BREAKS SQL TRANSFORMATION
```

**Why ViewTable is Correct**:
- ✅ **Preserves SQL transformation**: ViewTable executes the SqlDerivedFile's SQL query
- ✅ **Handles table name mapping**: Transforms pattern names to registered table names  
- ✅ **Streaming execution**: DataFusion optimizes the entire query plan
- ✅ **No materialization**: ViewTable is just a logical plan wrapper

**Why Direct Forwarding is Wrong**:
- ❌ **Loses SQL transformation**: SqlDerivedFile's SQL query is ignored
- ❌ **Breaks table delegation**: No way to reference multiple source tables
- ❌ **Wrong abstraction level**: Returns wrong TableProvider for the wrong SQL

**Key Insight**: SqlDerivedFile needs to execute ITS OWN SQL over registered source tables, not return a source table directly.

### Question 4: Error Handling Philosophy ✅ RESOLVED
**Problem**: Distinguishing between acceptable explicit errors vs dangerous silent fallbacks.

**SOLUTION**: `return Err(...)` is GOOD fail-fast behavior. Silent fallbacks are the problem.

**✅ GOOD - Explicit Error (Fail-Fast)**:
```rust
// This is GOOD - fails loudly and immediately:
return Err(tinyfs::Error::Other("File extraction not yet implemented"));
```

**❌ BAD - Silent Fallback (Masks Problems)**:
```rust
// This is BAD - pretends success when it should fail:
return Ok(0); // Incorrect placeholder for now
return Ok(Vec::new()); // Empty result hides the real issue
if matches.is_empty() { return Ok(()); } // Silent do-nothing fallback
```

**Key Insights from Fallback Anti-Pattern**:
- ✅ **Explicit errors are good** - they force calling code to handle edge cases
- ❌ **Silent fallbacks are bad** - they mask architectural problems and create inconsistent behavior
- ✅ **Fail-fast philosophy** - if something is wrong, fail immediately and loudly
- ❌ **"Just in case" fallbacks** - added without clear understanding of when they trigger

**Our Current Code Status**: We're using explicit errors correctly. The placeholders like `return Err("not yet implemented")` are proper fail-fast behavior, not problematic fallbacks.

### Question 5: Method Responsibility Boundaries ✅ RESOLVED
**Problem**: Unclear separation between pattern resolution, file extraction, and table provider creation.

**SOLUTION**: Clean separation with `as_table_provider()` as the single entry point.

**✅ CORRECT - Clear Responsibility Separation**:
```rust
// UTILITY LAYER: Pattern resolution to File instances
async fn resolve_pattern_to_oplog_files(&self, pattern: &str, entry_type: EntryType) -> TinyFSResult<Vec<Arc<dyn File>>> {
    // Single responsibility: TinyFS pattern matching + file extraction
    // Input: pattern string
    // Output: Arc<dyn File> instances (both OpLogFile and SqlDerivedFile)
}

// UTILITY LAYER: File instances to QueryableFile trait objects  
fn files_to_queryable(&self, files: Vec<Arc<dyn File>>) -> Vec<&dyn QueryableFile> {
    // Single responsibility: Type conversion using trait dispatch
    // Input: Arc<dyn File> instances
    // Output: QueryableFile trait objects
}

// ENTRY POINT: Complete table provider creation
async fn as_table_provider(&self, node_id: NodeID, part_id: NodeID, tx: &mut TransactionGuard<'_>) -> Result<Arc<dyn TableProvider>, TLogFSError> {
    // Single responsibility: Complete DataFusion integration
    // 1. Use utility functions to get source files
    // 2. Register source tables in SessionContext  
    // 3. Create ViewTable with transformed SQL
    // 4. Return final TableProvider
}
```

**Key Architectural Principles**:
- ✅ **Single Entry Point**: `as_table_provider()` is the only public interface for table provider creation
- ✅ **Utility Functions**: `resolve_pattern_to_oplog_files()` handles TinyFS complexity internally
- ✅ **Clear Boundaries**: Each method has exactly one responsibility
- ✅ **Transaction Context**: Managed at the entry point level, passed down as needed
- ✅ **No Hybrid Functions**: No "do X, but if that fails, do Y" patterns

**Method Responsibilities**:
- `resolve_pattern_to_oplog_files()`: TinyFS pattern resolution + file extraction
- `as_table_provider()`: DataFusion SessionContext management + ViewTable creation

---

*These questions need resolution before implementing the actual file extraction and QueryableFile integration. Each represents a fundamental architectural decision that will affect the maintainability and correctness of the solution.*

---

*This document captures the current understanding of the temporal-reduce dynamic factory debugging process. The core issue is identified and the solution path is clear: fix dynamic file detection in table provider creation.*