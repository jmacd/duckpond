# SQL-Derived File Chaining Problem & Solution Design

## Problem Statement

The SQL-derived factory in TLogFS currently fails when trying to execute queries on dynamic files (like `/test-locations/Silver`). The error occurs because the SQL executor assumes all files are regular `OpLogFile` instances stored in the oplog, but SQL-derived files are dynamically generated and don't have physical node_id/part_id representations.

### Current Failing Command
```bash
POND=/tmp/dynpond cargo run --bin pond cat '/test-locations/Silver'
```

**Error**: `Arrow error: FileHandle is not an OpLogFile`

### Expected Behavior
The command should execute the SQL-derived query for the `Silver` entry, which combines data from:
- `/hydrovu/devices/**/SilverVulink1.series` (with temporal bounds applied)
- `/hydrovu/devices/**/SilverVulink2.series` 
- `/hydrovu/devices/**/SilverAT500.series`

Expected output: Combined dataset with ~11,296 points (1315 + 9981 points from the two main sources).

## Root Cause Analysis

### 1. SQL Executor Limitation
The `execute_sql_on_file` function in `crates/tlogfs/src/query/sql_executor.rs` only handles regular files:

```rust
// Lines 53-58: Assumes all files are OpLogFile
let oplog_file = file_any.downcast_ref::<OpLogFile>()
    .ok_or_else(|| TLogFSError::ArrowMessage("FileHandle is not an OpLogFile".to_string()))?;
```

### 2. Dynamic File Architecture Gap
SQL-derived files implement the `File` trait but are not `OpLogFile` instances. They:
- Report correct `EntryType::FileSeries` metadata
- Provide `async_reader()` that returns Parquet-encoded query results
- Cannot be addressed by node_id/part_id (don't exist in oplog)

### 3. Chaining Requirement
The more complex challenge is **chaining**: SQL-derived file A referencing SQL-derived file B in its patterns. Current approach requires full materialization of intermediate results.

## Current State Analysis

### Existing Test: `test_sql_derived_chain`
The current test demonstrates a **materialization approach**:
1. Execute first SQL-derived file → store as `/intermediate.parquet`
2. Commit transaction to make intermediate result visible
3. Second SQL-derived file references `/intermediate.parquet` as regular file

**Problems with current approach**:
- **Performance**: Full materialization of intermediate results
- **Storage overhead**: Temporary files stored in oplog  
- **Transaction complexity**: Multiple transactions required
- **No true chaining**: Can't reference dynamic files directly

### Dynamic Factory Configuration Example
From `test-hydrovu-dynamic-config.yaml`:

```yaml
- name: "Silver"  
  factory: "sql-derived-series"
  config:
    patterns:
      vulink1: "/hydrovu/devices/**/SilverVulink1.series"
      vulink2: "/hydrovu/devices/**/SilverVulink2.series"
      at500: "/hydrovu/devices/**/SilverAT500.series"
    query: |
      SELECT * FROM vulink1
      UNION BY NAME
      SELECT * FROM vulink2  
      UNION BY NAME
      SELECT * FROM at500
      ORDER BY timestamp
```

## Solution Architecture

### Key Insights
1. **Dynamic files need custom table providers** that execute SQL queries directly
2. **Table providers must be registered at query execution time** within the same SessionContext
3. **Pattern resolution must handle both regular and dynamic files**
4. **Recursive resolution is needed** for chaining scenarios

### Phase 1: Enhanced SQL Executor
Modify `execute_sql_on_file` to handle both regular and dynamic files:

```rust
// Pseudo-code approach
match file_handle.downcast() {
    Some(oplog_file) => {
        // Existing path: create ListingTable with TinyFS ObjectStore
        let table_provider = create_listing_table_provider(node_id, part_id, tx).await?;
    }
    Some(sql_derived_file) => {
        // New path: create custom table provider for dynamic file
        let table_provider = sql_derived_file.create_table_provider(tx).await?;
    }
    _ => return Err("Unsupported file type")
}
```

### Phase 2: Dynamic Table Provider Creation
Create `SqlDerivedTableProvider` that:
- Implements DataFusion's `TableProvider` trait
- Recursively resolves pattern files (regular or dynamic)
- Executes SQL query against registered table providers
- Returns streaming results without materialization

### Phase 3: Enhanced Pattern Resolution
Modify `resolve_pattern_to_files` to return `ResolvedResource` enum:

```rust
enum ResolvedResource {
    RegularFile { path: String, node_id: String, part_id: String },
    DynamicFile { path: String, file_handle: FileHandle },
}
```

### Phase 4: Unified Table Registration
- Register both regular file ListingTables and dynamic file table providers
- Use unique table names to avoid conflicts
- Manage lifecycle within single SessionContext

## Implementation Challenges

### 1. Recursive Resolution
When SqlDerivedFile A references SqlDerivedFile B:
- A's pattern resolution must detect B is dynamic
- A must recursively create B's table provider
- Both must share the same SessionContext

### 2. Lifecycle Management
- Table providers registered at query execution time
- Need unique names to avoid conflicts
- Proper cleanup when query completes

### 3. Temporal Bounds Integration
- Regular files: temporal bounds applied via TinyFS ObjectStore
- Dynamic files: must propagate temporal bounds to underlying files
- Ensure consistent filtering across the chain

## Testing Strategy

### Unit Test: `test_temporal_bounds_on_file_series`
✅ **Completed**: Tests basic temporal bounds functionality on regular files
- Creates file series with 12 data points
- Sets temporal bounds [10000, 20000] ms using empty version approach  
- Verifies count reduces from 12 to 10 rows (excludes T=1s and T=50s)

### Integration Test Needed: True Chaining
Need test that verifies:
1. SqlDerivedFile A references SqlDerivedFile B directly (no materialization)
2. Temporal bounds on underlying files are respected
3. Query execution produces expected results
4. Performance is acceptable (no unnecessary I/O)

## Expected Workflow After Implementation

1. **`pond cat '/test-locations/Silver'`** resolves to dynamic file
2. **SQL executor** detects SqlDerivedFile, creates custom table provider
3. **Pattern resolution** finds underlying files (some with temporal bounds)
4. **Table registration** creates providers for all pattern sources
5. **Query execution** runs SQL against registered tables
6. **Streaming results** returned without intermediate materialization

## Files to Modify

1. **`crates/tlogfs/src/query/sql_executor.rs`**: Enhance to handle dynamic files
2. **`crates/tlogfs/src/sql_derived.rs`**: Add table provider creation methods
3. **`crates/tlogfs/src/file_table.rs`**: Add path-based table provider factory
4. **`crates/tlogfs/src/tests.rs`**: Add integration test for chaining

## Success Criteria

1. **`pond cat '/test-locations/Silver'`** executes successfully
2. **Temporal bounds are respected** in the combined result
3. **No intermediate materialization** required
4. **Chaining works recursively** (SqlDerivedFile → SqlDerivedFile)
5. **Performance is acceptable** (only necessary I/O)

---

*Document created: September 14, 2025*
*Context: Following investigation of SQL-derived file chaining failure and temporal bounds integration*