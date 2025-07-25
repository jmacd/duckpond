# Table Provider Architecture Refactoring Plan

## Overview

This document outlines the three-phase plan to correct the table provider architecture in DuckPond's DataFusion integration. The current confusion stems from misaligned purposes between DirectoryTable, MetadataTable, and SeriesTable.

## Current Architectural Issues

### ❌ **DirectoryTable Problem**
- **Current Issue**: Trying to expose `OplogEntry` records via IPC, which is wrong
- **Correct Purpose**: Should expose `VersionedDirectoryEntry` records stored INSIDE the `content` field of directory `OplogEntry` records
- **Desired Usage**: 
  ```sql
  SELECT name, child_node_id, operation_type, node_type 
  FROM directory_contents 
  WHERE parent_directory = '/some/path'
  ```

### ✅ **MetadataTable Purpose** (Correctly Understood)
- **Purpose**: Direct access to the entire TLogFS Delta Lake table (all `OplogEntry` records)
- **Content**: Complete filesystem metadata without content deserialization  
- **Usage**: Finding files, versions, timestamps, entry types across the entire filesystem

### ✅ **SeriesTable Purpose** (Correctly Understood)
- **Purpose**: Time-series queries combining metadata discovery with Parquet file reading
- **Architecture**: Query `MetadataTable` → find file:series versions → read each as Parquet via TinyFS → combine into unified table
- **Usage**: SQL queries with temporal predicates that push down to both metadata and Parquet layers

## Three-Phase Implementation Plan

---

### **Phase 1: Refine DirectoryTable (Get Tests to Pass)**

#### **Objective**
Transform DirectoryTable from incorrectly exposing `OplogEntry` records to properly exposing `VersionedDirectoryEntry` records from directory content fields.

#### **Technical Implementation**

**1.1 Update DirectoryTable Architecture**
- **File**: `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/query/operations.rs`
- **Change**: Modify DirectoryTable to deserialize `VersionedDirectoryEntry` from directory content
- **Schema**: Use `VersionedDirectoryEntry::for_arrow()` instead of `OplogEntry::for_arrow()`

**1.2 Directory Content Deserialization Pattern**
```rust
impl DirectoryTable {
    async fn scan_directory_entries(&self, filters: &[Expr]) -> Result<Vec<RecordBatch>> {
        // 1. Query MetadataTable for directory OplogEntry records
        // 2. For each directory entry, deserialize content field as VersionedDirectoryEntry[]
        // 3. Apply filters to VersionedDirectoryEntry records
        // 4. Return Arrow batches of VersionedDirectoryEntry data
    }
}
```

**1.3 Integration Pattern**
- DirectoryTable should accept a parent directory path or node_id parameter
- Use MetadataTable internally to find the directory OplogEntry
- Deserialize the content field to get `VersionedDirectoryEntry` records
- Present these records as a queryable table

**1.4 Test Requirements**
- Create test directories with known `VersionedDirectoryEntry` content
- Verify DirectoryTable can query directory contents via SQL
- Ensure proper filtering and projection work
- Validate schema matches `VersionedDirectoryEntry::for_arrow()`

---

### **Phase 2: Ensure MetadataTable Feature Completeness (Get Tests to Pass)**

#### **Objective**
Validate MetadataTable as the primary interface to TLogFS Delta Lake table, ensuring it provides complete metadata access without content deserialization.

#### **Technical Implementation**

**2.1 MetadataTable Feature Audit**
- **File**: `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/query/metadata.rs`
- **Current State**: Placeholder returning empty results
- **Required**: Full Delta Lake querying capability

**2.2 Core Functionality Requirements**
```rust
impl MetadataTable {
    // Essential methods for complete TLogFS access
    async fn query_by_node_id(&self, node_id: &str) -> Result<Vec<OplogEntry>>;
    async fn query_by_entry_type(&self, entry_type: EntryType) -> Result<Vec<OplogEntry>>;
    async fn query_by_time_range(&self, start: i64, end: i64) -> Result<Vec<OplogEntry>>;
    async fn query_file_series_versions(&self, node_id: &str) -> Result<Vec<OplogEntry>>;
}
```

**2.3 Delta Lake Integration**
- Replace placeholder implementation with actual Delta Lake querying
- Ensure proper predicate pushdown for efficient filtering
- Handle all `OplogEntry` fields except content (to avoid deserialization issues)
- Support temporal queries using `min_event_time`/`max_event_time`

**2.4 Test Requirements**
- Query existing OplogEntry records from test data
- Verify filtering by node_id, entry_type, version, timestamp
- Ensure no content field deserialization (avoid IPC issues)
- Validate SeriesTable can use MetadataTable for file discovery

---

### **Phase 3: Return to SQL Query Testing (Where We Were Before)**

#### **Objective**
Resume testing the `cat` command with SQL query options, now that the architectural foundation is corrected.

#### **Technical Implementation**

**3.1 SeriesTable Integration Validation**
- **File**: `/Volumes/sourcecode/src/duckpond/crates/cmd/src/commands/cat.rs`
- **Function**: `display_file_series_with_sql_and_node_id()`
- **Requirement**: SeriesTable uses MetadataTable for discovery, TinyFS for Parquet access

**3.2 End-to-End SQL Testing**
```bash
# Test cases to validate
cargo run cat '/ok/test.series' --sql "SELECT * FROM series WHERE timestamp > 1640995200000"
cargo run cat '/ok/test.series' --sql "SELECT COUNT(*) FROM series"
cargo run cat '/ok/test.series' --sql "SELECT * FROM series ORDER BY timestamp LIMIT 10"
```

**3.3 Predicate Pushdown Validation**
- Time range predicates should push down to both MetadataTable and Parquet readers
- Verify efficient querying without loading unnecessary data
- Ensure proper schema loading from Parquet files

**3.4 Error Resolution**
- Address original "failed to fill whole buffer" AsyncRead error
- Ensure proper separation of IPC (directories) vs Parquet (file content) access
- Validate streaming architecture handles large datasets

## Success Criteria

### **Phase 1 Success**
- [ ] DirectoryTable exposes `VersionedDirectoryEntry` schema
- [ ] Directory content queries work via SQL
- [ ] All DirectoryTable tests pass
- [ ] No more confusion about OplogEntry vs VersionedDirectoryEntry

### **Phase 2 Success**
- [ ] MetadataTable returns actual OplogEntry records from Delta Lake
- [ ] Efficient filtering by node_id, entry_type, timestamp works
- [ ] SeriesTable successfully discovers files via MetadataTable
- [ ] All MetadataTable tests pass

### **Phase 3 Success**
- [ ] SQL queries on file:series work end-to-end
- [ ] "failed to fill whole buffer" error is resolved
- [ ] Predicate pushdown functions correctly
- [ ] Cat command SQL interface fully operational

## Architecture After Completion

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   IpcTable      │    │  MetadataTable   │    │  SeriesTable    │
│                 │    │                  │    │                 │
│ Generic IPC     │    │ TLogFS OplogEntry│    │ File:Series     │
│ Reader          │    │ Metadata Access  │    │ Temporal Queries│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       │                       │
┌─────────────────┐              │                       │
│ DirectoryTable  │              │                       │
│                 │              │                       │
│VersionedDir     │◄─────────────┘                       │
│ Entry Queries   │                                      │
└─────────────────┘                                      │
                                                         │
                                ┌─────────────────────────┘
                                ▼
                        Uses MetadataTable for 
                        file discovery, then
                        TinyFS for Parquet access
```

## Dependencies and Risks

### **Phase 1 Risks**
- DirectoryTable refactoring may reveal additional IPC issues
- Need to ensure proper error handling for malformed directory content

### **Phase 2 Risks**  
- MetadataTable implementation complexity with Delta Lake integration
- Performance concerns with large OplogEntry tables

### **Phase 3 Risks**
- Complex interaction between MetadataTable discovery and TinyFS Parquet reading
- Predicate pushdown may require additional DataFusion optimization

## Timeline Estimate

- **Phase 1**: 1-2 days (DirectoryTable refactoring + tests)
- **Phase 2**: 2-3 days (MetadataTable full implementation + tests)  
- **Phase 3**: 1-2 days (SQL integration testing + debugging)

**Total**: 4-7 days for complete architectural correction and testing.
