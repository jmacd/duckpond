# Table Provider Architecture - Current State & Future Evolution

## Overview

This document describes the **successfully implemented and recently updated** table provider architecture in DuckPond's DataFusion integration. The system has evolved from 3 providers to 4 active providers after dead code removal and renaming for clarity.

## ✅ **Current Architecture Status: 4 ACTIVE PROVIDERS** ✅ (September 7, 2025)

### **🔥 Recent Updates (September 7, 2025)**
- **✅ Dead Code Removal**: Removed unused `IpcTable` and `IpcExec` (386 lines)
- **✅ Clarity Renaming**: `MetadataTable` → `NodeTable` for better semantic clarity
- **✅ Architecture Cleanup**: Clear separation between user data access and filesystem metadata

### ✅ **SeriesTable: PRODUCTION READY** ✅ 
- **Status**: **Complete end-to-end FileSeries SQL functionality working**
- **Purpose**: Time-series queries combining metadata discovery with Parquet file reading
- **File Types**: `file:series` (multi-version, append-only)
- **Architecture**: Query `NodeTable` → discover file:series versions → read via TinyFS → unified temporal queries
- **Integration**: Complete CLI integration via `cat` command with `--sql` flag

### ✅ **TableTable: PRODUCTION READY** ✅
- **Status**: **Complete single-version table SQL functionality working**
- **Purpose**: Single-version table queries
- **File Types**: `file:table` (single version, replacement-based)
- **Architecture**: Query `NodeTable` → discover latest table version → read via TinyFS → direct SQL access
- **Integration**: Complete CLI integration via `cat` command

### ✅ **NodeTable: SCHEMA COMPLETE, SQL INTERFACE PLANNED** ✅
- **Status**: **Programmatic API complete**, ⚠️ **SQL interface needed for overlap detection**
- **Purpose**: Direct access to node metadata (OplogEntry records) **without file names**
- **Content**: Complete node metadata with temporal columns, file types, versions
- **Current Capabilities**: Node-based queries, temporal filtering, version discovery
- **Architecture**: Avoids content field deserialization, preventing IPC issues
- **🎯 Next Phase**: Complete DataFusion TableProvider::scan() for SQL queries

### ✅ **DirectoryTable: ARCHITECTURALLY CORRECT, DEVELOPMENT NEEDED** ✅
- **Status**: **Properly designed for VersionedDirectoryEntry exposure**
- **Purpose**: Directory content queries providing **file names and paths**
- **Architecture**: NodeTable → directory OplogEntry → deserialize content → VersionedDirectoryEntry records
- **🎯 Next Phase**: Complete SQL interface for name resolution and path queries

## 🎯 **Updated Architecture: CLEAN 4-PROVIDER SYSTEM** 🎯

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SeriesTable   │    │   TableTable     │    │   NodeTable     │    │ DirectoryTable  │
│   ✅ COMPLETE   │    │   ✅ COMPLETE    │    │ ⚠️ SQL NEEDED   │    │ ⚠️ DEV NEEDED   │
│                 │    │                  │    │                 │    │                 │
│ FileSeries SQL  │    │ FileTable SQL    │    │ Node Metadata   │    │ Directory Names │
│ Temporal Queries│    │ Single Version   │    │ No Names/Paths  │    │ File Paths      │
│ + Parquet Data  │    │ + Parquet Data   │    │ Temporal Fields │    │ JOIN Partner    │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         ▼                       ▼                       │                       │
┌───────────────────────────────────────────────────────┼───────────────────────┘
│                    USER DATA ACCESS                   │
│                                                       │
│ CSV Files → Parquet → Temporal Metadata → SQL Queries│
│ Versioning → TLogFS Delta Storage → DataFusion ✅     │
└───────────────────────────────────────────────────────┘
                                                       │
                ┌──────────────────────────────────────┘
                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FILESYSTEM METADATA ACCESS                  │
│                                                                 │
│ Node Metadata (temporal, types) + Directory Names (paths)      │
│ SQL Overlap Detection + Name Resolution → Complete Analysis ⚠️  │
└─────────────────────────────────────────────────────────────────┘
```

### **Clear Architectural Separation:**

**User Data Layer** (Production Ready):
- `SeriesTable` - Multi-version time-series access
- `TableTable` - Single-version table access

**Filesystem Metadata Layer** (Development Needed):
- `NodeTable` - Node metadata without names (temporal overlap detection)
- `DirectoryTable` - Directory entries with names (name resolution)

### **Planned SQL Integration Architecture:**

```sql
-- Example: Find temporal overlaps with file names
WITH overlapping_nodes AS (
    SELECT a.node_id as node_a, b.node_id as node_b,
           a.file_type, a.min_event_time, a.max_event_time
    FROM nodes a 
    JOIN nodes b ON a.node_id < b.node_id
    WHERE a.file_type = 'file:series'
      AND a.max_event_time > b.min_event_time 
      AND a.min_event_time < b.max_event_time
),
node_names AS (
    SELECT child_node_id as node_id, name, path
    FROM directory_entries 
    WHERE operation_type = 'Insert'
)
SELECT o.*, n1.name as file_a_name, n2.name as file_b_name
FROM overlapping_nodes o
JOIN node_names n1 ON o.node_a = n1.node_id
JOIN node_names n2 ON o.node_b = n2.node_id;
```

## **🚀 Current Development Plan: SQL Interface for Metadata Analysis**

### **Phase 1: Complete NodeTable SQL Interface** ⏱️ *2-3 hours*

**Goal**: Enable SQL queries against node metadata for temporal overlap detection

**Implementation**:
1. **Complete `TableProvider::scan()` method** in `nodes.rs`
2. **Create NodeExecutionPlan** similar to SeriesExecutionPlan but for OplogEntry records
3. **Add to SQL executor** - Register NodeTable in DataFusion context

**Example Usage**:
```sql
-- Find temporal overlaps in FileSeries nodes
SELECT node_id, min_event_time, max_event_time, min_override, max_override
FROM nodes 
WHERE file_type = 'file:series'
  AND min_event_time IS NOT NULL;
```

### **Phase 2: Complete DirectoryTable SQL Interface** ⏱️ *2-3 hours*

**Goal**: Enable SQL queries against directory entries for name resolution

**Implementation**:
1. **Complete `TableProvider::scan()` method** in `operations.rs`
2. **Enhance DirectoryExecutionPlan** for efficient VersionedDirectoryEntry queries
3. **Add bulk directory queries** beyond single node_id filtering

**Example Usage**:
```sql
-- Get file names and paths for nodes
SELECT child_node_id, name, parent_path, operation_type
FROM directory_entries 
WHERE operation_type = 'Insert' 
  AND name LIKE '%.series';
```

### **Phase 3: Add "pond query-metadata" Commands** ⏱️ *1-2 hours*

**Goal**: CLI interface for metadata SQL queries with pretty printing

**Commands**:
```bash
# Query nodes directly
pond query-nodes "SELECT * FROM nodes WHERE file_type = 'file:series'"

# Query directory entries
pond query-directories "SELECT * FROM directory_entries WHERE name LIKE '%.series'"

# Combined metadata context (registers both tables)
pond query-metadata "
  SELECT n.node_id, n.min_event_time, d.name, d.parent_path
  FROM nodes n 
  JOIN directory_entries d ON n.node_id = d.child_node_id
  WHERE n.file_type = 'file:series'
"
```

### **Phase 4: Temporal Overlap Detection CLI** ⏱️ *2 hours*

**Goal**: SQL-driven overlap detection with file name resolution

**Integration**:
```bash
# Automated overlap detection with names
pond check-overlaps '/hydrovu/devices/**/SilverVulink*.series'

# Implementation: Expands pattern → SQL query across both tables
```

**Backend SQL**:
```sql
WITH overlapping_files AS (
  -- Complex overlap detection query combining NodeTable + DirectoryTable
  SELECT ... FROM nodes JOIN directory_entries ...
)
SELECT file_a_name, file_b_name, overlap_days FROM overlapping_files;
```

## Key Technical Achievements

### **1. Clean Architecture Separation** ✅
**User Data vs Filesystem Metadata**: Clear boundary between data access and metadata analysis
```rust
// User Data Access (Complete)
SeriesTable::new(path, node_table)  // Time-series data
TableTable::new(path, node_table)   // Single-version data

// Filesystem Metadata Access (In Development) 
NodeTable::new(delta_table)         // Node metadata (no names)
DirectoryTable::new(delta_table)    // Directory entries (with names)
```

### **2. Dead Code Elimination** ✅  
**Removed**: `IpcTable` and `IpcExec` (386 lines of unused code)
- Was never integrated into the working system
- DirectoryTable handles IPC directly via `arrow::ipc::reader::StreamReader`
- Simplified architecture from 5 → 4 active providers

### **3. Semantic Clarity** ✅
**Renamed**: `MetadataTable` → `NodeTable` for better understanding
- Makes clear it provides node metadata without file names/paths
- Emphasizes need for DirectoryTable JOIN operations to get complete information
- Aligns with temporal overlap detection requirements

### **4. Temporal Override System** ✅
**Architecture**: Per-file temporal bounds with current version propagation
```rust
// NodeTable provides temporal fields for overlap detection
pub struct OplogEntry {
    min_event_time: Option<i64>,     // Computed from data
    max_event_time: Option<i64>,     // Computed from data  
    min_override: Option<i64>,       // Manual override (current version defines all)
    max_override: Option<i64>,       // Manual override (current version defines all)
}
```

## Current Status & Next Steps

### **✅ Production Ready Components**
- **SeriesTable**: Complete FileSeries temporal queries ✅
- **TableTable**: Complete FileTable single-version queries ✅
- **NodeTable**: Programmatic API complete, SQL interface needed ⚠️
- **Temporal Override System**: Schema and logic complete ✅

### **⚠️ Development Needed Components**
- **NodeTable SQL Interface**: TableProvider::scan() implementation needed
- **DirectoryTable SQL Interface**: Complete VersionedDirectoryEntry SQL access
- **CLI Integration**: `pond query-nodes`, `pond query-directories`, `pond query-metadata`
- **Overlap Detection**: SQL-driven temporal analysis with name resolution

### **🎯 Immediate Next Actions**
1. **Complete NodeTable::scan()** - Enable `SELECT * FROM nodes WHERE file_type = 'file:series'`
2. **Complete DirectoryTable::scan()** - Enable `SELECT * FROM directory_entries WHERE name LIKE '%.series'`
3. **Add CLI Commands** - `pond query-metadata` with DataFusion context registration
4. **Implement Overlap Detection** - SQL queries across both tables for complete analysis

### **📊 Success Metrics**
- [ ] **NodeTable SQL**: `SELECT node_id, min_event_time FROM nodes` works
- [ ] **DirectoryTable SQL**: `SELECT name, child_node_id FROM directory_entries` works  
- [ ] **JOIN Queries**: Combined metadata + name queries work
- [ ] **CLI Integration**: Pretty-printed metadata query results
- [ ] **Overlap Detection**: End-to-end temporal overlap detection with file names

## Testing & Validation Status

### **✅ Current Test Coverage**
- **Unit Tests**: 27+ tests across tlogfs passing
- **Integration Tests**: End-to-end FileSeries workflow validated
- **CLI Tests**: Complete `cat` command SQL functionality working
- **Architecture Tests**: Dead code removal and renaming successful

### **⚠️ Needed Test Coverage**
- **NodeTable SQL**: DataFusion integration tests
- **DirectoryTable SQL**: VersionedDirectoryEntry query tests
- **Metadata CLI**: Command-line interface integration tests
- **Overlap Detection**: End-to-end temporal analysis validation

## Conclusion

The DuckPond table provider architecture has successfully evolved to a **clean, purpose-driven 4-provider system** with clear separation between user data access and filesystem metadata analysis.

### **Current State: September 7, 2025**

1. **Clean Architecture**: User data (SeriesTable, TableTable) + Filesystem metadata (NodeTable, DirectoryTable)
2. **Dead Code Eliminated**: Removed unused IpcTable (386 lines) 
3. **Semantic Clarity**: MetadataTable → NodeTable for better understanding
4. **Production Ready Data Access**: Complete FileSeries and FileTable SQL functionality
5. **Temporal Override Foundation**: Complete schema and programmatic API for overlap detection

### **Next Phase: Metadata SQL + Overlap Detection**

The system is positioned for the next evolution phase:
- **SQL-driven metadata analysis** via NodeTable and DirectoryTable
- **Temporal overlap detection** using SQL queries instead of complex Rust logic  
- **Complete name resolution** through JOIN operations between node metadata and directory entries
- **CLI integration** for interactive metadata exploration and pretty-printing

This architecture provides the **foundation for advanced temporal analytics** while maintaining the production-ready data access layer that already powers DuckPond's time-series data lake capabilities.

The clear separation between **data access** and **metadata analysis** creates a maintainable, extensible system ready for sophisticated temporal overlap detection and resolution workflows.

---

*Updated: September 7, 2025 - Architecture cleaned up and positioned for SQL-driven metadata analysis*
