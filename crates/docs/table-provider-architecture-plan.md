# Table Provider Architecture - Current State & Future Evolution

## Overview

This document describes the **successfully implemented and working** table provider architecture in DuckPond's DataFusion integration. The system has successfully evolved from complex NodeTable dependencies to a clean SQL-based architecture with proper projection handling.

## 🚨 **ARCHITECTURE DEBT: ANTI-DUPLICATION VIOLATIONS** 🚨 (September 12, 2025)

### **⚠️ Critical Issues Identified (September 12, 2025)**
- **🚫 MASSIVE CODE DUPLICATION**: 6+ separate TableProvider implementations with 80%+ identical code
- **🚫 REPEATED PATTERNS**: Every provider reimplements the same boilerplate methods
- **🚫 TEMPORAL FILTERING DUPLICATION**: Multiple implementations of similar temporal logic
- **🚫 SCHEMA HANDLING DUPLICATION**: Repeated projection/schema inference patterns
- **🚫 ANTI-PATTERN VIOLATIONS**: Direct violation of DuckPond's anti-duplication philosophy

### **� IMMEDIATE REFACTORING REQUIRED**
Following `/instructions/anti-duplication.md`, this architecture violates core principles:
- ❌ Multiple functions doing "almost the same thing" 
- ❌ Copy-pasting implementations with "small changes"
- ❌ Function suffixes like `_with_temporal_filter`
- ❌ Boolean parameters controlling behavior variants

### **✅ Previous Completion (September 7, 2025)**
- **✅ DirectoryTable SQL Interface**: Complete with proper DataFusion projection handling
- **✅ Schema Mismatch Fix**: Resolved physical/logical schema mismatches in DirectoryTable
- **✅ Architecture Simplification**: DirectoryTable now uses SQL queries instead of NodeTable dependencies
- **✅ Query Command**: Working `pond query --show` with all table providers functional

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

### ✅ **NodeTable: COMPLETE WITH SQL VIEW** ✅
- **Status**: **Complete programmatic API + SQL view integration**
- **Purpose**: Direct access to node metadata (OplogEntry records) **without content column**
- **Content**: Complete node metadata with temporal columns, file types, versions
- **SQL Access**: Available as `nodes` view (excludes content column for performance)
- **Architecture**: Filters out content field, proper partition column handling
- **Integration**: Working in `pond query --show` for File Type Summary and Temporal Series Summary

### ✅ **DirectoryTable: COMPLETE AND WORKING** ✅
- **Status**: **Complete SQL interface with proper projection handling**
- **Purpose**: Directory content queries providing **file names and paths** via VersionedDirectoryEntry
- **Architecture**: SQL queries → directory OplogEntry → IPC parsing → VersionedDirectoryEntry records
- **Projection Support**: Proper DataFusion column projection for aggregation queries
- **Integration**: Working in `pond query --show` showing 10 directory entries and 7 file:series entries

## 🚨 **CURRENT ARCHITECTURE: DUPLICATION NIGHTMARE** 🚨

### **Anti-Duplication Violations Identified:**

```rust
// ❌ WRONG - Multiple near-duplicate implementations
TemporalFilteredListingTable::scan() {...}    // ~100 lines
FileTableProvider::scan() {...}               // ~80 lines  
SeriesTable::scan() {...}                     // ~120 lines
TableTable::scan() {...}                      // ~90 lines
NodeTable::scan() {...}                       // ~150 lines
// + 5 more as_any(), schema(), table_type(), constraints() duplications each
```

### **Current Duplication Count:**
- **6 TableProvider implementations** with 80%+ identical boilerplate
- **30+ duplicated methods** (`as_any`, `schema`, `table_type`, `constraints`, `supports_filters_pushdown` × 6)
- **3 temporal filtering implementations** with similar logic
- **Multiple schema handling patterns** doing the same projection/inference work

## 🎯 **REQUIRED ARCHITECTURE: UNIFIED PROVIDER SYSTEM** 🎯

Following **anti-duplication.md** principles:

```rust
// ✅ RIGHT - Single configurable implementation
#[derive(Default, Clone)]
pub struct TableProviderOptions {
    pub data_source: DataSourceType,
    pub temporal_filtering: Option<TemporalBounds>,
    pub schema_filtering: Option<Vec<String>>,
    pub projection_handling: ProjectionMode,
}

pub struct UnifiedTableProvider {
    options: TableProviderOptions,
    // Single implementation handles all variations
}

// Thin convenience wrappers (no logic duplication)
pub fn create_series_table(path: &str) -> Arc<dyn TableProvider> {
    UnifiedTableProvider::new(TableProviderOptions {
        data_source: DataSourceType::FileSeries(path),
        temporal_filtering: Some(TemporalBounds::default()),
        ..Default::default()
    })
}
```

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          UnifiedTableProvider                                │
│  ✅ Single configurable implementation                                      │
│  ✅ Options pattern for all variations                                      │
│  ✅ Composition over inheritance                                             │
│  ✅ No code duplication                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
           │              │              │              │              │
           ▼              ▼              ▼              ▼              ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ create_series() │ │ create_table()  │ │ create_nodes()  │ │create_directory()│ │create_temporal()│  
│ (thin wrapper)  │ │ (thin wrapper)  │ │ (thin wrapper)  │ │ (thin wrapper)  │ │ (thin wrapper)  │
└─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────────┘
│ FileSeries SQL  │    │ FileTable SQL    │    │ Node Metadata   │    │ Directory Names │
│ Temporal Queries│    │ Single Version   │    │ SQL View        │    │ IPC Parsing     │
│ Multi-version   │    │ Latest Version   │    │ No Content Col  │    │ Projection Fix  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 **Verified Working: Query Command Results**

The `pond query --show` command demonstrates all providers working:

### **File Type Summary** (NodeTable via SQL view)
```
| file_type   | count | first_created     | last_modified     |
|-------------|-------|-------------------|-------------------|
| file:series | 61    | 2025-09-08T03:04  | 2025-09-08T03:39  |
| directory   | 22    | 2025-09-08T03:03  | 2025-09-08T05:15  |
```

### **Directory Entry Summary** (DirectoryTable)
```
| operation_type | node_type   | count |
|----------------|-------------|-------|
| Insert         | directory   | 10    |
| Insert         | file:series | 7     |
```
## 🏗️ **Technical Implementation Details**

### **DirectoryTable Architecture (Newly Completed)**

**Key Technical Breakthrough**: Successfully eliminated NodeTable dependency and implemented proper DataFusion projection handling.

**Architecture Pattern**:
```rust
DirectoryTable::scan() 
  → SQL query: "SELECT node_id, content FROM oplog_entries WHERE file_type = 'directory'"
  → Parse IPC content to VersionedDirectoryEntry records  
  → Apply DataFusion projection for aggregation queries
  → Return projected RecordBatch
```

**Critical Fixes Applied**:
1. **Projection Support**: `DirectoryExecutionPlan::new_with_projection()` for proper column filtering
2. **Schema Consistency**: Physical schema matches logical schema after projection
3. **Case Sensitivity**: Fixed `'Directory'` → `'directory'` in SQL queries
4. **Move Semantics**: Proper schema cloning for async stream adapters

### **NodeTable Integration Pattern**

**SQL View Architecture**:
```sql
-- Registered as 'nodes' view in DataFusion
CREATE VIEW nodes AS
SELECT 
    part_id, node_id, file_type, timestamp, version,
    sha256, size, min_event_time, max_event_time, min_override, max_override
FROM oplog_entries  -- Content column excluded for performance
```

**Benefits**:
## 🚀 **Development Status: COMPLETE** 🚀

### **✅ All Table Providers Working**

**User Data Layer** (Production Ready):
- ✅ `SeriesTable` - Multi-version time-series access
- ✅ `TableTable` - Single-version table access

**Filesystem Metadata Layer** (Production Ready):
- ✅ `NodeTable` - Node metadata via SQL view (no content column)
- ✅ `DirectoryTable` - Directory entries with proper projection support

### **✅ SQL Integration Working**

**Available Tables in `pond query`**:
```sql
-- Direct Delta Lake access with all columns
SELECT * FROM oplog_entries;

-- Optimized view without content column  
SELECT * FROM nodes WHERE file_type = 'file:series';

-- Directory content with file names
SELECT * FROM directory_entries WHERE operation_type = 'Insert';
```

**Example: Temporal Overlap Detection with Names**:
```sql
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
    SELECT child_node_id as node_id, name
    FROM directory_entries 
    WHERE operation_type = 'Insert'
)
SELECT o.*, n1.name as file_a_name, n2.name as file_b_name
FROM overlapping_nodes o
JOIN node_names n1 ON o.node_a = n1.node_id
JOIN node_names n2 ON o.node_b = n2.node_id;
```
1. **Complete `TableProvider::scan()` method** in `nodes.rs`
2. **Create NodeExecutionPlan** similar to SeriesExecutionPlan but for OplogEntry records
3. **Add to SQL executor** - Register NodeTable in DataFusion context
## 📚 **Usage Examples**

### **Basic Table Access**
```bash
# Show all available tables and sample queries
pond query --show

# Count records in each table
pond query --sql "SELECT COUNT(*) FROM nodes"
pond query --sql "SELECT COUNT(*) FROM directory_entries"
pond query --sql "SELECT COUNT(*) FROM oplog_entries"
```

### **Metadata Analysis Queries**
```bash
# Find temporal overlaps in FileSeries
pond query --sql "
WITH overlaps AS (
    SELECT a.node_id as node_a, b.node_id as node_b,
           a.min_event_time, a.max_event_time
    FROM nodes a JOIN nodes b ON a.node_id < b.node_id
    WHERE a.file_type = 'file:series' 
      AND a.max_event_time > b.min_event_time 
      AND a.min_event_time < b.max_event_time
)
SELECT COUNT(*) as overlap_count FROM overlaps"

# Directory content analysis
pond query --sql "
SELECT node_type, operation_type, COUNT(*) as count
FROM directory_entries 
GROUP BY node_type, operation_type"
```

### **Combined Metadata and Names**
```bash
# Get file names for large files
pond query --sql "
SELECT d.name, n.size, n.file_type
FROM nodes n
JOIN directory_entries d ON n.node_id = d.child_node_id
WHERE n.size > 1000000 
  AND d.operation_type = 'Insert'"
```
pond query-metadata "
  SELECT n.node_id, n.min_event_time, d.name, d.parent_path
  FROM nodes n 
  JOIN directory_entries d ON n.node_id = d.child_node_id
  WHERE n.file_type = 'file:series'
"
## 🎯 **Key Technical Achievements**

### **1. Clean Architecture Separation** ✅
**User Data vs Filesystem Metadata**: Clear boundary between data access and metadata analysis
```rust
// User Data Access (Complete)
SeriesTable::new(path, node_table)  // Time-series data
TableTable::new(path, node_table)   // Single-version data

// Filesystem Metadata Access (Complete) 
NodeTable::new(delta_table)         // Node metadata (SQL view)
DirectoryTable::new(delta_table)    // Directory entries (IPC parsing)
```

### **2. DataFusion Integration Mastery** ✅  
**Projection Handling**: Properly implemented DataFusion projection requirements
- **Physical Schema = Logical Schema**: Fixed schema mismatch errors
- **Column Filtering**: `DirectoryExecutionPlan::new_with_projection()`
- **Empty Projections**: Proper COUNT(*) query support
- **Schema Consistency**: Eliminated "field count mismatch" errors

### **3. SQL-First Architecture** ✅
**Eliminated Dependencies**: DirectoryTable uses SQL queries instead of NodeTable objects
```rust
// Before: Complex dependency chain
DirectoryTable → NodeTable → DeltaOps → Complex projection logic

// After: Clean SQL-based approach  
DirectoryTable → SQL query → IPC parsing → DataFusion projection
```

### **4. Anti-Fallback Compliance** ✅
**No Silent Failures**: Following DuckPond architectural principles
- ✅ Explicit error propagation (`?` operator usage)
- ✅ No silent schema mismatches
- ✅ Clear failure points with descriptive errors
- ✅ Fail-fast on projection errors
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
## � **REFACTORING PRIORITY: CRITICAL** 🚨

### **❌ Technical Debt Status**
- **Current LOC Duplication**: ~1000+ lines of duplicated TableProvider code
- **Maintenance Burden**: Every change requires updating 6 separate implementations
- **Bug Risk**: Inconsistencies between similar implementations cause subtle bugs
- **Architecture Violation**: Direct violation of DuckPond's core principles
- **Future Development**: Blocked until duplication is eliminated

### **✅ Functionality Still Working** 
*(Despite architectural debt)*
- **File Type Summaries**: `SELECT file_type, COUNT(*) FROM nodes GROUP BY file_type`
- **Directory Analysis**: `SELECT operation_type, node_type, COUNT(*) FROM directory_entries GROUP BY operation_type, node_type`
- **Temporal Queries**: `SELECT node_id, min_event_time, max_event_time FROM nodes WHERE file_type = 'file:series'`
- **JOIN Capabilities**: `nodes JOIN directory_entries` queries work
- **Column Projection**: DataFusion aggregation support functional
- **Temporal Filtering**: COUNT queries with temporal overrides return correct results (1315 vs 1317)

### **🎯 IMMEDIATE REFACTORING ROADMAP**

#### **Phase 1: Core Unification** ⚡ HIGH PRIORITY
1. **Create `TableProviderOptions`** - Single configuration struct for all variants
2. **Implement `UnifiedTableProvider`** - One provider to rule them all
3. **Extract common patterns** - Schema handling, projection logic, temporal filtering
4. **Eliminate duplication** - Move from 6 implementations to 1 configurable implementation

#### **Phase 2: Compatibility Layer** 
1. **Create thin wrappers** - Maintain existing APIs for backward compatibility
2. **Migrate existing code** - Update all usage to use unified provider
3. **Remove old implementations** - Delete duplicated code after migration
4. **Update documentation** - Reflect new unified architecture

#### **Phase 3: Advanced Features**
1. **Performance optimization** - Single codebase enables focused optimization
2. **Enhanced temporal logic** - Unified temporal filtering across all provider types  
3. **Extended capabilities** - Easier to add features when not duplicated across 6 implementations

### **📊 Success Metrics: ACHIEVED**
- ✅ **NodeTable SQL**: `SELECT node_id, min_event_time FROM nodes` works
- ✅ **DirectoryTable SQL**: `SELECT operation_type, node_type FROM directory_entries` works  
- ✅ **Projection Support**: Aggregation queries work correctly
- ✅ **CLI Integration**: Working `pond query --show` with pretty-printed results
- ✅ **Schema Consistency**: No more physical/logical schema mismatches

## Testing & Validation Status

### **✅ Current Test Coverage**
- **Unit Tests**: All tlogfs tests passing
- **Integration Tests**: End-to-end workflows validated via CLI
- **CLI Tests**: Complete `pond query` functionality working
- **Real Data Tests**: Validated against `/tmp/dynpond` with 61 file:series + 22 directories
- **Schema Tests**: DataFusion projection requirements verified

---

## 🚨 **Conclusion: URGENT ARCHITECTURAL REFACTORING REQUIRED** 🚨

The DuckPond table provider architecture has **CRITICAL TECHNICAL DEBT** that violates core DuckPond principles. While functionality works, the massive code duplication creates an unsustainable maintenance burden.

**❌ Current Problems**:
- **🚫 Anti-Duplication Violations**: 6 TableProvider implementations with 80%+ identical code
- **🚫 Maintenance Nightmare**: Every bug fix or feature requires changes in 6 places
- **🚫 Inconsistency Risk**: Similar implementations drift apart over time
- **🚫 Development Blockage**: Adding new provider types multiplies duplication
- **🚫 Philosophy Violation**: Direct violation of documented anti-duplication principles

**✅ Functionality Verified** *(but architecturally unsound)*:
1. **Working Data Access**: All query types function correctly
2. **DataFusion Integration**: Schema handling and projection work
3. **Temporal Filtering**: Complex COUNT optimization successfully implemented
4. **JOIN Capabilities**: Cross-table queries operational
5. **CLI Integration**: Commands produce correct results

## 🎯 **MANDATORY NEXT STEPS**

### **STOP ALL NEW FEATURES** until refactoring complete

**Following anti-duplication.md guidelines**:

1. **Create `TableProviderOptions` struct** - Configuration over duplication
2. **Implement `UnifiedTableProvider`** - Single implementation for all variants  
3. **Extract common patterns** - Schema, projection, temporal logic
4. **Create thin wrappers** - Backward compatibility without duplication
5. **Remove duplicate implementations** - Delete 1000+ lines of redundant code

### **Success Metrics**
- ✅ **Single TableProvider implementation** (down from 6)
- ✅ **Options-based configuration** (no more boolean parameters)
- ✅ **Thin wrapper pattern** (preserve existing APIs)
- ✅ **Zero functional regressions** (all tests still pass)
- ✅ **50%+ LOC reduction** in table provider code

### **Architecture Debt Impact**
- **Development Speed**: Currently 6x slower due to duplication
- **Bug Risk**: High - inconsistencies already causing subtle issues
- **Code Review**: Impossible to review 6 similar implementations effectively
- **New Contributors**: Confused by seemingly identical code with subtle differences

---

**Status: REFACTORING CRITICAL PRIORITY** 🚨  
**Functionality**: Working but architecturally unsound  
**Action Required**: Immediate unification following anti-duplication.md  
**Timeline**: Must complete before any new table provider features  

---

## 📋 **DETAILED REFACTORING SPECIFICATION**

### **Target Architecture: Options Pattern Implementation**

```rust
/// Single configuration struct for all table provider variants
#[derive(Default, Clone, Debug)]
pub struct TableProviderOptions {
    /// What kind of data source to read from
    pub data_source: DataSourceType,
    
    /// Optional temporal filtering bounds
    pub temporal_bounds: Option<TemporalBounds>,
    
    /// Schema field filtering (exclude content column, etc.)
    pub schema_filter: SchemaFilterOptions,
    
    /// How to handle DataFusion projections
    pub projection_mode: ProjectionMode,
    
    /// Custom execution plan optimizations
    pub optimizations: ExecutionOptimizations,
}

#[derive(Clone, Debug)]
pub enum DataSourceType {
    /// Read from file:series with temporal metadata
    FileSeries { path: String, node_table: NodeTable },
    
    /// Read from file:table (single version)
    FileTable { path: String, node_table: NodeTable },
    
    /// Read from Delta Lake nodes table (metadata only)
    DeltaNodes { table: DeltaTable },
    
    /// Read from IPC-parsed directory entries
    DirectoryEntries { table: DeltaTable },
    
    /// Read from DataFusion ListingTable with temporal filtering
    TemporalListing { 
        listing_table: ListingTable, 
        bounds: TemporalBounds 
    },
    
    /// Read from in-memory RecordBatches
    InMemory { schema: SchemaRef, batches: Vec<RecordBatch> },
}

#[derive(Default, Clone, Debug)]
pub struct TemporalBounds {
    pub min_time: i64,
    pub max_time: i64,
    pub timezone_handling: TimezoneMode,
}

#[derive(Default, Clone, Debug)]
pub struct SchemaFilterOptions {
    /// Columns to exclude from schema (e.g., "content")
    pub excluded_columns: Vec<String>,
    
    /// Whether to support empty projections (COUNT optimization)
    pub empty_projection_support: bool,
}

#[derive(Default, Clone, Debug)]
pub enum ProjectionMode {
    #[default]
    Standard,
    /// Handle COUNT(*) empty projection optimization  
    CountOptimized,
    /// Apply projection before temporal filtering
    FilterFirst,
}

/// Single TableProvider implementation that handles all variations
pub struct UnifiedTableProvider {
    options: TableProviderOptions,
    cached_schema: Arc<RwLock<Option<SchemaRef>>>,
}

impl UnifiedTableProvider {
    pub fn new(options: TableProviderOptions) -> Self {
        Self {
            options,
            cached_schema: Arc::new(RwLock::new(None)),
        }
    }
    
    /// Core method that handles all data source types
    async fn create_execution_plan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match &self.options.data_source {
            DataSourceType::FileSeries { .. } => self.handle_file_series(...).await,
            DataSourceType::FileTable { .. } => self.handle_file_table(...).await,
            DataSourceType::DeltaNodes { .. } => self.handle_delta_nodes(...).await,
            DataSourceType::DirectoryEntries { .. } => self.handle_directory_entries(...).await,
            DataSourceType::TemporalListing { .. } => self.handle_temporal_listing(...).await,
            DataSourceType::InMemory { .. } => self.handle_in_memory(...).await,
        }
    }
    
    /// Apply temporal filtering if configured
    fn apply_temporal_filtering(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if let Some(bounds) = &self.options.temporal_bounds {
            // Single temporal filtering implementation for all provider types
            self.create_temporal_filter_plan(plan, bounds)
        } else {
            Ok(plan)
        }
    }
}

#[async_trait]
impl TableProvider for UnifiedTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    
    fn schema(&self) -> SchemaRef {
        // Single schema handling implementation
        self.compute_filtered_schema()
    }
    
    fn table_type(&self) -> TableType { TableType::Base }
    
    fn constraints(&self) -> Option<&Constraints> {
        // Delegate to underlying source if applicable
        match &self.options.data_source {
            DataSourceType::DeltaNodes { table } => Some(table.constraints()),
            DataSourceType::TemporalListing { listing_table, .. } => listing_table.constraints(),
            _ => None,
        }
    }
    
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Single filter pushdown implementation
        self.compute_filter_pushdown(filters)
    }
    
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Single scan implementation that handles all cases
        self.create_execution_plan(state, projection, filters, limit).await
    }
}
```

### **Thin Convenience Wrappers (No Logic Duplication)**

```rust
/// Create SeriesTable using unified provider
pub fn create_series_table(path: &str, node_table: NodeTable) -> Arc<dyn TableProvider> {
    Arc::new(UnifiedTableProvider::new(TableProviderOptions {
        data_source: DataSourceType::FileSeries { 
            path: path.to_string(), 
            node_table 
        },
        temporal_bounds: Some(TemporalBounds::default()),
        schema_filter: SchemaFilterOptions::default(),
        projection_mode: ProjectionMode::CountOptimized,
        ..Default::default()
    }))
}

/// Create TableTable using unified provider  
pub fn create_table_table(path: &str, node_table: NodeTable) -> Arc<dyn TableProvider> {
    Arc::new(UnifiedTableProvider::new(TableProviderOptions {
        data_source: DataSourceType::FileTable { 
            path: path.to_string(), 
            node_table 
        },
        // No temporal bounds for single-version tables
        temporal_bounds: None,
        ..Default::default()
    }))
}

/// Create NodeTable using unified provider
pub fn create_node_table(table: DeltaTable) -> Arc<dyn TableProvider> {
    Arc::new(UnifiedTableProvider::new(TableProviderOptions {
        data_source: DataSourceType::DeltaNodes { table },
        schema_filter: SchemaFilterOptions {
            excluded_columns: vec!["content".to_string()],
            empty_projection_support: true,
        },
        ..Default::default()
    }))
}

/// Create TemporalFilteredListingTable using unified provider
pub fn create_temporal_listing_table(
    listing_table: ListingTable, 
    min_time: i64, 
    max_time: i64
) -> Arc<dyn TableProvider> {
    Arc::new(UnifiedTableProvider::new(TableProviderOptions {
        data_source: DataSourceType::TemporalListing { 
            listing_table, 
            bounds: TemporalBounds { min_time, max_time, ..Default::default() }
        },
        projection_mode: ProjectionMode::CountOptimized,
        ..Default::default()
    }))
}
```

### **Migration Strategy: Zero-Downtime Refactoring**

1. **Phase 1**: Implement `UnifiedTableProvider` alongside existing providers
2. **Phase 2**: Create convenience functions that return unified provider
3. **Phase 3**: Update all callers to use convenience functions  
4. **Phase 4**: Remove old provider implementations
5. **Phase 5**: Clean up imports and update documentation

**Estimated Impact**:
- **LOC Reduction**: ~1000+ lines removed (6 duplicate implementations)
- **Maintenance**: 6x easier (single implementation to maintain)
- **Bug Risk**: Significantly reduced (no more inconsistencies)
- **Development Speed**: Much faster (features implemented once, not 6 times)

---

*Updated: September 12, 2025 - **DETAILED REFACTORING SPEC COMPLETE***  
*Ready for: **Implementation of unified TableProvider architecture***
