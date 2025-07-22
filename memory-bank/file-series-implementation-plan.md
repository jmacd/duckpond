# File:Series Implementation Plan - Timeseries Data with DataFusion Integration

## Overview

Implementation plan for `file:series` support in DuckPond, leveragi### **Phase 2: DataFusion Query Integration**  
1. **Create SeriesTable** DataFusion TableProvider with time-range filtering
2. **Implement SeriesExt trait** for streaming time-range queries
3. **Add temporal stream merging logic** for combining multiple versions
4. **Test end-to-end query performance** with dual-level filtering
5. **Benchmark performance** against large datasets

### **Phase 3: Production Features**
1. **Flexible timestamp column detection** with auto-discovery
2. **Schema consistency validation** per series (enforce same schema + timestamp column)
3. **Comprehensive error handling** and validation
4. **CLI integration** for series operations (`duckpond series create`, `duckpond series query`)
5. **Documentation and examples** for series usage patterns

## Key Implementation Details production-ready system with DataFusion integration for efficient timeseries data management. This builds on the established TLogFS + DataFusion architecture to provide versioned timeseries storage with temporal range queries.

## Core Concept

### Event Time vs Write Time Distinction
- **Write Time**: `OplogEntry.timestamp` - when the version was recorded in TLogFS
- **Event Time**: Data column values - actual observation/event timestamps within the data
- **Key Insight**: Can write historical data after the fact, so event time ≠ write time

### Dual-Level Metadata Architecture
1. **OplogEntry Level**: Fast file-level filtering for version selection
2. **Parquet Statistics Level**: Standard fine-grained pruning within files

## Technical Architecture

### Delta Lake Metadata Analysis - Key Learnings

Based on analysis of the Delta Lake codebase (`./delta-rs`), we've validated that our proposed approach aligns perfectly with production-grade metadata systems. Here's how Delta Lake handles metadata for query optimization:

#### **Statistics Flow in Delta Lake**

1. **Write Path**: Statistics are extracted from Parquet file metadata and stored in Delta transaction log
   ```rust
   // Delta Lake Add action structure
   pub struct Add {
       pub path: String,
       pub size: i64,
       pub stats: Option<String>,  // JSON-serialized Parquet statistics
       // ... other fields
   }
   
   // Statistics structure
   pub struct Stats {
       pub num_records: i64,
       pub min_values: HashMap<String, ColumnValueStat>,  // Per-column minimums
       pub max_values: HashMap<String, ColumnValueStat>,  // Per-column maximums
       pub null_count: HashMap<String, ColumnCountStat>,  // Null counts
   }
   ```

2. **Query Path**: Delta Lake implements DataFusion's `PruningStatistics` trait to expose file-level metadata
   ```rust
   impl PruningStatistics for AddContainer<'_> {
       fn min_values(&self, column: &Column) -> Option<ArrayRef> {
           // Returns array of min values across all files for the column
       }
       fn max_values(&self, column: &Column) -> Option<ArrayRef> {
           // Returns array of max values across all files for the column
       }
   }
   ```

3. **Predicate Pushdown**: DataFusion's `PruningPredicate` automatically eliminates files based on statistics
   ```rust
   // Delta Lake's file filtering logic
   let pruning_predicate = PruningPredicate::try_new(expr, schema)?;
   let files_to_keep = pruning_predicate.prune(snapshot)?;
   ```

#### **Key Insights for DuckPond**

1. **Our dedicated columns approach is correct**: Delta Lake stores min/max values per column, validating our `min_event_time`/`max_event_time` design

2. **DataFusion integration is automatic**: Once we implement `PruningStatistics`, DataFusion handles all the query optimization logic

3. **Parquet statistics are leveraged**: Delta Lake relies on standard Parquet row group statistics for fine-grained pruning, confirming our dual-level approach

4. **JSON serialization is standard**: Delta Lake serializes statistics to JSON in the transaction log, but we can use typed columns for better performance

5. **File elimination is the key optimization**: Delta Lake's primary performance gain comes from eliminating entire files based on metadata, exactly what our OplogEntry filtering provides

#### **Performance Architecture Validation**

Delta Lake's approach confirms our performance hierarchy:

1. **Transaction Log Filtering** (fastest): Use metadata to eliminate entire files
   - Delta Lake: `Add.stats` JSON → file elimination
   - DuckPond: `OplogEntry.min_event_time/max_event_time` → file elimination

2. **Parquet Statistics** (automatic): DataFusion uses Parquet metadata for row group pruning
   - Both systems: Standard Apache Parquet statistics → automatic pruning

3. **Fine-grained Filtering** (within row groups): Page-level statistics for detailed pruning
   - Both systems: Standard Parquet page statistics → detailed elimination

This validates that our approach follows production-proven patterns used by major lakehouse systems.

### OplogEntry Schema Enhancement (Simplified Approach)

We'll add dedicated columns for the essential temporal metadata, keeping the implementation focused and simple:

```rust
pub struct OplogEntry {
    // Existing fields (unchanged)
    pub timestamp: i64,           // Write time - when version was recorded
    pub version: i64,
    pub entry_type: EntryType,
    
    // NEW: Event time metadata for series (dedicated columns only)
    pub min_event_time: Option<i64>,    // Min timestamp from data for fast SQL queries
    pub max_event_time: Option<i64>,    // Max timestamp from data for fast SQL queries
    pub event_time_column: Option<String>, // Hard-coded timestamp column name
    
    // ... other existing fields
}
```

**Design Rationale**:
- **Simple and focused** - only the essential fields for time series support
- **Fast range queries** via dedicated columns: `WHERE max_event_time >= ? AND min_event_time <= ?`
- **Type safety** - all fields are strongly typed, no JSON parsing
- **SQL-friendly** - direct column access for all temporal operations
- **Minimal complexity** - no generic metadata system to maintain
```

### Parquet Statistics Integration

Leverage **standard Apache Parquet statistics** for fine-grained pruning:
- **Row Group Level**: min/max timestamps per row group
- **Page Level**: Optional page-level statistics for finer granularity
- **Automatic DataFusion Integration**: No custom code needed - DataFusion automatically uses Parquet statistics for predicate pushdown

### Query Optimization Flow (Simplified)

```sql
-- Query: SELECT * FROM series WHERE timestamp BETWEEN '2023-01-01' AND '2023-01-31'

-- Step 1: OplogEntry filtering (fast file-level using dedicated columns)
SELECT file_path, event_time_column FROM operations_table 
WHERE entry_type = 'FileSeries'
AND max_event_time >= 1672531200000  -- Fast index scan on dedicated column
AND min_event_time <= 1675209599999  -- Fast index scan on dedicated column;

-- Step 2: Parquet-level pruning (automatic via DataFusion)
-- Uses standard Parquet statistics to eliminate row groups/pages
-- within the selected files
```

**Performance Benefits**:
- **Fast temporal filtering** via dedicated min/max columns
- **Simple column access** for timestamp column names
- **Standard Parquet statistics** provide automatic fine-grained pruning
- **No JSON parsing overhead** - all fields are directly accessible

## Prerequisites Analysis

### ✅ **Already Complete**
1. **EntryType::FileSeries** - Defined and integrated throughout the system
2. **Basic TLogFS handling** - FileSeries is handled in `create_node_from_oplog_entry`
3. **Arrow/Parquet integration** - Full `ParquetExt` trait with both high-level and low-level APIs
4. **DataFusion integration** - `OperationsTable` provides SQL access to OplogEntry records
5. **Schema management** - `ForArrow` trait and `OplogEntry` schema are production-ready

### ❌ **Missing Prerequisites (Must Complete First)**

#### 1. **OplogEntry Schema Extensions** 🚨 **CRITICAL BLOCKER**

**Current Problem**: The `OplogEntry` struct lacks event time metadata fields needed for efficient temporal queries.

**Schema Design Decision**: We'll use **dedicated columns only** for simplicity and focus:

```rust
pub struct OplogEntry {
    // ... existing fields (unchanged)
    pub timestamp: i64,           // Write time - when version was recorded
    pub version: i64,
    
    // NEW: Event time metadata for efficient DataFusion queries
    pub min_event_time: Option<i64>,    // Min timestamp from data for fast range queries
    pub max_event_time: Option<i64>,    // Max timestamp from data for fast range queries
    
    // NEW: Extended attributes for application-specific metadata
    pub extended_attributes: ExtendedAttributes,
}
```

**Rationale**:
- **Simple and focused** - only essential fields for time series functionality
- **Fast SQL queries** - dedicated columns enable efficient range filtering
- **Type safety** - all fields are strongly typed with compile-time guarantees
- **Minimal complexity** - no generic metadata system to design or maintain

#### 2. **ForArrow Schema Update** 🚨 **CRITICAL**

```rust
impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            // ... existing fields
            // NEW FIELDS:
            Arc::new(Field::new("min_event_time", DataType::Int64, true)),
            Arc::new(Field::new("max_event_time", DataType::Int64, true)),
            // Note: extended_attributes stored as JSON string or separate table
        ]
    }
}
```

#### 3. **Temporal Range Extraction Functions** ❌ **MISSING**

Functions to extract min/max timestamps from RecordBatch data don't exist yet.

#### 4. **Series-Specific Write Operations** ❌ **MISSING**

Need specialized methods for writing series data with temporal metadata extraction.

#### 5. **SeriesTable DataFusion Provider** ❌ **MISSING**

While `OperationsTable` exists, need specialized provider for time-range filtering.

#### 6. **OplogEntry Constructor Updates** ❌ **MISSING**

All existing constructors need to handle new optional fields.

## Implementation Phases

### **Phase 0: Schema Foundation** 🚨 **MUST DO FIRST**
1. **Extend OplogEntry struct** with dedicated temporal columns (min/max times + column name)
2. **Update ForArrow implementation** to include new fields  
3. **Update OplogEntry constructors** to handle new optional fields
4. **Update all existing write operations** to populate new fields as None initially
5. **Run all tests** to ensure schema changes don't break existing functionality
6. **Consider data migration strategy** for existing TLogFS data

### **Phase 1: Core Series Support**
1. **Add temporal extraction functions** (`extract_temporal_range_from_batch`)
2. **Extend ParquetExt** with series-specific methods (`create_series_from_batch`)
3. **Update TLogFS write operations** to populate event time metadata for FileSeries
4. **Create series metadata utilities** for handling timestamp column detection and validation
5. **Add comprehensive error handling** for invalid timestamp columns

### **Phase 2: DataFusion Query Integration**  
1. **Create SeriesTable** DataFusion TableProvider with time-range filtering
2. **Implement efficient SQL queries** leveraging dedicated min/max columns
3. **Add version consolidation logic** for handling overlapping time ranges
4. **Test end-to-end query performance** with dual-level filtering
5. **Benchmark performance** against large datasets

### **Phase 3: Production Features**
1. **Flexible timestamp column detection** with auto-discovery
2. **Schema consistency validation** per series (enforce same schema + timestamp column)
3. **Comprehensive error handling** and validation
4. **CLI integration** for series operations (`duckpond series create`, `duckpond series query`)
5. **Documentation and examples** for series usage patterns

## Application-Specific Metadata via Extended Attributes

### **Minimal Extended Attributes Design**

For the initial scope, we'll implement a simple extended attributes system with only the essential metadata:

```rust
use std::collections::HashMap;

/// Extended attributes - immutable metadata set at file creation
#[derive(Debug, Clone, PartialEq)]
pub struct ExtendedAttributes {
    /// Simple key → String value mapping
    /// For current scope: only "duckpond.timestamp_column"
    pub attributes: HashMap<String, String>,
}

/// DuckPond system metadata key constants
pub mod duckpond {
    pub const TIMESTAMP_COLUMN: &str = "duckpond.timestamp_column";
}

impl ExtendedAttributes {
    pub fn new() -> Self {
        Self { attributes: HashMap::new() }
    }
    
    /// Create from JSON string (for reading from OplogEntry)
    pub fn from_json(json: &str) -> Result<Self> {
        let attributes: HashMap<String, String> = serde_json::from_str(json)?;
        Ok(Self { attributes })
    }
    
    /// Serialize to JSON string (for storing in OplogEntry)
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.attributes)?)
    }
    
    /// Set timestamp column name (defaults to "Timestamp" if not set)
    pub fn set_timestamp_column(&mut self, column_name: &str) -> &mut Self {
        self.attributes.insert(duckpond::TIMESTAMP_COLUMN.to_string(), column_name.to_string());
        self
    }
    
    /// Get timestamp column name with default fallback
    pub fn timestamp_column(&self) -> &str {
        self.attributes.get(duckpond::TIMESTAMP_COLUMN)
            .map(|s| s.as_str())
            .unwrap_or("Timestamp")  // Default column name
    }
    
    /// Set/get raw attributes (for future extensibility)
    pub fn set_raw(&mut self, key: &str, value: &str) -> &mut Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }
    
    pub fn get_raw(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }
}
### **Enhanced NodeMetadata**

```rust
/// Enhanced NodeMetadata with extended attributes
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMetadata {
    /// Node version (incremented on each modification)
    pub version: u64,
    
    /// File size in bytes (None for directories and symlinks)
    pub size: Option<u64>,
    
    /// SHA256 checksum (Some for all files, None for directories/symlinks)
    pub sha256: Option<String>,
    
    /// Entry type (file, directory, symlink with file format variants)
    pub entry_type: EntryType,

    /// Entry timestamp
    pub timestamp: i64,
    
    /// Extended attributes - immutable metadata set at creation
    pub extended_attributes: ExtendedAttributes,
}
```

### **Series-Specific Extensions for OplogEntry**

For efficient querying via DataFusion, we'll add dedicated columns to `OplogEntry` derived from extended attributes:

```rust
pub struct OplogEntry {
    // Existing fields (unchanged)
    pub timestamp: i64,           // Write time - when version was recorded
    pub version: i64,
    pub entry_type: EntryType,
    
    // NEW: Event time metadata for efficient DataFusion queries
    pub min_event_time: Option<i64>,    // From Parquet statistics
    pub max_event_time: Option<i64>,    // From Parquet statistics
    
    // NEW: Extended attributes for application-specific metadata
    pub extended_attributes: ExtendedAttributes,
    
    // ... other existing fields
}
```

### **DataFusion Query Architecture**

This design enables the critical performance characteristic you identified:

```
User Query (DataFusion)
    ↓
Delta Lake Table Provider
    ↓ (predicate pushdown)
TLogFS Operations Table (DataFusion)
    ↓ (time range filtering)
SELECT versions WHERE min_event_time <= ? AND max_event_time >= ?
    ↓
Only relevant Parquet files loaded
```

### **Usage Examples**

```rust
use crate::metadata::{ExtendedAttributes, duckpond};

// Approach 1: Fluent interface with iterator
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_xattrs([
        (duckpond::TIMESTAMP_COLUMN.to_string(), "event_time".to_string()),
        ("custom.units".to_string(), "celsius".to_string()),
    ]);

// Approach 2: Fluent interface with ExtendedAttributes
let mut attrs = ExtendedAttributes::new();
attrs.set_timestamp_column("event_time");
attrs.set_raw("custom.sensor_id", "temp_001");

let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_extended_attributes(attrs);

// Approach 3: Simple timestamp column (most common case)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_temporal_extraction(Some("event_time"));

// Default case - uses "Timestamp" column
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_temporal_extraction(None);  // Uses "Timestamp" by default

// Write the data (this triggers temporal metadata extraction for FileSeries)
let mut parquet_writer = ArrowWriter::try_new(writer, batch.schema(), None)?;
parquet_writer.write(&batch)?;
parquet_writer.close()?;

// IMPORTANT: Extended attributes are IMMUTABLE after creation
// Subsequent writes to the same path will:
// 1. Use the existing extended attributes from the first version
// 2. Only update temporal metadata (min/max event times)
// 3. Validate schema consistency with the original version

// Appending to existing series (extended attributes inherited from v1)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?;  // No .with_* methods needed - uses existing metadata

// The writer automatically inherits:
// - timestamp_column from the first version
// - all extended attributes from the first version
// - schema validation against the first version
```

### **Min/Max Extraction from Parquet Write**

Extract min/max during the write operation for efficient DataFusion queries:

```rust
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::FileMetaData;

pub async fn write_series_with_metadata(
    record_batch: &RecordBatch,
    file_path: &Path,
    extended_attributes: ExtendedAttributes,
) -> Result<OplogEntry> {
    // 1. Get timestamp column from extended attributes (defaults to "Timestamp")
    let timestamp_column = extended_attributes.timestamp_column();
    
    // 2. Write Parquet file and capture metadata
    let file = File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None)?;
    writer.write(record_batch)?;
    let file_metadata: FileMetaData = writer.close()?;
    
    // 3. Extract min/max from Parquet file metadata (no re-reading!)
    let (min_time, max_time) = extract_time_range_from_parquet_metadata(
        &file_metadata, 
        timestamp_column
    )?;
    
    // 4. Create OplogEntry with temporal metadata for DataFusion queries
    Ok(OplogEntry {
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
        version: new_version,
        entry_type: EntryType::FileSeries,
        min_event_time: Some(min_time),
        max_event_time: Some(max_time),
        extended_attributes,
        // ... other fields
    })
}
```

fn extract_time_range_from_parquet_metadata(
    file_metadata: &FileMetaData,
    timestamp_column: &str,
) -> Result<(i64, i64)> {
    let schema = file_metadata.schema();
    
    // Find the timestamp column index
    let timestamp_index = schema
        .get_column_iter()
        .position(|col| col.name() == timestamp_column)
        .ok_or_else(|| format!("Timestamp column '{}' not found", timestamp_column))?;
    
    let mut overall_min = i64::MAX;
    let mut overall_max = i64::MIN;
    
    // Iterate through all row groups
    for row_group in file_metadata.row_groups() {
        if let Some(column_metadata) = row_group.column(timestamp_index) {
            if let Some(stats) = column_metadata.statistics() {
                // Extract min/max from row group statistics
                if let (Some(min_bytes), Some(max_bytes)) = (stats.min_bytes(), stats.max_bytes()) {
                    // Convert bytes to i64 timestamp (this depends on the timestamp type)
                    let min_val = bytes_to_timestamp(min_bytes)?;
                    let max_val = bytes_to_timestamp(max_bytes)?;
                    
                    overall_min = overall_min.min(min_val);
                    overall_max = overall_max.max(max_val);
                }
            }
        }
    }
    
    if overall_min == i64::MAX {
        return Err("No timestamp statistics found in Parquet file".into());
    }
    
    Ok((overall_min, overall_max))
}

fn bytes_to_timestamp(bytes: &[u8]) -> Result<i64> {
    // Convert Parquet statistic bytes to i64 timestamp
    // This implementation depends on the specific timestamp type
    // For TimestampMillisType, it's a direct i64 conversion
    if bytes.len() == 8 {
        Ok(i64::from_le_bytes(bytes.try_into()?))
    } else {
        Err("Invalid timestamp bytes length".into())
    }
}
```

## Simplified Series Interface with Extended Attributes

### **Enhanced Streaming Interface with Extended Attributes**

Building on the existing `create_file_path_streaming_with_type` method, we'll extend the returned `AsyncWrite` trait to support metadata:

```rust
/// Extended AsyncWrite that can capture metadata during the write process
/// NOTE: Extended attributes can ONLY be set on the FIRST version of a file
pub trait AsyncWriteWithMetadata: AsyncWrite + Send {
    /// Set extended attributes from key-value pairs (FIRST VERSION ONLY)
    /// Returns error if file already exists with different metadata
    fn with_xattrs<I>(self, attrs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)> + Send,
        Self: Sized;
    
    /// Set extended attributes from ExtendedAttributes struct (FIRST VERSION ONLY)
    /// Returns error if file already exists with different metadata
    fn with_extended_attributes(self, attrs: ExtendedAttributes) -> Self
    where
        Self: Sized;
    
    /// Enable temporal metadata extraction for FileSeries (FIRST VERSION ONLY)
    /// For subsequent versions, timestamp column is inherited from v1
    fn with_temporal_extraction(self, timestamp_column: Option<&str>) -> Self
    where
        Self: Sized;
}

/// Update the existing method to return the enhanced writer
impl WD {
    pub async fn create_file_path_streaming_with_type<P: AsRef<Path>>(
        &self, 
        path: P, 
        entry_type: EntryType
    ) -> Result<(NodePath, impl AsyncWriteWithMetadata)> {
        // Implementation:
        // 1. Check if file already exists
        // 2. If exists: return writer that inherits existing extended attributes
        // 3. If new: return writer that allows setting extended attributes
        // 4. Any .with_* calls on existing files are ignored/validated
    }
}
```

### **Usage Patterns (Immutable Extended Attributes)**

```rust
// === FIRST VERSION: Extended attributes can be set ===

// Pattern 1: Simple temporal extraction (most common for new files)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_temporal_extraction(Some("event_time"));  // Custom timestamp column

// Pattern 2: Multiple extended attributes (new files only)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_xattrs([
        ("duckpond.timestamp_column".to_string(), "event_time".to_string()),
        ("sensor.units".to_string(), "celsius".to_string()),
        ("sensor.location".to_string(), "building_a".to_string()),
    ]);

// === SUBSEQUENT VERSIONS: Extended attributes are inherited ===

// Pattern 3: Appending to existing file (extended attributes inherited)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?;
// No .with_* calls needed or allowed - metadata comes from v1

// Pattern 4: If you try to set different metadata on existing file
let result = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_temporal_extraction(Some("different_column"));  // This is ignored or warns

// The writer will:
// 1. Use "event_time" from the original version (not "different_column")
// 2. Inherit all extended attributes from v1
// 3. Only update min/max temporal metadata based on new data
```

// Write the actual data
let mut parquet_writer = ArrowWriter::try_new(writer, batch.schema(), None)?;
parquet_writer.write(&batch)?;
parquet_writer.close()?;  // This triggers metadata extraction and OplogEntry creation
```

### **Reading Interface (Builds on Existing Patterns)**

For reading, we'll provide high-level convenience methods that leverage the enhanced metadata:

```rust
#[async_trait::async_trait]
pub trait SeriesExt {
    /// Stream RecordBatches from all series versions that overlap with the time range
    /// This leverages the DataFusion → Delta Lake → TLogFS predicate pushdown
    async fn read_series_time_range<P>(
        &self,
        path: P,
        start_time: i64,
        end_time: i64,
    ) -> Result<SeriesStream>
    where
        P: AsRef<Path> + Send + Sync;
        
    /// Stream all versions of a series (no time filtering)
    async fn read_series_all<P>(
        &self,
        path: P,
    ) -> Result<SeriesStream>
    where
        P: AsRef<Path> + Send + Sync;
}
```

### **DataFusion Query Performance Architecture**

The key insight you identified - DataFusion query over Delta Lake Table Provider over TLogFS operations:

```sql
-- High-level user query
SELECT * FROM series 
WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
AND location = 'drill_site_alpha';

-- Pushdown to Delta Lake provider 
SELECT file_path, min_event_time, max_event_time, timestamp_column
FROM tlogfs_operations_table
WHERE entry_type = 'FileSeries'
AND node_id = 'series_id'
AND max_event_time >= 1704067200000  -- 2024-01-01 in milliseconds
AND min_event_time <= 1706745599999; -- 2024-01-31 in milliseconds

-- Result: Only relevant TLogFS versions loaded, not all versions
```

### **Implementation Benefits (Immutable Extended Attributes)**

1. **Leverages Existing Patterns**: Builds on `create_file_path_streaming_with_type` rather than introducing new methods
2. **Fluent Interface**: Composable `.with_xattrs()` and `.with_temporal_extraction()` methods for new files
3. **Immutable Metadata**: Extended attributes set once on creation, inherited by all subsequent versions
4. **Simplified Consistency**: No validation of metadata changes - first version defines the "identity"
5. **Automatic Inheritance**: Subsequent writes automatically use v1 metadata, no user configuration needed
6. **Clear Error Model**: Metadata conflicts are impossible since only v1 can set attributes
7. **Performance**: No metadata updates during append operations, only temporal min/max updates

### **Complete Usage Examples (Immutable Extended Attributes)**

```rust
use crate::metadata::{ExtendedAttributes, duckpond};

// === Creating the FIRST version (extended attributes allowed) ===

// Example 1: Simple series with custom timestamp column
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?
    .with_temporal_extraction(Some("event_time"));

let mut parquet_writer = ArrowWriter::try_new(writer, temperature_batch.schema(), None)?;
parquet_writer.write(&temperature_batch)?;
parquet_writer.close()?;

// Example 2: Rich metadata for the initial version
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/geology/drill_log.series", EntryType::FileSeries)
    .await?
    .with_xattrs([
        (duckpond::TIMESTAMP_COLUMN.to_string(), "depth_time".to_string()),
        ("drill.site_id".to_string(), "alpha_001".to_string()),
        ("drill.operator".to_string(), "geological_survey_inc".to_string()),
        ("drill.target_depth".to_string(), "2000.0".to_string()),
    ]);

let mut parquet_writer = ArrowWriter::try_new(writer, drill_batch.schema(), None)?;
parquet_writer.write(&drill_batch)?;
parquet_writer.close()?;

// === Adding SUBSEQUENT versions (extended attributes inherited) ===

// Example 3: Appending more temperature data (no metadata needed)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/sensors/temperature.series", EntryType::FileSeries)
    .await?;  // No .with_* calls - inherits "event_time" timestamp column

let mut parquet_writer = ArrowWriter::try_new(writer, new_temperature_batch.schema(), None)?;
parquet_writer.write(&new_temperature_batch)?;
parquet_writer.close()?;

// Example 4: More drill log data (inherits all metadata from v1)
let (node_path, writer) = wd
    .create_file_path_streaming_with_type("/geology/drill_log.series", EntryType::FileSeries)
    .await?;  // Inherits "depth_time" column and all drill.* attributes

let mut parquet_writer = ArrowWriter::try_new(writer, more_drill_batch.schema(), None)?;
parquet_writer.write(&more_drill_batch)?;
parquet_writer.close()?;
wd.create_series("/sensors/temperature.series", &batch, Some("event_time")).await?;

// Reading the data back with time-range queries
let stream = wd.read_series_time_range(
    "/sensors/temperature.series",
    start_timestamp,  // Only loads versions that overlap this range
    end_timestamp,
).await?;

println!("Found {} relevant versions", stream.total_versions);
stream.for_each(|batch| {
    println!("Processing {} temperature readings", batch.num_rows());
    Ok(())
}).await?;
```

### **Implementation Benefits**

1. **Minimal Scope**: Only one metadata key (`duckpond.timestamp_column`) with sensible default ("Timestamp")
2. **DataFusion Performance**: Critical predicate pushdown architecture for efficient time-range queries
3. **Delta Lake Integration**: Perfect alignment with Delta Lake Table Provider pattern
4. **Future Extensible**: Simple extended attributes structure can grow as needed
5. **Type Safety**: Arrow timestamp types provide built-in precision handling

### **Simplified Metadata Consistency (Immutable Extended Attributes)**

```rust
/// Implementation strategy for immutable extended attributes
impl AsyncWriteWithMetadata for MetadataAwareWriter {
    fn with_temporal_extraction(mut self, timestamp_column: Option<&str>) -> Self {
        match self.file_version {
            FileVersion::First => {
                // First version: allow setting timestamp column
                self.temporal_extraction = timestamp_column.map(|s| s.to_string());
                self
            }
            FileVersion::Subsequent { existing_timestamp_column, .. } => {
                // Subsequent version: inherit from v1, ignore user input
                self.temporal_extraction = Some(existing_timestamp_column);
                
                // Optionally warn if user tried to set different column
                if let Some(requested) = timestamp_column {
                    if requested != existing_timestamp_column {
                        log::warn!(
                            "Ignoring timestamp column '{}' for existing file, using '{}'",
                            requested, existing_timestamp_column
                        );
                    }
                }
                self
            }
        }
    }
}

/// No more complex validation needed!
/// The first version establishes the "contract" for all subsequent versions
enum FileVersion {
    First,
    Subsequent {
        existing_timestamp_column: String,
        existing_extended_attributes: ExtendedAttributes,
        existing_schema: SchemaRef,
    }
}
```
```
```

## User Interface Design

### File:Series Access Interface

For accessing `file:series` data, we'll provide a **streaming time-range query interface** that builds on DuckPond's existing Arrow streaming architecture:

```rust
#[async_trait::async_trait]
pub trait SeriesExt {
    /// Stream RecordBatches from all series versions that overlap with the time range
    async fn read_series_time_range<P>(
        &self,
        path: P,
        start_time: i64,          // Start of time range (inclusive)
        end_time: i64,            // End of time range (inclusive)
    ) -> Result<SeriesStream>
    where
        P: AsRef<Path> + Send + Sync;
    
    /// Stream RecordBatches from all series versions (no time filtering)
    async fn read_series_all<P>(
        &self,
        path: P,
    ) -> Result<SeriesStream>
    where
        P: AsRef<Path> + Send + Sync;
    
    /// Create a new series version with temporal metadata extraction
    async fn append_series_data<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        time_column: Option<&str>,  // Auto-detect if None
    ) -> Result<()>
    where
        P: AsRef<Path> + Send + Sync;
}

/// Stream of RecordBatches from multiple series versions
pub struct SeriesStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    pub total_versions: usize,    // How many file versions contribute to this stream
    pub time_range: (i64, i64),   // Actual time range covered by the data
}

impl SeriesStream {
    /// Collect all batches (memory-bounded alternative to collect_all)
    pub async fn try_collect(self) -> Result<Vec<RecordBatch>> {
        self.try_collect().await
    }
    
    /// Process batches one at a time (memory-efficient)
    pub async fn for_each<F>(mut self, mut f: F) -> Result<()>
    where
        F: FnMut(RecordBatch) -> Result<()>,
    {
        while let Some(batch) = self.try_next().await? {
            f(batch)?;
        }
        Ok(())
    }
}
```

### Usage Examples

```rust
// Reading time series data with specific time range
let stream = wd.read_series_time_range(
    "/sensors/temperature.series",
    start_time,  // 2024-01-01 00:00:00 as timestamp
    end_time,    // 2024-01-31 23:59:59 as timestamp
).await?;

println!("Reading from {} versions covering {:?}", stream.total_versions, stream.time_range);

// Reading ALL versions of a series (no time filtering)
let all_stream = wd.read_series_all("/sensors/temperature.series").await?;
println!("Reading complete history: {} versions", all_stream.total_versions);

// Process data in memory-efficient streaming fashion
stream.for_each(|batch| {
    println!("Processing batch with {} rows", batch.num_rows());
    // Process individual batch...
    Ok(())
}).await?;

// Writing new series data (auto-detects timestamp column)
let new_data = create_temperature_batch(recent_readings);
wd.append_series_data("/sensors/temperature.series", &new_data, None).await?;

// Writing with explicit timestamp column
wd.append_series_data("/sensors/pressure.series", &pressure_data, Some("event_time")).await?;
```

### Interface Benefits

1. **Memory Efficient**: Streams RecordBatches rather than loading all data into memory
2. **Version Transparent**: Automatically handles multiple file versions behind the scenes
3. **Flexible Querying**: Time-range queries OR complete history scanning
4. **Time-Range Optimized**: Only loads data that overlaps with the query range (when range specified)
5. **Arrow Native**: Consistent with existing DuckPond streaming patterns
6. **Auto-Discovery**: Can automatically detect timestamp columns
7. **Composable**: Results can be further processed with DataFusion or other Arrow tools

### Implementation Strategy

The interface will leverage the dual-level filtering architecture, following Delta Lake's proven approach:

```rust
impl SeriesExt for WD {
    async fn read_series_time_range<P>(
        &self,
        path: P,
        start_time: i64,
        end_time: i64,
    ) -> Result<SeriesStream> {
        // 1. Use OplogEntry metadata to find relevant file versions (like Delta Lake's Add actions)
        let series_table = SeriesTable::new(path.as_ref(), self.operations_table());
        let file_infos = series_table.scan_time_range(start_time, end_time).await?;
        
        self.create_series_stream(file_infos, Some((start_time, end_time))).await
    }
    
    async fn create_series_stream(
        &self,
        file_infos: Vec<FileInfo>,
        time_range: Option<(i64, i64)>,
    ) -> Result<SeriesStream> {
        // 2. Create streaming reader for each relevant file
        let mut streams = Vec::new();
        for file_info in &file_infos {
            // 3. Open each Parquet file with optional time-range filter
            // DataFusion will automatically use Parquet statistics for pruning (like Delta Lake)
            let parquet_stream = match time_range {
                Some((start, end)) => {
                    self.read_parquet_with_time_filter(
                        &file_info.file_path,
                        &file_info.event_time_column,
                        start,
                        end,
                    ).await?
                }
                None => {
                    // No time filtering - read entire file
                    self.read_parquet_stream(&file_info.file_path).await?
                }
            };
            streams.push(parquet_stream);
        }
        
        // 4. Merge streams in temporal order
        let merged_stream = merge_temporal_streams(streams).await?;
        
        // 5. Calculate actual time range from file metadata
        let actual_time_range = if file_infos.is_empty() {
            time_range.unwrap_or((0, 0))
        } else {
            let min_time = file_infos.iter().map(|f| f.min_event_time).min().unwrap_or(0);
            let max_time = file_infos.iter().map(|f| f.max_event_time).max().unwrap_or(0);
            (min_time, max_time)
        };
        
        Ok(SeriesStream {
            inner: Box::pin(merged_stream),
            total_versions: file_infos.len(),
            time_range: actual_time_range,
        })
    }
}
```

**Delta Lake Pattern Alignment**:
- **File-level filtering**: Use OplogEntry metadata (like Delta Lake's Add.stats) to eliminate entire files
- **Automatic Parquet pruning**: DataFusion uses standard Parquet statistics for row group elimination
- **Standard compliance**: Both approaches leverage Apache Parquet specification for fine-grained filtering
```

Does this interface design align with your vision? The streaming approach with time-range queries should provide excellent performance while maintaining DuckPond's memory-efficient patterns.

### Min/Max Extraction Strategy (Simplified)

```rust
pub fn write_series_data(
    record_batch: RecordBatch,
    file_path: &Path,
    time_column: &str,
) -> Result<OplogEntry> {
    // 1. Extract min/max from RecordBatch before writing
    let (min_time, max_time) = extract_temporal_range_from_batch(&record_batch, time_column)?;
    
    // 2. Write the Parquet file (will have consistent statistics)
    let mut writer = ArrowWriter::try_new(file, schema, writer_props)?;
    writer.write(&record_batch)?;
    writer.close()?;
    
    // 3. Create OplogEntry with temporal metadata
    OplogEntry::new()
        .with_min_event_time(min_time)              // Dedicated column for fast queries
        .with_max_event_time(max_time)              // Dedicated column for fast queries  
        .with_event_time_column(time_column.to_string()) // Store column name directly
}

fn extract_temporal_range_from_batch(
    batch: &RecordBatch,
    time_column: &str,
) -> Result<(i64, i64)> {
    let time_array = batch
        .column_by_name(time_column)
        .ok_or_else(|| arrow_err!("Time column '{}' not found", time_column))?;
    
    // Handle different timestamp types
    match time_array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = time_array.as_primitive::<TimestampMillisecondType>();
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = time_array.as_primitive::<TimestampMicrosecondType>();
            let min = array.iter().flatten().min().unwrap_or(0);
            let max = array.iter().flatten().max().unwrap_or(0);
            Ok((min, max))
        }
        // Handle other timestamp types (nanosecond, second)...
        _ => Err(arrow_err!("Unsupported timestamp type: {:?}", time_array.data_type()))
    }
}
```

### Flexible Time Column Handling (Simplified)

```rust
pub struct SeriesConfig {
    pub timestamp_column: String,  // "timestamp", "Timestamp", "event_time", etc.
    pub schema: SchemaRef,
    pub series_id: String,        // For consistency validation
}

impl SeriesConfig {
    /// Auto-detect timestamp column with priority order
    pub fn detect_timestamp_column(schema: &Schema) -> Result<String> {
        // Priority order for auto-detection
        let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
        
        for candidate in candidates {
            if let Some(field) = schema.column_with_name(candidate) {
                if matches!(field.1.data_type(), DataType::Timestamp(_, _)) {
                    return Ok(candidate.to_string());
                }
            }
        }
        
        Err(arrow_err!("No timestamp column found in schema"))
    }
    
    /// Validate that new data uses the same timestamp column and schema
    pub fn validate_consistency(
        existing_time_column: &str,
        existing_schema: &Schema,
        new_time_column: &str,
        new_schema: &Schema,
    ) -> Result<()> {
        // Check timestamp column consistency
        if existing_time_column != new_time_column {
            return Err(arrow_err!(
                "Timestamp column mismatch: expected '{}', got '{}'",
                existing_time_column, new_time_column
            ));
        }
        
        // Check schema consistency (simple field count and type check)
        if existing_schema.fields().len() != new_schema.fields().len() {
            return Err(arrow_err!("Schema field count mismatch"));
        }
        
        for (existing_field, new_field) in existing_schema.fields().iter().zip(new_schema.fields()) {
            if existing_field.name() != new_field.name() || existing_field.data_type() != new_field.data_type() {
                return Err(arrow_err!(
                    "Schema field mismatch: {} != {}",
                    existing_field, new_field
                ));
            }
        }
        
        Ok(())
    }
}
```

### SeriesTable DataFusion Provider (Simplified)

```rust
pub struct SeriesTable {
    operations_table: OperationsTable,
    series_id: String,           // Filter for specific series
}

impl TableProvider for SeriesTable {
    async fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
        // Step 1: Use OplogEntry dedicated columns for fast temporal filtering
        let time_range = extract_time_range_from_filters(filters);
        let relevant_files = self.filter_by_temporal_range(time_range).await?;
        
        // Step 2: Create Parquet scan with remaining filters
        // DataFusion will automatically use Parquet statistics for further pruning
        ParquetExec::new(relevant_files, remaining_filters, projection)
    }
}

impl SeriesTable {
    pub async fn scan_time_range(&self, start: i64, end: i64) -> Result<Vec<FileInfo>> {
        // Simple query using only dedicated columns for filtering
        // Timestamp column retrieved from extended_attributes
        let query = "
            SELECT file_path, min_event_time, max_event_time, extended_attributes
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            AND max_event_time >= ? 
            AND min_event_time <= ?
            ORDER BY min_event_time
        ";
        
        // Fast temporal filtering via dedicated columns only
        let raw_results = self.operations_table.query(query, &[&self.series_id, start, end]).await?;
        
        // Extract timestamp column from each result's extended_attributes
        let mut file_infos = Vec::new();
        for result in raw_results {
            let extended_attrs = ExtendedAttributes::from_json(&result.extended_attributes)?;
            file_infos.push(FileInfo {
                file_path: result.file_path,
                min_event_time: result.min_event_time,
                max_event_time: result.max_event_time,
                event_time_column: extended_attrs.timestamp_column().to_string(),
            });
        }
        
        Ok(file_infos)
    }
    
    pub async fn scan_all_versions(&self) -> Result<Vec<FileInfo>> {
        // Get ALL versions for this series (no time filtering)
        let query = "
            SELECT file_path, min_event_time, max_event_time, extended_attributes
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            ORDER BY min_event_time
        ";
        
        // No time range filtering - returns all versions
        let raw_results = self.operations_table.query(query, &[&self.series_id]).await?;
        
        // Extract timestamp column from each result's extended_attributes
        let mut file_infos = Vec::new();
        for result in raw_results {
            let extended_attrs = ExtendedAttributes::from_json(&result.extended_attributes)?;
            file_infos.push(FileInfo {
                file_path: result.file_path,
                min_event_time: result.min_event_time,
                max_event_time: result.max_event_time,
                event_time_column: extended_attrs.timestamp_column().to_string(),
            });
        }
        
        Ok(file_infos)
    }
    
    /// Get the timestamp column name from the first version
    pub async fn get_timestamp_column(&self) -> Result<String> {
        let query = "
            SELECT extended_attributes 
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            ORDER BY version 
            LIMIT 1
        ";
        
        let result = self.operations_table.query(query, &[&self.series_id]).await?;
        let first_result = result.first()
            .ok_or_else(|| arrow_err!("No versions found for series {}", self.series_id))?;
        
        let extended_attrs = ExtendedAttributes::from_json(&first_result.extended_attributes)?;
        Ok(extended_attrs.timestamp_column().to_string())
    }
}

pub struct FileInfo {
    pub file_path: String,
    pub min_event_time: i64,
    pub max_event_time: i64,
    pub event_time_column: String,
}
```
```

## Design Decisions and Rationale

### 1. Dedicated Columns Approach
- **OplogEntry**: Three dedicated columns (min_event_time, max_event_time, event_time_column) for all series metadata
- **Parquet Statistics**: Standard fine-grained pruning within files
- **Benefits**: Maximum simplicity + fast queries + type safety + no JSON complexity
- **Delta Lake Validation**: Delta Lake stores similar min/max metadata per file, confirming this approach scales to production lakehouse systems

### 2. On-the-Fly Min/Max Extraction
- **Approach**: Extract temporal range from RecordBatch before writing
- **Benefits**: More efficient, guaranteed consistency, simpler error handling
- **Result**: OplogEntry metadata automatically matches Parquet statistics

### 3. Event Time vs Write Time Separation  
- **Challenge**: Historical data can be written after observation time
- **Solution**: Store both write time (OplogEntry.timestamp) and event time (min/max_event_time)
- **Benefit**: Supports realistic data ingestion patterns

### 4. Simple Metadata Storage
- **All essential data**: Stored in typed, dedicated columns
- **No JSON complexity**: Direct column access for all operations
- **Type safety**: All fields are strongly typed with compile-time guarantees
- **Minimal overhead**: No serialization/deserialization costs

### 5. Schema Consistency Per Series
- **Simplification**: No schema evolution support - use new series for schema changes
- **Validation**: Direct comparison of schema fields and timestamp column names
- **Series identification**: Use node_id (existing field) to identify series
- **Benefit**: Simple implementation, clear upgrade path, no additional complexity

### 6. Overlap Handling via Query Logic
- **Reality**: Time ranges across versions can and will overlap
- **Solution**: SQL-based filtering finds all potentially relevant files
- **Resolution**: DataFusion + Parquet statistics handle fine-grained deduplication

## Benefits of This Architecture

### Performance Hierarchy
1. **Fastest**: OplogEntry filtering (eliminates entire files)
2. **Fast**: Parquet row group pruning (eliminates chunks within files)
3. **Detailed**: Parquet page pruning (fine-grained elimination)

### Standards Compliance
- **Parquet Statistics**: Uses official Apache Parquet specification
- **DataFusion Integration**: Automatic tool support, no custom query logic needed
- **Arrow Native**: Consistent with DuckPond's Arrow-first architecture
- **Delta Lake Patterns**: Follows the same metadata architecture used by production lakehouse systems

### Future-Proof Design
- **Tool Compatibility**: Standard Parquet statistics work with any tool
- **Extensibility**: Pattern can extend to non-temporal dimensions
- **Evolution**: Both levels can evolve independently

## Success Criteria

### Functional Requirements
- ✅ Support versioned timeseries data with event time metadata
- ✅ Efficient range queries leveraging DataFusion + Parquet statistics  
- ✅ Handle overlapping time ranges across versions
- ✅ Flexible timestamp column names and types
- ✅ Schema consistency validation per series

### Performance Requirements
- ✅ O(relevant_files) query time, not O(total_files)
- ✅ Automatic Parquet-level pruning via DataFusion
- ✅ Memory-efficient operations (consistent with existing streaming architecture)

### Integration Requirements  
- ✅ Builds on existing TLogFS + DataFusion architecture
- ✅ Extends OplogEntry schema without breaking changes
- ✅ Consistent with Arrow-first DuckPond patterns
- ✅ Standard Parquet compliance for tool compatibility

## Next Steps

1. **Review and validate** this plan with the current codebase
2. **Implement Phase 1** core series support with OplogEntry extensions
3. **Create test cases** for temporal range extraction and validation
4. **Build SeriesTable** DataFusion provider for query integration
5. **Add CLI support** for series operations and testing

This plan leverages DuckPond's existing production-ready architecture to add sophisticated timeseries capabilities while maintaining the system's core principles of efficiency, standards compliance, and Arrow-first design.
