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
    
    // NEW: Event time metadata for series (dedicated columns)
    pub min_event_time: Option<i64>,    // Min timestamp from data for fast range queries
    pub max_event_time: Option<i64>,    // Max timestamp from data for fast range queries
    pub event_time_column: Option<String>, // Timestamp column name ("timestamp", "Timestamp", etc.)
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
            Arc::new(Field::new("event_time_column", DataType::Utf8, true)),
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

The interface will leverage the dual-level filtering architecture:

```rust
impl SeriesExt for WD {
    async fn read_series_time_range<P>(
        &self,
        path: P,
        start_time: i64,
        end_time: i64,
    ) -> Result<SeriesStream> {
        // 1. Use OplogEntry metadata to find relevant file versions
        let series_table = SeriesTable::new(path.as_ref(), self.operations_table());
        let file_infos = series_table.scan_time_range(start_time, end_time).await?;
        
        self.create_series_stream(file_infos, Some((start_time, end_time))).await
    }
    
    async fn read_series_all<P>(
        &self,
        path: P,
    ) -> Result<SeriesStream> {
        // 1. Get ALL versions for this series (no time filtering)
        let series_table = SeriesTable::new(path.as_ref(), self.operations_table());
        let file_infos = series_table.scan_all_versions().await?;
        
        self.create_series_stream(file_infos, None).await
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
        // Simple query using only dedicated columns
        let query = "
            SELECT file_path, min_event_time, max_event_time, event_time_column
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            AND max_event_time >= ? 
            AND min_event_time <= ?
            ORDER BY min_event_time
        ";
        
        // Fast temporal filtering via dedicated columns only
        self.operations_table.query(query, &[&self.series_id, start, end]).await
    }
    
    pub async fn scan_all_versions(&self) -> Result<Vec<FileInfo>> {
        // Get ALL versions for this series (no time filtering)
        let query = "
            SELECT file_path, min_event_time, max_event_time, event_time_column
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            ORDER BY min_event_time
        ";
        
        // No time range filtering - returns all versions
        self.operations_table.query(query, &[&self.series_id]).await
    }
    
    /// Get the timestamp column name from the first version
    pub async fn get_timestamp_column(&self) -> Result<String> {
        let query = "
            SELECT event_time_column 
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND node_id = ?
            ORDER BY version 
            LIMIT 1
        ";
        
        let result = self.operations_table.query(query, &[&self.series_id]).await?;
        result.first()
            .ok_or_else(|| arrow_err!("No versions found for series {}", self.series_id))
            .map(|s| s.clone())
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
