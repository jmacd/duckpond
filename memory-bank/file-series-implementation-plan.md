# File:Series Implementation Plan - Timeseries Data with DataFusion Integration

## Overview

Implementation plan for `file:series` support in DuckPond, leveraging the existing production-ready system with DataFusion integration for efficient timeseries data management. This builds on the established TLogFS + DataFusion architecture to provide versioned timeseries storage with temporal range queries.

## Core Concept

### Event Time vs Write Time Distinction
- **Write Time**: `OplogEntry.timestamp` - when the version was recorded in TLogFS
- **Event Time**: Data column values - actual observation/event timestamps within the data
- **Key Insight**: Can write historical data after the fact, so event time ≠ write time

### Dual-Level Metadata Architecture
1. **OplogEntry Level**: Fast file-level filtering for version selection
2. **Parquet Statistics Level**: Standard fine-grained pruning within files

## Technical Architecture

### OplogEntry Schema Enhancement

```rust
pub struct OplogEntry {
    // Existing fields (unchanged)
    pub timestamp: i64,           // Write time - when version was recorded
    pub version: i64,
    pub entry_type: EntryType,
    
    // NEW: Event time metadata for series
    pub min_event_time: Option<i64>,      // Min value from data's timestamp column
    pub max_event_time: Option<i64>,      // Max value from data's timestamp column  
    pub event_time_column: Option<String>, // "timestamp", "Timestamp", etc.
    
    // ... other existing fields
}
```

### Parquet Statistics Integration

Leverage **standard Apache Parquet statistics** for fine-grained pruning:
- **Row Group Level**: min/max timestamps per row group
- **Page Level**: Optional page-level statistics for finer granularity
- **Automatic DataFusion Integration**: No custom code needed - DataFusion automatically uses Parquet statistics for predicate pushdown

### Query Optimization Flow

```sql
-- Query: SELECT * FROM series WHERE timestamp BETWEEN '2023-01-01' AND '2023-01-31'

-- Step 1: OplogEntry filtering (fast file-level)
SELECT file_path FROM operations_table 
WHERE entry_type = 'FileSeries'
AND max_event_time >= '2023-01-01'::timestamp  
AND min_event_time <= '2023-01-31'::timestamp;

-- Step 2: Parquet-level pruning (automatic via DataFusion)
-- Uses standard Parquet statistics to eliminate row groups/pages
-- within the selected files
```

## Implementation Phases

### Phase 1: Core Series Support
1. **Extend OplogEntry** with event time metadata fields
2. **Implement EntryType::FileSeries** handling in TLogFS write operations
3. **Add temporal metadata extraction** from RecordBatch timestamp columns
4. **Create series-aware write operations** with min/max computation

### Phase 2: DataFusion Query Integration  
1. **Create SeriesTable** DataFusion TableProvider
2. **Implement time-range query capabilities** via SQL over operations_table
3. **Add version consolidation logic** for handling overlapping time ranges
4. **Test end-to-end query performance** with dual-level filtering

### Phase 3: Production Features
1. **Flexible timestamp column detection** ("timestamp", "Timestamp", etc.)
2. **Schema consistency validation** per series (no evolution support)
3. **Comprehensive error handling** and validation
4. **CLI integration** for series operations

## Key Implementation Details

### Min/Max Extraction Strategy (On-the-Fly Approach)

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
    
    // 3. Create OplogEntry with pre-computed metadata
    OplogEntry::new()
        .with_min_event_time(min_time)
        .with_max_event_time(max_time)
        .with_event_time_column(time_column.to_string())
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
        // Handle other timestamp types...
    }
}
```

### Flexible Time Column Handling

```rust
pub struct SeriesConfig {
    pub timestamp_column: String,  // "timestamp", "Timestamp", "event_time", etc.
    pub schema: SchemaRef,
}

impl SeriesConfig {
    pub fn detect_timestamp_column(schema: &Schema) -> Result<String> {
        // Priority order for auto-detection
        let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts"];
        
        for candidate in candidates {
            if let Some(field) = schema.column_with_name(candidate) {
                if matches!(field.1.data_type(), DataType::Timestamp(_, _)) {
                    return Ok(candidate.to_string());
                }
            }
        }
        
        Err(arrow_err!("No timestamp column found in schema"))
    }
}
```

### SeriesTable DataFusion Provider

```rust
pub struct SeriesTable {
    operations_table: OperationsTable,
    series_config: SeriesConfig,
}

impl TableProvider for SeriesTable {
    async fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
        // Step 1: Use OplogEntry metadata to filter relevant files
        let time_range = extract_time_range_from_filters(filters);
        let relevant_files = self.operations_table
            .filter_by_temporal_range(time_range)
            .await?;
        
        // Step 2: Create Parquet scan with remaining filters
        // DataFusion will automatically use Parquet statistics for further pruning
        ParquetExec::new(relevant_files, remaining_filters, projection)
    }
}

impl SeriesTable {
    pub async fn scan_time_range(&self, start: i64, end: i64) -> Result<Vec<String>> {
        // Find all files whose time ranges overlap with query range
        let query = "
            SELECT file_path, min_event_time, max_event_time
            FROM operations_table 
            WHERE entry_type = 'FileSeries'
            AND event_time_column IS NOT NULL
            AND max_event_time >= ? 
            AND min_event_time <= ?
            ORDER BY min_event_time
        ";
        
        // Returns files that might contain data in the time range
        // Overlaps will be resolved by DataFusion + Parquet statistics
        self.operations_table.query(query, &[start, end]).await
    }
}
```

## Design Decisions and Rationale

### 1. Dual-Level Metadata Approach
- **OplogEntry**: Fast coarse-grained filtering eliminates entire files
- **Parquet Statistics**: Standard fine-grained pruning within files
- **Benefits**: Maximum performance + tool compatibility + standard compliance

### 2. On-the-Fly Min/Max Extraction
- **Approach**: Extract temporal range from RecordBatch before writing
- **Benefits**: More efficient, guaranteed consistency, simpler error handling
- **Result**: OplogEntry metadata automatically matches Parquet statistics

### 3. Event Time vs Write Time Separation  
- **Challenge**: Historical data can be written after observation time
- **Solution**: Store both write time (OplogEntry.timestamp) and event time (min/max_event_time)
- **Benefit**: Supports realistic data ingestion patterns

### 4. Flexible Timestamp Column Support
- **Reality**: Different datasets use different column names ("timestamp", "Timestamp", etc.)
- **Solution**: Store column name in OplogEntry, auto-detect with priority list
- **Future**: Could extend to other temporal dimensions beyond timestamps

### 5. Schema Consistency Per Series
- **Simplification**: No schema evolution support - use new series for schema changes
- **Validation**: Enforce consistent schema and timestamp column per series
- **Benefit**: Simpler implementation, clear upgrade path

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
