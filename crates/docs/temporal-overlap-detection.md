Y# Temporal Overlap Detection and Resolution

**Version**: 1.0  
**Date**: September 7, 2025  
**Status**: 🚧 **PLANNED** - Design document for temporal overlap handling in time series data

## Executive Summary

This document outlines the design for detecting and resolving temporal overlaps in time series data within DuckPond. The core problem occurs when multiple instruments at the same location have overlapping time ranges due to instrument replacement, maintenance, or operator error. Without proper handling, these overlaps create duplicate or conflicting data points that corrupt time series analysis.

## Problem Statement

### Real-World Scenario

In the HydroVu monitoring system, sensors are occasionally replaced:
- **SilverVulink1**: Original sensor (2024-01-01 to 2024-06-15)  
- **SilverVulink2**: Replacement sensor (2024-06-10 to 2024-12-31)
- **Overlap Period**: 2024-06-10 to 2024-06-15 (6 days)

During the overlap period, both sensors were recording data, creating:
- Duplicate timestamps with potentially different values
- Confusion about which data source is authoritative
- Incorrect results when merging time series via SQL JOIN operations

### Current SQL Problem

The Silver dataset query currently uses:
```sql
SELECT * FROM vulink1
NATURAL FULL OUTER JOIN vulink2
NATURAL FULL OUTER JOIN at500
ORDER BY timestamp
```

This creates **~8,000 empty rows** because the chained FULL OUTER JOINs generate a Cartesian product when temporal ranges don't align properly.

## Solution Overview

We need to join by distinct timestamp, but first we need to ensure
there are no duplicate ranges covered by existing data.

### Core Principle: Metadata-Driven Temporal Filtering

Instead of embedding temporal constraints in SQL queries, we store temporal overrides in file metadata and apply them automatically at the TLogFS layer. This keeps SQL queries clean while ensuring data integrity.

### Three-Phase Approach

1. **Detection**: Identify temporal overlaps using SQL queries against metadata
2. **Resolution**: Apply manual temporal bounds via `pond` commands, storing file-level temporal overrides that apply to the entire file series
3. **Enforcement**: Automatically filter data at read-time using stored overrides; the current version's temporal metadata defines the effective range for the entire file series

**Key Architectural Principle**: Temporal overrides are per-file, not per-version. The current version always defines the temporal range of the whole series. This simplifies management and aligns with the conceptual model that a file series represents a single logical time series.

As a minor detail, to manually update temporal bounds will require writing to the current version of a FileSeries without writing any new rows of data. Therefore, we need to ensure a few mechanical requirements:

a. FileSeries may contain empty versions with zero rows
b. Empty versions (zero rows) will have empty temporal bounds, means we need to support absent temporal bounds for empty versions

## Detailed Design

### Phase 1: SQL-Derived Factory Integration

#### Leveraging Existing SQL-Derived Series Factory

**Key Discovery**: The `sql-derived-series` factory already provides exactly what we need for temporal overlap detection. It:

1. **Combines Multiple FileSeries**: Automatically handles multiple files and versions using DataFusion's ListingTable
2. **Schema Harmonization**: Ensures compatible schemas across all input series via DataFusion
3. **UNION ALL Support**: Native support for `SELECT * FROM series1 UNION ALL SELECT * FROM series2` queries
4. **Pattern Matching**: Uses TinyFS pattern resolution (`/hydrovu/devices/**/SilverVulink*.series`)
5. **Origin Tracking Infrastructure**: Already assigns unique table names per resolved file set

#### Origin Column Extension Strategy

**Approach**: Extend the SQL-derived factory's table name generation to include origin information in queries:

```sql
-- Current: Basic UNION ALL
SELECT * FROM vulink1_abc123 UNION ALL SELECT * FROM vulink2_def456

-- Enhanced: Add origin column
SELECT *, 1 as _origin_id FROM vulink1_abc123 
UNION ALL 
SELECT *, 2 as _origin_id FROM vulink2_def456
```

#### Enhanced Temporal Metadata (Existing)

The TLogFS metadata already supports temporal overrides:

```rust
pub struct OplogEntry {
    // Existing temporal fields
    pub min_event_time: Option<i64>,     // Auto-detected from data
    pub max_event_time: Option<i64>,     // Auto-detected from data  
    pub min_override: Option<i64>,       // Manual temporal bounds
    pub max_override: Option<i64>,       // Manual temporal bounds
    // ...
}
```

**Key Insight**: The temporal metadata infrastructure is complete. We need to focus on the overlap detection algorithm, not metadata storage.

### Phase 2: Direct SQL-Derived Factory Construction

#### Virtual Origin Column Approach

Instead of boolean flags, we define the origin column as a virtual column that gets materialized only when projected. The SQL query determines whether origin tracking is needed:

```rust
pub struct OverlapDetectionBuilder {
    pattern: String,
    context: FactoryContext,
}

impl OverlapDetectionBuilder {
    pub async fn detect_overlaps(&self) -> Result<OverlapReport, Error> {
        // 1. Resolve pattern to individual series files
        let resolved_files = self.resolve_pattern_to_files().await?;
        
        // 2. Build dynamic patterns map (one entry per resolved file)
        let mut patterns = HashMap::new();
        for (index, file) in resolved_files.iter().enumerate() {
            let table_name = format!("series_{}", index + 1);
            patterns.insert(table_name, file.path.clone());
        }
        
        // 3. Construct query that projects _origin_id column
        let query = self.build_query_with_origin_projection(&patterns);
        
        // 4. Create SQL-derived factory configuration
        let config = SqlDerivedConfig {
            patterns,
            query: Some(query),
        };
        
        // 5. Execute factory - origin column materialized because it's projected
        let factory = SqlDerivedFile::new(config, self.context.clone(), SqlDerivedMode::Series)?;
        let arrow_batches = factory.collect_arrow_batches().await?;
        
        // 6. Apply overlap detection algorithm
        let detector = OverlapDetector::new(&resolved_files);
        detector.analyze_overlaps(arrow_batches)
    }
    
    fn build_query_with_origin_projection(&self, patterns: &HashMap<String, String>) -> String {
        let union_clauses: Vec<String> = patterns.keys().enumerate()
            .map(|(origin_id, table_name)| {
                // Origin column only materialized because query explicitly selects it
                .map(|(origin_id, table_name)| {
                // Add sequential origin tracking ID starting from 1
                format!("SELECT *, {} as _origin_id FROM {}", origin_id + 1, table_name)
            })
            .collect();
            
        // Explicit projection of _origin_id triggers materialization
        format!(
            "SELECT timestamp, value, _origin_id FROM ({}) ORDER BY timestamp", 
            union_clauses.join(" UNION ALL ")
        )
    }
    
    // Alternative: Query without origin tracking for simple union
    fn build_simple_union_query(&self, patterns: &HashMap<String, String>) -> String {
        let union_clauses: Vec<String> = patterns.keys()
            .map(|table_name| format!("SELECT * FROM {}", table_name))
            .collect();
            
        // No _origin_id projected = no materialization overhead
        format!(
            "SELECT timestamp, value FROM ({}) ORDER BY timestamp", 
            union_clauses.join(" UNION ALL ")
        )
    }
}
```

#### DataFusion Projection-Based Materialization

The origin column follows DataFusion's natural projection patterns:

**With Origin Tracking** (for overlap detection):
```sql
SELECT timestamp, value, _origin_id FROM (
  SELECT *, 1 as _origin_id FROM series_1 
  UNION ALL 
  SELECT *, 2 as _origin_id FROM series_2
) ORDER BY timestamp
```

**Without Origin Tracking** (for simple merging):
```sql
SELECT timestamp, value FROM (
  SELECT * FROM series_1 
  UNION ALL 
  SELECT * FROM series_2  
) ORDER BY timestamp
```

#### Benefits of Projection-Based Approach

1. **No Configuration Flags**: Column existence determined by query projection
2. **DataFusion Native**: Follows standard DataFusion column projection patterns
3. **Zero Overhead**: Origin column only computed when explicitly selected
4. **Query Flexibility**: Same factory can handle overlap detection, merging, analytics, and more
5. **Schema Clarity**: Virtual columns visible in schema but computed on-demand
6. **Naturally Extensible**: Adding new metadata columns follows the same pattern
7. **Optimizer Friendly**: DataFusion can eliminate unused columns automatically

**Note**: The `_origin_id` column name uses a leading underscore to signify it's an internal/metadata column, following common conventions for distinguishing system-generated columns from user data.
### Phase 3: Temporal Overlap Detection Algorithm

#### Core Algorithm: OpenTelemetry-Inspired Approach

Following the OpenTelemetry overlap resolution pattern, we interleave all data points and scan for consecutive runs from the same origin:

1. **Interleave Data**: Use SQL-derived factory with origin tracking to create unified timeline
2. **Scan for Runs**: Process ordered data points to identify consecutive sequences from same origin  
3. **Apply Validity Rules**: Consecutive points = valid run, alternating/singleton points = gaps
4. **Report Ranges**: Output valid temporal ranges per origin for human decision-making

#### Practical Example: Multiple Use Cases

The same factory construction approach supports different use cases based on projection:

```rust
// Overlap detection: projects _origin_id
let overlap_query = "SELECT timestamp, value, _origin_id FROM interleaved_data ORDER BY timestamp";

// Simple merging: no _origin_id projected  
let merge_query = "SELECT timestamp, value FROM interleaved_data ORDER BY timestamp";

// Analytics: projects _origin_id for grouping
let analytics_query = "SELECT _origin_id, COUNT(*), AVG(value) FROM interleaved_data GROUP BY _origin_id";

// Time range analysis: projects _origin_id for per-series ranges
let range_query = "SELECT _origin_id, MIN(timestamp), MAX(timestamp) FROM interleaved_data GROUP BY _origin_id";
```

Each query only computes what it needs. The origin column is always available in the schema but only materialized when explicitly projected.

#### Run Detection Logic (Post-Processing)

```rust
pub struct OverlapDetector {
    origin_lookup: OriginLookup,
}

impl OverlapDetector {
    pub fn detect_valid_ranges(&self, interleaved_data: Vec<RecordBatch>) -> Vec<ValidRange> {
        let mut valid_ranges = Vec::new();
        let mut current_run: Option<ActiveRun> = None;
        
        for batch in interleaved_data {
            for row_idx in 0..batch.num_rows() {
                let timestamp = extract_timestamp(&batch, row_idx);
                let origin_id = extract_origin_id(&batch, row_idx);
                let prev_origin = extract_prev_origin(&batch, row_idx);
                let next_origin = extract_next_origin(&batch, row_idx);
                
                // Apply OpenTelemetry overlap logic
                if self.is_valid_point(origin_id, prev_origin, next_origin) {
                    current_run = self.extend_or_start_run(current_run, origin_id, timestamp);
                } else {
                    // End current run and start gap
                    if let Some(run) = current_run.take() {
                        valid_ranges.push(run.into_valid_range());
                    }
                }
            }
        }
        
        valid_ranges
    }
    
    fn is_valid_point(&self, origin_id: i32, prev_origin: Option<i32>, next_origin: Option<i32>) -> bool {
        // Valid if: consecutive with same origin OR boundary conditions met
        prev_origin == Some(origin_id) || next_origin == Some(origin_id)
    }
}
```

### Phase 4: Command Interface

#### Overlap Detection Command

**Usage**: `pond detect-overlaps <pattern>`

**Example**:
```bash
pond detect-overlaps '/hydrovu/devices/**/SilverVulink*.series'
```

The command directly constructs a SQL-derived factory configuration for the overlap analysis:

1. **Direct Factory Construction**:
```rust
// In pond detect-overlaps command implementation
pub async fn detect_overlaps(pattern: &str) -> Result<OverlapReport, Error> {
    // Resolve pattern to individual series files
    let resolved_files = resolve_pattern_to_series_files(pattern).await?;
    
    // Construct SQL-derived factory config with origin tracking
    let config = SqlDerivedConfig {
        patterns: build_patterns_from_files(&resolved_files),
        query: Some(build_interleaved_query_with_origin(&resolved_files)),
        enable_origin_tracking: true, // Extension for origin column
    };
    
    // Create factory and execute query
    let factory = SqlDerivedFile::new(config, context, SqlDerivedMode::Series)?;
    let interleaved_data = factory.collect_arrow_batches().await?;
    
    // Apply OpenTelemetry overlap detection algorithm
    let overlap_detector = OverlapDetector::new();
    overlap_detector.detect_valid_ranges(interleaved_data)
}

fn build_interleaved_query_with_origin(files: &[ResolvedFile]) -> String {
    let union_clauses: Vec<String> = files.iter().enumerate()
        .map(|(i, file)| format!("SELECT *, {} as _origin_id FROM {}", i + 1, file.table_name))
        .collect();
    
    format!("SELECT * FROM ({}) ORDER BY timestamp", union_clauses.join(" UNION ALL "))
}
```

2. **Automatic Pattern-to-Table Mapping**: Each resolved file becomes a table in the patterns map
3. **Origin-Enhanced Query Generation**: Automatically builds the interleaved query with origin tracking
4. **Direct Arrow Processing**: Uses the factory's Arrow output for overlap algorithm

**Output**:
```
Temporal Overlap Analysis for pattern: /hydrovu/devices/**/SilverVulink*.series

Valid Temporal Ranges by Origin:
┌────────────────────────────────────────┬────────────────────┬────────────────────┬─────────────┬───────────┐
│ Series                                 │ Valid Start        │ Valid End          │ Row Count   │ Origin ID │
├────────────────────────────────────────┼────────────────────┼────────────────────┼─────────────┼───────────┤
│ /hydrovu/devices/site1/SilverVulink1  │ 2024-01-01T00:00:00│ 2024-06-09T23:59:00│ 8,640       │ 1         │
│ /hydrovu/devices/site1/SilverVulink2  │ 2024-06-16T00:00:00│ 2024-12-31T23:59:00│ 12,240      │ 2         │
└────────────────────────────────────────┴────────────────────┴────────────────────┴─────────────┴───────────┘

⚠️  DETECTED GAPS (points dropped due to alternating origins):
• 2024-06-10T00:00:00 to 2024-06-15T23:59:00 (6.0 days)
  └─ Reason: Alternating data points between SilverVulink1 and SilverVulink2

💡 RECOMMENDATION: Apply temporal bounds to resolve overlap:
   pond set-temporal-bounds '/hydrovu/devices/site1/SilverVulink1.series' --end '2024-06-09T23:59:59Z'
   pond set-temporal-bounds '/hydrovu/devices/site1/SilverVulink2.series' --start '2024-06-16T00:00:00Z'
```

#### Temporal Override Command

**Usage**: `pond set-temporal-bounds <path> --start <timestamp> --end <timestamp>`

**Examples**:
```bash
# Restrict SilverVulink1 to end before overlap
pond set-temporal-bounds '/hydrovu/devices/site1/SilverVulink1.series' \
  --end '2024-06-09T23:59:59Z'

# Restrict SilverVulink2 to start after overlap  
pond set-temporal-bounds '/hydrovu/devices/site1/SilverVulink2.series' \
  --start '2024-06-10T00:00:00Z'
```

**Validation Rules**:
- Override timestamps must be within auto-detected range of current version
- Start timestamp must be before end timestamp
- Overrides apply to the entire file series (all versions)
- Changes are made by updating the current version's metadata
- Command creates a new empty version if needed to store the override

## Implementation Strategy

### Phase 1: Direct Factory Construction (1-2 days)
1. **Create OverlapDetectionBuilder**: Build dynamic SQL-derived factory configurations from patterns
2. **Pattern Resolution**: Resolve glob patterns to individual series files for separate table mapping
3. **Query Generation**: Generate origin-tracked UNION ALL queries dynamically

### Phase 2: Overlap Detection Algorithm (2-3 days)  
1. **OverlapDetector**: Implement OpenTelemetry-inspired run detection logic
2. **Window Function Enhancement**: Add DataFusion window functions for lag/lead analysis
3. **Valid Range Calculation**: Apply consecutive point rules to identify valid temporal ranges

### Phase 3: Command Line Interface (1-2 days)
1. **pond detect-overlaps Command**: Create command using OverlapDetectionBuilder
2. **Human-Readable Reports**: Format output showing valid ranges and detected gaps
3. **Integration with Existing Commands**: Ensure compatibility with `pond set-temporal-bounds`

## Benefits of Direct Construction Approach

1. **Zero Changes to SQL-Derived Factory**: Uses existing factory as-is without modifications
2. **Dynamic Configuration**: Builds factory config on-demand based on resolved files
3. **Clean Architecture**: Overlap detection is separate concern from data access
4. **DataFusion Native**: Leverages full DataFusion capabilities for window functions
5. **Pattern Flexibility**: Each resolved file becomes its own table for precise origin tracking
6. **Fail-Fast Design**: Follows DuckPond's architectural principles - no fallbacks

## Estimated Implementation Time

- **Direct Factory Construction**: 1-2 days
- **Overlap Detection Algorithm**: 2-3 days  
- **Command Interface**: 1-2 days
- **Testing and Integration**: 1-2 days
- **Total**: 5-9 days

The key insight is that we can directly construct the SQL-derived factory configuration we need for overlap detection without modifying the factory itself. This approach is cleaner, more maintainable, and leverages all existing infrastructure while keeping the overlap detection logic as a separate concern.
```bash
pond verify-bounds '/hydrovu/devices/**/SilverVulink*.series'
```

**Output**:
```
✅ No temporal overlaps detected in pattern: /hydrovu/devices/**/SilverVulink*.series

Applied Overrides:
• /hydrovu/devices/site1/SilverVulink1.series: end → 2024-06-09T23:59:59Z
• /hydrovu/devices/site1/SilverVulink2.series: start → 2024-06-10T00:00:00Z
```

### Phase 4: Automatic Temporal Filtering

#### TLogFS Integration

When `SqlDerivedFile` or `OpLogFile` creates a RecordBatch stream:

1. **Check for Overrides**: Query current version metadata for temporal overrides
2. **Apply Filtering**: Filter RecordBatch stream to respect temporal bounds from current version
3. **Transparent Operation**: No changes needed to SQL queries in YAML configs
4. **Cross-Version Consistency**: All versions of a file use the same temporal bounds defined by current version

**Key Architectural Benefit**: Since temporal bounds are defined by the current version, there's no need to track which version of each file is being accessed - all versions of a file get the same temporal filtering.

#### Implementation Location

```rust
// In OpLogFile::record_batch_stream()
impl FileTable for OpLogFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        let mut stream = self.create_raw_stream().await?;
        
        // Apply temporal overrides from current version metadata
        if let Some(bounds) = self.get_current_version_temporal_overrides().await? {
            stream = stream.filter_temporal_range(bounds.start, bounds.end);
        }
        
        Ok(stream)
    }
}
```

### Phase 5: Enhanced SQL Queries

#### Original Problem Query (Silver)
```sql
-- PROBLEMATIC: Creates Cartesian product
SELECT * FROM vulink1
NATURAL FULL OUTER JOIN vulink2
NATURAL FULL OUTER JOIN at500
ORDER BY timestamp
```

#### Correct Time Series Merge Query
```sql
-- SOLUTION: Proper temporal joining with automatic filtering
SELECT * FROM vulink1
FULL OUTER JOIN vulink2 USING (timestamp)
FULL OUTER JOIN at500 USING (timestamp)
ORDER BY timestamp
```

**Key Improvements**:
- **Explicit JOIN conditions**: `USING (timestamp)` instead of `NATURAL`
- **Automatic temporal filtering**: Applied at TLogFS layer, not in SQL
- **Clean separation**: SQL handles schema merging, TLogFS handles temporal constraints

## Implementation Plan

### Step 1: Extend Metadata Schema
**Estimated Effort**: 2-3 days

- [ ] Add `temporal_overrides` field to file metadata structure (stored with current version, applies to entire series)
- [ ] Update metadata serialization/deserialization  
- [ ] Modify OpLogEntry table schema to include temporal override fields for current versions
- [ ] Create migration for existing metadata
- [ ] Ensure temporal bounds from current version are applied to all file access

### Step 2: Implement `pond set-temporal-bounds` Command
**Estimated Effort**: 2-3 days (simplified without version management)

- [ ] Add command parsing for temporal bounds syntax (no version parameter needed)
- [ ] Implement metadata update logic with validation for current version
- [ ] Add timestamp parsing and validation
- [ ] Create logic to create empty version if needed to store overrides
- [ ] Add comprehensive error handling
- [ ] Add unit tests for edge cases

### Step 3: Implement `pond check-overlaps` Command  
**Estimated Effort**: 3-4 days (simplified by only checking current versions)

- [ ] Implement pattern expansion to file paths
- [ ] Create SQL queries for temporal range extraction from current versions only
- [ ] Implement overlap detection algorithm (between files, not versions)
- [ ] Design and implement formatted output display (no version columns needed)
- [ ] Add integration tests with realistic data

### Step 4: Implement TLogFS Temporal Filtering
**Estimated Effort**: 2-3 days (simplified by current-version-only logic)

- [ ] Add temporal filtering to RecordBatch streams using current version metadata
- [ ] Integrate override loading into file reading logic (current version only)
- [ ] Ensure performance doesn't degrade for files without overrides
- [ ] Add comprehensive testing for cross-version temporal consistency

### Step 5: Add `pond verify-bounds` Command
**Estimated Effort**: 1-2 days

- [ ] Reuse overlap detection logic
- [ ] Add override display formatting
- [ ] Create success/failure reporting

### Step 6: Update SQL Queries and Documentation
**Estimated Effort**: 1 day

- [ ] Fix Silver dataset query syntax
- [ ] Update configuration documentation
- [ ] Add temporal override usage examples

**Total Estimated Effort**: 11-15 days (reduced from 14-19 days due to simplified per-file architecture)

## Success Metrics

### Functional Metrics
- [ ] **Overlap Detection**: Successfully identifies all temporal overlaps in test datasets
- [ ] **Override Application**: Temporal bounds are correctly applied and persisted
- [ ] **Data Integrity**: No duplicate timestamps in merged time series data
- [ ] **Query Performance**: No significant performance regression for files without overrides

### User Experience Metrics  
- [ ] **Command Usability**: Operators can detect and resolve overlaps without documentation
- [ ] **Error Clarity**: Clear error messages for invalid temporal bounds
- [ ] **Visual Feedback**: Easy-to-understand overlap reports and verification output

### Data Quality Metrics
- [ ] **Empty Row Elimination**: Silver dataset shows no empty rows after fix
- [ ] **Row Count Consistency**: Merged datasets have expected row counts
- [ ] **Temporal Continuity**: No gaps or overlaps in resolved time series

## Edge Cases and Considerations

### Multiple Overlaps
- **Scenario**: Three or more instruments with complex overlapping periods
- **Solution**: Iterative resolution with clear conflict reporting

### Version Management
- **Scenario**: New versions created after temporal overrides applied
- **Solution**: New versions inherit the file-level temporal overrides, maintaining consistency across all versions of the series

### Performance at Scale
- **Scenario**: Thousands of files with complex temporal patterns
- **Solution**: Efficient metadata indexing and lazy evaluation of temporal filters

### Data Recovery
- **Scenario**: Incorrect temporal overrides need to be removed
- **Solution**: `pond clear-temporal-bounds` command to reset to auto-detected ranges from current version data

## Future Enhancements

### Automatic Overlap Resolution
- **Concept**: ML-based detection of instrument replacement patterns
- **Implementation**: Statistical analysis of data quality during overlaps
- **Benefit**: Reduce manual intervention for common scenarios

### Temporal Interpolation
- **Concept**: Smart merging of overlapping measurements using interpolation
- **Implementation**: Configurable merge strategies (latest-wins, average, interpolate)
- **Benefit**: Preserve more data while maintaining integrity

### Real-Time Monitoring
- **Concept**: Continuous overlap detection as new data arrives
- **Implementation**: Event-driven overlap detection on data ingestion
- **Benefit**: Immediate feedback to operators about data quality issues

---

*This design ensures that time series data integrity is maintained through metadata-driven temporal filtering, keeping SQL queries clean while providing powerful tools for detecting and resolving real-world data quality issues.*
