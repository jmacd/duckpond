# Temporal Overlap Detection and Resolution

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

### Phase 1: Metadata Schema Extension

#### Current Temporal Metadata

Presently, the TlogFS metadata is defined:

```
    /// Time-series data with temporal range
    Series {
        min_timestamp: i64,
        max_timestamp: i64,
        timestamp_column: String,
    },
```

#### Enhanced Temporal Metadata

The revised approach stores temporal overrides at the file level, not per-version. This simplifies the design significantly:

**File-Level Temporal Metadata** (stored with current version):
- min_timestamp: optional minimum computed from actual data (unset for empty version)
- max_timestamp: optional maximum computed from actual data (unset for empty version)  
- min_override: optional manually applied min for entire file series (added/updated/removed by command-line)
- max_override: optional manually applied max for entire file series (added/updated/removed by command-line)

**Key Change**: Instead of having version-specific overrides, the current version's metadata defines the temporal bounds for the entire file series. When accessing any version of a file, the temporal filtering uses the bounds defined by the current version.

#### Storage Strategy
- **Primary Storage**: File metadata structure (existing pattern) - stored with current version but applies to entire file series
- **Query Access**: OpLogEntry table includes temporal fields for SQL-based analysis, using current version metadata
- **Effective Range Calculation**: `COALESCE(manual_override, auto_range)` from current version metadata
- **Cross-Version Application**: All versions of a file use the temporal bounds defined by the current version

### Phase 2: Overlap Detection via SQL

#### Step 1: Query Temporal Ranges

For pattern matching (e.g., `/hydrovu/devices/**/SilverVulink*.series`), we query the current version of each file:

```sql
SELECT 
    file_path,
    auto_min_timestamp,
    auto_max_timestamp,
    override_min_timestamp,  -- NULL if no override, applies to entire file series
    override_max_timestamp   -- NULL if no override, applies to entire file series
FROM oplog_entries 
WHERE file_path LIKE '/hydrovu/devices/%/SilverVulink%.series'
  AND entry_type = 'file_version'
  AND is_current_version = true  -- Only query current versions
ORDER BY file_path
```

#### Step 2: Calculate Effective Ranges

```sql
WITH effective_ranges AS (
  SELECT 
    file_path,
    COALESCE(override_min_timestamp, auto_min_timestamp) as effective_min,
    COALESCE(override_max_timestamp, auto_max_timestamp) as effective_max
  FROM temporal_metadata_query
)
```

#### Step 3: Detect Overlaps

```sql
-- Self-join to find overlapping ranges between different files
SELECT 
    a.file_path as file_a,
    b.file_path as file_b, 
    GREATEST(a.effective_min, b.effective_min) as overlap_start,
    LEAST(a.effective_max, b.effective_max) as overlap_end,
    EXTRACT(EPOCH FROM (LEAST(a.effective_max, b.effective_max) - GREATEST(a.effective_min, b.effective_min))) / 86400 as overlap_days
FROM effective_ranges a
JOIN effective_ranges b ON a.file_path < b.file_path  -- Only compare different files
WHERE a.effective_max > b.effective_min 
  AND a.effective_min < b.effective_max
ORDER BY overlap_days DESC
```

### Phase 3: Command Interface

#### Overlap Detection Command

**Usage**: `pond check-overlaps <pattern>`

**Example**:
```bash
pond check-overlaps '/hydrovu/devices/**/SilverVulink*.series'
```

**Output**:
```
Temporal Overlap Analysis for pattern: /hydrovu/devices/**/SilverVulink*.series

File Temporal Ranges (Current Versions):
┌─────────────────────────────────────────────┬────────────────────┬────────────────────┬──────────────┬──────────────┐
│ File                                        │ Start              │ End                │ Row Count    │ Override     │
├─────────────────────────────────────────────┼────────────────────┼────────────────────┼──────────────┼──────────────┤
│ /hydrovu/devices/site1/SilverVulink1.series│ 2024-01-01T00:00:00│ 2024-06-15T23:59:00│ 8,760        │ ✗ None       │
│ /hydrovu/devices/site1/SilverVulink2.series│ 2024-06-10T00:00:00│ 2024-12-31T23:59:00│ 12,480       │ ✗ None       │
└─────────────────────────────────────────────┴────────────────────┴────────────────────┴──────────────┴──────────────┘

⚠️  OVERLAPS DETECTED:
• SilverVulink1 ↔ SilverVulink2
  └─ Overlap: 2024-06-10T00:00:00 to 2024-06-15T23:59:00 (6.0 days)
  └─ Recommendation: Apply temporal bounds to resolve conflict

Total overlapping time: 6.0 days
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

#### Verification Command

**Usage**: `pond verify-bounds <pattern>`

**Example**:
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
