# Timeseries Join Factory - Design and Usage Guide

## Overview

The `timeseries-join` factory simplifies the common pattern of joining multiple time series sources by timestamp. It automatically generates the SQL with COALESCE, FULL OUTER JOIN, and EXCLUDE clauses that would otherwise need to be written manually.

## Motivation

Previously, joining time series data required verbose SQL like this in `combine.yaml`:

```yaml
entries:
  - name: "BDock"
    factory: "sql-derived-series"
    config:
      patterns:
        vulink: "/hydrovu/devices/**/BDockVulink.series"
        at500: "/hydrovu/devices/**/BDockAT500.series"
      query: |
        SELECT 
          COALESCE(vulink.timestamp, at500.timestamp) AS timestamp,
          vulink.* EXCLUDE (timestamp),
          at500.* EXCLUDE (timestamp)
        FROM vulink 
        FULL OUTER JOIN at500 ON vulink.timestamp = at500.timestamp
        ORDER BY timestamp
```

With `timeseries-join`, this becomes:

```yaml
entries:
  - name: "BDock"
    factory: "timeseries-join"
    config:
      patterns:
        vulink: "/hydrovu/devices/**/BDockVulink.series"
        at500: "/hydrovu/devices/**/BDockAT500.series"
```

## Configuration

### Basic Configuration

```yaml
entries:
  - name: "combined_data"
    factory: "timeseries-join"
    config:
      patterns:
        source1: "/path/to/source1.series"
        source2: "/path/to/source2.series"
```

### Full Configuration Options

```yaml
entries:
  - name: "combined_data"
    factory: "timeseries-join"
    config:
      # Named patterns - keys become table aliases in SQL
      patterns:
        vulink: "/hydrovu/devices/**/SilverVulink*.series"
        at500: "/hydrovu/devices/**/SilverAT500.series"
        
      # Optional: timestamp column name (defaults to "timestamp")
      time_column: "timestamp"
      
      # Optional: time range filtering
      time_range:
        begin: "2023-11-06T14:00:00Z"  # Optional start time (ISO 8601)
        end: "2024-04-06T07:00:00Z"    # Optional end time (ISO 8601)
```

## Time Range Filtering

The `time_range` field supports flexible temporal filtering:

### Unbounded (All Data)
```yaml
# Omit time_range entirely
config:
  patterns:
    source1: "/data/s1.series"
    source2: "/data/s2.series"
```

### Half-Bounded (Start Time Only)
```yaml
config:
  patterns:
    source1: "/data/s1.series"
    source2: "/data/s2.series"
  time_range:
    begin: "2023-01-01T00:00:00Z"
```

### Half-Bounded (End Time Only)
```yaml
config:
  patterns:
    source1: "/data/s1.series"
    source2: "/data/s2.series"
  time_range:
    end: "2024-01-01T00:00:00Z"
```

### Fully Bounded
```yaml
config:
  patterns:
    source1: "/data/s1.series"
    source2: "/data/s2.series"
  time_range:
    begin: "2023-11-06T14:00:00Z"
    end: "2024-04-06T07:00:00Z"
```

## Generated SQL

Given the configuration:

```yaml
config:
  patterns:
    vulink: "/hydrovu/devices/**/Vulink.series"
    at500: "/hydrovu/devices/**/AT500.series"
  time_column: "timestamp"
  time_range:
    begin: "2023-11-06T14:00:00Z"
    end: "2024-04-06T07:00:00Z"
```

The factory generates:

```sql
SELECT
  COALESCE(at500.timestamp, vulink.timestamp) AS timestamp,
  at500.* EXCLUDE (timestamp),
  vulink.* EXCLUDE (timestamp)
FROM at500
FULL OUTER JOIN vulink ON at500.timestamp = vulink.timestamp
WHERE timestamp >= '2023-11-06T14:00:00Z' AND timestamp <= '2024-04-06T07:00:00Z'
ORDER BY timestamp
```

### Join Semantics

- **FULL OUTER JOIN**: Preserves all timestamps from all sources
- **COALESCE**: Selects the first non-null timestamp value
- **EXCLUDE**: Prevents duplicate timestamp columns in the output
- **ORDER BY**: Results are sorted by timestamp

## Multiple Sources

The factory supports joining more than two sources:

```yaml
config:
  patterns:
    vulink: "/devices/**/Vulink.series"
    at500: "/devices/**/AT500.series"
    sensor3: "/devices/**/Sensor3.series"
```

Generates:

```sql
SELECT
  COALESCE(at500.timestamp, sensor3.timestamp, vulink.timestamp) AS timestamp,
  at500.* EXCLUDE (timestamp),
  sensor3.* EXCLUDE (timestamp),
  vulink.* EXCLUDE (timestamp)
FROM at500
FULL OUTER JOIN sensor3 ON at500.timestamp = sensor3.timestamp
FULL OUTER JOIN vulink ON at500.timestamp = vulink.timestamp
ORDER BY timestamp
```

## Architecture

### Implementation Pattern

The `timeseries-join` factory follows the same architecture as `temporal-reduce`:

1. **Wrapper File Type**: `TimeseriesJoinFile` wraps configuration
2. **Lazy SQL Generation**: SQL is generated on first access
3. **Internal Delegation**: Creates an internal `SqlDerivedFile` with generated SQL
4. **QueryableFile Trait**: Implements DataFusion integration
5. **Fail-Fast Validation**: Configuration errors caught at creation time

### Critical Design Patterns

✅ **Read `duckpond-system-patterns.md`** - Single transaction rule applies

The factory follows DuckPond's architectural principles:

- **Single Transaction Rule**: Reuses transaction context, never creates new transactions
- **Fail-Fast Validation**: Validates timestamps and patterns at configuration time
- **Delegation Pattern**: Internally uses `SqlDerivedFile` to avoid code duplication

## Validation

The factory performs validation at configuration time:

### Pattern Validation
- ❌ **Error**: Empty patterns
- ❌ **Error**: Single pattern (use `sql-derived-series` instead)
- ✅ **Valid**: Two or more patterns

### Timestamp Validation
- ❌ **Error**: Invalid ISO 8601 format
- ✅ **Valid**: RFC 3339 timestamps (e.g., `2023-11-06T14:00:00Z`)

## Use Cases

### 1. Multi-Sensor Data Combination

Combining data from different sensors at the same location:

```yaml
entries:
  - name: "dock_combined"
    factory: "timeseries-join"
    config:
      patterns:
        temperature: "/sensors/dock/temperature.series"
        pressure: "/sensors/dock/pressure.series"
        humidity: "/sensors/dock/humidity.series"
```

### 2. Historical Data Analysis

Analyzing a specific time window:

```yaml
entries:
  - name: "winter_2023_analysis"
    factory: "timeseries-join"
    config:
      patterns:
        indoor: "/climate/indoor.series"
        outdoor: "/climate/outdoor.series"
      time_range:
        begin: "2023-12-01T00:00:00Z"
        end: "2024-03-01T00:00:00Z"
```

### 3. Data Quality Analysis

Comparing data from multiple collection methods:

```yaml
entries:
  - name: "collection_comparison"
    factory: "timeseries-join"
    config:
      patterns:
        manual: "/data/manual_readings.series"
        automated: "/data/automated_readings.series"
        reference: "/data/reference_standard.series"
```

## Comparison with sql-derived-series

| Feature | timeseries-join | sql-derived-series |
|---------|----------------|-------------------|
| **Purpose** | Timestamp-based joins | Arbitrary SQL queries |
| **Configuration** | Declarative patterns | Full SQL query |
| **Time Filtering** | Built-in time_range | Manual WHERE clauses |
| **Join Type** | Always FULL OUTER JOIN | Any SQL join |
| **Complexity** | Low (patterns only) | High (full SQL) |
| **Flexibility** | Limited to timestamp joins | Unlimited |

**Use `timeseries-join` when:**
- Joining multiple time series by timestamp
- You want automatic COALESCE handling
- You need time range filtering
- Configuration simplicity is important

**Use `sql-derived-series` when:**
- You need custom SQL logic
- Joins are not based on simple timestamp equality
- You need aggregations or window functions
- You need complex WHERE clauses

## Testing

The factory includes comprehensive tests:

```rust
#[tokio::test]
async fn test_timeseries_join_factory_integration() {
    // Creates two test files with overlapping timestamps
    // Verifies FULL OUTER JOIN produces correct union
    // Confirms schema includes all columns from both sources
}
```

Run tests with:

```bash
cargo test --package tlogfs --lib timeseries_join
```

## Migration Guide

### From combine.yaml

**Before:**

```yaml
entries:
  - name: "Silver"  
    factory: "sql-derived-series"
    config:
      patterns:
        vulink: "/hydrovu/devices/**/SilverVulink*.series"
        at500: "/hydrovu/devices/**/SilverAT500.series"
      query: |
        SELECT 
          COALESCE(vulink.timestamp, at500.timestamp) AS timestamp,
          vulink.* EXCLUDE (timestamp),
          at500.* EXCLUDE (timestamp)
        FROM vulink
        FULL OUTER JOIN at500 ON vulink.timestamp = at500.timestamp
        ORDER BY timestamp
```

**After:**

```yaml
entries:
  - name: "Silver"  
    factory: "timeseries-join"
    config:
      patterns:
        vulink: "/hydrovu/devices/**/SilverVulink*.series"
        at500: "/hydrovu/devices/**/SilverAT500.series"
```

### Adding Time Filtering

If you had temporal filtering in WHERE clauses:

**Before:**

```yaml
query: |
  SELECT 
    COALESCE(vulink.timestamp, at500.timestamp) AS timestamp,
    vulink.* EXCLUDE (timestamp),
    at500.* EXCLUDE (timestamp)
  FROM vulink
  FULL OUTER JOIN at500 ON vulink.timestamp = at500.timestamp
  WHERE timestamp >= '2023-11-06T14:00:00Z' 
    AND timestamp <= '2024-04-06T07:00:00Z'
  ORDER BY timestamp
```

**After:**

```yaml
factory: "timeseries-join"
config:
  patterns:
    vulink: "/hydrovu/devices/**/SilverVulink*.series"
    at500: "/hydrovu/devices/**/SilverAT500.series"
  time_range:
    begin: "2023-11-06T14:00:00Z"
    end: "2024-04-06T07:00:00Z"
```

## Implementation Reference

- **Module**: `crates/tlogfs/src/timeseries_join.rs`
- **Registration**: Registered in `crates/tlogfs/src/lib.rs`
- **Factory Name**: `"timeseries-join"`
- **Base Pattern**: Similar to `temporal_reduce.rs`

## Future Enhancements

Potential future improvements:

1. **Custom Join Conditions**: Support for fuzzy timestamp matching
2. **Interpolation**: Fill missing values between timestamps
3. **Resampling**: Automatic temporal resampling to common intervals
4. **Column Selection**: Specify which columns to include from each source
5. **Join Type Selection**: Support for LEFT/RIGHT joins in addition to FULL OUTER

## Summary

The `timeseries-join` factory provides a high-level, declarative interface for the common pattern of joining time series data by timestamp. It reduces configuration complexity, improves readability, and adds built-in time range filtering while maintaining full compatibility with DuckPond's architectural patterns.
