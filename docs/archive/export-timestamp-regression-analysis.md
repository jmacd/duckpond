# Export Timestamp Regression Analysis

## Problem Summary

The DuckPond export pipeline is generating invalid JavaScript syntax in the Observable template output due to a regression in timestamp handling. The specific symptom is empty `start_time` and `end_time` values:

```javascript
{
  "start_time": ,     // Invalid JavaScript syntax
  "end_time": ,       // Invalid JavaScript syntax  
  "file": FileAttachment("Silver/res=12h/year=0/month=0/9m5CVmwnAwW8MUZ0.parquet"),
}
```

## Root Cause Investigation

### The Export Pipeline Flow

1. **Export Command**: `pond export --pattern '/reduced/single_site/*/*.series' --pattern '/templates/sites/site=*' --dir OUTDIR --temporal "year,month"`

2. **DataFusion Partitioning**: The export uses DataFusion's `COPY ... PARTITIONED BY (year, month)` command to create Hive-style partitioned directories based on:
   ```sql
   SELECT *, date_part('year', timestamp) as year, date_part('month', timestamp) as month FROM series
   ```

3. **Directory Structure Created**: DataFusion writes files to directories like:
   ```
   Silver/res=12h/year=2024/month=7/file.parquet  ✓ Valid
   Silver/res=12h/year=0/month=0/file.parquet     ✗ Invalid  
   ```

4. **Post-Export Scanning**: After export completes, `discover_exported_files()` scans the output directory and calls `extract_timestamps_from_path()` to parse partition directory names back into timestamps

5. **Template Generation**: These timestamps populate the Observable template, where `None` values render as empty strings, creating invalid JavaScript

### The Regression Source

The regression was introduced in commit after SHA `6490325156a1d12e1dc177a303f377516533d344` when error handling was changed from:

```rust
// OLD: Fail fast on timestamp parsing errors
let (start_time, end_time) = extract_timestamps_from_path(&relative_path)?;
```

To:

```rust
// NEW: Silent fallback to None values  
let (start_time, end_time) = extract_timestamps_from_path(&relative_path).unwrap_or((None, None));
```

This change masks the underlying problem: **DataFusion is creating `year=0/month=0` partitions because some source data has NULL or invalid timestamps.**

### The Fundamental Issue

**`year=0/month=0` partitions should never exist** because:

1. **FileSeries data is time series data** - there's always a valid timestamp
2. **Year 0 is invalid** - Unix timestamps start from 1970
3. **Month 0 is invalid** - months are 1-12
4. **These values indicate data corruption** - either NULL timestamps in source data or `date_part()` function failures

## Current Fallback Anti-Patterns

The export code violates DuckPond's [fallback anti-pattern philosophy](fallback-antipattern-philosophy.md) in multiple places:

### 1. HashMap Initialization Fallbacks

```rust
let mut temporal_parts = std::collections::HashMap::from([
    ("year", 0),     // FALLBACK: Invalid year that creates year=0 partitions
    ("month", 1),    // FALLBACK: Default month
    ("day", 1),      // FALLBACK: Default day
    ("hour", 0),     // FALLBACK: Default hour
    ("minute", 0),   // FALLBACK: Default minute
    ("second", 0),   // FALLBACK: Default second
]);
```

### 2. Function Parameter Fallbacks

In `calculate_end_time()`:
```rust
let year = *parts.get("year").unwrap_or(&1970) as i32;   // FALLBACK
let month = *parts.get("month").unwrap_or(&1) as u32;    // FALLBACK
let day = *parts.get("day").unwrap_or(&1) as u32;        // FALLBACK
let hour = *parts.get("hour").unwrap_or(&0) as u32;      // FALLBACK
let minute = *parts.get("minute").unwrap_or(&0) as u32;  // FALLBACK
let second = *parts.get("second").unwrap_or(&0) as u32;  // FALLBACK
```

In `build_utc_timestamp()` (similar pattern).

### 3. Error Masking Fallbacks

```rust
let (start_time, end_time) = extract_timestamps_from_path(&relative_path).unwrap_or((None, None));
```

## The --temporal Flag Contract

The `--temporal` flag specifies which temporal dimensions are expected in the partition structure:

- `--temporal="year,month"` means partitions MUST have valid `year=X/month=Y` directories
- `--temporal="year,month,day"` means partitions MUST have valid `year=X/month=Y/day=Z` directories

**The current code violates this contract** by using fallback defaults instead of requiring all specified temporal dimensions to be present with valid values.

## Proposed Architecture Fix

### 1. Remove All Fallbacks

Replace the current fallback-heavy approach with fail-fast validation:

```rust
// NEW: Option-based HashMap with explicit requirements
fn extract_timestamps_from_path(
    relative_path: &std::path::Path, 
    expected_temporal_parts: &[String]
) -> Result<(Option<i64>, Option<i64>)> {
    
    // Start with all defaults as Some(value)
    let mut temporal_parts: HashMap<&str, Option<i32>> = HashMap::from([
        ("year", Some(1970)),
        ("month", Some(1)),
        ("day", Some(1)),
        ("hour", Some(0)),
        ("minute", Some(0)),
        ("second", Some(0)),
    ]);
    
    // Override expected temporal parts to None (must be found in path)
    for required_part in expected_temporal_parts {
        temporal_parts.insert(required_part, None);
    }
    
    // Parse path and populate required fields
    // ...
    
    // FAIL FAST: Ensure all required fields were found with valid values
    for required_part in expected_temporal_parts {
        match temporal_parts.get(required_part) {
            Some(Some(value)) => {
                // Validate value is valid
                match required_part.as_str() {
                    "year" if *value < 1970 => return Err(anyhow::anyhow!("Invalid year {}: must be >= 1970", value)),
                    "month" if *value < 1 || *value > 12 => return Err(anyhow::anyhow!("Invalid month {}: must be 1-12", value)),
                    "day" if *value < 1 || *value > 31 => return Err(anyhow::anyhow!("Invalid day {}: must be 1-31", value)),
                    _ => {} // Valid
                }
            },
            Some(None) => return Err(anyhow::anyhow!("Required temporal part '{}' not found in path", required_part)),
            None => return Err(anyhow::anyhow!("Unknown temporal part '{}'", required_part)),
        }
    }
}
```

### 2. Update Function Signatures

Change `build_utc_timestamp()` and `calculate_end_time()` to work with `Option<i32>` values and fail fast on `None`:

```rust
fn build_utc_timestamp(parts: &HashMap<&str, Option<i32>>) -> Result<i64> {
    let year = parts.get("year").unwrap().ok_or_else(|| anyhow::anyhow!("Missing year"))? as i32;
    let month = parts.get("month").unwrap().ok_or_else(|| anyhow::anyhow!("Missing month"))? as u32;
    // etc - no fallbacks, explicit error handling
}
```

### 3. Propagate Expected Temporal Parts

Update call sites to pass the expected temporal parts down:

```rust
// In discover_exported_files() - need to get expected_temporal_parts parameter
let (start_time, end_time) = extract_timestamps_from_path(&relative_path, expected_temporal_parts)?;
```

## Expected Behavior After Fix

### When Export Encounters Invalid Data

The export should **fail fast** with a clear error message:

```
Error: Invalid year 0 in temporal path 'Silver/res=12h/year=0/month=0/file.parquet': must be >= 1970

This indicates NULL or invalid timestamps in the source FileSeries data.
Check the data integrity of the source file.
```

### When Export Succeeds

The Observable template should contain valid JavaScript:

```javascript
{
  "start_time": 1704067200,     // Valid Unix timestamp
  "end_time": 1706745599,       // Valid Unix timestamp  
  "file": FileAttachment("Silver/res=12h/year=2024/month=1/file.parquet"),
}
```

## Testing Strategy

1. **Build with fixes applied**
2. **Run existing export command** - should fail fast with clear error about `year=0`
3. **Investigate source data** to find records with NULL/invalid timestamps
4. **Fix data integrity issue** at the source
5. **Re-run export** - should succeed with valid timestamps

## Broader Investigation Needed

Once the export fails fast, investigate:

1. **Source Data Quality**: Why do some FileSeries records have NULL timestamps?
2. **Data Ingestion Pipeline**: Where are invalid timestamps being introduced?
3. **DataFusion date_part() Behavior**: How does `date_part('year', NULL)` behave?
4. **Schema Validation**: Should timestamp columns be non-nullable at the schema level?

This regression reveals a deeper data integrity issue that needs to be addressed at the data pipeline level, not masked with fallbacks in the export layer.