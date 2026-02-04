# Nullable Timestamp Fix - Summary

## Problem

FileSeries files were being created with nullable timestamp columns, causing invalid `year=0/month=0` temporal partitions during export. This violated the fail-fast design philosophy by allowing corrupted data to propagate through the system.

## Root Causes

### 1. NATURAL FULL OUTER JOIN in combine.yaml

**Original problematic query:**
```sql
SELECT * FROM vulink 
NATURAL FULL OUTER JOIN at500 
ORDER BY timestamp
```

**Issue:** DataFusion's type inference marks the timestamp column as nullable when using NATURAL FULL OUTER JOIN because:
- The join can produce rows where one side has NULL values
- Even though individual timestamps are non-null, the schema inference is conservative
- The result schema has `nullable=true` for the timestamp column

### 2. DATE_TRUNC without COALESCE in temporal-reduce

**Original problematic query:**
```sql
WITH time_buckets AS (
  SELECT DATE_TRUNC('hour', timestamp) AS time_bucket, ...
  FROM series
  GROUP BY DATE_TRUNC('hour', timestamp)
)
SELECT CAST(time_bucket AS TIMESTAMP) AS timestamp, ...
FROM time_buckets
```

**Issue:** 
- `DATE_TRUNC` can theoretically return NULL (if input is NULL)
- `CAST` alone doesn't change nullability, only the data type
- DataFusion's schema inference marks the result as nullable

## Solutions

### 1. Fix combine.yaml: Use COALESCE for FULL OUTER JOIN

**Fixed query:**
```sql
SELECT 
  COALESCE(vulink.timestamp, at500.timestamp) AS timestamp,
  vulink.* EXCLUDE (timestamp),
  at500.* EXCLUDE (timestamp)
FROM vulink 
FULL OUTER JOIN at500 ON vulink.timestamp = at500.timestamp
ORDER BY timestamp
```

**Why this works:**
- `COALESCE(a, b)` tells DataFusion "this will never be NULL" at the schema level
- Explicit column selection with `EXCLUDE (timestamp)` prevents duplicate columns
- Maintains same functional behavior as NATURAL JOIN

### 2. Fix temporal-reduce: Use COALESCE with CAST

**Fixed query:**
```sql
WITH time_buckets AS (
  SELECT 
    DATE_TRUNC('hour', timestamp) AS time_bucket,
    ...
  FROM series
  WHERE timestamp IS NOT NULL
  GROUP BY DATE_TRUNC('hour', timestamp)
)
SELECT 
  COALESCE(CAST(time_bucket AS TIMESTAMP), CAST(0 AS TIMESTAMP)) AS timestamp,
  ...
FROM time_buckets
ORDER BY time_bucket
```

**Why this works:**
- `WHERE timestamp IS NOT NULL` filters out nulls at query time
- `COALESCE(..., CAST(0 AS TIMESTAMP))` provides fallback (never actually used)
- DataFusion's schema inference sees the COALESCE and marks column as non-nullable

### 3. Fail-Fast Validation

**Created reusable validation function** in `crates/tlogfs/src/schema_validation.rs`:

```rust
pub fn validate_fileseries_timestamp(schema: &Arc<Schema>, context: &str) -> Result<(), String>
```

**Features:**
- Validates that FileSeries files have non-nullable timestamp columns
- Clear error messages pointing to SQL query issues
- Comprehensive unit tests
- Used in export command to fail fast on schema violations

## Alternative Approaches Considered

### SELECT DISTINCT + UNION Pattern

**Proposed alternative:**
```sql
WITH all_timestamps AS (
  SELECT DISTINCT timestamp FROM table1
  UNION
  SELECT DISTINCT timestamp FROM table2
)
SELECT t.timestamp, table1.*, table2.*
FROM all_timestamps t
LEFT JOIN table1 ON t.timestamp = table1.timestamp  
LEFT JOIN table2 ON t.timestamp = table2.timestamp
```

**Advantages:**
- Should produce non-nullable timestamps (UNION preserves non-nullability)
- Might be clearer intent than COALESCE
- Explicitly shows "gather all timestamps, then join data"

**Status:** Not tested yet, but worth considering for future refactoring

## Key Insights

1. **DataFusion's nullability inference is conservative**: It marks columns nullable unless it can prove they're not
2. **COALESCE is explicit proof of non-nullability**: It tells the type system "this has a fallback value"
3. **WHERE clauses don't affect schema**: Filtering happens at query time, schema inference happens at plan time
4. **CAST doesn't change nullability**: It only changes the data type

## Files Changed

1. `noyo/combine.yaml` - Fixed FULL OUTER JOIN queries with COALESCE
2. `crates/tlogfs/src/temporal_reduce.rs` - Fixed DATE_TRUNC queries with COALESCE
3. `crates/tlogfs/src/schema_validation.rs` - New reusable validation module
4. `crates/cmd/src/commands/export.rs` - Restored fail-fast schema validation
5. `crates/tlogfs/src/sql_derived.rs` - Updated test cases to use COALESCE pattern

## Testing

All tests pass:
- Schema validation unit tests verify nullable detection
- Export date arithmetic tests verify temporal partitioning
- SQL-derived tests verify COALESCE pattern works
- Integration tests verify end-to-end functionality

## Lessons Learned

1. **Fail-fast is crucial**: Silent workarounds hide bugs instead of fixing them
2. **Type system matters**: DataFusion's schema inference requires explicit non-null semantics
3. **Test SQL patterns**: Understanding DataFusion's nullability inference helps write correct queries
4. **Reusable validation**: Common validation functions make it easier to maintain consistency

## Next Steps

1. Consider testing SELECT DISTINCT + UNION pattern as alternative to COALESCE
2. Add more schema validation checks for other data quality issues
3. Document SQL patterns that produce non-nullable columns
4. Consider adding compile-time SQL query validation
