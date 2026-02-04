# Timeseries Pivot NULL Padding - Dead End

## Problem Statement

The `timeseries_pivot` factory needs to pivot specific columns across multiple time series inputs (e.g., Silver, FieldStation, BDock, Princess). The configuration specifies columns like:
- `AT500_Surface.DO.mg/L`
- `AT500_Bottom.DO.mg/L`

However, not all sites have all columns:
- **Silver**: Only has AT500_Surface data, NOT AT500_Bottom
- **FieldStation, BDock, Princess**: Have both AT500_Surface and AT500_Bottom

## Attempted Solution: COALESCE with NULL-Padding

### Approach
User proposed a COALESCE strategy:
1. For each input table, create a `{}_nulls` CTE with timestamp + all configured columns as NULL
2. FULL OUTER JOIN `{}_nulls` with the base table
3. Use COALESCE to pick real values when they exist, NULL otherwise

### SQL Pattern
```sql
WITH Silver_nulls AS (
  SELECT timestamp, 
         NULL AS "AT500_Surface.DO.mg/L", 
         NULL AS "AT500_Bottom.DO.mg/L" 
  FROM Silver
),
Silver_final AS (
  SELECT 
    COALESCE(Silver.timestamp, Silver_nulls.timestamp) AS timestamp,
    COALESCE(Silver."AT500_Surface.DO.mg/L", Silver_nulls."AT500_Surface.DO.mg/L") AS "AT500_Surface.DO.mg/L",
    COALESCE(Silver."AT500_Bottom.DO.mg/L", Silver_nulls."AT500_Bottom.DO.mg/L") AS "AT500_Bottom.DO.mg/L"
  FROM Silver_nulls 
  FULL OUTER JOIN Silver 
  ON Silver_nulls.timestamp = Silver.timestamp
)
```

### Why It Fails

**DataFusion Limitation**: After a FULL OUTER JOIN, DataFusion only makes available columns that actually exist in each table's schema.

After `Silver_nulls FULL OUTER JOIN Silver`, the available fields are:
- `Silver_nulls.timestamp` ✓
- `Silver_nulls."AT500_Surface.DO.mg/L"` ✓ (NULL)
- `Silver_nulls."AT500_Bottom.DO.mg/L"` ✓ (NULL)
- `Silver.timestamp` ✓
- `Silver."AT500_Surface.DO.mg/L"` ✓ (exists in Silver)
- `Silver."AT500_Bottom.DO.mg/L"` ❌ **Does NOT exist** - Silver doesn't have this column

When trying to reference `Silver."AT500_Bottom.DO.mg/L"` in the COALESCE:
```
Error: Schema error: No field named sql_derived_silver_c24fbb23c987bf74."AT500_Bottom.DO.mg/L"
Valid fields are ... sql_derived_silver_c24fbb23c987bf74."AT500_Surface.DO.mg/L" ...
```

### Key Insight

The COALESCE approach works in standard SQL databases (as shown in StackOverflow examples), but **DataFusion v49.0.2 does not allow referencing non-existent columns even after a FULL OUTER JOIN**.

This is fundamentally incompatible with the goal of creating a uniform schema across tables with different column sets.

## Alternative Approaches Considered

### 1. SELECT * with NULL columns
```sql
{}_final AS (SELECT *, NULL AS "col1", NULL AS "col2" FROM {})
```
**Problem**: Creates duplicate column names if column already exists in base table.

### 2. UNION ALL with schema definition
```sql
{}_final AS (
  SELECT timestamp, NULL AS "col1", NULL AS "col2" FROM {} WHERE false
  UNION ALL
  SELECT * FROM {}
)
```
**Problem**: UNION ALL requires matching schemas - can't match * with explicit columns.

### 3. SELECT * EXCLUDE
```sql
{}_final AS (SELECT *, NULL AS "col1", NULL AS "col2" FROM {})
-- Then in outer query, manually pick columns
```
**Status**: User rejected EXCLUDE for timeseries_pivot (said "you should not need an EXCLUDE for the timeseries pivot").

### 4. Schema Introspection
Query each table's schema to determine which columns exist, generate different SQL per table.
**Status**: User rejected as too complex ("I want the easiest solution").

## Conclusion

The COALESCE with NULL-padding strategy is **not viable with DataFusion** due to its inability to reference non-existent columns in SQL, even after a FULL OUTER JOIN.

Without schema introspection or using EXCLUDE, there is no way to:
1. Generate SQL that references columns by name in a COALESCE
2. Handle tables where those columns don't exist
3. Produce a uniform output schema with all configured columns

This is a **dead end** with the current DataFusion version and the constraint against using EXCLUDE or schema introspection.

## Current Code State

File: `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/timeseries_pivot.rs`

The `generate_pivot_sql()` function generates:
- `{}_nulls` CTE with timestamp + all configured columns as NULL
- `{}_final` CTE with FULL OUTER JOIN + COALESCE
- Main query that SELECTs from `{}_final` tables

This compiles but fails at runtime with schema errors when tables lack configured columns.

## Next Steps Required

Decision needed on:
1. Allow `SELECT * EXCLUDE` in timeseries_pivot implementation
2. Implement schema introspection despite complexity
3. Accept returning only columns that exist (no NULL padding)
4. Wait for DataFusion update that handles non-existent columns gracefully
5. Different SQL pattern not yet identified
