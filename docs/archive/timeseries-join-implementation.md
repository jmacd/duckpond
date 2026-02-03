# Timeseries Join Implementation

## Overview

`TimeseriesJoinFile` combines multiple time-series inputs into a single unified time series using DataFusion SQL. It handles:
- Multiple inputs with different schemas
- Optional scope prefixes to prevent column name collisions
- Time range filtering per input
- UNION BY NAME for same-scope inputs
- FULL OUTER JOIN for different-scope inputs

## Implementation Strategy

### Core Principles

1. **Scope is a column name prefix** - Applied by `ScopePrefixTableProvider` to prevent naming conflicts
2. **UNION BY NAME for same scope** - Inputs with the same scope value are combined using UNION BY NAME
3. **FULL OUTER JOIN for different scopes** - Scope groups are combined using FULL OUTER JOIN on timestamp
4. **No manual schema detection** - DataFusion and ScopePrefixTableProvider handle schemas automatically

### SQL Generation Algorithm

The `generate_union_join_sql()` function follows this process:

```rust
// 1. Build patterns map AND scope_prefixes map
for each input:
    patterns[format!("input{}", i)] = input.pattern
    if input.scope.is_some():
        scope_prefixes[format!("input{}", i)] = (input.scope, time_column)

// 2. Group inputs by scope
for each input:
    scope_key = input.scope.unwrap_or_else(|| format!("_none_{}", i))
    scope_groups[scope_key].push(i)

// 3. Create filtered CTEs (apply time range filters)
for each input:
    filtered{i} AS (
        SELECT * FROM input{i}  // input{i} is wrapped with ScopePrefixTableProvider!
        WHERE timestamp >= begin AND timestamp <= end
    )

// 4. Create combined scope CTEs (UNION BY NAME for multiple inputs)
for each scope_group:
    scope_combined{idx} AS (
        SELECT * FROM filtered{input0}  // Already has prefixed columns!
        UNION BY NAME
        SELECT * FROM filtered{input1}
        ...
    )

// 5. SELECT with FULL OUTER JOIN - let DataFusion expand * with prefixed columns
SELECT
  COALESCE(scope_combined0.timestamp, scope_combined1.timestamp, ...) AS timestamp,
  scope_combined0.* EXCLUDE (timestamp),  // Expands to prefixed columns automatically
  scope_combined1.* EXCLUDE (timestamp),
  ...
FROM scope_combined0
FULL OUTER JOIN scope_combined1 ON scope_combined0.timestamp = scope_combined1.timestamp
...
ORDER BY timestamp
```

### How ScopePrefixTableProvider Works

When DataFusion evaluates `SELECT * FROM input0`:

1. SqlDerivedConfig looks up `input0` in the patterns map
2. Finds it's registered with a scope prefix in the scope_prefixes map
3. Wraps the ListingTableProvider with `ScopePrefixTableProvider("Vulink", "timestamp")`
4. ScopePrefixTableProvider:
   - Gets the schema from the underlying ListingTableProvider
   - Transforms column names: `temp` → `Vulink.temp`, `conductivity` → `Vulink.conductivity`
   - Returns the modified schema to DataFusion
5. The CTE `filtered0` inherits this schema with prefixed columns
6. `scope_combined1` (UNION BY NAME) preserves the prefixed columns
7. The outer `SELECT scope_combined1.*` expands to the prefixed columns

## Example SQL Output

### Case 1: Same Scope, Multiple Inputs

Inputs:
- vulink1: timestamps [1,2,3], columns [temp, conductivity], scope=Vulink
- vulink2: timestamps [5,6,7], columns [temp, conductivity], scope=Vulink
- at500: timestamps [2,4,6], columns [pressure], scope=AT500_Surface

Generated SQL:
```sql
WITH
filtered0 AS (SELECT * FROM input0 WHERE timestamp <= '1970-01-01T00:00:03Z'),
filtered1 AS (SELECT * FROM input1 WHERE timestamp >= '1970-01-01T00:00:05Z'),
filtered2 AS (SELECT * FROM input2),
scope_combined0 AS (SELECT * FROM filtered2),
scope_combined1 AS (SELECT * FROM filtered0
UNION BY NAME
SELECT * FROM filtered1)
SELECT
  COALESCE(scope_combined0.timestamp, scope_combined1.timestamp) AS timestamp,
  scope_combined0.* EXCLUDE (timestamp),
  scope_combined1.* EXCLUDE (timestamp)
FROM scope_combined0
FULL OUTER JOIN scope_combined1 ON scope_combined0.timestamp = scope_combined1.timestamp
ORDER BY timestamp
```

Scope prefixes registered:
```rust
{
  "input0": ("Vulink", "timestamp"),
  "input1": ("Vulink", "timestamp"),
  "input2": ("AT500_Surface", "timestamp")
}
```

What happens:
- `SELECT * FROM input0` resolves to columns: `Vulink.timestamp`, `Vulink.temp`, `Vulink.conductivity`
- `SELECT * FROM input1` resolves to columns: `Vulink.timestamp`, `Vulink.temp`, `Vulink.conductivity`
- `SELECT * FROM input2` resolves to columns: `AT500_Surface.timestamp`, `AT500_Surface.pressure`
- UNION BY NAME combines filtered0 and filtered1 (same prefixed schema)
- Final SELECT expands `scope_combined0.*` → `AT500_Surface.pressure`
- Final SELECT expands `scope_combined1.*` → `Vulink.temp`, `Vulink.conductivity`

Result:
- 7 unique timestamps: [1, 2, 3, 4, 5, 6, 7]
- 4 columns: timestamp, AT500_Surface.pressure, Vulink.temp, Vulink.conductivity
- No duplicate timestamps

### Case 2: Different Scopes, Single Input Each

Inputs:
- temp_a: timestamps [1,2,3], columns [temp_a], scope=None
- temp_b: timestamps [2,3,4], columns [temp_b], scope=None

Generated SQL:
```sql
WITH
filtered0 AS (SELECT * FROM input0),
filtered1 AS (SELECT * FROM input1),
scope_combined0 AS (SELECT * FROM filtered0),
scope_combined1 AS (SELECT * FROM filtered1)
SELECT
  COALESCE(scope_combined0.timestamp, scope_combined1.timestamp) AS timestamp,
  scope_combined0.* EXCLUDE (timestamp),
  scope_combined1.* EXCLUDE (timestamp)
FROM scope_combined0
FULL OUTER JOIN scope_combined1 ON scope_combined0.timestamp = scope_combined1.timestamp
ORDER BY timestamp
```

Scope prefixes registered:
```rust
{} // Empty - no scopes for these inputs
```

Result:
- 4 unique timestamps: [1, 2, 3, 4]
- 3 columns: timestamp, temp_a, temp_b
- No scope prefixes (scope=None for both)

## Why This Approach Works

**Key insight:** We register scope prefixes on the **input pattern names** (`input0`, `input1`, etc.), not on the CTE names.

When DataFusion evaluates the query:
1. CTEs reference `input0`, `input1`, etc. via `SELECT * FROM inputN`
2. SqlDerivedConfig looks up `inputN` and finds it wrapped with ScopePrefixTableProvider
3. The wrapped provider returns a schema with prefixed columns
4. CTEs (`filtered0`, `scope_combined1`) inherit the prefixed columns
5. The final SELECT with `scope_combined1.*` expands to those prefixed columns

**No schema detection needed** - DataFusion's lazy schema resolution does all the work:
- ListingTableProvider reads schemas from Parquet files
- ScopePrefixTableProvider transforms the schemas
- CTEs propagate the transformed schemas
- `SELECT *` expands to the actual columns at execution time

## Why Not Apply Scope Prefixes to CTEs?

Initially attempted to apply scope prefixes to CTE names (`scope_combined0`, `scope_combined1`), but this doesn't work because:

1. CTEs are **subqueries** in the logical plan, not external table references
2. ScopePrefixTableProvider wraps **registered table providers** in the catalog
3. DataFusion never looks up CTEs in the catalog - they're resolved during parsing

The solution: Apply scope prefixes to the **source tables** (`inputN`) that the CTEs reference. The prefixed columns flow through the CTEs automatically.

## Test Coverage

All tests passing:
- ✅ `test_validation_errors` - validates configuration
- ✅ `test_timeseries_join_factory_integration` - no scopes, different columns
- ✅ `test_timeseries_join_with_scope_prefixes` - different scopes, single input each
- ✅ `test_timeseries_join_same_scope_non_overlapping_ranges` - same scope, multiple inputs

## Key Lessons

1. **Apply scope prefixes to source tables, not CTEs** - Register scope_prefixes on `inputN` pattern names, not on CTE names
2. **Let DataFusion do schema resolution** - No manual schema detection needed; ListingTableProvider + ScopePrefixTableProvider handle it
3. **UNION BY NAME preserves all rows** - Perfect for combining same-schema inputs with different timestamps
4. **FULL OUTER JOIN on first table** - JOIN scope_combined1 ON scope_combined0.timestamp = scope_combined1.timestamp
5. **Scope groups by scope value** - Inputs with scope=None get unique groups (format!("_none_{}", i))
6. **`SELECT *` expands at execution time** - No need to enumerate columns; DataFusion expands based on actual schema

## Common Pitfalls Avoided

❌ **Don't manually prefix in SQL** - Let ScopePrefixTableProvider do it:
```sql
-- WRONG: Manual prefixing
SELECT scope_combined1.temp AS "Vulink.temp"

-- RIGHT: Let provider handle it
SELECT scope_combined1.* EXCLUDE (timestamp)
```

❌ **Don't try to wrap CTEs** - Wrap the source tables instead:
```rust
// WRONG: Try to register scope_combined0 with prefix
scope_prefixes.insert("scope_combined0", ...)

// RIGHT: Register the source table
scope_prefixes.insert("input0", ...)
```

❌ **Don't manually detect schemas** - Let DataFusion's lazy resolution do it at query execution time
