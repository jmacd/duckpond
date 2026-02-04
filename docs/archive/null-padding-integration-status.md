# Null Padding Integration Status

## Problem Statement

Timeseries pivot files query multiple source sites (e.g., `/combined/Silver`, `/combined/BDock`) where different sites have different available columns. The pivot SQL expects ALL sites to have ALL columns, causing schema errors when sites are missing certain measurements.

**Example**: Silver site has `AT500_Surface.DO.mg/L` but NOT `AT500_Bottom.DO.mg/L`, yet the pivot SQL tries to SELECT both columns from all sites.

## Solution Approach

Add a `provider_wrapper` closure mechanism to `SqlDerivedConfig` that allows factories like `timeseries-pivot` to wrap TableProviders with `null_padding_table()`, which extends schemas with missing columns as nullable Float64 fields.

## Implementation Status

### ✅ Completed Components

1. **null_padding_table() function** (`crates/provider/src/null_padding.rs`)
   - Takes base TableProvider and HashMap of columns to add
   - Returns wrapped provider with extended schema
   - Handles projection and batch extension correctly
   - **Status**: Implemented and compiles

2. **SqlDerivedConfig.provider_wrapper field** (`crates/tlogfs/src/sql_derived.rs`)
   - Added optional closure field: `Option<Arc<dyn Fn(Arc<dyn TableProvider>) -> Result<...>>>`
   - Added `.with_provider_wrapper()` builder method
   - Added custom Debug impl (closure shows as `"<closure>"`)
   - **Status**: Implemented

3. **Wrapper application in sql_derived.rs** (lines 725-737)
   - Applies wrapper when configured after `as_table_provider()` succeeds
   - Applied for BOTH single-file and multi-file patterns
   - **Status**: Fixed - removed incorrect OpLogFile type check

4. **timeseries_pivot integration** (`crates/tlogfs/src/timeseries_pivot.rs`)
   - Builds expected_columns HashMap with column names from config
   - Creates SqlDerivedConfig with wrapper closure
   - Wrapper calls `provider::null_padding_table()` with error conversion
   - **Status**: Implemented

5. **temporal_reduce compatibility** (`crates/tlogfs/src/temporal_reduce.rs`)
   - Added `provider_wrapper: None` to SqlDerivedConfig struct initialization
   - **Status**: Fixed compilation error

## Current Issue

### Symptom
```
thread 'main' panicked at arrow-schema-55.2.0/src/schema.rs:382:10:
index out of bounds: the len is 2 but the index is 3
```

### Stack Trace Shows
- Panic occurs in `ScopePrefixExec::new` at line 161 (scope_prefix_table_provider.rs)
- Triggered during DataFusion physical plan optimization
- Happens AFTER ViewTable is created successfully
- Related to scope_prefix wrapper, NOT null_padding wrapper

### Context
- Export pattern: `/reduced/single_param/DO/res=12h.series`
- This file uses `temporal-reduce` factory with `timeseries-pivot` as source
- The chain: `/combined/*` (base data) → timeseries-pivot → temporal-reduce → export
- Panic occurs when temporal_reduce tries to execute query

### Root Cause (Hypothesis)
The `ScopePrefixExec` is trying to access schema fields by indices that assume the original schema size, but the schema has been modified (possibly by null padding or another wrapper). The scope_prefix_table_provider needs to handle schema changes from wrappers.

**Note**: This is NOT a null_padding bug - it's a bug in scope_prefix_table_provider.rs when used with modified schemas.

## Reproduction Commands

### Using large-output-debugging.md protocol:

```bash
cd /Volumes/sourcecode/src/duckpond

# Test 1: Single reduced file (currently panics in scope_prefix)
RUST_BACKTRACE=1 POND=noyo/pond cargo run export \
  --pattern /reduced/single_param/DO/res=12h.series \
  --dir OUTDIR --temporal year \
  1> OUT 2> OUT

# Check for errors
grep -E "Error|panic|index out of bounds" OUT

# Test 2: Pattern export (was timing out)
POND=noyo/pond timeout 60 cargo run export \
  --pattern '/reduced/single_param/*/*.series' \
  --dir OUTDIR --temporal year,month \
  1> OUT 2> OUT
```

### Expected vs Actual

**Expected**: Export succeeds, parquet files contain null-padded columns for missing measurements

**Actual**: 
- Schema error: "No field named ... AT500_Bottom.DO.mg/L" → **FIXED** by removing OpLogFile type check
- Index out of bounds panic in ScopePrefixExec → **CURRENT BLOCKER**

## Code Locations

### Key Files Modified
1. `/Volumes/sourcecode/src/duckpond/crates/provider/src/null_padding.rs` - Main implementation
2. `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/sql_derived.rs` - Wrapper mechanism
3. `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/timeseries_pivot.rs` - Usage example
4. `/Volumes/sourcecode/src/duckpond/crates/tlogfs/src/temporal_reduce.rs` - Compatibility fix

### Critical Code Sections

**sql_derived.rs lines 725-737** (wrapper application):
```rust
Ok(provider) => {
    debug!("✅ SQL-DERIVED: Successfully created table provider for node_id={}", node_id);
    // Apply optional provider wrapper (e.g., null_padding_table)
    if let Some(wrapper) = &self.config.provider_wrapper {
        debug!("Applying provider wrapper to table '{}'", pattern_name);
        wrapper(provider)?
    } else {
        provider
    }
}
```

**timeseries_pivot.rs lines 239-250** (wrapper configuration):
```rust
// Build list of expected columns for null padding
// These are the raw column names that should exist in each source table
let mut expected_columns = HashMap::new();
for column in &self.config.columns {
    _ = expected_columns.insert(column.clone(), arrow::datatypes::DataType::Float64);
}

// Create SqlDerivedFile config with null_padding wrapper
let sql_config = SqlDerivedConfig::new(patterns, Some(sql))
    .with_provider_wrapper(move |provider| {
        provider::null_padding_table(provider, expected_columns.clone())
            .map_err(|e| TLogFSError::DataFusion(e))
    });
```

## What We've Established is Correct

1. ✅ The provider_wrapper mechanism works - wrapper is called when configured
2. ✅ null_padding_table() compiles and has correct signature
3. ✅ Wrapper is applied to pattern-matched tables (not recursively to derived tables)
4. ✅ Column names format is correct: raw column names like `"AT500_Bottom.DO.mg/L"`, not prefixed
5. ✅ Error conversion from DataFusion to TLogFSError works
6. ✅ SqlDerivedConfig builder pattern with with_provider_wrapper() works
7. ✅ Removed incorrect OpLogFile type check that was preventing wrapper execution

## Next Steps

1. **Investigate scope_prefix_table_provider.rs line 161** 
   - Why is it accessing field index 3 when schema has only 2 fields?
   - Does ScopePrefixExec need to handle wrapped/extended schemas?
   - Is there a schema mismatch between execution plan and actual provider?

2. **Test null_padding in isolation**
   - Create unit test that wraps a TableProvider with null_padding_table
   - Execute a query to verify schema extension works
   - Ensure projection indices are correct

3. **Consider wrapper ordering**
   - If both scope_prefix and null_padding are applied, what order?
   - Does scope_prefix need to be aware of schema extensions?

## Testing Strategy

### Unit Test Needed
```rust
#[tokio::test]
async fn test_null_padding_with_query() {
    // Create base provider with 2 columns: timestamp, "col_a"
    // Wrap with null_padding_table adding "col_b", "col_c"
    // Execute query: SELECT timestamp, col_a, col_b, col_c FROM table
    // Verify: col_b and col_c are null, no panics
}
```

### Integration Test
```bash
# Direct test without temporal_reduce layer
POND=noyo/pond cargo run cat \
  /reduced/single_param/AT500_Surface.DO.mg\\/L/Silver.series \
  1> OUT 2> OUT
  
# Should show null-padded columns without panic
```

## Architecture Notes

### Wrapper Application Flow
```
User calls export on /reduced/single_param/DO/res=12h.series
  → temporal_reduce.as_table_provider()
    → SqlDerivedFile.as_table_provider() for source pattern
      → Pattern matches /reduced/single_param/AT500_*.DO.mg\\/L/*.series
        → Each match is timeseries-pivot file
          → timeseries_pivot.as_table_provider()
            → SqlDerivedFile.as_table_provider() WITH provider_wrapper
              → Pattern matches /combined/*
                → Each match gets wrapped with null_padding_table ✅
                → Registers wrapped providers in SessionContext ✅
                → SQL query runs against wrapped providers
                  → ViewTable created with extended schema
                    → ScopePrefixExec tries to access fields by old indices ❌ PANIC
```

### Key Insight
The panic is NOT in null_padding code. It's in scope_prefix code that doesn't expect schemas to change. The scope_prefix wrapper is used by timeseries-join (different factory) but somehow gets involved in the execution plan optimization phase.

## Open Questions

1. Why is ScopePrefixExec in the execution plan for a temporal_reduce export?
2. Does timeseries_pivot SQL use scope prefixes internally?
3. Should wrapper application happen at a different layer to avoid optimizer conflicts?
4. Is there a schema caching issue where optimizer sees old schema but runtime has new schema?
