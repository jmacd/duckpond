# Temporal Filtering Debug Log - Circular Problem Analysis

**Date:** September 11, 2025  
**Issue:** Temporal filtering not working despite `set-temporal-bounds` command  
**Symptom:** Query returns 1317 rows instead of filtered subset within 2024-01-01 to 2024-05-30 range

## The Circular Pattern We're Stuck In

### Layer 1: DataFusion Integration Issues ✅ RESOLVED
**Problem:** TemporalFilteredListingTable wrapper causing schema mismatches

**Specific Errors Encountered:**
1. `"No field named timestamp"` - FilterExec couldn't find timestamp column
2. `"Base plan has no schema fields"` - ListingTable.scan() returning 0-field schema  
3. `"Different number of fields: (physical) 5 vs (logical) 0"` - Schema mismatch between logical and physical plans

**Root Cause Discovery:**
- Initial assumption: SessionContext/ObjectStore registration inconsistency
- Actual cause: **Incomplete TableProvider implementation**
- Missing methods: `constraints()` and `supports_filters_pushdown()`

**DataFusion Architecture Confusion:**
DataFusion's TableProvider trait requires multiple methods for proper integration:
- `schema()` - Returns logical schema (was working)
- `scan()` - Creates physical execution plan (was working)  
- `constraints()` - Returns table constraints (was missing - caused logical planner issues)
- `supports_filters_pushdown()` - Tells optimizer about filter capabilities (was missing)

The "0 field logical schema" error occurred because DataFusion's logical planner couldn't properly analyze the table without complete TableProvider implementation, even though the physical scan worked fine.

**Resolution:**
Added proper delegation in TemporalFilteredListingTable:
```rust
fn constraints(&self) -> Option<&Constraints> {
    self.listing_table.constraints()
}

fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
    self.listing_table.supports_filters_pushdown(filters)
}
```

### Layer 2: Temporal Override Storage/Retrieval 🔄 UNVERIFIED
**Problem:** `get_temporal_overrides_for_node_id()` returns None despite `set-temporal-bounds` command

**Debug Evidence:**
```
23:35:35.124 debug tlogfs file_table Found temporal overrides for node 01992751-d4c0-7579-abaf-b4d33bcce96d: 1704067200000 to 1717113599000
```
Wait - this shows overrides ARE found! But later logs show:
```  
23:35:35.124 debug tlogfs file_table No temporal overrides found for node 01992751-d4c0-7579-abaf-b4d33bcce96d
```

**Contradiction Analysis:**
This suggests a **race condition** or **multiple lookups with different results**. The same node_id returns different results in different calls. This could be:
1. Transaction isolation issues
2. Multiple versions of the same node with different temporal override data
3. Timing-dependent database query results
4. Bug in `get_temporal_overrides_for_node_id()` logic

**Test Script Expectation vs Reality:**
Test script clearly shows:
```bash
cargo run --bin pond set-temporal-bounds /hydrovu/devices/6582334615060480/SilverVulink1.series \
  --min-time "2024-01-01 00:00:00" \
  --max-time "2024-05-30 23:59:59"
```

But debug logs suggest the temporal bounds either:
- Are not being stored by `set-temporal-bounds` command
- Are being stored but not retrieved consistently  
- Are being retrieved but with wrong node_id matching

### Layer 3: Temporal Filtering Logic Implementation 🔄 INCOMPLETE  
**Problem:** FilterExec implementation has compilation errors

**Specific Compilation Errors:**
```rust
error[E0425]: cannot find function `col` in this scope
error[E0425]: cannot find function `lit` in this scope  
error[E0433]: failed to resolve: use of undeclared type `ScalarValue`
error[E0308]: mismatched types - expected `&DFSchema`, found `&Arc<Schema>`
```

**Missing Imports:**
```rust
use datafusion::logical_expr::{col, lit};
use datafusion::common::ScalarValue;
```

**DataFusion API Confusion:**
- `create_physical_expr()` expects `&DFSchema` but `ExecutionPlan.schema()` returns `&Arc<Schema>`
- Need to convert between DataFusion's logical schema (`DFSchema`) and Arrow schema (`Schema`)
- FilterExec requires physical expressions, not logical expressions

**Temporal Filtering Logic Status:**
```rust
// This code exists but has compilation errors:
let timestamp_col = col("timestamp");
let min_timestamp = lit(ScalarValue::TimestampSecond(Some(min_seconds), Some(Arc::new(Utc))));
let max_timestamp = lit(ScalarValue::TimestampSecond(Some(max_seconds), Some(Arc::new(Utc))));

let combined_filter = timestamp_col.gt_eq(lit(min_timestamp)).and(timestamp_col.lt_eq(lit(max_timestamp)));
let physical_filter = create_physical_expr(&combined_filter, &base_plan.schema(), &ExecutionProps::new())?;
```

## The Debugging Circles

### Circle 1: Fix Filtering → Hit Schema Errors → Debug DataFusion
1. Try to implement temporal filtering with FilterExec
2. Get "No field named timestamp" error
3. Debug DataFusion schema inference
4. Discover ObjectStore/SessionContext issues
5. Fix TableProvider implementation  
6. Get filtering working... but no actual filtering happens

### Circle 2: Test Full Flow → Compilation Errors → Fix Imports → Schema Errors
1. Run test script to verify end-to-end flow
2. Hit compilation errors in temporal filtering code
3. Start fixing imports (`col`, `lit`, `ScalarValue`)
4. Hit schema mismatch errors (`DFSchema` vs `Schema`)
5. Go back to debugging DataFusion integration
6. Lose track of whether temporal overrides are actually stored

### Circle 3: Assume Components Work → Test Integration → Find Gaps
1. Assume `set-temporal-bounds` works correctly
2. Assume `get_temporal_overrides_for_node_id()` works correctly  
3. Focus on DataFusion filtering implementation
4. Discover debug logs show "No temporal overrides found"
5. Question whether storage/retrieval works
6. But then see contradictory logs showing overrides ARE found
7. Confusion about which layer is actually broken

## Root Cause Analysis

### The Real Problem
We're debugging **3 independent systems simultaneously**:
1. **Temporal override persistence** (TLogFS metadata storage)
2. **DataFusion integration** (TableProvider, schema inference, FilterExec)  
3. **Temporal filtering logic** (timestamp predicate creation, physical expression conversion)

### The Confusion Sources

**DataFusion Schema Confusion:**
- Arrow Schema vs DFSchema vs logical vs physical schemas
- TableProvider methods have subtle interdependencies  
- SessionContext object store registration timing issues
- FilterExec requires physical expressions but we're creating logical expressions

**TLogFS Temporal Override Confusion:**
- Debug logs show contradictory results for same node_id
- Unclear whether `set-temporal-bounds` actually persists data
- `get_temporal_overrides_for_node_id()` may have transaction isolation issues
- Node ID vs Part ID confusion in lookups

**Integration Confusion:**  
- Multiple SessionContext instances with different ObjectStore registrations
- Timing of schema inference vs ObjectStore registration
- Lazy evaluation in DataFusion causing different behavior at different times

## Recommended Next Steps

### Step 1: Verify Temporal Override Persistence
**DO NOT** try to fix filtering logic until this is confirmed:

```bash  
# Test ONLY the storage layer
DUCKPOND_LOG=debug cargo run --bin pond set-temporal-bounds /hydrovu/devices/6582334615060480/SilverVulink1.series --min-time "2024-01-01 00:00:00" --max-time "2024-05-30 23:59:59"

# Then test ONLY the retrieval layer  
DUCKPOND_LOG=debug cargo run --bin pond detect-overlaps "/hydrovu/devices/**/SilverVulink*.series"
```

Look for:
- Does `set-temporal-bounds` actually write to database?
- Does subsequent query find the written temporal bounds?
- Are node_id lookups consistent?

### Step 2: Fix Temporal Filtering Compilation
**DO NOT** run full integration test until code compiles:

```rust
// Add missing imports
use datafusion::logical_expr::{col, lit};
use datafusion::common::ScalarValue;

// Fix DFSchema vs Schema conversion
let df_schema = DFSchema::try_from_qualified_schema("", &base_plan.schema())?;
let physical_filter = create_physical_expr(&combined_filter, &df_schema, &ExecutionProps::new())?;
```

### Step 3: Integration Testing
Only after Steps 1 and 2 are verified independently, test the full flow.

## Key Insights

1. **DataFusion integration is complex** - incomplete TableProvider implementation caused mysterious schema errors
2. **Temporal override persistence may be unreliable** - contradictory debug logs suggest race conditions or lookup bugs
3. **Three-layer debugging is unsustainable** - must verify each layer independently
4. **Lazy evaluation causes timing-dependent bugs** - DataFusion's lazy schema inference interacts poorly with custom ObjectStores
5. **Schema type mismatches are subtle** - Arrow Schema vs DFSchema caused hours of debugging

## Status
- ✅ **DataFusion Integration Layer**: Working (TemporalFilteredListingTable properly delegates to ListingTable)
- 🔄 **Temporal Override Layer**: Unverified (contradictory debug logs need investigation)  
- 🔄 **Filtering Logic Layer**: Incomplete (compilation errors need fixing)
