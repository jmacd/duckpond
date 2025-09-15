# NodeVersionTable Migration Investigation Summary

## Investigation Overview
Investigation into replacing NodeVersionTable with enhanced TemporalFilteredListingTable to eliminate code duplication in TLogFS table providers.

## Key Architecture Discoveries

### 1. ObjectStore Path Handling is Critical
**The Challenge:** The primary complexity in replacing NodeVersionTable lies in proper ObjectStore path handling and registration patterns.

**Key Insight:** We only need **one ObjectStore instance** per DataFusion SessionContext, but each table provider must ensure proper file series registration.

**Before (Problematic):**  
- Multiple ObjectStore instances created per table provider
- Each with separate registries causing "File series not found in registry" errors
- Complex shared ObjectStore management attempts

**After (Simplified):**
- Each table provider creates its own ObjectStore via `create_listing_table_provider_with_options`
- DataFusion ListingTable handles ObjectStore lifecycle internally
- No manual ObjectStore sharing required

### 2. Timestamp Column Projection Requirements
**Critical Discovery:** When temporal range overrides are present, the `timestamp` column MUST be projected and available for filtering.

**The Issue:** TemporalFilteredListingTable applies temporal bounds by filtering on the timestamp column, but if the timestamp column is not included in the query projection, the filtering fails silently.

**Implications:**
- Queries must include timestamp column for temporal filtering to work
- Empty projections or column-specific projections without timestamp bypass temporal bounds
- This explains why some temporal overrides appeared to be ignored

### 3. VersionSelection Architecture Success
**Implemented Solution:**
```rust
pub enum VersionSelection {
    AllVersions,           // Replaces NodeVersionTable's multi-version capability  
    LatestVersion,         // For single latest version access
    SpecificVersion(u64),  // For specific version access
}
```

**Benefits:**
- Single code path handles all version selection scenarios
- Eliminates NodeVersionTable code duplication
- Clear API for version-specific table access

## NodeVersionTable Replacement Strategy

### What NodeVersionTable Did
- Created table providers for specific versions of FileSeries files
- Applied temporal bounds overrides from metadata
- Used only in `temporal.rs` detect-overlaps command
- Created individual table providers per version for UNION queries

### Replacement Approach
**Wrong Path (Initially Attempted):** Create shared ObjectStore and multiple specific version providers
**Right Path (Final Solution):** Use VersionSelection::AllVersions with single table provider per file series

```rust
// OLD: Multiple table providers per version
for version_info in versions {
    let table_provider = create_table_provider_for_version(version);
    // Register each version as separate table
}

// NEW: Single table provider handles all versions
let table_provider = create_listing_table_provider_with_options(
    node_id, part_id, state, ctx, VersionSelection::AllVersions
);
// Table provider internally handles UNION of all versions
```

## Implementation Learnings

### 1. DataFusion Schema Inference Dependencies
- Schema inference requires exact file path matching with .parquet extensions
- Empty files (size == 0) cause schema inference failures
- URL pattern construction must match ObjectStore file registration exactly

### 2. Temporal Override Metadata Handling
- Temporal overrides stored in latest version metadata
- Must be retrieved using node_id (not version-specific lookup)
- Applied at TemporalFilteredListingTable wrapper level

### 3. UNION Query Pattern
- detect-overlaps creates UNION ALL queries across multiple file series
- Each file series becomes one table provider with AllVersions selection
- Origin tracking maintained through synthetic column injection

## Architectural Philosophy Validation

### Fallback Anti-Pattern Avoided
Following the project's fallback anti-pattern philosophy, we avoided:
- Silent fallbacks when ObjectStore registration fails
- Default empty results when schema inference fails  
- Fallback table providers when temporal bounds can't be applied

**Instead:** Fail fast with clear error messages, forcing proper ObjectStore and schema setup.

### Single Responsibility Maintained
- Each table provider has clear version selection responsibility
- ObjectStore management handled by DataFusion internally
- Temporal filtering applied consistently via wrapper pattern

## Current Status

### ✅ Completed
1. **VersionSelection enum implementation** - Clean API for version control
2. **Enhanced create_listing_table_provider_with_options** - Unified table provider creation
3. **temporal.rs migration** - Uses TemporalFilteredListingTable instead of NodeVersionTable
4. **ObjectStore architecture simplification** - Removed complex shared ObjectStore attempts
5. **Schema inference fixes** - Proper .parquet extension handling

### 🔍 Key Insights for Future Development
1. **Always project timestamp column** when temporal bounds are expected
2. **One ObjectStore per SessionContext** is sufficient - don't over-engineer sharing
3. **Schema inference is fragile** - ensure exact URL pattern matching
4. **VersionSelection::AllVersions is usually preferred** over multiple specific version providers
5. **File series registration must happen before query execution** - no lazy loading

### 📋 Remaining Work
1. **Test detect-overlaps command** - Verify UNION query approach works correctly
2. **Remove NodeVersionTable code** - Complete the deduplication by deleting unused implementation

## Recommendations for Similar Migrations

### 1. Start with Path Analysis
- Map all URL patterns used by existing implementations
- Understand ObjectStore registration requirements
- Identify schema inference dependencies

### 2. Simplify Before Enhancing  
- Remove complex shared resource management
- Let DataFusion handle ObjectStore lifecycle
- Focus on clean API boundaries

### 3. Test Query Projection Requirements
- Verify timestamp column availability for temporal filtering
- Test both explicit and implicit column projections
- Validate schema inference with various file states

### 4. Follow Fail-Fast Philosophy
- Avoid silent fallbacks that mask configuration errors
- Provide clear error messages for common setup issues
- Force proper resource initialization

This investigation validates that table provider consolidation is achievable through thoughtful API design and careful attention to DataFusion's ObjectStore and schema inference requirements.