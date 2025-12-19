# Query Optimization Opportunities - TLogFS Persistence Layer

**Date**: November 20, 2025  
**Branch**: jmacd/fifteen  
**File**: `crates/tlogfs/src/persistence.rs`

## Executive Summary

Analysis of `query_records()` usage reveals 10+ functions fetching all historical versions from Delta Lake but only using the latest record. These inefficient queries create O(N) performance degradation where N = number of versions per node.

**Impact**: Core filesystem operations (stat, open, readlink, exists) are affected. A file with 50 versions requires fetching 50 records when only 1 is needed.

**Solution**: Implement `query_latest_record()` with `ORDER BY version DESC LIMIT 1` to achieve O(1) performance.

---

## Detailed Analysis

### Current SQL Query Pattern

```sql
SELECT * FROM delta_table 
WHERE part_id = '{}' AND node_id = '{}' 
ORDER BY timestamp DESC
```

**Problem**: No `LIMIT` clause - fetches ALL historical versions from Delta Lake.

---

## Functions with Inefficient Queries

### Category 1: Simple .first() Usage (Highest Priority)

These functions fetch all versions but immediately use only `.first()` - clear optimization targets.

#### 1. `load_node` (line 2478)
**Frequency**: Called on every file/directory access  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first() {
    node_factory::create_node_from_oplog_entry(record, ...)
}
```
**Impact**: Critical - this is the core node loading function used by all filesystem operations.

---

#### 2. `get_factory_for_node` (line 2540)
**Frequency**: Called when accessing dynamic directories/files  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first() {
    Ok(record.factory.clone())
}
```
**Impact**: High - dynamic nodes are commonly used for SQL-derived views and temporal reductions.

---

#### 3. `load_symlink_target` (line 2714)
**Frequency**: Called on every symlink access  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first() {
    let content = record.content.clone()...
}
```
**Impact**: Medium - symlinks less common but still core operation.

---

#### 4. `metadata` (line 2791)
**Frequency**: Called by stat() and similar operations  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first() {
    // Return metadata from record
}
```
**Impact**: High - metadata queries are very frequent.

---

#### 5. `metadata_u64` (line 2832)
**Frequency**: Called for timestamp/version metadata queries  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first() {
    match name {
        "timestamp" => Ok(Some(record.timestamp as u64)),
        "version" => Ok(Some(record.version as u64)),
        ...
    }
}
```
**Impact**: Medium - specific metadata field queries.

---

#### 6. `get_dynamic_node_config` (line 2268)
**Frequency**: Called when reading dynamic node configurations  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
if let Some(record) = records.first()
    && let Some(factory_type) = &record.factory
    && let Some(config_content) = &record.content
{
    Ok(Some((factory_type.clone(), config_content.clone())))
}
```
**Impact**: Medium - used by factory-based nodes.

---

### Category 2: Existence Checks

#### 7. `node_exists` (line 2685)
**Frequency**: Called by file system exists checks  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
Ok(!records.is_empty())
```
**Waste**: Fetches all records just to check if ANY exist.  
**Better Approach**: `SELECT 1 FROM ... LIMIT 1` or reuse `query_latest_record()`.  
**Impact**: Medium - exists checks are common.

---

### Category 3: Aggregation in Memory

#### 8. `get_next_version_for_node` (line 1419)
**Frequency**: Called before every write operation  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
let max_version = records.iter().map(|r| r.version).max()...
let next_version = max_version + 1;
```
**Alternatives**:
- Use `ORDER BY version DESC LIMIT 1` and increment
- Use SQL `SELECT MAX(version) FROM ...` (but that's a separate query type)

**Impact**: High - every write operation needs version increment.

---

### Category 4: Multiple Queries

#### 9. `update_dynamic_node_config` (line 2300)
**Frequency**: When updating dynamic node configurations  
**Current Code**:
```rust
// First query via get_dynamic_node_config
let existing_config = self.get_dynamic_node_config(node_id, part_id).await?;

// Second query - fetches same data again!
let records = self.query_records(part_id, node_id).await?;
let current_version = records.first().map(|r| r.version).unwrap_or(0);
```
**Waste**: Queries same data twice in succession.  
**Impact**: Medium - configuration updates are less frequent.

---

### Category 5: Conditional First Match

#### 10. `async_file_reader` (line 1611)
**Frequency**: Called when opening files for reading  
**Current Code**:
```rust
let records = self.query_records(part_id, node_id).await?;
// Find the latest record with actual content (skip empty temporal override versions)
let record = records.iter().find(|r| r.size.unwrap_or(0) > 0)...
```
**Complexity**: Needs first non-empty version (temporal overrides may be 0-byte).  
**Impact**: High - file reading is a core operation.  
**Note**: More complex case - may need special handling beyond simple LIMIT 1.

---

## Legitimate Use Cases (No Optimization Needed)

### `list_file_versions` (line 2908)
**Purpose**: List all historical versions of a file  
**Current Code**:
```rust
let mut records = self.query_records(part_id, node_id).await?;
records.sort_by_key(|record| record.timestamp);
// Return all versions with metadata
```
**Correct**: Legitimately needs all versions.

---

### `get_temporal_bounds_for_file_series` (line 1007)
**Purpose**: Get time range for temporal queries  
**Current Code**:
```rust
// Custom SQL query for FileSeries records
let file_series_records = ...filter to FileSeries only...;
let latest_version = file_series_records.iter().max_by_key(|r| r.version)?;
```
**Note**: Already uses custom SQL, but could still benefit from LIMIT 1 on the FileSeries filter.

---

## Proposed Solution

### Implementation Plan

1. **Create `query_latest_record()` function** (similar to existing `query_latest_directory_record`)
   ```rust
   async fn query_latest_record(
       &self,
       part_id: NodeID,
       node_id: NodeID,
   ) -> Result<Option<OplogEntry>, TLogFSError>
   ```

2. **SQL Query with LIMIT 1**:
   ```sql
   SELECT * FROM delta_table 
   WHERE part_id = '{}' AND node_id = '{}' 
   ORDER BY version DESC 
   LIMIT 1
   ```

3. **Check pending records** (same pattern as `query_latest_directory_record`):
   - Query committed record with LIMIT 1
   - Check pending records in `self.records`
   - Return latest between committed and pending

4. **Replace calls** in the 10 functions listed above

---

## Performance Impact Estimation

### Current Performance (O(N))
For a file with N versions:
- Database query fetches N records
- Serialization of N records from Parquet
- Memory allocation for N records
- Iteration/filtering in memory
- Uses first record

### Optimized Performance (O(1))
- Database query with LIMIT 1 fetches 1 record
- Serialization of 1 record
- Minimal memory allocation
- No iteration needed

### Real-World Scenario
File with 50 versions:
- **Current**: Fetch 50 records (~100KB), then use first one
- **Optimized**: Fetch 1 record (~2KB)
- **Speedup**: ~50x reduction in data transfer and processing

### System-Wide Impact
- `load_node`: Called on EVERY file access → massive impact
- `metadata`: Called on EVERY stat operation → major impact
- `node_exists`: Called on EVERY existence check → significant impact

**Estimated Overall Performance Improvement**: 20-50x for filesystem metadata operations on files with many versions.

---

## Implementation Priority

### Phase 1 (Immediate - Highest ROI)
1. `load_node` - affects all operations
2. `metadata` - affects all stat calls
3. `get_factory_for_node` - affects dynamic nodes

### Phase 2 (High Priority)
4. `node_exists` - affects existence checks
5. `get_next_version_for_node` - affects all writes
6. `load_symlink_target` - affects symlink operations

### Phase 3 (Medium Priority)
7. `metadata_u64` - specific metadata queries
8. `get_dynamic_node_config` - dynamic node configs
9. `update_dynamic_node_config` - config updates (also fix double query)

### Phase 4 (Lower Priority but Easy)
10. `async_file_reader` - more complex case (needs first non-empty)

---

## Testing Strategy

1. **Unit Tests**: Verify `query_latest_record()` returns correct record
2. **Regression Tests**: Existing tests should pass unchanged
3. **Performance Tests**: Measure query time improvement on multi-version files
4. **Integration Tests**: Verify pending records are correctly handled

---

## Related Work

This optimization complements the recently completed **Directory Storage Redesign** which:
- Changed directory storage from incremental deltas to full snapshots
- Implemented `query_latest_directory_record()` with LIMIT 1
- Achieved O(1) directory reads instead of O(N)

The same pattern applies to regular file/node queries.

---

## Risks and Mitigation

### Risk: Pending Records Not Checked
**Mitigation**: Follow the pattern from `query_latest_directory_record` - check both committed and pending records.

### Risk: Version vs Timestamp Ordering
**Mitigation**: Use `ORDER BY version DESC` instead of `ORDER BY timestamp DESC` for strict monotonic ordering.

### Risk: Breaking Change
**Mitigation**: Keep `query_records()` unchanged for functions that need all versions. Add new function `query_latest_record()`.

---

## Conclusion

The optimization opportunity is clear and the implementation pattern is proven (already working for directories). This will provide significant performance improvements for all filesystem metadata operations, especially on files with many versions.

**Recommendation**: Implement `query_latest_record()` and migrate functions in priority order, starting with `load_node`, `metadata`, and `get_factory_for_node`.
