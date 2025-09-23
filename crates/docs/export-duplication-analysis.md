# Export Duplication Issue: Root Cause Analysis

**Date**: September 22, 2025  
**Issue**: Export command creating 2 files per temporal partition instead of 1  
**Status**: Root cause identified, architectural fix needed

## Problem Statement

When running the export command with temporal partitioning:
```bash
pond export --pattern '/test-locations/**/res=1d.series' --dir /tmp/pond-export --temporal "year,month"
```

The system creates **2 Parquet files per temporal partition** instead of the expected 1 file:
```
/tmp/pond-export/BDockDownsampled/year=2024/month=7/s1aLlaTjH6Ieg1DH.parquet
/tmp/pond-export/BDockDownsampled/year=2024/month=7/5ExQ611uH0Mg1Zeq.parquet
```

This pattern appears consistently across all partitions, suggesting the same logical data is being processed twice with different identifiers.

## Investigation History

### Initial Hypothesis: DataFusion COPY Behavior
- **Suspected**: DataFusion's COPY command was incorrectly sharding data
- **Investigation**: Added debug logging to export command to trace DataFusion execution
- **Result**: DataFusion is working correctly; the issue is upstream in pattern matching

### Pattern Matching Analysis
- **Discovery**: TinyFS visitor pattern processes files based on NodeID uniqueness
- **Added Logging**: Debug output in `discover_export_targets()` and `ExportTargetVisitor::visit()`
- **Observation**: Same logical file appears to be processed multiple times with different NodeIDs

### Dynamic File System Investigation
- **Key Finding**: The export pattern `/test-locations/**/res=1d.series` matches files in a dynamic directory
- **Configuration**: `/test-locations/` is a dynamic directory defined by `test-hydrovu-dynamic-config.yaml`
- **Architecture**: Dynamic directories use factory system to create SQL-derived and temporal-reduce nodes

## Root Cause Analysis

### The Dynamic Directory Configuration

The issue stems from this configuration in `test-hydrovu-dynamic-config.yaml`:

```yaml
entries:
  - name: "BDock"
    factory: "sql-derived-series"
    # ... config ...

  - name: "BDockDownsampled"
    factory: "temporal-reduce"
    config:
      source: "/test-locations/BDock"  # References the BDock entry above
      # ... temporal reduction config ...
```

### The Problem: Multiple Dynamic Directory Instances

**Debug Log Evidence**:
```
[2025-09-23T06:43:04Z INFO  tlogfs::dynamic_dir] DynamicDirDirectory::new - creating directory with 5 entries, parent NodeID: NodeID(Uuid([1, 153, 117, 79, 192, 12, 125, 183, ...]))
[2025-09-23T06:43:04Z INFO  tlogfs::dynamic_dir] DynamicDirDirectory::new - creating directory with 5 entries, parent NodeID: NodeID(Uuid([1, 153, 117, 79, 243, 222, 120, 193, ...]))
[2025-09-23T06:43:18Z INFO  tlogfs::dynamic_dir] DynamicDirDirectory::new - creating directory with 5 entries, parent NodeID: NodeID(Uuid([1, 153, 117, 79, 243, 253, 113, 226, ...]))
```

**Analysis**: The same dynamic directory configuration is being instantiated **9 times** during export, each with a different parent NodeID.

### Factory System Architecture Issue

**Current Behavior**:
1. Export pattern matching requests `/test-locations/BDockDownsampled/`
2. TinyFS resolves this path, which triggers the dynamic directory factory
3. Factory calls `create_dynamic_dir_handle_with_context()` 
4. **Every call creates a NEW `DynamicDirDirectory` instance**
5. Each instance calls `tinyfs::NodeID::generate()` for child nodes
6. Same logical content gets different NodeIDs across instances

**Code Location**: `crates/tlogfs/src/dynamic_dir.rs:244-248`
```rust
fn create_dynamic_dir_handle_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle> {
    let config: DynamicDirConfig = serde_json::from_value(config)?;
    let dynamic_dir = DynamicDirDirectory::new(config, context.clone()); // ← NEW INSTANCE EVERY TIME
    Ok(dynamic_dir.create_handle())
}
```

## Architecture Analysis

### Expected Behavior (Within Transaction)
Within a single transaction, the same path should always resolve to the same NodeID:
- `/test-locations/BDockDownsampled/` → **Same NodeID**
- `/test-locations/BDockDownsampled/res=1d.series` → **Same NodeID**

### Current Broken Behavior
Multiple requests for the same path create fresh instances:
- Request 1: `/test-locations/BDockDownsampled/` → NodeID_A
- Request 2: `/test-locations/BDockDownsampled/` → NodeID_B ← **WRONG**

### TinyFS Deduplication Logic
TinyFS visitor pattern correctly deduplicates by NodeID:
```rust
if !visited_nodes.insert(node_id) {
    continue; // Skip already processed node
}
```

**This works correctly** - the issue is that the same logical node gets different NodeIDs due to multiple dynamic directory instances.

## Hierarchical Responsibility

### Parent Directory Contract
The parent directory mounting `/test-locations/` should:
1. **Assign stable NodeID** to the dynamic directory on first access
2. **Cache and reuse** the same NodeRef for subsequent accesses
3. **Maintain transaction-scoped consistency**

### Dynamic Directory Contract  
Each dynamic directory instance should:
1. **Cache child NodeRefs** in `entry_cache` (✅ Already implemented)
2. **Use consistent NodeIDs** for same entries within the instance (✅ Already implemented)

### The Missing Link
**The parent directory is not caching the dynamic directory NodeRef**. Each access to `/test-locations/` creates a new dynamic directory instance instead of reusing the cached one.

## Solution Architecture

### Option 1: Factory-Level Caching (REJECTED - Too Complex)
- Cache directory instances by configuration hash
- Added complex NodeID derivation from content
- **Problem**: Over-engineered, doesn't address core hierarchy issue

### Option 2: Parent Directory Responsibility (CORRECT)
- **Insight**: The parent directory should maintain stable NodeIDs for its children
- **Implementation**: Fix the parent directory's child caching behavior
- **Scope**: Minimal, architectural sound

### Option 3: Transaction-Scoped Directory Cache (ALTERNATIVE)
- Cache `(config, NodeRef)` pairs in FactoryContext
- Same configuration within transaction returns same NodeRef
- **Benefit**: Simple, transaction-isolated

## Technical Implementation Details

### Current Entry Caching (Works Within Instance)
Each `DynamicDirDirectory` has:
```rust
entry_cache: tokio::sync::RwLock<HashMap<String, NodeRef>>
```

This correctly caches `BDock` → NodeRef mappings **within a single directory instance**.

### The Real Issue: Multiple Instances
The problem is **not** within directory instances, but that we create **multiple instances** of the same directory.

### Directory Mounting Question
**Key Unknown**: Where and how is `/test-locations/` mounted in the filesystem hierarchy?
- Is it in the root directory?
- Is it another dynamic directory?
- How is the `test-hydrovu-dynamic-config.yaml` configuration loaded and mounted?

## Failed Approaches Attempted

### 1. Complex NodeID Caching
- Added `dynamic_node_cache` to FactoryContext
- Mapped `(parent_node_id, entry_name)` → `child_node_id`
- **Result**: Still created multiple parent instances with different NodeIDs

### 2. Content-Based NodeID Generation  
- Added `NodeID::from_content()` using SHA-256 hashing
- Attempted to derive deterministic NodeIDs from configuration
- **Result**: Over-engineered solution to wrong problem

### 3. Factory Registry Caching
- Attempted to cache directory handles in factory system
- **Result**: Architectural complexity without addressing root cause

## Current Status

### Code State
- ✅ Reverted complex caching implementations
- ✅ Identified root cause in factory system
- ✅ Confirmed TinyFS deduplication logic is correct
- ❌ Dynamic directory factory still creates multiple instances

### Debug Capabilities
- ✅ Comprehensive debug logging in export command
- ✅ Dynamic directory creation logging
- ✅ Large output debugging technique documented and used
- ✅ Pattern matching visitor logging

### Next Steps Required
1. **Identify parent directory**: Where is `/test-locations/` mounted?
2. **Fix parent caching**: Ensure parent directory maintains stable NodeIDs
3. **Verify transaction isolation**: Same path → same NodeID within transaction
4. **Test export deduplication**: Confirm only 1 file per partition

## Architecture Compliance

### DuckPond Principles Alignment
- ✅ **Fail-Fast Philosophy**: Issue identified rather than masked
- ✅ **Transaction Isolation**: Problem is transaction-scoped consistency
- ✅ **Type Safety**: TinyFS abstractions working correctly
- ⚠️ **ACID Compliance**: Potential transaction consistency issue

### Anti-Pattern Avoidance
- ✅ **No Fallbacks**: Avoided masking symptoms with fallback logic
- ✅ **Explicit Errors**: Clear identification of root cause
- ✅ **Architectural Focus**: Addressing design issue, not symptoms

## Conclusion

The export duplication issue is **not a bug in DataFusion, TinyFS, or the export command**. It is an **architectural consistency issue** in the dynamic directory factory system where:

1. **Same configuration** creates **multiple instances** instead of **reusing cached instances**
2. **Multiple instances** have **different NodeIDs** for **same logical content**  
3. **TinyFS deduplication** correctly processes **each unique NodeID** separately
4. **Result**: Same logical data exported multiple times

The fix requires ensuring **factory-level or parent-level caching** so that the same directory configuration returns the same NodeRef within a transaction scope.