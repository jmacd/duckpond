# TLogFS Directory State Management Redesign

## Problem Statement

The current directory state management in TLogFS creates duplicate OpLog records per transaction due to an architectural mismatch between the old incremental model and the new full snapshot model.

### Current Broken Flow
1. `create_dir_node()` calls `store_node()` which writes an empty directory version to `self.records`
2. Parent directory's `insert()` calls `update_directory_content()` which writes a populated directory version to `self.records`
3. Result: **2 OpLog records per directory per transaction** (empty + populated)
4. Expected: **1 OpLog record per directory per transaction** (final snapshot only)

### Root Cause: Dead Code from Old Architecture
- `has_pending_operations()` checks `self.operations` HashMap which is **never populated**
- `self.operations` is architectural debt from the old incremental model
- New snapshot model uses full directory state encoded as Arrow IPC
- Timing issue: when `store_node()` checks `has_pending_operations()`, the subsequent `insert()` hasn't written yet

### Test Impact
- `test_multiple_series_appends_directory_updates` expects 4 directory records, finds 6
- Creates: root, /devices, /devices/sensor_123, /devices/sensor_123/data.series
- Expected records:
  1. root v1 (bootstrap with devices)
  2. root v2 (with devices after create)
  3. devices v1 (with sensor_123)
  4. sensor_123 v1 (with data.series)
- Actual records add: devices v1 empty, sensor_123 v1 empty from `store_node()`

## Current State (76/85 tests passing)

### Fixed Issues
✅ SQL syntax error in Display traits (brackets in UUIDs) - 51→55 tests
✅ Implemented `load_nodes_batched` with in-memory structure checks
✅ Fixed template factory registration - 55→58 tests  
✅ Implemented `OpLogDirectory::get()` and `insert()` with Arrow IPC encoding/decoding - 58→65 tests
✅ Added `FileID::from_physical_dir_node_id()` for self-partitioned directories
✅ Fixed version numbering bug (`enumerate()` vs `record.version`) - 65→76 tests

### Remaining Issues
❌ 1 test blocked by directory state architecture (this redesign)
❌ 8 tests with sql_derived pattern matching issues (separate issue)

## Proposed Architecture

### Design Principle (from user guidance)
> "When we load the latest version of the directory, at that point we have full state in memory; we need to maintain the current state and write it again if it's modified, as a new version, one version per transaction containing the final outcome of the transaction"

### In-Memory State Management

Add to `State` struct:
```rust
/// In-memory directory state during transaction
struct DirectoryState {
    /// Whether this directory has been modified during this transaction
    modified: bool,
    /// Map of child name -> DirectoryEntry for O(1) lookups and duplicate detection
    mapping: HashMap<String, DirectoryEntry>,
}

/// Maps FileID (directory) -> DirectoryState (loaded/modified state)
directories: HashMap<FileID, DirectoryState>,
```

### Modified Operations Flow

#### 1. Loading Directory State
When `OpLogDirectory::get()` is called:
```rust
// Check if already loaded in memory
if let Some(dir_state) = state.directories.get(&dir_id) {
    let entries: Vec<DirectoryEntry> = dir_state.mapping.values().cloned().collect();
    return Ok(Some(entries));
}

// Load from OpLog if not in memory
let entries = load_from_oplog(...);
let mapping: HashMap<String, DirectoryEntry> = entries
    .into_iter()
    .map(|entry| (entry.name.clone(), entry))
    .collect();

// Cache in memory for this transaction (not yet modified)
state.directories.insert(dir_id, DirectoryState {
    modified: false,
    mapping,
});

Ok(Some(state.directories.get(&dir_id).unwrap().mapping.values().cloned().collect()))
```

#### 2. Modifying Directory State
When `OpLogDirectory::insert()` is called:
```rust
// Ensure directory is loaded (calls get() if not present)
if !state.directories.contains_key(&dir_id) {
    self.get(state, &dir_id).await?;
}

// Get mutable reference to directory state
let dir_state = state.directories.get_mut(&dir_id)
    .ok_or_else(|| Error::Internal("Directory should be loaded"))?;

// Check for duplicate entry (O(1) lookup)
if dir_state.mapping.contains_key(&entry.name) {
    return Err(Error::EntryExists(entry.name.clone()));
}

// Insert new entry and mark as modified
dir_state.mapping.insert(entry.name.clone(), entry);
dir_state.modified = true;

// NO immediate write to self.records!
```

#### 3. Creating Empty Directories
When `store_node()` is called for a directory:
```rust
// Check if directory already loaded in memory
if state.directories.contains_key(&file_id) {
    return Ok(()); // Skip - already being tracked
}

// Initialize empty directory in memory (not yet modified)
state.directories.insert(file_id, DirectoryState {
    modified: false,
    mapping: HashMap::new(),
});
```

#### 4. Commit-Time Flush
Add new method called at transaction commit:
```rust
impl State {
    async fn flush_directory_operations(&mut self) -> Result<()> {
        for (dir_id, dir_state) in &self.directories {
            // Only write directories that were modified
            if !dir_state.modified {
                continue;
            }
            
            // Convert HashMap back to Vec for encoding
            let entries: Vec<DirectoryEntry> = dir_state.mapping.values().cloned().collect();
            
            // Encode full snapshot
            let content = encode_directory_entries(&entries)?;
            
            // Write ONE OpLog record with final state
            self.records.push(OpLogRecord {
                node_id: dir_id.node_id(),
                part_id: dir_id.part_id(),
                version: self.next_version_for(dir_id),
                operation: OpLogOperation::UpdateDirectory { content },
                timestamp: Utc::now(),
            });
        }
        
        // Clear transient state
        self.directories.clear();
        
        Ok(())
    }
}
```

#### 5. Updated `has_pending_operations()`
```rust
fn has_pending_operations(&self, file_id: &FileID) -> bool {
    self.directories.get(file_id)
        .map(|dir_state| dir_state.modified)
        .unwrap_or(false)
}

// Alternative: check if directory is tracked at all (loaded or modified)
fn is_directory_loaded(&self, file_id: &FileID) -> bool {
    self.directories.contains_key(file_id)
}
```

### Call Sites to Modify

1. **crates/tlogfs/src/persistence.rs**
   - Line 1114-1117: Update `has_pending_operations()` to check `modified_directories`
   - Line 2702-2722: Remove `update_directory_content()` or make it update in-memory state only
   - Line 2614-2628: Update `store_node()` to initialize empty dirs in memory
   - Add `flush_directory_operations()` method
   - Line ~450-460: Call `flush_directory_operations()` before commit

2. **crates/tlogfs/src/directory.rs**
   - Line 71-117: Update `get()` to check in-memory state first
   - Line 119-159: Update `insert()` to modify in-memory state, mark as modified

3. **State struct initialization**
   - Initialize `pending_directories: HashMap::new()`
   - Initialize `modified_directories: HashSet::new()`

## Expected Outcomes

### Test Results
- `test_multiple_series_appends_directory_updates` should pass with exactly 4 directory records
- No duplicate empty directory versions
- One directory version per transaction with final snapshot state

### Performance Benefits
- Fewer OpLog records (1 per directory vs 2)
- Smaller Delta Lake tables
- Faster queries (fewer versions to scan)

### Architectural Clarity
- Clear separation: in-memory mutation during transaction, batch write at commit
- Eliminates dead code (`self.operations` map)
- Aligns with full snapshot model introduced in commit af023caa4fa8

## Implementation Steps

1. **Add DirectoryState struct and field to State**
   - Define `DirectoryState { modified: bool, mapping: HashMap<String, DirectoryEntry> }`
   - Add `directories: HashMap<FileID, DirectoryState>` to State struct

2. **Implement in-memory caching in `get()`**
   - Check if `directories.contains_key(dir_id)` first
   - If present, return entries from mapping as Vec
   - If not present, load from OpLog, convert to HashMap, insert with `modified: false`

3. **Modify `insert()` to update in-memory state**
   - Ensure directory loaded (call get() if not in `directories`)
   - Get mutable reference to DirectoryState
   - Check `mapping.contains_key(entry.name)` for O(1) duplicate detection
   - Insert into mapping, set `modified: true`
   - Remove call to `update_directory_content()`

4. **Update `store_node()` for empty directories**
   - Check if `directories.contains_key(file_id)`, skip if present
   - Initialize empty DirectoryState with `modified: false, mapping: HashMap::new()`

5. **Implement `flush_directory_operations()`**
   - Iterate `directories`, filter by `modified: true`
   - Convert mapping values to Vec<DirectoryEntry>
   - Encode and write final snapshots to self.records
   - Clear entire `directories` HashMap

6. **Update `has_pending_operations()`**
   - Check `directories.get(file_id).map(|ds| ds.modified).unwrap_or(false)`

7. **Integrate flush at commit time**
   - Call `flush_directory_operations()` before writing records to Delta Lake

8. **Remove or deprecate `update_directory_content()`**
   - Dead code after this redesign

9. **Test and validate**
   - Run `test_multiple_series_appends_directory_updates` - should find exactly 4 records
   - Run full test suite
   - Verify OpLog record counts match expectations

## Migration Notes

This is a **breaking change** to internal transaction semantics but maintains external API compatibility. All directory operations will continue to work the same from the user's perspective, but the internal timing of OpLog writes changes from immediate to commit-time.

### Rollback Safety
If transaction fails or is aborted:
- In-memory state (`pending_directories`, `modified_directories`) is discarded
- No OpLog records written until successful commit
- Existing behavior preserved

### Concurrency Considerations
- Each transaction has its own `State` instance
- No shared mutable state between transactions
- Full isolation maintained

## References

- FileID refactoring commit: (current branch jmacd/fifteen)
- Directory snapshot model introduced: commit af023caa4fa8
- Conversation context: docs/ai-quick-start.md, docs/duckpond-system-patterns.md
- Test file: crates/tlogfs/src/tests.rs line 1006
