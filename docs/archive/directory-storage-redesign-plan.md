# Directory Storage Redesign Plan

## 🔴 CRITICAL PERFORMANCE PROBLEM

**The current directory implementation reads EVERY historical version on every access.**

### Real-World Impact

**Example**: A directory modified in 50 transactions:
```
Transaction 1: Create directory with 10 files
Transaction 2: Add 5 more files  
Transaction 3: Delete 2 files
Transaction 4: Add 3 files
...
Transaction 50: Rename 1 file

Current read cost: Deserialize 50 Arrow IPC batches + apply 50 sets of operations + deduplicate
```

**Performance degradation**:
- Small directory (10 files): Fast initially, but slows linearly with transaction count
- After 10 transactions: 10x slower than initial
- After 50 transactions: 50x slower than initial
- After 100 transactions: 100x slower than initial

**This is fundamentally broken** - directory read performance degrades with transaction history length, not directory size.

### Code Evidence

```rust
// From crates/tlogfs/src/persistence.rs:1905
async fn query_directory_entries(&self, part_id: NodeID) -> Result<Vec<VersionedDirectoryEntry>> {
    let records = self.query_records(part_id, part_id).await?;
    
    let mut all_entries = Vec::new();
    for record in records {  // ❌ ITERATES THROUGH ALL VERSIONS
        if record.file_type.is_directory() && let Some(content) = &record.content {
            let dir_entries = self.deserialize_directory_entries(content)?;
            all_entries.extend(dir_entries);  // ❌ ACCUMULATES ALL OPERATIONS
        }
    }
    
    // ❌ MUST DEDUPLICATE ACROSS ALL VERSIONS
    let mut seen_names = HashSet::new();
    let mut deduplicated_entries = Vec::new();
    
    for entry in all_entries {
        if !seen_names.contains(&entry.name) {
            seen_names.insert(entry.name.clone());
            if matches!(entry.operation_type, OperationType::Insert | OperationType::Update) {
                deduplicated_entries.push(entry);
            }
        }
    }
    
    Ok(deduplicated_entries)
}
```

**Every directory read:**
1. Queries ALL versions from Delta Lake
2. Deserializes ALL versions (Arrow IPC decode for each)
3. Extends into single vector (memory allocation for each version)
4. Deduplicates across ALL operations
5. Filters by operation type

**This is O(N) where N = number of transactions, not directory size.**

## Current Architecture Analysis

### Current Implementation

**Problem**: Directories use incremental append-only versioning which requires reading all historical versions:

1. **Storage Pattern**: Each transaction writes a delta of directory changes (Insert/Delete operations)
   - Version 1: `[Insert("file1"), Insert("file2")]`
   - Version 2: `[Insert("file3"), Delete("file1")]`
   - Version 3: `[Insert("file4")]`

2. **Reading Pattern**: Must read ALL versions and apply operations sequentially:
   ```rust
   async fn query_directory_entries() {
       let records = self.query_records(part_id, part_id).await?;
       
       // Iterate through ALL versions
       for record in records {
           let dir_entries = self.deserialize_directory_entries(content)?;
           all_entries.extend(dir_entries);
       }
       
       // Deduplicate by name, keeping latest operation
       // Must process every version to get final state
   }
   ```

3. **Writing Pattern**: Only writes incremental changes
   ```rust
   async fn flush_directory_operations() {
       for (entry_name, operation) in operations {
           match operation {
               InsertWithType(node_id, entry_type) => {
                   versioned_entries.push(VersionedDirectoryEntry::new(
                       entry_name, Some(node_id), OperationType::Insert, entry_type
                   ));
               }
               DeleteWithType(entry_type) => {
                   versioned_entries.push(VersionedDirectoryEntry::new(
                       entry_name, None, OperationType::Delete, entry_type
                   ));
               }
           }
       }
   }
   ```

### Current Schema Structures

**OpLogEntry** (in Delta Lake):
```rust
pub struct OplogEntry {
    pub part_id: String,           // Partition key
    pub node_id: String,           // For directories: node_id == part_id
    pub file_type: EntryType,      // DirectoryPhysical or DirectoryDynamic
    pub timestamp: i64,
    pub version: i64,
    pub content: Option<Vec<u8>>,  // Arrow IPC encoded VersionedDirectoryEntry[]
    pub txn_seq: i64,
    // ... other fields for files
}
```

**VersionedDirectoryEntry** (in content field):
```rust
pub struct VersionedDirectoryEntry {
    pub name: String,              // Entry name
    pub child_node_id: String,     // Child node ID (hex string)
    pub operation_type: OperationType, // Insert, Delete, Update
    pub entry_type: EntryType,     // File type (physical/dynamic distinction)
}

pub enum OperationType {
    Insert,
    Delete,
    Update,
}
```

### Performance Characteristics

**Current System**:
- ✅ **Space efficient** (in theory): Only stores deltas
- ❌ **Read inefficient**: Must read N versions for directory with N modifications
- ❌ **Complexity**: Deduplication logic, operation ordering, tombstone handling
- ❌ **Poor access pattern**: Random access requires full scan

**Example**: Directory with 100 files created over 50 transactions:
- Storage: 50 small Arrow IPC records (good)
- Read cost: Deserialize 50 records, apply operations, deduplicate (bad)

## Proposed Architecture

### Key Changes

1. **Full-state snapshots**: Store complete directory state in each version
2. **Add format field**: Track storage format in OpLogEntry for future extensibility
3. **Remove operation_type**: No longer needed with full-state storage
4. **Add version_last_modified**: Track which version last touched each entry

### New Schema

**OpLogEntry** (enhanced):
```rust
pub struct OplogEntry {
    pub part_id: String,
    pub node_id: String,
    pub file_type: EntryType,
    pub timestamp: i64,
    pub version: i64,
    pub content: Option<Vec<u8>>,
    
    // NEW FIELD - Required, non-nullable
    pub format: StorageFormat,  // "inline", "fulldir", (future: "hashdir", "b-tree")
    
    pub txn_seq: i64,
    // ... other fields unchanged
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum StorageFormat {
    #[serde(rename = "inline")]
    Inline,      // Small files, symlinks, dynamic nodes (content inline)
    
    #[serde(rename = "fulldir")]
    FullDir,     // Physical directories (full snapshot)
    
    // Future formats:
    // #[serde(rename = "hashdir")]
    // HashDir,  // Hash-based directory structure for very large dirs
    
    // #[serde(rename = "btree")]
    // BTreeDir, // B-tree indexed directory for sorted access
}
```

**DirectoryEntry** (simplified from VersionedDirectoryEntry):
```rust
// RENAMED: VersionedDirectoryEntry -> DirectoryEntry
pub struct DirectoryEntry {
    pub name: String,              // Entry name
    pub child_node_id: String,     // Child node ID (hex string)
    pub entry_type: EntryType,     // File type (physical/dynamic distinction)
    pub version_last_modified: i64, // NEW: Track which version last touched this entry
}

// REMOVED: OperationType enum - no longer needed
```

### New Access Patterns

**Reading** (single version) - 🚀 **MAJOR PERFORMANCE WIN**:
```rust
async fn query_directory_entries(part_id: NodeID) -> Result<Vec<DirectoryEntry>> {
    let records = self.query_records(part_id, part_id).await?;
    
    // ✅ Get ONLY the latest version (format == "fulldir")
    let latest_record = records.first().ok_or(NotFound)?;
    
    if latest_record.format != StorageFormat::FullDir {
        return Err(InvalidFormat);
    }
    
    // ✅ Single deserialize - complete directory state
    let entries = self.deserialize_directory_entries(&latest_record.content)?;
    
    // ✅ No deduplication needed - already complete state
    // ✅ No iteration through history
    // ✅ No operation application logic
    // ✅ Constant time regardless of transaction count
    
    Ok(entries)
}
```

**Performance comparison**:
- **Old**: O(T) where T = number of transactions touching directory
- **New**: O(1) - reads only latest version
- **Speedup**: 50x for directory with 50 transaction history

**Writing** (full snapshot):
```rust
async fn flush_directory_operations() {
    for (part_id, operations) in pending_dirs {
        // 1. Load current directory state (if exists)
        let mut current_entries = match self.load_directory_state(part_id).await {
            Ok(entries) => entries.into_iter()
                .map(|e| (e.name.clone(), e))
                .collect::<HashMap<_, _>>(),
            Err(_) => HashMap::new(), // New directory
        };
        
        // 2. Apply pending operations to build new complete state
        for (entry_name, operation) in operations {
            match operation {
                InsertWithType(child_node_id, entry_type) => {
                    current_entries.insert(entry_name, DirectoryEntry {
                        name: entry_name.clone(),
                        child_node_id: child_node_id.to_string(),
                        entry_type,
                        version_last_modified: self.get_next_version(part_id).await?,
                    });
                }
                DeleteWithType(_) => {
                    current_entries.remove(&entry_name);
                }
                RenameWithType(new_name, child_node_id, entry_type) => {
                    current_entries.remove(&entry_name);
                    current_entries.insert(new_name.clone(), DirectoryEntry {
                        name: new_name,
                        child_node_id: child_node_id.to_string(),
                        entry_type,
                        version_last_modified: self.get_next_version(part_id).await?,
                    });
                }
            }
        }
        
        // 3. Serialize complete directory state
        let all_entries: Vec<DirectoryEntry> = current_entries.into_values().collect();
        let content_bytes = self.serialize_directory_entries(&all_entries)?;
        
        // 4. Create OpLogEntry with format = "fulldir"
        let record = OplogEntry {
            part_id: part_id.to_string(),
            node_id: part_id.to_string(),
            file_type: EntryType::DirectoryPhysical,
            timestamp: now,
            version: next_version,
            content: Some(content_bytes),
            format: StorageFormat::FullDir,  // NEW
            txn_seq: self.txn_seq,
            // ... other fields
        };
        
        self.records.push(record);
    }
}
```

### Trade-offs Analysis

**New System**:
- ✅ **Read efficient**: O(1) version reads (latest only) - **THE PRIMARY GOAL**
- ✅ **Predictable performance**: Read cost independent of transaction history
- ✅ **Simple logic**: No deduplication, no operation ordering, no iteration
- ✅ **Better caching**: Can cache complete directory state efficiently
- ✅ **Fast lookups**: Random access without full scan
- ❌ **More write data**: Must write entire directory state each time
- ❌ **Larger storage**: Full snapshots vs deltas

**Critical Insight**: The current system is optimized for write efficiency but penalizes reads exponentially. Directory reads are **far more common** than writes in typical workloads (10:1 or higher ratio).

**Storage Impact Example**:
- Directory with 100 files, modified 50 times
- **Old**: 50 small records (1-5 entries each) = ~10 KB total
- **New**: 50 full snapshots (100 entries each) = ~500 KB total
- **Trade-off**: 50x storage cost, but **50x faster reads + constant-time performance**

**Performance Impact Example** (what we actually care about):
- Directory with 100 files, modified 50 times, accessed 500 times
- **Old**: 500 reads × 50 versions = 25,000 deserializations + deduplication operations
- **New**: 500 reads × 1 version = 500 deserializations (no deduplication)
- **Net win**: 50x faster reads overwhelms storage cost

**Reality Check**: 
- Typical pond directories: 10-100 entries
- Typical modification frequency: 1-10 transactions
- Typical read:write ratio: 10:1 or higher
- Storage cost for 100-entry directory × 10 versions: ~50 KB (negligible)
- **The current O(N) read cost is killing performance for NO practical benefit**

## Implementation Plan

### Phase 1: Schema Updates ✓ (No breaking changes yet)

**Step 1.1**: Add `StorageFormat` enum to `schema.rs`
```rust
// In crates/tlogfs/src/schema.rs

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum StorageFormat {
    #[serde(rename = "inline")]
    Inline,
    
    #[serde(rename = "fulldir")]
    FullDir,
}

impl Default for StorageFormat {
    fn default() -> Self {
        Self::Inline
    }
}
```

**Step 1.2**: Add `format` field to `OplogEntry`
```rust
// In crates/tlogfs/src/schema.rs

pub struct OplogEntry {
    // ... existing fields ...
    pub format: StorageFormat,  // NEW - non-nullable, always set
    pub txn_seq: i64,
}
```

**Step 1.3**: Update `ForArrow` implementation
```rust
impl ForArrow for OplogEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            // ... existing fields ...
            Arc::new(Field::new("format", DataType::Utf8, false)), // NEW - required field
            Arc::new(Field::new("txn_seq", DataType::Int64, false)),
        ]
    }
}
```

**Step 1.4**: Update all `OplogEntry` constructors
```rust
// Update new_small_file, new_large_file, new_inline, etc.
pub fn new_inline(...) -> Self {
    Self {
        // ... existing fields ...
        format: StorageFormat::Inline,  // Small files, symlinks, dynamic nodes
        txn_seq,
    }
}

pub fn new_directory(...) -> Self {
    Self {
        // ... existing fields ...
        format: StorageFormat::FullDir,  // Physical directories
        txn_seq,
    }
}
```

**Step 1.5**: Rename `VersionedDirectoryEntry` to `DirectoryEntry`
```rust
// Rename struct
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub child_node_id: String,
    pub entry_type: EntryType,
    pub version_last_modified: i64,  // NEW - track modification version
}

// Add migration support for old name
#[deprecated(note = "Use DirectoryEntry instead")]
pub type VersionedDirectoryEntry = DirectoryEntry;
```

**Step 1.6**: Remove `operation_type` field and `OperationType` enum
```rust
// Remove from DirectoryEntry
// pub operation_type: OperationType,  // REMOVED

// Remove enum
// pub enum OperationType { ... }  // REMOVED
```

**Step 1.7**: Update `ForArrow` for `DirectoryEntry`
```rust
impl ForArrow for DirectoryEntry {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("child_node_id", DataType::Utf8, false)),
            Arc::new(Field::new("entry_type", DataType::Utf8, false)),
            Arc::new(Field::new("version_last_modified", DataType::Int64, false)), // NEW
        ]
    }
}
```

### Phase 2: Persistence Layer Updates

**Step 2.1**: Update `serialize_directory_entries` / `deserialize_directory_entries`
- No changes needed - they already use Arrow IPC
- But rename references from `VersionedDirectoryEntry` to `DirectoryEntry`

**Step 2.2**: Implement `load_directory_state` helper
```rust
// In crates/tlogfs/src/persistence.rs (InnerState)

/// Load current directory state (latest version only)
async fn load_directory_state(
    &self,
    part_id: NodeID,
) -> Result<Vec<DirectoryEntry>, TLogFSError> {
    let records = self.query_records(part_id, part_id).await?;
    
    // Get latest version with format == "fulldir"
    let latest = records
        .iter()
        .find(|r| r.format == StorageFormat::FullDir)
        .ok_or_else(|| TLogFSError::NodeNotFound { 
            path: PathBuf::from(format!("Directory {}", part_id))
        })?;
    
    if let Some(content) = &latest.content {
        self.deserialize_directory_entries(content)
    } else {
        Ok(Vec::new())
    }
}
```

**Step 2.3**: Update `flush_directory_operations` to write full snapshots
```rust
async fn flush_directory_operations(&mut self) -> Result<(), TLogFSError> {
    // ... existing setup ...
    
    for (part_id, operations) in pending_dirs {
        // 1. Load current state (or empty map for new directories)
        let mut current_state: HashMap<String, DirectoryEntry> = 
            match self.load_directory_state(part_id).await {
                Ok(entries) => entries.into_iter()
                    .map(|e| (e.name.clone(), e))
                    .collect(),
                Err(_) => HashMap::new(),
            };
        
        // 2. Apply pending operations
        let next_version = self.get_next_version_for_node(part_id, part_id).await?;
        
        for (entry_name, operation) in operations {
            match operation {
                DirectoryOperation::InsertWithType(child_node_id, entry_type) => {
                    current_state.insert(entry_name.clone(), DirectoryEntry {
                        name: entry_name,
                        child_node_id: child_node_id.to_string(),
                        entry_type,
                        version_last_modified: next_version,
                    });
                }
                DirectoryOperation::DeleteWithType(_) => {
                    current_state.remove(&entry_name);
                }
                DirectoryOperation::RenameWithType(new_name, child_node_id, entry_type) => {
                    current_state.remove(&entry_name);
                    current_state.insert(new_name.clone(), DirectoryEntry {
                        name: new_name,
                        child_node_id: child_node_id.to_string(),
                        entry_type,
                        version_last_modified: next_version,
                    });
                }
            }
        }
        
        // 3. Serialize complete state
        let all_entries: Vec<DirectoryEntry> = current_state.into_values().collect();
        let content_bytes = self.serialize_directory_entries(&all_entries)?;
        
        // 4. Create full snapshot OplogEntry
        let record = OplogEntry::new_directory_full_snapshot(
            part_id,
            Utc::now().timestamp_micros(),
            next_version,
            content_bytes,
            self.txn_seq,
        );
        
        self.records.push(record);
    }
    
    // ... handle empty directories ...
    Ok(())
}
```

**Step 2.4**: Add new constructor `OplogEntry::new_directory_full_snapshot`
```rust
// In crates/tlogfs/src/schema.rs

impl OplogEntry {
    /// Create entry for directory full snapshot (new storage format)
    #[must_use]
    pub fn new_directory_full_snapshot(
        part_id: NodeID,
        timestamp: i64,
        version: i64,
        content: Vec<u8>,  // Serialized DirectoryEntry[]
        txn_seq: i64,
    ) -> Self {
        Self {
            part_id: part_id.to_string(),
            node_id: part_id.to_string(),  // For dirs: node_id == part_id
            file_type: EntryType::DirectoryPhysical,
            timestamp,
            version,
            content: Some(content),
            sha256: None,
            size: None,
            min_event_time: None,
            max_event_time: None,
            min_override: None,
            max_override: None,
            extended_attributes: None,
            factory: None,
            format: StorageFormat::FullDir,  // Full directory snapshot
            txn_seq,
        }
    }
}
```

**Step 2.5**: Update `query_directory_entries` to **ONLY READ LATEST VERSION** 🚀
```rust
async fn query_directory_entries(
    &self,
    part_id: NodeID,
) -> Result<Vec<DirectoryEntry>, TLogFSError> {
    let records = self.query_records(part_id, part_id).await?;
    
    // 🚀 CRITICAL FIX: Only need the latest version with full snapshots
    // OLD CODE iterated through ALL records - this is the core improvement
    let latest_record = records
        .iter()
        .filter(|r| r.file_type.is_directory())
        .max_by_key(|r| r.version)
        .ok_or_else(|| TLogFSError::NodeNotFound {
            path: PathBuf::from(format!("Directory {}", part_id))
        })?;
    
    // Validate format
    if latest_record.format != StorageFormat::FullDir {
        return Err(TLogFSError::ArrowMessage(format!(
            "Expected fulldir format, got {:?}",
            latest_record.format
        )));
    }
    
    // ✅ Single deserialize - complete directory state
    // ✅ No iteration through history
    // ✅ No deduplication across versions
    // ✅ No operation type filtering
    if let Some(content) = &latest_record.content {
        self.deserialize_directory_entries(content)
    } else {
        Ok(Vec::new())
    }
}
```

**Step 2.6**: Update `query_single_directory_entry` similarly
```rust
async fn query_single_directory_entry(
    &self,
    part_id: NodeID,
    entry_name: &str,
) -> Result<Option<DirectoryEntry>, TLogFSError> {
    // Check pending operations first (unchanged)
    if let Some(operations) = self.operations.get(&part_id) {
        // ... existing pending check logic ...
    }
    
    // Load complete directory state
    let entries = self.query_directory_entries(part_id).await?;
    
    // Simple lookup - no deduplication needed
    Ok(entries.into_iter().find(|e| e.name == entry_name))
}
```

### Phase 3: Update References

**Step 3.1**: Update all type aliases and imports
```rust
// Replace all instances of VersionedDirectoryEntry with DirectoryEntry
// Search: "VersionedDirectoryEntry"
// Replace: "DirectoryEntry"

// Files to update:
// - crates/tlogfs/src/schema.rs
// - crates/tlogfs/src/persistence.rs
// - crates/tlogfs/src/directory.rs
// - crates/tlogfs/src/query/operations.rs
```

**Step 3.2**: Update DirectoryTable query implementation
```rust
// In crates/tlogfs/src/query/operations.rs

// Update parse_directory_content to handle new schema
async fn parse_directory_content(&self, content: &[u8]) -> Result<Vec<DirectoryEntry>> {
    // ... Arrow IPC deserialization (same as before) ...
    // Now returns DirectoryEntry instead of VersionedDirectoryEntry
}
```

**Step 3.3**: Update directory operation builders
```rust
// In crates/tlogfs/src/directory.rs

// When creating DirectoryEntry instances, include version_last_modified
// This is handled in flush_directory_operations, so no changes needed here
```

### Phase 4: Testing & Validation

**Step 4.1**: Update unit tests
- Test full snapshot read/write
- Test directory modification (insert/delete/rename)
- Test empty directory handling
- Test format field validation

**Step 4.2**: Add integration tests
- Test directory with many entries (100+)
- Test directory modification over multiple transactions
- Test version_last_modified tracking
- Test backward compatibility (reading old format - FUTURE)

**Step 4.3**: Performance benchmarks
- Compare read performance: old vs new
- Measure storage overhead
- Test with varying directory sizes (10, 100, 1000 entries)

### Phase 5: Migration Strategy (FUTURE)

Since we're starting fresh with no backward compatibility requirements, this phase is for future reference if we need to migrate data:

**Option A: On-the-fly migration** (lazy)
- Keep old deserialization code for `operation_type` field
- When reading old format, convert to full snapshot on next write
- Gradual migration as directories are modified

**Option B: Batch migration** (eager)
- Write migration script that rewrites all directory entries
- Convert all directories to full snapshot format
- Remove old deserialization code

**Decision**: Not needed now (fresh start), but document for future.

## Success Criteria

### Primary Goal (Performance)
1. ✅ **All directory reads require ONLY latest version** - O(1) instead of O(N) where N = transaction count
2. ✅ **No deduplication logic** - eliminated entirely from read path
3. ✅ **No iteration through versions** - single record read + deserialize
4. ✅ **Read performance constant-time** - independent of transaction history length

### Secondary Goals (Correctness)
5. ✅ `format` field present in all new OpLogEntry records
6. ✅ All tests pass with new schema
7. ✅ Storage overhead acceptable (< 10x for typical directories < 1000 entries)

### Performance Metrics
- **Baseline**: Directory with 100 entries, 50 modification transactions
- **Target**: Read time independent of transaction count (50x improvement)
- **Acceptable**: Storage increase < 500 KB (50 snapshots × 100 entries)

## Open Questions

1. **Q**: What about very large directories (10,000+ entries)?
   **A**: Address later with `StorageFormat::HashDir` or `StorageFormat::BTreeDir`

2. **Q**: Should we compress directory content?
   **A**: Delta Lake already compresses Parquet files; Arrow IPC is already compact

3. **Q**: Do we need to support reading old format?
   **A**: No - explicitly stated "no backwards compatibility needed"

4. **Q**: What about directory versioning history?
   **A**: Still preserved - each version is a complete snapshot at that point in time

5. **Q**: How to optimize large directory writes?
   **A**: Future optimization - could delta-encode content at Arrow level, but start simple

## Related Documents

- `docs/duckpond-system-patterns.md` - Transaction patterns
- `docs/directory-entry-node-type-enhancement.md` - Previous directory enhancement
- `docs/fs_architecture_analysis.md` - Original directory versioning design

## Timeline Estimate

- Phase 1 (Schema): 1-2 hours
- Phase 2 (Persistence): 2-3 hours  
- Phase 3 (References): 1 hour
- Phase 4 (Testing): 2-3 hours
- **Total**: 6-9 hours

## Notes

- This is a **simplified architecture** that trades storage for performance
- The constants favor simplicity: directories are small, modifications are infrequent
- Future optimization paths are preserved via `format` field
- The version_last_modified field enables future optimizations (incremental sync, conflict detection)
