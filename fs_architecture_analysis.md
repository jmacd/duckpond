# TinyFS Architecture Analysis: Layered Design

## Current Architecture Issues

The current design has `nodes` and `restored_nodes` in the central `FS` struct, which creates architectural tension:

1. **Mixed Responsibilities**: FS handles both coordination AND storage
2. **Backend Complexity**: Backends need to coordinate with FS for node registration  
3. **Duplication**: Node information exists in both FS state and backend storage
4. **No Memory Control**: Unbounded node storage growth
5. **Missing Directory Mutations**: No support for versioned directory operations

## Recommended Architecture: Simplified Two-Layer Design

Given your clarifications about:
- Single backend in production (no runtime flexibility needed)  
- Directory versioning and mutation support with Delta Lake native deletion
- NodeID/PartID relationship (each node belongs to a containing directory)
- Preference for simplicity and clear separation
- Delta Lake's built-in time travel eliminates need for custom time travel APIs
- Caching can be added later once core architecture is solid

I recommend a **simplified two-layer architecture** that cleanly separates concerns while deferring caching complexity:

### Simplified Two-Layer Architecture

```rust
// Layer 1: Pure persistence (no caching, no node management)
trait PersistenceLayer: Send + Sync {
    async fn load_node(&self, node_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID) -> Result<bool>;
    
    // Directory operations with versioning (Delta Lake handles time travel)
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()>;
    
    async fn commit(&self) -> Result<()>;
}

// Layer 2: FS coordinator (no storage, no caching - pure coordination)
struct FS {
    persistence: Arc<dyn PersistenceLayer>,
    busy: Arc<Mutex<HashSet<NodeID>>>, // Only coordination state for loop detection
}

impl FS {
    async fn with_persistence_layer<P: PersistenceLayer + 'static>(
        persistence: P,
    ) -> Result<Self> {
        Ok(FS {
            persistence: Arc::new(persistence),
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }
    
    async fn get_node(&self, node_id: NodeID) -> Result<NodeRef> {
        // Load directly from persistence (no caching)
        let node_type = self.persistence.load_node(node_id).await?;
        let node = NodeRef::new(Arc::new(Mutex::new(Node { node_type, id: node_id })));
        Ok(node)
    }
    
    async fn create_node(&self, part_id: NodeID, node_type: NodeType) -> Result<NodeRef> {
        let node_id = NodeID::new_sequential();
        self.persistence.store_node(node_id, part_id, node_type.clone()).await?;
        
        let node = NodeRef::new(Arc::new(Mutex::new(Node { node_type, id: node_id })));
        Ok(node)
    }
}
```

### Layer Responsibilities

1. **PersistenceLayer (OpLogBackend)**: Pure Delta Lake operations, no caching, no node management
2. **FS**: Pure coordinator with minimal state (only `busy` for loop detection)

**Note**: Caching will be added as a future optimization once the core architecture is solid. Delta Lake's built-in time travel capabilities eliminate the need for custom time travel APIs.

### Benefits

1. **Clear Separation**: Each layer has one responsibility
2. **Simple FS**: No mixed responsibilities, minimal state
3. **No Duplication**: Each piece of state has a single home  
4. **Testable**: Each layer can be tested in isolation
5. **NodeID/PartID Support**: Each node correctly tracks its containing directory
6. **Delta Lake Native**: Leverages Delta Lake's built-in features instead of reimplementing them

This eliminates the duplication and mixed responsibilities while keeping the architecture as simple as possible. Caching can be added later as a transparent optimization.

### Migration Strategy

**Note**: The pseudocode assumes `NodeID::from_hex_string()` method exists. This needs to be added to the TinyFS `NodeID` implementation:

```rust
// crates/tinyfs/src/node.rs
impl NodeID {
    pub fn from_hex_string(hex: &str) -> Result<Self, std::num::ParseIntError> {
        let id = u64::from_str_radix(hex, 16)?;
        Ok(NodeID(id as usize))
    }
}
```

### Phase 1: Extract PersistenceLayer from OpLogBackend

```rust
// crates/tinyfs/src/persistence.rs
trait PersistenceLayer: Send + Sync {
    async fn load_node(&self, node_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID) -> Result<bool>;
    
    // Directory operations (no time travel - use Delta Lake's built-in features)
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()>;
    
    async fn commit(&self) -> Result<()>;
}

// Move OpLog logic to pure persistence
struct OpLogPersistence {
    store_path: String,
    session_ctx: SessionContext,
    pending_records: Arc<Mutex<Vec<Record>>>,
}

impl PersistenceLayer for OpLogPersistence {
    async fn load_node(&self, node_id: NodeID) -> Result<NodeType> {
        // Move current OpLogBackend::get_or_load_node logic here
        // Return just the NodeType, no NodeRef management
    }
}
```

### Phase 2: Update FS to use PersistenceLayer directly

```rust
// crates/tinyfs/src/fs.rs
struct FS {
    persistence: Arc<dyn PersistenceLayer>,
    busy: Arc<Mutex<HashSet<NodeID>>>,
}

impl FS {
    async fn with_persistence_layer<P: PersistenceLayer + 'static>(
        persistence: P,
    ) -> Result<Self> {
        Ok(FS {
            persistence: Arc::new(persistence),
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }
    
    // Clean, simple interface - no caching, no mixed responsibilities
    async fn get_node(&self, node_id: NodeID) -> Result<NodeRef> {
        let node_type = self.persistence.load_node(node_id).await?;
        let node = NodeRef::new(Arc::new(Mutex::new(Node { node_type, id: node_id })));
        Ok(node)
    }
}
```

### Phase 3: Future Caching Layer (Optional)

When performance becomes a concern, a caching layer can be added transparently:

```rust
// Future: crates/tinyfs/src/cache.rs
struct CacheLayer {
    persistence: Arc<dyn PersistenceLayer>,
    cache: Arc<Mutex<LruCache<NodeID, NodeRef>>>,
}

// FS can optionally use CacheLayer instead of PersistenceLayer directly
// All existing APIs remain unchanged
```

## Directory Versioning and Mutation Strategy

### Background: DuckPond Prototype Approach

The DuckPond prototype used an **append-only** model:
- Files were versioned with each update creating a new version
- Directories were immutable - only insertions allowed, no deletions/modifications  
- No APIs existed for modifying directory structure

### New Requirement: Mutable Directories

For the production system, we need to support:
- Directory entry deletion
- Directory entry renaming/moving
- Atomic directory operations
- Efficient cleanup of old entries using Delta Lake's native deletion

### Proposed Mutation Strategy: Tombstones with Delta Lake Cleanup

#### Core Approach: Append-Only with Periodic Cleanup

```rust
// Directory entries with lifecycle tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectoryEntry {
    name: String,
    child_node_id: String,
    operation_type: OperationType,
    timestamp: i64,
    version: i64,  // Monotonic version counter
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum OperationType {
    Insert,   // Add new entry
    Delete,   // Mark entry as deleted (tombstone)
}

#[derive(Debug, Clone)]
enum DirectoryOperation {
    Insert(NodeID),  // child_node_id to insert
    Delete,          // Mark entry as deleted
}

impl PersistenceLayer for OpLogPersistence {
    async fn update_directory_entry(
        &self, 
        parent_node_id: NodeID,  // This IS the part_id for directory entries
        entry_name: &str,
        operation: DirectoryOperation
    ) -> Result<()> {
        match operation {
            DirectoryOperation::Insert(child_node_id) => {
                let entry = DirectoryEntry {
                    name: entry_name.to_string(),
                    child_node_id: child_node_id.to_hex_string(),
                    operation_type: OperationType::Insert,
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
                    version: self.next_version().await?,
                };
                // Store with parent_node_id as part_id (directory entries belong to their containing directory)
                self.append_directory_record(parent_node_id, entry).await
            },
            
            DirectoryOperation::Delete => {
                // Create tombstone entry
                let tombstone = DirectoryEntry {
                    name: entry_name.to_string(),
                    child_node_id: String::new(), // Empty for tombstones
                    operation_type: OperationType::Delete,
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
                    version: self.next_version().await?,
                };
                // Store with parent_node_id as part_id
                self.append_directory_record(parent_node_id, tombstone).await
            },
        }
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        // Query all entries for this directory (parent_node_id is the part_id)
        let all_entries = self.query_directory_entries(parent_node_id).await?;
        
        // Apply operations in version order to get current state
        let mut current_state = HashMap::new();
        
        for entry in all_entries.into_iter().sorted_by_key(|e| e.version) {
            match entry.operation_type {
                OperationType::Insert => {
                    if let Ok(child_id) = NodeID::from_hex_string(&entry.child_node_id) {
                        current_state.insert(entry.name, child_id);
                    }
                },
                OperationType::Delete => {
                    current_state.remove(&entry.name);
                }
            }
        }
        
        Ok(current_state)
    }
}
```

#### Delta Lake Native Cleanup

Instead of permanent tombstones, use Delta Lake's `DELETE` operations for periodic cleanup:

See https://delta-io.github.io/delta-rs/python/api_reference.html#deltalake.table.DeltaTable.delete

### Part ID Usage Patterns

The `part_id` parameter is crucial for Delta Lake partitioning and represents the **containing directory** for each node:

1. **Regular Files**: `part_id` = parent directory's `node_id`
2. **Directories**: `part_id` = their own `node_id` (directories are self-contained partitions)  
3. **Symlinks**: `part_id` = parent directory's `node_id`
4. **Directory Entries**: `part_id` = parent directory's `node_id` (entries belong to their containing directory)
5. **Root Directory**: `part_id` = `node_id` = `"0000000000000000"` (special case)

This ensures proper Delta Lake partitioning where:
- Each directory becomes its own partition
- All nodes within a directory are co-located
- Directory entries are stored with their containing directory
- Efficient queries by directory (common filesystem access pattern)

### Benefits of Tombstone + Delta Lake Cleanup Approach

1. **Full History**: Complete audit trail of all directory changes
2. **Delta Lake Native**: Uses Delta Lake's built-in time travel instead of custom APIs
3. **Append-Only**: Leverages Delta Lake's strengths
4. **Efficient Cleanup**: Native DELETE operations remove old data
5. **Atomic Operations**: Directory changes are single Delta Lake transactions
6. **Simple Implementation**: No complex caching invalidation needed

### Trade-offs

- **Storage Growth**: More records over time (mitigated by Delta Lake cleanup)
- **Query Complexity**: Need to apply operations in sequence (but cached after first load)

This approach preserves the append-only benefits while enabling directory mutations and leveraging Delta Lake's native features for both time travel and cleanup.
