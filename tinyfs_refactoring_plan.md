# TinyFS Refactoring Plan: Layered Architecture Implementation

## Overview

This plan details the step-by-step refactoring of TinyFS from the current mixed-responsibility architecture to a clean two-layer approach that supports:

- Memory-bounded caching with LRU eviction
- Directory versioning and mutation with tombstone-based invalidation
- Clear separation of concerns
- NodeID/PartID relationship tracking (each node belongs to a containing directory)

Note: Computation caching for expensive derived files will be deferred and implemented using memory backend for computed results.

## Current State Analysis

### Existing Architecture
```rust
// Current problematic structure
struct FS {
    state: Arc<Mutex<State>>,  // Mixed responsibilities
    backend: Arc<dyn FilesystemBackend>,
}

struct State {
    nodes: Vec<NodeRef>,           // Sequential nodes (confusing)
    restored_nodes: HashMap<NodeID, NodeRef>,  // Sparse nodes (confusing)
    busy: HashSet<NodeID>,         // Coordination state (appropriate)
}
```

### Problems to Solve
1. **Mixed Responsibilities**: FS manages both coordination and storage
2. **No Memory Control**: Unbounded node storage
3. **No Directory Mutation Support**: Missing versioning and invalidation
4. **Duplication**: Node information exists in multiple places
5. **Missing NodeID/PartID Relationship**: Nodes don't track containing directory

## Target Architecture

### Layer Structure
```
┌─────────────────────────────────┐
│      Layer 2: FS (Coordinator)  │
│      - Path resolution          │
│      - Loop detection (busy)    │ 
│      - API surface              │
└─────────────┬───────────────────┘
              │
┌─────────────▼───────────────────┐
│     Layer 1: CacheLayer         │
│     - Memory-bounded LRU cache  │
│     - NodeRef management        │
│     - Cache invalidation        │
└─────────────┬───────────────────┘
              │
┌─────────────▼───────────────────┐
│   Layer 0: PersistenceLayer     │
│   - Pure Delta Lake operations  │
│   - Directory versioning        │
│   - Time travel queries         │
│   - NodeID/PartID tracking      │
└─────────────────────────────────┘
```

**Note**: Computation caching for expensive derived files (e.g., downsampled timeseries) will be implemented using memory backend for computed results, keeping the core architecture simple.

## Phase-by-Phase Implementation Plan

### Phase 1: Extract PersistenceLayer (Foundation)

**Goal**: Create pure persistence layer with directory versioning support

#### Step 1.1: Create PersistenceLayer Trait
```bash
# Create new file
touch crates/tinyfs/src/persistence.rs
```

**File**: `crates/tinyfs/src/persistence.rs`
```rust
use crate::node::{NodeID, NodeType};
use crate::error::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::SystemTime;

/// Pure persistence layer - no caching, no NodeRef management
#[async_trait]
pub trait PersistenceLayer: Send + Sync {
    // Node operations (with part_id for containing directory)
    async fn load_node(&self, node_id: NodeID) -> Result<NodeType>;
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()>;
    async fn exists_node(&self, node_id: NodeID) -> Result<bool>;
    
    // Directory operations with versioning
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>>;
    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()>;
    async fn load_directory_at_time(&self, parent_node_id: NodeID, timestamp: SystemTime) -> Result<HashMap<String, NodeID>>;
    
    // Transaction management
    async fn commit(&self) -> Result<()>;
    async fn rollback(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum DirectoryOperation {
    Insert(NodeID),
    Delete,
    Rename(String, NodeID),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub child_node_id: String,
    pub operation_type: OperationType,
    pub timestamp: i64,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationType {
    Insert,
    Delete,
    Update,
}
```

#### Step 1.2: Extract OpLogPersistence from OpLogBackend

**Goal**: Move pure persistence logic out of backend

**File**: `crates/oplog/src/tinylogfs/persistence.rs`
```rust
use super::error::TinyLogFSError;
use super::schema::{OplogEntry, DirectoryEntry, OperationType};
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use crate::delta::Record;
use deltalake::{DeltaOps, protocol::SaveMode};
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use async_trait::async_trait;

pub struct OpLogPersistence {
    store_path: String,
    session_ctx: SessionContext,
    pending_records: Arc<tokio::sync::Mutex<Vec<Record>>>,
    table_name: String,
    version_counter: Arc<tokio::sync::Mutex<i64>>,
}

impl OpLogPersistence {
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError> {
        // Move initialization logic from OpLogBackend
        // Focus only on Delta Lake operations
    }
    
    async fn next_version(&self) -> Result<i64, TinyLogFSError> {
        let mut counter = self.version_counter.lock().await;
        *counter += 1;
        Ok(*counter)
    }
    
    async fn query_directory_entries(&self, parent_node_id: NodeID) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Query all directory entries for parent, ordered by version
        let sql = format!(
            "SELECT * FROM {} WHERE part_id = '{}' AND file_type = 'directory_entry' ORDER BY version",
            self.table_name, parent_node_id
        );
        
        // Execute query and deserialize entries
        // ... implementation details
    }
}

#[async_trait]
impl PersistenceLayer for OpLogPersistence {
    async fn load_node(&self, node_id: NodeID) -> TinyFSResult<NodeType> {
        // Move get_or_load_node logic here, return NodeType only
        // No NodeRef creation at this layer
    }
    
    async fn store_node(&self, node_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
        // Store node to Delta Lake
        // Add to pending_records for batch commit
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> TinyFSResult<HashMap<String, NodeID>> {
        let all_entries = self.query_directory_entries(parent_node_id).await
            .map_err(|e| tinyfs::Error::Backend(e.to_string()))?;
        
        // Apply operations in version order
        let mut current_state = HashMap::new();
        for entry in all_entries.into_iter() {
            match entry.operation_type {
                OperationType::Insert | OperationType::Update => {
                    current_state.insert(entry.name, NodeID::from_string(&entry.child_node_id));
                },
                OperationType::Delete => {
                    current_state.remove(&entry.name);
                }
            }
        }
        
        Ok(current_state)
    }
    
    async fn update_directory_entry(
        &self, 
        parent_node_id: NodeID, 
        entry_name: &str, 
        operation: DirectoryOperation
    ) -> TinyFSResult<()> {
        let entry = match operation {
            DirectoryOperation::Insert(child_node_id) => DirectoryEntry {
                name: entry_name.to_string(),
                child_node_id: child_node_id.to_string(),
                operation_type: OperationType::Insert,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                version: self.next_version().await.map_err(|e| tinyfs::Error::Backend(e.to_string()))?,
            },
            DirectoryOperation::Delete => DirectoryEntry {
                name: entry_name.to_string(),
                child_node_id: String::new(),
                operation_type: OperationType::Delete,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                version: self.next_version().await.map_err(|e| tinyfs::Error::Backend(e.to_string()))?,
            },
            DirectoryOperation::Rename(new_name, child_node_id) => {
                // Handle as delete + insert
                self.update_directory_entry(parent_node_id, entry_name, DirectoryOperation::Delete).await?;
                return self.update_directory_entry(parent_node_id, &new_name, DirectoryOperation::Insert(child_node_id)).await;
            }
        };
        
        // Add to pending records
        self.append_directory_record(parent_node_id, entry).await
            .map_err(|e| tinyfs::Error::Backend(e.to_string()))
    }
    
    async fn commit(&self) -> TinyFSResult<()> {
        // Commit pending records to Delta Lake
        // Move commit logic from OpLogBackend
    }
    
    async fn rollback(&self) -> TinyFSResult<()> {
        // Clear pending records
        self.pending_records.lock().await.clear();
        Ok(())
    }
}
```

#### Step 1.3: Update Module Exports

**File**: `crates/tinyfs/src/lib.rs`
```rust
// Add persistence module
mod persistence;
pub use persistence::{PersistenceLayer, DirectoryOperation};
```

**File**: `crates/oplog/src/tinylogfs/mod.rs`
```rust
// Add persistence module
pub mod persistence;
pub use persistence::OpLogPersistence;
```

### Phase 2: Implement CacheLayer (Memory Management)

**Goal**: Create memory-bounded cache with LRU eviction

#### Step 2.1: Add LRU Cache Dependency

**File**: `crates/tinyfs/Cargo.toml`
```toml
[dependencies]
lru = "0.12"
# ... existing dependencies
```

#### Step 2.2: Create CacheLayer

**File**: `crates/tinyfs/src/cache.rs`
```rust
use crate::persistence::PersistenceLayer;
use crate::node::{NodeID, NodeType, NodeRef, Node};
use crate::error::Result;
use lru::LruCache;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::num::NonZeroUsize;
use tokio::sync::Mutex;

pub struct CacheLayer {
    persistence: Arc<dyn PersistenceLayer>,
    cache: Arc<Mutex<LruCache<NodeID, NodeRef>>>,
    max_memory_bytes: usize,
    current_memory: Arc<AtomicUsize>,
}

impl CacheLayer {
    pub fn new(persistence: Arc<dyn PersistenceLayer>, max_memory_mb: usize) -> Self {
        let capacity = NonZeroUsize::new(1000).unwrap(); // Initial capacity, will grow/shrink based on memory
        
        Self {
            persistence,
            cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            max_memory_bytes: max_memory_mb * 1024 * 1024,
            current_memory: Arc::new(AtomicUsize::new(0)),
        }
    }
    
    pub async fn get_or_load_node(&self, node_id: NodeID) -> Result<NodeRef> {
        // Check cache first
        {
            let mut cache = self.cache.lock().await;
            if let Some(node) = cache.get(&node_id) {
                return Ok(node.clone());
            }
        }
        
        // Load from persistence
        let node_type = self.persistence.load_node(node_id).await?;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        
        // Add to cache with memory tracking
        self.insert_with_eviction(node_id, node.clone()).await?;
        
        Ok(node)
    }
    
    pub async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: NodeType) -> Result<()> {
        // Store in persistence with part_id
        self.persistence.store_node(node_id, part_id, &node_type).await?;
        
        // Update cache
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        
        self.insert_with_eviction(node_id, node).await?;
        
        Ok(())
    }
    
    pub async fn invalidate_node(&self, node_id: NodeID) {
        let mut cache = self.cache.lock().await;
        if let Some((_, evicted_node)) = cache.pop_entry(&node_id) {
            let size = self.estimate_node_size(&evicted_node).await;
            self.current_memory.fetch_sub(size, Ordering::Relaxed);
        }
    }
    
    pub async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        self.persistence.load_directory_entries(parent_node_id).await
    }
    
    pub async fn update_directory_entry(
        &self, 
        parent_node_id: NodeID, 
        entry_name: &str, 
        operation: crate::persistence::DirectoryOperation
    ) -> Result<()> {
        // Update in persistence
        self.persistence.update_directory_entry(parent_node_id, entry_name, operation).await?;
        
        // Invalidate parent directory cache
        self.invalidate_node(parent_node_id).await;
        
        Ok(())
    }
    
    pub async fn update_directory_with_invalidation(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: crate::persistence::DirectoryOperation,
    ) -> Result<()> {
        self.update_directory_entry(parent_node_id, entry_name, operation).await
    }
    
    async fn insert_with_eviction(&self, node_id: NodeID, node: NodeRef) -> Result<()> {
        let node_size = self.estimate_node_size(&node).await;
        
        let mut cache = self.cache.lock().await;
        
        // Evict until we have enough memory
        while self.current_memory.load(Ordering::Relaxed) + node_size > self.max_memory_bytes {
            if let Some((_, evicted_node)) = cache.pop_lru() {
                let evicted_size = self.estimate_node_size(&evicted_node).await;
                self.current_memory.fetch_sub(evicted_size, Ordering::Relaxed);
            } else {
                // Cache is empty but still over memory limit
                // This node is too large for the cache
                return Err(crate::Error::OutOfMemory(format!("Node {} too large for cache", node_id)));
            }
        }
        
        cache.put(node_id, node);
        self.current_memory.fetch_add(node_size, Ordering::Relaxed);
        
        Ok(())
    }
    
    async fn estimate_node_size(&self, node: &NodeRef) -> usize {
        // Estimate memory usage of a node
        // This is a heuristic - can be refined based on actual usage
        match node.try_lock() {
            Ok(node_guard) => {
                match &node_guard.node_type {
                    NodeType::File(handle) => {
                        // Estimate based on file content size
                        // For now, use a fixed overhead plus content size estimation
                        1024 + handle.content().map(|c| c.len()).unwrap_or(0)
                    },
                    NodeType::Directory(_) => {
                        // Directory overhead plus estimated entries
                        2048 // Base directory overhead
                    },
                    NodeType::Symlink(_) => {
                        512 // Small overhead for symlinks
                    }
                }
            },
            Err(_) => {
                // Couldn't lock, use conservative estimate
                1024
            }
        }
    }
    
    pub async fn commit(&self) -> Result<()> {
        self.persistence.commit().await
    }
}
```

### Phase 3: Update FS to use Layers

**Goal**: Replace mixed-responsibility FS with clean coordinator

#### Step 3.1: Update FS Structure

**File**: `crates/tinyfs/src/fs.rs`
```rust
use crate::cache::CacheLayer;
use crate::persistence::PersistenceLayer;
use crate::node::{NodeID, NodeType, NodeRef};
use crate::error::Result;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct FS {
    cache: Arc<CacheLayer>,
    busy: Arc<Mutex<HashSet<NodeID>>>, // Only coordination state for loop detection
}

impl FS {
    pub async fn with_persistence_layer<P: PersistenceLayer + 'static>(
        persistence: P,
        cache_size_mb: usize,
    ) -> Result<Self> {
        let cache_layer = Arc::new(CacheLayer::new(Arc::new(persistence), cache_size_mb));
        
        Ok(FS {
            cache: cache_layer,
            busy: Arc::new(Mutex::new(HashSet::new())),
        })
    }
    
    pub async fn get_node(&self, node_id: NodeID) -> Result<NodeRef> {
        self.cache.get_or_load_node(node_id).await
    }
    
    pub async fn create_node(&self, part_id: NodeID, node_type: NodeType) -> Result<NodeRef> {
        let node_id = NodeID::new_sequential();
        self.cache.store_node(node_id, part_id, node_type).await?;
        self.get_node(node_id).await
    }
    
    pub async fn update_directory(
        &self,
        parent_node_id: NodeID,
        entry_name: &str,
        operation: DirectoryOperation,
    ) -> Result<()> {
        self.cache.update_directory_with_invalidation(parent_node_id, entry_name, operation).await
    }
    
    // Loop detection for recursive operations  
    pub async fn mark_busy(&self, node_id: NodeID) -> Result<bool> {
        let mut busy = self.busy.lock().await;
        Ok(busy.insert(node_id))
    }
    
    pub async fn mark_not_busy(&self, node_id: NodeID) {
        let mut busy = self.busy.lock().await;
        busy.remove(&node_id);
    }
    
    pub async fn is_busy(&self, node_id: NodeID) -> bool {
        let busy = self.busy.lock().await;
        busy.contains(&node_id)
    }
    
    pub async fn commit(&self) -> Result<()> {
        self.cache.commit().await
    }
}
```

### Phase 4: Derived File Computation Strategy (Future)

**Goal**: Implement derived file caching using memory backend

For expensive derived computations like downsampled timeseries, we'll use a separate memory-backed filesystem:

```rust
// Example usage for derived files
use crate::backend::memory::MemoryBackend;

pub struct DerivedFileManager {
    source_fs: Arc<FS>,
    memory_fs: Arc<FS>, // Uses memory backend for computed results
}

impl DerivedFileManager {
    pub async fn new(source_fs: Arc<FS>) -> Result<Self> {
        let memory_backend = MemoryBackend::new();
        let memory_fs = Arc::new(FS::with_persistence_layer(memory_backend, 128).await?);
        
        Ok(Self { source_fs, memory_fs })
    }
    
    pub async fn get_downsampled_timeseries(
        &self,
        source_node_id: NodeID,
        resolution: Duration,
    ) -> Result<NodeRef> {
        let computation_key = format!("downsample_{}_{}", source_node_id, resolution.as_secs());
        let derived_node_id = NodeID::from_string(&computation_key);
        
        // Check if already computed
        if let Ok(cached_node) = self.memory_fs.get_node(derived_node_id).await {
            return Ok(cached_node);
        }
        
        // Compute derived result
        let source_node = self.source_fs.get_node(source_node_id).await?;
        let downsampled_data = expensive_downsample_operation(&source_node).await?;
        
        // Store in memory filesystem
        let derived_node = self.memory_fs.create_node(
            NodeID::root(), // Or appropriate parent
            NodeType::File(downsampled_data)
        ).await?;
        
        Ok(derived_node)
    }
}
```

This approach:
- Keeps the core architecture simple (2 layers)
- Uses existing memory backend for computation caching
- Provides clear separation between persistent and computed data
- Allows easy expansion later if needed

### Phase 5: Update OpLogBackend Integration

**Goal**: Integrate OpLogBackend with new PersistenceLayer

#### Step 5.1: Create OpLogPersistence Implementation

**File**: `crates/oplog/src/tinylogfs/persistence.rs`
```rust
use async_trait::async_trait;
use tinyfs::persistence::{PersistenceLayer, DirectoryOperation};
use tinyfs::node::{NodeID, NodeType};
use tinyfs::error::Result;
use std::collections::HashMap;
use std::time::SystemTime;

pub struct OpLogPersistence {
    // Move current OpLogBackend fields here
    store_path: String,
    session_ctx: SessionContext,
    pending_records: Arc<Mutex<Vec<Record>>>,
}

#[async_trait]
impl PersistenceLayer for OpLogPersistence {
    async fn load_node(&self, node_id: NodeID) -> Result<NodeType> {
        // Move load logic from OpLogBackend::get_or_load_node
        // Return only NodeType, no NodeRef creation
    }
    
    async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> Result<()> {
        // Move store logic from OpLogBackend
        // Include part_id in the record for containing directory tracking
        let record = Record {
            id: node_id.to_string(),
            part_id: part_id.to_string(), // NEW: track containing directory
            node_type: serde_json::to_string(node_type)?,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64,
        };
        
        self.pending_records.lock().await.push(record);
        Ok(())
    }
    
    async fn exists_node(&self, node_id: NodeID) -> Result<bool> {
        // Query Delta Lake for node existence
    }
    
    async fn load_directory_entries(&self, parent_node_id: NodeID) -> Result<HashMap<String, NodeID>> {
        // Query directory entries with tombstone handling
        // Apply operations in version order to get current state
    }
    
    async fn update_directory_entry(&self, parent_node_id: NodeID, entry_name: &str, operation: DirectoryOperation) -> Result<()> {
        // Create directory entry records with versioning
        // Support Insert, Delete, Rename operations
    }
    
    async fn load_directory_at_time(&self, parent_node_id: NodeID, timestamp: SystemTime) -> Result<HashMap<String, NodeID>> {
        // Time travel query for directory state
        // Filter entries by timestamp and apply operations
    }
    
    async fn commit(&self) -> Result<()> {
        // Move commit logic from OpLogBackend
        // Write all pending records to Delta Lake
    }
}

impl OpLogPersistence {
    pub async fn new(store_path: &str) -> Result<Self> {
        // Move constructor logic from OpLogBackend
    }
}
```

#### Step 5.2: Update OpLogBackend to use new architecture  

**File**: `crates/oplog/src/tinylogfs/backend.rs`
```rust
use tinyfs::FS;
use crate::persistence::OpLogPersistence;

pub async fn create_oplog_fs(store_path: &str, cache_size_mb: usize) -> Result<FS> {
    let persistence = OpLogPersistence::new(store_path).await?;
    FS::with_persistence_layer(persistence, cache_size_mb).await
}

// Remove old OpLogBackend struct - replaced by OpLogPersistence + FS layers
```

### Phase 6: Update Tests and Integration

**Goal**: Ensure all tests pass with new architecture

#### Step 6.1: Update Backend Tests

**File**: `crates/oplog/src/tinylogfs/tests.rs`
```rust
use super::persistence::OpLogPersistence;
use tinyfs::persistence::PersistenceLayer;

#[tokio::test]
async fn test_persistence_layer() {
    let persistence = OpLogPersistence::new("./test_data").await.unwrap();
    
    // Test node operations
    let node_id = NodeID::new(1);
    let part_id = NodeID::new(0); // Root directory
    let node_type = NodeType::File(vec![1, 2, 3]);
    
    persistence.store_node(node_id, part_id, &node_type).await.unwrap();
    let loaded = persistence.load_node(node_id).await.unwrap();
    assert_eq!(loaded, node_type);
    
    // Test directory operations
    persistence.update_directory_entry(
        part_id, 
        "test.txt", 
        DirectoryOperation::Insert(node_id)
    ).await.unwrap();
    
    let entries = persistence.load_directory_entries(part_id).await.unwrap();
    assert_eq!(entries.get("test.txt"), Some(&node_id));
}

#[tokio::test] 
async fn test_cache_layer() {
    let persistence = OpLogPersistence::new("./test_data").await.unwrap();
    let cache = CacheLayer::new(Arc::new(persistence), 64); // 64MB cache
    
    // Test memory management and LRU eviction
}

#[tokio::test]
async fn test_full_integration() {
    let fs = create_oplog_fs("./test_data", 128).await.unwrap();
    
    // Test all FS operations work through the new layers
}
```

#### Step 4.2: Update Module Exports

**File**: `crates/tinyfs/src/lib.rs`
```rust
// Add new modules
mod persistence;
mod cache;
mod computation;

// Export new interfaces
pub use persistence::{PersistenceLayer, DirectoryOperation};
pub use cache::CacheLayer;

// ... existing exports remain the same
```

### Phase 5: Migration and Testing

#### Step 5.1: Update OpLogBackend to Use New Architecture

**File**: `crates/oplog/src/tinylogfs/backend.rs`
```rust
// Simplify OpLogBackend to be a thin wrapper around OpLogPersistence

use super::persistence::OpLogPersistence;
use tinyfs::{FilesystemBackend, FS, PersistenceLayer};
use async_trait::async_trait;

pub struct OpLogBackend {
    persistence: OpLogPersistence,
}

impl OpLogBackend {
    pub async fn new(store_path: &str) -> Result<Self, super::error::TinyLogFSError> {
        let persistence = OpLogPersistence::new(store_path).await?;
        Ok(OpLogBackend { persistence })
    }
    
    pub async fn create_filesystem(
        self,
        cache_size_mb: usize,
        max_computations: usize,
    ) -> Result<FS, super::error::TinyLogFSError> {
        FS::with_persistence_layer(self.persistence, cache_size_mb, max_computations).await
            .map_err(|e| super::error::TinyLogFSError::TinyFS(e))
    }
}

// Keep minimal FilesystemBackend impl for compatibility during transition
#[async_trait]
impl FilesystemBackend for OpLogBackend {
    // ... existing methods delegate to persistence layer
    // This interface will be deprecated once migration is complete
}
```

#### Step 5.2: Update Tests

**File**: `crates/oplog/src/tinylogfs/tests.rs`
```rust
// Update tests to use new layered architecture

async fn create_test_filesystem() -> Result<(FS, TempDir), TinyLogFSError> {
    let temp_dir = TempDir::new().map_err(TinyLogFSError::Io)?;
    let store_path = temp_dir.path().join("test_store");
    let store_path_str = store_path.to_string_lossy();

    let backend = OpLogBackend::new(&store_path_str).await?;
    let fs = backend.create_filesystem(
        64, // 64MB cache
        100 // 100 max computations
    ).await?;

    Ok((fs, temp_dir))
}

#[tokio::test]
async fn test_directory_versioning() -> Result<(), Box<dyn std::error::Error>> {
    let (fs, _temp_dir) = create_test_filesystem().await?;
    
    let root = fs.root().await?;
    let test_dir = root.create_dir_path("test").await?;
    let test_dir_id = test_dir.node_path().id().await;
    
    // Add file to directory
    let file_node = fs.create_node(NodeType::File(/* ... */)).await?;
    let file_id = file_node.lock().await.id;
    
    fs.add_directory_entry(test_dir_id, "file.txt", file_id).await?;
    
    // Verify entry exists
    let entries = fs.list_directory_entries(test_dir_id).await?;
    assert!(entries.contains_key("file.txt"));
    
    // Remove entry
#### Step 6.2: Update Integration Tests

**File**: `crates/oplog/src/tinylogfs/tests.rs`
```rust
// Update tests to use new layered architecture

async fn create_test_filesystem() -> Result<(FS, TempDir), TinyLogFSError> {
    let temp_dir = TempDir::new().map_err(TinyLogFSError::Io)?;
    let store_path = temp_dir.path().join("test_store");
    let store_path_str = store_path.to_string_lossy();

    let persistence = OpLogPersistence::new(&store_path_str).await?;
    let fs = FS::with_persistence_layer(persistence, 64).await?; // 64MB cache

    Ok((fs, temp_dir))
}

#[tokio::test]
async fn test_directory_versioning() -> Result<(), Box<dyn std::error::Error>> {
    let (fs, _temp_dir) = create_test_filesystem().await?;
    
    let root = fs.root().await?;
    let test_dir = root.create_dir_path("test").await?;
    let test_dir_id = test_dir.node_path().id().await;
    
    // Add file to directory
    let file_node = fs.create_node(NodeType::File(vec![1, 2, 3])).await?;
    let file_id = file_node.lock().await.id;
    
    fs.add_directory_entry(test_dir_id, "file.txt", file_id).await?;
    
    // Verify entry exists
    let entries = fs.list_directory_entries(test_dir_id).await?;
    assert!(entries.contains_key("file.txt"));
    
    // Remove entry
    fs.remove_directory_entry(test_dir_id, "file.txt").await?;
    
    // Verify entry removed
    let entries = fs.list_directory_entries(test_dir_id).await?;
    assert!(!entries.contains_key("file.txt"));
    
    fs.commit().await?;
    Ok(())
}

#[tokio::test] 
async fn test_memory_bounded_cache() -> Result<(), Box<dyn std::error::Error>> {
    // Create small cache to test eviction
    let persistence = OpLogPersistence::new("./test_data").await?;
    let fs = FS::with_persistence_layer(persistence, 1).await?; // 1MB cache
    
    // Create many nodes to trigger eviction
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let large_data = vec![0u8; 50_000]; // 50KB per node
        let node = fs.create_node(NodeID::root(), NodeType::File(large_data)).await?;
        node_ids.push(node.lock().await.id);
    }
    
    // Verify we can still access nodes (some from cache, some from persistence)
    for node_id in node_ids {
        let node = fs.get_node(node_id).await?;
        assert!(node.lock().await.node_type.is_file());
    }
    
    Ok(())
}
```

## Benefits of Refactored Architecture

### 1. Clear Separation of Concerns
- **PersistenceLayer**: Pure Delta Lake operations, no caching
- **CacheLayer**: Memory management, no persistence logic  
- **FS**: Pure coordination, no storage responsibilities

### 2. Memory Control
- Configurable cache size limits
- LRU eviction when memory limit exceeded
- Memory usage estimation and tracking

### 3. Directory Versioning
- Full mutation support (insert, delete, rename)
- Complete history preservation
- Time-travel queries via Delta Lake
- Efficient invalidation with tombstone approach
- NodeID/PartID relationship tracking

### 4. Future Derived File Optimization
- Use memory backend for computed results
- Simple and clean architecture
- Easy to extend when needed

### 5. Simplified Testing
- Each layer can be unit tested independently
- Mock implementations easy to create
- Clear interfaces reduce test complexity

## Implementation Timeline

- **Phase 1** (PersistenceLayer): 2-3 days
- **Phase 2** (CacheLayer): 2-3 days  
- **Phase 3** (FS Refactor): 1-2 days
- **Phase 4** (Derived File Strategy): Deferred - use memory backend
- **Phase 5** (OpLog Integration): 2-3 days
- **Phase 6** (Migration/Testing): 2-3 days

**Total Estimated Time**: 9-14 days

This plan maintains backward compatibility during the transition while systematically eliminating the mixed responsibilities and providing the foundation needed for DuckPond's production use cases. The simplified two-layer approach keeps complexity manageable while providing a clear path for future enhancements.
