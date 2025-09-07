# TinyFS Extension Plan for Persistence Support

## 🎯 Problem Statement
The `test_pond_persistence_across_reopening` test fails because TinyFS does not expose the necessary types and methods for backends to properly reconstruct NodeRef objects from persistent storage.

## 🔬 Root Cause Analysis

**What Works**:
- ✅ Root directory restoration with fixed node IDs
- ✅ Delta Lake data persistence and retrieval 
- ✅ DirectoryEntry storage and lookup
- ✅ All storage and query layers function correctly

**What Fails**: 
- ❌ NodeRef reconstruction from DirectoryEntry data
- ❌ TinyFS internal types (Node, NodeType) not accessible to backends
- ❌ No mechanism for restoring nodes with specific IDs

**Debug Evidence**:
```
OpLogDirectory::get('a') - ✅ FOUND entry 'a' with child node_id: ...
OpLogDirectory::get('a') - ❌ Cannot create NodeRef due to TinyFS architectural constraints
Directory 'a' should persist after reopening pond
```

## 🛠️ Required TinyFS Changes

### 1. Export Internal Types
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/lib.rs`
```rust
// Add to public exports
pub use node::{Node, NodeType};
```

### 2. Make Node and NodeType Public  
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/node.rs`
```rust
// Change visibility
pub struct Node {
    pub id: NodeID,
    pub node_type: NodeType,
}

pub enum NodeType {
    File(crate::file::Handle),
    Directory(crate::dir::Handle), 
    Symlink(crate::symlink::Handle),
}
```

### 3. Add Node Restoration to FS
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/fs.rs`
```rust
impl FS {
    /// Restore a node with a specific ID (for persistence backends)
    pub async fn restore_node(&self, node_id: NodeID, node_type: NodeType) -> NodeRef {
        let mut state = self.state.lock().await;
        let node = NodeRef::new(Arc::new(tokio::sync::Mutex::new(Node { 
            node_type, 
            id: node_id 
        })));
        state.restored_nodes.insert(node_id, node.clone());
        node
    }
}
```

### 4. Support Sparse Node IDs
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/fs.rs`
```rust
struct State {
    nodes: Vec<NodeRef>,          // Sequential nodes
    restored_nodes: HashMap<NodeID, NodeRef>,  // Restored nodes with specific IDs
    busy: HashSet<NodeID>,
}
```

### 5. Extend Backend Interface
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/backend.rs`
```rust
#[async_trait]
pub trait FilesystemBackend: Send + Sync {
    // ...existing methods...
    
    /// Register all restored nodes during filesystem initialization
    async fn register_restored_nodes(&self, fs: &mut FS) -> Result<()> {
        // Default implementation for non-persistent backends
        Ok(())
    }
}
```

### 6. Update FS Initialization
**File**: `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/fs.rs`
```rust
impl FS {
    pub async fn with_backend<B: FilesystemBackend + 'static>(backend: B) -> Result<Self> {
        let backend = Arc::new(backend);
        
        // Create filesystem with root
        let root_dir = match backend.restore_root_directory().await? {
            Some(existing_root) => existing_root,
            None => backend.create_root_directory().await?
        };
        
        let mut fs = FS { /* ... */ };
        
        // Register all restored nodes
        backend.register_restored_nodes(&mut fs).await?;
        
        Ok(fs)
    }
}
```

## 🔧 Implementation Steps

### Phase 1: Core TinyFS Changes
1. Export Node and NodeType from lib.rs
2. Make Node and NodeType public in node.rs  
3. Add restore_node() method to FS
4. Update State to support restored nodes
5. Compile and test existing functionality

### Phase 2: Backend Integration
1. Add register_restored_nodes() to FilesystemBackend trait
2. Update FS initialization to call the new method
3. Implement register_restored_nodes() in OpLogBackend
4. Test basic restoration functionality

### Phase 3: OpLogBackend Implementation
1. Scan Delta Lake for all node entries during initialization
2. Create appropriate handles (directory, file, symlink) for each entry
3. Register nodes with filesystem using restore_node()
4. Update Directory.get() to use restored nodes
5. Run test_pond_persistence_across_reopening

### Phase 4: Validation
1. Verify all existing tests still pass
2. Confirm persistence test passes
3. Test edge cases and error handling
4. Update documentation

## 📋 Current File Changes Made

**Files Modified**:
- `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/backend.rs` - Added create_root_directory()
- `/Volumes/sourcecode/src/duckpond/crates/tinyfs/src/fs.rs` - Modified to use create_root_directory()
- `/Volumes/sourcecode/src/duckpond/crates/oplog/src/tinylogfs/backend.rs` - Implemented create_root_directory()
- `/Volumes/sourcecode/src/duckpond/crates/oplog/src/tinylogfs/directory.rs` - Fixed DirectoryEntry storage format

**Current Status**: Ready to proceed with TinyFS core changes to enable proper NodeRef reconstruction.
