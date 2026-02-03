# DuckPond Production Requirements Document (Updated)

## Repository Overview

This repository contains an original proof-of-concept implementation for DuckPond at the top-level of the repository, with a main crate, hydrovu, and pond sub-modules. This implementation has been frozen, and GitHub Copilot will not modify any of the Rust code under ./src, however it makes a good reference as to the intentions behind our new development.

## Current Status

### Completed Foundation Crates

#### 1. TinyFS Crate (`./crates/tinyfs`) - ✅ COMPLETE
- **Purpose**: In-memory filesystem abstraction with dynamic files
- **Key Features**:
  - Files, directories, and symlinks with reference counting
  - Working directory API with path resolution
  - Dynamic directories via custom `Directory` trait implementations
  - Pattern matching with glob support and capture groups
  - Recursive operations and filesystem traversal
- **Architecture**: `FS` → `WD` → `NodePath` → `Node(File|Directory|Symlink)`
- **Status**: Core implementation complete and tested

#### 2. OpLog Crate (`./crates/oplog`) - ✅ COMPLETE
- **Purpose**: Operation logging system using Delta Lake + DataFusion
- **Key Features**:
  - Two-layer data storage (Delta Lake outer + Arrow IPC inner)
  - ACID guarantees, time travel, and schema evolution
  - SQL queries over serialized Arrow IPC data
  - Custom DataFusion TableProvider (`ByteStreamTable`)
- **Architecture**: `Record` (node_id, timestamp, version, content) + `Entry` (serialized as Arrow IPC)
- **Status**: Implementation complete with comprehensive testing

#### 3. CMD Crate (`./crates/cmd`) - ✅ COMPLETE
- **Purpose**: Command-line interface for pond management
- **Key Features**:
  - `pond init` - Initialize new ponds with empty Delta Lake store
  - `pond show` - Display operation log contents with formatted output
  - Environment integration using `POND` environment variable
  - Comprehensive error handling and validation
- **Architecture**: Uses `clap` for CLI, integrates with OpLog for store management
- **Status**: Core commands implemented with integration testing

## Integration Phase: TinyLogFS

### Objective
Create a new submodule `tinylogfs` within the OpLog crate that combines TinyFS's in-memory filesystem abstraction with OpLog's persistent storage capabilities.

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   TinyLogFS                         │
│  ┌─────────────────┐    ┌─────────────────────────┐ │
│  │   TinyFS Layer  │    │     OpLog Layer         │ │
│  │   (In-Memory)   │◄──►│   (Delta Lake +         │ │
│  │                 │    │    DataFusion)          │ │
│  │ • Derived files │    │ • Persistence           │ │
│  │ • Pattern match │    │ • ACID guarantees       │ │
│  │ • Navigation    │    │ • Time travel           │ │
│  └─────────────────┘    │ • SQL queries           │ │
│                         └─────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Implementation Strategy

#### Phase 1: Schema Design for Filesystem Operations ✅ COMPLETE

**Implementation Complete**: The TinyLogFS schema design has been successfully implemented and tested.

The oplog crate's partition ID ("part_id") matches the node_id for directory files. For files and symlinks, the part_id is the parent ID of the containing directory. The oplog Record's content field contains an Arrow IPC encoding of a record batch consisting of OplogEntry:

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OplogEntry {
    pub part_id: String,        // hex encoded partition ID (parent for files/symlinks)
    pub node_id: String,        // hex encoded NodeID
    pub file_type: String,      // "file", "directory", "symlink"  
    pub metadata: String,       // JSON-encoded extensible attributes
    pub content: Vec<u8>,       // Type-specific content
}
```

For directory nodes, the content of the OplogEntry contains a nested Arrow IPC encoding of a record batch consisting of DirectoryEntry:

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DirectoryEntry {
    pub name: String,           // Entry name within directory
    pub child_node_id: String,  // Hex encoded NodeID
}
```

**Implemented Features**:
- ✅ OplogEntry and DirectoryEntry structs with ForArrow trait implementation
- ✅ OplogEntryTable and DirectoryEntryTable DataFusion table providers
- ✅ Custom OplogEntryExec execution plan for nested data deserialization
- ✅ Helper functions for Arrow IPC encoding/decoding
- ✅ Updated CMD `pond show` command to display OplogEntry records
- ✅ Updated CMD `pond init` command to create OplogEntry-based tables

**Testing Status**: End-to-end functionality verified with `pond init` and `pond show` commands.

**Architecture Validation**: 
- ✅ **DataFusion Projection Fix**: Custom table providers properly respect column projection
- ✅ **Schema Compatibility**: OplogEntry/DirectoryEntry schemas work seamlessly with Delta Lake
- ✅ **Nested Data Handling**: Arrow IPC serialization of DirectoryEntry within OplogEntry.content works correctly
- ✅ **CLI Integration**: Both `pond init` and `pond show` commands work end-to-end with OplogEntry schema

#### Phase 2: Refined Hybrid Storage Architecture ⚡ UPDATED DESIGN

**Design Objective**: Implement a refined `TinyLogFS` struct that simplifies the architecture by using single-threaded design with `Rc<RefCell<_>>` instead of `Arc<RwLock<_>>`, Arrow Array builders for transaction state, and enhanced table providers that can snapshot builders to RecordBatch on-the-fly.

##### Refined Core Architecture Design

```rust
use std::rc::Rc;
use std::cell::RefCell;
use arrow_array::builder::{StringBuilder, Int64Builder, BinaryBuilder};

pub struct TinyLogFS {
    // Fast in-memory filesystem for hot operations (single-threaded)
    memory_fs: tinyfs::FS,
    
    // Persistent storage configuration
    oplog_store_path: String,
    
    // Transaction state using Arrow Array builders
    transaction_state: RefCell<TransactionState>,
    
    // Node metadata tracking
    node_metadata: RefCell<HashMap<String, NodeMetadata>>,
    
    // Last sync timestamp
    last_sync_timestamp: RefCell<SystemTime>,
}

#[derive(Debug)]
struct TransactionState {
    // Arrow Array builders for pending transaction data
    part_id_builder: StringBuilder,      // Partition IDs (parent directory)
    timestamp_builder: Int64Builder,     // Operation timestamps  
    version_builder: Int64Builder,       // Version numbers
    content_builder: BinaryBuilder,      // Serialized Arrow IPC content
    
    // Transaction metadata
    transaction_id: String,
    started_at: SystemTime,
    operation_count: usize,
}

#[derive(Debug, Clone)]
struct NodeMetadata {
    node_id: String,
    file_type: String,
    created_at: SystemTime,
    modified_at: SystemTime,
    parent_id: Option<String>,
    is_dirty: bool,
}

impl TinyLogFS {
    /// Create new TinyLogFS instance, loading existing state from OpLog
    pub async fn new(store_path: &str) -> Result<Self, TinyLogFSError>;
    
    /// Initialize empty TinyLogFS with root directory
    pub async fn init_empty(store_path: &str) -> Result<Self, TinyLogFSError>;
    
    // === Core Filesystem Operations ===
    
    /// Create a new file with content, returns NodePath for navigation
    pub async fn create_file(&mut self, path: &Path, content: &[u8]) -> Result<NodePath, TinyLogFSError>;
    
    /// Create a new directory, returns WorkingDirectory for navigation  
    pub async fn create_directory(&mut self, path: &Path) -> Result<WD, TinyLogFSError>;
    
    /// Create a symlink pointing to target
    pub async fn create_symlink(&mut self, path: &Path, target: &Path) -> Result<NodePath, TinyLogFSError>;
    
    /// Read file content by path or NodeID
    pub async fn read_file(&self, path: &Path) -> Result<Vec<u8>, TinyLogFSError>;
    pub async fn read_file_by_id(&self, node_id: &str) -> Result<Vec<u8>, TinyLogFSError>;
    
    /// Update file content
    pub async fn update_file(&mut self, path: &Path, content: &[u8]) -> Result<(), TinyLogFSError>;
    pub async fn update_file_by_id(&mut self, node_id: &str, content: &[u8]) -> Result<(), TinyLogFSError>;
    
    /// Delete file, directory, or symlink
    pub async fn delete(&mut self, path: &Path) -> Result<(), TinyLogFSError>;
    pub async fn delete_by_id(&mut self, node_id: &str) -> Result<(), TinyLogFSError>;
    
    /// List directory contents
    pub async fn list_directory(&self, path: &Path) -> Result<Vec<DirEntry>, TinyLogFSError>;
    
    // === State Management ===
    
    /// Get current working directory interface
    pub fn working_directory(&self) -> Result<WD, TinyLogFSError>;
    
    /// Get reference to underlying TinyFS for advanced operations
    pub fn memory_fs(&self) -> &tinyfs::FS;
    
    /// Check if node exists (fast, memory-only check)
    pub fn exists(&self, path: &Path) -> bool;
    
    /// Get node metadata without loading content
    pub async fn get_metadata(&self, path: &Path) -> Result<NodeMetadata, TinyLogFSError>;
    
    // === Persistence Operations ===
    
    /// Flush pending transactions to OpLog by snapshotting builders to RecordBatch
    pub async fn commit(&mut self) -> Result<CommitResult, TinyLogFSError>;
    
    /// Force commit of specific operations
    pub async fn commit_partial(&mut self, operation_count: usize) -> Result<CommitResult, TinyLogFSError>;
    
    /// Restore filesystem state from OpLog (useful for recovery)
    pub async fn restore_from_oplog(&mut self) -> Result<RestoreResult, TinyLogFSError>;
    
    /// Restore filesystem state to specific timestamp
    pub async fn restore_to_timestamp(&mut self, timestamp: SystemTime) -> Result<RestoreResult, TinyLogFSError>;
    
    /// Get current transaction status and pending operation count
    pub fn get_status(&self) -> TinyLogFSStatus;
    
    // === Query Operations ===
    
    /// Query filesystem history using SQL
    pub async fn query_history(&self, sql: &str) -> Result<Vec<RecordBatch>, TinyLogFSError>;
    
    /// Get all versions of a specific node
    pub async fn get_node_history(&self, node_id: &str) -> Result<Vec<HistoryEntry>, TinyLogFSError>;
    
    /// Find nodes by metadata criteria
    pub async fn find_nodes(&self, criteria: &NodeSearchCriteria) -> Result<Vec<String>, TinyLogFSError>;
}

impl TransactionState {
    fn new() -> Self {
        Self {
            part_id_builder: StringBuilder::new(),
            timestamp_builder: Int64Builder::new(),
            version_builder: Int64Builder::new(),
            content_builder: BinaryBuilder::new(),
            transaction_id: uuid::Uuid::new_v4().to_string(),
            started_at: SystemTime::now(),
            operation_count: 0,
        }
    }
    
    /// Add a filesystem operation to the transaction builders
    fn add_operation(&mut self, operation: &FilesystemOperation) -> Result<(), TinyLogFSError> {
        match operation {
            FilesystemOperation::CreateFile { path, content, node_id, parent_id } => {
                self.part_id_builder.append_value(parent_id);
                self.timestamp_builder.append_value(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);
                self.version_builder.append_value(1); // Initial version
                
                // Create OplogEntry and serialize as Arrow IPC
                let oplog_entry = OplogEntry {
                    part_id: parent_id.clone(),
                    node_id: node_id.clone(),
                    file_type: "file".to_string(),
                    metadata: "{}".to_string(), // Empty JSON object
                    content: content.clone(),
                };
                let serialized = serialize_oplog_entry(&oplog_entry)?;
                self.content_builder.append_value(&serialized);
                
                self.operation_count += 1;
            },
            FilesystemOperation::CreateDirectory { path, node_id } => {
                self.part_id_builder.append_value(node_id); // Directory is its own partition
                self.timestamp_builder.append_value(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);
                self.version_builder.append_value(1);
                
                // Create OplogEntry with empty directory content
                let directory_entries: Vec<DirectoryEntry> = vec![];
                let directory_content = serialize_directory_entries(&directory_entries)?;
                
                let oplog_entry = OplogEntry {
                    part_id: node_id.clone(),
                    node_id: node_id.clone(),
                    file_type: "directory".to_string(),
                    metadata: "{}".to_string(),
                    content: directory_content,
                };
                let serialized = serialize_oplog_entry(&oplog_entry)?;
                self.content_builder.append_value(&serialized);
                
                self.operation_count += 1;
            },
            // ... handle other operation types
        }
        Ok(())
    }
    
    /// Snapshot the current builders into a RecordBatch
    fn snapshot_to_record_batch(&mut self) -> Result<RecordBatch, TinyLogFSError> {
        let part_id_array = self.part_id_builder.finish();
        let timestamp_array = self.timestamp_builder.finish();
        let version_array = self.version_builder.finish();
        let content_array = self.content_builder.finish();
        
        // Create new builders for next batch
        self.part_id_builder = StringBuilder::new();
        self.timestamp_builder = Int64Builder::new();
        self.version_builder = Int64Builder::new();
        self.content_builder = BinaryBuilder::new();
        
        // Reset operation count
        self.operation_count = 0;
        
        // Create RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("part_id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("version", DataType::Int64, false),
            Field::new("content", DataType::Binary, false),
        ]));
        
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(part_id_array),
                Arc::new(timestamp_array),
                Arc::new(version_array),
                Arc::new(content_array),
            ],
        ).map_err(|e| TinyLogFSError::Arrow(e.to_string()))
    }
}

#[derive(Debug, Clone)]
enum FilesystemOperation {
    CreateFile { 
        path: PathBuf, 
        content: Vec<u8>,
        node_id: String,
        parent_id: String,
    },
    CreateDirectory { 
        path: PathBuf,
        node_id: String,
    },
    CreateSymlink { 
        path: PathBuf, 
        target: PathBuf,
        node_id: String,
        parent_id: String,
    },
    UpdateFile { 
        node_id: String, 
        content: Vec<u8> 
    },
    DeleteNode { 
        node_id: String 
    },
    UpdateDirectory {
        node_id: String,
        entries: Vec<DirectoryEntry>,
    },
}

#[derive(Debug)]
pub struct NodeMetadata {
    pub node_id: String,
    pub file_type: FileType,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub size: u64,
    pub parent_id: Option<String>,
    pub custom_attributes: serde_json::Value,
}

#[derive(Debug)]
pub enum FileType {
    File,
    Directory,
    Symlink { target: PathBuf },
}

#[derive(Debug)]
pub struct CommitResult {
    pub operations_committed: usize,
    pub transaction_id: String,
    pub commit_duration: Duration,
    pub bytes_written: u64,
    pub record_batch_size: usize,
}

#[derive(Debug)]
pub struct RestoreResult {
    pub nodes_restored: usize,
    pub restore_duration: Duration,
    pub target_timestamp: SystemTime,
}

#[derive(Debug)]
pub struct TinyLogFSStatus {
    pub pending_operations: usize,
    pub total_nodes: usize,
    pub last_commit: Option<SystemTime>,
    pub memory_usage: usize,
    pub transaction_id: String,
}

#[derive(Debug)]
pub struct HistoryEntry {
    pub timestamp: SystemTime,
    pub operation: String,
    pub node_state: OplogEntry,
}

#[derive(Debug)]
pub struct NodeSearchCriteria {
    pub file_type: Option<FileType>,
    pub created_after: Option<SystemTime>,
    pub created_before: Option<SystemTime>,
    pub metadata_query: Option<String>, // JSON path query
}

#[derive(Debug, thiserror::Error)]
pub enum TinyLogFSError {
    #[error("OpLog error: {0}")]
    OpLog(#[from] oplog::OpLogError),
    
    #[error("TinyFS error: {0}")]
    TinyFS(#[from] tinyfs::Error),
    
    #[error("Node not found: {path}")]
    NodeNotFound { path: PathBuf },
    
    #[error("Transaction error: {message}")]
    Transaction { message: String },
    
    #[error("Commit error: {message}")]
    Commit { message: String },
    
    #[error("Restore error: {message}")]
    Restore { message: String },
    
    #[error("Arrow error: {0}")]
    Arrow(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

##### Enhanced Transaction Model

**Design Principle**: TinyLogFS uses Arrow Array builders to accumulate filesystem operations in memory before committing them as RecordBatch entries to the OpLog. This provides excellent performance for batched operations while maintaining ACID guarantees.

**Transaction Flow**:
1. **Operation**: Filesystem operation updates in-memory TinyFS and adds to transaction builders
2. **Accumulation**: Multiple operations build up arrays in the TransactionState
3. **Snapshot**: `commit()` snapshots builders to RecordBatch and writes to Delta Lake
4. **Reset**: Builders are reset for next batch of operations
5. **Cleanup**: Transaction state cleared and metadata updated

**Builder Strategy**: Using Arrow Array builders provides several advantages:
- **Memory Efficiency**: Columnar layout is memory-efficient for repeated operations
- **Type Safety**: Compile-time guarantees about data types and schema compatibility
- **Performance**: Direct conversion to RecordBatch without intermediate serialization
- **Flexibility**: Easy to add new columns or operation types

**Error Recovery**: If commit fails, the transaction builders retain their state and commit can be retried. If process crashes before commit, pending operations are lost (acceptable for CLI tool usage pattern).

#### Phase 3: Enhanced OpLog-Backed Directory Implementation 📋 UPDATED DESIGN

**Design Objective**: Replace TinyFS's `MemoryDirectory` with an `OpLogDirectory` implementation that uses `Rc<RefCell<TinyLogFS>>` for back-references and provides seamless integration with the transaction system.

##### OpLogDirectory Architecture

```rust
use std::rc::{Rc, Weak};
use std::cell::RefCell;

pub struct OpLogDirectory {
    // Core identity
    node_id: String,
    oplog_partition_id: String,
    
    // Performance caching
    entry_cache: RefCell<BTreeMap<String, CachedEntry>>,
    cache_dirty: RefCell<bool>,
    
    // Back-reference to TinyLogFS for transaction integration
    tinylogfs: Weak<RefCell<TinyLogFS>>,
    
    // Metadata
    metadata: RefCell<DirectoryMetadata>,
    last_loaded: RefCell<SystemTime>,
}

#[derive(Debug, Clone)]
struct CachedEntry {
    node_ref: NodeRef,
    last_accessed: SystemTime,
    is_dirty: bool,
    directory_entry: DirectoryEntry, // Cache the DirectoryEntry for this node
}

#[derive(Debug, Clone)]
struct DirectoryMetadata {
    created_at: SystemTime,
    modified_at: SystemTime,
    entry_count: usize,
    custom_attributes: serde_json::Value,
}

impl Directory for OpLogDirectory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>, tinyfs::Error> {
        // 1. Check in-memory cache first (fast path)
        if let Some(cached) = self.entry_cache.borrow_mut().get_mut(name) {
            cached.last_accessed = SystemTime::now();
            return Ok(Some(cached.node_ref.clone()));
        }
        
        // 2. Query OpLog for directory entries (slow path)
        let entries = self.load_directory_entries_from_oplog()?;
        
        // 3. Find requested entry and construct NodeRef
        if let Some(dir_entry) = entries.iter().find(|e| e.name == name) {
            let node_ref = self.reconstruct_node_ref(&dir_entry.child_node_id)?;
            
            // 4. Cache result for future access
            self.cache_entry(name.to_string(), node_ref.clone(), dir_entry.clone());
            
            Ok(Some(node_ref))
        } else {
            Ok(None)
        }
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> Result<(), tinyfs::Error> {
        // 1. Create DirectoryEntry for the new node
        let node_id = node.id().to_string();
        let directory_entry = DirectoryEntry {
            name: name.clone(),
            child_node_id: node_id.clone(),
        };
        
        // 2. Update in-memory cache immediately
        let cached_entry = CachedEntry {
            node_ref: node.clone(),
            last_accessed: SystemTime::now(),
            is_dirty: true,
            directory_entry: directory_entry.clone(),
        };
        self.entry_cache.borrow_mut().insert(name.clone(), cached_entry);
        
        // 3. Mark directory as dirty for next commit
        *self.cache_dirty.borrow_mut() = true;
        
        // 4. Update metadata
        {
            let mut metadata = self.metadata.borrow_mut();
            metadata.modified_at = SystemTime::now();
            metadata.entry_count += 1;
        }
        
        // 5. Add to TinyLogFS transaction builders (if reference available)
        if let Some(tinylogfs_ref) = self.tinylogfs.upgrade() {
            let operation = FilesystemOperation::UpdateDirectory {
                node_id: self.node_id.clone(),
                entries: self.get_all_entries()?,
            };
            tinylogfs_ref.borrow_mut().add_pending_operation(operation)?;
        }
        
        Ok(())
    }
    
    fn remove(&mut self, name: &str) -> Result<Option<NodeRef>, tinyfs::Error> {
        // 1. Remove from cache and get old value
        let old_entry = self.entry_cache.borrow_mut().remove(name);
        
        if old_entry.is_some() {
            // 2. Mark directory as dirty for commit
            *self.cache_dirty.borrow_mut() = true;
            
            // 3. Update metadata
            {
                let mut metadata = self.metadata.borrow_mut();
                metadata.modified_at = SystemTime::now();
                metadata.entry_count = metadata.entry_count.saturating_sub(1);
            }
            
            // 4. Add to TinyLogFS transaction builders
            if let Some(tinylogfs_ref) = self.tinylogfs.upgrade() {
                let operation = FilesystemOperation::UpdateDirectory {
                    node_id: self.node_id.clone(),
                    entries: self.get_all_entries()?,
                };
                tinylogfs_ref.borrow_mut().add_pending_operation(operation)?;
            }
        }
        
        Ok(old_entry.map(|e| e.node_ref))
    }
    
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>, tinyfs::Error> {
        // Load all entries from OpLog and cache, then return iterator
        let entries = self.load_all_entries()?;
        let iter = entries.into_iter().map(|(name, node_ref)| (name, node_ref));
        Ok(Box::new(iter))
    }
}

impl OpLogDirectory {
    pub fn new(
        node_id: String,
        tinylogfs: Weak<RefCell<TinyLogFS>>
    ) -> Result<Self, TinyLogFSError> {
        Ok(Self {
            node_id: node_id.clone(),
            oplog_partition_id: node_id, // Directory is its own partition
            entry_cache: RefCell::new(BTreeMap::new()),
            cache_dirty: RefCell::new(false),
            tinylogfs,
            metadata: RefCell::new(DirectoryMetadata {
                created_at: SystemTime::now(),
                modified_at: SystemTime::now(),
                entry_count: 0,
                custom_attributes: serde_json::Value::Object(Default::default()),
            }),
            last_loaded: RefCell::new(SystemTime::now()),
        })
    }
    
    fn load_directory_entries_from_oplog(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Get TinyLogFS reference for OpLog access
        let tinylogfs_ref = self.tinylogfs.upgrade()
            .ok_or_else(|| TinyLogFSError::Transaction { 
                message: "TinyLogFS reference dropped".to_string() 
            })?;
        
        // Query: SELECT * FROM oplog_entries WHERE part_id = ? ORDER BY timestamp DESC
        let query = format!(
            "SELECT content FROM oplog_entries WHERE part_id = '{}' AND file_type = 'directory' ORDER BY timestamp DESC LIMIT 1",
            self.node_id
        );
        
        let results = tinylogfs_ref.borrow().query_history(&query)?;
        
        if let Some(batch) = results.first() {
            // Extract content column and deserialize as OplogEntry
            let content_array = batch.column_by_name("content")
                .ok_or_else(|| TinyLogFSError::Transaction { 
                    message: "Missing content column".to_string() 
                })?;
            
            if let Some(content_bytes) = content_array.as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .and_then(|arr| if arr.len() > 0 { Some(arr.value(0)) } else { None })
            {
                let oplog_entry: OplogEntry = deserialize_oplog_entry(content_bytes)?;
                let directory_entries: Vec<DirectoryEntry> = deserialize_directory_entries(&oplog_entry.content)?;
                Ok(directory_entries)
            } else {
                Ok(vec![]) // Empty directory
            }
        } else {
            Ok(vec![]) // Directory not found in OpLog
        }
    }
    
    fn reconstruct_node_ref(&self, child_node_id: &str) -> Result<NodeRef, TinyLogFSError> {
        // Get TinyLogFS reference
        let tinylogfs_ref = self.tinylogfs.upgrade()
            .ok_or_else(|| TinyLogFSError::Transaction { 
                message: "TinyLogFS reference dropped".to_string() 
            })?;
        
        // Query OpLog for child node entry
        let query = format!(
            "SELECT file_type, content FROM oplog_entries WHERE node_id = '{}' ORDER BY timestamp DESC LIMIT 1",
            child_node_id
        );
        
        let results = tinylogfs_ref.borrow().query_history(&query)?;
        
        if let Some(batch) = results.first() {
            // Determine node type and construct appropriate NodeRef
            // This would involve creating File, Directory, or Symlink nodes
            // with lazy loading from OpLog
            
            // For now, return placeholder - full implementation would
            // create proper NodeRef instances based on file_type
            todo!("Implement NodeRef reconstruction from OpLog data")
        } else {
            Err(TinyLogFSError::NodeNotFound { 
                path: PathBuf::from(child_node_id) 
            })
        }
    }
    
    fn cache_entry(&self, name: String, node_ref: NodeRef, directory_entry: DirectoryEntry) {
        let cached_entry = CachedEntry {
            node_ref,
            last_accessed: SystemTime::now(),
            is_dirty: false, // Fresh from OpLog
            directory_entry,
        };
        self.entry_cache.borrow_mut().insert(name, cached_entry);
    }
    
    fn get_all_entries(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Collect all cached entries as DirectoryEntry records
        let cache = self.entry_cache.borrow();
        let entries: Vec<DirectoryEntry> = cache.values()
            .map(|cached| cached.directory_entry.clone())
            .collect();
        Ok(entries)
    }
    
    pub fn get_status(&self) -> DirectoryStatus {
        let cache = self.entry_cache.borrow();
        let metadata = self.metadata.borrow();
        DirectoryStatus {
            cached_entries: cache.len(),
            is_dirty: *self.cache_dirty.borrow(),
            last_loaded: Some(*self.last_loaded.borrow()),
            entry_count: metadata.entry_count,
        }
    }
}

#[derive(Debug)]
pub struct DirectoryStatus {
    pub cached_entries: usize,
    pub is_dirty: bool,
    pub last_loaded: Option<SystemTime>,
    pub entry_count: usize,
}
```

##### Integration with TinyFS using Rc<RefCell<_>>

**Reference Management**: OpLogDirectory uses `Weak<RefCell<TinyLogFS>>` to avoid circular references while maintaining access to the parent filesystem for transaction operations.

**Factory Pattern**: TinyLogFS will provide a directory factory that creates OpLogDirectory instances:

```rust
impl TinyLogFS {
    fn create_directory_impl(&self, node_id: String) -> Result<Box<dyn Directory>, TinyLogFSError> {
        let weak_ref = Rc::downgrade(&Rc::new(RefCell::new(self.clone())));
        let oplog_dir = OpLogDirectory::new(node_id, weak_ref)?;
        Ok(Box::new(oplog_dir))
    }
}
```

**Transaction Integration**: Directory modifications automatically add operations to the TinyLogFS transaction builders, ensuring that directory changes are included in the next commit.

**Lazy Loading Strategy**: Directory contents are loaded from OpLog only when first accessed, and results are cached for subsequent operations. This maintains TinyFS's performance characteristics while providing persistence.

#### Phase 4: Enhanced Table Provider with Builder Snapshotting 🔄 UPDATED SPECIFICATION

**Design Objective**: Enhance the existing table providers to support snapshotting transaction builders to RecordBatch on-the-fly, allowing SQL queries to see pending transactions that haven't been committed yet.

##### Enhanced Table Provider Architecture

```rust
pub struct EnhancedOplogEntryTable {
    // Existing Delta Lake data source
    delta_table: Option<DeltaTable>,
    
    // Live transaction data from TinyLogFS
    tinylogfs_ref: Option<Weak<RefCell<TinyLogFS>>>,
    
    // Schema compatibility
    schema: Arc<Schema>,
    
    // Configuration for including pending transactions
    include_pending: bool,
}

impl TableProvider for EnhancedOplogEntryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(EnhancedOplogEntryExec::new(
            self.delta_table.clone(),
            self.tinylogfs_ref.clone(),
            projection.cloned(),
            self.include_pending,
        )))
    }
}

pub struct EnhancedOplogEntryExec {
    delta_table: Option<DeltaTable>,
    tinylogfs_ref: Option<Weak<RefCell<TinyLogFS>>>,
    projection: Option<Vec<usize>>,
    include_pending: bool,
}

impl ExecutionPlan for EnhancedOplogEntryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        // Return appropriate schema based on projection
        if let Some(projection) = &self.projection {
            let fields: Vec<_> = projection.iter()
                .map(|&i| OPLOG_ENTRY_SCHEMA.field(i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        } else {
            OPLOG_ENTRY_SCHEMA.clone()
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition > 0 {
            return Err(DataFusionError::Execution(
                "EnhancedOplogEntryExec only supports single partition".to_string(),
            ));
        }

        // Create stream that combines Delta Lake data with pending transactions
        let stream = EnhancedOplogEntryStream::new(
            self.delta_table.clone(),
            self.tinylogfs_ref.clone(),
            self.projection.clone(),
            self.include_pending,
        );
        
        Ok(Box::pin(stream))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "EnhancedOplogEntryExec cannot have children".to_string(),
            ))
        }
    }
}

struct EnhancedOplogEntryStream {
    // Delta Lake stream (committed data)
    delta_stream: Option<SendableRecordBatchStream>,
    
    // Pending transaction data
    pending_batch: Option<RecordBatch>,
    pending_sent: bool,
    
    // State
    schema: Arc<Schema>,
}

impl EnhancedOplogEntryStream {
    fn new(
        delta_table: Option<DeltaTable>,
        tinylogfs_ref: Option<Weak<RefCell<TinyLogFS>>>,
        projection: Option<Vec<usize>>,
        include_pending: bool,
    ) -> Self {
        // Initialize Delta Lake stream for committed data
        let delta_stream = delta_table.map(|table| {
            // Create appropriate stream from Delta Lake
            // This would involve DataFusion's Delta Lake table provider
            todo!("Create Delta Lake stream")
        });
        
        // Snapshot pending transactions if requested
        let pending_batch = if include_pending {
            if let Some(tinylogfs_weak) = &tinylogfs_ref {
                if let Some(tinylogfs_ref) = tinylogfs_weak.upgrade() {
                    // Snapshot current transaction builders to RecordBatch
                    let tinylogfs = tinylogfs_ref.borrow();
                    let transaction_state = tinylogfs.transaction_state.borrow();
                    
                    // Create a clone of builders for snapshotting without affecting the original
                    if transaction_state.operation_count > 0 {
                        match transaction_state.snapshot_to_record_batch() {
                            Ok(batch) => Some(batch),
                            Err(_) => None, // Log error but continue
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        
        Self {
            delta_stream,
            pending_batch,
            pending_sent: false,
            schema: OPLOG_ENTRY_SCHEMA.clone(), // Apply projection if needed
        }
    }
}

impl RecordBatchStream for EnhancedOplogEntryStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Stream for EnhancedOplogEntryStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First, emit pending transactions (if any)
        if let Some(pending_batch) = &self.pending_batch {
            if !self.pending_sent {
                self.pending_sent = true;
                return Poll::Ready(Some(Ok(pending_batch.clone())));
            }
        }
        
        // Then, emit committed data from Delta Lake
        if let Some(ref mut delta_stream) = self.delta_stream {
            delta_stream.poll_next_unpin(cx)
        } else {
            Poll::Ready(None)
        }
    }
}
```

##### Integration with TinyLogFS Commands

**Enhanced Query Support**: The enhanced table provider enables SQL queries that can see both committed data and pending transactions:

```rust
impl TinyLogFS {
    /// Query with option to include pending transactions
    pub async fn query_with_pending(&self, sql: &str, include_pending: bool) -> Result<Vec<RecordBatch>, TinyLogFSError> {
        // Create enhanced table provider that can snapshot transaction builders
        let table_provider = EnhancedOplogEntryTable::new(
            self.get_delta_table()?,
            Some(Rc::downgrade(&Rc::new(RefCell::new(self.clone())))),
            include_pending,
        );
        
        // Execute query with DataFusion
        let ctx = SessionContext::new();
        ctx.register_table("oplog_entries", Arc::new(table_provider))?;
        
        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;
        
        Ok(results)
    }
}
```

**CLI Usage**: This enables powerful CLI commands that can show both committed and pending state:

```bash
# Show only committed data
pond query "SELECT * FROM oplog_entries WHERE file_type = 'file'"

# Show committed data + pending transactions
pond query --include-pending "SELECT * FROM oplog_entries WHERE file_type = 'file'"

# Show current status including pending operations
pond status --show-pending
```

**Test Scenario Implementation**: Your specific test scenario can now be implemented:

```rust
#[tokio::test]
async fn test_create_file_and_symlink_with_pending_queries() -> Result<(), TinyLogFSError> {
    // Initialize pond
    let mut tinylogfs = TinyLogFS::init_empty("test_pond").await?;
    
    // Create file "A"
    tinylogfs.create_file(Path::new("/A"), b"content of file A").await?;
    
    // Create symlink "B" -> "A"  
    tinylogfs.create_symlink(Path::new("/B"), Path::new("/A")).await?;
    
    // Query pending transactions before commit (should show 2 entries)
    let pending_results = tinylogfs.query_with_pending(
        "SELECT name, file_type FROM oplog_entries ORDER BY timestamp",
        true
    ).await?;
    assert_eq!(pending_results.len(), 1); // One batch
    let batch = &pending_results[0];
    assert_eq!(batch.num_rows(), 2); // Two operations
    
    // Commit the transaction
    let commit_result = tinylogfs.commit().await?;
    assert_eq!(commit_result.operations_committed, 2);
    
    // Query committed data (should show same 2 entries, now persisted)
    let committed_results = tinylogfs.query_history(
        "SELECT name, file_type FROM oplog_entries ORDER BY timestamp"
    ).await?;
    assert_eq!(committed_results.len(), 1);
    let batch = &committed_results[0];
    assert_eq!(batch.num_rows(), 2);
    
    Ok(())
}
```

##### Benefits of Enhanced Architecture

**Real-time Visibility**: SQL queries can see pending operations before they're committed, enabling powerful debugging and development workflows.

**Batch Efficiency**: Arrow Array builders accumulate operations efficiently in columnar format.

**Type Safety**: Compile-time guarantees about schema compatibility between pending and committed data.

**Performance**: Minimal overhead for queries that don't need pending data (include_pending = false).

**Flexibility**: Easy to extend with additional metadata or operation types.

##### Transaction Model Refinement

**Design Principle**: TinyLogFS operates with "accumulate-then-commit" transactions, where filesystem operations build up in Arrow Array builders and are committed as RecordBatch entries to the OpLog. The enhanced table provider can snapshot these builders for real-time queries.

**Transaction Flow**:
1. **Operation**: Filesystem operation updates in-memory TinyFS and adds to transaction builders
2. **Accumulation**: Multiple operations build up arrays in the TransactionState  
3. **Live Query**: Enhanced table provider can snapshot builders for SQL queries
4. **Commit**: `commit()` snapshots builders to RecordBatch and writes to Delta Lake
5. **Reset**: Builders are reset for next batch of operations

**Snapshot Strategy**: Builder snapshotting creates a copy of the current arrays without affecting the original builders, allowing queries to see pending state while operations continue to accumulate.

**Error Recovery**: If commit fails, the transaction builders retain their state and commit can be retried. The enhanced table provider ensures consistent views of both committed and pending data.
        
        self.entry_cache.write().unwrap().insert(name.clone(), cached_entry);
        
        // 2. Mark directory as dirty for next sync
        self.cache_dirty.store(true, Ordering::Relaxed);
        
        // 3. Update metadata
        self.metadata.modified_at = SystemTime::now();
        self.metadata.entry_count += 1;
        
        // 4. Mark in TinyLogFS dirty tracking (if reference available)
        if let Some(tinylogfs) = self.tinylogfs.upgrade() {
            tinylogfs.mark_node_dirty(&self.node_id);
        }
        
        Ok(())
    }
    
    fn remove(&mut self, name: &str) -> Result<Option<NodeRef>, tinyfs::Error> {
        // 1. Remove from cache and get old value
        let old_entry = self.entry_cache.write().unwrap().remove(name);
        
        if old_entry.is_some() {
            // 2. Mark directory as dirty for sync
            self.cache_dirty.store(true, Ordering::Relaxed);
            
            // 3. Update metadata
            self.metadata.modified_at = SystemTime::now();
            self.metadata.entry_count = self.metadata.entry_count.saturating_sub(1);
            
            // 4. Mark in TinyLogFS dirty tracking
            if let Some(tinylogfs) = self.tinylogfs.upgrade() {
                tinylogfs.mark_node_dirty(&self.node_id);
            }
        }
        
        Ok(old_entry.map(|e| e.node_ref))
    }
    
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>, tinyfs::Error> {
        // Load all entries from OpLog and return iterator
        let entries = self.load_all_entries()?;
        let iter = entries.into_iter().map(|(name, node_ref)| (name, node_ref));
        Ok(Box::new(iter))
    }
}

impl OpLogDirectory {
    pub async fn new(
        node_id: String,
        tinylogfs: Weak<TinyLogFS>
    ) -> Result<Self, TinyLogFSError> {
        // Initialize directory with empty cache
        // Load will happen lazily on first access
    }
    
    pub async fn load_from_oplog(&mut self) -> Result<(), TinyLogFSError> {
        // Query OpLog for all DirectoryEntry records in this partition
        // Populate entry_cache with all entries
        // Update metadata from OpLog
    }
    
    async fn load_directory_entries_from_oplog(&self) -> Result<Vec<DirectoryEntry>, TinyLogFSError> {
        // Query: SELECT * FROM oplog_entries WHERE part_id = ? ORDER BY timestamp DESC
        // Deserialize content as DirectoryEntry records
        // Return latest state for each entry name
    }
    
    fn reconstruct_node_ref(&self, child_node_id: &str) -> Result<NodeRef, TinyLogFSError> {
        // Query OpLog for child node entry
        // Determine node type (file, directory, symlink)
        // Construct appropriate NodeRef with lazy loading
    }
    
    fn cache_entry(&self, name: String, node_ref: NodeRef) {
        // Add entry to cache with current timestamp
        // Optionally implement cache size limits and LRU eviction
    }
    
    pub async fn sync_to_oplog(&self) -> Result<(), TinyLogFSError> {
        // Only sync if cache_dirty is true
        // Serialize all cached entries as DirectoryEntry records
        // Write to OpLog as single OplogEntry with nested DirectoryEntry content
        // Clear dirty flag
    }
    
    pub fn get_status(&self) -> DirectoryStatus {
        // Return cache statistics and dirty state
    }
}

#[derive(Debug)]
pub struct DirectoryStatus {
    pub cached_entries: usize,
    pub is_dirty: bool,
    pub last_loaded: Option<SystemTime>,
    pub entry_count: usize,
}
```

##### Integration with TinyFS

**Replacement Strategy**: OpLogDirectory will implement the existing `Directory` trait, making it a drop-in replacement for `MemoryDirectory` in TinyFS nodes.

**Factory Pattern**: TinyLogFS will provide a directory factory that creates OpLogDirectory instances instead of MemoryDirectory:

```rust
impl TinyLogFS {
    fn create_directory_impl(&self, node_id: String) -> Result<Box<dyn Directory>, TinyLogFSError> {
        let oplog_dir = OpLogDirectory::new(
            node_id,
            Arc::downgrade(&Arc::new(self.clone())) // Weak reference to avoid cycles
        )?;
        Ok(Box::new(oplog_dir))
    }
}
```

**Lazy Loading Strategy**: Directory contents are loaded from OpLog only when first accessed, maintaining TinyFS's performance characteristics for hot paths while providing persistence for cold data.
```

#### Phase 5: Data Flow Design 🔄 SIMPLIFIED SPECIFICATION

**Design Objective**: Define streamlined data flow patterns that leverage Arrow Array builders and single-threaded architecture for optimal performance and simplicity.

##### Write Operations Flow

```
User Operation → TinyLogFS API → In-Memory FS Update → Transaction Builders → Commit → OpLog Persistence
```

**Detailed Write Flow**:

1. **User Invokes Operation**: `create_file()`, `update_file()`, `create_directory()`, etc.

2. **TinyLogFS Processes Request**:
   - Validates path and permissions
   - Generates NodeID for new nodes (UUID v4)
   - Determines partition ID (parent directory for files/symlinks, self for directories)

3. **In-Memory FS Update**:
   - Creates/updates TinyFS nodes immediately
   - OpLogDirectory instances update their entry caches
   - File content stored in memory for fast subsequent access

4. **Transaction Builder Update**:
   - Operation added to TransactionState builders (part_id, timestamp, version, content)
   - Builders accumulate operations in columnar format
   - Node metadata updated in local HashMap

5. **Commit** (on explicit `commit()` call or CLI exit):
   - Snapshot builders to RecordBatch
   - Write RecordBatch to Delta Lake in single transaction
   - Reset builders for next batch
   - Update sync timestamp

**Write Operation Examples**:
```rust
// File operations
create_file("/path/to/file.txt", b"content") 
→ Builders: part_id="parent_dir_id", content=OplogEntry{file_type:"file", content:b"content"}

// Directory operations  
create_directory("/path/to/dir")
→ Builders: part_id="dir_id", content=OplogEntry{file_type:"directory", content:encoded_entries}

// Symlink operations
create_symlink("/path/to/link", "/target/path")
→ Builders: part_id="parent_dir_id", content=OplogEntry{file_type:"symlink", content:b"/target/path"}
```

##### Read Operations Flow

```
User Request → Memory FS Lookup → Cache Hit/Miss → OpLog Query (if needed) → Response
```

**Detailed Read Flow**:

1. **Fast Path (Memory Hit)**:
   - Check TinyFS in-memory nodes first
   - Return immediately if node exists and is cached

2. **Medium Path (Memory Miss, OpLog Hit)**:
   - Query OpLog using enhanced table provider
   - Deserialize OplogEntry to reconstruct node
   - Add to TinyFS memory for future access
   - Return reconstructed data

3. **Slow Path (Complete Miss)**:
   - Return "not found" error
   - No additional queries for non-existent paths

**Read Query Patterns**:
```sql
-- Get specific file content
SELECT content FROM oplog_entries WHERE node_id = ?

-- Get directory contents (with pending transactions visible)
SELECT content FROM oplog_entries WHERE node_id = ? AND file_type = 'directory'

-- Get file metadata only (projected query)
SELECT part_id, node_id, file_type FROM oplog_entries WHERE node_id = ?
```

##### Commit Operations Flow

```
Commit Request → Builder Snapshot → RecordBatch Creation → Delta Lake Write → Builder Reset
```

**Detailed Commit Flow**:

1. **Snapshot Builders**:
   - Copy current Arrow Array builder contents
   - Create arrays for part_id, timestamp, version, content columns
   - Preserve builder state for continued operations

2. **Create RecordBatch**:
   - Combine arrays into single RecordBatch
   - Apply proper schema validation
   - Include operation metadata

3. **Delta Lake Transaction**:
   - Write RecordBatch to Delta Lake store
   - Use part_id for partitioning
   - Ensure ACID guarantees

4. **Reset State**:
   - Clear builders for next batch
   - Update sync timestamps
   - Mark all nodes as clean

##### Restore Operations Flow  

```
OpLog Query → Timestamp Filtering → Operation Replay → Memory FS Reconstruction
```

**Detailed Restore Flow**:

1. **Query OpLog State**:
   ```sql
   SELECT * FROM oplog_entries 
   WHERE timestamp <= ? 
   ORDER BY part_id, node_id, timestamp DESC
   ```

2. **Determine Latest State**:
   - Group by (part_id, node_id) 
   - Take most recent entry for each node
   - Handle deletions appropriately

3. **Reconstruct Filesystem Tree**:
   - Start with root directory node
   - Recursively build directory tree from DirectoryEntry records  
   - Create File and Symlink nodes as referenced

4. **Populate TinyFS**:
   - Clear existing in-memory FS
   - Create nodes in dependency order (directories before contents)
   - Set up all NodeRef relationships
   - Initialize clean state (no pending operations)

##### Consistency Guarantees

**ACID Properties**:
- **Atomicity**: All operations in transaction builders committed as single RecordBatch
- **Consistency**: TinyFS constraints enforced before adding to builders
- **Isolation**: In-memory FS provides consistent view within CLI session
- **Durability**: Delta Lake guarantees persistence after successful commit

**Error Handling**:
- **Commit Failure**: Builder state preserved, commit can be retried
- **Restore Failure**: Previous memory state preserved, error reported
- **Builder Overflow**: Automatic commit when builders reach size threshold
- **Corruption Detection**: Schema validation during RecordBatch creation

##### Performance Optimizations

**Arrow Builder Benefits**:
- **Columnar Efficiency**: Optimal memory layout for batch operations
- **Type Safety**: Compile-time schema validation
- **Zero-Copy**: Direct conversion to RecordBatch
- **Compression**: Arrow's built-in compression for large datasets

**Caching Strategy**:
- **Hot Data**: Recently accessed nodes stay in TinyFS memory
- **Directory Entries**: OpLogDirectory caches entry mappings in RefCell
- **Metadata**: Node metadata cached separately from content
- **Simple Eviction**: LRU eviction for directory caches when needed

**Query Optimizations**:
- **Partition Pruning**: Queries filtered by part_id for directory locality
- **Column Projection**: Enhanced table provider respects projections
- **Pending Visibility**: Optional inclusion of uncommitted transactions
- **Schema Reuse**: Consistent schema across committed and pending data

#### Phase 6: Partitioning Strategy and CLI Extensions

**Partition Strategy**:
- **Partition Key**: `part_id` (parent directory UUID for files/symlinks, self UUID for directories)
- **Sort Key**: `timestamp` (operation order within partition)
- **Benefits**: 
  - Query locality for directory operations
  - Parallel restoration of independent subtrees
  - Efficient time-travel queries for individual nodes

**CLI Extensions** (Updated for simplified architecture):

```rust
enum Commands {
    Init,                    // Existing: Create empty pond
    Show,                    // Existing: Display operation log
    
    // New filesystem commands  
    Ls { 
        path: Option<String>,
        long: bool,          // -l flag for detailed output
        pending: bool,       // --pending flag to show uncommitted changes
    },
    Cat { 
        path: String 
    },
    Mkdir { 
        path: String,
        parents: bool,       // -p flag for parent creation
    },
    Touch { 
        path: String 
    },
    Rm {
        path: String,
        recursive: bool,     // -r flag for directories
    },
    Ln {
        target: String,
        link: String,
    },
    
    // Transaction commands
    Commit {
        message: Option<String>, // Optional commit message
    },
    Status {
        pending: bool,       // --pending flag to show uncommitted operations
    },
    
    // Query commands
    Query {
        sql: String,
        pending: bool,       // --pending flag to include uncommitted data
    },
}
```

**Test Scenario Implementation**: Your specific test can now be implemented simply:

```rust
#[tokio::test]
async fn test_create_file_and_symlink_with_enhanced_queries() -> Result<(), TinyLogFSError> {
    // Initialize pond  
    let mut tinylogfs = TinyLogFS::init_empty("test_pond").await?;
    
    // Create file "A"
    tinylogfs.create_file(Path::new("/A"), b"content of file A").await?;
    
    // Create symlink "B" -> "A"
    tinylogfs.create_symlink(Path::new("/B"), Path::new("/A")).await?;
    
    // Query with pending transactions visible (should show 2 entries)
    let results = tinylogfs.query_with_pending(
        "SELECT COUNT(*) as count FROM oplog_entries",
        true  // include_pending = true
    ).await?;
    
    // Verify 2 operations are visible before commit
    assert_eq!(extract_count(&results), 2);
    
    // Commit the transaction
    let commit_result = tinylogfs.commit().await?;
    assert_eq!(commit_result.operations_committed, 2);
    
    // Query committed data (should show same 2 entries, now persisted)
    let committed_results = tinylogfs.query_history(
        "SELECT COUNT(*) as count FROM oplog_entries"
    ).await?;
    assert_eq!(extract_count(&committed_results), 2);
    
    println!("✅ Test passed: Created file A and symlink B→A, committed, verified 2 entries");
    Ok(())
}
```

### Integration with Existing Components

#### Command-Line Interface Extensions

Extend CMD crate with new commands:

```rust
enum Commands {
    Init,                    // Existing: Create empty pond
    Show,                    // Existing: Display operation log
    
    // New TinyLogFS commands
    Ls { path: Option<String> },           // List directory contents  
    Cat { path: String },                  // Display file contents
    Mkdir { path: String },                // Create directory
    Touch { path: String },                // Create empty file
    Sync,                                  // Force sync to OpLog
    Restore,                               // Rebuild from OpLog
    Status,                                // Show dirty/clean state
}
```

#### Mirror Synchronization (Future Phase)

Create local filesystem mirror from TinyLogFS state:

```rust
pub struct MirrorSync {
    tinylogfs: TinyLogFS,
    mirror_root: PathBuf,
}

impl MirrorSync {
    pub async fn sync_to_filesystem(&self) -> Result<()> {
        // Write TinyLogFS state to actual files on disk
    }
    
    pub async fn detect_external_changes(&self) -> Result<Vec<Change>> {
        // Compare filesystem with TinyLogFS state
    }
}
```

## Proof-of-Concept Compatibility

The new TinyLogFS system maintains compatibility with proof-of-concept concepts:

- **Resource Management**: YAML-driven resource definitions
- **Data Pipeline**: HydroVu → Arrow → TinyLogFS → Parquet export
- **Backup/Restore**: Delta Lake provides better consistency than individual files
- **Directory Abstraction**: Enhanced `TreeLike` trait implementation using TinyLogFS

## Technologies and Dependencies

### Core Stack
- **Language**: Rust 2021 edition
- **Data Storage**: Apache Arrow, Parquet, Delta Lake (delta-rs 0.26)
- **Query Engine**: DataFusion 47.0.0 (replacing DuckDB)
- **Filesystem**: TinyFS abstraction + OpLog persistence
- **CLI**: clap 4.x for command-line interface

### Integration Dependencies
- **Arrow IPC**: For serializing filesystem operation data
- **Delta Lake Partitioning**: For efficient node-based queries
- **DataFusion SQL**: For filesystem history and analytics queries
- **Tokio**: For async operations in persistence layer

## Development Phases

### Phase 1: Core Integration ✅ COMPLETE
- [x] TinyFS implementation complete
- [x] OpLog implementation complete  
- [x] CMD implementation complete
- [x] **Design TinyLogFS schemas and architecture**
- [x] **Implement OplogEntry and DirectoryEntry structures**
- [x] **Create OplogEntry and DirectoryEntry table providers**
- [x] **Update CMD to use new TinyLogFS structures**
- [x] **End-to-end testing with pond init/show commands**
- [x] **DataFusion projection fix for custom table providers**

**Phase 1 Achievements**:
- ✅ **Solid Foundation**: TinyLogFS schema design proven with end-to-end testing
- ✅ **DataFusion Integration**: Custom table providers work correctly with projection
- ✅ **CLI Integration**: Both `pond init` and `pond show` commands work with OplogEntry schema
- ✅ **Architecture Validation**: Two-layer storage approach (Delta Lake + Arrow IPC) is effective

### Phase 2: Basic Operations 🚀 COMPREHENSIVE DESIGN

**Design Objective**: Implement the complete TinyLogFS hybrid filesystem with all basic file operations, CLI commands, and sync/restore functionality.

##### 2.1: Core TinyLogFS Implementation

**Priority**: Implement the main `TinyLogFS` struct with hybrid architecture

- **TinyLogFS struct**: Core hybrid filesystem combining tinyfs::FS with OpLog persistence  
- **State management**: Dirty tracking, node caching, transaction handling
- **Factory methods**: `new()`, `init_empty()`, proper initialization from existing OpLog stores
- **Error handling**: Comprehensive `TinyLogFSError` type with proper error propagation

**API Design**: Full implementation of the Phase 2 API specification including:
- File operations: `create_file()`, `read_file()`, `update_file()`, `delete()`  
- Directory operations: `create_directory()`, `list_directory()`
- Symlink operations: `create_symlink()`, resolve symlink targets
- Metadata operations: `get_metadata()`, `exists()`, `get_status()`

##### 2.2: OpLog-Backed Directory Implementation

**Priority**: Replace MemoryDirectory with persistent OpLogDirectory

- **OpLogDirectory struct**: Implementation of Directory trait with OpLog persistence
- **Caching strategy**: Entry cache with dirty tracking and LRU eviction  
- **Lazy loading**: Directory contents loaded from OpLog only when accessed
- **Integration**: Drop-in replacement for MemoryDirectory in TinyFS

**Performance Features**:
- Fast path: Cached entries served from memory
- Slow path: OpLog queries for cache misses with result caching
- Batch updates: Multiple directory changes synced together
- Consistency: Directory state always consistent with OpLog

##### 2.3: Sync and Restore Functionality

**Priority**: Implement persistence mechanisms for filesystem state

- **Sync operations**: `sync()`, `sync_nodes()` with transaction safety
- **Restore operations**: `restore_from_oplog()`, `restore_to_timestamp()`
- **Transaction handling**: Atomic operations with rollback on failure
- **State validation**: Integrity checks during restore operations

**Data Flow Implementation**:
- Write path: Memory updates → dirty tracking → batch OpLog sync
- Read path: Memory cache → OpLog query → cache population  
- Restore path: OpLog query → operation replay → memory reconstruction

##### 2.4: CLI Extensions for Filesystem Operations

**Priority**: Extend CMD crate with filesystem commands

```rust
enum Commands {
    // Existing pond management
    Init,                    // ✅ Create empty pond
    Show,                    // ✅ Display operation log
    
    // New filesystem commands  
    Ls { 
        path: Option<String>,
        long: bool,      // -l flag for detailed output
        all: bool,       // -a flag for hidden files
    },
    Cat { 
        path: String 
    },
    Mkdir { 
        path: String,
        parents: bool,   // -p flag for parent creation
    },
    Touch { 
        path: String 
    },
    Rm {
        path: String,
        recursive: bool, // -r flag for directories
        force: bool,     // -f flag to ignore errors
    },
    Ln {
        target: String,
        link: String,
        symbolic: bool,  // -s flag for symlinks
    },
    
    // Persistence commands
    Sync {
        nodes: Option<Vec<String>>, // Specific nodes to sync
    },
    Restore {
        timestamp: Option<String>,  // ISO timestamp for point-in-time restore
    },
    Status,                         // Show dirty/clean state
    
    // Query commands
    History { 
        path: Option<String>,       // History for specific path
        limit: Option<usize>,       // Number of entries
    },
    Query {
        sql: String,                // Raw SQL query over filesystem data
    },
}
```

**Command Implementation Details**:

```rust
// ls command - List directory contents
async fn cmd_ls(tinylogfs: &TinyLogFS, path: Option<String>, long: bool, all: bool) -> Result<()> {
    let path = path.unwrap_or_else(|| "/".to_string());
    let entries = tinylogfs.list_directory(Path::new(&path)).await?;
    
    for entry in entries {
        if long {
            // Show detailed information: permissions, size, modified time
            let metadata = tinylogfs.get_metadata(&entry.path).await?;
            println!("{} {} {} {}", 
                format_permissions(&metadata),
                format_size(metadata.size),
                format_time(&metadata.modified_at),
                entry.name
            );
        } else {
            println!("{}", entry.name);
        }
    }
    Ok(())
}

// cat command - Display file contents
async fn cmd_cat(tinylogfs: &TinyLogFS, path: String) -> Result<()> {
    let content = tinylogfs.read_file(Path::new(&path)).await?;
    
    // Try to display as UTF-8 text, fall back to hex dump for binary
    match std::str::from_utf8(&content) {
        Ok(text) => print!("{}", text),
        Err(_) => {
            println!("Binary file detected, showing hex dump:");
            for (i, chunk) in content.chunks(16).enumerate() {
                print!("{:08x}: ", i * 16);
                for byte in chunk {
                    print!("{:02x} ", byte);
                }
                println!();
            }
        }
    }
    Ok(())
}

// mkdir command - Create directories  
async fn cmd_mkdir(tinylogfs: &mut TinyLogFS, path: String, parents: bool) -> Result<()> {
    if parents {
        // Create parent directories as needed
        let path_obj = Path::new(&path);
        if let Some(parent) = path_obj.parent() {
            if !tinylogfs.exists(parent) {
                tinylogfs.create_directory(parent).await?;
            }
        }
    }
    
    tinylogfs.create_directory(Path::new(&path)).await?;
    println!("Created directory: {}", path);
    Ok(())
}

// sync command - Persist dirty changes
async fn cmd_sync(tinylogfs: &mut TinyLogFS, nodes: Option<Vec<String>>) -> Result<()> {
    let result = match nodes {
        Some(node_ids) => tinylogfs.sync_nodes(&node_ids).await?,
        None => tinylogfs.sync().await?,
    };
    
    println!("Sync completed:");
    println!("  Nodes synced: {}", result.nodes_synced);
    println!("  Duration: {:?}", result.sync_duration);
    println!("  Bytes written: {}", result.bytes_written);
    println!("  Transaction ID: {}", result.transaction_id);
    Ok(())
}

// status command - Show filesystem status
async fn cmd_status(tinylogfs: &TinyLogFS) -> Result<()> {
    let status = tinylogfs.get_status();
    
    println!("TinyLogFS Status:");
    println!("  Total nodes: {}", status.total_nodes);
    println!("  Dirty nodes: {}", status.dirty_nodes);
    println!("  Cached nodes: {}", status.cached_nodes);
    println!("  Memory usage: {} bytes", status.memory_usage);
    
    match status.last_sync {
        Some(timestamp) => println!("  Last sync: {:?}", timestamp),
        None => println!("  Last sync: Never"),
    }
    
    Ok(())
}
```

**CLI Integration Strategy**:
- **Environment Variables**: Use `POND` environment variable for store path
- **Error Handling**: User-friendly error messages with suggestions
- **Output Formatting**: Consistent formatting across all commands
- **Help System**: Comprehensive help text and examples for each command
- **Auto-sync**: Automatically sync before CLI exit unless `--no-sync` flag provided

**Testing Strategy**:
- **Integration Tests**: End-to-end tests for each CLI command
- **Error Cases**: Test error handling and edge cases
- **Performance Tests**: Verify CLI commands perform adequately with large filesystems
- **Compatibility Tests**: Ensure CLI works across different platforms and terminal types

### Phase 3: Advanced Features
- [ ] Incremental sync optimization
- [ ] Filesystem history queries via SQL
- [ ] Local mirror synchronization
- [ ] Performance optimization and caching
- [ ] Migration tools from proof-of-concept format

### Phase 4: Production Features
- [ ] Comprehensive error recovery
- [ ] Backup and restore workflows  
- [ ] Integration with proof-of-concept pipelines
- [ ] Performance benchmarking and optimization
- [ ] Documentation and API stabilization

## Success Criteria

### Phase 1 Criteria ✅ ACHIEVED
1. **Schema Foundation**: OplogEntry and DirectoryEntry schemas work with Delta Lake ✅
2. **DataFusion Integration**: Custom table providers respect projection and handle nested data ✅  
3. **CLI Compatibility**: pond init/show commands work with TinyLogFS structures ✅
4. **End-to-End Testing**: Complete workflow from schema design to CLI execution ✅

### Phase 2 Success Criteria 🎯 TARGET
1. **Functional Compatibility**: TinyLogFS provides same API surface as TinyFS with persistence
2. **Performance**: In-memory operations remain fast, sync operations complete in reasonable time
3. **Persistence Guarantee**: All operations durably stored in Delta Lake with ACID properties
4. **CLI Functionality**: Complete filesystem operations available through pond commands
5. **State Management**: Sync/restore operations work correctly with proper error handling

### Phase 3+ Success Criteria 📋 PLANNED  
1. **Query Capability**: SQL queries over filesystem history and metadata work correctly
2. **Integration**: CMD tool can manage TinyLogFS-based ponds end-to-end
3. **Migration Path**: Clear upgrade path from current proof-of-concept implementation
4. **Production Readiness**: Comprehensive error recovery, performance optimization, and documentation

This integration represents the critical next step in building a production-ready local-first data lake system that combines the best aspects of both in-memory performance and durable persistence.

### Phase 2 Implementation Roadmap - Refined Architecture

### Current Status: Phase 1 Complete ✅

The foundation is solid and ready for refined Phase 2 implementation:
- **TinyLogFS schemas** are proven and tested
- **DataFusion integration** works correctly with custom table providers  
- **CLI integration** provides working pond management commands
- **Architecture decisions** are validated through end-to-end testing

### Phase 2 Next Steps 🚀 - Simplified Single-Threaded Design

**Immediate Priorities** (Next 2-3 weeks):

#### 2.1: Core TinyLogFS with Arrow Builders ⚡ HIGH PRIORITY
- **TinyLogFS struct**: Single-threaded design using simple owned types + RefCell for interior mutability
- **TransactionState**: Arrow Array builders (StringBuilder, Int64Builder, BinaryBuilder) instead of HashMap
- **Factory methods**: `new()`, `init_empty()`, proper initialization from existing OpLog stores
- **Builder management**: Efficient accumulation and snapshotting of operations to RecordBatch

#### 2.2: OpLog-Backed Implementations with Rc<RefCell<_>> 🔧 HIGH PRIORITY  
- **OpLogDirectory**: Use `Rc<RefCell<TinyLogFS>>` instead of `Weak<TinyLogFS>` for back-references
- **Integration**: Drop-in replacement for MemoryDirectory with lazy loading
- **Caching strategy**: RefCell-based entry cache with efficient updates
- **Transaction integration**: Automatic addition to TinyLogFS builders on directory changes

#### 2.3: Enhanced Table Provider with Builder Snapshotting 📊 HIGH PRIORITY
- **EnhancedOplogEntryTable**: Table provider that can snapshot pending transactions
- **Builder snapshotting**: Snapshot Arrow builders to RecordBatch on-the-fly for queries
- **Live visibility**: SQL queries can see uncommitted operations when requested
- **Schema consistency**: Pending and committed data use same schema

#### 2.4: Refined CLI with Transaction Commands 💻 MEDIUM PRIORITY
```rust
// Updated CLI commands for refined architecture
pond init                          // Initialize empty pond
pond ls [--pending]                 // List files, optionally show pending changes  
pond cat <path>                     // Display file content
pond mkdir <path>                   // Create directory
pond touch <path>                   // Create empty file
pond commit ["message"]             // Commit pending operations
pond status [--pending]             // Show pond status with optional pending operations
pond query <sql> [--pending]       // SQL query with optional pending transaction visibility
```

#### 2.5: Test Scenario Implementation 🧪 HIGH PRIORITY
```rust
// Your specific test scenario
#[tokio::test]  
async fn test_file_and_symlink_scenario() -> Result<(), TinyLogFSError> {
    let mut tinylogfs = TinyLogFS::init_empty("test_pond").await?;
    
    // Create file "A"
    tinylogfs.create_file(Path::new("/A"), b"content").await?;
    
    // Create symlink "B" -> "A"  
    tinylogfs.create_symlink(Path::new("/B"), Path::new("/A")).await?;
    
    // Show 2 entries with pending transactions visible
    let results = tinylogfs.query_with_pending(
        "SELECT * FROM oplog_entries ORDER BY timestamp", 
        true
    ).await?;
    assert_eq!(results[0].num_rows(), 2);
    
    // Commit and verify persistence
    tinylogfs.commit().await?;
    
    // Verify committed data
    let committed = tinylogfs.query_history("SELECT * FROM oplog_entries").await?;
    assert_eq!(committed[0].num_rows(), 2);
    
    Ok(())
}
```

**Technical Focus Areas**:
- **Simplicity**: Single-threaded design eliminates locking complexity
- **Performance**: Arrow builders provide optimal columnar accumulation
- **Visibility**: Enhanced table providers enable real-time transaction visibility  
- **Type Safety**: Compile-time guarantees throughout the pipeline

**Success Metrics for Refined Phase 2**:
- ✅ Complete filesystem operations working through CLI
- ✅ Sub-50ms commit operations for typical workloads (improved from previous 100ms target)
- ✅ Zero data loss with proper transaction handling
- ✅ Memory usage scales linearly with filesystem size
- ✅ Real-time visibility of pending operations through SQL queries

### Architecture Confidence 💪 - Refined Design Benefits

The refined Phase 2 architecture provides significant improvements over the original design:

**Single-Threaded Simplicity**:
- **No Arc<RwLock<_>>**: Eliminates lock contention and deadlock possibilities
- **Simple Ownership**: Clear ownership model with RefCell for interior mutability  
- **Easier Testing**: Deterministic behavior without concurrency concerns
- **Better Performance**: No lock overhead for hot path operations

**Arrow Builder Efficiency**:
- **Columnar Accumulation**: Operations accumulated in optimal memory layout
- **Type Safety**: Compile-time schema validation for all operations
- **Zero-Copy Conversion**: Direct builder-to-RecordBatch conversion
- **Memory Efficiency**: Builders handle large batches without intermediate allocations

**Enhanced Query Capabilities**:
- **Real-time Visibility**: Pending transactions visible in SQL queries when requested
- **Consistent Schema**: Same schema for committed and pending data
- **Performance**: Minimal overhead when pending visibility not needed  
- **Debugging**: Powerful debugging capabilities with transaction visibility

**Simplified Integration**:
- **Rc<RefCell<_>>**: Clear ownership model for OpLog-backed implementations
- **Lazy Loading**: Directory contents loaded only when needed
- **Automatic Transactions**: Directory changes automatically added to transaction builders
- **Clean Interfaces**: Simple trait implementations without complex lifetime management

Phase 2 implementation can proceed with confidence in this refined, simpler, and more efficient architecture.
