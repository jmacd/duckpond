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

#### Phase 2: Hybrid Storage Architecture ⚡ IN DESIGN

**Design Objective**: Implement the core `TinyLogFS` struct that seamlessly combines TinyFS's in-memory performance with OpLog's persistent storage, providing ACID guarantees while maintaining fast filesystem operations.

##### Core Architecture Design

```rust
pub struct TinyLogFS {
    // Fast in-memory filesystem for hot operations
    memory_fs: Arc<RwLock<tinyfs::FS>>,
    
    // Persistent storage configuration
    oplog_store_path: String,
    
    // State management for persistence
    dirty_nodes: Arc<RwLock<HashSet<String>>>,  // NodeIDs needing persistence
    node_cache: Arc<RwLock<HashMap<String, CachedNode>>>, // Recently accessed nodes
    
    // Mapping between TinyFS nodes and OpLog entries
    node_to_partition: Arc<RwLock<HashMap<String, String>>>, // NodeID -> PartitionID
    
    // Transaction state
    current_transaction: Arc<RwLock<Option<TransactionState>>>,
    last_sync_timestamp: Arc<RwLock<SystemTime>>,
}

#[derive(Debug, Clone)]
struct CachedNode {
    oplog_entry: OplogEntry,
    last_accessed: SystemTime,
    is_dirty: bool,
}

#[derive(Debug)]
struct TransactionState {
    transaction_id: String,
    dirty_operations: Vec<FilesystemOperation>,
    started_at: SystemTime,
}

#[derive(Debug, Clone)]
enum FilesystemOperation {
    CreateFile { path: PathBuf, content: Vec<u8> },
    CreateDirectory { path: PathBuf },
    CreateSymlink { path: PathBuf, target: PathBuf },
    UpdateFile { node_id: String, content: Vec<u8> },
    DeleteNode { node_id: String },
    MoveNode { node_id: String, new_path: PathBuf },
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
    
    /// Get underlying TinyFS for advanced operations
    pub fn memory_fs(&self) -> Arc<RwLock<tinyfs::FS>>;
    
    /// Check if node exists (fast, memory-only check)
    pub fn exists(&self, path: &Path) -> bool;
    
    /// Get node metadata without loading content
    pub async fn get_metadata(&self, path: &Path) -> Result<NodeMetadata, TinyLogFSError>;
    
    // === Persistence Operations ===
    
    /// Flush all dirty nodes to OpLog in a single transaction
    pub async fn sync(&mut self) -> Result<SyncResult, TinyLogFSError>;
    
    /// Force sync of specific nodes
    pub async fn sync_nodes(&mut self, node_ids: &[String]) -> Result<SyncResult, TinyLogFSError>;
    
    /// Restore filesystem state from OpLog (useful for recovery)
    pub async fn restore_from_oplog(&mut self) -> Result<RestoreResult, TinyLogFSError>;
    
    /// Restore filesystem state to specific timestamp
    pub async fn restore_to_timestamp(&mut self, timestamp: SystemTime) -> Result<RestoreResult, TinyLogFSError>;
    
    /// Get sync status and dirty node information
    pub fn get_status(&self) -> TinyLogFSStatus;
    
    // === Query Operations ===
    
    /// Query filesystem history using SQL
    pub async fn query_history(&self, sql: &str) -> Result<Vec<RecordBatch>, TinyLogFSError>;
    
    /// Get all versions of a specific node
    pub async fn get_node_history(&self, node_id: &str) -> Result<Vec<HistoryEntry>, TinyLogFSError>;
    
    /// Find nodes by metadata criteria
    pub async fn find_nodes(&self, criteria: &NodeSearchCriteria) -> Result<Vec<String>, TinyLogFSError>;
}

#[derive(Debug)]
pub struct NodeMetadata {
    pub node_id: String,
    pub file_type: FileType,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub size: u64,
    pub custom_attributes: serde_json::Value,
}

#[derive(Debug)]
pub enum FileType {
    File,
    Directory,
    Symlink { target: PathBuf },
}

#[derive(Debug)]
pub struct SyncResult {
    pub nodes_synced: usize,
    pub transaction_id: String,
    pub sync_duration: Duration,
    pub bytes_written: u64,
}

#[derive(Debug)]
pub struct RestoreResult {
    pub nodes_restored: usize,
    pub restore_duration: Duration,
    pub target_timestamp: SystemTime,
}

#[derive(Debug)]
pub struct TinyLogFSStatus {
    pub dirty_nodes: usize,
    pub cached_nodes: usize,
    pub last_sync: Option<SystemTime>,
    pub memory_usage: usize,
    pub total_nodes: usize,
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
    
    #[error("Sync error: {message}")]
    Sync { message: String },
    
    #[error("Restore error: {message}")]
    Restore { message: String },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

##### Transaction Model

**Design Principle**: TinyLogFS operates with "auto-commit" transactions by default, where each operation immediately updates the in-memory filesystem and marks nodes as dirty. The CLI tool will call `sync()` before exit to persist all changes in a single Delta Lake transaction.

**Transaction Flow**:
1. **Operation**: Filesystem operation updates in-memory TinyFS
2. **Dirty Tracking**: Node marked in `dirty_nodes` set  
3. **Batching**: Multiple operations accumulate dirty nodes
4. **Sync**: Single `sync()` call writes all dirty nodes to OpLog as one transaction
5. **Cleanup**: Dirty set cleared, caches updated

**Error Recovery**: If sync fails, in-memory state remains valid and sync can be retried. If process crashes before sync, changes are lost (acceptable for CLI tool usage pattern).

#### Phase 3: OpLog-Backed Directory Implementation 📋 DESIGNED

**Design Objective**: Replace TinyFS's `MemoryDirectory` with a persistent `OpLogDirectory` implementation that maintains fast access patterns while automatically persisting changes to OpLog.

##### OpLogDirectory Architecture

```rust
pub struct OpLogDirectory {
    // Core identity
    node_id: String,
    oplog_partition_id: String,
    
    // Performance caching
    entry_cache: Arc<RwLock<BTreeMap<String, CachedEntry>>>,
    cache_dirty: Arc<AtomicBool>,
    
    // Back-reference to TinyLogFS for persistence operations
    tinylogfs: Weak<TinyLogFS>,
    
    // Metadata
    metadata: DirectoryMetadata,
    last_loaded: Arc<RwLock<SystemTime>>,
}

#[derive(Debug, Clone)]
struct CachedEntry {
    node_ref: NodeRef,
    last_accessed: SystemTime,
    is_dirty: bool,
    oplog_entry: Option<OplogEntry>, // Cache the OplogEntry for this node
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
        if let Some(cached) = self.entry_cache.read().unwrap().get(name) {
            cached.last_accessed = SystemTime::now();
            return Ok(Some(cached.node_ref.clone()));
        }
        
        // 2. Query OpLog for directory entries (slow path)
        let entries = self.load_directory_entries_from_oplog()?;
        
        // 3. Find requested entry and construct NodeRef
        if let Some(dir_entry) = entries.iter().find(|e| e.name == name) {
            let node_ref = self.reconstruct_node_ref(&dir_entry.child_node_id)?;
            
            // 4. Cache result for future access
            self.cache_entry(name.to_string(), node_ref.clone());
            
            Ok(Some(node_ref))
        } else {
            Ok(None)
        }
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> Result<(), tinyfs::Error> {
        // 1. Update in-memory cache immediately
        let cached_entry = CachedEntry {
            node_ref: node.clone(),
            last_accessed: SystemTime::now(),
            is_dirty: true,
            oplog_entry: None, // Will be populated during sync
        };
        
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

#### Phase 4: Data Flow Design 🔄 DETAILED SPECIFICATION

**Design Objective**: Define the complete data flow patterns for write operations, read operations, and restore operations, ensuring consistency and performance.

##### Write Operations Flow

```
User Operation → TinyLogFS API → In-Memory FS Update → Dirty Tracking → Batch Sync → OpLog Persistence
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

4. **Dirty Tracking**:
   - Node ID added to `dirty_nodes` HashSet
   - OpLogDirectory sets `cache_dirty = true`
   - `last_modified` timestamp updated

5. **Batch Sync** (on explicit `sync()` call or CLI exit):
   - Collect all dirty nodes
   - Serialize each node as OplogEntry
   - For directories: serialize entry cache as DirectoryEntry records in content field
   - Write all OplogEntry records to Delta Lake in single transaction
   - Clear dirty flags and update sync timestamp

**Write Operation Types**:
```rust
// File operations
create_file("/path/to/file.txt", b"content") 
→ OplogEntry { part_id: parent_dir_id, node_id: new_file_id, file_type: "file", content: b"content" }

// Directory operations  
create_directory("/path/to/dir")
→ OplogEntry { part_id: dir_id, node_id: dir_id, file_type: "directory", content: encode_directory_entries([]) }

// Symlink operations
create_symlink("/path/to/link", "/target/path")
→ OplogEntry { part_id: parent_dir_id, node_id: link_id, file_type: "symlink", content: b"/target/path" }
```

##### Read Operations Flow

```
User Request → Cache Check → Memory FS Lookup → OpLog Query (if needed) → Cache Update → Response
```

**Detailed Read Flow**:

1. **Fast Path (Cache Hit)**:
   - Check TinyFS in-memory nodes
   - Return immediately if node exists and is cached

2. **Medium Path (Memory Miss, OpLog Hit)**:
   - Query OpLog for node by node_id
   - Deserialize OplogEntry to reconstruct node
   - Add to TinyFS memory and cache
   - Return reconstructed data

3. **Slow Path (Complete Miss)**:
   - Return "not found" error
   - No OpLog queries for non-existent paths

**Read Query Patterns**:
```sql
-- Get specific file content
SELECT content FROM oplog_entries WHERE node_id = ?

-- Get directory contents
SELECT content FROM oplog_entries WHERE node_id = ? AND file_type = 'directory'
-- Then decode content as DirectoryEntry records

-- Get file metadata only
SELECT part_id, node_id, file_type, metadata FROM oplog_entries WHERE node_id = ?
```

##### Restore Operations Flow

```
OpLog Query → Timestamp Filtering → Operation Replay → Memory FS Reconstruction → Cache Population
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
   - Handle deletions (entries marked as deleted)

3. **Reconstruct Filesystem Tree**:
   - Start with root directory node
   - Recursively build directory tree from DirectoryEntry records
   - Create File and Symlink nodes as referenced

4. **Populate TinyFS**:
   - Clear existing in-memory FS
   - Create nodes in dependency order (directories before contents)
   - Set up all NodeRef relationships
   - Mark all nodes as clean (not dirty)

**Restore Strategies**:
```rust
// Full restore to latest state
restore_from_oplog() → queries latest entries for all nodes

// Point-in-time restore  
restore_to_timestamp(timestamp) → queries entries <= timestamp

// Selective restore
restore_nodes(node_ids) → queries specific nodes only

// Incremental restore
restore_since_timestamp(timestamp) → queries entries > timestamp, apply to existing state
```

##### Consistency Guarantees

**ACID Properties**:
- **Atomicity**: All dirty nodes synced in single Delta Lake transaction
- **Consistency**: TinyFS constraints enforced before OpLog writes
- **Isolation**: In-memory FS provides snapshot isolation within CLI session
- **Durability**: Delta Lake guarantees persistence after successful sync

**Error Handling**:
- **Sync Failure**: In-memory state preserved, sync can be retried
- **Restore Failure**: Previous memory state preserved, error reported
- **Corruption Detection**: Content hashes validated during restore
- **Partial Operations**: Directory operations maintain parent-child consistency

##### Performance Optimizations

**Caching Strategy**:
- **Hot Data**: Recently accessed nodes stay in TinyFS memory
- **Directory Entries**: OpLogDirectory caches entry mappings
- **Metadata**: Node metadata cached separately from content
- **LRU Eviction**: Configurable cache size limits with LRU eviction

**Batch Optimizations**:
- **Bulk Sync**: Multiple nodes written in single Delta Lake transaction
- **Compression**: Large directory contents compressed in OpLog
- **Lazy Loading**: Directory contents loaded only when accessed
- **Parallel Restore**: Independent subtrees restored in parallel

**Query Optimizations**:
- **Partition Pruning**: Queries filtered by part_id for directory locality
- **Column Projection**: Only required columns loaded (especially content)
- **Index Usage**: Delta Lake file-level indexes for timestamp and node_id
- **Connection Pooling**: DataFusion connection reuse across operations

#### Phase 5: Partitioning Strategy

- **Partition Key**: `node_id` (directory/file UUID)
- **Sort Key**: `timestamp` (operation order)
- **Benefits**: 
  - Query locality for directory operations
  - Parallel restoration of independent subtrees
  - Efficient time-travel queries for individual nodes

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

## Phase 2 Implementation Roadmap

### Current Status: Phase 1 Complete ✅

The foundation is solid and ready for Phase 2 implementation:
- **TinyLogFS schemas** are proven and tested
- **DataFusion integration** works correctly with custom table providers  
- **CLI integration** provides working pond management commands
- **Architecture decisions** are validated through end-to-end testing

### Phase 2 Next Steps 🚀

**Immediate Priorities** (Next 2-3 weeks):
1. **TinyLogFS Core Implementation**: Build the hybrid filesystem struct with state management
2. **OpLogDirectory Implementation**: Create persistent Directory trait implementation
3. **Basic File Operations**: Implement create, read, update, delete with OpLog persistence
4. **CLI Extensions**: Add ls, cat, mkdir, touch, sync, restore commands

**Technical Focus Areas**:
- **Performance**: Maintain TinyFS speed while adding persistence
- **Consistency**: Ensure ACID properties through Delta Lake transactions
- **Error Handling**: Robust error recovery and transaction rollback
- **Testing**: Comprehensive integration tests for all new functionality

**Success Metrics for Phase 2**:
- Complete filesystem operations working through CLI
- Sub-100ms sync operations for typical workloads  
- Zero data loss with proper transaction handling
- Memory usage stays reasonable for expected filesystem sizes

### Architecture Confidence 💪

The Phase 1 implementation has validated key architectural decisions:
- **Two-layer storage**: Delta Lake + Arrow IPC provides both performance and flexibility
- **Partitioning strategy**: part_id approach organizes data effectively for query locality
- **DataFusion integration**: Custom table providers handle complex nested data efficiently
- **CLI approach**: pond commands provide intuitive interface for filesystem operations

Phase 2 implementation can proceed with confidence in these foundational choices.
