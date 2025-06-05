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

#### Phase 2: Hybrid Storage Architecture

Create `TinyLogFS` struct that manages both layers:

```rust
pub struct TinyLogFS {
    // Fast in-memory layer
    memory_fs: tinyfs::FS,
    
    // Persistent storage
    oplog_store_path: String,
	
	// Additional fields to cache state.
}

impl TinyLogFS {
    pub async fn new(store_path: &str) -> Result<Self>;
    pub async fn create_file(&mut self, path: &Path, content: &[u8]) -> Result<NodePath>;
    pub async fn create_directory(&mut self, path: &Path) -> Result<WD>;
    pub async fn create_symlink(&mut self, path: &Path, target: &Path) -> Result<NodePath>;

	// Additional methods to read current state from the backing store while using the FS.
}
```

Note that the file system is transactional and it will flush its
changes in a single commit then exit from the CLI. The TinyLogFS will
need to buffer changes in memory, write them to Parquet files, then
commit the changes.

#### Phase 3: OpLog-Backed Directory Implementation

Replace `MemoryDirectory` with persistent implementation:

```rust
pub struct OpLogDirectory {
    node_id: String,
    oplog_partition_id: String,
    
    // Fast cache for recently accessed entries
    entry_cache: BTreeMap<String, NodeRef>,
    cache_dirty: bool,
    
    // Reference back to the TinyLogFS for persistence operations
    tinylogfs: Weak<RefCell<TinyLogFS>>,
}

impl Directory for OpLogDirectory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        // 1. Check in-memory cache first
        // 2. If not found, query OpLog for directory entries
        // 3. Reconstruct NodeRef and cache result
    }
    
    fn insert(&mut self, name: String, node: NodeRef) -> Result<()> {
        // 1. Update in-memory cache
        // 2. Mark node as dirty for next sync
        // 3. Optionally trigger immediate sync for critical operations
    }
    
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>> {
        // Query OpLog for all entries in this directory partition
    }
}
```

#### Phase 4: Data Flow Design

**Write Operations**:
1. TinyFS operation (create/update/delete) modifies in-memory state
2. Node marked as dirty in `dirty_nodes` set
3. Periodic or manual sync writes dirty nodes to OpLog as Entry records
4. OpLog stores operations partitioned by `node_id` for query locality

**Read Operations**:
1. TinyFS serves from in-memory cache for hot paths
2. Cache misses trigger OpLog queries to reconstruct state
3. Query results cached in memory for subsequent operations

**Restore Operations**:
1. Query OpLog partitions ordered by timestamp
2. Replay operation sequence to rebuild directory states
3. Reconstruct in-memory TinyFS structure
4. Validate integrity using content hashes

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

### Phase 2: Basic Operations
- [ ] File create/read/update/delete operations
- [ ] Directory create/list operations
- [ ] Symlink create/read operations
- [ ] Sync and restore functionality
- [ ] CLI extensions for filesystem operations

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

1. **Functional Compatibility**: TinyLogFS provides same API surface as TinyFS
2. **Persistence Guarantee**: All operations durably stored in Delta Lake with ACID properties
3. **Performance**: In-memory operations remain fast, sync operations complete reasonably
4. **Query Capability**: SQL queries over filesystem history and metadata work correctly
5. **Integration**: CMD tool can manage TinyLogFS-based ponds end-to-end
6. **Migration Path**: Clear upgrade path from current proof-of-concept implementation

This integration represents the critical next step in building a production-ready local-first data lake system that combines the best aspects of both in-memory performance and durable persistence.
