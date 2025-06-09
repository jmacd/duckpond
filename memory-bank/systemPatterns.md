# System Patterns - DuckPond Architecture

## Overall Architecture: Three-Layer System

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface Layer                     │
│  CLI Tool • Web Static Sites • Observable Framework         │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 Processing Layer                            │
│  Resource Pipeline • Data Transformation • Downsampling    │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                  Storage Layer                              │
│   TinyFS + OpLog • Local Mirror • Cloud Backup             │
└─────────────────────────────────────────────────────────────┘
```

## Proof-of-Concept Architecture (./src - FROZEN)

### Directory Abstraction Pattern
```rust
trait TreeLike {
    fn entries(&mut self, pond: &mut Pond) -> BTreeSet<DirEntry>;
    fn realpath(&mut self, pond: &mut Pond, entry: &DirEntry) -> Option<PathBuf>;
    fn copy_version_to(&mut self, pond: &mut Pond, ...) -> Result<()>;
    fn sql_for_version(&mut self, pond: &mut Pond, ...) -> Result<String>;
}
```
**Purpose**: Unified interface for real directories, synthetic trees, and derived content

### Resource Management Pattern
- **YAML Configuration**: Declarative resource definitions
- **UUID-based Identity**: Unique identification for resource instances
- **Lifecycle Management**: Init → Start → Run → Backup sequence
- **Dependency Resolution**: Automatic ordering of resource processing

### Data Pipeline Pattern
```
HydroVu API → Arrow Records → Parquet Files → DuckDB Queries → Web Export
```
1. **Collection**: API clients fetch raw data
2. **Standardization**: Convert to common Arrow schema
3. **Storage**: Write partitioned Parquet files
4. **Processing**: Generate multiple time resolutions
5. **Export**: Create static web assets

## Replacement Crates Architecture (./crates - ACTIVE)

### TinyFS Pattern: Virtual Filesystem with Backend Abstraction
```rust
// Core abstractions
FS -> WD -> NodePath -> Node(File|Directory|Symlink)

// Backend trait for pluggable storage
trait FilesystemBackend {
    fn create_file(&self, content: &[u8]) -> Result<file::Handle>;
    fn create_directory(&self) -> Result<dir::Handle>;
    fn create_symlink(&self, target: &str) -> Result<symlink::Handle>;
}

// Memory backend implementation
struct MemoryBackend {
    // Uses memory module types (MemoryFile, MemoryDirectory, MemorySymlink)
}

// OpLog backend implementation (future)
struct OpLogBackend {
    store_path: String,
    pending_operations: RefCell<Vec<Operation>>,
    node_cache: RefCell<HashMap<String, NodeMetadata>>,
}

// Dynamic content  
trait Directory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>>;
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>>;
}

// Dependency injection
let fs = FS::with_backend(Rc::new(MemoryBackend::new()));
let fs = FS::with_backend(Rc::new(OpLogBackend::new(store_path)));
```

**Key Patterns**:
- **🎯 Backend Abstraction**: Core filesystem logic completely decoupled from storage implementation
- **🔧 Pluggable Storage**: Runtime selection between memory, persistent, or custom backends
- **⚡ Dependency Injection**: `FS::with_backend()` enables clean storage abstraction
- **📦 Import Isolation**: Core modules (`fs.rs`, `wd.rs`) have no storage dependencies
- **✅ Zero Breaking Changes**: Existing APIs unchanged, full backward compatibility
- **🔄 Fallible Operations**: Backend methods return `Result<Handle>` for error handling
- **🏗️ Clean Architecture**: Storage implementation details hidden behind trait interface
- **📝 Production Ready**: Memory and OpLog backends interchangeable through same interface

### OpLog Pattern: Two-Layer Data Storage
```
┌─────────────────────────────────────────────┐
│               DataFusion SQL                │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│           ByteStreamTable                   │
│      (Custom TableProvider)                 │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│             Delta Lake                      │
│    Records: node_id, timestamp, version    │
│              content: Vec<u8>               │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│           Arrow IPC Stream                  │
│     Entries: name, node_id, ...            │
│        (Serialized in content)              │
└─────────────────────────────────────────────┘
```

**Benefits**:
- **Schema Evolution**: Inner schema changes without outer table migrations
- **ACID Guarantees**: Delta Lake provides transaction safety
- **Query Performance**: DataFusion SQL over Arrow-native data
- **Time Travel**: Built-in versioning and historical queries

### CMD Pattern: Command-line Interface
```rust
// Command structure with clap
#[derive(Parser)]
#[command(name = "pond")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,  // Initialize new pond
    Show,  // Display operation log
}

// Environment integration
let pond_path = std::env::var("POND")?;
let store_path = PathBuf::from(pond_path).join("store");
```

**Key Patterns**:
- **Environment Configuration**: `POND` variable for store location
- **Subcommand Structure**: Extensible command design
- **Error Propagation**: Comprehensive validation and user feedback
- **Integration Testing**: Subprocess validation of actual CLI behavior

## Integration Patterns

### State Management: TinyFS Backend Integration
```rust
// Backend architecture enables pluggable storage
trait FilesystemBackend {
    fn create_file(&self, content: &[u8]) -> Result<file::Handle>;
    fn create_directory(&self) -> Result<dir::Handle>;
    fn create_symlink(&self, target: &str) -> Result<symlink::Handle>;
}

// OpLog backend implementation (future)
impl FilesystemBackend for OpLogBackend {
    fn create_file(&self, content: &[u8]) -> Result<file::Handle> {
        // 1. Generate node ID and persist to Delta Lake
        // 2. Cache metadata for fast access
        // 3. Return handle connected to persistent storage
    }
}

// Clean architecture benefits
- Core TinyFS logic unchanged
- Storage implementation swappable at runtime
- Memory backend for fast operations
- OpLog backend for persistent storage
```
timestamp = operation_time  // For time travel
content = Arrow IPC bytes  // For schema flexibility
```

### Local Mirror Pattern
```
Source of Truth (Delta Lake) → Physical Files (Host FS)
```
- **Bidirectional Sync**: Changes propagate both directions
- **Lazy Materialization**: Physical files created on demand
- **External Access**: Other tools can read physical copies
- **Reconstruction**: Mirror rebuildable from Delta Lake

### Query Integration Pattern
```rust
// TinyFS provides filesystem abstraction
let fs = FS::new();
let wd = fs.root().open_dir_path("/data/sensors")?;

// OpLog provides historical queries
let ctx = SessionContext::new();
ctx.register_table("sensor_logs", oplog_table)?;
let df = ctx.sql("SELECT * FROM sensor_logs WHERE node_id = ?")?;
```

## Component Relationships

### Data Flow: Collection to Export
1. **Input**: HydroVu API, file inbox, manual data entry
2. **Processing**: Arrow transformation, validation, enrichment
3. **Storage**: TinyFS structure stored in OpLog partitions
4. **Querying**: DataFusion SQL over historical operations
5. **Export**: Static files for web frameworks

### Error Handling Patterns
- **Result Propagation**: Consistent `anyhow::Result<T>` usage
- **Context Enrichment**: `.with_context()` for debugging
- **Graceful Degradation**: Continue processing on partial failures
- **Recovery Mechanisms**: Rebuild from source of truth

### Async Patterns
- **Tokio Runtime**: Full async/await throughout
- **Stream Processing**: `RecordBatchStream` for large datasets
- **Parallel Operations**: Concurrent resource processing
- **Backpressure**: Bounded channels and flow control

## Critical Implementation Decisions

### Schema Management
- **Arrow as Foundation**: Consistent schema across all components
- **Nested Serialization**: IPC format for complex data structures
- **Type Safety**: Rust structs with `ForArrow` trait implementation
- **Validation**: Schema compatibility checks during updates

### Performance Optimization
- **Columnar Processing**: Arrow-native operations throughout
- **Partitioning Strategy**: Time and node-based partitions
- **Lazy Evaluation**: Defer computation until needed
- **Caching**: Intermediate results and computed paths

### Reliability Features
- **Transactional Updates**: ACID guarantees for state changes
- **Consistency Checks**: Hash verification and integrity validation
- **Backup Strategy**: Incremental cloud synchronization
- **Recovery Procedures**: Multiple restore pathways

### Node ID Generation Pattern
```rust
/// Generate a random 64-bit node ID encoded as 16 hex digits
fn generate_node_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let mut hasher = DefaultHasher::new();
    
    // Use current time and thread ID for uniqueness
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    
    timestamp.hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    
    let hash = hasher.finish();
    format!("{:016x}", hash)
}
```

**Design Principles**:
- **Simple Format**: 16 hex characters (64 bits) instead of 128-bit UUIDs
- **No External Dependencies**: Uses Rust standard library only
- **Sufficient Uniqueness**: Timestamp + thread ID provides practical uniqueness
- **Deterministic Length**: Always exactly 16 characters for consistent storage

### TinyFS Integration Pattern
```rust
// Node ID generation used in TinyFS
impl FilesystemBackend for OpLogBackend {
    fn create_file(&self, content: &[u8]) -> Result<file::Handle> {
        let node_id = generate_node_id();
        // Persist file with generated node ID
    }
}
```
