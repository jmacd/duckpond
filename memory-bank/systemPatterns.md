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

### TinyFS Pattern: Virtual Filesystem
```rust
// Core abstractions
FS -> WD -> NodePath -> Node(File|Directory|Symlink)

// Dynamic content
trait Directory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>>;
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)>>>;
}
```

**Key Patterns**:
- **Immutable Structure**: Filesystem state managed through functional updates
- **Reference Counting**: `NodeRef` provides shared ownership
- **Path Resolution**: Unified path handling with glob support
- **Dynamic Directories**: Custom implementations for computed content

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

## Integration Patterns

### State Management: TinyFS ↔ OpLog
```rust
// Store tinyfs directory state in oplog nodes
node_id -> sequence of Record<Entry> -> current directory state

// Partition strategy
partition_key = node_id  // For query locality
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
