# TLogFS - Transaction Log Filesystem Overview

TLogFS is a Delta Lake-backed filesystem implementation that provides ACID-compliant, versioned storage with advanced query capabilities. It serves as the core persistence layer for the DuckPond ecosystem, bridging traditional filesystem operations with modern data lakehouse architectures.

## Architecture Overview

TLogFS follows a clean architecture pattern with clear separation of concerns:

- **Delta Lake Storage**: Uses Delta Lake as the underlying storage format for versioned, ACID-compliant transactions
- **Arrow IPC Serialization**: Efficient binary serialization for filesystem metadata and content
- **Transaction Management**: RAII-based transaction guards ensuring proper commit/rollback semantics
- **DataFusion Integration**: SQL query capabilities over filesystem metadata and content
- **Dynamic Factory System**: Extensible plugin architecture for computed filesystem objects

## Core Components

### 1. Schema Layer (`schema.rs`)

The schema layer defines the core data structures used throughout TLogFS:

**`OplogEntry`** - Central filesystem operation record:
```rust
pub struct OplogEntry {
    pub node_id: String,      // Unique node identifier
    pub file_type: String,    // "file", "directory", "symlink", etc.
    pub parent_path: String,  // Parent directory path
    pub name: String,         // Entry name
    pub version: i64,         // Version number for this entry
    pub timestamp: i64,       // Operation timestamp (microseconds)
    pub content: Vec<u8>,     // Arrow IPC serialized content
    pub large_file_sha256: Option<String>, // SHA256 for large files
    pub extended_attributes: Option<Vec<u8>>, // Extended metadata
}
```

**Key Features:**
- **Arrow Schema Generation**: Automatic Arrow/Delta Lake schema generation via `ForArrow` trait
- **Content Addressing**: Large file support with SHA256 content addressing
- **Extended Attributes**: Immutable metadata for specialized file types
- **Temporal Metadata**: Built-in timestamp tracking for all operations

**Specialized Entry Types:**
- **`VersionedDirectoryEntry`**: Directory contents with insert/delete operations
- **`ExtendedAttributes`**: Type-specific metadata (temporal ranges for series, schemas for tables)
- **`OperationType`**: Directory mutation tracking (Insert/Delete operations)

### 2. Persistence Layer (`persistence.rs`)

The persistence layer implements the core storage and retrieval logic:

**`OpLogPersistence`** - Main persistence implementation:
- **Delta Lake Integration**: Direct DeltaTable operations for versioned storage
- **Transaction Management**: Begin/commit/rollback transaction lifecycle
- **Node Factory**: Creation of filesystem objects (files, directories, symlinks)
- **Large File Handling**: Automatic spillover for content above threshold (64KB)
- **Query Interface**: DataFusion integration for metadata queries

**`State`** - Shared persistence state:
- **Thread-Safe Access**: Arc<Mutex<InnerState>> for concurrent operations  
- **PersistenceLayer Implementation**: Full TinyFS persistence interface
- **Factory Integration**: Dynamic object creation via factory registry
- **Metadata Caching**: Efficient metadata retrieval and caching

**Key Architecture Decisions:**
- **Single Source of Truth**: All state comes from Delta Lake, no local caching
- **Clean Separation**: Persistence layer is completely independent of filesystem objects
- **Fail-Fast Philosophy**: Following DuckPond's anti-fallback pattern, errors propagate explicitly

### 3. Transaction Management (`transaction_guard.rs`)

Transaction management follows the RAII pattern to ensure proper resource cleanup:

**`TransactionGuard`** - RAII transaction wrapper:
```rust
pub struct TransactionGuard<'a> {
    persistence: &'a mut OpLogPersistence,
}
```

**Key Features:**
- **Automatic Cleanup**: Drop implementation ensures transaction cleanup
- **State Access**: Provides access to persistence layer for operations
- **Commit Control**: Explicit commit with metadata support
- **Deref Pattern**: Direct access to underlying FS operations

**Usage Pattern:**
```rust
let mut persistence = OpLogPersistence::create(path).await?;
{
    let tx = persistence.begin().await?;
    // Perform operations through tx
    tx.commit(metadata).await?;
} // Automatic cleanup on scope exit
```

### 4. File System Objects

TLogFS implements clean, stateless filesystem objects:

**`OpLogFile` (`file.rs`)**:
- **Stateless Design**: All data comes from persistence layer
- **AsyncReadSeek**: Standard async I/O interface
- **Transaction Integration**: Write operations integrate with transaction lifecycle
- **Large File Support**: Automatic promotion to external storage for large files

**`OpLogDirectory` (`directory.rs`)**:
- **Stream-Based Listings**: Efficient directory traversal via async streams
- **Dynamic Entries**: Integration with factory system for computed objects
- **Versioned Content**: Directory contents tracked through VersionedDirectoryEntry

**`OpLogSymlink` (`symlink.rs`)**:
- **Target Resolution**: Efficient symlink target retrieval
- **Metadata Integration**: Full metadata support for symlinks

### 5. File Writing System (`file_writer.rs`)

The file writer provides a clean write path with automatic content analysis:

**`FileWriter`** - Main file writing interface:
- **Hybrid Storage**: Automatic promotion from memory to external storage
- **Content Analysis**: Automatic metadata extraction for specialized file types
- **Transaction Integration**: Writes are transaction-scoped and atomic
- **Large File Handling**: Transparent spillover with content addressing

**Specialized Processors:**
- **`SeriesProcessor`**: Extracts temporal metadata from Parquet files
- **`TableProcessor`**: Validates schema and structure for table files
- **Content-Addressed Storage**: SHA256-based deduplication for large files

### 6. Query System (`query/`)

The query system provides DataFusion integration for filesystem queries:

**Core Query Tables:**

**`MetadataTable` (`metadata.rs`)**:
- **OplogEntry Queries**: Direct access to filesystem metadata without content deserialization
- **Universal Access**: Works for all entry types (files, directories, symlinks)
- **Efficient Filtering**: Leverages Delta Lake metadata for fast queries

**`IpcTable` (`ipc.rs`)**:
- **Generic Arrow IPC**: Queries arbitrary Arrow IPC data from content fields
- **Flexible Schema**: Schema provided at construction time
- **Transaction Integration**: Optional txn_seq column for version tracking

**Specialized Query Providers:**

**`UnifiedTableProvider` (`unified.rs`)**:
- **Common Implementation**: Eliminates ~80% duplication between table types
- **File Discovery**: Efficient file version discovery and filtering
- **Streaming Execution**: Memory-efficient processing of large datasets
- **Schema Harmonization**: Automatic schema evolution handling

**`SeriesTable` (`series.rs`)**:
- **Time-Range Queries**: Efficient temporal filtering using metadata
- **Parquet Statistics**: Leverages Parquet metadata for fine-grained pruning
- **Version Discovery**: Automatic handling of multiple file versions

**Advanced Query Features:**
- **ORDER BY Support**: Full SQL ORDER BY with schema harmonization
- **Projection Pushdown**: Column pruning for efficient queries  
- **Predicate Pushdown**: Filter pushdown to storage layer
- **Statistics Integration**: Parquet and Delta Lake statistics utilization

### 7. Dynamic Factory System (`factory.rs`)

The factory system enables extensible filesystem objects:

**`FactoryRegistry`** - Plugin management:
- **Linkme Integration**: Compile-time factory registration
- **Context-Aware Creation**: Factory functions receive pond context
- **Validation**: Configuration validation before object creation

**Built-in Factories:**

**`HostmountDirectory` (`hostmount.rs`)**:
- **Host Filesystem Bridge**: Present host directories within pond
- **Configuration**: Simple directory path specification
- **Transparent Access**: Standard filesystem operations on host files

**`CsvDirectory` (`csv_directory.rs`)**:
- **CSV Discovery**: Automatic discovery and conversion of CSV files
- **Parquet Conversion**: On-demand CSV-to-Parquet conversion
- **Schema Detection**: Automatic schema inference with configuration overrides
- **Caching**: Configurable cache timeout for converted files

**`SqlDerivedFile` (`sql_derived.rs`)**:
- **SQL Computation**: Files generated from SQL queries over pond data
- **DataFusion Integration**: Full SQL query support over existing data
- **Streaming Results**: Memory-efficient processing of query results
- **Dynamic Schema**: Result schema derived from SQL query

### 8. Large File Handling (`large_files.rs`)

TLogFS provides sophisticated large file management:

**Storage Strategy:**
- **Threshold-Based**: Files >64KB automatically promoted to external storage
- **Content Addressing**: SHA256-based deduplication and integrity
- **Hierarchical Storage**: Automatic directory structure for scalability
- **Migration Support**: Automatic migration from flat to hierarchical structure

**`HybridWriter`**:
- **Streaming Interface**: AsyncWrite implementation with incremental hashing
- **Automatic Promotion**: Transparent spillover when size threshold reached
- **Integrity Checking**: SHA256 verification on read operations

### 9. Error Handling (`error.rs`)

TLogFS follows the fail-fast philosophy with comprehensive error types:

**`TLogFSError`** - Unified error handling:
- **Error Chain Integration**: Proper error source tracking
- **Domain-Specific Errors**: Specialized errors for different subsystems
- **Context Preservation**: Rich error context for debugging
- **No Fallbacks**: Following DuckPond's anti-fallback architectural principle

## Advanced Features

### Schema Evolution and Query Optimization

TLogFS handles schema evolution gracefully:

- **Schema Harmonization**: Automatic schema unification across file versions
- **Column Addition**: New columns handled transparently with null defaults
- **DataFusion Integration**: Leverages DataFusion's schema evolution capabilities
- **Statistics Preservation**: Maintains Parquet statistics across schema changes

### Transaction Isolation and ACID Properties

TLogFS provides full ACID guarantees:

- **Atomicity**: All-or-nothing transaction commits via Delta Lake
- **Consistency**: Schema validation and referential integrity
- **Isolation**: Transaction-scoped visibility of changes
- **Durability**: Delta Lake transaction log ensures persistence

### Performance Optimizations

- **Metadata Caching**: Intelligent caching of frequently accessed metadata
- **Lazy Loading**: Content loaded only when accessed
- **Statistics Pruning**: File elimination using temporal and structural metadata
- **Streaming Execution**: Memory-efficient processing for large datasets

## Integration with DuckPond Ecosystem

### TinyFS Integration

TLogFS implements the `PersistenceLayer` trait, providing:
- **Pure Filesystem API**: Standard file/directory/symlink operations
- **Node-Based Architecture**: Unique NodeID system for efficient operations
- **Working Directory Contexts**: Scoped filesystem operations
- **Arrow Integration**: Data-aware filesystem operations

### Steward Orchestration

TLogFS serves as the storage backend for Steward's dual filesystem architecture:
- **Data Filesystem**: User content and computed objects
- **Control Filesystem**: Transaction metadata and configuration
- **Coordinated Transactions**: Steward manages consistent updates across both filesystems

### DataFusion Query Engine

TLogFS provides rich query capabilities:
- **SQL Interface**: Standard SQL queries over filesystem metadata and content
- **Table Providers**: DataFusion integration for filesystem objects
- **Query Optimization**: Predicate and projection pushdown
- **Statistics Integration**: Leverages Parquet and Delta Lake statistics

## Testing and Validation

TLogFS includes comprehensive test coverage:

### Unit Tests (`tests.rs`)
- **Transaction Guard Testing**: Basic usage and read-after-create scenarios
- **Single Version Series**: Isolated testing of file:series operations
- **Storage Investigation**: Deep testing of storage structure and metadata

### Query Testing (`query/order_by_*_test.rs`)
- **Schema Harmonization**: Testing ORDER BY operations with schema evolution
- **Index Bounds**: Regression testing for projection index issues
- **Complex Scenarios**: Multi-version files with schema differences

### Test Utilities (`test_utils.rs`)
- **DRY Test Patterns**: Reusable test builders and environments
- **RecordBatch Generation**: Standardized test data creation
- **Environment Setup**: Consistent test environment patterns

## Development Areas

### Current Strengths
- ✅ **Robust Transaction Management**: RAII guards with proper cleanup
- ✅ **Rich Query Interface**: DataFusion integration with SQL support  
- ✅ **Schema Evolution**: Graceful handling of schema changes
- ✅ **Large File Support**: Content-addressed storage with deduplication
- ✅ **Dynamic Objects**: Extensible factory system for computed objects
- ✅ **ACID Compliance**: Full transaction guarantees via Delta Lake

### Areas for Enhancement
- 🔄 **Performance Optimization**: Query performance tuning and caching strategies
- 🔄 **Schema Migration**: Tools for explicit schema migration operations
- 🔄 **Monitoring Integration**: Enhanced observability and metrics collection
- 🔄 **Backup/Restore**: Point-in-time recovery and backup strategies
- 🔄 **Compaction**: Delta Lake table maintenance and optimization

## Architecture Philosophy

TLogFS embodies DuckPond's core architectural principles:

### Fail-Fast Design
- **No Fallbacks**: Errors are explicit and propagated clearly
- **Single Source of Truth**: All state comes from Delta Lake persistence
- **Explicit Error Handling**: Rich error types with proper context

### Clean Architecture
- **Separation of Concerns**: Clear boundaries between persistence, filesystem, and query layers
- **Dependency Injection**: Persistence layer injected into filesystem objects
- **Stateless Objects**: Filesystem objects delegate to persistence layer

### Modern Data Stack Integration
- **Arrow Native**: Arrow IPC for efficient serialization throughout
- **DataFusion Compatible**: Standard DataFusion table providers
- **Delta Lake Foundation**: Leverages Delta Lake for versioning and ACID properties

TLogFS represents a modern approach to filesystem implementation, combining traditional filesystem semantics with lakehouse architecture benefits, providing a robust foundation for the DuckPond ecosystem's data management needs.
