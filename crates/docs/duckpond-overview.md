## Repository Overview

This repository contains an original proof-of-concept implementation
for DuckPond at the top-level of the repository, with a main crate,
hydrovu, and pond sub-modules. This implementation has been frozen,
and GitHub Copilot will not modify any of the Rust code under ./src,
however it makes a good reference as to the intentions behind our new
development.

## DuckPond Architecture: Layered Query-Native Filesystem

DuckPond implements a **query-native filesystem** built on a carefully orchestrated stack of complementary technologies. Understanding this layered architecture is essential for working with the system:

```
┌─────────────────────────────────────────────────────────────┐
│                     APPLICATIONS                            │
│  CLI Commands • HydroVu Collector • Web Interfaces         │
├─────────────────────────────────────────────────────────────┤
│                      STEWARD                                │
│    Dual Filesystem Orchestration • Transaction Guards      │
├─────────────────────────────────────────────────────────────┤
│                      TINYFS                                 │
│   Type-Safe Filesystem API • Path Preservation • Nodes     │
├─────────────────────────────────────────────────────────────┤
│                      TLOGFS                                 │
│  OpLog Persistence • Dynamic Factories • Query Integration │
├─────────────────────────────────────────────────────────────┤
│              DELTA LAKE + DATAFUSION                       │
│   ACID Storage • Versioning • SQL Query Engine             │
└─────────────────────────────────────────────────────────────┘
```

**Key Architectural Principles:**

🔹 **Query-First Design**: Every filesystem object can be queried via SQL - files, directories, metadata, and computed objects all expose DataFusion interfaces

🔹 **Type-Safe Abstraction**: TinyFS provides compile-time safety for filesystem operations while preserving hierarchical context and access paths

🔹 **ACID Compliance**: Delta Lake ensures transaction safety, versioning, and crash consistency across all operations

🔹 **Dynamic Objects**: The factory system creates queryable filesystem objects (SQL-derived files, CSV directories, temporal aggregations) that appear native to the filesystem

🔹 **Path & Context Preservation**: Unlike traditional filesystems, DuckPond tracks both node identity AND the path used to access nodes, enabling complex hierarchical operations

## Data Flow Architecture

**Write Path**: `Application → Steward → TinyFS → TLogFS → Delta Lake`
**Query Path**: `DataFusion ← TLogFS QueryableFile ← TinyFS Node ← Transaction Guard`
**Factory Path**: `SQL Query → Dynamic Object → QueryableFile → Native Filesystem Node`

## Proof-of-concept

The Duckpond proof of concept demonstrates how we want a local-first
Parquet-oriented file system to look and feel. The main objective of
this code base was to collect HydroVu and LakeTech timeseries
(different ways), translate them into a common Arrow-based
representation, then compute downsampled timeseries for fast page
loads on a static web site.

The ./src/config/example folder contains the yaml files used to build the
proof-of-concept website, they contain a full Duckpond "pipeline" from
HydroVu and Inbox to organized and downsampled timeseries for viewing
as static websites.

## Current Production Crates

### Tinyfs

The `tinyfs` crate implements a **type-safe filesystem abstraction** - NOT a traditional filesystem. This is a Rust API surface with strong typing, safety guarantees, and complex navigation patterns that preserve both node identity and access paths.

**Key Features:**
- Pure filesystem API (files, directories, symbolic links)
- Pluggable persistence architecture via `PersistenceLayer` trait
- Working directory contexts (`WD`) for filesystem operations
- Node-based architecture with unique `NodeID`s
- Arrow integration for data-aware operations
- Async/await support throughout

**Major Interfaces:**
- `FS` - Main filesystem struct with persistence layer
- `WD` (Working Directory) - Context for file operations
- `PersistenceLayer` trait - Pluggable storage backends
- `Node`, `File`, `Directory`, `Symlink` - Filesystem object types
- Arrow integration via dedicated arrow module

**⚠️ Critical Architecture Note: TinyFS Navigation Complexity**

TinyFS is fundamentally different from traditional filesystems. It requires understanding multiple abstraction layers and their relationships:

**1. Transaction → Filesystem Access Pattern:**
```rust
// Start with transaction guard (entry point)
let tx_guard = persistence.begin().await?;

// Access root filesystem through guard (Deref to FS)
let root_wd = tx_guard.root();

// Navigate to specific paths (preserves path context)
let file_node = root_wd.lookup("sensors/data.parquet").await?;
```

**2. Node Resolution & Type Safety:**
```rust
// TinyFS tracks BOTH the node AND the path used to access it
match file_node {
    Lookup::Found(node_path) => {
        // NodePath preserves both Node and access path
        let node_ref = node_path.borrow().await;  // NodePathRef
        
        // Type-safe access through NodeType enum
        match node_ref.node_type() {
            NodeType::File(file_handle) => {
                // Access actual file implementation
            }
        }
    }
}
```

**3. Path Preservation & dir::Pathed<>:**
- TinyFS maintains `dir::Pathed<Handle>` to track access paths
- Essential for hierarchical operations and parent directory context
- Path information flows through all operations

**4. File Handle → DataFusion Integration:**
```rust
// In TLogFS context, files are OpLogFiles accessible via as_any()
let file_guard = file_handle.0.lock().await;  // Note: private field access
if let Some(oplog_file) = file_guard.as_any().downcast_ref::<OpLogFile>() {
    // Now can access DataFusion capabilities
}
```

**Why This Complexity Exists:**
- **Type Safety**: Prevents incorrect file/directory operations at compile time
- **Path Tracking**: Maintains hierarchical context for all operations  
- **Concurrency Safety**: Arc<Mutex<>> patterns ensure thread-safe access
- **Pluggable Persistence**: Abstract enough to support multiple storage backends
- **Query Integration**: Seamless DataFusion integration requires type dispatch

**Common Navigation Pitfalls:**
- Confusing `NodeRef` vs `NodePath` vs `NodePathRef`
- Forgetting to preserve path context during operations
- Incorrect async lock acquisition ordering
- Missing type downcasting for DataFusion access

### TLogFS

The `tlogfs` crate implements **Delta Lake-backed filesystem persistence** - the critical bridge between TinyFS abstractions and DataFusion queries. This layer makes every filesystem operation queryable via SQL while maintaining ACID guarantees.

**Architecture Role**: TLogFS implements the `PersistenceLayer` trait from TinyFS, storing all filesystem operations as Arrow-formatted records in Delta Lake tables. Every file, directory, and metadata change becomes part of a queryable, versioned data lake.

**Key Features:**
- **Delta Lake Foundation**: All filesystem operations stored as versioned, ACID-compliant transactions
- **Arrow IPC Serialization**: Efficient binary storage format for filesystem metadata and content
- **QueryableFile Trait**: Files expose DataFusion `TableProvider` interfaces for direct SQL queries
- **Dynamic Factory System**: Create computed filesystem objects (SQL-derived files, temporal aggregations) that appear as native files
- **Transaction Sequencing**: Delta Lake versions become filesystem transaction boundaries
- **Unified Query Interface**: Query filesystem metadata, file contents, and computed objects with the same SQL API

**DataFusion Integration Architecture:**
```rust
// Every TLogFS file implements QueryableFile trait
trait QueryableFile {
    async fn as_table_provider(&self, ...) -> Arc<dyn TableProvider>;
}

// Files become queryable through DataFusion
let table_provider = oplog_file.as_table_provider(node_id, part_id, tx).await?;
let ctx = session_context.await?;
ctx.register_table("my_data", table_provider)?;
let df = ctx.sql("SELECT * FROM my_data WHERE temperature > 25").await?;
```

**Major Interfaces:**
- `OpLogPersistence` - Main persistence layer implementing TinyFS `PersistenceLayer` trait
- `TransactionGuard` - ACID transaction management with automatic Delta Lake versioning
- `OpLogFile` - Files that implement `QueryableFile` trait for DataFusion integration
- `SqlDerivedFile` - Dynamic files created from SQL queries over other files
- `FactoryRegistry` - Extensible system for creating computed filesystem objects
- `DirectoryTable`, `MetadataTable` - Query filesystem structure via SQL
- `TinyFsObjectStore` - DataFusion ObjectStore integration for seamless file access

## Layer Integration: How It All Works Together

**The Complete Stack in Action:**

1. **Applications** use **Steward** transaction guards to ensure ACID operations across dual filesystems
2. **Steward** provides transaction coordination, delegating filesystem operations to **TinyFS**  
3. **TinyFS** enforces type safety and path preservation, delegating persistence to **TLogFS**
4. **TLogFS** stores operations in **Delta Lake** and exposes files as **DataFusion** table providers
5. **DataFusion** executes SQL queries directly over Delta Lake storage via custom ObjectStore

**Query Integration Throughout:**
- **Files**: Implement `QueryableFile` trait → DataFusion `TableProvider`
- **Directories**: Queryable via `DirectoryTable` exposing filesystem metadata
- **Dynamic Objects**: SQL-derived files, temporal aggregations, CSV directories all appear as native filesystem objects
- **Transactions**: All operations logged in queryable Delta Lake tables with full version history

**Example: Complete Operation Flow**
```rust
// 1. Application creates transaction via Steward
let tx_guard = steward.begin_transaction().await?;

// 2. TinyFS navigation with path preservation
let file_node = tx_guard.root().lookup("sensors/temperature.parquet").await?;

// 3. Type-safe file access with TLogFS downcasting
let node_ref = file_node.borrow().await;
let file_handle = node_ref.as_file()?;
let oplog_file = file_handle.get_file().await.lock().await;

// 4. DataFusion integration via QueryableFile trait
if let Some(queryable) = try_as_queryable_file(&**oplog_file) {
    let table_provider = queryable.as_table_provider(node_id, part_id, &mut tx_guard).await?;
    
    // 5. SQL query execution over Delta Lake storage
    let ctx = tx_guard.session_context().await?;
    ctx.register_table("sensors", table_provider)?;
    let df = ctx.sql("SELECT * FROM sensors WHERE temperature > 25").await?;
    let results = df.collect().await?;
}

// 6. Transaction commit with Delta Lake versioning
tx_guard.commit(None).await?;
```

This architecture enables:
- **Query Everything**: All filesystem objects, metadata, and dynamic computations are SQL-queryable
- **Type Safety**: Compile-time prevention of filesystem API misuse
- **ACID Compliance**: Full transaction safety with automatic rollback and recovery
- **Version History**: Complete audit trail of all filesystem operations
- **Dynamic Objects**: Computed files (SQL views, aggregations) appear as native filesystem objects

### Steward

The `steward` crate orchestrates dual filesystem management with advanced transaction patterns:

**Key Features:**
- Manages both "data" and "control" filesystems using TLogFS
- Transaction guards (`StewardTransactionGuard`) for coordinated data/control operations
- Transaction sequencing and metadata tracking
- Post-commit action coordination
- Recovery mechanisms for crashed transactions
- Transaction descriptors (`TxDesc`) for command metadata

**Major Interfaces:**
- `Ship` - Main steward struct managing dual filesystems
- `Ship::create_pond()` - Create new pond with proper initialization
- `Ship::open_pond()` - Open existing pond
- `StewardTransactionGuard` - Transaction guard with dual filesystem access
- Transaction patterns: `transact()` closure and `begin_transaction()` manual
- Recovery system for crash consistency
- Access to underlying tlogfs through transaction guards

### CMD

The `cmd` crate provides the comprehensive `pond` CLI tool:

**Key Features:**
- Command-line interface built with `clap`
- Full integration with steward for pond management
- Transaction-based architecture using steward guards
- Supports multiple filesystem contexts (data/control)
- Pattern-based file operations with glob support
- Schema introspection and data export capabilities
- DataFusion SQL query support in `cat` command

**Major Commands:**
- `pond init` - Initialize new pond
- `pond recover` - Recover from crashes
- `pond cat` - Display file contents with SQL support and multiple output formats
- `pond show` - Display pond contents
- `pond list` - File listing with patterns
- `pond describe` - Schema introspection
- `pond copy` - Copy files between locations
- `pond mkdir` - Create directories
- `pond mknod` - Create filesystem nodes
- Filesystem choice flags for accessing data vs control filesystems

### HydroVu

The `hydrovu` crate provides data collection from HydroVu water monitoring systems:

**Key Features:**
- HydroVu API client with OAuth2 authentication
- Automatic data collection and transformation to Arrow format
- Integration with steward for transactional data storage
- Configurable device monitoring and parameter collection
- Schema management for water quality timeseries data

**Major Interfaces:**
- `HydroVuCollector` - Main data collection orchestrator
- `Client` - HydroVu API client with authentication
- Configuration system for devices and collection parameters
- Integration with pond filesystem for data storage
- Timeseries data transformation and standardization

### Current Transaction Architecture

**Transaction Guard Pattern:**
- `StewardTransactionGuard` provides coordinated access to both data and control filesystems
- Two transaction patterns:
  - `transact()` - Closure-based automatic commit/rollback
  - `begin_transaction()` - Manual transaction management
- Transaction sequence derived from `table.version() + 1`
- Automatic metadata recording in control filesystem
- Access to underlying tlogfs persistence through guards

**Dual Filesystem Architecture:**
- **Data filesystem**: User data and computed objects
- **Control filesystem**: Transaction metadata and steward configuration  
- Both use TLogFS with Delta Lake storage
- Coordinated by steward transaction guards for consistency
- Query access to underlying DeltaTable through transaction guards

**DataFusion Integration:**
- Query interfaces built on Delta Lake storage
- Direct access to filesystem metadata through DataFusion
- SQL query support in CLI commands (e.g., `pond cat` with SQL filtering)
- Unified table providers for filesystem objects

### Dynamic Filesystem Objects

TLogFS supports dynamic objects through its extensible factory system:
- **CSV directories** - Present CSV files as queryable DataFusion tables
- **SQL-derived nodes** - Computed filesystem objects from SQL queries 
- **Hostmount directories** - Mirror host filesystem paths within pond
- **Factory registry system** - Extensible plugin architecture for custom objects
- **Arrow-based computation** - Efficient data processing for dynamic objects

### Development Status

**Current State (August 2025):**
- ✅ Core filesystem stack (diagnostics → tinyfs → tlogfs → steward) fully operational
- ✅ Transaction guards providing safe dual-filesystem access
- ✅ CLI with comprehensive command set and DataFusion SQL integration
- ✅ HydroVu data collection with pond integration
- ✅ Comprehensive test coverage across all crates
- ✅ Factory system for dynamic filesystem objects
- 🔄 Ongoing restoration of advanced features and integrations

**Architecture Achievements:**
- Transaction-based architecture ensures ACID compliance
- Delta Lake provides versioned, queryable storage
- Dual filesystem design separates concerns cleanly
- Guard pattern enables safe concurrent access to persistence layers
- DataFusion integration enables SQL queries over filesystem metadata

### Testing and Validation

**Test Coverage:**
- ✅ Unit tests for all core components (diagnostics, tinyfs, tlogfs, steward, cmd)
- ✅ Integration tests demonstrating full pond lifecycle
- ✅ Transaction guard tests with commit/rollback scenarios
- ✅ CLI command tests with direct API usage (bypassing main CLI)
- ✅ Recovery scenario testing for crash consistency
- ✅ DataFusion query integration tests

**Validation Approach:**
- Temporary directory isolation for all tests
- Transaction-based test patterns mimicking real usage
- Output capture and validation for CLI commands
- Comprehensive error path testing
- Memory usage and performance characteristics validation

