## Repository Overview

This repository contains an original proof-of-concept implementation
for DuckPond at the top-level of the repository, with a main crate,
hydrovu, and pond sub-modules. This implementation has been frozen,
and GitHub Copilot will not modify any of the Rust code under ./src,
however it makes a good reference as to the intentions behind our new
development.

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

The `tinyfs` crate implements a pure filesystem abstraction with pluggable persistence:

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

### TLogFS

The `tlogfs` crate implements Delta Lake-backed filesystem persistence with advanced query capabilities:

**Key Features:**
- Delta Lake integration for versioned, ACID-compliant storage
- Arrow IPC serialization for efficient data storage  
- Transaction sequencing using Delta Lake versions
- Directory operation coalescing for performance
- DataFusion query interfaces for filesystem metadata
- Dynamic factory system for computed filesystem objects
- Large file handling with separate storage strategies
- Query optimization and schema evolution support

**Major Interfaces:**
- `OpLogPersistence` - Main persistence layer implementing `PersistenceLayer`
- `TransactionGuard` - Individual transaction management with commit/rollback
- `OplogEntry` schema for filesystem operations
- `VersionedDirectoryEntry` for directory state
- Query interfaces: `DirectoryTable`, `MetadataTable`, `UnifiedTableProvider`
- Factory system: `FactoryRegistry`, `FactoryContext` for dynamic objects
- Delta Lake integration: Direct access to `DeltaTable` for queries
- File writers with clean write path and content addressing

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

