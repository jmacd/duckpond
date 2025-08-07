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

The ./crates sub-folder contains production-quality software implementing 
the DuckPond architecture. Each crate has a specific role in the overall system:

### Core Architecture Overview

```
cmd (pond CLI) → steward (Ship) → tlogfs (OpLogPersistence) → tinyfs (FS) → diagnostics
                      ↓
                 Delta Lake Storage
```

### Diagnostics

The `diagnostics` crate provides centralized logging for all DuckPond components:

**Key Features:**
- Environment-controlled logging via `DUCKPOND_LOG` (off/info/debug/warn/error)
- Built on the `emit` library with structured logging capabilities
- Ergonomic macros: `debug!()`, `info!()`, `warn!()`, `error!()`
- Automatic variable capture from scope
- Wildcard import pattern: `use diagnostics::*;`

**Major Interface:**
- `init_diagnostics()` - Initialize logging based on environment
- Logging macros with automatic field capture
- Integration across all other crates for consistent diagnostics

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

The `tlogfs` crate implements Delta Lake-backed filesystem persistence:

**Key Features:**
- Delta Lake integration for versioned, ACID-compliant storage
- Arrow IPC serialization for efficient data storage
- Transaction sequencing using Delta Lake versions
- Directory operation coalescing for performance
- DataFusion query interfaces for filesystem metadata
- Dynamic factory system for computed filesystem objects
- Large file handling with separate storage strategies

**Major Interfaces:**
- `OpLogPersistence` - Main persistence layer implementing `PersistenceLayer`
- `OplogEntry` schema for filesystem operations
- `VersionedDirectoryEntry` for directory state
- `DeltaTableManager` for Delta Lake operations
- Query interfaces: `DirectoryTable`, `MetadataTable`, `SeriesTable`
- Factory system for dynamic filesystem objects (CSV, SQL-derived, etc.)
- `create_oplog_fs()` - Main entry point for filesystem creation

### Steward

The `steward` crate orchestrates dual filesystem management:

**Key Features:**
- Manages both "data" and "control" filesystems using TLogFS
- Transaction sequencing and metadata tracking
- Post-commit action coordination
- Recovery mechanisms for crashed transactions
- Transaction descriptors (`TxDesc`) for command metadata

**Major Interfaces:**
- `Ship` - Main steward struct managing dual filesystems
- `Ship::initialize_new_pond()` - Create new pond with proper initialization
- `Ship::open_existing_pond()` - Open existing pond
- Transaction lifecycle: begin, commit, rollback
- Recovery system for crash consistency
- `TxDesc` for command metadata serialization

### CMD

The `cmd` crate provides the `pond` CLI tool:

**Key Features:**
- Command-line interface built with `clap`
- Integrates with steward for pond management
- Supports multiple filesystem contexts (data/control)
- Pattern-based file operations with glob support
- Schema introspection and data export capabilities

**Major Interfaces:**
- `pond init` - Initialize new pond
- `pond recover` - Recover from crashes
- `pond show` - Display pond contents
- `pond list` - File listing with patterns
- `pond describe` - Schema introspection
- Filesystem choice flags for accessing data vs control filesystems

### Current Transaction Model

**Single Command = Single Transaction:**
- Each CLI command is one atomic Delta Lake transaction
- Transaction sequence derived from `table.version() + 1`
- No cross-command state persistence
- Commit or rollback required to finalize transactions

**Dual Filesystem Architecture:**
- **Data filesystem**: User data and computed objects
- **Control filesystem**: Transaction metadata and steward configuration
- Both use TLogFS with Delta Lake storage
- Coordinated by steward for consistency

### Dynamic Filesystem Objects

TLogFS supports dynamic objects through its factory system:
- CSV directories that present CSV files as queryable tables
- SQL-derived nodes computed from other filesystem data  
- Hostmount directories that mirror host filesystem paths
- Extensible factory registration system

