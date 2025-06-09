# DuckPond - Project Brief

## Project Overview
DuckPond is a "very small data lake" designed as a local-first, Parquet-oriented file system for timeseries data collection, processing, and analysis. The project consists of a proof-of-concept implementation (frozen) and new production-quality replacement crates.

## Core Mission
Build a local-first data system that:
- Collects timeseries data from environmental monitoring sources (HydroVu, LakeTech)
- Transforms data into standardized Arrow-based representations
- Stores data efficiently using Parquet format
- Enables fast querying and analysis using DuckDB/DataFusion
- Generates downsampled timeseries for web visualization
- Provides backup and restore capabilities

## Key Components

### Proof-of-Concept (./src) - FROZEN REFERENCE
- **Main binary**: Command-line interface for pond operations
- **Pond module**: Core data management with directory abstraction
- **HydroVu integration**: Environmental data collection from www.hydrovu.com
- **Backup/restore**: S3-compatible cloud backup system
- **Data pipeline**: YAML-driven resource configuration and processing

### Replacement Crates (./crates) - ACTIVE DEVELOPMENT

#### 1. TinyFS Crate (`./crates/tinyfs`)
- **Purpose**: Virtual filesystem abstraction with pluggable storage backends
- **Architecture**: Backend trait system for clean separation of logic and storage
- **Core APIs**: `FS`, `WD` (working directory), `NodePath`, glob patterns
- **Advanced Features**: Dynamic directories via custom `Directory` trait implementations
- **Backend Architecture**: ✅ **COMPLETED** - Pluggable storage system via `FilesystemBackend` trait
  - Core filesystem logic completely decoupled from storage implementation
  - `MemoryBackend` for testing and lightweight use (via dedicated `memory/` module)
  - Ready for `OpLogBackend` for persistent storage with Delta Lake
- **Key Innovation**: Clean architecture enabling different storage systems through dependency injection
- **Zero Breaking Changes**: All existing APIs unchanged, 22 tests passing

#### 2. OpLog Crate (`./crates/oplog`) 
- **Purpose**: Operation logging system using Delta Lake + DataFusion
- **Architecture**: Two-layer data storage (Delta Lake outer, Arrow IPC inner)
- **Features**: ACID guarantees, time travel, schema evolution, SQL queries
- **Status**: Implementation complete and tested

#### 3. CMD Crate (`./crates/cmd`)
- **Purpose**: Command-line interface for pond management and operations
- **Features**: `pond init`, `pond show`, environment integration, error handling
- **Architecture**: Uses `clap` for CLI, integrates with TinyFS and OpLog
- **Status**: Core commands implemented and tested

#### 4. TinyLogFS Integration (`./crates/oplog/src/tinylogfs.rs`)
- **Purpose**: Hybrid filesystem combining TinyFS in-memory performance with OpLog persistence
- **Phase 1**: ✅ Complete - Schema design (OplogEntry/DirectoryEntry) with DataFusion integration
- **Next Phase**: OpLogBackend implementation using the new FilesystemBackend trait
- **Architecture Benefit**: Backend refactoring enables clean OpLog integration through standard interface
- **Features**: Real-time transaction visibility, commit/restore API, enhanced table providers

## Integration Vision
The replacement crates work together with a refined hybrid filesystem approach:

1. **TinyLogFS Hybrid Architecture**: Single-threaded design combining TinyFS in-memory performance with OpLog persistence using Arrow builder transaction state
2. **Enhanced Query Capabilities**: Real-time visibility of pending transactions through table provider builder snapshotting
3. **Simplified API**: Clear `commit()/restore()` semantics replacing complex sync operations
4. **Local-first mirroring**: Physical file copies synchronized from Delta Lake source of truth
5. **Arrow-native processing**: DataFusion provides efficient columnar query processing
6. **Improved reliability**: Delta Lake provides better consistency than individual Parquet files

## Current Focus
- **TinyFS Memory Module Organization**: ✅ Complete - Moved memory implementations to dedicated module structure
- **TinyLogFS Phase 2 Implementation**: Refined single-threaded architecture with Arrow builder transaction state
- **Enhanced Table Providers**: Real-time transaction visibility through builder snapshotting
- **CLI Extensions**: File operations with commit/restore API (ls, cat, mkdir, touch)
- **Performance Optimization**: Leverage single-threaded design benefits for cache locality and elimination of lock contention

## Technologies
- **Language**: Rust 2021 edition
- **Data**: Apache Arrow, Parquet, Delta Lake (delta-rs)
- **Query**: DataFusion, DuckDB (legacy)
- **Storage**: Local filesystem + cloud backup (S3-compatible)
- **Config**: YAML-based resource definitions
