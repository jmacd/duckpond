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
- **Purpose**: In-memory filesystem abstraction with dynamic files
- **Features**: Files, directories, symlinks, pattern matching, recursive descent
- **Key APIs**: `FS`, `WD` (working directory), `NodePath`, glob patterns
- **Advanced**: Dynamic directories via custom `Directory` trait implementations

#### 2. OpLog Crate (`./crates/oplog`) 
- **Purpose**: Operation logging system using Delta Lake + DataFusion
- **Architecture**: Two-layer data storage (Delta Lake outer, Arrow IPC inner)
- **Features**: ACID guarantees, time travel, schema evolution, SQL queries
- **Status**: Implementation complete and tested

## Integration Vision
The replacement crates will work together to recreate the proof-of-concept functionality:

1. **TinyFS state stored in OpLog nodes**: Directory structures as Delta Lake partitions
2. **Local-first mirroring**: Physical file copies synchronized from Delta Lake source of truth
3. **Enhanced querying**: DataFusion replaces/augments DuckDB for Arrow-native processing
4. **Improved reliability**: Delta Lake provides better consistency than individual Parquet files

## Current Focus
- Complete TinyFS implementation
- Integrate TinyFS with OpLog for state management
- Develop command-line tool for mirror reconstruction
- Maintain compatibility with proof-of-concept pipeline concepts

## Technologies
- **Language**: Rust 2021 edition
- **Data**: Apache Arrow, Parquet, Delta Lake (delta-rs)
- **Query**: DataFusion, DuckDB (legacy)
- **Storage**: Local filesystem + cloud backup (S3-compatible)
- **Config**: YAML-based resource definitions
