# Technical Context - DuckPond Implementation

## Technology Stack

### Crate Architecture (Memory-Safe and Production Ready)

#### Five-Crate Clean Architecture with Memory Safety
The DuckPond system has evolved into a production-ready five-crate architecture with comprehensive memory safety guarantees:

```
crates/
├── tinyfs/           # Virtual filesystem abstraction (memory-safe)
├── oplog/            # Delta Lake operation logging (core types)  
├── tlogfs/           # Integration layer (tinyfs + oplog + DataFusion)
├── steward/          # Transaction management and coordination
├── cmd/              # CLI interface using tlogfs
└── diagnostics/      # Logging and diagnostic functionality
```

**Dependency Relationships**:
- **cmd** → **steward** → **tlogfs** (CLI uses complete transaction-safe filesystem)
- **tlogfs** → **oplog** + **tinyfs** (integration layer)
- **oplog**, **tinyfs**, **diagnostics** → independent (no circular dependencies)

#### TinyFS Crate (`./crates/tinyfs`)
- **Purpose**: Virtual filesystem abstraction with pluggable backends
- **Architecture**: Backend trait system for storage independence  
- **Memory Safety**: ✅ **COMPLETED** - All dangerous `&[u8]` interfaces replaced with streaming patterns
- **Convenience Helpers**: ✅ **Available** - Safe `async_helpers::convenience` module for test code
- **Glob System**: ✅ **Production Ready** - Comprehensive pattern matching with `/**` recursive support
- **Arrow Integration**: ✅ **Complete** - Full Arrow Parquet integration with type safety
- **Seek Support**: ✅ **Unified Architecture** - AsyncRead + AsyncSeek with 65 tests passing
- **Key Components**: 
  - `WD` (Working Directory): Path resolution and traversal with memory-safe operations
  - `ParquetExt` trait: High-level ForArrow and low-level RecordBatch operations
  - `AsyncReadSeek`: Unified streaming interface with seek capabilities
- **Status**: ✅ Production ready with comprehensive memory safety and Arrow ecosystem integration

#### OpLog Crate (`./crates/oplog`)
- **Purpose**: Core Delta Lake types and error handling
- **Architecture**: Foundational types for operation logging
- **Key Types**: `OplogEntry`, `ForArrow` trait, error types, large file storage
- **Status**: ✅ Production ready with stable schema and large file support

#### TLogFS Crate (`./crates/tlogfs`) 
- **Purpose**: Integration layer combining TinyFS + OpLog with DataFusion queries
- **Architecture**: Complete filesystem implementation with persistence and SQL
- **Large File Storage**: ✅ **Complete** - 64 KiB threshold with content-addressed external storage
- **Memory Safety**: ✅ **Secured** - Uses streaming patterns throughout for safe operations
- **Query Interface**: DataFusion SQL capabilities through structured abstraction layers (53 tests passing)
- **Status**: ✅ Production ready with comprehensive functionality and memory safety

#### Steward Crate (`./crates/steward`)
- **Purpose**: Transaction management and coordination layer  
- **Architecture**: Multi-filesystem transaction coordination with ACID guarantees
- **Memory Safety**: ✅ **Implemented** - Uses streaming patterns for transaction metadata
- **Features**: Crash recovery, transaction safety, multi-filesystem coordination
- **Status**: ✅ Production ready with 11 tests passing and comprehensive transaction safety

#### CMD Crate (`./crates/cmd`)
- **Purpose**: Command-line interface for pond operations
- **Dependencies**: Uses `steward` for all transaction-safe operations
- **Memory Safety**: ✅ **Guaranteed** - All file operations use memory-safe patterns
- **Features**: Complete command set with Parquet display and streaming operations
- **Status**: ✅ Production ready with enhanced diagnostics and memory-safe operations

#### Diagnostics Crate (`./crates/diagnostics`)
- **Purpose**: Logging and diagnostic functionality
- **Features**: Structured logging with proper error surfacing (no more silent failures)
- **Integration**: Used throughout system for enhanced error visibility
- **Status**: ✅ Production ready with 2 tests passing and enhanced error reporting

### Core Technologies

#### Apache Arrow Ecosystem
- **Version**: 55.x (aligned across all crates)
- **Purpose**: Columnar data format, schema definitions, IPC serialization
- **Key APIs**: `RecordBatch`, `Schema`, `Field`, `StreamWriter/Reader`
- **Usage**: Foundation for all data operations

#### Delta Lake (delta-rs)
- **Version**: 0.26 with datafusion feature
- **Purpose**: ACID storage layer with versioning and time travel
- **Key APIs**: `DeltaOps`, `create()`, `write()`, `load()`
- **Learning Status**: Proficient with basic operations, advanced features in progress

#### DataFusion
- **Version**: 47.0.0
- **Purpose**: SQL query engine and DataFrame processing
- **Key APIs**: `SessionContext`, `TableProvider`, `ExecutionPlan`
- **Integration**: Custom table providers for Delta Lake bridge
- **Current Challenge**: Table registration conflicts in SessionContext requiring API research for proper deregistration/conditional registration patterns

#### DuckDB (Legacy)
- **Version**: Latest stable
- **Purpose**: SQL processing in proof-of-concept
- **Status**: Being replaced by DataFusion for Arrow-native processing
- **Usage**: Still used for complex SQL operations in proof-of-concept

### Supporting Technologies

#### TinyFS Architecture Evolution
- **Current State**: ✅ **MEMORY-SAFE PRODUCTION READY** - Complete architecture with guaranteed memory safety
- **Memory Safety**: ✅ **ACHIEVED** - All dangerous `&[u8]` interfaces removed from production code
- **Arrow Parquet Integration**: ✅ **Complete** - Full ForArrow trait with high-level and low-level APIs
- **Streaming Architecture**: ✅ **Implemented** - Memory-efficient operations for files of any size
- **Seek Support**: ✅ **Unified Architecture** - AsyncRead + AsyncSeek interface eliminating dual methods  
- **Large File Storage**: ✅ **Operational** - 64 KiB threshold with content-addressed external storage
- **Memory Efficiency**: ✅ **Achieved** - O(batch_size) vs O(file_size) streaming operations
- **Test Coverage**: ✅ **Comprehensive** - 142 tests passing across all functionality
- **Error Handling**: ✅ **Enhanced** - Silent failures eliminated, proper error surfacing
- **Type Safety**: ✅ **Guaranteed** - Entry type preservation works correctly across operations
  - **Phase 1**: ✅ **Complete** - PersistenceLayer trait and OpLogPersistence implementation
  - **Phase 2**: ✅ **Complete** - Direct OplogEntry storage eliminating Record wrapper confusion
  - **Phase 3**: ✅ **Complete** - Full Arrow integration with ForArrow trait and RecordBatch APIs
  - **Phase 4**: ✅ **Complete** - Memory-efficient streaming with unified seek architecture
  - **Phase 5**: ✅ **Complete** - Memory safety cleanup with critical bug fixes
- **Key Innovation**: Memory-safe backend abstraction enabling different storage systems through dependency injection
- **Directory Mutations**: Tombstone-based versioning with Delta Lake native cleanup
- **Production Achievement**: Memory-safe system with comprehensive Arrow ecosystem integration and enhanced reliability

#### Serialization & Configuration
- **serde**: Rust struct serialization
- **serde_arrow**: Bridge between Rust structs and Arrow
- **YAML**: Configuration file format (via `serde_yaml`)
- **tera**: Template engine for configuration expansion

#### Storage & Backup
- **Parquet**: Columnar file format for analytics
- **S3-compatible APIs**: Cloud backup (Cloudflare R2, AWS S3)
- **zstd**: Compression for backup operations

## Development Environment

### Build System
- **Cargo**: Standard Rust package manager
- **Workspaces**: Multi-crate project structure
- **Linting**: Strict clippy configuration with custom rules
- **Testing**: Standard Rust testing with tokio integration

### Dependencies Overview
```toml
# Core data processing
deltalake = { version = "0.26", features = ["datafusion"] }
datafusion = "47.0.0"
arrow = { version = "55", features = ["prettyprint"] }
arrow-array = "55.1.0"
serde_arrow = { version = "0.13.3", features = ["arrow-55"] }

# Async and utilities
tokio = { version = "1", features = ["full"] }
async-stream = "0.3"
anyhow = "1.0"

# Configuration and serialization
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
tera = "1.19"

# Command-line interface
clap = { version = "4.5.4", features = ["derive"] }
assert_cmd = "2.0"  # For CLI testing
tempfile = "3.8"    # For test environments

# Legacy proof-of-concept
duckdb = { version = "1.0", features = ["bundled"] }
parquet = "55.0"
```

## Architecture Constraints

### Version Compatibility
- **Critical**: Arrow/DataFusion/Delta versions must align
- **Challenge**: Rapid evolution of Delta Lake ecosystem
- **Strategy**: Pin to tested version combinations
- **Monitoring**: Regular updates with compatibility testing

### Memory Management
- **Arrow Columnar**: Efficient memory layout for analytics
- **Reference Counting**: `Arc<T>` for shared data structures
- **Stream Processing**: Bounded memory usage for large datasets
- **Async Boundaries**: Careful lifetime management across await points

### Performance Requirements
- **Query Latency**: Sub-second response for common operations
- **Throughput**: Handle continuous data ingestion
- **Storage Efficiency**: Compact representation with compression
- **Parallelism**: Multi-core processing for batch operations

## Current Technical State

### Proof-of-Concept (./src) - FROZEN
```rust
// Core patterns established
trait TreeLike { /* directory abstraction */ }
trait ForArrow { /* schema conversion */ }
struct Pond { /* resource management */ }

// Working features
- Command-line interface ✅
- YAML resource configuration ✅
- HydroVu API integration ✅
- Parquet file management ✅
- S3 backup/restore ✅
- DuckDB query processing ✅
- Static website generation ✅
```

### TinyFS Crate (./crates/tinyfs)
```rust
// Core implementation complete
struct FS { /* filesystem root */ }
struct WD { /* working directory */ }
struct NodePath { /* path + node reference */ }

// Memory implementations (dedicated module)
mod memory {
    pub struct MemoryFile { /* in-memory file content */ }
    pub struct MemoryDirectory { /* BTreeMap-based directory */ }
    pub struct MemorySymlink { /* simple path target storage */ }
}

// Advanced features
- Glob pattern matching ✅
- Dynamic directories ✅
- Symlink support ✅
- Recursive operations ✅
- Memory module organization ✅
```

### OpLog Crate (./crates/oplog)
```rust
// Implementation complete and tested
struct Record { /* outer schema */ }
struct Entry { /* inner schema */ }
struct ByteStreamTable { /* DataFusion integration */ }

// Verified functionality
- Delta Lake read/write ✅
- Arrow IPC serialization ✅
- DataFusion table provider ✅
- SQL queries over nested data ✅
```

### CMD Crate (./crates/cmd)
```rust
// Command-line interface implementation
#[derive(Parser)]
struct Cli { /* clap integration */ }

// Core commands implemented
- pond init: Create new ponds ✅
- pond show: Display operation logs ✅
- Environment integration (POND variable) ✅
- Comprehensive error handling ✅
- Integration testing with subprocess validation ✅
```

## Integration Challenges

### Schema Evolution
- **Problem**: DataFusion schema changes require table recreation
- **Solution**: Arrow IPC inner layer provides flexibility
- **Status**: Implemented and tested

### Async Stream Integration
- **Problem**: DataFusion requires specific stream traits
- **Solution**: `async-stream` crate for proper adaptation
- **Status**: Working correctly

### Type Safety Across Boundaries
- **Problem**: Arrow ↔ Rust type conversions
- **Solution**: `ForArrow` trait with compile-time validation
- **Status**: Established pattern, needs expansion

## Operational Considerations

### Error Handling Strategy
- **Library**: `anyhow` for context-rich errors
- **Pattern**: Result propagation with contextual information
- **Recovery**: Graceful degradation where possible
- **Logging**: Structured logging for operational insight

### Testing Approach
- **Unit Tests**: Individual component validation
- **Integration Tests**: End-to-end data flow verification  
- **CLI Testing**: Subprocess validation using `assert_cmd` crate
- **Property Tests**: Schema compatibility and data integrity
- **Performance Tests**: Benchmark critical operations

### Development Workflow
- **Workspace Structure**: Multi-crate project with shared dependencies
- **CLI Development**: Test-driven development with subprocess validation
- **Error Handling**: Consistent patterns with `anyhow` for user-facing tools
- **Documentation**: Memory bank system for project continuity

### Documentation Standards
- **Code Comments**: Inline documentation for complex algorithms
- **API Documentation**: Rustdoc for public interfaces
- **Architecture Documents**: High-level system design
- **Memory Bank**: Operational knowledge preservation

## Future Technical Evolution

### Near-term Goals
1. Complete TinyFS ↔ OpLog integration
2. Implement local mirror synchronization
3. Develop command-line reconstruction tool
4. Performance optimization and benchmarking

### Long-term Vision
1. Replace DuckDB with DataFusion throughout
2. Advanced query optimization
3. Distributed processing capabilities
4. Real-time data streaming support

#### Rust Ecosystem
- **Edition**: 2021
- **Features**: Heavy use of async/await, advanced type system
- **Key Crates**: 
  - `serde_arrow` for struct ↔ Arrow conversion
  - `async-stream` for DataFusion integration
  - `anyhow` for error handling
  - `tokio` for async runtime
  - `async-trait` for async trait implementations
  - `uuid` for unique identifiers

#### Diagnostics and Logging System (July 2, 2025)
- **Architecture**: Shared `diagnostics` crate with emit-rs backend
- **Purpose**: Structured, configurable logging for debugging and monitoring
- **Key Components**:
  - `diagnostics` crate: Centralized logging configuration and macros
  - `emit-rs`: Professional structured logging backend
  - `emit_term`: Terminal output formatter
- **Macros**: `log_info!`, `log_debug!` with key-value syntax
- **Configuration**: Environment variable `DUCKPOND_LOG` (off, info, debug)
- **Benefits**: Performance-friendly, structured output, configurable levels
- **Status**: ✅ Complete - all legacy print statements eliminated
