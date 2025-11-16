# DuckPond Current Architecture Overview (November 2025)

## Executive Summary

DuckPond is a **query-native filesystem** that treats all filesystem operations as queryable data. It combines type-safe Rust abstractions with DataFusion SQL queries and Delta Lake ACID storage to create a unified platform where files, directories, metadata, and computed objects are all first-class SQL citizens.

**Core Innovation**: Every filesystem object can be queried with SQL, and SQL queries can create new filesystem objects that appear as native files and directories.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                        APPLICATIONS                             │
│  pond CLI • HydroVu Collector • Custom Tools                   │
├─────────────────────────────────────────────────────────────────┤
│                         STEWARD                                 │
│  Transaction Lifecycle • Control Table (Delta Lake)            │
│  Post-Commit Actions • Recovery Management                      │
├─────────────────────────────────────────────────────────────────┤
│                         TINYFS                                  │
│  Type-Safe API • Path Preservation • Node Navigation           │
│  PersistenceLayer Trait • Memory & Arrow Backends              │
├─────────────────────────────────────────────────────────────────┤
│                         TLOGFS                                  │
│  OpLog Persistence (Delta) • Dynamic Factory System            │
│  QueryableFile Trait • TinyFS ObjectStore Integration          │
├─────────────────────────────────────────────────────────────────┤
│                   DELTA LAKE + DATAFUSION                       │
│  ACID Storage • Versioning • SQL Query Engine                  │
│  ObjectStore Abstraction • Parquet Format                      │
└─────────────────────────────────────────────────────────────────┘
```

## Crate Structure

### Core Crates

#### `tinyfs` - Type-Safe Filesystem Abstraction
**Purpose**: Pure filesystem API with strong typing and pluggable persistence

**Key Components**:
- `FS` - Main filesystem with async operations
- `WD` (Working Directory) - Context for file operations
- `Node`, `File`, `Directory`, `Symlink` - Filesystem object types
- `PersistenceLayer` trait - Pluggable storage backends
- `NodeID` - Unique identifiers for all filesystem objects
- Arrow integration for data-aware operations

**Navigation Pattern**:
```rust
let tx_guard = persistence.begin().await?;
let root_wd = tx_guard.root();
let file_node = root_wd.lookup("sensors/data.parquet").await?;
```

**Key Insight**: TinyFS preserves both node identity AND access path through `dir::Pathed<Handle>`, enabling hierarchical operations while maintaining type safety.

#### `tlogfs` - Delta Lake Backed Filesystem
**Purpose**: Implements `PersistenceLayer` using Delta Lake, making all operations queryable

**Major Features**:
- **OpLog Persistence**: All filesystem operations stored as Arrow-formatted records in Delta Lake
- **QueryableFile Trait**: Files expose DataFusion `TableProvider` interfaces
- **Transaction Guards**: ACID guarantees with automatic rollback
- **Dynamic Factory System**: Create computed filesystem objects (SQL-derived files, temporal aggregations)
- **Schema Management**: Arrow schema validation and evolution
- **TinyFS ObjectStore**: DataFusion integration for seamless file access

**Core Files**:
- `persistence.rs` - OpLogPersistence implementation, State management
- `transaction_guard.rs` - ACID transaction lifecycle
- `query/queryable_file.rs` - Trait for SQL-queryable files
- `file.rs` - OpLogFile implementation
- `directory.rs` - OpLogDirectory implementation
- `factory.rs` - Dynamic factory registry system
- `tinyfs_object_store.rs` - DataFusion ObjectStore implementation

**Key Pattern - Single Transaction Rule**:
```rust
// ✅ CORRECT - Single transaction per operation
let tx_guard = persistence.begin().await?;
let state = tx_guard.state()?;
let df_context = state.datafusion_context();

// ❌ WRONG - Multiple transactions cause panic
let tx1 = persistence.begin().await?; // First transaction
let tx2 = persistence.begin().await?; // PANIC! Duplicate transaction
```

#### `steward` - Transaction Coordination & Audit
**Purpose**: Manages transaction lifecycle with control table for audit and recovery

**Components**:
- `Ship` - Main orchestrator managing data filesystem and control table
- `StewardTransactionGuard` - Wraps TLogFS transaction with lifecycle tracking
- `ControlTable` - Delta Lake-based transaction audit log with JSON query support

**Architecture**:
- **Data Filesystem**: OpLogPersistence (TLogFS) at `{pond_path}/data` - user data and computed objects
- **Control Table**: Delta Lake table at `{pond_path}/control` - transaction metadata, factory modes, pond identity
- Control table records transaction lifecycle (begin → data_committed → completed)
- Enables recovery from incomplete transactions

**Control Table Features**:
- Partitioned by `record_category` ("metadata" vs "transaction")
- JSON fields for CLI args, environment, factory modes
- DataFusion JSON functions for querying configuration
- Transaction lifecycle tracking (begin → data_committed → completed)
- Post-commit task coordination and sequencing

**Transaction Types**:
- **Write**: Modifies data filesystem, increments sequence number
- **Read**: Read-only, no sequence increment
- **PostCommit**: Factory execution after data commit

#### `cmd` - Command-Line Interface
**Purpose**: Comprehensive `pond` CLI built with clap

**Commands**:
- `pond init` - Initialize new pond with proper transaction #1
- `pond recover` - Recover from incomplete transactions
- `pond cat [--sql "SELECT ..."]` - Display files with SQL filtering
- `pond show [--mode brief|detailed]` - Display pond metadata
- `pond list [pattern]` - List files with glob patterns
- `pond describe` - Schema introspection
- `pond copy` - Copy between ponds or filesystems
- `pond mkdir` / `pond mknod` - Create filesystem objects
- `pond run` - Execute factory commands (HydroVu collection, remote sync)
- `pond control` - Query control table with SQL
- `pond temporal` - Set/detect temporal bounds for files

**Output Formats**: CSV, JSON, Parquet, Arrow IPC

#### `hydrovu` - Water Quality Data Collection
**Purpose**: HydroVu API integration with factory-based execution

**Features**:
- OAuth2 authentication with sensitive data taxonomy
- Device configuration and parameter collection
- Automatic Arrow schema generation
- Wide-format timeseries storage (timestamp as primary key)
- Factory-based execution via `pond run`

**Usage Pattern**:
```yaml
# /etc/system.d/20-hydrovu config
factory: "hydrovu"
config:
  client_id: "[REDACTED]"
  client_secret: "[REDACTED]"
  hydrovu_path: "/hydrovu"
  max_points_per_run: 5000
  devices:
    - name: "Boat Dock"
      id: 123456
      scope: "BDock"
```

```bash
# Collect data
pond run /etc/system.d/20-hydrovu collect
```

#### `utilities` - Shared Utilities
Simple utility crate with common helpers like banner formatting.

## Dynamic Factory System

### Overview
Factories create computed filesystem objects that appear as native files and directories. They are registered at compile-time using `linkme` and can be invoked declaratively via YAML configuration.

### Factory Types

#### 1. **sql-derived-table** & **sql-derived-series** Factories
Transform data using SQL queries over existing files.

**Modes**:
- `table`: Single file operations (errors if pattern matches >1 file)
- `series`: Multi-file and multi-version operations (FileSeries)

**Configuration**:
```yaml
entries:
  - name: "filtered_data"
    factory: "sql-derived-table"
    config:
      in_pattern: "/raw/measurements.parquet"
      query: "SELECT * FROM source WHERE temperature > 25"
```

**Architecture**: Uses DataFusion's native `ListingTable` with TinyFS ObjectStore for:
- Predicate pushdown to storage layer
- Streaming execution (no full dataset load)
- Optimal partition pruning

#### 2. **temporal-reduce** Factory
Creates time-bucketed aggregations with multiple resolution levels.

**Configuration**:
```yaml
entries:
  - name: "downsampled"
    factory: "temporal-reduce"
    config:
      in_pattern: "/raw/*"
      time_column: "timestamp"
      resolutions: [1h, 6h, 1d]
      aggregations:
        - type: "avg"
          columns: ["temperature", "pressure"]
        - type: "min"
        - type: "max"
```

**Output Structure**:
```
/downsampled/
  res=1h.series   # 1-hour buckets
  res=6h.series   # 6-hour buckets
  res=1d.series   # 1-day buckets
```

#### 3. **timeseries-join** Factory
Joins multiple time series by timestamp with full outer join semantics.

**Configuration**:
```yaml
entries:
  - name: "combined"
    factory: "timeseries-join"
    config:
      time_column: "timestamp"
      inputs:
        - pattern: "/sensors/temperature/*"
          scope: "temp"
        - pattern: "/sensors/pressure/*"
          scope: "press"
          range:
            begin: "2024-01-01T00:00:00Z"
```

**Features**:
- COALESCE timestamp from all inputs
- Optional per-input time range filtering
- Scope prefixing for column disambiguation
- Full outer join with EXCLUDE join condition

#### 4. **timeseries-pivot** Factory
Pivots long-format timeseries data to wide format (coming soon).

#### 5. **template** Factory
Renders Tera templates with discovered file patterns.

**Configuration**:
```yaml
entries:
  - name: "reports"
    factory: "template"
    config:
      in_pattern: "/data/*"
      out_pattern: "$0"
      template_file: "/templates/report.md.tmpl"
```

#### 6. **dynamic-dir** Factory
Composes multiple factories into a single directory.

**Configuration**:
```yaml
entries:
  - name: "analytics"
    factory: "dynamic-dir"
    config:
      entries:
        - name: "raw"
          factory: "sql-derived-series"
          config: { ... }
        - name: "aggregated"
          factory: "temporal-reduce"
          config: { ... }
```

#### 7. **remote** Factory (Executable)
Manages backup and replication to S3-compatible object stores.

**Commands**:
- `push` - Upload new transaction bundles (primary pond mode)
- `pull` - Download and apply bundles (replica pond mode)
- `replicate` - Generate replication configuration
- `list-bundles` - Show available backups
- `verify` - Check backup integrity

**Configuration**:
```yaml
factory: "remote"
config:
  url: "s3://backup-bucket"
  region: "us-west-2"
  key: "[REDACTED]"
  secret: "[REDACTED]"
  compression_level: 3
```

**Architecture**:
- Streaming tar+zstd compression (no temp files)
- Direct upload to object_store (local or S3)
- Bundle metadata with file manifests
- Pond identity preservation for replicas

#### 8. **hydrovu** Factory (Executable)
HydroVu data collection factory.

**Commands**:
- `collect` - Fetch sensor data from HydroVu API

## Transaction Architecture

### Single Transaction Rule (CRITICAL)

**The system enforces ONE active transaction per operation.** This is validated with a runtime panic guard.

```rust
// ✅ CORRECT Pattern
async fn operation() -> Result<()> {
    let tx_guard = persistence.begin().await?;
    
    // All work uses this single transaction
    let state = tx_guard.state()?;
    let df_ctx = state.datafusion_context();
    let root = tx_guard.root();
    
    // Helper functions receive transaction guard
    helper_function(&mut tx_guard).await?;
    
    tx_guard.commit(metadata).await?;
    Ok(())
}

async fn helper_function(
    tx: &mut StewardTransactionGuard<'_>
) -> Result<()> {
    let ctx = tx.session_context().await?;
    // Use passed transaction context
    Ok(())
}

// ❌ WRONG Pattern - Creates duplicate transactions
async fn broken_helper(store_path: &str) -> Result<()> {
    // This creates a second transaction - PANIC!
    let mut persistence = OpLogPersistence::open(store_path).await?;
    let tx = persistence.begin().await?; // Runtime panic here
    // ...
}
```

### Transaction Lifecycle

**Steward Transaction Flow**:
1. `Ship::begin_transaction()` creates `StewardTransactionGuard`
2. Control table records "begin" event
3. User operations modify data filesystem
4. `commit()` calls:
   - Data filesystem commit (Delta Lake transaction)
   - Control table records "data_committed" event
   - Post-commit factories execute
   - Control table records "completed" event
5. Transaction sequence increments

**Transaction Types**:
- **Write**: Modifies data, creates new Delta Lake version
- **Read**: No modifications, no version increment
- **PostCommit**: Factory execution in separate transaction

### State Management

The `State` object coordinates all operations within a transaction:

```rust
pub struct State {
    inner: Arc<Mutex<InnerState>>,
    object_store: Arc<OnceCell<TinyFsObjectStore>>,
    dynamic_node_cache: Arc<Mutex<HashMap<...>>>,
    template_variables: Arc<Mutex<HashMap<...>>>,
    table_provider_cache: Arc<Mutex<HashMap<...>>>,
}
```

**Key Points**:
- Single State per transaction
- Shared DataFusion SessionContext
- TableProvider caching (by node_id, part_id, version)
- Dynamic node caching for factory-created objects
- Template variable management

## DataFusion Integration

### QueryableFile Trait

All files that support SQL queries implement this trait:

```rust
#[async_trait]
pub trait QueryableFile: File {
    async fn as_table_provider(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        state: &State,
    ) -> Result<Arc<dyn TableProvider>, TLogFSError>;
}
```

**Implementers**:
- `OpLogFile` - Basic TLogFS files (Parquet, CSV, etc.)
- `SqlDerivedFile` - SQL-transformed files
- `TemporalReduceSqlFile` - Time-bucketed aggregations

### TinyFS ObjectStore

Custom `ObjectStore` implementation enables DataFusion to read TLogFS files:

```rust
// URL Pattern: tinyfs:///part/{part_id}/node/{node_id}/version/{version}
let url = "tinyfs:///part/abc123/node/def456/version/latest";

// DataFusion ListingTable can read directly from pond
let listing_table = ListingTable::try_new(config)?;
let df = ctx.sql("SELECT * FROM listing_table WHERE temp > 25").await?;
```

**Features**:
- Version selection (latest, all, specific)
- Partition pruning by node_id and part_id
- Streaming reads (no full file load)
- Metadata caching

### NodeID/PartID Relationships

**Critical for Delta Lake partition pruning**:

```
DeltaLake partition structure:
/table/part_id={parent_dir_id}/node_id={file_id}/version={v}/

Physical File:     part_id = parent_directory_node_id
Directory:         part_id = node_id (same value)
```

**Why this matters**: Correct part_id enables efficient partition filtering in Delta Lake queries.

```rust
// ✅ CORRECT - Use parent directory as part_id for files
let parent_path = file_path.parent();
let parent_node = root.resolve_path(&parent_path).await?;
let part_id = parent_node.id().await;

// ❌ WRONG - Using file's node_id as part_id breaks pruning
let part_id = file_node_id; // Full table scan!
```

## File Version Management

### Temporal Bounds

Files can have explicit temporal bounds stored in extended metadata:

```rust
// Set temporal bounds for a file
pond temporal set-bounds /sensors/temperature.parquet \
  --begin "2024-01-01T00:00:00Z" \
  --end "2024-12-31T23:59:59Z"

// Detect overlapping time ranges
pond temporal detect-overlaps /sensors/*
```

**Use Cases**:
- Efficient time-range queries without scanning files
- Overlap detection for data quality
- Partition pruning in temporal queries

### Version History

All file modifications create new versions (Delta Lake semantics):

```rust
// List all versions with metadata
let versions = root.list_file_versions(&path).await?;

for v in versions {
    println!("Version {}: {} bytes, modified at {}",
        v.version,
        v.size,
        v.modified_timestamp
    );
    
    // Extended metadata includes temporal bounds
    if let Some(meta) = v.extended_metadata {
        if let Some(min_time) = meta.get("min_event_time") {
            println!("  Time range: {} to {}",
                min_time,
                meta.get("max_event_time")?
            );
        }
    }
}
```

## Control Table Schema

The control table uses Delta Lake with partitioned storage:

```sql
-- Partitioning
record_category STRING  -- "metadata" or "transaction"

-- Transaction identity
txn_seq BIGINT          -- Sequence number (0 = initial metadata)
txn_id UUID             -- Transaction UUID
based_on_seq BIGINT     -- Parent transaction

-- Lifecycle
record_type STRING      -- begin | data_committed | completed | failed
timestamp BIGINT        -- Microseconds since epoch
transaction_type STRING -- read | write | post_commit

-- Context (JSON fields)
cli_args STRING         -- JSON array of command arguments
environment STRING      -- JSON object with env vars

-- Data linkage
data_fs_version BIGINT  -- Delta Lake version after commit

-- Error tracking
error_message STRING
duration_ms BIGINT

-- Post-commit coordination
parent_txn_seq BIGINT   -- Which transaction triggered this
execution_seq INT       -- Order in factory list
factory_name STRING     -- Which factory is executing
```

**Query Examples**:

```sql
-- Get all write transactions
SELECT txn_seq, cli_args, duration_ms
FROM control_table
WHERE record_category = 'transaction'
  AND transaction_type = 'write'
  AND record_type = 'completed';

-- Find failed transactions
SELECT txn_seq, error_message
FROM control_table
WHERE record_type = 'failed';

-- Get factory mode configuration
SELECT json_get_str(factory_modes, 'remote') as remote_mode
FROM control_table
WHERE record_category = 'metadata'
ORDER BY timestamp DESC
LIMIT 1;

-- Query with DataFusion JSON functions
SELECT txn_seq,
       cli_args->0 as command,
       environment->>'USER' as user
FROM control_table
WHERE cli_args ? 'mknod';
```

## Pond Identity & Replication

### Pond Metadata

Every pond has immutable identity metadata:

```rust
pub struct PondMetadata {
    pub pond_id: Uuid,              // UUID v7
    pub birth_timestamp: i64,       // Creation time (microseconds)
    pub birth_hostname: String,     // Where created
    pub birth_username: String,     // Who created it
}
```

**Preserved across replicas**: When creating a replica, the original pond's identity is maintained.

### Replication Workflow

**1. Primary Pond Setup**:
```bash
# Initialize pond with remote backup
pond init /data/primary
pond mknod /etc/system.d/10-remote --config remote-config.yaml

# Push mode: automatic backup post-commit
# (configured via factory_modes in control table)
```

**2. Generate Replication Config**:
```bash
pond run /etc/system.d/10-remote replicate > replica-config-base64.txt
```

**3. Create Replica**:
```bash
# Initialize replica with preserved identity
pond init /data/replica --config $(cat replica-config-base64.txt)

# Pull new bundles
pond run /etc/system.d/10-remote pull
```

**Bundle Format**:
- Streaming tar+zstd compression
- Metadata JSON sidecar
- Named by transaction sequence: `txn_00005.bundle.tar.zst`
- Contains all changed files for that transaction

## Error Handling Philosophy

### Fail-Fast Pattern

**Zero tolerance for silent fallbacks.** All errors must be surfaced and addressed.

```rust
// ❌ SILENT FALLBACK - Masks real problems
match root.list_file_versions(&path).await {
    Ok(infos) => infos,
    Err(e) => {
        log::warn!("Failed to list versions: {}", e);
        vec![] // Silent failure - hides infrastructure issues
    }
}

// ✅ FAIL-FAST - Forces problem diagnosis
let version_infos = root
    .list_file_versions(&path)
    .await
    .map_err(|e| StewardError::Dyn(
        format!("Cannot list versions for {}: {}", path, e).into()
    ))?;
```

### Distinguish Business Cases from System Failures

```rust
// ✅ LEGITIMATE BUSINESS CASE
match max_timestamp {
    None => {
        log::info!("No existing data for device {}, starting fresh", id);
        0 // Valid starting point
    }
    Some(ts) => ts,
}

// ✅ SYSTEM FAILURE - Propagate error
Err(e) => {
    return Err(StewardError::Dyn(
        format!("System error reading timestamps: {}", e).into()
    ));
}
```

## Data Taxonomy (Sensitive Data)

The `data_taxonomy` module provides type-safe wrappers for sensitive configuration:

```rust
use tlogfs::data_taxonomy::{ApiKey, ApiSecret, ServiceEndpoint};

pub struct HydroVuConfig {
    pub client_id: ApiKey<String>,      // Redacts in logs/serialization
    pub client_secret: ApiSecret<String>, // Redacts in logs/serialization
    pub endpoint: ServiceEndpoint<String>, // Marks as network endpoint
}

// Access actual values when needed
let key = config.client_id.as_declassified();
```

**Automatic Behavior**:
- `Debug` prints `[REDACTED]`
- `Serialize` outputs `[REDACTED]`
- Explicit `as_declassified()` required for access

## Testing Patterns

### Transaction-Based Tests

```rust
#[tokio::test]
async fn test_operation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pond_path = temp_dir.path().to_path_buf();
    
    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path).await.unwrap();
    
    // Begin transaction
    let meta = PondUserMetadata::new(vec!["test".into()]);
    let mut tx = ship.begin_transaction(&meta, TransactionType::Write)
        .await
        .unwrap();
    
    // Perform operations
    let root = tx.root();
    root.create_dir_path("/test").await.unwrap();
    
    // Commit
    tx.commit(None).await.unwrap();
}
```

### Factory Testing

```rust
#[tokio::test]
async fn test_sql_derived_factory() {
    // Create test pond with sample data
    let temp_dir = tempfile::tempdir().unwrap();
    let mut ship = Ship::create_pond(temp_dir.path()).await.unwrap();
    
    // Write source data
    let meta = PondUserMetadata::new(vec!["test".into()]);
    let mut tx = ship.begin_transaction(&meta, TransactionType::Write)
        .await.unwrap();
        
    // ... write parquet data ...
    tx.commit(None).await.unwrap();
    
    // Create SQL-derived node
    let config = SqlDerivedConfig {
        patterns: vec!["/source.parquet".into()],
        query: "SELECT * FROM source WHERE value > 10".into(),
    };
    
    // ... test factory creation and query ...
}
```

## CLI Examples

### Basic Operations

```bash
# Initialize pond
pond init /data/mypond

# Create directories
pond mkdir /data/mypond/sensors

# List files
pond list /data/mypond/sensors

# Copy file into pond
pond copy local.csv /sensors/data.csv

# Query file with SQL
pond cat /sensors/data.csv --sql "SELECT * FROM source WHERE temp > 25"

# Show pond metadata
pond show /data/mypond --mode detailed

# Describe file schema
pond describe /sensors/data.csv
```

### Factory Operations

```bash
# Create SQL-derived file
cat > config.yaml << EOF
factory: "sql-derived-table"
config:
  in_pattern: "/raw/measurements.parquet"
  query: "SELECT timestamp, temperature FROM source WHERE temperature > 20"
EOF

pond mknod --config config.yaml /processed/filtered.series

# Query the derived file
pond cat /processed/filtered.series

# Create temporal aggregation
cat > reduce.yaml << EOF
factory: "temporal-reduce"
config:
  in_pattern: "/sensors/*"
  time_column: "timestamp"
  resolutions: [1h, 1d]
  aggregations:
    - type: "avg"
      columns: ["temperature"]
EOF

pond mknod --config reduce.yaml /aggregated/hourly

# List available resolutions
pond list /aggregated/hourly
# Output: res=1h.series, res=1d.series
```

### Control Table Queries

```bash
# Query transaction history
pond control --sql "
  SELECT txn_seq, cli_args, duration_ms, timestamp
  FROM control_table
  WHERE record_category = 'transaction'
    AND record_type = 'completed'
  ORDER BY txn_seq DESC
  LIMIT 10"

# Find failed operations
pond control --sql "
  SELECT txn_seq, error_message
  FROM control_table
  WHERE record_type = 'failed'"

# Get factory configuration
pond control --sql "
  SELECT json_get_str(factory_modes, 'remote') as mode
  FROM control_table
  WHERE record_category = 'metadata'
  ORDER BY timestamp DESC
  LIMIT 1"
```

### Backup & Replication

```bash
# Setup primary pond with remote backup
cat > remote.yaml << EOF
factory: "remote"
config:
  url: "s3://my-backup-bucket"
  region: "us-west-2"
  key: "ACCESS_KEY"
  secret: "SECRET_KEY"
  compression_level: 3
EOF

pond mknod --config remote.yaml /etc/system.d/10-remote

# Manual push (normally automatic post-commit)
pond run /etc/system.d/10-remote push

# Create replica
pond run /etc/system.d/10-remote replicate > replica.txt
pond init /data/replica --config $(cat replica.txt)

# Pull updates on replica
cd /data/replica
pond run /etc/system.d/10-remote pull
```

## Performance Considerations

### TableProvider Caching

State maintains a cache of TableProvider instances to avoid:
- Repeated schema inference
- Duplicate ListingTable creation
- Redundant file metadata queries

**Cache Key**: `(node_id, part_id, version_selection)`

### Streaming vs In-Memory

**Prefer streaming for large datasets**:
- `ListingTable` for file-based sources (streams from storage)
- `MemTable` only for small in-memory data

### Partition Pruning

**Critical for performance**:
- Use correct part_id (parent directory for files)
- Delta Lake partitions by `part_id` and `node_id`
- Queries with node_id/part_id filters use partition pruning

```sql
-- ✅ Efficient - uses partition pruning
SELECT * FROM delta_table
WHERE node_id = 'abc123' AND part_id = 'parent456';

-- ❌ Slow - full table scan
SELECT * FROM delta_table
WHERE timestamp > '2024-01-01';
```

## Known Limitations & Future Work

### Current Limitations

1. **No distributed execution**: Single-node DataFusion only
2. **Limited predicate pushdown**: Temporal filters applied after scan
3. **No incremental aggregations**: Temporal reduce recalculates all buckets
4. **Bundle-based replication**: Not optimized for high-frequency updates
5. **Schema evolution**: Limited support for schema changes

### Planned Enhancements

1. **Incremental temporal reduce**: Only recalculate affected buckets
2. **Better temporal filtering**: Push time bounds to Parquet row groups
3. **Streaming replication**: Continuous sync instead of pull-based
4. **Advanced factories**: More SQL operations, ML transforms
5. **Query optimization**: Better use of statistics and metadata

## Development Guidelines

### Critical Rules

1. **One transaction per operation** - Never create multiple concurrent transaction guards
2. **Pass transaction guards** - Don't pass `store_path` to helpers, pass `&mut StewardTransactionGuard`
3. **Fail fast** - No silent fallbacks or default values on system errors
4. **Correct part_id** - Files use parent directory node_id for partition pruning
5. **Factory composability** - New factories should integrate with dynamic-dir
6. **Test with transactions** - All tests should use proper transaction lifecycle

### Common Anti-Patterns

```rust
// ❌ WRONG: Multiple transactions
let tx1 = persistence.begin().await?;
let tx2 = persistence.begin().await?; // PANIC!

// ❌ WRONG: Helper creates own transaction
async fn helper(store_path: &str) -> Result<()> {
    let mut p = OpLogPersistence::open(store_path).await?;
    let tx = p.begin().await?; // Duplicate!
}

// ❌ WRONG: Silent fallback
.unwrap_or_default() // Masks errors

// ❌ WRONG: Wrong part_id
let part_id = node_id; // Should be parent_node_id for files!
```

### Correct Patterns

```rust
// ✅ Single transaction
let tx = persistence.begin().await?;
helper(&mut tx).await?;
tx.commit(meta).await?;

// ✅ Pass transaction guard
async fn helper(tx: &mut StewardTransactionGuard<'_>) -> Result<()> {
    let ctx = tx.session_context().await?;
    // Use passed context
}

// ✅ Fail fast
let result = operation().await
    .map_err(|e| Error::from(e))?;

// ✅ Correct part_id
let parent_node_id = resolve_parent(&file_path).await?;
let part_id = parent_node_id;
```

## Architecture Decisions

### Why Delta Lake?

- ACID transactions with automatic rollback
- Time travel and versioning built-in
- Parquet format for efficient columnar storage
- Wide ecosystem compatibility

### Why DataFusion?

- Native Rust with excellent async support
- Extensible via ObjectStore trait
- SQL dialect familiar to users
- Streaming execution model

### Why Separate Control Table?

- Separates concerns (user data vs transaction audit)
- Control table always available even if data filesystem corrupted
- Post-commit actions tracked independently
- Recovery logic simplified
- Transaction metadata queryable via SQL without affecting user data

### Why Factory System?

- Declarative configuration over imperative code
- Composability through dynamic-dir
- SQL-native data transformations
- Enables non-programmers to create complex pipelines

## Glossary

- **Pond**: A DuckPond filesystem instance (directory with data/ and control/ subdirs)
- **NodeID**: Unique identifier for filesystem objects (files, dirs, symlinks)
- **PartID**: Partition identifier (parent directory node_id for files)
- **Transaction Guard**: RAII wrapper ensuring ACID transaction semantics
- **State**: Per-transaction coordinator for DataFusion context and caches
- **Factory**: Plugin that creates dynamic filesystem objects
- **QueryableFile**: Trait for files that expose SQL query interfaces
- **OpLog**: Operation log stored in Delta Lake
- **Control Table**: Transaction audit log in secondary filesystem
- **Bundle**: Compressed backup archive (tar+zstd) of pond data
- **Temporal Bounds**: Explicit time range metadata for files

---

**Last Updated**: November 15, 2025  
**Version**: Based on jmacd/fifteen branch

For historical context, see:
- `docs/duckpond-overview.md` - Original architecture vision
- `docs/duckpond-system-patterns.md` - Critical implementation patterns
- Individual `docs/*-complete.md` files - Feature completion snapshots
