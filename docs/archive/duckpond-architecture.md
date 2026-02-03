# DuckPond Architecture Overview (December 2025)

## Executive Summary

DuckPond is a **query-native filesystem** that treats all filesystem operations as queryable data. It combines type-safe Rust abstractions with DataFusion SQL queries and Delta Lake ACID storage to create a unified platform where files, directories, metadata, and computed objects are all first-class SQL citizens.

**Core Innovation**: Every filesystem object can be queried with SQL, and SQL queries can create new filesystem objects that appear as native files and directories.

**Version**: Current as of December 25, 2025 (based on main branch)

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

DuckPond consists of eight Rust crates organized into three layers:

### Core Filesystem Layer

#### `tinyfs` - Type-Safe Filesystem Abstraction
**Purpose**: Pure filesystem API with strong typing and pluggable persistence

**Key Components**:
- `FS` - Main filesystem with async operations
- `WD` (Working Directory) - Context for file operations with path preservation
- `Node`, `File`, `Directory`, `Symlink` - Filesystem object types
- `PersistenceLayer` trait - Pluggable storage backends
- `NodeID`, `FileID`, `PartID` - Unique identifiers for filesystem objects
- `TransactionGuard` - ACID transaction wrapper
- `CachingPersistence` - Transparent node caching decorator

**Navigation Pattern**:
```rust
let tx_guard = persistence.begin().await?;
let root_wd = tx_guard.root();
let file_node = root_wd.lookup("sensors/data.parquet").await?;
let file_handle = file_node.as_file()?;
let data = file_handle.read_to_end().await?;
```

**Key Insight**: TinyFS preserves both node identity AND access path through `dir::Pathed<Handle>`, enabling hierarchical operations while maintaining type safety.

**Exports**:
- Core API: `FS`, `WD`, `Node`, `File`, `Directory`, `Symlink`
- Identifiers: `NodeID`, `FileID`, `PartID`, `NodePath`
- Persistence: `PersistenceLayer`, `TransactionGuard`, `FileVersionInfo`
- Context: `FactoryContext`, `ProviderContext`, `PondMetadata`
- Testing: `MemoryPersistence`, `buffer_helpers`

#### `tlogfs` - Delta Lake Backed Filesystem
**Purpose**: Implements `PersistenceLayer` using Delta Lake, making all operations queryable

**Major Features**:
- **OpLogPersistence**: All filesystem operations stored as Arrow-formatted records in Delta Lake
- **Transaction Guards**: ACID guarantees with automatic rollback
- **Dynamic Factory System**: Create computed filesystem objects (SQL-derived files, temporal aggregations)
- **Schema Management**: Arrow schema validation and evolution
- **TinyFS ObjectStore**: DataFusion integration for seamless file access via `tinyfs://` URLs
- **Large Files**: Content-addressed storage for files >10MB with SHA256 deduplication
- **Data Taxonomy**: Type-safe wrappers for sensitive data (API keys, secrets, endpoints)

**Core Modules**:
- `persistence.rs` - OpLogPersistence implementation with State management
  - `State`: Per-transaction coordinator with DataFusion SessionContext
  - `InnerState`: Records, directory cache, Delta table references
  - `TableProviderKey`: Cache key for avoiding repeated schema inference
- `transaction_guard.rs` - ACID transaction lifecycle with panic guards
- `query/` - DataFusion query execution
  - `sql_executor.rs`: Execute SQL on files with temporal filtering
  - `temporal_filter.rs`: Apply time-range filters to queries
- `file.rs` - OpLogFile implementation
- `directory.rs` - OpLogDirectory with in-memory cache
- `symlink.rs` - OpLogSymlink implementation
- `schema.rs` - Arrow schemas for OplogEntry and DirectoryEntry
- `large_files.rs` - Content-addressed storage for large files
- `data_taxonomy.rs` - Sensitive data classification with automatic redaction
- `txn_metadata.rs` - Transaction metadata types (PondTxnMetadata, PondUserMetadata)

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

**Delta Lake Schema**:
The OplogEntry is stored in Delta Lake with these key columns:
- Partition: `part_id` (parent directory for files, self for directories)
- Identity: `node_id`, `name`, `entry_type`, `version`
- File Content: `sha256` (content hash), `size`, `file_type`
- Temporal: `timestamp`, `min_event_time`, `max_event_time`, `min_override`, `max_override`
- Dynamic: `factory` (factory type), `extended_attributes` (JSON metadata)
- Audit: `txn_seq` (transaction sequence number)

#### `steward` - Transaction Coordination & Audit
**Purpose**: Manages transaction lifecycle with control table for audit and recovery

**Components**:
- `Ship` - Main orchestrator managing data filesystem and control table
- `StewardTransactionGuard` - Wraps TLogFS transaction with lifecycle tracking
- `ControlTable` - Delta Lake-based transaction audit log with JSON query support

**Architecture**:
- **Data Filesystem**: OpLogPersistence (TLogFS) at `{pond_path}/data` - user data and computed objects
- **Control Table**: Separate Delta Lake table at `{pond_path}/control` - transaction metadata
- Control table records transaction lifecycle (begin → data_committed → completed)
- Enables recovery from incomplete transactions
- Tracks post-commit factory execution with sequencing

**Control Table Features**:
- Partitioned by `record_category` ("metadata" vs "transaction")
- JSON fields for CLI args, environment, factory modes
- DataFusion JSON functions for querying configuration
- Transaction lifecycle tracking with duration metrics
- Post-commit task coordination and error tracking
- Pond identity preservation (UUID, birth_timestamp, birth_hostname)

**Transaction Types**:
- **Write**: Modifies data filesystem, increments sequence number
- **Read**: Read-only, no sequence increment
- **PostCommit**: Factory execution after data commit

### Data Access & Transformation Layer

#### `provider` - URL-Based Access & Factory Infrastructure
**Purpose**: Unified data access and factory plugin system

**Key Subsystems**:

**URL-Based Access**:
- Format: `scheme://[compression]/path/pattern?options`
- Schemes: `series`, `table`, `csv`, `excelhtml`, `file`
- Compression: `gzip`, `zstd`, `bzip2` (via host component)
- Query params: Format-specific options (delimiter, batch_size, etc.)

**Format Registry**:
- `FormatProvider` trait for pluggable format handlers
- Registered via `linkme` distributed slices
- Built-in: CSV, Parquet, JSON (OpenTelemetry), Excel HTML
- Automatic decompression based on URL host

**Factory Registry**:
- `DynamicFactory` trait for creating dynamic nodes
- Compile-time registration with `linkme::distributed_slice`
- Two creation modes: `create_directory()` and `create_file()`
- Configuration validation at node creation time
- Execution context with mode control (PondReadWriter, ControlWriter, TableTransform)

**TinyFS ObjectStore**:
- Implements `object_store::ObjectStore` for DataFusion ListingTable
- Path format: `/node/{node_id}/part/{part_id}/version/{version}`
- Supports latest, specific, and all versions
- Metadata caching to avoid redundant queries

**Transform Pipeline**:
- Composable table transformations applied to TableProviders
- `column-rename` factory: Rename/drop columns with glob patterns
- `scope_prefix_table_provider`: Prepend scope to column names
- Used by derivative factories (timeseries-join, temporal-reduce)

**TableProvider Creation**:
- `create_table_provider()`: Generic TableProvider from URL pattern
- `create_listing_table_provider()`: Native ListingTable for files
- `create_latest_table_provider()`: Convenience for latest version
- Caching by (node_id, part_id, version) to avoid repeated schema inference

**Key Modules**:
- `registry.rs` - Factory registration and execution
- `factory/` - Built-in factory implementations
- `format_registry.rs` - Format provider registration
- `tinyfs_object_store.rs` - ObjectStore implementation
- `table_creation.rs` - TableProvider creation utilities
- `transform/` - Table transformation factories
- `version_selection.rs` - Version selection strategies

### Application & Domain Layer

#### `cmd` - Command-Line Interface
**Purpose**: Comprehensive `pond` CLI built with clap

**Command Structure**:
```
pond [--pond PATH] [--var key=value] COMMAND [ARGS]
```

**Commands**:
- **Initialization**: `init`, `recover` - pond lifecycle
- **Filesystem**: `mkdir`, `list`, `cat`, `describe`, `copy`
- **Dynamic Nodes**: `mknod` - create factory-based nodes
- **Execution**: `run` - execute factory commands
- **Control**: `control` - query transaction history
- **Temporal**: `temporal` - set/detect time bounds

**Output Formats**: CSV, JSON, Parquet, Arrow IPC, Table (pretty-print)

**Key Features**:
- Template variable expansion with `-v key=value`
- SQL query execution on files with `--sql`
- Time range filtering with `--time-start` and `--time-end`
- Glob pattern support for bulk operations
- Control table querying with DataFusion JSON functions

**Major Commands**:
- `init`: Create new pond or restore from backup/replica config
- `recover`: Restore incomplete transactions after crash
- `show`: Display pond metadata and statistics
- `control`: Transaction history and pond configuration
  - `recent`: Show recent transactions with summary
  - `detail`: Detailed lifecycle for specific transaction
  - `incomplete`: Show failed or incomplete operations
  - `sync`: Retry failed pushes or pull new bundles
  - `show-config`: Display pond identity and factory modes
- `list`: List files with Unix-style ls -l output
- `describe`: Show Arrow schemas for files
- `cat`: Read files with optional SQL filtering
- `copy`: Copy files in/out of pond with format conversion
- `mknod`: Create dynamic nodes from YAML configuration
- `run`: Execute factory commands with argument parsing
- `temporal`: Set or detect temporal bounds for time series

#### `hydrovu` - Water Quality Data Collection
**Purpose**: HydroVu API integration with factory-based execution

**Architecture**:
- OAuth2 client with automatic token refresh
- Device-based configuration with parameter mapping
- Wide-format time series (timestamp as primary key)
- Automatic Arrow schema generation from API metadata

**Factory Configuration**:
```yaml
factory: "hydrovu"
config:
  client_id: "xxx"              # ApiKey<String> - auto-redacted
  client_secret: "yyy"          # ApiSecret<String> - auto-redacted
  hydrovu_path: "/hydrovu"      # Base path for device data
  max_points_per_run: 5000      # Rate limiting
  devices:
    - name: "Boat Dock"
      id: 123456
      scope: "BDock"             # Column name prefix
```

**Data Model**:
- Each device gets a directory: `/hydrovu/devices/{device_id}/`
- Each parameter gets a file: `{scope}.{parameter}.{unit}.series`
- Wide-format: One row per timestamp with all parameters as columns
- Incremental: Tracks last timestamp to avoid duplicates

**Commands**:
- `collect`: Fetch new data from HydroVu API

**Key Modules**:
- `client.rs` - OAuth2 HydroVu API client
- `models.rs` - HydroVuConfig, HydroVuDevice, API response types
- `factory.rs` - Factory registration and command execution

#### `remote` - Backup & Replication System
**Purpose**: Streaming backup to S3-compatible storage with replica support

**Chunked Parquet Architecture**:
- One Delta Lake partition per backup: `bundle_id=<uuid>/`
- ~16MB chunks with BLAKE3 Merkle verification (SeriesOutboard)
- Transaction metadata stored in same partition
- Content-addressed by bundle_id (UUID per backup)

**Object Store Support**:
- AWS S3 (with region and credentials)
- MinIO, Cloudflare R2 (with custom endpoint)
- Local filesystem (file:// URLs)

**Factory Configuration**:
```yaml
factory: "remote"
config:
  url: "s3://backup-bucket/path"
  region: "us-west-2"           # For AWS S3
  access_key: "xxx"
  secret_key: "yyy"
  endpoint: ""                  # For MinIO/R2
```

**Commands**:
- `push`: Upload new bundles (primary pond, post-commit execution)
- `pull`: Download and apply bundles (replica pond)
- `replicate`: Generate base64 replication config for creating replicas
- `list-files`: Show available bundles in remote storage
- `verify`: Check bundle integrity with SHA256 validation

**Factory Modes** (set in control table):
- **push**: Primary pond mode - automatic post-commit backup
- **pull**: Replica pond mode - manual or scheduled sync

**Replication Workflow**:
1. Primary: `pond init /primary` → sets remote factory mode to "push"
2. Primary: Configure remote factory at `/etc/system.d/10-remote`
3. Primary: `pond run /etc/system.d/10-remote replicate` → generates config
4. Replica: `pond init /replica --config BASE64_CONFIG` → creates replica with preserved identity
5. Replica: `pond run /etc/system.d/10-remote pull` → syncs bundles

**Key Modules**:
- `table.rs` - RemoteTable (Delta Lake backup table)
- `writer.rs` - ChunkedWriter for streaming uploads
- `reader.rs` - ChunkedReader for streaming downloads
- `changes.rs` - Detect changed files from Delta log
- `factory.rs` - Factory registration and command execution
- `schema.rs` - Backup table schema and metadata types

#### `utilities` - Shared Helpers
**Purpose**: Common utilities used across crates

**Current Contents**:
- Banner formatting for CLI output
- Shared string manipulation helpers

## Dynamic Factory System

### Overview
Factories create computed filesystem objects that appear as native files and directories. They are registered at compile-time using `linkme` and can be invoked declaratively via YAML configuration or programmatically.

**Registration Pattern**:
```rust
use provider::register_dynamic_factory;

register_dynamic_factory! {
    name: "my-factory",
    creates_directory: false,
    validate_config_fn: validate_my_factory_config,
    create_file_fn: create_my_factory_file,
}
```

### Factory Types

#### 1. **sql-derived-table** & **sql-derived-series** Factories
Transform data using SQL queries over existing files.

**Modes**:
- `table`: Single file operations (errors if pattern matches >1 file)
- `series`: Multi-file and multi-version operations (FileSeries)

**Configuration**:
```yaml
factory: "sql-derived-table"
config:
  patterns:
    source: "table:///raw/measurements.parquet"
  query: "SELECT * FROM source WHERE temperature > 25"
```

**Architecture**:
- Uses DataFusion's native `ListingTable` with TinyFS ObjectStore
- Predicate pushdown to storage layer
- Streaming execution (no full dataset load)
- Optimal partition pruning
- TableProvider caching by (node_id, part_id, version)

**URL Pattern Support**:
- Multiple named patterns: `{"temp": "series:///sensors/temp*.series", "press": "series:///sensors/press*.series"}`
- Each pattern becomes a queryable table in SQL
- Automatic UNION ALL BY NAME for files matching same pattern
- Transform pipelines via `transforms` field

#### 2. **temporal-reduce** Factory
Creates time-bucketed aggregations with multiple resolution levels.

**Configuration**:
```yaml
factory: "temporal-reduce"
config:
  in_pattern: "series:///raw/*"
  out_pattern: "$0"              # Basename from captured group
  time_column: "timestamp"
  resolutions: [1h, 6h, 1d]      # Parsed with humantime
  aggregations:
    - type: "avg"
      columns: ["temperature", "pressure"]
    - type: "min"                # columns optional = all numeric
    - type: "max"
  transforms: ["/etc/hydro_rename"]  # Optional pre-processing
```

**Output Structure**:
```
/downsampled/
  res=1h.series   # 1-hour buckets (GROUP BY time_bucket('1 hour', timestamp))
  res=6h.series   # 6-hour buckets
  res=1d.series   # 1-day buckets
```

**SQL Generation**:
- Uses `date_bin()` (DataFusion) or `time_bucket()` (DuckDB) for bucketing
- One SQL query per resolution
- Aggregation types: AVG, MIN, MAX, COUNT, SUM
- Column glob pattern expansion from discovered schema

**Implementation**:
- `TemporalReduceDirectory`: Dynamic directory with resolution-based files
- `TemporalReduceSqlFile`: Lazy SQL generation to avoid schema cascade
- Discovered columns cached to prevent O(N) cascade during export

#### 3. **timeseries-join** Factory
Joins multiple time series by timestamp with full outer join semantics.

**Configuration**:
```yaml
factory: "timeseries-join"
config:
  time_column: "timestamp"
  inputs:
    - pattern: "series:///sensors/temperature/*"
      scope: "temp"                    # Column prefix
      transforms: ["/etc/hydro_rename"]  # Per-input transforms
      range:
        begin: "2024-01-01T00:00:00Z"  # ISO 8601
        end: "2024-12-31T23:59:59Z"
    - pattern: "series:///sensors/pressure/*"
      scope: "press"
```

**Features**:
- COALESCE timestamp from all inputs (handles NULLs)
- Optional per-input time range filtering with WHERE clauses
- Scope prefixing for column disambiguation
- Full outer join with `ON TRUE` and `EXCLUDE` join condition for cartesian product avoidance
- Transform pipelines applied before scope prefixing

**SQL Generation**:
```sql
WITH input0 AS (
  SELECT * FROM series_input0
  WHERE timestamp >= '2024-01-01T00:00:00Z'
),
input1 AS (
  SELECT * FROM series_input1
)
SELECT 
  COALESCE(input0.timestamp, input1.timestamp) AS timestamp,
  input0.* EXCLUDE (timestamp),
  input1.* EXCLUDE (timestamp)
FROM input0
FULL OUTER JOIN input1 ON input0.timestamp = input1.timestamp
```

**Validation**:
- Requires at least 2 inputs
- Validates ISO 8601 timestamp formats
- Checks URL schemes (series, csv, excelhtml, file)

#### 4. **timeseries-pivot** Factory
Pivots long-format timeseries data to wide format.

**Configuration**:
```yaml
factory: "timeseries-pivot"
config:
  in_pattern: "series:///long_format/*"
  time_column: "timestamp"
  pivot_column: "parameter_name"  # Becomes column names
  value_column: "value"            # Becomes cell values
```

**SQL Generation**:
- Uses DataFusion's PIVOT syntax
- Discovers unique pivot values from source data
- Creates one column per pivot value

#### 5. **column-rename** Factory
Transform factory for renaming or dropping columns.

**Configuration**:
```yaml
factory: "column-rename"
config:
  renames:
    "old_name": "new_name"
    "Temp.*": "Temperature_$1"  # Glob pattern with capture
  drops: ["unwanted_column"]
```

**Usage**:
- Applied via `transforms` field in other factories
- Returns transformed TableProvider

#### 6. **template** Factory
Renders Tera templates with discovered file patterns.

**Configuration**:
```yaml
factory: "template"
config:
  in_pattern: "series:///data/*"
  out_pattern: "$0.html"
  template_file: "/templates/report.md.tmpl"
```

**Template Variables**:
- Captured groups from pattern: `$0`, `$1`, etc.
- CLI variables: `-v key=value`
- File metadata: size, timestamp, schema

#### 7. **dynamic-dir** Factory
Composes multiple factories into a single directory.

**Configuration**:
```yaml
factory: "dynamic-dir"
config:
  entries:
    - name: "raw"
      factory: "sql-derived-series"
      config:
        patterns:
          source: "series:///input/*"
        query: "SELECT * FROM source"
    - name: "aggregated"
      factory: "temporal-reduce"
      config:
        in_pattern: "series:///../raw"  # Relative reference
        time_column: "timestamp"
        resolutions: [1h, 1d]
        aggregations:
          - type: "avg"
```

**Features**:
- Hierarchical composition of factories
- Entry caching to avoid recreating nodes
- Deterministic FileID generation from config hash
- Supports both directory and file factories

#### 8. **remote** Factory (Executable)
Manages backup and replication to S3-compatible object stores.

**Commands**:
- `push` - Upload new transaction bundles (primary pond mode)
- `pull` - Download and apply bundles (replica pond mode)
- `replicate` - Generate replication configuration
- `list-files` - Show available backups
- `verify` - Check backup integrity

**Configuration**:
```yaml
factory: "remote"
config:
  url: "s3://backup-bucket"
  region: "us-west-2"
  access_key: "xxx"
  secret_key: "yyy"
  endpoint: ""                # For MinIO/R2
```

**Architecture**:
- Chunked parquet format (~16MB chunks) with BLAKE3 Merkle verification
- Direct upload to Delta Lake (local or object_store)
- SeriesOutboard for streaming validation
- Pond identity preservation for replicas
- Factory mode determines execution (push vs pull)

**Chunked Parquet Schema**:
```
bundle_id=<uuid>/part-00000.parquet
  ├── chunk_id: Int64           # 0, 1, 2, ... ordering
  ├── chunk_hash: String        # Cumulative BLAKE3 bao-root
  ├── chunk_outboard: Binary    # SeriesOutboard for verification
  ├── chunk_data: Binary        # ~16MB chunk
  └── root_hash: String         # Final file BLAKE3
```

#### 9. **hydrovu** Factory (Executable)
HydroVu data collection factory.

**Commands**:
- `collect` - Fetch sensor data from HydroVu API

**Configuration**:
```yaml
factory: "hydrovu"
config:
  client_id: "xxx"              # ApiKey<String>
  client_secret: "yyy"          # ApiSecret<String>
  hydrovu_path: "/hydrovu"
  max_points_per_run: 5000
  devices:
    - name: "Boat Dock"
      id: 123456
      scope: "BDock"
```

**Data Layout**:
```
/hydrovu/devices/
  ├── 123456/
  │   ├── BDock.Temperature.C.series
  │   ├── BDock.Pressure.psi.series
  │   └── BDock.BatteryVoltage.V.series
  └── 789012/
      └── ...
```

### Factory Execution Model

**Execution Modes**:
- `PondReadWriter`: Can read and write pond data (most factories)
- `ControlWriter`: Can write to control table (remote push/pull)
- `TableTransform`: Returns TableProvider without creating node (column-rename)

**Execution Flow**:
1. Parse factory config from YAML
2. Validate config with factory-specific validator
3. Create node (directory or file) with FactoryContext
4. For executables, parse command-line args with clap
5. Execute command with appropriate transaction type
6. For post-commit factories, Steward sequences execution

**Post-Commit Execution**:
- Triggered after successful data commit
- Recorded in control table with execution sequence
- Errors don't roll back data commit (defensive)
- Used by remote factory in "push" mode for automatic backup

## Transaction Architecture

### Single Transaction Rule (CRITICAL)

**The system enforces ONE active transaction per operation.** This is validated with a runtime panic guard in `TinyFsTransactionState`.

```rust
// ✅ CORRECT Pattern
async fn operation(pond_path: &Path) -> Result<()> {
    let mut ship = Ship::open_pond(pond_path).await?;
    let meta = PondUserMetadata::new(vec!["operation".into()]);
    let mut tx_guard = ship.begin_transaction(&meta, TransactionType::Write).await?;
    
    // All work uses this single transaction
    let root = tx_guard.root();
    let ctx = tx_guard.session_context().await?;
    
    // Helper functions receive transaction guard
    helper_function(&mut tx_guard).await?;
    
    tx_guard.commit(None).await?;
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
async fn broken_operation(pond_path: &Path) -> Result<()> {
    let mut ship = Ship::open_pond(pond_path).await?;
    let meta = PondUserMetadata::new(vec!["op".into()]);
    let tx1 = ship.begin_transaction(&meta, TransactionType::Write).await?;
    
    // This panics - TinyFsTransactionState detects duplicate transaction!
    broken_helper(pond_path).await?;
    
    Ok(())
}

async fn broken_helper(pond_path: &Path) -> Result<()> {
    let mut ship = Ship::open_pond(pond_path).await?;  // Shares TransactionState
    let meta = PondUserMetadata::new(vec!["helper".into()]);
    // PANIC! TinyFsTransactionState sees active transaction
    let tx2 = ship.begin_transaction(&meta, TransactionType::Write).await?;
    Ok(())
}
```

**Why This Matters**:
- Prevents nested transactions that could corrupt state
- Ensures consistent DataFusion SessionContext
- Avoids deadlocks from multiple table locks
- Makes transaction boundaries explicit and auditable

### Transaction Lifecycle

**Steward Transaction Flow**:

1. **Begin Phase**:
   ```rust
   let mut tx = ship.begin_transaction(&metadata, TransactionType::Write).await?;
   ```
   - Control table records "begin" event with CLI args, environment
   - TLogFS creates State with fresh SessionContext
   - TransactionState marked as active (panic guard activated)

2. **Operation Phase**:
   ```rust
   let root = tx.root();
   root.create_dir_path("/new/directory").await?;
   let file = root.create_file("/data.parquet").await?;
   // ... perform filesystem operations ...
   ```
   - All operations use single State instance
   - Directory cache tracks modifications
   - Records buffered in memory (not yet persisted)

3. **Commit Phase**:
   ```rust
   tx.commit(None).await?;
   ```
   - Data filesystem commit (writes OplogEntry to Delta Lake)
   - Control table records "data_committed" with Delta version
   - Post-commit factories execute if configured
   - Control table records "completed" event
   - Transaction sequence increments
   - TransactionState marked as inactive

4. **Post-Commit Phase** (if configured):
   ```rust
   // Steward automatically executes post-commit factories
   // Each in separate transaction with its own begin/commit cycle
   ```
   - Triggered by factory modes in control table
   - Executed in sequence with `execution_seq` ordering
   - Errors don't roll back data commit (defensive)
   - Control table tracks post-commit lifecycle

**Transaction Types**:

- **Write**: Modifies data, creates new Delta Lake version
  ```rust
  let tx = ship.begin_transaction(&meta, TransactionType::Write).await?;
  ```
  - Increments `txn_seq` and `data_fs_version`
  - Control table records transaction lifecycle
  - Can trigger post-commit factories

- **Read**: No modifications, no version increment
  ```rust
  let tx = ship.begin_transaction(&meta, TransactionType::Read).await?;
  ```
  - No sequence increment
  - Validates no writes occur (runtime check)
  - Shorter lifecycle in control table

- **PostCommit**: Factory execution after data commit
  ```rust
  // Created internally by Steward
  let tx = ship.begin_post_commit_transaction(&parent_meta, factory_name).await?;
  ```
  - Has `parent_txn_seq` linking to triggering transaction
  - Tracked with `execution_seq` for ordering
  - Errors logged but don't block parent transaction

### State Management

The `State` object coordinates all operations within a transaction:

```rust
#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<InnerState>>,
    object_store: Arc<OnceCell<TinyFsObjectStore>>,
    session_context: Arc<SessionContext>,  // Outside lock to avoid deadlocks
    template_variables: Arc<Mutex<HashMap<String, Value>>>,
    table_provider_cache: Arc<Mutex<HashMap<TableProviderKey, Arc<dyn TableProvider>>>>,
    txn_state: Arc<TinyFsTransactionState>,  // Shared panic guard
}

pub struct InnerState {
    path: PathBuf,
    table: DeltaTable,
    records: Vec<OplogEntry>,  // Buffered operations
    directories: HashMap<FileID, DirectoryState>,  // Directory cache
    session_context: Arc<SessionContext>,
    txn_seq: i64,
}
```

**Key Points**:
- Single State per transaction
- Shared DataFusion SessionContext across all operations
- TableProvider caching by (node_id, part_id, version) to avoid repeated schema inference
- Template variable storage for CLI variable expansion
- Directory cache for O(1) lookups and duplicate detection

**Directory State**:
```rust
pub struct DirectoryState {
    pub modified: bool,  // Track if directory changed
    pub mapping: HashMap<String, DirectoryEntry>,  // O(1) lookups
}
```
- Lazy-loaded on first access
- Tracks modifications for efficient commit
- Duplicate name detection
- Supports dynamic and physical directories

## DataFusion Integration

### Query Execution Model

**Core Principle**: All files in DuckPond can be queried with SQL through DataFusion's `TableProvider` trait.

#### TinyFS ObjectStore

Custom `ObjectStore` implementation enables DataFusion to read TLogFS files natively:

```rust
// URL Pattern: /node/{node_id}/part/{part_id}/version/{version}
// Example: /node/abc123/part/def456/version/latest

let url = "tinyfs:///node/abc123/part/def456/version/latest";
let object_store = TinyFsObjectStore::new(persistence);

// DataFusion ListingTable can read directly from pond
let config = ListingTableConfig::new(...)
    .with_listing_options(ListingOptions::new(format));
let listing_table = ListingTable::try_new(config)?;
let df = ctx.sql("SELECT * FROM listing_table WHERE temp > 25").await?;
```

**Path Structure**:
- `/node/{node_id}/part/{part_id}/version/{version}` - Specific version
- `/node/{node_id}/part/{part_id}/version/latest` - Latest version
- `/node/{node_id}/part/{part_id}/version/all` - All versions (UNION ALL)

**Features**:
- Version selection (latest, all, specific)
- Partition pruning by node_id and part_id
- Streaming reads (no full file load)
- Metadata caching for repeated access
- Automatic file discovery via `list()`

**Registration**:
```rust
// Register with DataFusion URL handlers
register_tinyfs_object_store(session_context, persistence);

// Now DataFusion can resolve tinyfs:// URLs
let df = ctx.read_table(ListingTable::new(...)).await?;
```

### NodeID/PartID Relationships

**Critical for Delta Lake partition pruning**:

```
DeltaLake partition structure:
/data/part_id={parent_dir_id}/node_id={file_id}/version={v}/

Physical File:     part_id = parent_directory_node_id
Physical Directory: part_id = node_id (self-partitioned)
Dynamic File:      part_id = parent_directory_node_id (even if parent is root)
```

**Why this matters**: Correct part_id enables efficient partition filtering in Delta Lake queries.

```rust
// ✅ CORRECT - Use parent directory as part_id for files
let parent_path = file_path.parent().unwrap_or("/");
let parent_node = root.resolve_path(&parent_path).await?;
let part_id = parent_node.id().await.part_id();

// ❌ WRONG - Using file's node_id as part_id breaks pruning
let part_id = file_node_id.part_id(); // Causes full table scan!
```

**FileID Structure**:
```rust
pub struct FileID {
    node_id: NodeID,  // Unique identifier for this node
    part_id: PartID,  // Partition (parent directory for files, self for dirs)
}
```

### TableProvider Creation & Caching

**Unified Creation Interface**:
```rust
// From URL pattern with automatic format detection
let provider = create_table_provider(
    url_pattern,
    state,
    version_selection,
    temporal_filter,
).await?;

// Specifically for ListingTable (file-based)
let provider = create_listing_table_provider(
    matched_files,
    state,
    version_selection,
).await?;

// Convenience for latest version
let provider = create_latest_table_provider(
    file_node,
    state,
).await?;
```

**Cache Key**:
```rust
pub struct TableProviderKey {
    node_id: NodeID,
    part_id: PartID,
    version: VersionSelection,  // Latest, All, or Specific(u64)
}
```

**Caching Benefits**:
- Avoid repeated schema inference (expensive for large files)
- Reuse ListingTable instances across queries
- Prevent redundant file metadata queries
- Share TableProviders between operations

**Cache Invalidation**:
- Per-transaction cache (cleared on commit)
- Not shared across transactions
- Versioned cache ensures correctness

### Query Patterns

#### Direct File Queries

```rust
// Via pond cat command
pond cat /sensors/temperature.csv \
  --sql "SELECT timestamp, temperature FROM source WHERE temperature > 25"

// Internally uses:
let provider = execute_sql_on_file(&file_path, &sql, state).await?;
let batches = provider.scan(...).await?;
```

#### Factory SQL Transformations

```rust
// sql-derived-table factory
let config = SqlDerivedConfig {
    patterns: vec![("source".into(), "table:///raw/data.parquet".parse()?)],
    query: Some("SELECT * FROM source WHERE value > 100".into()),
    ..Default::default()
};

// Creates SqlDerivedFile that wraps TableProvider
let file = SqlDerivedFile::new(config, context).await?;

// Reading the file executes the SQL
let provider = file.as_table_provider(...).await?;
let batches = provider.scan(...).await?;
```

#### Temporal Filtering

```rust
// Applied at query execution time
let filter = TemporalFilter {
    time_column: "timestamp".into(),
    min_time: Some(1704067200000000), // Microseconds since epoch
    max_time: Some(1735689599999999),
};

// Wrapped around base TableProvider
let filtered = apply_temporal_filter(base_provider, filter)?;
```

### Streaming vs In-Memory

**ListingTable (Streaming)**:
- For file-based data (Parquet, CSV, etc.)
- Streams from ObjectStore without loading full file
- Supports predicate pushdown
- Optimal for large datasets
- Used by sql-derived-table/series factories

**MemTable (In-Memory)**:
- For small, already-loaded data
- RecordBatch fully in memory
- Fast for small datasets
- Used for DirectBytes entry type
- No predicate pushdown

**Decision Flow**:
```rust
match entry_type {
    EntryType::FileParquet | EntryType::FileCsv => {
        // Use ListingTable for streaming
        create_listing_table_provider(files, state, version).await?
    }
    EntryType::DirectBytes => {
        // Use MemTable for in-memory data
        let batch = read_bytes_as_record_batch(bytes)?;
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?)
    }
    _ => return Err(Error::UnsupportedFormat),
}
```

### SQL Transform Pipeline

**Transform Application Order**:
1. Base TableProvider (file or listing table)
2. Factory transforms (via `transforms` field)
3. Scope prefix (for timeseries-join)
4. SQL query execution
5. Optional post-query transforms

**Example**:
```yaml
factory: "timeseries-join"
config:
  time_column: "timestamp"
  inputs:
    - pattern: "series:///raw/*"
      scope: "sensor1"
      transforms: ["/etc/hydro_rename"]  # Step 2: Column rename
  # Step 3: Scope prefix (sensor1.temperature)
  # Step 4: FULL OUTER JOIN SQL
```

**Transform Factory**:
```rust
// Column rename transform
let renamed_provider = create_transform(
    "/etc/hydro_rename",
    base_provider,
    context,
).await?;

// Returns new TableProvider with transformed schema
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

Every pond has immutable identity metadata stored in the control table:

```rust
pub struct PondMetadata {
    pub pond_id: Uuid,              // UUID v7 (time-ordered)
    pub birth_timestamp: i64,       // Creation time (microseconds since epoch)
    pub birth_hostname: String,     // Where created
    pub birth_username: String,     // Who created it
}
```

**Properties**:
- Created once during `pond init`
- Stored in control table metadata partition
- Preserved across replicas (replicas share source pond identity)
- Queryable via `pond control show-config`

**Purpose**:
- Track pond lineage and origin
- Identify replicas vs primaries
- Audit trail for data provenance
- Recovery validation

### Factory Modes

Factory execution behavior configured in control table:

```rust
// Query factory modes
SELECT json_get_str(factory_modes, 'remote') as remote_mode
FROM control_table
WHERE record_category = 'metadata'
ORDER BY timestamp DESC
LIMIT 1;
```

**Modes**:
- `remote` factory:
  - `push`: Primary pond mode (automatic post-commit backup)
  - `pull`: Replica pond mode (manual sync)
- Extensible for other factories

**Setting Modes**:
```bash
pond control set-config factory_modes.remote push
```

### Replication Workflow

**1. Primary Pond Setup**:
```bash
# Initialize pond
pond init /data/primary

# Configure remote backup
cat > remote.yaml << EOF
factory: "remote"
config:
  url: "s3://my-bucket/primary-backup"
  region: "us-west-2"
  access_key: "xxx"
  secret_key: "yyy"
EOF

pond mknod --config remote.yaml /etc/system.d/10-remote

# Factory mode automatically set to "push" during pond init
# Each write transaction triggers post-commit backup
```

**2. Generate Replication Config**:
```bash
# Create base64-encoded replication config
pond run /etc/system.d/10-remote replicate > replica-config.txt

# Config includes:
# - Remote storage location
# - Pond identity (UUID, birth timestamp, hostname, username)
# - Region and credentials
```

**3. Create Replica**:
```bash
# Initialize replica with preserved identity
pond init /data/replica --config $(cat replica-config.txt)

# This creates:
# - Empty data filesystem structure
# - Control table with source pond's identity
# - Remote factory configured in "pull" mode
# - No transaction #1 (will come from first bundle)

# Pull existing bundles
cd /data/replica
pond run /etc/system.d/10-remote pull

# Repeat to sync new changes
pond run /etc/system.d/10-remote pull
```

**4. Replica Operation**:
```bash
# Check for new bundles
pond run /etc/system.d/10-remote pull

# View replication status
pond control recent --limit 20

# Verify bundle integrity
pond run /etc/system.d/10-remote verify
```

### Bundle Format

**Structure** (Chunked Parquet in Delta Lake):
```
bundle_id=550e8400-e29b-41d4-a716-446655440000/
  └── part-00000.parquet       # All chunks for this backup
      ├── chunk_id: 0, 1, 2...   # Ordering within file
      ├── chunk_hash: String     # Cumulative BLAKE3 bao-root
      ├── chunk_outboard: Binary # SeriesOutboard for verification
      ├── chunk_data: Binary     # ~16MB chunks
      ├── original_path: String  # Source path in pond
      └── root_hash: String      # Final file BLAKE3
```

**Transaction Metadata** (stored as file_type="metadata"):
```json
{
  \"pond_txn_id\": 5,
  \"files\": [
    {
      \"path\": \"part_id=abc123/node_id=def456/version=1/file.parquet\",
      \"root_hash\": \"abc123...\",
      \"size\": 1024000
    }
  ]
}
```

**Properties**:
- Chunked parquet format (~16MB chunks) with BLAKE3 Merkle verification
- One Delta Lake partition per transaction (incremental)
- Partitioned by bundle_id (UUID per backup)
- Self-contained with all transaction data
- Includes pond identity for verification

**Restoration Process**:
1. Download bundle from object store
2. Validate metadata (pond_id matches, txn_seq is sequential)
3. Extract parquet files to temporary directory
4. Replay transaction to data filesystem
5. Record transaction in control table
6. Increment last_write_seq

### Recovery Scenarios

**Primary Pond Crash**:
```bash
# On primary (after crash/restart)
pond recover /data/primary

# Checks control table for incomplete transactions
# Rolls back any uncommitted changes
# Restores consistent state
```

**Replica Restoration**:
```bash
# Create new replica from scratch
pond init /data/new-replica --config $(cat replica-config.txt)

# Pull all bundles (automatic sequencing)
pond run /etc/system.d/10-remote pull

# Replica now matches primary up to latest synced bundle
```

**Partial Backup Loss**:
```bash
# Verify backup integrity
pond run /etc/system.d/10-remote verify

# If bundles missing, shows gap in sequence
# Manual intervention required (re-push from primary if still available)
```

## Development Guidelines

### Critical Rules

1. **One transaction per operation** - Never create multiple concurrent transaction guards
   - Pass `&mut StewardTransactionGuard` to helper functions
   - Don't pass pond_path or persistence to helpers
   - Runtime panic enforced by `TinyFsTransactionState`

2. **Fail fast** - No silent fallbacks or default values on system errors
   - Use `.map_err()` to propagate errors with context
   - Distinguish business cases (None) from system failures (Err)
   - Only use fallbacks for legitimate business logic

3. **Correct part_id** - Files use parent directory node_id for partition pruning
   - Resolve parent path to get part_id
   - Don't use file's own node_id
   - Critical for Delta Lake query performance

4. **Factory composability** - New factories should integrate with dynamic-dir
   - Implement proper `validate_config` function
   - Support both directory and file creation modes
   - Document configuration schema

5. **Test with transactions** - All tests should use proper transaction lifecycle
   - Use `Ship::create_pond()` for test setup
   - Begin transactions with proper metadata
   - Commit or rollback explicitly

6. **Read docs before suggesting code** - See `.github/copilot-instructions.md`
   - Check trigger words for required documentation
   - Follow single transaction rule
   - Apply fail-fast philosophy
   - Use file redirection for debugging large output

### Common Anti-Patterns

```rust
// ❌ WRONG: Multiple transactions
let tx1 = persistence.begin().await?;
let tx2 = persistence.begin().await?; // PANIC!

// ❌ WRONG: Helper creates own transaction
async fn helper(pond_path: &Path) -> Result<()> {
    let mut ship = Ship::open_pond(pond_path).await?;
    let tx = ship.begin_transaction(...).await?; // Duplicate!
}

// ❌ WRONG: Silent fallback
let versions = root.list_file_versions(&path).await
    .unwrap_or_default(); // Masks errors!

// ❌ WRONG: Wrong part_id
let part_id = file_id.part_id(); // Should be parent_node_id!
```

### Correct Patterns

```rust
// ✅ Single transaction
let mut tx = ship.begin_transaction(&meta, TransactionType::Write).await?;
helper(&mut tx).await?;
tx.commit(None).await?;

// ✅ Pass transaction guard
async fn helper(tx: &mut StewardTransactionGuard<'_>) -> Result<()> {
    let ctx = tx.session_context().await?;
    // Use passed context
}

// ✅ Fail fast with context
let versions = root
    .list_file_versions(&path)
    .await
    .map_err(|e| StewardError::Dyn(
        format!("Cannot list versions for {}: {}", path, e).into()
    ))?;

// ✅ Correct part_id
let parent_path = file_path.parent().unwrap_or("/");
let parent_node = root.resolve_path(&parent_path).await?;
let part_id = parent_node.id().await.part_id();

// ✅ Business case vs error
match max_timestamp {
    None => {
        log::info!("No existing data, starting fresh");
        0  // Valid business case
    }
    Some(ts) => ts,
}
```

### Testing Patterns

**Transaction-Based Tests**:
```rust
#[tokio::test]
async fn test_operation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pond_path = temp_dir.path();
    
    // Initialize pond
    let mut ship = Ship::create_pond(pond_path).await.unwrap();
    
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
    
    // Verify
    let mut tx = ship.begin_transaction(&meta, TransactionType::Read)
        .await
        .unwrap();
    let root = tx.root();
    assert!(root.lookup("/test").await.is_ok());
}
```

**Factory Testing**:
```rust
#[tokio::test]
async fn test_sql_derived_factory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut ship = Ship::create_pond(temp_dir.path()).await.unwrap();
    
    // Write source data
    let meta = PondUserMetadata::new(vec!["test".into()]);
    let mut tx = ship.begin_transaction(&meta, TransactionType::Write)
        .await.unwrap();
    let root = tx.root();
    
    // Create test parquet file
    let file = root.create_file("/source.parquet").await.unwrap();
    let batch = create_test_record_batch();
    write_parquet_to_file(&file, &batch).await.unwrap();
    
    tx.commit(None).await.unwrap();
    
    // Create SQL-derived node
    let config = SqlDerivedConfig {
        patterns: HashMap::from([(
            "source".into(),
            "table:///source.parquet".parse().unwrap()
        )]),
        query: Some("SELECT * FROM source WHERE value > 10".into()),
        ..Default::default()
    };
    
    let config_bytes = serde_json::to_vec(&config).unwrap();
    
    let mut tx = ship.begin_transaction(&meta, TransactionType::Write)
        .await.unwrap();
    let root = tx.root();
    
    // Create dynamic file
    root.create_dynamic_file("/derived.series", "sql-derived-table", &config_bytes)
        .await.unwrap();
    
    tx.commit(None).await.unwrap();
    
    // Query the derived file
    let mut tx = ship.begin_transaction(&meta, TransactionType::Read)
        .await.unwrap();
    let root = tx.root();
    let file = root.lookup("/derived.series").await.unwrap().as_file().unwrap();
    
    let provider = file.as_table_provider(...).await.unwrap();
    let batches = collect_batches(provider).await.unwrap();
    
    assert_eq!(batches.len(), 1);
    // Verify filtered data...
}
```

### Debugging Tips

**Large Output Debugging**:
```bash
# ❌ WRONG: Output truncated by grep/tail
cargo test 2>&1 | grep ERROR

# ✅ CORRECT: Full output to file, then analyze
cargo test 1> OUT 2> OUT
grep ERROR OUT
# Or use pond's grep_search tool on OUT file
```

**Transaction Tracing**:
```bash
# Enable detailed logging
RUST_LOG=tlogfs=debug,steward=debug pond list /**/*

# Query control table for transaction history
pond control recent --limit 50

# Show specific transaction details
pond control detail --txn-seq 42
```

**State Inspection**:
```rust
// In code, inspect State contents
let state = tx.state()?;
let df_ctx = state.datafusion_context();

// Check table provider cache
let cache_key = TableProviderKey { node_id, part_id, version };
let cached = state.get_cached_table_provider(&cache_key).await;

// Inspect directory cache
let dir_state = state.get_directory_state(file_id).await?;
println!("Modified: {}, Entries: {}", dir_state.modified, dir_state.mapping.len());
```

## Performance Considerations

### TableProvider Caching

State maintains a cache of TableProvider instances to avoid:
- Repeated schema inference (expensive for large files)
- Duplicate ListingTable creation
- Redundant file metadata queries

**Cache Key**: `(node_id, part_id, version_selection)`

**Cache Lifetime**: Per-transaction (cleared on commit)

### Streaming vs In-Memory

**Prefer streaming for large datasets**:
- `ListingTable` for file-based sources (streams from storage)
- `MemTable` only for small in-memory data
- Predicate pushdown reduces data scanned

**Performance Comparison**:
```
ListingTable (1GB parquet):
  - Memory: ~10MB (streaming)
  - Predicate pushdown: Yes
  - Startup: Fast (lazy)

MemTable (1GB in-memory):
  - Memory: ~1GB (full load)
  - Predicate pushdown: No
  - Startup: Slow (full load)
```

### Partition Pruning

**Critical for performance**:
- Use correct part_id (parent directory for files)
- Delta Lake partitions by `part_id` and `node_id`
- Queries with node_id/part_id filters use partition pruning

```sql
-- ✅ Efficient - uses partition pruning
SELECT * FROM delta_table
WHERE part_id = 'abc123' AND node_id = 'def456';

-- ❌ Slow - full table scan
SELECT * FROM delta_table
WHERE timestamp > '2024-01-01';
```

### Directory Caching

**Optimizations**:
- Lazy loading on first access
- O(1) lookups via HashMap
- Modification tracking to avoid unnecessary writes
- Duplicate name detection without full scan

**Cache Hit Rates**:
- First access: Directory loaded from Delta Lake
- Subsequent accesses: O(1) HashMap lookup
- Directory modifications: In-memory only until commit

### Large File Handling

**Content-Addressed Storage**:
- Files >10MB stored separately with SHA256 hash
- Deduplication across identical files
- OplogEntry references SHA256, not content
- Reduces Delta Lake table size

**Streaming Writes**:
```rust
// ✅ Streaming write (low memory)
let mut writer = file.writer().await?;
loop {
    let chunk = read_chunk().await?;
    writer.write_all(&chunk).await?;
}
writer.commit().await?;

// ❌ Full load (high memory for large files)
let data = read_entire_file().await?;
file.write_all(&data).await?;
```

### Query Optimization

**Predicate Pushdown**:
- Filters pushed to Parquet readers
- Row group skipping based on statistics
- Column projection reduces I/O

**Temporal Filtering**:
```rust
// Applied at TableProvider level
let filter = TemporalFilter {
    time_column: "timestamp",
    min_time: Some(start_micros),
    max_time: Some(end_micros),
};

// Pushdown to Parquet row groups
// Skip row groups outside time range
```

**Schema Inference Caching**:
- Schema discovered once per (node_id, part_id, version)
- Cached in State for transaction lifetime
- Avoids repeated Parquet metadata reads

## Glossary

- **Pond**: A DuckPond filesystem instance (directory with `data/` and `control/` subdirs)
- **NodeID**: Unique identifier for filesystem objects (UUID-based)
- **PartID**: Partition identifier - parent directory's node_id for files, self for directories
- **FileID**: Combination of NodeID and PartID uniquely identifying a node
- **Transaction Guard**: RAII wrapper ensuring ACID transaction semantics (`StewardTransactionGuard`)
- **State**: Per-transaction coordinator for DataFusion context, caches, and directory state
- **Factory**: Plugin that creates dynamic filesystem objects (SQL views, aggregations, executables)
- **OpLog**: Operation log stored in Delta Lake (all filesystem operations as Arrow records)
- **Control Table**: Transaction audit log in separate Delta Lake table (lifecycle, metadata, errors)
- **Bundle**: Chunked parquet backup of pond data for single transaction (stored in Delta Lake)
- **Temporal Bounds**: Explicit time range metadata for files (min_event_time, max_event_time)
- **EntryType**: Classification of filesystem entries (FileParquet, FileCsv, DirectoryDynamic, etc.)
- **FileSeries**: Versioned time-series file with multiple versions
- **FileTable**: Single-file entry (non-versioned or latest version only)
- **ListingTable**: DataFusion's streaming table provider for file-based data
- **MemTable**: DataFusion's in-memory table provider
- **TinyFS ObjectStore**: Custom ObjectStore implementation bridging TinyFS and DataFusion
- **Scope Prefix**: Column name prefix for disambiguating joined tables (e.g., "sensor1.temperature")
- **Transform Pipeline**: Composable TableProvider transformations applied before query execution
- **Post-Commit**: Factory execution phase after successful data commit (for backup, alerts, etc.)
- **Data Taxonomy**: Type-safe wrappers for sensitive configuration (ApiKey, ApiSecret, ServiceEndpoint)

## Architecture Decisions

### Why Delta Lake?

- **ACID transactions**: Automatic rollback, atomic operations
- **Time travel**: Built-in versioning with transaction log
- **Parquet format**: Efficient columnar storage with compression
- **Wide ecosystem**: Compatible with Spark, pandas, polars
- **Statistics**: Min/max values enable partition pruning
- **Standard**: Open protocol with multiple implementations

### Why DataFusion?

- **Native Rust**: Excellent async support, type safety, performance
- **Extensible**: ObjectStore trait enables TinyFS integration
- **SQL dialect**: Familiar syntax for users
- **Streaming execution**: Process larger-than-memory datasets
- **Arrow-native**: Zero-copy data transfer
- **Active development**: Regular releases with new features

### Why Separate Control Table?

- **Concerns separation**: User data vs transaction audit
- **Always available**: Control table accessible even if data corrupted
- **Post-commit tracking**: Factory execution tracked independently
- **Recovery simplified**: Transaction lifecycle visible
- **Queryable metadata**: SQL access to transaction history
- **Performance**: Smaller table for metadata queries

### Why Factory System?

- **Declarative configuration**: YAML over imperative code
- **Composability**: dynamic-dir enables hierarchical transformations
- **SQL-native**: Data transformations use familiar SQL syntax
- **Accessibility**: Non-programmers can create pipelines
- **Extensibility**: New factories via compile-time registration
- **Type safety**: Rust validation at node creation

### Why Streaming Bundles?

- **Memory efficient**: No temp files for large transactions
- **Incremental backup**: One bundle per transaction
- **Self-contained**: Each bundle has metadata and files
- **Content-addressed**: Deduplication via SHA256
- **Resumable**: Can retry individual bundle uploads
- **Verifiable**: SHA256 checks ensure integrity

### Why Single Transaction Rule?

- **Consistency**: Single SessionContext avoids conflicting state
- **Deadlock prevention**: One active transaction = no lock contention
- **Audit clarity**: One transaction = one control table entry
- **Recovery simplified**: No partial commits from nested transactions
- **Performance**: Caches shared within transaction
- **Debugging**: Clear transaction boundaries

## Known Limitations & Future Work

### Current Limitations

1. **No distributed execution**: Single-node DataFusion only
   - Multi-GB datasets work fine with streaming
   - Multi-TB would need distributed compute

2. **Limited predicate pushdown**: Temporal filters applied after scan
   - Parquet row group filtering works
   - Could optimize with statistics-based skipping

3. **No incremental aggregations**: Temporal reduce recalculates all buckets
   - Each query rescans source data
   - Could track "dirty" buckets and recompute only those

4. **Bundle-based replication**: Not optimized for high-frequency updates
   - Good for hourly/daily sync
   - Real-time sync would need different approach

5. **Schema evolution**: Limited support for schema changes
   - Adding columns works
   - Removing/renaming columns requires migration

6. **No concurrent transactions**: Single writer only
   - Fine for single-application use
   - Multi-writer would need distributed coordination

### Planned Enhancements

1. **Incremental temporal reduce**: Only recalculate affected buckets
   - Track last update time per bucket
   - Compare with source data timestamps
   - Selective recomputation

2. **Better temporal filtering**: Push time bounds to Parquet row groups
   - Store min/max timestamps in row group metadata
   - Skip row groups outside query range
   - Significant speedup for time-range queries

3. **Streaming replication**: Continuous sync instead of pull-based
   - Watch for new bundles
   - Automatic apply on detection
   - Near real-time replica updates

4. **Advanced factories**: More SQL operations, ML transforms
   - Window functions for moving averages
   - Statistical aggregations (percentiles, stddev)
   - ML model inference as factory

5. **Query optimization**: Better use of statistics and metadata
   - Query cost estimation
   - Automatic index selection
   - Materialized view candidates

6. **Compression options**: Per-file compression settings
   - zstd, gzip, bzip2 at file level
   - Configurable compression levels
   - Transparent decompression

7. **Multi-writer support**: Distributed coordination
   - Optimistic concurrency control
   - Conflict resolution strategies
   - Distributed transaction log

## Related Documentation

- **System Patterns**: `docs/duckpond-system-patterns.md` - Critical implementation patterns
- **Anti-Patterns**: `docs/anti-duplication.md` - Code reuse guidelines
- **Fallback Philosophy**: `docs/fallback-antipattern-philosophy.md` - Error handling principles
- **Large Output Debugging**: `docs/large-output-debugging.md` - Debugging best practices
- **Copilot Instructions**: `.github/copilot-instructions.md` - AI assistant guidelines

---

**Last Updated**: December 25, 2025  
**Version**: v0.15.0
**Branch**: main (8 crates: tinyfs, tlogfs, steward, provider, cmd, hydrovu, remote, utilities)
