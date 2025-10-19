# Steward Post-Commit Factory Execution Design

## Executive Summary

This document describes a feature for the steward crate that sequences **post-commit operations** through factory execution. Building on the run configuration factory pattern established in `run-config-factory-design.md`, this extends factories to declare execution modes (in-transaction writer vs. post-commit reader) and enables steward to automatically execute post-commit factories after successful transaction commits.

## Motivation

**Current State:**
- Factories can execute via `pond run /path/to/config` (manual invocation)
- HydroVu factory runs data collection within a write transaction
- No automatic post-commit processing (validation, notifications, aggregations, exports)
- No sequencing mechanism for post-transaction operations

**Desired State:**
- Factories declare their execution capabilities (writer, post-commit reader, or both)
- Steward automatically discovers and executes post-commit factories after successful commits
- Post-commit progress tracked in control filesystem for crash recovery
- Configuration-based orchestration via filesystem patterns (e.g., `/etc/system.d/*`)
- Failures tracked and retryable without corrupting transaction state

**Use Cases:**
1. **Data Validation**: Post-commit schema validation, integrity checks
2. **Notification Systems**: Send alerts/webhooks after data ingestion
3. **Aggregation Pipelines**: Compute rollups/summaries after raw data writes
4. **Export Handlers**: Push data to external systems after commit
5. **Monitoring/Metrics**: Track data quality, collection rates, system health

## Architecture Overview

### Execution Mode Enum

Factories declare their execution capabilities via a new `ExecutionMode` enum:

```rust
/// Execution mode for factory runners
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Execute within a write transaction (can modify pond data)
    /// Examples: HydroVu collector, data importers
    InTransactionWriter,
    
    /// Execute after commit in a read-only transaction (cannot modify pond data)
    /// Examples: validators, notification systems, export handlers
    PostCommitReader,
}
```

**Design Decision: Runtime Mode Parameter**
- The same factory implementation can support multiple modes
- The `execute` function receives `ExecutionMode` as a parameter
- Factory declares supported modes via new field `supported_execution_modes: &[ExecutionMode]`
- Allows code reuse (e.g., a validator that can run in both modes for testing)

### Enhanced DynamicFactory Struct

```rust
pub struct DynamicFactory {
    pub name: &'static str,
    pub description: &'static str,
    
    // Existing fields...
    pub create_directory: Option<...>,
    pub create_file: Option<...>,
    pub validate_config: fn(config: &[u8]) -> TinyFSResult<Value>,
    pub try_as_queryable: Option<...>,
    pub initialize: Option<...>,
    
    /// Execute a run configuration
    /// Now receives ExecutionMode to indicate context (in-transaction vs post-commit)
    pub execute: Option<
        fn(
            config: Value,
            context: FactoryContext,
            mode: ExecutionMode,
        ) -> Pin<Box<dyn Future<Output = Result<(), TLogFSError>> + Send>>,
    >,
    
    /// NEW: Declare which execution modes this factory supports
    /// If empty, factory does not support execution at all
    pub supported_execution_modes: &'static [ExecutionMode],
}
```

### Factory Configuration Pattern

#### Post-Commit Readers: `/etc/system.d/`

Post-commit factories configured in the data filesystem under `/etc/system.d/`:

```
/etc/system.d/
  ├── 10-validate-schema
  ├── 20-send-notifications
  └── 30-export-data
```

**Execution:** Automatic after any write transaction commit (orchestrated by steward)

**Naming Convention:**
- Numeric prefixes (10-, 20-, 30-) control execution order
- Lexicographic sorting determines sequence
- Gaps allow insertion of new handlers without renumbering

**Discovery:** Pattern `/etc/system.d/*` expanded via `collect_matches()`

### Steward Commit Enhancement

The steward transaction commit process is extended to execute post-commit factories:

```
┌─────────────────────────────────────────────────────────────┐
│           User Write Transaction                             │
│  - Writes data to pond                                      │
│  - e.g., HydroVu collection, CSV import                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│               StewardTransactionGuard::commit()              │
│  1. Commit user transaction to Delta Lake (version N)       │
│  2. Record commit in control table                          │
│  3. Discover post-commit factories (/etc/system.d/*)        │
│  4. For each factory (in order):                            │
│     a. Open NEW read transaction (skip_post_commit=true)    │
│     b. Execute factory(config, context, PostCommitReader)   │
│     c. Record execution status in control table             │
│     d. Commit read transaction                              │
│     e. Continue even if factory fails                       │
│  5. Return (with warning if any failures)                   │
└─────────────────────────────────────────────────────────────┘
```

**Critical Pattern Compliance:**
- ✅ **Single Transaction Rule**: Post-commit factories get FRESH read transactions
- ✅ **Transaction Guard Lifecycle**: Each post-commit execution is a complete transaction cycle
- ✅ **State Management**: Each transaction has its own State instance (no sharing)
- ✅ **Fail-Fast Philosophy**: Failures recorded explicitly, not hidden
- ✅ **Local-First**: Failures don't block user operations

### Post-Commit Execution Flow

**High-Level Steps:**

1. **Discovery Phase**
   - Open read transaction with `skip_post_commit=true`
   - Use `collect_matches("/etc/system.d/*")` to find factories
   - Filter for factories supporting PostCommitReader mode
   - Read config file contents
   - Commit discovery transaction

2. **Execution Phase**
   - For each discovered factory (in lexicographic order):
   - Record "started" in control table
   - Open NEW read transaction with `skip_post_commit=true`
   - Create FactoryContext
   - Execute factory in PostCommitReader mode
   - Commit read transaction
   - Record "completed" or "failed" in control table
   - Continue to next factory regardless of outcome

3. **Completion**
   - Log summary of successes/failures
   - Return status (may include warning about failures)
   - All outcomes recorded in control table for recovery

### Control Table Schema Extension

Track post-commit execution in the control filesystem:

```sql
-- Existing transaction tracking
CREATE TABLE transactions (
    txn_seq BIGINT,
    txn_id STRING,
    state STRING,  -- "begin", "data_committed", "completed", "failed"
    ...
);

-- NEW: Post-commit execution tracking
CREATE TABLE post_commit_executions (
    txn_seq BIGINT,              -- Parent write transaction
    txn_id STRING,               -- Parent transaction ID
    execution_seq INT,           -- Order within post-commit sequence (1, 2, 3, ...)
    factory_name STRING,         -- Factory that executed
    config_path STRING,          -- Path to config node (e.g., /etc/system.d/10-validate)
    state STRING,                -- "started", "completed", "failed"
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_ms BIGINT,
    error_message STRING,        -- NULL on success
    retry_count INT              -- For future retry logic
);
```

**Recovery Scenario:**
If steward crashes during post-commit execution:
1. Control table shows which factories completed successfully
2. Recovery logic can resume from last successful factory
3. Parent transaction remains committed (no rollback of user data)

## Detailed Design

### 1. ExecutionMode Enum (New File)

**File:** `crates/tlogfs/src/execution_mode.rs` (NEW)

```rust
use serde::{Deserialize, Serialize};

/// Execution mode for factory runners
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Execute within a write transaction (can modify pond data)
    /// 
    /// Capabilities:
    /// - Full read/write access to pond filesystem
    /// - Can create/modify/delete files and directories
    /// - Runs BEFORE transaction commit
    /// - Errors cause transaction rollback
    /// 
    /// Use cases:
    /// - Data ingestion (HydroVu collector)
    /// - Data transformation pipelines
    /// - Bulk imports
    InTransactionWriter,
    
    /// Execute after commit in a read-only transaction
    /// 
    /// Capabilities:
    /// - Read-only access to pond filesystem
    /// - Cannot modify pond data
    /// - Runs AFTER transaction commit
    /// - Errors do not affect committed data
    /// 
    /// Use cases:
    /// - Data validation
    /// - Notifications/webhooks
    /// - Export to external systems
    /// - Aggregate computations
    /// - Monitoring/metrics
    PostCommitReader,
}

impl ExecutionMode {
    /// Returns true if this mode allows writing to the pond
    pub fn is_writer(&self) -> bool {
        matches!(self, ExecutionMode::InTransactionWriter)
    }
    
    /// Returns true if this mode runs after commit
    pub fn is_post_commit(&self) -> bool {
        matches!(self, ExecutionMode::PostCommitReader)
    }
    
    /// Human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            ExecutionMode::InTransactionWriter => "In-transaction writer (can modify pond data)",
            ExecutionMode::PostCommitReader => "Post-commit reader (read-only, runs after commit)",
        }
    }
}
```

### 2. Enhanced Factory System

**File:** `crates/tlogfs/src/factory.rs` (MODIFICATIONS)

```rust
// Add to imports
use crate::execution_mode::ExecutionMode;

pub struct DynamicFactory {
    pub name: &'static str,
    pub description: &'static str,
    
    // ... existing fields ...
    
    /// Execute a run configuration with specified mode
    pub execute: Option<
        fn(
            config: Value,
            context: FactoryContext,
            mode: ExecutionMode,
        ) -> Pin<Box<dyn Future<Output = Result<(), TLogFSError>> + Send>>,
    >,
    
    /// Declare which execution modes this factory supports
    /// Empty slice means factory does not support execution
    pub supported_execution_modes: &'static [ExecutionMode],
}

impl FactoryRegistry {
    /// Execute a run configuration using the specified factory and mode
    pub async fn execute(
        factory_name: &str,
        config: &[u8],
        context: FactoryContext,
        mode: ExecutionMode,
    ) -> Result<(), TLogFSError> {
        let factory = Self::get_factory(factory_name).ok_or_else(|| {
            TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Unknown factory: {}",
                factory_name
            )))
        })?;
        
        // Validate factory supports this execution mode
        if !factory.supported_execution_modes.contains(&mode) {
            return Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Factory '{}' does not support execution mode: {:?}",
                factory_name, mode
            ))));
        }
        
        let config_value = (factory.validate_config)(config)
            .map_err(|e| TLogFSError::TinyFS(e))?;
        
        if let Some(execute_fn) = factory.execute {
            execute_fn(config_value, context, mode).await
        } else {
            Err(TLogFSError::TinyFS(tinyfs::Error::Other(format!(
                "Factory '{}' does not support execution",
                factory_name
            ))))
        }
    }
    
    /// Check if a factory supports a specific execution mode
    pub fn supports_execution_mode(factory_name: &str, mode: ExecutionMode) -> bool {
        if let Some(factory) = Self::get_factory(factory_name) {
            factory.supported_execution_modes.contains(&mode)
        } else {
            false
        }
    }
}
```

**Macro Extension:**

```rust
#[macro_export]
macro_rules! register_executable_factory {
    (
        name: $name:expr,
        description: $description:expr,
        file: $file_fn:expr,
        validate: $validate_fn:expr,
        initialize: $initialize_fn:expr,
        execute: $execute_fn:expr,
        modes: [$($mode:expr),+ $(,)?]
    ) => {
        paste::paste! {
            fn [<file_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = tinyfs::Result<tinyfs::FileHandle>> + Send>> {
                Box::pin($file_fn(config, context))
            }
            
            fn [<initialize_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), $crate::TLogFSError>> + Send>> {
                Box::pin($initialize_fn(config, context))
            }
            
            fn [<execute_wrapper_ $name:snake>](
                config: serde_json::Value,
                context: $crate::factory::FactoryContext,
                mode: $crate::execution_mode::ExecutionMode,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), $crate::TLogFSError>> + Send>> {
                Box::pin($execute_fn(config, context, mode))
            }
            
            #[linkme::distributed_slice($crate::factory::DYNAMIC_FACTORIES)]
            static [<FACTORY_ $name:snake:upper>]: $crate::factory::DynamicFactory = $crate::factory::DynamicFactory {
                name: $name,
                description: $description,
                create_directory: None,
                create_file: Some([<file_wrapper_ $name:snake>]),
                validate_config: $validate_fn,
                try_as_queryable: None,
                initialize: Some([<initialize_wrapper_ $name:snake>]),
                execute: Some([<execute_wrapper_ $name:snake>]),
                supported_execution_modes: &[$($mode),+],
            };
        }
    };
}
```

### 3. Test Executor Factory (Example Implementation)

**File:** `crates/tlogfs/src/test_executor_factory.rs` (NEW)

```rust
//! Test executor factory for demonstrating post-commit execution
//!
//! This factory prints diagnostic messages to demonstrate execution modes.
//! It's useful for testing steward's post-commit orchestration without
//! side effects.

use crate::factory::{ConfigFile, FactoryContext};
use crate::execution_mode::ExecutionMode;
use crate::TLogFSError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyfs::{FileHandle, Result as TinyFSResult};

/// Test executor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestExecutorConfig {
    /// Message to print when executing
    pub message: String,
    
    /// Whether to simulate failure
    #[serde(default)]
    pub fail: bool,
    
    /// Delay in milliseconds (for testing timing)
    #[serde(default)]
    pub delay_ms: u64,
}

fn validate_test_executor_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let config_str = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid UTF-8: {}", e)))?;
    
    let config: TestExecutorConfig = serde_yaml::from_str(config_str)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML: {}", e)))?;
    
    serde_json::to_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))
}

async fn create_test_executor_file(
    config: Value,
    _context: FactoryContext,
) -> TinyFSResult<FileHandle> {
    let parsed_config: TestExecutorConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid config: {}", e)))?;
    
    let config_yaml = serde_yaml::to_string(&parsed_config)
        .map_err(|e| tinyfs::Error::Other(format!("Serialization error: {}", e)))?
        .into_bytes();
    
    Ok(ConfigFile::new(config_yaml).create_handle())
}

async fn execute_test_executor(
    config: Value,
    context: FactoryContext,
    mode: ExecutionMode,
) -> Result<(), TLogFSError> {
    let config: TestExecutorConfig = serde_json::from_value(config)
        .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("Invalid config: {}", e))))?;
    
    log::info!("🧪 TEST EXECUTOR: {}", config.message);
    log::info!("   Mode: {:?} ({})", mode, mode.description());
    log::info!("   Parent Node ID: {:?}", context.parent_node_id);
    
    // Simulate delay if configured
    if config.delay_ms > 0 {
        log::info!("   Sleeping for {}ms...", config.delay_ms);
        tokio::time::sleep(tokio::time::Duration::from_millis(config.delay_ms)).await;
    }
    
    // Demonstrate mode-specific behavior
    match mode {
        ExecutionMode::InTransactionWriter => {
            log::info!("   ✓ Running in-transaction (can write data)");
            // Could perform filesystem operations here
        }
        ExecutionMode::PostCommitReader => {
            log::info!("   ✓ Running post-commit (read-only)");
            // Read-only operations only
        }
    }
    
    // Simulate failure if configured
    if config.fail {
        log::error!("   ✗ Simulated failure!");
        return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
            "Simulated failure".to_string()
        )));
    }
    
    log::info!("   ✓ Test executor completed successfully");
    Ok(())
}

// Register factory supporting BOTH execution modes
crate::register_executable_factory!(
    name: "test-executor",
    description: "Test executor for demonstrating execution modes",
    file: create_test_executor_file,
    validate: validate_test_executor_config,
    initialize: |_config, _context| Box::pin(async { Ok(()) }),
    execute: execute_test_executor,
    modes: [
        crate::execution_mode::ExecutionMode::InTransactionWriter,
        crate::execution_mode::ExecutionMode::PostCommitReader
    ]
);
```

### 4. HydroVu Factory Update

**File:** `crates/hydrovu/src/factory.rs` (MODIFICATIONS)

```rust
// Update execute function signature
async fn execute_hydrovu(
    config: Value,
    context: FactoryContext,
    mode: ExecutionMode,
) -> Result<(), TLogFSError> {
    // Validate we're running in the correct mode
    if mode != ExecutionMode::InTransactionWriter {
        return Err(TLogFSError::TinyFS(tinyfs::Error::Other(
            "HydroVu collector requires InTransactionWriter mode".to_string()
        )));
    }
    
    // ... rest of existing implementation ...
}

// Update registration
tlogfs::register_executable_factory!(
    name: "hydrovu",
    description: "HydroVu data collector configuration",
    file: create_hydrovu_file,
    validate: validate_hydrovu_config,
    initialize: initialize_hydrovu,
    execute: execute_hydrovu,
    modes: [tlogfs::execution_mode::ExecutionMode::InTransactionWriter]
);
```

### 5. Steward Post-Commit Orchestration

**File:** `crates/steward/src/guard.rs` (MODIFICATIONS)

```rust
impl StewardTransactionGuard<'_> {
    pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
        // ... existing commit logic (Steps 1-2) ...
        
        // NEW: Step 3 - Execute post-commit factories
        if let Some(()) = write_occurred {
            self.execute_post_commit_sequence().await?;
        }
        
        Ok(write_occurred)
    }
    
    /// Execute the post-commit factory sequence
    async fn execute_post_commit_sequence(&mut self) -> Result<(), StewardError> {
        log::info!("Starting post-commit factory sequence for txn_seq={}", self.txn_seq);
        
        // Discover post-commit factories from /etc/system.d/*
        let factories = self.discover_post_commit_factories().await?;
        
        if factories.is_empty() {
            log::debug!("No post-commit factories configured");
            return Ok(());
        }
        
        log::info!("Discovered {} post-commit factories", factories.len());
        
        // Execute each factory in sequence
        for (execution_seq, (factory_name, config_path, config_bytes)) in factories.iter().enumerate() {
            let execution_seq = (execution_seq + 1) as i32; // 1-indexed
            
            log::info!(
                "Executing post-commit factory {}/{}: {} ({})",
                execution_seq,
                factories.len(),
                config_path,
                factory_name
            );
            
            // Record start in control table
            self.ship.control_table.record_post_commit_started(
                self.txn_seq,
                self.txn_id.clone(),
                execution_seq,
                factory_name.clone(),
                config_path.clone(),
            ).await?;
            
            let start_time = std::time::Instant::now();
            
            // Execute the factory in a NEW read transaction
            let result = self.execute_post_commit_factory(
                factory_name.clone(),
                config_path.clone(),
                config_bytes.clone(),
            ).await;
            
            let duration_ms = start_time.elapsed().as_millis() as i64;
            
            // Record result in control table
            match result {
                Ok(()) => {
                    log::info!("Post-commit factory succeeded: {} ({}ms)", config_path, duration_ms);
                    self.ship.control_table.record_post_commit_completed(
                        self.txn_seq,
                        self.txn_id.clone(),
                        execution_seq,
                        duration_ms,
                    ).await?;
                }
                Err(e) => {
                    let error_msg = format!("{}", e);
                    log::error!("Post-commit factory failed: {} - {}", config_path, error_msg);
                    self.ship.control_table.record_post_commit_failed(
                        self.txn_seq,
                        self.txn_id.clone(),
                        execution_seq,
                        error_msg,
                        duration_ms,
                    ).await?;
                    
                    // FAIL FAST: Stop processing remaining factories
                    return Err(StewardError::PostCommitFailed(config_path.clone(), e.to_string()));
                }
            }
        }
        
        log::info!("Post-commit factory sequence completed successfully");
        Ok(())
    }
    
    /// Discover post-commit factory configurations from /etc/system.d/*
    async fn discover_post_commit_factories(&self) -> Result<Vec<(String, String, Vec<u8>)>, StewardError> {
        // Open a temporary read transaction to query the data filesystem
        let mut temp_tx = self.ship.begin_transaction_internal(
            vec!["steward".to_string(), "discover-post-commit".to_string()],
            HashMap::new(),
            false, // read-only
        ).await?;
        
        let root = temp_tx.data_tx.as_ref().unwrap().root();
        
        // Check if /etc/system.d exists
        let system_d_path = "/etc/system.d";
        let entries = match root.read_dir(system_d_path).await {
            Ok(entries) => entries,
            Err(tinyfs::Error::NotFound) => {
                log::debug!("No /etc/system.d directory found");
                return Ok(Vec::new());
            }
            Err(e) => return Err(StewardError::from(e)),
        };
        
        // Collect config files, sorted by name
        let mut factories = Vec::new();
        for entry in entries {
            let entry_path = format!("{}/{}", system_d_path, entry.name);
            
            // Read the config file
            let node = match root.lookup(&entry_path).await? {
                tinyfs::Lookup::Found(node) => node,
                _ => continue,
            };
            
            // Must be a file
            let file_handle = match node.as_file() {
                Ok(fh) => fh,
                Err(_) => {
                    log::warn!("Skipping non-file entry: {}", entry_path);
                    continue;
                }
            };
            
            // Read file content (config bytes)
            let config_bytes = {
                let file = file_handle.get_file().await;
                let file_guard = file.lock().await;
                let mut reader = file_guard.open().await?;
                let mut buf = Vec::new();
                tokio::io::copy(&mut reader, &mut buf).await?;
                buf
            };
            
            // Get factory name from node attributes
            let node_ref = node.borrow().await;
            let attrs = node_ref.attributes();
            let factory_name = attrs
                .get("factory_name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| StewardError::Other(format!("No factory_name for {}", entry_path)))?
                .to_string();
            
            // Validate factory supports PostCommitReader mode
            if !tlogfs::factory::FactoryRegistry::supports_execution_mode(
                &factory_name,
                tlogfs::execution_mode::ExecutionMode::PostCommitReader
            ) {
                log::warn!(
                    "Skipping {}: factory '{}' does not support PostCommitReader mode",
                    entry_path,
                    factory_name
                );
                continue;
            }
            
            factories.push((factory_name, entry_path, config_bytes));
        }
        
        // Commit the temporary read transaction
        temp_tx.commit().await?;
        
        // Sort by config path (lexicographic = numeric prefix order)
        factories.sort_by(|a, b| a.1.cmp(&b.1));
        
        Ok(factories)
    }
    
    /// Execute a single post-commit factory in a NEW read transaction
    async fn execute_post_commit_factory(
        &mut self,
        factory_name: String,
        config_path: String,
        config_bytes: Vec<u8>,
    ) -> Result<(), StewardError> {
        // Open a NEW read transaction
        let mut read_tx = self.ship.begin_transaction_internal(
            vec![
                "steward".to_string(),
                "post-commit".to_string(),
                factory_name.clone(),
            ],
            HashMap::new(),
            false, // read-only
        ).await?;
        
        // Create factory context with read-only state
        let state = read_tx.data_tx.as_ref().unwrap().state()?;
        let context = tlogfs::factory::FactoryContext::new(
            state,
            tinyfs::NodeID::root(), // Parent is conceptually root for system.d configs
        );
        
        // Execute the factory in PostCommitReader mode
        let result = tlogfs::factory::FactoryRegistry::execute(
            &factory_name,
            &config_bytes,
            context,
            tlogfs::execution_mode::ExecutionMode::PostCommitReader,
        ).await;
        
        // Commit the read transaction (records completion in control table)
        read_tx.commit().await?;
        
        result.map_err(|e| StewardError::from(e))
    }
}
```

### 6. Control Table Extension

**File:** `crates/steward/src/control_table.rs` (MODIFICATIONS)

Add new methods for tracking post-commit execution:

```rust
impl ControlTable {
    /// Record the start of post-commit factory execution
    pub async fn record_post_commit_started(
        &self,
        parent_txn_seq: i64,
        parent_txn_id: String,
        execution_seq: i32,
        factory_name: String,
        config_path: String,
    ) -> Result<(), StewardError> {
        let timestamp = chrono::Utc::now();
        
        let record = RecordBatch::try_from_iter(vec![
            ("parent_txn_seq", Arc::new(Int64Array::from(vec![parent_txn_seq])) as ArrayRef),
            ("parent_txn_id", Arc::new(StringArray::from(vec![parent_txn_id])) as ArrayRef),
            ("execution_seq", Arc::new(Int32Array::from(vec![execution_seq])) as ArrayRef),
            ("factory_name", Arc::new(StringArray::from(vec![factory_name])) as ArrayRef),
            ("config_path", Arc::new(StringArray::from(vec![config_path])) as ArrayRef),
            ("state", Arc::new(StringArray::from(vec!["started"])) as ArrayRef),
            ("started_at", Arc::new(TimestampMicrosecondArray::from(vec![timestamp.timestamp_micros()])) as ArrayRef),
        ])?;
        
        // Append to post_commit_executions table
        // ... Delta Lake write logic ...
        
        Ok(())
    }
    
    pub async fn record_post_commit_completed(
        &self,
        parent_txn_seq: i64,
        parent_txn_id: String,
        execution_seq: i32,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        // Update record with state="completed", completed_at, duration_ms
        // ... Delta Lake update logic ...
        Ok(())
    }
    
    pub async fn record_post_commit_failed(
        &self,
        parent_txn_seq: i64,
        parent_txn_id: String,
        execution_seq: i32,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        // Update record with state="failed", error_message, duration_ms
        // ... Delta Lake update logic ...
        Ok(())
    }
    
    /// Query post-commit execution status for a transaction
    pub async fn get_post_commit_status(
        &self,
        txn_seq: i64,
    ) -> Result<Vec<PostCommitExecution>, StewardError> {
        // Query post_commit_executions table for parent_txn_seq
        // Return list of executions with their status
        // ... Delta Lake query logic ...
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct PostCommitExecution {
    pub execution_seq: i32,
    pub factory_name: String,
    pub config_path: String,
    pub state: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub duration_ms: Option<i64>,
    pub error_message: Option<String>,
}
```

### 7. CLI Integration

**File:** `crates/cmd/src/commands/run.rs` (MODIFICATIONS)

Update `pond run` command to pass execution mode:

```rust
pub async fn run_command(
    ship_context: &ShipContext,
    config_path: &str,
) -> Result<()> {
    // ... existing path resolution ...
    
    // Determine execution mode based on config location
    // Default: InTransactionWriter (for manual execution)
    let mode = tlogfs::execution_mode::ExecutionMode::InTransactionWriter;
    
    // Execute in write transaction
    ship.transact(
        vec!["pond".to_string(), "run".to_string(), config_path.to_string()],
        move |tx, _fs| {
            let factory_name = factory_name.clone();
            let config_content = config_content.clone();
            Box::pin(async move {
                let state = tx.state()?;
                let context = tlogfs::factory::FactoryContext::new(
                    state,
                    tinyfs::NodeID::root(),
                );
                
                // Execute with explicit mode
                tlogfs::factory::FactoryRegistry::execute(
                    &factory_name,
                    &config_content,
                    context,
                    mode, // Pass execution mode
                )
                .await
                .map_err(|e| StewardError::from(e))?;
                
                Ok(())
            })
        }
    ).await?;
    
    println!("✓ Configuration executed successfully");
    Ok(())
}
```

## Testing Strategy

### Unit Tests

**Test Factory Execution Modes:**
```rust
#[tokio::test]
async fn test_factory_execution_modes() {
    // Create test executor config
    let config = TestExecutorConfig {
        message: "Test message".to_string(),
        fail: false,
        delay_ms: 0,
    };
    
    // Test InTransactionWriter mode
    let result = execute_test_executor(
        serde_json::to_value(&config).unwrap(),
        test_context(),
        ExecutionMode::InTransactionWriter,
    ).await;
    assert!(result.is_ok());
    
    // Test PostCommitReader mode
    let result = execute_test_executor(
        serde_json::to_value(&config).unwrap(),
        test_context(),
        ExecutionMode::PostCommitReader,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_factory_mode_validation() {
    // HydroVu should only support InTransactionWriter
    assert!(FactoryRegistry::supports_execution_mode(
        "hydrovu",
        ExecutionMode::InTransactionWriter
    ));
    assert!(!FactoryRegistry::supports_execution_mode(
        "hydrovu",
        ExecutionMode::PostCommitReader
    ));
}
```

### Integration Tests

**Test Post-Commit Orchestration:**
```bash
#!/bin/bash
# test-post-commit.sh

set -e

POND=/tmp/test-pond-post-commit
EXE=./target/debug/pond

rm -rf $POND
export POND

# Initialize pond
$EXE init

# Create directories
$EXE mkdir /etc/system.d

# Create test executor configs
cat > /tmp/test-10.yaml << EOF
message: "First post-commit handler"
fail: false
delay_ms: 100
EOF

cat > /tmp/test-20.yaml << EOF
message: "Second post-commit handler"
fail: false
delay_ms: 100
EOF

# Register post-commit factories
$EXE mknod test-executor /etc/system.d/10-first --config-path /tmp/test-10.yaml
$EXE mknod test-executor /etc/system.d/20-second --config-path /tmp/test-20.yaml

# Create a test data file to trigger post-commit
$EXE mknod file /test/data.txt

# The commit should trigger post-commit execution
# Check logs for execution messages

echo "✓ Post-commit test completed"
```

**Test Failure Handling:**
```bash
# Create failing executor
cat > /tmp/test-fail.yaml << EOF
message: "This will fail"
fail: true
EOF

$EXE mknod test-executor /etc/system.d/30-fail --config-path /tmp/test-fail.yaml

# This operation should fail during post-commit
# But the data write should be committed
$EXE mknod file /test/more-data.txt || echo "Expected failure"

# Verify /test/more-data.txt exists (data was committed)
$EXE cat /test/more-data.txt

# Check control table for failure record
# ... query logic ...
```

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
- [ ] Create `execution_mode.rs` with `ExecutionMode` enum
- [ ] Update `DynamicFactory` struct with `supported_execution_modes` field
- [ ] Update `FactoryRegistry::execute()` to accept `ExecutionMode` parameter
- [ ] Extend `register_executable_factory!` macro to support modes
- [ ] Update HydroVu factory to declare mode support
- [ ] Unit tests for execution mode validation

### Phase 2: Test Executor Factory (Week 1)
- [ ] Create `test_executor_factory.rs`
- [ ] Implement `TestExecutorConfig` struct
- [ ] Implement execution with mode-specific behavior
- [ ] Register with both mode support
- [ ] Integration tests for test executor

### Phase 3: Control Table Extension (Week 2)
- [ ] Design post_commit_executions table schema
- [ ] Implement `record_post_commit_started()`
- [ ] Implement `record_post_commit_completed()`
- [ ] Implement `record_post_commit_failed()`
- [ ] Implement `get_post_commit_status()`
- [ ] Unit tests for control table operations

### Phase 4: Steward Orchestration (Week 2-3)
- [ ] Implement `discover_post_commit_factories()` in guard.rs
- [ ] Implement `execute_post_commit_factory()` in guard.rs
- [ ] Implement `execute_post_commit_sequence()` in guard.rs
- [ ] Integrate post-commit sequence into `commit()` method
- [ ] Add proper error handling and logging
- [ ] Integration tests for orchestration

### Phase 5: CLI & Documentation (Week 3)
- [ ] Update `pond run` command to support modes
- [ ] Add `pond show-post-commit` command (query control table)
- [ ] Create setup script examples with /etc/system.d
- [ ] Write user-facing documentation
- [ ] Create example post-commit factories

### Phase 6: Advanced Features (Future)
- [ ] Retry logic for failed post-commit executions
- [ ] Parallel execution for independent factories
- [ ] Factory dependencies (execution order constraints)
- [ ] Conditional execution based on transaction content
- [ ] Post-commit factory for notification webhooks
- [ ] Post-commit factory for S3 export

## System Pattern Compliance

This design follows all critical DuckPond system patterns:

### ✅ Single Transaction Rule
- Post-commit factories get FRESH read transactions
- No concurrent transaction guards
- Each factory execution is isolated

### ✅ Transaction Guard Lifecycle
- Each post-commit factory has complete transaction lifecycle:
  1. `begin_transaction_internal()`
  2. Execute factory
  3. `commit()`
- Proper state cleanup on success/failure

### ✅ State Management
- Each transaction creates its own State instance
- No State sharing between transactions
- Factory gets State via FactoryContext

### ✅ Fail-Fast Philosophy
- Post-commit failures halt sequence immediately
- No silent fallbacks or error suppression
- Failures recorded in control table for visibility

### ✅ Path vs NodeID Boundaries
- Discovery phase uses paths (`/etc/system.d/*`)
- Execution phase uses NodeID/PartID internally
- Clear layer separation maintained

### ✅ TableProvider Ownership
- Post-commit factories are read-only
- No TableProvider duplication concerns
- All queries go through shared DataFusion context

## Benefits

### 1. **Automatic Post-Processing**
- No manual steps after data ingestion
- Validation, notifications, exports happen automatically
- Consistent processing for all write transactions

### 2. **Crash Recovery**
- Post-commit progress tracked in control table
- Can resume from last successful factory
- Parent transaction remains committed (no data loss)

### 3. **Extensibility**
- Easy to add new post-commit handlers
- Configuration-based orchestration
- No code changes to steward for new factory types

### 4. **Separation of Concerns**
- Writers focus on data ingestion
- Readers focus on post-processing
- Clear execution mode boundaries

### 5. **Testability**
- Test executor factory for development
- Can test post-commit logic in isolation
- Simulation of failures without affecting data

### 6. **Observability**
- Control table tracks all executions
- Duration metrics for each factory
- Error messages for debugging

## Future Enhancements

### Retry Logic
Automatically retry failed post-commit executions:
```rust
pub struct PostCommitConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}
```

### Parallel Execution
Execute independent factories concurrently:
```yaml
# /etc/system.d/config.yaml
parallel_groups:
  - [10-validate, 11-check-schema]  # Run in parallel
  - [20-notify-email, 21-notify-slack, 22-notify-webhook]  # Run in parallel
  - [30-export-s3]  # Run after notifications
```

### Conditional Execution
Execute factories based on transaction content:
```yaml
# /etc/system.d/10-notify-on-hydrovu.yaml
factory: webhook-notifier
condition:
  path_pattern: "/hydrovu/**"
config:
  webhook_url: "https://..."
```

### Factory Dependencies
Declare execution order constraints:
```yaml
# /etc/system.d/20-aggregate.yaml
factory: aggregator
depends_on:
  - 10-validate-schema
config:
  ...
```

## Refined Design Based on Codebase Investigation

### Investigation Summary

After reviewing the codebase, key findings:

1. **✅ Ship::begin_transaction() exists** - Public method taking `TransactionOptions`, returns `StewardTransactionGuard`
2. **✅ Factory storage** - Factory names stored in OpLog via `State::get_factory_for_node(node_id, part_id)`
3. **✅ Pattern matching** - TinyFS provides `collect_matches(pattern)` for wildcard expansion
4. **✅ SessionContext** - Available via `tx.session_context().await?` delegating to State
5. **✅ Control Table** - Separate Delta Lake table with independent transaction management
6. **✅ Sequential execution** - System is sequential by design, no concurrent transaction issues

### Architecture Constraints

**Guard Pattern:**  
`commit()` consumes `self` to prevent double-commit. Post-commit execution must happen INSIDE `commit()` after data filesystem commits but before returning.

**Ship Ownership:**  
`StewardTransactionGuard` holds `&'a mut ControlTable`, NOT `&'a mut Ship`. To create new transactions for post-commit execution, pass Ship reference via parameter.

**Control Table:**  
Single Delta Lake table with its own transaction management. Will extend schema to track post-commit execution.

## Core Design Decisions

### 1. Post-Commit Execution Location

**Decision:** Post-commit execution happens INSIDE `StewardTransactionGuard::commit()`
- After data transaction commits to Delta Lake
- Before commit() returns to caller
- Requires Ship reference passed as parameter to commit()

### 2. Modified Commit Signature

**Change:** Add `ship: &mut Ship` parameter to enable post-commit transaction creation

**Breaking Change Impact:** All callers must update from `tx.commit().await?` to `tx.commit(&mut ship).await?`

### 3. Independent Factory Execution

**Decision:** Each factory's success/failure tracked separately
- Factory F failing doesn't block Factory G execution
- Control table records all outcomes for later retry
- Local-first: failures deferred, don't block user operations

### 4. Nested Post-Commit Prevention

**Decision:** Add `skip_post_commit` flag to `TransactionOptions`
- Post-commit read transactions set this flag
- Prevents infinite recursion if factory writes data

### 5. Factory Discovery Path

**Decision:** Hard-code `/etc/system.d/*` pattern for post-commit factories
- Use TinyFS `collect_matches()` for wildcard expansion
- Numeric prefixes (10-validate, 20-notify) control execution order via lexicographic sort
- Don't document conventions yet - get it working first

### 6. Read-Only Enforcement

**Decision:** Advisory with structural prevention
- Post-commit factories receive read transaction objects
- Steward controls commit - factories can't commit writes
- Must ensure read transactions see just-committed Delta Lake version

**Critical:** Read transactions must be applied at the just-committed version, not stale data

## Control Table Schema Extension

### Approach

Extend the existing single control table with post-commit tracking fields. Keep it simple.

### Required Fields

Add to existing `TransactionRecord` or related tracking:
- `post_commit_execution_seq` - Order within post-commit sequence (1, 2, 3...)
- `post_commit_factory_name` - Which factory executed
- `post_commit_config_path` - Path to config (e.g., /etc/system.d/10-validate)
- `post_commit_state` - "started" | "completed" | "failed"
- `post_commit_error` - Error message if failed

### Implementation Note

Study the existing control table schema and extend minimally. The control table already tracks transaction lifecycle - we're adding post-commit lifecycle to that.

## Implementation Requirements

### Required Code Changes

1. **TransactionOptions Struct**
   - Add `skip_post_commit: bool` field
   - Add builder method `with_skip_post_commit()`

2. **StewardTransactionGuard::commit() Signature**
   - Change from `commit(mut self)` to `commit(mut self, ship: &mut Ship)`
   - **Breaking change** - all call sites must update

3. **Control Table Methods**
   - Extend schema for post-commit fields
   - Add `record_post_commit_started()`
   - Add `record_post_commit_completed()`
   - Add `record_post_commit_failed()`

4. **Post-Commit Orchestration**
   - Implement discovery using `collect_matches("/etc/system.d/*")`
   - Execute each factory in new read transaction with `skip_post_commit=true`
   - Track status independently
   - Don't fail-fast - continue on failures

5. **Factory Registry**
   - Add `ExecutionMode` enum
   - Update `execute()` to take mode parameter
   - Add `supported_execution_modes` field to `DynamicFactory`
   - Add `supports_execution_mode()` query method

6. **Test Executor Factory**
   - Simple factory for testing post-commit
   - Supports both InTransactionWriter and PostCommitReader modes
   - Can simulate failures for testing

### Critical Implementation Details

**Delta Lake Version Visibility:**
- Post-commit read transactions MUST see just-committed data
- Need to verify/ensure read transactions use correct Delta Lake version
- Test this explicitly

**Transaction Creation:**
- Post-commit uses `Ship::begin_transaction()` with read options
- Set `skip_post_commit=true` to prevent recursion
- Each factory gets isolated transaction

**Error Handling:**
- Individual factory failures recorded but don't halt sequence
- Control table tracks all outcomes
- commit() can return warning about failures

## Implementation Phases

### Phase 1: Core Infrastructure
- Add ExecutionMode enum
- Add skip_post_commit to TransactionOptions  
- Update commit() signature across codebase
- Update DynamicFactory struct
- Unit tests

### Phase 2: Test Executor Factory
- Create simple test factory
- Support both execution modes
- Integration tests

### Phase 3: Control Table Extension
- Extend schema (study existing structure first)
- Add recording methods
- Add query methods
- Tests

### Phase 4: Post-Commit Orchestration
- Implement discovery
- Implement execution loop
- Integrate into commit()
- Integration tests

### Phase 5: Recovery & Tooling
- Add recovery command
- Add status query command
- Documentation
- End-to-end tests

## Verification Requirements

### Critical Tests

1. **Version Visibility Test**
   - Write data in transaction
   - Verify post-commit factory sees new data
   - Verify not reading stale version

2. **Independent Execution Test**
   - Three factories: success, fail, success
   - Verify all three execute
   - Verify all three tracked independently in control table

3. **Nested Prevention Test**
   - Post-commit factory attempts transaction
   - Verify no infinite loop
   - Verify skip_post_commit prevents recursion

4. **Discovery Test**
   - Create /etc/system.d with ordered configs
   - Verify correct discovery order
   - Verify non-factories skipped

## Design Principles

### Focus on Architecture

This document describes WHAT needs to change, not HOW to implement it. When implementing:

1. **Study the existing code** - Don't copy pseudocode
2. **Follow established patterns** - Match existing style
3. **Make minimal changes** - Extend, don't replace
4. **Test incrementally** - Each phase builds on previous

### Keep It Simple

- One control table, extended simply
- Hard-coded `/etc/system.d/*` path
- Sequential execution only
- Advisory read-only (structural prevention via transactions)
- Get it working before optimizing

### Local-First Design

- Failed post-commit operations don't block users
- Track failures for later retry
- No distributed coordination
- Simple recovery via command

## Summary

**Core Changes:**
1. Add `skip_post_commit` flag
2. Change `commit()` signature to take `&mut Ship`
3. Extend control table for post-commit tracking
4. Implement discovery and execution
5. Update all call sites (breaking change)

**Architecture:**
- Post-commit inside commit() after data commit
- Independent factory execution
- Control table tracks all outcomes
- Read transactions prevent writes structurally
- Must ensure version visibility

**Next Step:**
Begin Phase 1 implementation by studying existing code and making minimal extensions.

---

## Summary and Next Steps

### Design Status

This design has been refined based on thorough codebase investigation and provides a robust, extensible system for post-commit factory execution.

**✅ Resolved Architectural Questions:**
1. **Ship Reference** - Pass `&mut Ship` to `commit()` method
2. **Transaction Pattern** - Post-commit inside `commit()`, after data commit, before return
3. **Factory Storage** - OpLog storage via `get_factory_for_node()` (already implemented)
4. **Pattern Matching** - TinyFS `collect_matches()` (already implemented)
5. **Independent Execution** - Each factory tracked separately, failures don't block others
6. **Nested Prevention** - `skip_post_commit` flag in TransactionOptions
7. **Discovery Algorithm** - `collect_matches("/etc/system.d/*")` with lexicographic sort

**⚠️ Implementation Tasks:**
1. Add `skip_post_commit` field to `TransactionOptions`
2. Modify `commit()` signature to take `&mut Ship`
3. Extend control table schema for post-commit tracking
4. Implement discovery, execution, and status recording
5. Update all `commit()` call sites
6. Add recovery command for retrying failed post-commit operations

**❓ Verification Needed:**
1. Delta Lake version visibility for post-commit read transactions
2. Control table schema design (separate table vs extended fields)
3. Symlink vs direct file convention preference
4. Advisory vs enforced read-only for post-commit transactions

### Architecture Compliance

**✅ DuckPond System Patterns:**
- Single Transaction Rule: Sequential transactions, not concurrent ✓
- Transaction Guard Lifecycle: Each post-commit has complete lifecycle ✓
- State Management: Each transaction gets own State ✓
- Fail-Fast Philosophy: Failures recorded, not hidden ✓
- No Fallback Antipattern: Explicit error handling ✓

**✅ Local-First Design:**
- Post-commit failures don't block user transactions ✓
- Failed operations tracked for later retry ✓
- No distributed coordination required ✓

### Implementation Phases (Updated)

**Phase 1: Core Infrastructure (Week 1)**
- [x] Design complete and refined
- [ ] Add `ExecutionMode` enum
- [ ] Add `skip_post_commit` to TransactionOptions
- [ ] Update `commit()` signature
- [ ] Update `DynamicFactory` with execution modes
- [ ] Update all `commit()` call sites in codebase
- [ ] Unit tests for execution mode validation

**Phase 2: Test Executor Factory (Week 1)**
- [ ] Create test_executor_factory.rs
- [ ] Support both InTransactionWriter and PostCommitReader
- [ ] Demonstrate independent success/failure
- [ ] Integration tests

**Phase 3: Control Table Extension (Week 2)**
- [ ] Design post_commit_executions schema
- [ ] Implement recording methods
- [ ] Implement query methods
- [ ] Unit tests

**Phase 4: Post-Commit Orchestration (Week 2-3)**
- [ ] Implement `discover_post_commit_factories()`
- [ ] Implement `execute_post_commit_sequence()`
- [ ] Implement `execute_single_post_commit_factory()`
- [ ] Add post-commit to `commit()` method
- [ ] Integration tests with test executor
- [ ] Test independent factory execution

**Phase 5: Recovery & CLI (Week 3)**
- [ ] Implement `pond recover --post-commit`
- [ ] Implement `pond status --post-commit`
- [ ] Create example post-commit factories
- [ ] Update documentation
- [ ] End-to-end tests

**Phase 6: Advanced Features (Future)**
- [ ] Automatic recovery on startup
- [ ] Parallel execution groups
- [ ] Conditional execution based on transaction content
- [ ] Retry strategies with backoff
- [ ] Post-commit metrics and monitoring

### Breaking Changes

**API Changes:**
```rust
// Before
tx.commit().await?;

// After
tx.commit(&mut ship).await?;
```

**Impact:** All call sites must be updated. Compiler will catch missing parameter.

**Migration Strategy:**
1. Update Ship methods first (transact, transact_with_session, etc.)
2. Update cmd layer
3. Run tests to find remaining call sites
4. Document in CHANGELOG

### Verification Tests

**Critical Tests to Write:**
1. **Version Visibility Test:**
   - Write data in transaction N
   - Commit
   - Open post-commit read transaction
   - Verify can query newly written data

2. **Independent Execution Test:**
   - Create 3 post-commit factories: success, fail, success
   - Verify factory 1 succeeds
   - Verify factory 2 fails
   - Verify factory 3 still executes and succeeds
   - Verify control table records all three statuses

3. **Nested Prevention Test:**
   - Post-commit factory attempts to write data
   - Verify no infinite loop
   - Verify skip_post_commit flag works

4. **Recovery Test:**
   - Execute transaction with post-commit failures
   - Run recovery command
   - Verify only failed factories re-execute
   - Verify successful factories not re-run

### Questions for Design Review

1. **Control Table Schema:** Separate table vs extended TransactionRecord?
2. **Recovery Timing:** Phase 5 or defer to Phase 6?
3. **Read-Only Enforcement:** Advisory or enforced at API level?
4. **Symlink Convention:** Document as recommended pattern?
5. **Error Return:** Should `commit()` return special type for post-commit failures?

### Recommendation

**Ready to begin Phase 1 implementation** with the following approach:

1. Start with minimal viable implementation:
   - ExecutionMode enum
   - skip_post_commit flag
   - Modified commit() signature
   - Basic post-commit sequence (no recovery yet)

2. Test thoroughly with test executor factory

3. Add control table tracking and recovery in subsequent phases

4. Keep implementation incremental and testable throughout

The design is well-grounded in the existing codebase and follows established patterns. The main implementation work is straightforward extension of existing mechanisms.
