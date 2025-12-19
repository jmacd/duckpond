// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Control Table - DeltaLake-based transaction tracking for Steward
//!
//! This is a clean rewrite following these principles:
//! - Type-safe enums instead of string constants
//! - Typed Uuid instead of String
//! - serde_arrow for Arrow serialization/deserialization
//! - Single SessionContext registered once and reused
//! - Explicit create/open pattern from tlogfs
//! - Partitioned by record_category: "metadata" vs "transaction" (like tlogfs directory partitions)
//! - Configuration fields repeated in each record for easy extraction
//! - JSON fields (cli_args, environment, factory_modes) with DataFusion JSON functions
//!
//! # Partitioning Strategy
//!
//! Like tlogfs uses partitions for different directories, we partition by `record_category`:
//! - `PARTITION_METADATA`: Initial pond metadata and configuration updates
//! - `PARTITION_TRANSACTION`: All transaction lifecycle records
//!
//! This allows efficient querying: `WHERE record_category = 'transaction'` uses partition pruning.
//!
//! # JSON Query Examples
//!
//! Thanks to `datafusion-functions-json`, you can query JSON fields directly:
//!
//! ```sql
//! -- Get transactions where cli_args contains "init"
//! SELECT txn_seq, json_as_text(cli_args, 0) as command
//! FROM transactions
//! WHERE record_category = 'transaction'
//!   AND json_contains(cli_args, 0) AND json_get_str(cli_args, 0) = 'init'
//!
//! -- Get factory mode for 'remote' factory from metadata partition
//! SELECT json_get_str(factory_modes, 'remote') as remote_mode
//! FROM transactions
//! WHERE record_category = 'metadata'
//! ORDER BY timestamp DESC
//! LIMIT 1
//!
//! -- Get environment variable from transaction
//! SELECT txn_seq, json_get_str(environment, 'USER') as user
//! FROM transactions
//! WHERE record_category = 'transaction'
//! WHERE json_contains(environment, 'USER')
//!
//! -- Using operators (-> for json_get, ->> for json_as_text, ? for json_contains)
//! SELECT txn_seq, factory_modes->'remote' as remote_mode
//! FROM transactions
//! WHERE factory_modes ? 'remote'
//! ```

use crate::StewardError;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use uuid7::Uuid;

// Re-export pond metadata types from tlogfs
pub use tlogfs::{PondMetadata, PondTxnMetadata};

// Partition values - like tlogfs uses for directory partitions
pub const PARTITION_METADATA: &str = "metadata";
pub const PARTITION_TRANSACTION: &str = "transaction";

/// Transaction record state lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordType {
    /// Transaction started
    Begin,
    /// Data successfully committed to data filesystem
    DataCommitted,
    /// Transaction completed (for read-only transactions)
    Completed,
    /// Transaction failed with error
    Failed,
    /// Post-commit task queued for execution
    PostCommitPending,
    /// Post-commit task execution started
    PostCommitStarted,
    /// Post-commit task completed successfully
    PostCommitCompleted,
    /// Post-commit task failed with error
    PostCommitFailed,
}

/// Type of transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    /// Read-only transaction
    Read,
    /// Write transaction (modifies data filesystem)
    Write,
    /// Post-commit task execution
    PostCommit,
}

/// Transaction record in control table
/// Configuration is embedded in each record for easy extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    // === Partition key (separates metadata from transactions) ===
    /// "metadata" for configuration records, "transaction" for transaction log entries
    pub record_category: String,

    // === Primary identifiers ===
    /// Transaction sequence number (0 is reserved for initial metadata record)
    pub txn_seq: i64,
    /// Transaction UUID (links to Delta commit metadata)
    pub txn_id: Uuid,
    /// Sequence number this transaction is based on (for validation)
    pub based_on_seq: Option<i64>,

    // === Transaction lifecycle ===
    pub record_type: RecordType,
    /// Microseconds since epoch (DeltaLake timestamp format)
    pub timestamp: i64,
    pub transaction_type: TransactionType,

    // === Context capture (stored as JSON strings for Delta Lake compatibility) ===
    /// CLI arguments that initiated this transaction (JSON array)
    pub cli_args: String,
    /// Environment variables and user metadata (JSON object)
    pub environment: String,

    // === Data filesystem linkage ===
    /// Delta Lake version of data filesystem after commit
    pub data_fs_version: Option<i64>,

    // === Error tracking ===
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,

    // === Post-commit task tracking ===
    /// Parent transaction sequence that triggered this post-commit task
    pub parent_txn_seq: Option<i64>,
    /// Ordinal position in post-commit factory list (1, 2, 3...)
    pub execution_seq: Option<i32>,
    /// Factory name that created/executed this task
    pub factory_name: Option<String>,
    /// Path to factory config file
    pub config_path: Option<String>,

    // === Embedded configuration (repeated for easy extraction) ===
    /// Pond UUID (repeated in every record)
    pub pond_id: Uuid,
    /// Factory execution modes as JSON (e.g., {"remote":"push"}, repeated in every record)
    pub factory_modes: String,
}

impl TransactionRecord {
    /// Create a new transaction begin record with embedded configuration
    pub fn new_begin(
        txn_meta: &PondTxnMetadata,
        based_on_seq: Option<i64>,
        transaction_type: TransactionType,
        pond_id: Uuid,
        factory_modes: &HashMap<String, String>,
    ) -> Self {
        Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id,
            based_on_seq,
            record_type: RecordType::Begin,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type,
            cli_args: serde_json::to_string(&txn_meta.user.args)
                .unwrap_or_else(|_| "[]".to_string()),
            environment: serde_json::to_string(&txn_meta.user.vars)
                .unwrap_or_else(|_| "{}".to_string()),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
            pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// Create a data_committed record
    pub fn new_data_committed(
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        data_fs_version: i64,
        duration_ms: i64,
        pond_id: Uuid,
        factory_modes: &HashMap<String, String>,
    ) -> Self {
        Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id,
            based_on_seq: None,
            record_type: RecordType::DataCommitted,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type,
            cli_args: serde_json::to_string(&txn_meta.user.args)
                .unwrap_or_else(|_| "[]".to_string()),
            environment: "{}".to_string(), // Don't repeat from begin record
            data_fs_version: Some(data_fs_version),
            error_message: None,
            duration_ms: Some(duration_ms),
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
            pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// Create a failed record
    pub fn new_failed(
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        error_message: String,
        duration_ms: i64,
        pond_id: Uuid,
        factory_modes: &HashMap<String, String>,
    ) -> Self {
        Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id,
            based_on_seq: None,
            record_type: RecordType::Failed,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type,
            cli_args: serde_json::to_string(&txn_meta.user.args)
                .unwrap_or_else(|_| "[]".to_string()),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: Some(error_message),
            duration_ms: Some(duration_ms),
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
            pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// Create a completed record (for read-only transactions)
    pub fn new_completed(
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        duration_ms: i64,
        pond_id: Uuid,
        factory_modes: &HashMap<String, String>,
    ) -> Self {
        Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id,
            based_on_seq: None,
            record_type: RecordType::Completed,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type,
            cli_args: serde_json::to_string(&txn_meta.user.args)
                .unwrap_or_else(|_| "[]".to_string()),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: Some(duration_ms),
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
            pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        }
    }

    /// Create a post-commit pending record
    pub fn new_post_commit_pending(
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        factory_name: String,
        config_path: String,
        pond_metadata: &PondMetadata,
        factory_modes: &HashMap<String, String>,
    ) -> Result<Self, StewardError> {
        Ok(Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: 0,             // Post-commit tasks don't have their own txn_seq
            txn_id: uuid7::uuid7(), // Generate new UUID for this task
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: RecordType::PostCommitPending,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::PostCommit,
            cli_args: "[]".to_string(),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq as i32),
            factory_name: Some(factory_name),
            config_path: Some(config_path),
            pond_id: pond_metadata.pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        })
    }

    /// Create a post-commit started record
    pub fn new_post_commit_started(
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        pond_metadata: &PondMetadata,
        factory_modes: &HashMap<String, String>,
    ) -> Result<Self, StewardError> {
        Ok(Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: 0,
            txn_id: uuid7::uuid7(),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: RecordType::PostCommitStarted,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::PostCommit,
            cli_args: "[]".to_string(),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq as i32),
            factory_name: None,
            config_path: None,
            pond_id: pond_metadata.pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        })
    }

    /// Create a post-commit completed record
    pub fn new_post_commit_completed(
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        duration_ms: i64,
        pond_metadata: &PondMetadata,
        factory_modes: &HashMap<String, String>,
    ) -> Result<Self, StewardError> {
        Ok(Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: 0,
            txn_id: uuid7::uuid7(),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: RecordType::PostCommitCompleted,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::PostCommit,
            cli_args: "[]".to_string(),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: Some(duration_ms),
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq as i32),
            factory_name: None,
            config_path: None,
            pond_id: pond_metadata.pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        })
    }

    /// Create a post-commit failed record
    pub fn new_post_commit_failed(
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        error_message: String,
        duration_ms: i64,
        pond_metadata: &PondMetadata,
        factory_modes: &HashMap<String, String>,
    ) -> Result<Self, StewardError> {
        Ok(Self {
            record_category: PARTITION_TRANSACTION.into(),
            txn_seq: 0,
            txn_id: uuid7::uuid7(),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: RecordType::PostCommitFailed,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::PostCommit,
            cli_args: "[]".to_string(),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: Some(error_message),
            duration_ms: Some(duration_ms),
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq as i32),
            factory_name: None,
            config_path: None,
            pond_id: pond_metadata.pond_id,
            factory_modes: serde_json::to_string(factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        })
    }
}

/// Control table for tracking transaction lifecycle and sequencing
pub struct ControlTable {
    /// Path to the Delta Lake table
    path: String,
    /// The Delta Lake table instance
    table: DeltaTable,
    /// Shared SessionContext with control table registered
    /// Created once during initialization, reused for all queries
    /// DataFusion automatically sees new Delta commits when querying
    session_context: Arc<SessionContext>,
    /// Cached pond metadata (immutable pond identity)
    pond_metadata: PondMetadata,
    /// Cached factory execution modes (mutable configuration)
    /// Updated whenever factory modes are changed
    factory_modes: HashMap<String, String>,
    /// Cached settings (mutable configuration)
    /// Updated whenever settings are changed
    settings: HashMap<String, String>,
}

impl ControlTable {
    /// Get the Arrow schema for the control table
    /// Schema matches TransactionRecord structure
    /// Partitioned by record_category: "metadata" for configuration, "transaction" for txn records
    #[must_use]
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            // Partition key - separates metadata from transaction records (like tlogfs directory partitions)
            Field::new("record_category", DataType::Utf8, false), // "metadata" | "transaction"
            // Primary identifiers
            Field::new("txn_seq", DataType::Int64, false),
            Field::new("txn_id", DataType::Utf8, false), // Serialized UUID
            Field::new("based_on_seq", DataType::Int64, true),
            // Transaction lifecycle
            Field::new("record_type", DataType::Utf8, false), // Enum as string
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("transaction_type", DataType::Utf8, false), // Enum as string
            // Context capture (JSON strings)
            Field::new("cli_args", DataType::Utf8, false), // JSON array
            Field::new("environment", DataType::Utf8, false), // JSON object
            // Data filesystem linkage
            Field::new("data_fs_version", DataType::Int64, true),
            // Error tracking
            Field::new("error_message", DataType::Utf8, true),
            Field::new("duration_ms", DataType::Int64, true),
            // Post-commit task tracking
            Field::new("parent_txn_seq", DataType::Int64, true),
            Field::new("execution_seq", DataType::Int32, true),
            Field::new("factory_name", DataType::Utf8, true),
            Field::new("config_path", DataType::Utf8, true),
            // Embedded configuration (repeated in every record)
            Field::new("pond_id", DataType::Utf8, false), // Serialized UUID
            Field::new("factory_modes", DataType::Utf8, false), // JSON object
        ]))
    }

    /// Convert Arrow schema to Delta Lake schema
    fn delta_schema() -> Vec<DeltaStructField> {
        let arrow_schema = Self::arrow_schema();
        arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let delta_type = match field.data_type() {
                    DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
                    DataType::Int32 => DeltaDataType::Primitive(PrimitiveType::Integer),
                    DataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        DeltaDataType::Primitive(PrimitiveType::Timestamp)
                    }
                    _ => panic!("Unsupported Arrow type: {:?}", field.data_type()),
                };

                DeltaStructField::new(field.name().clone(), delta_type, field.is_nullable())
            })
            .collect()
    }

    /// Create a new control table at the given path with initial pond metadata
    /// Returns an error if the table already exists (following tlogfs pattern)
    pub async fn create<P: AsRef<Path>>(
        path: P,
        pond_metadata: &PondMetadata,
    ) -> Result<Self, StewardError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!("Creating new control table at {}", path_str);

        // Ensure directory exists
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                StewardError::ControlTable(format!("Failed to create directory: {}", e))
            })?;
        }

        // Create table with ErrorIfExists mode (fail if already exists)
        let table = DeltaOps::try_from_uri(&path_str)
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to initialize table: {}", e)))?
            .create()
            .with_columns(Self::delta_schema())
            .with_partition_columns(vec!["record_category"]) // Partition like tlogfs uses directory partitions
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!(
                    "Failed to create table (already exists?): {}",
                    e
                ))
            })?;

        // Create SessionContext and register table ONCE
        let mut session_context = SessionContext::new();

        // Register JSON functions for querying JSON fields (cli_args, environment, factory_modes)
        datafusion_functions_json::register_all(&mut session_context).map_err(|e| {
            StewardError::ControlTable(format!("Failed to register JSON functions: {}", e))
        })?;

        let session_context = Arc::new(session_context);

        _ = session_context
            .register_table("transactions", Arc::new(table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let mut control_table = Self {
            path: path_str.clone(),
            table,
            session_context,
            pond_metadata: pond_metadata.clone(),
            factory_modes: HashMap::new(), // Start with empty factory modes
            settings: HashMap::new(),      // Start with empty settings
        };

        // Write an initial metadata record so the table isn't empty
        // Uses metadata partition and txn_seq=0 (reserved for metadata)
        // Real transactions start at txn_seq=1 in the transaction partition
        let initial_record = TransactionRecord {
            record_category: PARTITION_METADATA.into(),
            txn_seq: 0, // Reserved for metadata
            txn_id: uuid7::uuid7(),
            based_on_seq: None,
            record_type: RecordType::Begin,
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::Write,
            cli_args: serde_json::to_string(&vec!["create_pond".to_string()])
                .unwrap_or_else(|_| "[]".to_string()),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("pond_metadata".to_string()),
            config_path: None,
            pond_id: pond_metadata.pond_id,
            factory_modes: "{}".to_string(),
        };

        control_table.write_record(initial_record).await?;

        debug!(
            "Created control table for pond {} at {}",
            pond_metadata.pond_id, control_table.path
        );

        Ok(control_table)
    }

    /// Open an existing control table at the given path
    /// Returns an error if the table doesn't exist
    /// Loads pond metadata and factory modes from the table
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, StewardError> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        debug!("Opening existing control table at {}", path_str);

        let table = deltalake::open_table(&path_str).await.map_err(|e| {
            StewardError::ControlTable(format!(
                "Failed to open control table at {}: {}",
                path_str, e
            ))
        })?;

        // Create SessionContext and register table ONCE
        let mut session_context = SessionContext::new();

        // Register JSON functions for querying JSON fields (cli_args, environment, factory_modes)
        datafusion_functions_json::register_all(&mut session_context).map_err(|e| {
            StewardError::ControlTable(format!("Failed to register JSON functions: {}", e))
        })?;

        let session_context = Arc::new(session_context);

        _ = session_context
            .register_table("transactions", Arc::new(table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let mut control_table = Self {
            path: path_str,
            table,
            session_context,
            pond_metadata: PondMetadata::default(), // Placeholder, will load below
            factory_modes: HashMap::new(),
            settings: HashMap::new(),
        };

        // Load pond metadata from most recent record
        control_table.pond_metadata = control_table.load_pond_metadata().await?;

        // Load factory modes from most recent record
        control_table.factory_modes = control_table.load_factory_modes().await?;

        // Load settings from most recent record
        control_table.settings = control_table.load_settings().await?;

        debug!(
            "Opened control table for pond {} at {}",
            control_table.pond_metadata.pond_id, control_table.path
        );

        Ok(control_table)
    }

    /// Create or open a control table (convenience method)
    pub async fn open_or_create<P: AsRef<Path>>(
        path: P,
        create_new: bool,
        pond_metadata: Option<&PondMetadata>,
    ) -> Result<Self, StewardError> {
        if create_new {
            let metadata = pond_metadata.ok_or_else(|| {
                StewardError::ControlTable(
                    "pond_metadata is required when creating a new control table".to_string(),
                )
            })?;
            Self::create(path, metadata).await
        } else {
            Self::open(path).await
        }
    }

    /// Get access to the underlying Delta table for querying
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Get the shared SessionContext for querying control table
    /// Use this for all queries - no need to create new contexts
    #[must_use]
    pub fn session_context(&self) -> Arc<SessionContext> {
        Arc::clone(&self.session_context)
    }

    /// Get pond metadata (cached)
    #[must_use]
    pub fn pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Get factory modes (cached)
    #[must_use]
    pub fn factory_modes(&self) -> &HashMap<String, String> {
        &self.factory_modes
    }

    /// Load pond metadata from the most recent metadata record
    /// Queries the metadata partition for efficient lookup
    async fn load_pond_metadata(&self) -> Result<PondMetadata, StewardError> {
        let df = self
            .session_context
            .sql("SELECT pond_id FROM transactions WHERE record_category = 'metadata' ORDER BY timestamp DESC LIMIT 1")
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to query pond_id: {}", e))
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Err(StewardError::ControlTable(
                "No records found in control table".to_string(),
            ));
        }

        // Use serde_arrow to deserialize
        #[derive(Deserialize)]
        struct PondIdRow {
            pond_id: String,
        }

        let rows: Vec<PondIdRow> = serde_arrow::from_record_batch(&batches[0]).map_err(|e| {
            StewardError::ControlTable(format!("Failed to deserialize pond_id: {}", e))
        })?;

        let pond_id_str = rows
            .first()
            .ok_or_else(|| StewardError::ControlTable("No pond_id found".to_string()))?
            .pond_id
            .clone();

        let pond_id = FromStr::from_str(&pond_id_str).map_err(|e: uuid7::ParseError| {
            StewardError::ControlTable(format!("Invalid pond_id UUID: {}", e))
        })?;

        // For now, just return a PondMetadata with the pond_id
        // TODO: Store full birth metadata in a special record or derive from first transaction
        Ok(PondMetadata {
            pond_id,
            birth_timestamp: 0, // TODO: Load from first record
            birth_hostname: "unknown".to_string(),
            birth_username: "unknown".to_string(),
        })
    }

    /// Load factory modes from the most recent metadata record
    /// Queries the metadata partition for efficient lookup
    async fn load_factory_modes(&self) -> Result<HashMap<String, String>, StewardError> {
        let df = self
            .session_context
            .sql("SELECT factory_modes FROM transactions WHERE record_category = 'metadata' ORDER BY timestamp DESC LIMIT 1")
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to query factory_modes: {}", e))
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            // No records yet, return empty map
            return Ok(HashMap::new());
        }

        // Use serde_arrow to deserialize
        #[derive(Deserialize)]
        struct FactoryModesRow {
            factory_modes: String, // JSON string
        }

        let rows: Vec<FactoryModesRow> =
            serde_arrow::from_record_batch(&batches[0]).map_err(|e| {
                StewardError::ControlTable(format!("Failed to deserialize factory_modes: {}", e))
            })?;

        let json_str = rows
            .first()
            .map(|r| r.factory_modes.clone())
            .unwrap_or_else(|| "{}".to_string());

        // Parse JSON string to HashMap
        serde_json::from_str(&json_str).map_err(|e| {
            StewardError::ControlTable(format!("Failed to parse factory_modes JSON: {}", e))
        })
    }

    /// Load settings from control table
    async fn load_settings(&self) -> Result<HashMap<String, String>, StewardError> {
        let df = self
            .session_context
            .sql("SELECT environment FROM transactions WHERE record_category = 'metadata' AND factory_name = 'settings' ORDER BY timestamp DESC LIMIT 1")
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to query settings: {}", e))
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            // No settings yet, return empty map
            return Ok(HashMap::new());
        }

        // Use serde_arrow to deserialize
        #[derive(Deserialize)]
        struct SettingsRow {
            environment: String, // JSON string
        }

        let rows: Vec<SettingsRow> = serde_arrow::from_record_batch(&batches[0]).map_err(|e| {
            StewardError::ControlTable(format!("Failed to deserialize settings: {}", e))
        })?;

        let json_str = rows
            .first()
            .map(|r| r.environment.clone())
            .unwrap_or_else(|| "{}".to_string());

        // Parse JSON string to HashMap
        serde_json::from_str(&json_str).map_err(|e| {
            StewardError::ControlTable(format!("Failed to parse settings JSON: {}", e))
        })
    }

    /// Write a transaction record to the control table using serde_arrow
    async fn write_record(&mut self, record: TransactionRecord) -> Result<(), StewardError> {
        // Convert struct to Arrow RecordBatch using serde_arrow
        let schema = Self::arrow_schema();
        let arrays = serde_arrow::to_arrow(schema.fields(), &[record]).map_err(|e| {
            StewardError::ControlTable(format!("Failed to convert to arrow: {}", e))
        })?;

        let batch = arrow_array::RecordBatch::try_new(schema, arrays).map_err(|e| {
            StewardError::ControlTable(format!("Failed to create record batch: {}", e))
        })?;

        // Write to Delta Lake
        let old_version = self.table.version();
        let table = DeltaOps(self.table.clone())
            .write(vec![batch])
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to write to Delta Lake: {}", e))
            })?;

        let new_version = table.version();
        debug!(
            "Control table write: version {:?} -> {:?}",
            old_version, new_version
        );

        // Update cached table reference
        self.table = table;

        // Re-register table with SessionContext to see new version
        _ = self
            .session_context
            .deregister_table("transactions")
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to deregister table: {}", e))
            })?;

        _ = self
            .session_context
            .register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to re-register table: {}", e))
            })?;

        Ok(())
    }

    /// Record the beginning of a transaction
    pub async fn record_begin(
        &mut self,
        txn_meta: &PondTxnMetadata,
        based_on_seq: Option<i64>,
        transaction_type: TransactionType,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_begin(
            txn_meta,
            based_on_seq,
            transaction_type,
            self.pond_metadata.pond_id,
            &self.factory_modes,
        );
        self.write_record(record).await
    }

    /// Record successful data filesystem commit
    pub async fn record_data_committed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        data_fs_version: i64,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_data_committed(
            txn_meta,
            transaction_type,
            data_fs_version,
            duration_ms,
            self.pond_metadata.pond_id,
            &self.factory_modes,
        );
        self.write_record(record).await
    }

    /// Record transaction failure
    pub async fn record_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_failed(
            txn_meta,
            transaction_type,
            error_message,
            duration_ms,
            self.pond_metadata.pond_id,
            &self.factory_modes,
        );
        self.write_record(record).await
    }

    /// Record completed read transaction
    pub async fn record_completed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_completed(
            txn_meta,
            transaction_type,
            duration_ms,
            self.pond_metadata.pond_id,
            &self.factory_modes,
        );
        self.write_record(record).await
    }

    /// Set factory execution mode and update cache
    pub async fn set_factory_mode(
        &mut self,
        factory_name: &str,
        mode: &str,
    ) -> Result<(), StewardError> {
        // Update cached factory_modes
        _ = self
            .factory_modes
            .insert(factory_name.to_string(), mode.to_string());

        // Write a special record to persist the change in the metadata partition
        // This makes it easy to extract the latest configuration via partition pruning
        let record = TransactionRecord {
            record_category: PARTITION_METADATA.into(),
            txn_seq: 0, // Reserved for configuration records
            txn_id: uuid7::uuid7(),
            based_on_seq: None,
            record_type: RecordType::Begin, // Arbitrary, not used for config records
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::Write, // Arbitrary
            cli_args: serde_json::to_string(&vec![
                "set_factory_mode".to_string(),
                factory_name.to_string(),
                mode.to_string(),
            ])
            .unwrap_or_else(|_| "[]".to_string()),
            environment: "{}".to_string(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some(factory_name.to_string()),
            config_path: Some(mode.to_string()),
            pond_id: self.pond_metadata.pond_id,
            factory_modes: serde_json::to_string(&self.factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        };

        self.write_record(record).await?;
        debug!("Set factory mode for '{}' to: {}", factory_name, mode);
        Ok(())
    }

    /// Get factory execution mode (from cache)
    #[must_use]
    pub fn get_factory_mode(&self, factory_name: &str) -> Option<String> {
        self.factory_modes.get(factory_name).cloned()
    }

    /// Set a control table setting (persisted configuration)
    pub async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), StewardError> {
        // Update cached settings
        _ = self.settings.insert(key.to_string(), value.to_string());

        // Write a special record to persist the change in the metadata partition
        // Use factory_name="settings" to distinguish from factory_mode records
        let record = TransactionRecord {
            record_category: PARTITION_METADATA.into(),
            txn_seq: 0, // Reserved for configuration records
            txn_id: uuid7::uuid7(),
            based_on_seq: None,
            record_type: RecordType::Begin, // Arbitrary, not used for config records
            timestamp: Utc::now().timestamp_micros(),
            transaction_type: TransactionType::Write, // Arbitrary
            cli_args: serde_json::to_string(&vec![
                "set_setting".to_string(),
                key.to_string(),
                value.to_string(),
            ])
            .unwrap_or_else(|_| "[]".to_string()),
            environment: serde_json::to_string(&self.settings).unwrap_or_else(|_| "{}".to_string()),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("settings".to_string()), // Special marker for settings
            config_path: Some(key.to_string()), // Store key in config_path for easy querying
            pond_id: self.pond_metadata.pond_id,
            factory_modes: serde_json::to_string(&self.factory_modes)
                .unwrap_or_else(|_| "{}".to_string()),
        };

        self.write_record(record).await?;
        debug!("Set setting '{}' to: {}", key, value);
        Ok(())
    }

    /// Get a control table setting (from cache)
    #[must_use]
    pub fn get_setting(&self, key: &str) -> Option<String> {
        self.settings.get(key).cloned()
    }

    /// Get all settings (from cache)
    #[must_use]
    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    /// Query the last write transaction sequence number
    pub async fn get_last_write_sequence(&self) -> Result<i64, StewardError> {
        let df = self
            .session_context
            .sql(
                "SELECT MAX(txn_seq) as max_seq FROM transactions 
                 WHERE transaction_type = 'write' AND txn_seq > 0",
            )
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to query max sequence: {}", e))
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0);
        }

        #[derive(Deserialize)]
        struct MaxSeqRow {
            max_seq: Option<i64>,
        }

        let rows: Vec<MaxSeqRow> = serde_arrow::from_record_batch(&batches[0]).map_err(|e| {
            StewardError::ControlTable(format!("Failed to deserialize max_seq: {}", e))
        })?;

        Ok(rows.first().and_then(|r| r.max_seq).unwrap_or(0))
    }

    /// Print pond banner showing metadata
    #[allow(clippy::print_stdout)]
    pub fn print_banner(&self) {
        println!();
        pond_metadata_banner(&self.pond_metadata);
        println!();
    }

    /// Set pond metadata in control table (writes metadata records with txn_seq = 0)
    pub async fn set_pond_metadata(&mut self, metadata: &PondMetadata) -> Result<(), StewardError> {
        // Just update the cached copy - we don't write metadata records to the control table
        // since it's initialized once at pond creation
        self.pond_metadata = metadata.clone();
        Ok(())
    }

    /// Get pond metadata (from cache)
    #[must_use]
    pub fn get_pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Record post-commit task pending execution
    pub async fn record_post_commit_pending(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        factory_name: String,
        config_path: String,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_post_commit_pending(
            txn_meta,
            execution_seq,
            factory_name,
            config_path,
            &self.pond_metadata,
            &self.factory_modes,
        )?;

        self.write_record(record).await?;
        debug!(
            "Recorded post-commit pending for txn_seq={}, execution_seq={}",
            txn_meta.txn_seq, execution_seq
        );
        Ok(())
    }

    /// Record post-commit task execution started
    pub async fn record_post_commit_started(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_post_commit_started(
            txn_meta,
            execution_seq,
            &self.pond_metadata,
            &self.factory_modes,
        )?;

        self.write_record(record).await?;
        debug!(
            "Recorded post-commit started for txn_seq={}, execution_seq={}",
            txn_meta.txn_seq, execution_seq
        );
        Ok(())
    }

    /// Record post-commit task completion
    pub async fn record_post_commit_completed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_post_commit_completed(
            txn_meta,
            execution_seq,
            duration_ms,
            &self.pond_metadata,
            &self.factory_modes,
        )?;

        self.write_record(record).await?;
        debug!(
            "Recorded post-commit completed for txn_seq={}, execution_seq={}",
            txn_meta.txn_seq, execution_seq
        );
        Ok(())
    }

    /// Record post-commit task failure
    pub async fn record_post_commit_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let record = TransactionRecord::new_post_commit_failed(
            txn_meta,
            execution_seq,
            error_message,
            duration_ms,
            &self.pond_metadata,
            &self.factory_modes,
        )?;

        self.write_record(record).await?;
        debug!(
            "Recorded post-commit failed for txn_seq={}, execution_seq={}",
            txn_meta.txn_seq, execution_seq
        );
        Ok(())
    }

    /// Find incomplete transactions (for recovery)
    pub async fn find_incomplete_transactions(
        &self,
    ) -> Result<Vec<(PondTxnMetadata, i64)>, StewardError> {
        // Query for transactions that have begun but not finished
        // A transaction is complete if it has either:
        // - "data_committed" (successful write)
        // - "completed" (successful read or no-op write)
        // - "failed" (explicit failure)
        let df = self
            .session_context
            .sql(
                "WITH txn_states AS (
                    SELECT 
                        txn_seq,
                        txn_id,
                        MAX(CASE WHEN record_type = 'begin' THEN 1 ELSE 0 END) as has_begin,
                        MAX(CASE WHEN record_type = 'data_committed' THEN 1 ELSE 0 END) as has_data_committed,
                        MAX(CASE WHEN record_type = 'completed' THEN 1 ELSE 0 END) as has_completed,
                        MAX(CASE WHEN record_type = 'failed' THEN 1 ELSE 0 END) as has_failed,
                        MAX(CASE WHEN record_type = 'data_committed' THEN data_fs_version ELSE 0 END) as data_fs_version,
                        MAX(CASE WHEN record_type = 'begin' THEN cli_args ELSE '[]' END) as cli_args,
                        MAX(CASE WHEN record_type = 'begin' THEN environment ELSE '{}' END) as environment
                    FROM transactions
                    WHERE txn_seq > 0  -- Exclude metadata records
                    GROUP BY txn_seq, txn_id
                )
                SELECT txn_seq, txn_id, data_fs_version, cli_args, environment
                FROM txn_states
                WHERE has_begin = 1 
                  AND has_data_committed = 0 
                  AND has_completed = 0 
                  AND has_failed = 0
                ORDER BY txn_seq",
            )
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to query incomplete transactions: {}", e))
            })?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct IncompleteRow {
            txn_seq: i64,
            txn_id: String,
            data_fs_version: i64,
            cli_args: String,
            environment: String,
        }

        let mut results = Vec::new();
        for batch in batches {
            let rows: Vec<IncompleteRow> = serde_arrow::from_record_batch(&batch).map_err(|e| {
                StewardError::ControlTable(format!(
                    "Failed to deserialize incomplete transactions: {}",
                    e
                ))
            })?;

            for row in rows {
                // Create minimal PondTxnMetadata for recovery
                // Note: This doesn't have the full user metadata, just enough for recovery tracking
                let txn_id = Uuid::from_str(&row.txn_id).map_err(|e| {
                    StewardError::ControlTable(format!("Invalid UUID in control table: {}", e))
                })?;

                // Parse CLI args from JSON
                let args: Vec<String> = serde_json::from_str(&row.cli_args)
                    .unwrap_or_else(|_| vec!["recovery".to_string()]);

                // Parse environment from JSON
                let vars: HashMap<String, String> =
                    serde_json::from_str(&row.environment).unwrap_or_default();

                let user_meta = tlogfs::PondUserMetadata { txn_id, args, vars };
                let txn_meta = PondTxnMetadata::new(row.txn_seq, user_meta);
                results.push((txn_meta, row.data_fs_version));
            }
        }

        Ok(results)
    }
}

/// Format pond metadata as a banner for display
#[allow(clippy::print_stdout)]
pub fn pond_metadata_banner(data: &PondMetadata) {
    let datetime = DateTime::from_timestamp(
        data.birth_timestamp / 1_000_000,
        ((data.birth_timestamp % 1_000_000) * 1000) as u32,
    )
    .unwrap_or_else(Utc::now);

    let created_str = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let left = vec![
        format!("Pond {}", data.pond_id),
        format!("Created {}", created_str),
    ];

    let right = vec![data.birth_username.clone(), data.birth_hostname.clone()];

    println!(
        "{}",
        utilities::banner::format_banner_from_iters(None, left, right)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_control_table() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let control_path = temp_dir.path().join("control");

        let pond_metadata = PondMetadata::default();
        let table = ControlTable::create(&control_path, &pond_metadata)
            .await
            .expect("Failed to create control table");

        // Verify table exists
        assert!(control_path.exists());

        // Verify pond_id is cached
        assert_eq!(table.pond_metadata().pond_id, pond_metadata.pond_id);

        // Verify initial state
        let last_seq = table
            .get_last_write_sequence()
            .await
            .expect("Failed to get last sequence");
        assert_eq!(last_seq, 0, "Initial sequence should be 0");
    }

    #[tokio::test]
    async fn test_open_existing_control_table() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let control_path = temp_dir.path().join("control");

        // Create table
        let pond_metadata = PondMetadata::default();
        let original_pond_id = pond_metadata.pond_id;

        let _table1 = ControlTable::create(&control_path, &pond_metadata)
            .await
            .expect("Failed to create control table");

        // Open existing table
        let table2 = ControlTable::open(&control_path)
            .await
            .expect("Failed to open existing control table");

        // Verify pond_id is preserved
        assert_eq!(table2.pond_metadata().pond_id, original_pond_id);
    }

    #[tokio::test]
    async fn test_factory_modes() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let control_path = temp_dir.path().join("control");

        let pond_metadata = PondMetadata::default();
        let mut table = ControlTable::create(&control_path, &pond_metadata)
            .await
            .expect("Failed to create control table");

        // Set factory mode
        table
            .set_factory_mode("remote", "push")
            .await
            .expect("Failed to set factory mode");

        // Verify from cache
        assert_eq!(table.get_factory_mode("remote"), Some("push".to_string()));

        // Reopen and verify persistence
        let table2 = ControlTable::open(&control_path)
            .await
            .expect("Failed to reopen");
        assert_eq!(table2.get_factory_mode("remote"), Some("push".to_string()));
    }

    #[tokio::test]
    async fn test_json_query_functions() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let control_path = temp_dir.path().join("control");

        let pond_metadata = PondMetadata::default();
        let mut table = ControlTable::create(&control_path, &pond_metadata)
            .await
            .expect("Failed to create control table");

        // Set a factory mode to have data to query
        table
            .set_factory_mode("remote", "push")
            .await
            .expect("Failed to set factory mode");

        // Test JSON query using datafusion-functions-json
        // Query factory_modes using json_get_str function
        let df = table
            .session_context()
            .sql("SELECT json_get_str(factory_modes, 'remote') as remote_mode FROM transactions WHERE txn_seq = 0 LIMIT 1")
            .await
            .expect("Failed to execute SQL");

        let batches = df.collect().await.expect("Failed to collect results");
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);

        // Verify we can deserialize the result
        #[derive(Deserialize)]
        struct RemoteModeRow {
            remote_mode: String,
        }

        let rows: Vec<RemoteModeRow> =
            serde_arrow::from_record_batch(&batches[0]).expect("Failed to deserialize");

        assert_eq!(rows[0].remote_mode, "push");
    }
}
