//! Control Table - DeltaLake-based transaction tracking for Steward
//!
//! Replaces the control filesystem TLogFS instance with a direct DeltaLake table
//! that tracks transaction lifecycle, sequences, and enables future replication.

use crate::{StewardError, PondTxnMetadata, PondMetadata};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use deltalake::DeltaTable;
use deltalake::operations::DeltaOps;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use uuid7::Uuid;

/// Transaction record for control table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub txn_seq: i64,
    pub txn_id: String,
    pub based_on_seq: Option<i64>, // @@@ WHY THIS?
    pub record_type: String, // "begin" | "data_committed" | "failed" | "completed" | "post_commit_pending" | "post_commit_started" | "post_commit_completed" | "post_commit_failed"
    pub timestamp: i64,      // Microseconds since epoch (for DeltaLake compatibility)
    pub transaction_type: String, // "read" | "write" | "post_commit"
    pub cli_args: Vec<String>,
    pub environment: HashMap<String, String>,
    pub data_fs_version: Option<i64>,
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,

    // Post-commit task tracking (NULL for regular transactions)
    pub parent_txn_seq: Option<i64>, // Pond transaction that triggered this post-commit task
    pub execution_seq: Option<i32>,  // Ordinal in post-commit factory list (1, 2, 3...)
    pub factory_name: Option<String>, // Factory that executed (for post-commit tasks)
    pub config_path: Option<String>, // Path to config file (e.g., /etc/system.d/10-validate)
}

/// Control table for tracking transaction lifecycle and sequencing
pub struct ControlTable {
    /// Path to the Delta Lake table
    #[allow(dead_code)]
    path: String,
    /// The Delta Lake table instance
    table: DeltaTable,
    /// Shared SessionContext with control table registered
    /// Following tlogfs pattern: register table once, reuse context for all queries
    /// DataFusion will automatically see new Delta commits when querying
    session_context: Arc<SessionContext>,
}

impl ControlTable {
    /// Create a new control table or open an existing one
    pub async fn new(path: &str) -> Result<Self, StewardError> {
        debug!("Initializing control table at {}", path);

        // Try to open existing table first
        match deltalake::open_table(path).await {
            Ok(table) => {
                debug!("Opened existing control table at {}", path);

                // Create SessionContext and register control table (following tlogfs pattern)
                let session_context = Arc::new(SessionContext::new());
                session_context
                    .register_table("transactions", Arc::new(table.clone()))
                    .map_err(|e| {
                        StewardError::ControlTable(format!(
                            "Failed to register control table: {}",
                            e
                        ))
                    })?;

                Ok(Self {
                    path: path.to_string(),
                    table,
                    session_context,
                })
            }
            Err(_) => {
		// @@@ Not sure! This create-new-or-open appears to be
		// creating junk when the environment is incorrect?
		
                // Table doesn't exist or path doesn't exist, create it
                debug!("Creating new control table at {}", path);

                // Ensure the directory exists
                std::fs::create_dir_all(path).map_err(|e| {
                    StewardError::ControlTable(format!("Failed to create directory: {}", e))
                })?;

                // Create table by writing an empty batch with the correct schema
                let schema = Self::arrow_schema();
                let empty_batch = RecordBatch::new_empty(schema);

                let table = DeltaOps::try_from_uri(path)
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to initialize table: {}", e))
                    })?
                    .write(vec![empty_batch])
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to create table: {}", e))
                    })?;

                // Create SessionContext and register control table (following tlogfs pattern)
                let session_context = Arc::new(SessionContext::new());
                session_context
                    .register_table("transactions", Arc::new(table.clone()))
                    .map_err(|e| {
                        StewardError::ControlTable(format!(
                            "Failed to register control table: {}",
                            e
                        ))
                    })?;

		let mut table = Self {
                    path: path.to_string(),
                    table,
                    session_context,
                };

		let pond_metadata = PondMetadata::new();

		table.set_pond_metadata(&pond_metadata).await?;

                Ok(table)
            }
        }
    }

    /// Get the Arrow schema for the control table
    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            // Primary transaction identifiers
            Field::new("txn_seq", DataType::Int64, false),
            Field::new("txn_id", DataType::Utf8, false),
            Field::new("based_on_seq", DataType::Int64, true),
            // Transaction lifecycle
            Field::new("record_type", DataType::Utf8, false), // "begin" | "data_committed" | "failed" | "completed" | "post_commit_pending" | "post_commit_started" | "post_commit_completed" | "post_commit_failed"
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("transaction_type", DataType::Utf8, false), // "read" | "write" | "post_commit"
            // Context capture
            Field::new(
                "cli_args",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new(
                "environment",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            // Data filesystem linkage
            Field::new("data_fs_version", DataType::Int64, true),
            // Error tracking
            Field::new("error_message", DataType::Utf8, true),
            Field::new("duration_ms", DataType::Int64, true),
            // Post-commit task tracking (NULL for regular transactions)
            Field::new("parent_txn_seq", DataType::Int64, true), // Pond txn that triggered this task
            Field::new("execution_seq", DataType::Int32, true),  // Ordinal: 1, 2, 3...
            Field::new("factory_name", DataType::Utf8, true),    // Factory name
            Field::new("config_path", DataType::Utf8, true),     // Config file path
        ]))
    }

    /// Get access to the underlying Delta table for querying
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Get the shared SessionContext for querying control table
    /// Following tlogfs pattern: use single SessionContext, DataFusion sees new commits automatically
    pub fn session_context(&self) -> Arc<SessionContext> {
        Arc::clone(&self.session_context)
    }

    /// Set factory execution mode for a specific factory
    /// This determines the default command/mode when the factory executes
    ///
    /// For remote factory:
    /// - "push": Run post-commit to push backups (source pond)
    /// - "pull": Only run on manual sync to pull updates (replica pond)
    ///
    /// Future factories may have different mode values
    pub async fn set_factory_mode(
        &mut self,
        factory_name: &str,
        mode: &str,
    ) -> Result<(), StewardError> {
        // Store as a special transaction record with txn_seq = 0 (reserved for settings)
        let setting_key = format!("factory_mode:{}", factory_name);

        let records = vec![TransactionRecord {
            txn_seq: 0, // Reserved for pond settings
            txn_id: "pond-setting".to_string(),
            based_on_seq: None,
            record_type: "setting".to_string(),
            timestamp: chrono::Utc::now().timestamp_micros(),
            transaction_type: "setting".to_string(),
            cli_args: vec![setting_key.clone(), mode.to_string()],
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some(setting_key),
            config_path: Some(mode.to_string()),
        }];

        self.write_record(records[0].clone()).await?;
        debug!("Set factory mode for '{}' to: {}", factory_name, mode);
        Ok(())
    }

    /// Get factory execution mode for a specific factory
    /// Returns "push" by default for remote factory if not set
    pub async fn get_factory_mode(&self, factory_name: &str) -> Result<String, StewardError> {
        let setting_key = format!("factory_mode:{}", factory_name);

        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let sql = format!(
            r#"
            SELECT config_path 
            FROM transactions 
            WHERE txn_seq = 0 
              AND factory_name = '{}'
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
            setting_key.replace("'", "''") // SQL escape
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to query settings: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                if let Some(col) = batch.column_by_name("config_path") {
                    if let Some(array) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                        if !array.is_null(0) {
                            return Ok(array.value(0).to_string());
                        }
                    }
                }
            }
        }

        // Default to "push" for remote factory if not set
        if factory_name == "remote" {
            Ok("push".to_string())
        } else {
            Ok("default".to_string())
        }
    }

    /// Get all factory modes from master control record
    /// Returns a HashMap of factory_name -> mode
    pub async fn get_all_factory_modes(
        &self,
    ) -> Result<std::collections::HashMap<String, String>, StewardError> {
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let sql = r#"
            SELECT 
                SUBSTRING(factory_name, 14) as name,  -- Strip "factory_mode:" prefix
                config_path as mode
            FROM transactions 
            WHERE txn_seq = 0 
              AND factory_name LIKE 'factory_mode:%'
            ORDER BY timestamp DESC
        "#;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to query settings: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        debug!("get_all_factory_modes: Got {} batches", batches.len());

        let mut modes = std::collections::HashMap::new();

        for batch in batches {
            debug!("  Batch has {} rows", batch.num_rows());
            if let (Some(name_col), Some(mode_col)) =
                (batch.column_by_name("name"), batch.column_by_name("mode"))
            {
                debug!("    Found name and mode columns");
                debug!("    name column type: {:?}", name_col.data_type());
                debug!("    mode column type: {:?}", mode_col.data_type());

                // Extract names - handle both Utf8 and Utf8View
                let names: Vec<Option<String>> = if let Some(array) =
                    name_col.as_any().downcast_ref::<arrow_array::StringArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else if let Some(array) = name_col
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else {
                    debug!("    Failed to cast name column");
                    continue;
                };

                // Extract modes - handle both Utf8 and Utf8View
                let modes_vec: Vec<Option<String>> = if let Some(array) =
                    mode_col.as_any().downcast_ref::<arrow_array::StringArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else if let Some(array) = mode_col
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else {
                    debug!("    Failed to cast mode column");
                    continue;
                };

                debug!("    Successfully extracted {} rows", names.len());
                for i in 0..names.len() {
                    if let (Some(name), Some(mode)) = (&names[i], &modes_vec[i]) {
                        debug!("    Row {}: name='{}', mode='{}'", i, name, mode);
                        modes.entry(name.clone()).or_insert(mode.clone());
                    } else {
                        debug!("    Row {}: NULL values", i);
                    }
                }
            } else {
                debug!("    name or mode column not found");
            }
        }

        debug!(
            "get_all_factory_modes: Returning {} modes: {:?}",
            modes.len(),
            modes
        );
        Ok(modes)
    }

    /// Set pond identity metadata (called once during pond creation)
    /// This metadata is immutable and should be preserved across replicas
    pub async fn set_pond_metadata(&mut self, metadata: &PondMetadata) -> Result<(), StewardError> {
        // Store each field as a separate setting record with txn_seq = 0
        let timestamp = chrono::Utc::now().timestamp_micros();

        // Store pond_id
        self.write_record(TransactionRecord {
            txn_seq: 0,
            txn_id: "pond-metadata".to_string(),
            based_on_seq: None,
            record_type: "metadata".to_string(),
            timestamp,
            transaction_type: "metadata".to_string(),
            cli_args: vec!["pond_id".to_string(), metadata.pond_id.to_string()],
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("pond_id".to_string()),
            config_path: Some(metadata.pond_id.to_string()),
        })
        .await?;

        // Store birth_timestamp
        self.write_record(TransactionRecord {
            txn_seq: 0,
            txn_id: "pond-metadata".to_string(),
            based_on_seq: None,
            record_type: "metadata".to_string(),
            timestamp,
            transaction_type: "metadata".to_string(),
            cli_args: vec![
                "birth_timestamp".to_string(),
                metadata.birth_timestamp.to_string(),
            ],
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("birth_timestamp".to_string()),
            config_path: Some(metadata.birth_timestamp.to_string()),
        })
        .await?;

        // Store birth_hostname
        self.write_record(TransactionRecord {
            txn_seq: 0,
            txn_id: "pond-metadata".to_string(),
            based_on_seq: None,
            record_type: "metadata".to_string(),
            timestamp,
            transaction_type: "metadata".to_string(),
            cli_args: vec![
                "birth_hostname".to_string(),
                metadata.birth_hostname.clone(),
            ],
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("birth_hostname".to_string()),
            config_path: Some(metadata.birth_hostname.clone()),
        })
        .await?;

        // Store birth_username
        self.write_record(TransactionRecord {
            txn_seq: 0,
            txn_id: "pond-metadata".to_string(),
            based_on_seq: None,
            record_type: "metadata".to_string(),
            timestamp,
            transaction_type: "metadata".to_string(),
            cli_args: vec![
                "birth_username".to_string(),
                metadata.birth_username.clone(),
            ],
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: Some("birth_username".to_string()),
            config_path: Some(metadata.birth_username.clone()),
        })
        .await?;

        debug!(
            "Set pond metadata: id={}, birth={}, host={}, user={}",
            metadata.pond_id,
            metadata.birth_timestamp,
            metadata.birth_hostname,
            metadata.birth_username
        );
        Ok(())
    }

    pub async fn print_banner(&self) -> Result<(), StewardError> {
	let pond_metadata = self.get_pond_metadata().await?;
        println!();
        pond_metadata_banner(&pond_metadata);
        println!();
	Ok(())
    }
    
    /// Get pond identity metadata from the control table
    /// Returns None if metadata has not been set (legacy ponds)
    pub async fn get_pond_metadata(&self) -> Result<PondMetadata, StewardError> {
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        // Query for all metadata settings
        let sql = r#"
            SELECT factory_name, config_path
            FROM transactions
            WHERE txn_seq = 0
              AND factory_name IN ('pond_id', 'birth_timestamp', 'birth_hostname', 'birth_username')
            ORDER BY timestamp DESC
        "#;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to query metadata: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        let mut metadata_map: HashMap<String, String> = HashMap::new();

        for batch in batches {
            if let (Some(name_col), Some(value_col)) = (
                batch.column_by_name("factory_name"),
                batch.column_by_name("config_path"),
            ) {
                // Handle both Utf8 and Utf8View for column types
                let names: Vec<Option<String>> = if let Some(array) =
                    name_col.as_any().downcast_ref::<arrow_array::StringArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else if let Some(array) = name_col
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else {
                    continue;
                };

                let values: Vec<Option<String>> = if let Some(array) = value_col
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>(
                ) {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else if let Some(array) = value_col
                    .as_any()
                    .downcast_ref::<arrow_array::StringViewArray>()
                {
                    (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i).to_string())
                            }
                        })
                        .collect()
                } else {
                    continue;
                };

                for i in 0..names.len() {
                    if let (Some(name), Some(value)) = (&names[i], &values[i]) {
                        metadata_map.entry(name.clone()).or_insert(value.clone());
                    }
                }
            }
        }

        // Check if we have all required fields
        if let (
            Some(pond_id),
            Some(birth_timestamp_str),
            Some(birth_hostname),
            Some(birth_username),
        ) = (
            metadata_map.get("pond_id"),
            metadata_map.get("birth_timestamp"),
            metadata_map.get("birth_hostname"),
            metadata_map.get("birth_username"),
        ) {
            let birth_timestamp = birth_timestamp_str.parse::<i64>().map_err(|e| {
                StewardError::ControlTable(format!("Invalid birth_timestamp: {}", e))
            })?;

            Ok(PondMetadata {
                pond_id: Uuid::from_str(pond_id)?,
                birth_timestamp,
                birth_hostname: birth_hostname.clone(),
                birth_username: birth_username.clone(),
            })
        } else {
            Err(StewardError::ControlTable(format!("Missing metadata: {:?}", metadata_map)))
        }
    }

    /// Query the last write transaction sequence number
    /// Returns 0 if no write transactions exist yet
    async fn get_last_write_sequence(&self) -> Result<i64, crate::StewardError> {
        // Use DataFusion to query - DeltaTable implements TableProvider
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| {
                crate::StewardError::ControlTable(format!("Failed to register table: {}", e))
            })?;

        let df = ctx
            .sql(
                "SELECT MAX(txn_seq) as max_seq FROM transactions WHERE transaction_type = 'write'",
            )
            .await
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to query: {}", e)))?;

        let batches = df.collect().await.map_err(|e| {
            crate::StewardError::ControlTable(format!("Failed to collect results: {}", e))
        })?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0); // No transactions yet
        }

        let batch = &batches[0];
        let max_seq_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                crate::StewardError::ControlTable("Failed to downcast max_seq".to_string())
            })?;

        if max_seq_array.is_null(0) {
            Ok(0) // No write transactions yet
        } else {
            Ok(max_seq_array.value(0))
        }
    }

    /// Record the beginning of a transaction
    /// @@@ Cleanup
    pub async fn record_begin(
        &mut self,
	meta: &PondTxnMetadata,
        based_on_seq: Option<i64>,
        transaction_type: &str,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            record_type: "begin".to_string(),
            transaction_type: transaction_type.to_string(),
            txn_seq: meta.txn_seq,
            txn_id: meta.user.txn_id.to_string(),
            cli_args: meta.user.args.clone(),
            environment: meta.user.vars.clone(),
            based_on_seq,
            timestamp,
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record successful data filesystem commit
    pub async fn record_data_committed(
        &mut self,
	txn_meta: &PondTxnMetadata,
        transaction_type: &str,
        data_fs_version: i64,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id.to_string(),
            based_on_seq: None,
            record_type: "data_committed".to_string(),
            timestamp,
            transaction_type: transaction_type.to_string(), // Must match begin record for SQL queries
            cli_args: txn_meta.user.args.clone(),
            environment: HashMap::new(), // Avoid repeating
            data_fs_version: Some(data_fs_version),
            error_message: None,
            duration_ms: Some(duration_ms),
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record transaction failure
    pub async fn record_failed(
        &mut self,
	txn_meta: &PondTxnMetadata,
        transaction_type: &str,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id.to_string(),
            cli_args: txn_meta.user.args.clone(),
            environment: HashMap::new(), // Do not repeat
            based_on_seq: None,
            record_type: "failed".to_string(),
            timestamp,
            transaction_type: transaction_type.to_string(), // Must match begin record for SQL queries
            data_fs_version: None,
            error_message: Some(error_message),
            duration_ms: Some(duration_ms),
            // Post-commit fields (NULL for regular transactions)
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record completed read transaction
    pub async fn record_completed(
        &mut self,
	txn_meta: &PondTxnMetadata,
        transaction_type: &str,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id.to_string(),
            cli_args: txn_meta.user.args.clone(),
            environment: HashMap::new(), // Do not repeat
            based_on_seq: None,
            record_type: "completed".to_string(),
            timestamp,
            transaction_type: transaction_type.to_string(), // Must match begin record for SQL queries
            data_fs_version: None,
            error_message: None,
            duration_ms: Some(duration_ms),
            // Post-commit fields (NULL for regular transactions)
            parent_txn_seq: None,
            execution_seq: None,
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record pending post-commit task (before execution)
    /// parent_txn_seq is the POND transaction sequence that triggered this task
    /// execution_seq is the ordinal in the factory list (1, 2, 3...)
    pub async fn record_post_commit_pending(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i32,
        factory_name: String,
        config_path: String,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();
	
        let record = TransactionRecord {
            txn_seq: 0, // Will be assigned by Delta Lake append

            txn_id: format!("post-commit-{}-{}", txn_meta.txn_seq, execution_seq),
            based_on_seq: Some(txn_meta.txn_seq), // Based on the pond transaction
            record_type: "post_commit_pending".to_string(),
            timestamp,
            transaction_type: "post_commit".to_string(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq),
            factory_name: Some(factory_name),
            config_path: Some(config_path),
        };

        self.write_record(record).await
    }

    /// Record post-commit execution start
    pub async fn record_post_commit_started(
        &mut self,
	txn_meta: &PondTxnMetadata,
        execution_seq: i32,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: 0,
            txn_id: format!("post-commit-{}-{}", txn_meta.txn_seq, execution_seq),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: "post_commit_started".to_string(),
            timestamp,
            transaction_type: "post_commit".to_string(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq),
            factory_name: None, // Not needed for started/completed/failed records
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record post-commit execution success
    pub async fn record_post_commit_completed(
        &mut self,
	txn_meta: &PondTxnMetadata,
        execution_seq: i32,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: 0,
            txn_id: format!("post-commit-{}-{}", txn_meta.txn_seq, execution_seq),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: "post_commit_completed".to_string(),
            timestamp,
            transaction_type: "post_commit".to_string(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: Some(duration_ms),
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Record post-commit execution failure
    pub async fn record_post_commit_failed(
        &mut self,
	txn_meta: &PondTxnMetadata,
        execution_seq: i32,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq: 0,
            txn_id: format!("post-commit-{}-{}", txn_meta.txn_seq, execution_seq),
            based_on_seq: Some(txn_meta.txn_seq),
            record_type: "post_commit_failed".to_string(),
            timestamp,
            transaction_type: "post_commit".to_string(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: Some(error_message),
            duration_ms: Some(duration_ms),
            parent_txn_seq: Some(txn_meta.txn_seq),
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
        };

        self.write_record(record).await
    }

    /// Write a transaction record to the control table using serde_arrow
    async fn write_record(&mut self, record: TransactionRecord) -> Result<(), crate::StewardError> {
        // Convert struct to Arrow RecordBatch using serde_arrow
        let record_type = record.record_type.clone(); // Clone before moving
        let records = vec![record];
        let schema = Self::arrow_schema();
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).map_err(|e| {
            crate::StewardError::ControlTable(format!("Failed to convert to arrow: {}", e))
        })?;

        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
            crate::StewardError::ControlTable(format!("Failed to create record batch: {}", e))
        })?;

        // Write to Delta Lake
        let old_version = self.table.version();
        let table = DeltaOps(self.table.clone())
            .write(vec![batch])
            .await
            .map_err(|e| {
                crate::StewardError::ControlTable(format!("Failed to write to Delta Lake: {}", e))
            })?;

        // Update our cached table reference
        let new_version = table.version();
        log::debug!(
            "Control table write: version {:?} -> {:?}, record_type={}",
            old_version,
            new_version,
            record_type
        );
        self.table = table;

        // Re-register table with SessionContext to see new version (following tlogfs pattern)
        // DataFusion/Delta will automatically read the latest _delta_log when querying
        self.session_context
            .deregister_table("transactions")
            .map_err(|e| {
                crate::StewardError::ControlTable(format!("Failed to deregister table: {}", e))
            })?;

        self.session_context
            .register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| {
                crate::StewardError::ControlTable(format!("Failed to re-register table: {}", e))
            })?;

        Ok(())
    }

    /// Reload the table to see latest commits
    /// Call this before querying if other processes may have written to the control table
    pub async fn reload(&mut self) -> Result<(), StewardError> {
        self.table = deltalake::open_table(&self.path)
            .await
            .map_err(|e| StewardError::ControlTable(format!("Failed to reload table: {}", e)))?;
        Ok(())
    }

    /// Find incomplete write transactions for recovery
    ///
    /// Returns transactions that are incomplete:
    /// - Have begin records but no data_committed/completed/failed (crashed during transaction)
    ///
    /// Note: Write transactions that complete successfully have "begin" + "data_committed"
    /// Read transactions that complete successfully have "begin" + "completed"
    /// Failed transactions have "begin" + "failed"
    ///
    /// Returns (PondTxnMetadata, data_fs_version) tuples with complete metadata including
    /// args, vars, and the data_fs_version from any partial commit.
    pub async fn find_incomplete_transactions(
        &self,
    ) -> Result<Vec<(tlogfs::PondTxnMetadata, i64)>, StewardError> {
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        // Query for incomplete write transactions:
        // Those with "begin" but no subsequent record (data_committed, completed, or failed)
        // Fetch all metadata in one query to avoid separate lookups
        let sql = r#"
            SELECT DISTINCT 
                begin_rec.txn_seq,
                begin_rec.txn_id,
                begin_rec.cli_args,
                begin_rec.environment,
                COALESCE(commit_rec.data_fs_version, 0) as data_fs_version
            FROM transactions begin_rec
            LEFT JOIN (
                SELECT txn_seq, data_fs_version 
                FROM transactions 
                WHERE data_fs_version IS NOT NULL
            ) commit_rec ON begin_rec.txn_seq = commit_rec.txn_seq
            WHERE begin_rec.transaction_type = 'write'
              AND begin_rec.record_type = 'begin'
              AND NOT EXISTS (
                  SELECT 1 FROM transactions c
                  WHERE c.txn_seq = begin_rec.txn_seq
                    AND c.record_type IN ('data_committed', 'completed', 'failed')
              )
            ORDER BY begin_rec.txn_seq
        "#;

        let df = ctx.sql(sql).await.map_err(|e| {
            StewardError::ControlTable(format!("Failed to query incomplete transactions: {}", e))
        })?;

        let batches = df.collect().await.map_err(|e| {
            StewardError::ControlTable(format!("Failed to collect query results: {}", e))
        })?;

        let mut incomplete = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let txn_seq_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    StewardError::ControlTable("txn_seq column is not Int64Array".to_string())
                })?;

            let txn_id_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    StewardError::ControlTable("txn_id column is not StringArray".to_string())
                })?;

            let cli_args_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .ok_or_else(|| {
                    StewardError::ControlTable("cli_args column is not ListArray".to_string())
                })?;

            let environment_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .ok_or_else(|| {
                    StewardError::ControlTable("environment column is not MapArray".to_string())
                })?;

            let data_fs_version_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    StewardError::ControlTable(
                        "data_fs_version column is not Int64Array".to_string(),
                    )
                })?;

            for i in 0..batch.num_rows() {
                let txn_seq = txn_seq_array.value(i);
                let txn_id_str = txn_id_array.value(i);

                // Parse UUID
                let txn_id = Uuid::from_str(txn_id_str)
                    .map_err(|e| StewardError::ControlTable(format!("Invalid UUID: {}", e)))?;

                // Extract cli_args
                let cli_args_value = cli_args_array.value(i);
                let cli_args_string_array = cli_args_value
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| {
                        StewardError::ControlTable("cli_args values are not StringArray".to_string())
                    })?;

                let mut cli_args = Vec::new();
                for j in 0..cli_args_string_array.len() {
                    cli_args.push(cli_args_string_array.value(j).to_string());
                }

                // Extract environment map
                let environment_value = environment_array.value(i);
                let keys_array = environment_value
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| {
                        StewardError::ControlTable("environment keys are not StringArray".to_string())
                    })?;
                let values_array = environment_value
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| {
                        StewardError::ControlTable("environment values are not StringArray".to_string())
                    })?;

                let mut environment = std::collections::HashMap::new();
                for j in 0..keys_array.len() {
                    environment.insert(
                        keys_array.value(j).to_string(),
                        values_array.value(j).to_string(),
                    );
                }

                let data_fs_version = data_fs_version_array.value(i);

                // Construct complete PondUserMetadata with all fields
                let user_metadata = tlogfs::PondUserMetadata {
                    txn_id,
                    args: cli_args,
                    vars: environment,
                };

                let txn_metadata = tlogfs::PondTxnMetadata::new(txn_seq, user_metadata);
                incomplete.push((txn_metadata, data_fs_version));
            }
        }

        Ok(incomplete)
    }
}


/// Format pond metadata as a banner for display
pub fn pond_metadata_banner(data: &PondMetadata) {
    let datetime = DateTime::from_timestamp(
        data.birth_timestamp / 1_000_000,
        ((data.birth_timestamp % 1_000_000) * 1000) as u32,
    )
    .unwrap_or_else(|| Utc::now());

    let created_str = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let left = vec![
        format!("Pond {}", data.pond_id),
        format!("Created {}", created_str),
    ];

    let right = vec![data.birth_username.clone(), data.birth_hostname.clone()];

    println!("{}", utilities::banner::format_banner_from_iters(None, left, right));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_control_table() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let control_path = temp_dir.path().join("control");
        let control_path_str = control_path.to_string_lossy().to_string();

        let table = ControlTable::new(&control_path_str)
            .await
            .expect("Failed to create control table");

        // Verify table exists
        assert!(control_path.exists());

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
        let control_path_str = control_path.to_string_lossy().to_string();

        // Create table
        let _table1 = ControlTable::new(&control_path_str)
            .await
            .expect("Failed to create control table");

        // Open existing table
        let table2 = ControlTable::new(&control_path_str)
            .await
            .expect("Failed to open existing control table");

        // Verify we can query it
        let last_seq = table2
            .get_last_write_sequence()
            .await
            .expect("Failed to get last sequence");
        assert_eq!(last_seq, 0);
    }
}

