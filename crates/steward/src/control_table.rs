//! Control Table - DeltaLake-based transaction tracking for Steward
//!
//! Replaces the control filesystem TLogFS instance with a direct DeltaLake table
//! that tracks transaction lifecycle, sequences, and enables future replication.

use crate::StewardError;
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use deltalake::operations::DeltaOps;
use deltalake::DeltaTable;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Transaction record for control table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub txn_seq: i64,
    pub txn_id: String,
    pub based_on_seq: Option<i64>,
    pub record_type: String, // "begin" | "data_committed" | "failed" | "completed"
    pub timestamp: i64,      // Microseconds since epoch (for DeltaLake compatibility)
    pub transaction_type: String, // "read" | "write"
    pub cli_args: Vec<String>,
    pub environment: HashMap<String, String>,
    pub data_fs_version: Option<i64>,
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
}

/// Control table for tracking transaction lifecycle and sequencing
pub struct ControlTable {
    /// Path to the Delta Lake table
    #[allow(dead_code)]
    path: String,
    /// The Delta Lake table instance
    table: DeltaTable,
}

impl ControlTable {
    /// Create a new control table or open an existing one
    pub async fn new(path: &str) -> Result<Self, StewardError> {
        debug!("Initializing control table at {}", path);

        // Try to open existing table first
        match deltalake::open_table(path).await {
            Ok(table) => {
                debug!("Opened existing control table at {}", path);
                Ok(Self {
                    path: path.to_string(),
                    table,
                })
            }
            Err(_) => {
                // Table doesn't exist or path doesn't exist, create it
                debug!("Creating new control table at {}", path);
                
                // Ensure the directory exists
                std::fs::create_dir_all(path)
                    .map_err(|e| StewardError::ControlTable(format!("Failed to create directory: {}", e)))?;
                
                // Create table by writing an empty batch with the correct schema
                let schema = Self::arrow_schema();
                let empty_batch = RecordBatch::new_empty(schema);
                
                let table = DeltaOps::try_from_uri(path)
                    .await
                    .map_err(|e| StewardError::ControlTable(format!("Failed to initialize table: {}", e)))?
                    .write(vec![empty_batch])
                    .await
                    .map_err(|e| StewardError::ControlTable(format!("Failed to create table: {}", e)))?;

                Ok(Self {
                    path: path.to_string(),
                    table,
                })
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
            Field::new("record_type", DataType::Utf8, false), // "begin" | "data_committed" | "failed" | "completed"
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("transaction_type", DataType::Utf8, false), // "read" | "write"
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
        ]))
    }

    /// Get access to the underlying Delta table for querying
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Query the last write transaction sequence number
    /// Returns 0 if no write transactions exist yet
    #[allow(dead_code)]
    pub async fn get_last_write_sequence(&self) -> Result<i64, crate::StewardError> {
        // Use DataFusion to query - DeltaTable implements TableProvider
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let df = ctx.sql("SELECT MAX(txn_seq) as max_seq FROM transactions WHERE transaction_type = 'write'")
            .await
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to query: {}", e)))?;

        let batches = df.collect().await
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to collect results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(0); // No transactions yet
        }

        let batch = &batches[0];
        let max_seq_array = batch.column(0).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| crate::StewardError::ControlTable("Failed to downcast max_seq".to_string()))?;

        if max_seq_array.is_null(0) {
            Ok(0) // No write transactions yet
        } else {
            Ok(max_seq_array.value(0))
        }
    }

    /// Record the beginning of a transaction
    pub async fn record_begin(
        &mut self,
        txn_seq: i64,
        based_on_seq: Option<i64>,
        txn_id: String,
        transaction_type: &str,
        cli_args: Vec<String>,
        environment: HashMap<String, String>,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();

        let record = TransactionRecord {
            txn_seq,
            txn_id,
            based_on_seq,
            record_type: "begin".to_string(),
            timestamp,
            transaction_type: transaction_type.to_string(),
            cli_args,
            environment,
            data_fs_version: None,
            error_message: None,
            duration_ms: None,
        };

        self.write_record(record).await
    }

    /// Record successful data filesystem commit
    pub async fn record_data_committed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        data_fs_version: i64,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();
            

        let record = TransactionRecord {
            txn_seq,
            txn_id,
            based_on_seq: None,
            record_type: "data_committed".to_string(),
            timestamp,
            transaction_type: String::new(), // Not relevant for commit record
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: Some(data_fs_version),
            error_message: None,
            duration_ms: Some(duration_ms),
        };

        self.write_record(record).await
    }

    /// Record transaction failure
    pub async fn record_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();
            

        let record = TransactionRecord {
            txn_seq,
            txn_id,
            based_on_seq: None,
            record_type: "failed".to_string(),
            timestamp,
            transaction_type: String::new(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: Some(error_message),
            duration_ms: Some(duration_ms),
        };

        self.write_record(record).await
    }

    /// Record completed read transaction
    pub async fn record_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        duration_ms: i64,
    ) -> Result<(), crate::StewardError> {
        let timestamp = chrono::Utc::now().timestamp_micros();
            

        let record = TransactionRecord {
            txn_seq,
            txn_id,
            based_on_seq: None,
            record_type: "completed".to_string(),
            timestamp,
            transaction_type: String::new(),
            cli_args: Vec::new(),
            environment: HashMap::new(),
            data_fs_version: None,
            error_message: None,
            duration_ms: Some(duration_ms),
        };

        self.write_record(record).await
    }

    /// Write a transaction record to the control table using serde_arrow
    async fn write_record(&mut self, record: TransactionRecord) -> Result<(), crate::StewardError> {
        // Convert struct to Arrow RecordBatch using serde_arrow
        let records = vec![record];
        let schema = Self::arrow_schema();
        let arrays = serde_arrow::to_arrow(schema.fields(), &records)
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to convert to arrow: {}", e)))?;

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to create record batch: {}", e)))?;

        // Write to Delta Lake
        let table = DeltaOps(self.table.clone())
            .write(vec![batch])
            .await
            .map_err(|e| crate::StewardError::ControlTable(format!("Failed to write to Delta Lake: {}", e)))?;

        // Update our cached table reference
        self.table = table;

        Ok(())
    }

    /// Reload the table to see latest commits
    #[allow(dead_code)]
    async fn reload(&mut self) -> Result<(), StewardError> {
        self.table = deltalake::open_table(&self.path).await
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
    /// Returns (txn_seq, txn_id, data_fs_version) where data_fs_version is 0 for abandoned begins.
    pub async fn find_incomplete_transactions(&self) -> Result<Vec<(i64, String, i64)>, StewardError> {
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        // Query for incomplete write transactions:
        // Those with "begin" but no subsequent record (data_committed, completed, or failed)
        let sql = r#"
            SELECT DISTINCT 
                begin_rec.txn_seq,
                begin_rec.txn_id,
                0 as data_fs_version
            FROM transactions begin_rec
            WHERE begin_rec.transaction_type = 'write'
              AND begin_rec.record_type = 'begin'
              AND NOT EXISTS (
                  SELECT 1 FROM transactions c
                  WHERE c.txn_seq = begin_rec.txn_seq
                    AND c.record_type IN ('data_committed', 'completed', 'failed')
              )
            ORDER BY begin_rec.txn_seq
        "#;

        let df = ctx.sql(sql).await
            .map_err(|e| StewardError::ControlTable(format!("Failed to query incomplete transactions: {}", e)))?;
        
        let batches = df.collect().await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect query results: {}", e)))?;

        let mut incomplete = Vec::new();
        
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let txn_seq_array = batch.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| StewardError::ControlTable("txn_seq column is not Int64Array".to_string()))?;
            
            let txn_id_array = batch.column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| StewardError::ControlTable("txn_id column is not StringArray".to_string()))?;
            
            let data_fs_version_array = batch.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| StewardError::ControlTable("data_fs_version column is not Int64Array".to_string()))?;

            for i in 0..batch.num_rows() {
                let txn_seq = txn_seq_array.value(i);
                let txn_id = txn_id_array.value(i).to_string();
                let data_fs_version = data_fs_version_array.value(i);
                
                incomplete.push((txn_seq, txn_id, data_fs_version));
            }
        }

        Ok(incomplete)
    }

    /// Get details of a specific incomplete transaction (for recovery error messages)
    /// Returns (cli_args, data_fs_version)
    pub async fn get_incomplete_transaction_details(&self, txn_seq: i64) -> Result<(Vec<String>, i64), StewardError> {
        let ctx = SessionContext::new();
        ctx.register_table("transactions", Arc::new(self.table.clone()))
            .map_err(|e| StewardError::ControlTable(format!("Failed to register table: {}", e)))?;

        let sql = format!(r#"
            SELECT 
                begin_rec.cli_args,
                COALESCE(commit_rec.data_fs_version, 0) as data_fs_version
            FROM transactions begin_rec
            LEFT JOIN (
                SELECT txn_seq, data_fs_version 
                FROM transactions 
                WHERE data_fs_version IS NOT NULL
            ) commit_rec ON begin_rec.txn_seq = commit_rec.txn_seq
            WHERE begin_rec.txn_seq = {}
              AND begin_rec.record_type = 'begin'
            LIMIT 1
        "#, txn_seq);

        let df = ctx.sql(&sql).await
            .map_err(|e| StewardError::ControlTable(format!("Failed to query transaction details: {}", e)))?;
        
        let batches = df.collect().await
            .map_err(|e| StewardError::ControlTable(format!("Failed to collect query results: {}", e)))?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Err(StewardError::ControlTable(format!("Transaction {} not found", txn_seq)));
        }

        let batch = &batches[0];
        
        // Get cli_args (list of strings)
        let cli_args_array = batch.column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .ok_or_else(|| StewardError::ControlTable("cli_args column is not ListArray".to_string()))?;
        
        let data_fs_version_array = batch.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StewardError::ControlTable("data_fs_version column is not Int64Array".to_string()))?;

        // Extract first row
        let cli_args_value = cli_args_array.value(0);
        let cli_args_string_array = cli_args_value
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| StewardError::ControlTable("cli_args values are not StringArray".to_string()))?;
        
        let mut cli_args = Vec::new();
        for i in 0..cli_args_string_array.len() {
            cli_args.push(cli_args_string_array.value(i).to_string());
        }

        let data_fs_version = data_fs_version_array.value(0);

        Ok((cli_args, data_fs_version))
    }
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
