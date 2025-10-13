//! Steward Transaction Guard - Wraps tlogfs transaction guard with steward-specific logic

use crate::{control_table::ControlTable, StewardError};
use tlogfs::{transaction_guard::TransactionGuard};
use tinyfs::FS;
use std::ops::Deref;
use std::sync::Arc;
use log::{debug, info};

/// Steward transaction guard that ensures proper sequencing of data and control filesystem operations
pub struct StewardTransactionGuard<'a> {
    /// The underlying tlogfs transaction guard
    data_tx: Option<TransactionGuard<'a>>,
    /// Transaction sequence number
    txn_seq: i64,
    /// Transaction ID generated for this transaction
    txn_id: String,
    /// Transaction type: "read" or "write"
    transaction_type: String,
    /// Command arguments that initiated this transaction
    args: Vec<String>,
    /// Reference to control table for transaction tracking
    control_table: &'a mut ControlTable,
    /// Start time for duration tracking
    start_time: std::time::Instant,
}

impl<'a> StewardTransactionGuard<'a> {
    /// Create a new steward transaction guard
    pub(crate) fn new(
        data_tx: TransactionGuard<'a>, 
        txn_seq: i64,
        txn_id: String, 
        transaction_type: String,
        args: Vec<String>,
        control_table: &'a mut ControlTable,
    ) -> Self {
        Self {
            data_tx: Some(data_tx),
            txn_seq,
            txn_id,
            transaction_type,
            args,
            control_table,
            start_time: std::time::Instant::now(),
        }
    }

    /// Get the transaction ID
    pub fn txn_id(&self) -> &str {
        &self.txn_id
    }

    /// Get the transaction type ("read" or "write")
    pub fn transaction_type(&self) -> &str {
        &self.transaction_type
    }

    /// Check if this is a write transaction
    pub fn is_write_transaction(&self) -> bool {
        self.transaction_type == "write"
    }

    /// Check if this is a read transaction
    pub fn is_read_transaction(&self) -> bool {
        self.transaction_type == "read"
    }

    /// Get the command arguments
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// Take the underlying transaction guard (for commit)
    pub(crate) fn take_transaction(&mut self) -> Option<TransactionGuard<'a>> {
        self.data_tx.take()
    }

    /// Get access to the underlying transaction state (for queries)
    pub fn state(&self) -> Result<tlogfs::persistence::State, tlogfs::TLogFSError> {
        self.data_tx.as_ref()
            .ok_or_else(|| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction guard has been consumed".to_string())))?
            .state()
    }

    /// Get access to the underlying data persistence layer for read operations
    /// This allows access to the DeltaTable and other query components
    pub fn data_persistence(&self) -> Result<&tlogfs::OpLogPersistence, tlogfs::TLogFSError> {
        Ok(self.data_tx.as_ref()
            .ok_or_else(|| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction guard has been consumed".to_string())))?
            .persistence())
    }

    /// Get or create a DataFusion SessionContext with TinyFS ObjectStore registered
    /// 
    /// This method ensures a single SessionContext per transaction, preventing
    /// ObjectStore registry conflicts when creating multiple table providers.
    /// Delegates to the underlying TLogFS transaction guard.
    pub async fn session_context(&mut self) -> Result<Arc<datafusion::execution::context::SessionContext>, tlogfs::TLogFSError> {
        self.data_tx.as_mut()
            .ok_or_else(|| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction guard has been consumed".to_string())))?
            .session_context()
            .await
    }

    /// Get access to the TinyFS ObjectStore instance used by the SessionContext
    /// This allows direct operations on the same ObjectStore that DataFusion uses
    pub async fn object_store(&mut self) -> Result<Arc<tlogfs::tinyfs_object_store::TinyFsObjectStore>, tlogfs::TLogFSError> {
        self.data_tx.as_mut()
            .ok_or_else(|| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction guard has been consumed".to_string())))?
            .object_store()
            .await
    }

    /// Get mutable access to the underlying TransactionGuard for tlogfs operations
    /// This allows tlogfs functions to accept the transaction guard directly
    pub fn transaction_guard(&mut self) -> Result<&mut TransactionGuard<'a>, tlogfs::TLogFSError> {
        self.data_tx.as_mut()
            .ok_or_else(|| tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction guard has been consumed".to_string())))
    }

    /// Commit the transaction with proper steward sequencing
    /// Returns whether a write transaction occurred
    pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
        let args_fmt = format!("{:?}", self.args);
        debug!("Committing steward transaction {} {}", &self.txn_id, &args_fmt);

        // Calculate duration for recording
        let duration_ms = self.start_time.elapsed().as_millis() as i64;

        // Get current table version before commit (the commit will increment it)
        let pre_commit_version = self.data_tx.as_ref()
            .ok_or_else(|| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction already consumed".to_string()))))?
            .persistence()
            .table()
            .version();

        // Step 1: Create transaction metadata
        let txn_metadata = {
            let mut metadata = std::collections::HashMap::new();
            let pond_txn = serde_json::json!({
                "txn_id": self.txn_id.clone(),
                "txn_seq": self.txn_seq,
                "args": args_fmt
            });
            metadata.insert("pond_txn".to_string(), pond_txn);
            metadata
        };

        // Step 2: Extract the underlying transaction guard and commit it
        let data_tx = self.take_transaction()
            .ok_or_else(|| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction already consumed".to_string()))))?;
        
        let commit_result = data_tx.commit(Some(txn_metadata.clone())).await;

        // Step 3: Record transaction lifecycle in control table based on result
        match commit_result {
            Ok(Some(())) => {
                // Write transaction committed successfully - version is pre_commit_version + 1
                let new_version = pre_commit_version.unwrap_or(0) + 1;
                
                // VALIDATION: If this was marked as a read transaction but wrote data, fail
                if self.transaction_type == "read" {
                    self.control_table.record_failed(
                        self.txn_seq,
                        self.txn_id.clone(),
                        "Read transaction attempted to write data".to_string(),
                        duration_ms,
                    ).await
                        .map_err(|e| StewardError::ControlTable(format!("Failed to record failure: {}", e)))?;
                    return Err(StewardError::ReadTransactionAttemptedWrite);
                }
                
                self.control_table.record_data_committed(
                    self.txn_seq,
                    self.txn_id.clone(),
                    new_version,
                    duration_ms,
                ).await
                    .map_err(|e| StewardError::ControlTable(format!("Failed to record commit: {}", e)))?;
                info!("Steward transaction {} committed (seq={}, version={})", self.txn_id, self.txn_seq, new_version);
                Ok(Some(()))
            }
            Ok(None) => {
                // Read-only transaction completed successfully
                // Note: Write transactions that make no changes are allowed (idempotent operations like mkdir -p)
                // We record them as "completed" rather than "data_committed" since no version was created
                
                self.control_table.record_completed(
                    self.txn_seq,
                    self.txn_id.clone(),
                    duration_ms,
                ).await
                    .map_err(|e| StewardError::ControlTable(format!("Failed to record completion: {}", e)))?;
                
                if self.transaction_type == "write" {
                    info!("Write-no-op steward transaction {} completed (seq={})", self.txn_id, self.txn_seq);
                } else {
                    info!("Read-only steward transaction {} completed (seq={})", self.txn_id, self.txn_seq);
                }
                Ok(None)
            }
            Err(e) => {
                // Transaction failed - record error
                let error_msg = format!("{}", e);
                self.control_table.record_failed(
                    self.txn_seq,
                    self.txn_id.clone(),
                    error_msg.clone(),
                    duration_ms,
                ).await
                    .map_err(|e| StewardError::ControlTable(format!("Failed to record failure: {}", e)))?;
                Err(StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))
            }
        }
    }
}

impl<'a> Deref for StewardTransactionGuard<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        self.data_tx.as_ref().expect("Transaction guard has been consumed")
    }
}

impl<'a> Drop for StewardTransactionGuard<'a> {
    fn drop(&mut self) {
        if self.data_tx.is_some() {
            debug!("Steward transaction guard dropped without commit - transaction will rollback {}", &self.txn_id);
        }
    }
}
