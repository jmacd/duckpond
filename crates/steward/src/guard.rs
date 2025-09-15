//! Steward Transaction Guard - Wraps tlogfs transaction guard with steward-specific logic

use crate::StewardError;
use tlogfs::{transaction_guard::TransactionGuard, OpLogPersistence};
use tinyfs::FS;
use std::ops::Deref;
use std::sync::Arc;
use diagnostics::*;

/// Steward transaction guard that ensures proper sequencing of data and control filesystem operations
pub struct StewardTransactionGuard<'a> {
    /// The underlying tlogfs transaction guard
    data_tx: Option<TransactionGuard<'a>>,
    /// Transaction ID generated for this transaction
    txn_id: String,
    /// Command arguments that initiated this transaction
    args: Vec<String>,
    /// Reference to control persistence for metadata recording
    control_persistence: &'a mut OpLogPersistence,
}

impl<'a> StewardTransactionGuard<'a> {
    /// Create a new steward transaction guard
    pub(crate) fn new(
        data_tx: TransactionGuard<'a>, 
        txn_id: String, 
        args: Vec<String>,
        control_persistence: &'a mut OpLogPersistence,
    ) -> Self {
        Self {
            data_tx: Some(data_tx),
            txn_id,
            args,
            control_persistence,
        }
    }

    /// Get the transaction ID
    pub fn txn_id(&self) -> &str {
        &self.txn_id
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
        debug!("Committing steward transaction", txn_id: &self.txn_id, args: &args_fmt);

        // Step 1: Create transaction metadata
        let txn_metadata = {
            let mut metadata = std::collections::HashMap::new();
            let pond_txn = serde_json::json!({
                "txn_id": self.txn_id.clone(),
                "args": args_fmt
            });
            metadata.insert("pond_txn".to_string(), pond_txn);
            metadata
        };

        // Step 2: Extract the underlying transaction guard and commit it
        let data_tx = self.take_transaction()
            .ok_or_else(|| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other("Transaction already consumed".to_string()))))?;
        
        let did = data_tx.commit(Some(txn_metadata.clone())).await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Step 3: If this was a write transaction, record metadata in control filesystem
        if let Some(_) = did {
            crate::Ship::record_transaction_metadata_with_persistence(self.control_persistence, &self.txn_id, &self.args).await?;
            info!("Steward transaction committed {args_fmt}");
        } else {
            info!("Read-only steward transaction completed successfully");
        }

        Ok(did)
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
            debug!("Steward transaction guard dropped without commit - transaction will rollback", txn_id: &self.txn_id);
        }
    }
}
