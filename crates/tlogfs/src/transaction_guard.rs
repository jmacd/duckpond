use super::error::TLogFSError;
use super::persistence::{OpLogPersistence, State};
use super::txn_metadata::PondTxnMetadata;
use log::info;
use std::ops::Deref;
use std::path::PathBuf;
use tinyfs::FS;
use tinyfs::Result as TinyFSResult;

#[cfg(test)]
use super::txn_metadata::PondUserMetadata;

/// Transaction Guard - Enforces proper transaction usage patterns
///
/// This guard embeds a tinyfs::TransactionGuard and adds tlogfs-specific concerns:
/// - Transaction metadata (txn_id, args, vars)
/// - Commit logic for Delta Lake
/// - Write vs read transaction tracking
///
/// The embedded tinyfs guard handles the single-transaction enforcement via TransactionState.
pub struct TransactionGuard<'a> {
    /// The embedded tinyfs transaction guard (owns FS, manages active state)
    inner: tinyfs::TransactionGuard,
    /// Reference to the persistence layer for commit operations
    persistence: &'a mut OpLogPersistence,
    /// Transaction metadata (txn_id, args, vars) provided at begin()
    metadata: PondTxnMetadata,
    /// Whether this is a write transaction (true) or read transaction (false)
    is_write: bool,
}

impl<'a> TransactionGuard<'a> {
    /// Create a new transaction guard
    ///
    /// This should only be called by OpLogPersistence::begin_write() or begin_read()
    pub(crate) fn new(
        inner: tinyfs::TransactionGuard,
        persistence: &'a mut OpLogPersistence,
        metadata: &PondTxnMetadata,
        is_write: bool,
    ) -> Self {
        Self {
            inner,
            persistence,
            metadata: metadata.clone(),
            is_write,
        }
    }

    /// Get the transaction sequence number
    #[must_use]
    pub fn sequence(&self) -> i64 {
        self.metadata.txn_seq
    }

    /// Get access to the underlying persistence layer
    ///
    /// All operations should go through this persistence layer.
    /// The guard just ensures proper transaction scoping and cleanup.
    pub fn state(&self) -> Result<State, TLogFSError> {
        self.persistence.state()
    }

    /// Get access to the underlying persistence layer for read operations
    /// This allows access to query methods like getting the DeltaTable
    #[must_use]
    pub fn persistence(&self) -> &OpLogPersistence {
        self.persistence
    }

    /// Get the shared DataFusion SessionContext - convenience method that delegates to State
    ///
    /// This is a convenience method that maintains the one-line property while internally
    /// using the State's session_context method for proper architecture.
    pub async fn session_context(
        &mut self,
    ) -> Result<std::sync::Arc<datafusion::execution::context::SessionContext>, TLogFSError> {
        let state = self.state()?;
        state.session_context().await
    }

    /// Get access to the TinyFS ObjectStore instance - convenience method that delegates to State
    ///
    /// This is a convenience method that maintains the one-line property while internally
    /// using the State's object_store method for proper architecture.
    pub async fn object_store(
        &mut self,
    ) -> Result<std::sync::Arc<crate::tinyfs_object_store::TinyFsObjectStore>, TLogFSError> {
        let state = self.state()?;
        // Ensure SessionContext and ObjectStore are initialized
        _ = state.session_context().await?;
        state
            .object_store()
            .ok_or_else(|| TLogFSError::ArrowMessage("ObjectStore not initialized".to_string()))
    }

    /// Deltalake store path
    pub(crate) fn store_path(&self) -> &PathBuf {
        &self.persistence.path
    }

    /// Commit the transaction
    ///
    /// For write transactions: commits changes to Delta Lake and updates last_txn_seq.
    /// For read transactions: returns None without committing (read transactions don't modify state).
    ///
    /// All metadata (txn_id, args, vars) was provided at `begin()`, so commit()
    /// requires no additional parameters. The guard has everything it needs.
    ///
    /// This is the clean production API that Steward uses.
    ///
    /// The embedded tinyfs::TransactionGuard will be dropped after commit, clearing the transaction state.
    pub async fn commit(self) -> TinyFSResult<Option<()>> {
        if !self.is_write {
            // Read transactions don't commit - just return success
            // tinyfs guard drop will clear the transaction state
            return Ok(None);
        }

        let result = self.persistence.commit(self.metadata.clone()).await;
        // tinyfs guard drop happens here, clearing the transaction state

        result.map_err(|e| tinyfs::Error::Other(format!("Transaction commit failed: {}", e)))
    }

    /// Commit with test metadata - convenience for tests
    ///
    /// This provides a convenient way for tests to commit without manually creating metadata.
    /// Uses txn_seq=2 which is correct for the common case of a single transaction after
    /// pond creation (root init uses txn_seq=1).
    ///
    /// For tests with multiple transactions, use `commit_test_with_sequence()` instead.
    ///
    /// **Should only be used in test code.**
    #[cfg(test)]
    pub async fn commit_test(self) -> TinyFSResult<()> {
        let metadata = PondTxnMetadata::new(
            2,
            PondUserMetadata::new(vec!["test".to_string(), "transaction".to_string()]),
        );
        let result = self.persistence.commit(metadata).await;

        result
            .map_err(|e| tinyfs::Error::Other(format!("Transaction commit failed: {}", e)))
            .map(|_| ())
    }

    /// Commit with test metadata using explicit sequence number - for multi-commit tests
    ///
    /// Most tests should use `commit_test()` which defaults to txn_seq=2.
    /// This variant is for tests with multiple commits that need to specify different
    /// sequence numbers (e.g., 2, 3, 4 for a test with three transactions).
    ///
    /// **Should only be used in test code.**
    #[cfg(test)]
    pub async fn commit_test_with_sequence(self, txn_seq: i64) -> TinyFSResult<Option<()>> {
        let metadata = PondTxnMetadata::new(
            txn_seq,
            PondUserMetadata::new(vec!["test".to_string(), "transaction".to_string()]),
        );
        let result = self.persistence.commit(metadata).await;

        result.map_err(|e| tinyfs::Error::Other(format!("Transaction commit failed: {}", e)))
    }
}

impl<'a> Deref for TransactionGuard<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> Drop for TransactionGuard<'a> {
    /// Automatic cleanup on drop
    ///
    /// If the transaction hasn't been explicitly committed or rolled back,
    /// we clean up the state and fs to allow new transactions to begin.
    /// This happens when read-only transactions go out of scope without commit.
    ///
    /// The embedded tinyfs::TransactionGuard will handle clearing the TransactionState.
    fn drop(&mut self) {
        if self.persistence.state.is_some() {
            info!(
                "Transaction {} dropped without explicit commit - cleaning up state",
                self.metadata.txn_seq
            );
            self.persistence.state = None;
            self.persistence.fs = None;
            // tinyfs::TransactionGuard drop will clear the txn_state
        }
    }
}
