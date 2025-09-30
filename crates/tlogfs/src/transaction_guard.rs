use super::persistence::{State, OpLogPersistence};
use tinyfs::Result as TinyFSResult;
use tinyfs::FS;
use std::ops::Deref;
use super::error::TLogFSError;
use log::info;


/// Transaction Guard - Enforces proper transaction usage patterns
/// 
/// The guard provides RAII-style cleanup and access to the underlying persistence layer.
/// Operations are performed through the persistence layer accessed via the guard.
/// Optionally provides a DataFusion SessionContext with TinyFS ObjectStore for queries.
pub struct TransactionGuard<'a> {
    /// Reference to the persistence layer
    persistence: &'a mut OpLogPersistence,
}

impl<'a> TransactionGuard<'a> {
    /// Create a new transaction guard
    /// 
    /// This should only be called by OpLogPersistence::begin()
    pub(crate) fn new(persistence: &'a mut OpLogPersistence) -> Self {
        Self {
            persistence,
        }
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
    pub fn persistence(&self) -> &OpLogPersistence {
        self.persistence
    }

    /// Get the shared DataFusion SessionContext - convenience method that delegates to State
    /// 
    /// This is a convenience method that maintains the one-line property while internally
    /// using the State's session_context method for proper architecture.
    pub async fn session_context(&mut self) -> Result<std::sync::Arc<datafusion::execution::context::SessionContext>, TLogFSError> {
        let state = self.state()?;
        state.session_context().await
    }

    /// Get access to the TinyFS ObjectStore instance - convenience method that delegates to State
    /// 
    /// This is a convenience method that maintains the one-line property while internally
    /// using the State's object_store method for proper architecture.
    pub async fn object_store(&mut self) -> Result<std::sync::Arc<crate::tinyfs_object_store::TinyFsObjectStore>, TLogFSError> {
        let state = self.state()?;
        // Ensure SessionContext and ObjectStore are initialized
        state.session_context().await?;
        state.object_store().ok_or_else(|| TLogFSError::ArrowMessage("ObjectStore not initialized".to_string()))
    }

    /// Deltalake store path
    pub(crate) fn store_path(&self) -> String {
        self.persistence.path.clone()
    }
    
    pub async fn commit(
        self, 
        metadata: Option<std::collections::HashMap<String, serde_json::Value>>
    ) -> TinyFSResult<Option<()>> {
        let result = self.persistence.commit(metadata).await;
        
        result.map_err(|e| tinyfs::Error::Other(format!("Transaction commit failed: {}", e)))
    }
}

impl<'a> Deref for TransactionGuard<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
	self.persistence.fs.as_ref().unwrap()
    }
}

impl<'a> Drop for TransactionGuard<'a> {
    /// Automatic cleanup on drop
    /// 
    /// If the transaction hasn't been explicitly committed or rolled back,
    /// we log a warning and mark for lazy cleanup by the persistence layer.
    fn drop(&mut self) {
	if self.persistence.state.is_some() {
            info!("Transaction dropped without commit");
	}
    }
}
