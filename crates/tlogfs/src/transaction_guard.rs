use super::persistence::{State, OpLogPersistence};
use tinyfs::Result as TinyFSResult;
use tinyfs::FS;
use std::ops::Deref;
use diagnostics::*;


/// Transaction Guard - Enforces proper transaction usage patterns
/// 
/// The guard provides RAII-style cleanup and access to the underlying persistence layer.
/// Operations are performed through the persistence layer accessed via the guard.
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
    pub(crate) fn state(&self) -> State {
        self.persistence.state()
    }

    /// Deltalake store path
    pub(crate) fn store_path(&self) -> String {
        self.persistence.path.clone()
    }
    
    /// Commit the transaction with optional metadata.
    /// Returns the committed version number if operations were performed.
    pub async fn commit(
        self, 
        metadata: Option<std::collections::HashMap<String, serde_json::Value>>
    ) -> TinyFSResult<Option<u64>> {
        debug!("Committing transaction");
        
        // Delegate to persistence layer with metadata
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

// impl<'a> DerefMut for TransactionGuard<'a> {
//     type Target = FS;
//     fn deref_mut(&self) -> &Self::Target {
// 	self.persistence.fs.as_ref().unwrap()
//     }
// }

impl<'a> Drop for TransactionGuard<'a> {
    /// Automatic cleanup on drop
    /// 
    /// If the transaction hasn't been explicitly committed or rolled back,
    /// we log a warning and mark for lazy cleanup by the persistence layer.
    fn drop(&mut self) {
        debug!("Transaction dropped without commit");
            
        // Note: We cannot call async methods in Drop, so we rely on the persistence
        // layer to clean up stale transaction state when the next transaction begins
	// @@@ No
    }
}
