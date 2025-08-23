use super::persistence::{State, OpLogPersistence};
use tinyfs::Result as TinyFSResult;
use tinyfs::FS;
use std::ops::Deref;
use super::error::TLogFSError;
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
    pub fn state(&self) -> Result<State, TLogFSError> {
        self.persistence.state()
    }

    /// Get access to the underlying persistence layer for read operations
    /// This allows access to query methods like getting the DeltaTable
    pub fn persistence(&self) -> &OpLogPersistence {
        self.persistence
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
	if self.persistence.state.is_some() {
            info!("Transaction dropped without commit");
	}
    }
}
