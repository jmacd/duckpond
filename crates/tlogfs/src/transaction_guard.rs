use super::persistence::{State, OpLogPersistence};
use tinyfs::Result as TinyFSResult;
use tinyfs::FS;
use std::ops::Deref;
use std::sync::Arc;
use super::error::TLogFSError;
use diagnostics::*;


/// Transaction Guard - Enforces proper transaction usage patterns
/// 
/// The guard provides RAII-style cleanup and access to the underlying persistence layer.
/// Operations are performed through the persistence layer accessed via the guard.
/// Optionally provides a DataFusion SessionContext with TinyFS ObjectStore for queries.
pub struct TransactionGuard<'a> {
    /// Reference to the persistence layer
    persistence: &'a mut OpLogPersistence,
    /// Optional DataFusion SessionContext - lazy initialized on first access
    session_context: Option<Arc<datafusion::execution::context::SessionContext>>,
    /// Optional TinyFS ObjectStore - created when SessionContext is initialized
    /// This allows direct access to the same ObjectStore instance that DataFusion uses
    object_store: Option<Arc<crate::tinyfs_object_store::TinyFsObjectStore>>,
}

impl<'a> TransactionGuard<'a> {
    /// Create a new transaction guard
    /// 
    /// This should only be called by OpLogPersistence::begin()
    pub(crate) fn new(persistence: &'a mut OpLogPersistence) -> Self {
        Self {
            persistence,
            session_context: None,
            object_store: None,
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

    /// Get or create a DataFusion SessionContext with TinyFS ObjectStore registered
    /// 
    /// This method ensures a single SessionContext per transaction, preventing
    /// ObjectStore registry conflicts when creating multiple table providers.
    pub async fn session_context(&mut self) -> Result<Arc<datafusion::execution::context::SessionContext>, TLogFSError> {
        if self.session_context.is_none() {
            let ctx = Arc::new(datafusion::execution::context::SessionContext::new());
            
            // Register the TinyFS ObjectStore with the context and store the instance
            // This prevents "File series not found in registry" errors and allows direct access
            let state = self.state()?;
            let object_store = crate::file_table::register_tinyfs_object_store_with_context(&ctx, state).await?;
            
            self.session_context = Some(ctx);
            self.object_store = Some(object_store);
        }
        
        Ok(self.session_context.as_ref().unwrap().clone())
    }

    /// Get access to the TinyFS ObjectStore instance used by the SessionContext
    /// This allows direct operations on the same ObjectStore that DataFusion uses
    pub async fn object_store(&mut self) -> Result<Arc<crate::tinyfs_object_store::TinyFsObjectStore>, TLogFSError> {
        // Ensure SessionContext and ObjectStore are initialized
        self.session_context().await?;
        Ok(self.object_store.as_ref().unwrap().clone())
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
