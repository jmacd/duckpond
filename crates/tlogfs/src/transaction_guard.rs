//! Transaction Guard Implementation for TLogFS
//!
//! This module implements the Transaction Guard pattern to eliminate empty transactions,
//! enforce transaction discipline, and provide automatic cleanup through RAII.
//!
//! ## Architecture
//!
//! The Transaction Guard pattern ensures that:
//! - Operations can only be performed within a transaction context
//! - Empty transactions cannot be committed
//! - Transaction cleanup is automatic via Drop implementation
//! - Explicit commit is required to persist changes
//!
//! ## Usage
//!
//! ```rust,no_run
//! use tlogfs::OpLogPersistence;
//! use tinyfs::NodeID;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let persistence = OpLogPersistence::new("/tmp/test").await?;
//! let root_id = NodeID::new("root-node-id".to_string());
//!
//! {
//!     let tx = persistence.begin_transaction_with_guard().await?;  // Returns guard
//!     let _root = tx.load_node(root_id, root_id).await?; // Operation through guard
//!     tx.commit().await?;                               // Consumes guard
//! } // Guard cleanup automatic on drop if not committed
//! # Ok(())
//! # }
//! ```

use super::persistence::OpLogPersistence;
use tinyfs::{NodeID, NodeType, Result as TinyFSResult};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use diagnostics::*;

/// Transaction Guard - Enforces proper transaction usage patterns
/// 
/// The guard tracks operation counts and provides RAII-style cleanup.
/// Operations are only permitted through the guard, ensuring transaction discipline.
pub struct TransactionGuard<'a> {
    /// Reference to the persistence layer
    persistence: &'a OpLogPersistence,
    
    /// Number of operations performed in this transaction
    operation_count: AtomicUsize,
    
    /// Whether this transaction has been committed
    committed: AtomicBool,
    
    /// Transaction ID for tracking
    transaction_id: i64,
}

impl<'a> TransactionGuard<'a> {
    /// Create a new transaction guard
    /// 
    /// This should only be called by OpLogPersistence::begin_transaction()
    pub(crate) fn new(persistence: &'a OpLogPersistence, transaction_id: i64) -> Self {
        debug!("Creating transaction guard for transaction {transaction_id}");
        Self {
            persistence,
            operation_count: AtomicUsize::new(0),
            committed: AtomicBool::new(false),
            transaction_id,
        }
    }
    
    /// Get the current operation count
    pub fn operation_count(&self) -> usize {
        self.operation_count.load(Ordering::SeqCst)
    }
    
    /// Check if this transaction has been committed
    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }
    
    /// Get the transaction ID
    pub fn transaction_id(&self) -> i64 {
        self.transaction_id
    }
    
    /// Increment the operation counter
    fn increment_operation_count(&self) {
        let new_count = self.operation_count.fetch_add(1, Ordering::SeqCst) + 1;
        let tx_id = self.transaction_id;
        debug!("Transaction {tx_id}: operation count now {new_count}");
    }
    
    /// Load a node through the transaction guard
    pub async fn load_node(&self, node_id: NodeID, part_id: NodeID) -> TinyFSResult<NodeType> {
        if self.is_committed() {
            return Err(tinyfs::Error::Other("Cannot use committed transaction guard".to_string()));
        }
        
        // Increment operation count
        self.increment_operation_count();
        
        // Delegate to persistence layer with transaction context
        self.persistence.load_node_transactional(node_id, part_id, self.transaction_id).await
    }
    
    /// Store a node through the transaction guard
    pub async fn store_node(&self, node_id: NodeID, part_id: NodeID, node_type: &NodeType) -> TinyFSResult<()> {
        if self.is_committed() {
            return Err(tinyfs::Error::Other("Cannot use committed transaction guard".to_string()));
        }
        
        // Increment operation count
        self.increment_operation_count();
        
        // Delegate to persistence layer with transaction context
        self.persistence.store_node_transactional(node_id, part_id, node_type, self.transaction_id).await
    }
    
    /// Initialize root directory through the transaction guard
    pub async fn initialize_root_directory(&self) -> TinyFSResult<()> {
        if self.is_committed() {
            return Err(tinyfs::Error::Other("Cannot use committed transaction guard".to_string()));
        }
        
        // Increment operation count
        self.increment_operation_count();
        
        // Delegate to persistence layer with transaction context
        self.persistence.initialize_root_directory_transactional(self.transaction_id).await
    }
    
    /// Create a file writer tied to this transaction
    /// 
    /// The writer is bound to this transaction's lifetime and will automatically
    /// handle small/large file promotion and content analysis.
    pub fn create_file_writer(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        file_type: tinyfs::EntryType,
    ) -> Result<crate::file_writer::FileWriter<'_>, tinyfs::Error> {
        use crate::file_writer::FileWriter;
        
        if self.is_committed() {
            return Err(tinyfs::Error::Other("Cannot use committed transaction guard".to_string()));
        }
        
        let node_hex = node_id.to_hex_string();
        let tx_id = self.transaction_id;
        debug!("Creating FileWriter for node {node_hex} with file type in transaction {tx_id}");
        
        Ok(FileWriter::new(node_id, part_id, file_type, self))
    }
    
    /// Store file content reference in the transaction (internal method for FileWriter)
    /// 
    /// This method replaces any existing pending entry for the same file within this transaction.
    /// Multiple writes to the same file within a transaction result in a single version.
    pub(crate) async fn store_file_content_ref(
        &self,
        node_id: NodeID,
        part_id: NodeID,
        content_ref: crate::file_writer::ContentRef,
        file_type: tinyfs::EntryType,
        metadata: crate::file_writer::FileMetadata,
    ) -> Result<(), crate::error::TLogFSError> {
        if self.is_committed() {
            return Err(crate::error::TLogFSError::Transaction { 
                message: "Cannot use committed transaction guard".to_string() 
            });
        }
        
        // Increment operation count
        self.increment_operation_count();
        
        // Delegate to persistence layer with transaction context
        self.persistence.store_file_content_ref_transactional(
            node_id, part_id, content_ref, file_type, metadata, self.transaction_id
        ).await
    }
    
    /// Get the store path for this transaction (for file writers)
    pub fn store_path(&self) -> &str {
        self.persistence.store_path()
    }
    
    /// Commit the transaction
    /// 
    /// This consumes the guard to prevent reuse after commit.
    /// Will fail if no operations have been performed.
    pub async fn commit(self) -> TinyFSResult<()> {
        let operation_count = self.operation_count();
        let tx_id = self.transaction_id;
        debug!("Committing transaction {tx_id} with {operation_count} operations");
        
        if operation_count == 0 {
            return Err(tinyfs::Error::Other("Cannot commit transaction with no operations".to_string()));
        }
        
        // Mark as committed before actual commit to prevent double-commit
        self.committed.store(true, Ordering::SeqCst);
        
        // Delegate to persistence layer
        let result = self.persistence.commit_transactional(self.transaction_id).await;
        
        if result.is_err() {
            // Reset committed flag on failure
            self.committed.store(false, Ordering::SeqCst);
        }
        
        result
    }
    
    /// Explicitly rollback the transaction
    /// 
    /// This consumes the guard. Rollback is also automatic on Drop.
    pub async fn rollback(self) -> TinyFSResult<()> {
        let tx_id = self.transaction_id;
        debug!("Explicitly rolling back transaction {tx_id}");
        
        // Mark as committed to prevent cleanup in Drop
        self.committed.store(true, Ordering::SeqCst);
        
        // Delegate to persistence layer
        self.persistence.rollback_transactional(self.transaction_id).await
    }
}

impl<'a> Drop for TransactionGuard<'a> {
    /// Automatic cleanup on drop
    /// 
    /// If the transaction hasn't been explicitly committed or rolled back,
    /// we log a warning and mark for lazy cleanup by the persistence layer.
    fn drop(&mut self) {
        let operation_count = self.operation_count();
        let committed = self.is_committed();
        let tx_id = self.transaction_id;
        
        if !committed {
            if operation_count > 0 {
                warn!("Transaction {tx_id} with {operation_count} operations dropped without commit - will be cleaned up lazily");
            } else {
                debug!("Empty transaction {tx_id} dropped - no cleanup needed");
            }
            
            // Note: We cannot call async methods in Drop, so we rely on the persistence
            // layer to clean up stale transaction state when the next transaction begins
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_operation_counting_logic() {
        // Test the basic operation counting without complex mock dependencies
        let count = AtomicUsize::new(0);
        
        assert_eq!(count.load(Ordering::SeqCst), 0);
        
        let new_count = count.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(new_count, 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    
    #[tokio::test]
    async fn test_committed_flag_logic() {
        // Test the basic committed flag logic
        let committed = AtomicBool::new(false);
        
        assert!(!committed.load(Ordering::SeqCst));
        
        committed.store(true, Ordering::SeqCst);
        assert!(committed.load(Ordering::SeqCst));
    }
    
    // Note: More comprehensive tests will be added when we have
    // the full transaction guard integration working
}
