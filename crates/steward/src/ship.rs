//! Ship - The main steward struct that orchestrates primary and secondary filesystems

use crate::{get_control_path, get_data_path, StewardError};
use anyhow::Result;
use std::path::Path;
use tinyfs::FS;

/// Ship manages both a primary "data" filesystem and a secondary "control" filesystem
/// It provides the main interface for pond operations while handling post-commit actions
pub struct Ship {
    /// Primary filesystem for user data
    data_fs: FS,
    /// Secondary filesystem for steward control and transaction metadata
    control_fs: FS,
    /// Path to the pond root
    pond_path: String,
}

impl Ship {
    /// Create a new Ship instance with both data and control filesystems
    /// 
    /// This will create both ${pond_path}/data and ${pond_path}/control directories
    /// and initialize tlogfs instances in each.
    pub async fn new<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        let pond_path_str = pond_path.as_ref().to_string_lossy().to_string();
        let data_path = get_data_path(pond_path.as_ref());
        let control_path = get_control_path(pond_path.as_ref());

        diagnostics::log_info!("Initializing Ship at pond: {pond_path}", pond_path: pond_path_str);
        
        // Create directories if they don't exist
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let data_path_str = data_path.to_string_lossy().to_string();
        let control_path_str = control_path.to_string_lossy().to_string();

        // Force cache invalidation before creating new filesystem instances
        // This ensures we get fresh data, avoiding race conditions where
        // a previous command's committed data isn't visible to this command
        let temp_delta_manager = tlogfs::DeltaTableManager::new();
        temp_delta_manager.invalidate_table(&data_path_str).await;
        temp_delta_manager.invalidate_table(&control_path_str).await;

        // Initialize data filesystem
        let data_fs = tlogfs::create_oplog_fs(&data_path_str)
            .await
            .map_err(StewardError::DataInit)?;

        // Initialize control filesystem  
        let control_fs = tlogfs::create_oplog_fs(&control_path_str)
            .await
            .map_err(StewardError::ControlInit)?;

        diagnostics::log_debug!("Ship initialized successfully");
        
        Ok(Ship {
            data_fs,
            control_fs,
            pond_path: pond_path_str,
        })
    }

    /// Get a reference to the primary data filesystem
    /// This allows cmd operations to work with the data filesystem directly
    pub fn data_fs(&self) -> &FS {
        &self.data_fs
    }

    /// Get a mutable reference to the primary data filesystem
    /// This allows cmd operations to perform transactions on the data filesystem
    pub fn data_fs_mut(&mut self) -> &mut FS {
        &mut self.data_fs
    }

    /// Get the path to the data filesystem (for commands that need direct access)
    pub fn data_path(&self) -> String {
        crate::get_data_path(&std::path::Path::new(&self.pond_path))
            .to_string_lossy()
            .to_string()
    }

    /// Commit a transaction and handle post-commit actions
    /// 
    /// This commits the data filesystem transaction and then records
    /// transaction metadata in the control filesystem
    pub async fn commit_transaction(&mut self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Ship committing transaction");

        // Commit the data filesystem transaction
        self.data_fs.commit().await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Get the transaction sequence number from the data filesystem
        // TODO: Get actual transaction sequence from committed data
        let txn_seq = self.get_next_transaction_sequence().await?;
        
        // Write transaction metadata to control filesystem
        self.record_transaction_metadata(txn_seq).await?;
        
        diagnostics::log_info!("Transaction committed successfully", transaction_seq: txn_seq);
        Ok(())
    }

    /// Get the next transaction sequence number
    /// For now, this is a placeholder implementation
    async fn get_next_transaction_sequence(&self) -> Result<u64, StewardError> {
        // TODO: Implement proper transaction sequence tracking
        // For now, return a placeholder sequence
        Ok(1)
    }

    /// Record transaction metadata in the control filesystem
    /// Creates a file at /txn/${txn_seq} with transaction details
    async fn record_transaction_metadata(&mut self, txn_seq: u64) -> Result<(), StewardError> {
        diagnostics::log_debug!("Recording transaction metadata for sequence", txn_seq: txn_seq);
        
        // Create the transaction metadata file path
        let txn_path = format!("/txn/{}", txn_seq);
        
        // For now, create an empty file as requested
        // TODO: Serialize actual transaction metadata
        let _empty_content: Vec<u8> = vec![];
        
        // Write to control filesystem
        // TODO: Use proper tlogfs write operations once we determine the API
        // For now, this is a placeholder
        
        diagnostics::log_debug!("Transaction metadata recorded at path", txn_path: txn_path);
        Ok(())
    }

    /// Check if recovery is needed by verifying transaction sequence consistency
    /// Returns true if there are missing /txn/${seq} files that need recovery
    pub async fn needs_recovery(&self) -> Result<bool, StewardError> {
        // TODO: Implement recovery detection logic
        // For now, always return false (no recovery needed)
        Ok(false)
    }

    /// Perform recovery for missing transaction metadata
    /// This is a placeholder implementation as requested
    pub async fn recover(&mut self) -> Result<(), StewardError> {
        diagnostics::log_info!("Starting recovery process");
        
        // TODO: Implement actual recovery logic
        // For now, this is a placeholder that does nothing
        
        diagnostics::log_info!("Recovery completed");
        Ok(())
    }
}

// Implement Debug for Ship
impl std::fmt::Debug for Ship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ship")
            .field("pond_path", &self.pond_path)
            .finish()
    }
}
