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

    /// Get a reference to the secondary control filesystem
    /// This allows read-only commands to access transaction metadata
    pub fn control_fs(&self) -> &FS {
        &self.control_fs
    }

    /// Get a mutable reference to the secondary control filesystem
    /// This allows steward to perform control filesystem operations
    pub fn control_fs_mut(&mut self) -> &mut FS {
        &mut self.control_fs
    }

    /// Get the path to the data filesystem (for commands that need direct access)
    pub fn data_path(&self) -> String {
        crate::get_data_path(&std::path::Path::new(&self.pond_path))
            .to_string_lossy()
            .to_string()
    }

    /// Get the pond path
    pub fn pond_path(&self) -> &str {
        &self.pond_path
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
        // Get the current version from the data filesystem's Delta table
        let data_path_str = self.data_path();
        let delta_manager = tlogfs::DeltaTableManager::new();
        
        match delta_manager.get_table(&data_path_str).await {
            Ok(table) => {
                let current_version = table.version();
                // The transaction sequence is the current version 
                // (which should be the version we just committed to)
                Ok(current_version as u64)
            }
            Err(_) => {
                // If we can't get the table, assume this is the first transaction
                Ok(0)
            }
        }
    }

    /// Record transaction metadata in the control filesystem
    /// Creates a file at /txn/${txn_seq} with transaction details
    async fn record_transaction_metadata(&mut self, txn_seq: u64) -> Result<(), StewardError> {
        diagnostics::log_debug!("Recording transaction metadata for sequence", txn_seq: txn_seq);
        
        // Create the transaction metadata file path
        let txn_path = format!("/txn/{}", txn_seq);
        
        // For now, create an empty file as requested
        // TODO: Serialize actual transaction metadata
        let empty_content: Vec<u8> = vec![];
        
        // CRITICAL FIX: Ensure /txn directory exists BEFORE starting transaction
        // This prevents directory/file conflicts in the transaction metadata system
        self.ensure_txn_directory_exists().await?;
        
        // Begin transaction on control filesystem
        self.control_fs.begin_transaction().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Get root directory of control filesystem
        let control_root = self.control_fs.root().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Create the transaction metadata file (empty for now)
        // Since /txn directory is guaranteed to exist, this should always succeed
        control_root.create_file_path(&txn_path, &empty_content).await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Commit the control filesystem transaction
        self.control_fs.commit().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        diagnostics::log_debug!("Transaction metadata recorded at path", txn_path: txn_path);
        Ok(())
    }

    /// Ensure the /txn directory exists in the control filesystem
    /// This is called outside transaction context to avoid directory/file conflicts
    async fn ensure_txn_directory_exists(&mut self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Ensuring /txn directory exists in control filesystem");
        
        // Get root directory of control filesystem
        let control_root = self.control_fs.root().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Check if /txn directory exists - if not, create it
        match control_root.open_dir_path("/txn").await {
            Ok(_) => {
                // Directory exists, nothing to do
                diagnostics::log_debug!("Directory /txn already exists in control filesystem");
                Ok(())
            },
            Err(_) => {
                // Directory doesn't exist, create it in its own transaction
                diagnostics::log_debug!("Creating /txn directory in control filesystem");
                
                // Begin transaction just for directory creation
                self.control_fs.begin_transaction().await
                    .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create the directory
                control_root.create_dir_path("/txn").await
                    .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Commit the directory creation
                self.control_fs.commit().await
                    .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                diagnostics::log_debug!("Successfully created /txn directory in control filesystem");
                Ok(())
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_ship_creation() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Test Ship creation
        let ship = Ship::new(&pond_path).await.expect("Failed to create ship");
        
        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);
        
        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");
        
        // Verify ship provides access to data filesystem
        let _data_fs = ship.data_fs();
        
        // Test that pond path is stored correctly
        assert_eq!(ship.pond_path, pond_path.to_string_lossy().to_string());
    }

    #[tokio::test]
    async fn test_ship_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::new(&pond_path).await.expect("Failed to create ship");
        
        // Begin transaction on data filesystem
        ship.data_fs().begin_transaction().await.expect("Failed to begin transaction");
        
        // Commit through steward (this should work even with no operations)
        ship.commit_transaction().await.expect("Failed to commit transaction");
    }

    #[test]
    fn test_path_helpers() {
        let pond_path = std::path::Path::new("/test/pond");
        
        let data_path = get_data_path(pond_path);
        let control_path = get_control_path(pond_path);
        
        assert_eq!(data_path, pond_path.join("data"));
        assert_eq!(control_path, pond_path.join("control"));
    }
}
