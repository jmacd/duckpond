//! Ship - The main steward struct that orchestrates primary and secondary filesystems

use crate::{get_control_path, get_data_path, StewardError, TxDesc, RecoveryResult};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use tinyfs::FS;
use tlogfs::OpLogPersistence;

/// Ship manages both a primary "data" filesystem and a secondary "control" filesystem
/// It provides the main interface for pond operations while handling post-commit actions
pub struct Ship {
    /// Primary filesystem for user data
    data_fs: FS,
    /// Secondary filesystem for steward control and transaction metadata
    control_fs: FS,
    /// Direct access to data persistence layer for metadata operations
    data_persistence: OpLogPersistence,
    /// Direct access to control persistence layer for transaction operations
    control_persistence: OpLogPersistence,
    /// Path to the pond root
    pond_path: String,
    /// Current transaction descriptor (if any)
    current_tx_desc: Option<TxDesc>,
}

impl Ship {
    /// Initialize a completely new pond with proper transaction #1.
    /// 
    /// This is the standard way to create a new pond. It:
    /// 1. Creates the filesystem infrastructure (data and control directories)
    /// 2. Initializes the control filesystem with /txn directory structure  
    /// 3. Creates the initial data transaction #1 that every pond must have
    /// 4. Records the data transaction metadata in control FS /txn/1
    /// 
    /// This is what the `pond init` command uses internally.
    /// 
    /// Use `open_existing_pond()` to work with ponds that already exist.
    pub async fn initialize_new_pond<P: AsRef<Path>>(pond_path: P, init_args: Vec<String>) -> Result<Self, StewardError> {
        // Create infrastructure  
        let mut ship = Self::create_infrastructure(pond_path).await?;
        
        // Initialize control filesystem with /txn directory (control FS transaction #1)
        ship.initialize_control_filesystem().await?;
        
        // Create the mandatory data transaction #1 for the pond init
        ship.begin_transaction_with_args(init_args).await?;
        
        // Initialize the root directory in the transaction context (without committing)
        ship.data_persistence.initialize_root_directory().await
            .map_err(StewardError::DataInit)?;
        
        // Get the root directory (should now work since we initialized it)
        let data_root = ship.data_fs().root().await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
        // Create initial pond directory structure (this generates actual filesystem operations)
        // This is what gets committed as the pond initialization transaction
        data_root.create_dir_path("/data").await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Commit the init transaction (this will record metadata in control FS transaction #2)
        ship.commit_transaction().await?;
        
        Ok(ship)
    }

    /// Initialize the control filesystem with its directory structure.
    /// This is control FS transaction #1 - creates root directory and /txn directory.
    /// Called during pond initialization before any data transactions.
    async fn initialize_control_filesystem(&mut self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Initializing control filesystem with directory structure");
        
        // Begin control FS transaction #1
        self.control_fs.begin_transaction().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Initialize the root directory in the transaction context (without committing)
        self.control_persistence.initialize_root_directory().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::from(e)))?;
        
        // Get/create root directory of control filesystem
        let control_root = self.control_fs.root().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Create the /txn directory for transaction metadata
        control_root.create_dir_path("/txn").await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Commit control FS transaction #1
        self.control_fs.commit().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        diagnostics::log_debug!("Control filesystem initialized with directory structure");
        Ok(())
    }
    
    /// Open an existing, properly initialized pond.
    /// 
    /// This assumes the pond already exists and has been properly initialized
    /// (i.e., it has /txn/1 and subsequent transactions).
    /// 
    /// Use `initialize_new_pond()` to create new ponds.
    pub async fn open_existing_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Self::create_infrastructure(pond_path).await
    }
    
    /// Internal method to create just the filesystem infrastructure.
    /// 
    /// This creates the data and control directories and initializes tlogfs instances,
    /// but does NOT create any transactions. It's used internally by both
    /// initialize_new_pond() and open_existing_pond().
    async fn create_infrastructure<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        let pond_path_str = pond_path.as_ref().to_string_lossy().to_string();
        let data_path = get_data_path(pond_path.as_ref());
        let control_path = get_control_path(pond_path.as_ref());

        diagnostics::log_info!("Initializing Ship at pond: {pond_path}", pond_path: pond_path_str);
        
        // Create directories if they don't exist
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let data_path_str = data_path.to_string_lossy().to_string();
        let control_path_str = control_path.to_string_lossy().to_string();

        // Initialize data filesystem with direct persistence access
        let data_persistence = tlogfs::OpLogPersistence::new(&data_path_str)
            .await
            .map_err(StewardError::DataInit)?;
        let data_fs = tinyfs::FS::with_persistence_layer(data_persistence.clone()).await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Initialize control filesystem (store persistence layer for direct access)
        let control_persistence = tlogfs::OpLogPersistence::new(&control_path_str)
            .await
            .map_err(StewardError::ControlInit)?;
        let control_fs = tinyfs::FS::with_persistence_layer(control_persistence.clone()).await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;

        diagnostics::log_debug!("Ship infrastructure initialized successfully");
        
        Ok(Ship {
            data_fs,
            control_fs,
            data_persistence,
            control_persistence,
            pond_path: pond_path_str,
            current_tx_desc: None,
        })
    }

    /// Begin a transaction with command arguments for metadata tracking
    pub async fn begin_transaction_with_args(&mut self, args: Vec<String>) -> Result<(), StewardError> {
        let args_debug = format!("{:?}", args);
        diagnostics::log_debug!("Beginning transaction with args", args: args_debug);
        
        // Store transaction descriptor
        self.current_tx_desc = Some(TxDesc::new(args));
        
        // Begin transaction on data filesystem
        self.data_fs.begin_transaction().await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        Ok(())
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

    /// Get a reference to the data persistence layer (for specialized operations like FileSeries)
    pub fn data_persistence(&self) -> &OpLogPersistence {
        &self.data_persistence
    }

    /// Get the pond path
    pub fn pond_path(&self) -> &str {
        &self.pond_path
    }

    /// Commit a transaction and handle post-commit actions
    /// 
    /// This commits the data filesystem transaction with metadata for crash recovery
    /// and then records transaction metadata in the control filesystem
    pub async fn commit_transaction(&mut self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Ship committing transaction with metadata");

        // Prepare transaction metadata for crash recovery
        let tx_metadata = if let Some(ref tx_desc) = self.current_tx_desc {
            HashMap::from([
                ("steward_tx_args".to_string(), serde_json::Value::String(tx_desc.to_json()?)),
                ("steward_recovery_needed".to_string(), serde_json::Value::Bool(true)),
                ("steward_timestamp".to_string(), serde_json::Value::Number(
                    serde_json::Number::from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                    )
                )),
            ])
        } else {
            HashMap::new()
        };

        // Commit the data filesystem transaction WITH METADATA
        // We need to access the underlying persistence layer directly
        self.commit_data_fs_with_metadata(tx_metadata).await?;
        
        // Get the transaction version number from the committed data filesystem
        let txn_version = self.get_committed_transaction_version().await?;
        
        // Write transaction metadata to control filesystem
        self.record_transaction_metadata(txn_version).await?;
        
        // For debugging: print the transaction version
        diagnostics::log_info!("Transaction committed with version", txn_version: txn_version);
        
        // Clear current transaction descriptor
        self.current_tx_desc = None;
        
        diagnostics::log_info!("Transaction committed successfully", transaction_version: txn_version);
        Ok(())
    }

    /// Commit data filesystem with metadata for crash recovery
    async fn commit_data_fs_with_metadata(
        &mut self, 
        metadata: HashMap<String, serde_json::Value>
    ) -> Result<(), StewardError> {
        // Use the direct persistence layer access to commit with metadata
        self.data_persistence.commit_with_metadata(Some(metadata)).await
            .map_err(|e| StewardError::DataInit(e))?;
        
        Ok(())
    }

    /// Get the committed transaction version number
    /// Uses the current Delta Lake table version directly
    async fn get_committed_transaction_version(&self) -> Result<u64, StewardError> {
        // Get the current version from the data filesystem's Delta table
        let data_path_str = self.data_path();
        
        // Force fresh table read to avoid any caching issues
        // Create a fresh table builder to ensure we get latest version
        let table_builder = deltalake::DeltaTableBuilder::from_uri(&data_path_str);
        
        match table_builder.load().await {
            Ok(table) => {
                let current_version = table.version();
                // Use Delta Lake version directly - no artificial mapping
                diagnostics::debug!("Transaction version from Delta Lake", version: current_version);
                Ok(current_version as u64)
            }
            Err(error) => {
                // Check if this is a "table doesn't exist" error vs. other errors
                let error_msg = error.to_string();
                if self.is_table_not_found_error(&error_msg) {
                    // This should not happen - get_committed_transaction_sequence is called AFTER commit
                    // If we're calling this and there's no table, something is wrong
                    diagnostics::error!("get_committed_transaction_sequence called but no Delta table exists at {data_path_str}");
                    Err(StewardError::DeltaLake(format!(
                        "Logic error: transaction sequence requested but no Delta table exists at {}",
                        data_path_str
                    )))
                } else {
                    // This is a real error that should be propagated (permission, corruption, etc.)
                    diagnostics::error!("Failed to access Delta table at {data_path_str}: {error_msg}");
                    Err(StewardError::DeltaLake(format!(
                        "Cannot determine transaction sequence: failed to access Delta table at {}: {}",
                        data_path_str, error_msg
                    )))
                }
            }
        }
    }

    /// Helper to distinguish "table not found" from other Delta table errors
    fn is_table_not_found_error(&self, error_msg: &str) -> bool {
        // Common patterns for "table doesn't exist" errors
        error_msg.contains("not found") ||
        error_msg.contains("does not exist") ||
        error_msg.contains("No such file or directory") ||
        error_msg.contains("The system cannot find the path specified") ||
        // Delta-specific patterns
        error_msg.contains("Not a Delta table") ||
        error_msg.contains("Unable to find Delta table") ||
        // Object store patterns
        error_msg.contains("container not found") ||
        error_msg.contains("key not found")
    }

    /// Record transaction metadata in the control filesystem
    /// Creates a file at /txn/${txn_version} with transaction details as JSON
    async fn record_transaction_metadata(&mut self, txn_version: u64) -> Result<(), StewardError> {
        diagnostics::log_debug!("Recording transaction metadata", version: txn_version);
        diagnostics::log_debug!("Recording transaction metadata for version", txn_version: txn_version);
        
        // Create the transaction metadata file path
        let txn_path = format!("/txn/{}", txn_version);
        diagnostics::log_debug!("Transaction metadata path", path: &txn_path);
        diagnostics::log_debug!("Transaction metadata path", txn_path: txn_path);
        
        // Serialize transaction descriptor to JSON with trailing newline
        // Ensure we have transaction descriptor for metadata recording
        let tx_desc = self.current_tx_desc.as_ref()
            .ok_or_else(|| StewardError::DataInit(tlogfs::TLogFSError::ArrowMessage(
                "Transaction descriptor required for commit".to_string()
            )))?;
        
        let mut json_content = tx_desc.to_json()?;
        json_content.push('\n');
        let json_len = json_content.len();
        let tx_content = json_content.into_bytes();
        let bytes_len = tx_content.len();
        
        diagnostics::log_debug!("Transaction content", json_len: json_len, bytes_len: bytes_len);
        
        // Begin transaction on control filesystem
        self.control_fs.begin_transaction().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Get root directory of control filesystem
        let control_root = self.control_fs.root().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Create the transaction metadata file with JSON content using streaming
        {
            let (_, mut writer) = control_root.create_file_path_streaming(&txn_path).await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
            use tokio::io::AsyncWriteExt;
            writer.write_all(&tx_content).await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("IO error: {}", e)))))?;
            writer.shutdown().await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("IO error: {}", e)))))?;
        }
        
        diagnostics::log_debug!("Transaction file created", txn_path: &txn_path);
        
        // Commit the control filesystem transaction
        self.control_fs.commit().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        diagnostics::log_debug!("Transaction metadata recorded at path", txn_path: txn_path);
        Ok(())
    }

    /// Read transaction metadata from control filesystem
    pub async fn read_transaction_metadata(&self, txn_version: u64) -> Result<Option<TxDesc>, StewardError> {
        let txn_path = format!("/txn/{}", txn_version);
        
        // Get root directory of control filesystem
        let control_root = self.control_fs.root().await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Try to read the transaction file using the convenient buffer helper
        match control_root.read_file_path_to_vec(&txn_path).await {
            Ok(content) => {
                if content.is_empty() {
                    return Err(StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Transaction file {} is empty", txn_path)))));
                }
                let json_str = String::from_utf8_lossy(&content);
                let tx_desc = TxDesc::from_json(&json_str)?;
                Ok(Some(tx_desc))
            }
            Err(tinyfs::Error::NotFound(_)) => {
                // File doesn't exist
                Ok(None)
            }
            Err(e) => {
                // Other error - propagate it
                Err(StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))
            }
        }
    }

    /// Get the next transaction sequence number

    /// Check if recovery is needed by verifying transaction sequence consistency
    /// Returns error if recovery is needed
    pub async fn check_recovery_needed(&self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Checking if recovery is needed");
        
        let data_path_str = self.data_path();
        
        let current_version = match deltalake::open_table(&data_path_str).await {
            Ok(table) => table.version() as u64,
            Err(error) => {
                let error_msg = error.to_string();
                if self.is_table_not_found_error(&error_msg) {
                    // No data table yet, no recovery needed
                    diagnostics::debug!("No data table found at {data_path_str}, no recovery needed");
                    return Ok(());
                } else {
                    // This is a real error - we can't check recovery state
                    diagnostics::error!("Failed to access Delta table during recovery check at {data_path_str}: {error_msg}");
                    return Err(StewardError::DeltaLake(format!(
                        "Cannot check recovery status: failed to access Delta table at {}: {}",
                        data_path_str, error_msg
                    )));
                }
            }
        };
        
        // Check if control metadata exists for the committed transaction
        // Use Delta Lake version directly for the metadata file
        diagnostics::log_debug!("Checking for transaction metadata", version: current_version);
        diagnostics::log_debug!("Checking for transaction metadata", current_version: current_version);
        if self.read_transaction_metadata(current_version).await?.is_none() {
            diagnostics::log_debug!("Missing transaction metadata", version: current_version);
            diagnostics::log_debug!("Missing transaction metadata", version: current_version);
            return Err(StewardError::RecoveryNeeded { sequence: current_version });
        }
        
        diagnostics::log_debug!("No recovery needed");
        Ok(())
    }

    /// Perform crash recovery
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        diagnostics::log_info!("Starting crash recovery process");
        
        let data_path_str = self.data_path();
        
        let data_table = match deltalake::open_table(&data_path_str).await {
            Ok(table) => table,
            Err(_) => {
                diagnostics::log_debug!("No data table found, no recovery needed");
                return Ok(RecoveryResult {
                    recovered_count: 0,
                    was_needed: false,
                });
            }
        };
        
        let current_version = data_table.version() as u64;
        let mut recovered_count = 0;
        
        // Check for missing control metadata from version 0 to current version
        // Use Delta Lake versions directly
        for version in 0..=current_version {
            if self.read_transaction_metadata(version).await?.is_none() {
                diagnostics::log_info!("Found missing control metadata for version", version: version);
                
                // Try to recover from data FS commit metadata
                self.recover_transaction_metadata_from_data_fs(version).await?;
                recovered_count += 1;
            }
        }
        
        diagnostics::log_info!("Crash recovery completed", recovered_count: recovered_count);
        Ok(RecoveryResult {
            recovered_count,
            was_needed: recovered_count > 0,
        })
    }

    /// Recover transaction metadata from data FS commit metadata
    async fn recover_transaction_metadata_from_data_fs(&mut self, txn_version: u64) -> Result<(), StewardError> {
        diagnostics::log_info!("Recovering transaction metadata from data FS commit", version: txn_version);
        
        // Get commit metadata from data filesystem
        let commit_metadata = self.get_data_fs_commit_metadata(txn_version).await?;
        
        if let Some(metadata) = commit_metadata {
            // Extract steward metadata from commit info
            if let Some(steward_tx_args) = metadata.get("steward_tx_args") {
                if let Some(tx_args_json) = steward_tx_args.as_str() {
                    // Parse the stored transaction descriptor
                    let recovered_tx_desc = TxDesc::from_json(tx_args_json)?;
                    
                    // Set this as current transaction descriptor for recording
                    self.current_tx_desc = Some(recovered_tx_desc);
                    
                    // Record the recovered metadata in control filesystem
                    self.record_transaction_metadata(txn_version).await?;
                    
                    // Clear the descriptor
                    self.current_tx_desc = None;
                    
                    diagnostics::log_info!("Successfully recovered transaction metadata", version: txn_version);
                    return Ok(());
                }
            }
        }
        
        // If we get here, recovery failed - this should not happen in a working system
        return Err(StewardError::DataInit(tlogfs::TLogFSError::ArrowMessage(
            format!("Failed to recover metadata for transaction {}: no steward metadata found in Delta Lake commit", txn_version)
        )));
    }

    /// Get commit metadata from data filesystem for a specific version
    async fn get_data_fs_commit_metadata(&self, version: u64) -> Result<Option<HashMap<String, serde_json::Value>>, StewardError> {
        diagnostics::log_debug!("Attempting to get commit metadata", version: version);
        
        // Use the underlying persistence layer's get_commit_metadata method
        let result = self.data_persistence.get_commit_metadata(version).await
            .map_err(|e| StewardError::DataInit(e))?;
        
        match &result {
            Some(metadata) => {
                let metadata_keys_debug = format!("{:?}", metadata.keys().collect::<Vec<_>>());
                diagnostics::log_debug!("Found commit metadata", version: version, metadata_keys: metadata_keys_debug);
            }
            None => {
                diagnostics::log_debug!("No commit metadata found", version: version);
            }
        }
        
        Ok(result)
    }

    /// Execute recovery command
    pub async fn execute_recovery(&mut self) -> Result<RecoveryResult, StewardError> {
        self.recover().await
    }

    /// Initialize a complete pond following the proper initialization pattern
    /// 
    /// ✅ This creates a FULLY INITIALIZED pond with /txn/1 transaction metadata.
    /// 
    /// This method follows the same pattern as the `cmd init` command:
    /// 1. Sets up filesystem infrastructure 
    /// 2. Begins a transaction with provided arguments
    /// 3. Creates the root directory
    /// 4. Commits the transaction, creating /txn/1 metadata
    /// 
    /// After calling this method, the pond is ready for normal operations
    /// and has the required transaction sequence starting from 1.
    /// 
    /// This is the RECOMMENDED way to create a new pond programmatically.
    pub async fn initialize_pond<P: AsRef<Path>>(pond_path: P, init_args: Vec<String>) -> Result<Self, StewardError> {
        // Step 1: Set up filesystem infrastructure
        let mut ship = Self::create_infrastructure(pond_path).await?;
        
        // Step 2: Begin transaction with init arguments  
        ship.begin_transaction_with_args(init_args).await?;
        
        // Step 3: Create initial pond directory structure (this generates actual filesystem operations)
        let data_root = ship.data_fs().root().await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        data_root.create_dir_path("/data").await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // Step 4: Commit transaction - this creates both:
        // - The /data directory operation in the data filesystem (transaction #1)
        // - The /txn/1 metadata file in the control filesystem
        ship.commit_transaction().await?;
        
        diagnostics::log_info!("Pond fully initialized with transaction #1");
        Ok(ship)
    }

    /// Load an existing pond that was previously initialized
    /// 
    /// ✅ This connects to an EXISTING pond that already has /txn/1 and transaction history.
    /// 
    /// Use this method to:
    /// - Load an existing pond for normal operations
    /// - Load a pond for recovery operations  
    /// - Load a pond created by the `cmd init` command
    /// 
    /// This method will fail if the pond doesn't exist or is corrupted.
    /// For recovery scenarios, use this method followed by recovery operations.
    pub async fn load_existing_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Self::create_infrastructure(pond_path).await
    }
}

// Implement Debug for Ship
impl std::fmt::Debug for Ship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ship")
            .field("pond_path", &self.pond_path)
            .field("current_tx_desc", &self.current_tx_desc)
            .field("has_data_persistence", &true)
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

        // Use production initialization code (same as pond init)
        let ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to initialize pond");
        
        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);
        
        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");
        
        // Verify ship provides access to data filesystem
        let _data_fs = ship.data_fs();
        
        // Test that pond path is stored correctly
        assert_eq!(ship.pond_path, pond_path.to_string_lossy().to_string());
        
        // Test that we can open the same pond (like production commands do)
        let _opened_ship = Ship::open_existing_pond(&pond_path).await.expect("Should be able to open existing pond");
    }

    #[tokio::test]
    async fn test_ship_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use the same constructor as production (pond init)
        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to initialize pond");
        
        // Begin a second transaction with test arguments
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.begin_transaction_with_args(args).await.expect("Failed to begin transaction");
        
        // Do some filesystem operation to ensure the transaction has operations to commit
        let root = ship.data_fs().root().await.expect("Failed to get root");
        tinyfs::async_helpers::convenience::create_file_path(&root, "/test.txt", b"test content").await.expect("Failed to create test file");
        
        // Commit through steward
        ship.commit_transaction().await.expect("Failed to commit transaction");
    }

    #[tokio::test]
    async fn test_transaction_metadata_persistence() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to initialize pond");
        
        // Begin a second transaction with specific args  
        let args = vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()];
        ship.begin_transaction_with_args(args.clone()).await.expect("Failed to begin transaction");
        
        // Create actual filesystem operations (required for commit)
        let data_root = ship.data_fs().root().await.expect("Failed to get data root");
        tinyfs::async_helpers::convenience::create_file_path(&data_root, "/file2.txt", b"copied content").await.expect("Failed to create file");
        
        // Commit transaction
        ship.commit_transaction().await.expect("Failed to commit transaction");
        
        // The second transaction should be version 1 (init was version 0)
        let tx_desc = ship.read_transaction_metadata(1).await.expect("Failed to read metadata")
            .expect("Transaction metadata should exist");
        
        assert_eq!(tx_desc.args, args);
        assert_eq!(tx_desc.command_name(), Some("copy"));
    }

    #[test]
    fn test_path_helpers() {
        let pond_path = std::path::Path::new("/test/pond");
        
        let data_path = get_data_path(pond_path);
        let control_path = get_control_path(pond_path);
        
        assert_eq!(data_path, pond_path.join("data"));
        assert_eq!(control_path, pond_path.join("control"));
    }

    #[test]
    fn test_tx_desc_serialization() {
        let tx_desc = TxDesc::new(vec!["copy".to_string(), "file1".to_string(), "file2".to_string()]);
        
        let json = tx_desc.to_json().expect("Should serialize to JSON");
        let deserialized = TxDesc::from_json(&json).expect("Should deserialize from JSON");
        
        assert_eq!(tx_desc.args, deserialized.args);
        assert_eq!(tx_desc.command_name(), Some("copy"));
    }

    #[tokio::test]
    async fn test_normal_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to initialize pond");
        
        // Check that recovery is not needed after init
        ship.check_recovery_needed().await.expect("Recovery should not be needed after init");
        
        // Begin a second transaction with arguments
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.begin_transaction_with_args(args.clone()).await.expect("Failed to begin transaction");
        
        // Do some operation on data filesystem
        let data_root = ship.data_fs().root().await.expect("Failed to get data root");
        tinyfs::async_helpers::convenience::create_file_path(&data_root, "/test.txt", b"test content").await.expect("Failed to create file");
        
        // Commit through steward
        ship.commit_transaction().await.expect("Failed to commit transaction");
        
        // Check that recovery is still not needed after successful commit
        ship.check_recovery_needed().await.expect("Recovery should not be needed after successful commit");
        
        // Verify transaction metadata was recorded (should be version 1 after init version 0)
        let tx_desc = ship.read_transaction_metadata(1).await.expect("Failed to read metadata")
            .expect("Transaction metadata should exist");
        
        assert_eq!(tx_desc.args, args);
        assert_eq!(tx_desc.command_name(), Some("test"));
    }

    #[tokio::test]
    async fn test_crash_recovery_scenario() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // FIRST: Create a properly initialized pond (like cmd init does)
        {
            let _ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to create ship");
        }

        // SECOND: Simulate crash scenario during the second transaction (version 1)
        {
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to create ship");
            
            // Begin the second transaction (this will be version 1)
            let args = vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()];
            ship.begin_transaction_with_args(args.clone()).await.expect("Failed to begin transaction");
            
            // Do operation on data filesystem
            let data_root = ship.data_fs().root().await.expect("Failed to get data root");
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/file1.txt", b"content1").await.expect("Failed to create file");
            
            // Commit the data filesystem WITH metadata (like commit_transaction would do)
            // but then simulate crash before writing control filesystem metadata
            
            // Prepare transaction metadata for crash recovery (like commit_transaction does)
            let tx_desc = ship.current_tx_desc.as_ref().expect("Transaction descriptor should exist");
            let mut tx_metadata = HashMap::new();
            tx_metadata.insert("steward_recovery_needed".to_string(), serde_json::Value::Bool(true));
            tx_metadata.insert("steward_tx_args".to_string(), serde_json::Value::String(tx_desc.to_json().expect("Failed to serialize tx args")));
            
            ship.commit_data_fs_with_metadata(tx_metadata).await.expect("Failed to commit data fs with metadata");
            
            // DON'T call record_transaction_metadata() to simulate crash
            // The data filesystem has the commit with steward metadata, but /txn/1 is missing
        }
        
        // THIRD: Create a new ship (simulating restart after crash)
        {
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to create ship after crash");
            
            // Check recovery is needed - this should fail for version 1
            let result = ship.check_recovery_needed().await;
            assert!(result.is_err(), "Should detect that recovery is needed");
            
            if let Err(StewardError::RecoveryNeeded { sequence }) = result {
                assert_eq!(sequence, 1, "Should need recovery for version 1");
            } else {
                panic!("Should have RecoveryNeeded error");
            }
            
            // Perform recovery
            let recovery_result = ship.recover().await.expect("Failed to recover");
            assert_eq!(recovery_result.recovered_count, 1, "Should have recovered 1 transaction");
            assert!(recovery_result.was_needed, "Recovery should have been needed");
            
            // Now recovery should not be needed
            ship.check_recovery_needed().await.expect("Recovery should not be needed after recovery");
            
            // Verify transaction metadata was recovered
            let tx_desc = ship.read_transaction_metadata(1).await.expect("Failed to read metadata")
                .expect("Transaction metadata should exist after recovery");
            
            // Should have recovered the actual command args from the commit metadata
            assert_eq!(tx_desc.command_name(), Some("copy"));
            assert_eq!(tx_desc.args, vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()]);
        }
    }

    #[tokio::test]
    async fn test_multiple_transaction_recovery() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["test".to_string()]).await.expect("Failed to create ship");
        
        // Commit several transactions normally
        for i in 1..=3 {
            let args = vec!["test".to_string(), format!("operation{}", i)];
            ship.begin_transaction_with_args(args).await.expect("Failed to begin transaction");
            
            let data_root = ship.data_fs().root().await.expect("Failed to get data root");
            tinyfs::async_helpers::convenience::create_file_path(&data_root, &format!("/file{}.txt", i), format!("content{}", i).as_bytes())
                .await.expect("Failed to create file");
            
            ship.commit_transaction().await.expect("Failed to commit transaction");
        }
        
        // The key test is that recovery works with existing completed transactions
        let recovery_result = ship.recover().await.expect("Failed to recover");
        
        // Since all transactions were completed normally, no recovery should be needed
        assert_eq!(recovery_result.recovered_count, 0, "No recovery should be needed for completed transactions");
        assert!(!recovery_result.was_needed, "Recovery should not have been needed");
        
        ship.check_recovery_needed().await.expect("Recovery should not be needed after recovery");
    }

    #[tokio::test]
    async fn test_recovery_command_interface() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["test".to_string()]).await.expect("Failed to create ship");
        
        // Test execute_recovery when no recovery is needed
        let recovery_result = ship.execute_recovery().await.expect("Failed to execute recovery");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);
    }

    #[tokio::test]
    async fn test_crash_recovery_with_metadata_extraction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Step 1: Initialize pond using production code (pond init)
        {
            let _ship = Ship::initialize_new_pond(&pond_path, vec!["init".to_string()]).await.expect("Failed to initialize pond");
        }

        // Step 2: Create a transaction with metadata that commits to data FS but crashes before control FS
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to open existing pond");
            
            // Begin transaction with specific command args
            let copy_args = vec!["pond".to_string(), "copy".to_string(), "source.txt".to_string(), "dest.txt".to_string()];
            ship.begin_transaction_with_args(copy_args.clone()).await.expect("Failed to begin transaction");
            
            // Do actual file operation
            let data_root = ship.data_fs().root().await.expect("Failed to get data root");
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/dest.txt", b"copied content").await.expect("Failed to create file");
            
            // SIMULATE CRASH: Commit data FS with metadata but don't record in control FS
            let metadata = HashMap::from([
                ("steward_tx_args".to_string(), serde_json::Value::String(
                    ship.current_tx_desc.as_ref().unwrap().to_json().unwrap()
                )),
                ("steward_recovery_needed".to_string(), serde_json::Value::Bool(true)),
                ("steward_timestamp".to_string(), serde_json::Value::Number(
                    serde_json::Number::from(1234567890u64)
                )),
            ]);
            
            // Commit data filesystem with metadata (this would normally be followed by control FS update)
            ship.commit_data_fs_with_metadata(metadata).await.expect("Failed to commit data FS with metadata");
            
            // DON'T call the full commit_transaction() - this simulates the crash
            // The data FS is committed with metadata, but control FS has no /txn/2 file
        }

        // Step 3: Recovery after crash using production code
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to open existing pond for recovery");
            
            // Check that recovery is needed
            let check_result = ship.check_recovery_needed().await;
            assert!(check_result.is_err(), "Should detect that recovery is needed");
            
            if let Err(StewardError::RecoveryNeeded { sequence }) = check_result {
                assert_eq!(sequence, 1, "Should need recovery for version 1 (the copy command)");
            } else {
                panic!("Expected RecoveryNeeded error, got: {:?}", check_result);
            }
            
            // Perform recovery
            let recovery_result = ship.recover().await.expect("Recovery should succeed");
            assert_eq!(recovery_result.recovered_count, 1, "Should recover exactly 1 transaction");
            assert!(recovery_result.was_needed, "Recovery should have been needed");
            
            // Verify recovery is no longer needed
            ship.check_recovery_needed().await.expect("Recovery should not be needed after successful recovery");
            
            // Verify the recovered transaction metadata contains the original command args
            let recovered_tx = ship.read_transaction_metadata(1).await.expect("Failed to read recovered metadata")
                .expect("Recovered transaction metadata should exist");
            
            assert_eq!(recovered_tx.args, vec!["pond".to_string(), "copy".to_string(), "source.txt".to_string(), "dest.txt".to_string()]);
            assert_eq!(recovered_tx.command_name(), Some("pond"));
            
            // Verify the data file still exists (data wasn't lost in crash)
            let data_root = ship.data_fs().root().await.expect("Failed to get data root");
            let reader = data_root.async_reader_path("/dest.txt").await.expect("File should exist after recovery");
            let file_content = tinyfs::buffer_helpers::read_all_to_vec(reader).await.expect("Failed to read file content");
            assert_eq!(file_content, b"copied content");
        }
    }

    #[tokio::test]
    async fn test_no_recovery_needed_for_consistent_state() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::initialize_new_pond(&pond_path, vec!["test".to_string()]).await.expect("Failed to create ship");
        
        // Do normal complete transactions
        for i in 1..=3 {
            let args = vec!["pond".to_string(), "mkdir".to_string(), format!("/dir{}", i)];
            ship.begin_transaction_with_args(args).await.expect("Failed to begin transaction");
            
            let data_root = ship.data_fs().root().await.expect("Failed to get data root");
            data_root.create_dir_path(&format!("/dir{}", i)).await.expect("Failed to create directory");
            
            ship.commit_transaction().await.expect("Failed to commit transaction");
        }
        
        // Check that no recovery is needed
        ship.check_recovery_needed().await.expect("No recovery should be needed for consistent state");
        
        // Run recovery anyway - should be no-op
        let recovery_result = ship.recover().await.expect("Recovery should succeed even when not needed");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);
        
                // State should still be consistent
        ship.check_recovery_needed().await.expect("State should remain consistent after no-op recovery");
    }
}
