//! Ship - The main steward struct that orchestrates primary and secondary filesystems

use crate::{get_control_path, get_data_path, StewardError, TxDesc, RecoveryResult};
use std::collections::HashMap;
use std::path::Path;
use tlogfs::{OpLogPersistence, transaction_guard::TransactionGuard};

/// Ship manages both a primary "data" filesystem and a secondary "control" filesystem
/// It provides the main interface for pond operations while handling post-commit actions
pub struct Ship {
    /// Direct access to data persistence layer for transaction operations
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
    pub async fn initialize_new_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        // Create infrastructure  
        let mut ship = Self::create_infrastructure(pond_path).await?;
        
        // Initialize control filesystem with /txn directory (control FS transaction #1)
        ship.initialize_control_filesystem().await?;
        
        // Pond is now ready - no data transaction needed for initialization
        Ok(ship)
    }

    /// Initialize the control filesystem with its directory structure.
    /// This is control FS transaction #1 - creates root directory and /txn directory.
    /// Called during pond initialization before any data transactions.
    async fn initialize_control_filesystem(&mut self) -> Result<(), StewardError> {
        diagnostics::log_debug!("Initializing control filesystem with directory structure");
        
        // Create additional directories in a separate transaction
        {
            diagnostics::log_debug!("Creating transaction for /txn directory creation");
	    let tx = self.control_persistence.begin().await
		.map_err(|e| StewardError::ControlInit(e))?;
            
            // Get root directory of control filesystem through the transaction guard
            let control_root = tx.root().await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Create the /txn directory for transaction metadata
            diagnostics::log_debug!("Creating /txn directory in control filesystem");
            control_root.create_dir_path("/txn").await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            diagnostics::log_debug!("Committing transaction for /txn directory");
            // Commit the transaction
            tx.commit(None).await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        } // Transaction guard automatically cleaned up here
        
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
        let data_persistence = tlogfs::OpLogPersistence::create(&data_path_str)
            .await
            .map_err(StewardError::DataInit)?;

        // Initialize control filesystem (store persistence layer for direct access)
        let control_persistence = tlogfs::OpLogPersistence::create(&control_path_str)
            .await
            .map_err(StewardError::ControlInit)?;

        diagnostics::log_debug!("Ship infrastructure initialized successfully");
        
        Ok(Ship {
            data_persistence,
            control_persistence,
            pond_path: pond_path_str,
            current_tx_desc: None,
        })
    }

    /// Execute operations within a scoped data filesystem transaction
    /// The transaction commits on Ok(()) return, rolls back on Err() return
    pub async fn with_data_transaction<F, R>(&mut self, args: Vec<String>, f: F) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(&'a TransactionGuard<'a>, &'a tinyfs::FS) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StewardError>> + Send + 'a>>,
    {
        let args_debug = format!("{:?}", args);
        diagnostics::log_debug!("Beginning scoped transaction with args", args: args_debug);
        
        // Store transaction descriptor for metadata
        self.current_tx_desc = Some(TxDesc::new(args));
        
        // Create transaction guard
        let tx = self.data_persistence.begin().await
            .map_err(|e| StewardError::DataInit(e))?;
        
        // Execute the user function with both the transaction guard and filesystem
        let result = f(&tx, &*tx).await;
        
        match result {
            Ok(value) => {
                // Success - commit the data transaction first
                let tx_metadata = self.current_tx_desc.as_ref()
                    .map(|desc| {
                        let mut metadata = std::collections::HashMap::new();
                        // Store full TxDesc as JSON for recovery compatibility
                        let tx_desc_json = desc.to_json().expect("Failed to serialize TxDesc");
                        metadata.insert("steward_tx_args".to_string(), serde_json::Value::String(tx_desc_json));
                        metadata
                    });
                
                // Step 1: Commit data transaction and get version
                let committed_version = tx.commit(tx_metadata).await
                    .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                // Step 2: Intermediate operations would go here in the future
                // (replication, backup, etc.)
                
                // Step 3: Record transaction metadata in control filesystem
                if let Some(version) = committed_version {
                    self.record_transaction_metadata(version).await?;
                    diagnostics::log_info!("Scoped transaction committed with version", txn_version: version);
                } else {
                    diagnostics::log_info!("Read-only scoped transaction completed successfully");
                }
                
                // Clear transaction descriptor
                self.current_tx_desc = None;
                
                Ok(value)
            }
            Err(e) => {
                // Error - transaction guard will auto-rollback on drop
                diagnostics::log_debug!("Scoped transaction failed, auto-rolling back");
                self.current_tx_desc = None;
                Err(e)
            }
        }
    }

    /// Begin a coordinated transaction with command arguments
    /// This starts the multi-step commit process described in the design document
    pub async fn begin_transaction_with_args(&mut self, args: Vec<String>) -> Result<TransactionGuard<'_>, StewardError> {
        // Store transaction descriptor for metadata
        self.current_tx_desc = Some(TxDesc { args });
        
        // 1. Begin Data FS transaction guard
        let tx = self.data_persistence.begin().await
            .map_err(|e| StewardError::DataInit(e))?;
        
        Ok(tx)
    }

    /// Commit the data transaction and return the committed version number
    /// This implements steps 3-4 from the coordination pseudocode
    pub async fn commit_data_transaction(&mut self, tx: TransactionGuard<'_>) -> Result<u64, StewardError> {
        // 3. Success => commit data transaction with metadata
        let tx_metadata = self.current_tx_desc.as_ref()
            .map(|desc| {
                let mut metadata = std::collections::HashMap::new();
                // Store full TxDesc as JSON for recovery compatibility
                let tx_desc_json = desc.to_json().expect("Failed to serialize TxDesc");
                metadata.insert("steward_tx_args".to_string(), serde_json::Value::String(tx_desc_json));
                metadata
            });
        
        let committed_version = tx.commit(tx_metadata).await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        // 4. Return committed transaction number
        match committed_version {
            Some(version) => Ok(version),
            None => Err(StewardError::DataInit(tlogfs::TLogFSError::ArrowMessage(
                "Data transaction was read-only - no version created".to_string()
            )))
        }
    }

    /// Complete the coordinated commit by recording control metadata
    /// This implements steps 6-9 from the coordination pseudocode
    pub async fn commit_transaction_metadata(&mut self, txn_version: u64) -> Result<(), StewardError> {
        // 6. Begin Control FS transaction guard
        // 7. Modify the control FS
        // 8. Commit
        self.record_transaction_metadata(txn_version).await?;
        
        // Clear transaction descriptor
        self.current_tx_desc = None;
        
        // 9. Return
        Ok(())
    }

    /// Execute operations within a scoped control filesystem transaction
    /// The transaction commits on Ok(()) return, rolls back on Err() return
    pub async fn with_control_transaction<F, R>(&mut self, f: F) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(&'a TransactionGuard<'a>, &'a tinyfs::FS) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StewardError>> + Send + 'a>>,
    {
        // Create transaction guard
        let tx = self.control_persistence.begin().await
            .map_err(|e| StewardError::ControlInit(e))?;
        
        // Execute the user function with the transaction guard and filesystem
        let result = f(&tx, &*tx).await;
        
        match result {
            Ok(value) => {
                // Success - commit the control transaction
                tx.commit(None).await
                    .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
                Ok(value)
            }
            Err(e) => {
                // Error - transaction guard will auto-rollback on drop
                Err(e)
            }
        }
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
        
        // Use scoped control filesystem transaction to create metadata file
        self.with_control_transaction(|_tx, control_fs| Box::pin(async move {
            // Get root directory of control filesystem
            let control_root = control_fs.root().await
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
            Ok(())
        })).await?;
        
        diagnostics::log_debug!("Transaction metadata recorded successfully for version", txn_version: txn_version);
        Ok(())
    }

    /// Read transaction metadata from control filesystem
    pub async fn read_transaction_metadata(&mut self, txn_version: u64) -> Result<Option<TxDesc>, StewardError> {
        let txn_path = format!("/txn/{}", txn_version);
        
        // Use the existing control persistence layer with a transaction
        let tx = self.control_persistence.begin().await
            .map_err(|e| StewardError::ControlInit(e))?;
            
        // Get root directory of control filesystem through transaction
        let control_root = tx.root().await
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
    pub async fn check_recovery_needed(&mut self) -> Result<(), StewardError> {
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
        
        // Check if control metadata exists for committed user transactions
        // Version 0 is always the root directory bootstrap (no steward metadata expected)
        if current_version > 0 {
            diagnostics::log_debug!("Checking for transaction metadata", version: current_version);
            if self.read_transaction_metadata(current_version).await?.is_none() {
                diagnostics::log_debug!("Missing transaction metadata", version: current_version);
                return Err(StewardError::RecoveryNeeded { sequence: current_version });
            }
        } else {
            diagnostics::log_debug!("Version 0 is bootstrap, no metadata check needed");
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
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                // Table doesn't exist yet - this is expected for first-time initialization
                diagnostics::log_debug!("No data table found, no recovery needed");
                return Ok(RecoveryResult {
                    recovered_count: 0,
                    was_needed: false,
                });
            }
            Err(e) => {
                // Real system error - filesystem corruption, permissions, I/O failure, etc.
                // These should not be masked as "no recovery needed"
                diagnostics::log_error!("CRITICAL: Failed to access data table during recovery check: {error}", error: e);
                return Err(StewardError::DeltaLake(format!("Recovery check failed: {}", e)));
            }
        };
        
        let current_version = data_table.version() as u64;
        let mut recovered_count = 0;
        
        // Check for missing control metadata from version 1 to current version
        // Skip version 0 as it's always root directory initialization (no steward metadata)
        for version in 1..=current_version {
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
        
        // Step 2: Use scoped transaction with init arguments  
        ship.with_data_transaction(init_args, |_tx, fs| Box::pin(async move {
            // Step 3: Create initial pond directory structure (this generates actual filesystem operations)
            let data_root = fs.root().await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            data_root.create_dir_path("/data").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Transaction automatically commits on Ok return
            Ok(())
        })).await?;
        
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
        let ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);
        
        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");
        
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
        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Begin a second transaction with test arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.with_data_transaction(args, |_tx, fs| Box::pin(async move {
            // Do some filesystem operation to ensure the transaction has operations to commit
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&root, "/test.txt", b"test content").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute scoped transaction");
    }

    #[tokio::test]
    async fn test_transaction_metadata_persistence() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Begin a second transaction with specific args using scoped transaction 
        let args = vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()];
        ship.with_data_transaction(args.clone(), |_tx, fs| Box::pin(async move {
            // Create actual filesystem operations (required for commit)
            let data_root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/file2.txt", b"copied content").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute scoped transaction");
        
        // The copy command should be version 1 (first real data transaction)
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
        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Check that recovery is not needed after init
        ship.check_recovery_needed().await.expect("Recovery should not be needed after init");
        
        // Begin a second transaction with arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.with_data_transaction(args.clone(), |_tx, fs| Box::pin(async move {
            // Do some operation on data filesystem
            let data_root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/test.txt", b"test content").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute scoped transaction");
        
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

        // FIRST: Create a properly initialized pond and simulate a crash scenario
        {
            let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to create ship");
            
            // Use coordinated transaction to simulate crash between data commit and control commit
            let args = vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()];
            
            // Step 1-3: Begin transaction, modify, commit data FS
            {
                let tx = ship.begin_transaction_with_args(args.clone()).await.expect("Failed to begin transaction");
                
                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");
                
                // Modify data during the transaction
                tinyfs::async_helpers::convenience::create_file_path(&data_root, "/file1.txt", b"content1").await
                    .expect("Failed to create file");
                
                // Commit data transaction directly through the guard (embeds recovery metadata)
                let tx_desc = TxDesc::new(args.clone());
                let tx_desc_json = tx_desc.to_json().expect("Failed to serialize TxDesc");
                tx.commit(Some(std::collections::HashMap::from([
                    ("steward_tx_args".to_string(), serde_json::Value::String(tx_desc_json))
                ]))).await.expect("Failed to commit transaction")
                    .expect("Transaction should have committed with operations");
            }
            
            // SIMULATE CRASH HERE - don't call commit_control_metadata()
            // This leaves data committed but control metadata missing
            
            println!("✅ Simulated crash after data commit");
        } // Ship drops here, simulating crash
        
        // SECOND: Create a new ship (simulating restart) and test recovery
        {
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to open pond after crash");
            
            // Recovery should be needed because control metadata is missing
            let recovery_result = match ship.check_recovery_needed().await {
                Err(StewardError::RecoveryNeeded { sequence }) => {
                    println!("✅ Detected recovery needed for sequence: {}", sequence);
                    // Perform actual recovery
                    ship.recover().await.expect("Recovery should succeed")
                }
                Ok(()) => panic!("Recovery should have been needed after crash"),
                Err(e) => panic!("Unexpected error during recovery check: {:?}", e),
            };
            
            assert!(recovery_result.was_needed, "Recovery should have been needed");
            assert_eq!(recovery_result.recovered_count, 1, "Should have recovered one transaction");
            
            // After recovery, no further recovery should be needed
            ship.check_recovery_needed().await.expect("Recovery should not be needed after successful recovery");
            
            // Verify data survived the crash and recovery
            ship.with_data_transaction(vec!["verify".to_string()], |_tx, fs| Box::pin(async move {
                let data_root = fs.root().await.expect("Failed to get data root");
                let file_content = data_root.read_file_path_to_vec("/file1.txt").await
                    .expect("File should exist after recovery");
                assert_eq!(file_content, b"content1");
                Ok(())
            })).await.expect("Failed to verify data after recovery");
            
            println!("✅ Recovery completed successfully");
        }
    }

    #[tokio::test]
    async fn test_multiple_transaction_recovery() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to create ship");
        
        // Commit several transactions normally using scoped pattern
        for i in 1..=3 {
            let args = vec!["test".to_string(), format!("operation{}", i)];
            ship.with_data_transaction(args, |_tx, fs| Box::pin(async move {
                let data_root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                tinyfs::async_helpers::convenience::create_file_path(&data_root, &format!("/file{}.txt", i), format!("content{}", i).as_bytes())
                    .await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                Ok(())
            })).await.expect("Failed to execute scoped transaction");
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

        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to create ship");
        
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
            let _ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to initialize pond");
        }

        // Step 2: Create a transaction with metadata that commits to data FS but crashes before control FS
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_existing_pond(&pond_path).await.expect("Failed to open existing pond");
            
            // Simulate a crash scenario using coordinated transaction approach
            let copy_args = vec!["pond".to_string(), "copy".to_string(), "source.txt".to_string(), "dest.txt".to_string()];
            
            {
                let tx = ship.begin_transaction_with_args(copy_args.clone()).await.expect("Failed to begin transaction");
                
                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");
                
                // Do actual file operation
                tinyfs::async_helpers::convenience::create_file_path(&data_root, "/dest.txt", b"copied content").await
                    .expect("Failed to create dest file");
                
                // Commit data transaction directly through the guard (embeds recovery metadata)
                let tx_desc = TxDesc::new(copy_args.clone());
                let tx_desc_json = tx_desc.to_json().expect("Failed to serialize TxDesc");
                tx.commit(Some(std::collections::HashMap::from([
                    ("steward_tx_args".to_string(), serde_json::Value::String(tx_desc_json))
                ]))).await.expect("Failed to commit transaction")
                    .expect("Transaction should have committed with operations");
            }
            
            // SIMULATE CRASH: Don't record control metadata
            println!("✅ Simulated crash after data commit");
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
            ship.with_data_transaction(vec!["verify".to_string()], |_tx, fs| Box::pin(async move {
                let data_root = fs.root().await.expect("Failed to get data root");
                let reader = data_root.async_reader_path("/dest.txt").await.expect("File should exist after recovery");
                let file_content = tinyfs::buffer_helpers::read_all_to_vec(reader).await.expect("Failed to read file content");
                assert_eq!(file_content, b"copied content");
                Ok(())
            })).await.expect("Failed to verify file after recovery");
        }
    }

    #[tokio::test]
    async fn test_no_recovery_needed_for_consistent_state() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::initialize_new_pond(&pond_path).await.expect("Failed to create ship");
        
        // Do normal complete transactions using scoped pattern
        for i in 1..=3 {
            let args = vec!["pond".to_string(), "mkdir".to_string(), format!("/dir{}", i)];
            ship.with_data_transaction(args, |_tx, fs| Box::pin(async move {
                let data_root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                data_root.create_dir_path(&format!("/dir{}", i)).await
                    .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                Ok(())
            })).await.expect("Failed to execute scoped transaction");
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
