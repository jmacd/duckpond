//! Ship - The main steward struct that orchestrates primary and secondary filesystems

use crate::{get_control_path, get_data_path, StewardError, TxDesc, RecoveryResult, StewardTransactionGuard};
use std::path::Path;
use tlogfs::{OpLogPersistence};
use anyhow::Result;
use log::{debug, info};

/// Ship manages both a primary "data" filesystem and a secondary "control" filesystem
/// It provides the main interface for pond operations while handling post-commit actions
pub struct Ship {
    /// Direct access to data persistence layer for transaction operations
    data_persistence: OpLogPersistence,
    /// Direct access to control persistence layer for transaction operations
    control_persistence: OpLogPersistence,
    /// Path to the pond root
    pond_path: String,
}

impl Ship {
    /// Initialize a completely new pond with proper transaction #1.
    ///
    /// Use `open_pond()` to work with ponds that already exist.
    pub async fn create_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        // Create infrastructure  
        let mut ship = Self::create_infrastructure(pond_path, true).await?;
        
        // Initialize control filesystem with /txn directory (control FS transaction #1)
        ship.initialize_control_filesystem().await?;
        
        // Pond is now ready - no data transaction needed for initialization
        Ok(ship)
    }

    /// Initialize the control filesystem with its directory structure.
    /// This is control FS transaction #1 - creates root directory and /txn directory.
    /// Called during pond initialization before any data transactions.
    async fn initialize_control_filesystem(&mut self) -> Result<(), StewardError> {
        debug!("Initializing control filesystem with directory structure");
        
        // Create additional directories in a separate transaction
        {
            debug!("Creating transaction for /txn directory creation");
	    let tx = self.control_persistence.begin().await
		.map_err(|e| StewardError::ControlInit(e))?;
            
            // Get root directory of control filesystem through the transaction guard
            let control_root = tx.root().await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Create the /txn directory for transaction metadata
            debug!("Creating /txn directory in control filesystem");
            control_root.create_dir_path("/txn").await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            debug!("Committing transaction for /txn directory");
            // Commit the transaction
            tx.commit(None).await
                .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        } // Transaction guard automatically cleaned up here
        
        debug!("Control filesystem initialized with directory structure");
        Ok(())
    }
    
    /// Open an existing, pre-initialized pond.
    pub async fn open_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Self::create_infrastructure(pond_path, false).await
    }
    
    /// Internal method to create just the filesystem infrastructure.
    /// 
    /// This creates the data and control directories and initializes tlogfs instances,
    /// but does NOT create any transactions. It's used internally by both
    /// initialize_new_pond() and open_existing_pond().
    async fn create_infrastructure<P: AsRef<Path>>(pond_path: P, create_new: bool) -> Result<Self, StewardError> {
        let pond_path_str = pond_path.as_ref().to_string_lossy().to_string();
        let data_path = get_data_path(pond_path.as_ref());
        let control_path = get_control_path(pond_path.as_ref());

        info!("opening pond: {pond_path_str}");
        
        // Create directories if they don't exist
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let data_path_str = data_path.to_string_lossy().to_string();
        let control_path_str = control_path.to_string_lossy().to_string();

        debug!("initializing data FS {data_path_str}");

        // Initialize data filesystem with direct persistence access
        let data_persistence = 
            tlogfs::OpLogPersistence::open_or_create(&data_path_str, create_new).await
	    .map_err(StewardError::DataInit)?;

        debug!("initializing control FS {control_path_str}");

        // Initialize control filesystem (store persistence layer for direct access)
        let control_persistence = 
            tlogfs::OpLogPersistence::open_or_create(&control_path_str, create_new).await
	    .map_err(StewardError::ControlInit)?;
        
        Ok(Ship {
            data_persistence,
            control_persistence,
            pond_path: pond_path_str,
        })
    }

    /// Execute operations within a scoped data filesystem transaction
    /// The transaction commits on Ok(()) return, rolls back on Err() return
    /// Now uses the StewardTransactionGuard for consistent sequencing with begin_transaction()
    pub async fn transact<F, R>(&mut self, args: Vec<String>, f: F) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(&'a StewardTransactionGuard<'a>, &'a tinyfs::FS) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StewardError>> + Send + 'a>>,
    {
        let args_fmt = format!("{:?}", args);
        debug!("Beginning scoped transaction {args_fmt}");
        
        // Create steward transaction guard (same as begin_transaction)
        let tx = self.begin_transaction(args).await?;
        
        // Execute the user function with both the steward guard and filesystem
        let result = f(&tx, &*tx).await;
        
        match result {
            Ok(value) => {
                // Success - commit using steward guard (ensures proper sequencing)
                tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Error - steward transaction guard will auto-rollback on drop
                let error_msg = format!("{}", e);
                debug!("Scoped transaction failed {error_msg}");
                Err(e)
            }
        }
    }

    /// Begin a coordinated transaction with command arguments
    pub async fn begin_transaction(&mut self, args: Vec<String>) -> Result<StewardTransactionGuard<'_>, StewardError> {
        self.begin_transaction_with_variables(args, None).await
    }

    /// Begin a coordinated transaction with command arguments and optional template variables
    pub async fn begin_transaction_with_variables(
        &mut self, 
        args: Vec<String>, 
        template_variables: Option<std::collections::HashMap<String, String>>
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        let txn_id = uuid7::uuid7().to_string();
        let args_fmt = format!("{:?}", args);
        debug!("Beginning steward transaction {txn_id} {args_fmt}");
        
        // Begin Data FS transaction guard
        let data_tx = self.data_persistence.begin().await
            .map_err(|e| StewardError::DataInit(e))?;

        // Set template variables on the persistence state if provided
        if let Some(variables) = template_variables {
            debug!("Setting template variables on state: {:?}", variables);
            let state = data_tx.state()?;
            state.set_template_variables(variables);
        }
        
        let steward_guard = StewardTransactionGuard::new(data_tx, txn_id, args, &mut self.control_persistence);
        
        Ok(steward_guard)
    }

    /// Commit a steward transaction guard with proper sequencing
    /// This method provides the control persistence access needed for proper sequencing
    pub async fn commit_transaction(&mut self, guard: StewardTransactionGuard<'_>) -> Result<Option<()>, StewardError> {
        guard.commit().await
    }

    /// Record transaction metadata in the control filesystem
    /// Creates a file at /txn/${txn_version} with transaction details as JSON
    /// This is used by both Ship and StewardTransactionGuard to avoid code duplication
    pub async fn record_transaction_metadata_with_persistence(
        control_persistence: &mut OpLogPersistence,
        txn_id: &str,
        args: &Vec<String>
    ) -> Result<(), StewardError> {
        debug!("Recording transaction metadata {txn_id}");

        // Create the transaction metadata file path
        let txn_path = format!("/txn/{}", txn_id);

        // Serialize transaction descriptor to JSON with trailing newline
        let tx_desc = TxDesc {
            txn_id: txn_id.to_string(),
            args: args.clone(),
        };

        let mut json_content = tx_desc.to_json()?;
        json_content.push('\n');
        let tx_content = json_content.into_bytes();
        
        // Use scoped control filesystem transaction to create metadata file
        let control_tx = control_persistence.begin().await
            .map_err(|e| StewardError::ControlInit(e))?;
        
        // Get root directory of control filesystem through the transaction guard
        let control_root = control_tx.root().await
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
        
        debug!("Transaction file created {txn_path}");
        
        // Commit the control transaction
        control_tx.commit(None).await
            .map_err(|e| StewardError::ControlInit(tlogfs::TLogFSError::TinyFS(e)))?;
        
        debug!("Transaction metadata recorded successfully {txn_id}");
        Ok(())
    }

    /// Read transaction metadata from control filesystem
    pub async fn read_transaction_metadata(&mut self, txn_id: String) -> Result<Option<TxDesc>, StewardError> {
        let txn_path = format!("/txn/{}", txn_id);
        
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
    pub async fn check_recovery_needed(&mut self) -> Result<Option<TxDesc>, StewardError> {
        debug!("Checking if recovery is needed");

        // Need to use a read-only data transaction to access commit metadata
        let last_info = self.transact(vec!["check_recovery".to_string()], |tx, _fs| Box::pin(async move {
            // This is a read-only operation, so no actual filesystem changes
            // Get the last commit metadata directly from persistence layer
            Ok(tx.state()?.get_last_commit_metadata().await
		.map_err(|e| StewardError::DataInit(e))?)
        })).await?;

        if let Some(ref info) = last_info {
            if let Some(pond_txn_value) = info.get("pond_txn") {
                if let Some(pond_txn_obj) = pond_txn_value.as_object() {
                    let txn_id = pond_txn_obj.get("txn_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| StewardError::DeltaLake("missing txn_id in pond_txn".into()))?;
                    let args_json = pond_txn_obj.get("args")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| StewardError::DeltaLake("missing args in pond_txn".into()))?;
                    
                    debug!("Checking for transaction metadata {txn_id}");
                    if self.read_transaction_metadata(txn_id.to_string()).await?.is_none() {
                        // Parse the TxDesc from the commit metadata
                        let tx_desc = TxDesc::from_json(args_json)?;
                        return Err(StewardError::RecoveryNeeded { 
                            txn_id: txn_id.to_string(), 
                            tx_desc 
                        });
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Err(StewardError::DeltaLake("pond_txn is not an object".into()));
                }
            } else {
                debug!("No pond_txn found in commit metadata");
                return Ok(None);
            }
        } else {
            debug!("No commit metadata found");
            return Ok(None);
        }
    }

    /// Perform crash recovery
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        info!("Starting crash recovery process");
        
        // Need to ensure we can access metadata - use a read-only transaction first
        let info_result = self.transact(vec!["recovery_check".to_string()], |tx, _fs| Box::pin(async move {
            Ok(tx.state()?.get_last_commit_metadata().await
            .map_err(|e| StewardError::DeltaLake(format!("Failed to get commit metadata: {}", e)))?)
        })).await;

        let info = match info_result {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                // No metadata means no transactions to recover
                info!("No transactions found to recover");
                return Ok(RecoveryResult {
                    recovered_count: 0,
                    was_needed: false,
                });
            }
            Err(e) => return Err(e),
        };

        let pond_txn_value = match info.get("pond_txn") {
            Some(value) => value,
            None => {
                // No pond_txn means no steward transactions to recover
                info!("No steward transactions found to recover");
                return Ok(RecoveryResult {
                    recovered_count: 0,
                    was_needed: false,
                });
            }
        };
        
        let pond_txn_obj = pond_txn_value.as_object()
            .ok_or_else(|| StewardError::DeltaLake("pond_txn is not an object".into()))?;
        
        let txn_id = pond_txn_obj.get("txn_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StewardError::DeltaLake("missing txn_id in pond_txn".into()))?
            .to_string();
        let txn_args = pond_txn_obj.get("args")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StewardError::DeltaLake("missing args in pond_txn".into()))?;
        
        let mut recovered_count = 0;
        
        if self.read_transaction_metadata(txn_id.clone()).await?.is_none() {
            info!("Found missing control metadata for transaction {txn_id}");
                
            // Try to recover from data FS commit metadata
            self.recover_transaction_metadata_from_data_fs(&txn_id, txn_args).await?;
            recovered_count += 1;
        }
        
        info!("Crash recovery completed {recovered_count}");
        Ok(RecoveryResult {
            recovered_count,
            was_needed: recovered_count > 0,
        })
    }

    /// Recover transaction metadata from data FS commit metadata
    async fn recover_transaction_metadata_from_data_fs(&mut self, txn_id: &str, txn_args_json: &str) -> Result<(), StewardError> {
        info!("Recovering transaction metadata from data FS commit for {txn_id}");
        
        // Parse the stored transaction descriptor from the args JSON
        let recovered_tx_desc = TxDesc::from_json(txn_args_json)?;
        
        // Record the recovered metadata in control filesystem
        Self::record_transaction_metadata_with_persistence(&mut self.control_persistence, txn_id, &recovered_tx_desc.args).await?;
        
        info!("Successfully recovered transaction metadata {txn_id}");
        Ok(())
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
        let mut ship = Self::create_infrastructure(pond_path, true).await?;
        
        // Step 2: Use scoped transaction with init arguments  
        ship.transact(init_args, |_tx, fs| Box::pin(async move {
            // Step 3: Create initial pond directory structure (this generates actual filesystem operations)
            let data_root = fs.root().await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            data_root.create_dir_path("/data").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Transaction automatically commits on Ok return
            Ok(())
        })).await?;
        
        info!("Pond fully initialized with transaction #1");
        Ok(ship)
    }
}

// Implement Debug for Ship
impl std::fmt::Debug for Ship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ship")
            .field("pond_path", &self.pond_path)
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
        let ship = Ship::create_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);
        
        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");
        
        // Test that pond path is stored correctly
        assert_eq!(ship.pond_path, pond_path.to_string_lossy().to_string());
        
        // Test that we can open the same pond (like production commands do)
        let _opened_ship = Ship::open_pond(&pond_path).await.expect("Should be able to open existing pond");
    }

    #[tokio::test]
    async fn test_ship_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use the same constructor as production (pond init)
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Begin a second transaction with test arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.transact(args, |_tx, fs| Box::pin(async move {
            // Do some filesystem operation to ensure the transaction has operations to commit
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&root, "/test.txt", b"test content").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute scoped transaction");
    }

    #[tokio::test]
    async fn test_normal_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to initialize pond");
        
        // Check that recovery is not needed after init
        ship.check_recovery_needed().await.expect("Recovery should not be needed after init");
        
        // Begin a second transaction with arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        ship.transact(args.clone(), |_tx, fs| Box::pin(async move {
            // Do some operation on data filesystem
            let data_root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&data_root, "/test.txt", b"test content").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Failed to execute scoped transaction");
        
        // Check that recovery is still not needed after successful commit
        let recovery_check = ship.check_recovery_needed().await;
        match recovery_check {
            Ok(None) => {
                // This is expected for a successful commit with no recovery needed
            }
            Ok(Some(tx_desc)) => {
                panic!("Unexpected TxDesc returned when no recovery should be needed: {:?}", tx_desc);
            }
            Err(StewardError::RecoveryNeeded { txn_id, tx_desc }) => {
                panic!("Recovery should not be needed after successful commit, but got recovery needed for {}: {:?}", txn_id, tx_desc);
            }
            Err(e) => {
                panic!("Unexpected error during recovery check: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_crash_recovery_scenario() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // FIRST: Create a properly initialized pond and simulate a crash scenario
        {
            let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create ship");
            
            // Use coordinated transaction to simulate crash between data commit and control commit
            let args = vec!["copy".to_string(), "file1.txt".to_string(), "file2.txt".to_string()];
            
            // Step 1-3: Begin transaction, modify, commit data FS
            let mut tx = {
                let tx = ship.begin_transaction(vec![]).await.expect("Failed to begin transaction");
                
                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");
                
                // Modify data during the transaction
                tinyfs::async_helpers::convenience::create_file_path(&data_root, "/file1.txt", b"content1").await
                    .expect("Failed to create file");
                
                tx
            };
            
            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing
            let txn_id = uuid7::uuid7().to_string();
            let tx_desc = TxDesc::new(&txn_id, args.clone());
            let tx_desc_json = tx_desc.to_json().expect("Failed to serialize TxDesc");
            let pond_txn = serde_json::json!({
                "txn_id": txn_id,
                "args": tx_desc_json
            });
            
            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx.take_transaction().expect("Transaction guard should be available");
            raw_tx.commit(Some(std::collections::HashMap::from([
                ("pond_txn".to_string(), pond_txn),
            ]))).await.expect("Failed to commit transaction")
                .expect("Transaction should have committed with operations");
            
            // SIMULATE CRASH HERE - don't call commit_control_metadata()
            // This leaves data committed but control metadata missing
            
            println!("✅ Simulated crash after data commit");
        } // Ship drops here, simulating crash
        
        // SECOND: Create a new ship (simulating restart) and test recovery
        {
            let mut ship = Ship::open_pond(&pond_path).await.expect("Failed to open pond after crash");
            
            // Recovery should be needed because control metadata is missing
            let recovery_result = match ship.check_recovery_needed().await {
                Err(StewardError::RecoveryNeeded { txn_id, tx_desc }) => {
                    println!("✅ Detected recovery needed for txn_id: {}", txn_id);
                    println!("✅ Recovery TxDesc: {:?}", tx_desc);
                    // Perform actual recovery
                    ship.recover().await.expect("Recovery should succeed")
                }
                Ok(Some(_)) => panic!("Recovery should have been needed but TxDesc was returned"),
                Ok(None) => panic!("Recovery should have been needed after crash"),
                Err(e) => panic!("Unexpected error during recovery check: {:?}", e),
            };
            
            assert!(recovery_result.was_needed, "Recovery should have been needed");
            assert_eq!(recovery_result.recovered_count, 1, "Should have recovered one transaction");
            
            // After recovery, no further recovery should be needed
            ship.check_recovery_needed().await.expect("Recovery should not be needed after successful recovery");
            
            // Verify data survived the crash and recovery
            ship.transact(vec!["verify".to_string()], |_tx, fs| Box::pin(async move {
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

        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create ship");
        
        // Commit several transactions normally using scoped pattern
        for i in 1..=3 {
            let args = vec!["test".to_string(), format!("operation{}", i)];
            ship.transact(args, |_tx, fs| Box::pin(async move {
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

        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create ship");
        
        // Test recover when no recovery is needed
        let recovery_result = ship.recover().await.expect("Failed to execute recovery");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);
    }

    #[tokio::test]
    async fn test_crash_recovery_with_metadata_extraction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Step 1: Initialize pond using production code (pond init)
        {
            let _ship = Ship::create_pond(&pond_path).await.expect("Failed to initialize pond");
        }

        // Step 2: Create a transaction with metadata that commits to data FS but crashes before control FS
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path).await.expect("Failed to open existing pond");
            
            // Simulate a crash scenario using coordinated transaction approach
            let copy_args = vec!["pond".to_string(), "copy".to_string(), "source.txt".to_string(), "dest.txt".to_string()];
            
            let mut tx = {
                let tx = ship.begin_transaction(vec![]).await.expect("Failed to begin transaction");
                
                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");
                
                // Do actual file operation
                tinyfs::async_helpers::convenience::create_file_path(&data_root, "/dest.txt", b"copied content").await
                    .expect("Failed to create dest file");
                
                tx
            };
            
            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing
            let txn_id = uuid7::uuid7().to_string();
            let tx_desc = TxDesc::new(&txn_id, copy_args.clone());
            let tx_desc_json = tx_desc.to_json().expect("Failed to serialize TxDesc");
            let pond_txn = serde_json::json!({
                "txn_id": txn_id,
                "args": tx_desc_json
            });
            
            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx.take_transaction().expect("Transaction guard should be available");
            raw_tx.commit(Some(std::collections::HashMap::from([
                ("pond_txn".to_string(), pond_txn),
            ]))).await.expect("Failed to commit transaction")
                .expect("Transaction should have committed with operations");
            
            // SIMULATE CRASH: Don't record control metadata
            println!("✅ Simulated crash after data commit");
        }

        // Step 3: Recovery after crash using production code
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path).await.expect("Failed to open existing pond for recovery");
            
            // Check that recovery is needed
            let check_result = ship.check_recovery_needed().await;
            assert!(check_result.is_err(), "Should detect that recovery is needed");
            
            let recovered_tx_desc = if let Err(StewardError::RecoveryNeeded { txn_id, tx_desc }) = check_result {
                println!("Should need recovery for txn_id: {}", txn_id);
                println!("Recovery TxDesc: {:?}", tx_desc);
                tx_desc
            } else {
                panic!("Expected RecoveryNeeded error, got: {:?}", check_result);
            };
            
            // Perform recovery
            let recovery_result = ship.recover().await.expect("Recovery should succeed");
            assert_eq!(recovery_result.recovered_count, 1, "Should recover exactly 1 transaction");
            assert!(recovery_result.was_needed, "Recovery should have been needed");
            
            // Verify recovery is no longer needed
            ship.check_recovery_needed().await.expect("Recovery should not be needed after successful recovery");
            
            // Verify the recovered transaction descriptor matches what we expected
            assert_eq!(recovered_tx_desc.args, vec!["pond".to_string(), "copy".to_string(), "source.txt".to_string(), "dest.txt".to_string()]);
            assert_eq!(recovered_tx_desc.command_name(), Some("pond"));
            
            // Verify the data file still exists (data wasn't lost in crash)
            ship.transact(vec!["verify".to_string()], |_tx, fs| Box::pin(async move {
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

        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create ship");
        
        // Do normal complete transactions using scoped pattern
        for i in 1..=3 {
            let args = vec!["pond".to_string(), "mkdir".to_string(), format!("/dir{}", i)];
            ship.transact(args, |_tx, fs| Box::pin(async move {
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

    #[tokio::test]
    async fn test_new_transaction_api() {
        // Test the new begin_transaction API to ensure it has proper sequencing
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_new_api");

        // Initialize a new pond
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create pond");

        // Test: Use begin_transaction - this demonstrates the API
        let tx = ship.begin_transaction(vec!["test".to_string()]).await
            .expect("Failed to begin transaction");
        
        // Perform some work
        let root = tx.root().await.expect("Failed to get root");
        tinyfs::async_helpers::convenience::create_file_path(&root, "/test_file.txt", b"test content").await
            .expect("Failed to create file");
        
        // The commit step demonstrates the borrow checker challenge
        // ship.commit_transaction(tx).await - this would fail due to borrow checker
        // Instead, we rely on the steward guard's commit method which has the proper sequencing
        tx.commit().await.expect("Failed to commit steward transaction");

        println!("✅ New transaction API works with proper sequencing via steward guard commit");
    }
}
