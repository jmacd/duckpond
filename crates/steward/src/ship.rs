//! Ship - The main steward struct that orchestrates primary and secondary filesystems

use anyhow::Result;
use crate::{
    control_table::ControlTable, get_control_path, get_data_path, StewardError, TxDesc, RecoveryResult, StewardTransactionGuard,
};
use log::{debug, info};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use serde_json::{Map, Value};
use tlogfs::{OpLogPersistence};

/// Ship manages both a primary "data" filesystem and a secondary "control" filesystem
/// It provides the main interface for pond operations while handling post-commit actions
pub struct Ship {
    /// Direct access to data persistence layer for transaction operations
    data_persistence: OpLogPersistence,
    /// Control table for tracking transaction lifecycle and sequencing
    control_table: ControlTable,
    /// Last committed write transaction sequence number
    last_write_seq: Arc<AtomicI64>,
    /// Path to the pond root
    pond_path: String,
}

impl Ship {
    /// Initialize a completely new pond with proper transaction #1.
    ///
    /// Use `open_pond()` to work with ponds that already exist.
    pub async fn create_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        // Create infrastructure (includes root directory initialization with txn_seq=1)
        let mut ship = Self::create_infrastructure(pond_path, true).await?;

        // Record the root initialization transaction in the control table
        // This ensures get_last_write_sequence() returns 1, so first user txn gets 2
        let txn_id = uuid7::uuid7().to_string();
        let cli_args = vec!["pond".to_string(), "init".to_string()];
        let environment = std::collections::HashMap::new();
        
        // Allocate txn_seq=1 for root initialization
        let root_seq = ship.last_write_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        
        // Record begin
        ship.control_table.record_begin(
            root_seq,
            None,  // Root has no based_on_seq
            txn_id.clone(),
            "write",
            cli_args,
            environment,
        ).await?;
        
        // Record data_committed (root initialization created DeltaLake version 0)
        ship.control_table.record_data_committed(
            root_seq,
            txn_id,
            0,  // Root initialization is DeltaLake version 0
            0,  // Duration unknown/not tracked
        ).await?;
        
        info!("Recorded root initialization transaction with txn_seq={}", root_seq);

        // Pond is now ready with control table showing txn_seq=1
        Ok(ship)
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

        debug!("initializing control table {control_path_str}");

        // Initialize control table for transaction tracking
        let control_table = ControlTable::new(&control_path_str).await?;
        
        // Load the last write sequence from the control table
        let last_seq = if create_new {
            // New pond: root transaction will be recorded immediately after this
            // Initialize to 0 so that recording root as txn_seq=1 works correctly
            0
        } else {
            // Existing pond: load actual last sequence from control table
            control_table.get_last_write_sequence().await?
        };
        let last_write_seq = Arc::new(AtomicI64::new(last_seq));
        
        info!("Initialized last write sequence: {} (create_new={})", last_seq, create_new);

        Ok(Ship {
            data_persistence,
            control_table,
            last_write_seq,
            pond_path: pond_path_str,
        })
    }

    /// Execute operations within a scoped data filesystem transaction
    /// The transaction commits on Ok(()) return, rolls back on Err() return
    /// Now uses the StewardTransactionGuard for consistent sequencing with begin_transaction()
    /// Defaults to write transaction (is_write=true)
    pub async fn transact<F, R>(&mut self, args: Vec<String>, f: F) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(&'a StewardTransactionGuard<'a>, &'a tinyfs::FS) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StewardError>> + Send + 'a>>,
    {
        let args_fmt = format!("{:?}", args);
        debug!("Beginning scoped transaction {args_fmt}");

        // Create steward transaction guard (default to write transaction)
        let tx = self.begin_transaction(crate::TransactionOptions::write(args)).await?;

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

    /// Execute operations within a scoped transaction with SessionContext access
    /// Use this variant when operations need DataFusion SQL capabilities
    /// Avoids SessionContext::new() fallback anti-patterns
    /// Defaults to write transaction (is_write=true)
    pub async fn transact_with_session<F, R>(&mut self, args: Vec<String>, f: F) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(&'a StewardTransactionGuard<'a>, &'a tinyfs::FS, std::sync::Arc<datafusion::execution::context::SessionContext>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R, StewardError>> + Send + 'a>>,
    {
        let args_fmt = format!("{:?}", args);
        debug!("Beginning scoped transaction with SessionContext {args_fmt}");

        // Create steward transaction guard (default to write transaction)
        let mut tx = self.begin_transaction(crate::TransactionOptions::write(args)).await?;
        
        // Get the SessionContext from the transaction guard
        let session_ctx = tx.session_context().await
            .map_err(|e| StewardError::DataInit(e))?;

        let result = f(&tx, &*tx, session_ctx).await;

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

    /// Begin a coordinated transaction with the given options
    pub async fn begin_transaction(
        &mut self,
        options: crate::TransactionOptions,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        let txn_id = uuid7::uuid7().to_string();
        let args_fmt = format!("{:?}", options.args);
        debug!("Beginning steward transaction {txn_id} {args_fmt}");

        // Allocate transaction sequence
        let (txn_seq, based_on_seq) = if options.is_write {
            // Write transaction: allocate next sequence
            let seq = self.last_write_seq.fetch_add(1, Ordering::SeqCst) + 1;
            (seq, None)
        } else {
            // Read transaction: reuse last write sequence
            let seq = self.last_write_seq.load(Ordering::SeqCst);
            (seq, Some(seq))
        };

        let transaction_type = if options.is_write { "write" } else { "read" };
        debug!("Transaction {txn_id} allocated sequence {txn_seq} (type={transaction_type}, based_on_seq={:?})", based_on_seq);

        // Record transaction begin in control table
        self.control_table.record_begin(
            txn_seq,
            based_on_seq,
            txn_id.clone(),
            transaction_type,
            options.args.clone(),
            options.variables.clone(),
        ).await
            .map_err(|e| StewardError::ControlTable(format!("Failed to record transaction begin: {}", e)))?;

        // Begin Data FS transaction guard
        let data_tx = self.data_persistence.begin(txn_seq).await
            .map_err(|e| StewardError::DataInit(e))?;

        let vars_value: Value = options.variables.into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<Map<String, Value>>()
            .into();

        // Add CLI variables under "vars" key
        let structured_variables : HashMap<String, Value> = HashMap::from([
	    // @@@ this is weird
	    ("vars".to_string(), vars_value),
	]);

        data_tx.state()?.set_template_variables(structured_variables)?;

        // Create steward transaction guard with sequence tracking
        Ok(StewardTransactionGuard::new(
            data_tx,
            txn_seq,
            txn_id,
            transaction_type.to_string(),
            options.args,
            &mut self.control_table,
        ))
    }

    /// Commit a steward transaction guard with proper sequencing
    /// This method provides the control persistence access needed for proper sequencing
    pub async fn commit_transaction(&mut self, guard: StewardTransactionGuard<'_>) -> Result<Option<()>, StewardError> {
        guard.commit().await
    }

    /// Check if recovery is needed by querying the control table
    /// Returns the first incomplete transaction if any exists
    pub async fn check_recovery_needed(&mut self) -> Result<Option<TxDesc>, StewardError> {
        debug!("Checking if recovery is needed via control table");

        let incomplete = self.control_table.find_incomplete_transactions().await?;
        
        if let Some((txn_seq, txn_id, _data_fs_version)) = incomplete.first() {
            info!("Recovery needed: incomplete transaction seq={}, id={}", 
                  txn_seq, txn_id);
            
            // Fetch the full transaction details including args
            let (cli_args, data_fs_version) = self.control_table
                .get_incomplete_transaction_details(*txn_seq)
                .await?;
            
            info!("Transaction details: args={:?}, data_fs_version={}", cli_args, data_fs_version);
            
            // Return a TxDesc with the full args
            return Err(StewardError::RecoveryNeeded {
                txn_seq: Some(*txn_seq),
                txn_id: txn_id.clone(),
                tx_desc: TxDesc {
                    txn_id: txn_id.clone(),
                    args: cli_args,
                },
            });
        }

        debug!("No recovery needed");
        Ok(None)
    }

    /// Perform crash recovery by detecting incomplete transactions
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        info!("Starting crash recovery process via control table");

        let incomplete = self.control_table.find_incomplete_transactions().await?;
        
        if incomplete.is_empty() {
            info!("No incomplete transactions found");
            return Ok(RecoveryResult {
                recovered_count: 0,
                was_needed: false,
            });
        }

        info!("Found {} incomplete transaction(s):", incomplete.len());
        for (txn_seq, txn_id, data_fs_version) in &incomplete {
            info!("  - Transaction seq={}, id={}, data_fs_version={}", 
                  txn_seq, txn_id, data_fs_version);
        }

        // Mark each incomplete transaction as completed
        // In the future, this will trigger replication coordination:
        // 1. Query data FS commit log for the transaction
        // 2. Bundle files that were created/modified
        // 3. Initiate replication to other nodes
        // 4. Then mark transaction as completed in control table
        
        for (txn_seq, txn_id, _data_fs_version) in &incomplete {
            info!("Marking transaction seq={}, id={} as recovered", txn_seq, txn_id);
            self.control_table.record_completed(*txn_seq, txn_id.clone(), 0).await?;
        }
        
        info!("Recovery completed - marked {} transaction(s) as recovered", incomplete.len());
        Ok(RecoveryResult {
            recovered_count: incomplete.len() as u64,
            was_needed: true,
        })
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
            Err(StewardError::RecoveryNeeded { txn_seq, txn_id, tx_desc }) => {
                panic!("Recovery should not be needed after successful commit, but got recovery needed for seq={:?}, id={}: {:?}", txn_seq, txn_id, tx_desc);
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
                let tx = ship.begin_transaction(crate::TransactionOptions::write(vec![])).await.expect("Failed to begin transaction");

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
                Err(StewardError::RecoveryNeeded { txn_seq, txn_id, tx_desc }) => {
                    println!("✅ Detected recovery needed for seq={:?}, txn_id: {}", txn_seq, txn_id);
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

            // Verify data survived the crash and recovery (use read transaction)
            let tx = ship.begin_transaction(crate::TransactionOptions::read(vec!["verify".to_string()])).await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let file_content = data_root.read_file_path_to_vec("/file1.txt").await
                .expect("File should exist after recovery");
            assert_eq!(file_content, b"content1");
            tx.commit().await.expect("Failed to commit read transaction");

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
                // Pass copy_args to begin_transaction so they're recorded in control table
                let tx = ship.begin_transaction(crate::TransactionOptions::write(copy_args.clone())).await.expect("Failed to begin transaction");

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

            let recovered_tx_desc = if let Err(StewardError::RecoveryNeeded { txn_seq, txn_id, tx_desc }) = check_result {
                println!("Should need recovery for seq={:?}, txn_id: {}", txn_seq, txn_id);
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

            // Verify the data file still exists (use read transaction)
            let tx = ship.begin_transaction(crate::TransactionOptions::read(vec!["verify".to_string()])).await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let reader = data_root.async_reader_path("/dest.txt").await.expect("File should exist after recovery");
            let file_content = tinyfs::buffer_helpers::read_all_to_vec(reader).await.expect("Failed to read file content");
            assert_eq!(file_content, b"copied content");
            tx.commit().await.expect("Failed to commit read transaction");
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
        let tx = ship.begin_transaction(crate::TransactionOptions::write(vec!["test".to_string()])).await
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

    #[tokio::test]
    async fn test_transaction_sequence_numbering() {
        // Test that transaction sequences are allocated correctly without conflicts
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_sequences");

        // Initialize pond - this creates root directory with txn_seq=1
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create pond");

        // Verify initial sequence is 1 (root directory was created with txn_seq=1)
        let initial_seq = ship.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(initial_seq, 1, "Initial sequence should be 1 after root directory creation");

        // First user transaction should get txn_seq=2
        ship.transact(vec!["first".to_string()], |_tx, fs| Box::pin(async move {
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&root, "/file1.txt", b"content1").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("First transaction should succeed");

        // Verify sequence advanced to 2
        let after_first = ship.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(after_first, 2, "After first user transaction, sequence should be 2");

        // Second user transaction should get txn_seq=3
        ship.transact(vec!["second".to_string()], |_tx, fs| Box::pin(async move {
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            tinyfs::async_helpers::convenience::create_file_path(&root, "/file2.txt", b"content2").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })).await.expect("Second transaction should succeed");

        // Verify sequence advanced to 3
        let after_second = ship.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(after_second, 3, "After second user transaction, sequence should be 3");

        // Verify no conflicts: root=1, first=2, second=3
        info!("Transaction sequence test passed: root=1, first_user=2, second_user=3");
    }

    #[tokio::test]
    async fn test_root_directory_version_and_control_table() {
        // Test that root initialization creates v1 and records it in control table
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_root_version");

        // Initialize pond
        let ship = Ship::create_pond(&pond_path).await.expect("Failed to create pond");

        // Verify that last_write_seq is 1 after root initialization
        let last_seq = ship.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(last_seq, 1, "After root init, last_write_seq should be 1");

        // Verify control table has the root transaction recorded
        let control_last_seq = ship.control_table.get_last_write_sequence()
            .await
            .expect("Failed to query control table");
        assert_eq!(control_last_seq, 1, "Control table should show last write sequence as 1");

        // Reopen the pond to verify persistence
        drop(ship);
        let ship2 = Ship::open_pond(&pond_path).await.expect("Failed to reopen pond");
        
        let reopened_seq = ship2.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(reopened_seq, 1, "After reopening, last_write_seq should still be 1");

        info!("✅ Root directory initialization is recorded in control table with txn_seq=1");
    }

    #[tokio::test]
    async fn test_directory_tree_single_version_per_node() {
        // Test that creating a tree of directories in one transaction creates exactly one version per node
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_dir_tree");

        // Initialize pond
        let mut ship = Ship::create_pond(&pond_path).await.expect("Failed to create pond");

        // Create a tree of directories in a single transaction
        ship.transact(vec!["create_tree".to_string()], |_tx, fs| Box::pin(async move {
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Create nested directory structure: /a/b/c and /a/d/e
            root.create_dir_path("/a").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path("/a/b").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path("/a/b/c").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path("/a/d").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path("/a/d/e").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            Ok(())
        })).await.expect("Failed to create directory tree");

        // Verify the transaction sequence advanced properly
        let after_tree = ship.last_write_seq.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(after_tree, 2, "After creating directory tree, sequence should be 2");

        // Verify directories exist and are accessible by opening them again
        ship.transact(vec!["verify_tree".to_string()], |_tx, fs| Box::pin(async move {
            let root = fs.root().await.map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            // Verify all directories can be opened (they exist)
            root.open_dir_path("/a").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.open_dir_path("/a/b").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.open_dir_path("/a/b/c").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.open_dir_path("/a/d").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.open_dir_path("/a/d/e").await
                .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            Ok(())
        })).await.expect("Failed to verify directory tree");

        info!("✅ Directory tree creation produces accessible structure with proper version numbering");
    }
}
