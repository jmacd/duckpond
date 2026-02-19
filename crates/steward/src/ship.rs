// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Ship - Transaction coordinator managing data filesystem and control table

use crate::{
    RecoveryResult, StewardError, StewardTransactionGuard,
    control_table::{ControlTable, TransactionType},
    get_control_path, get_data_path,
};
use anyhow::Result;
use log::{debug, info};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::ops::AsyncFnOnce;
use std::path::{Path, PathBuf};
use tlogfs::{OpLogPersistence, PondMetadata, PondTxnMetadata, PondUserMetadata};

/// Ship manages the data filesystem (OpLogPersistence) and control table (ControlTable)
/// providing transaction coordination, audit logging, and post-commit action sequencing
pub struct Ship {
    data_persistence: OpLogPersistence,
    control_table: ControlTable,
    last_write_seq: i64,
    pond_path: PathBuf,
}

impl Ship {
    /// Initialize a completely new pond with proper transaction #1.
    ///
    /// Use `open_pond()` to work with ponds that already exist.
    pub async fn create_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        let meta = PondUserMetadata::new(vec!["pond".to_string(), "init".to_string()]);

        // Create infrastructure (includes root directory initialization with txn_seq=1)
        // Pass metadata so root transaction has proper audit trail
        let mut ship = Self::create_infrastructure(
            pond_path,
            true,
            Some(meta.clone()),
            None, // No preserved metadata for fresh pond
        )
        .await?;

        // After create_infrastructure with create_new=true, tlogfs has already
        // created and committed transaction #1 (root init). Sync Ship's counter.
        let root_seq = ship.data_persistence.last_txn_seq();
        ship.last_write_seq = root_seq;

        debug!(
            "Synced last_write_seq={} from data_persistence after root init",
            root_seq
        );

        let txn_metadata = PondTxnMetadata::new(root_seq, meta);

        // Record begin for the root transaction (already committed by tlogfs)
        ship.control_table
            .record_begin(
                &txn_metadata,
                None, // Root has no based_on_seq
                TransactionType::Write,
            )
            .await?;

        // Record data_committed (root initialization created DeltaLake version 0)
        ship.control_table
            .record_data_committed(
                &txn_metadata,
                TransactionType::Write,
                0, // Root initialization is DeltaLake version 0
                0, // Duration unknown/not tracked
            )
            .await?;

        // Record completed for the root transaction
        ship.control_table
            .record_completed(
                &txn_metadata,
                TransactionType::Write,
                0, // Duration unknown/not tracked
            )
            .await?;

        debug!(
            "Recorded root initialization transaction with txn_seq={}",
            root_seq
        );

        // Set pond identity metadata
        let metadata = PondMetadata::default();
        info!(
            "Pond created with ID: {} at {} by {}@{}",
            metadata.pond_id,
            chrono::DateTime::from_timestamp_micros(metadata.birth_timestamp)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            metadata.birth_username,
            metadata.birth_hostname
        );
        ship.control_table.set_pond_metadata(&metadata).await?;

        // Set default factory modes for primary pond
        // "remote" factory runs in "push" mode (automatic post-commit backup)
        ship.control_table
            .set_factory_mode("remote", "push")
            .await?;
        debug!("Set remote factory mode to 'push' for primary pond");

        // Pond is now ready with control table showing txn_seq=1
        Ok(ship)
    }

    /// Create pond infrastructure for bundle restoration (replication/backup restore).
    ///
    /// Unlike `create_pond()`, this creates the pond structure WITHOUT recording
    /// the initial transaction #1. The first bundle will create txn_seq=1 with
    /// the original command metadata from the source pond.
    ///
    /// Use this ONLY when restoring from bundles. Use `create_pond()` for normal initialization.
    ///
    /// # Arguments
    /// * `pond_path` - Path to the pond directory
    /// * `preserve_metadata` - Optional pond metadata from source (for replicas)
    pub async fn create_pond_for_restoration<P: AsRef<Path>>(
        pond_path: P,
        preserve_metadata: PondMetadata, // Required - restoring means preserving identity
    ) -> Result<Self, StewardError> {
        let control_path = get_control_path(pond_path.as_ref());
        let data_path = get_data_path(pond_path.as_ref());

        debug!("Creating pond for restoration: {:?}", pond_path.as_ref());

        // Create directories
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let control_path_str = control_path.to_string_lossy().to_string();
        let data_path_str = data_path.to_string_lossy().to_string();

        // Create ONLY the control table with preserved metadata
        debug!(
            "Creating control table for restoration with preserved pond identity: {}",
            preserve_metadata.pond_id
        );
        let control_table = ControlTable::create(&control_path_str, &preserve_metadata).await?;

        // Create an empty data Delta table structure (schema + _delta_log/, but no data)
        // Bundles will be replayed through replay_transaction() which writes to this table
        debug!("Creating empty data table structure at {}", data_path_str);
        let data_persistence = OpLogPersistence::create_empty(&data_path_str)
            .await
            .map_err(StewardError::DataInit)?;

        debug!(
            "Created restoration-ready pond (empty data table, bundles will populate via replay)"
        );

        Ok(Ship {
            data_persistence,
            control_table,
            last_write_seq: 0, // No transactions yet - bundles will provide them
            pond_path: pond_path.as_ref().to_path_buf(),
        })
    }

    /// Open an existing, pre-initialized pond.
    pub async fn open_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Self::create_infrastructure(pond_path, false, None, None).await
    }

    /// Internal method to create just the filesystem infrastructure.
    ///
    /// This creates the data and control directories and initializes tlogfs instances,
    /// but does NOT create any transactions. It's used internally by both
    /// initialize_new_pond() and open_existing_pond().
    ///
    /// When `create_new=true`, `root_metadata` should contain (txn_id, cli_args) to
    /// properly record the command that created the pond.
    ///
    /// # Arguments
    /// * `pond_path` - Path to the pond directory
    /// * `create_new` - Whether to create a new pond vs open existing
    /// * `root_metadata` - Optional (txn_id, cli_args) for initial transaction
    /// * `preserve_metadata` - Optional pond metadata to preserve (for replicas)
    async fn create_infrastructure<P: AsRef<Path>>(
        pond_path: P,
        create_new: bool,
        txn_metadata: Option<PondUserMetadata>,
        preserve_metadata: Option<PondMetadata>,
    ) -> Result<Self, StewardError> {
        let data_path = get_data_path(pond_path.as_ref());
        let control_path = get_control_path(pond_path.as_ref());

        debug!("opening pond: {:?}", pond_path.as_ref());

        // Create directories if they don't exist
        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&control_path)?;

        let data_path_str = data_path.to_string_lossy().to_string();
        let control_path_str = control_path.to_string_lossy().to_string();

        debug!("initializing data FS {data_path_str}");

        // Initialize data filesystem - automatically creates root directory if create_new=true
        let data_persistence =
            OpLogPersistence::open_or_create(&data_path_str, create_new, txn_metadata)
                .await
                .map_err(StewardError::DataInit)?;

        debug!("initializing control table {control_path_str}");

        // Initialize control table for transaction tracking
        let control_table = if create_new {
            // Creating new pond - use preserved metadata if provided, otherwise create fresh metadata
            assert!(preserve_metadata.is_none());
            let metadata = PondMetadata::default();
            debug!(
                "Creating control table with pond identity: {}",
                metadata.pond_id
            );
            ControlTable::create(&control_path_str, &metadata).await?
        } else {
            // Opening existing pond - metadata already exists in control table
            debug!("Opening existing control table");
            ControlTable::open(&control_path_str).await?
        };

        // Initialize last_write_seq from tlogfs (which loads from Delta metadata)
        // tlogfs is the authoritative source - it reads from actual Delta commits
        let last_seq = data_persistence.last_txn_seq();

        debug!(
            "Initialized last_seq={} from data_persistence (create_new={})",
            last_seq, create_new
        );

        Ok(Ship {
            data_persistence,
            control_table,
            last_write_seq: last_seq,
            pond_path: pond_path.as_ref().to_path_buf(),
        })
    }

    /// Execute operations within a scoped write transaction.
    ///
    /// Runs the async closure with `&FS` access, auto-commits on `Ok`,
    /// auto-aborts on `Err`. The closure's error type must convert into
    /// `StewardError` (which `tinyfs::Error` and `tlogfs::TLogFSError` do
    /// via `#[from]` impls — just use `?`).
    pub async fn write_transaction<F>(
        &mut self,
        meta: &PondUserMetadata,
        f: F,
    ) -> Result<(), StewardError>
    where
        F: for<'a> AsyncFnOnce(&'a tinyfs::FS) -> Result<(), StewardError>,
    {
        debug!("Beginning write transaction {:?}", meta);
        let tx = self.begin_write(meta).await?;
        let result = f(&tx).await;
        match result {
            Ok(()) => {
                _ = tx.commit().await?;
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                debug!("Transaction failed: {error_msg}");
                Err(e)
            }
        }
    }

    /// Replay a transaction from backup with original sequence number
    ///
    /// This is specifically for pond restoration - it takes bundle files that already
    /// contain the Delta commit log with the correct txn_seq, and applies them directly.
    ///
    /// Unlike transact() which creates NEW transactions, this replays EXISTING transactions
    /// from backups, preserving their original sequence numbers.
    ///
    /// # Arguments
    /// * `txn_seq` - Original transaction sequence number from source pond
    /// * `args` - Original command arguments
    /// * `f` - Function to apply bundle files (receives transaction guard + state)
    ///
    /// # Returns
    /// Result from the function execution
    pub async fn replay_transaction<F, R>(
        &mut self,
        txn_meta: &PondTxnMetadata,
        f: F,
    ) -> Result<R, StewardError>
    where
        F: for<'a> FnOnce(
            &'a StewardTransactionGuard<'a>,
            &'a tinyfs::FS,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<R, StewardError>> + Send + 'a>,
        >,
    {
        debug!(
            "Replaying transaction txn_seq={} {:?}",
            txn_meta.txn_seq, &txn_meta.user.args
        );

        // Begin transaction replay with specific sequence number
        let tx = self.begin_transaction_replay(txn_meta).await?;

        let result = f(&tx, &tx).await;

        match result {
            Ok(value) => {
                // Success - commit using steward guard (ensures proper sequencing)
                _ = tx.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Error - steward transaction guard will auto-rollback on drop
                let error_msg = format!("{}", e);
                debug!("Transaction replay failed {error_msg}");
                Err(e)
            }
        }
    }

    /// Get a reference to the control table for querying pond settings
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        &self.control_table
    }

    /// Get a mutable reference to the control table for modifying pond settings
    pub fn control_table_mut(&mut self) -> &mut ControlTable {
        &mut self.control_table
    }

    /// Begin a coordinated transaction with the given options
    pub async fn begin_read(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        self.begin_txn(false, meta).await
    }

    pub async fn begin_write(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        self.begin_txn(true, meta).await
    }

    async fn begin_txn(
        &mut self,
        is_write: bool,
        meta: &PondUserMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        debug!("Beginning steward transaction {meta:?}");

        // Allocate transaction sequence
        let (txn_seq, based_on_seq) = if is_write {
            // Write transaction: allocate next sequence
            self.last_write_seq += 1;
            let seq = self.last_write_seq;
            (seq, None)
        } else {
            // Read transaction: reuse last write sequence for read atomicity
            let seq = self.last_write_seq;
            (seq, Some(seq))
        };

        let txn_meta = PondTxnMetadata::new(txn_seq, meta.clone());

        let transaction_type = if is_write {
            TransactionType::Write
        } else {
            TransactionType::Read
        };
        debug!(
            "Transaction {} allocated sequence {txn_seq} (type={:?}, based_on_seq={:?})",
            &meta.txn_id, transaction_type, based_on_seq,
        );

        // Record transaction begin in control table
        self.control_table
            .record_begin(&txn_meta, based_on_seq, transaction_type)
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to record transaction begin: {}", e))
            })?;

        // Begin Data FS transaction guard with metadata
        let data_tx = if is_write {
            self.data_persistence
                .begin_write(&txn_meta)
                .await
                .map_err(StewardError::DataInit)?
        } else {
            self.data_persistence
                .begin_read(&txn_meta)
                .await
                .map_err(StewardError::DataInit)?
        };

        let vars_value: Value = meta
            .vars
            .clone()
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<Map<String, Value>>()
            .into();

        let structured_variables: HashMap<String, Value> =
            HashMap::from([("vars".to_string(), vars_value)]);

        // This is kind of weird, we should probably just let PondUserMetadata
        // pass through @@@.
        data_tx
            .state()?
            .set_template_variables(structured_variables)?;

        // Create steward transaction guard with sequence tracking
        // Pass pond_path so guard can reload OpLogPersistence for post-commit
        Ok(StewardTransactionGuard::new(
            data_tx,
            &txn_meta,
            transaction_type,
            &mut self.control_table,
            &self.pond_path,
        ))
    }

    /// Replay a transaction from backup with a specific sequence number
    ///
    /// This is used during pond restoration to replay transactions with their ORIGINAL
    /// sequence numbers from the source pond. Unlike begin_transaction() which allocates
    /// a new sequence, this method uses the provided sequence number directly.
    ///
    /// # Critical Invariant
    /// The provided txn_seq MUST equal last_write_seq + 1, or restoration will fail.
    /// Bundles must be applied in strict sequential order.
    ///
    /// # Arguments
    /// * `txn_seq` - The original transaction sequence number from the source pond
    /// * `args` - The original command arguments that created this transaction
    /// * `variables` - Any template variables from the original transaction
    ///
    /// # Returns
    /// A transaction guard configured for the specific sequence number
    pub async fn begin_transaction_replay(
        &mut self,
        txn_meta: &PondTxnMetadata,
    ) -> Result<StewardTransactionGuard<'_>, StewardError> {
        let txn_id = txn_meta.user.txn_id;
        let txn_seq = txn_meta.txn_seq;
        debug!(
            "Replaying transaction at txn_seq={} {:?}",
            txn_seq, &txn_meta.user.args
        );

        // Validate sequence - must be exactly next expected sequence
        let expected_seq = self.last_write_seq + 1;
        if txn_meta.txn_seq != expected_seq {
            return Err(StewardError::ControlTable(format!(
                "Transaction replay sequence mismatch: bundle has txn_seq={} but replica expects {}. \
                Bundles must be applied in sequential order without gaps.",
                txn_seq, expected_seq
            )));
        }

        // Update last_write_seq to this sequence
        self.last_write_seq = txn_seq;

        debug!("Transaction replay {txn_id:?} using sequence {txn_seq} (type=write, replay=true)",);

        // Record transaction begin in control table
        self.control_table
            .record_begin(
                txn_meta,
                None, // Replayed @@@ transactions are not "based on" anything
                TransactionType::Write,
            )
            .await
            .map_err(|e| {
                StewardError::ControlTable(format!("Failed to record transaction begin: {}", e))
            })?;

        // Begin Data FS transaction guard with metadata

        // For now, we still need to begin a transaction to get State access
        let data_tx = self
            .data_persistence
            .begin_write(txn_meta)
            .await
            .map_err(StewardError::DataInit)?;

        let vars_value: Value = txn_meta
            .user
            .vars
            .clone()
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<Map<String, Value>>()
            .into();

        // Add CLI variables under "vars" key
        let structured_variables: HashMap<String, Value> =
            HashMap::from([("vars".to_string(), vars_value)]);

        data_tx
            .state()?
            .set_template_variables(structured_variables)?;

        // Create steward transaction guard with sequence tracking
        Ok(StewardTransactionGuard::new(
            data_tx,
            txn_meta,
            TransactionType::Write,
            &mut self.control_table,
            self.pond_path.clone(),
        ))
    }

    /// Commit a steward transaction guard with proper sequencing
    /// This method provides the control persistence access needed for proper sequencing
    /// and handles post-commit factory execution for write transactions
    pub async fn commit_transaction(
        &mut self,
        guard: StewardTransactionGuard<'_>,
    ) -> Result<Option<()>, StewardError> {
        // Check if this is a write transaction before consuming the guard
        let is_write = guard.is_write_transaction();

        // Commit the guard (this releases the borrow on control_table)
        let commit_result = guard.commit().await?;

        // If this was a write transaction that committed data, run post-commit factories
        if is_write && commit_result.is_some() {
            // TODO: Discover and execute post-commit factories from /etc/system.d/*
            // This happens AFTER the guard is consumed, so we have full access to self
            debug!("Post-commit processing would run here for write transaction");
        }

        Ok(commit_result)
    }

    /// Check if recovery is needed by querying the control table
    /// Returns Ok(()) if no recovery needed, otherwise returns RecoveryNeeded error
    pub async fn check_recovery_needed(&mut self) -> Result<(), StewardError> {
        debug!("Checking if recovery is needed via control table");

        let incomplete = self.control_table.find_incomplete_transactions().await?;

        if let Some((txn_meta, data_fs_version)) = incomplete.first() {
            info!(
                "Recovery needed: incomplete transaction seq={}, id={}",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );

            info!(
                "Transaction details: args={:?}, vars={:?}, data_fs_version={}",
                &txn_meta.user.args, &txn_meta.user.vars, data_fs_version
            );

            // Return RecoveryNeeded error with complete metadata
            return Err(StewardError::RecoveryNeeded {
                txn_meta: txn_meta.clone(),
            });
        }

        debug!("No recovery needed");
        Ok(())
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
        for (txn_meta, data_fs_version) in &incomplete {
            info!(
                "  - Transaction seq={}, id={}, data_fs_version={}",
                txn_meta.txn_seq, txn_meta.user.txn_id, data_fs_version
            );
        }

        // Mark each incomplete transaction as completed
        // In the future, this will trigger replication coordination:
        // 1. Query data FS commit log for the transaction
        // 2. Bundle files that were created/modified
        // 3. Initiate replication to other nodes
        // 4. Then mark transaction as completed in control table

        for (txn_meta, _data_fs_version) in &incomplete {
            info!(
                "Marking transaction seq={}, id={} as recovered",
                txn_meta.txn_seq, txn_meta.user.txn_id
            );
            self.control_table
                .record_completed(txn_meta, TransactionType::Read, 0) // Assume read for recovery
                .await?;
        }

        info!(
            "Recovery completed - marked {} transaction(s) as recovered",
            incomplete.len()
        );
        Ok(RecoveryResult {
            recovered_count: incomplete.len() as u64,
            was_needed: true,
        })
    }

    /// Initialize a complete pond following the proper initialization pattern
    pub async fn initialize_pond<P: AsRef<Path>>(
        pond_path: P,
        txn_meta: &PondUserMetadata,
    ) -> Result<Self, StewardError> {
        // Step 1: Set up filesystem infrastructure (creates root with txn_seq=1)
        let mut ship = Self::create_infrastructure(
            pond_path,
            true,
            Some(txn_meta.clone()),
            None, // No preserved metadata for fresh pond
        )
        .await?;

        // Record root initialization in control table
        let txn_meta = PondTxnMetadata::new(1, txn_meta.clone());
        ship.control_table
            .record_begin(
                &txn_meta,
                None, // Hmm, or 0 @@@
                TransactionType::Write,
            )
            .await?;

        // Step 2: Use scoped transaction with init arguments
        // @@@ a litle awkward that we have two clones of PondTxnMetadata.
        ship.write_transaction(&txn_meta.user, async |fs| {
            // Step 3: Create initial pond directory structure (this generates actual filesystem operations)
            let data_root = fs.root().await?;
            _ = data_root.create_dir_path("/data").await?;

            // Transaction automatically commits on Ok return
            Ok(())
        })
        .await?;

        ship.control_table
            // @@@ define duration
            .record_data_committed(&txn_meta, TransactionType::Write, 0, 0)
            .await?;

        ship.control_table
            .record_completed(&txn_meta, TransactionType::Write, 0)
            .await?;

        ship.last_write_seq = 1;

        debug!("Pond fully initialized with transaction #1");
        Ok(ship)
    }

    /// Query OpLog records for a specific transaction sequence
    ///
    /// This is a testing API that provides access to the underlying OpLog records
    /// for verification purposes. Returns tuples of (node_id_hex, part_id_hex, version).
    ///
    /// # Arguments
    /// * `txn_seq` - The transaction sequence number to query, or None for all records
    ///
    /// # Returns
    /// Vector of tuples containing (node_id, part_id, version) as hex strings and i64
    pub async fn query_oplog_records(
        &self,
        txn_seq: Option<i64>,
    ) -> Result<Vec<(String, String, i64)>, StewardError> {
        self.data_persistence
            .query_oplog_by_txn_seq(txn_seq)
            .await
            .map_err(StewardError::DataInit)
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
        let ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to initialize pond");

        // Verify directories were created
        let data_path = get_data_path(&pond_path);
        let control_path = get_control_path(&pond_path);

        assert!(data_path.exists(), "Data directory should exist");
        assert!(control_path.exists(), "Control directory should exist");

        // Test that pond path is stored correctly
        assert_eq!(ship.pond_path, pond_path);

        // Test that we can open the same pond (like production commands do)
        let _opened_ship = Ship::open_pond(&pond_path)
            .await
            .expect("Should be able to open existing pond");
    }

    #[tokio::test]
    async fn test_ship_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use the same constructor as production (pond init)
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to initialize pond");

        // Begin a second transaction with test arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        let meta = PondUserMetadata::new(args);
        ship.write_transaction(&meta, async |fs| {
            // Do some filesystem operation to ensure the transaction has operations to commit
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/test.txt",
                b"test content",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Failed to execute scoped transaction");
    }

    #[tokio::test]
    async fn test_normal_commit_transaction() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        // Use production initialization (pond init) - this creates version 0
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to initialize pond");

        // Check that recovery is not needed after init
        ship.check_recovery_needed()
            .await
            .expect("Recovery should not be needed after init");

        // Begin a second transaction with arguments using scoped transaction
        let args = vec!["test".to_string(), "arg1".to_string(), "arg2".to_string()];
        let meta = PondUserMetadata::new(args.clone());
        ship.write_transaction(&meta, async |fs| {
            // Do some operation on data filesystem
            let data_root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &data_root,
                "/test.txt",
                b"test content",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Failed to execute scoped transaction");

        // Check that recovery is still not needed after successful commit
        let recovery_check = ship.check_recovery_needed().await;
        match recovery_check {
            Ok(()) => {
                // This is expected for a successful commit with no recovery needed
            }
            Err(StewardError::RecoveryNeeded { txn_meta }) => {
                panic!(
                    "Recovery should not be needed after successful commit, but got recovery needed for seq={}, id={}: {:?}",
                    txn_meta.txn_seq, txn_meta.user.txn_id, txn_meta.user.args
                );
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
            let mut ship = Ship::create_pond(&pond_path)
                .await
                .expect("Failed to create ship");

            // Use coordinated transaction to simulate crash between data commit and control commit
            let meta = PondUserMetadata::new(vec![
                "copy".to_string(),
                "file1.txt".to_string(),
                "file2.txt".to_string(),
            ]);

            // Step 1-3: Begin transaction, modify, commit data FS
            let mut tx = ship
                .begin_write(&meta)
                .await
                .expect("Failed to begin transaction");

            // Get data FS root from the transaction guard
            let data_root = tx.root().await.expect("Failed to get data root");

            // Modify data during the transaction
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &data_root,
                "/file1.txt",
                b"content1",
            )
            .await
            .expect("Failed to create file");

            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing

            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx
                .take_transaction()
                .expect("Transaction guard should be available");

            // Commit the transaction (metadata was already provided at begin)
            raw_tx
                .commit()
                .await
                .expect("Failed to commit transaction")
                .expect("Transaction should have committed with operations");

            // SIMULATE CRASH HERE - don't call commit_control_metadata()
            // This leaves data committed but control metadata missing

            debug!("✅ Simulated crash after data commit");
        } // Ship drops here, simulating crash

        // SECOND: Create a new ship (simulating restart) and test recovery
        {
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open pond after crash");

            // Recovery should be needed because control metadata is missing
            let recovery_result = match ship.check_recovery_needed().await {
                Err(StewardError::RecoveryNeeded { txn_meta }) => {
                    debug!(
                        "✅ Detected recovery needed for seq={}, txn_id: {}",
                        txn_meta.txn_seq, txn_meta.user.txn_id
                    );
                    debug!("✅ Recovery metadata: {:?}", txn_meta);
                    // Perform actual recovery
                    ship.recover().await.expect("Recovery should succeed")
                }
                Ok(()) => panic!("Recovery should have been needed after crash"),
                Err(e) => panic!("Unexpected error during recovery check: {:?}", e),
            };

            assert!(
                recovery_result.was_needed,
                "Recovery should have been needed"
            );
            assert_eq!(
                recovery_result.recovered_count, 1,
                "Should have recovered one transaction"
            );

            // After recovery, no further recovery should be needed
            ship.check_recovery_needed()
                .await
                .expect("Recovery should not be needed after successful recovery");

            // Verify data survived the crash and recovery (use read transaction)
            let tx = ship
                .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
                .await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let file_content = data_root
                .read_file_path_to_vec("/file1.txt")
                .await
                .expect("File should exist after recovery");
            assert_eq!(file_content, b"content1");
            _ = tx
                .commit()
                .await
                .expect("Failed to commit read transaction");

            debug!("✅ Recovery completed successfully");
        }
    }

    #[tokio::test]
    async fn test_multiple_transaction_recovery() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create ship");

        // Commit several transactions normally using scoped pattern
        for i in 1..=3 {
            let args = vec!["test".to_string(), format!("operation{}", i)];
            let meta = PondUserMetadata::new(args);
            ship.write_transaction(&meta, async |fs| {
                let data_root = fs.root().await?;
                _ = tinyfs::async_helpers::convenience::create_file_path(
                    &data_root,
                    &format!("/file{}.txt", i),
                    format!("content{}", i).as_bytes(),
                )
                .await?;
                Ok(())
            })
            .await
            .expect("Failed to execute scoped transaction");
        }

        // The key test is that recovery works with existing completed transactions
        let recovery_result = ship.recover().await.expect("Failed to recover");

        // Since all transactions were completed normally, no recovery should be needed
        assert_eq!(
            recovery_result.recovered_count, 0,
            "No recovery should be needed for completed transactions"
        );
        assert!(
            !recovery_result.was_needed,
            "Recovery should not have been needed"
        );

        ship.check_recovery_needed()
            .await
            .expect("Recovery should not be needed after recovery");
    }

    #[tokio::test]
    async fn test_recovery_command_interface() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create ship");

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
            let _ship = Ship::create_pond(&pond_path)
                .await
                .expect("Failed to initialize pond");
        }

        // Step 2: Create a transaction with metadata that commits to data FS but crashes before control FS
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open existing pond");

            // Simulate a crash scenario using coordinated transaction approach
            let copy_args = vec![
                "pond".to_string(),
                "copy".to_string(),
                "source.txt".to_string(),
                "dest.txt".to_string(),
            ];

            let mut tx = {
                // Pass copy_args to begin_transaction so they're recorded in control table
                let tx = ship
                    .begin_write(&PondUserMetadata::new(copy_args.clone()))
                    .await
                    .expect("Failed to begin transaction");

                // Get data FS root from the transaction guard
                let data_root = tx.root().await.expect("Failed to get data root");

                // Do actual file operation
                _ = tinyfs::async_helpers::convenience::create_file_path(
                    &data_root,
                    "/dest.txt",
                    b"copied content",
                )
                .await
                .expect("Failed to create dest file");

                tx
            };

            // For testing purposes, we need to manually commit without using the steward commit logic
            // This simulates a crash where the data transaction commits but control metadata is missing

            // Extract the raw transaction guard for direct commit (testing only)
            let raw_tx = tx
                .take_transaction()
                .expect("Transaction guard should be available");

            // Commit the transaction (metadata was already provided at begin)
            raw_tx
                .commit()
                .await
                .expect("Failed to commit transaction")
                .expect("Transaction should have committed with operations");

            // SIMULATE CRASH: Don't record control metadata
            debug!("✅ Simulated crash after data commit");
        }

        // Step 3: Recovery after crash using production code
        {
            // Use production code to open existing pond
            let mut ship = Ship::open_pond(&pond_path)
                .await
                .expect("Failed to open existing pond for recovery");

            // Check that recovery is needed
            let check_result = ship.check_recovery_needed().await;
            assert!(
                check_result.is_err(),
                "Should detect that recovery is needed"
            );

            let recovered_txn_meta =
                if let Err(StewardError::RecoveryNeeded { txn_meta }) = check_result {
                    debug!(
                        "Should need recovery for seq={}, txn_id: {}",
                        txn_meta.txn_seq, txn_meta.user.txn_id
                    );
                    debug!("Recovery metadata: {:?}", txn_meta);
                    txn_meta
                } else {
                    panic!("Expected RecoveryNeeded error, got: {:?}", check_result);
                };

            // Perform recovery
            let recovery_result = ship.recover().await.expect("Recovery should succeed");
            assert_eq!(
                recovery_result.recovered_count, 1,
                "Should recover exactly 1 transaction"
            );
            assert!(
                recovery_result.was_needed,
                "Recovery should have been needed"
            );

            // Verify recovery is no longer needed
            ship.check_recovery_needed()
                .await
                .expect("Recovery should not be needed after successful recovery");

            // Verify the recovered transaction descriptor matches what we expected
            assert_eq!(
                recovered_txn_meta.user.args,
                vec![
                    "pond".to_string(),
                    "copy".to_string(),
                    "source.txt".to_string(),
                    "dest.txt".to_string()
                ]
            );
            assert_eq!(
                recovered_txn_meta.user.args.first().map(|s| s.as_str()),
                Some("pond")
            );

            // Verify the data file still exists (use read transaction)
            let tx = ship
                .begin_read(&PondUserMetadata::new(vec!["verify".to_string()]))
                .await
                .expect("Failed to begin read transaction");
            let data_root = tx.root().await.expect("Failed to get data root");
            let reader = data_root
                .async_reader_path("/dest.txt")
                .await
                .expect("File should exist after recovery");
            let file_content = tinyfs::buffer_helpers::read_all_to_vec(reader)
                .await
                .expect("Failed to read file content");
            assert_eq!(file_content, b"copied content");
            _ = tx
                .commit()
                .await
                .expect("Failed to commit read transaction");
        }
    }

    #[tokio::test]
    async fn test_no_recovery_needed_for_consistent_state() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond");

        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create ship");

        // Do normal complete transactions using scoped pattern
        for i in 1..=3 {
            let args = vec![
                "pond".to_string(),
                "mkdir".to_string(),
                format!("/dir{}", i),
            ];
            let meta = PondUserMetadata::new(args);
            ship.write_transaction(&meta, async |fs| {
                let data_root = fs.root().await?;
                _ = data_root
                    .create_dir_path(&format!("/dir{}", i))
                    .await?;
                Ok(())
            })
            .await
            .expect("Failed to execute scoped transaction");
        }

        // Check that no recovery is needed
        ship.check_recovery_needed()
            .await
            .expect("No recovery should be needed for consistent state");

        // Run recovery anyway - should be no-op
        let recovery_result = ship
            .recover()
            .await
            .expect("Recovery should succeed even when not needed");
        assert_eq!(recovery_result.recovered_count, 0);
        assert!(!recovery_result.was_needed);

        // State should still be consistent
        ship.check_recovery_needed()
            .await
            .expect("State should remain consistent after no-op recovery");
    }

    #[tokio::test]
    async fn test_new_transaction_api() {
        // Test the new begin_transaction API to ensure it has proper sequencing
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_new_api");

        // Initialize a new pond
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create pond");

        // Test: Use begin_transaction - this demonstrates the API
        let tx = ship
            .begin_write(&PondUserMetadata::new(vec!["test".to_string()]))
            .await
            .expect("Failed to begin transaction");

        // Perform some work
        let root = tx.root().await.expect("Failed to get root");
        _ = tinyfs::async_helpers::convenience::create_file_path(
            &root,
            "/test_file.txt",
            b"test content",
        )
        .await
        .expect("Failed to create file");

        // The commit step demonstrates the borrow checker challenge
        // ship.commit_transaction(tx).await - this would fail due to borrow checker
        // Instead, we rely on the steward guard's commit method which has the proper sequencing
        _ = tx
            .commit()
            .await
            .expect("Failed to commit steward transaction");

        debug!("✅ New transaction API works with proper sequencing via steward guard commit");
    }

    #[tokio::test]
    async fn test_transaction_sequence_numbering() {
        // Test that transaction sequences are allocated correctly without conflicts
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_pond_sequences");

        // Initialize pond - this creates root directory with txn_seq=1
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create pond");

        // Verify initial sequence is 1 (root directory was created with txn_seq=1)
        let initial_seq = ship.last_write_seq;
        assert_eq!(
            initial_seq, 1,
            "Initial sequence should be 1 after root directory creation"
        );

        // First user transaction should get txn_seq=2
        let meta = PondUserMetadata::new(vec!["first".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/file1.txt",
                b"content1",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("First transaction should succeed");

        // Verify sequence advanced to 2
        let after_first = ship.last_write_seq;

        assert_eq!(
            after_first, 2,
            "After first user transaction, sequence should be 2"
        );

        // Second user transaction should get txn_seq=3
        let meta = PondUserMetadata::new(vec!["second".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;
            _ = tinyfs::async_helpers::convenience::create_file_path(
                &root,
                "/file2.txt",
                b"content2",
            )
            .await?;
            Ok(())
        })
        .await
        .expect("Second transaction should succeed");

        // Verify sequence advanced to 3
        let after_second = ship.last_write_seq;
        assert_eq!(
            after_second, 3,
            "After second user transaction, sequence should be 3"
        );

        // Verify no conflicts: root=1, first=2, second=3
        debug!("Transaction sequence test passed: root=1, first_user=2, second_user=3");
    }

    #[tokio::test]
    async fn test_root_directory_version_and_control_table() {
        // Test that root initialization creates v1 and records it in control table
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_root_version");

        // Initialize pond
        let ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create pond");

        // Verify that last_write_seq is 1 after root initialization
        let last_seq = ship.last_write_seq;
        assert_eq!(last_seq, 1, "After root init, last_write_seq should be 1");

        // Verify control table has the root transaction recorded
        let control_last_seq = ship
            .control_table
            .get_last_write_sequence()
            .await
            .expect("Failed to query control table");
        assert_eq!(
            control_last_seq, 1,
            "Control table should show last write sequence as 1"
        );

        // Reopen the pond to verify persistence
        drop(ship);
        let ship2 = Ship::open_pond(&pond_path)
            .await
            .expect("Failed to reopen pond");

        let reopened_seq = ship2.last_write_seq;
        assert_eq!(
            reopened_seq, 1,
            "After reopening, last_write_seq should still be 1"
        );

        debug!("✅ Root directory initialization is recorded in control table with txn_seq=1");
    }

    #[tokio::test]
    async fn test_directory_tree_single_version_per_node() {
        // Test that creating a tree of directories in one transaction creates exactly one version per node
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let pond_path = temp_dir.path().join("test_dir_tree");

        // Initialize pond
        let mut ship = Ship::create_pond(&pond_path)
            .await
            .expect("Failed to create pond");

        // Create a tree of directories in a single transaction
        let meta = PondUserMetadata::new(vec!["create_tree".to_string()]);
        ship.write_transaction(&meta, async |fs| {
            let root = fs.root().await?;

            // Create nested directory structure: /a/b/c and /a/d/e
            _ = root.create_dir_path("/a").await?;
            _ = root.create_dir_path("/a/b").await?;
            _ = root.create_dir_path("/a/b/c").await?;
            _ = root.create_dir_path("/a/d").await?;
            _ = root.create_dir_path("/a/d/e").await?;

            Ok(())
        })
        .await
        .expect("Failed to create directory tree");

        // Verify the transaction sequence advanced properly
        let after_tree = ship.last_write_seq;

        assert_eq!(
            after_tree, 2,
            "After creating directory tree, sequence should be 2"
        );

        // Query OpLog records for transaction 2 to verify version numbers
        let records = ship
            .query_oplog_records(Some(2))
            .await
            .expect("Failed to query OpLog records");

        debug!("Found {} records in txn_seq=2", records.len());
        for (node_id, part_id, version) in records.iter() {
            debug!(
                "  Record: node_id={}, part_id={}, version={}",
                node_id, part_id, version
            );
        }

        // Group records by node_id to count versions per node
        use std::collections::HashMap;
        let mut node_versions: HashMap<String, Vec<i64>> = HashMap::new();

        for (node_id, _part_id, version) in records.iter() {
            node_versions
                .entry(node_id.clone())
                .or_default()
                .push(*version);
        }

        // Verify each node has exactly one version in transaction 2
        for (node_id, versions) in node_versions.iter() {
            assert_eq!(
                versions.len(),
                1,
                "Node {} should have exactly 1 version in txn_seq=2, but has {} versions: {:?}",
                node_id,
                versions.len(),
                versions
            );

            // Verify version numbers: root gets v2, new nodes get v1
            // Node IDs are stored in full UUID format, but root is identified by starting with "00000000"
            if node_id.starts_with("00000000") {
                assert_eq!(
                    versions[0], 2,
                    "Root directory ({}) should have v2 in second transaction",
                    node_id
                );
            } else {
                assert_eq!(
                    versions[0], 1,
                    "New directory {} should have v1, got v{}",
                    node_id, versions[0]
                );
            }
        }

        // Verify we created the expected number of directories
        // Root (updated) + /a + /a/b + /a/b/c + /a/d + /a/d/e = 6 nodes total
        assert_eq!(
            node_versions.len(),
            6,
            "Should have 6 nodes in txn_seq=2 (root + 5 new dirs), got: {:?}",
            node_versions.keys().collect::<Vec<_>>()
        );

        debug!(
            "✅ Directory tree creation produces exactly one version per node with correct numbering"
        );
    }
}
