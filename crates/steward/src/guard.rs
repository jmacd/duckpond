// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward Transaction Guard - Wraps tlogfs transaction with audit logging

use crate::{
    PondTxnMetadata, StewardError,
    control_table::{ControlTable, TransactionType},
};
use log::{debug, error, info};
use provider::FactoryRegistry;
use provider::registry::ExecutionContext;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinyfs::FS;
use tlogfs::transaction_guard::TransactionGuard;

/// Configuration for a post-commit factory to be executed
struct PostCommitFactoryConfig {
    factory_name: String,
    config_path: String,
    config_bytes: Vec<u8>,
    parent_node_id: tinyfs::FileID,
    factory_mode: String,
}

/// Steward transaction guard wraps TLogFS transaction with control table lifecycle tracking
pub struct StewardTransactionGuard<'a> {
    /// The underlying tlogfs transaction guard
    data_tx: Option<TransactionGuard<'a>>,
    txn_meta: PondTxnMetadata,
    /// Transaction type: read or write
    transaction_type: TransactionType,
    /// Reference to control table for transaction tracking
    control_table: &'a mut ControlTable,
    /// Start time for duration tracking
    start_time: std::time::Instant,
    /// Pond path for reloading OpLogPersistence during post-commit
    pond_path: PathBuf,
    /// Track if commit() was called to record failure on drop if not
    committed: bool,
}

impl<'a> StewardTransactionGuard<'a> {
    /// Create a new steward transaction guard
    pub(crate) fn new<P: AsRef<Path>>(
        data_tx: TransactionGuard<'a>,
        txn_meta: &PondTxnMetadata,
        transaction_type: TransactionType,
        control_table: &'a mut ControlTable,
        path: P,
    ) -> Self {
        Self {
            data_tx: Some(data_tx),
            txn_meta: txn_meta.clone(),
            transaction_type,
            control_table,
            start_time: std::time::Instant::now(),
            pond_path: path.as_ref().to_path_buf(),
            committed: false,
        }
    }

    /// Get the transaction ID
    #[must_use]
    pub fn txn_meta(&self) -> &PondTxnMetadata {
        &self.txn_meta
    }

    /// Get the transaction type
    #[must_use]
    pub fn transaction_type(&self) -> TransactionType {
        self.transaction_type
    }

    /// Check if this is a write transaction
    #[must_use]
    pub fn is_write_transaction(&self) -> bool {
        self.transaction_type == TransactionType::Write
    }

    /// Check if this is a read transaction
    #[must_use]
    pub fn is_read_transaction(&self) -> bool {
        self.transaction_type == TransactionType::Read
    }

    /// Get access to the control table for querying transaction metadata
    /// This allows accessing control table data without violating borrow rules
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        self.control_table
    }

    /// Take the underlying transaction guard (for commit)
    pub(crate) fn take_transaction(&mut self) -> Option<TransactionGuard<'a>> {
        self.data_tx.take()
    }

    /// Get access to the underlying transaction state (for queries)
    ///
    /// Prefer `provider_context()` when all you need is a `ProviderContext`.
    /// This method exposes tlogfs internals and should only be used when
    /// you need oplog-specific operations like `get_factory_for_node()` or
    /// `query_records()`.
    pub fn state(&self) -> Result<tlogfs::persistence::State, tlogfs::TLogFSError> {
        self.data_tx
            .as_ref()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .state()
    }

    /// Get a `ProviderContext` for this transaction.
    ///
    /// This is the preferred way to get provider/factory context. It avoids
    /// exposing tlogfs internals (`State`) to callers that only need to
    /// create table providers or initialize factories.
    pub fn provider_context(
        &self,
    ) -> Result<tinyfs::ProviderContext, tlogfs::TLogFSError> {
        Ok(self.state()?.as_provider_context())
    }

    /// Get access to the underlying data persistence layer for read operations
    /// This allows access to the DeltaTable and other query components
    pub fn data_persistence(&self) -> Result<&tlogfs::OpLogPersistence, tlogfs::TLogFSError> {
        Ok(self
            .data_tx
            .as_ref()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .persistence())
    }

    /// Get or create a DataFusion SessionContext with TinyFS ObjectStore registered
    ///
    /// This method ensures a single SessionContext per transaction, preventing
    /// ObjectStore registry conflicts when creating multiple table providers.
    /// Delegates to the underlying TLogFS transaction guard.
    pub async fn session_context(
        &mut self,
    ) -> Result<Arc<datafusion::execution::context::SessionContext>, tlogfs::TLogFSError> {
        self.data_tx
            .as_mut()
            .ok_or_else(|| {
                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction guard has been consumed".to_string(),
                ))
            })?
            .session_context()
            .await
    }

    /// Get mutable access to the underlying TransactionGuard for tlogfs operations
    /// This allows tlogfs functions to accept the transaction guard directly
    pub fn transaction_guard(&mut self) -> Result<&mut TransactionGuard<'a>, tlogfs::TLogFSError> {
        self.data_tx.as_mut().ok_or_else(|| {
            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                "Transaction guard has been consumed".to_string(),
            ))
        })
    }

    /// Abort the transaction and record it as failed
    /// Use this when an error occurs that should be recorded in the control table
    pub async fn abort(mut self, error: impl std::fmt::Display) -> StewardError {
        let duration_ms = self.start_time.elapsed().as_millis() as i64;
        let error_msg = error.to_string();

        debug!(
            "Aborting steward transaction {} (seq={}): {}",
            &self.txn_meta.user.txn_id, self.txn_meta.txn_seq, &error_msg
        );

        // Record the failure in control table
        if let Err(e) = self
            .control_table
            .record_failed(
                &self.txn_meta,
                self.transaction_type,
                error_msg.clone(),
                duration_ms,
            )
            .await
        {
            log::error!("Failed to record transaction failure: {}", e);
        }

        // Drop the transaction guard without committing (will rollback)
        self.committed = true; // Mark as handled to avoid Drop warning
        drop(self.data_tx.take());

        // Return the original error wrapped in StewardError
        StewardError::Aborted(error_msg)
    }

    /// Commit the transaction with proper steward sequencing
    /// Returns whether a write transaction occurred
    pub async fn commit(mut self) -> Result<Option<()>, StewardError> {
        let args_fmt = format!("{:?}", &self.txn_meta.user.args);
        debug!(
            "Committing steward transaction {} {}",
            &self.txn_meta.user.txn_id, &args_fmt
        );

        // Calculate duration for recording
        let duration_ms = self.start_time.elapsed().as_millis() as i64;

        // Get current table version before commit (the commit will increment it)
        let pre_commit_version = self
            .data_tx
            .as_ref()
            .ok_or_else(|| {
                StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                    "Transaction already consumed".to_string(),
                )))
            })?
            .persistence()
            .table()
            .version();

        // Step 1: Transaction metadata was already provided at begin()
        // Step 2: Extract the underlying transaction guard and commit it
        let data_tx = self.take_transaction().ok_or_else(|| {
            StewardError::DataInit(tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                "Transaction already consumed".to_string(),
            )))
        })?;

        let commit_result = data_tx.commit().await;

        // Step 3: Record transaction lifecycle in control table based on result
        match commit_result {
            Ok(Some(())) => {
                // Write transaction committed successfully - version is pre_commit_version + 1
                let new_version = pre_commit_version.unwrap_or(0) + 1;

                // VALIDATION: If this was marked as a read transaction but wrote data, fail
                if self.transaction_type == TransactionType::Read {
                    self.control_table
                        .record_failed(
                            &self.txn_meta,
                            self.transaction_type,
                            "Read transaction attempted to write data".to_string(),
                            duration_ms,
                        )
                        .await
                        .map_err(|e| {
                            StewardError::ControlTable(format!("Failed to record failure: {}", e))
                        })?;
                    return Err(StewardError::ReadTransactionAttemptedWrite);
                }

                self.control_table
                    .record_data_committed(
                        &self.txn_meta,
                        self.transaction_type,
                        new_version,
                        duration_ms,
                    )
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to record commit: {}", e))
                    })?;
                info!(
                    "Steward transaction {} committed (seq={}, version={})",
                    &self.txn_meta.user.txn_id, self.txn_meta.txn_seq, new_version
                );

                // Mark as committed
                self.committed = true;

                // Run post-commit factories for write transactions
                // This happens AFTER commit but uses a NEW transaction
                self.run_post_commit_factories().await;

                Ok(Some(()))
            }
            Ok(None) => {
                // Read-only transaction completed successfully
                // Note: Write transactions that make no changes are allowed (idempotent operations like mkdir -p)
                // We record them as "completed" rather than "data_committed" since no version was created

                self.control_table
                    .record_completed(&self.txn_meta, self.transaction_type, duration_ms)
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to record completion: {}", e))
                    })?;

                if self.transaction_type == TransactionType::Write {
                    debug!(
                        "Write-no-op steward transaction {} completed (seq={})",
                        &self.txn_meta.user.txn_id, self.txn_meta.txn_seq
                    );
                } else {
                    debug!(
                        "Read-only steward transaction {} completed (seq={})",
                        &self.txn_meta.user.txn_id, self.txn_meta.txn_seq
                    );
                }
                self.committed = true;
                Ok(None)
            }
            Err(e) => {
                // Transaction failed - record error
                let error_msg = format!("{}", e);
                self.control_table
                    .record_failed(
                        &self.txn_meta,
                        self.transaction_type,
                        error_msg.clone(),
                        duration_ms,
                    )
                    .await
                    .map_err(|e| {
                        StewardError::ControlTable(format!("Failed to record failure: {}", e))
                    })?;
                self.committed = true;
                Err(StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))
            }
        }
    }

    /// Run post-commit factories after a successful write transaction
    /// This discovers and executes factories from /etc/system.d/* in order
    /// Only runs factories configured with "push" mode (skips "pull" mode factories)
    async fn run_post_commit_factories(&mut self) {
        debug!("Starting post-commit factory discovery and execution");

        // Discover post-commit factory configurations
        let factory_configs = match self.discover_post_commit_factories().await {
            Ok(configs) => configs,
            Err(e) => {
                log::warn!("Failed to discover post-commit factories: {}", e);
                return;
            }
        };

        if factory_configs.is_empty() {
            debug!("No post-commit factories found");
            return;
        }

        debug!("Discovered {} factory node(s)", factory_configs.len());

        // Filter factories based on their execution mode
        let mut factories_to_run = Vec::new();

        for mut config in factory_configs {
            // Check factory mode setting - set default "push" for unconfigured factories
            let factory_mode = match self.control_table.get_factory_mode(&config.factory_name) {
                Some(mode) => mode,
                None => {
                    // Default to "push" mode for post-commit factories in /etc/system.d/
                    info!(
                        "Factory '{}' has no mode configured, defaulting to 'push' (automatic execution)",
                        config.factory_name
                    );
                    // Set the default mode in control table for future use
                    if let Err(e) = self
                        .control_table
                        .set_factory_mode(&config.factory_name, "push")
                        .await
                    {
                        error!(
                            "Failed to set default factory mode for '{}': {}",
                            config.factory_name, e
                        );
                        continue; // Skip this factory if we can't set mode
                    }
                    "push".to_string()
                }
            };

            if factory_mode == "pull" {
                info!(
                    "Skipping factory '{}' (mode: pull, only runs on manual sync)",
                    config.factory_name
                );
                continue;
            }

            debug!(
                "Will execute factory '{}' (mode: {})",
                config.factory_name, factory_mode
            );
            config.factory_mode = factory_mode;
            factories_to_run.push(config);
        }

        if factories_to_run.is_empty() {
            info!("No factories configured for post-commit execution");
            return;
        }

        info!(
            "Executing {} post-commit factor{}",
            factories_to_run.len(),
            if factories_to_run.len() == 1 {
                "y"
            } else {
                "ies"
            }
        );

        // Record pending status for all factories to run
        for (execution_seq, config) in factories_to_run.iter().enumerate() {
            let execution_seq = (execution_seq + 1) as i64; // 1-indexed, i64 to match record_post_commit_* signatures
            if let Err(e) = self
                .control_table
                .record_post_commit_pending(
                    &self.txn_meta,
                    execution_seq,
                    config.factory_name.clone(),
                    config.config_path.clone(),
                )
                .await
            {
                log::error!(
                    "Failed to record post-commit pending for {}: {}",
                    config.config_path,
                    e
                );
                // Continue despite tracking failure
            }
        }

        let total_factories = factories_to_run.len();

        // Execute each factory independently
        for (execution_seq, config) in factories_to_run.into_iter().enumerate() {
            let execution_seq = (execution_seq + 1) as i64; // 1-indexed, i64 to match record_post_commit_* signatures
            debug!(
                "Executing post-commit factory {}/{}: {} from {} (mode: {})",
                execution_seq,
                total_factories,
                config.factory_name,
                config.config_path,
                config.factory_mode
            );

            // Record started status
            if let Err(e) = self
                .control_table
                .record_post_commit_started(&self.txn_meta, execution_seq)
                .await
            {
                log::error!(
                    "Failed to record post-commit started for {}: {}",
                    config.config_path,
                    e
                );
                // Continue despite tracking failure
            }

            let start_time = std::time::Instant::now();

            // Execute the factory
            match self
                .execute_post_commit_factory(
                    &config.factory_name,
                    &config.config_path,
                    &config.config_bytes,
                    config.parent_node_id,
                    &config.factory_mode,
                )
                .await
            {
                Ok(()) => {
                    let duration_ms = start_time.elapsed().as_millis() as i64;
                    info!(
                        "Post-commit factory succeeded: {} ({}ms)",
                        config.config_path, duration_ms
                    );

                    // Record completion
                    if let Err(e) = self
                        .control_table
                        .record_post_commit_completed(&self.txn_meta, execution_seq, duration_ms)
                        .await
                    {
                        log::error!(
                            "Failed to record post-commit completion for {}: {}",
                            config.config_path,
                            e
                        );
                    }
                }
                Err(e) => {
                    let duration_ms = start_time.elapsed().as_millis() as i64;
                    let error_message = format!("{}", e);
                    log::error!(
                        "Post-commit factory failed: {} - {}",
                        config.config_path,
                        error_message
                    );

                    // Record failure
                    if let Err(e) = self
                        .control_table
                        .record_post_commit_failed(
                            &self.txn_meta,
                            execution_seq,
                            error_message,
                            duration_ms,
                        )
                        .await
                    {
                        log::error!(
                            "Failed to record post-commit failure for {}: {}",
                            config.config_path,
                            e
                        );
                    }

                    // Continue to next factory despite failure
                }
            }
        }

        info!("Post-commit factory execution complete");
    }

    /// Discover post-commit factory configurations from /etc/system.d/*
    /// Returns factory configs sorted by config_path
    async fn discover_post_commit_factories(
        &self,
    ) -> Result<Vec<PostCommitFactoryConfig>, StewardError> {
        debug!("Discovering post-commit factories from /etc/system.d/*");

        // Post-commit discovery happens AFTER the transaction is committed
        // We need a NEW read transaction to see the committed data
        // This is a separate operation, not part of the original write transaction
        let data_path = crate::get_data_path(Path::new(&self.pond_path));
        let mut data_persistence = tlogfs::OpLogPersistence::open(&data_path)
            .await
            .map_err(StewardError::DataInit)?;

        // Open a read transaction for discovery
        // Use the CURRENT txn_seq (the one we just committed), not +1
        // Read transactions must use the last write sequence
        let discovery_metadata = PondTxnMetadata::new(
            self.txn_meta.txn_seq,
            tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "post-commit-discovery".to_string(),
            ]),
        );
        let discovery_tx = data_persistence
            .begin_read(&discovery_metadata)
            .await
            .map_err(StewardError::DataInit)?;

        let fs = FS::new(discovery_tx.state()?)
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        let root = fs
            .root()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Check if /etc/system.d exists before trying to match
        debug!(
            "Checking for /etc/system.d directory at txn_seq={}",
            self.txn_meta.txn_seq
        );
        match root.resolve_path("/etc/system.d").await {
            Ok((_wd, _lookup)) => {
                debug!("Resolved /etc/system.d");
            }
            Err(e) => {
                debug!("Failed to resolve /etc/system.d: {}", e);
                // Commit the discovery transaction before returning
                _ = discovery_tx.commit().await;
                return Ok(Vec::new());
            }
        }

        // Use collect_matches to find all configs in /etc/system.d/*
        // Returns Vec<(NodePath, Vec<String>)> where first element is path, second is captures
        debug!("Looking for configs matching /etc/system.d/*");
        let matches = root
            .collect_matches("/etc/system.d/*")
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        debug!("Found {} matches for /etc/system.d/*", matches.len());

        let mut factory_configs = Vec::new();

        for (node_path, _captures) in matches {
            let config_path_str = node_path.path().to_string_lossy().to_string();
            let config_file_id = node_path.id();
            debug!(
                "Found post-commit config: {} (id={})",
                config_path_str, config_file_id
            );

            // Get parent directory ID (needed for factory context)
            let parent_path = node_path.dirname();
            let (parent_wd, _lookup) = match root.resolve_path(&parent_path).await {
                Ok(result) => result,
                Err(e) => {
                    log::warn!(
                        "Failed to resolve parent path {}: {}",
                        parent_path.display(),
                        e
                    );
                    continue;
                }
            };
            let parent_id = parent_wd.node_path().id();

            // Read the config file using async_reader_path + read_to_end
            let config_bytes = {
                let mut reader = match root.async_reader_path(node_path.path()).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("Failed to open config {}: {}", config_path_str, e);
                        continue;
                    }
                };

                let mut buffer = Vec::new();
                match tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer).await {
                    Ok(bytes_read) => {
                        debug!("Read {} bytes from config {}", bytes_read, config_path_str);
                        debug!("Config content: {}", String::from_utf8_lossy(&buffer));
                        buffer
                    }
                    Err(e) => {
                        log::warn!("Failed to read config {}: {}", config_path_str, e);
                        continue;
                    }
                }
            };

            // Get the factory name from the oplog entry for this config file
            let factory_name = match discovery_tx
                .state()?
                .get_factory_for_node(config_file_id)
                .await
            {
                Ok(Some(name)) => name,
                Ok(None) => {
                    log::warn!("No factory associated with config: {}", config_path_str);
                    continue;
                }
                Err(e) => {
                    log::warn!("Failed to get factory for {}: {}", config_path_str, e);
                    continue;
                }
            };

            factory_configs.push(PostCommitFactoryConfig {
                factory_name,
                config_path: config_path_str,
                config_bytes,
                parent_node_id: parent_id,
                factory_mode: String::new(), // Will be set in run_post_commit_factories
            });
        }

        // Commit the post-commit discovery transaction
        _ = discovery_tx
            .commit()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Sort by config_path for deterministic execution order
        factory_configs.sort_by(|a, b| a.config_path.cmp(&b.config_path));

        Ok(factory_configs)
    }

    /// Execute a single post-commit factory
    async fn execute_post_commit_factory(
        &mut self,
        factory_name: &str,
        config_path: &str,
        config_bytes: &[u8],
        parent_node_id: tinyfs::FileID,
        factory_mode: &str,
    ) -> Result<(), StewardError> {
        debug!(
            "Executing post-commit factory: {} with config {} (mode: {})",
            factory_name, config_path, factory_mode
        );

        // factory_mode is passed as parameter, no need to query again

        debug!("Factory {} has mode: {}", factory_name, factory_mode);

        // Reload OpLogPersistence for a fresh read transaction
        let data_path = crate::get_data_path(&self.pond_path);
        let mut data_persistence = tlogfs::OpLogPersistence::open(&data_path)
            .await
            .map_err(StewardError::DataInit)?;

        // Open a new read transaction for factory execution
        // Use self.txn_seq + 1 to read the data that was just committed
        let metadata = PondTxnMetadata::new(
            self.txn_meta.txn_seq + 1,
            tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "post-commit-factory".to_string(),
            ]),
        );
        let factory_tx = data_persistence
            .begin_write(&metadata)
            .await
            .map_err(StewardError::DataInit)?;

        // Query pond metadata from control table
        let pond_metadata = self.control_table.get_pond_metadata();

        // Create factory context with pond metadata
        let state = factory_tx.state()?;
        let provider_context = state.as_provider_context();
        let factory_context = provider::FactoryContext::with_metadata(
            provider_context,
            parent_node_id,
            pond_metadata.clone(),
        )
        .with_txn_seq(self.txn_meta.txn_seq);

        // Pass factory mode as args[0]
        let args = vec![factory_mode.to_string()];

        // Execute the factory as a ControlWriter.
        let result = FactoryRegistry::execute(
            factory_name,
            config_bytes,
            factory_context,
            ExecutionContext::control_writer(args),
        )
        .await;

        // Commit the post-commit factory execution transaction
        // Metadata was already provided at begin()
        _ = factory_tx
            .commit()
            .await
            .map_err(|e| StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        result.map_err(StewardError::DataInit)
    }
}

impl<'a> Deref for StewardTransactionGuard<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        self.data_tx
            .as_ref()
            .expect("Transaction guard has been consumed")
    }
}

impl<'a> Drop for StewardTransactionGuard<'a> {
    fn drop(&mut self) {
        if self.data_tx.is_some() && !self.committed {
            // Transaction was neither committed nor explicitly failed
            // This happens when an error occurs before commit() is called
            // The transaction will rollback but we can't record failure here (Drop isn't async)
            log::warn!(
                "Steward transaction guard dropped without commit - transaction will rollback (txn={}, seq={}). \
                 Note: This transaction will show as INCOMPLETE in control table.",
                &self.txn_meta.user.txn_id,
                self.txn_meta.txn_seq,
            );
        }
    }
}
