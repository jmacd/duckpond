// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Steward and Transaction dispatch enums
//!
//! These enums wrap the concrete steward implementations (Pond, Host) and
//! forward all method calls via match dispatch. Commands use `Steward` and
//! `Transaction` as their interface, never referencing the concrete types
//! directly.

use crate::{
    RecoveryResult, StewardError, StewardTransactionGuard,
    control_table::{ControlTable, TransactionType},
    ship::Ship,
};
use std::ops::{AsyncFnOnce, Deref};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinyfs::FS;
use tlogfs::{PondMetadata, PondTxnMetadata, PondUserMetadata};

// ---------------------------------------------------------------------------
// Steward enum -- wraps the concrete steward implementations
// ---------------------------------------------------------------------------

/// Dispatch enum for steward implementations.
///
/// Currently only supports Pond (tlogfs-backed) steward. Host steward
/// will be added as a second variant when the hostmount persistence
/// layer is ready.
pub enum Steward {
    /// Full tlogfs-backed steward with Delta Lake transactions and control table
    Pond(Ship),
}

impl Steward {
    // -- Pond-specific constructors (wrap Ship) --

    /// Initialize a completely new pond.
    pub async fn create_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Ship::create_pond(pond_path).await?))
    }

    /// Open an existing pond.
    pub async fn open_pond<P: AsRef<Path>>(pond_path: P) -> Result<Self, StewardError> {
        Ok(Steward::Pond(Ship::open_pond(pond_path).await?))
    }

    /// Create pond infrastructure for bundle restoration.
    pub async fn create_pond_for_restoration<P: AsRef<Path>>(
        pond_path: P,
        preserve_metadata: PondMetadata,
    ) -> Result<Self, StewardError> {
        Ok(Steward::Pond(
            Ship::create_pond_for_restoration(pond_path, preserve_metadata).await?,
        ))
    }

    // -- Transaction lifecycle --

    /// Begin a read transaction.
    pub async fn begin_read(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<Transaction<'_>, StewardError> {
        match self {
            Steward::Pond(ship) => Ok(Transaction::Pond(ship.begin_read(meta).await?)),
        }
    }

    /// Begin a write transaction.
    pub async fn begin_write(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<Transaction<'_>, StewardError> {
        match self {
            Steward::Pond(ship) => Ok(Transaction::Pond(ship.begin_write(meta).await?)),
        }
    }

    /// Execute operations within a scoped write transaction.
    ///
    /// Runs the async closure with `&FS` access, auto-commits on `Ok`,
    /// auto-aborts on `Err`.
    pub async fn write_transaction<F>(
        &mut self,
        meta: &PondUserMetadata,
        f: F,
    ) -> Result<(), StewardError>
    where
        F: for<'a> AsyncFnOnce(&'a FS) -> Result<(), StewardError>,
    {
        match self {
            Steward::Pond(ship) => ship.write_transaction(meta, f).await,
        }
    }

    // -- Control table access (pond-specific) --

    /// Get a reference to the control table.
    ///
    /// Panics if called on a non-Pond steward (not yet applicable).
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        match self {
            Steward::Pond(ship) => ship.control_table(),
        }
    }

    /// Get a mutable reference to the control table.
    ///
    /// Panics if called on a non-Pond steward (not yet applicable).
    pub fn control_table_mut(&mut self) -> &mut ControlTable {
        match self {
            Steward::Pond(ship) => ship.control_table_mut(),
        }
    }

    // -- Recovery (pond-specific) --

    /// Check if recovery is needed.
    pub async fn check_recovery_needed(&mut self) -> Result<(), StewardError> {
        match self {
            Steward::Pond(ship) => ship.check_recovery_needed().await,
        }
    }

    /// Run recovery.
    pub async fn recover(&mut self) -> Result<RecoveryResult, StewardError> {
        match self {
            Steward::Pond(ship) => ship.recover().await,
        }
    }

    // -- Access to the underlying Ship (for pond-specific operations) --

    /// Get the underlying Ship if this is a Pond steward.
    ///
    /// Use this for pond-specific operations that are not part of the
    /// common Steward interface (e.g., `replay_transaction`,
    /// `query_oplog_records`).
    #[must_use]
    pub fn as_pond(&self) -> Option<&Ship> {
        match self {
            Steward::Pond(ship) => Some(ship),
        }
    }

    /// Get the underlying Ship mutably if this is a Pond steward.
    pub fn as_pond_mut(&mut self) -> Option<&mut Ship> {
        match self {
            Steward::Pond(ship) => Some(ship),
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction enum -- wraps the concrete transaction guard implementations
// ---------------------------------------------------------------------------

/// Dispatch enum for transaction guard implementations.
///
/// Wraps the concrete transaction guards and forwards all method calls.
/// Implements `Deref<Target=FS>` for filesystem access.
pub enum Transaction<'a> {
    /// Full tlogfs transaction with control table tracking
    Pond(StewardTransactionGuard<'a>),
}

impl<'a> Transaction<'a> {
    // -- Metadata --

    /// Get the transaction metadata.
    #[must_use]
    pub fn txn_meta(&self) -> &PondTxnMetadata {
        match self {
            Transaction::Pond(guard) => guard.txn_meta(),
        }
    }

    /// Get the transaction type.
    #[must_use]
    pub fn transaction_type(&self) -> TransactionType {
        match self {
            Transaction::Pond(guard) => guard.transaction_type(),
        }
    }

    /// Check if this is a write transaction.
    #[must_use]
    pub fn is_write_transaction(&self) -> bool {
        match self {
            Transaction::Pond(guard) => guard.is_write_transaction(),
        }
    }

    /// Check if this is a read transaction.
    #[must_use]
    pub fn is_read_transaction(&self) -> bool {
        match self {
            Transaction::Pond(guard) => guard.is_read_transaction(),
        }
    }

    // -- Backend-agnostic access --

    /// Get a `ProviderContext` for this transaction.
    pub fn provider_context(
        &self,
    ) -> Result<tinyfs::ProviderContext, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.provider_context(),
        }
    }

    /// Get or create a DataFusion SessionContext.
    pub async fn session_context(
        &mut self,
    ) -> Result<Arc<datafusion::execution::context::SessionContext>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.session_context().await,
        }
    }

    // -- Control table (pond-specific) --

    /// Get access to the control table.
    ///
    /// Panics if called on a non-Pond transaction (not yet applicable).
    #[must_use]
    pub fn control_table(&self) -> &ControlTable {
        match self {
            Transaction::Pond(guard) => guard.control_table(),
        }
    }

    // -- Convenience methods --

    /// Look up the factory name associated with a filesystem node.
    pub async fn get_factory_for_node(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Option<String>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.get_factory_for_node(id).await,
        }
    }

    /// Query oplog records for a filesystem node.
    pub async fn query_records(
        &self,
        id: tinyfs::FileID,
    ) -> Result<Vec<tlogfs::OplogEntry>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.query_records(id).await,
        }
    }

    /// Get the commit history from the underlying Delta table.
    pub async fn get_commit_history(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<deltalake::kernel::CommitInfo>, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.get_commit_history(limit).await,
        }
    }

    /// Get the on-disk store path for the data filesystem.
    pub fn store_path(&self) -> Result<PathBuf, tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.store_path(),
        }
    }

    /// Initialize the root directory in the oplog.
    pub async fn initialize_root_directory(&self) -> Result<(), tlogfs::TLogFSError> {
        match self {
            Transaction::Pond(guard) => guard.initialize_root_directory().await,
        }
    }

    // -- Lifecycle --

    /// Commit the transaction.
    pub async fn commit(self) -> Result<Option<()>, StewardError> {
        match self {
            Transaction::Pond(guard) => guard.commit().await,
        }
    }

    /// Abort the transaction and record it as failed.
    pub async fn abort(self, error: impl std::fmt::Display) -> StewardError {
        match self {
            Transaction::Pond(guard) => guard.abort(error).await,
        }
    }
}

impl<'a> Deref for Transaction<'a> {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        match self {
            Transaction::Pond(guard) => guard,
        }
    }
}
