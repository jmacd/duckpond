// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Host filesystem steward -- lightweight, no transactions or control table
//!
//! `HostSteward` wraps a host directory via `tinyfs::hostmount`, providing
//! a `HostTransaction` that gives `FS` + `ProviderContext` access.
//! There are no real transactions -- the host filesystem is always
//! immediately consistent.

use crate::StewardError;
use datafusion::execution::context::SessionContext;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tinyfs::{FS, PersistenceLayer, ProviderContext};
use tlogfs::PondUserMetadata;

/// Lightweight steward for the host filesystem.
///
/// No control table, no audit log, no Delta Lake transactions.
/// Each "transaction" simply creates an `FS` from the hostmount
/// persistence layer and a standalone DataFusion session.
pub struct HostSteward {
    /// Root directory for the hostmount
    root_path: PathBuf,
}

impl HostSteward {
    /// Create a new host steward rooted at the given directory.
    ///
    /// The directory must exist and be accessible.
    #[must_use]
    pub fn new(root_path: PathBuf) -> Self {
        Self { root_path }
    }

    /// Get the root path of this host steward.
    #[must_use]
    pub fn root_path(&self) -> &std::path::Path {
        &self.root_path
    }

    /// Begin a host "transaction" (creates FS + session).
    ///
    /// There is no distinction between read and write transactions
    /// on the host filesystem -- both return the same `HostTransaction`.
    /// The `meta` is carried for debugging/logging, not for audit.
    pub async fn begin(
        &mut self,
        meta: &PondUserMetadata,
    ) -> Result<HostTransaction, StewardError> {
        let persistence = tinyfs::hostmount::HostmountPersistence::new(self.root_path.clone())?;
        let persistence_arc: Arc<dyn PersistenceLayer> = Arc::new(persistence);
        let fs = FS::from_arc(persistence_arc.clone());
        let session = Arc::new(SessionContext::new());

        log::debug!(
            "[HOST] Begin transaction: root={:?}, meta={:?}",
            self.root_path,
            meta.args
        );

        Ok(HostTransaction {
            fs,
            session,
            persistence: persistence_arc,
            meta: meta.clone(),
        })
    }
}

/// A host filesystem "transaction".
///
/// Not a real transaction -- the host filesystem has no commit/rollback.
/// Provides `FS` access (via `Deref`) and `ProviderContext` /
/// `SessionContext` for DataFusion queries against host files.
pub struct HostTransaction {
    /// The hostmount-backed filesystem
    fs: FS,
    /// Standalone DataFusion session
    session: Arc<SessionContext>,
    /// Persistence layer (shared with fs, used for ProviderContext)
    persistence: Arc<dyn PersistenceLayer>,
    /// Command metadata for debugging (not persisted -- host has no control table)
    meta: PondUserMetadata,
}

impl HostTransaction {
    /// Get a `ProviderContext` for this host transaction.
    #[must_use]
    pub fn provider_context(&self) -> ProviderContext {
        ProviderContext::new(self.session.clone(), self.persistence.clone())
    }

    /// Get the DataFusion `SessionContext`.
    #[must_use]
    pub fn session_context(&self) -> Arc<SessionContext> {
        self.session.clone()
    }

    /// Look up the factory name for a filesystem node.
    ///
    /// On the host filesystem, factory names will eventually come from
    /// the URL scheme (e.g., `host+sitegen://site.yaml`). For now,
    /// returns `None` -- factory support is wired in Phase 5.
    pub async fn get_factory_for_node(
        &self,
        _id: tinyfs::FileID,
    ) -> Result<Option<String>, tinyfs::Error> {
        Ok(None)
    }

    /// Get the debug metadata for this transaction.
    #[must_use]
    pub fn meta(&self) -> &PondUserMetadata {
        &self.meta
    }
}

impl Deref for HostTransaction {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}
