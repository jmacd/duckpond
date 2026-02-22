// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Provider context and factory context for dynamic node creation
//!
//! This module defines the abstraction layer between factories and persistence implementations.
//! ProviderContext holds a tinyfs Persistence layer for transaction management.

use crate::{FileID, PersistenceLayer};
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

/// Result type for tinyfs context operations
pub type Result<T> = std::result::Result<T, crate::Error>;

/// Provider context - holds tinyfs Persistence for transaction management
///
/// This struct provides factories with:
/// - Direct access to DataFusion SessionContext for SQL execution
/// - Table provider caching for performance optimization
/// - tinyfs Persistence layer for creating transaction guards
///
/// All fields are concrete - no trait objects to downcast.
#[derive(Clone)]
pub struct ProviderContext {
    /// DataFusion session for SQL execution (direct access, no async needed)
    pub datafusion_session: Arc<SessionContext>,

    /// Table provider cache for performance
    pub table_provider_cache: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, Arc<dyn datafusion::catalog::TableProvider>>,
        >,
    >,

    /// TinyFS persistence layer for transaction management
    pub persistence: Arc<dyn PersistenceLayer>,
}

impl ProviderContext {
    /// Create a new provider context from concrete values
    pub fn new(
        datafusion_session: Arc<SessionContext>,
        persistence: Arc<dyn PersistenceLayer>,
    ) -> Self {
        Self {
            datafusion_session,
            table_provider_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            persistence,
        }
    }

    /// Get cached TableProvider by cache key
    #[must_use]
    pub fn get_table_provider_cache(
        &self,
        key: &str,
    ) -> Option<Arc<dyn datafusion::catalog::TableProvider>> {
        self.table_provider_cache.lock().ok()?.get(key).cloned()
    }

    /// Set cached TableProvider
    pub fn set_table_provider_cache(
        &self,
        key: String,
        provider: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> Result<()> {
        _ = self
            .table_provider_cache
            .lock()
            .map_err(|e| crate::Error::Other(format!("Mutex poisoned: {}", e)))?
            .insert(key, provider);
        Ok(())
    }

    /// Create a filesystem from the persistence layer
    #[must_use]
    pub fn filesystem(&self) -> crate::FS {
        crate::FS::from_arc(self.persistence.clone())
    }

    /// Begin a transaction with the transaction guard pattern
    ///
    /// Returns a TransactionGuard that enforces the single-transaction rule.
    /// The guard provides access to the filesystem and automatically cleans up on drop.
    pub fn begin_transaction(&self) -> crate::Result<crate::TransactionGuard> {
        let fs = self.filesystem();
        let txn_state = self.persistence.transaction_state();
        txn_state.begin(fs, None)
    }

    /// Create a minimal test context with sensible defaults
    ///
    /// This is a convenience constructor for testing that creates a ProviderContext with:
    /// - Fresh DataFusion SessionContext (default configuration)
    /// - Empty template variables HashMap
    /// - Provided persistence layer
    ///
    /// This enables testing provider code without requiring tlogfs State.
    ///
    /// # Example
    /// ```ignore
    /// use tinyfs::{ProviderContext, MemoryPersistence};
    /// use std::sync::Arc;
    ///
    /// let persistence = Arc::new(MemoryPersistence::default());
    /// let context = ProviderContext::new_for_testing(persistence);
    /// ```
    pub fn new_for_testing(persistence: Arc<dyn PersistenceLayer>) -> Self {
        // Create default DataFusion session
        let datafusion_session = Arc::new(SessionContext::new());

        Self::new(datafusion_session, persistence)
    }
}

/// Factory context for creating dynamic nodes
///
/// This struct provides the complete context needed by factories:
/// - Access to persistence layer via ProviderContext
/// - FileID providing node and partition identity
/// - Optional pond metadata (pond_id, birth_timestamp, etc.)
/// - Current transaction sequence number
#[derive(Clone)]
pub struct FactoryContext {
    /// Access to persistence layer operations
    pub context: ProviderContext,
    /// FileID for context-aware factories (provides both node and partition info)
    pub file_id: FileID,
    /// Pond identity metadata (pond_id, birth_timestamp, etc.)
    /// Provided by Steward when creating factory contexts
    pub pond_metadata: Option<PondMetadata>,
    /// Current transaction sequence number from persistence layer
    /// Provided by Steward for backup/replication operations
    pub txn_seq: i64,
}

impl FactoryContext {
    /// Create a new factory context with the given provider context and file_id
    #[must_use]
    pub fn new(context: ProviderContext, file_id: FileID) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: None,
            txn_seq: 0,
        }
    }

    /// Create a factory context with pond metadata
    #[must_use]
    pub fn with_metadata(
        context: ProviderContext,
        file_id: FileID,
        pond_metadata: PondMetadata,
    ) -> Self {
        Self {
            context,
            file_id,
            pond_metadata: Some(pond_metadata),
            txn_seq: 0,
        }
    }

    /// Set the transaction sequence number
    #[must_use]
    pub fn with_txn_seq(mut self, txn_seq: i64) -> Self {
        self.txn_seq = txn_seq;
        self
    }
}

/// Pond identity metadata - immutable information about the pond's origin
///
/// This metadata is created once when the pond is initialized and preserved across replicas.
/// It provides traceability and identity for distributed pond systems.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PondMetadata {
    /// Unique identifier for this pond (UUID v7)
    pub pond_id: uuid7::Uuid,
    /// Timestamp when this pond was originally created (microseconds since epoch)
    pub birth_timestamp: i64,
    /// Hostname where the pond was originally created
    pub birth_hostname: String,
    /// Username who originally created the pond
    pub birth_username: String,
}

impl Default for PondMetadata {
    /// Create new pond metadata for a freshly initialized pond
    fn default() -> Self {
        let pond_id = uuid7::uuid7();
        let birth_timestamp = chrono::Utc::now().timestamp_micros();

        // Note: std::net::hostname() is unstable, using placeholder
        let birth_hostname = "unknown".into();

        let birth_username = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or("unknown".into());

        Self {
            pond_id,
            birth_timestamp,
            birth_hostname,
            birth_username,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ProviderContext;
    use crate::memory::persistence::MemoryPersistence;
    use datafusion::execution::context::SessionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_provider_context_transaction_guard() {
        // Create a provider context with memory persistence
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));

        // Begin a transaction using the guard pattern
        let guard = context
            .begin_transaction()
            .expect("Should create transaction");

        // Access filesystem through the guard
        let root = guard.root().await.expect("Should get root");

        // Verify root exists
        assert_eq!(root.node_path().path, PathBuf::from("/"));

        // Guard automatically cleans up on drop
        drop(guard);

        // Can create another transaction after the first one is dropped
        let _guard2 = context
            .begin_transaction()
            .expect("Should create second transaction");
    }

    #[tokio::test]
    async fn test_provider_context_filesystem() {
        // Create a provider context
        let persistence = MemoryPersistence::default();
        let session = Arc::new(SessionContext::new());
        let context = ProviderContext::new(session, Arc::new(persistence));

        // Get filesystem directly (no guard - for cases where guard isn't needed)
        let fs = context.filesystem();
        let root = fs.root().await.expect("Should get root");

        assert_eq!(root.node_path().path, PathBuf::from("/"));
    }
}
