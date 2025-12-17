//! Transaction guard for enforcing single-transaction pattern in TinyFS
//!
//! This module provides RAII-style transaction management with runtime detection
//! of programming errors (attempting to create multiple concurrent transactions).
//!
//! Each persistence layer embeds a `TransactionState` which tracks active transactions.
//! The guard ensures only one active transaction per persistence layer at a time.

use crate::Result;
use crate::fs::FS;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// Internal transaction state shared across persistence layers
///
/// This is embedded as `Mutex<TxnState>` in each persistence layer.
#[derive(Debug, Default)]
pub(crate) struct TxnState {
    /// Whether this transaction is active (for debug/testing)
    pub(crate) active: bool,
    /// Optional transaction sequence number (used by tlogfs)
    pub(crate) sequence: Option<i64>,
}

/// Wrapper around Mutex<TxnState> for embedding in persistence layers
///
/// Persistence layers should own this (via Arc) and pass it to begin()
#[derive(Debug, Clone)]
pub struct TransactionState {
    pub(crate) inner: Arc<Mutex<TxnState>>,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TxnState::default())),
        }
    }
}

impl TransactionState {
    /// Create a new transaction state
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a transaction
    ///
    /// Returns an error if a transaction is already active.
    /// This enforces the single-transaction pattern.
    pub fn begin(&self, fs: FS, sequence: Option<i64>) -> Result<TransactionGuard> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| crate::Error::Other("Transaction mutex poisoned".into()))?;

        if state.active {
            return Err(crate::Error::Other(
                "FATAL: Attempted to create a second transaction while one is already active.\n\
                \n\
                This is a programming error. You should:\n\
                1. Pass the existing transaction guard/context down the call stack, OR\n\
                2. Commit/drop the existing transaction before starting a new one.\n\
                \n\
                Common causes:\n\
                - Helper functions calling begin() instead of taking a guard parameter\n\
                - Forgetting to pass transaction context through async functions\n\
                - Opening a transaction inside a factory (factories receive context)\n\
                \n\
                See docs/duckpond-system-patterns.md for correct transaction patterns."
                    .into(),
            ));
        }

        state.active = true;
        state.sequence = sequence;
        drop(state); // Release lock immediately

        Ok(TransactionGuard {
            fs,
            state_ref: self.inner.clone(),
            sequence,
        })
    }

    /// Mark transaction as active (internal use by persistence layers)
    ///
    /// Returns an error if a transaction is already active.
    pub fn mark_active(&self, sequence: Option<i64>) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| crate::Error::Other("Transaction mutex poisoned".into()))?;

        if state.active {
            return Err(crate::Error::Other(
                "FATAL: Attempted to mark transaction active while one is already active.".into(),
            ));
        }

        state.active = true;
        state.sequence = sequence;
        Ok(())
    }

    /// Clear transaction state (internal use by persistence layers)
    pub fn clear(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.active = false;
            state.sequence = None;
        }
    }

    /// Check if a transaction is currently active
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.lock().map(|state| state.active).unwrap_or(false)
    }
}

/// Transaction guard for TinyFS operations
///
/// Enforces the single-transaction pattern. Only one guard can be active
/// at a time per persistence layer.
///
/// # Usage
///
/// ```no_run
/// # use tinyfs::{FS, TransactionState};
/// # async fn example(fs: FS, txn_state: &TransactionState) -> tinyfs::Result<()> {
/// let guard = txn_state.begin(fs, None)?;
///
/// // Use filesystem through the guard
/// let root = guard.root().await?;
/// let file = root.create_file_at("/data.txt").await?;
///
/// // Guard automatically releases on drop
/// # Ok(())
/// # }
/// ```
pub struct TransactionGuard {
    fs: FS,
    state_ref: Arc<Mutex<TxnState>>,
    sequence: Option<i64>,
}

impl TransactionGuard {
    /// Get the transaction sequence number (if set)
    #[must_use]
    pub fn sequence(&self) -> Option<i64> {
        self.sequence
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state_ref.lock() {
            state.active = false;
            state.sequence = None;
        }
    }
}

impl Deref for TransactionGuard {
    type Target = FS;

    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::persistence::MemoryPersistence;

    #[tokio::test]
    async fn test_single_transaction_allowed() {
        let persistence = MemoryPersistence::default();
        let txn_state = TransactionState::new();
        let fs = FS::new(persistence).await.expect("Failed to create FS");

        assert!(!txn_state.is_active());

        let _guard = txn_state
            .begin(fs, None)
            .expect("Should create transaction");
        assert!(txn_state.is_active());

        drop(_guard);
        assert!(!txn_state.is_active());
    }

    #[tokio::test]
    #[should_panic(
        expected = "FATAL: Attempted to create a second transaction while one is already active"
    )]
    async fn test_double_transaction_panics() {
        let persistence = MemoryPersistence::default();
        let txn_state = TransactionState::new();
        let fs1 = FS::new(persistence.clone())
            .await
            .expect("Failed to create FS");
        let fs2 = FS::new(persistence).await.expect("Failed to create FS");

        let _guard1 = txn_state
            .begin(fs1, None)
            .expect("First transaction should succeed");
        // This should panic because txn_state is already borrowed mutably
        let _guard2 = txn_state
            .begin(fs2, None)
            .expect("Second transaction should panic");
    }

    #[tokio::test]
    async fn test_sequential_transactions_allowed() {
        let persistence = MemoryPersistence::default();
        let txn_state = TransactionState::new();

        {
            let fs = FS::new(persistence.clone())
                .await
                .expect("Failed to create FS");
            let _guard = txn_state
                .begin(fs, None)
                .expect("First transaction should succeed");
            assert!(txn_state.is_active());
        }

        assert!(!txn_state.is_active());

        {
            let fs = FS::new(persistence).await.expect("Failed to create FS");
            let _guard = txn_state
                .begin(fs, None)
                .expect("Second transaction should succeed");
            assert!(txn_state.is_active());
        }

        assert!(!txn_state.is_active());
    }

    #[tokio::test]
    async fn test_transaction_sequence_tracking() {
        let persistence = MemoryPersistence::default();
        let txn_state = TransactionState::new();
        let fs = FS::new(persistence).await.expect("Failed to create FS");

        let guard = txn_state
            .begin(fs, Some(42))
            .expect("Should create transaction");
        assert_eq!(guard.sequence(), Some(42));
    }
}
