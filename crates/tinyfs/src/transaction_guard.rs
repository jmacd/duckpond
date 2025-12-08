//! Transaction guard for enforcing single-transaction pattern in TinyFS
//!
//! This module provides RAII-style transaction management with runtime detection
//! of programming errors (attempting to create multiple concurrent transactions).
//!
//! The guard uses thread-local storage to track active transactions and panics
//! if a second transaction is attempted while one is active.

use crate::fs::FS;
use crate::Result;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

thread_local! {
    static ACTIVE_TRANSACTION: RefCell<bool> = RefCell::new(false);
}

/// Transaction guard for TinyFS operations
///
/// Enforces the single-transaction pattern by tracking active transactions
/// in thread-local storage. Creating a second guard while one is active
/// will panic with a detailed error message.
///
/// # Usage
///
/// ```no_run
/// # use tinyfs::{FS, TransactionGuard};
/// # async fn example(fs: FS) -> tinyfs::Result<()> {
/// let guard = TransactionGuard::begin(fs)?;
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
}

impl TransactionGuard {
    /// Begin a new transaction
    ///
    /// # Panics
    ///
    /// Panics if a transaction is already active in this thread.
    /// This is intentional - multiple concurrent transactions is a programming error.
    pub fn begin(fs: FS) -> Result<Self> {
        ACTIVE_TRANSACTION.with(|active| {
            let mut guard = active.try_borrow_mut()
                .expect("FATAL: Attempted to borrow ACTIVE_TRANSACTION while already borrowed. This indicates a serious programming error.");
            
            if *guard {
                panic!(
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
                );
            }
            
            *guard = true;
        });
        
        Ok(Self { fs })
    }

    /// Check if a transaction is currently active (for testing)
    #[cfg(test)]
    pub fn is_active() -> bool {
        ACTIVE_TRANSACTION.with(|active| *active.borrow())
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        ACTIVE_TRANSACTION.with(|active| {
            let mut guard = active.borrow_mut();
            *guard = false;
        });
    }
}

impl Deref for TransactionGuard {
    type Target = FS;
    
    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}

impl DerefMut for TransactionGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::persistence::MemoryPersistence;

    #[tokio::test]
    async fn test_single_transaction_allowed() {
        let persistence = MemoryPersistence::default();
        let fs = FS::new(persistence).await.expect("Failed to create FS");
        
        assert!(!TransactionGuard::is_active());
        
        let _guard = TransactionGuard::begin(fs).expect("Should create transaction");
        assert!(TransactionGuard::is_active());
        
        drop(_guard);
        assert!(!TransactionGuard::is_active());
    }

    #[tokio::test]
    #[should_panic(expected = "FATAL: Attempted to create a second transaction while one is already active")]
    async fn test_double_transaction_panics() {
        let persistence = MemoryPersistence::default();
        let fs1 = FS::new(persistence.clone()).await.expect("Failed to create FS");
        let fs2 = FS::new(persistence).await.expect("Failed to create FS");
        
        let _guard1 = TransactionGuard::begin(fs1).expect("First transaction should succeed");
        let _guard2 = TransactionGuard::begin(fs2).expect("Second transaction should panic");
    }

    #[tokio::test]
    async fn test_sequential_transactions_allowed() {
        let persistence = MemoryPersistence::default();
        
        {
            let fs = FS::new(persistence.clone()).await.expect("Failed to create FS");
            let _guard = TransactionGuard::begin(fs).expect("First transaction should succeed");
            assert!(TransactionGuard::is_active());
        }
        
        assert!(!TransactionGuard::is_active());
        
        {
            let fs = FS::new(persistence).await.expect("Failed to create FS");
            let _guard = TransactionGuard::begin(fs).expect("Second transaction should succeed");
            assert!(TransactionGuard::is_active());
        }
        
        assert!(!TransactionGuard::is_active());
    }
}
