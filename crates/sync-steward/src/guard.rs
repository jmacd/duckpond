// SPDX-License-Identifier: Apache-2.0

//! Transaction guards.

use sync_store::{Op, Store};

use crate::error::{Result, StewardError};
use crate::steward::{CommitOutcome, Steward};

/// Internal state of a [`WriteGuard`], moved out of the guard on commit
/// or abort.
pub(crate) struct WriteGuardInner {
    pub(crate) txn_seq: i64,
    pub(crate) txn_id: String,
    pub(crate) started_micros: i64,
    pub(crate) parent_seq: Option<i64>,
    pub(crate) ops: Vec<Op>,
    pub(crate) consumed: bool,
}

/// A handle to a single in-flight write transaction.
///
/// Operations buffer in memory until [`WriteGuard::commit`] or
/// [`WriteGuard::abort`] is called.  Dropping a guard without calling
/// either method is a usage error: the guard records nothing and the
/// transaction will look "incomplete" in the control table.  Tests
/// catch this; production code should always commit or abort.
pub struct WriteGuard<'a> {
    steward: &'a mut Steward,
    inner: Option<WriteGuardInner>,
}

impl<'a> WriteGuard<'a> {
    pub(crate) fn new(steward: &'a mut Steward, inner: WriteGuardInner) -> Self {
        Self {
            steward,
            inner: Some(inner),
        }
    }

    /// The transaction's allocated `txn_seq`.
    pub fn txn_seq(&self) -> i64 {
        self.inner.as_ref().expect("guard already consumed").txn_seq
    }

    /// Buffer a Put.
    pub fn put(&mut self, partition: &str, key: &str, value: Vec<u8>) -> Result<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| StewardError::ApiMisuse("guard already consumed".into()))?;
        inner.ops.push(Op::Put {
            partition: partition.to_string(),
            key: key.to_string(),
            value,
        });
        Ok(())
    }

    /// Buffer a Delete.
    pub fn delete(&mut self, partition: &str, key: &str) -> Result<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| StewardError::ApiMisuse("guard already consumed".into()))?;
        inner.ops.push(Op::Delete {
            partition: partition.to_string(),
            key: key.to_string(),
        });
        Ok(())
    }

    /// Commit the buffered operations.  Records a `DataCommitted` (or
    /// `Completed` if no ops) row in the control table.
    pub async fn commit(mut self) -> Result<CommitOutcome> {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| StewardError::ApiMisuse("guard already consumed".into()))?;
        inner.consumed = true;
        self.steward.finish_commit(inner).await
    }

    /// Abort the transaction.  Records a `Failed` row in the control table.
    pub async fn abort(mut self, reason: impl Into<String>) -> Result<()> {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| StewardError::ApiMisuse("guard already consumed".into()))?;
        inner.consumed = true;
        self.steward.finish_abort(inner, reason.into()).await
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take()
            && !inner.consumed
        {
            // The user dropped the guard without calling commit or
            // abort.  We can't run async cleanup from Drop, so log
            // loudly and leave the Begin record orphaned.  Tests
            // for this scenario verify that incomplete_transactions
            // detects the orphan.
            log::warn!(
                "WriteGuard for txn_seq={} dropped without commit() or abort(); \
                 transaction will appear as incomplete in the control table",
                inner.txn_seq
            );
        }
    }
}

/// A handle to a read-only view of the store.  No control-table writes.
pub struct ReadGuard<'a> {
    store: &'a Store,
    local_pond_id: uuid::Uuid,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(store: &'a Store, local_pond_id: uuid::Uuid) -> Self {
        Self {
            store,
            local_pond_id,
        }
    }

    /// Read the current value of `(partition, key)` from the local pond.
    pub async fn get(&self, partition: &str, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get(self.local_pond_id, partition, key).await?)
    }

    /// List all live items in `partition` for the local pond.
    pub async fn list(&self, partition: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self.store.list(self.local_pond_id, partition).await?)
    }

    /// All known partitions for the local pond.
    pub async fn partitions(&self) -> Result<Vec<String>> {
        Ok(self.store.partitions(self.local_pond_id).await?)
    }
}
