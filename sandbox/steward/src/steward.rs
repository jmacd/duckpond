// SPDX-License-Identifier: Apache-2.0

//! The [`Steward`]: transaction lifecycle + control table + per-commit
//! partition checksums.
//!
//! Layout on disk:
//!
//! ```text
//! <pond>/
//!   data/        <- managed by sandbox-store
//!   control/     <- managed by control_table.rs
//! ```
//!
//! Construction:
//!
//! - [`Steward::create`] makes both subdirectories fresh.
//! - [`Steward::open`] opens an existing pond.
//!
//! Mutation goes through [`Steward::begin_write`], which returns a
//! [`WriteGuard`].  The guard accepts puts and deletes, then is
//! consumed by [`WriteGuard::commit`] (success path) or
//! [`WriteGuard::abort`] (failure path).  Both paths write a complete
//! pair of lifecycle records (Begin -> DataCommitted | Failed |
//! Completed) so a crash mid-commit is observable.
//!
//! Reads go through [`Steward::begin_read`] which returns a [`ReadGuard`].
//! Read guards do NOT write any control-table records (per the design;
//! see the design doc for rationale).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use sandbox_store::Store;
use sandbox_store::checksum::{Checksum, PartitionChecksum};

use crate::control_table::{
    ChecksumValue, CommitKind, ControlRecord, ControlTable, DataCommittedMetadata,
    PartitionChecksums, RecordKind, new_txn_id,
};
use crate::error::Result;
use crate::guard::{ReadGuard, WriteGuard, WriteGuardInner};

/// Result of a successful write commit.
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    /// The transaction's `txn_seq`.
    pub txn_seq: i64,
    /// Whether anything was actually written.
    pub had_data: bool,
    /// Per-partition checksums recorded for this commit.  Includes
    /// partitions touched by this commit AND the most recent recorded
    /// checksum for every partition that existed previously, so this
    /// map is always a complete snapshot at `txn_seq`.
    pub partition_checksums: PartitionChecksums,
    /// Commit kind written to the control table.
    pub commit_kind: CommitKind,
}

/// The pond steward.  Owns the data store and the control table, plus
/// the in-memory `last_write_seq` allocator.
pub struct Steward {
    pond_path: PathBuf,
    /// Wrapped in `Arc<Mutex>` so `WriteGuard` can borrow it mutably
    /// while the steward holds a reference for read-after-commit.
    /// Not actually shared across threads; this is an internal
    /// convention to satisfy borrow checking.
    store: Store,
    control: ControlTable,
    last_write_seq: i64,
    /// Strategy used when computing partition checksums on commit.
    /// Boxed so different stores can use different strategies, but the
    /// strategy is fixed for the lifetime of the steward.
    checksum_strategy: Arc<dyn PartitionChecksum>,
}

/// Pluggable construction options.
pub struct StewardOptions {
    /// Strategy for per-partition checksums.
    pub checksum_strategy: Arc<dyn PartitionChecksum>,
}

impl Default for StewardOptions {
    fn default() -> Self {
        Self {
            checksum_strategy: Arc::new(sandbox_store::checksum::Merkle::new()),
        }
    }
}

impl Steward {
    /// Create a new pond at `pond_path` with default options.
    pub async fn create(pond_path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_options(pond_path, StewardOptions::default()).await
    }

    /// Create a new pond at `pond_path` with custom options.
    pub async fn create_with_options(
        pond_path: impl AsRef<Path>,
        opts: StewardOptions,
    ) -> Result<Self> {
        let pond_path = pond_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&pond_path)?;
        let data_path = pond_path.join("data");
        let control_path = pond_path.join("control");
        let store = Store::create(&data_path).await?;
        let control = ControlTable::create(&control_path).await?;
        Ok(Self {
            pond_path,
            store,
            control,
            last_write_seq: 0,
            checksum_strategy: opts.checksum_strategy,
        })
    }

    /// Open an existing pond.  Loads `last_write_seq` from the control
    /// table.
    pub async fn open(pond_path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(pond_path, StewardOptions::default()).await
    }

    /// Open an existing pond with custom options.
    pub async fn open_with_options(
        pond_path: impl AsRef<Path>,
        opts: StewardOptions,
    ) -> Result<Self> {
        let pond_path = pond_path.as_ref().to_path_buf();
        let data_path = pond_path.join("data");
        let control_path = pond_path.join("control");
        let store = Store::open(&data_path).await?;
        let control = ControlTable::open(&control_path).await?;
        let last_write_seq = control.last_txn_seq().await?;
        Ok(Self {
            pond_path,
            store,
            control,
            last_write_seq,
            checksum_strategy: opts.checksum_strategy,
        })
    }

    /// Path the pond was created/opened at.
    pub fn path(&self) -> &Path {
        &self.pond_path
    }

    /// Most recent allocated `txn_seq` (across all kinds, not just
    /// committed transactions).
    pub fn last_write_seq(&self) -> i64 {
        self.last_write_seq
    }

    /// Most recent `txn_seq` for which a `DataCommitted` record exists.
    pub async fn last_committed_seq(&self) -> Result<i64> {
        self.control.last_committed_seq().await
    }

    /// All control-table records, oldest first.  Optional limit returns
    /// only the most recent N.
    pub async fn log(&self, limit: Option<usize>) -> Result<Vec<ControlRecord>> {
        let mut all = self.control.all_records().await?;
        if let Some(n) = limit
            && all.len() > n
        {
            let start = all.len() - n;
            all = all.split_off(start);
        }
        Ok(all)
    }

    /// Resolve the partition_checksums map at `txn_seq`.
    pub async fn partition_checksums_at(&self, txn_seq: i64) -> Result<Option<PartitionChecksums>> {
        self.control.partition_checksums_at(txn_seq).await
    }

    /// Set a configuration key.
    pub async fn config_set(&mut self, key: &str, value: &str) -> Result<()> {
        self.control.config_set(key, value).await
    }

    /// Read a configuration key.  `None` if never set.
    pub async fn config_get(&self, key: &str) -> Result<Option<String>> {
        self.control.config_get(key).await
    }

    /// All current settings (latest-write-wins per key).
    pub async fn config_list(&self) -> Result<HashMap<String, String>> {
        self.control.config_list().await
    }

    /// Incomplete transactions (Begin without terminal record).
    pub async fn incomplete_transactions(&self) -> Result<Vec<ControlRecord>> {
        self.control.incomplete_transactions().await
    }

    /// Begin a write transaction.  Allocates the next `txn_seq`,
    /// records a Begin row, and returns a guard.
    pub async fn begin_write(&mut self) -> Result<WriteGuard<'_>> {
        let txn_seq = self.last_write_seq + 1;
        let txn_id = new_txn_id();
        let now = Utc::now().timestamp_micros();
        self.control
            .write_record(ControlRecord {
                record_kind: RecordKind::Begin,
                txn_seq,
                txn_id: txn_id.clone(),
                commit_kind: None,
                parent_seq: None,
                duration_ms: None,
                ts_micros: now,
                metadata_json: "{}".to_string(),
            })
            .await?;
        self.last_write_seq = txn_seq;
        let parent_seq = self.control.last_committed_seq().await?;
        let parent_seq = if parent_seq == 0 {
            None
        } else {
            Some(parent_seq)
        };

        let inner = WriteGuardInner {
            txn_seq,
            txn_id,
            started_micros: now,
            parent_seq,
            ops: Vec::new(),
            consumed: false,
        };
        Ok(WriteGuard::new(self, inner))
    }

    /// Begin a read transaction.  No control-table writes.
    pub async fn begin_read(&self) -> Result<ReadGuard<'_>> {
        Ok(ReadGuard::new(&self.store))
    }

    /// Internal: commit a write guard.  Caller is the guard itself.
    pub(crate) async fn finish_commit(&mut self, inner: WriteGuardInner) -> Result<CommitOutcome> {
        let WriteGuardInner {
            txn_seq,
            txn_id,
            started_micros,
            parent_seq,
            ops,
            consumed: _,
        } = inner;

        let had_data = !ops.is_empty();

        if had_data {
            // Apply data writes.
            self.store
                .apply_batch(txn_seq, started_micros, ops.clone())
                .await?;
        }

        // Compute partition checksums for ALL partitions known to the
        // store after this commit.  We carry forward unaffected
        // partitions' checksums (which are the same as they were at
        // parent_seq) so partition_checksums_at(txn_seq) is a complete
        // snapshot.
        let all_partitions = self.store.partitions().await?;
        let mut checksums: PartitionChecksums = HashMap::new();
        for partition in &all_partitions {
            let cs = self
                .store
                .compute_partition_checksum(partition, &*self.checksum_strategy)
                .await?;
            checksums.insert(partition.clone(), cs);
        }

        let metadata = DataCommittedMetadata {
            partition_checksums: checksums
                .iter()
                .map(|(k, v)| (k.clone(), ChecksumValue::from(v)))
                .collect(),
        };
        let metadata_json = serde_json::to_string(&metadata)?;

        let now = Utc::now().timestamp_micros();
        let duration_ms = ((now - started_micros) / 1000).max(0);

        if had_data {
            self.control
                .write_record(ControlRecord {
                    record_kind: RecordKind::DataCommitted,
                    txn_seq,
                    txn_id,
                    commit_kind: Some(CommitKind::Write),
                    parent_seq,
                    duration_ms: Some(duration_ms),
                    ts_micros: now,
                    metadata_json,
                })
                .await?;
        } else {
            // Nothing was written: record Completed instead of
            // DataCommitted.  Idempotent / no-op writes still get a
            // terminal lifecycle record so the txn isn't "incomplete".
            self.control
                .write_record(ControlRecord {
                    record_kind: RecordKind::Completed,
                    txn_seq,
                    txn_id,
                    commit_kind: None,
                    parent_seq,
                    duration_ms: Some(duration_ms),
                    ts_micros: now,
                    metadata_json: "{}".to_string(),
                })
                .await?;
        }

        Ok(CommitOutcome {
            txn_seq,
            had_data,
            partition_checksums: checksums,
            commit_kind: CommitKind::Write,
        })
    }

    /// Internal: abort a write guard with an error message.
    pub(crate) async fn finish_abort(
        &mut self,
        inner: WriteGuardInner,
        reason: String,
    ) -> Result<()> {
        let now = Utc::now().timestamp_micros();
        let duration_ms = ((now - inner.started_micros) / 1000).max(0);
        let mut meta = HashMap::new();
        meta.insert("reason".to_string(), reason);
        self.control
            .write_record(ControlRecord {
                record_kind: RecordKind::Failed,
                txn_seq: inner.txn_seq,
                txn_id: inner.txn_id,
                commit_kind: None,
                parent_seq: inner.parent_seq,
                duration_ms: Some(duration_ms),
                ts_micros: now,
                metadata_json: serde_json::to_string(&meta)?,
            })
            .await?;
        Ok(())
    }

    /// Internal: provide read-only access to the underlying store for
    /// post-commit verification.  Used by the verify-cmd todo.
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// Internal: provide read-only access to the checksum strategy.
    pub(crate) fn checksum_strategy(&self) -> &dyn PartitionChecksum {
        &*self.checksum_strategy
    }

    /// Best-effort error context: return a reference to the underlying
    /// path, used in error messages from external crates.
    #[allow(dead_code)]
    pub(crate) fn pond_path(&self) -> &Path {
        &self.pond_path
    }
}

/// Re-compute the per-partition checksums at the current state and
/// compare against the most recent `DataCommitted` record.
pub async fn verify_local(steward: &Steward) -> Result<VerifyReport> {
    let last_seq = steward.last_committed_seq().await?;
    if last_seq == 0 {
        return Ok(VerifyReport {
            ok: true,
            mismatches: Vec::new(),
            recomputed_seq: 0,
        });
    }
    let recorded = steward
        .partition_checksums_at(last_seq)
        .await?
        .unwrap_or_default();
    let partitions = steward.store().partitions().await?;
    let mut mismatches = Vec::new();
    let strategy = steward.checksum_strategy();
    for partition in partitions {
        let live: Checksum = steward
            .store()
            .compute_partition_checksum(&partition, strategy)
            .await?;
        match recorded.get(&partition) {
            Some(rec) if rec == &live => {}
            Some(rec) => mismatches.push(VerifyMismatch {
                partition: partition.clone(),
                recorded: Some(rec.clone()),
                live,
            }),
            None => mismatches.push(VerifyMismatch {
                partition: partition.clone(),
                recorded: None,
                live,
            }),
        }
    }
    // Also catch partitions that were in the recorded snapshot but no
    // longer have any rows (shouldn't happen with our model, but
    // defensible to detect).
    for (partition, rec) in &recorded {
        if !steward
            .store()
            .partitions()
            .await?
            .iter()
            .any(|p| p == partition)
        {
            mismatches.push(VerifyMismatch {
                partition: partition.clone(),
                recorded: Some(rec.clone()),
                live: Checksum::new(steward.checksum_strategy().kind(), Vec::new()),
            });
        }
    }
    Ok(VerifyReport {
        ok: mismatches.is_empty(),
        mismatches,
        recomputed_seq: last_seq,
    })
}

/// Result of a [`verify_local`] run.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    /// `true` iff every partition's recomputed checksum equals the
    /// recorded checksum at the latest committed seq.
    pub ok: bool,
    /// Partitions whose recomputed and recorded checksums differ.
    pub mismatches: Vec<VerifyMismatch>,
    /// The committed seq the comparison was made against.
    pub recomputed_seq: i64,
}

/// One partition's verification mismatch.
#[derive(Debug, Clone)]
pub struct VerifyMismatch {
    /// Partition key.
    pub partition: String,
    /// Checksum recorded by the commit (if any).
    pub recorded: Option<Checksum>,
    /// Checksum recomputed from the current data.
    pub live: Checksum,
}
