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
use uuid::Uuid;

use crate::control_table::{
    ChecksumValue, CommitKind, ControlRecord, ControlTable, DataCommittedMetadata,
    PartitionChecksums, RecordKind, new_txn_id,
};
use crate::error::{Result, StewardError};
use crate::guard::{ReadGuard, WriteGuard, WriteGuardInner};

/// Setting key under which the pond's `store_id` is persisted in the
/// control table.  Set once at `Steward::create`; immutable thereafter.
const STORE_ID_KEY: &str = "store_id";

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
    /// Pond family identity.  Set at `create`, loaded at `open`.
    /// Immutable for the lifetime of the steward.
    store_id: Uuid,
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
    /// `store_id` to assign to a new pond.  `None` (the default) mints
    /// a fresh UUIDv4 at `create`.  `Some(id)` is used to bootstrap a
    /// replica with the same identity as the source (the
    /// restart-from-compact path; not yet exercised by `open`, which
    /// always reads from the persisted setting).
    pub store_id: Option<Uuid>,
}

impl Default for StewardOptions {
    fn default() -> Self {
        Self {
            checksum_strategy: Arc::new(sandbox_store::checksum::Merkle::new()),
            store_id: None,
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
        let mut control = ControlTable::create(&control_path).await?;
        let store_id = opts.store_id.unwrap_or_else(Uuid::new_v4);
        control
            .config_set(STORE_ID_KEY, &store_id.to_string())
            .await?;
        Ok(Self {
            pond_path,
            store_id,
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
    ///
    /// Errors with [`StewardError::LegacyPond`] if the pond's control
    /// table does not contain a `store_id` setting.  The sandbox does
    /// NOT silently backfill an identity for legacy ponds; an operator
    /// must migrate or recreate.
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
        let store_id_str = control.config_get(STORE_ID_KEY).await?.ok_or_else(|| {
            StewardError::LegacyPond(format!(
                "{}: no `{}` setting in control table",
                pond_path.display(),
                STORE_ID_KEY,
            ))
        })?;
        let store_id = Uuid::parse_str(&store_id_str).map_err(|e| {
            StewardError::Invariant(format!(
                "control table contained malformed `{}` setting `{}`: {}",
                STORE_ID_KEY, store_id_str, e,
            ))
        })?;
        Ok(Self {
            pond_path,
            store_id,
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

    /// The pond family's `store_id`.  Minted at `Steward::create` and
    /// preserved across all clones (replicas restored from the same
    /// remote bundle history share this identity).
    pub fn store_id(&self) -> Uuid {
        self.store_id
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

    /// Resolve the data store's Delta Lake version at `txn_seq`.
    /// Returns `None` if no `DataCommitted` record exists for that seq;
    /// returns `Some(0)` for legacy records written before the
    /// `data_delta_version` field existed.
    pub async fn data_delta_version_at(&self, txn_seq: i64) -> Result<Option<i64>> {
        self.control.data_delta_version_at(txn_seq).await
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
        let checksums = self.snapshot_all_partition_checksums().await?;

        let metadata = DataCommittedMetadata {
            partition_checksums: checksums
                .iter()
                .map(|(k, v)| (k.clone(), ChecksumValue::from(v)))
                .collect(),
            data_delta_version: self.store.delta_version(),
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

    /// Run a Delta-level compaction over the store and record the
    /// lifecycle in the control table.
    ///
    /// Allocates a `txn_seq` and writes a `Begin` record before the
    /// compaction, mirroring `begin_write`.  Then:
    ///
    /// - If `Store::compact` returns an error, writes a `Failed` record
    ///   with the error reason and returns the error.
    /// - If compaction was a no-op (zero files added/removed -- empty
    ///   table, nonexistent partition filter, or already-optimal layout),
    ///   writes a `Completed` record.  `last_committed_seq` does NOT
    ///   advance, because no new Delta commit was produced.
    /// - Otherwise snapshots per-partition checksums before and after
    ///   the optimize, asserts they are equal (compaction must be
    ///   logically transparent), then writes a `DataCommitted` record
    ///   with `commit_kind = Compact`.
    ///
    /// `filter`, when set, restricts compaction to a single
    /// `partition_key` value.  Other partitions' data files are
    /// untouched.
    ///
    /// The returned [`CommitOutcome`] always carries
    /// `commit_kind = Compact` regardless of whether the underlying
    /// transaction recorded `DataCommitted` or `Completed`; callers
    /// distinguish via `had_data` (true => real Delta commit).
    pub async fn compact(&mut self, filter: Option<&str>) -> Result<CommitOutcome> {
        // 1. Allocate seq and write Begin (mirror begin_write prologue).
        let txn_seq = self.last_write_seq + 1;
        let txn_id = new_txn_id();
        let started = Utc::now().timestamp_micros();
        self.control
            .write_record(ControlRecord {
                record_kind: RecordKind::Begin,
                txn_seq,
                txn_id: txn_id.clone(),
                commit_kind: None,
                parent_seq: None,
                duration_ms: None,
                ts_micros: started,
                metadata_json: "{}".to_string(),
            })
            .await?;
        self.last_write_seq = txn_seq;
        let parent_seq = match self.control.last_committed_seq().await? {
            0 => None,
            n => Some(n),
        };

        // 2. Snapshot pre-compaction checksums for every known partition.
        let pre = self.snapshot_all_partition_checksums().await?;

        // 3. Run optimize.  On error, record Failed and return.
        let metrics = match self.store.compact(filter).await {
            Ok(m) => m,
            Err(e) => {
                let reason = format!("compact failed: {}", e);
                let now = Utc::now().timestamp_micros();
                let duration_ms = ((now - started) / 1000).max(0);
                let mut meta = HashMap::new();
                meta.insert("reason".to_string(), reason);
                let _ = self
                    .control
                    .write_record(ControlRecord {
                        record_kind: RecordKind::Failed,
                        txn_seq,
                        txn_id,
                        commit_kind: None,
                        parent_seq,
                        duration_ms: Some(duration_ms),
                        ts_micros: now,
                        metadata_json: serde_json::to_string(&meta)?,
                    })
                    .await;
                return Err(e.into());
            }
        };

        // 4. No-op compact: write Completed, do NOT advance last_committed_seq.
        if metrics.is_noop() {
            let now = Utc::now().timestamp_micros();
            let duration_ms = ((now - started) / 1000).max(0);
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
            return Ok(CommitOutcome {
                txn_seq,
                had_data: false,
                partition_checksums: pre,
                commit_kind: CommitKind::Compact,
            });
        }

        // 5. Real compaction: snapshot post and assert invariance.
        let post = self.snapshot_all_partition_checksums().await?;
        assert_compaction_invariant(&pre, &post)?;

        // 6. Record DataCommitted with kind=Compact.
        let metadata = DataCommittedMetadata {
            partition_checksums: post
                .iter()
                .map(|(k, v)| (k.clone(), ChecksumValue::from(v)))
                .collect(),
            data_delta_version: self.store.delta_version(),
        };
        let metadata_json = serde_json::to_string(&metadata)?;
        let now = Utc::now().timestamp_micros();
        let duration_ms = ((now - started) / 1000).max(0);
        self.control
            .write_record(ControlRecord {
                record_kind: RecordKind::DataCommitted,
                txn_seq,
                txn_id,
                commit_kind: Some(CommitKind::Compact),
                parent_seq,
                duration_ms: Some(duration_ms),
                ts_micros: now,
                metadata_json,
            })
            .await?;

        Ok(CommitOutcome {
            txn_seq,
            had_data: true,
            partition_checksums: post,
            commit_kind: CommitKind::Compact,
        })
    }

    /// Helper: snapshot every known partition's content checksum
    /// using the steward's strategy.
    async fn snapshot_all_partition_checksums(&self) -> Result<PartitionChecksums> {
        let partitions = self.store.partitions().await?;
        let mut out: PartitionChecksums = HashMap::new();
        for p in partitions {
            let cs = self
                .store
                .compute_partition_checksum(&p, &*self.checksum_strategy)
                .await?;
            out.insert(p, cs);
        }
        Ok(out)
    }
}

/// Verify that `pre` and `post` represent the same logical content:
/// same set of partition keys AND identical checksum bytes per key.
/// Compaction must NOT change either.  Returns
/// `StewardError::Invariant` describing the first mismatch found.
fn assert_compaction_invariant(pre: &PartitionChecksums, post: &PartitionChecksums) -> Result<()> {
    if pre.len() != post.len() {
        return Err(crate::error::StewardError::Invariant(format!(
            "compaction changed partition count: pre={} post={}",
            pre.len(),
            post.len(),
        )));
    }
    for (p, pre_cs) in pre {
        match post.get(p) {
            Some(post_cs) if post_cs == pre_cs => {}
            Some(post_cs) => {
                return Err(crate::error::StewardError::Invariant(format!(
                    "compaction altered partition {:?} checksum: pre={} post={}",
                    p,
                    pre_cs.hex(),
                    post_cs.hex(),
                )));
            }
            None => {
                return Err(crate::error::StewardError::Invariant(format!(
                    "compaction removed partition {:?}",
                    p
                )));
            }
        }
    }
    for p in post.keys() {
        if !pre.contains_key(p) {
            return Err(crate::error::StewardError::Invariant(format!(
                "compaction introduced new partition {:?}",
                p
            )));
        }
    }
    Ok(())
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
