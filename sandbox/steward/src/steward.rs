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
    self, ChecksumValue, CommitKind, ControlRecord, ControlTable, DataCommittedMetadata,
    PartitionChecksums, RecordKind, new_txn_id,
};
use crate::error::{Result, StewardError};
use crate::guard::{ReadGuard, WriteGuard, WriteGuardInner};

/// Setting key under which the pond's `store_id` is persisted in the
/// control table.  Set once at `Steward::create`; immutable thereafter.
const STORE_ID_KEY: &str = "store_id";

/// Well-known pond_id used for bootstrap-time settings that must be
/// readable BEFORE the local pond_id is known.  Currently only
/// `STORE_ID_KEY` uses this.  Other settings (e.g.,
/// `last_pulled_seq:<remote>`) are scoped to the local pond_id.
const BOOTSTRAP_POND_ID: Uuid = Uuid::nil();

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
            .config_set(BOOTSTRAP_POND_ID, STORE_ID_KEY, &store_id.to_string())
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
        let store_id_str = control
            .config_get(BOOTSTRAP_POND_ID, STORE_ID_KEY)
            .await?
            .ok_or_else(|| {
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
        let last_write_seq = control.last_txn_seq(store_id).await?;
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
        self.control.last_committed_seq(self.store_id).await
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
        self.control
            .partition_checksums_at(self.store_id, txn_seq)
            .await
    }

    /// Resolve the data store's Delta Lake version at `txn_seq`.
    /// Returns `None` if no `DataCommitted` record exists for that seq;
    /// returns `Some(0)` for legacy records written before the
    /// `data_delta_version` field existed.
    pub async fn data_delta_version_at(&self, txn_seq: i64) -> Result<Option<i64>> {
        self.control
            .data_delta_version_at(self.store_id, txn_seq)
            .await
    }

    /// Look up the `DataCommitted` record for `txn_seq`.  Returns
    /// `None` if no such record exists (the txn was Failed,
    /// Completed, no-op compact, or never allocated).
    pub async fn data_committed_record(&self, txn_seq: i64) -> Result<Option<ControlRecord>> {
        let log = self.control.all_records().await?;
        Ok(log
            .into_iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq))
    }

    /// Read the Add and Remove file actions recorded by the data
    /// store at `version`.  Delegates to
    /// [`sandbox_store::Store::actions_at_version`].
    pub async fn actions_at_version(
        &self,
        version: i64,
    ) -> Result<(Vec<sandbox_store::AddPath>, Vec<sandbox_store::RemovePath>)> {
        Ok(self.store.actions_at_version(version).await?)
    }

    /// Read the bytes of a data-store-relative file path.  Used by
    /// `remote-push` to slurp parquet files emitted by Delta commits.
    pub fn read_data_file(&self, rel_path: &str) -> Result<Vec<u8>> {
        let p = self.store.path().join(rel_path);
        Ok(std::fs::read(p)?)
    }

    /// Compute the current live partition_checksums for every
    /// partition known to the data store, using the steward's
    /// configured checksum strategy.  Used by `verify_local` and by
    /// `verify_against_remote` (in sandbox-remote) for drift
    /// detection without needing a recorded snapshot.
    pub async fn compute_live_checksums(&self) -> Result<PartitionChecksums> {
        let partitions = self.store.partitions(self.store_id).await?;
        let mut out = PartitionChecksums::new();
        for partition in partitions {
            let cs = self
                .store
                .compute_partition_checksum(self.store_id, &partition, &*self.checksum_strategy)
                .await?;
            let _ = out.insert(partition, cs);
        }
        Ok(out)
    }

    /// Reclaim disk space on the data store by running delta-rs
    /// vacuum.  After `compact()` removes the small parquets via
    /// Delta tombstones, those files are still on disk until vacuum
    /// physically deletes them.  `vacuum` returns the count of
    /// reclaimed files.
    ///
    /// Mirrors the vacuum that `Remote::maintain` does on the
    /// remote.  Together with periodic `compact()`, this keeps the
    /// source's data dir size bounded by the logical state size,
    /// not the cumulative write history.
    pub async fn vacuum(&mut self) -> Result<usize> {
        Ok(self.store.vacuum().await?)
    }

    /// Compact the steward's CONTROL table (separate from the data
    /// store).  Every begin_write/commit/abort/compact/push/config_set
    /// adds a record, so the control table itself grows without
    /// bound unless periodically maintained.  This wraps delta-rs
    /// optimize on the control table; logical content is unchanged
    /// (the audit log is append-only).  Returns
    /// (num_files_added, num_files_removed).
    pub async fn compact_control(&mut self) -> Result<(u64, u64)> {
        self.control.compact().await
    }

    /// Reclaim disk on the control table by running delta-rs vacuum.
    /// Symmetric counterpart to `vacuum()` for the data store.
    /// Returns the count of files reclaimed.
    pub async fn vacuum_control(&mut self) -> Result<usize> {
        self.control.vacuum().await
    }

    /// Apply one bundle pulled from a remote, end-to-end:
    ///
    /// 1. Idempotence: if a `DataCommitted` record already exists at
    ///    `txn_seq`, returns Ok(()) immediately (this bundle was
    ///    applied by a prior pull attempt).
    /// 2. Validates every add/remove path: must be relative, must
    ///    not contain `..`, must contain exactly one
    ///    `partition_key=<value>` Hive segment.  Percent-decodes the
    ///    partition value.
    /// 3. Writes each add's bytes to `<data_path>/<path>` (creating
    ///    parent directories as needed).
    /// 4. Builds `Action::Add` (with `data_change` per `commit_kind`:
    ///    `true` for Write, `false` for Compact) and `Action::Remove`
    ///    (`data_change = false`) and commits all actions in one
    ///    Delta version on the data store.
    /// 5. Writes a `DataCommitted` record on the consumer's control
    ///    table mirroring the source bundle's metadata
    ///    (txn_seq, commit_kind, parent_seq, partition_checksums,
    ///    data_delta_version on the consumer).
    /// 6. Updates `self.last_write_seq` to `max(self.last_write_seq,
    ///    txn_seq)` so subsequent `begin_write` calls allocate
    ///    seqs above any pulled history.
    ///
    /// Used by `Remote::pull` (in the sandbox-remote crate) for the
    /// mirror-mode pull lifecycle.  See DESIGN.md §2.5.4.
    pub async fn apply_pulled_bundle(
        &mut self,
        txn_seq: i64,
        commit_kind: CommitKind,
        parent_seq: i64,
        adds: Vec<(String, Vec<u8>)>,
        removes: Vec<String>,
        partition_checksums: PartitionChecksums,
    ) -> Result<()> {
        // 1. Idempotence: skip if already applied.
        if self.data_committed_record(txn_seq).await?.is_some() {
            return Ok(());
        }

        // 2. Validate every path; reject anything suspicious.
        let mut add_partition_values: Vec<HashMap<String, Option<String>>> =
            Vec::with_capacity(adds.len());
        for (path, _) in &adds {
            add_partition_values.push(parse_partition_values(path)?);
        }
        for path in &removes {
            // Removes still need validation; we don't need their
            // partition_values in the Remove action (Remove can omit
            // them per Delta protocol), but the path must still be
            // safe.
            let _ = parse_partition_values(path)?;
        }

        // 3. Write each add's bytes.
        let data_path = self.store.path();
        let mut add_sizes: Vec<i64> = Vec::with_capacity(adds.len());
        for (rel_path, bytes) in &adds {
            let abs = data_path.join(rel_path);
            if let Some(parent) = abs.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&abs, bytes)?;
            add_sizes.push(bytes.len() as i64);
        }

        // 4. Build actions and commit one Delta version.
        let now_ms = Utc::now().timestamp_millis();
        let data_change_on_add = matches!(commit_kind, CommitKind::Write);
        let mut actions: Vec<deltalake::kernel::Action> =
            Vec::with_capacity(adds.len() + removes.len());
        for ((path, _bytes), (size, partition_values)) in adds
            .iter()
            .zip(add_sizes.iter().zip(add_partition_values.into_iter()))
        {
            let add = deltalake::kernel::Add {
                path: path.clone(),
                partition_values,
                size: *size,
                modification_time: now_ms,
                data_change: data_change_on_add,
                ..Default::default()
            };
            actions.push(deltalake::kernel::Action::Add(add));
        }
        for path in &removes {
            let remove = deltalake::kernel::Remove {
                path: path.clone(),
                data_change: false,
                deletion_timestamp: Some(now_ms),
                ..Default::default()
            };
            actions.push(deltalake::kernel::Action::Remove(remove));
        }
        let op = match commit_kind {
            CommitKind::Write => deltalake::protocol::DeltaOperation::Write {
                mode: deltalake::protocol::SaveMode::Append,
                partition_by: Some(vec!["pond_id".to_string(), "partition_key".to_string()]),
                predicate: None,
            },
            CommitKind::Compact => deltalake::protocol::DeltaOperation::Optimize {
                predicate: None,
                target_size: 0,
            },
        };
        let new_version = self.store.commit_actions(actions, op).await?;

        // 5. Write a DataCommitted record on the consumer control
        //    table mirroring the source's bundle metadata.
        let metadata = DataCommittedMetadata {
            partition_checksums: partition_checksums
                .iter()
                .map(|(k, v)| (k.clone(), ChecksumValue::from(v)))
                .collect(),
            data_delta_version: new_version,
        };
        let metadata_json = serde_json::to_string(&metadata)?;
        let now_micros = Utc::now().timestamp_micros();
        let parent_opt = if parent_seq == 0 {
            None
        } else {
            Some(parent_seq)
        };
        self.control
            .write_record(ControlRecord {
                pond_id: self.store_id,
                record_kind: RecordKind::DataCommitted,
                txn_seq,
                txn_id: control_table::new_txn_id(),
                commit_kind: Some(commit_kind),
                parent_seq: parent_opt,
                duration_ms: Some(0),
                ts_micros: now_micros,
                metadata_json,
            })
            .await?;

        // 6. Advance the in-memory allocator so subsequent
        //    begin_write doesn't collide with pulled history.
        if txn_seq > self.last_write_seq {
            self.last_write_seq = txn_seq;
        }

        Ok(())
    }

    /// Write a `PostPushPending` record to the control table.  Returns
    /// the freshly-generated `txn_id` so subsequent
    /// `record_post_push_completed` / `record_post_push_failed` calls
    /// can pair against it.
    pub async fn record_post_push_pending(&mut self, txn_seq: i64) -> Result<String> {
        let txn_id = control_table::new_txn_id();
        let now = Utc::now().timestamp_micros();
        self.control
            .write_record(ControlRecord {
                pond_id: self.store_id,
                record_kind: RecordKind::PostPushPending,
                txn_seq,
                txn_id: txn_id.clone(),
                commit_kind: None,
                parent_seq: None,
                duration_ms: None,
                ts_micros: now,
                metadata_json: "{}".to_string(),
            })
            .await?;
        Ok(txn_id)
    }

    /// Write a `PostPushCompleted` record paired with a prior
    /// `PostPushPending` (same `txn_id`).  `started_micros` is the
    /// timestamp of the Pending record so duration can be computed.
    pub async fn record_post_push_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        started_micros: i64,
    ) -> Result<()> {
        let now = Utc::now().timestamp_micros();
        let duration_ms = ((now - started_micros) / 1000).max(0);
        self.control
            .write_record(ControlRecord {
                pond_id: self.store_id,
                record_kind: RecordKind::PostPushCompleted,
                txn_seq,
                txn_id,
                commit_kind: None,
                parent_seq: None,
                duration_ms: Some(duration_ms),
                ts_micros: now,
                metadata_json: "{}".to_string(),
            })
            .await?;
        Ok(())
    }

    /// Write a `PostPushFailed` record paired with a prior
    /// `PostPushPending` (same `txn_id`).  `reason` is recorded in
    /// `metadata_json` as `{"reason":"..."}` for operator visibility.
    pub async fn record_post_push_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        started_micros: i64,
        reason: String,
    ) -> Result<()> {
        let now = Utc::now().timestamp_micros();
        let duration_ms = ((now - started_micros) / 1000).max(0);
        let mut meta = HashMap::new();
        meta.insert("reason".to_string(), reason);
        self.control
            .write_record(ControlRecord {
                pond_id: self.store_id,
                record_kind: RecordKind::PostPushFailed,
                txn_seq,
                txn_id,
                commit_kind: None,
                parent_seq: None,
                duration_ms: Some(duration_ms),
                ts_micros: now,
                metadata_json: serde_json::to_string(&meta)?,
            })
            .await?;
        Ok(())
    }

    /// Set a configuration key.
    pub async fn config_set(&mut self, key: &str, value: &str) -> Result<()> {
        self.control.config_set(self.store_id, key, value).await
    }

    /// Read a configuration key.  `None` if never set.
    pub async fn config_get(&self, key: &str) -> Result<Option<String>> {
        self.control.config_get(self.store_id, key).await
    }

    /// All current settings (latest-write-wins per key).
    pub async fn config_list(&self) -> Result<HashMap<String, String>> {
        self.control.config_list(self.store_id).await
    }

    /// Incomplete transactions (Begin without terminal record).
    pub async fn incomplete_transactions(&self) -> Result<Vec<ControlRecord>> {
        self.control.incomplete_transactions(self.store_id).await
    }

    /// Begin a write transaction.  Allocates the next `txn_seq`,
    /// records a Begin row, and returns a guard.
    pub async fn begin_write(&mut self) -> Result<WriteGuard<'_>> {
        let txn_seq = self.last_write_seq + 1;
        let txn_id = new_txn_id();
        let now = Utc::now().timestamp_micros();
        self.control
            .write_record(ControlRecord {
                pond_id: self.store_id,
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
        let parent_seq = self.control.last_committed_seq(self.store_id).await?;
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
        Ok(ReadGuard::new(&self.store, self.store_id))
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
                .apply_batch(self.store_id, txn_seq, started_micros, ops.clone())
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
                    pond_id: self.store_id,
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
                    pond_id: self.store_id,
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
                pond_id: self.store_id,
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
                pond_id: self.store_id,
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
        let parent_seq = match self.control.last_committed_seq(self.store_id).await? {
            0 => None,
            n => Some(n),
        };

        // 2. Snapshot pre-compaction checksums for every known partition.
        let pre = self.snapshot_all_partition_checksums().await?;

        // 3. Run optimize.  On error, record Failed and return.
        let metrics = match self.store.compact(self.store_id, filter).await {
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
                        pond_id: self.store_id,
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
                    pond_id: self.store_id,
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
                pond_id: self.store_id,
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
        let partitions = self.store.partitions(self.store_id).await?;
        let mut out: PartitionChecksums = HashMap::new();
        for p in partitions {
            let cs = self
                .store
                .compute_partition_checksum(self.store_id, &p, &*self.checksum_strategy)
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
    let partitions = steward.store().partitions(steward.store_id()).await?;
    let mut mismatches = Vec::new();
    let strategy = steward.checksum_strategy();
    for partition in partitions {
        let live: Checksum = steward
            .store()
            .compute_partition_checksum(steward.store_id(), &partition, strategy)
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
            .partitions(steward.store_id())
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

/// Parse a Hive-style data file path of the form
/// `pond_id=<uuid>/partition_key=<value>/<file>` (with `<value>`
/// possibly percent-encoded) into a Delta-protocol `partition_values`
/// map.
///
/// Hardened against malformed / hostile input from a remote:
/// rejects absolute paths, paths containing `..` segments, paths
/// missing either `pond_id=` or `partition_key=` segments, paths with
/// multiple of either, and the `__HIVE_DEFAULT_PARTITION__` null
/// sentinel (both columns are non-nullable, so the sentinel should
/// never appear).
fn parse_partition_values(path: &str) -> Result<HashMap<String, Option<String>>> {
    if path.is_empty() {
        return Err(StewardError::Invariant("data path is empty".into()));
    }
    if path.starts_with('/') {
        return Err(StewardError::Invariant(format!(
            "data path is absolute: {:?}",
            path
        )));
    }
    let mut found_pond: Option<String> = None;
    let mut found_partition: Option<String> = None;
    for segment in path.split('/') {
        if segment == ".." {
            return Err(StewardError::Invariant(format!(
                "data path contains `..` segment: {:?}",
                path
            )));
        }
        if let Some(rest) = segment.strip_prefix("pond_id=") {
            if found_pond.is_some() {
                return Err(StewardError::Invariant(format!(
                    "data path has multiple `pond_id=` segments: {:?}",
                    path
                )));
            }
            if rest == "__HIVE_DEFAULT_PARTITION__" {
                return Err(StewardError::Invariant(format!(
                    "pond_id is non-nullable in this schema; null sentinel at: {:?}",
                    path
                )));
            }
            let decoded = percent_decode(rest).map_err(|e| {
                StewardError::Invariant(format!("invalid percent-encoding in {:?}: {}", path, e))
            })?;
            found_pond = Some(decoded);
        } else if let Some(rest) = segment.strip_prefix("partition_key=") {
            if found_partition.is_some() {
                return Err(StewardError::Invariant(format!(
                    "data path has multiple `partition_key=` segments: {:?}",
                    path
                )));
            }
            if rest == "__HIVE_DEFAULT_PARTITION__" {
                return Err(StewardError::Invariant(format!(
                    "partition_key is non-nullable in this schema; null sentinel at: {:?}",
                    path
                )));
            }
            let decoded = percent_decode(rest).map_err(|e| {
                StewardError::Invariant(format!("invalid percent-encoding in {:?}: {}", path, e))
            })?;
            found_partition = Some(decoded);
        }
    }
    let pond = found_pond.ok_or_else(|| {
        StewardError::Invariant(format!("data path missing `pond_id=` segment: {:?}", path))
    })?;
    let partition = found_partition.ok_or_else(|| {
        StewardError::Invariant(format!(
            "data path missing `partition_key=` segment: {:?}",
            path
        ))
    })?;
    let mut out = HashMap::new();
    let _ = out.insert("pond_id".to_string(), Some(pond));
    let _ = out.insert("partition_key".to_string(), Some(partition));
    Ok(out)
}

/// Minimal percent-decoder for Delta partition values.  We avoid
/// pulling in `percent-encoding` as a dependency just for this; the
/// sandbox kv store's keys never use exotic encodings.
fn percent_decode(s: &str) -> std::result::Result<String, String> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(format!("truncated escape at offset {}", i));
            }
            let h = char::from(bytes[i + 1])
                .to_digit(16)
                .ok_or_else(|| format!("bad hex digit at offset {}", i + 1))?;
            let l = char::from(bytes[i + 2])
                .to_digit(16)
                .ok_or_else(|| format!("bad hex digit at offset {}", i + 2))?;
            out.push(((h << 4) | l) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| format!("invalid UTF-8: {}", e))
}

#[cfg(test)]
mod parse_partition_values_tests {
    use super::*;

    const TEST_POND_ID: &str = "0000a1a1-0000-7000-8000-000000000000";

    #[test]
    fn happy_path() {
        let pv = parse_partition_values(&format!(
            "pond_id={}/partition_key=p1/part-001.parquet",
            TEST_POND_ID
        ))
        .unwrap();
        assert_eq!(pv.get("partition_key"), Some(&Some("p1".to_string())));
        assert_eq!(pv.get("pond_id"), Some(&Some(TEST_POND_ID.to_string())));
    }

    #[test]
    fn percent_encoded_value() {
        let pv = parse_partition_values(&format!(
            "pond_id={}/partition_key=p%201/part-001.parquet",
            TEST_POND_ID
        ))
        .unwrap();
        assert_eq!(pv.get("partition_key"), Some(&Some("p 1".to_string())));
    }

    #[test]
    fn rejects_absolute() {
        assert!(
            parse_partition_values(&format!("/pond_id={}/partition_key=p/x", TEST_POND_ID))
                .is_err()
        );
    }

    #[test]
    fn rejects_dotdot() {
        assert!(
            parse_partition_values(&format!("../pond_id={}/partition_key=p/x", TEST_POND_ID))
                .is_err()
        );
        assert!(
            parse_partition_values(&format!(
                "pond_id={}/partition_key=p/../etc/passwd",
                TEST_POND_ID
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_missing_partition_key() {
        assert!(parse_partition_values("part-001.parquet").is_err());
        assert!(
            parse_partition_values(&format!("pond_id={}/part-001.parquet", TEST_POND_ID)).is_err()
        );
    }

    #[test]
    fn rejects_missing_pond_id() {
        assert!(parse_partition_values("partition_key=p/part-001.parquet").is_err());
    }

    #[test]
    fn rejects_multiple_partition_keys() {
        assert!(
            parse_partition_values(&format!(
                "pond_id={}/partition_key=a/partition_key=b/x",
                TEST_POND_ID
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_multiple_pond_ids() {
        assert!(
            parse_partition_values(&format!(
                "pond_id={}/pond_id={}/partition_key=p/x",
                TEST_POND_ID, TEST_POND_ID
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_null_sentinel() {
        assert!(
            parse_partition_values(&format!(
                "pond_id={}/partition_key=__HIVE_DEFAULT_PARTITION__/x",
                TEST_POND_ID
            ))
            .is_err()
        );
        assert!(
            parse_partition_values("pond_id=__HIVE_DEFAULT_PARTITION__/partition_key=p/x").is_err()
        );
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_partition_values("").is_err());
    }
}
