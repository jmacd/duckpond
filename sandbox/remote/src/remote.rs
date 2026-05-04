// SPDX-License-Identifier: Apache-2.0

//! The [`Remote`] type: a Delta-Lake-backed remote-sync target.
//!
//! See `../../DESIGN.md` §2.5 for the full design.  In short: a remote
//! is a single Delta Lake table per pond family.  Push runs after a
//! source-side commit; it builds one bundle (1 manifest row + N
//! checksum rows + C data rows) and writes it as a single Delta commit
//! on the remote.  The remote table's `sandbox.store_id` configuration
//! property identifies the pond family; all push and pull operations
//! validate this against the steward's `store_id` first.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use chrono::Utc;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use deltalake::protocol::SaveMode;
use sandbox_steward::{CommitKind, Steward};
use url::Url;
use uuid::Uuid;

use crate::chunking::{ChunkRecord, assemble_file, chunk_bytes};
use crate::error::{RemoteError, Result};
use crate::schema::{
    self, RemoteRow, RowBody, delta_columns, partition_columns, record_batch_to_rows,
    rows_to_record_batch,
};
use sandbox_steward::PartitionChecksums;
use sandbox_store::checksum::Checksum;

/// Delta table configuration key under which the source pond's
/// `store_id` is recorded.  Set once at [`Remote::create`] and read on
/// every [`Remote::open`].
pub const STORE_ID_PROPERTY: &str = "sandbox.store_id";

const TABLE_NAME: &str = "remote";

/// The remote-sync handle.  Wraps a Delta Lake table.
pub struct Remote {
    path: PathBuf,
    store_id: Uuid,
    table: DeltaTable,
    session_ctx: Arc<SessionContext>,
}

/// Header information for one bundle on the remote, decoded from a
/// `manifest` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleHeader {
    /// The source-pond transaction sequence this bundle represents.
    pub txn_seq: i64,
    /// `Write` or `Compact`.
    pub commit_kind: CommitKind,
    /// Predecessor commit's `txn_seq`, or 0 if this is the root.
    pub parent_seq: i64,
    /// When the bundle was pushed.
    pub ts_micros: i64,
}

/// Result of a successful [`Remote::pull`].
#[derive(Debug, Clone)]
pub struct PullReport {
    /// Bundles applied during this pull, in seq order.  Empty if
    /// the consumer was already caught up.
    pub bundles_applied: Vec<BundleHeader>,
    /// Consumer's `last_pulled_seq` after this pull.
    pub last_pulled_seq: i64,
}

/// Options for [`Remote::maintain`].
#[derive(Debug, Clone, Copy)]
pub struct MaintainOptions {
    /// Number of compact bundles to retain at the tail.  Must be
    /// `>= 1` (refuses to leave no restart point).  Default 2.
    pub keep_compact_bundles: usize,
    /// If true, run delta-rs vacuum after the DELETE so unreferenced
    /// parquet files are physically reclaimed.  Default true.
    pub vacuum_after: bool,
}

impl Default for MaintainOptions {
    fn default() -> Self {
        Self {
            keep_compact_bundles: 2,
            vacuum_after: true,
        }
    }
}

/// Result of a successful [`Remote::maintain`].
#[derive(Debug, Clone)]
pub struct MaintainReport {
    /// Bundles with `txn_seq < horizon` were deleted.  Equal to the
    /// `txn_seq` of the Nth-most-recent compact bundle.
    pub horizon: i64,
    /// Count of (manifest + checksum + data) rows removed by the
    /// DELETE commit.
    pub rows_deleted: i64,
    /// Count of parquet files reclaimed by vacuum (0 if
    /// `vacuum_after = false`).
    pub files_vacuumed: usize,
}

/// Steward setting key under which a consumer records the highest
/// bundle seq it has pulled from a particular remote.  Discriminated
/// by the remote's on-disk path so a single steward can pull from
/// multiple remotes (e.g., primary + offsite archive) and track each
/// independently.
fn last_pulled_seq_key(remote_path: &Path) -> String {
    format!("last_pulled_seq:{}", remote_path.display())
}

impl Remote {
    /// Create a fresh remote at `path` with the given `store_id`.
    /// Errors if a Delta table already exists at `path`.
    pub async fn create(path: impl AsRef<Path>, store_id: Uuid) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        let url = url_from_path(&path)?;

        let mut config: HashMap<String, Option<String>> = HashMap::new();
        let _ = config.insert(STORE_ID_PROPERTY.to_string(), Some(store_id.to_string()));

        let table = DeltaTable::try_from_url(url)
            .await?
            .create()
            .with_columns(delta_columns())
            .with_partition_columns(partition_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_configuration(config)
            // Our `sandbox.store_id` is a custom property; without
            // this delta-rs rejects the key as unknown.
            .with_raise_if_key_not_exists(false)
            .await?;

        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            store_id,
            table,
            session_ctx,
        })
    }

    /// Open an existing remote at `path`.  Reads `store_id` from the
    /// Delta table's `sandbox.store_id` configuration property; errors
    /// if it is missing or unparseable.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let url = url_from_path(&path)?;
        let table = deltalake::open_table(url).await?;
        let store_id = read_store_id(&table)?;
        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            store_id,
            table,
            session_ctx,
        })
    }

    /// On-disk path the remote was created/opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The remote's recorded `store_id`.
    pub fn store_id(&self) -> Uuid {
        self.store_id
    }

    /// All bundles on the remote, ordered by `txn_seq`.
    pub async fn list_bundles(&self) -> Result<Vec<BundleHeader>> {
        let sql = format!(
            "SELECT * FROM {table} WHERE {col} = '{val}' ORDER BY {seq} ASC",
            table = TABLE_NAME,
            col = schema::col::PARTITION_KIND,
            val = schema::PARTITION_KIND_MANIFEST,
            seq = schema::col::TXN_SEQ,
        );
        let batches: Vec<RecordBatch> = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for b in &batches {
            for row in record_batch_to_rows(b)? {
                if let RowBody::Manifest {
                    commit_kind,
                    parent_seq,
                } = row.body
                {
                    out.push(BundleHeader {
                        txn_seq: row.txn_seq,
                        commit_kind,
                        parent_seq,
                        ts_micros: row.ts_micros,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Largest `txn_seq` for which a bundle exists on the remote, or
    /// `None` if the remote has no bundles yet.
    pub async fn latest_seq(&self) -> Result<Option<i64>> {
        let bundles = self.list_bundles().await?;
        Ok(bundles.iter().map(|b| b.txn_seq).max())
    }

    /// Smallest `txn_seq` for which a bundle exists on the remote, or
    /// `None` if the remote has no bundles yet.
    pub async fn oldest_available_seq(&self) -> Result<Option<i64>> {
        let bundles = self.list_bundles().await?;
        Ok(bundles.iter().map(|b| b.txn_seq).min())
    }

    /// Push the bundle for `txn_seq` from `steward` to this remote.
    ///
    /// See `../../DESIGN.md` §2.5.3 for the lifecycle.  In one
    /// paragraph: looks up the source's `DataCommitted` record,
    /// extracts Add/Remove file actions for the corresponding Delta
    /// version, chunks each Add file into [`crate::CHUNK_SIZE_BYTES`]
    /// pieces with per-chunk and per-file BLAKE3 hashes, snapshots the
    /// partition_checksums recorded at commit time, and writes one
    /// Delta commit on the remote inserting all rows together.
    ///
    /// Idempotent: if the manifest row for `txn_seq` already exists on
    /// the remote, returns `Ok(())` immediately (and writes a
    /// `PostPushCompleted` if missing locally).
    pub async fn push(&mut self, steward: &mut Steward, txn_seq: i64) -> Result<()> {
        // 1. Verify store_id matches.
        if self.store_id != steward.store_id() {
            return Err(RemoteError::StoreIdMismatch {
                remote: self.store_id,
                steward: steward.store_id(),
            });
        }

        // 2. Look up the source's DataCommitted record.
        let dc = steward
            .data_committed_record(txn_seq)
            .await?
            .ok_or(RemoteError::NoSuchCommit(txn_seq))?;
        let dc_meta: sandbox_steward::DataCommittedMetadata =
            serde_json::from_str(&dc.metadata_json)?;
        let commit_kind = dc.commit_kind.ok_or_else(|| {
            RemoteError::Schema(format!(
                "DataCommitted at txn_seq {} missing commit_kind",
                txn_seq
            ))
        })?;
        let parent_seq = dc.parent_seq.unwrap_or(0);

        // 3. Idempotence: if a manifest row already exists on the
        // remote for this txn_seq, the push is a no-op success.  Bring
        // local control table into agreement by writing
        // PostPushCompleted if the most recent PostPush for this
        // txn_seq is still Pending.
        if self.has_manifest_for(txn_seq).await? {
            self.reconcile_post_push_after_idempotent_skip(steward, txn_seq)
                .await?;
            return Ok(());
        }

        // 4. Write PostPushPending locally.
        let pending_started = Utc::now().timestamp_micros();
        let txn_id = steward.record_post_push_pending(txn_seq).await?;

        // Build the bundle inside a closure so any failure path can
        // record PostPushFailed with the reason and propagate.
        let result = self
            .build_and_commit_bundle(steward, &dc_meta, txn_seq, commit_kind, parent_seq)
            .await;

        match result {
            Ok(()) => {
                steward
                    .record_post_push_completed(txn_seq, txn_id, pending_started)
                    .await?;
                Ok(())
            }
            Err(e) => {
                let reason = format!("{}", e);
                let _ = steward
                    .record_post_push_failed(txn_seq, txn_id, pending_started, reason)
                    .await;
                Err(e)
            }
        }
    }

    /// Pull all bundles from the remote that the consumer (`steward`)
    /// has not yet applied.  Mirror mode only.
    ///
    /// See `../../DESIGN.md` §2.5.4.  Lifecycle:
    /// 1. Verify the remote's `store_id` matches the steward's.
    /// 2. Read consumer's `last_pulled_seq:<remote_url>` setting (0 if
    ///    unset).
    /// 3. Retention horizon check: error with [`RemoteError::BehindRetention`]
    ///    if the remote has pruned bundles the consumer needs.
    /// 4. List the remote's manifest rows; pick those with
    ///    `txn_seq > last_pulled_seq`, sorted ascending.
    /// 5. For each bundle in seq order:
    ///    1. Read its data and checksum rows from the remote.
    ///    2. Group `DataAdd` chunks by `file_path`; reassemble each
    ///       file via `chunking::assemble_file` (verifies per-chunk
    ///       and per-file BLAKE3 BEFORE writing).
    ///    3. Collect `DataRemove` paths.
    ///    4. Build the partition_checksums map from `Checksum` rows.
    ///    5. Apply via `Steward::apply_pulled_bundle`.
    ///    6. Update the consumer's `last_pulled_seq` setting to this
    ///       bundle's seq AFTER the apply succeeds (per-bundle progress).
    /// 6. Return [`PullReport`].
    ///
    /// Idempotent: a re-pull when the consumer is fully caught up
    /// returns an empty `PullReport`.  A re-pull after a crash in the
    /// middle of step 5 picks up from the last successfully-applied
    /// bundle (`apply_pulled_bundle` itself short-circuits on
    /// already-applied seqs as belt-and-suspenders).
    pub async fn pull(&self, steward: &mut Steward) -> Result<PullReport> {
        // 1. Verify store_id.
        if self.store_id != steward.store_id() {
            return Err(RemoteError::StoreIdMismatch {
                remote: self.store_id,
                steward: steward.store_id(),
            });
        }

        // 2. Read consumer's last_pulled_seq for this remote URL.
        let setting_key = last_pulled_seq_key(&self.path);
        let last_pulled = match steward.config_get(&setting_key).await? {
            None => 0i64,
            Some(s) => s.parse::<i64>().map_err(|e| {
                RemoteError::Schema(format!(
                    "consumer setting `{}` is not a valid i64: `{}` ({})",
                    setting_key, s, e
                ))
            })?,
        };

        // 3. Retention horizon check.
        if let Some(oldest) = self.oldest_available_seq().await?
            && oldest > last_pulled + 1
        {
            return Err(RemoteError::BehindRetention {
                last_pulled,
                oldest_available: oldest,
            });
        }

        // 4. Pick bundles with seq > last_pulled.
        let bundles: Vec<BundleHeader> = self
            .list_bundles()
            .await?
            .into_iter()
            .filter(|b| b.txn_seq > last_pulled)
            .collect();

        // 5. Apply each bundle in seq order with per-bundle progress.
        let mut applied: Vec<BundleHeader> = Vec::with_capacity(bundles.len());
        let mut highest = last_pulled;
        for bundle in bundles {
            let (adds, removes) = self.read_data_for_bundle(bundle.txn_seq).await?;
            let partition_checksums = self.read_checksums_for_bundle(bundle.txn_seq).await?;
            steward
                .apply_pulled_bundle(
                    bundle.txn_seq,
                    bundle.commit_kind,
                    bundle.parent_seq,
                    adds,
                    removes,
                    partition_checksums,
                )
                .await?;
            steward
                .config_set(&setting_key, &bundle.txn_seq.to_string())
                .await?;
            highest = bundle.txn_seq;
            applied.push(bundle);
        }

        Ok(PullReport {
            bundles_applied: applied,
            last_pulled_seq: highest,
        })
    }

    /// Prune old bundles from the remote, retaining the last
    /// [`MaintainOptions::keep_compact_bundles`] compact bundles as
    /// restart points.  See `../../DESIGN.md` §2.5.5.
    ///
    /// Lifecycle:
    /// 1. Validate `keep_compact_bundles >= 1`; else
    ///    [`RemoteError::InvalidRetention`].
    /// 2. List compact bundles; if fewer than `N` exist, refuse with
    ///    [`RemoteError::InsufficientCompactBundles`] (would leave no
    ///    restart point).
    /// 3. Compute horizon = the Nth-most-recent compact bundle's
    ///    `txn_seq`.  All bundles with `txn_seq < horizon` will be
    ///    pruned; the Nth compact itself remains as the oldest
    ///    restart point.
    /// 4. Single Delta DELETE on the remote with predicate
    ///    `txn_seq < horizon` -- atomically removes manifest +
    ///    checksum + data rows for all pruned bundles.
    /// 5. If `vacuum_after`, run delta-rs vacuum to reclaim parquet
    ///    files no longer referenced by any active commit.
    /// 6. Return [`MaintainReport`].
    ///
    /// Idempotent: a second call with the same options is a no-op
    /// (horizon stays where it was; no rows match the predicate).
    pub async fn maintain(&mut self, opts: MaintainOptions) -> Result<MaintainReport> {
        // 1. Validate.
        if opts.keep_compact_bundles == 0 {
            return Err(RemoteError::InvalidRetention(0));
        }

        // 2. List compact bundles.
        let bundles = self.list_bundles().await?;
        let mut compacts: Vec<&BundleHeader> = bundles
            .iter()
            .filter(|b| matches!(b.commit_kind, CommitKind::Compact))
            .collect();
        compacts.sort_by_key(|b| b.txn_seq);
        if compacts.len() < opts.keep_compact_bundles {
            return Err(RemoteError::InsufficientCompactBundles {
                have: compacts.len(),
                need: opts.keep_compact_bundles,
            });
        }

        // 3. horizon = Nth-most-recent compact's seq.
        let horizon_index = compacts.len() - opts.keep_compact_bundles;
        let horizon = compacts[horizon_index].txn_seq;

        // 4. DELETE rows below horizon as one Delta commit.
        let rows_deleted = self.delete_below_horizon(horizon).await?;

        // 5. Optional vacuum.
        let files_vacuumed = if opts.vacuum_after {
            self.vacuum_zero_retention().await?
        } else {
            0
        };

        Ok(MaintainReport {
            horizon,
            rows_deleted,
            files_vacuumed,
        })
    }

    /// Wipe `consumer_path` (after safety checks) and bootstrap a
    /// fresh consumer pond from the remote's oldest available
    /// compact bundle.  See `../../DESIGN.md` §2.5.4
    /// "restart from compact" path.
    ///
    /// Recovery flow for a consumer that hits
    /// [`RemoteError::BehindRetention`] on pull:
    ///
    /// ```ignore
    /// match remote.pull(&mut consumer).await {
    ///     Err(RemoteError::BehindRetention { .. }) => {
    ///         let path = consumer.path().to_path_buf();
    ///         drop(consumer);
    ///         consumer = remote.restart_from_compact(&path).await?;
    ///     }
    ///     other => other?,
    /// }
    /// ```
    ///
    /// Lifecycle:
    /// 1. Validate: remote has at least one compact bundle, else
    ///    [`RemoteError::NoRestartPoint`].
    /// 2. Pick the oldest compact bundle (the current retention
    ///    horizon) as the baseline.
    /// 3. Safety wipe: if `consumer_path` exists, require it to be
    ///    a same-family pond (open as Steward; store_id must match
    ///    the remote's).  Otherwise [`RemoteError::RestartPathNotPond`]
    ///    or [`RemoteError::StoreIdMismatch`].  Then drop and
    ///    recursively remove the directory.
    /// 4. Create a fresh Steward at `consumer_path` with the remote's
    ///    `store_id`.
    /// 5. Read the baseline compact bundle's adds + checksums from
    ///    the remote.
    /// 6. Apply via `Steward::apply_pulled_bundle` with EMPTY removes
    ///    -- the consumer is fresh, has no prior parquets to remove,
    ///    and the compact's Adds alone reconstruct the source's
    ///    logical state at that txn_seq (compaction is checksum-
    ///    invariant by design, so the compact's recorded
    ///    partition_checksums match the consumer's resulting state).
    /// 7. Set the consumer's `last_pulled_seq:<remote_path>` setting
    ///    to the baseline compact's seq.
    /// 8. Call `self.pull(&mut consumer)` to apply all bundles after
    ///    the baseline (catch up to latest).
    /// 9. Return the new Steward.
    pub async fn restart_from_compact(&self, consumer_path: &Path) -> Result<Steward> {
        // 1. Validate: at least one compact bundle.
        let bundles = self.list_bundles().await?;
        let baseline = bundles
            .iter()
            .filter(|b| matches!(b.commit_kind, CommitKind::Compact))
            .min_by_key(|b| b.txn_seq)
            .ok_or(RemoteError::NoRestartPoint)?
            .clone();

        // 2./3. Safety wipe.
        if consumer_path.exists() {
            // Try to open as a Steward to confirm it's a pond.
            match Steward::open(consumer_path).await {
                Ok(existing) => {
                    if existing.store_id() != self.store_id {
                        return Err(RemoteError::StoreIdMismatch {
                            remote: self.store_id,
                            steward: existing.store_id(),
                        });
                    }
                    drop(existing);
                }
                Err(e) => {
                    return Err(RemoteError::RestartPathNotPond {
                        path: consumer_path.display().to_string(),
                        reason: format!("{}", e),
                    });
                }
            }
            std::fs::remove_dir_all(consumer_path)?;
        }

        // 4. Create fresh Steward with remote's store_id.
        let mut consumer = Steward::create_with_options(
            consumer_path,
            sandbox_steward::StewardOptions {
                store_id: Some(self.store_id),
                ..Default::default()
            },
        )
        .await?;

        // 5. Read baseline compact bundle from remote.
        let (adds, _removes_ignored) = self.read_data_for_bundle(baseline.txn_seq).await?;
        let partition_checksums = self.read_checksums_for_bundle(baseline.txn_seq).await?;

        // 6. Apply with empty removes.
        consumer
            .apply_pulled_bundle(
                baseline.txn_seq,
                baseline.commit_kind,
                baseline.parent_seq,
                adds,
                Vec::new(),
                partition_checksums,
            )
            .await?;

        // 7. Set last_pulled_seq so subsequent pull doesn't re-apply
        //    the baseline and doesn't trip the retention check.
        let setting_key = last_pulled_seq_key(&self.path);
        consumer
            .config_set(&setting_key, &baseline.txn_seq.to_string())
            .await?;

        // 8. Catch up to latest.
        let _report = self.pull(&mut consumer).await?;

        // 9. Return the new Steward.
        Ok(consumer)
    }

    async fn delete_below_horizon(&mut self, horizon: i64) -> Result<i64> {
        let predicate = format!("{} < {}", schema::col::TXN_SEQ, horizon);
        let (new_table, metrics) = self
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(metrics.num_deleted_rows as i64)
    }

    async fn vacuum_zero_retention(&mut self) -> Result<usize> {
        let (new_table, metrics) = self
            .table
            .clone()
            .vacuum()
            .with_retention_period(chrono::Duration::seconds(0))
            .with_enforce_retention_duration(false)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(metrics.files_deleted.len())
    }

    /// Read all data rows for `txn_seq` from the remote, reassemble
    /// each `DataAdd` file's chunks (verifying BLAKE3 in the
    /// process), and return `(adds, removes)` where adds are
    /// `(path, bytes)` and removes are paths.
    async fn read_data_for_bundle(
        &self,
        txn_seq: i64,
    ) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>)> {
        let sql = format!(
            "SELECT * FROM {table} WHERE {kind} = '{d}' AND {seq} = {n}",
            table = TABLE_NAME,
            kind = schema::col::PARTITION_KIND,
            d = schema::PARTITION_KIND_DATA,
            seq = schema::col::TXN_SEQ,
            n = txn_seq,
        );
        let batches: Vec<RecordBatch> = self.session_ctx.sql(&sql).await?.collect().await?;

        // Group DataAdd chunks by file_path; collect DataRemove paths.
        let mut chunks_by_path: HashMap<String, Vec<ChunkRecord>> = HashMap::new();
        let mut size_and_hash: HashMap<String, (i64, [u8; schema::BLAKE3_LEN])> = HashMap::new();
        let mut removes: Vec<String> = Vec::new();
        for batch in &batches {
            for row in record_batch_to_rows(batch)? {
                match row.body {
                    RowBody::DataAdd {
                        file_path,
                        file_size,
                        file_blake3,
                        chunk_id,
                        chunk_data,
                        chunk_blake3,
                        ..
                    } => {
                        chunks_by_path
                            .entry(file_path.clone())
                            .or_default()
                            .push(ChunkRecord {
                                chunk_id,
                                chunk_data,
                                chunk_blake3,
                            });
                        let _ = size_and_hash.insert(file_path, (file_size, file_blake3));
                    }
                    RowBody::DataRemove { file_path } => removes.push(file_path),
                    _ => {
                        return Err(RemoteError::Schema(format!(
                            "data partition contains a non-data row at txn_seq {}",
                            txn_seq
                        )));
                    }
                }
            }
        }

        // Sort chunks per file by chunk_id, then assemble.
        let mut adds: Vec<(String, Vec<u8>)> = Vec::with_capacity(chunks_by_path.len());
        for (path, mut chunks) in chunks_by_path {
            chunks.sort_by_key(|c| c.chunk_id);
            let (file_size, file_blake3) = size_and_hash.get(&path).copied().ok_or_else(|| {
                RemoteError::Schema(format!(
                    "missing file_size/file_blake3 for path {} at txn_seq {}",
                    path, txn_seq
                ))
            })?;
            let bytes = assemble_file(&chunks, file_size, &file_blake3)?;
            adds.push((path, bytes));
        }
        // Stable order for tests.
        adds.sort_by(|a, b| a.0.cmp(&b.0));
        removes.sort();
        Ok((adds, removes))
    }

    /// Read all checksum rows for `txn_seq` from the remote and
    /// build a [`PartitionChecksums`] map.
    pub(crate) async fn read_checksums_for_bundle(
        &self,
        txn_seq: i64,
    ) -> Result<PartitionChecksums> {
        let sql = format!(
            "SELECT * FROM {table} WHERE {kind} = '{c}' AND {seq} = {n}",
            table = TABLE_NAME,
            kind = schema::col::PARTITION_KIND,
            c = schema::PARTITION_KIND_CHECKSUM,
            seq = schema::col::TXN_SEQ,
            n = txn_seq,
        );
        let batches: Vec<RecordBatch> = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = PartitionChecksums::new();
        for batch in &batches {
            for row in record_batch_to_rows(batch)? {
                if let RowBody::Checksum {
                    partition_key,
                    checksum_kind,
                    checksum_bytes,
                } = row.body
                {
                    let prev = out.insert(
                        partition_key.clone(),
                        Checksum::new(checksum_kind, checksum_bytes.to_vec()),
                    );
                    if prev.is_some() {
                        return Err(RemoteError::Schema(format!(
                            "duplicate checksum row for partition `{}` at txn_seq {}",
                            partition_key, txn_seq,
                        )));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn build_and_commit_bundle(
        &mut self,
        steward: &Steward,
        dc_meta: &sandbox_steward::DataCommittedMetadata,
        txn_seq: i64,
        commit_kind: CommitKind,
        parent_seq: i64,
    ) -> Result<()> {
        // 5. Read data_delta_version.
        if dc_meta.data_delta_version <= 0 {
            return Err(RemoteError::Schema(format!(
                "DataCommitted at txn_seq {} has invalid data_delta_version {}",
                txn_seq, dc_meta.data_delta_version,
            )));
        }
        let version = dc_meta.data_delta_version;

        // 6. Get Add/Remove file actions.
        let (adds, removes) = steward.actions_at_version(version).await?;

        let now = Utc::now().timestamp_micros();
        let mut rows: Vec<RemoteRow> =
            Vec::with_capacity(1 + dc_meta.partition_checksums.len() + adds.len() + removes.len());

        // 10. Manifest row (one per bundle).
        rows.push(RemoteRow {
            txn_seq,
            ts_micros: now,
            body: RowBody::Manifest {
                commit_kind,
                parent_seq,
            },
        });

        // 9. Checksum rows.
        for (partition_key, cv) in &dc_meta.partition_checksums {
            let checksum = sandbox_store::checksum::Checksum::from(cv);
            if checksum.bytes.len() != schema::BLAKE3_LEN {
                return Err(RemoteError::Schema(format!(
                    "checksum for partition `{}` has {} bytes, expected {}",
                    partition_key,
                    checksum.bytes.len(),
                    schema::BLAKE3_LEN,
                )));
            }
            let mut arr = [0u8; schema::BLAKE3_LEN];
            arr.copy_from_slice(&checksum.bytes);
            rows.push(RemoteRow {
                txn_seq,
                ts_micros: now,
                body: RowBody::Checksum {
                    partition_key: partition_key.clone(),
                    checksum_kind: cv.kind,
                    checksum_bytes: arr,
                },
            });
        }

        // 7. DataAdd rows (one per chunk per added file).
        for add in &adds {
            let bytes = steward.read_data_file(&add.path)?;
            let chunked = chunk_bytes(&bytes);
            let chunk_count = chunked.chunk_count();
            for chunk in chunked.chunks {
                rows.push(RemoteRow {
                    txn_seq,
                    ts_micros: now,
                    body: RowBody::DataAdd {
                        file_path: add.path.clone(),
                        file_size: chunked.file_size,
                        file_blake3: chunked.file_blake3,
                        chunk_count,
                        chunk_id: chunk.chunk_id,
                        chunk_data: chunk.chunk_data,
                        chunk_blake3: chunk.chunk_blake3,
                    },
                });
            }
        }

        // 8. DataRemove rows (compact only; Write commits have no removes).
        for remove in &removes {
            rows.push(RemoteRow {
                txn_seq,
                ts_micros: now,
                body: RowBody::DataRemove {
                    file_path: remove.path.clone(),
                },
            });
        }

        // 11. Single Delta commit on the remote.
        let batch = rows_to_record_batch(&rows)?;
        let new_table = self.table.clone().write(vec![batch]).await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// Has any manifest row been pushed for `txn_seq` yet?
    async fn has_manifest_for(&self, txn_seq: i64) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) AS n FROM {table} WHERE {kind} = '{m}' AND {seq} = {n}",
            table = TABLE_NAME,
            kind = schema::col::PARTITION_KIND,
            m = schema::PARTITION_KIND_MANIFEST,
            seq = schema::col::TXN_SEQ,
            n = txn_seq,
        );
        let batches: Vec<RecordBatch> = self.session_ctx.sql(&sql).await?.collect().await?;
        for b in &batches {
            if b.num_rows() == 0 {
                continue;
            }
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    RemoteError::Schema("COUNT(*) result column is not Int64".to_string())
                })?;
            if !arr.is_null(0) && arr.value(0) > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// On idempotent re-push, ensure the local control table reflects
    /// success: if the latest PostPush record for `txn_seq` is Pending
    /// (a previous push committed remotely but crashed before
    /// recording PostPushCompleted), write a Completed now to close
    /// the lifecycle.  No-op otherwise.
    async fn reconcile_post_push_after_idempotent_skip(
        &self,
        steward: &mut Steward,
        txn_seq: i64,
    ) -> Result<()> {
        let log = steward.log(None).await?;
        let mut latest_for_seq: Option<&sandbox_steward::ControlRecord> = None;
        for r in &log {
            if r.txn_seq != txn_seq {
                continue;
            }
            if matches!(
                r.record_kind,
                sandbox_steward::RecordKind::PostPushPending
                    | sandbox_steward::RecordKind::PostPushCompleted
                    | sandbox_steward::RecordKind::PostPushFailed
            ) && latest_for_seq
                .map(|prev| r.ts_micros > prev.ts_micros)
                .unwrap_or(true)
            {
                latest_for_seq = Some(r);
            }
        }
        if let Some(r) = latest_for_seq
            && r.record_kind == sandbox_steward::RecordKind::PostPushPending
        {
            // Treat this as resumption from a crash: write Completed
            // with the same txn_id, using the Pending row's timestamp
            // as the started time.
            steward
                .record_post_push_completed(txn_seq, r.txn_id.clone(), r.ts_micros)
                .await?;
        }
        Ok(())
    }
}

fn url_from_path(path: &Path) -> Result<Url> {
    Url::from_directory_path(path)
        .or_else(|_| Url::from_file_path(path))
        .map_err(|_| RemoteError::InvalidRemote(format!("invalid path: {}", path.display())))
}

fn build_session_ctx(table: &DeltaTable) -> Result<Arc<SessionContext>> {
    let ctx = SessionContext::new();
    let _ = ctx.register_table(TABLE_NAME, Arc::new(table.clone()))?;
    Ok(Arc::new(ctx))
}

fn read_store_id(table: &DeltaTable) -> Result<Uuid> {
    let snapshot = table.snapshot().map_err(|e| {
        RemoteError::InvalidRemote(format!("could not read remote snapshot: {}", e))
    })?;
    let metadata = snapshot.metadata();
    let raw = metadata
        .configuration()
        .get(STORE_ID_PROPERTY)
        .ok_or_else(|| {
            RemoteError::InvalidRemote(format!(
                "remote table missing `{}` configuration property",
                STORE_ID_PROPERTY
            ))
        })?;
    Uuid::parse_str(raw).map_err(|e| {
        RemoteError::InvalidRemote(format!(
            "remote `{}` property is not a valid UUID: `{}` ({})",
            STORE_ID_PROPERTY, raw, e
        ))
    })
}
