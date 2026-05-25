// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `ShipRemoteSteward`: adapts duckpond's [`Ship`] to the
//! [`sync_remote::RemoteSteward`] trait so that `sync_remote::Remote`
//! can drive duckpond's tlogfs data store (push/pull/maintain) using
//! the same code path it uses for `sync_steward::Steward`.
//!
//! ## What this adapter does
//!
//! Wraps `&mut Ship` and translates the 13 trait methods into:
//! - Reads from the duckpond [`ControlTable`] (raw config, log,
//!   DataCommitted lookups, PostPush lifecycle).
//! - Reads from the [`OpLogPersistence`] DeltaTable
//!   (`actions_at_version`, `read_data_file`).
//! - For `apply_pulled_bundle`: validates each path is of the duckpond
//!   layout `pond_id=<uuid>/part_id=<uuid>/<file>.parquet`, writes the
//!   parquet bytes under `<pond>/data/`, commits Add/Remove actions to
//!   the data DeltaTable, writes a mirroring `DataCommitted` record on
//!   the control table, and bumps `last_write_seq`.  For mirror mode
//!   (bundle.pond_id == ship.pond_id) and -- since D5 -- for cross-pond
//!   import (bundle.pond_id != ship.pond_id), the bundle's `pond_id` is
//!   stamped into the partition path.
//!
//! ## What this adapter does NOT do (yet)
//!
//! - For `apply_pulled_bundle`: cross-pond import (foreign `bundle.pond_id`)
//!   is rejected for now; the partition layout supports it but no caller
//!   wires it up.  See D5.7 in `docs/d5-resume.md`.
//!
//! ## Path layout
//!
//! Duckpond tlogfs paths returned by `actions_at_version` look like:
//!
//! ```text
//! pond_id=<uuid>/part_id=<uuid>/part-00000-<uuid>-c000.snappy.parquet
//! ```
//!
//! Matches the sync_store layout shape (pond_id outermost) and the
//! sandbox prototype's cross-pond import contract.  The adapter parses
//! the leading `pond_id=<uuid>/part_id=<uuid>/` segments via
//! [`parse_pond_part_path`] and rejects anything else.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use log::debug;
use uuid::Uuid;

use sync_remote::RemoteSteward;
use sync_steward::{
    CommitKind, ControlRecord, DataCommittedMetadata, PartitionChecksums, PulledBundle, RecordKind,
    Result as StewardResult, StewardError as SyncStewardError, new_txn_id,
};
use sync_store::checksum::{Leaf, Merkle, PartitionChecksum};
use sync_store::{AddPath, RemovePath};

use crate::Ship;
use tlogfs::schema::OplogEntry;

/// Adapter wrapping `&mut Ship` so that `sync_remote::Remote` can push
/// to and pull from this duckpond instance.
///
/// Lifetime parameter `'a` bounds the borrow on the underlying `Ship`.
/// Constructed by `pond push`/`pond pull` CLI verbs and post-commit
/// auto-push.
pub struct ShipRemoteSteward<'a> {
    ship: &'a mut Ship,
}

impl<'a> ShipRemoteSteward<'a> {
    /// Wrap a `Ship` for sync-remote operations.
    pub fn new(ship: &'a mut Ship) -> Self {
        Self { ship }
    }
}

/// Map any error implementing `std::error::Error + Send + Sync` into
/// `StewardError::Adapter`.
fn adapt_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> SyncStewardError {
    SyncStewardError::Adapter(Box::new(e))
}

/// Parse and validate a duckpond data path.  Expects the form
/// `pond_id=<uuid>/part_id=<uuid>/<filename>` (D5+).  Returns
/// `(pond_id, part_id)` on success.  Rejects:
///
/// - Absolute paths.
/// - Paths containing `..` or `.` segments.
/// - Paths that do not start with `pond_id=<uuid>/part_id=<uuid>/`.
/// - Empty `pond_id` or `part_id` values.
/// - Multiple `pond_id=` or `part_id=` segments.
/// - Missing filename after the partition prefix.
fn parse_pond_part_path(path: &str) -> Result<(String, String), SyncStewardError> {
    if path.starts_with('/') {
        return Err(SyncStewardError::Invariant(format!(
            "data path must be relative: {:?}",
            path
        )));
    }
    let segments: Vec<&str> = path.split('/').collect();
    for seg in &segments {
        if *seg == ".." || *seg == "." || seg.is_empty() {
            return Err(SyncStewardError::Invariant(format!(
                "data path contains invalid segment {:?}: {:?}",
                seg, path
            )));
        }
    }
    if segments.len() < 3 {
        return Err(SyncStewardError::Invariant(format!(
            "data path must have form `pond_id=<uuid>/part_id=<uuid>/<file>`: {:?}",
            path
        )));
    }
    let pond_id = segments[0].strip_prefix("pond_id=").ok_or_else(|| {
        SyncStewardError::Invariant(format!(
            "data path must start with `pond_id=<uuid>/`: {:?}",
            path
        ))
    })?;
    if pond_id.is_empty() {
        return Err(SyncStewardError::Invariant(format!(
            "data path has empty pond_id value: {:?}",
            path
        )));
    }
    let part_id = segments[1].strip_prefix("part_id=").ok_or_else(|| {
        SyncStewardError::Invariant(format!(
            "data path must have `part_id=<uuid>/` as second segment: {:?}",
            path
        ))
    })?;
    if part_id.is_empty() {
        return Err(SyncStewardError::Invariant(format!(
            "data path has empty part_id value: {:?}",
            path
        )));
    }
    for seg in &segments[2..] {
        if seg.starts_with("pond_id=") {
            return Err(SyncStewardError::Invariant(format!(
                "data path contains multiple pond_id segments: {:?}",
                path
            )));
        }
        if seg.starts_with("part_id=") {
            return Err(SyncStewardError::Invariant(format!(
                "data path contains multiple part_id segments: {:?}",
                path
            )));
        }
    }
    Ok((pond_id.to_string(), part_id.to_string()))
}

#[async_trait]
impl<'a> RemoteSteward for ShipRemoteSteward<'a> {
    fn store_id(&self) -> Uuid {
        self.ship.control_table().pond_id_uuid()
    }

    fn path(&self) -> &Path {
        self.ship.pond_path()
    }

    async fn data_committed_record(
        &self,
        pond_id: Uuid,
        txn_seq: i64,
    ) -> StewardResult<Option<ControlRecord>> {
        let records = self
            .ship
            .control_table()
            .inner()
            .all_records_for(pond_id)
            .await?;
        Ok(records
            .into_iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq))
    }

    async fn log(&self, limit: Option<usize>) -> StewardResult<Vec<ControlRecord>> {
        let mut all = self.ship.control_table().inner().all_records().await?;
        if let Some(n) = limit
            && all.len() > n
        {
            let start = all.len() - n;
            all = all.split_off(start);
        }
        Ok(all)
    }

    async fn actions_at_version(
        &self,
        pond_id: Uuid,
        version: i64,
    ) -> StewardResult<(Vec<AddPath>, Vec<RemovePath>)> {
        let table = self.ship.data_persistence().table();
        let bytes = table
            .log_store()
            .read_commit_entry(version)
            .await
            .map_err(adapt_err)?
            .ok_or_else(|| {
                SyncStewardError::Invariant(format!(
                    "no Delta commit log entry at version {}",
                    version
                ))
            })?;
        let actions = deltalake::logstore::get_actions(version, &bytes).map_err(adapt_err)?;
        // D5.3: filter Add/Remove file actions by the `pond_id`
        // partition value, so cross-pond Delta commits (D5.7) only
        // contribute their local-pond rows to push bundles.  Today
        // every commit is single-pond and the filter is a no-op,
        // but the contract is established here.
        let pond_id_str = pond_id.to_string();
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for action in actions {
            match action {
                deltalake::kernel::Action::Add(a) => {
                    let matches = matches!(
                        a.partition_values.get("pond_id"),
                        Some(Some(v)) if v == &pond_id_str
                    );
                    if matches {
                        adds.push(AddPath {
                            path: a.path,
                            size: a.size,
                        });
                    }
                }
                deltalake::kernel::Action::Remove(r) => {
                    // Remove.partition_values is Option<HashMap>;
                    // when present, prefer it.  When absent (older
                    // Delta protocol versions or producers that
                    // omit it), fall back to a path-prefix check.
                    let by_partition = r
                        .partition_values
                        .as_ref()
                        .and_then(|pv| pv.get("pond_id"))
                        .and_then(|v| v.as_ref());
                    let matches = match by_partition {
                        Some(v) => v == &pond_id_str,
                        None => r.path.starts_with(&format!("pond_id={}/", pond_id_str)),
                    };
                    if matches {
                        removes.push(RemovePath { path: r.path });
                    }
                }
                _ => {}
            }
        }
        Ok((adds, removes))
    }

    fn read_data_file(&self, rel_path: &str) -> StewardResult<Vec<u8>> {
        let data_path = crate::get_data_path(self.ship.pond_path());
        let abs = data_path.join(rel_path);
        std::fs::read(&abs).map_err(|e| {
            SyncStewardError::Adapter(Box::new(std::io::Error::new(
                e.kind(),
                format!("read_data_file {:?}: {}", abs, e),
            )))
        })
    }

    /// Override the default `pond_id=<uuid>/` check in the trait: duckpond's
    /// tlogfs data table is partitioned by `(pond_id, part_id)` as of D5,
    /// so outbound bundle paths must match the form
    /// `pond_id=<uuid>/part_id=<uuid>/<file>`.  Until cross-pond import
    /// is wired through (D5.7), we additionally require that the path's
    /// `pond_id` matches the local Ship's `pond_id`: every outbound row
    /// belongs to the local pond.
    fn validate_local_data_path(&self, path: &str) -> StewardResult<()> {
        let (pond_id, _part_id) = parse_pond_part_path(path)?;
        let local = self.store_id().to_string();
        if pond_id != local {
            return Err(SyncStewardError::Invariant(format!(
                "outbound data path has foreign pond_id {} (local pond_id {}): {:?}",
                pond_id, local, path
            )));
        }
        Ok(())
    }

    async fn record_post_push_pending(&mut self, txn_seq: i64) -> StewardResult<String> {
        self.ship
            .control_table_mut()
            .record_post_push_pending(txn_seq)
            .await
            .map_err(adapt_err)
    }

    async fn record_post_push_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
    ) -> StewardResult<()> {
        self.ship
            .control_table_mut()
            .record_post_push_completed(txn_seq, txn_id, pending_started_micros)
            .await
            .map_err(adapt_err)
    }

    async fn record_post_push_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
        reason: String,
    ) -> StewardResult<()> {
        self.ship
            .control_table_mut()
            .record_post_push_failed(txn_seq, txn_id, pending_started_micros, reason)
            .await
            .map_err(adapt_err)
    }

    async fn config_get(&self, key: &str) -> StewardResult<Option<String>> {
        self.ship
            .control_table()
            .raw_config_get(key)
            .await
            .map_err(adapt_err)
    }

    async fn config_set(&mut self, key: &str, value: &str) -> StewardResult<()> {
        self.ship
            .control_table_mut()
            .raw_config_set(key, value)
            .await
            .map_err(adapt_err)
    }

    async fn apply_pulled_bundle(&mut self, bundle: PulledBundle) -> StewardResult<()> {
        let PulledBundle {
            pond_id,
            txn_seq,
            commit_kind,
            parent_seq,
            adds,
            removes,
            partition_checksums,
        } = bundle;

        // D5.1 scope: only mirror mode (bundle.pond_id == local pond_id).
        // Cross-pond import (a different bundle.pond_id) becomes valid
        // once the cross-pond UX surface lands (D5.7); for now the
        // partition layout supports it but no caller wires it up.
        let local_pond_id = self.store_id();
        if pond_id != local_pond_id {
            return Err(SyncStewardError::Adapter(Box::new(std::io::Error::other(
                format!(
                    "duckpond apply_pulled_bundle: cross-pond import is not yet wired \
                     through (bundle pond_id {} != local pond_id {}); see docs/d5-resume.md \
                     section D5.7",
                    pond_id, local_pond_id
                ),
            ))));
        }

        // 1. Idempotence: skip if already applied.
        if self
            .data_committed_record(pond_id, txn_seq)
            .await?
            .is_some()
        {
            debug!(
                "apply_pulled_bundle: skipping seq={} (DataCommitted already exists)",
                txn_seq
            );
            return Ok(());
        }

        // 2. Validate every path; reject anything that does not match
        //    the duckpond `pond_id=<uuid>/part_id=<uuid>/<file>` layout.
        //    Also require the path's pond_id to match the bundle's
        //    pond_id (paths cannot smuggle data into a different pond).
        let bundle_pond_id_str = pond_id.to_string();
        let mut add_partition_values: Vec<HashMap<String, Option<String>>> =
            Vec::with_capacity(adds.len());
        for (path, _) in &adds {
            let (path_pond_id, path_part_id) = parse_pond_part_path(path)?;
            if path_pond_id != bundle_pond_id_str {
                return Err(SyncStewardError::Invariant(format!(
                    "bundle pond_id {} does not match path pond_id {}: {:?}",
                    bundle_pond_id_str, path_pond_id, path
                )));
            }
            let pv = HashMap::from([
                ("pond_id".to_string(), Some(path_pond_id)),
                ("part_id".to_string(), Some(path_part_id)),
            ]);
            add_partition_values.push(pv);
        }
        for path in &removes {
            let (path_pond_id, _) = parse_pond_part_path(path)?;
            if path_pond_id != bundle_pond_id_str {
                return Err(SyncStewardError::Invariant(format!(
                    "bundle pond_id {} does not match path pond_id {}: {:?}",
                    bundle_pond_id_str, path_pond_id, path
                )));
            }
        }

        // 3. Write each add's bytes under <pond>/data/.
        let data_path = crate::get_data_path(self.ship.pond_path());
        let mut add_sizes: Vec<i64> = Vec::with_capacity(adds.len());
        for (rel_path, bytes) in &adds {
            let abs = data_path.join(rel_path);
            if let Some(parent) = abs.parent() {
                std::fs::create_dir_all(parent).map_err(adapt_err)?;
            }
            std::fs::write(&abs, bytes).map_err(adapt_err)?;
            add_sizes.push(bytes.len() as i64);
        }

        // 4. Build Delta actions and commit one version on the data
        //    DeltaTable.  Carry `txn_seq` in pond_txn metadata so that
        //    `OpLogPersistence::open_or_create` re-reads it correctly
        //    on the next pond open.
        let now_ms = Utc::now().timestamp_millis();
        let data_change_on_add = matches!(commit_kind, CommitKind::Write);
        let mut actions: Vec<deltalake::kernel::Action> =
            Vec::with_capacity(adds.len() + removes.len());
        for ((path, _bytes), (size, partition_values)) in
            adds.iter().zip(add_sizes.iter().zip(add_partition_values))
        {
            actions.push(deltalake::kernel::Action::Add(deltalake::kernel::Add {
                path: path.clone(),
                partition_values,
                size: *size,
                modification_time: now_ms,
                data_change: data_change_on_add,
                ..Default::default()
            }));
        }
        for path in &removes {
            actions.push(deltalake::kernel::Action::Remove(
                deltalake::kernel::Remove {
                    path: path.clone(),
                    data_change: false,
                    deletion_timestamp: Some(now_ms),
                    ..Default::default()
                },
            ));
        }
        let op = match commit_kind {
            CommitKind::Write => deltalake::protocol::DeltaOperation::Write {
                mode: deltalake::protocol::SaveMode::Append,
                partition_by: Some(vec!["part_id".to_string()]),
                predicate: None,
            },
            CommitKind::Compact => deltalake::protocol::DeltaOperation::Optimize {
                predicate: None,
                target_size: 0,
            },
        };

        // Carry pond_txn metadata so the on-disk Delta commit records
        // the bundle's txn_seq, allowing future opens of this pond to
        // discover the right last_txn_seq from Delta history.
        let txn_meta = tlogfs::PondTxnMetadata {
            txn_seq,
            user: tlogfs::PondUserMetadata::new(vec![
                "internal".to_string(),
                "apply_pulled_bundle".to_string(),
            ]),
            pond_id: pond_id.to_string(),
        };
        let commit_metadata = txn_meta.to_delta_metadata();

        let new_version = {
            use deltalake::kernel::transaction::CommitBuilder;
            let data = self.ship.data_persistence_mut();
            let table = data.table();
            let snapshot_ref: Option<&dyn deltalake::kernel::transaction::TableReference> = table
                .snapshot()
                .ok()
                .map(|s| s as &dyn deltalake::kernel::transaction::TableReference);
            let log_store = table.log_store().clone();
            let _commit = CommitBuilder::default()
                .with_actions(actions)
                .with_app_metadata(commit_metadata)
                .build(snapshot_ref, log_store, op)
                .await
                .map_err(adapt_err)?;
            // Reload table state so subsequent reads see the new commit.
            let mut new_table = data.table().clone();
            new_table.update_state().await.map_err(adapt_err)?;
            let version = new_table.version().unwrap_or(0);
            data.set_table(new_table);
            // Advance the in-memory last_txn_seq so that subsequent
            // writes in this same session use the right next-seq.
            // The on-disk commit metadata already carries `txn_seq`
            // so future opens of this pond will re-read it.
            data.sync_last_txn_seq(txn_seq);
            version
        };

        // 5. Write a mirroring DataCommitted record on the control
        //    table at (pond_id, txn_seq).
        let metadata = DataCommittedMetadata {
            partition_checksums: partition_checksums
                .iter()
                .map(|(k, v)| (k.clone(), sync_steward::ChecksumValue::from(v)))
                .collect(),
            data_delta_version: new_version,
        };
        let metadata_json = serde_json::to_string(&metadata).map_err(adapt_err)?;
        let now_micros = Utc::now().timestamp_micros();
        let parent_opt = if parent_seq == 0 {
            None
        } else {
            Some(parent_seq)
        };
        self.ship
            .control_table_mut()
            .inner_mut()
            .write_record(ControlRecord {
                pond_id,
                record_kind: RecordKind::DataCommitted,
                txn_seq,
                txn_id: new_txn_id(),
                commit_kind: Some(commit_kind),
                parent_seq: parent_opt,
                duration_ms: Some(0),
                ts_micros: now_micros,
                metadata_json,
            })
            .await?;

        // 6. Advance the Ship's in-memory allocator (mirror mode only;
        //    D4 enforces mirror mode above).
        self.ship.sync_last_write_seq(txn_seq);

        Ok(())
    }

    async fn drop_pond_data(&mut self, pond_id: Uuid) -> StewardResult<()> {
        // Drop OplogEntry rows for this pond_id from the data table.
        // Note that pond_id is a regular column on OplogEntry (not a
        // partition column pre-D5), so this DELETE rewrites partitions
        // rather than dropping files wholesale.
        let predicate = format!("pond_id = '{}'", pond_id);
        let data = self.ship.data_persistence_mut();
        let table = data.table().clone();
        let (new_table, _metrics) = table
            .delete()
            .with_predicate(predicate)
            .await
            .map_err(adapt_err)?;
        data.set_table(new_table);

        // Drop matching control records.
        self.ship
            .control_table_mut()
            .inner_mut()
            .drop_pond_records(pond_id)
            .await?;
        Ok(())
    }

    /// Compute live partition checksums for every part_id that has
    /// at least one OplogEntry row under `pond_id` in the current
    /// data table.
    ///
    /// Each OplogEntry row is a leaf; its key is `"<node_id>/<version>"`
    /// (unique within a partition because versions are strictly monotonic
    /// per node), and its value-digest is the BLAKE3 of the row's
    /// `serde_json` serialization.  The serde_json layout uses the
    /// struct's declared field order, so the canonicalization is
    /// stable across producers and across deserialize/reserialize cycles.
    /// Note that this is a logical row hash; it is **not** the parquet
    /// byte representation, so re-compaction (Delta optimize) that
    /// rewrites parquet files without changing row content does not
    /// shift the checksum.
    ///
    /// The Merkle strategy then folds the sorted leaves into the
    /// partition checksum.  All replicated OplogEntry fields contribute
    /// to the hash, including `pond_id`, `part_id`, and `txn_seq`, so
    /// foreign-pond rows (cross-pond import) produce checksums that
    /// match the producer's `compute_live_checksums(pond_id)` for the
    /// same pond_id.
    ///
    /// Implementation builds a fresh `SessionContext` over a clone of
    /// the data table's current snapshot, so the call works regardless
    /// of whether a transaction is active on the underlying `Ship`
    /// (the verify path is read-only and runs outside a write txn).
    async fn compute_live_checksums(&self, pond_id: Uuid) -> StewardResult<PartitionChecksums> {
        let table = self.ship.data_persistence().table().clone();
        let ctx = datafusion::execution::context::SessionContext::new();
        let _previous = ctx
            .register_table("delta_table_live", Arc::new(table))
            .map_err(adapt_err)?;

        let pond_id_str = pond_id.to_string();
        let sql = format!(
            "SELECT * FROM delta_table_live WHERE pond_id = '{}' \
             ORDER BY part_id, node_id, version",
            pond_id_str
        );
        let batches = ctx
            .sql(&sql)
            .await
            .map_err(adapt_err)?
            .collect()
            .await
            .map_err(adapt_err)?;

        // Group rows by part_id; per group, build owned (key,
        // value_blake3) leaves.  We store owned strings/arrays so the
        // borrow-bound `Leaf<'_>` can be constructed via `as_str`/`&`
        // in the final Merkle pass.
        let mut by_partition: HashMap<String, Vec<(String, [u8; 32])>> = HashMap::new();
        for batch in batches {
            let rows: Vec<OplogEntry> =
                serde_arrow::from_record_batch(&batch).map_err(adapt_err)?;
            for row in rows {
                let key = format!("{}/{}", row.node_id, row.version);
                let part = row.part_id.to_string();
                let canonical = serde_json::to_vec(&row).map_err(adapt_err)?;
                let digest = *blake3::hash(&canonical).as_bytes();
                by_partition.entry(part).or_default().push((key, digest));
            }
        }

        let strategy = Merkle::new();
        let mut out = PartitionChecksums::new();
        for (partition, kv) in by_partition {
            let leaves: Vec<Leaf<'_>> = kv
                .iter()
                .map(|(k, v)| Leaf {
                    key: k.as_str(),
                    value_blake3: v,
                })
                .collect();
            let _previous = out.insert(partition, strategy.compute(&leaves));
        }
        Ok(out)
    }
}

/// Outcome of [`push_pending_to_remote`].
#[derive(Debug, Clone, Copy)]
pub struct PushOutcome {
    /// Lower bound (exclusive) that the push started from -- the
    /// `last_pushed_seq` watermark before this call.
    pub previous_last_pushed: i64,
    /// Inclusive upper bound the push targeted (Ship's `last_write_seq`).
    pub upper_seq: i64,
    /// Number of `txn_seq` values for which `Remote::push` returned `Ok`.
    pub pushed: usize,
    /// Number of `txn_seq` values that were skipped because the remote
    /// reported `NoSuchCommit` (e.g. read-only or hole-filling txns
    /// claim a sequence but write no data).
    pub skipped: usize,
}

/// Push every pending `txn_seq` from `last_pushed_seq + 1` (clamped to
/// 2, so the bootstrap `pond_init` is never attempted) through
/// `ship.last_write_seq()` into the remote described by `attachment`.
///
/// This is the shared driver used by both the `pond push` CLI verb
/// ([`crate::ShipRemoteSteward`]) and the post-commit auto-push
/// dispatcher in [`crate::guard`].  Errors from `Remote::push` other
/// than `NoSuchCommit` abort the loop and propagate up.
///
/// # Why skip seq 1?
///
/// The very first transaction recorded by `pond init` writes a
/// `DataCommitted` record with `data_delta_version == 0` (no parquet
/// payload yet -- only the empty pond skeleton).  `Remote::push`
/// rejects that with a `Schema` error.  Until `Remote::push` itself
/// learns to treat `data_delta_version == 0` as a clean skip, the
/// driver-side clamp avoids the error entirely.
pub async fn push_pending_to_remote(
    ship: &mut Ship,
    attachment: &crate::RemoteAttachment,
) -> Result<PushOutcome, sync_remote::RemoteError> {
    use sync_remote::{Remote, RemoteSteward};

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }
    let storage_options = attachment.to_storage_options();
    let mut remote = Remote::open_at_url(&attachment.url, storage_options).await?;

    let upper = ship.last_write_seq();
    let setting_key = format!("last_pushed_seq:{}", remote.url());

    let previous_last_pushed = {
        let adapter = ShipRemoteSteward::new(ship);
        adapter
            .config_get(&setting_key)
            .await
            .ok()
            .flatten()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };

    let start = std::cmp::max(previous_last_pushed + 1, 2);

    let mut outcome = PushOutcome {
        previous_last_pushed,
        upper_seq: upper,
        pushed: 0,
        skipped: 0,
    };

    if start > upper {
        return Ok(outcome);
    }

    for seq in start..=upper {
        let mut adapter = ShipRemoteSteward::new(ship);
        match remote.push(&mut adapter, seq).await {
            Ok(()) => outcome.pushed += 1,
            Err(sync_remote::RemoteError::NoSuchCommit(_)) => outcome.skipped += 1,
            Err(e) => return Err(e),
        }
    }

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    const POND: &str = "01234567-89ab-cdef-0123-456789abcdef";
    const PART: &str = "fedcba98-7654-3210-fedc-ba9876543210";

    #[test]
    fn parse_pond_part_path_happy() {
        let (p, q) = parse_pond_part_path(&format!(
            "pond_id={}/part_id={}/part-00000-xxx-c000.snappy.parquet",
            POND, PART
        ))
        .expect("ok");
        assert_eq!(p, POND);
        assert_eq!(q, PART);
    }

    #[test]
    fn parse_pond_part_path_rejects_absolute() {
        assert!(
            parse_pond_part_path(&format!("/pond_id={}/part_id={}/y.parquet", POND, PART)).is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_dotdot() {
        assert!(
            parse_pond_part_path(&format!("pond_id={}/part_id={}/../y.parquet", POND, PART))
                .is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_dot() {
        assert!(
            parse_pond_part_path(&format!("pond_id={}/part_id={}/./y.parquet", POND, PART))
                .is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_missing_pond_id_prefix() {
        assert!(parse_pond_part_path(&format!("part_id={}/y.parquet", PART)).is_err());
        assert!(
            parse_pond_part_path(&format!("part_id={}/pond_id={}/y.parquet", PART, POND)).is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_missing_part_id_prefix() {
        assert!(parse_pond_part_path(&format!("pond_id={}/y.parquet", POND)).is_err());
        assert!(
            parse_pond_part_path(&format!("pond_id={}/something_else/y.parquet", POND)).is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_empty_pond_id() {
        assert!(parse_pond_part_path(&format!("pond_id=/part_id={}/y.parquet", PART)).is_err());
    }

    #[test]
    fn parse_pond_part_path_rejects_empty_part_id() {
        assert!(parse_pond_part_path(&format!("pond_id={}/part_id=/y.parquet", POND)).is_err());
    }

    #[test]
    fn parse_pond_part_path_rejects_missing_filename() {
        assert!(parse_pond_part_path(&format!("pond_id={}/part_id={}", POND, PART)).is_err());
        assert!(parse_pond_part_path(&format!("pond_id={}/part_id={}/", POND, PART)).is_err());
    }

    #[test]
    fn parse_pond_part_path_rejects_multiple_pond_ids() {
        assert!(
            parse_pond_part_path(&format!(
                "pond_id={}/part_id={}/pond_id={}/y.parquet",
                POND, PART, POND
            ))
            .is_err()
        );
    }

    #[test]
    fn parse_pond_part_path_rejects_multiple_part_ids() {
        assert!(
            parse_pond_part_path(&format!(
                "pond_id={}/part_id={}/part_id={}/y.parquet",
                POND, PART, PART
            ))
            .is_err()
        );
    }
}
