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
//! - For `apply_pulled_bundle`: validates that the bundle's `pond_id`
//!   matches `Ship`'s identity (mirror mode), validates each path is
//!   of the duckpond layout `part_id=<uuid>/<file>.parquet`, writes
//!   the parquet bytes under `<pond>/data/`, commits Add/Remove
//!   actions to the data DeltaTable, writes a mirroring
//!   `DataCommitted` record on the control table, and bumps
//!   `last_write_seq`.
//!
//! ## What this adapter does NOT do (yet)
//!
//! - **Cross-pond import** (`bundle.pond_id != ship.pond_id`) is
//!   rejected as `StewardError::Adapter`.  This is the D4 scope
//!   restriction documented in `docs/d4-resumption.md` gotcha #7; D5
//!   adds the tlogfs `(pond_id, part_id)` partitioning that makes
//!   cross-pond import viable.
//! - `compute_live_checksums` returns `unimplemented!("D3")` -- the
//!   duckpond row schema differs from `sync_store`'s k/v schema, so
//!   the checksum strategy needs a tlogfs-aware re-implementation
//!   (D3 scope, after D4).
//!
//! ## Path layout
//!
//! Duckpond tlogfs paths returned by `actions_at_version` look like:
//!
//! ```text
//! part_id=<uuid>/part-00000-<uuid>-c000.snappy.parquet
//! ```
//!
//! These differ from sync_store paths
//! (`pond_id=<uuid>/partition_key=<value>/...`).  The adapter parses
//! the leading `part_id=<uuid>/` segment via [`parse_part_id_path`]
//! and rejects anything else.

use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use chrono::Utc;
use log::debug;
use uuid::Uuid;

use sync_remote::RemoteSteward;
use sync_steward::{
    CommitKind, ControlRecord, DataCommittedMetadata, PartitionChecksums, PulledBundle, RecordKind,
    Result as StewardResult, StewardError as SyncStewardError, new_txn_id,
};
use sync_store::{AddPath, RemovePath};

use crate::Ship;

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
/// `part_id=<uuid>/<filename>`.  Returns the `part_id` value on
/// success.  Rejects:
///
/// - Absolute paths.
/// - Paths containing `..` segments.
/// - Paths that do not start with `part_id=<uuid>/`.
/// - Multiple `part_id=` segments.
fn parse_part_id_path(path: &str) -> Result<String, SyncStewardError> {
    if path.starts_with('/') {
        return Err(SyncStewardError::Invariant(format!(
            "data path must be relative: {:?}",
            path
        )));
    }
    let mut segments = path.split('/');
    let first = segments.next().ok_or_else(|| {
        SyncStewardError::Invariant(format!("data path has no segments: {:?}", path))
    })?;
    let rest: Vec<&str> = segments.collect();
    for seg in std::iter::once(first).chain(rest.iter().copied()) {
        if seg == ".." || seg == "." || seg.is_empty() {
            return Err(SyncStewardError::Invariant(format!(
                "data path contains invalid segment {:?}: {:?}",
                seg, path
            )));
        }
    }
    let part_id = if let Some(rest) = first.strip_prefix("part_id=") {
        if rest.is_empty() {
            return Err(SyncStewardError::Invariant(format!(
                "data path has empty part_id value: {:?}",
                path
            )));
        }
        rest.to_string()
    } else {
        return Err(SyncStewardError::Invariant(format!(
            "data path must start with `part_id=<uuid>/`: {:?}",
            path
        )));
    };
    if rest.is_empty() {
        return Err(SyncStewardError::Invariant(format!(
            "data path must have a filename after part_id: {:?}",
            path
        )));
    }
    for seg in &rest {
        if seg.starts_with("part_id=") {
            return Err(SyncStewardError::Invariant(format!(
                "data path contains multiple part_id segments: {:?}",
                path
            )));
        }
    }
    Ok(part_id)
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
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for action in actions {
            match action {
                deltalake::kernel::Action::Add(a) => adds.push(AddPath {
                    path: a.path,
                    size: a.size,
                }),
                deltalake::kernel::Action::Remove(r) => removes.push(RemovePath { path: r.path }),
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

    /// Override the default `pond_id=<uuid>/` check: duckpond's tlogfs
    /// data table is partitioned by `part_id` only (not `pond_id`), so
    /// outbound bundle paths must match the form
    /// `part_id=<uuid>/<file>`.  Since a single duckpond data table
    /// only ever contains rows belonging to one pond, the
    /// `part_id` shape itself is sufficient to assert "local pond";
    /// no per-pond_id check is possible until D5 adds a `pond_id`
    /// partition column.
    fn validate_local_data_path(&self, path: &str) -> StewardResult<()> {
        parse_part_id_path(path).map(|_| ())
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

        // D4 scope: only duckpond <-> duckpond mirror mode.  Cross-pond
        // import requires D5's (pond_id, part_id) partition layout.
        let local_pond_id = self.store_id();
        if pond_id != local_pond_id {
            return Err(SyncStewardError::Adapter(Box::new(std::io::Error::other(
                format!(
                    "duckpond apply_pulled_bundle: cross-pond import is not supported \
                     in D4 (bundle pond_id {} != local pond_id {}); see docs/d4-resumption.md \
                     gotcha #7",
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
        //    the duckpond `part_id=<uuid>/<file>` layout.
        let mut add_partition_values: Vec<HashMap<String, Option<String>>> =
            Vec::with_capacity(adds.len());
        for (path, _) in &adds {
            let part_id = parse_part_id_path(path)?;
            let pv = HashMap::from([("part_id".to_string(), Some(part_id))]);
            add_partition_values.push(pv);
        }
        for path in &removes {
            let _ = parse_part_id_path(path)?;
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

    async fn compute_live_checksums(&self, _pond_id: Uuid) -> StewardResult<PartitionChecksums> {
        // Deferred to D3 (pond verify).  The duckpond OplogEntry row
        // schema differs from sync_store's k/v schema, so the checksum
        // strategy needs a tlogfs-aware re-implementation; see
        // docs/d4-resumption.md `compute_live_checksums` note.
        Err(SyncStewardError::Adapter(Box::new(std::io::Error::other(
            "ShipRemoteSteward::compute_live_checksums is deferred to D3 \
             (pond verify); duckpond's row schema requires a tlogfs-aware \
             checksum strategy",
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_part_id_path_happy() {
        let p = parse_part_id_path(
            "part_id=01234567-89ab-cdef-0123-456789abcdef/part-00000-xxx-c000.snappy.parquet",
        )
        .expect("ok");
        assert_eq!(p, "01234567-89ab-cdef-0123-456789abcdef");
    }

    #[test]
    fn parse_part_id_path_rejects_absolute() {
        assert!(parse_part_id_path("/part_id=x/y.parquet").is_err());
    }

    #[test]
    fn parse_part_id_path_rejects_dotdot() {
        assert!(parse_part_id_path("part_id=x/../y.parquet").is_err());
    }

    #[test]
    fn parse_part_id_path_rejects_missing_prefix() {
        assert!(parse_part_id_path("pond_id=x/part_id=y/z.parquet").is_err());
    }

    #[test]
    fn parse_part_id_path_rejects_empty_part_id() {
        assert!(parse_part_id_path("part_id=/y.parquet").is_err());
    }

    #[test]
    fn parse_part_id_path_rejects_multiple_part_ids() {
        assert!(parse_part_id_path("part_id=a/part_id=b/y.parquet").is_err());
    }
}
