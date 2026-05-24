// SPDX-License-Identifier: Apache-2.0

//! `RemoteSteward`: the slim trait by which [`crate::Remote`] talks to
//! a steward.
//!
//! The trait extracts the surface that `Remote::{push, pull, maintain,
//! restart_pond_from_compact, verify}` needs from a steward.  It does
//! NOT cover construction (`Steward::create`/`open`) nor the data
//! store row schema -- those are concrete to each consumer.  This lets
//! the same `Remote` drive both:
//!
//! - [`sync_steward::Steward`] (the sandbox prototype), where the data
//!   store is a [`sync_store::Store`] with k/v rows.
//! - The duckpond runtime, where the data store is `tlogfs` and the
//!   control table is `steward::ControlTable` (the wrapper over
//!   [`sync_steward::ControlTable`] introduced in remote-redesign D2).
//!
//! Implementations of this trait carry their own concrete error types
//! by mapping into [`sync_steward::StewardError::Adapter`] at the
//! boundary.

use std::path::Path;

use async_trait::async_trait;
use uuid::Uuid;

use sync_steward::{ControlRecord, PartitionChecksums, PulledBundle, Result as StewardResult};
use sync_store::{AddPath, RemovePath};

/// The slim subset of `sync_steward::Steward` that [`crate::Remote`]
/// operations depend on.
///
/// All errors flow through [`sync_steward::StewardError`]; adapter
/// implementations map their native errors via
/// [`sync_steward::StewardError::Adapter`].
#[async_trait]
pub trait RemoteSteward: Send + Sync {
    /// Pond family identity that this steward owns.  In mirror mode
    /// (`Remote::push`), this is the only `pond_id` whose data is
    /// pushed; in cross-pond import mode (`Remote::pull` from a
    /// foreign remote), it differs from the remote's `store_id`.
    fn store_id(&self) -> Uuid;

    /// On-disk path where the steward keeps its data + control state.
    /// Used by `Remote::restart_*_from_compact` for bootstrap output;
    /// most other methods do not need it.
    fn path(&self) -> &Path;

    /// Look up the `DataCommitted` lifecycle record at
    /// `(pond_id, txn_seq)` on the control table.  Returns `None` if
    /// no such record exists.  `Remote::push` uses this to locate the
    /// source Delta version (via `DataCommittedMetadata`).
    async fn data_committed_record(
        &self,
        pond_id: Uuid,
        txn_seq: i64,
    ) -> StewardResult<Option<ControlRecord>>;

    /// Return all control records for the local steward (ordered by
    /// `(txn_seq ASC, ts_micros ASC)`), optionally trimmed to the
    /// most recent `limit`.  `Remote::push` uses this for
    /// idempotent-skip reconciliation of `PostPush*` records.
    async fn log(&self, limit: Option<usize>) -> StewardResult<Vec<ControlRecord>>;

    /// Read the Delta `Add`/`Remove` file actions recorded at
    /// `version` on the underlying data store.  `Remote::push` uses
    /// this to enumerate parquet files to chunk and upload.
    async fn actions_at_version(
        &self,
        version: i64,
    ) -> StewardResult<(Vec<AddPath>, Vec<RemovePath>)>;

    /// Read the raw bytes of a data-store-relative file path (e.g.
    /// one of the [`AddPath::path`] values returned by
    /// [`Self::actions_at_version`]).
    fn read_data_file(&self, rel_path: &str) -> StewardResult<Vec<u8>>;

    /// Defense-in-depth: validate that a Delta `Add`/`Remove` path
    /// returned by [`Self::actions_at_version`] belongs to this
    /// steward's local pond (`store_id`).  Called by `Remote::push`
    /// once per file before bundling.
    ///
    /// The default implementation checks for the sync_store partition
    /// layout `pond_id=<store_id>/`.  Stewards using a different data
    /// layout (e.g., the D4 duckpond adapter, which uses
    /// `part_id=<uuid>/<file>` because tlogfs does not yet have a
    /// `pond_id` partition column) override this method with their
    /// own scheme.
    ///
    /// In normal operation this is guaranteed by construction
    /// (`data_committed_record` is per-pond_id, compact is
    /// per-pond_id, apply_batch stamps the local pond_id), but the
    /// check guards against any future bug that would leak foreign
    /// rows into our own bundle.
    fn validate_local_data_path(&self, path: &str) -> StewardResult<()> {
        let prefix = format!("pond_id={}/", self.store_id());
        if path.contains(&prefix) {
            Ok(())
        } else {
            Err(sync_steward::StewardError::Invariant(format!(
                "path `{}` is not in local pond {}",
                path,
                self.store_id()
            )))
        }
    }

    /// Record a `PostPushPending` lifecycle record and return the
    /// `txn_id` assigned to it (so subsequent `Completed`/`Failed`
    /// records can reuse it).
    async fn record_post_push_pending(&mut self, txn_seq: i64) -> StewardResult<String>;

    /// Record `PostPushCompleted` for `(txn_seq, txn_id)`, computing
    /// duration from `pending_started_micros`.
    async fn record_post_push_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
    ) -> StewardResult<()>;

    /// Record `PostPushFailed` for `(txn_seq, txn_id)` with a reason.
    async fn record_post_push_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
        reason: String,
    ) -> StewardResult<()>;

    /// Read a per-replica `Setting` row by key.  `Remote::push` and
    /// `Remote::pull` use this for `last_pushed_seq:<url>` /
    /// `last_pulled_seq:<url>` watermarks.
    async fn config_get(&self, key: &str) -> StewardResult<Option<String>>;

    /// Write (latest-wins) a per-replica `Setting` row.
    async fn config_set(&mut self, key: &str, value: &str) -> StewardResult<()>;

    /// Apply one bundle pulled from a remote: writes the parquet
    /// bytes to the data store, commits the Add/Remove actions, and
    /// writes a mirroring `DataCommitted` record on the control
    /// table.  `bundle.pond_id` may be `self.store_id()` (mirror) or
    /// foreign (cross-pond import).
    async fn apply_pulled_bundle(&mut self, bundle: PulledBundle) -> StewardResult<()>;

    /// Drop ALL data and lifecycle records belonging to `pond_id`
    /// (used by `restart_pond_from_compact` to wipe a foreign
    /// import's footprint before re-bootstrapping it).
    async fn drop_pond_data(&mut self, pond_id: Uuid) -> StewardResult<()>;

    /// Compute live partition checksums over the data store for
    /// `pond_id`, using the steward's configured checksum strategy.
    /// Used by [`crate::verify::verify_against_remote`] (D3).
    async fn compute_live_checksums(&self, pond_id: Uuid) -> StewardResult<PartitionChecksums>;
}

// -- blanket impl: sync_steward::Steward implements RemoteSteward --

#[async_trait]
impl RemoteSteward for sync_steward::Steward {
    fn store_id(&self) -> Uuid {
        sync_steward::Steward::store_id(self)
    }

    fn path(&self) -> &Path {
        sync_steward::Steward::path(self)
    }

    async fn data_committed_record(
        &self,
        pond_id: Uuid,
        txn_seq: i64,
    ) -> StewardResult<Option<ControlRecord>> {
        sync_steward::Steward::data_committed_record(self, pond_id, txn_seq).await
    }

    async fn log(&self, limit: Option<usize>) -> StewardResult<Vec<ControlRecord>> {
        sync_steward::Steward::log(self, limit).await
    }

    async fn actions_at_version(
        &self,
        version: i64,
    ) -> StewardResult<(Vec<AddPath>, Vec<RemovePath>)> {
        sync_steward::Steward::actions_at_version(self, version).await
    }

    fn read_data_file(&self, rel_path: &str) -> StewardResult<Vec<u8>> {
        sync_steward::Steward::read_data_file(self, rel_path)
    }

    async fn record_post_push_pending(&mut self, txn_seq: i64) -> StewardResult<String> {
        sync_steward::Steward::record_post_push_pending(self, txn_seq).await
    }

    async fn record_post_push_completed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
    ) -> StewardResult<()> {
        sync_steward::Steward::record_post_push_completed(
            self,
            txn_seq,
            txn_id,
            pending_started_micros,
        )
        .await
    }

    async fn record_post_push_failed(
        &mut self,
        txn_seq: i64,
        txn_id: String,
        pending_started_micros: i64,
        reason: String,
    ) -> StewardResult<()> {
        sync_steward::Steward::record_post_push_failed(
            self,
            txn_seq,
            txn_id,
            pending_started_micros,
            reason,
        )
        .await
    }

    async fn config_get(&self, key: &str) -> StewardResult<Option<String>> {
        sync_steward::Steward::config_get(self, key).await
    }

    async fn config_set(&mut self, key: &str, value: &str) -> StewardResult<()> {
        sync_steward::Steward::config_set(self, key, value).await
    }

    async fn apply_pulled_bundle(&mut self, bundle: PulledBundle) -> StewardResult<()> {
        sync_steward::Steward::apply_pulled_bundle(self, bundle).await
    }

    async fn drop_pond_data(&mut self, pond_id: Uuid) -> StewardResult<()> {
        sync_steward::Steward::drop_pond_data(self, pond_id).await
    }

    async fn compute_live_checksums(&self, pond_id: Uuid) -> StewardResult<PartitionChecksums> {
        sync_steward::Steward::compute_live_checksums(self, pond_id).await
    }
}
