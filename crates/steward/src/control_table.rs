// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Control Table - thin wrapper around [`sync_steward::ControlTable`].
//!
//! Built during the remote-redesign D2 substantive phase.  The rich
//! append-only `TransactionRecord` schema previously living here has been
//! replaced by the lean `sync_steward` schema (`pond_id`, `record_kind`,
//! `txn_seq`, `txn_id`, `commit_kind`, `parent_seq`, `duration_ms`,
//! `ts_micros`, `metadata_json`).  This wrapper:
//!
//! - Preserves the public API surface (`record_*`, cached config getters,
//!   `session_context`, etc.) that `Ship`, `StewardTransactionGuard` and
//!   `cmd/*` rely on -- callers do not need to change shape.
//! - Stores pond identity under [`BOOTSTRAP_POND_ID`] (`Uuid::nil()`) as
//!   `sync_steward` config rows, per the D2 plan; D5 will migrate
//!   identity to the bootstrap row of the data Delta table.
//! - Stores factory modes and per-instance settings under the local
//!   `pond_id` namespace with the `"factory_mode:"` and `"setting:"`
//!   key prefixes respectively.
//! - Drops the `record_import_*` / `update_import_watermark` /
//!   `query_import_partitions` methods entirely; D5 brings cross-pond
//!   import back via the row-level `pond_id` partitioning of tlogfs.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};
use sync_steward::{
    CommitKind, ControlRecord, ControlTable as InnerControlTable, DataCommittedMetadata,
    RecordKind, new_txn_id,
};
use uuid::Uuid as StdUuid;

use crate::StewardError;

// Re-export the pond metadata types so callers can `use steward::ControlTable`
// alongside `use steward::PondMetadata` without a separate tlogfs import.
pub use tlogfs::{PondMetadata, PondTxnMetadata};

/// Bootstrap pond_id used as the namespace for instance-wide config that
/// is not bound to any particular pond (today: pond identity).  Matches
/// the sandbox prototype convention adopted by `sync_steward`.
const BOOTSTRAP_POND_ID: StdUuid = StdUuid::nil();

const KEY_STORE_ID: &str = "store_id";
const KEY_BIRTH_TIMESTAMP: &str = "birth_timestamp";
const KEY_BIRTH_HOSTNAME: &str = "birth_hostname";
const KEY_BIRTH_USERNAME: &str = "birth_username";

const FACTORY_MODE_PREFIX: &str = "factory_mode:";
const SETTING_PREFIX: &str = "setting:";

/// Lifecycle classification preserved for source-level compatibility with
/// pre-D2 callers.  Each variant maps onto the lean schema's
/// [`RecordKind`] (+ optional [`CommitKind`]) at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    Read,
    Write,
    PostCommit,
}

/// JSON payload stored in `metadata_json` for `PostPush*` records,
/// carrying the post-commit task attributes that the lean schema no
/// longer has dedicated columns for.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PostCommitMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    factory_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error_message: Option<String>,
}

/// Thin wrapper over [`sync_steward::ControlTable`] exposing the
/// duckpond-flavored `record_*` API and caching pond identity, factory
/// modes and settings on top of the lean `config_*` primitives.
pub struct ControlTable {
    inner: InnerControlTable,
    pond_metadata: PondMetadata,
    factory_modes: HashMap<String, String>,
    settings: HashMap<String, String>,
}

impl ControlTable {
    /// Create a new control table at `path` and persist `pond_metadata`
    /// under the bootstrap namespace.  Errors if the table already exists.
    pub async fn create<P: AsRef<Path>>(
        path: P,
        pond_metadata: &PondMetadata,
    ) -> Result<Self, StewardError> {
        let mut inner = InnerControlTable::create(path.as_ref())
            .await
            .map_err(map_err)?;
        seed_pond_metadata(&mut inner, pond_metadata).await?;
        Ok(Self {
            inner,
            pond_metadata: pond_metadata.clone(),
            factory_modes: HashMap::new(),
            settings: HashMap::new(),
        })
    }

    /// Open an existing control table at `path`.  Reads back pond
    /// identity, factory modes and settings into the in-memory caches.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, StewardError> {
        let inner = InnerControlTable::open(path.as_ref())
            .await
            .map_err(map_err)?;
        let pond_metadata = load_pond_metadata(&inner).await?;
        let pond_id_uuid = pond_id_to_std(&pond_metadata.pond_id);
        let local_config = inner.config_list(pond_id_uuid).await.map_err(map_err)?;
        let (factory_modes, settings) = split_config(local_config);
        Ok(Self {
            inner,
            pond_metadata,
            factory_modes,
            settings,
        })
    }

    /// Convenience: create or open based on `create_new`.
    pub async fn open_or_create<P: AsRef<Path>>(
        path: P,
        create_new: bool,
        pond_metadata: Option<&PondMetadata>,
    ) -> Result<Self, StewardError> {
        if create_new {
            let metadata = pond_metadata.ok_or_else(|| {
                StewardError::ControlTable(
                    "pond_metadata is required when creating a new control table".to_string(),
                )
            })?;
            Self::create(path, metadata).await
        } else {
            Self::open(path).await
        }
    }

    // -------- Accessors --------

    /// Underlying [`DeltaTable`] handle for maintenance operations.
    #[must_use]
    pub fn table(&self) -> &DeltaTable {
        self.inner.delta_table()
    }

    /// Replace the underlying [`DeltaTable`] handle.  Called by
    /// [`crate::maintenance::maintain_table`] after vacuum/optimize
    /// produces a new table reference.  Best-effort: a failure to
    /// re-register only affects subsequent SQL queries on the cached
    /// session context.
    pub fn set_table(&mut self, table: DeltaTable) {
        if let Err(e) = self.inner.set_delta_table(table) {
            log::warn!("control_table set_table: re-register failed: {}", e);
        }
    }

    /// Shared DataFusion session context with the control table
    /// registered under [`sync_steward::TABLE_NAME`] (`"control"`).
    /// JSON helper functions are registered so callers can query
    /// `metadata_json` via `json_get_str` and friends.  A fresh
    /// context is built on every call so callers always see the
    /// latest Delta version of the table.
    #[must_use]
    pub fn session_context(&self) -> Arc<SessionContext> {
        let mut ctx = SessionContext::new();
        if let Err(e) = datafusion_functions_json::register_all(&mut ctx) {
            log::warn!(
                "control_table session_context: register JSON functions failed: {}",
                e
            );
        }
        if let Err(e) = ctx.register_table(
            sync_steward::TABLE_NAME,
            Arc::new(self.inner.delta_table().clone()),
        ) {
            log::warn!(
                "control_table session_context: register table failed: {}",
                e
            );
        }
        Arc::new(ctx)
    }

    /// Cached pond identity (immutable for the lifetime of the pond).
    #[must_use]
    pub fn pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Alias for [`Self::pond_metadata`].  Preserved for caller
    /// compatibility with the pre-D2 API.
    #[must_use]
    pub fn get_pond_metadata(&self) -> &PondMetadata {
        &self.pond_metadata
    }

    /// Replace cached pond metadata and persist it under the bootstrap
    /// namespace.  Used by restoration flows where the source pond's
    /// identity must be preserved on a fresh replica.
    pub async fn set_pond_metadata(&mut self, metadata: &PondMetadata) -> Result<(), StewardError> {
        seed_pond_metadata(&mut self.inner, metadata).await?;
        self.pond_metadata = metadata.clone();
        Ok(())
    }

    /// Cached factory execution modes (name -> mode string).
    #[must_use]
    pub fn factory_modes(&self) -> &HashMap<String, String> {
        &self.factory_modes
    }

    /// Cached factory execution mode for `name`, if set.
    #[must_use]
    pub fn get_factory_mode(&self, name: &str) -> Option<String> {
        self.factory_modes.get(name).cloned()
    }

    /// Set factory execution mode and persist under the local pond_id
    /// with the `"factory_mode:"` prefix.
    pub async fn set_factory_mode(&mut self, name: &str, mode: &str) -> Result<(), StewardError> {
        let key = format!("{}{}", FACTORY_MODE_PREFIX, name);
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .config_set(pond_id, &key, mode)
            .await
            .map_err(map_err)?;
        let _previous = self
            .factory_modes
            .insert(name.to_string(), mode.to_string());
        Ok(())
    }

    /// Cached per-instance settings.
    #[must_use]
    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    /// Cached value of setting `key`, if set.
    #[must_use]
    pub fn get_setting(&self, key: &str) -> Option<String> {
        self.settings.get(key).cloned()
    }

    /// Set per-instance setting and persist under the local pond_id with
    /// the `"setting:"` prefix.
    pub async fn set_setting(&mut self, key: &str, value: &str) -> Result<(), StewardError> {
        let inner_key = format!("{}{}", SETTING_PREFIX, key);
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .config_set(pond_id, &inner_key, value)
            .await
            .map_err(map_err)?;
        let _previous = self.settings.insert(key.to_string(), value.to_string());
        Ok(())
    }

    // -------- Lifecycle records --------

    /// Record transaction `Begin`.  `_based_on_seq` and
    /// `_transaction_type` are accepted for source-level API
    /// compatibility but no longer persisted (the lean schema treats
    /// every begin uniformly).
    pub async fn record_begin(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _based_on_seq: Option<i64>,
        _transaction_type: TransactionType,
    ) -> Result<(), StewardError> {
        let record = self.base_record(RecordKind::Begin, txn_meta);
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Record successful data filesystem commit.
    pub async fn record_data_committed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        data_fs_version: i64,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let metadata = DataCommittedMetadata {
            partition_checksums: HashMap::new(),
            data_delta_version: data_fs_version,
        };
        let metadata_json = serde_json::to_string(&metadata).unwrap_or_else(|_| "{}".into());
        let mut record = self.base_record(RecordKind::DataCommitted, txn_meta);
        record.commit_kind = Some(CommitKind::Write);
        record.duration_ms = Some(duration_ms);
        record.metadata_json = metadata_json;
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Record transaction failure.
    pub async fn record_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let metadata_json = serde_json::to_string(&serde_json::json!({
            "reason": error_message,
        }))
        .unwrap_or_else(|_| "{}".into());
        let mut record = self.base_record(RecordKind::Failed, txn_meta);
        record.duration_ms = Some(duration_ms);
        record.metadata_json = metadata_json;
        self.inner.write_record(record).await.map_err(map_err)
    }

    /// Record completed read transaction (or no-op write).
    pub async fn record_completed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        _transaction_type: TransactionType,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let mut record = self.base_record(RecordKind::Completed, txn_meta);
        record.duration_ms = Some(duration_ms);
        self.inner.write_record(record).await.map_err(map_err)
    }

    // -------- Post-commit records --------

    /// Record that a post-commit factory task has been queued.
    pub async fn record_post_commit_pending(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        factory_name: String,
        config_path: String,
    ) -> Result<(), StewardError> {
        let metadata = PostCommitMetadata {
            execution_seq: Some(execution_seq),
            factory_name: Some(factory_name),
            config_path: Some(config_path),
            error_message: None,
        };
        self.write_post_commit(RecordKind::PostPushPending, txn_meta, &metadata, None)
            .await
    }

    /// Record that a post-commit factory task has started executing.
    pub async fn record_post_commit_started(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
    ) -> Result<(), StewardError> {
        let metadata = PostCommitMetadata {
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
            error_message: None,
        };
        self.write_post_commit(RecordKind::PostPushStarted, txn_meta, &metadata, None)
            .await
    }

    /// Record successful post-commit factory completion.
    pub async fn record_post_commit_completed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let metadata = PostCommitMetadata {
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
            error_message: None,
        };
        self.write_post_commit(
            RecordKind::PostPushCompleted,
            txn_meta,
            &metadata,
            Some(duration_ms),
        )
        .await
    }

    /// Record failed post-commit factory execution.
    pub async fn record_post_commit_failed(
        &mut self,
        txn_meta: &PondTxnMetadata,
        execution_seq: i64,
        error_message: String,
        duration_ms: i64,
    ) -> Result<(), StewardError> {
        let metadata = PostCommitMetadata {
            execution_seq: Some(execution_seq),
            factory_name: None,
            config_path: None,
            error_message: Some(error_message),
        };
        self.write_post_commit(
            RecordKind::PostPushFailed,
            txn_meta,
            &metadata,
            Some(duration_ms),
        )
        .await
    }

    async fn write_post_commit(
        &mut self,
        kind: RecordKind,
        txn_meta: &PondTxnMetadata,
        metadata: &PostCommitMetadata,
        duration_ms: Option<i64>,
    ) -> Result<(), StewardError> {
        let metadata_json = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".into());
        let record = ControlRecord {
            pond_id: pond_id_to_std(&self.pond_metadata.pond_id),
            record_kind: kind,
            txn_seq: txn_meta.txn_seq,
            // Each post-commit record gets its own UUID so that multiple
            // factories sharing one parent transaction do not collide on
            // (pond_id, txn_seq, txn_id) when read back.
            txn_id: new_txn_id(),
            commit_kind: None,
            parent_seq: Some(txn_meta.txn_seq),
            duration_ms,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json,
        };
        self.inner.write_record(record).await.map_err(map_err)
    }

    // -------- Queries --------

    /// Highest committed write sequence number, or 0 if none.  Replaces
    /// the pre-D2 implementation that scanned `transaction_type='write'`
    /// rows directly.
    pub async fn get_last_write_sequence(&self) -> Result<i64, StewardError> {
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        self.inner
            .last_committed_seq(pond_id)
            .await
            .map_err(map_err)
    }

    /// Incomplete transactions for the local pond_id.  The returned
    /// [`PondTxnMetadata`] carries the original `txn_seq` and `txn_id`
    /// but an empty `args` vector -- the lean schema does not persist
    /// CLI arguments (per the D2 plan).  The trailing `i64` is always 0
    /// (no data_fs_version is recorded for incomplete transactions
    /// because they never reached `DataCommitted`).
    pub async fn find_incomplete_transactions(
        &self,
    ) -> Result<Vec<(PondTxnMetadata, i64)>, StewardError> {
        let pond_id = pond_id_to_std(&self.pond_metadata.pond_id);
        let records = self
            .inner
            .incomplete_transactions(pond_id)
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let txn_id = uuid7::Uuid::from_str(&record.txn_id).map_err(|e| {
                StewardError::ControlTable(format!("Invalid txn_id UUID in control table: {}", e))
            })?;
            let user = tlogfs::PondUserMetadata {
                txn_id,
                args: Vec::new(),
            };
            let txn_meta = PondTxnMetadata::new(record.txn_seq, user);
            out.push((txn_meta, 0));
        }
        Ok(out)
    }

    /// Print the pond identity banner using cached metadata.
    #[allow(clippy::print_stdout)]
    pub fn print_banner(&self) {
        println!();
        pond_metadata_banner(&self.pond_metadata);
        println!();
    }

    // -------- internals --------

    fn base_record(&self, kind: RecordKind, txn_meta: &PondTxnMetadata) -> ControlRecord {
        ControlRecord {
            pond_id: pond_id_to_std(&self.pond_metadata.pond_id),
            record_kind: kind,
            txn_seq: txn_meta.txn_seq,
            txn_id: txn_meta.user.txn_id.to_string(),
            commit_kind: None,
            parent_seq: None,
            duration_ms: None,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json: "{}".to_string(),
        }
    }
}

// ---- helpers ----

fn pond_id_to_std(id: &uuid7::Uuid) -> StdUuid {
    StdUuid::from_bytes(*id.as_bytes())
}

fn map_err(e: sync_steward::StewardError) -> StewardError {
    StewardError::ControlTable(format!("{}", e))
}

async fn seed_pond_metadata(
    inner: &mut InnerControlTable,
    m: &PondMetadata,
) -> Result<(), StewardError> {
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_STORE_ID, &m.pond_id.to_string())
        .await
        .map_err(map_err)?;
    inner
        .config_set(
            BOOTSTRAP_POND_ID,
            KEY_BIRTH_TIMESTAMP,
            &m.birth_timestamp.to_string(),
        )
        .await
        .map_err(map_err)?;
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_BIRTH_HOSTNAME, &m.birth_hostname)
        .await
        .map_err(map_err)?;
    inner
        .config_set(BOOTSTRAP_POND_ID, KEY_BIRTH_USERNAME, &m.birth_username)
        .await
        .map_err(map_err)?;
    Ok(())
}

async fn load_pond_metadata(inner: &InnerControlTable) -> Result<PondMetadata, StewardError> {
    let bootstrap = inner
        .config_list(BOOTSTRAP_POND_ID)
        .await
        .map_err(map_err)?;
    let pond_id_str = bootstrap.get(KEY_STORE_ID).ok_or_else(|| {
        StewardError::ControlTable(format!(
            "Control table is missing pond identity (no '{}' setting under bootstrap pond_id)",
            KEY_STORE_ID
        ))
    })?;
    let pond_id = pond_id_str.parse::<uuid7::Uuid>().map_err(|e| {
        StewardError::ControlTable(format!(
            "Invalid '{}' value in control table: {}",
            KEY_STORE_ID, e
        ))
    })?;
    let birth_timestamp = bootstrap
        .get(KEY_BIRTH_TIMESTAMP)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let birth_hostname = bootstrap
        .get(KEY_BIRTH_HOSTNAME)
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let birth_username = bootstrap
        .get(KEY_BIRTH_USERNAME)
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    Ok(PondMetadata {
        pond_id,
        birth_timestamp,
        birth_hostname,
        birth_username,
    })
}

fn split_config(
    all: HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut modes = HashMap::new();
    let mut settings = HashMap::new();
    for (k, v) in all {
        if let Some(name) = k.strip_prefix(FACTORY_MODE_PREFIX) {
            let _previous = modes.insert(name.to_string(), v);
        } else if let Some(name) = k.strip_prefix(SETTING_PREFIX) {
            let _previous = settings.insert(name.to_string(), v);
        }
    }
    (modes, settings)
}

/// Render the pond identity banner to stdout.
#[allow(clippy::print_stdout)]
pub fn pond_metadata_banner(data: &PondMetadata) {
    let datetime = DateTime::from_timestamp(
        data.birth_timestamp / 1_000_000,
        ((data.birth_timestamp % 1_000_000) * 1000) as u32,
    )
    .unwrap_or_else(Utc::now);
    let created_str = datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    let left = vec![
        format!("Pond {}", data.pond_id),
        format!("Created {}", created_str),
    ];

    let right = vec![data.birth_username.clone(), data.birth_hostname.clone()];

    println!(
        "{}",
        utilities::banner::format_banner_from_iters(None, left, right)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn create_then_open_round_trips_identity_and_modes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");

        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();
        assert_eq!(table.pond_metadata().pond_id, metadata.pond_id);

        table.set_factory_mode("remote", "push").await.unwrap();
        assert_eq!(table.get_factory_mode("remote"), Some("push".to_string()));

        let table2 = ControlTable::open(&path).await.unwrap();
        assert_eq!(table2.pond_metadata().pond_id, metadata.pond_id);
        assert_eq!(
            table2.pond_metadata().birth_username,
            metadata.birth_username
        );
        assert_eq!(table2.get_factory_mode("remote"), Some("push".to_string()));
    }

    #[tokio::test]
    async fn settings_roundtrip_via_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();

        table
            .set_setting("hostmount_path", "/mnt/data")
            .await
            .unwrap();
        assert_eq!(
            table.get_setting("hostmount_path"),
            Some("/mnt/data".to_string())
        );

        let table2 = ControlTable::open(&path).await.unwrap();
        assert_eq!(
            table2.get_setting("hostmount_path"),
            Some("/mnt/data".to_string())
        );
        // Settings must not leak into factory_modes via the shared
        // config namespace.
        assert!(table2.get_factory_mode("hostmount_path").is_none());
    }

    #[tokio::test]
    async fn last_write_sequence_tracks_committed_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 0);

        let user = tlogfs::PondUserMetadata::new(vec!["init".to_string()]);
        let txn_meta = PondTxnMetadata::new(1, user);
        table
            .record_begin(&txn_meta, None, TransactionType::Write)
            .await
            .unwrap();
        // Begin alone doesn't bump committed seq.
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 0);

        table
            .record_data_committed(&txn_meta, TransactionType::Write, 7, 12)
            .await
            .unwrap();
        assert_eq!(table.get_last_write_sequence().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn incomplete_transactions_lists_begin_without_terminal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control");
        let metadata = PondMetadata::default();
        let mut table = ControlTable::create(&path, &metadata).await.unwrap();

        let user = tlogfs::PondUserMetadata::new(vec!["wip".to_string()]);
        let txn_meta = PondTxnMetadata::new(2, user);
        table
            .record_begin(&txn_meta, None, TransactionType::Write)
            .await
            .unwrap();

        let incomplete = table.find_incomplete_transactions().await.unwrap();
        assert_eq!(incomplete.len(), 1, "begin-without-terminal must appear");
        assert_eq!(incomplete[0].0.txn_seq, 2);
        assert_eq!(incomplete[0].1, 0, "no data_fs_version for incomplete txn");

        // Completing it removes it from the list.
        table
            .record_completed(&txn_meta, TransactionType::Write, 0)
            .await
            .unwrap();
        let still = table.find_incomplete_transactions().await.unwrap();
        assert!(still.is_empty(), "completed txn must not appear incomplete");
    }
}
