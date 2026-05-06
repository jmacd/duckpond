// SPDX-License-Identifier: Apache-2.0

//! Control table: lifecycle records, settings, partition checksums.
//!
//! The control table is a Delta Lake table peer of the data store.  It
//! holds two kinds of payloads in the same row schema:
//!
//! 1. **Lifecycle records**: one row per state transition of a
//!    transaction (Begin, DataCommitted, Failed, Completed,
//!    PostPushPending/Started/Completed/Failed).
//! 2. **Settings**: key/value configuration written by `Steward::config_set`.
//!    Stored as a `Setting` record kind with the key in `txn_id` and
//!    the value in `metadata_json`.  Latest write per key wins.
//!
//! The schema is intentionally narrow.  Ad-hoc fields go through
//! `metadata_json`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use chrono::Utc;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTable;
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use deltalake::protocol::SaveMode;
use serde::{Deserialize, Serialize};
use sync_store::checksum::{Checksum, ChecksumKind};
use url::Url;
use uuid::Uuid;

use crate::error::{Result, StewardError};

/// Lifecycle record kinds.  Stored as a string in the control table for
/// readability and forward-compatibility (new variants don't break old
/// readers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// Transaction has been allocated a `txn_seq` but no data is committed yet.
    Begin,
    /// A data write or compact transaction committed successfully.
    DataCommitted,
    /// Transaction failed before producing data.
    Failed,
    /// A read transaction or no-op write transaction completed.
    Completed,
    /// A post-commit push has been scheduled.
    PostPushPending,
    /// Push started.
    PostPushStarted,
    /// Push completed successfully.
    PostPushCompleted,
    /// Push failed.
    PostPushFailed,
    /// A configuration key/value setting (latest write wins).
    Setting,
}

impl RecordKind {
    /// Stable serialized name.
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordKind::Begin => "begin",
            RecordKind::DataCommitted => "data_committed",
            RecordKind::Failed => "failed",
            RecordKind::Completed => "completed",
            RecordKind::PostPushPending => "post_push_pending",
            RecordKind::PostPushStarted => "post_push_started",
            RecordKind::PostPushCompleted => "post_push_completed",
            RecordKind::PostPushFailed => "post_push_failed",
            RecordKind::Setting => "setting",
        }
    }
}

/// A transaction is either a normal write or a compaction.  Recorded on
/// `DataCommitted` records to disambiguate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitKind {
    /// User data write.
    Write,
    /// Delta-level compaction; logical content unchanged.
    Compact,
}

impl CommitKind {
    /// Stable serialized name.
    pub fn as_str(&self) -> &'static str {
        match self {
            CommitKind::Write => "write",
            CommitKind::Compact => "compact",
        }
    }
}

/// One control-table row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlRecord {
    /// Pond identity that owns this record.  For local lifecycle and
    /// PostPush records, this is the local pond's id.  For records
    /// mirroring a foreign pond (i.e., a `DataCommitted` written by
    /// `apply_pulled_bundle`), this is the foreign pond's id.  For
    /// `Setting` records (per-replica config), this is the local pond
    /// instance's id.  Stored as the canonical lowercase-hyphenated
    /// UUID string.
    pub pond_id: Uuid,
    /// Lifecycle kind.
    pub record_kind: RecordKind,
    /// Transaction sequence (or `0` for [`RecordKind::Setting`]).
    /// Per-pond_id namespaced -- two different pond_ids may share a
    /// `txn_seq` value without collision.
    pub txn_seq: i64,
    /// Transaction id (UUID v4) for lifecycle records, or the setting
    /// key for `Setting` records.
    pub txn_id: String,
    /// Set on `DataCommitted`.
    pub commit_kind: Option<CommitKind>,
    /// `None` means root (first commit).  Set on `DataCommitted`.
    pub parent_seq: Option<i64>,
    /// Set on `DataCommitted`/`Failed`/`Completed`/`PostPush*`.
    pub duration_ms: Option<i64>,
    /// UTC microseconds since unix epoch.
    pub ts_micros: i64,
    /// Free-form metadata.  Defaults to `"{}"`.
    pub metadata_json: String,
}

/// Map of `partition_key -> Checksum` recorded on a [`RecordKind::DataCommitted`].
pub type PartitionChecksums = HashMap<String, Checksum>;

/// JSON form of the `metadata_json` payload on `DataCommitted` records.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataCommittedMetadata {
    /// Partition checksums for partitions touched by this commit, plus
    /// every other partition's most recent checksum (so that
    /// `partition_checksums_at(seq)` is a complete snapshot).
    #[serde(default)]
    pub partition_checksums: HashMap<String, ChecksumValue>,
    /// The data store's Delta Lake version at the moment this commit
    /// was recorded.  `remote-push` reads this to locate the
    /// commit-log entry whose Add/Remove actions describe the bundle.
    /// Older records (written before this field existed) deserialize
    /// to `0`; treat that as "unknown".
    #[serde(default)]
    pub data_delta_version: i64,
}

/// Serialized form of [`Checksum`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChecksumValue {
    /// Strategy that produced the bytes.
    pub kind: ChecksumKind,
    /// Lowercase hex-encoded bytes.
    pub hex: String,
}

impl From<&Checksum> for ChecksumValue {
    fn from(c: &Checksum) -> Self {
        Self {
            kind: c.kind,
            hex: c.hex(),
        }
    }
}

impl From<&ChecksumValue> for Checksum {
    fn from(v: &ChecksumValue) -> Self {
        let mut bytes = Vec::with_capacity(v.hex.len() / 2);
        let mut iter = v.hex.chars();
        while let (Some(a), Some(b)) = (iter.next(), iter.next()) {
            let high = a.to_digit(16).unwrap_or(0) as u8;
            let low = b.to_digit(16).unwrap_or(0) as u8;
            bytes.push((high << 4) | low);
        }
        Checksum::new(v.kind, bytes)
    }
}

const TABLE_NAME: &str = "control";

const COL_POND_ID: &str = "pond_id";
const COL_RECORD_KIND: &str = "record_kind";
const COL_TXN_SEQ: &str = "txn_seq";
const COL_TXN_ID: &str = "txn_id";
const COL_COMMIT_KIND: &str = "commit_kind";
const COL_PARENT_SEQ: &str = "parent_seq";
const COL_DURATION_MS: &str = "duration_ms";
const COL_TS_MICROS: &str = "ts_micros";
const COL_METADATA_JSON: &str = "metadata_json";
const COL_HAS_COMMIT_KIND: &str = "has_commit_kind";
const COL_HAS_PARENT_SEQ: &str = "has_parent_seq";
const COL_HAS_DURATION_MS: &str = "has_duration_ms";

fn arrow_schema() -> Arc<ArrowSchema> {
    // We avoid nullable Int64 columns for simplicity by carrying
    // explicit `has_*` boolean columns alongside; a `0` in `parent_seq`
    // when `has_parent_seq=false` is meaningless.
    Arc::new(ArrowSchema::new(vec![
        Field::new(COL_POND_ID, DataType::Utf8, false),
        Field::new(COL_RECORD_KIND, DataType::Utf8, false),
        Field::new(COL_TXN_SEQ, DataType::Int64, false),
        Field::new(COL_TXN_ID, DataType::Utf8, false),
        Field::new(COL_COMMIT_KIND, DataType::Utf8, false),
        Field::new(COL_HAS_COMMIT_KIND, DataType::Boolean, false),
        Field::new(COL_PARENT_SEQ, DataType::Int64, false),
        Field::new(COL_HAS_PARENT_SEQ, DataType::Boolean, false),
        Field::new(COL_DURATION_MS, DataType::Int64, false),
        Field::new(COL_HAS_DURATION_MS, DataType::Boolean, false),
        Field::new(COL_TS_MICROS, DataType::Int64, false),
        Field::new(COL_METADATA_JSON, DataType::Utf8, false),
    ]))
}

fn delta_columns() -> Vec<DeltaStructField> {
    vec![
        DeltaStructField::new(
            COL_POND_ID,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_RECORD_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_TXN_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_TXN_ID,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_COMMIT_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_COMMIT_KIND,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_PARENT_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_PARENT_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_DURATION_MS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_HAS_DURATION_MS,
            DeltaDataType::Primitive(PrimitiveType::Boolean),
            false,
        ),
        DeltaStructField::new(
            COL_TS_MICROS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            COL_METADATA_JSON,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
    ]
}

/// The control table.
pub struct ControlTable {
    path: PathBuf,
    table: DeltaTable,
    session_ctx: Arc<SessionContext>,
}

impl ControlTable {
    /// Create a new control table at `path` (errors if exists).
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let url = url_from_path(&path)?;
        let table = DeltaTable::try_from_url(url)
            .await?
            .create()
            .with_columns(delta_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;

        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// Open an existing control table.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let url = url_from_path(&path)?;
        let table = deltalake::open_table(url).await?;
        let session_ctx = build_session_ctx(&table)?;
        Ok(Self {
            path,
            table,
            session_ctx,
        })
    }

    /// On-disk path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Run delta-rs `optimize(Compact)` on the control table to merge
    /// small parquets into larger ones.  Logical content is unchanged
    /// (control records are append-only audit history).  Returns
    /// `(num_files_added, num_files_removed)` so callers can detect
    /// no-ops.
    pub async fn compact(&mut self) -> Result<(u64, u64)> {
        use deltalake::operations::optimize::OptimizeType;
        let (new_table, metrics) = self
            .table
            .clone()
            .optimize()
            .with_type(OptimizeType::Compact)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok((metrics.num_files_added, metrics.num_files_removed))
    }

    /// Run delta-rs vacuum on the control table to physically reclaim
    /// parquets that are no longer referenced by any active commit
    /// (e.g., the small parquets tombstoned by `compact`).  Uses
    /// retention=0 / enforce=false, matching the prototype defaults
    /// used elsewhere.  Returns the count of files reclaimed.
    pub async fn vacuum(&mut self) -> Result<usize> {
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

    /// Delete all records belonging to `pond_id` from the control
    /// table.  Used by the per-pond_id `restart_from_compact`
    /// recovery pathway when a foreign import is being re-bootstrapped.
    ///
    /// Setting records (per-pond instance config) are NOT deleted by
    /// this call -- they are written under the local pond_id and live
    /// in the pond's own seq space, untouched by foreign-pond drops.
    pub async fn drop_pond_records(&mut self, pond_id: Uuid) -> Result<()> {
        let predicate = format!("{} = '{}'", COL_POND_ID, pond_id);
        let (new_table, _metrics) = self
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// Append a single record to the control table.
    pub async fn write_record(&mut self, record: ControlRecord) -> Result<()> {
        let schema = arrow_schema();
        let n = 1;

        let pond_ids = vec![record.pond_id.to_string()];
        let record_kinds = vec![record.record_kind.as_str().to_string()];
        let txn_seqs = vec![record.txn_seq];
        let txn_ids = vec![record.txn_id.clone()];
        let commit_kinds = vec![
            record
                .commit_kind
                .map(|k| k.as_str().to_string())
                .unwrap_or_default(),
        ];
        let has_commit = vec![record.commit_kind.is_some()];
        let parent_seqs = vec![record.parent_seq.unwrap_or(0)];
        let has_parent = vec![record.parent_seq.is_some()];
        let durations = vec![record.duration_ms.unwrap_or(0)];
        let has_duration = vec![record.duration_ms.is_some()];
        let timestamps = vec![record.ts_micros];
        let metadatas = vec![record.metadata_json.clone()];

        let _ = n;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(pond_ids)),
                Arc::new(StringArray::from(record_kinds)),
                Arc::new(Int64Array::from(txn_seqs)),
                Arc::new(StringArray::from(txn_ids)),
                Arc::new(StringArray::from(commit_kinds)),
                Arc::new(BooleanArray::from(has_commit)),
                Arc::new(Int64Array::from(parent_seqs)),
                Arc::new(BooleanArray::from(has_parent)),
                Arc::new(Int64Array::from(durations)),
                Arc::new(BooleanArray::from(has_duration)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(metadatas)),
            ],
        )?;

        let new_table = self.table.clone().write(vec![batch]).await?;
        self.table = new_table;
        self.session_ctx = build_session_ctx(&self.table)?;
        Ok(())
    }

    /// All records, ordered by `(txn_seq ASC, ts_micros ASC)`.  Returns
    /// records from ALL pond_ids (cross-pond inspection).  Use
    /// [`Self::all_records_for`] to scope to a single pond.
    pub async fn all_records(&self) -> Result<Vec<ControlRecord>> {
        self.all_records_inner(None).await
    }

    /// All records for `pond_id`, ordered by `(txn_seq ASC, ts_micros ASC)`.
    pub async fn all_records_for(&self, pond_id: Uuid) -> Result<Vec<ControlRecord>> {
        self.all_records_inner(Some(pond_id)).await
    }

    async fn all_records_inner(&self, pond_id: Option<Uuid>) -> Result<Vec<ControlRecord>> {
        let where_clause = pond_id
            .map(|p| format!("WHERE {pid} = '{p}' ", pid = COL_POND_ID, p = p))
            .unwrap_or_default();
        let sql = format!(
            "SELECT {pid}, {rk}, {ts}, {tid}, {ck}, {hck}, {ps}, {hps}, {dm}, {hdm}, {tsm}, {md} \
             FROM {table} \
             {where_clause}\
             ORDER BY {ts} ASC, {tsm} ASC",
            pid = COL_POND_ID,
            rk = COL_RECORD_KIND,
            ts = COL_TXN_SEQ,
            tid = COL_TXN_ID,
            ck = COL_COMMIT_KIND,
            hck = COL_HAS_COMMIT_KIND,
            ps = COL_PARENT_SEQ,
            hps = COL_HAS_PARENT_SEQ,
            dm = COL_DURATION_MS,
            hdm = COL_HAS_DURATION_MS,
            tsm = COL_TS_MICROS,
            md = COL_METADATA_JSON,
            table = TABLE_NAME,
            where_clause = where_clause,
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut out = Vec::new();
        for batch in batches {
            decode_records(&batch, &mut out)?;
        }
        Ok(out)
    }

    /// Records of a specific kind for `pond_id`, oldest-first.
    pub async fn records_of_kind(
        &self,
        pond_id: Uuid,
        kind: RecordKind,
    ) -> Result<Vec<ControlRecord>> {
        let all = self.all_records_for(pond_id).await?;
        Ok(all.into_iter().filter(|r| r.record_kind == kind).collect())
    }

    /// Largest `txn_seq` recorded in any kind of record (Begin or
    /// later) for `pond_id`.  `0` if `pond_id` has no records.
    pub async fn last_txn_seq(&self, pond_id: Uuid) -> Result<i64> {
        let sql = format!(
            "SELECT MAX({ts}) AS m FROM {table} \
             WHERE {pid} = '{p}' AND {rk} != '{setting}'",
            ts = COL_TXN_SEQ,
            table = TABLE_NAME,
            pid = COL_POND_ID,
            p = pond_id,
            rk = COL_RECORD_KIND,
            setting = RecordKind::Setting.as_str(),
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut max: i64 = 0;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| StewardError::Invariant("last_txn_seq: column not Int64".into()))?;
            if !arr.is_null(0) {
                max = max.max(arr.value(0));
            }
        }
        Ok(max)
    }

    /// Largest `txn_seq` for which a `DataCommitted` record exists for
    /// `pond_id`.
    pub async fn last_committed_seq(&self, pond_id: Uuid) -> Result<i64> {
        let sql = format!(
            "SELECT MAX({ts}) AS m FROM {table} \
             WHERE {pid} = '{p}' AND {rk} = '{kind}'",
            ts = COL_TXN_SEQ,
            table = TABLE_NAME,
            pid = COL_POND_ID,
            p = pond_id,
            rk = COL_RECORD_KIND,
            kind = RecordKind::DataCommitted.as_str(),
        );
        let batches = self.session_ctx.sql(&sql).await?.collect().await?;
        let mut max: i64 = 0;
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| StewardError::Invariant("last_committed_seq: not Int64".into()))?;
            if !arr.is_null(0) {
                max = max.max(arr.value(0));
            }
        }
        Ok(max)
    }

    /// Records that look like they may belong to incomplete transactions
    /// for `pond_id`: `Begin` rows with no matching terminal record
    /// (`DataCommitted`, `Failed`, or `Completed`) for the same `txn_seq`.
    pub async fn incomplete_transactions(&self, pond_id: Uuid) -> Result<Vec<ControlRecord>> {
        let all = self.all_records_for(pond_id).await?;
        let mut by_seq: HashMap<i64, Vec<&ControlRecord>> = HashMap::new();
        for r in &all {
            if matches!(r.record_kind, RecordKind::Setting) {
                continue;
            }
            by_seq.entry(r.txn_seq).or_default().push(r);
        }
        let mut out = Vec::new();
        for (_seq, recs) in by_seq {
            let has_terminal = recs.iter().any(|r| {
                matches!(
                    r.record_kind,
                    RecordKind::DataCommitted | RecordKind::Failed | RecordKind::Completed
                )
            });
            if !has_terminal
                && let Some(begin) = recs.iter().find(|r| r.record_kind == RecordKind::Begin)
            {
                out.push((*begin).clone());
            }
        }
        out.sort_by_key(|r| r.txn_seq);
        Ok(out)
    }

    /// Resolve the partition_checksums map at `(pond_id, txn_seq)`.
    /// Returns the `metadata.partition_checksums` field of the
    /// `DataCommitted` record at that seq, or `None` if no such commit
    /// exists for that pond_id.
    pub async fn partition_checksums_at(
        &self,
        pond_id: Uuid,
        txn_seq: i64,
    ) -> Result<Option<PartitionChecksums>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        let mut out = PartitionChecksums::new();
        for (k, v) in meta.partition_checksums {
            out.insert(k, Checksum::from(&v));
        }
        Ok(Some(out))
    }

    /// Resolve the data store's Delta Lake version at
    /// `(pond_id, txn_seq)`.  Returns the `metadata.data_delta_version`
    /// field of the `DataCommitted` record at that seq, or `None` if
    /// no such commit exists.
    pub async fn data_delta_version_at(&self, pond_id: Uuid, txn_seq: i64) -> Result<Option<i64>> {
        let all = self.all_records_for(pond_id).await?;
        let rec = all
            .iter()
            .find(|r| r.record_kind == RecordKind::DataCommitted && r.txn_seq == txn_seq);
        let Some(rec) = rec else {
            return Ok(None);
        };
        let meta: DataCommittedMetadata = serde_json::from_str(&rec.metadata_json)?;
        Ok(Some(meta.data_delta_version))
    }

    /// Set a configuration key/value scoped to `pond_id`.  Records a
    /// [`RecordKind::Setting`] row.  Settings are pond-instance state
    /// (e.g., `last_pulled_seq:<remote>`); the Steward always uses its
    /// local pond_id when calling this.
    pub async fn config_set(&mut self, pond_id: Uuid, key: &str, value: &str) -> Result<()> {
        let mut meta = HashMap::new();
        meta.insert("v".to_string(), value.to_string());
        let metadata_json = serde_json::to_string(&meta)?;
        let rec = ControlRecord {
            pond_id,
            record_kind: RecordKind::Setting,
            txn_seq: 0,
            txn_id: key.to_string(),
            commit_kind: None,
            parent_seq: None,
            duration_ms: None,
            ts_micros: Utc::now().timestamp_micros(),
            metadata_json,
        };
        self.write_record(rec).await
    }

    /// Read the current value of `(pond_id, key)`, or `None` if never set.
    pub async fn config_get(&self, pond_id: Uuid, key: &str) -> Result<Option<String>> {
        let map = self.config_list(pond_id).await?;
        Ok(map.get(key).cloned())
    }

    /// Read all current settings for `pond_id` (latest-write-wins per key).
    pub async fn config_list(&self, pond_id: Uuid) -> Result<HashMap<String, String>> {
        // Latest-write-wins per key.  Walk all Setting records and
        // overlay in `ts_micros` order.
        let all = self.all_records_for(pond_id).await?;
        let mut settings: Vec<(i64, String, String)> = Vec::new();
        for rec in all.iter().filter(|r| r.record_kind == RecordKind::Setting) {
            let map: HashMap<String, String> = serde_json::from_str(&rec.metadata_json)?;
            if let Some(v) = map.get("v") {
                settings.push((rec.ts_micros, rec.txn_id.clone(), v.clone()));
            }
        }
        settings.sort_by_key(|(ts, _, _)| *ts);
        let mut out = HashMap::new();
        for (_ts, k, v) in settings {
            out.insert(k, v);
        }
        Ok(out)
    }
}

fn url_from_path(path: &Path) -> Result<Url> {
    Url::from_directory_path(path)
        .or_else(|_| Url::from_file_path(path))
        .map_err(|_| StewardError::InvalidPath(path.display().to_string()))
}

fn build_session_ctx(table: &DeltaTable) -> Result<Arc<SessionContext>> {
    let ctx = SessionContext::new();
    _ = ctx.register_table(TABLE_NAME, Arc::new(table.clone()))?;
    Ok(Arc::new(ctx))
}

fn read_string(col: &dyn Array, row: usize) -> Result<String> {
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(arr.value(row).to_string());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok(arr.value(row).to_string());
    }
    Err(StewardError::Invariant(format!(
        "expected Utf8 or Utf8View, got {:?}",
        col.data_type()
    )))
}

fn read_i64(col: &dyn Array, row: usize) -> Result<i64> {
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| StewardError::Invariant("expected Int64".into()))?;
    Ok(arr.value(row))
}

fn read_bool(col: &dyn Array, row: usize) -> Result<bool> {
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| StewardError::Invariant("expected Boolean".into()))?;
    Ok(arr.value(row))
}

fn parse_record_kind(s: &str) -> Result<RecordKind> {
    Ok(match s {
        "begin" => RecordKind::Begin,
        "data_committed" => RecordKind::DataCommitted,
        "failed" => RecordKind::Failed,
        "completed" => RecordKind::Completed,
        "post_push_pending" => RecordKind::PostPushPending,
        "post_push_started" => RecordKind::PostPushStarted,
        "post_push_completed" => RecordKind::PostPushCompleted,
        "post_push_failed" => RecordKind::PostPushFailed,
        "setting" => RecordKind::Setting,
        other => {
            return Err(StewardError::Invariant(format!(
                "unknown record_kind {:?}",
                other
            )));
        }
    })
}

fn parse_commit_kind(s: &str) -> Result<CommitKind> {
    Ok(match s {
        "write" => CommitKind::Write,
        "compact" => CommitKind::Compact,
        other => {
            return Err(StewardError::Invariant(format!(
                "unknown commit_kind {:?}",
                other
            )));
        }
    })
}

fn decode_records(batch: &RecordBatch, out: &mut Vec<ControlRecord>) -> Result<()> {
    let cols: HashMap<String, usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().to_string(), i))
        .collect();

    let need = |name: &str| -> Result<usize> {
        cols.get(name)
            .copied()
            .ok_or_else(|| StewardError::Invariant(format!("missing column {}", name)))
    };

    let i_pid = need(COL_POND_ID)?;
    let i_rk = need(COL_RECORD_KIND)?;
    let i_ts = need(COL_TXN_SEQ)?;
    let i_tid = need(COL_TXN_ID)?;
    let i_ck = need(COL_COMMIT_KIND)?;
    let i_hck = need(COL_HAS_COMMIT_KIND)?;
    let i_ps = need(COL_PARENT_SEQ)?;
    let i_hps = need(COL_HAS_PARENT_SEQ)?;
    let i_dm = need(COL_DURATION_MS)?;
    let i_hdm = need(COL_HAS_DURATION_MS)?;
    let i_tsm = need(COL_TS_MICROS)?;
    let i_md = need(COL_METADATA_JSON)?;

    for row in 0..batch.num_rows() {
        let pond_id_str = read_string(batch.column(i_pid).as_ref(), row)?;
        let pond_id = Uuid::parse_str(&pond_id_str).map_err(|e| {
            StewardError::Invariant(format!(
                "control table pond_id `{}` is not a valid UUID: {}",
                pond_id_str, e
            ))
        })?;
        let rk = parse_record_kind(&read_string(batch.column(i_rk).as_ref(), row)?)?;
        let txn_seq = read_i64(batch.column(i_ts).as_ref(), row)?;
        let txn_id = read_string(batch.column(i_tid).as_ref(), row)?;
        let has_ck = read_bool(batch.column(i_hck).as_ref(), row)?;
        let commit_kind = if has_ck {
            Some(parse_commit_kind(&read_string(
                batch.column(i_ck).as_ref(),
                row,
            )?)?)
        } else {
            None
        };
        let has_ps = read_bool(batch.column(i_hps).as_ref(), row)?;
        let parent_seq = if has_ps {
            Some(read_i64(batch.column(i_ps).as_ref(), row)?)
        } else {
            None
        };
        let has_dm = read_bool(batch.column(i_hdm).as_ref(), row)?;
        let duration_ms = if has_dm {
            Some(read_i64(batch.column(i_dm).as_ref(), row)?)
        } else {
            None
        };
        let ts_micros = read_i64(batch.column(i_tsm).as_ref(), row)?;
        let metadata_json = read_string(batch.column(i_md).as_ref(), row)?;
        out.push(ControlRecord {
            pond_id,
            record_kind: rk,
            txn_seq,
            txn_id,
            commit_kind,
            parent_seq,
            duration_ms,
            ts_micros,
            metadata_json,
        });
    }
    Ok(())
}

/// Build a `txn_id` (UUID v4 hex) for a new transaction.
pub fn new_txn_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_and_delta_schemas_agree() {
        let arrow = arrow_schema();
        let delta = delta_columns();
        assert_eq!(arrow.fields().len(), delta.len());
        for (af, df) in arrow.fields().iter().zip(delta.iter()) {
            assert_eq!(af.name(), df.name());
        }
    }

    #[test]
    fn record_kind_roundtrip() {
        for k in [
            RecordKind::Begin,
            RecordKind::DataCommitted,
            RecordKind::Failed,
            RecordKind::Completed,
            RecordKind::PostPushPending,
            RecordKind::PostPushStarted,
            RecordKind::PostPushCompleted,
            RecordKind::PostPushFailed,
            RecordKind::Setting,
        ] {
            assert_eq!(parse_record_kind(k.as_str()).unwrap(), k);
        }
    }

    #[test]
    fn checksum_value_roundtrip() {
        let original = Checksum::new(
            ChecksumKind::Merkle,
            vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef],
        );
        let serialized = ChecksumValue::from(&original);
        let deserialized = Checksum::from(&serialized);
        assert_eq!(original, deserialized);
    }
}
