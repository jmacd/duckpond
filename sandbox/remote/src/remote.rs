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

use crate::chunking::chunk_bytes;
use crate::error::{RemoteError, Result};
use crate::schema::{
    self, RemoteRow, RowBody, delta_columns, partition_columns, record_batch_to_rows,
    rows_to_record_batch,
};

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
