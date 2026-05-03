// SPDX-License-Identifier: Apache-2.0

//! Schema definitions for the remote-sync Delta Lake table.
//!
//! The remote table holds three kinds of rows distinguished by a single
//! `partition_kind` column with three bounded values.  Per-row layout:
//!
//! | column                  | type    | populated on            |
//! |-------------------------|---------|-------------------------|
//! | `partition_kind`        | Utf8    | every row (partition)   |
//! | `txn_seq`               | Int64   | every row               |
//! | `ts_micros`             | Int64   | every row               |
//! | `commit_kind`           | Utf8    | `manifest`              |
//! | `parent_seq`            | Int64   | `manifest`              |
//! | `partition_key`         | Utf8    | `checksum`              |
//! | `checksum_kind`         | Utf8    | `checksum`              |
//! | `checksum_bytes`        | Binary  | `checksum`              |
//! | `file_path`             | Utf8    | `data`                  |
//! | `file_action`           | Utf8    | `data`                  |
//! | `file_size`             | Int64   | `data`                  |
//! | `file_blake3`           | Binary  | `data`                  |
//! | `chunk_count`           | Int64   | `data` (Add only)       |
//! | `chunk_id`              | Int64   | `data` (Add only)       |
//! | `chunk_data`            | Binary  | `data` (Add only)       |
//! | `chunk_blake3`          | Binary  | `data` (Add only)       |
//!
//! Variant-specific columns are nullable.  In-memory the union shape is
//! enforced via the [`RowBody`] enum; on read, schema violations
//! (a row with the wrong combination of nulls for its `partition_kind`)
//! surface as [`crate::RemoteError::Schema`].

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{UInt16Type, UInt32Type};
use arrow_array::{
    Array, BinaryArray, DictionaryArray, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use deltalake::kernel::{
    DataType as DeltaDataType, PrimitiveType, StructField as DeltaStructField,
};
use sandbox_steward::CommitKind;
use sandbox_store::checksum::ChecksumKind;

use crate::error::{RemoteError, Result};

/// Length of a BLAKE3 digest in bytes.
pub const BLAKE3_LEN: usize = 32;

/// Maximum size of a single `chunk_data` payload.  Files larger than
/// this are split into multiple `DataAdd` rows.
pub const CHUNK_SIZE_BYTES: usize = 16 * 1024 * 1024;

/// Column names.  Centralized to avoid stringly-typed bugs.
pub mod col {
    /// `partition_kind` -- Delta partition column; one of
    /// [`super::PARTITION_KIND_MANIFEST`], [`super::PARTITION_KIND_CHECKSUM`],
    /// or [`super::PARTITION_KIND_DATA`].
    pub const PARTITION_KIND: &str = "partition_kind";
    /// `txn_seq` -- the source-pond transaction sequence this row belongs to.
    pub const TXN_SEQ: &str = "txn_seq";
    /// `ts_micros` -- when the row was pushed (microseconds since unix epoch).
    pub const TS_MICROS: &str = "ts_micros";
    /// `commit_kind` -- on `manifest` rows: `"write"` or `"compact"`.
    pub const COMMIT_KIND: &str = "commit_kind";
    /// `parent_seq` -- on `manifest` rows: predecessor commit's `txn_seq` (0 if root).
    pub const PARENT_SEQ: &str = "parent_seq";
    /// `partition_key` -- on `checksum` rows: source-pond partition this checksum covers.
    pub const PARTITION_KEY: &str = "partition_key";
    /// `checksum_kind` -- on `checksum` rows: `"merkle"` or `"homomorphic"`.
    pub const CHECKSUM_KIND: &str = "checksum_kind";
    /// `checksum_bytes` -- on `checksum` rows: the 32-byte content checksum.
    pub const CHECKSUM_BYTES: &str = "checksum_bytes";
    /// `file_path` -- on `data` rows: source-pond-relative file path.
    pub const FILE_PATH: &str = "file_path";
    /// `file_action` -- on `data` rows: [`super::FILE_ACTION_ADD`] or [`super::FILE_ACTION_REMOVE`].
    pub const FILE_ACTION: &str = "file_action";
    /// `file_size` -- on `data` rows: total file size in bytes (0 for remove markers).
    pub const FILE_SIZE: &str = "file_size";
    /// `file_blake3` -- on `data` rows: BLAKE3 of the full file (zero-byte for remove markers).
    pub const FILE_BLAKE3: &str = "file_blake3";
    /// `chunk_count` -- on `data` Add rows: number of chunks for this file.
    pub const CHUNK_COUNT: &str = "chunk_count";
    /// `chunk_id` -- on `data` Add rows: this chunk's 0-based index.
    pub const CHUNK_ID: &str = "chunk_id";
    /// `chunk_data` -- on `data` Add rows: the raw chunk bytes (<= [`super::CHUNK_SIZE_BYTES`]).
    pub const CHUNK_DATA: &str = "chunk_data";
    /// `chunk_blake3` -- on `data` Add rows: BLAKE3 of `chunk_data`.
    pub const CHUNK_BLAKE3: &str = "chunk_blake3";
}

/// Stable wire string for the manifest partition.
pub const PARTITION_KIND_MANIFEST: &str = "manifest";
/// Stable wire string for the checksum partition.
pub const PARTITION_KIND_CHECKSUM: &str = "checksum";
/// Stable wire string for the data partition.
pub const PARTITION_KIND_DATA: &str = "data";

/// Stable wire string for an "add file" data row.
pub const FILE_ACTION_ADD: &str = "add";
/// Stable wire string for a "remove file" data row.
pub const FILE_ACTION_REMOVE: &str = "remove";

/// Stable wire string for [`CommitKind::Write`].
pub const COMMIT_KIND_WRITE: &str = "write";
/// Stable wire string for [`CommitKind::Compact`].
pub const COMMIT_KIND_COMPACT: &str = "compact";

/// Stable wire string for [`ChecksumKind::Merkle`].
pub const CHECKSUM_KIND_MERKLE: &str = "merkle";
/// Stable wire string for [`ChecksumKind::Homomorphic`].
pub const CHECKSUM_KIND_HOMOMORPHIC: &str = "homomorphic";

/// In-memory representation of one row in the remote Delta table.
///
/// On disk every variant-specific column is nullable.  This type makes
/// the union shape unrepresentable-when-invalid: each variant carries
/// only its own columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRow {
    /// The source-pond transaction sequence this row belongs to.
    pub txn_seq: i64,
    /// When the row was pushed (microseconds since unix epoch).
    pub ts_micros: i64,
    /// The variant-specific payload.
    pub body: RowBody,
}

/// The variant-specific payload of a [`RemoteRow`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowBody {
    /// Bundle header (one row per bundle).  `parent_seq` is 0 for the
    /// root commit.
    Manifest {
        /// `Write` or `Compact`.
        commit_kind: CommitKind,
        /// Predecessor commit's `txn_seq`, or 0 if this is the root.
        parent_seq: i64,
    },
    /// Per-source-partition content checksum (N rows per bundle, one per source partition).
    Checksum {
        /// The source-pond partition this checksum covers.
        partition_key: String,
        /// Strategy that produced [`Self::Checksum::checksum_bytes`].
        checksum_kind: ChecksumKind,
        /// The 32-byte content checksum.
        checksum_bytes: [u8; BLAKE3_LEN],
    },
    /// One chunk of one file added by this commit.  Files larger than
    /// [`CHUNK_SIZE_BYTES`] produce multiple `DataAdd` rows with a
    /// shared `file_path` / `file_size` / `file_blake3` and incrementing
    /// `chunk_id` from 0 to `chunk_count - 1`.
    DataAdd {
        /// Source-pond-relative path to the parquet file.
        file_path: String,
        /// Total file size in bytes (same on every chunk row of one file).
        file_size: i64,
        /// BLAKE3 of the full file (same on every chunk row of one file).
        file_blake3: [u8; BLAKE3_LEN],
        /// Total number of chunks for this file (same on every chunk row).
        chunk_count: i64,
        /// This chunk's 0-based index.
        chunk_id: i64,
        /// This chunk's bytes (<= [`CHUNK_SIZE_BYTES`]).
        chunk_data: Vec<u8>,
        /// BLAKE3 of [`Self::DataAdd::chunk_data`].
        chunk_blake3: [u8; BLAKE3_LEN],
    },
    /// Tombstone for a file removed by this commit (compact only).
    DataRemove {
        /// Source-pond-relative path to the parquet file that was removed.
        file_path: String,
    },
}

impl RowBody {
    /// Wire string for the [`col::PARTITION_KIND`] column.
    pub fn partition_kind(&self) -> &'static str {
        match self {
            Self::Manifest { .. } => PARTITION_KIND_MANIFEST,
            Self::Checksum { .. } => PARTITION_KIND_CHECKSUM,
            Self::DataAdd { .. } | Self::DataRemove { .. } => PARTITION_KIND_DATA,
        }
    }

    /// Wire string for the [`col::FILE_ACTION`] column on `data` rows;
    /// `None` for non-data variants.
    pub fn file_action(&self) -> Option<&'static str> {
        match self {
            Self::DataAdd { .. } => Some(FILE_ACTION_ADD),
            Self::DataRemove { .. } => Some(FILE_ACTION_REMOVE),
            _ => None,
        }
    }
}

/// Delta partition columns for the remote table.
pub fn partition_columns() -> Vec<&'static str> {
    vec![col::PARTITION_KIND]
}

/// Arrow schema used for [`RecordBatch`] construction.
pub fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        // Always populated.
        Field::new(col::PARTITION_KIND, DataType::Utf8, false),
        Field::new(col::TXN_SEQ, DataType::Int64, false),
        Field::new(col::TS_MICROS, DataType::Int64, false),
        // Manifest only.
        Field::new(col::COMMIT_KIND, DataType::Utf8, true),
        Field::new(col::PARENT_SEQ, DataType::Int64, true),
        // Checksum only.
        Field::new(col::PARTITION_KEY, DataType::Utf8, true),
        Field::new(col::CHECKSUM_KIND, DataType::Utf8, true),
        Field::new(col::CHECKSUM_BYTES, DataType::Binary, true),
        // Data only.
        Field::new(col::FILE_PATH, DataType::Utf8, true),
        Field::new(col::FILE_ACTION, DataType::Utf8, true),
        Field::new(col::FILE_SIZE, DataType::Int64, true),
        Field::new(col::FILE_BLAKE3, DataType::Binary, true),
        Field::new(col::CHUNK_COUNT, DataType::Int64, true),
        Field::new(col::CHUNK_ID, DataType::Int64, true),
        Field::new(col::CHUNK_DATA, DataType::Binary, true),
        Field::new(col::CHUNK_BLAKE3, DataType::Binary, true),
    ]))
}

/// Delta Lake `StructField` schema for `CreateBuilder::with_columns`.
///
/// Must agree with [`arrow_schema`] in name, type, and nullability.
/// Tests assert this.
pub fn delta_columns() -> Vec<DeltaStructField> {
    vec![
        DeltaStructField::new(
            col::PARTITION_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        DeltaStructField::new(
            col::TXN_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            col::TS_MICROS,
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        DeltaStructField::new(
            col::COMMIT_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        DeltaStructField::new(
            col::PARENT_SEQ,
            DeltaDataType::Primitive(PrimitiveType::Long),
            true,
        ),
        DeltaStructField::new(
            col::PARTITION_KEY,
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        DeltaStructField::new(
            col::CHECKSUM_KIND,
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        DeltaStructField::new(
            col::CHECKSUM_BYTES,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
        DeltaStructField::new(
            col::FILE_PATH,
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        DeltaStructField::new(
            col::FILE_ACTION,
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        DeltaStructField::new(
            col::FILE_SIZE,
            DeltaDataType::Primitive(PrimitiveType::Long),
            true,
        ),
        DeltaStructField::new(
            col::FILE_BLAKE3,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
        DeltaStructField::new(
            col::CHUNK_COUNT,
            DeltaDataType::Primitive(PrimitiveType::Long),
            true,
        ),
        DeltaStructField::new(
            col::CHUNK_ID,
            DeltaDataType::Primitive(PrimitiveType::Long),
            true,
        ),
        DeltaStructField::new(
            col::CHUNK_DATA,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
        DeltaStructField::new(
            col::CHUNK_BLAKE3,
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
    ]
}

/// Convert a slice of [`RemoteRow`]s into a single Arrow [`RecordBatch`]
/// matching [`arrow_schema`].  Variant-specific columns are populated
/// only on the variant they apply to; everything else is `null`.
pub fn rows_to_record_batch(rows: &[RemoteRow]) -> Result<RecordBatch> {
    let n = rows.len();

    let mut partition_kinds: Vec<&'static str> = Vec::with_capacity(n);
    let mut txn_seqs: Vec<i64> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut commit_kinds: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut parent_seqs: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut partition_keys: Vec<Option<String>> = Vec::with_capacity(n);
    let mut checksum_kinds: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut checksum_bytes: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut file_paths: Vec<Option<String>> = Vec::with_capacity(n);
    let mut file_actions: Vec<Option<&'static str>> = Vec::with_capacity(n);
    let mut file_sizes: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut file_blake3s: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut chunk_counts: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut chunk_ids: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut chunk_data: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
    let mut chunk_blake3s: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

    for row in rows {
        partition_kinds.push(row.body.partition_kind());
        txn_seqs.push(row.txn_seq);
        ts.push(row.ts_micros);

        match &row.body {
            RowBody::Manifest {
                commit_kind,
                parent_seq,
            } => {
                commit_kinds.push(Some(commit_kind_str(*commit_kind)));
                parent_seqs.push(Some(*parent_seq));
                partition_keys.push(None);
                checksum_kinds.push(None);
                checksum_bytes.push(None);
                file_paths.push(None);
                file_actions.push(None);
                file_sizes.push(None);
                file_blake3s.push(None);
                chunk_counts.push(None);
                chunk_ids.push(None);
                chunk_data.push(None);
                chunk_blake3s.push(None);
            }
            RowBody::Checksum {
                partition_key,
                checksum_kind,
                checksum_bytes: cb,
            } => {
                commit_kinds.push(None);
                parent_seqs.push(None);
                partition_keys.push(Some(partition_key.clone()));
                checksum_kinds.push(Some(checksum_kind_str(*checksum_kind)));
                checksum_bytes.push(Some(cb.to_vec()));
                file_paths.push(None);
                file_actions.push(None);
                file_sizes.push(None);
                file_blake3s.push(None);
                chunk_counts.push(None);
                chunk_ids.push(None);
                chunk_data.push(None);
                chunk_blake3s.push(None);
            }
            RowBody::DataAdd {
                file_path,
                file_size,
                file_blake3,
                chunk_count,
                chunk_id,
                chunk_data: cd,
                chunk_blake3: cb,
            } => {
                commit_kinds.push(None);
                parent_seqs.push(None);
                partition_keys.push(None);
                checksum_kinds.push(None);
                checksum_bytes.push(None);
                file_paths.push(Some(file_path.clone()));
                file_actions.push(Some(FILE_ACTION_ADD));
                file_sizes.push(Some(*file_size));
                file_blake3s.push(Some(file_blake3.to_vec()));
                chunk_counts.push(Some(*chunk_count));
                chunk_ids.push(Some(*chunk_id));
                chunk_data.push(Some(cd.clone()));
                chunk_blake3s.push(Some(cb.to_vec()));
            }
            RowBody::DataRemove { file_path } => {
                commit_kinds.push(None);
                parent_seqs.push(None);
                partition_keys.push(None);
                checksum_kinds.push(None);
                checksum_bytes.push(None);
                file_paths.push(Some(file_path.clone()));
                file_actions.push(Some(FILE_ACTION_REMOVE));
                file_sizes.push(None);
                file_blake3s.push(None);
                chunk_counts.push(None);
                chunk_ids.push(None);
                chunk_data.push(None);
                chunk_blake3s.push(None);
            }
        }
    }

    let schema = arrow_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(partition_kinds)),
            Arc::new(Int64Array::from(txn_seqs)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(commit_kinds)),
            Arc::new(Int64Array::from(parent_seqs)),
            Arc::new(StringArray::from(partition_keys)),
            Arc::new(StringArray::from(checksum_kinds)),
            Arc::new(BinaryArray::from_opt_vec(opt_vecs_as_slices(
                &checksum_bytes,
            ))),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(StringArray::from(file_actions)),
            Arc::new(Int64Array::from(file_sizes)),
            Arc::new(BinaryArray::from_opt_vec(opt_vecs_as_slices(&file_blake3s))),
            Arc::new(Int64Array::from(chunk_counts)),
            Arc::new(Int64Array::from(chunk_ids)),
            Arc::new(BinaryArray::from_opt_vec(opt_vecs_as_slices(&chunk_data))),
            Arc::new(BinaryArray::from_opt_vec(opt_vecs_as_slices(
                &chunk_blake3s,
            ))),
        ],
    )?;
    Ok(batch)
}

fn opt_vecs_as_slices(v: &[Option<Vec<u8>>]) -> Vec<Option<&[u8]>> {
    v.iter().map(|o| o.as_deref()).collect()
}

/// Decode a [`RecordBatch`] (matching [`arrow_schema`]) back into
/// [`RemoteRow`]s.  Each row's `partition_kind` (and `file_action` for
/// data rows) selects the variant; the relevant columns are then read
/// and required to be non-null.  An unexpected combination is a
/// [`RemoteError::Schema`] error -- the on-disk row violated the union
/// shape (would indicate a foreign writer or schema corruption).
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<RemoteRow>> {
    let schema = arrow_schema();
    if batch.schema().fields().len() != schema.fields().len() {
        return Err(RemoteError::Schema(format!(
            "expected {} columns, got {}",
            schema.fields().len(),
            batch.schema().fields().len(),
        )));
    }

    let n = batch.num_rows();
    let kinds = string_col(batch, col::PARTITION_KIND)?;
    let seqs = i64_col(batch, col::TXN_SEQ)?;
    let ts = i64_col(batch, col::TS_MICROS)?;
    let commit_kinds = string_col(batch, col::COMMIT_KIND)?;
    let parent_seqs = i64_col(batch, col::PARENT_SEQ)?;
    let partition_keys = string_col(batch, col::PARTITION_KEY)?;
    let checksum_kinds = string_col(batch, col::CHECKSUM_KIND)?;
    let checksum_bytes = binary_col(batch, col::CHECKSUM_BYTES)?;
    let file_paths = string_col(batch, col::FILE_PATH)?;
    let file_actions = string_col(batch, col::FILE_ACTION)?;
    let file_sizes = i64_col(batch, col::FILE_SIZE)?;
    let file_blake3s = binary_col(batch, col::FILE_BLAKE3)?;
    let chunk_counts = i64_col(batch, col::CHUNK_COUNT)?;
    let chunk_ids = i64_col(batch, col::CHUNK_ID)?;
    let chunk_data_col = binary_col(batch, col::CHUNK_DATA)?;
    let chunk_blake3s = binary_col(batch, col::CHUNK_BLAKE3)?;

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let kind = kinds[i]
            .as_deref()
            .ok_or_else(|| RemoteError::Schema(format!("row {}: partition_kind is null", i)))?;
        let txn_seq =
            seqs[i].ok_or_else(|| RemoteError::Schema(format!("row {}: txn_seq is null", i)))?;
        let ts_micros =
            ts[i].ok_or_else(|| RemoteError::Schema(format!("row {}: ts_micros is null", i)))?;

        let body = match kind {
            PARTITION_KIND_MANIFEST => RowBody::Manifest {
                commit_kind: parse_commit_kind(commit_kinds[i].as_deref(), i)?,
                parent_seq: parent_seqs[i].ok_or_else(|| {
                    RemoteError::Schema(format!("row {}: manifest missing parent_seq", i))
                })?,
            },
            PARTITION_KIND_CHECKSUM => RowBody::Checksum {
                partition_key: partition_keys[i].clone().ok_or_else(|| {
                    RemoteError::Schema(format!("row {}: checksum missing partition_key", i))
                })?,
                checksum_kind: parse_checksum_kind(checksum_kinds[i].as_deref(), i)?,
                checksum_bytes: parse_blake3(checksum_bytes[i].as_deref(), i, "checksum_bytes")?,
            },
            PARTITION_KIND_DATA => {
                let action = file_actions[i].as_deref().ok_or_else(|| {
                    RemoteError::Schema(format!("row {}: data row missing file_action", i))
                })?;
                let file_path = file_paths[i].clone().ok_or_else(|| {
                    RemoteError::Schema(format!("row {}: data row missing file_path", i))
                })?;
                match action {
                    FILE_ACTION_ADD => RowBody::DataAdd {
                        file_path,
                        file_size: file_sizes[i].ok_or_else(|| {
                            RemoteError::Schema(format!("row {}: DataAdd missing file_size", i))
                        })?,
                        file_blake3: parse_blake3(file_blake3s[i].as_deref(), i, "file_blake3")?,
                        chunk_count: chunk_counts[i].ok_or_else(|| {
                            RemoteError::Schema(format!("row {}: DataAdd missing chunk_count", i))
                        })?,
                        chunk_id: chunk_ids[i].ok_or_else(|| {
                            RemoteError::Schema(format!("row {}: DataAdd missing chunk_id", i))
                        })?,
                        chunk_data: chunk_data_col[i].clone().ok_or_else(|| {
                            RemoteError::Schema(format!("row {}: DataAdd missing chunk_data", i))
                        })?,
                        chunk_blake3: parse_blake3(chunk_blake3s[i].as_deref(), i, "chunk_blake3")?,
                    },
                    FILE_ACTION_REMOVE => RowBody::DataRemove { file_path },
                    other => {
                        return Err(RemoteError::Schema(format!(
                            "row {}: unknown file_action `{}`",
                            i, other,
                        )));
                    }
                }
            }
            other => {
                return Err(RemoteError::Schema(format!(
                    "row {}: unknown partition_kind `{}`",
                    i, other,
                )));
            }
        };

        out.push(RemoteRow {
            txn_seq,
            ts_micros,
            body,
        });
    }
    Ok(out)
}

fn commit_kind_str(k: CommitKind) -> &'static str {
    match k {
        CommitKind::Write => COMMIT_KIND_WRITE,
        CommitKind::Compact => COMMIT_KIND_COMPACT,
    }
}

fn parse_commit_kind(s: Option<&str>, row: usize) -> Result<CommitKind> {
    match s {
        Some(COMMIT_KIND_WRITE) => Ok(CommitKind::Write),
        Some(COMMIT_KIND_COMPACT) => Ok(CommitKind::Compact),
        Some(other) => Err(RemoteError::Schema(format!(
            "row {}: unknown commit_kind `{}`",
            row, other
        ))),
        None => Err(RemoteError::Schema(format!(
            "row {}: manifest missing commit_kind",
            row
        ))),
    }
}

fn checksum_kind_str(k: ChecksumKind) -> &'static str {
    match k {
        ChecksumKind::Merkle => CHECKSUM_KIND_MERKLE,
        ChecksumKind::Homomorphic => CHECKSUM_KIND_HOMOMORPHIC,
    }
}

fn parse_checksum_kind(s: Option<&str>, row: usize) -> Result<ChecksumKind> {
    match s {
        Some(CHECKSUM_KIND_MERKLE) => Ok(ChecksumKind::Merkle),
        Some(CHECKSUM_KIND_HOMOMORPHIC) => Ok(ChecksumKind::Homomorphic),
        Some(other) => Err(RemoteError::Schema(format!(
            "row {}: unknown checksum_kind `{}`",
            row, other
        ))),
        None => Err(RemoteError::Schema(format!(
            "row {}: checksum missing checksum_kind",
            row
        ))),
    }
}

fn parse_blake3(bytes: Option<&[u8]>, row: usize, field: &str) -> Result<[u8; BLAKE3_LEN]> {
    let b = bytes.ok_or_else(|| RemoteError::Schema(format!("row {}: {} is null", row, field)))?;
    if b.len() != BLAKE3_LEN {
        return Err(RemoteError::Schema(format!(
            "row {}: {} has {} bytes, expected {}",
            row,
            field,
            b.len(),
            BLAKE3_LEN
        )));
    }
    let mut out = [0u8; BLAKE3_LEN];
    out.copy_from_slice(b);
    Ok(out)
}

fn string_col(batch: &RecordBatch, name: &str) -> Result<Vec<Option<String>>> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| RemoteError::Schema(format!("missing column `{}`", name)))?;
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect());
    }
    // DataFusion may dict-encode partition columns; handle UInt16 and
    // UInt32 key types (the two it commonly produces for partition
    // columns of low cardinality).
    if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<UInt16Type>>() {
        let values = arr
            .values()
            .as_string_opt::<i32>()
            .ok_or_else(|| {
                RemoteError::Schema(format!(
                    "column `{}` dictionary values are not Utf8, got {:?}",
                    name,
                    arr.values().data_type()
                ))
            })?
            .clone();
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    let k = arr.keys().value(i) as usize;
                    if values.is_null(k) {
                        None
                    } else {
                        Some(values.value(k).to_string())
                    }
                }
            })
            .collect());
    }
    if let Some(arr) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
        let values = arr
            .values()
            .as_string_opt::<i32>()
            .ok_or_else(|| {
                RemoteError::Schema(format!(
                    "column `{}` dictionary values are not Utf8, got {:?}",
                    name,
                    arr.values().data_type()
                ))
            })?
            .clone();
        return Ok((0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    let k = arr.keys().value(i) as usize;
                    if values.is_null(k) {
                        None
                    } else {
                        Some(values.value(k).to_string())
                    }
                }
            })
            .collect());
    }
    Err(RemoteError::Schema(format!(
        "column `{}` is not Utf8/Utf8View/Dictionary<_, Utf8>, got {:?}",
        name,
        col.data_type()
    )))
}

fn i64_col(batch: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| RemoteError::Schema(format!("missing column `{}`", name)))?;
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| RemoteError::Schema(format!("column `{}` is not Int64", name)))?;
    Ok((0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            }
        })
        .collect())
}

fn binary_col(batch: &RecordBatch, name: &str) -> Result<Vec<Option<Vec<u8>>>> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| RemoteError::Schema(format!("missing column `{}`", name)))?;
    let arr = col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| RemoteError::Schema(format!("column `{}` is not Binary", name)))?;
    Ok((0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_vec())
            }
        })
        .collect())
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
            assert_eq!(af.name(), df.name(), "field names must match");
            assert_eq!(
                af.is_nullable(),
                df.is_nullable(),
                "nullability must match for column {}",
                af.name()
            );
        }
    }

    #[test]
    fn partition_kind_constants_are_distinct() {
        assert_ne!(PARTITION_KIND_MANIFEST, PARTITION_KIND_CHECKSUM);
        assert_ne!(PARTITION_KIND_CHECKSUM, PARTITION_KIND_DATA);
        assert_ne!(PARTITION_KIND_MANIFEST, PARTITION_KIND_DATA);
    }

    #[test]
    fn chunk_size_is_16_mib() {
        assert_eq!(CHUNK_SIZE_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn blake3_len_matches_blake3_crate() {
        assert_eq!(BLAKE3_LEN, blake3::OUT_LEN);
    }

    #[test]
    fn row_body_partition_kind_matches_constants() {
        let m = RowBody::Manifest {
            commit_kind: CommitKind::Write,
            parent_seq: 0,
        };
        let c = RowBody::Checksum {
            partition_key: "p".into(),
            checksum_kind: ChecksumKind::Merkle,
            checksum_bytes: [0u8; BLAKE3_LEN],
        };
        let a = RowBody::DataAdd {
            file_path: "x".into(),
            file_size: 0,
            file_blake3: [0u8; BLAKE3_LEN],
            chunk_count: 1,
            chunk_id: 0,
            chunk_data: vec![],
            chunk_blake3: [0u8; BLAKE3_LEN],
        };
        let r = RowBody::DataRemove {
            file_path: "y".into(),
        };
        assert_eq!(m.partition_kind(), PARTITION_KIND_MANIFEST);
        assert_eq!(c.partition_kind(), PARTITION_KIND_CHECKSUM);
        assert_eq!(a.partition_kind(), PARTITION_KIND_DATA);
        assert_eq!(r.partition_kind(), PARTITION_KIND_DATA);
        assert_eq!(m.file_action(), None);
        assert_eq!(c.file_action(), None);
        assert_eq!(a.file_action(), Some(FILE_ACTION_ADD));
        assert_eq!(r.file_action(), Some(FILE_ACTION_REMOVE));
    }
}
