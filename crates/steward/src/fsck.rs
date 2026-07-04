// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond fsck` -- local, offline filesystem-check.
//!
//! Computes a single deterministic **root checksum** that exhaustively
//! commits to every `OplogEntry` row in the pond's data table, across
//! every `pond_id` (including cross-pond imports).  The root is a Merkle
//! tree of Merkle trees:
//!
//! ```text
//! root                                  <- fold of all partition checksums
//! |- partition (pond_id, part_id) -> per-partition Merkle checksum
//! |  |- row leaf = blake3(serde_json(OplogEntry))
//! |  +- ...
//! +- ...
//! ```
//!
//! The per-partition checksum is the same layout-/compaction-independent
//! Merkle used by remote verification (see
//! [`crate::remote_adapter::compute_live_checksums_for_table`]); fsck adds
//! the second-level fold so two replicas can be compared with a single
//! hex string.
//!
//! ## Content-checksum pass
//!
//! The row Merkle commits to each file's *recorded* `blake3`, but not to
//! the bytes it points at.  When [`FsckOptions::verify_content`] is set
//! (the default), fsck additionally:
//!
//! - re-hashes inline file content via [`OplogEntry::verified_content`], and
//! - re-reads every external `_large_files/blake3=<hash>.parquet` blob
//!   through [`ChunkedReader`], which validates the blob's embedded hash
//!   chain; for non-series version/table entries it also confirms the
//!   reassembled bytes hash to the recorded `blake3`.
//!
//! Content failures are collected into [`FsckReport::errors`] rather than
//! aborting, so a single corrupt blob does not hide the rest of the report.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::context::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use sync_store::checksum::{Checksum, Leaf, Merkle, PartitionChecksum};
use tinyfs::EntryType;
use tlogfs::large_files::find_large_file_path;
use tlogfs::schema::OplogEntry;
use utilities::chunked_files::ChunkedReader;

use crate::{Ship, StewardError, get_data_path};

/// Options controlling an [`fsck`] run.
#[derive(Debug, Clone, Copy)]
pub struct FsckOptions {
    /// Re-read and re-hash every file's content (inline and external
    /// large-file blobs) to confirm the bytes match their recorded
    /// `blake3`.  When `false` (`--quick`), only the structural row
    /// Merkle / root checksum is computed.
    pub verify_content: bool,
}

impl Default for FsckOptions {
    fn default() -> Self {
        Self {
            verify_content: true,
        }
    }
}

/// The Merkle checksum of one partition `(pond_id, part_id)`.
#[derive(Debug, Clone)]
pub struct PartitionDigest {
    /// Owning pond UUID (the pond that created the rows).
    pub pond_id: String,
    /// Partition UUID.
    pub part_id: String,
    /// Number of `OplogEntry` rows folded into this partition.
    pub rows: usize,
    /// The partition's Merkle checksum.
    pub checksum: Checksum,
}

/// A single content-verification failure found by [`fsck`].
#[derive(Debug, Clone)]
pub struct FsckError {
    /// Owning pond UUID of the offending row.
    pub pond_id: String,
    /// Partition UUID of the offending row.
    pub part_id: String,
    /// Node UUID of the offending row.
    pub node_id: String,
    /// Version of the offending row.
    pub version: i64,
    /// Human-readable description of the failure.
    pub detail: String,
}

/// Result of an [`fsck`] run.
#[derive(Debug, Clone)]
pub struct FsckReport {
    /// Single root checksum committing to every row in every partition.
    /// Two replicas are byte-identical iff their roots match (and both
    /// pass content verification).
    pub root: Checksum,
    /// Per-partition digests, sorted by `(pond_id, part_id)`.
    pub partitions: Vec<PartitionDigest>,
    /// Total `OplogEntry` rows folded into the root.
    pub rows_checked: usize,
    /// Number of external large-file blobs re-read and validated.
    pub blobs_checked: usize,
    /// Number of inline-content rows re-hashed.
    pub inline_checked: usize,
    /// Content-verification failures (empty on a clean pond).
    pub errors: Vec<FsckError>,
}

impl FsckReport {
    /// `true` iff no content-verification errors were found.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Hex-encoded root checksum.
    #[must_use]
    pub fn root_hex(&self) -> String {
        self.root.hex()
    }
}

/// Run a filesystem check over `ship`'s data table.
///
/// Reads the full data table once, builds the per-partition and root
/// Merkle checksums, and (unless [`FsckOptions::verify_content`] is
/// `false`) re-validates every file's content against its recorded
/// `blake3`.  Returns an error only for failures that prevent the check
/// from running at all (e.g. the data table cannot be read); per-file
/// content mismatches are reported in [`FsckReport::errors`].
pub async fn fsck(ship: &Ship, opts: FsckOptions) -> Result<FsckReport, StewardError> {
    let table = ship.data_persistence().table().clone();
    let data_dir = get_data_path(ship.pond_path());

    let ctx = SessionContext::new();
    let _previous = ctx
        .register_table("fsck_live", Arc::new(table))
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    let batches = ctx
        .sql("SELECT * FROM fsck_live ORDER BY pond_id, part_id, node_id, version")
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?
        .collect()
        .await
        .map_err(|e| StewardError::DeltaLake(e.to_string()))?;

    // Group rows by (pond_id, part_id); each leaf is keyed by
    // node_id/version with value blake3(serde_json(row)).
    let mut by_partition: BTreeMap<(String, String), Vec<(String, [u8; 32])>> = BTreeMap::new();
    let mut errors: Vec<FsckError> = Vec::new();
    let mut rows_checked = 0usize;
    let mut blobs_checked = 0usize;
    let mut inline_checked = 0usize;

    for batch in &batches {
        let rows: Vec<OplogEntry> = serde_arrow::from_record_batch(batch)
            .map_err(|e| StewardError::DeltaLake(e.to_string()))?;
        for row in rows {
            rows_checked += 1;
            let leaf_key = format!("{}/{}", row.node_id, row.version);
            let digest = crate::remote_adapter::row_leaf_digest(&row)?;
            by_partition
                .entry((row.pond_id.clone(), row.part_id.to_string()))
                .or_default()
                .push((leaf_key, digest));

            if opts.verify_content
                && let Err(detail) =
                    verify_row_content(&row, &data_dir, &mut blobs_checked, &mut inline_checked)
                        .await
            {
                errors.push(FsckError {
                    pond_id: row.pond_id.clone(),
                    part_id: row.part_id.to_string(),
                    node_id: row.node_id.to_string(),
                    version: row.version,
                    detail,
                });
            }
        }
    }

    let strategy = Merkle::new();
    let mut partitions: Vec<PartitionDigest> = Vec::with_capacity(by_partition.len());
    // Second-level leaves: one per partition, value = blake3 of the
    // partition checksum bytes (normalizes any strategy to 32 bytes).
    let mut root_leaves_owned: Vec<(String, [u8; 32])> = Vec::with_capacity(by_partition.len());

    for ((pond_id, part_id), kv) in by_partition {
        let leaves: Vec<Leaf<'_>> = kv
            .iter()
            .map(|(k, v)| Leaf {
                key: k.as_str(),
                value_blake3: v,
            })
            .collect();
        let checksum = strategy.compute(&leaves);
        let root_value = *blake3::hash(&checksum.bytes).as_bytes();
        let root_key = format!("{}/{}", pond_id, part_id);
        root_leaves_owned.push((root_key, root_value));
        partitions.push(PartitionDigest {
            pond_id,
            part_id,
            rows: kv.len(),
            checksum,
        });
    }

    let root_leaves: Vec<Leaf<'_>> = root_leaves_owned
        .iter()
        .map(|(k, v)| Leaf {
            key: k.as_str(),
            value_blake3: v,
        })
        .collect();
    let root = strategy.compute(&root_leaves);

    Ok(FsckReport {
        root,
        partitions,
        rows_checked,
        blobs_checked,
        inline_checked,
        errors,
    })
}

/// Verify one row's content against its recorded `blake3`.  Returns
/// `Err(detail)` describing the first failure; `Ok(())` when the row
/// either has no content to check or verifies cleanly.
async fn verify_row_content(
    row: &OplogEntry,
    data_dir: &Path,
    blobs_checked: &mut usize,
    inline_checked: &mut usize,
) -> Result<(), String> {
    if row.is_large_file() {
        let blake3 = row
            .blake3
            .as_deref()
            .ok_or_else(|| "large-file row missing blake3".to_string())?;

        let path = find_large_file_path(data_dir, blake3)
            .await
            .map_err(|e| format!("locating blob for blake3={blake3}: {e}"))?
            .ok_or_else(|| format!("large-file blob missing for blake3={blake3}"))?;

        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| format!("reading blob {}: {e}", path.display()))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|e| format!("opening blob parquet for blake3={blake3}: {e}"))?
            .build()
            .map_err(|e| format!("building blob reader for blake3={blake3}: {e}"))?;

        let mut blob_batches = Vec::new();
        for b in reader {
            blob_batches
                .push(b.map_err(|e| format!("reading blob batch for blake3={blake3}: {e}"))?);
        }

        // ChunkedReader validates the blob's embedded chunk-hash chain
        // and reassembles the content bytes.
        let mut sink: Vec<u8> = Vec::new();
        ChunkedReader::new(blob_batches)
            .read_to_writer(&mut sink)
            .await
            .map_err(|e| format!("blob integrity check failed for blake3={blake3}: {e}"))?;
        *blobs_checked += 1;

        // For non-series entries the recorded blake3 is blake3(content),
        // so tie the reassembled bytes back to the row's recorded hash.
        // FilePhysicalSeries stores a cumulative bao root that cannot be
        // compared to a single version's content, so rely on the blob's
        // internal validation above.
        if row.file_type != EntryType::FilePhysicalSeries {
            let actual = blake3::hash(&sink).to_hex().to_string();
            if actual != blake3 {
                return Err(format!(
                    "blob content hash {actual} does not match recorded blake3 {blake3}"
                ));
            }
        }
        Ok(())
    } else if row.content.is_some() && row.file_type.is_file() {
        let _verified = row.verified_content().map_err(|e| e.to_string())?;
        *inline_checked += 1;
        Ok(())
    } else {
        Ok(())
    }
}
