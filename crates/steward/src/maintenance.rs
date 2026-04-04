// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Automatic Delta Lake table maintenance: checkpoint, vacuum, and compact.
//!
//! Every Delta table accumulates JSON commit log files and small parquet data
//! files over time.  Without periodic maintenance, opening a table requires
//! parsing *all* log files from scratch, causing linear growth in CPU and
//! memory.
//!
//! This module provides best-effort maintenance that runs after successful
//! write transactions:
//!
//! - **Checkpoint** -- collapse the JSON delta log into a single parquet
//!   checkpoint file.  Created every `CHECKPOINT_INTERVAL` versions.
//! - **Log cleanup** -- remove expired JSON log files older than the
//!   retention period (only after a checkpoint exists).
//! - **Vacuum** -- delete parquet data files no longer referenced by the
//!   current table version (lite mode by default).
//! - **Compact** -- merge many small parquet files into fewer large ones
//!   (only on explicit request via `pond maintain --compact`).

use chrono::Duration;
use deltalake::checkpoints;
use deltalake::operations::optimize::OptimizeType;
use deltalake::DeltaTable;
use log::{debug, info, warn};

/// How often to create checkpoints (every N versions).
/// Delta Lake standard is 10.
const CHECKPOINT_INTERVAL: u64 = 10;

/// Default retention period for vacuum.
/// Files older than this and no longer referenced are eligible for deletion.
/// Zero means vacuum everything not in the active version.
const VACUUM_RETENTION_HOURS: u64 = 0;

/// Default target size for compaction (128 MB).
const COMPACT_TARGET_SIZE: u64 = 128 * 1024 * 1024;

/// Result of a maintenance run on a single table.
#[derive(Debug, Default)]
pub struct MaintenanceResult {
    /// Name of the table that was maintained (for logging).
    pub table_name: String,
    /// Current table version after maintenance.
    pub version: i64,
    /// Whether a checkpoint was created.
    pub checkpoint_created: bool,
    /// Number of expired log files cleaned up.
    pub logs_cleaned: usize,
    /// Number of stale data files vacuumed.
    pub files_vacuumed: usize,
    /// Whether compaction was performed.
    pub compacted: bool,
    /// Compaction metrics (only if compacted).
    pub compact_files_added: u64,
    pub compact_files_removed: u64,
}

/// Combined report for both data and control tables.
#[derive(Debug, Default)]
pub struct MaintenanceReport {
    pub data: Option<MaintenanceResult>,
    pub control: Option<MaintenanceResult>,
}

impl std::fmt::Display for MaintenanceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (v{})", self.table_name, self.version)?;
        if self.checkpoint_created {
            write!(f, " checkpoint")?;
        }
        if self.logs_cleaned > 0 {
            write!(f, " cleaned={}", self.logs_cleaned)?;
        }
        if self.files_vacuumed > 0 {
            write!(f, " vacuumed={}", self.files_vacuumed)?;
        }
        if self.compacted {
            write!(
                f,
                " compacted(+{}/- {})",
                self.compact_files_added, self.compact_files_removed
            )?;
        }
        if !self.checkpoint_created
            && self.logs_cleaned == 0
            && self.files_vacuumed == 0
            && !self.compacted
        {
            write!(f, " (no maintenance needed)")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for MaintenanceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref d) = self.data {
            writeln!(f, "  {}", d)?;
        }
        if let Some(ref c) = self.control {
            write!(f, "  {}", c)?;
        }
        Ok(())
    }
}

/// Run checkpoint and vacuum on a Delta table.
///
/// When `force` is true, checkpoint is always created (for explicit `pond maintain`).
/// When `force` is false, checkpoint is only created at interval boundaries
/// (for automatic post-commit maintenance).
///
/// This is best-effort: errors are logged as warnings and do not propagate.
/// The table reference is consumed and a new (possibly updated) table is
/// returned, since vacuum/optimize operations produce a new DeltaTable.
pub async fn maintain_table(
    table: DeltaTable,
    table_name: &str,
    force: bool,
    do_compact: bool,
) -> (DeltaTable, MaintenanceResult) {
    let mut result = MaintenanceResult {
        table_name: table_name.to_string(),
        version: table.version().unwrap_or(-1),
        ..Default::default()
    };

    let mut table = table;

    // 1. Checkpoint if needed (or forced)
    let version = table.version().unwrap_or(0);
    let should_checkpoint =
        version > 0 && (force || (version as u64) % CHECKPOINT_INTERVAL == 0);
    if should_checkpoint {
        debug!(
            "[MAINTAIN] Creating checkpoint for {} at version {}",
            table_name, version
        );
        match checkpoints::create_checkpoint(&table, None).await {
            Ok(()) => {
                info!(
                    "[MAINTAIN] Checkpoint created for {} at version {}",
                    table_name, version
                );
                result.checkpoint_created = true;
            }
            Err(e) => {
                warn!(
                    "[MAINTAIN] Checkpoint failed for {} at version {}: {}",
                    table_name, version, e
                );
            }
        }
    }

    // 2. Clean up expired log files (only meaningful after a checkpoint exists)
    if result.checkpoint_created {
        match checkpoints::cleanup_metadata(&table, None).await {
            Ok(cleaned) => {
                if cleaned > 0 {
                    info!(
                        "[MAINTAIN] Cleaned {} expired log files for {}",
                        cleaned, table_name
                    );
                }
                result.logs_cleaned = cleaned;
            }
            Err(e) => {
                warn!(
                    "[MAINTAIN] Log cleanup failed for {}: {}",
                    table_name, e
                );
            }
        }
    }

    // 3. Vacuum stale data files
    match table
        .clone()
        .vacuum()
        .with_retention_period(Duration::hours(VACUUM_RETENTION_HOURS as i64))
        .with_enforce_retention_duration(false)
        .await
    {
        Ok((new_table, metrics)) => {
            let count = metrics.files_deleted.len();
            if count > 0 {
                info!(
                    "[MAINTAIN] Vacuumed {} stale files from {}",
                    count, table_name
                );
            }
            result.files_vacuumed = count;
            table = new_table;
        }
        Err(e) => {
            warn!("[MAINTAIN] Vacuum failed for {}: {}", table_name, e);
        }
    }

    // 4. Compact small files (only when explicitly requested)
    if do_compact {
        debug!("[MAINTAIN] Running compaction for {}", table_name);
        match table
            .clone()
            .optimize()
            .with_type(OptimizeType::Compact)
            .with_target_size(COMPACT_TARGET_SIZE)
            .await
        {
            Ok((new_table, metrics)) => {
                info!(
                    "[MAINTAIN] Compacted {}: +{} files / -{} files",
                    table_name, metrics.num_files_added, metrics.num_files_removed
                );
                result.compacted = true;
                result.compact_files_added = metrics.num_files_added;
                result.compact_files_removed = metrics.num_files_removed;
                table = new_table;
            }
            Err(e) => {
                warn!("[MAINTAIN] Compaction failed for {}: {}", table_name, e);
            }
        }
    }

    result.version = table.version().unwrap_or(-1);
    (table, result)
}
