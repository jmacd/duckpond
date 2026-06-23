// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the steward write lock (D5.7a.1).
//!
//! These tests verify two end-user-visible invariants that the lower-level
//! `write_lock` unit tests cannot exercise:
//!
//! 1. **Cross-Ship process exclusion.** When one `Ship` instance holds an
//!    active write transaction on a pond, a second `Ship` opened on the
//!    same pond must fail to begin its own write with `PondLocked`.  Once
//!    the first writer drops its guard, the lock must be released so the
//!    second writer can proceed.
//!
//! 2. **No control-table records for reads.** After D5.7a.1, read
//!    transactions must not emit `begin` / `completed` records into the
//!    steward control table.  Counting the control rows before and after
//!    a read transaction proves the change.

use anyhow::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use steward::{PondUserMetadata, Ship, StewardError};
use tempfile::tempdir;

/// Count rows in the control delta table (across all partitions).
async fn count_control_rows(control_path: &std::path::Path) -> Result<usize> {
    let url = url::Url::from_directory_path(control_path)
        .or_else(|_| url::Url::from_file_path(control_path))
        .map_err(|_| anyhow::anyhow!("invalid control path"))?;
    let table = deltalake::open_table(url).await?;
    let ctx = SessionContext::new();
    let _ = ctx.register_table("control", Arc::new(table))?;
    let df = ctx.sql("SELECT COUNT(*) AS c FROM control").await?;
    let batches = df.collect().await?;
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count is int64");
    Ok(arr.value(0) as usize)
}

#[tokio::test]
async fn write_lock_blocks_concurrent_writer() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");

    // Create pond + first Ship.
    let mut ship_a = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("create_pond: {e}"))?;

    let meta_a = PondUserMetadata::new(vec!["test".into(), "writer-a".into()]);
    let guard_a = ship_a
        .begin_write(&meta_a)
        .await
        .map_err(|e| anyhow::anyhow!("ship A begin_write: {e}"))?;

    // Second Ship on the same pond — should observe the lock.
    let mut ship_b = Ship::open_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("ship B open_pond: {e}"))?;
    let meta_b = PondUserMetadata::new(vec!["test".into(), "writer-b".into()]);
    match ship_b.begin_write(&meta_b).await {
        Ok(_g) => panic!("second writer should be rejected while ship A holds the lock"),
        Err(StewardError::PondLocked {
            holder_pid, path, ..
        }) => {
            assert_eq!(holder_pid, Some(std::process::id()));
            assert!(path.ends_with("write.lock"));
        }
        Err(other) => panic!("expected PondLocked, got: {other:?}"),
    }

    // Drop ship A's guard — releases the lock.
    drop(guard_a);

    // ship B can now acquire.
    let guard_b = ship_b
        .begin_write(&meta_b)
        .await
        .map_err(|e| anyhow::anyhow!("ship B begin_write after A drop: {e}"))?;
    drop(guard_b);

    Ok(())
}

#[tokio::test]
async fn read_transaction_emits_no_control_records() -> Result<()> {
    let temp = tempdir()?;
    let pond_path = temp.path().join("pond");
    let control_path = steward::get_control_path(&pond_path);

    let mut ship = Ship::create_pond(&pond_path, "test-host")
        .await
        .map_err(|e| anyhow::anyhow!("create_pond: {e}"))?;

    let before = count_control_rows(&control_path).await?;

    // Open and commit a read transaction.  After D5.7a.1, this must not
    // append any rows to the control table.
    let meta = PondUserMetadata::new(vec!["test".into(), "reader".into()]);
    let guard = ship
        .begin_read(&meta)
        .await
        .map_err(|e| anyhow::anyhow!("begin_read: {e}"))?;
    let _ = guard
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("commit read: {e}"))?;

    let after = count_control_rows(&control_path).await?;
    assert_eq!(
        before, after,
        "read transaction should not append rows to the control table \
         (before={before}, after={after})"
    );
    Ok(())
}
