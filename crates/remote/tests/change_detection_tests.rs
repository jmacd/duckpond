// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for Delta Lake change detection functionality
//!
//! This test suite verifies that we can correctly detect file changes
//! from Delta Lake transaction logs.

use log::debug;
use remote::detect_changes_from_delta_log;
use tlogfs::{OpLogPersistence, PondTxnMetadata, PondUserMetadata, TLogFSError};

/// Helper to create a test pond path
fn test_pond_path() -> String {
    use std::env;
    let test_name = format!(
        "test_change_detection_{}_{:?}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        std::thread::current().id()
    );
    let test_dir = env::temp_dir().join(test_name);
    test_dir.to_string_lossy().to_string()
}

/// Test basic change detection from a Delta Lake version
#[tokio::test]
async fn test_detect_changes_from_commit() -> Result<(), Box<dyn std::error::Error>> {
    let pond_path = test_pond_path();
    let _ = std::fs::remove_dir_all(&pond_path);

    let mut persistence = OpLogPersistence::create(
        &pond_path,
        PondUserMetadata::new(vec!["test".to_string(), "create".to_string()]),
    )
    .await?;

    {
        let metadata = PondTxnMetadata::new(
            persistence.last_txn_seq() + 1,
            PondUserMetadata::new(vec!["test".to_string()]),
        );
        let tx = persistence.begin_write(&metadata).await?;
        let root = tx.root().await.map_err(TLogFSError::TinyFS)?;

        _ = root
            .create_dir_path("/test_data")
            .await
            .map_err(TLogFSError::TinyFS)?;
        _ = root
            .create_dir_path("/test_data/subdir1")
            .await
            .map_err(TLogFSError::TinyFS)?;
        _ = root
            .create_dir_path("/test_data/subdir2")
            .await
            .map_err(TLogFSError::TinyFS)?;

        tx.commit().await.map_err(TLogFSError::TinyFS)?;
    }

    let table = persistence.table();
    let current_version = table.version().unwrap_or(0);

    let changeset = detect_changes_from_delta_log(table, current_version).await?;

    assert!(
        !changeset.added.is_empty(),
        "Expected to detect at least one added file"
    );

    let _ = std::fs::remove_dir_all(&pond_path);
    Ok(())
}

/// Test change detection with multiple commits
#[tokio::test]
async fn test_detect_changes_multiple_commits() -> Result<(), Box<dyn std::error::Error>> {
    let pond_path = test_pond_path();
    let _ = std::fs::remove_dir_all(&pond_path);

    let mut persistence = OpLogPersistence::create(
        &pond_path,
        PondUserMetadata::new(vec!["test".to_string(), "create".to_string()]),
    )
    .await?;

    {
        let metadata = PondTxnMetadata::new(
            persistence.last_txn_seq() + 1,
            PondUserMetadata::new(vec!["test".to_string()]),
        );
        let tx = persistence.begin_write(&metadata).await?;
        let root = tx.root().await.map_err(TLogFSError::TinyFS)?;
        _ = root
            .create_dir_path("/data1")
            .await
            .map_err(TLogFSError::TinyFS)?;
        tx.commit().await.map_err(TLogFSError::TinyFS)?;
    }

    let version1 = persistence.table().version().unwrap_or(0);

    {
        let metadata = PondTxnMetadata::new(
            persistence.last_txn_seq() + 1,
            PondUserMetadata::new(vec!["test".to_string()]),
        );
        let tx = persistence.begin_write(&metadata).await?;
        let root = tx.root().await.map_err(TLogFSError::TinyFS)?;
        _ = root
            .create_dir_path("/data2")
            .await
            .map_err(TLogFSError::TinyFS)?;
        _ = root
            .create_dir_path("/data3")
            .await
            .map_err(TLogFSError::TinyFS)?;
        tx.commit().await.map_err(TLogFSError::TinyFS)?;
    }

    let version2 = persistence.table().version().unwrap_or(0);

    let changeset1 = detect_changes_from_delta_log(persistence.table(), version1).await?;
    let changeset2 = detect_changes_from_delta_log(persistence.table(), version2).await?;

    assert!(
        !changeset1.added.is_empty(),
        "Commit 1 should have added files"
    );
    assert!(
        !changeset2.added.is_empty(),
        "Commit 2 should have added files"
    );

    let _ = std::fs::remove_dir_all(&pond_path);
    Ok(())
}
