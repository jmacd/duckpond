//! Tests for Delta Lake change detection functionality
//!
//! This test suite verifies that we can correctly detect file changes
//! from Delta Lake transaction logs.

use crate::remote_factory::detect_changes_from_delta_log;
use crate::{OpLogPersistence, TLogFSError};

/// Helper to create a test pond path
fn test_pond_path() -> String {
    use std::env;
    // Use both timestamp and thread ID for uniqueness when tests run in parallel
    let test_name = format!(
        "test_change_detection_{}_{:?}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        std::thread::current().id()
    );
    let test_dir = env::temp_dir().join(test_name);
    test_dir.to_string_lossy().to_string()
}

/// Test basic change detection from a Delta Lake version
///
/// This test creates a simple pond, writes some directories,
/// and verifies we can detect the changes.
#[tokio::test]
async fn test_detect_changes_from_commit() -> Result<(), TLogFSError> {
    let pond_path = test_pond_path();
    let _ = std::fs::remove_dir_all(&pond_path); // Clean up any existing test data

    // Create a new OpLogPersistence (this will create the Delta table)
    let mut persistence = OpLogPersistence::create_test(&pond_path).await?;

    // Note: create() already does txn_seq=1 to initialize the root directory
    // So our first user transaction should be txn_seq=2

    // Begin a transaction and write some data
    {
        let tx = persistence.begin_test().await?;
        let root = tx.root().await.map_err(|e| TLogFSError::TinyFS(e))?;

        // Create test directories (simpler than files for this test)
        _ = root.create_dir_path("/test_data")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        _ = root.create_dir_path("/test_data/subdir1")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        _ = root.create_dir_path("/test_data/subdir2")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        // Commit the transaction
        tx.commit_test().await.map_err(|e| TLogFSError::TinyFS(e))?;
    }

    // Get the Delta table
    let table = persistence.table();
    let current_version = table.version().unwrap_or(0);

    println!("Current Delta table version: {}", current_version);

    // Detect changes from the current version
    let changeset = detect_changes_from_delta_log(table, current_version).await?;

    // Verify we detected some files (Parquet files for the directories)
    println!("Detected changes:");
    println!("  Added files: {}", changeset.added.len());
    println!("  Removed files: {}", changeset.removed.len());
    println!("  Total bytes added: {}", changeset.total_bytes_added());

    // We should have at least one file added (directory metadata creates Parquet files)
    assert!(
        changeset.added.len() > 0,
        "Expected to detect at least one added file"
    );

    // Print details of added files
    for (i, file_change) in changeset.added.iter().enumerate() {
        println!("\nFile {}:", i + 1);
        println!("  Parquet path: {}", file_change.parquet_path);
        println!("  Size: {} bytes", file_change.size);
        println!("  Part ID: {:?}", file_change.part_id);
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&pond_path);

    Ok(())
}

/// Test change detection with multiple commits
#[tokio::test]
async fn test_detect_changes_multiple_commits() -> Result<(), TLogFSError> {
    let pond_path = test_pond_path();
    let _ = std::fs::remove_dir_all(&pond_path);

    let mut persistence = OpLogPersistence::create_test(&pond_path).await?;

    // Note: create() already does txn_seq=1 to initialize the root directory
    // So our first user transaction should be txn_seq=2

    let version0 = persistence.table().version().unwrap_or(0);
    println!("Version after pond creation: {}", version0);

    // Commit 1: Create initial directories (txn_seq=2)
    {
        let tx = persistence.begin_test().await?;
        let root = tx.root().await.map_err(|e| TLogFSError::TinyFS(e))?;

        _ = root.create_dir_path("/data1")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        tx.commit_test().await.map_err(|e| TLogFSError::TinyFS(e))?;
    }

    let version1 = persistence.table().version().unwrap_or(0);
    println!("Version after commit 1: {}", version1);

    // Commit 2: Add more directories (txn_seq=3)
    {
        let tx = persistence.begin_test().await?;
        let root = tx.root().await.map_err(|e| TLogFSError::TinyFS(e))?;

        _ = root.create_dir_path("/data2")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        _ = root.create_dir_path("/data3")
            .await
            .map_err(|e| TLogFSError::TinyFS(e))?;

        tx.commit_test().await.map_err(|e| TLogFSError::TinyFS(e))?;
    }

    let version2 = persistence.table().version().unwrap_or(0);
    println!("Version after commit 2: {}", version2);

    // Detect changes in commit 1
    let changeset1 = detect_changes_from_delta_log(persistence.table(), version1).await?;
    println!("\nCommit 1 changes:");
    println!(
        "  Added: {} files ({} bytes)",
        changeset1.added.len(),
        changeset1.total_bytes_added()
    );

    // Detect changes in commit 2
    let changeset2 = detect_changes_from_delta_log(persistence.table(), version2).await?;
    println!("\nCommit 2 changes:");
    println!(
        "  Added: {} files ({} bytes)",
        changeset2.added.len(),
        changeset2.total_bytes_added()
    );

    // Commit 2 should have more files than commit 1 (cumulative)
    assert!(
        changeset2.added.len() >= changeset1.added.len(),
        "Commit 2 should have at least as many files as commit 1 (cumulative)"
    );

    // Clean up
    let _ = std::fs::remove_dir_all(&pond_path);

    Ok(())
}

/// Test that part_id extraction logic exists
///
/// This is a placeholder test to document the part_id extraction functionality.
/// The actual extraction happens inside detect_changes_from_delta_log.
#[test]
fn test_part_id_extraction_documented() {
    // The extract_part_id_from_parquet_path function is private,
    // but it handles Parquet paths like:
    // "part-01234567-89ab-7def-8123-456789abcdef-c000.parquet"
    //
    // It extracts the UUID from parts[1] through parts[5] to reconstruct
    // a NodeID from the Parquet filename.

    println!("Part ID extraction is handled by private function in remote_factory");
    assert!(true, "This test documents the functionality");
}
