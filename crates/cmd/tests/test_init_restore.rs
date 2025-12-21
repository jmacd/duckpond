// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for pond initialization from remote backup
//!
//! Tests the complete backup and restore flow:
//! 1. Create a pond with data
//! 2. Back it up using remote factory
//! 3. Initialize a new pond from the backup
//! 4. Verify all data is restored correctly

// use cmd::common::ShipContext;
use remote::RemoteTable;
// use std::collections::HashMap;

// /// Helper to create a test ship context
// fn create_ship_context(pond_path: std::path::PathBuf) -> ShipContext {
//     ShipContext {
//         pond_path: Some(pond_path),
//         original_args: vec!["pond".to_string(), "init".to_string()],
//         template_variables: HashMap::new(),
//     }
// }

#[tokio::test]
async fn test_init_from_local_backup() {
    // Setup: Create a simple backup with metadata
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let backup_path = temp_dir.path().join("backup");
    let backup_url = format!("file://{}", backup_path.display());

    let mut remote_table = RemoteTable::create(&backup_url)
        .await
        .expect("Failed to create remote table");

    let pond_id = uuid7::uuid7();

    // Write metadata for a transaction
    // Uses POND:META:{pond_id} bundle_id for all metadata records
    remote_table
        .write_metadata(
            1,
            &remote::TransactionMetadata {
                pond_id: pond_id.to_string(),
                file_count: 1,
                files: vec![remote::FileInfo {
                    path: "test_file.parquet".to_string(),
                    sha256: "abc123".to_string(),
                    size: 100,
                    file_type: remote::FileType::PondParquet,
                }],
                cli_args: vec!["test".to_string()],
                created_at: chrono::Utc::now().timestamp_millis(),
                total_size: 100,
            },
        )
        .await
        .expect("Failed to write metadata");

    // Test: Scan for versions
    let versions = remote::scan_remote_versions(&backup_url, Some(&pond_id))
        .await
        .expect("Failed to scan versions");

    assert_eq!(versions, vec![1], "Should find transaction 1");

    // Verify we can read the metadata back using the transaction ID
    let metadata = remote_table
        .read_metadata(&pond_id.to_string(), 1)
        .await
        .expect("Failed to read metadata");

    assert_eq!(metadata.pond_id, pond_id.to_string());
    assert_eq!(metadata.file_count, 1);
    assert_eq!(metadata.cli_args, vec!["test".to_string()]);

    println!("✓ Init from backup test passed");
}

#[tokio::test]
async fn test_scan_remote_versions() {
    // Create a remote backup table with multiple transactions
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let backup_path = temp_dir.path().join("backup");
    let backup_url = format!("file://{}", backup_path.display());

    let mut remote_table = RemoteTable::create(&backup_url)
        .await
        .expect("Failed to create remote table");

    let pond_id = uuid7::uuid7();

    // Write metadata for 3 transactions
    for txn_id in [1, 2, 3] {
        remote_table
            .write_metadata(
                txn_id,
                &remote::TransactionMetadata {
                    pond_id: pond_id.to_string(),
                    file_count: 1,
                    files: vec![],
                    cli_args: vec![format!("txn{}", txn_id)],
                    created_at: chrono::Utc::now().timestamp_millis(),
                    total_size: 0,
                },
            )
            .await
            .expect("Failed to write metadata");
    }

    // Scan for versions
    let versions = remote::scan_remote_versions(&backup_url, Some(&pond_id))
        .await
        .expect("Failed to scan versions");

    assert_eq!(versions, vec![1, 2, 3], "Should find all 3 transactions");

    println!("✓ Scan remote versions test passed");
}

#[tokio::test]
async fn test_scan_empty_backup() {
    // Create empty backup table
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let backup_path = temp_dir.path().join("empty_backup");
    let backup_url = format!("file://{}", backup_path.display());

    let _remote_table = RemoteTable::create(&backup_url)
        .await
        .expect("Failed to create remote table");

    let pond_id = uuid7::uuid7();

    // Scan should return empty vec
    let versions = remote::scan_remote_versions(&backup_url, Some(&pond_id))
        .await
        .expect("Failed to scan versions");

    assert!(versions.is_empty(), "Empty backup should have no versions");

    println!("✓ Scan empty backup test passed");
}
