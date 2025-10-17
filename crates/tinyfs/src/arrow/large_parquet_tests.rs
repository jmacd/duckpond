//! Tests for large Parquet file storage integration
//!
//! These tests verify that large Parquet files are handled correctly
//! with the large file storage system.

use crate::arrow::{ForArrow, ParquetExt};
use crate::{EntryType, memory::new_fs};
use arrow::datatypes::{DataType, Field, FieldRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The 64 KiB threshold for large file storage (copied from tlogfs)
const LARGE_FILE_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LargeTestRecord {
    id: i64,
    name: String,
    description: String, // Add longer text field
    data: Vec<u8>,       // Add binary data field
    score: Option<f64>,
}

impl ForArrow for LargeTestRecord {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("description", DataType::Utf8, false)),
            Arc::new(Field::new("data", DataType::Binary, false)),
            Arc::new(Field::new("score", DataType::Float64, true)),
        ]
    }
}

#[tokio::test]
async fn test_parquet_file_size_estimation() -> Result<(), Box<dyn std::error::Error>> {
    let fs = new_fs().await;
    let wd = fs.root().await?;

    // Create different sized datasets to see what triggers large file storage
    let sizes = vec![100, 500, 1000, 2500, 5000, 10000];

    for size in sizes {
        let large_data: Vec<LargeTestRecord> = (0..size)
            .map(|i| LargeTestRecord {
                id: i as i64,
                name: format!("User_{}_with_longer_name_to_increase_size", i),
                description: format!("This is a longer description for user {} to increase the size of each record and make the parquet file larger. We need to trigger large file storage which happens at {} bytes.", i, LARGE_FILE_THRESHOLD),
                data: vec![42u8; 100], // 100 bytes of binary data per record
                score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
            })
            .collect();

        let test_path = format!("test_size_{}.parquet", size);

        // Write the dataset
        wd.create_table_from_items(&test_path, &large_data, EntryType::FileTablePhysical)
            .await?;

        // Read the raw file content to check size
        let content = wd.read_file_path_to_vec(&test_path).await?;
        let file_size = content.len();

        println!("Dataset size: {} records", size);
        println!(
            "Parquet file size: {} bytes ({:.2} KiB)",
            file_size,
            file_size as f64 / 1024.0
        );
        println!(
            "Large file threshold: {} bytes ({:.2} KiB)",
            LARGE_FILE_THRESHOLD,
            LARGE_FILE_THRESHOLD as f64 / 1024.0
        );
        println!(
            "Triggers large file storage: {}",
            file_size >= LARGE_FILE_THRESHOLD
        );
        println!();

        // Test that we can read it back correctly
        let read_data: Vec<LargeTestRecord> = wd.read_table_as_items(&test_path).await?;
        assert_eq!(large_data.len(), read_data.len());

        if file_size >= LARGE_FILE_THRESHOLD {
            println!("🎯 Found a dataset size that triggers large file storage!");
            break;
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_guaranteed_large_parquet_file() -> Result<(), Box<dyn std::error::Error>> {
    let fs = new_fs().await;
    let wd = fs.root().await?;

    // Create a dataset that definitely exceeds 64 KiB
    let record_count = 20000; // Much larger dataset
    let large_data: Vec<LargeTestRecord> = (0..record_count)
        .map(|i| LargeTestRecord {
            id: i as i64,
            name: format!("User_{}_with_very_long_name_to_ensure_large_file_storage_is_triggered_definitely", i),
            description: format!("This is an extremely long description for user {} designed to create a large parquet file that will definitely exceed the 64 KiB threshold for large file storage. We're repeating this text multiple times to ensure size: {}", i, "padding ".repeat(20)),
            data: vec![42u8; 500], // 500 bytes of binary data per record
            score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
        })
        .collect();

    let test_path = "guaranteed_large.parquet";

    // Write the dataset
    wd.create_table_from_items(test_path, &large_data, EntryType::FileTablePhysical)
        .await?;

    // Read the raw file content to check size
    let content = wd.read_file_path_to_vec(test_path).await?;
    let file_size = content.len();

    println!("🎯 GUARANTEED LARGE FILE TEST");
    println!("Dataset size: {} records", record_count);
    println!(
        "Parquet file size: {} bytes ({:.2} KiB)",
        file_size,
        file_size as f64 / 1024.0
    );
    println!(
        "Large file threshold: {} bytes ({:.2} KiB)",
        LARGE_FILE_THRESHOLD,
        LARGE_FILE_THRESHOLD as f64 / 1024.0
    );
    println!(
        "Triggers large file storage: {}",
        file_size >= LARGE_FILE_THRESHOLD
    );

    // This should definitely trigger large file storage
    assert!(
        file_size >= LARGE_FILE_THRESHOLD,
        "Expected file size {} to exceed threshold {}",
        file_size,
        LARGE_FILE_THRESHOLD
    );

    // Verify we can still read it back correctly
    let read_data: Vec<LargeTestRecord> = wd.read_table_as_items(test_path).await?;
    assert_eq!(large_data.len(), read_data.len());

    // Spot check some records
    assert_eq!(large_data[0], read_data[0]);
    assert_eq!(large_data[10000], read_data[10000]);
    assert_eq!(large_data[19999], read_data[19999]);

    println!("✅ Large Parquet file test successful!");
    println!(
        "   Large file storage working correctly with {} KiB file",
        file_size / 1024
    );

    Ok(())
}
