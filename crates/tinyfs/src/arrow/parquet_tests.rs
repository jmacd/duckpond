//! Comprehensive tests for the full ParquetExt trait
//!
//! Tests both high-level ForArrow integration and low-level RecordBatch operations.

use arrow_array::{RecordBatch, record_batch};
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use crate::arrow::{ParquetExt, ForArrow};
use crate::memory::new_fs;
use crate::EntryType;

/// Test data structure that implements ForArrow 
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestRecord {
    id: i64,
    name: String,
    score: Option<f64>,
}

impl ForArrow for TestRecord {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("score", DataType::Float64, true)), // nullable
        ]
    }
}

#[tokio::test]
async fn test_full_parquet_roundtrip_with_forarrow() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Create test data
    let test_data = vec![
        TestRecord { id: 1, name: "Alice".to_string(), score: Some(95.5) },
        TestRecord { id: 2, name: "Bob".to_string(), score: None },
        TestRecord { id: 3, name: "Charlie".to_string(), score: Some(87.3) },
    ];
    
    let test_path = "test_records.parquet";
    
    // Write using the high-level ForArrow API
    wd.create_table_from_items(test_path, &test_data, EntryType::FileTablePhysical).await?;
    
    // Read back using the high-level ForArrow API
    let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;
    
    // Verify the data matches
    assert_eq!(test_data.len(), read_data.len());
    for (original, read) in test_data.iter().zip(read_data.iter()) {
        assert_eq!(original, read);
    }
    
    println!("✅ Full ParquetExt ForArrow roundtrip successful!");
    println!("   Processed {} records with mixed nullable/non-nullable fields", read_data.len());
    
    Ok(())
}

#[tokio::test]
async fn test_low_level_recordbatch_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Create a RecordBatch using Arrow macros
    let batch = record_batch!(
        ("product", Utf8, ["Widget A", "Widget B", "Widget C"]),
        ("quantity", Int64, [100_i64, 250_i64, 75_i64]),
        ("price", Float64, [19.99, 15.50, 8.25])
    )?;
    
    let test_path = "products.parquet";
    
    // Write using low-level RecordBatch API
    wd.create_table_from_batch(test_path, &batch, EntryType::FileTablePhysical).await?;
    
    // Read back using low-level RecordBatch API
    let read_batch = wd.read_table_as_batch(test_path).await?;
    
    // Verify schema and data
    assert_eq!(batch.schema(), read_batch.schema());
    assert_eq!(batch.num_rows(), read_batch.num_rows());
    assert_eq!(batch.num_columns(), read_batch.num_columns());
    
    // Verify column data
    use arrow_array::{StringArray, Int64Array, Float64Array};
    
    let original_products = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let read_products = read_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(original_products, read_products);
    
    let original_quantities = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let read_quantities = read_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(original_quantities, read_quantities);
    
    let original_prices = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let read_prices = read_batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(original_prices, read_prices);
    
    println!("✅ Low-level RecordBatch operations successful!");
    println!("   Verified schema and data integrity for {} rows", read_batch.num_rows());
    
    Ok(())
}

#[tokio::test]
async fn test_large_dataset_batching() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Create a large dataset (more than DEFAULT_BATCH_SIZE = 1000)
    let large_data: Vec<TestRecord> = (0..2500)
        .map(|i| TestRecord {
            id: i as i64,
            name: format!("User_{}", i),
            score: if i % 3 == 0 { None } else { Some((i as f64) * 0.1) },
        })
        .collect();
    
    let test_path = "large_dataset.parquet";
    
    // Write the large dataset
    wd.create_table_from_items(test_path, &large_data, EntryType::FileTablePhysical).await?;
    
    // Read it back
    let read_data: Vec<TestRecord> = wd.read_table_as_items(test_path).await?;
    
    // Verify all data is preserved
    assert_eq!(large_data.len(), read_data.len());
    
    // Spot check some records
    assert_eq!(large_data[0], read_data[0]);
    assert_eq!(large_data[1000], read_data[1000]);
    assert_eq!(large_data[2499], read_data[2499]);
    
    // Check that nullable fields are handled correctly
    let none_count_original = large_data.iter().filter(|r| r.score.is_none()).count();
    let none_count_read = read_data.iter().filter(|r| r.score.is_none()).count();
    assert_eq!(none_count_original, none_count_read);
    
    println!("✅ Large dataset batching successful!");
    println!("   Processed {} records with automatic batching", read_data.len());
    println!("   Nullable field handling: {} None values preserved", none_count_read);
    
    Ok(())
}

#[tokio::test]
async fn test_entry_type_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Test different entry types
    let test_data = vec![
        TestRecord { id: 1, name: "Entry1".to_string(), score: Some(100.0) },
        TestRecord { id: 2, name: "Entry2".to_string(), score: Some(200.0) },
    ];
    
    // Test with FileTable entry type
    let table_path = "table_entries.parquet";
    wd.create_table_from_items(table_path, &test_data, EntryType::FileTablePhysical).await?;
    
    // Test with FileData entry type
    let data_path = "data_entries.parquet";
    wd.create_table_from_items(data_path, &test_data, EntryType::FileDataPhysical).await?;
    
    // Verify both can be read back correctly
    let table_data: Vec<TestRecord> = wd.read_table_as_items(table_path).await?;
    let data_data: Vec<TestRecord> = wd.read_table_as_items(data_path).await?;
    
    assert_eq!(test_data, table_data);
    assert_eq!(test_data, data_data);
    
    println!("✅ Entry type integration successful!");
    println!("   Verified FileTable and FileData entry types work correctly");
    
    Ok(())
}
