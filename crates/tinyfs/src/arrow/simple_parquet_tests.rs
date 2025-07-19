//! Test for simple Parquet integration

use arrow_array::{RecordBatch, record_batch};
use crate::arrow::SimpleParquetExt;
use crate::memory::new_fs;
use crate::EntryType;

#[tokio::test]
async fn test_parquet_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Create a simple test batch using Arrow macros
    let batch = record_batch!(
        ("name", Utf8, ["Alice", "Bob", "Charlie"]),
        ("age", Int64, [25_i64, 30_i64, 35_i64])
    )?;
    
    // Test file path
    let test_path = "test_data.parquet";
    
    // Write the batch to parquet
    wd.write_parquet(test_path, &batch, EntryType::FileTable).await?;
    
    // Read it back
    let read_batch = wd.read_parquet(test_path).await?;
    
    // Verify the data is the same
    assert_eq!(batch.num_rows(), read_batch.num_rows());
    assert_eq!(batch.num_columns(), read_batch.num_columns());
    assert_eq!(batch.schema(), read_batch.schema());
    
    // Verify column data using downcast
    use arrow_array::{StringArray, Int64Array};
    let original_names = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let read_names = read_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(original_names, read_names);
    
    let original_ages = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let read_ages = read_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(original_ages, read_ages);
    
    println!("✅ Parquet roundtrip test successful!");
    println!("   Written and read {} rows with {} columns", read_batch.num_rows(), read_batch.num_columns());
    
    Ok(())
}

#[tokio::test]
async fn test_parquet_with_entry_type() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test filesystem and get root WD
    let fs = new_fs().await;
    let wd = fs.root().await?;
    
    // Create test data using Arrow macros (with nullable column)
    let batch = record_batch!(
        ("id", Int64, [1_i64, 2_i64, 3_i64]),
        ("value", Utf8, [Some("test1"), None, Some("test3")])
    )?;
    
    let test_path = "typed_data.parquet";
    
    // Write the batch
    wd.write_parquet(test_path, &batch, EntryType::FileTable).await?;
    
    // Verify we can read it back (entry type functionality is tested elsewhere)
    let read_batch = wd.read_parquet(test_path).await?;
    assert_eq!(batch.num_rows(), read_batch.num_rows());
    
    println!("✅ Parquet entry type test successful!");
    println!("   Note: Entry type verification requires additional metadata APIs");
    
    Ok(())
}
