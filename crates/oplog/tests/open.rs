use deltalake::open_table;
use oplog::delta::{create_table, ByteStreamTable};

use tempfile::tempdir;
use datafusion::prelude::*;
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema};

#[tokio::test]
async fn test_adminlog() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let table_path = tmp.path().join("admin_table").to_string_lossy().to_string();
    println!("Creating Delta Lake table at: {}", &table_path);

    // Create initial empty table if it doesn't exist
    open_table(&table_path).await.expect_err("not found");

    // Initialize the table with the schema
    create_table(&table_path).await?;

    Ok(())
}

#[tokio::test]
async fn test_bytestream_table() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let table_path = tmp.path().join("test_table").to_string_lossy().to_string();
    
    println!("Creating Delta Lake table for ByteStreamTable test at: {}", &table_path);

    // Create the Delta Lake table with test data
    create_table(&table_path).await?;

    // Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Create schema for the inner Entry data
    let entry_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("node_id", DataType::Utf8, false),
    ]));
    
    // Register our custom ByteStreamTable
    let byte_stream_table = Arc::new(ByteStreamTable::new(entry_schema, table_path));
    ctx.register_table("entries", byte_stream_table)?;
    
    // Query the table to read the nested Entry data
    let df = ctx.sql("SELECT * FROM entries").await?;
    let results = df.collect().await?;
    
    println!("ByteStreamTable query results:");
    for batch in &results {
        println!("{}", arrow::util::pretty::pretty_format_batches(&[batch.clone()])?);
    }
    
    // Verify we got some data
    assert!(!results.is_empty(), "Should have received some data from ByteStreamTable");
    
    if let Some(first_batch) = results.first() {
        assert!(first_batch.num_rows() > 0, "Should have at least one row");
        assert_eq!(first_batch.num_columns(), 2, "Should have 2 columns (name, node_id)");
    }

    Ok(())
}
