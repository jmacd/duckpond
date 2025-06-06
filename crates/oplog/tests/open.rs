use deltalake::open_table;
use oplog::tinylogfs::create_oplog_table;
use oplog::content::ContentTable;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_adminlog() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let table_path = tmp.path().join("admin_table").to_string_lossy().to_string();
    println!("Creating Delta Lake table at: {}", &table_path);

    // Create initial empty table if it doesn't exist
    open_table(&table_path).await.expect_err("not found");

    // Initialize the table with the schema
    create_oplog_table(&table_path).await?;

    Ok(())
}

#[tokio::test]
async fn test_content_table() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let table_path = tmp.path().join("test_table").to_string_lossy().to_string();

    println!(
        "Creating Delta Lake table for ContentTable test at: {}",
        &table_path
    );

    // Create the Delta Lake table with test data
    create_oplog_table(&table_path).await?;

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Create schema for the inner Entry data
    let entry_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("node_id", DataType::Utf8, false),
    ]));

    // Register our custom ContentTable
    let byte_stream_table = Arc::new(ContentTable::new(entry_schema, table_path));
    ctx.register_table("entries", byte_stream_table)?;

    // Instead of using SQL, create a DataFrame directly from the table
    // and apply filters using DataFusion's programmatic API
    use datafusion::logical_expr::{col, lit};

    // Create a DataFrame from the registered table
    let table_provider = ctx.table("entries").await?;

    // Demonstrate various programmatic DataFusion operations
    let filtered_df = table_provider
        .filter(col("node_id").eq(lit("0000000000000000")))?
        .select(vec![col("name"), col("node_id")])?
        .limit(0, Some(10))?;

    let results = filtered_df.collect().await?;

    println!("ContentTable query results:");
    for batch in &results {
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
        );
    }

    // Verify we got some data
    assert!(
        !results.is_empty(),
        "Should have received some data from ContentTable"
    );

    if let Some(first_batch) = results.first() {
        assert!(first_batch.num_rows() > 0, "Should have at least one row");
        assert_eq!(
            first_batch.num_columns(),
            2,
            "Should have 2 columns (name, node_id)"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_delta_record_filtering() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let table_path = tmp
        .path()
        .join("filter_test_table")
        .to_string_lossy()
        .to_string();

    println!(
        "Creating Delta Lake table for direct record filtering at: {}",
        &table_path
    );

    // Create the Delta Lake table with test data
    create_oplog_table(&table_path).await?;

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Register the Delta Lake table directly (not the ContentTable)
    let delta_table = deltalake::open_table(&table_path).await?;
    ctx.register_table("raw_records", Arc::new(delta_table))?;

    // Query the raw Delta Lake records using programmatic DataFusion API
    use datafusion::logical_expr::{col, lit};

    let table = ctx.table("raw_records").await?;

    // Filter by specific part_id and version using the outer Record schema
    let filtered_df = table
        .filter(col("part_id").eq(lit("0000000000000000")))? // Filter by specific node_id
        .filter(col("version").eq(lit(0i64)))? // Filter by specific version
        .select(vec![
            col("part_id"),
            col("timestamp"),
            col("version"),
            col("content"),
        ])?;

    let results = filtered_df.collect().await?;

    println!("Delta Lake record filtering results:");
    for batch in &results {
        println!("Schema: {:?}", batch.schema());
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
        );
    }

    // Verify we got the expected data
    assert!(!results.is_empty(), "Should have received filtered data");

    let first_batch = results.first().unwrap();
    assert!(first_batch.num_rows() > 0, "Should have at least one row");
    assert_eq!(
        first_batch.num_columns(),
        4,
        "Should have 4 columns (node_id, timestamp, version, content)"
    );

    // Verify the filtered node_id - handle dictionary array
    let node_id_array = first_batch.column(0); // node_id is first column
    if let Some(dict_array) = node_id_array
        .as_any()
        .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>()
    {
        let values = dict_array
            .values()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let key = dict_array.key(0).unwrap();
        assert_eq!(values.value(key as usize), "0000000000000000");
    } else {
        panic!("Expected dictionary array for node_id");
    }

    // Verify the filtered version - version is third column (0-indexed: 0=node_id, 1=timestamp, 2=version)
    let version_array = first_batch.column(2);
    let version_array = version_array
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(version_array.value(0), 0);

    Ok(())
}
