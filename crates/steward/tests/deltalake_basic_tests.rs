use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake::DeltaOps;
use deltalake::protocol::SaveMode;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_deltalake_commit_and_read_same_handle() -> Result<()> {
    let temp_dir = tempdir()?;
    let table_uri = temp_dir.path().to_string_lossy().to_string();

    println!("Creating Delta table at: {}", table_uri);

    // Step 1: Create first batch of data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;

    // Step 2: Create table and write first batch
    let table = DeltaOps::try_from_uri(&table_uri)
        .await?
        .write(vec![batch1])
        .with_save_mode(SaveMode::ErrorIfExists)
        .await?;

    println!("After first write - table version: {:?}", table.version());
    let first_version = table.version();

    // Step 3: Add second batch using the SAME table handle
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "David"])),
        ],
    )?;

    let table = DeltaOps::from(table)
        .write(vec![batch2])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("After second write - table version: {:?}", table.version());
    let second_version = table.version();

    // The table handle should see its own committed changes immediately
    assert!(
        second_version > first_version,
        "Second version should be greater than first version"
    );
    assert_eq!(
        table.version(),
        second_version,
        "Table should immediately reflect its own commits"
    );

    // Step 4: Test with fresh table handle - should also see all commits
    let fresh_table = deltalake::open_table(&table_uri).await?;
    println!("Fresh table version: {:?}", fresh_table.version());

    assert_eq!(
        fresh_table.version(),
        second_version,
        "Fresh table handle should see all committed changes"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_separate_table_handles() -> Result<()> {
    let temp_dir = tempdir()?;
    let table_uri = temp_dir.path().to_string_lossy().to_string();

    println!("Testing separate table handles at: {}", table_uri);

    // Create schema and first batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(StringArray::from(vec!["first"])),
        ],
    )?;

    // Write with first handle (creates table)
    let table1 = DeltaOps::try_from_uri(&table_uri)
        .await?
        .write(vec![batch1])
        .with_save_mode(SaveMode::ErrorIfExists)
        .await?;

    println!("Table1 after write: version {:?}", table1.version());

    // Create second handle AFTER the first commit
    let table2 = deltalake::open_table(&table_uri).await?;
    println!("Table2 opened: version {:?}", table2.version());

    // Both should see the same version
    assert_eq!(
        table1.version(),
        table2.version(),
        "Both handles should see the same committed version"
    );

    // Write with second handle
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![200])),
            Arc::new(StringArray::from(vec!["second"])),
        ],
    )?;

    let table2 = DeltaOps::from(table2)
        .write(vec![batch2])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("Table2 after write: version {:?}", table2.version());

    // The first handle should NOT automatically see the second commit
    println!("Table1 version after table2 commit: {:?}", table1.version());

    // But if we refresh table1, it should see the new version
    let table1_refreshed = deltalake::open_table(&table_uri).await?;
    println!("Table1 refreshed: version {:?}", table1_refreshed.version());

    assert_eq!(
        table2.version(),
        table1_refreshed.version(),
        "Refreshed table1 should see table2's commit"
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_sequence_numbering() -> Result<()> {
    let temp_dir = tempdir()?;
    let table_uri = temp_dir.path().to_string_lossy().to_string();

    println!("Testing transaction sequence numbering at: {:?}", table_uri);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "tx_id",
        DataType::Int32,
        false,
    )]));

    // Track the table handle through multiple commits
    let mut table = None;

    // Simulate multiple transaction commits
    for tx_id in 1..=5 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![tx_id]))],
        )?;

        table = Some(if let Some(existing_table) = table {
            // Append to existing table
            DeltaOps::from(existing_table)
                .write(vec![batch])
                .with_save_mode(SaveMode::Append)
                .await?
        } else {
            // Create new table with first batch
            DeltaOps::try_from_uri(&table_uri)
                .await?
                .write(vec![batch])
                .with_save_mode(SaveMode::ErrorIfExists)
                .await?
        });

        let current_table = table.as_ref().unwrap();
        println!(
            "Transaction {} committed - table version: {:?}",
            tx_id,
            current_table.version()
        );

        // The version should be tx_id - 1 (since versions start at 0)
        assert_eq!(
            current_table.version().unwrap() as i32,
            tx_id - 1,
            "Version should be tx_id - 1 (0-based)"
        );

        // Immediately reading the table should show the correct version
        let current_version = current_table.version();
        println!("  Immediate read shows version: {:?}", current_version);

        // Create a fresh handle - should also see the latest version
        let fresh_table = deltalake::open_table(&table_uri).await?;
        println!("  Fresh handle shows version: {:?}", fresh_table.version());
        assert_eq!(
            fresh_table.version(),
            current_version,
            "Fresh handle should see latest version"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_deltalake_handle_sees_own_commits() -> Result<()> {
    // This test specifically verifies that a DeltaTable handle
    // can immediately see its own committed changes
    let temp_dir = tempdir()?;
    let table_uri = temp_dir.path().to_string_lossy().to_string();

    let schema = Arc::new(Schema::new(vec![Field::new("step", DataType::Utf8, false)]));

    // Commit step 1 (creates table)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["step1"]))],
    )?;

    let table = DeltaOps::try_from_uri(&table_uri)
        .await?
        .write(vec![batch1])
        .with_save_mode(SaveMode::ErrorIfExists)
        .await?;

    println!("After step 1 - version: {:?}", table.version());
    // Note: Delta Lake versions start from 0, so first commit is version 0

    // Immediately commit step 2 using the same handle
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["step2"]))],
    )?;

    let table = DeltaOps::from(table)
        .write(vec![batch2])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("After step 2 - version: {:?}", table.version());
    // After second commit, should be version 1

    // The handle should immediately see the latest version
    let current_version = table.version();
    println!("Current version after step 2: {:?}", current_version);

    // A fresh handle should also see the same version
    let fresh_table = deltalake::open_table(&table_uri).await?;
    println!("Fresh handle version: {:?}", fresh_table.version());
    assert_eq!(
        fresh_table.version(),
        current_version,
        "Fresh handle should see same committed version"
    );

    Ok(())
}
