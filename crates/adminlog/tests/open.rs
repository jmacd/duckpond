use deltalake::open_table;
use adminlog::delta::create_table;

use tempfile::tempdir;

#[tokio::test]
async fn test_adminlog() -> Result<(), Box<dyn std::error::Error>> {
    //let tmp = tempdir()?;
    //let table_path = tmp.path().to_string_lossy().to_string();
    let table_path = "hello".to_string();
    println!("Creating Delta Lake table at: {}", &table_path);

    // Create initial empty table if it doesn't exist
    open_table(&table_path).await.expect_err("not found");

    // Initialize the table with the schema
    create_table(&table_path).await?;

    // // Add initial log entry
    // add_log_entry(&table_path, "Table created").await?;

    // // Read the latest message and append a new one
    // let last_rec = get_last_record(&table_path).await?;

    // assert_eq!(last_rec.message, "Table created");

    // // Add a new log entry
    // add_log_entry(&table_path, "System check performed").await?;

    // // Read again to confirm our change
    // let updated = get_last_record(&table_path).await?;

    // assert_eq!(updated.message, "System check performed");

    // add_log_entry(&table_path, "Maintenance scheduled").await?;

    // // Display table history
    // let table = open_table(&table_path).await?;
    // println!("Table version: {}", table.version());

    Ok(())
}
