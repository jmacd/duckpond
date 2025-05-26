use deltalake::open_table;
use oplog::delta::create_table;

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

    Ok(())
}
