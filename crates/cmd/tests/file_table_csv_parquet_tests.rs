//! FileTable CSV-to-Parquet Integration Tests
//! 
//! Tests the complete CSV-to-table, table-to-Parquet cycle:
//! 1. Import CSV files to file:table paths using `copy --format=parquet`
//! 2. Query data using SQL via `cat` command with DataFusion
//! 3. Verify schema and metadata using `describe` command
//! 4. Compare with file:series behavior for consistency
//! 5. Test magic number detection for automatic format classification

use tempfile::tempdir;
use std::fs;
use std::path::Path;

// Import the command functions directly
use cmd::commands::{init, copy, show, cat, mkdir, describe};
use cmd::common::{FilesystemChoice, ShipContext};

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
}

/// Create test CSV files with different data types for comprehensive testing
fn create_test_csv_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    // Simple financial data with timestamps
    let financial_csv = r#"timestamp,symbol,price,volume
1672531200000,AAPL,150.25,1000000
1672531260000,AAPL,150.50,950000
1672531320000,AAPL,149.75,1100000
1672531380000,GOOGL,2800.00,500000
1672531440000,GOOGL,2805.25,475000"#;

    // Sensor data with different numeric types
    let sensor_csv = r#"timestamp,device_id,temperature,humidity,active
1672531200000,sensor_001,22.5,45.2,true
1672531260000,sensor_001,23.1,46.1,true
1672531320000,sensor_002,21.8,44.7,false
1672531380000,sensor_002,22.3,45.5,true
1672531440000,sensor_003,24.0,43.0,true"#;

    // Large dataset for performance testing
    let mut large_csv = String::from("timestamp,id,value,category\n");
    for i in 0..1000 {
        large_csv.push_str(&format!(
            "{},{},{},{}\n", 
            1672531200000i64 + (i * 60000), // 1 minute intervals
            i,
            i * 10,  // Numeric value, not string
            i % 5
        ));
    }

    let financial_path = dir.join("financial_data.csv");
    let sensor_path = dir.join("sensor_data.csv");
    let large_path = dir.join("large_dataset.csv");

    fs::write(&financial_path, financial_csv)?;
    fs::write(&sensor_path, sensor_csv)?;
    fs::write(&large_path, large_csv)?;

    Ok(vec![financial_path, sensor_path, large_path])
}

/// Helper function to initialize pond for testing
async fn init_command_with_pond(pond_path: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    let args = vec!["pond".to_string(), "init".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    init::init_command(&ship_context).await
}

/// Helper function to copy a file with specified format
async fn copy_file_with_format(
    source_path: &str, 
    dest_path: &str, 
    format: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(), 
        "copy".to_string(),
        "--format".to_string(),
        format.to_string(),
        source_path.to_string(), 
        dest_path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    let ship = ship_context.create_ship_with_transaction().await?;
    
    copy::copy_command(ship, &[source_path.to_string()], dest_path, format).await
}

/// Helper function to cat a path with table display
async fn cat_path_as_table(
    path: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "cat".to_string(),
        "--display".to_string(),
        "table".to_string(),
        path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    cat::cat_command_with_sql(&ship_context, path, FilesystemChoice::Data, "table", None, None, None, None).await
}

/// Helper function to cat a path with SQL query
async fn cat_path_with_sql(
    path: &str,
    query: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "cat".to_string(),
        path.to_string(),
        "--query".to_string(),
        query.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    cat::cat_command_with_sql(&ship_context, path, FilesystemChoice::Data, "raw", None, None, None, Some(query)).await
}

/// Helper function to describe a path
async fn describe_path(
    path: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "describe".to_string(),
        path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    describe::describe_command(&ship_context, path, FilesystemChoice::Data).await
}

/// Helper function to create a directory
async fn mkdir_path(
    path: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "mkdir".to_string(),
        path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    let ship = ship_context.create_ship_with_transaction().await?;
    mkdir::mkdir_command(ship, path).await
}

/// Helper function to show metadata
async fn show_metadata(
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "show".to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    show::show_command(&ship_context, FilesystemChoice::Data, |output| {
        println!("{}", output);
    }).await
}

// Note: Version replacement test removed - copy command doesn't support overwriting existing files
// This is a design limitation of the current system
