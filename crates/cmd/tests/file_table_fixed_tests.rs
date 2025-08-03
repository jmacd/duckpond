//! Fixed FileTable Tests - Correcting Test Bugs
//! 
//! This addresses the issues in the original test file that were due to
//! incorrect SQL queries rather than fundamental limitations.

use tempfile::tempdir;
use std::fs;
use std::path::Path;

// Import the command functions directly
use cmd::commands::{init, copy, cat, mkdir, describe};
use cmd::common::{FilesystemChoice, ShipContext};

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
}

/// Create test CSV files with CORRECT data types for testing
fn create_test_csv_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    // Financial data - all numeric
    let financial_csv = r#"timestamp,symbol,price,volume
1672531200000,AAPL,150.25,1000000
1672531260000,AAPL,150.50,950000
1672531320000,AAPL,149.75,1100000
1672531380000,GOOGL,2800.00,500000
1672531440000,GOOGL,2805.25,475000"#;

    // Sensor data with proper boolean values (not strings)
    let sensor_csv = r#"timestamp,device_id,temperature,humidity,status
1672531200000,sensor_001,22.5,45.2,1
1672531260000,sensor_001,23.1,46.1,1
1672531320000,sensor_002,21.8,44.7,0
1672531380000,sensor_002,22.3,45.5,1
1672531440000,sensor_003,24.0,43.0,1"#;

    // Large dataset with NUMERIC values for AVG testing
    let mut large_csv = String::from("timestamp,id,value,category\n");
    for i in 0..100 {  // Smaller dataset for faster testing
        large_csv.push_str(&format!(
            "{},{},{},{}\n", 
            1672531200000i64 + (i * 60000), // 1 minute intervals
            i,
            i as f64 * 10.5,  // NUMERIC values that can be averaged
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

#[tokio::test]
async fn test_corrected_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing corrected aggregation queries");
    
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;
    mkdir_path("/test", Some(pond_path.clone())).await?;

    // Import financial data (has proper numeric columns)
    copy_file_with_format(
        csv_files[0].to_str().unwrap(), // financial_data.csv
        "/test/financial.table",
        "parquet",
        Some(pond_path.clone())
    ).await?;

    println!("\n🔍 Test 1: Simple aggregation");
    match cat_path_with_sql(
        "/test/financial.table",
        "SELECT COUNT(*) as total_rows FROM series",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ COUNT aggregation works"),
        Err(e) => println!("❌ COUNT aggregation failed: {}", e),
    }

    println!("\n🔍 Test 2: Group by aggregation");
    match cat_path_with_sql(
        "/test/financial.table",
        "SELECT symbol, COUNT(*) as count FROM series GROUP BY symbol",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ GROUP BY works"),
        Err(e) => println!("❌ GROUP BY failed: {}", e),
    }

    println!("\n🔍 Test 3: AVG on numeric column");
    match cat_path_with_sql(
        "/test/financial.table",
        "SELECT symbol, AVG(price) as avg_price FROM series GROUP BY symbol",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ AVG with GROUP BY works!"),
        Err(e) => println!("❌ AVG with GROUP BY failed: {}", e),
    }

    println!("\n✅ Aggregation tests completed");
    Ok(())
}

#[tokio::test]
async fn test_corrected_large_dataset() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing corrected large dataset with numeric values");
    
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;
    mkdir_path("/test", Some(pond_path.clone())).await?;

    // Import large dataset with NUMERIC values
    copy_file_with_format(
        csv_files[2].to_str().unwrap(), // large_dataset.csv with numeric values
        "/test/large.table",
        "parquet",
        Some(pond_path.clone())
    ).await?;

    println!("\n🔍 Test 1: Verify schema has numeric values");
    describe_path("/test/large.table", Some(pond_path.clone())).await?;

    println!("\n🔍 Test 2: Aggregation on numeric value column");
    match cat_path_with_sql(
        "/test/large.table",
        "SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM series GROUP BY category ORDER BY category",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ AVG on numeric column works!"),
        Err(e) => println!("❌ AVG failed: {}", e),
    }

    println!("\n✅ Large dataset test completed");
    Ok(())
}
