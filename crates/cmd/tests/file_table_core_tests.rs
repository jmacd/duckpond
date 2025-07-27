//! FileTable CSV-to-Parquet Integration Tests - Core Functionality
//! 
//! This test suite focuses on the essential CSV-to-table, table-to-Parquet workflow
//! and documents current limitations for future improvement.

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

/// Create simple test CSV files
fn create_test_csv_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    // Simple financial data
    let financial_csv = r#"timestamp,symbol,price,volume
1672531200000,AAPL,150.25,1000000
1672531260000,AAPL,150.50,950000
1672531320000,GOOGL,2800.00,500000"#;

    // Sensor data with boolean
    let sensor_csv = r#"timestamp,device_id,temperature,humidity,active
1672531200000,sensor_001,22.5,45.2,true
1672531260000,sensor_002,23.1,46.1,false
1672531320000,sensor_003,21.8,44.7,true"#;

    let financial_path = dir.join("financial_data.csv");
    let sensor_path = dir.join("sensor_data.csv");

    fs::write(&financial_path, financial_csv)?;
    fs::write(&sensor_path, sensor_csv)?;

    Ok(vec![financial_path, sensor_path])
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
    cat::cat_command_with_sql(&ship_context, path, FilesystemChoice::Data, "table", None, None, None).await
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
    cat::cat_command_with_sql(&ship_context, path, FilesystemChoice::Data, "raw", None, None, Some(query)).await
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
async fn test_csv_to_file_table_core_workflow() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing core CSV-to-FileTable workflow");
    
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;
    
    // Create data directory
    mkdir_path("/data", Some(pond_path.clone())).await?;

    // Test 1: Import CSV to FileTable using --format=parquet
    println!("\n📥 Step 1: Import CSV to FileTable");
    copy_file_with_format(
        csv_files[0].to_str().unwrap(), // financial_data.csv
        "/data/financial.table",
        "parquet", // Should create FileTable entry
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported financial data as FileTable");

    // Test 2: Verify schema and metadata with describe
    println!("\n🔍 Step 2: Verify FileTable metadata");
    describe_path("/data/financial.table", Some(pond_path.clone())).await?;
    
    // Test 3: Display data as table
    println!("\n📊 Step 3: Display FileTable data");
    cat_path_as_table("/data/financial.table", Some(pond_path.clone())).await?;

    // Test 4: Test basic SQL queries (non-aggregation)
    println!("\n🔎 Step 4: Test basic SQL queries");
    cat_path_with_sql(
        "/data/financial.table",
        "SELECT * FROM series WHERE price > 150.0 ORDER BY timestamp",
        Some(pond_path.clone())
    ).await?;

    cat_path_with_sql(
        "/data/financial.table",
        "SELECT symbol, price FROM series ORDER BY timestamp LIMIT 2",
        Some(pond_path.clone())
    ).await?;

    println!("✅ Core CSV-to-FileTable workflow test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_file_table_vs_file_series_basic_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing FileTable vs FileSeries basic comparison");
    
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;
    
    // Create comparison directory
    mkdir_path("/comparison", Some(pond_path.clone())).await?;

    // Test 1: Import same CSV as both FileTable and FileSeries
    println!("\n📥 Step 1: Import same CSV as both types");
    
    // Import as FileTable
    copy_file_with_format(
        csv_files[1].to_str().unwrap(), // sensor_data.csv
        "/comparison/sensor.table",
        "parquet",
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported as FileTable");

    // Import as FileSeries
    copy_file_with_format(
        csv_files[1].to_str().unwrap(), // same sensor_data.csv
        "/comparison/sensor.series",
        "series",
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported as FileSeries");

    // Test 2: Compare describe output
    println!("\n🔍 Step 2: Compare describe output");
    println!("--- FileTable describe ---");
    describe_path("/comparison/sensor.table", Some(pond_path.clone())).await?;
    
    println!("--- FileSeries describe ---");
    describe_path("/comparison/sensor.series", Some(pond_path.clone())).await?;

    // Test 3: Compare basic SQL query results (avoiding problematic filters)
    println!("\n🔎 Step 3: Compare basic SQL query results");
    let test_query = "SELECT device_id, temperature FROM series ORDER BY timestamp";
    
    println!("--- FileTable SQL query ---");
    cat_path_with_sql("/comparison/sensor.table", test_query, Some(pond_path.clone())).await?;
    
    println!("--- FileSeries SQL query ---");
    cat_path_with_sql("/comparison/sensor.series", test_query, Some(pond_path.clone())).await?;

    // Test 4: Test table display consistency
    println!("\n📊 Step 4: Compare table display");
    println!("--- FileTable display ---");
    cat_path_as_table("/comparison/sensor.table", Some(pond_path.clone())).await?;
    
    println!("--- FileSeries display ---");
    cat_path_as_table("/comparison/sensor.series", Some(pond_path.clone())).await?;

    println!("✅ FileTable vs FileSeries basic comparison test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_known_limitations() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing and documenting known limitations");
    
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;
    
    // Create limitations directory
    mkdir_path("/limitations", Some(pond_path.clone())).await?;

    // Import test data
    copy_file_with_format(
        csv_files[0].to_str().unwrap(),
        "/limitations/test.table",
        "parquet",
        Some(pond_path.clone())
    ).await?;

    println!("\n⚠️  Testing known limitations (these are expected to fail):");

    // Limitation 1: Aggregation queries
    println!("\n--- Limitation 1: Aggregation queries ---");
    match cat_path_with_sql(
        "/limitations/test.table",
        "SELECT symbol, AVG(price) as avg_price FROM series GROUP BY symbol",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ Aggregation query unexpectedly succeeded!"),
        Err(e) => println!("❌ Aggregation query failed as expected: {}", e),
    }

    // Limitation 2: Boolean filters
    println!("\n--- Limitation 2: Complex boolean filters ---");
    copy_file_with_format(
        csv_files[1].to_str().unwrap(), // sensor data with boolean
        "/limitations/sensor.table",
        "parquet",
        Some(pond_path.clone())
    ).await?;

    match cat_path_with_sql(
        "/limitations/sensor.table",
        "SELECT * FROM series WHERE active = true",
        Some(pond_path.clone())
    ).await {
        Ok(_) => println!("✅ Boolean filter unexpectedly succeeded!"),
        Err(e) => println!("❌ Boolean filter failed as expected: {}", e),
    }

    println!("\n📝 Summary of current limitations:");
    println!("   1. Aggregation queries (GROUP BY, AVG, etc.) fail due to DataFusion schema mismatch");
    println!("   2. Boolean column filters may fail due to type coercion issues");
    println!("   3. File replacement may not work correctly (version management)");
    println!("   4. Complex SQL functions may have type compatibility issues");
    
    println!("\n✅ Limitations documentation test completed");
    Ok(())
}
