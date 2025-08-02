//! File:Series CLI Integration Tests
//! 
//! Tests the complete CLI workflow for file:series data:
//! 1. Import CSV files to file:series paths using `copy` command
//! 2. View metadata using `show` command 
//! 3. Read all data using `cat` command across multiple versions

use tempfile::tempdir;
use std::fs;
use std::path::Path;

// Import the command functions directly
use cmd::commands::{init, copy, show, cat, mkdir};
use cmd::common::{FilesystemChoice, ShipContext};

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
}

/// Create test CSV files with sensor data across multiple time periods
fn create_test_csv_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
    // Version 1: Morning sensor readings
    let csv1_content = r#"timestamp,sensor_id,temperature,humidity
2024-01-15T08:00:00,sensor_001,22.5,45.2
2024-01-15T08:15:00,sensor_001,23.1,46.1
2024-01-15T08:30:00,sensor_002,21.8,44.7
2024-01-15T08:45:00,sensor_002,22.3,45.5"#;

    // Version 2: Afternoon sensor readings  
    let csv2_content = r#"timestamp,sensor_id,temperature,humidity
2024-01-15T14:00:00,sensor_001,26.2,42.1
2024-01-15T14:15:00,sensor_001,26.8,41.5
2024-01-15T14:30:00,sensor_002,25.9,43.2
2024-01-15T14:45:00,sensor_002,26.4,42.8"#;

    // Version 3: Evening sensor readings
    let csv3_content = r#"timestamp,sensor_id,temperature,humidity
2024-01-15T20:00:00,sensor_001,24.1,47.3
2024-01-15T20:15:00,sensor_001,23.7,48.1
2024-01-15T20:30:00,sensor_002,23.5,47.9
2024-01-15T20:45:00,sensor_002,23.2,48.5"#;

    let csv1_path = dir.join("morning_readings.csv");
    let csv2_path = dir.join("afternoon_readings.csv");
    let csv3_path = dir.join("evening_readings.csv");

    fs::write(&csv1_path, csv1_content)?;
    fs::write(&csv2_path, csv2_content)?;
    fs::write(&csv3_path, csv3_content)?;

    Ok(vec![csv1_path, csv2_path, csv3_path])
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

/// Helper function to show a path
async fn show_path(
    path: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "show".to_string(), 
        path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    show::show_command(&ship_context, FilesystemChoice::Data, |output| {
        println!("{}", output);
    }).await
}

/// Helper function to cat a path
async fn cat_path(
    path: &str,
    pond_path: Option<std::path::PathBuf>
) -> anyhow::Result<()> {
    let args = vec![
        "pond".to_string(),
        "cat".to_string(),
        path.to_string()
    ];
    let ship_context = ShipContext::new(pond_path, args);
    cat::cat_command_with_sql(&ship_context, path, FilesystemChoice::Data, "raw", None, None, None, None).await
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
async fn test_file_series_csv_import_workflow() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Create sensors directory
    mkdir_path("/sensors", Some(pond_path.clone())).await?;

    // Import each CSV file as a new version of the same file:series
    // This should create multiple versions of the same file:series with temporal metadata
    let series_path = "/sensors/temperature.series";

    println!("📥 Importing CSV files to file:series path: {}", series_path);

    // Version 1: Morning readings
    copy_file_with_format(
        csv_files[0].to_str().unwrap(),
        series_path,
        "series", // Special format to indicate file:series destination
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported morning readings (Version 1)");

    // Version 2: Afternoon readings  
    copy_file_with_format(
        csv_files[1].to_str().unwrap(),
        series_path,
        "series", // Same path creates new version
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported afternoon readings (Version 2)");

    // Version 3: Evening readings
    copy_file_with_format(
        csv_files[2].to_str().unwrap(),
        series_path,
        "series", // Same path creates new version
        Some(pond_path.clone())
    ).await?;
    println!("✅ Imported evening readings (Version 3)");

    // Show the file:series metadata
    println!("\n📊 Showing file:series metadata:");
    show_path(series_path, Some(pond_path.clone())).await?;

    // Cat all the data across all versions
    println!("\n📖 Reading all data from file:series:");
    cat_path(series_path, Some(pond_path.clone())).await?;

    println!("✅ File:series CLI workflow test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_file_series_multiple_series() -> Result<(), Box<dyn std::error::Error>> {
    // Setup test environment  
    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize pond
    init_command_with_pond(Some(pond_path.clone())).await?;

    // Create sensors directory
    mkdir_path("/sensors", Some(pond_path.clone())).await?;

    // Create two different file:series for temperature and humidity
    let temp_series = "/sensors/temperature.series";
    let humidity_series = "/sensors/humidity.series";

    // Import to temperature series
    copy_file_with_format(
        csv_files[0].to_str().unwrap(),
        temp_series,
        "series",
        Some(pond_path.clone())
    ).await?;

    copy_file_with_format(
        csv_files[1].to_str().unwrap(),
        temp_series,
        "series", 
        Some(pond_path.clone())
    ).await?;

    // Import to humidity series (same data but different series)
    copy_file_with_format(
        csv_files[0].to_str().unwrap(),
        humidity_series,
        "series",
        Some(pond_path.clone())
    ).await?;

    copy_file_with_format(
        csv_files[2].to_str().unwrap(),
        humidity_series,
        "series",
        Some(pond_path.clone())
    ).await?;

    // Show both series
    println!("📊 Temperature series metadata:");
    show_path(temp_series, Some(pond_path.clone())).await?;

    println!("\n📊 Humidity series metadata:");
    show_path(humidity_series, Some(pond_path.clone())).await?;

    // Cat data from both series
    println!("\n📖 Temperature series data:");
    cat_path(temp_series, Some(pond_path.clone())).await?;

    println!("\n📖 Humidity series data:");
    cat_path(humidity_series, Some(pond_path.clone())).await?;

    println!("✅ Multiple file:series test completed successfully");
    Ok(())
}

#[tokio::test] 
async fn test_file_series_show_command_clarity() -> Result<(), Box<dyn std::error::Error>> {
    // This test focuses on ensuring the `show` command provides clear,
    // legible information about file:series entries including:
    // - Entry type clearly marked as "file:series" 
    // - Temporal metadata (min/max event times)
    // - Version count and timestamp column information
    // - Extended attributes

    let (tmp_dir, pond_path) = setup_test_pond()?;
    let csv_files = create_test_csv_files(tmp_dir.path())?;

    // Initialize and setup
    init_command_with_pond(Some(pond_path.clone())).await?;
    
    mkdir_path("/data", Some(pond_path.clone())).await?;

    let series_path = "/data/sensor_readings.series";

    // Import multiple versions
    for (i, csv_file) in csv_files.iter().enumerate() {
        copy_file_with_format(
            csv_file.to_str().unwrap(),
            series_path,
            "series",
            Some(pond_path.clone())
        ).await?;
        println!("Imported version {} to {}", i + 1, series_path);
    }

    // The main test: show command should be clear and informative
    println!("\n=== SHOW COMMAND OUTPUT FOR FILE:SERIES ===");
    show_path(series_path, Some(pond_path.clone())).await?;
    println!("=== END SHOW COMMAND OUTPUT ===");

    // This test passes if show command runs without error
    // Visual inspection of output will validate clarity and completeness
    println!("✅ Show command clarity test completed");
    Ok(())
}
