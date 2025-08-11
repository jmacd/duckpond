use anyhow::Result;
use cmd::commands::{init, show, copy};
use cmd::common::{ShipContext, FilesystemChoice};
use tempfile::tempdir;
use std::path::PathBuf;

#[tokio::test] 
async fn test_large_file_query_fallback() -> Result<()> {
    // This test verifies that large files are displayed correctly in the show command
    // and don't show as 0-byte entries
    
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("test-pond");
    
    // Initialize a pond using the command function directly
    let ship_context = ShipContext::new(Some(PathBuf::from(pond_path)), vec![]); // Empty original_args for tests
    
    // Initialize pond
    init::init_command(&ship_context).await?;
    
    // Create a large CSV file in temp directory (>64KB to trigger external storage)
    let large_csv_path = temp_dir.path().join("large_test.csv");
    let mut large_csv_content = String::from("timestamp,value\n");
    for i in 0..4000 {  // Increased from 2000 to 4000 to ensure >64KB
        large_csv_content.push_str(&format!("167253{:04}000,{}.5\n", i, i));
    }
    std::fs::write(&large_csv_path, large_csv_content)?;

    // Copy the large file to pond using copy command
    {
        let ship = ship_context.create_ship_with_transaction().await?;
        copy::copy_command(
            ship,
            &[large_csv_path.to_str().unwrap().to_string()],
            "/data/large_test.series",
            "data",
        ).await?;
    }

    // Run the show command and capture output (with proper transaction management)
    let show_output = {
        let _ship = ship_context.create_ship_with_transaction().await?;
        
        let mut output = String::new();
        show::show_command(&ship_context, FilesystemChoice::Data, |content| {
            output.push_str(&content);
        }).await?;
        
        // Show command doesn't modify data, so we can just let the transaction complete
        output
    };
    
    println!("Show command output:\n{}", show_output);
    
    // Verify the output contains indicators of a large file
    // Note: CSV files with format="series" are currently stored as FileData, not FileSeries
    // due to copy command logic that only allows Parquet files to be FileSeries
    assert!(show_output.contains("FileData"), "Should show FileData entry type (CSV files are stored as FileData even with series format)");
    assert!(show_output.contains("Large file") || show_output.contains("bytes"), "Should indicate file size");
    
    // Verify there are no 0-byte entries (which was the bug)
    assert!(!show_output.contains("(0 bytes)"), "Should not show 0-byte entries");
    assert!(!show_output.contains("0 bytes"), "Should not show 0 bytes anywhere");
    
    // Verify it shows actual size information
    assert!(show_output.contains("bytes"), "Should show byte size information");
    
    println!("✅ Large file test passed - show command correctly displays large file metadata");
    
    Ok(())
}
