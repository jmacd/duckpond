use tempfile::tempdir;

// Import the command functions directly
use cmd::commands::{init, copy, mkdir, show};
use cmd::common::{FilesystemChoice, ShipContext};

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
}

/// Helper function for tests to show pond contents and return output as a string
async fn show_for_test(pond_path: Option<std::path::PathBuf>, filesystem: FilesystemChoice) -> anyhow::Result<String> {
    let args = vec!["pond".to_string(), "show".to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    
    let mut result = String::new();
    show::show_command(&ship_context, filesystem, |output| {
        result = output;
    }).await?;
    
    Ok(result)
}

/// Create test files in a temporary directory
fn create_test_files(dir: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file_a_path = dir.join("A");
    let file_b_path = dir.join("B");
    let file_c_path = dir.join("C");

    std::fs::write(&file_a_path, "Aaaaa\n")?;
    std::fs::write(&file_b_path, "Bbbbb\n")?;
    std::fs::write(&file_c_path, "Ccccc\n")?;

    Ok(vec![
        file_a_path.to_string_lossy().to_string(),
        file_b_path.to_string_lossy().to_string(),
        file_c_path.to_string_lossy().to_string(),
    ])
}

#[tokio::test]
async fn test_transaction_sequencing() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, pond_path) = setup_test_pond()?;
    let tmp_dir = tempdir()?;
    
    // Create test files
    let test_files = create_test_files(tmp_dir.path())?;
    
    // Execute the exact sequence from test.sh
    
    // Command 1: init
    let args1 = vec!["pond".to_string(), "init".to_string()];
    let ship_context1 = ShipContext::new(Some(pond_path.clone()), args1);
    init::init_command(&ship_context1).await?;
    
    // Command 2: copy /tmp/{A,B,C} /
    let mut args2 = vec!["pond".to_string(), "copy".to_string()];
    args2.extend(test_files.clone());
    args2.push("/".to_string());
    let ship_context2 = ShipContext::new(Some(pond_path.clone()), args2);
    let ship2 = ship_context2.create_ship_with_transaction().await?;
    copy::copy_command(ship2, &test_files, "/").await?;
    
    // Command 3: mkdir /ok
    let args3 = vec!["pond".to_string(), "mkdir".to_string(), "/ok".to_string()];
    let ship_context3 = ShipContext::new(Some(pond_path.clone()), args3);
    let ship3 = ship_context3.create_ship_with_transaction().await?;
    mkdir::mkdir_command(ship3, "/ok").await?;
    
    // Command 4: copy /tmp/{A,B,C} /ok
    let mut args4 = vec!["pond".to_string(), "copy".to_string()];
    args4.extend(test_files.clone());
    args4.push("/ok".to_string());
    let ship_context4 = ShipContext::new(Some(pond_path.clone()), args4);
    let ship4 = ship_context4.create_ship_with_transaction().await?;
    copy::copy_command(ship4, &test_files, "/ok").await?;
    
    // Get show output from data filesystem
    let show_output = show_for_test(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    
    println!("=== SHOW OUTPUT ===");
    println!("{}", show_output);
    println!("=== END OUTPUT ===");
    
    // Verify we have exactly 4 transactions by counting transaction headers
    let transaction_count = show_output.matches("Transaction #").count();
    assert_eq!(
        transaction_count, 
        4, 
        "Expected 4 transactions (1 init, 1 mkdir, 2 copies), but found {}. This indicates that commands are being batched into the same transaction instead of each creating its own transaction.",
        transaction_count
    );
    
    // Basic verification that we have separate transactions (don't care about exact format)
    assert!(show_output.contains("Transaction"), "Should have transaction output");
    assert!(transaction_count >= 4, "Should have at least 4 separate transactions");
    
    Ok(())
}
