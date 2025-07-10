use tempfile::tempdir;
use regex::Regex;

// Import the command functions directly
use crate::commands::{init, copy, mkdir, show};
use crate::common::FilesystemChoice;

/// Setup a test environment with a temporary pond
fn setup_test_pond() -> Result<(tempfile::TempDir, std::path::PathBuf), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    
    Ok((tmp, pond_path))
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
    init::init_command_with_pond(Some(pond_path.clone())).await?;
    
    // Command 2: copy /tmp/{A,B,C} /
    copy::copy_command_with_pond(&test_files, "/", Some(pond_path.clone())).await?;
    
    // Command 3: mkdir /ok
    mkdir::mkdir_command_with_pond("/ok", Some(pond_path.clone())).await?;
    
    // Command 4: copy /tmp/{A,B,C} /ok
    copy::copy_command_with_pond(&test_files, "/ok", Some(pond_path.clone())).await?;
    
    // Get show output from data filesystem
    let show_output = show::show_command_as_string_with_pond(Some(pond_path.clone()), FilesystemChoice::Data).await?;
    
    println!("=== SHOW OUTPUT ===");
    println!("{}", show_output);
    println!("=== END OUTPUT ===");
    
    // Parse transactions using regex
    let transaction_regex = Regex::new(r"=== Transaction #(\d+) ===")?;
    let transaction_matches: Vec<_> = transaction_regex.captures_iter(&show_output).collect();
    
    let transaction_count = transaction_matches.len();
    
    println!("Found {} transactions", transaction_count);
    
    // We expect 4 transactions:
    // 1. init
    // 2. copy /tmp/{A,B,C} /
    // 3. mkdir /ok  
    // 4. copy /tmp/{A,B,C} /ok
    assert_eq!(
        transaction_count, 
        4, 
        "Expected 4 transactions (1 init, 1 mkdir, 2 copies), but found {}. This indicates that commands are being batched into the same transaction instead of each creating its own transaction.",
        transaction_count
    );
    
    // Additional verification: check that transactions are numbered correctly
    for (index, cap) in transaction_matches.iter().enumerate() {
        let transaction_number: usize = cap[1].parse()?;
        let expected_number = index + 1;
        assert_eq!(
            transaction_number, 
            expected_number,
            "Transaction {} should be numbered {}, but found {}",
            index + 1,
            expected_number, 
            transaction_number
        );
    }
    
    // Verify summary shows correct counts
    assert!(show_output.contains("=== Summary ==="));
    
    let summary_regex = Regex::new(r"Transactions: (\d+)")?;
    if let Some(cap) = summary_regex.captures(&show_output) {
        let summary_transaction_count: usize = cap[1].parse()?;
        assert_eq!(
            summary_transaction_count,
            4,
            "Summary should show 4 transactions, but shows {}",
            summary_transaction_count
        );
    } else {
        panic!("Could not find transaction count in summary");
    }
    
    Ok(())
}