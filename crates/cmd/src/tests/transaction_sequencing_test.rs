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
    let args1 = vec!["pond".to_string(), "init".to_string()];
    init::init_command_with_pond_and_args(Some(pond_path.clone()), args1).await?;
    
    // Command 2: copy /tmp/{A,B,C} /
    let mut args2 = vec!["pond".to_string(), "copy".to_string()];
    args2.extend(test_files.clone());
    args2.push("/".to_string());
    copy::copy_command_with_pond_and_args(&test_files, "/", Some(pond_path.clone()), args2).await?;
    
    // Command 3: mkdir /ok
    let args3 = vec!["pond".to_string(), "mkdir".to_string(), "/ok".to_string()];
    mkdir::mkdir_command_with_pond_and_args("/ok", Some(pond_path.clone()), args3).await?;
    
    // Command 4: copy /tmp/{A,B,C} /ok
    let mut args4 = vec!["pond".to_string(), "copy".to_string()];
    args4.extend(test_files.clone());
    args4.push("/ok".to_string());
    copy::copy_command_with_pond_and_args(&test_files, "/ok", Some(pond_path.clone()), args4).await?;
    
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
    
    // Verify we have exactly 4 transactions by counting transaction headers
    let transaction_count = show_output.matches("=== Transaction #").count();
    assert_eq!(
        transaction_count,
        4,
        "Should have exactly 4 transactions, but found {}",
        transaction_count
    );

    // Verify we have the expected transaction numbers
    assert!(show_output.contains("=== Transaction #001 ==="));
    assert!(show_output.contains("=== Transaction #002 ==="));
    assert!(show_output.contains("=== Transaction #003 ==="));
    assert!(show_output.contains("=== Transaction #004 ==="));
    
    Ok(())
}