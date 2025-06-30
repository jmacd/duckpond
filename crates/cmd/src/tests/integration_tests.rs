use assert_cmd::prelude::*;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_pond_init_and_show() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");

    // Get the path to the pond binary
    let mut cmd = Command::cargo_bin("pond")?;

    // Test init command
    let output = cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Check that init succeeded
    assert!(
        output.status.success(),
        "Init command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Initializing pond at:"));
    assert!(stdout.contains("✅ Pond initialized successfully"));

    // Verify the store directory was created
    let store_path = pond_path.join("store");
    assert!(store_path.exists(), "Store directory was not created");

    // Test show command
    let mut show_cmd = Command::cargo_bin("pond")?;
    let show_output = show_cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Check that show succeeded
    assert!(
        show_output.status.success(),
        "Show command failed: {}",
        String::from_utf8_lossy(&show_output.stderr)
    );

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);
    assert!(show_stdout.contains("=== DuckPond Operation Log ==="));
    assert!(show_stdout.contains("=== Summary ==="));
    assert!(show_stdout.contains("Transactions:"));
    assert!(show_stdout.contains("Entries:"));

    // Test that init fails if run again
    let mut init_again_cmd = Command::cargo_bin("pond")?;
    let init_again_output = init_again_cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Should fail with error
    assert!(
        !init_again_output.status.success(),
        "Init should fail when pond already exists"
    );
    let init_again_stderr = String::from_utf8_lossy(&init_again_output.stderr);
    assert!(init_again_stderr.contains("Pond already exists"));

    Ok(())
}

#[test]
fn test_show_without_init() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("nonexistent_pond");

    // Test show command on non-existent pond
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    // Should fail with error
    assert!(
        !output.status.success(),
        "Show should fail when pond doesn't exist"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Pond does not exist"));
    assert!(stderr.contains("Run 'pond init' first"));

    Ok(())
}

#[test]
fn test_missing_pond_env() -> Result<(), Box<dyn std::error::Error>> {
    // Test init command without POND environment variable
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd.arg("init").env_remove("POND").output()?;

    // Should fail with error
    assert!(
        !output.status.success(),
        "Init should fail when POND env var is not set"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("POND environment variable not set"));

    Ok(())
}

#[test]
fn test_help_command() -> Result<(), Box<dyn std::error::Error>> {
    // Test help command
    let mut cmd = Command::cargo_bin("pond")?;
    let output = cmd.arg("--help").output()?;

    // Should succeed
    assert!(output.status.success(), "Help command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pond"));
    assert!(stdout.contains("init"));
    assert!(stdout.contains("show"));

    Ok(())
}

#[test]
fn test_copy_command_integration() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let pond_path = tmp.path().join("test_pond");
    let temp_files_dir = tmp.path().join("temp_files");

    // Create temporary directory for source files
    std::fs::create_dir_all(&temp_files_dir)?;

    // Create 3 temporary files with different content
    let file1_path = temp_files_dir.join("file1.txt");
    let file2_path = temp_files_dir.join("file2.txt");
    let file3_path = temp_files_dir.join("file3.txt");

    std::fs::write(&file1_path, "Content of file1")?;
    std::fs::write(&file2_path, "Content of file2")?;
    std::fs::write(&file3_path, "Content of file3")?;

    // Step 1: Initialize a new pond
    let mut init_cmd = Command::cargo_bin("pond")?;
    let init_output = init_cmd
        .arg("init")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        init_output.status.success(),
        "Init command failed: {}",
        String::from_utf8_lossy(&init_output.stderr)
    );

    let init_stdout = String::from_utf8_lossy(&init_output.stdout);
    assert!(init_stdout.contains("✅ Pond initialized successfully"));

    // Step 2: Copy 3 temporary files from host FS into pond root
    let mut copy_cmd = Command::cargo_bin("pond")?;
    let copy_output = copy_cmd
        .arg("copy")
        .arg(file1_path.to_string_lossy().to_string())
        .arg(file2_path.to_string_lossy().to_string())
        .arg(file3_path.to_string_lossy().to_string())
        .arg("/")  // Copy to pond root directory
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        copy_output.status.success(),
        "Copy command failed: {}",
        String::from_utf8_lossy(&copy_output.stderr)
    );

    let copy_stdout = String::from_utf8_lossy(&copy_output.stdout);
    assert!(copy_stdout.contains("file1.txt"));
    assert!(copy_stdout.contains("file2.txt"));
    assert!(copy_stdout.contains("file3.txt"));

    // Step 3: Use "list" with a pattern that matches all three files
    let mut list_cmd = Command::cargo_bin("pond")?;
    let list_output = list_cmd
        .arg("list")
        .arg("*") // Pattern to match all files in root
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        list_output.status.success(),
        "List command failed: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(list_stdout.contains("file1.txt"));
    assert!(list_stdout.contains("file2.txt"));
    assert!(list_stdout.contains("file3.txt"));

    // Verify that the listing shows file size information
    assert!(list_stdout.contains("15")); // "Content of file1" is 15 bytes
    assert!(list_stdout.contains("15")); // "Content of file2" is 15 bytes
    assert!(list_stdout.contains("15")); // "Content of file3" is 15 bytes

    // Step 4: Use show to print the log
    let mut show_cmd = Command::cargo_bin("pond")?;
    let show_output = show_cmd
        .arg("show")
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    assert!(
        show_output.status.success(),
        "Show command failed: {}",
        String::from_utf8_lossy(&show_output.stderr)
    );

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);

    // Step 5: Verify that show output listing is correct

    // Should see operation log header
    assert!(show_stdout.contains("=== DuckPond Operation Log ==="));

    // Should see summary section
    assert!(show_stdout.contains("=== Summary ==="));
    assert!(show_stdout.contains("Transactions:"));
    assert!(show_stdout.contains("Entries:"));

    // Should be able to see two transactions (init, copy)
    // The summary should show 2 transactions
    assert!(show_stdout.contains("Transactions: 2"));

    // Should see entries for the operations
    // Init creates 1 entry (root directory), copy creates 3 entries (3 files)
    // So we should see 4 entries total
    assert!(show_stdout.contains("Entries: 4"));

    // The copy entry in the log should contain the three file names
    assert!(show_stdout.contains("file1.txt"));
    assert!(show_stdout.contains("file2.txt"));
    assert!(show_stdout.contains("file3.txt"));

    // Should see CreateFile operations in the log
    assert!(show_stdout.contains("CreateFile"));

    // Should see three new nodes created (one for each file)
    // The output should show node IDs for the created files
    let node_count = show_stdout.matches("CreateFile").count();
    assert_eq!(node_count, 3, "Should have 3 CreateFile operations");

    Ok(())
}
