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

#[tokio::test]
async fn test_copy_command_unit() -> Result<(), Box<dyn std::error::Error>> {
    use crate::commands::copy::copy_command;
    use tempfile::tempdir;
    use std::env;
    
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

    // Set up pond environment
    unsafe {
        env::set_var("POND", pond_path.to_string_lossy().to_string());
    }

    // Initialize the pond using the filesystem directly
    let store_path = pond_path.join("store");
    std::fs::create_dir_all(&store_path)?;
    let fs = tinylogfs::create_oplog_fs(&store_path.to_string_lossy()).await?;
    fs.commit().await?; // Initialize empty pond
    
    // Test copying files
    let sources = vec![
        file1_path.to_string_lossy().to_string(),
        file2_path.to_string_lossy().to_string(),
        file3_path.to_string_lossy().to_string(),
    ];
    
    // This should succeed without hanging
    copy_command(&sources, "/").await?;
    
    // Verify the files were copied by checking the filesystem
    let fs = tinylogfs::create_oplog_fs(&store_path.to_string_lossy()).await?;
    let root = fs.root().await?;
    
    // Check that all three files exist in the pond
    use tinyfs::Lookup;
    
    let file1_result = root.in_path("file1.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file1_result, "file1.txt should exist in pond");
    
    let file2_result = root.in_path("file2.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file2_result, "file2.txt should exist in pond");
    
    let file3_result = root.in_path("file3.txt", |_wd, lookup| async move {
        match lookup {
            Lookup::Found(_) => Ok(true),
            _ => Ok(false),
        }
    }).await?;
    assert!(file3_result, "file3.txt should exist in pond");

    Ok(())
}

#[test]
fn test_copy_command_integration_debug() -> Result<(), Box<dyn std::error::Error>> {
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

    eprintln!("DEBUG: Test setup complete, about to run init");

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

    eprintln!("DEBUG: Init completed successfully, about to run copy");

    // Step 2: Copy 3 temporary files from host FS into pond root
    let mut copy_cmd = Command::cargo_bin("pond")?;
    
    // Add a timeout to the copy command using std::process
    eprintln!("DEBUG: About to execute copy command");
    let copy_output = copy_cmd
        .arg("copy")
        .arg(file1_path.to_string_lossy().to_string())
        .arg(file2_path.to_string_lossy().to_string())
        .arg(file3_path.to_string_lossy().to_string())
        .arg("/")  // Copy to pond root directory
        .env("POND", pond_path.to_string_lossy().to_string())
        .output()?;

    eprintln!("DEBUG: Copy command completed");
    eprintln!("DEBUG: Copy stdout: {}", String::from_utf8_lossy(&copy_output.stdout));
    eprintln!("DEBUG: Copy stderr: {}", String::from_utf8_lossy(&copy_output.stderr));

    assert!(
        copy_output.status.success(),
        "Copy command failed: {}",
        String::from_utf8_lossy(&copy_output.stderr)
    );

    Ok(())
}
