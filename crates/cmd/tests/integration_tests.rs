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
    assert!(show_stdout.contains("=== Pond Contents ==="));
    assert!(show_stdout.contains("Total entries:"));

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
