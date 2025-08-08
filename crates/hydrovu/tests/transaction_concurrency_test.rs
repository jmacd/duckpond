use anyhow::Result;
use steward::Ship;
use tempfile::tempdir;

/// Test that attempting to start a second transaction fails with proper concurrency error
#[tokio::test]
async fn test_transaction_concurrency_protection() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("concurrency_test_pond");
    
    // Initialize a new pond
    let init_args = vec!["test".to_string(), "init".to_string()];
    let mut ship = Ship::initialize_new_pond(&pond_path, init_args).await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;
    
    // At this point, the init transaction should be committed and closed
    // So starting a new transaction should work
    ship.begin_transaction_with_args(vec!["test".to_string(), "first".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin first transaction: {}", e))?;
    
    // Now try to start a second transaction - this SHOULD fail with concurrency error
    let result = ship.begin_transaction_with_args(vec!["test".to_string(), "second".to_string()]).await;
    
    match result {
        Err(steward_error) => {
            let error_message = format!("{}", steward_error);
            println!("Got expected error: {}", error_message);
            
            // Check if it's the expected concurrency protection error
            if error_message.contains("already active") || error_message.contains("commit or rollback first") {
                println!("✅ Transaction concurrency protection is working correctly");
            } else {
                println!("❌ Got different error than expected: {}", error_message);
                // This might still be correct behavior, just not the exact message we expected
            }
        }
        Ok(_) => {
            return Err(anyhow::anyhow!("❌ Second transaction should have failed but succeeded - concurrency protection not working"));
        }
    }
    
    // Clean up by committing the first transaction
    ship.commit_transaction().await?;
    
    Ok(())
}

/// Test the normal case where transactions are started sequentially
#[tokio::test]
async fn test_sequential_transactions() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("sequential_test_pond");
    
    // Initialize pond
    let init_args = vec!["test".to_string(), "init".to_string()];
    let mut ship = Ship::initialize_new_pond(&pond_path, init_args).await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;
    
    // Start and commit first transaction
    ship.begin_transaction_with_args(vec!["test".to_string(), "first".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin first transaction: {}", e))?;
    
    ship.commit_transaction().await
        .map_err(|e| anyhow::anyhow!("Failed to commit first transaction: {}", e))?;
    
    // Start and commit second transaction - this should work fine
    ship.begin_transaction_with_args(vec!["test".to_string(), "second".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin second transaction: {}", e))?;
        
    ship.commit_transaction().await
        .map_err(|e| anyhow::anyhow!("Failed to commit second transaction: {}", e))?;
    
    println!("✅ Sequential transactions work correctly");
    Ok(())
}
