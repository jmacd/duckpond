use anyhow::Result;
use steward::Ship;
use tempfile::tempdir;

/// Test to debug Delta Lake version numbering and control filesystem transaction correspondence
#[tokio::test]
async fn test_debug_transaction_versions() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("debug_versions_pond");
    
    println!("=== Testing Delta Lake version progression ===");
    
    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path).await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;
    
    println!("✅ Pond initialized successfully");
    
    // Check what version we're at after initialization
    // We can't access get_committed_transaction_version directly since it's private,
    // but we can see what happens when we try the next transaction
    
    // Try first additional transaction
    println!("--- Starting first additional transaction ---");
    match ship.transact(
        vec!["test".to_string(), "debug-tx1".to_string()],
        |_tx, _fs| Box::pin(async move {
            println!("✅ First additional transaction started");
            Ok(())
        })
    ).await {
        Ok(_) => {
            println!("✅ First additional transaction committed successfully");
        }
        Err(e) => {
            println!("❌ First additional transaction failed: {}", e);
            return Err(anyhow::anyhow!("First transaction failed: {}", e));
        }
    }
    
    // Try second additional transaction
    println!("--- Starting second additional transaction ---");
    match ship.transact(
        vec!["test".to_string(), "debug-tx2".to_string()],
        |_tx, _fs| Box::pin(async move {
            println!("✅ Second additional transaction started");
            Ok(())
        })
    ).await {
        Ok(_) => {
            println!("✅ Second additional transaction committed successfully");
        }
        Err(e) => {
            println!("❌ Second additional transaction failed: {}", e);
            return Err(anyhow::anyhow!("Second transaction failed: {}", e));
        }
    }
    
    println!("=== All transactions completed successfully ===");
    Ok(())
}

/// Test to check Delta Lake table directly
#[tokio::test]
async fn test_delta_table_version_inspection() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("delta_inspection_pond");
    
    println!("=== Inspecting Delta Lake table versions directly ===");
    
    // Initialize pond
    let _ship = Ship::open_pond(&pond_path).await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;
    
    // Check Delta table version after init
    let data_path = pond_path.join("data");
    let data_path_str = format!("file://{}", data_path.display());
    
    println!("Delta table path: {}", data_path_str);
    
    match deltalake::DeltaTableBuilder::from_uri(&data_path_str).load().await {
        Ok(table) => {
            println!("✅ Delta table loaded after init, version: {}", table.version());
        }
        Err(e) => {
            println!("❌ Failed to load Delta table after init: {}", e);
        }
    }
    
    // Open pond again and do a transaction
    let mut ship2 = Ship::open_pond(&pond_path).await
        .map_err(|e| anyhow::anyhow!("Failed to reopen pond: {}", e))?;
    
    match ship2.transact(
        vec!["test".to_string(), "inspect-tx1".to_string()],
        |_tx, _fs| Box::pin(async move {
            // Transaction automatically commits
            Ok(())
        })
    ).await {
        Ok(_) => {
            println!("✅ Transaction committed");
            
            // Check version after commit
            match deltalake::DeltaTableBuilder::from_uri(&data_path_str).load().await {
                Ok(table) => {
                    println!("✅ Delta table version after first additional commit: {}", table.version());
                }
                Err(e) => {
                    println!("❌ Failed to load Delta table after commit: {}", e);
                }
            }
        }
        Err(e) => {
            println!("❌ Transaction commit failed: {}", e);
        }
    }
    
    Ok(())
}
