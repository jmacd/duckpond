use anyhow::Result;
use steward::{Ship, PondUserMetadata};
use tempfile::tempdir;

/// Test that a Ship can be properly dropped and a new Ship opened on the same pond
#[tokio::test]
async fn test_ship_drop_and_reopen() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("lifecycle_test_pond");

    // Initialize pond with first Ship instance
    {
        let _ship = Ship::create_pond(&pond_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

        // Ship goes out of scope and should be properly dropped here
    }

    // Now try to open the same pond with a new Ship instance
    let mut ship2 = Ship::open_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to reopen pond: {}", e))?;

    // Try to start a transaction on the reopened pond using scoped transactions
    let meta = PondUserMetadata::new(vec!["test".to_string(), "after-reopen".to_string()]);
    ship2
        .transact(
            &meta,
            |_tx, _fs| {
                Box::pin(async move {
                    // Transaction automatically commits on Ok
                    Ok(())
                })
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute transaction after reopen: {}", e))?;

    println!("✅ Ship drop and reopen works correctly");
    Ok(())
}

/// Test that keeping the same Ship instance and doing multiple transactions works
#[tokio::test]
async fn test_ship_multiple_transactions_same_instance() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("same_instance_test_pond");

    // Initialize pond and keep the Ship instance
    let mut ship = Ship::create_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Do first transaction on same Ship instance using scoped transactions
    let meta = PondUserMetadata::new(vec!["test".to_string(), "first-transaction".to_string()]);
    ship.transact(
        &meta,
        |_tx, _fs| {
            Box::pin(async move {
                // Transaction automatically commits on Ok
                Ok(())
            })
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to execute first transaction: {}", e))?;

    // Do second transaction on same Ship instance
    let meta = PondUserMetadata::new(vec!["test".to_string(), "second-transaction".to_string()]);
    ship.transact(
        &meta,
        |_tx, _fs| {
            Box::pin(async move {
                // Transaction automatically commits on Ok
                Ok(())
            })
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to execute second transaction: {}", e))?;

    println!("✅ Multiple transactions on same Ship instance work correctly");
    Ok(())
}
