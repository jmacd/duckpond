// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use log::debug;
use steward::{PondUserMetadata, Ship};
use tempfile::tempdir;

/// Test to debug Delta Lake version numbering and control filesystem transaction correspondence
#[tokio::test]
async fn test_debug_transaction_versions() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("debug_versions_pond");

    debug!("=== Testing Delta Lake version progression ===");

    // Initialize pond
    let mut ship = Ship::create_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    debug!("✅ Pond initialized successfully");

    // Check what version we're at after initialization
    // We can't access get_committed_transaction_version directly since it's private,
    // but we can see what happens when we try the next transaction

    // Try first additional transaction
    debug!("--- Starting first additional transaction ---");
    let meta = PondUserMetadata::new(vec!["test".to_string(), "debug-tx1".to_string()]);
    match ship
        .write_transaction(&meta, async |_fs| {
            debug!("✅ First additional transaction started");
            Ok(())
        })
        .await
    {
        Ok(_) => {
            debug!("✅ First additional transaction committed successfully");
        }
        Err(e) => {
            debug!("❌ First additional transaction failed: {}", e);
            return Err(anyhow::anyhow!("First transaction failed: {}", e));
        }
    }

    // Try second additional transaction
    debug!("--- Starting second additional transaction ---");
    let meta = PondUserMetadata::new(vec!["test".to_string(), "debug-tx2".to_string()]);
    match ship
        .write_transaction(&meta, async |_fs| {
            debug!("✅ Second additional transaction started");
            Ok(())
        })
        .await
    {
        Ok(_) => {
            debug!("✅ Second additional transaction committed successfully");
        }
        Err(e) => {
            debug!("❌ Second additional transaction failed: {}", e);
            return Err(anyhow::anyhow!("Second transaction failed: {}", e));
        }
    }

    debug!("=== All transactions completed successfully ===");
    Ok(())
}

/// Test to check Delta Lake table directly
#[tokio::test]
async fn test_delta_table_version_inspection() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("delta_inspection_pond");

    debug!("=== Inspecting Delta Lake table versions directly ===");

    // Initialize pond
    let _ship = Ship::create_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize pond: {}", e))?;

    // Check Delta table version after init
    let data_path = pond_path.join("data");
    let data_path_str = format!("file://{}", data_path.display());

    debug!("Delta table path: {}", data_path_str);

    let url = url::Url::parse(&data_path_str)?;
    let builder = deltalake::DeltaTableBuilder::from_uri(url)?;
    match builder.load().await {
        Ok(table) => {
            debug!(
                "✅ Delta table loaded after init, version: {:?}",
                table.version()
            );
        }
        Err(e) => {
            debug!("❌ Failed to load Delta table after init: {:?}", e);
        }
    }

    // Open pond again and do a transaction
    let mut ship2 = Ship::open_pond(&pond_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to reopen pond: {}", e))?;

    let meta = PondUserMetadata::new(vec!["test".to_string(), "inspect-tx1".to_string()]);
    match ship2
        .write_transaction(&meta, async |_fs| {
            // Transaction automatically commits
            Ok(())
        })
        .await
    {
        Ok(_) => {
            debug!("✅ Transaction committed");

            // Check version after commit
            let url = url::Url::parse(&data_path_str)?;
            let builder = deltalake::DeltaTableBuilder::from_uri(url)?;
            match builder.load().await {
                Ok(table) => {
                    debug!(
                        "✅ Delta table version after first additional commit: {:?}",
                        table.version()
                    );
                }
                Err(e) => {
                    debug!("❌ Failed to load Delta table after commit: {}", e);
                }
            }
        }
        Err(e) => {
            debug!("❌ Transaction commit failed: {}", e);
        }
    }

    Ok(())
}
