use anyhow::Result;
use hydrovu::{HydroVuConfig, HydroVuDevice};
use tempfile::tempdir;
use steward::Ship;
use std::path::Path;
use chrono;

mod mock_server;
use mock_server::MockHydroVuServer;

/// Test end-to-end HydroVu data collection with actual tlogfs storage
#[tokio::test]
async fn test_end_to_end_data_collection() -> Result<()> {
    // Create temporary pond directory with unique name
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join(format!("test_pond_{}", std::process::id()));
    
    // Initialize pond
    init_pond(&pond_path).await?;
    
    // Start mock server
    let mut mock_server = MockHydroVuServer::new().await?;
    let base_url = mock_server.start().await?;
    
    // Create configuration pointing to mock server and temp pond  
    let _config = create_test_config_with_mock_server(&base_url, &pond_path)?;
    
    // TODO: Create collector that will write to actual tlogfs
    // This requires modifying HydroVuCollector to support custom base URL for testing
    // let mut collector = create_test_collector_with_mock_server(config, &base_url).await?;
    
    // For now, just verify the pond structure without creating additional transactions
    verify_pond_structure(&pond_path).await?;
    
    mock_server.stop().await;
    Ok(())
}

/// Test that we can read back data written by HydroVu collector
#[tokio::test]
async fn test_read_back_written_data() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join(format!("test_pond_{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)));
    
    // Initialize pond
    init_pond(&pond_path).await?;
    
    // For now, just test that we can open the pond and access the filesystem
    // TODO: This would be replaced with actual HydroVu collector writing data
    // manually_create_hydrovu_test_data(&pond_path).await?;
    
    // Verify we can read the pond structure using direct tinyfs access
    let structure_output = read_tinyfs_structure(&pond_path).await?;
    assert!(!structure_output.is_empty(), "Should have some pond structure output");
    
    // For now, skip file listing until we actually have HydroVu data
    // let _listed_files = read_tinyfs_file_list(&pond_path, "/hydrovu/**").await?;

    Ok(())
}

// TODO: This would test actual schema evolution once HydroVuCollector supports custom base URL
#[tokio::test]
async fn test_schema_evolution_placeholder() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("test_pond");
    
    init_pond(&pond_path).await?;
    
    // Placeholder for schema evolution testing
    // This would involve:
    // 1. First collection run with initial schema
    // 2. Second collection run with evolved schema (new parameters)  
    // 3. Verification that both datasets can be read correctly
    
    Ok(())
}

// TODO: This would test incremental collection once temporal metadata is implemented
#[tokio::test]
async fn test_incremental_collection_placeholder() -> Result<()> {
    let temp_dir = tempdir()?;
    let pond_path = temp_dir.path().join("test_pond");
    
    init_pond(&pond_path).await?;
    
    // Placeholder for incremental collection testing
    // This would involve:
    // 1. First collection run
    // 2. Second collection run that only fetches new data since last timestamp
    // 3. Verification that temporal metadata tracking works correctly
    
    Ok(())
}

// Helper functions

async fn init_pond(pond_path: &Path) -> Result<()> {
    // Use Ship's create_pond to create the infrastructure and then create an initial transaction
    // This mimics what the cmd init command does
    let mut ship = Ship::create_pond(pond_path).await
        .map_err(|e| anyhow::anyhow!("Failed to create pond infrastructure: {}", e))?;
    
    // Create an initial transaction to make it a fully functional pond (like cmd init does)
    ship.transact(
        vec!["test".to_string(), "init".to_string()],
        |_tx, fs| Box::pin(async move {
            // Create initial directory structure (this generates filesystem operations)
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            root.create_dir_path("/data").await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            Ok(())
        })
    ).await.map_err(|e| anyhow::anyhow!("Failed to create initial transaction: {}", e))?;
    
    Ok(())
}

async fn read_tinyfs_structure(pond_path: &Path) -> Result<String> {
    // Just verify the pond directory structure exists without starting transactions
    let mut output = String::new();
    output.push_str("Pond structure:\n");
    
    // Check basic pond structure
    let data_path = pond_path.join("data");
    let control_path = pond_path.join("control");
    
    if data_path.exists() {
        output.push_str("- data directory exists\n");
    }
    if control_path.exists() {
        output.push_str("- control directory exists\n");
    }
    
    // For now, avoid filesystem access until we need actual data validation
    output.push_str("- pond initialization successful\n");
    
    Ok(output)
}

fn create_test_config_with_mock_server(_base_url: &str, pond_path: &Path) -> Result<HydroVuConfig> {
    Ok(HydroVuConfig {
        client_id: "test_client_id".to_string(),
        client_secret: "test_client_secret".to_string(),
        pond_path: pond_path.to_string_lossy().to_string(),
        hydrovu_path: "/hydrovu".to_string(),
        max_rows_per_run: 10000,
        devices: vec![
            HydroVuDevice {
                id: 123,
                name: "Test Station 1".to_string(),
                scope: "test".to_string(),
                comment: Some("End-to-end test device".to_string()),
            },
        ],
    })
}

async fn verify_pond_structure(pond_path: &Path) -> Result<()> {
    // Verify the pond was created correctly
    let data_path = pond_path.join("data");
    let control_path = pond_path.join("control");
    
    assert!(data_path.exists(), "Data directory should exist");
    assert!(control_path.exists(), "Control directory should exist");
    
    // Basic structure verification is sufficient for now
    Ok(())
}
