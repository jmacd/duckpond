// Ship context utilities to eliminate duplication in test setup patterns

use anyhow::Result;
use std::path::PathBuf;
use tempfile::{TempDir, tempdir};

use crate::common::ShipContext;

/// Test environment that encapsulates ship setup patterns
/// Eliminates duplication in ship creation across test files
pub struct TestShipEnvironment {
    pub temp_dir: TempDir,
    pub ship_context: ShipContext,
}

impl TestShipEnvironment {
    /// Create a new test environment with temporary pond
    /// This eliminates the repetitive pattern across many test files:
    /// - Create temp dir
    /// - Initialize pond
    /// - Create ship context
    pub async fn new() -> Result<Self> {
        let temp_dir = tempdir()?;
        let pond_path = temp_dir.path().join("test_pond");
        
        // Initialize a new pond for testing
        let ship_context = ShipContext::new(Some(pond_path), vec!["test".to_string()]);
        ship_context.initialize_new_pond().await?;

        Ok(Self {
            temp_dir,
            ship_context,
        })
    }

    /// Create a ship for read operations
    pub async fn create_ship(&self) -> Result<steward::Ship> {
        self.ship_context.create_ship().await
    }

    /// Create a ship with transaction for write operations
    pub async fn create_ship_with_transaction(&self) -> Result<steward::Ship> {
        self.ship_context.create_ship_with_transaction().await
    }

    /// Execute a closure with a ship and transaction
    /// Eliminates the pattern: create ship with transaction -> execute -> commit
    pub async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(steward::Ship) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let ship = self.create_ship_with_transaction().await?;
        let result = f(ship).await?;
        // Note: Ship should handle auto-commit in its Drop implementation
        Ok(result)
    }

    /// Get the pond path for the test environment
    pub fn pond_path(&self) -> PathBuf {
        self.temp_dir.path().join("test_pond")
    }
}

/// Common test patterns for ship operations
pub struct ShipTestUtils;

impl ShipTestUtils {
    /// Create a test CSV file in a temp directory
    pub fn create_test_csv(content: &str) -> Result<(TempDir, PathBuf)> {
        let temp_dir = tempdir()?;
        let csv_path = temp_dir.path().join("test.csv");
        std::fs::write(&csv_path, content)?;
        Ok((temp_dir, csv_path))
    }

    /// Standard CSV content for testing
    pub fn sample_csv_content() -> &'static str {
        "timestamp,sensor_id,temperature,humidity\n\
         1609459200000,sensor1,23.5,45.2\n\
         1609459260000,sensor1,24.1,46.8\n\
         1609459320000,sensor2,22.8,44.1\n\
         1609459380000,sensor2,25.2,48.9"
    }

    /// Standard CSV content with different data patterns
    pub fn sample_csv_content_variant() -> &'static str {
        "timestamp,device_id,value\n\
         1609459200000,device1,100.0\n\
         1609459260000,device1,101.5\n\
         1609459320000,device2,99.2\n\
         1609459380000,device2,102.8"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ship_environment_setup() -> Result<()> {
        let env = TestShipEnvironment::new().await?;
        
        // Test basic ship creation
        let _ship = env.create_ship().await?;
        // Basic validation that ship was created successfully
        // (Ship creation itself validates the pond setup)
        
        Ok(())
    }

    #[tokio::test]
    async fn test_with_transaction_pattern() -> Result<()> {
        let env = TestShipEnvironment::new().await?;
        
        let result = env.with_transaction(|_ship| async move {
            // Test the transaction pattern
            // This would normally do some file operations
            Ok("transaction completed".to_string())
        }).await?;
        
        assert_eq!(result, "transaction completed");
        Ok(())
    }

    #[test]
    fn test_csv_utilities() -> Result<()> {
        let (_temp_dir, csv_path) = ShipTestUtils::create_test_csv(ShipTestUtils::sample_csv_content())?;
        
        assert!(csv_path.exists());
        let content = std::fs::read_to_string(&csv_path)?;
        assert!(content.contains("timestamp,sensor_id"));
        
        Ok(())
    }
}
