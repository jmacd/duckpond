use crate::models::HydroVuConfig;
use anyhow::{Context, Result};
use std::path::Path;

/// Load configuration from YAML file
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<HydroVuConfig> {
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;
    
    let config: HydroVuConfig = serde_yaml_ng::from_str(&content)
        .with_context(|| "Failed to parse YAML configuration")?;
    
    validate_config(&config)?;
    Ok(config)
}

/// Validate configuration
pub fn validate_config(config: &HydroVuConfig) -> Result<()> {
    if config.client_id.is_empty() {
        anyhow::bail!("client_id cannot be empty");
    }
    
    if config.client_secret.is_empty() {
        anyhow::bail!("client_secret cannot be empty");
    }
    
    if config.pond_path.is_empty() {
        anyhow::bail!("pond_path cannot be empty");
    }
    
    if config.devices.is_empty() {
        anyhow::bail!("At least one device must be configured");
    }
    
    for device in &config.devices {
        if device.name.is_empty() {
            anyhow::bail!("Device name cannot be empty for device ID {}", device.id);
        }
        if device.scope.is_empty() {
            anyhow::bail!("Device scope cannot be empty for device ID {}", device.id);
        }
    }
    
    if config.max_points_per_run == 0 {
        anyhow::bail!("max_points_per_run must be greater than 0");
    }
    
    Ok(())
}

/// Save configuration to YAML file  
pub fn save_config<P: AsRef<Path>>(config: &HydroVuConfig, path: P) -> Result<()> {
    let yaml = serde_yaml_ng::to_string(config)
        .with_context(|| "Failed to serialize configuration to YAML")?;
    
    std::fs::write(&path, yaml)
        .with_context(|| format!("Failed to write config file: {}", path.as_ref().display()))?;
    
    Ok(())
}

/// Create an example configuration file
pub fn create_example_config<P: AsRef<Path>>(path: P) -> Result<()> {
    let example_config = HydroVuConfig {
        client_id: "your_oauth_client_id".to_string(),
        client_secret: "your_oauth_client_secret".to_string(),
        pond_path: "/path/to/pond".to_string(),
        hydrovu_path: "/hydrovu".to_string(),
        max_points_per_run: 10000,
        devices: vec![
            crate::models::HydroVuDevice {
                id: 123,
                name: "Sensor 1".to_string(),
                scope: "site_a".to_string(),
                comment: Some("Primary temperature sensor".to_string()),
            },
            crate::models::HydroVuDevice {
                id: 456,
                name: "Sensor 2".to_string(),
                scope: "site_b".to_string(),
                comment: None,
            },
        ],
    };
    
    save_config(&example_config, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_save_config() -> Result<()> {
        let mut config = HydroVuConfig::default();
        // Fill required fields to make config valid
        config.client_id = "test_client_id".to_string();
        config.client_secret = "test_client_secret".to_string();
        config.devices.push(crate::models::HydroVuDevice {
            id: 123,
            name: "Test Device".to_string(),
            scope: "all".to_string(),
            comment: Some("Test device comment".to_string()),
        });
        
        let temp_file = NamedTempFile::new()?;
        save_config(&config, temp_file.path())?;
        
        let loaded_config = load_config(temp_file.path())?;
        assert_eq!(config.client_id, loaded_config.client_id);
        assert_eq!(config.client_secret, loaded_config.client_secret);
        assert_eq!(config.pond_path, loaded_config.pond_path);
        
        Ok(())
    }
    
    #[test]
    fn test_validate_config() {
        let mut config = HydroVuConfig::default();
        
        // Should fail with empty fields
        assert!(validate_config(&config).is_err());
        
        // Fill required fields
        config.client_id = "test_id".to_string();
        config.client_secret = "test_secret".to_string();
        config.devices.push(crate::models::HydroVuDevice {
            id: 123,
            name: "Test Device".to_string(),
            scope: "test".to_string(),
            comment: None,
        });
        
        // Should pass validation
        assert!(validate_config(&config).is_ok());
    }
}
