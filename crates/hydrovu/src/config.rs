use crate::models::HydroVuConfig;
use anyhow::{Context, Result};
use std::path::Path;

/// Load configuration from YAML file
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<HydroVuConfig> {
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;

    let config: HydroVuConfig =
        serde_yaml::from_str(&content).with_context(|| "Failed to parse YAML configuration")?;

    validate_config(&config)?;
    Ok(config)
}

/// Validate configuration
pub(crate) fn validate_config(config: &HydroVuConfig) -> Result<()> {
    // if config.client_id.is_empty() {
    //     anyhow::bail!("client_id cannot be empty");
    // }

    // if config.client_secret.is_empty() {
    //     anyhow::bail!("client_secret cannot be empty");
    // }

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
