use anyhow::Result;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::log_debug;

/// Cat file
pub async fn cat_command(ship_context: &ShipContext, path: &str, filesystem: FilesystemChoice) -> Result<()> {
    log_debug!("Reading file from pond: {path}", path: path);
    
    let ship = ship_context.create_ship().await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let root = fs.root().await?;
    match root.read_file_path(path).await {
        Ok(content) => {
            let content_str = String::from_utf8_lossy(&content);
            print!("{}", content_str);
            Ok(())
        },
        Err(e) => Err(anyhow::anyhow!("Failed to read file '{}': {}", path, e)),
    }
}
