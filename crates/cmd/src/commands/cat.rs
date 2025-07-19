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
    match root.async_reader_path(path).await {
        Ok(reader) => {
            let content = tinyfs::buffer_helpers::read_all_to_vec(reader).await
                .map_err(|e| anyhow::anyhow!("Failed to read file content: {}", e))?;
            
            // Output raw bytes directly to stdout (handles both text and binary files)
            use std::io::{self, Write};
            io::stdout().write_all(&content)
                .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {}", e))?;
            Ok(())
        },
        Err(e) => Err(anyhow::anyhow!("Failed to read file '{}': {}", path, e)),
    }
}
