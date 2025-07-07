use anyhow::{Result, anyhow};

use crate::common::{create_ship, FilesystemChoice};
use diagnostics::log_debug;

pub async fn cat_command(path: &str, filesystem: FilesystemChoice) -> Result<()> {
    log_debug!("Reading file from pond: {path}", path: path);
    
    // Create steward Ship instance to check if pond exists
    let ship = create_ship(None).await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    // Try to read the file
    let root = fs.root().await?;
    
    match root.read_file_path(path).await {
        Ok(content) => {
            let content_str = String::from_utf8_lossy(&content);
            print!("{}", content_str);
            Ok(())
        },
        Err(_) => {
            Err(anyhow!("File '{}' not found in {} filesystem", path, match filesystem {
                FilesystemChoice::Data => "data",
                FilesystemChoice::Control => "control",
            }))
        }
    }
}
