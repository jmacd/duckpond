use anyhow::Result;

use crate::common::{read_file_via_steward, FilesystemChoice};
use diagnostics::log_debug;

pub async fn cat_command(path: &str, filesystem: FilesystemChoice) -> Result<()> {
    log_debug!("Reading file from pond: {path}", path: path);
    
    // Use the new steward-based read helper for consistent coordination
    match read_file_via_steward(path, filesystem, None).await {
        Ok(content) => {
            let content_str = String::from_utf8_lossy(&content);
            print!("{}", content_str);
            Ok(())
        },
        Err(e) => Err(e),
    }
}
