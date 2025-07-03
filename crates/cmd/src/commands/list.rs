use anyhow::{Result, anyhow};

use crate::common::{get_pond_path, FileInfoVisitor};
use diagnostics::log_debug;

pub async fn list_command(pattern: &str, show_all: bool) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    log_debug!("Listing files matching pattern from pond: {pattern}", pattern: pattern);

    // Check if pond exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Create filesystem and get root directory
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;

    // Create a visitor to collect file information
    let mut visitor = FileInfoVisitor::new(show_all);
    let results = root.visit_with_visitor(pattern, &mut visitor).await?;

    // Sort results by path for consistent output
    let mut results = results;
    results.sort_by(|a, b| a.path.cmp(&b.path));

    // Print results in DuckPond-specific format
    for file_info in results {
        print!("{}", file_info.format_duckpond_style());
    }

    Ok(())
}
