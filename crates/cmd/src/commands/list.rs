use anyhow::{Result, anyhow};
use std::path::PathBuf;

use crate::common::{get_pond_path_with_override, FileInfoVisitor};
use diagnostics::log_debug;

pub async fn list_command(pattern: &str, show_all: bool) -> Result<()> {
    let _results = list_command_with_pond(pattern, show_all, None).await?;
    
    // Print results in DuckPond-specific format
    // We need to get the file info again to format properly
    let store_path = get_pond_path_with_override(None)?;
    let store_path_str = store_path.to_string_lossy();
    
    let fs = tlogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    
    let mut visitor = FileInfoVisitor::new(show_all);
    let file_results = root.visit_with_visitor(pattern, &mut visitor).await?;
    
    // Sort results by path for consistent output
    let mut file_results = file_results;
    file_results.sort_by(|a, b| a.path.cmp(&b.path));
    
    for file_info in file_results {
        print!("{}", file_info.format_duckpond_style());
    }
    
    Ok(())
}

pub async fn list_command_with_pond(pattern: &str, show_all: bool, pond_path: Option<PathBuf>) -> Result<Vec<String>> {
    let store_path = get_pond_path_with_override(pond_path)?;
    let store_path_str = store_path.to_string_lossy();

    log_debug!("Listing files matching pattern from pond: {pattern}", pattern: pattern);

    // Check if pond exists
    let delta_manager = tlogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Create filesystem and get root directory
    let fs = tlogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;

    // Create a visitor to collect file information
    let mut visitor = FileInfoVisitor::new(show_all);
    let results = root.visit_with_visitor(pattern, &mut visitor).await?;

    // Sort results by path for consistent output
    let mut results = results;
    results.sort_by(|a, b| a.path.cmp(&b.path));

    // Collect paths and return them
    let mut paths = Vec::new();
    for file_info in results {
        paths.push(file_info.path.clone());
    }

    Ok(paths)
}
