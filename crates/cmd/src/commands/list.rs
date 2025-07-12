use anyhow::Result;

use crate::common::{list_files_via_steward, FileInfoVisitor, FilesystemChoice};

pub async fn list_command(pattern: &str, show_all: bool, filesystem: FilesystemChoice) -> Result<()> {
    // Use the new steward-based listing helper for consistent coordination
    let mut visitor = FileInfoVisitor::new(show_all);
    let mut file_results = list_files_via_steward(pattern, &mut visitor, filesystem, None).await?;
    
    // Sort results by path for consistent output
    file_results.sort_by(|a, b| a.path.cmp(&b.path));
    
    for file_info in file_results {
        print!("{}", file_info.format_duckpond_style());
    }
    
    Ok(())
}

#[cfg(test)]
pub async fn list_command_with_pond(pattern: &str, show_all: bool, pond_path: Option<std::path::PathBuf>, filesystem: FilesystemChoice) -> Result<Vec<String>> {
    diagnostics::log_debug!("Listing files matching pattern from pond: {pattern}", pattern: pattern);

    // Use the new steward-based listing helper
    let mut visitor = FileInfoVisitor::new(show_all);
    let mut results = list_files_via_steward(pattern, &mut visitor, filesystem, pond_path).await?;

    // Sort results by path for consistent output
    results.sort_by(|a, b| a.path.cmp(&b.path));

    // Collect paths and return them
    let mut paths = Vec::new();
    for file_info in results {
        paths.push(file_info.path.clone());
    }

    Ok(paths)
}
