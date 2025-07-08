use anyhow::Result;

use crate::common::{create_filesystem_for_reading, FileInfoVisitor, FilesystemChoice};

pub async fn list_command(pattern: &str, show_all: bool, filesystem: FilesystemChoice) -> Result<()> {
    // Create filesystem instance and get results
    let fs = create_filesystem_for_reading(None, filesystem).await?;
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

#[cfg(test)]
pub async fn list_command_with_pond(pattern: &str, show_all: bool, pond_path: Option<std::path::PathBuf>, filesystem: FilesystemChoice) -> Result<Vec<String>> {
    diagnostics::log_debug!("Listing files matching pattern from pond: {pattern}", pattern: pattern);

    // Create filesystem instance based on choice  
    let fs = create_filesystem_for_reading(pond_path, filesystem).await?;
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
