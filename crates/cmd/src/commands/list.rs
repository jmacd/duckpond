use anyhow::Result;

use crate::common::{FileInfoVisitor, FilesystemChoice, ShipContext};

/// Internal shared implementation
async fn list_files_internal(ship_context: &ShipContext, pattern: &str, show_all: bool, filesystem: FilesystemChoice) -> Result<Vec<crate::common::FileInfo>> {
    let ship = ship_context.create_ship().await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };
    
    let mut visitor = FileInfoVisitor::new(show_all);
    let root = fs.root().await?;
    let mut file_results = root.visit_with_visitor(pattern, &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to list files matching '{}' from {} filesystem: {}", 
            pattern, 
            match filesystem {
                FilesystemChoice::Data => "data",
                FilesystemChoice::Control => "control",
            },
            e))?;
    
    // Sort results by path for consistent output
    file_results.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(file_results)
}

/// List files using ShipContext
pub async fn list_command(ship_context: &ShipContext, pattern: &str, show_all: bool, filesystem: FilesystemChoice) -> Result<()> {
    let file_results = list_files_internal(ship_context, pattern, show_all, filesystem).await?;
    
    for file_info in file_results {
        print!("{}", file_info.format_duckpond_style());
    }
    
    Ok(())
}

#[cfg(test)]
pub async fn list_for_test(pattern: &str, show_all: bool, pond_path: Option<std::path::PathBuf>) -> Result<Vec<String>> {
    let args = vec!["pond".to_string(), "list".to_string(), pattern.to_string()];
    let ship_context = ShipContext::new(pond_path, args);
    
    // Use the shared implementation but return paths instead of printing
    let file_results = list_files_internal(&ship_context, pattern, show_all, FilesystemChoice::Data).await?;
    Ok(file_results.into_iter().map(|file_info| file_info.path).collect())
}
