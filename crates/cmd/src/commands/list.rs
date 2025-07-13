use anyhow::Result;

use crate::common::{FileInfoVisitor, FilesystemChoice, ShipContext};

/// List files with a closure for handling output
pub async fn list_command<F>(
    ship_context: &ShipContext, 
    pattern: &str, 
    show_all: bool, 
    filesystem: FilesystemChoice,
    mut handler: F
) -> Result<()>
where
    F: FnMut(String),
{
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
    
    for file_info in file_results {
        handler(file_info.format_duckpond_style());
    }
    
    Ok(())
}
