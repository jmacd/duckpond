use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, Args};

/// Helper function to parse directory content and extract child entries
fn parse_directory_content(content: &[u8]) -> Result<Vec<oplog::tinylogfs::schema::DirectoryEntry>, Box<dyn std::error::Error>> {
    if content.is_empty() {
        return Ok(vec![]);
    }

    // Try to deserialize directory entries from Arrow IPC format
    use arrow::ipc::reader::StreamReader;
    use arrow_array::Array;
    
    let cursor = std::io::Cursor::new(content);
    let mut reader = StreamReader::try_new(cursor, None)?;
    
    let mut entries = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch?;
        
        // Try to parse as VersionedDirectoryEntry first (new format)
        if batch.num_columns() >= 5 {
            if let (Some(names), Some(child_ids)) = (
                batch.column_by_name("name"),
                batch.column_by_name("child_node_id")
            ) {
                let name_array = names.as_any().downcast_ref::<arrow_array::StringArray>()
                    .ok_or("Expected string array for name")?;
                let child_array = child_ids.as_any().downcast_ref::<arrow_array::StringArray>()
                    .ok_or("Expected string array for child_node_id")?;
                
                for i in 0..batch.num_rows() {
                    entries.push(oplog::tinylogfs::schema::DirectoryEntry {
                        name: name_array.value(i).to_string(),
                        child: child_array.value(i).to_string(),
                    });
                }
            }
        } 
        // Fall back to old DirectoryEntry format
        else if batch.num_columns() >= 2 {
            if let (Some(names), Some(child_ids)) = (
                batch.column_by_name("name"),
                batch.column_by_name("child")
            ) {
                let name_array = names.as_any().downcast_ref::<arrow_array::StringArray>()
                    .ok_or("Expected string array for name")?;
                let child_array = child_ids.as_any().downcast_ref::<arrow_array::StringArray>()
                    .ok_or("Expected string array for child")?;
                
                for i in 0..batch.num_rows() {
                    entries.push(oplog::tinylogfs::schema::DirectoryEntry {
                        name: name_array.value(i).to_string(),
                        child: child_array.value(i).to_string(),
                    });
                }
            }
        }
    }
    
    Ok(entries)
}

/// Helper function to format node ID consistently
/// Shows 8 least-significant hex digits by default, full width only when needed
fn format_node_id(node_id: &str) -> String {
    if node_id.len() <= 8 {
        return node_id.to_string();
    }
    
    // For 16-character hex strings, check if first 8 characters are all zeros
    if node_id.len() == 16 {
        let high_part = &node_id[0..8];
        let low_part = &node_id[8..16];
        
        if high_part == "00000000" {
            // Only show the low 8 digits
            low_part.to_string()
        } else {
            // Show full width when high part is non-zero
            node_id.to_string()
        }
    } else {
        // For other lengths, just show first 8 characters
        node_id[..8.min(node_id.len())].to_string()
    }
}

/// Helper function to truncate strings for display
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Helper function to format file size in human-readable format
fn format_file_size(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "pond")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    /// Enable verbose output, including performance counters
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond with an empty oplog
    Init,
    /// Show the operation log contents
    Show(ShowArgs),
    /// Create a file in the TinyLogFS (Phase 2 demo)
    Touch {
        /// File path to create
        path: String,
        /// Optional content for the file
        #[arg(short, long)]
        content: Option<String>,
    },
    /// Read a file from TinyLogFS (Phase 2 demo)
    Cat {
        /// File path to read
        path: String,
    },
    /// Commit pending operations to OpLog (Phase 2 demo)
    Commit,
    /// Show TinyLogFS status (Phase 2 demo)
    Status,
    /// Copy a file from host filesystem to TinyLogFS
    Copy {
        /// Source file path on host filesystem
        source: String,
        /// Destination path in TinyLogFS
        dest: String,
    },
}

#[derive(Args)]
struct ShowArgs {
    /// Filter by partition ID (hex string)
    #[arg(short, long)]
    partition: Option<String>,
    
    /// Filter by minimum timestamp (RFC3339 format)
    #[arg(long)]
    since: Option<String>,
    
    /// Filter by maximum timestamp (RFC3339 format)  
    #[arg(long)]
    until: Option<String>,
    
    /// Limit number of entries to show
    #[arg(short, long)]
    limit: Option<usize>,
    
    /// Show verbose details (directory contents, file sizes, etc.)
    #[arg(short, long)]
    verbose: bool,
}

fn find_pond_dir() -> Result<PathBuf> {
    match env::var("POND") {
        Ok(val) => Ok(PathBuf::from(val)),
        Err(_) => Err(anyhow!("POND environment variable not set")),
    }
}

fn get_store_path() -> Result<PathBuf> {
    let pond_dir = find_pond_dir()?;
    Ok(pond_dir.join("store"))
}

/// Check if a Delta table exists using DeltaTableManager (object_store compatible)


async fn init_command() -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Initializing pond at: {}", store_path.display());

    // Check if pond already exists by trying to open it
    let delta_manager = oplog::tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_ok() {
        return Err(anyhow!("Pond already exists at {}", store_path.display()));
    }

    // Create the store directory if it doesn't exist
    std::fs::create_dir_all(&store_path)?;

    // Initialize the oplog with an empty root directory entry using OplogEntry schema
    oplog::tinylogfs::create_oplog_table(&store_path_str).await
        .map_err(|e| anyhow!("Failed to initialize pond at {}: {}", store_path.display(), e))?;

    println!("Successfully initialized pond with empty root directory");
    Ok(())
}

async fn show_command(args: &ShowArgs, verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Opening pond at: {}", store_path.display());

    // First, validate that the pond exists using the same method as init
    let delta_manager = oplog::tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist at {}. Run 'pond init' first.", store_path.display()));
    }

    // Get persistence layer for metrics
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Failed to open pond at {}: {}", store_path.display(), e))?;

    // Use the new OplogEntry table provider for filesystem operations
    use datafusion::prelude::*;
    use datafusion::logical_expr::{col, lit};
    let ctx = SessionContext::new();

    // Register the OplogEntry table that can read filesystem operations
    let oplog_table = oplog::entry::OplogEntryTable::new(store_path_str.to_string());
    ctx.register_table("filesystem_ops", std::sync::Arc::new(oplog_table))
        .map_err(|e| anyhow!("Failed to register table: {}", e))?;

    // Build the query based on filters
    let mut base_query = ctx.table("filesystem_ops").await?;
    
    // Apply partition filter if specified
    if let Some(partition_id) = &args.partition {
        base_query = base_query.filter(col("part_id").eq(lit(partition_id.clone())))?;
    }
    
    // Apply time range filters if specified
    // Note: We'd need to add timestamp to the schema for this to work fully
    // For now, we'll document this as a future enhancement
    
    // Apply limit if specified
    if let Some(limit) = args.limit {
        base_query = base_query.limit(0, Some(limit))?;
    }

    // Human-readable format with detailed explanation
    println!("\n=== DuckPond Operation Log ===");
    
    // We need to get the content field too to parse directory entries and file names
    let full_df = base_query.clone()
        .select(vec![col("part_id"), col("node_id"), col("file_type"), col("content")])?
        .sort(vec![col("part_id").sort(true, true), col("node_id").sort(true, true)])?;
            
            let batches = full_df.collect().await?;
            let mut total_entries = 0;
            let mut entries_by_type = std::collections::HashMap::new();
            let mut operation_counter = 1;
            
            // Group entries by node_id to show versions clearly
            let mut entries_by_node: std::collections::HashMap<String, Vec<(String, String, String, Vec<u8>)>> = std::collections::HashMap::new();
            
            // Collect all entries grouped by node_id
            for batch in &batches {
                let part_ids = batch.column_by_name("part_id").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                let node_ids = batch.column_by_name("node_id").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                let file_types = batch.column_by_name("file_type").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                let contents = batch.column_by_name("content").unwrap()
                    .as_any().downcast_ref::<arrow_array::BinaryArray>().unwrap();

                for i in 0..batch.num_rows() {
                    let part_id = part_ids.value(i).to_string();
                    let node_id = node_ids.value(i).to_string();
                    let file_type = file_types.value(i).to_string();
                    let content = contents.value(i).to_vec();
                    
                    entries_by_node.entry(node_id.clone())
                        .or_insert_with(Vec::new)
                        .push((part_id, node_id, file_type, content));
                }
            }
            
            // Now display entries grouped by node, showing versions
            let mut node_entries: Vec<_> = entries_by_node.iter().collect();
            node_entries.sort_by_key(|(node_id, _)| *node_id);
            
            for (node_index, (node_id, entries)) in node_entries.iter().enumerate() {
                let node_short = format_node_id(node_id);
                
                // Sort entries to show them in a consistent order (by content size for directories)
                let mut sorted_entries = (*entries).clone();
                sorted_entries.sort_by_key(|(_, _, _, content)| content.len());
                
                for (version_num, (part_id, _, file_type, content)) in sorted_entries.iter().enumerate() {
                    total_entries += 1;
                    *entries_by_type.entry(file_type.to_string()).or_insert(0) += 1;
                    
                    let part_short = format_node_id(part_id);
                    
                    // Use emoji to indicate parent relationship more compactly
                    let parent_indicator = if part_id == *node_id { 
                        "🏠" // "home" emoji for self-reference (directories)
                    } else { 
                        "📁" // folder emoji for parent directory
                    };
                    
                    let size_formatted = format_file_size(content.len());
                    
                    match file_type.as_str() {
                        "directory" => {
                            // Parse directory content to get entry count and names
                            let dir_info = if let Ok(dir_entries) = parse_directory_content(content) {
                                if dir_entries.is_empty() {
                                    format!("(empty) - {}", size_formatted)
                                } else {
                                    format!("({} entries) - {}", dir_entries.len(), size_formatted)
                                }
                            } else {
                                format!("(unparseable) - {}", size_formatted)
                            };
                            
                            println!("📁 Op#{:02} {}{:<4} [dir ] {} {} {}",
                                operation_counter, node_short, format!(" v{}", version_num + 1), parent_indicator, part_short, dir_info);
                            
                            if args.verbose {
                                // Show directory entries with target node IDs
                                if let Ok(dir_entries) = parse_directory_content(content) {
                                    for entry in &dir_entries {
                                        let target_short = format_node_id(&entry.child);
                                        println!("   ├─ '{}' -> {}", entry.name, target_short);
                                    }
                                    if dir_entries.is_empty() {
                                        println!("   └─ (no entries)");
                                    }
                                } else {
                                    println!("   └─ (unable to parse directory content)");
                                }
                            }
                        },
                        "file" => {
                            // For files, show content preview or just size with better formatting
                            let content_info = if content.len() <= 50 {
                                let content_str = String::from_utf8_lossy(content);
                                let clean_content = content_str.trim().replace('\n', "\\n").replace('\r', "\\r");
                                format!("'{}' ({})", truncate_string(&clean_content, 40), size_formatted)
                            } else {
                                size_formatted
                            };
                            
                            // Always show version for files, even if it's v1
                            let file_version = format!(" v{}", version_num + 1);
                            
                            println!("📄 Op#{:02} {}{:<4} [file] {} {} - {}", 
                                operation_counter, node_short, file_version, parent_indicator, part_short, content_info);
                        },
                        "symlink" => {
                            let target = String::from_utf8_lossy(content);
                            let target_display = truncate_string(target.trim(), 50);
                            
                            // Always show version for symlinks, even if it's v1
                            let link_version = format!(" v{}", version_num + 1);
                            
                            println!("🔗 Op#{:02} {}{:<4} [link] {} {} -> '{}' ({})", 
                                operation_counter, node_short, link_version, parent_indicator, part_short, target_display, size_formatted);
                        },
                        _ => {
                            // Always show version for unknown types, even if it's v1
                            let unknown_version = format!(" v{}", version_num + 1);
                            
                            println!("❓ Op#{:02} {}{:<4} [????] {} {} - {}", 
                                operation_counter, node_short, unknown_version, parent_indicator, part_short, size_formatted);
                        }
                    }
                    operation_counter += 1;
                }
                
                // Add a blank line between different nodes only if:
                // 1. This node has multiple versions AND
                // 2. There are more nodes coming after this one AND
                // 3. The next node also has entries (for better visual separation)
                if sorted_entries.len() > 1 && 
                   node_index < node_entries.len() - 1 &&
                   node_entries[node_index + 1].1.len() > 1 {
                    println!();
                }
            }
            
            println!("=== Summary ===");
            println!("Total entries: {}", total_entries);
            for (file_type, count) in entries_by_type {
                println!("  {}: {}", file_type, count);
            }
            
            // Show any actual problems (identical records = bugs)
            let problematic_nodes: Vec<_> = node_entries.iter()
                .filter(|(_, entries)| {
                    entries.len() > 1 && 
                    entries.len() == 2 && 
                    entries[0].3 == entries[1].3  // identical content = bug
                })
                .collect();
            
            if !problematic_nodes.is_empty() {
                println!("\n⚠️  Issues detected:");
                for (node_id, entries) in problematic_nodes {
                    let node_short = format_node_id(node_id);
                    let file_type = &entries[0].2;
                    println!("  {} [{}]: {} identical records (possible bug)", node_short, file_type, entries.len());
                }
            }

    // Show performance metrics if verbose mode is enabled
    if verbose {
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }

    Ok(())
}

async fn touch_command(path: &str, content: Option<&str>, verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("Creating file '{}' in pond at: {}", path, store_path.display());
    
    // Try to create OpLogPersistence - this will fail if pond doesn't exist
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Pond does not exist at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;
    
    // For now, we'll create a simple demonstration
    // TODO: Implement actual file creation using TinyLogFS, then auto-commit
    let content_str = content.unwrap_or("").to_string();
    println!("Would create file '{}' with content: '{}'", path, content_str);
    println!("Note: Use 'copy' command for actual file creation");
    
    // Show performance metrics if verbose mode is enabled
    if verbose {
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }
    
    Ok(())
}

async fn cat_command(path: &str, verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("Reading file '{}' from pond at: {}", path, store_path.display());
    
    // Try to create OpLogPersistence - this will fail if pond doesn't exist
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Pond does not exist at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;
    
    // For now, we'll create a simple demonstration
    // In the future, this will use TinyFS + OpLogBackend
    println!("Would read file '{}' from TinyFS", path);
    println!("Note: TinyFS + OpLogBackend integration pending completion");
    
    // Show performance metrics if verbose mode is enabled
    if verbose {
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }
    
    Ok(())
}

async fn commit_command(verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("Committing pending operations in pond at: {}", store_path.display());
    
    // Get the persistence layer directly so we can call commit on it
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Failed to open TinyLogFS at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;
    
    // Access the persistence layer through the TinyFS PersistenceLayer trait
    use tinyfs::persistence::PersistenceLayer;
    persistence.commit().await
        .map_err(|e| anyhow!("Failed to commit operations: {}", e))?;
    
    println!("✅ Successfully committed pending operations to Delta Lake");
    
    // Show performance metrics if verbose mode is enabled
    if verbose {
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }
    
    Ok(())
}

async fn status_command(verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("TinyLogFS status for pond at: {}", store_path.display());
    
    // Try to create OpLogPersistence - this will fail if pond doesn't exist
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Pond does not exist at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;
    
    // For now, we'll create a simple demonstration
    // In the future, this will show TinyFS status
    println!("Would show TinyFS transaction state and pending operations");
    println!("Note: TinyFS + OpLogBackend integration pending completion");
    
    // Show performance metrics if verbose mode is enabled
    if verbose {
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }
    
    Ok(())
}

async fn copy_command(source: &str, dest: &str, verbose: bool) -> Result<()> {
    let store_path = get_store_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("Copying '{}' to TinyLogFS path '{}' in pond at: {}", source, dest, store_path.display());
    
    // Read the source file from host filesystem
    let content = std::fs::read(source)
        .map_err(|e| anyhow!("Failed to read source file '{}': {}", source, e))?;
    
    println!("📄 Read {} bytes from '{}'", content.len(), source);
    
    // Create TinyLogFS instance
    let fs = oplog::tinylogfs::create_oplog_fs(&store_path_str).await
        .map_err(|e| anyhow!("Failed to open TinyLogFS at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;
    
    // Get root working directory
    let root_wd = fs.root().await
        .map_err(|e| anyhow!("Failed to get root directory: {}", e))?;
    
    // Create the file in TinyLogFS
    let _node_path = root_wd.create_file_path(dest, &content).await
        .map_err(|e| anyhow!("Failed to create file '{}' in TinyLogFS: {}", dest, e))?;
    
    // Auto-commit the changes to ensure persistence
    println!("💾 Committing changes...");
    
    // Use the FS commit method to commit pending operations
    fs.commit().await
        .map_err(|e| anyhow!("Failed to commit changes: {}", e))?;

    println!("✅ Successfully copied '{}' to TinyLogFS as '{}' and committed", source, dest);
    
    // Show performance metrics if verbose mode is enabled  
    if verbose {
        // Create a new persistence instance to read metrics
        let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
            .map_err(|e| anyhow!("Failed to read metrics: {}", e))?;
        println!("\n=== Performance Metrics ===");
        persistence.print_io_metrics().await;
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Init => init_command().await,
        Commands::Show(args) => show_command(args, cli.verbose).await,
        Commands::Touch { path, content } => touch_command(path, content.as_deref(), cli.verbose).await,
        Commands::Cat { path } => cat_command(path, cli.verbose).await,
        Commands::Commit => commit_command(cli.verbose).await,
        Commands::Status => status_command(cli.verbose).await,
        Commands::Copy { source, dest } => copy_command(source, dest, cli.verbose).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_init_and_show() -> Result<()> {
        let tmp = tempdir()?;
        let pond_path = tmp.path().join("test_pond");

        // Set the POND environment variable
        unsafe {
            env::set_var("POND", pond_path.to_string_lossy().to_string());
        }

        // Test init command
        init_command().await?;

        // Verify the store was created by trying to open it
        let store_path = get_store_path()?;
        let store_path_str = store_path.to_string_lossy();
        let delta_manager = oplog::tinylogfs::DeltaTableManager::new();
        let _table = delta_manager.get_table(&store_path_str).await
            .expect("Pond should exist after init");

        // Test show command with default arguments
        let show_args = ShowArgs {
            partition: None,
            since: None,
            until: None,
            limit: None,
            verbose: false,
        };
        show_command(&show_args, false).await?;

        // Test that init fails if run again
        let result = init_command().await;
        assert!(result.is_err());

        Ok(())
    }
}
