use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, Args};

/// Helper function to parse directory content and extract child entries
fn parse_directory_content(content: &[u8]) -> Result<Vec<oplog::tinylogfs::schema::DirectoryEntry>, Box<dyn std::error::Error>> {
    // For now, just return empty - we'll implement proper parsing later
    // This is a placeholder to show the structure
    if content.is_empty() {
        Ok(vec![])
    } else {
        // Try to deserialize directory entries from Arrow IPC format
        // This is complex, so for now we'll just show that we have content
        Err("Directory content parsing not yet implemented".into())
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
    /// Output format: 'table' (default), 'raw', or 'human'
    #[arg(short, long, default_value = "human")]
    format: String,
    
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
    
    /// Show tinylogfs details (directory contents, file sizes, etc.)
    #[arg(long)]
    tinylogfs: bool,
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

    // First, validate that the pond exists and get persistence layer for metrics
    let persistence = oplog::tinylogfs::OpLogPersistence::new(&store_path_str).await
        .map_err(|e| anyhow!("Pond does not exist at {}. Run 'pond init' first. Error: {}", store_path.display(), e))?;

    // Use the new OplogEntry table provider for filesystem operations
    use datafusion::prelude::*;
    use datafusion::logical_expr::{col, lit};
    let ctx = SessionContext::new();

    // Register the OplogEntry table that can read filesystem operations
    let oplog_table = oplog::entry::OplogEntryTable::new(store_path_str.to_string());
    ctx.register_table("filesystem_ops", std::sync::Arc::new(oplog_table))
        .map_err(|e| anyhow!("Failed to register table: {}", e))?;

    // Build the query based on filters
    let mut query_builder = ctx.table("filesystem_ops").await?;
    
    // Apply partition filter if specified
    if let Some(partition_id) = &args.partition {
        query_builder = query_builder.filter(col("part_id").eq(lit(partition_id.clone())))?;
    }
    
    // Apply time range filters if specified
    // Note: We'd need to add timestamp to the schema for this to work fully
    // For now, we'll document this as a future enhancement
    
    // Apply limit if specified
    if let Some(limit) = args.limit {
        query_builder = query_builder.limit(0, Some(limit))?;
    }
    
    // Select appropriate columns and sort
    let df = match args.format.as_str() {
        "raw" => {
            query_builder
                .select(vec![col("part_id"), col("node_id"), col("file_type"), col("content")])?
                .sort(vec![col("part_id").sort(true, true), col("node_id").sort(true, true)])?
        }
        "table" => {
            query_builder
                .select(vec![col("part_id"), col("node_id"), col("file_type")])?
                .sort(vec![col("part_id").sort(true, true), col("node_id").sort(true, true)])?
        }
        "human" => {
            query_builder
                .select(vec![col("part_id"), col("node_id"), col("file_type")])?
                .sort(vec![col("part_id").sort(true, true), col("node_id").sort(true, true)])?
        }
        _ => {
            return Err(anyhow!("Unknown format '{}'. Use 'table', 'raw', or 'human'", args.format));
        }
    };

    match args.format.as_str() {
        "human" => {
            // Human-readable format with detailed explanation
            println!("\n=== DuckPond Operation Log ===");
            
            // We need to get the content field too to parse directory entries and file names
            let raw_df = ctx.table("filesystem_ops").await?;
            
            // Apply the same filters but get all columns including content
            let mut query_builder = raw_df;
            if let Some(partition_id) = &args.partition {
                query_builder = query_builder.filter(col("part_id").eq(lit(partition_id.clone())))?;
            }
            if let Some(limit) = args.limit {
                query_builder = query_builder.limit(0, Some(limit))?;
            }
            
            let full_df = query_builder
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
            
            for (node_id, entries) in &node_entries {
                let node_short = &node_id[..8];
                
                // Sort entries to show them in a consistent order (by content size for directories)
                let mut sorted_entries = (*entries).clone();
                sorted_entries.sort_by_key(|(_, _, _, content)| content.len());
                
                for (version_num, (part_id, _, file_type, content)) in sorted_entries.iter().enumerate() {
                    total_entries += 1;
                    *entries_by_type.entry(file_type.to_string()).or_insert(0) += 1;
                    
                    let part_short = if part_id == *node_id { 
                        "self".to_string() 
                    } else { 
                        part_id[..8].to_string() 
                    };
                    
                    let version_indicator = if entries.len() > 1 {
                        format!(" v{}", version_num + 1)
                    } else {
                        String::new()
                    };
                    
                    match file_type.as_str() {
                        "directory" => {
                            let dir_info = if content.len() <= 800 {
                                "(empty)"
                            } else {
                                "(has entries)"
                            };
                            println!("📁 Op#{:02} {}{} [directory] (parent: {}) - {} bytes {}",
                                operation_counter, node_short, version_indicator, part_short, content.len(), dir_info);
                            
                            if args.tinylogfs {
                                // Try to parse directory content to show child entries
                                if let Ok(entries) = parse_directory_content(content) {
                                    for entry in entries {
                                        println!("   └─ '{}' -> {}", entry.name, &entry.child[..8]);
                                    }
                                } else {
                                    println!("   └─ (unable to parse directory content)");
                                }
                            }
                        },
                        "file" => {
                            // For files, we could show content preview or just size
                            let size_info = if content.len() <= 50 {
                                format!("'{}' ({} bytes)", 
                                    String::from_utf8_lossy(content).trim(), content.len())
                            } else {
                                format!("{} bytes", content.len())
                            };
                            println!("📄 Op#{:02} {}{} [file] (in: {}) - {}", 
                                operation_counter, node_short, version_indicator, part_short, size_info);
                        },
                        "symlink" => {
                            let target = String::from_utf8_lossy(content);
                            println!("🔗 Op#{:02} {}{} [symlink] (in: {}) -> '{}'", 
                                operation_counter, node_short, version_indicator, part_short, target);
                        },
                        _ => {
                            println!("❓ Op#{:02} {}{} [{}] (in: {}) - {} bytes", 
                                operation_counter, node_short, version_indicator, file_type, part_short, content.len());
                        }
                    }
                    operation_counter += 1;
                }
                
                // Add a blank line between different nodes if there are multiple versions
                if sorted_entries.len() > 1 {
                    println!();
                }
            }
            
            println!("=== Summary ===");
            println!("Total entries: {}", total_entries);
            for (file_type, count) in entries_by_type {
                println!("  {}: {}", file_type, count);
            }
            
            // Show any duplicates clearly
            let duplicates: Vec<_> = node_entries.iter()
                .filter(|(_, entries)| entries.len() > 1)
                .collect();
            
            if !duplicates.is_empty() {
                println!("\n⚠️  Multiple versions detected:");
                for (node_id, entries) in duplicates {
                    let node_short = &node_id[..8];
                    let file_type = &entries[0].2;
                    if entries.len() == 2 && entries[0].3 == entries[1].3 {
                        println!("  {} [{}]: {} identical records (possible bug)", node_short, file_type, entries.len());
                    } else {
                        println!("  {} [{}]: {} versions (normal versioning)", node_short, file_type, entries.len());
                    }
                }
            }
        }
        "table" | "raw" => {
            println!("Filesystem operations:");
            df.show().await?;
        }
        _ => unreachable!(),
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
            format: "human".to_string(),
            partition: None,
            since: None,
            until: None,
            limit: None,
            tinylogfs: false,
        };
        show_command(&show_args, false).await?;

        // Test that init fails if run again
        let result = init_command().await;
        assert!(result.is_err());

        Ok(())
    }
}
