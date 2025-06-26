use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, Args};

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
            
            let batches = df.collect().await?;
            let mut total_entries = 0;
            let mut entries_by_type = std::collections::HashMap::new();
            
            for batch in &batches {
                let part_ids = batch.column_by_name("part_id").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                let node_ids = batch.column_by_name("node_id").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
                let file_types = batch.column_by_name("file_type").unwrap()
                    .as_any().downcast_ref::<arrow_array::StringArray>().unwrap();

                for i in 0..batch.num_rows() {
                    let part_id = part_ids.value(i);
                    let node_id = node_ids.value(i);
                    let file_type = file_types.value(i);
                    
                    total_entries += 1;
                    *entries_by_type.entry(file_type.to_string()).or_insert(0) += 1;
                    
                    // Format for human readability
                    let node_short = &node_id[..8]; // First 8 chars of hex ID
                    let part_short = if part_id == node_id { 
                        "self".to_string() 
                    } else { 
                        part_id[..8].to_string() 
                    };
                    
                    match file_type {
                        "directory" => println!("📁 {} [{}] (parent: {})", node_short, file_type, part_short),
                        "file" => println!("📄 {} [{}] (in: {})", node_short, file_type, part_short),
                        "symlink" => println!("🔗 {} [{}] (in: {})", node_short, file_type, part_short),
                        _ => println!("❓ {} [{}] (in: {})", node_short, file_type, part_short),
                    }
                    
                    if args.tinylogfs {
                        // Show additional TinyLogFS details would go here
                        // This would require parsing the content field for directories
                        println!("   (TinyLogFS details: use --format=raw to see content)");
                    }
                }
            }
            
            println!("\n=== Summary ===");
            println!("Total entries: {}", total_entries);
            for (file_type, count) in entries_by_type {
                println!("  {}: {}", file_type, count);
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
