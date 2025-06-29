use std::env;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use async_trait::async_trait;

#[derive(Parser)]
#[command(author, version, about = "DuckPond - A very small data lake")]
#[command(name = "pond")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new pond
    Init,
    /// Show pond contents
    Show,
    /// List files and directories (ls -l style)
    List {
        /// Pattern to match (supports wildcards, defaults to "**/*")
        #[arg(default_value = "**/*")]
        pattern: String,
        /// Show all files including hidden ones
        #[arg(short, long)]
        all: bool,
    },
    /// Read a file from the pond
    Cat {
        /// File path to read
        path: String,
    },
    /// Copy a file into the pond
    Copy {
        /// Source file path
        source: String,
        /// Destination path in pond
        dest: String,
    },
    /// Create a directory in the pond
    Mkdir {
        /// Directory path to create
        path: String,
    },
}

fn get_pond_path() -> Result<PathBuf> {
    match env::var("POND") {
        Ok(val) => Ok(PathBuf::from(val).join("store")),
        Err(_) => Err(anyhow!("POND environment variable not set")),
    }
}

async fn init_command() -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Initializing pond at: {}", store_path.display());

    // Check if pond already exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_ok() {
        return Err(anyhow!("Pond already exists"));
    }

    // Create directory and initialize
    std::fs::create_dir_all(&store_path)?;
    tinylogfs::create_oplog_table(&store_path_str).await?;

    println!("✅ Pond initialized successfully");
    Ok(())
}

async fn show_command() -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Check if pond exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }

    // Use DataFusion to query the raw delta records
    use datafusion::prelude::*;
    let ctx = SessionContext::new();
    
    // Register the delta table directly to get Record schema (part_id, timestamp, version, content)
    let table = delta_manager.get_table(&store_path_str).await?;
    ctx.register_table("raw_records", std::sync::Arc::new(table))?;

    // Query all records ordered by transaction boundaries
    let df = ctx.sql("SELECT * FROM raw_records ORDER BY version").await?;
    let batches = df.collect().await?;

    println!("=== DuckPond Operation Log ===");
    let mut transaction_count = 0;
    let mut entry_count = 0;

    for batch in &batches {
        let part_ids = batch.column_by_name("part_id")
            .ok_or_else(|| anyhow!("part_id column not found"))?
            .as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>()
            .ok_or_else(|| anyhow!("part_id is not a DictionaryArray"))?;
        let versions = batch.column_by_name("version")
            .ok_or_else(|| anyhow!("version column not found"))?
            .as_any().downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| anyhow!("version is not an Int64Array"))?;
        let contents = batch.column_by_name("content")
            .ok_or_else(|| anyhow!("content column not found"))?
            .as_any().downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| anyhow!("content is not a BinaryArray"))?;

        // Try to get timestamp column if it exists
        let timestamps = batch.column_by_name("timestamp")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>());

        for i in 0..batch.num_rows() {
            transaction_count += 1;
            let part_id_key = part_ids.key(i).unwrap();
            let part_id = part_ids.values()
                .as_any().downcast_ref::<arrow_array::StringArray>().unwrap()
                .value(part_id_key as usize);
            let version = versions.value(i);
            let content_bytes = contents.value(i);

            println!("=== Transaction #{:03} ===", transaction_count);
            println!("  Partition: {}", format_node_id(part_id));
            
            // Only show timestamp if column exists
            if let Some(ts_array) = timestamps {
                let timestamp_us = ts_array.value(i);
                let dt = chrono::DateTime::from_timestamp(
                    timestamp_us / 1_000_000, 
                    ((timestamp_us % 1_000_000) * 1000) as u32
                ).unwrap_or_default();
                println!("  Timestamp: {} ({})", dt.format("%Y-%m-%d %H:%M:%S%.3f UTC"), timestamp_us);
            }
            
            println!("  Version:   {}", version);
            println!("  Content:   {} bytes", content_bytes.len());

            // Parse OplogEntry from content
            match parse_oplog_entry_content(content_bytes) {
                Ok(oplog_entry) => {
                    entry_count += 1;
                    println!("  ┌─ Entry #{}: {} [{}] -> {}", 
                        entry_count,
                        format_node_id(&oplog_entry.node_id),
                        oplog_entry.file_type,
                        format_node_id(&oplog_entry.part_id)
                    );
                    
                    // Parse type-specific content
                    match oplog_entry.file_type.as_str() {
                        "directory" => {
                            println!("  │  Directory entries: {} bytes", oplog_entry.content.len());
                            match parse_directory_content(&oplog_entry.content) {
                                Ok(dir_entries) => {
                                    if dir_entries.is_empty() {
                                        println!("  │  └─ (empty directory)");
                                    } else {
                                        for (idx, entry) in dir_entries.iter().enumerate() {
                                            let is_last = idx == dir_entries.len() - 1;
                                            let connector = if is_last { "└─" } else { "├─" };
                                            println!("  │  {} '{}' -> {}", 
                                                connector, entry.name, format_node_id(&entry.child_node_id));
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("  │  └─ Error parsing directory: {}", e);
                                }
                            }
                        },
                        "file" => {
                            let size = oplog_entry.content.len();
                            println!("  │  File size: {}", format_file_size(size));
                            if size > 0 && size <= 100 {
                                // Show preview for small files
                                let preview = String::from_utf8_lossy(&oplog_entry.content);
                                let preview = preview.replace('\n', "\\n").replace('\r', "\\r");
                                let preview = truncate_string(&preview, 60);
                                println!("  │  Preview: '{}'", preview);
                            }
                        },
                        "symlink" => {
                            let target = String::from_utf8_lossy(&oplog_entry.content);
                            println!("  │  Target: '{}'", target.trim());
                        },
                        _ => {
                            println!("  │  Unknown type: {} bytes", oplog_entry.content.len());
                        }
                    }
                    println!("  └─");
                }
                Err(e) => {
                    println!("  └─ Error parsing OplogEntry: {}", e);
                }
            }
            println!();
        }
    }

    println!("=== Summary ===");
    println!("Transactions: {}", transaction_count);
    println!("Entries: {}", entry_count);

    Ok(())
}

// Helper function to format node IDs 
fn format_node_id(node_id: &str) -> String {
    if node_id.len() >= 8 {
        format!("{}..{}", &node_id[..4], &node_id[node_id.len()-4..])
    } else {
        node_id.to_string()
    }
}

// Helper function to format file sizes
fn format_file_size(size: usize) -> String {
    if size >= 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{:.1}KB", size as f64 / 1024.0)
    } else {
        format!("{}B", size)
    }
}

// Helper function to truncate strings
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

// Helper function to parse OplogEntry from IPC bytes
fn parse_oplog_entry_content(content: &[u8]) -> Result<tinylogfs::OplogEntry> {
    use arrow::ipc::reader::StreamReader;
    
    let cursor = std::io::Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)?;
    
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    if let Some(batch) = batches.first() {
        let entries: Vec<tinylogfs::OplogEntry> = serde_arrow::from_record_batch(batch)?;
        entries.into_iter().next()
            .ok_or_else(|| anyhow!("No OplogEntry found in batch"))
    } else {
        Err(anyhow!("No batches found in OplogEntry IPC stream"))
    }
}

// Helper function to parse directory content
fn parse_directory_content(content: &[u8]) -> Result<Vec<tinylogfs::VersionedDirectoryEntry>> {
    if content.is_empty() {
        return Ok(Vec::new());
    }
    
    use arrow::ipc::reader::StreamReader;
    
    let cursor = std::io::Cursor::new(content);
    let reader = StreamReader::try_new(cursor, None)?;
    
    let mut all_entries = Vec::new();
    for batch_result in reader {
        let batch = batch_result?;
        let entries: Vec<tinylogfs::VersionedDirectoryEntry> = serde_arrow::from_record_batch(&batch)?;
        all_entries.extend(entries);
    }
    
    Ok(all_entries)
}

async fn cat_command(path: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();
    
    println!("Reading file '{}' from pond...", path);
    
    // Check if pond exists
    let delta_manager = tinylogfs::DeltaTableManager::new();
    if delta_manager.get_table(&store_path_str).await.is_err() {
        return Err(anyhow!("Pond does not exist. Run 'pond init' first."));
    }
    
    // For now, this is a placeholder - would need TinyFS integration to actually read files
    println!("Note: File reading from pond not yet implemented");
    println!("Use 'pond show' to see what's in the pond");
    
    Ok(())
}

async fn copy_command(source: &str, dest: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    // Read source file
    let content = std::fs::read(source)
        .map_err(|e| anyhow!("Failed to read '{}': {}", source, e))?;

    println!("Copying '{}' to pond as '{}'...", source, dest);

    // Create filesystem and copy file
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    root.create_file_path(dest, &content).await?;
    fs.commit().await?;

    println!("✅ File copied successfully");
    Ok(())
}

async fn mkdir_command(path: &str) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Creating directory '{}' in pond...", path);

    // Create filesystem and create directory
    let fs = tinylogfs::create_oplog_fs(&store_path_str).await?;
    let root = fs.root().await?;
    root.create_dir_path(path).await?;
    fs.commit().await?;

    println!("✅ Directory created successfully");
    Ok(())
}

async fn list_command(pattern: &str, show_all: bool) -> Result<()> {
    let store_path = get_pond_path()?;
    let store_path_str = store_path.to_string_lossy();

    println!("Listing files matching '{}' from pond...", pattern);

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
    root.visit_with_visitor(pattern, &mut visitor).await?;

    // Sort results by path for consistent output
    let mut results = visitor.results;
    results.sort_by(|a, b| a.path.cmp(&b.path));

    // Print results in ls -l format
    for file_info in results {
        print!("{}", file_info.format_ls_style());
    }

    Ok(())
}

// Helper struct to store file information for ls -l output
#[derive(Debug)]
struct FileInfo {
    path: String,
    node_type: String,
    size: usize,
    timestamp: Option<i64>,
    symlink_target: Option<String>,
}

impl FileInfo {
    fn format_ls_style(&self) -> String {
        let type_char = match self.node_type.as_str() {
            "directory" => "d",
            "symlink" => "l",
            _ => "-",
        };

        let size_str = if self.node_type == "directory" {
            format!("{:>8}", "-")
        } else {
            format!("{:>8}", format_file_size(self.size))
        };

        let time_str = if let Some(timestamp_us) = self.timestamp {
            let dt = chrono::DateTime::from_timestamp(
                timestamp_us / 1_000_000, 
                ((timestamp_us % 1_000_000) * 1000) as u32
            ).unwrap_or_else(|| chrono::Utc::now());
            dt.format("%b %d %H:%M").to_string()
        } else {
            "           ".to_string()
        };

        let symlink_part = if let Some(target) = &self.symlink_target {
            format!(" -> {}", target)
        } else {
            String::new()
        };

        format!("{}rwxr-xr-x 1 user group {} {} {}{}\n",
                type_char, size_str, time_str, self.path, symlink_part)
    }
}

// Visitor implementation to collect file information
struct FileInfoVisitor {
    results: Vec<FileInfo>,
    show_all: bool,
}

impl FileInfoVisitor {
    fn new(show_all: bool) -> Self {
        Self {
            results: Vec::new(),
            show_all,
        }
    }
}

#[async_trait]
impl tinyfs::Visitor<FileInfo> for FileInfoVisitor {
    async fn visit(&mut self, node: tinyfs::NodePath, _captured: &[String]) -> tinyfs::Result<FileInfo> {
        let node_ref = node.borrow().await;
        let path = node.path().to_string_lossy().to_string();
        
        // Skip hidden files unless --all is specified
        let basename = node.basename();
        if !self.show_all && basename.starts_with('.') && basename != "." && basename != ".." {
            return Err(tinyfs::Error::Other("Hidden file skipped".to_string()));
        }

        match node_ref.node_type() {
            tinyfs::NodeType::File(file_handle) => {
                let content = file_handle.content().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "file".to_string(),
                    size: content.len(),
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: None,
                })
            }
            tinyfs::NodeType::Directory(_) => {
                Ok(FileInfo {
                    path,
                    node_type: "directory".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: None,
                })
            }
            tinyfs::NodeType::Symlink(symlink_handle) => {
                let target = symlink_handle.readlink().await.unwrap_or_default();
                Ok(FileInfo {
                    path,
                    node_type: "symlink".to_string(),
                    size: 0,
                    timestamp: None, // TODO: Extract from oplog metadata
                    symlink_target: Some(target.to_string_lossy().to_string()),
                })
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => init_command().await,
        Commands::Show => show_command().await,
        Commands::Cat { path } => cat_command(&path).await,
        Commands::Copy { source, dest } => copy_command(&source, &dest).await,
        Commands::Mkdir { path } => mkdir_command(&path).await,
        Commands::List { pattern, all } => list_command(&pattern, all).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_operations() -> Result<()> {
        let tmp = tempdir()?;
        
        // Set environment
        unsafe {
            std::env::set_var("POND", tmp.path());
        }

        // Test init
        init_command().await?;

        // Test show
        show_command().await?;

        Ok(())
    }
}
