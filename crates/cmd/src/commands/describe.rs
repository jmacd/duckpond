use anyhow::Result;

use crate::common::{FilesystemChoice, ShipContext, FileInfoVisitor};
use diagnostics::log_debug;

/// Describe command - shows file types and schemas for files matching the pattern
pub async fn describe_command(ship_context: &ShipContext, pattern: &str, filesystem: FilesystemChoice) -> Result<()> {
    log_debug!("describe_command called");

    let ship = ship_context.create_ship().await?;
    let fs = match filesystem {
        FilesystemChoice::Data => ship.data_fs(),
        FilesystemChoice::Control => ship.control_fs(),
    };

    let root = fs.root().await?;

    // Use the same file matching logic as list command
    let mut visitor = FileInfoVisitor::new(false); // don't show hidden files
    let mut files = root.visit_with_visitor(pattern, &mut visitor).await
        .map_err(|e| anyhow::anyhow!("Failed to find files with pattern '{}': {}", pattern, e))?;

    if files.is_empty() {
        println!("No files found matching pattern '{}'", pattern);
        return Ok(());
    }

    // Sort results by path for consistent output  
    files.sort_by(|a, b| a.path.cmp(&b.path));

    println!("=== File Schema Description ===");
    println!("Pattern: {}", pattern);
    println!("Files found: {}\n", files.len());

    for file_info in files {
        println!("📄 {}", file_info.path);
        println!("   Type: {:?}", file_info.metadata.entry_type);
        
        match file_info.metadata.entry_type {
            tinyfs::EntryType::FileSeries => {
                println!("   Format: Parquet series");
                match describe_file_series_schema(&root, &file_info.path).await {
                    Ok(schema_info) => {
                        println!("   Schema: {} fields", schema_info.field_count);
                        for field_info in schema_info.fields {
                            println!("     • {}: {}", field_info.name, field_info.data_type);
                        }
                        if let Some(timestamp_col) = schema_info.timestamp_column {
                            println!("   Timestamp Column: {}", timestamp_col);
                        }
                    }
                    Err(e) => {
                        println!("   Schema: Error loading schema - {}", e);
                    }
                }
            }
            tinyfs::EntryType::FileTable => {
                println!("   Format: Parquet table");
                match describe_file_table_schema(&root, &file_info.path).await {
                    Ok(schema_info) => {
                        println!("   Schema: {} fields", schema_info.field_count);
                        for field_info in schema_info.fields {
                            println!("     • {}: {}", field_info.name, field_info.data_type);
                        }
                    }
                    Err(e) => {
                        println!("   Schema: Error loading schema - {}", e);
                    }
                }
            }
            tinyfs::EntryType::FileData => {
                println!("   Format: Raw data");
                match detect_content_type(&root, &file_info.path).await {
                    Ok(content_type) => {
                        println!("   Content: {}", content_type);
                    }
                    Err(_) => {
                        println!("   Content: Binary/unknown");
                    }
                }
            }
            tinyfs::EntryType::Directory => {
                println!("   Format: Directory");
                // For directories, we can't easily get the entry count without iterating
                println!("   Contains: Multiple entries");
            }
            tinyfs::EntryType::Symlink => {
                println!("   Format: Symbolic link");
                if let Some(target) = &file_info.symlink_target {
                    println!("   Target: {}", target);
                } else {
                    println!("   Target: Unknown");
                }
            }
        }
        
        println!("   Size: {} bytes", file_info.metadata.size.unwrap_or(0));
        println!("   Version: {}", file_info.metadata.version);
        println!();
    }

    Ok(())
}

#[derive(Debug)]
pub struct SchemaInfo {
    pub field_count: usize,
    pub fields: Vec<FieldInfo>,
    pub timestamp_column: Option<String>,
}

#[derive(Debug)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
}

/// Describe the schema of a file:series
async fn describe_file_series_schema(root: &tinyfs::WD, path: &str) -> Result<SchemaInfo> {
    // Get the first version to read its schema
    let file_versions = root.list_file_versions(path).await
        .map_err(|e| anyhow::anyhow!("Failed to list file versions: {}", e))?;
    
    if file_versions.is_empty() {
        return Err(anyhow::anyhow!("No versions found in series"));
    }

    // Read the first version to get the schema
    let first_version = &file_versions[0];
    let version_data = root.read_file_version(path, Some(first_version.version)).await
        .map_err(|e| anyhow::anyhow!("Failed to read version {}: {}", first_version.version, e))?;

    // Parse the Parquet schema
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    let cursor = std::io::Cursor::new(version_data);
    let builder = ParquetRecordBatchStreamBuilder::new(cursor).await
        .map_err(|e| anyhow::anyhow!("Failed to read Parquet schema: {}", e))?;
    
    let schema = builder.schema();
    
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
        });
    }
    
    // Try to detect timestamp column
    let timestamp_column = schema.fields().iter()
        .find(|field| {
            matches!(field.name().to_lowercase().as_str(), 
                "timestamp" | "time" | "event_time" | "ts" | "datetime")
        })
        .map(|field| field.name().clone());
    
    Ok(SchemaInfo {
        field_count: schema.fields().len(),
        fields,
        timestamp_column,
    })
}

/// Describe the schema of a file:table
async fn describe_file_table_schema(root: &tinyfs::WD, path: &str) -> Result<SchemaInfo> {
    // Read the file content - use None for latest version
    let file_data = root.read_file_version(path, None).await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;

    // Parse the Parquet schema
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    let cursor = std::io::Cursor::new(file_data);
    let builder = ParquetRecordBatchStreamBuilder::new(cursor).await
        .map_err(|e| anyhow::anyhow!("Failed to read Parquet schema: {}", e))?;
    
    let schema = builder.schema();
    
    let mut fields = Vec::new();
    for field in schema.fields() {
        fields.push(FieldInfo {
            name: field.name().clone(),
            data_type: format!("{:?}", field.data_type()),
        });
    }
    
    Ok(SchemaInfo {
        field_count: schema.fields().len(),
        fields,
        timestamp_column: None, // file:table doesn't necessarily have timestamp semantics
    })
}

/// Try to detect content type for file:data
async fn detect_content_type(root: &tinyfs::WD, path: &str) -> Result<String> {
    // Read the first few bytes to detect content type - use None for latest version
    let file_data = root.read_file_version(path, None).await
        .map_err(|e| anyhow::anyhow!("Failed to read file: {}", e))?;
    
    if file_data.is_empty() {
        return Ok("Empty file".to_string());
    }
    
    // Check for common file types
    if file_data.len() >= 4 {
        let header = &file_data[0..4];
        if header == b"PAR1" {
            return Ok("Parquet file".to_string());
        }
        if header.starts_with(b"\x7fELF") {
            return Ok("ELF binary".to_string());
        }
        if header.starts_with(b"\x89PNG") {
            return Ok("PNG image".to_string());
        }
        if header.starts_with(b"GIF8") {
            return Ok("GIF image".to_string());
        }
        if header.starts_with(b"\xff\xd8\xff") {
            return Ok("JPEG image".to_string());
        }
        if header == b"PK\x03\x04" || header == b"PK\x05\x06" || header == b"PK\x07\x08" {
            return Ok("ZIP archive".to_string());
        }
    }
    
    // Check if it's text
    if file_data.iter().take(1024).all(|&b| b.is_ascii() && (b >= 32 || b == b'\n' || b == b'\r' || b == b'\t')) {
        // Try to guess text format
        let sample = String::from_utf8_lossy(&file_data[0..1024.min(file_data.len())]);
        if sample.contains(',') && sample.lines().count() > 1 {
            return Ok("CSV text".to_string());
        }
        if sample.starts_with('{') || sample.starts_with('[') {
            return Ok("JSON text".to_string());
        }
        if sample.contains('\t') {
            return Ok("TSV text".to_string());
        }
        return Ok("Plain text".to_string());
    }
    
    Ok(format!("Binary data ({} bytes)", file_data.len()))
}
