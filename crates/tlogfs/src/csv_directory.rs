//! CSV Directory dynamic factory for TLogFS
//!
//! This factory creates dynamic directories that discover CSV files and present them as
//! converted Parquet file:table entries with transparent CSV-to-Parquet conversion.

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use std::path::PathBuf;
use std::pin::Pin;
use tinyfs::{FS, DirHandle, FileHandle, Result as TinyFSResult, Directory, File, NodeRef, Metadata, NodeMetadata, EntryType, AsyncReadSeek};
use async_trait::async_trait;
use tokio::io::AsyncWrite;
use diagnostics::*;
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;

// Arrow and DataFusion imports for CSV processing
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Configuration for CSV directory discovery and conversion
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CsvDirectoryConfig {
    /// Glob pattern for discovering CSV files (e.g., "/path/to/csvs/*.csv")
    pub source: String,
    /// Optional explicit schema definition
    pub schema: Option<Vec<CsvColumnConfig>>,
    /// Whether to treat first row as header (default: true)
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    /// Delimiter character (default: comma)
    #[serde(default = "default_delimiter")]
    pub delimiter: u8,
    /// Quote character (default: double quote)
    #[serde(default = "default_quote")]
    pub quote: u8,
    /// Cache timeout in seconds for converted Parquet (default: 24 hours)
    #[serde(default = "default_cache_timeout")]
    pub cache_timeout_seconds: u64,
}

/// Configuration for individual CSV columns when explicit schema is provided
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CsvColumnConfig {
    /// Column name
    pub name: String,
    /// Column data type (Arrow type string like "Int64", "Utf8", "Float64")
    pub data_type: String,
    /// Whether column is nullable (default: true)
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_has_header() -> bool { true }
fn default_delimiter() -> u8 { b',' }
fn default_quote() -> u8 { b'"' }
fn default_cache_timeout() -> u64 { 24 * 60 * 60 } // 24 hours
fn default_nullable() -> bool { true }

/// Dynamic directory that discovers and converts CSV files to Parquet on demand
pub struct CsvDirectory {
    config: CsvDirectoryConfig,
    context: FactoryContext,
}

/// Dynamic file representing a CSV converted to Parquet on demand
pub struct CsvFile {
    csv_path: PathBuf,
    config: CsvDirectoryConfig,
    context: FactoryContext,
}

impl CsvDirectoryConfig {
    /// Convert configured schema to Arrow Schema
    pub fn to_arrow_schema(&self) -> TinyFSResult<Option<SchemaRef>> {
        if let Some(ref schema_config) = self.schema {
            let fields: Result<Vec<Field>, tinyfs::Error> = schema_config.iter()
                .map(|col| {
                    let data_type = self.parse_data_type(&col.data_type)
                        .map_err(|e| tinyfs::Error::Other(e))?;
                    Ok(Field::new(&col.name, data_type, col.nullable))
                })
                .collect();
            
            match fields {
                Ok(fields) => Ok(Some(Arc::new(Schema::new(fields)))),
                Err(e) => Err(tinyfs::Error::Other(format!("Invalid schema configuration: {}", e))),
            }
        } else {
            Ok(None)
        }
    }

    /// Parse Arrow DataType from string
    fn parse_data_type(&self, type_str: &str) -> Result<DataType, String> {
        match type_str.to_lowercase().as_str() {
            "int8" => Ok(DataType::Int8),
            "int16" => Ok(DataType::Int16),
            "int32" => Ok(DataType::Int32),
            "int64" => Ok(DataType::Int64),
            "uint8" => Ok(DataType::UInt8),
            "uint16" => Ok(DataType::UInt16),
            "uint32" => Ok(DataType::UInt32),
            "uint64" => Ok(DataType::UInt64),
            "float32" => Ok(DataType::Float32),
            "float64" => Ok(DataType::Float64),
            "utf8" | "string" => Ok(DataType::Utf8),
            "boolean" | "bool" => Ok(DataType::Boolean),
            "timestamp" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)),
            "date32" => Ok(DataType::Date32),
            "date64" => Ok(DataType::Date64),
            _ => Err(format!("Unsupported data type: {}", type_str)),
        }
    }
}

impl CsvDirectory {
    pub fn new(config: CsvDirectoryConfig, context: FactoryContext) -> Self {
        info!("CsvDirectory::new_with_context - discovering CSV files with pattern {pattern}", 
                  pattern: config.source);
        Self {
            config,
            context,
        }
    }

    /// Create a DirHandle from this CSV directory
    pub fn create_handle(self) -> DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }

    /// Discover CSV files matching the configured pattern
    async fn discover_csv_files(&self) -> TinyFSResult<Vec<PathBuf>> {
        let pattern = &self.config.source;
        info!("CsvDirectory::discover_csv_files - scanning pattern {pattern}", 
                  pattern: pattern);

        let mut csv_files = Vec::new();

        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;

        // Use collect_matches to find CSV files with the given pattern
        match fs.root().await?.collect_matches(&pattern).await {
            Ok(matches) => {
                for (node_path, _captured) in matches {
                    let path_str = node_path.path.to_string_lossy().to_string();
                    if path_str.ends_with(".csv") {
                        info!("CsvDirectory::discover_csv_files - found CSV file {path}", 
                                  path: &path_str);
                        csv_files.push(PathBuf::from(path_str));
                    }
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!("CsvDirectory::discover_csv_files - failed to match pattern {pattern}: {error}", 
                           pattern: pattern, error: &error_msg);
                return Err(tinyfs::Error::Other(format!("Failed to match CSV pattern: {}", e)));
            }
        }
        
        let count = csv_files.len();
        info!("CsvDirectory::discover_csv_files - discovered {count} CSV files", 
                      count: count);
        Ok(csv_files)
    }
}

impl CsvFile {
    pub fn new(
        csv_path: PathBuf, 
        config: CsvDirectoryConfig, 
        context: FactoryContext,
    ) -> Self {
        Self {
            csv_path,
            config,
            context,
        }
    }

    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))

    }

    /// Convert CSV to Parquet data using streaming approach
    async fn convert_to_parquet(&self) -> TinyFSResult<Vec<u8>> {
        let path_str = self.csv_path.display().to_string();
        info!("CsvFile::convert_to_parquet - converting {path} to Parquet", 
                  path: path_str);
	
        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
	
        let root_wd = fs.root().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
                
        // Get streaming reader for the CSV file
        let path_string = self.csv_path.to_string_lossy().to_string();
        info!("CsvFile::convert_to_parquet - opening TinyFS stream for {path}", 
                  path: path_string);
        
        let csv_reader_stream = root_wd.async_reader_path(&path_string).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to open TinyFS file stream: {}", e)))?;
    
	// CSV to Parquet conversion using streaming approach (adapted from copy.rs)
	let parquet_data = self.csv_stream_to_parquet_conversion(csv_reader_stream).await?;
    
	let path_str = self.csv_path.display().to_string();
	let buffer_size = parquet_data.len();
        info!("CsvFile::convert_to_parquet - converted {path} to {size} bytes of Parquet", 
                  path: path_str,
                  size: buffer_size);
	
	Ok(parquet_data)
    }

    /// CSV stream to Parquet conversion logic (streaming approach)
    async fn csv_stream_to_parquet_conversion(&self, mut csv_stream: Pin<Box<dyn tinyfs::AsyncReadSeek>>) -> TinyFSResult<Vec<u8>> {
        use arrow_csv::{ReaderBuilder, reader::Format};
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        use std::sync::Arc;
        use tokio::io::AsyncReadExt;

        // Read the stream into a buffer for Arrow CSV processing
        // Note: Arrow CSV reader currently requires a seekable source, so we need to buffer
        // In the future, this could be optimized with a streaming CSV parser
        let mut csv_content = Vec::new();
        csv_stream.read_to_end(&mut csv_content).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to read CSV stream: {}", e)))?;

        // Step 1: Infer schema from CSV data
        let mut cursor = Cursor::new(&csv_content);
        let format = Format::default()
            .with_header(self.config.has_header)
            .with_delimiter(self.config.delimiter)
            .with_quote(self.config.quote);
            
        let (schema, _) = format.infer_schema(&mut cursor, Some(100))
            .map_err(|e| tinyfs::Error::Other(format!("Failed to infer CSV schema: {}", e)))?;

        // Step 2: Rewind and read CSV data
        cursor.set_position(0);
        
        let csv_reader = ReaderBuilder::new(Arc::new(schema))
            .with_format(format)
            .build(cursor)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to create CSV reader: {}", e)))?;

        // Step 3: Collect all batches first
        let mut batches = Vec::new();
        let mut batch_schema = None;
        
        for batch_result in csv_reader {
            let batch = batch_result
                .map_err(|e| tinyfs::Error::Other(format!("Failed to read CSV batch: {}", e)))?;
            
            if batch_schema.is_none() {
                batch_schema = Some(batch.schema());
            }
            
            batches.push(batch);
        }

        // Step 4: Write all batches to Parquet
        let mut parquet_buffer = Vec::new();
        
        if let Some(schema) = batch_schema {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None)
                .map_err(|e| tinyfs::Error::Other(format!("Failed to create Parquet writer: {}", e)))?;
            
            for batch in batches {
                writer.write(&batch)
                    .map_err(|e| tinyfs::Error::Other(format!("Failed to write Parquet batch: {}", e)))?;
            }

            writer.close()
                .map_err(|e| tinyfs::Error::Other(format!("Failed to close Parquet writer: {}", e)))?;
        }

        Ok(parquet_buffer)
    }
}

#[async_trait]
impl Directory for CsvDirectory {
    async fn get(&self, name: &str) -> tinyfs::Result<Option<NodeRef>> {
        info!("CsvDirectory::get - looking for {name}", name: name);

        let csv_files = self.discover_csv_files().await?;
        
        // Look for a CSV file that would produce this name when converted
        for csv_path in csv_files {
            let file_stem = csv_path.file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("unknown");
            
            // Check if this CSV file should produce a file with the requested name
            let parquet_name = format!("{}.parquet", file_stem);
            if name == parquet_name {
                let csv_path_str = csv_path.display().to_string();
                info!("CsvDirectory::get - found matching CSV file {path} for {name}", 
                          path: csv_path_str, name: name);
                
                let csv_file = CsvFile::new(
                    csv_path.clone(), 
                    self.config.clone(),
                    self.context.clone(),
                );
                let node_ref = tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                    id: tinyfs::NodeID::generate(),
                    node_type: tinyfs::NodeType::File(csv_file.create_handle()),
                })));
                return Ok(Some(node_ref));
            }
        }

        info!("CsvDirectory::get - no matching CSV file found for {name}", name: name);
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _id: NodeRef) -> tinyfs::Result<()> {
        info!("CsvDirectory::insert - mutation not permitted on CSV directory");
        Err(tinyfs::Error::Other("CSV directory is read-only".to_string()))
    }

    async fn entries(&self) -> tinyfs::Result<std::pin::Pin<Box<dyn futures::Stream<Item = tinyfs::Result<(String, NodeRef)>> + Send>>> {
        use futures::stream;
        
        info!("CsvDirectory::entries - listing discovered CSV files");
        
        let csv_files = self.discover_csv_files().await?;
        let mut entries = Vec::new();

        for csv_path in csv_files {
            let file_stem = csv_path.file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("unknown");
            
            // Create entry name as CSV filename with .parquet extension
            let entry_name = format!("{}.parquet", file_stem);
            let csv_path_str = csv_path.display().to_string();
            
            info!("CsvDirectory::entries - creating entry {name} from CSV {path}", 
                      name: entry_name, path: csv_path_str);
            
            let csv_file = CsvFile::new(
                csv_path.clone(), 
                self.config.clone(),
                self.context.clone(),
            );
            
            let node_ref = tinyfs::NodeRef::new(Arc::new(tokio::sync::Mutex::new(tinyfs::Node {
                id: tinyfs::NodeID::generate(),
                node_type: tinyfs::NodeType::File(csv_file.create_handle()),
            })));
            
            entries.push(Ok((entry_name, node_ref)));
        }

        let entries_len = entries.len();
        info!("CsvDirectory::entries - returning {count} entries", count: entries_len);
        Ok(Box::pin(stream::iter(entries)))
    }
}

#[async_trait]
impl Metadata for CsvDirectory {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::Directory,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl File for CsvFile {
    async fn async_reader(&self) -> tinyfs::Result<Pin<Box<dyn AsyncReadSeek>>> {
        let path_str = self.csv_path.display().to_string();
        info!("CsvFile::async_reader - providing Parquet data for {path}", 
                  path: path_str);
        
        let parquet_data = self.convert_to_parquet().await?;
        let cursor = std::io::Cursor::new(parquet_data.clone());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> tinyfs::Result<Pin<Box<dyn AsyncWrite + Send>>> {
        info!("CsvFile::async_writer - mutation not permitted on CSV file");
        Err(tinyfs::Error::Other("CSV file is read-only".to_string()))
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Metadata for CsvFile {
    async fn metadata(&self) -> tinyfs::Result<NodeMetadata> {
        let parquet_data = self.convert_to_parquet().await?;
        
        // Calculate SHA256 of the Parquet data
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&parquet_data);
        let sha256 = format!("{:x}", hasher.finalize());

        Ok(NodeMetadata {
            version: 1,
            size: Some(parquet_data.len() as u64),
            sha256: Some(sha256),
            entry_type: EntryType::FileTable,
                timestamp: 0, // Could use CSV file mtime here
        })
    }
}

// Factory functions for the linkme registration system
fn create_csv_dir_handle_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle> {
    let config: CsvDirectoryConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid CSV directory config: {}", e)))?;
    
    let csv_dir = CsvDirectory::new(config, context.clone());
    Ok(csv_dir.create_handle())
}

fn validate_csv_directory_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse the config as YAML first (since that's what users provide)
    let yaml_config: CsvDirectoryConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;
    
    // Validate the source pattern is provided
    if yaml_config.source.is_empty() {
        return Err(tinyfs::Error::Other("Source pattern cannot be empty".to_string()));
    }

    // Validate schema if provided
    if let Some(ref schema) = yaml_config.schema {
        for column in schema {
            // Try to parse the data type to validate it
            if let Err(e) = yaml_config.parse_data_type(&column.data_type) {
                return Err(tinyfs::Error::Other(format!("Invalid data type '{}' for column '{}': {}", 
                    column.data_type, column.name, e)));
            }
        }
    }

    // Convert to JSON Value for internal processing
    let json_value = serde_json::to_value(yaml_config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config to JSON: {}", e)))?;

    Ok(json_value)
}

// Register the CSV directory factory
register_dynamic_factory!(
    name: "csvdir",
    description: "Create dynamic directories that discover CSV files and present them as converted Parquet file:table entries",
    directory_with_context: create_csv_dir_handle_with_context,
    validate: validate_csv_directory_config
);
