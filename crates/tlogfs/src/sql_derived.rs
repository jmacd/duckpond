//! SQL-derived dynamic node factory for TLogFS
//!
//! This factory enables creation of dynamic tables derived from SQL queries over existing pond data.

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use tinyfs::{FileHandle, Result as TinyFSResult, File, Metadata, NodeMetadata, EntryType, AsyncReadSeek, FS};
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use datafusion::datasource::{MemTable, TableProvider};
use async_trait::async_trait;
use tokio::io::AsyncWrite;
use tokio_util::bytes::Bytes;

/// Configuration for SQL-derived file generation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlDerivedConfig {
    /// Source data path (can be FileSeries or regular file)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Source pattern for matching multiple FileSeries files (e.g., "/data/*.parquet", "/**/*.parquet")
    /// This mode only supports FileSeries files
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_pattern: Option<String>,
    /// SQL query to execute on the source data  
    pub query: String,
}

/// File node that executes SQL query and returns Parquet data
#[derive(Clone)] 
pub struct SqlDerivedFile {
    config: SqlDerivedConfig,
    context: FactoryContext,
}

impl SqlDerivedFile {
    pub fn new(config: SqlDerivedConfig, context: FactoryContext) -> TinyFSResult<Self> {
        Ok(Self { config, context })
    }
    
    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Execute the SQL query and return results as Parquet bytes
    async fn execute_query_to_parquet(&self) -> TinyFSResult<Vec<u8>> {
        // Create DataFusion context
        let ctx = SessionContext::new();
        
        // Create filesystem access using the context pattern from csv_directory
        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS: {}", e)))?;
        
        let tinyfs_root = fs.root().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get TinyFS root: {}", e)))?;
        
        // Determine which mode we're in: single source or pattern matching
        let table_provider: Arc<dyn TableProvider> = if let Some(source_path) = &self.config.source {
            // Single source mode - resolve the source path
            let node_path = tinyfs_root.get_node_path(source_path).await
                .map_err(|e| tinyfs::Error::Other(format!("Source path '{}' not found: {}", source_path, e)))?;
            
            // Get node reference and determine type
            let node_ref = node_path.borrow().await;
            let file_node = node_ref.as_file()
                .map_err(|e| tinyfs::Error::Other(format!("Source is not a file: {e}")))?;
            let node_metadata = file_node.metadata().await
                .map_err(|e| tinyfs::Error::Other(format!("Failed to get metadata: {e}")))?;
            
            // Create appropriate TableProvider based on source type
            match node_metadata.entry_type {
                EntryType::FileSeries => {
                    println!("Creating SeriesTable for FileSeries source: {}", source_path);
                    
                    // For FileSeries, we need to read ALL versions and union them
                    // This is different from FileTable which just has one version
                    self.create_memtable_from_file_series(&[source_path.clone()]).await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to create MemTable from FileSeries: {e}")))?
                }
                EntryType::FileTable => {
                    println!("Creating MemTable for FileTable source: {}", source_path);
                    
                    // Read file content directly and create MemTable
                    use tokio::io::AsyncReadExt;
                    let mut reader = file_node.async_reader().await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to create file reader: {e}")))?;
                    let mut content = Vec::new();
                    reader.read_to_end(&mut content).await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to read file content: {e}")))?;
                    
                    self.create_memtable_from_bytes(content).await
                        .map_err(|e| tinyfs::Error::Other(format!("Failed to create MemTable: {e}")))?
                }
                _ => {
                    return Err(tinyfs::Error::Other(format!("Unsupported source type: {:?}", node_metadata.entry_type)));
                }
            }
        } else if let Some(pattern) = &self.config.source_pattern {
            // Pattern matching mode - find all matching FileSeries files
            println!("Using pattern matching for source: {}", pattern);
            
            let matching_files = self.resolve_pattern_to_file_series(&tinyfs_root, pattern).await?;
            
            if matching_files.is_empty() {
                return Err(tinyfs::Error::Other(format!("No FileSeries files found matching pattern: {}", pattern)));
            }
            
            println!("Found {} matching FileSeries files", matching_files.len());
            
            // Create unified MemTable from all matching FileSeries
            self.create_memtable_from_file_series(&matching_files).await
                .map_err(|e| tinyfs::Error::Other(format!("Failed to create MemTable from pattern: {e}")))?
        } else {
            return Err(tinyfs::Error::Other("Either source or source_pattern must be specified".to_string()));
        };
        
        // Register the source table in DataFusion
        let table_name = "source";
        ctx.register_table(table_name, table_provider)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to register table: {}", e)))?;
        
        println!("Executing SQL query: {}", self.config.query);
        
        // Execute the SQL query
        let df = ctx.sql(&self.config.query).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to execute query: {e}")))?;
        
        // Collect results into batches
        let batches = df.collect().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to collect query results: {e}")))?;
        
        if batches.is_empty() {
            return Ok(Vec::new()); // Empty result is valid
        }
        
        // Convert results to Parquet format
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let schema = batches[0].schema();
        let mut parquet_buffer = Vec::new();
        let cursor = Cursor::new(&mut parquet_buffer);
        
        let mut writer = ArrowWriter::try_new(cursor, schema, None)
            .map_err(|e| tinyfs::Error::Other(format!("Failed to create Parquet writer: {e}")))?;
        
        for batch in batches {
            writer.write(&batch)
                .map_err(|e| tinyfs::Error::Other(format!("Failed to write Parquet batch: {e}")))?;
        }
        
        writer.close()
            .map_err(|e| tinyfs::Error::Other(format!("Failed to close Parquet writer: {e}")))?;
        
        println!("Generated {} bytes of Parquet data", parquet_buffer.len());
        Ok(parquet_buffer)
    }
    
    /// Create MemTable from Parquet bytes
    async fn create_memtable_from_bytes(&self, content: Vec<u8>) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        // Parse Parquet data using Bytes (which implements ChunkReader)
        let bytes = Bytes::from(content);
        let parquet_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| DataFusionError::Plan(format!("Failed to create Parquet reader: {e}")))?;
        
        let schema = parquet_reader.schema().clone();
        let mut batches = Vec::new();
        
        // Read all record batches
        for batch_result in parquet_reader.build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to build Parquet reader: {e}")))? {
            let batch = batch_result
                .map_err(|e| DataFusionError::Plan(format!("Failed to read Parquet batch: {e}")))?;
            batches.push(batch);
        }
        
        // Create MemTable
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| DataFusionError::Plan(format!("Failed to create MemTable: {e}")))?;
        
        Ok(Arc::new(table))
    }

    /// Create MemTable from FileSeries by reading all versions and unioning them
    async fn create_memtable_from_file_series(&self, source_paths: &[String]) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        // Get TinyFS root to access file versions directly
        let fs = FS::new(self.context.state.clone()).await
            .map_err(|e| DataFusionError::Plan(format!("Failed to get TinyFS: {e}")))?;
        let tinyfs_root = fs.root().await
            .map_err(|e| DataFusionError::Plan(format!("Failed to get TinyFS root: {e}")))?;
        
        let mut all_batches = Vec::new();
        let mut unified_schema: Option<arrow::datatypes::SchemaRef> = None;
        
        // Process each source file
        for source_path in source_paths {
            println!("Scanning FileSeries versions for path: {}", source_path);
            
            // For each FileSeries, discover all versions and union them
            let mut version = 1u64;
            
            loop {
                match tinyfs_root.read_file_version(source_path, Some(version)).await {
                    Ok(version_data) => {
                        println!("Found version {} with {} bytes in {}", version, version_data.len(), source_path);
                        
                        // Parse this version's Parquet data
                        let bytes = tokio_util::bytes::Bytes::from(version_data);
                        let parquet_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                            .map_err(|e| DataFusionError::Plan(format!("Failed to create Parquet reader for version {} of {}: {e}", version, source_path)))?;
                        
                        // Get schema from first version, validate compatibility for subsequent versions
                        let schema = parquet_reader.schema().clone();
                        if unified_schema.is_none() {
                            unified_schema = Some(schema.clone());
                        } else {
                            // In a production implementation, we'd validate schema compatibility here
                            // For now, we assume all versions have compatible schemas
                        }
                        
                        // Read all record batches from this version
                        let mut reader = parquet_reader.build()
                            .map_err(|e| DataFusionError::Plan(format!("Failed to build Parquet reader for version {} of {}: {e}", version, source_path)))?;
                        
                        while let Some(batch_result) = reader.next() {
                            let batch = batch_result
                                .map_err(|e| DataFusionError::Plan(format!("Failed to read batch from version {} of {}: {e}", version, source_path)))?;
                            all_batches.push(batch);
                        }
                        
                        version += 1;
                    }
                    Err(_) => {
                        // No more versions available for this file
                        println!("No more versions found in {}. Total versions processed: {}", source_path, version - 1);
                        break;
                    }
                }
            }
        }
        
        if all_batches.is_empty() {
            return Err(DataFusionError::Plan(format!("No data found in FileSeries: {:?}", source_paths)));
        }
        
        let schema = unified_schema.unwrap();
        let total_batches = all_batches.len();
        let total_files = source_paths.len();
        
        // Create MemTable with all batches from all versions of all files
        let table = MemTable::try_new(schema, vec![all_batches])
            .map_err(|e| DataFusionError::Plan(format!("Failed to create unified MemTable: {e}")))?;
        
        println!("Created unified MemTable from {} files with {} total batches", total_files, total_batches);
        Ok(Arc::new(table))
    }

    /// Resolve a pattern to a list of FileSeries file paths
    async fn resolve_pattern_to_file_series(&self, tinyfs_root: &tinyfs::WD, pattern: &str) -> TinyFSResult<Vec<String>> {
        // Use TinyFS collect_matches to find all files matching the pattern
        let matches = tinyfs_root.collect_matches(pattern).await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to resolve pattern '{}': {}", pattern, e)))?;
        
        let mut file_series_paths = Vec::new();
        
        for (node_path, _captured) in matches {
            let node_ref = node_path.borrow().await;
            
            // Check if this is a file
            if let Ok(file_node) = node_ref.as_file() {
                // Check if it's a FileSeries
                if let Ok(metadata) = file_node.metadata().await {
                    if metadata.entry_type == EntryType::FileSeries {
                        // Get the path as a string
                        let path_str = node_path.path().to_string_lossy().to_string();
                        file_series_paths.push(path_str);
                    }
                }
            }
        }
        
        Ok(file_series_paths)
    }
}

// Async trait implementations

#[async_trait]
impl File for SqlDerivedFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        let parquet_data = self.execute_query_to_parquet().await?;
        let cursor = std::io::Cursor::new(parquet_data);
        Ok(Box::pin(cursor))
    }
    
    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("SQL-derived file is read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for SqlDerivedFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        let parquet_data = self.execute_query_to_parquet().await?;
        
        // Calculate SHA256
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&parquet_data);
        let sha256 = format!("{:x}", hasher.finalize());
        
        Ok(NodeMetadata {
            version: 1,
            size: Some(parquet_data.len() as u64),
            sha256: Some(sha256),
            entry_type: EntryType::FileTable,
            timestamp: 0,
        })
    }
}

// Factory functions for linkme registration

fn create_sql_derived_handle_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle> {
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    let sql_file = SqlDerivedFile::new(cfg, context.clone())?;
    Ok(sql_file.create_handle())
}

fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
    // Parse as YAML first (user format)
    let yaml_config: SqlDerivedConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid YAML config: {}", e)))?;
    
    // Validate that exactly one of source or source_pattern is provided
    match (&yaml_config.source, &yaml_config.source_pattern) {
        (Some(source), None) => {
            if source.is_empty() {
                return Err(tinyfs::Error::Other("Source path cannot be empty".to_string()));
            }
        }
        (None, Some(pattern)) => {
            if pattern.is_empty() {
                return Err(tinyfs::Error::Other("Source pattern cannot be empty".to_string()));
            }
        }
        (Some(_), Some(_)) => {
            return Err(tinyfs::Error::Other("Cannot specify both source and source_pattern".to_string()));
        }
        (None, None) => {
            return Err(tinyfs::Error::Other("Must specify either source or source_pattern".to_string()));
        }
    }
    
    if yaml_config.query.is_empty() {
        return Err(tinyfs::Error::Other("SQL query cannot be empty".to_string()));
    }
    
    // Convert to JSON for internal use
    serde_json::to_value(yaml_config)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config: {}", e)))
}

// Register the factory
register_dynamic_factory!(
    name: "sql-derived",
    description: "Create dynamic SQL-derived tables from pond data sources",
    file_with_context: create_sql_derived_handle_with_context,
    validate: validate_sql_derived_config
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::OpLogPersistence;
    use tempfile::TempDir;

    /// Helper function to set up test environment with sample Parquet data
    async fn setup_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create TinyFS root to work with
        let fs = FS::new(state.clone()).await.unwrap();
        let root = fs.root().await.unwrap();
        
        // Create test Parquet data with meaningful content
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"])),
                Arc::new(Int32Array::from(vec![100, 200, 150, 300, 250])),
            ],
        ).unwrap();
        
        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        
        // Create the source data file using TinyFS convenience API
        use tinyfs::async_helpers::convenience;
        let _data_file = convenience::create_file_path_with_type(
            &root, 
            "/data.parquet", 
            &parquet_buffer,
            EntryType::FileTable
        ).await.unwrap();
        
        // Commit this transaction so the source file is visible to subsequent reads
        tx_guard.commit(None).await.unwrap();
    }

    /// Helper function to set up test environment with FileSeries data
    async fn setup_file_series_test_data(persistence: &mut OpLogPersistence) {
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create TinyFS root to work with
        let fs = FS::new(state.clone()).await.unwrap();
        let root = fs.root().await.unwrap();
        
        // Create test Parquet data with meaningful content (different from FileTable test)
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Int32, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("reading", DataType::Int32, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![101, 102, 103, 104, 105])),
                Arc::new(StringArray::from(vec!["Building A", "Building B", "Building C", "Building A", "Building B"])),
                Arc::new(Int32Array::from(vec![75, 82, 68, 90, 77])),
            ],
        ).unwrap();
        
        // Write to Parquet format
        let mut parquet_buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut parquet_buffer);
            let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        
        // Create the source data as FileSeries (not FileTable)
        use tinyfs::async_helpers::convenience;
        let _series_file = convenience::create_file_path_with_type(
            &root, 
            "/sensor_data.parquet", 
            &parquet_buffer,
            EntryType::FileSeries  // This is the key difference
        ).await.unwrap();
        
        // Commit this transaction so the source file is visible to subsequent reads
        tx_guard.commit(None).await.unwrap();
    }

    /// Helper function to set up multi-version FileSeries test data
    async fn setup_file_series_multi_version_data(persistence: &mut OpLogPersistence, num_versions: usize) {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("sensor_id", DataType::Int32, false),
            Field::new("location", DataType::Utf8, false),
            Field::new("reading", DataType::Int32, false),
        ]));

        for version in 1..=num_versions {
            let tx_guard = persistence.begin().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            
            // Create different data for each version
            let base_sensor_id = 100 + (version * 10) as i32;
            let base_reading = 70 + (version * 10) as i32;
            
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![
                        base_sensor_id + 1, 
                        base_sensor_id + 2, 
                        base_sensor_id + 3
                    ])),
                    Arc::new(StringArray::from(vec![
                        format!("Building {}", version),
                        format!("Building {}", version),
                        format!("Building {}", version),
                    ])),
                    Arc::new(Int32Array::from(vec![
                        base_reading, 
                        base_reading + 5, 
                        base_reading + 10
                    ])),
                ],
            ).unwrap();
            
            // Write to Parquet format
            let mut parquet_buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            }
            
            // Write to the SAME path for all versions - TLogFS will handle versioning
            let mut writer = root.async_writer_path_with_type("/multi_sensor_data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_sql_derived_config_validation() {
        let valid_config = r#"
source: "/test/data.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;
        
        let result = validate_sql_derived_config(valid_config.as_bytes());
        assert!(result.is_ok());

        // Test valid pattern config
        let valid_pattern_config = r#"
source_pattern: "/data/*.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;
        
        let result = validate_sql_derived_config(valid_pattern_config.as_bytes());
        assert!(result.is_ok());
        
        // Test empty source
        let invalid_config = r#"
source: ""
query: "SELECT * FROM source"
"#;
        
        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
        
        // Test empty pattern
        let invalid_config = r#"
source_pattern: ""
query: "SELECT * FROM source"
"#;
        
        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
        
        // Test both source and pattern specified
        let invalid_config = r#"
source: "/test/data.parquet"
source_pattern: "/data/*.parquet"
query: "SELECT * FROM source"
"#;
        
        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
        
        // Test neither source nor pattern specified
        let invalid_config = r#"
query: "SELECT * FROM source"
"#;
        
        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
        
        // Test empty query
        let invalid_config = r#"
source: "/test/data.parquet"
query: ""
"#;
        
        let result = validate_sql_derived_config(invalid_config.as_bytes());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_derived_pattern_matching() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up multiple FileSeries files that match a pattern
        {
            // Create sensor_data1.parquet
            let tx_guard = persistence.begin().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;
            
            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));
            
            // First file: sensor_data1.parquet
            let batch1 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![101, 102])),
                    Arc::new(StringArray::from(vec!["Building A", "Building B"])),
                    Arc::new(Int32Array::from(vec![80, 85])),
                ],
            ).unwrap();
            
            let mut parquet_buffer1 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer1);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch1).unwrap();
                writer.close().unwrap();
            }
            
            let mut writer = root.async_writer_path_with_type("/sensor_data1.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer1).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        {
            // Create sensor_data2.parquet
            let tx_guard = persistence.begin().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;
            
            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));
            
            // Second file: sensor_data2.parquet  
            let batch2 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![201, 202])),
                    Arc::new(StringArray::from(vec!["Building C", "Building D"])),
                    Arc::new(Int32Array::from(vec![90, 95])),
                ],
            ).unwrap();
            
            let mut parquet_buffer2 = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer2);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch2).unwrap();
                writer.close().unwrap();
            }
            
            let mut writer = root.async_writer_path_with_type("/sensor_data2.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer2).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        // Now test pattern matching across both files
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: None,
            source_pattern: Some("/sensor_data*.parquet".to_string()),
            query: "SELECT location, reading FROM source WHERE reading > 85 ORDER BY reading DESC".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents from both files
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from both files matching reading > 85
        // File 1: Building A (80) - excluded, Building B (85) - excluded because 85 is not > 85  
        // File 2: Building C (90), Building D (95) - both included
        // So we expect 2 rows
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check that we have the right data ordered by reading DESC
        use arrow::array::{StringArray, Int32Array};
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let readings = result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        
        // Should be ordered by reading DESC: Building D (95), Building C (90)
        assert_eq!(locations.value(0), "Building D");
        assert_eq!(readings.value(0), 95);
        
        assert_eq!(locations.value(1), "Building C");
        assert_eq!(readings.value(1), 90);
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_recursive_pattern_matching() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries files in different directories
        {
            // Create /sensors/building_a/data.parquet
            let tx_guard = persistence.begin().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;
            
            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));
            
            let batch_a = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![301])),
                    Arc::new(StringArray::from(vec!["Building A"])),
                    Arc::new(Int32Array::from(vec![100])),
                ],
            ).unwrap();
            
            let mut parquet_buffer_a = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_a);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_a).unwrap();
                writer.close().unwrap();
            }
            
            // Create directory first, then file
            root.create_dir_path("/sensors").await.unwrap();
            root.create_dir_path("/sensors/building_a").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/sensors/building_a/data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_a).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        {
            // Create /sensors/building_b/data.parquet
            let tx_guard = persistence.begin().await.unwrap();
            let root = tx_guard.root().await.unwrap();
            
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use parquet::arrow::ArrowWriter;
            use std::io::Cursor;
            
            let schema = Arc::new(Schema::new(vec![
                Field::new("sensor_id", DataType::Int32, false),
                Field::new("location", DataType::Utf8, false),
                Field::new("reading", DataType::Int32, false),
            ]));
            
            let batch_b = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![302])),
                    Arc::new(StringArray::from(vec!["Building B"])),
                    Arc::new(Int32Array::from(vec![110])),
                ],
            ).unwrap();
            
            let mut parquet_buffer_b = Vec::new();
            {
                let cursor = Cursor::new(&mut parquet_buffer_b);
                let mut writer = ArrowWriter::try_new(cursor, schema.clone(), None).unwrap();
                writer.write(&batch_b).unwrap();
                writer.close().unwrap();
            }
            
            root.create_dir_path("/sensors/building_b").await.unwrap();
            let mut writer = root.async_writer_path_with_type("/sensors/building_b/data.parquet", EntryType::FileSeries).await.unwrap();
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_buffer_b).await.unwrap();
            writer.flush().await.unwrap();
            writer.shutdown().await.unwrap();
            
            tx_guard.commit(None).await.unwrap();
        }
        
        // Now test recursive pattern matching across all nested files
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: None,
            source_pattern: Some("/**/data.parquet".to_string()),
            query: "SELECT location, reading, sensor_id FROM source ORDER BY sensor_id".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents from both nested files
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from both nested directories
        // Building A: sensor_id 301, reading 100
        // Building B: sensor_id 302, reading 110
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 3);
        
        use arrow::array::{StringArray, Int32Array};
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let readings = result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let sensor_ids = result_batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        
        // Should be ordered by sensor_id: Building A (301), Building B (302)
        assert_eq!(sensor_ids.value(0), 301);
        assert_eq!(locations.value(0), "Building A");
        assert_eq!(readings.value(0), 100);
        
        assert_eq!(sensor_ids.value(1), 302);
        assert_eq!(locations.value(1), "Building B");
        assert_eq!(readings.value(1), 110);
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_single_version() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data (single version)
        setup_file_series_test_data(&mut persistence).await;
        
        // Create and test the SQL-derived file with FileSeries source
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with FileSeries source
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: Some("/sensor_data.parquet".to_string()),
            source_pattern: None,
            query: "SELECT location, reading * 1.5 as adjusted_reading FROM source WHERE reading > 75 ORDER BY adjusted_reading DESC".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // Verify we got the expected derived data from FileSeries
        // Original data: sensor readings [75, 82, 68, 90, 77] for locations ["Building A", "Building B", "Building C", "Building A", "Building B"]
        // Query: WHERE reading > 75, SELECT location, reading * 1.5 as adjusted_reading, ORDER BY adjusted_reading DESC
        // Expected: Building A (90*1.5=135), Building B (82*1.5=123), Building B (77*1.5=115.5)
        // Note: 75 is not > 75, and 68 < 75, so they're excluded
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "adjusted_reading");
        
        // This transaction is read-only, so just let it end without committing
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_two_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data with 2 versions
        setup_file_series_multi_version_data(&mut persistence, 2).await;
        
        // For now, test against the first version until we implement union logic
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with multi-version FileSeries source
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: Some("/multi_sensor_data.parquet".to_string()),
            source_pattern: None,
            query: "SELECT location, reading FROM source WHERE reading > 75 ORDER BY reading DESC".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // For now, we expect data from version 1 only (readings: [70, 75, 80])
        // Query: WHERE reading > 75, so we should get reading=80 from Building 1
        assert!(result_batch.num_rows() >= 1, "Should have at least 1 row with reading > 75");
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "location");
        assert_eq!(schema.field(1).name(), "reading");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_file_series_three_versions() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up FileSeries test data with 3 versions
        setup_file_series_multi_version_data(&mut persistence, 3).await;
        
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file that should union all 3 versions
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: Some("/multi_sensor_data.parquet".to_string()),
            source_pattern: None,
            // This query should return data from all 3 versions
            query: "SELECT location, reading, sensor_id FROM source ORDER BY sensor_id".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents from all versions
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // We should have data from all 3 versions (3 rows per version = 9 total rows)
        // Version 1: sensor_ids 111, 112, 113 (Building 1)
        // Version 2: sensor_ids 121, 122, 123 (Building 2)  
        // Version 3: sensor_ids 131, 132, 133 (Building 3)
        assert_eq!(result_batch.num_rows(), 9);
        assert_eq!(result_batch.num_columns(), 3);
        
        // Check that we have sensor IDs from all versions
        use arrow::array::{StringArray, Int32Array};
        let sensor_ids = result_batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let locations = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        
        // Verify we have sensor IDs from all 3 versions (ordered by sensor_id)
        assert_eq!(sensor_ids.value(0), 111); // First version
        assert_eq!(locations.value(0), "Building 1");
        
        assert_eq!(sensor_ids.value(3), 121); // Second version (after 111,112,113)
        assert_eq!(locations.value(3), "Building 2");
        
        assert_eq!(sensor_ids.value(6), 131); // Third version (after 111,112,113,121,122,123)
        assert_eq!(locations.value(6), "Building 3");
        
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up test data
        setup_test_data(&mut persistence).await;
        
        // Create and test the SQL-derived file  
        let tx_guard = persistence.begin().await.unwrap();
        let state = tx_guard.state().unwrap();
        
        // Create the SQL-derived file with read-only state context
        let context = FactoryContext::new(state);
        let config = SqlDerivedConfig {
            source: Some("/data.parquet".to_string()),
            source_pattern: None,
            query: "SELECT name, value * 2 as doubled_value FROM source WHERE value > 150 ORDER BY doubled_value DESC".to_string(),
        };

        let sql_derived_file = SqlDerivedFile::new(config, context).unwrap();
        
        // Read the Parquet result and verify contents
        let mut reader = sql_derived_file.async_reader().await.unwrap();
        let mut result_data = Vec::new();
        use tokio::io::AsyncReadExt;
        reader.read_to_end(&mut result_data).await.unwrap();
        
        // Parse and verify the results
        use tokio_util::bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        
        let bytes = Bytes::from(result_data);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut record_batch_reader = parquet_reader.build().unwrap();
        
        let result_batch = record_batch_reader.next().unwrap().unwrap();
        
        // Verify we got the expected derived data
        // Query: "SELECT name, value * 2 as doubled_value FROM source WHERE value > 150 ORDER BY doubled_value DESC"
        // Expected: David (300*2=600), Eve (250*2=500), Bob (200*2=400)
        // Note: Charlie (150) is excluded because 150 is not > 150
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);
        
        // Check column names
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "doubled_value");
        
        // Check data - should be ordered by doubled_value DESC
        use arrow::array::{StringArray, Int64Array};
        let names = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let doubled_values = result_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(names.value(0), "David");   // 300 * 2 = 600 (highest)
        assert_eq!(doubled_values.value(0), 600);
        
        assert_eq!(names.value(1), "Eve");     // 250 * 2 = 500 (second)
        assert_eq!(doubled_values.value(1), 500);
        
        assert_eq!(names.value(2), "Bob");     // 200 * 2 = 400 (third)
        assert_eq!(doubled_values.value(2), 400);
        
        // This transaction is read-only, so just let it end without committing
        tx_guard.commit(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_derived_chain() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // Set up test data
        setup_test_data(&mut persistence).await;
        
        // Test chaining: Create two SQL-derived nodes, where one refers to the other
        {
            let tx_guard = persistence.begin().await.unwrap();
            let state = tx_guard.state().unwrap();
            
            // Create TinyFS root to work with for storing intermediate results
            let fs = FS::new(state.clone()).await.unwrap();
            let root = fs.root().await.unwrap();
            
            // Create the first SQL-derived file (filters and transforms original data)
            let context = FactoryContext::new(state.clone());
            let first_config = SqlDerivedConfig {
                source: Some("/data.parquet".to_string()),
                source_pattern: None,
                query: "SELECT name, value + 50 as adjusted_value FROM source WHERE value >= 200 ORDER BY adjusted_value".to_string(),
            };
            
            let first_sql_file = SqlDerivedFile::new(first_config, context.clone()).unwrap();
            
            // Read the first SQL-derived result and store it as an intermediate file
            let mut first_reader = first_sql_file.async_reader().await.unwrap();
            let mut first_result_data = Vec::new();
            use tokio::io::AsyncReadExt;
            first_reader.read_to_end(&mut first_result_data).await.unwrap();
            
            // Store the first result as an intermediate Parquet file
            use tinyfs::async_helpers::convenience;
            let _intermediate_file = convenience::create_file_path_with_type(
                &root,
                "/intermediate.parquet",
                &first_result_data,
                EntryType::FileTable
            ).await.unwrap();
            
            // Commit to make the intermediate file visible
            tx_guard.commit(None).await.unwrap();
        }
        
        // Second transaction: Create the second SQL-derived node that chains from the first
        {
            let tx_guard = persistence.begin().await.unwrap();
            let state = tx_guard.state().unwrap();
            
            let context = FactoryContext::new(state);
            let second_config = SqlDerivedConfig {
                source: Some("/intermediate.parquet".to_string()),
                source_pattern: None,
                query: "SELECT name, adjusted_value * 2 as final_value FROM source WHERE adjusted_value > 250 ORDER BY final_value DESC".to_string(),
            };
            
            let second_sql_file = SqlDerivedFile::new(second_config, context).unwrap();
            
            // Read the final chained result
            let mut second_reader = second_sql_file.async_reader().await.unwrap();
            let mut final_result_data = Vec::new();
            use tokio::io::AsyncReadExt;
            second_reader.read_to_end(&mut final_result_data).await.unwrap();
            
            // Parse and verify the final chained results
            use tokio_util::bytes::Bytes;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            let bytes = Bytes::from(final_result_data);
            let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let mut record_batch_reader = parquet_reader.build().unwrap();
            
            let result_batch = record_batch_reader.next().unwrap().unwrap();
            
            // Verify the chained transformation worked correctly
            // Original data: Alice=100, Bob=200, Charlie=150, David=300, Eve=250
            // First query: WHERE value >= 200, SELECT value + 50 as adjusted_value
            //   -> Bob=250, David=350, Eve=300
            // Second query: WHERE adjusted_value > 250, SELECT adjusted_value * 2 as final_value, ORDER BY final_value DESC
            //   -> David=700, Eve=600 (Bob excluded because 250 is not > 250)
            assert_eq!(result_batch.num_rows(), 2);
            assert_eq!(result_batch.num_columns(), 2);
            
            // Check column names
            let schema = result_batch.schema();
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(schema.field(1).name(), "final_value");
            
            // Check data - should be ordered by final_value DESC
            use arrow::array::{StringArray, Int64Array};
            let names = result_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let final_values = result_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            
            assert_eq!(names.value(0), "David");   // (300 + 50) * 2 = 700 (highest)
            assert_eq!(final_values.value(0), 700);
            
            assert_eq!(names.value(1), "Eve");     // (250 + 50) * 2 = 600 (second)
            assert_eq!(final_values.value(1), 600);
            
            // Bob would be (200 + 50) * 2 = 500, but excluded because 250 is not > 250
            
            tx_guard.commit(None).await.unwrap();
        }
    }
}
