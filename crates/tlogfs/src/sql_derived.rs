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
    pub source: String,
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
        
        // Resolve the source path
        let source_path = &self.config.source;
        let node_path = tinyfs_root.get_node_path(source_path).await
            .map_err(|e| tinyfs::Error::Other(format!("Source path '{}' not found: {}", source_path, e)))?;
        
        // Get node reference and determine type
        let node_ref = node_path.borrow().await;
        let file_node = node_ref.as_file()
            .map_err(|e| tinyfs::Error::Other(format!("Source is not a file: {e}")))?;
        let node_metadata = file_node.metadata().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get metadata: {e}")))?;
        
        // Create appropriate TableProvider based on source type
        let table_provider: Arc<dyn TableProvider> = match node_metadata.entry_type {
            EntryType::FileSeries => {
                println!("Creating SeriesTable for FileSeries source: {source_path}");
                
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
            EntryType::FileTable => {
                println!("Creating MemTable for FileTable source: {source_path}");
                
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
    
    // Validate required fields
    if yaml_config.source.is_empty() {
        return Err(tinyfs::Error::Other("Source path cannot be empty".to_string()));
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

    #[tokio::test]
    async fn test_sql_derived_config_validation() {
        let valid_config = r#"
source: "/test/data.parquet"
query: "SELECT * FROM source WHERE value > 10"
"#;
        
        let result = validate_sql_derived_config(valid_config.as_bytes());
        assert!(result.is_ok());
        
        // Test empty source
        let invalid_config = r#"
source: ""
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
    async fn test_sql_derived_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut persistence = OpLogPersistence::create(temp_dir.path().to_str().unwrap()).await.unwrap();
        
        // First transaction: Create the source data file
        {
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
            
            // Create the source data file using TinyFS convenience API - use root path to avoid directory issues
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
        
        // Second transaction: Create and test the SQL-derived file  
        {
            let tx_guard = persistence.begin().await.unwrap();
            let state = tx_guard.state().unwrap();
            
            // Create the SQL-derived file with read-only state context
            let context = FactoryContext::new(state);
            let config = SqlDerivedConfig {
                source: "/data.parquet".to_string(),
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
    }
}
