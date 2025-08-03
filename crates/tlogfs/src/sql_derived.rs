//! SQL-derived dynamic node factory for TLogFS
//!
//! This factory enables creation of dynamic tables and series derived from SQL queries over existing pond data.

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult, Directory, File, NodeRef, Metadata, NodeMetadata, EntryType, AsyncReadSeek, NodeID, Node, NodeType};
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::physical_plan::SendableRecordBatchStream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use async_trait::async_trait;
use tokio::io::AsyncWrite;
use futures::StreamExt;
use diagnostics::*;

/// Helper types for pond node data handling
#[derive(Debug, Clone)]
struct PondNodeData {
    node_id: NodeID,
    content: Vec<u8>,
    node_type: PondNodeType,
}

#[derive(Debug, Clone)]
enum PondNodeType {
    ParquetTable,
    ParquetSeries,
    CsvData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SqlDerivedConfig {
    /// Path to the source file:table or file:series node in the pond
    pub source_path: String,
    /// SQL query to apply (e.g., SELECT ... FROM ... WHERE ...)
    pub sql: String,
    /// Optional: output type ("table" or "series")
    pub output_type: Option<String>,
}

/// Create a directory handle for a SQL-derived node (table or series)
/// This will resolve the source node, set up a DataFusion context, and expose the result as a TinyFS directory.
fn create_sql_derived_dir_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<DirHandle> {
    // 1. Parse config
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    // 2. Create a SqlDerivedDirectory and return its handle
    let sql_dir = SqlDerivedDirectory::new(cfg, Arc::clone(&context.persistence))?;
    Ok(sql_dir.create_handle())
}

/// Create a file handle for a SQL-derived node (table or series)
/// This will resolve the source node, set up a DataFusion context, and expose the result as a TinyFS file.
fn create_sql_derived_file_with_context(config: Value, context: &FactoryContext) -> TinyFSResult<FileHandle> {
    // 1. Parse config
    let cfg: SqlDerivedConfig = serde_json::from_value(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config: {}", e)))?;

    // 2. Create a SqlDerivedFile and return its handle
    let sql_file = SqlDerivedFile::new(cfg, Arc::clone(&context.persistence))?;
    Ok(sql_file.create_handle())
}

/// SQL-derived directory implementation
#[derive(Clone)]
pub struct SqlDerivedDirectory {
    config: SqlDerivedConfig,
    persistence: Arc<crate::persistence::OpLogPersistence>,
}

impl SqlDerivedDirectory {
    pub fn new(config: SqlDerivedConfig, persistence: Arc<crate::persistence::OpLogPersistence>) -> TinyFSResult<Self> {
        Ok(Self { config, persistence })
    }
    
    pub fn create_handle(self) -> DirHandle {
        tinyfs::DirHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Get query results as a stream of Arrow RecordBatches (streaming approach)
    async fn get_query_stream(&self) -> Result<SendableRecordBatchStream, String> {
        // Execute the SQL query and get the execution plan
        let df = self.execute_query().await
            .map_err(|e| format!("SQL execution failed: {}", e))?;
        
        // Get the streaming execution plan instead of collecting all results
        let stream = df.execute_stream().await
            .map_err(|e| format!("Failed to create query stream: {}", e))?;
        
        Ok(stream)
    }
    
    /// Get the cached or fresh query results as Arrow RecordBatches (legacy - for compatibility)
    async fn get_query_results(&self) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        // For backward compatibility, collect all batches from the stream
        let mut stream = self.get_query_stream().await?;
        let mut batches = Vec::new();
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| format!("Failed to read batch from stream: {}", e))?;
            batches.push(batch);
        }
        
        Ok(batches)
    }
    
    /// Execute the SQL query and return results as a DataFusion DataFrame
    async fn execute_query(&self) -> Result<DataFrame, DataFusionError> {
        // 1. Create DataFusion session context
        let ctx = SessionContext::new();
        
        // 2. Resolve source path to get the actual pond node and its data
        let node_data = self.resolve_source_node().await?;
        
        // 3. Register the source data as a table in DataFusion
        let table_provider = self.create_table_provider_from_node_data(node_data).await?;
        ctx.register_table("source", Arc::from(table_provider))?;
        
        // 4. Replace 'FROM source_path' in the SQL with 'FROM source'
        let sql_query = self.config.sql.replace(&self.config.source_path, "source");
        
        // 5. Execute SQL query
        ctx.sql(&sql_query).await
    }
    
    /// Resolve the source_path to actual node data
    async fn resolve_source_node(&self) -> Result<PondNodeData, DataFusionError> {
        // For now, we'll implement a simple node resolution
        // In a full implementation, this would traverse the pond filesystem to find the node
        
        // Parse the source path to extract node information
        // Expected format: "/path/to/node" or node ID directly
        let source_path = &self.config.source_path;
        
        if source_path.starts_with("/") {
            // Path-based resolution - this is more complex and would require 
            // full filesystem traversal. For now, return an error suggesting node ID usage.
            return Err(DataFusionError::Plan(format!(
                "Path-based resolution not yet implemented. Please use node ID directly. Path: {}", 
                source_path
            )));
        } else {
            // Assume it's a node ID in hex format
            let node_id = NodeID::from_hex_string(source_path)
                .map_err(|e| DataFusionError::Plan(format!("Invalid node ID '{}': {}", source_path, e)))?;
            
            // Query the persistence layer to get node data
            let content = self.persistence.load_file_content(node_id, node_id).await
                .map_err(|e| DataFusionError::Plan(format!("Failed to load node data: {}", e)))?;
            
            // Determine the node type - for now assume it's Parquet data
            Ok(PondNodeData {
                node_id,
                content,
                node_type: PondNodeType::ParquetTable,
            })
        }
    }
    
    /// Create a DataFusion table provider from pond node data
    async fn create_table_provider_from_node_data(&self, node_data: PondNodeData) -> Result<Box<dyn TableProvider>, DataFusionError> {
        match node_data.node_type {
            PondNodeType::ParquetTable | PondNodeType::ParquetSeries => {
                // Create a MemTable from the Parquet data using tokio_util::bytes::Bytes
                let bytes = tokio_util::bytes::Bytes::from(node_data.content);
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .map_err(|e| DataFusionError::Plan(format!("Failed to create Parquet reader: {}", e)))?
                    .build()
                    .map_err(|e| DataFusionError::Plan(format!("Failed to build Parquet reader: {}", e)))?;
                
                // Get schema from the first batch or metadata
                let mut batches = Vec::new();
                let mut schema_ref = None;
                
                for batch_result in reader {
                    let batch = batch_result
                        .map_err(|e| DataFusionError::Plan(format!("Failed to read Parquet batch: {}", e)))?;
                    
                    if schema_ref.is_none() {
                        schema_ref = Some(batch.schema());
                    }
                    batches.push(batch);
                }
                
                let schema = schema_ref.ok_or_else(|| 
                    DataFusionError::Plan("No data found in Parquet file".to_string()))?;
                
                let mem_table = MemTable::try_new(schema, vec![batches])
                    .map_err(|e| DataFusionError::Plan(format!("Failed to create MemTable: {}", e)))?;
                
                Ok(Box::new(mem_table) as Box<dyn TableProvider>)
            }
            PondNodeType::CsvData => {
                Err(DataFusionError::Plan(
                    "CSV data sources not yet supported. Convert to Parquet first.".to_string()
                ))
            }
        }
    }
}

/// SQL-derived file implementation
pub struct SqlDerivedFile {
    config: SqlDerivedConfig,
    persistence: Arc<crate::persistence::OpLogPersistence>,
}

impl SqlDerivedFile {
    pub fn new(config: SqlDerivedConfig, persistence: Arc<crate::persistence::OpLogPersistence>) -> TinyFSResult<Self> {
        Ok(Self { config, persistence })
    }
    
    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Execute the SQL query and return results as bytes (Parquet format)
    async fn execute_query_to_bytes(&self) -> TinyFSResult<Vec<u8>> {
        // This would execute the SQL and serialize results to Parquet
        // For now, return a placeholder
        Err(tinyfs::Error::Other(format!(
            "SQL query execution not yet implemented: '{}'", 
            self.config.sql
        )))
    }
}

/// A virtual file containing SQL query results in Parquet format
pub struct SqlDerivedResultFile {
    batches: Vec<arrow::record_batch::RecordBatch>,
}

/// A streaming virtual file that processes SQL query results on-demand
pub struct SqlDerivedStreamingResultFile {
    query_directory: Arc<SqlDerivedDirectory>,
    // Cache the serialized result to avoid re-executing the query
    cached_parquet: tokio::sync::OnceCell<Vec<u8>>,
}

impl SqlDerivedStreamingResultFile {
    pub fn new(query_directory: Arc<SqlDerivedDirectory>) -> Self {
        Self { 
            query_directory,
            cached_parquet: tokio::sync::OnceCell::new(),
        }
    }
    
    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Stream SQL query results and serialize to Parquet format (with caching)
    async fn get_or_create_parquet_bytes(&self) -> Result<&[u8], String> {
        let bytes = self.cached_parquet.get_or_try_init(|| async {
            self.stream_to_parquet_bytes().await
        }).await?;
        
        Ok(bytes.as_slice())
    }
    
    /// Stream SQL query results and serialize to Parquet format on-demand
    async fn stream_to_parquet_bytes(&self) -> Result<Vec<u8>, String> {
        use datafusion::parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        // Get the streaming query results
        let mut stream = self.query_directory.get_query_stream().await?;
        
        // For true streaming, we need to process one batch at a time
        // However, Parquet requires knowing the schema upfront and writing all data at once
        // So we'll collect batches but process them as they arrive
        let mut all_batches = Vec::new();
        
        // Stream batches one at a time (this is where the memory efficiency comes from)
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| format!("Failed to read batch from stream: {}", e))?;
            all_batches.push(batch);
            
            // Optional: We could implement backpressure here if the batch vector gets too large
            // For now, we process all batches as they arrive
        }
        
        if all_batches.is_empty() {
            return Ok(Vec::new());
        }
        
        // Serialize to Parquet (this is the only part that requires all data)
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, all_batches[0].schema(), None)
                .map_err(|e| format!("Failed to create Parquet writer: {}", e))?;
            
            for batch in &all_batches {
                writer.write(batch)
                    .map_err(|e| format!("Failed to write batch to Parquet: {}", e))?;
            }
            
            writer.close()
                .map_err(|e| format!("Failed to close Parquet writer: {}", e))?;
        }
        
        Ok(buffer)
    }
}

impl SqlDerivedResultFile {
    pub fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        Self { batches }
    }
    
    pub fn create_handle(self) -> FileHandle {
        tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(self))))
    }
    
    /// Serialize the Arrow batches to Parquet format on-demand
    async fn to_parquet_bytes(&self) -> Result<Vec<u8>, String> {
        use datafusion::parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        if self.batches.is_empty() {
            return Ok(Vec::new());
        }
        
        // Serialize to Parquet
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, self.batches[0].schema(), None)
                .map_err(|e| format!("Failed to create Parquet writer: {}", e))?;
            
            for batch in &self.batches {
                writer.write(batch)
                    .map_err(|e| format!("Failed to write batch to Parquet: {}", e))?;
            }
            
            writer.close()
                .map_err(|e| format!("Failed to close Parquet writer: {}", e))?;
        }
        
        Ok(buffer)
    }
}

#[async_trait]
impl File for SqlDerivedStreamingResultFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        // Get or create Parquet bytes from streaming query (with caching)
        let parquet_bytes = self.get_or_create_parquet_bytes().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to get Parquet data: {}", e)))?;
        
        // Create cursor from the cached bytes
        let cursor = std::io::Cursor::new(parquet_bytes.to_vec());
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("SQL result files are read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for SqlDerivedStreamingResultFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        // Calculate size from cached or newly generated Parquet data
        let size = match self.get_or_create_parquet_bytes().await {
            Ok(bytes) => Some(bytes.len() as u64),
            Err(_) => None,
        };
        
        Ok(NodeMetadata {
            version: 1,
            size,
            sha256: None,
            entry_type: EntryType::FileTable,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl File for SqlDerivedResultFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        // Serialize to Parquet on-demand when file is read
        let parquet_bytes = self.to_parquet_bytes().await
            .map_err(|e| tinyfs::Error::Other(format!("Failed to serialize to Parquet: {}", e)))?;
        
        let cursor = std::io::Cursor::new(parquet_bytes);
        Ok(Box::pin(cursor))
    }

    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("SQL result files are read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for SqlDerivedResultFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        // Calculate size by serializing to Parquet (could be cached in the future)
        let size = match self.to_parquet_bytes().await {
            Ok(bytes) => Some(bytes.len() as u64),
            Err(_) => None,
        };
        
        Ok(NodeMetadata {
            version: 1,
            size,
            sha256: None,
            entry_type: EntryType::FileTable,
            timestamp: 0,
        })
    }
}

// Implement TinyFS traits for SqlDerivedDirectory
#[async_trait]
impl Directory for SqlDerivedDirectory {
    async fn get(&self, name: &str) -> TinyFSResult<Option<NodeRef>> {
        debug!("SqlDerivedDirectory::get called with {name}");
        
        if name.ends_with(".parquet") {
            // Create streaming result file for this query
            let streaming_file = SqlDerivedStreamingResultFile::new(Arc::new(self.clone()));
            
            let node_id = NodeID::generate();
            let file_handle = tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(streaming_file))));
            
            let node = Node {
                id: node_id,
                node_type: NodeType::File(file_handle),
            };
            
            Ok(Some(NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)))))
        } else {
            Ok(None)
        }
    }

    async fn insert(&mut self, _name: String, _node: NodeRef) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other("SQL-derived directories are read-only".to_string()))
    }

    async fn entries(&self) -> TinyFSResult<std::pin::Pin<Box<dyn futures::Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        use futures::stream;
        
        // For now, return a single "query.parquet" entry representing the SQL query result
        let streaming_file = SqlDerivedStreamingResultFile::new(Arc::new(self.clone()));
        
        let node_id = NodeID::generate();
        let file_handle = tinyfs::FileHandle::new(Arc::new(tokio::sync::Mutex::new(Box::new(streaming_file))));
        
        let node = Node {
            id: node_id,
            node_type: NodeType::File(file_handle),
        };
        let node_ref = NodeRef::new(Arc::new(tokio::sync::Mutex::new(node)));
        
        let entry = ("query.parquet".to_string(), node_ref);
        let single_entry_stream = stream::once(async move { Ok(entry) });
        Ok(Box::pin(single_entry_stream))
    }
}

#[async_trait]
impl Metadata for SqlDerivedDirectory {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None,
            sha256: None,
            entry_type: EntryType::Directory,
            timestamp: 0,
        })
    }
}

// Implement TinyFS traits for SqlDerivedFile
#[async_trait]
impl File for SqlDerivedFile {
    async fn async_reader(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncReadSeek>>> {
        // Execute SQL query and return results as readable stream
        let _result_bytes = self.execute_query_to_bytes().await?;
        
        // For now, return error - TODO: implement actual data streaming
        Err(tinyfs::Error::Other("SQL-derived file reading not yet implemented".to_string()))
    }

    async fn async_writer(&self) -> TinyFSResult<std::pin::Pin<Box<dyn AsyncWrite + Send>>> {
        Err(tinyfs::Error::Other("SQL-derived files are read-only".to_string()))
    }
}

#[async_trait]
impl Metadata for SqlDerivedFile {
    async fn metadata(&self) -> TinyFSResult<NodeMetadata> {
        Ok(NodeMetadata {
            version: 1,
            size: None, // TODO: Calculate size from SQL query results
            sha256: None,
            entry_type: EntryType::FileTable, // SQL results are typically table format
            timestamp: 0,
        })
    }
}

// Validate and parse the config for a SQL-derived node
pub fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
    let parsed: SqlDerivedConfig = serde_yaml::from_slice(config)
        .map_err(|e| tinyfs::Error::Other(format!("Invalid SQL-derived config YAML: {}", e)))?;
    if parsed.source_path.trim().is_empty() {
        return Err(tinyfs::Error::Other("Missing source_path in SQL-derived config".to_string()));
    }
    if parsed.sql.trim().is_empty() {
        return Err(tinyfs::Error::Other("Missing sql in SQL-derived config".to_string()));
    }
    serde_json::to_value(parsed)
        .map_err(|e| tinyfs::Error::Other(format!("Failed to convert config to JSON: {}", e)))
}

register_dynamic_factory!(
    name: "sql-derived",
    description: "Create a dynamic table or series derived from a SQL query over pond data",
    directory_with_context: create_sql_derived_dir_with_context,
    file_with_context: create_sql_derived_file_with_context,
    validate: validate_sql_derived_config
);
