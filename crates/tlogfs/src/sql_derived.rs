//! SQL-derived dynamic node factory for TLogFS
//!
//! This factory enables creation of dynamic tables and series derived from SQL queries over existing pond data.

use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::sync::Arc;
use tinyfs::{DirHandle, FileHandle, Result as TinyFSResult, Directory, File, NodeRef, Metadata, NodeMetadata, EntryType, AsyncReadSeek};
use crate::register_dynamic_factory;
use crate::factory::FactoryContext;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use async_trait::async_trait;
use tokio::io::AsyncWrite;

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
    
    /// Execute the SQL query and return results as a DataFusion DataFrame
    async fn execute_query(&self) -> Result<DataFrame, DataFusionError> {
        // 1. Create DataFusion session context
        let ctx = SessionContext::new();
        
        // 2. Resolve source path to get the actual pond node
        // For now, this is stubbed - we need to implement path resolution
        // TODO: Use self.persistence to resolve self.config.source_path to actual data
        
        // 3. Register the source data as a table in DataFusion
        // TODO: Create table provider from pond node data
        
        // 4. Execute SQL query
        ctx.sql(&self.config.sql).await
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

// Implement TinyFS traits for SqlDerivedDirectory
#[async_trait]
impl Directory for SqlDerivedDirectory {
    async fn get(&self, _name: &str) -> TinyFSResult<Option<NodeRef>> {
        // SQL-derived directories are typically single-result containers
        // For now, return None to indicate empty directory
        Ok(None)
    }

    async fn insert(&mut self, _name: String, _node: NodeRef) -> TinyFSResult<()> {
        Err(tinyfs::Error::Other("SQL-derived directories are read-only".to_string()))
    }

    async fn entries(&self) -> TinyFSResult<std::pin::Pin<Box<dyn futures::Stream<Item = TinyFSResult<(String, NodeRef)>> + Send>>> {
        use futures::stream;
        
        // For now, return an empty stream
        // TODO: Execute SQL query and return results as directory entries
        let empty_stream = stream::empty();
        Ok(Box::pin(empty_stream))
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
fn validate_sql_derived_config(config: &[u8]) -> TinyFSResult<Value> {
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
