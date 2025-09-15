//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use datafusion::physical_plan::SendableRecordBatchStream;
use crate::error::TLogFSError;
use crate::transaction_guard::TransactionGuard;

/// Execute a SQL query against a TLogFS file and return a streaming result
/// 
/// This is the main interface for executing SQL queries against any TLogFS file type
/// (FileTable, FileSeries, SqlDerivedFile, etc.). The file is automatically registered
/// as a table named "series" in the DataFusion context.
/// 
/// Returns a stream of RecordBatch results for efficient processing of large datasets.
/// 
/// # Arguments
/// * `tinyfs_wd` - TinyFS working directory for file resolution
/// * `path` - Path to the TLogFS file
/// * `sql_query` - SQL query to execute (the file will be available as "series" table)
/// 
/// # Returns
/// A stream of RecordBatch results from the query execution
pub async fn execute_sql_on_file<'a>(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    sql_query: &str,
    tx: &mut TransactionGuard<'a>,
) -> Result<SendableRecordBatchStream, TLogFSError> {
    // Get SessionContext from transaction (anti-duplication)
    let ctx = tx.session_context().await?;
    
    // Resolve path to get node_id and part_id directly (anti-duplication - no wrapper function)
    use tinyfs::Lookup;
    let (_, lookup_result) = tinyfs_wd.resolve_path(path).await.map_err(TLogFSError::TinyFS)?;
    
    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    // Use trait dispatch instead of type checking - follows anti-duplication principles
                    use crate::query::QueryableFile;
                    
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;
                    let file_any = file_guard.as_any();
                    
                    // Check for QueryableFile implementations - OpLogFile and SqlDerivedFile both implement it
                    use crate::file::OpLogFile;
                    use crate::sql_derived::SqlDerivedFile;
                    
                    // Unified workflow: Both SqlDerivedFile and OpLogFile use QueryableFile trait
                    // The key insight: SqlDerivedFile's table provider now handles lazy resolution internally
                    
                    let node_id = tinyfs::NodeID::new(node_path.id().await.to_hex_string());
                    let part_id = tinyfs::NodeID::new({
                        let parent_path = node_path.dirname();
                        let parent_node_path = tinyfs_wd.resolve_path(&parent_path).await
                            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to resolve parent path: {}", e)))?;
                        match parent_node_path.1 {
                            tinyfs::Lookup::Found(parent_node) => parent_node.id().await.to_hex_string(),
                            _ => tinyfs::NodeID::root().to_hex_string(),
                        }
                    });
                    
                    // Try SqlDerivedFile first
                    if let Some(sql_derived_file) = file_any.downcast_ref::<SqlDerivedFile>() {
                        let table_provider = sql_derived_file.as_table_provider(node_id, part_id, tx).await?;
                        drop(file_guard);
                        
                        ctx.register_table(datafusion::sql::TableReference::bare("series"), table_provider)
                            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table 'series': {}", e)))?;
                    }
                    // Try OpLogFile second
                    else if let Some(oplog_file) = file_any.downcast_ref::<OpLogFile>() {
                        let table_provider = oplog_file.as_table_provider(node_id, part_id, tx).await?;
                        drop(file_guard);
                        
                        ctx.register_table(datafusion::sql::TableReference::bare("series"), table_provider)
                            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table 'series': {}", e)))?;
                    }
                    else {
                        return Err(TLogFSError::ArrowMessage("File does not implement QueryableFile trait".to_string()));
                    }
                    
                    // Unified SQL execution - works for both file types now!
                    let df = ctx.sql(sql_query).await
                        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute SQL query '{}': {}", sql_query, e)))?;
                    
                    let stream = df.execute_stream().await
                        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create result stream: {}", e)))?;
                    
                    return Ok(stream);
                },
                _ => {
                    return Err(TLogFSError::ArrowMessage(
                        format!("Path {} points to unsupported entry type for table operations: {:?}", path, metadata.entry_type)
                    ));
                }
            }
        },
        Lookup::NotFound(full_path, _) => {
            return Err(TLogFSError::ArrowMessage(format!("File not found: {}", full_path.display())));
        },
        Lookup::Empty(_) => {
            return Err(TLogFSError::ArrowMessage("Empty path provided".to_string()));
        }
    }
}


