//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::TableReference;
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
    
    let (node_id, part_id) = match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    // Extract node_id and part_id from the file_handle
                    use crate::file::OpLogFile;
                    let file_arc = file_handle.handle.get_file().await;
                    let (node_id, part_id) = {
                        let file_guard = file_arc.lock().await;
                        let file_any = file_guard.as_any();
                        let oplog_file = file_any.downcast_ref::<OpLogFile>()
                            .ok_or_else(|| TLogFSError::ArrowMessage("FileHandle is not an OpLogFile".to_string()))?;
                        (oplog_file.get_node_id(), oplog_file.get_part_id())
                    };
                    (node_id, part_id)
                },
                _ => {
                    return Err(TLogFSError::ArrowMessage(
                        format!("Path {} points to unsupported entry type for table operations: {:?}", path, metadata.entry_type)
                    ))
                }
            }
        },
        Lookup::NotFound(full_path, _) => {
            return Err(TLogFSError::ArrowMessage(format!("File not found: {}", full_path.display())));
        },
        Lookup::Empty(_) => {
            return Err(TLogFSError::ArrowMessage("Empty path provided".to_string()));
        }
    };
    
    // Create table provider directly with resolved node_id and part_id
    let table_provider = crate::file_table::create_listing_table_provider(node_id, part_id, tx).await?;
    
    // Register the table with the name "series"
    ctx.register_table(TableReference::bare("series"), table_provider)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table 'series': {}", e)))?;
    
    // Execute the SQL query
    let df = ctx.sql(sql_query).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute SQL query '{}': {}", sql_query, e)))?;
    
    // Return the streaming results
    let stream = df.execute_stream().await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create result stream: {}", e)))?;
    
    Ok(stream)
}


