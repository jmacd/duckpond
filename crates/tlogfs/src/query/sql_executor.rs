//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::sql::TableReference;
use crate::error::TLogFSError;
use crate::file_table::create_table_provider_from_path;

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
/// 
/// # Example
/// ```rust,no_run
/// use tlogfs::query::execute_sql_on_file;
/// use futures::StreamExt;
/// 
/// # async fn example(tinyfs_wd: &tinyfs::WD) -> Result<(), tlogfs::TLogFSError> {
/// // Execute a streaming query
/// let mut stream = execute_sql_on_file(
///     &tinyfs_wd, 
///     "/my/file.series",
///     "SELECT * FROM series"
/// ).await?;
/// 
/// // Process results as they arrive
/// while let Some(batch_result) = stream.next().await {
///     let batch = batch_result?;
///     // Process each batch...
/// }
/// # Ok(())
/// # }
/// ```
pub async fn execute_sql_on_file(
    tinyfs_wd: &tinyfs::WD,
    persistence: &crate::OpLogPersistence,
    path: &str,
    sql_query: &str,
) -> Result<SendableRecordBatchStream, TLogFSError> {
    // Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Create a table provider from the file path
    let table_provider = create_table_provider_from_path(tinyfs_wd, persistence, path).await?;
    
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
