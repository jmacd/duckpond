//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::sql::TableReference;
use crate::error::TLogFSError;
use crate::file_table::create_table_provider_from_path;

/// Execute a SQL query against a TLogFS file
/// 
/// This is the main interface for executing SQL queries against any TLogFS file type
/// (FileTable, FileSeries, SqlDerivedFile, etc.). The file is automatically registered
/// as a table named "series" in the DataFusion context.
/// 
/// # Arguments
/// * `tinyfs_wd` - TinyFS working directory for file resolution
/// * `path` - Path to the TLogFS file
/// * `sql_query` - SQL query to execute (the file will be available as "series" table)
/// 
/// # Returns
/// Vector of RecordBatch results from the query execution
/// 
/// # Example
/// ```rust,no_run
/// use tlogfs::query::execute_sql_on_file;
/// 
/// # async fn example(tinyfs_wd: &tinyfs::WD) -> Result<(), tlogfs::TLogFSError> {
/// // Execute a simple query
/// let batches = execute_sql_on_file(
///     &tinyfs_wd, 
///     "/my/file.series",
///     "SELECT * FROM series"
/// ).await?;
/// 
/// // Execute an aggregation query
/// let count_batches = execute_sql_on_file(
///     &tinyfs_wd,
///     "/my/file.series", 
///     "SELECT count(*) FROM series"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn execute_sql_on_file(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    sql_query: &str,
) -> Result<Vec<RecordBatch>, TLogFSError> {
    // Create a DataFusion context
    let ctx = SessionContext::new();
    
    // Create a table provider from the file path
    let table_provider = create_table_provider_from_path(tinyfs_wd, path).await?;
    
    // Register the table with the name "series"
    ctx.register_table(TableReference::bare("series"), table_provider)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to register table 'series': {}", e)))?;
    
    // Execute the SQL query
    let df = ctx.sql(sql_query).await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to execute SQL query '{}': {}", sql_query, e)))?;
    
    // Collect and return the results
    let batches = df.collect().await
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to collect query results: {}", e)))?;
    
    Ok(batches)
}

/// Format RecordBatch results as a pretty-printed string with row summary
/// 
/// This is a convenience function for displaying query results in a human-readable format.
/// Includes a "Summary: X total rows" line at the end for compatibility with existing tests.
/// 
/// # Arguments
/// * `batches` - Vector of RecordBatch results to format
/// 
/// # Returns
/// String containing the formatted table output with row summary
pub fn format_query_results(batches: &[RecordBatch]) -> Result<String, TLogFSError> {
    use arrow::util::pretty::pretty_format_batches;
    
    if batches.is_empty() {
        return Ok("No data found\n".to_string());
    }
    
    let formatted = pretty_format_batches(batches)
        .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to format query results: {}", e)))?
        .to_string();
    
    // Calculate total row count across all batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    
    // Append row summary for compatibility with existing tests
    let result = if total_rows > 0 {
        format!("{}\nSummary: {} total rows", formatted, total_rows)
    } else {
        "No data found\n".to_string()
    };
    
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Tests would go here - for now just compilation tests
    #[test]
    fn test_module_compiles() {
        // This ensures the module compiles correctly
        assert!(true);
    }
}
