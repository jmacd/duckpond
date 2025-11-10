//! SQL execution interface for TLogFS files
//!
//! This module provides a simple interface to execute SQL queries against TLogFS files
//! without requiring the caller to understand the underlying DataFusion setup.

use crate::error::TLogFSError;
use crate::sql_derived::try_as_queryable_file;
use crate::transaction_guard::TransactionGuard;
use datafusion::physical_plan::SendableRecordBatchStream; // Import the canonical version

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
    let (_, lookup_result) = tinyfs_wd
        .resolve_path(path)
        .await
        .map_err(TLogFSError::TinyFS)?;

    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;

            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;

            match metadata.entry_type {
                tinyfs::EntryType::FileTablePhysical
                | tinyfs::EntryType::FileTableDynamic
                | tinyfs::EntryType::FileSeriesPhysical
                | tinyfs::EntryType::FileSeriesDynamic => {
                    // Use trait dispatch instead of type checking - follows anti-duplication principles

                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    // Simple and direct: get NodeIDs without unnecessary conversions
                    let node_id = node_path.id().await;
                    let part_id = {
                        let parent_path = node_path.dirname();
                        let parent_node_path =
                            tinyfs_wd.resolve_path(&parent_path).await.map_err(|e| {
                                TLogFSError::ArrowMessage(format!(
                                    "Failed to resolve parent path: {}",
                                    e
                                ))
                            })?;
                        match parent_node_path.1 {
                            Lookup::Found(parent_node) => parent_node.id().await,
                            _ => tinyfs::NodeID::root(),
                        }
                    };

                    // Single workflow: Use QueryableFile trait dispatch instead of type checking
                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        let state = tx.state()?;
                        let table_provider = queryable_file
                            .as_table_provider(node_id, part_id, &state)
                            .await?;
                        drop(file_guard);

                        ctx.register_table(
                            datafusion::sql::TableReference::bare("series"),
                            table_provider,
                        )
                        .map_err(|e| {
                            TLogFSError::ArrowMessage(format!(
                                "Failed to register table 'series': {}",
                                e
                            ))
                        })?;
                    } else {
                        return Err(TLogFSError::ArrowMessage(
                            "File does not implement QueryableFile trait".to_string(),
                        ));
                    }

                    // Unified SQL execution - works for both file types now!
                    let df = ctx.sql(sql_query).await.map_err(|e| {
                        TLogFSError::ArrowMessage(format!(
                            "Failed to execute SQL query '{}': {}",
                            sql_query, e
                        ))
                    })?;

                    let stream = df.execute_stream().await.map_err(|e| {
                        TLogFSError::ArrowMessage(format!("Failed to create result stream: {}", e))
                    })?;

                    Ok(stream)
                }
                _ => Err(TLogFSError::ArrowMessage(format!(
                    "Path {} points to unsupported entry type for table operations: {:?}",
                    path, metadata.entry_type
                ))),
            }
        }
        Lookup::NotFound(full_path, _) => Err(TLogFSError::ArrowMessage(format!(
            "File not found: {}",
            full_path.display()
        ))),
        Lookup::Empty(_) => Err(TLogFSError::ArrowMessage("Empty path provided".to_string())),
    }
}

/// Get the schema for a TLogFS file without executing any queries
///
/// # Arguments
/// * `tinyfs_wd` - Working directory handle for path resolution
/// * `path` - Path to the TLogFS file
/// * `tx` - Transaction guard for consistent access
///
/// # Returns
/// The Arrow schema for the file
pub async fn get_file_schema(
    tinyfs_wd: &tinyfs::WD,
    path: &str,
    state: &crate::persistence::State,
) -> Result<arrow::datatypes::SchemaRef, TLogFSError> {
    // Resolve path - same pattern as execute_sql_on_file
    use tinyfs::Lookup;
    let (_, lookup_result) = tinyfs_wd
        .resolve_path(path)
        .await
        .map_err(TLogFSError::TinyFS)?;

    match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                TLogFSError::ArrowMessage(format!("Path {} does not point to a file: {}", path, e))
            })?;

            // Get the entry type and metadata
            let metadata = file_handle.metadata().await.map_err(TLogFSError::TinyFS)?;

            match metadata.entry_type {
                tinyfs::EntryType::FileTablePhysical
                | tinyfs::EntryType::FileTableDynamic
                | tinyfs::EntryType::FileSeriesPhysical
                | tinyfs::EntryType::FileSeriesDynamic => {
                    let file_arc = file_handle.handle.get_file().await;
                    let file_guard = file_arc.lock().await;

                    let node_id = node_path.id().await;
                    let part_id = {
                        let parent_path = node_path.dirname();
                        let parent_node_path =
                            tinyfs_wd.resolve_path(&parent_path).await.map_err(|e| {
                                TLogFSError::ArrowMessage(format!(
                                    "Failed to resolve parent path: {}",
                                    e
                                ))
                            })?;
                        match parent_node_path.1 {
                            Lookup::Found(parent_node) => parent_node.id().await,
                            _ => tinyfs::NodeID::root(),
                        }
                    };

                    // Get QueryableFile and table provider
                    if let Some(queryable_file) = try_as_queryable_file(&**file_guard) {
                        let table_provider = queryable_file
                            .as_table_provider(node_id, part_id, state)
                            .await?;
                        drop(file_guard);

                        // Get schema directly from table provider
                        Ok(table_provider.schema())
                    } else {
                        Err(TLogFSError::ArrowMessage(
                            "File does not implement QueryableFile trait".to_string(),
                        ))
                    }
                }
                _ => Err(TLogFSError::ArrowMessage(format!(
                    "Unsupported file type for schema extraction: {:?}",
                    metadata.entry_type
                ))),
            }
        }
        Lookup::NotFound(full_path, _) => Err(TLogFSError::ArrowMessage(format!(
            "File not found: {}",
            full_path.display()
        ))),
        Lookup::Empty(_) => Err(TLogFSError::ArrowMessage("Empty path provided".to_string())),
    }
}
