use anyhow::{Result, anyhow};
use arrow_csv::WriterBuilder;
use futures::StreamExt;
use std::io;
use std::sync::Arc;

use crate::common::{FilesystemChoice, ShipContext};
use diagnostics::*;

/// Execute SQL queries against pond metadata using oplog_entries, nodes view, and DirectoryTable
pub async fn query_command(
    ship_context: &ShipContext,
    filesystem: &FilesystemChoice,
    sql: &str,
    output_format: &str,
) -> Result<()> {
    debug!("query_command called with sql: {sql}, format: {output_format}");
    
    // For now, only support data filesystem - control filesystem access would require different API
    if *filesystem == FilesystemChoice::Control {
        return Err(anyhow!("Control filesystem access not yet implemented for query command"));
    }

    let mut ship = ship_context.open_pond().await?;
    
    // Use manual transaction pattern for DataFusion setup
    let mut tx = ship.begin_transaction(ship_context.original_args.clone()).await?;
    
    // Get data persistence to access the Delta table
    let persistence = tx.data_persistence()
        .map_err(|e| anyhow!("Failed to get data persistence: {}", e))?;
    
    // Get the Delta table from persistence
    let delta_table = persistence.table().clone();
    
    // Use transaction's SessionContext instead of creating new one (anti-duplication)
    let session_context = tx.session_context().await
        .map_err(|e| anyhow!("Failed to get session context from transaction: {}", e))?;
    
    // Register Delta table directly for full oplog entries (includes content column)
    session_context.register_table("oplog_entries", Arc::new(delta_table.clone()))
        .map_err(|e| anyhow!("Failed to register oplog_entries table: {}", e))?;
    
    // Create a SQL view for nodes that excludes the content column (for performance)
    let create_nodes_view = "
        CREATE VIEW nodes AS
        SELECT 
            part_id, node_id, file_type, timestamp, version,
            sha256, size, min_event_time, max_event_time, min_override, max_override
        FROM oplog_entries
    ";
    session_context.sql(create_nodes_view).await
        .map_err(|e| anyhow!("Failed to create nodes view: {}", e))?
        .collect().await
        .map_err(|e| anyhow!("Failed to execute CREATE VIEW for nodes: {}", e))?;
    
    // Register DirectoryTable for IPC-parsed directory content queries
    let directory_table = Arc::new(tlogfs::query::DirectoryTable::new(delta_table.clone()));
    session_context.register_table("directory_entries", directory_table.clone())
        .map_err(|e| anyhow!("Failed to register directory_entries table: {}", e))?;
    
    // Also register shorter aliases for convenience
    session_context.register_table("oplog", Arc::new(delta_table.clone()))
        .map_err(|e| anyhow!("Failed to register oplog table alias: {}", e))?;
    
    session_context.register_table("d", directory_table.clone())
        .map_err(|e| anyhow!("Failed to register directory_entries table alias: {}", e))?;
    
    info!("Executing SQL query: {sql}");
    
    // Execute the SQL query
    let df = session_context.sql(sql).await
        .map_err(|e| anyhow!("Failed to parse SQL query: {}", e))?;
    
    let stream = df.execute_stream().await
        .map_err(|e| anyhow!("Failed to execute query: {}", e))?;
    
    // Process results based on output format
    match output_format {
        "table" => {
            // Use DataFusion's pretty print for table format
            let batches: Vec<_> = stream.collect().await;
            let batches: Result<Vec<_>, _> = batches.into_iter().collect();
            let batches = batches.map_err(|e| anyhow!("Error collecting results: {}", e))?;
            
            if batches.is_empty() {
                println!("No results found.");
                return Ok(());
            }
            
            // Use DataFusion's built-in pretty formatting
            let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| anyhow!("Failed to format results as table: {}", e))?;
            println!("{}", formatted);
        }
        "csv" => {
            // Use Arrow CSV writer for CSV format
            let stdout = io::stdout();
            let mut stdout_lock = stdout.lock();
            
            let mut csv_writer = WriterBuilder::new()
                .build(&mut stdout_lock);
            
            let mut stream = stream;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result.map_err(|e| anyhow!("Error in query stream: {}", e))?;
                csv_writer.write(&batch)
                    .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
            }
        }
        "json" => {
            // JSON output - simplified approach without arrow_json dependency
            println!("{{\"note\": \"JSON output not implemented yet - use CSV or table format\"}}");
            warn!("JSON output requires arrow_json dependency which is not available");
        }
        "count" => {
            // Just count the rows
            let mut total_rows = 0;
            let mut stream = stream;
            
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result.map_err(|e| anyhow!("Error in query stream: {}", e))?;
                total_rows += batch.num_rows();
            }
            
            println!("{}", total_rows);
        }
        _ => {
            return Err(anyhow!("Unsupported output format: {}. Use 'table', 'csv', 'json', or 'count'.", output_format));
        }
    }
    
    Ok(())
}

/// Execute a predefined SQL query that shows pond system state summary
pub async fn query_show_command(
    ship_context: &ShipContext,
    filesystem: &FilesystemChoice,
) -> Result<()> {
    info!("Showing pond system state using SQL queries");
    
    // Predefined queries that show useful system state information
    let queries = vec![
        ("File Type Summary", 
         "SELECT file_type, COUNT(*) as count, 
                 MIN(timestamp) as first_created,
                 MAX(timestamp) as last_modified
          FROM nodes 
          GROUP BY file_type 
          ORDER BY count DESC"),
        
        ("Temporal Series Summary",
         "SELECT node_id, 
                 min_event_time, 
                 max_event_time,
                 (max_event_time - min_event_time) as time_span_ms
          FROM nodes 
          WHERE file_type = 'file:series' 
            AND min_event_time IS NOT NULL 
            AND max_event_time IS NOT NULL
          ORDER BY time_span_ms DESC 
          LIMIT 10"),
        
        ("Directory Entry Summary", 
         "SELECT operation_type, node_type, COUNT(*) as count
          FROM directory_entries 
          GROUP BY operation_type, node_type 
          ORDER BY count DESC"),
        
        ("Large Files Summary",
         "SELECT node_id, size, file_type
          FROM nodes 
          WHERE size IS NOT NULL 
            AND size > 1000000
          ORDER BY size DESC 
          LIMIT 10"),
    ];
    
    for (title, sql) in queries {
        println!("\n=== {} ===", title);
        match query_command(ship_context, &filesystem, sql, "table").await {
            Ok(()) => {
                // Query executed successfully
            }
            Err(e) => {
                warn!("Query '{title}' failed: {e}");
                println!("(Query failed: {})", e);
            }
        }
    }
    
    println!("\n=== Available Tables ===");
    println!("- 'nodes' (alias 'n'): OplogEntry metadata without content");
    println!("  Columns: part_id, node_id, file_type, version, timestamp, min_event_time, max_event_time, sha256, size");
    println!("- 'directory_entries' (alias 'd'): Directory content with file names");
    println!("  Columns: name, child_node_id, operation_type, node_type");
    println!("\nUse 'pond query --sql \"YOUR_SQL_HERE\"' for custom queries.");
    
    Ok(())
}
