use anyhow::{Result, anyhow};
use arrow_csv::WriterBuilder;
use futures::StreamExt;
use std::io;

use crate::common::ShipContext;
use log::debug;

/// Execute SQL queries against pond metadata using oplog_entries, nodes view, and DirectoryTable
pub async fn query_command(
    ship_context: &ShipContext,
    sql: &str,
    output_format: &str,
) -> Result<()> {
    debug!("query_command called with sql: {sql}, format: {output_format}");

    let mut ship = ship_context.open_pond().await?;

    // Use manual transaction pattern for DataFusion setup
    let mut tx = ship
        .begin_transaction(steward::TransactionOptions::read(
            ship_context.original_args.clone(),
        ))
        .await?;

    // Get data persistence to access the Delta table
    let _persistence = tx
        .data_persistence()
        .map_err(|e| anyhow!("Failed to get data persistence: {}", e))?;

    // Get the Delta table from persistence
    // NOTE: Use pre-registered fundamental tables instead of registering duplicates
    // delta_table is already registered in State constructor

    // Use transaction's SessionContext instead of creating new one (anti-duplication)
    let session_context = tx
        .session_context()
        .await
        .map_err(|e| anyhow!("Failed to get session context from transaction: {}", e))?;

    // NOTE: fundamental tables like 'delta_table' are pre-registered in State constructor
    // Following anti-duplication: no duplicate table registration needed

    // Create a SQL view for nodes that excludes the content column (for performance)
    let create_nodes_view = "
        CREATE VIEW nodes AS
        SELECT 
            part_id, node_id, file_type, timestamp, version,
            sha256, size, min_event_time, max_event_time, min_override, max_override
        FROM delta_table
        WHERE file_type != 'Directory'
    ";
    session_context
        .sql(create_nodes_view)
        .await
        .map_err(|e| anyhow!("Failed to create nodes view: {}", e))?
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to execute CREATE VIEW for nodes: {}", e))?;

    // Register shorter alias for convenience - use CREATE VIEW instead of duplicate table registration
    session_context
        .sql("CREATE VIEW oplog AS SELECT * FROM delta_table")
        .await
        .map_err(|e| anyhow!("Failed to create oplog view: {}", e))?
        .collect()
        .await
        .map_err(|e| anyhow!("Failed to execute CREATE VIEW for oplog: {}", e))?;

    debug!("Executing SQL query: {sql}");

    // Execute the SQL query
    let df = session_context
        .sql(sql)
        .await
        .map_err(|e| anyhow!("Failed to parse SQL query: {}", e))?;

    let stream = df
        .execute_stream()
        .await
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

            let mut csv_writer = WriterBuilder::new().build(&mut stdout_lock);

            let mut stream = stream;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result.map_err(|e| anyhow!("Error in query stream: {}", e))?;
                csv_writer
                    .write(&batch)
                    .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
            }
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
            return Err(anyhow!(
                "Unsupported output format: {}. Use 'table', 'csv', 'json', or 'count'.",
                output_format
            ));
        }
    }

    Ok(())
}
