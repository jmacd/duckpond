/// Updated cat.rs showing DRY implementation using unified providers
/// This eliminates the duplication between FileTable and FileSeries handling

// In display_file_with_sql_and_node_id function:

async fn display_file_with_sql_and_node_id(ship: &steward::Ship, path: &str, node_id: &str, time_start: Option<i64>, time_end: Option<i64>, sql_query: Option<&str>) -> Result<()> {
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    
    // Create DataFusion session context
    let ctx = SessionContext::new();
    
    // Get TinyFS root for file access
    let tinyfs_root = ship.data_fs().root().await?;
    
    // Create MetadataTable for metadata queries
    let data_path = ship.data_path();
    let delta_manager = tlogfs::DeltaTableManager::new();
    let metadata_table = tlogfs::query::MetadataTable::new(data_path.clone(), delta_manager);
    
    // Determine the entry type to choose the right provider
    let metadata_result = tinyfs_root.metadata_for_path(path).await;
    let is_series = match &metadata_result {
        Ok(metadata) => metadata.entry_type == tinyfs::EntryType::FileSeries,
        Err(_) => true, // Default to series for backward compatibility
    };
    
    // Create appropriate provider using unified architecture - NO MORE DUPLICATION!
    let mut table_provider = if is_series {
        tlogfs::query::providers::compat::create_series_table_with_tinyfs_and_node_id(
            path.to_string(),
            node_id.to_string(),
            metadata_table,
            Arc::new(tinyfs_root)
        )
    } else {
        tlogfs::query::providers::compat::create_table_table_with_tinyfs_and_node_id(
            path.to_string(),
            node_id.to_string(),
            metadata_table,
            Arc::new(tinyfs_root)
        )
    };
    
    // Load schema - UNIFIED LOGIC
    table_provider.load_schema_from_data().await
        .map_err(|e| anyhow::anyhow!("Failed to load schema: {}", e))?;
    
    // Register table - UNIFIED LOGIC
    ctx.register_table(TableReference::bare("series"), Arc::new(table_provider))
        .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))?;
    
    // Rest of the function remains the same...
    // Build SQL query with time filtering
    let base_query = sql_query.unwrap_or("SELECT * FROM series");
    let final_query = if time_start.is_some() || time_end.is_some() {
        // Add time range predicates to the WHERE clause
        let mut conditions = Vec::new();
        
        if let Some(start) = time_start {
            conditions.push(format!("timestamp >= {}", start));
        }
        if let Some(end) = time_end {
            conditions.push(format!("timestamp <= {}", end));
        }
        
        let time_filter = conditions.join(" AND ");
        
        if base_query.to_lowercase().contains("where") {
            format!("{} AND {}", base_query, time_filter)
        } else {
            format!("{} WHERE {}", base_query, time_filter)
        }
    } else {
        base_query.to_string()
    };
    
    log_debug!("Executing SQL query with node_id {node_id}: {final_query}", node_id: node_id, final_query: final_query);
    
    // Execute the SQL query - UNIFIED LOGIC
    let dataframe = ctx.sql(&final_query).await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {}", e))?;
    
    // Collect and display results - UNIFIED LOGIC  
    let batches = dataframe.collect().await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {}", e))?;
    
    if batches.is_empty() {
        println!("No data found after filtering");
    } else {
        println!("=== SQL Query Results ===");
        let pretty_output = arrow_cast::pretty::pretty_format_batches(&batches)
            .map_err(|e| anyhow::anyhow!("Failed to format results: {}", e))?;
        println!("{}", pretty_output);
        
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("\nSummary: {} total rows", total_rows);
    }
    
    Ok(())
}
