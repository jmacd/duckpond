use hydrovu::{HydroVuCollector, HydroVuConfig, Client, WideRecord, HydroVuDevice};
use anyhow::{Result, Context};
use std::env;
use std::path::Path;
use std::collections::HashMap;
use diagnostics::*;
use steward::Ship;
use chrono::DateTime;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 2 {
        print_usage(&args[0]);
        return Ok(());
    }
    
    let config_path = &args[1];
    
    if !Path::new(config_path).exists() {
        return Err(anyhow::anyhow!("Configuration file not found: {config_path}"));
    }
    
    info!("=== HydroVu Comprehensive Data Collection Test ===");
    info!("Loading configuration from: {config_path}");
    
    // Load configuration
    let config = hydrovu::config::load_config(config_path)
        .with_context(|| format!("Failed to load configuration from {config_path}"))?;
    
    let device_count = config.devices.len();
    let max_points = config.max_points_per_run;
    let pond_path = &config.pond_path;
    
    info!("Configuration loaded successfully");
    info!("Target devices: {device_count}");
    info!("Max points per transaction: {max_points}");
    info!("Pond path: {pond_path}");
    
    // Initialize pond (equivalent to "pond init")
    info!("=== Phase 1: Pond Initialization ===");
    let mut ship = initialize_pond(&config.pond_path).await?;
    info!("Pond initialized successfully");
    
    // Create directory structure for HydroVu
    create_hydrovu_directories(&mut ship, &config).await?;
    info!("HydroVu directory structure created");
    
    // Phase 1: Collect all data and track final timestamps
    // Note: We don't use the ship variable here since HydroVuCollector::new() creates its own
    let mut collector = HydroVuCollector::new(config.clone()).await
        .map_err(|e| anyhow::anyhow!("Failed to create collector: {}", e))?;
    
    let final_timestamps = collector.collect_data_with_tracking().await?;
    
    info!("=== Phase 1 Complete ===");
    let device_count = final_timestamps.len();
    info!("Collected data for {device_count} devices");
    for (device_id, timestamp) in &final_timestamps {
        let date_str = hydrovu::utc2date(*timestamp).unwrap_or_else(|_| "invalid date".to_string());
        info!("Device {device_id}: final timestamp {timestamp} ({date_str})");
    }
    
    // Drop the collector to ensure its transactions are committed
    // The collector holds its own Ship instance with active transactions
    drop(collector);
    
    info!("Phase 1 transactions committed, starting Phase 2 verification");
    
    // Drop the original ship and create a fresh one for Phase 2
    // This ensures Phase 2 gets a clean view of all committed changes from Phase 1
    drop(ship);
    let ship = Ship::open_pond(&config.pond_path).await
        .with_context(|| format!("Failed to reopen pond for Phase 2: {}", config.pond_path))?;
    
    run_phase_2_verification(&config, &final_timestamps, ship).await?;
    
    // Report final results
    info!("=== Test Results Summary ===");
    let device_count = final_timestamps.len();
    info!("Phase 1 completed successfully for {device_count} devices");
    info!("Phase 2 verification completed");
    
    Ok(())
}

/// Phase 2: Verify collected data against fresh API calls with random chunking
async fn run_phase_2_verification(
    config: &HydroVuConfig,
    final_timestamps: &HashMap<i64, i64>,
    mut ship: steward::Ship,
) -> Result<()> {
    info!("Starting Phase 2: verification against fresh API calls");
    
    // Clone the data we need before moving into the closure
    let config_clone = config.clone();
    let final_timestamps_clone = final_timestamps.clone();
    
    // Create a transaction to access the MetadataTable
    let mut tx = ship.begin_transaction(vec!["test-runner".to_string(), "phase2_verification".to_string()]).await?;
    let tx_result: anyhow::Result<()> = {
        // Access MetadataTable through the transaction guard
                let data_persistence = tx.data_persistence()
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                
                let mut delta_table = data_persistence.table().clone();
                delta_table.update().await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                
                let _metadata_table = tlogfs::query::NodeTable::new(delta_table);
                
                info!("MetadataTable created successfully through transaction");
                info!("Phase 2 verification basic setup complete");
                
                let device_count = final_timestamps_clone.len();
                info!("Final timestamps for {device_count} devices");
                
                // Create fresh HydroVu client for verification API calls  
                let client = Client::new(config_clone.client_id.clone(), config_clone.client_secret.clone()).await
                    .map_err(|e| anyhow::anyhow!("Failed to create verification client: {e}"))?;
                
                // Create table provider for querying stored data (following cat command pattern)
                let fs = &*tx;  // Deref StewardTransactionGuard to get FS  
                let root = fs.root().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                
                // Resolve path to get node_id and part_id directly (anti-duplication - no wrapper function)
                use tinyfs::Lookup;
                let (parent_wd, lookup_result) = root.resolve_path(&config_clone.hydrovu_path).await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                
                let (node_id, part_id) = match lookup_result {
                    Lookup::Found(node_path) => {
                        (parent_wd.node_path().id().await, node_path.id().await)
                    },
                    Lookup::NotFound(full_path, _) => {
                        return Err(anyhow::anyhow!("File not found: {}", full_path.display()));
                    },
                    Lookup::Empty(_) => {
                        return Err(anyhow::anyhow!("Empty path provided"));
                    }
                };
                
                // Create table provider directly with resolved node_id and part_id  
                let mut tx_guard = tx.transaction_guard().map_err(|e| anyhow::anyhow!("{e}"))?;
                let table_provider = tlogfs::file_table::create_listing_table_provider(node_id, part_id, &mut tx_guard)
                    .await.map_err(|e| anyhow::anyhow!("{e}"))?;
                
                info!("Created fresh API client and SeriesTable for verification");
                
                // Track verification results
                let mut verification_results = VerificationResults::new();
                
                // Verify each device's data
                for (&device_id, &final_timestamp) in &final_timestamps_clone {
                    info!("Starting verification for device {device_id}");
                    
                    match verify_device_data(
                        &client, 
                        &table_provider,
                        &config_clone,
                        device_id, 
                        final_timestamp,
                        &mut verification_results,
                        &mut tx,  // Pass transaction for filesystem access
                    ).await {
                        Ok(_) => {
                            info!("Device {device_id} verification completed successfully");
                        }
                        Err(e) => {
                            error!("Device {device_id} verification failed: {e}");
                            verification_results.record_device_error(device_id, format!("{e}"));
                        }
                    }
                }
                
                // Report final verification results
                info!("=== Phase 2 Verification Results ===");
                verification_results.report();
                
                Ok(())
    };
    
    tx_result?;
    tx.commit().await?;
    
    Ok(())
}

fn print_usage(program_name: &str) {
    println!("HydroVu Comprehensive Data Collection Test");
    println!();
    println!("Usage: {program_name} <config-file>");
    println!();
    println!("Example:");
    println!("  {program_name} hydrovu-config.yaml");
}

async fn initialize_pond(pond_path: &str) -> Result<Ship> {
    // Remove existing pond if it exists
    if Path::new(pond_path).exists() {
        info!("Removing existing pond at: {pond_path}");
        std::fs::remove_dir_all(pond_path)
            .with_context(|| format!("Failed to remove existing pond at {pond_path}"))?;
    }
    
    // Initialize new pond
    info!("Creating new pond at: {pond_path}");
    let ship = Ship::create_pond(pond_path).await
        .with_context(|| format!("Failed to initialize pond at {pond_path}"))?;

    Ok(ship)
}

async fn create_hydrovu_directories(ship: &mut Ship, config: &hydrovu::HydroVuConfig) -> Result<()> {
    let hydrovu_path = config.hydrovu_path.clone();
    let devices = config.devices.clone();
    
    ship.transact(
        vec!["test-runner".to_string(), "create_dirs".to_string()],
        move |_tx, fs| {
            let hydrovu_path = hydrovu_path.clone();
            let devices = devices.clone();
            Box::pin(async move {
                // Get filesystem root
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create base HydroVu directory
                root.create_dir_path(&hydrovu_path).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create devices directory
                let devices_path = format!("{}/devices", hydrovu_path);
                root.create_dir_path(&devices_path).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create directory for each device
                for device in &devices {
                    let device_id = device.id;
                    let device_path = format!("{devices_path}/{device_id}");
                    root.create_dir_path(&device_path).await
                        .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                    debug!("Created device directory: {device_path}");
                }
                
                Ok(())
            })
        }
    ).await?;
    
    Ok(())
}

/// Track verification results across all devices
struct VerificationResults {
    total_devices: usize,
    successful_devices: usize,
    failed_devices: Vec<(i64, String)>,
    partial_devices: Vec<(i64, String)>,
    total_records_compared: usize,
    matching_records: usize,
    mismatched_records: usize,
}

impl VerificationResults {
    fn new() -> Self {
        Self {
            total_devices: 0,
            successful_devices: 0,
            failed_devices: Vec::new(),
            partial_devices: Vec::new(),
            total_records_compared: 0,
            matching_records: 0,
            mismatched_records: 0,
        }
    }
    
    fn record_device_error(&mut self, device_id: i64, error: String) {
        self.total_devices += 1;
        self.failed_devices.push((device_id, error));
    }
    
    fn record_partial_verification(&mut self, device_id: i64, reason: String, matched: usize, mismatched: usize) {
        self.total_devices += 1;
        self.successful_devices += 1; // Still count as successful even if partial
        self.partial_devices.push((device_id, reason));
        self.total_records_compared += matched + mismatched;
        self.matching_records += matched;
        self.mismatched_records += mismatched;
    }
    
    fn record_device_success(&mut self, matched: usize, mismatched: usize) {
        self.total_devices += 1;
        self.successful_devices += 1;
        self.total_records_compared += matched + mismatched;
        self.matching_records += matched;
        self.mismatched_records += mismatched;
    }
    
    fn report(&self) {
        let total = self.total_devices;
        let successful = self.successful_devices;
        let failed_count = self.failed_devices.len();
        let partial_count = self.partial_devices.len();
        
        info!("Devices processed: {total}");
        info!("Successful devices: {successful}");
        info!("Failed devices: {failed_count}");
        
        if partial_count > 0 {
            info!("Partially verified devices: {partial_count}");
            for (device_id, reason) in &self.partial_devices {
                warn!("Device {device_id} partial: {reason}");
            }
        }
        
        if !self.failed_devices.is_empty() {
            for (device_id, error) in &self.failed_devices {
                error!("Device {device_id} failed: {error}");
            }
        }
        
        if self.total_records_compared > 0 {
            let total_compared = self.total_records_compared;
            let matching = self.matching_records;
            let mismatched = self.mismatched_records;
            let match_rate = (self.matching_records as f64 / self.total_records_compared as f64) * 100.0;
            
            info!("Total records compared: {total_compared}");
            info!("Matching records: {matching}");
            info!("Mismatched records: {mismatched}");
            info!("Match rate: {match_rate}%");
            
            if partial_count > 0 {
                warn!("Note: {partial_count} devices had partial verification due to API errors");
            }
        }
    }
}

/// Verify a single device's data against fresh API calls
async fn verify_device_data(
    client: &Client,
    table_provider: &std::sync::Arc<dyn datafusion::catalog::TableProvider>,
    config: &HydroVuConfig,
    device_id: i64,
    final_timestamp: i64,
    verification_results: &mut VerificationResults,
    tx: &mut steward::StewardTransactionGuard<'_>,
) -> Result<()> {
    // 1. Find device config
    let device = config.devices.iter()
        .find(|d| d.id == device_id)
        .ok_or_else(|| anyhow::anyhow!("Device {device_id} not found in configuration"))?;
    
    let device_name = &device.name;
    info!("Verifying device {device_id} ({device_name})");
    
    // 2. Query stored data from SeriesTable
    let stored_records = query_stored_device_data(table_provider, device_id, final_timestamp, tx, config).await?;
    let stored_count = stored_records.len();
    info!("Found {stored_count} stored records for device {device_id}");
    
    if stored_records.is_empty() {
        info!("No stored data found for device {device_id}, skipping verification");
        verification_results.record_device_success(0, 0);
        return Ok(());
    }
    
    // 3. Make fresh API calls with chunked scanning
    let api_records = fetch_fresh_api_data(client, device, final_timestamp, config).await?;
    let api_count = api_records.len();
    info!("Retrieved {api_count} fresh API records for device {device_id}");
    
    // 4. Compare the datasets
    let (matched, mismatched) = compare_datasets(&stored_records, &api_records, device_id)?;
    
    // 5. Check for partial verification due to API errors
    if api_count < stored_count {
        warn!("Device {device_id}: Partial verification - API returned {api_count} records but stored data has {stored_count} records");
        warn!("This may indicate API errors prevented full data retrieval for verification");
    }
    
    // 6. Record results
    if api_count < stored_count {
        let partial_reason = format!("API returned {api_count} records, stored data has {stored_count} records - possible API errors");
        verification_results.record_partial_verification(device_id, partial_reason, matched, mismatched);
    } else {
        verification_results.record_device_success(matched, mismatched);
    }
    
    if mismatched > 0 {
        let total = matched + mismatched;
        warn!("Device {device_id}: {mismatched} mismatched records out of {total}");
    } else {
        info!("Device {device_id}: All {matched} records matched perfectly");
    }
    
    Ok(())
}

/// Query stored device data from TableProvider using SQL
async fn query_stored_device_data(
    _table_provider: &std::sync::Arc<dyn datafusion::catalog::TableProvider>,
    device_id: i64,
    _final_timestamp: i64,
    tx: &mut steward::StewardTransactionGuard<'_>,
    config: &HydroVuConfig,
) -> Result<Vec<WideRecord>> {
    use datafusion::sql::TableReference;
    use arrow::array::Array;  // For is_null method
    
    debug!("Querying stored data for device {device_id}");
    
    // Get TinyFS root from the transaction
    let fs = &**tx;  // Deref StewardTransactionGuard to get FS
    let tinyfs_root = fs.root().await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {e}"))?;
    
    // Use the transaction's managed SessionContext (not our own!)
    let ctx = tx.session_context().await
        .map_err(|e| anyhow::anyhow!("Failed to get session context: {e}"))?;
    
    // Use the actual series path from Phase 1 data storage
    // Find device name from config
    let device = config.devices.iter()
        .find(|d| d.id == device_id)
        .ok_or_else(|| anyhow::anyhow!("Device with ID {device_id} not found in config"))?;
    
    let device_series_path = format!("{}/devices/{device_id}/{}.series", config.hydrovu_path, device.name);
    
    debug!("Looking for series data at path: {device_series_path}");
    
    // Get the actual node_id by looking up the path in TinyFS
    let node_path = tinyfs_root.get_node_path(&device_series_path).await
        .map_err(|e| anyhow::anyhow!("Failed to resolve series path to node_id: {e}"))?;
    let node_id = node_path.node.id().await.to_hex_string();
    
    debug!("Found node_id {node_id} for device {device_id} at path {device_series_path}");
    
    // Create FileTable provider for series data
    let mut tx_guard = tx.transaction_guard()
        .map_err(|e| anyhow::anyhow!("Failed to get transaction guard: {}", e))?;
    
    // Resolve path to get node_id and part_id directly (anti-duplication - no wrapper function)
    use tinyfs::Lookup;
    let (_, lookup_result) = tinyfs_root.resolve_path(&device_series_path).await
        .map_err(|e| anyhow::anyhow!("Failed to resolve path: {}", e))?;
    
    let (resolved_node_id, part_id) = match lookup_result {
        Lookup::Found(node_path) => {
            let node_guard = node_path.borrow().await;
            let file_handle = node_guard.as_file().map_err(|e| {
                anyhow::anyhow!("Path {} does not point to a file: {}", device_series_path, e)
            })?;
            
            // Get the entry type and metadata
            let metadata = file_handle.metadata().await
                .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))?;
            
            match metadata.entry_type {
                tinyfs::EntryType::FileTable | tinyfs::EntryType::FileSeries => {
                    // Extract node_id and part_id from the file_handle
                    use tlogfs::file::OpLogFile;
                    let file_arc = file_handle.handle.get_file().await;
                    let (node_id, part_id) = {
                        let file_guard = file_arc.lock().await;
                        let file_any = file_guard.as_any();
                        let oplog_file = file_any.downcast_ref::<OpLogFile>()
                            .ok_or_else(|| anyhow::anyhow!("FileHandle is not an OpLogFile"))?;
                        (oplog_file.get_node_id(), oplog_file.get_part_id())
                    };
                    (node_id, part_id)
                },
                _ => {
                    return Err(anyhow::anyhow!(
                        "Path {} points to unsupported entry type for table operations: {:?}", 
                        device_series_path, metadata.entry_type
                    ));
                }
            }
        },
        Lookup::NotFound(full_path, _) => {
            return Err(anyhow::anyhow!("File not found: {}", full_path.display()));
        },
        Lookup::Empty(_) => {
            return Err(anyhow::anyhow!("Empty path provided"));
        }
    };
    
    let provider = match tlogfs::file_table::create_listing_table_provider(resolved_node_id, part_id, &mut tx_guard).await {
        Ok(provider) => provider,
        Err(_) => {
            // Fallback: For now, return an error until we have FileTable implementations
            return Err(anyhow::anyhow!("FileTable integration not yet implemented for {}", device_series_path));
        }
    };
    
    // Debug: Print the loaded schema
    let schema = provider.schema();
    let field_count = schema.fields().len();
    debug!("FileTableProvider loaded schema with {field_count} fields");
    
    // Register the provider with DataFusion
    ctx.register_table(TableReference::bare("series"), provider)
        .map_err(|e| anyhow::anyhow!("Failed to register table provider: {e}"))?;
    
    // Execute SQL query to get all records for this device
    // Note: No need to filter by location_id since each series file is device-specific
    // TEMPORARILY RESTORE ORDER BY to test if sorting is causing the schema mismatch issue
    let sql_query = "SELECT * FROM series ORDER BY timestamp".to_string();
    debug!("Executing SQL query: {sql_query}");
    
    let df = ctx.sql(&sql_query).await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {e}"))?;
    
    let record_batches = df.collect().await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {e}"))?;
    
    // Convert Arrow RecordBatch back to Vec<WideRecord>
    let mut records = Vec::new();
    
    for batch in &record_batches {
        let num_rows = batch.num_rows();
        
        // Get the timestamp column
        let timestamp_array = batch.column_by_name("timestamp")
            .ok_or_else(|| anyhow::anyhow!("Missing timestamp column"))?;
        
        // Debug: Check the actual timestamp column type
        let timestamp_data_type = timestamp_array.data_type();
        debug!("Timestamp column data type: {timestamp_data_type}");
        
        // Extract timestamps - handle different timestamp formats
        // Based on HydroVu code, it should be TimestampSecondArray with timezone
        let timestamps = if let Some(ts_seconds) = timestamp_array.as_any().downcast_ref::<arrow::array::TimestampSecondArray>() {
            debug!("Using TimestampSecondArray (expected format)");
            ts_seconds
        } else if let Some(_ts_micros) = timestamp_array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
            debug!("Found TimestampMicrosecondArray, need to convert to seconds");
            return Err(anyhow::anyhow!("TimestampMicrosecondArray found but TimestampSecondArray expected"));
        } else {
            return Err(anyhow::anyhow!("Unsupported timestamp column type: {timestamp_data_type:?}"));
        };
        
        // Create WideRecord for each row
        for row_idx in 0..num_rows {
            // Convert timestamp from seconds to DateTime
            let timestamp_seconds = timestamps.value(row_idx);
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(
                timestamp_seconds,
                0
            ).ok_or_else(|| anyhow::anyhow!("Invalid timestamp value: {timestamp_seconds}"))?;
            
            // Use the device_id from the function parameter since each series is device-specific
            let location_id = device_id;
            
            // Extract all parameter columns (skip timestamp)
            let mut parameters = std::collections::BTreeMap::new();
            
            for field in batch.schema().fields() {
                let col_name = field.name();
                if col_name == "timestamp" {
                    continue;
                }
                
                let column = batch.column_by_name(col_name)
                    .ok_or_else(|| anyhow::anyhow!("Column {col_name} not found in batch"))?;
                if let Some(float_array) = column.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    let value = if float_array.is_null(row_idx) {
                        None
                    } else {
                        Some(float_array.value(row_idx))
                    };
                    parameters.insert(col_name.clone(), value);
                }
            }
            
            records.push(WideRecord {
                timestamp,
                location_id,
                parameters,
            });
        }
    }
    
    let count = records.len();
    let batch_count = record_batches.len();
    debug!("Retrieved {count} stored records for device {device_id} from {batch_count} batches");
    Ok(records)
}

/// Fetch fresh API data by scanning with max_points_per_run chunks until all data retrieved
async fn fetch_fresh_api_data(
    client: &Client,
    device: &HydroVuDevice,
    final_timestamp: i64,
    config: &HydroVuConfig,
) -> Result<Vec<WideRecord>> {
    let device_id = device.id;
    let device_name = &device.name;
    let chunk_size = config.max_points_per_run;
    
    info!("Fetching fresh API data for device {device_id} ({device_name}) using chunk size {chunk_size}");
    info!("Target end timestamp: {final_timestamp}");
    
    // Fetch names data needed for conversion
    let names = client.fetch_names().await?;
    
    let mut all_records = Vec::new();
    let mut current_timestamp = 0;
    let mut session_count = 0;
    
    // Continue fetching until we reach the final_timestamp from Phase 1
    while current_timestamp < final_timestamp {
        session_count += 1;
        info!("Starting API session {session_count} from timestamp {current_timestamp}");
        
        let session_records = fetch_api_session(
            client,
            device,
            &names,
            current_timestamp,
            final_timestamp,
            chunk_size,
            session_count,
        ).await?;
        
        let session_count_actual = session_records.len();
        info!("Session {session_count} completed: {session_count_actual} records");
        
        if session_records.is_empty() {
            info!("No more data available from timestamp {current_timestamp}, ending verification");
            break;
        }
        
        // Update current_timestamp to continue from the last record + 1
        if let Some(latest_record) = session_records.last() {
            current_timestamp = latest_record.timestamp.timestamp() + 1;
        }
        
        all_records.extend(session_records);
        
        let total_count = all_records.len();
        info!("Total records collected: {total_count}");
        
        // Check if we've reached or passed the target timestamp
        if current_timestamp >= final_timestamp {
            info!("Reached target timestamp {final_timestamp}, verification complete");
            break;
        }
    }
    
    let total_count = all_records.len();
    info!("Completed comprehensive API scan for device {device_id}: {total_count} records from {session_count} sessions");
    
    Ok(all_records)
}

/// Fetch one API session (up to 100k points) starting from given timestamp
async fn fetch_api_session(
    client: &Client,
    device: &HydroVuDevice,
    names: &hydrovu::Names,
    start_timestamp: i64,
    final_timestamp: i64,
    chunk_size: usize,
    session_number: usize,
) -> Result<Vec<WideRecord>> {
    let device_id = device.id;
    let device_name = &device.name;
    
    let mut session_records = Vec::new();
    let mut current_timestamp = start_timestamp;
    let mut chunk_count = 0;
    
    loop {
        chunk_count += 1;
        debug!("Session {session_number}, chunk {chunk_count}: fetching from timestamp {current_timestamp}");
        
        // Make API call for this chunk with retry logic
        let location_readings = match client.fetch_location_data(device_id, current_timestamp, chunk_size).await {
            Ok(readings) => readings,
            Err(e) => {
                warn!("First attempt failed for device {device_id} at timestamp {current_timestamp}: {e}");
                
                // Check if this is a 500 error that we should retry
                let error_string = format!("{e}");
                if error_string.contains("500") || error_string.contains("Internal Server Error") {
                    warn!("Detected 500 Internal Server Error, retrying once...");
                    
                    // Wait a moment before retry
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    
                    match client.fetch_location_data(device_id, current_timestamp, chunk_size).await {
                        Ok(readings) => {
                            info!("Retry successful for device {device_id} at timestamp {current_timestamp}");
                            readings
                        },
                        Err(retry_e) => {
                            error!("Retry also failed for device {device_id} at timestamp {current_timestamp}: {retry_e}");
                            error!("=== 500 Error Report ===");
                            error!("Device: {device_id} ({device_name})");
                            error!("Session: {session_number}, Chunk: {chunk_count}");
                            error!("Timestamp: {current_timestamp}");
                            error!("Chunk size: {chunk_size}");
                            let records_so_far = session_records.len();
                            error!("Session records collected so far: {records_so_far}");
                            error!("First error: {e}");
                            error!("Retry error: {retry_e}");
                            
                            // If we have collected some data in this session, return what we have
                            if !session_records.is_empty() {
                                warn!("Returning {records_so_far} records from session {session_number} before error");
                                return Ok(session_records);
                            } else {
                                return Err(anyhow::anyhow!("Failed to fetch any data for device {device_id}: {retry_e}"));
                            }
                        }
                    }
                } else {
                    // For non-500 errors, fail immediately
                    return Err(anyhow::anyhow!("API error for device {device_id}: {e}"));
                }
            }
        };
        
        // Check if we have any readings data
        let has_data = !location_readings.parameters.is_empty() && 
            location_readings.parameters.iter().any(|p| !p.readings.is_empty());
            
        if !has_data {
            debug!("No more data available for device {device_id} at timestamp {current_timestamp} in session {session_number}");
            break;
        }
        
        // Convert LocationReadings to WideRecord using existing logic
        let chunk_records = WideRecord::from_location_readings(
            &location_readings,
            &names.units,
            &names.parameters,
            device,
        )?;
        
        let chunk_size_actual = chunk_records.len();
        debug!("Session {session_number}, chunk {chunk_count}: retrieved {chunk_size_actual} records");
        
        if chunk_records.is_empty() {
            debug!("Empty chunk received for device {device_id} in session {session_number}, ending session");
            break;
        }
        
        // Filter records to only include those up to the final_timestamp from Phase 1
        let filtered_chunk: Vec<WideRecord> = chunk_records
            .into_iter()
            .filter(|record| record.timestamp.timestamp() <= final_timestamp)
            .collect();
        
        let filtered_count = filtered_chunk.len();
        
        // Update current_timestamp to the latest timestamp in this chunk for next iteration
        if let Some(latest_record) = filtered_chunk.last() {
            current_timestamp = latest_record.timestamp.timestamp() + 1; // Start next chunk after this timestamp
        }
        
        session_records.extend(filtered_chunk);
        
        let session_total = session_records.len();
        debug!("Session {session_number}: added {filtered_count} records from chunk {chunk_count} (session total: {session_total})");
        
        // If we got fewer records than requested, we've reached the end of this session
        if chunk_size_actual < chunk_size {
            debug!("Session {session_number}: chunk {chunk_count} returned {chunk_size_actual} < {chunk_size}, ending session");
            break;
        }
        
        // Safety check: if we've hit or passed the final timestamp, stop this session
        if current_timestamp >= final_timestamp {
            debug!("Session {session_number}: reached final timestamp {final_timestamp}, ending session");
            break;
        }
    }
    
    let session_count = session_records.len();
    debug!("Session {session_number} complete: {session_count} records from {chunk_count} chunks");
    
    Ok(session_records)
}

/// Compare stored and API datasets record by record
fn compare_datasets(
    stored_records: &[WideRecord],
    api_records: &[WideRecord],
    device_id: i64,
) -> Result<(usize, usize)> {
    let stored_count = stored_records.len();
    let api_count = api_records.len();
    
    info!("Comparing datasets for device {device_id}: {stored_count} stored records vs {api_count} API records");
    
    // Create hashmaps indexed by timestamp for efficient comparison
    let stored_map: HashMap<DateTime<chrono::Utc>, &WideRecord> = stored_records.iter()
        .map(|r| (r.timestamp, r))
        .collect();
    
    let api_map: HashMap<DateTime<chrono::Utc>, &WideRecord> = api_records.iter()
        .map(|r| (r.timestamp, r))
        .collect();
    
    let mut matched = 0;
    let mut mismatched = 0;
    let mut stored_only = 0;
    let mut api_only = 0;
    
    // Compare all timestamps that exist in both datasets
    for timestamp in stored_map.keys() {
        if let Some(api_record) = api_map.get(timestamp) {
            let stored_record = stored_map[timestamp];
            
            if records_match(stored_record, api_record) {
                matched += 1;
            } else {
                mismatched += 1;
                warn!("Mismatch at timestamp {timestamp} for device {device_id}");
                
                let stored_timestamp = stored_record.timestamp;
                let stored_location_id = stored_record.location_id;
                let stored_params_count = stored_record.parameters.len();
                debug!("Stored record: timestamp={stored_timestamp}, location_id={stored_location_id}, params_count={stored_params_count}");
                
                let api_timestamp = api_record.timestamp;
                let api_location_id = api_record.location_id;
                let api_params_count = api_record.parameters.len();
                debug!("API record: timestamp={api_timestamp}, location_id={api_location_id}, params_count={api_params_count}");
            }
        } else {
            stored_only += 1;
        }
    }
    
    // Count API records that don't exist in stored data
    for timestamp in api_map.keys() {
        if !stored_map.contains_key(timestamp) {
            api_only += 1;
        }
    }
    
    let total_compared = matched + mismatched;
    
    info!("Comparison results for device {device_id}:");
    info!("  - Matched records: {matched}");
    info!("  - Mismatched records: {mismatched}");
    info!("  - Records only in stored data: {stored_only}");
    info!("  - Records only in API data: {api_only}");
    info!("  - Total records compared: {total_compared}");
    
    if stored_only > 0 {
        warn!("Device {device_id}: {stored_only} records exist in stored data but not in API data - this might indicate API data is incomplete");
    }
    
    if api_only > 0 {
        warn!("Device {device_id}: {api_only} records exist in API data but not in stored data - this might indicate newer data since Phase 1");
    }
    
    Ok((matched, mismatched))
}

/// Compare two WideRecords for equality
fn records_match(record1: &WideRecord, record2: &WideRecord) -> bool {
    // Compare basic fields
    if record1.timestamp != record2.timestamp || record1.location_id != record2.location_id {
        return false;
    }
    
    // Compare parameters map - handle Option<f64> with NaN considerations
    if record1.parameters.len() != record2.parameters.len() {
        return false;
    }
    
    for (key, value1) in &record1.parameters {
        match (value1, record2.parameters.get(key)) {
            (None, None | Some(None)) => continue,
            (Some(v1), Some(Some(v2))) => {
                // Handle NaN comparison - NaN != NaN in IEEE 754
                if v1.is_nan() && v2.is_nan() {
                    continue;
                }
                if (v1 - v2).abs() > f64::EPSILON {
                    return false;
                }
            }
            _ => return false, // One is Some, other is None
        }
    }
    
    true
}
