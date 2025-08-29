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
    ship.transact(
        vec!["test-runner".to_string(), "phase2_verification".to_string()],
        move |tx, _fs| {
            Box::pin(async move {
                // Access MetadataTable through the transaction guard
                let data_persistence = tx.data_persistence()
                    .map_err(|e| steward::StewardError::DataInit(e))?;
                
                // CRITICAL: Update the Delta table to see latest commits from Phase 1
                // The persistence layer Delta table might be stale and not show recent commits
                let mut delta_table = data_persistence.table().clone();
                delta_table.update().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Delta(e)))?;
                
                let metadata_table = tlogfs::query::MetadataTable::new(delta_table);
                
                info!("MetadataTable created successfully through transaction");
                info!("Phase 2 verification basic setup complete");
                
                let device_count = final_timestamps_clone.len();
                info!("Final timestamps for {device_count} devices");
                
                // Create fresh HydroVu client for verification API calls  
                let client = Client::new(config_clone.client_id.clone(), config_clone.client_secret.clone()).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create verification client: {e}"),
                    ))))?;
                
                // Create SeriesTable for querying stored data
                let series_table = tlogfs::query::SeriesTable::new(
                    config_clone.hydrovu_path.clone(),
                    metadata_table,
                );
                
                info!("Created fresh API client and SeriesTable for verification");
                
                // Track verification results
                let mut verification_results = VerificationResults::new();
                
                // Verify each device's data
                for (&device_id, &final_timestamp) in &final_timestamps_clone {
                    info!("Starting verification for device {device_id}");
                    
                    match verify_device_data(
                        &client, 
                        &series_table,
                        &config_clone,
                        device_id, 
                        final_timestamp,
                        &mut verification_results,
                        &tx,  // Pass transaction for filesystem access
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
            })
        }
    ).await?;
    
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
            total_records_compared: 0,
            matching_records: 0,
            mismatched_records: 0,
        }
    }
    
    fn record_device_error(&mut self, device_id: i64, error: String) {
        self.total_devices += 1;
        self.failed_devices.push((device_id, error));
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
        
        info!("Devices processed: {total}");
        info!("Successful devices: {successful}");
        info!("Failed devices: {failed_count}");
        
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
        }
    }
}

/// Verify a single device's data against fresh API calls
async fn verify_device_data(
    client: &Client,
    series_table: &tlogfs::query::SeriesTable,
    config: &HydroVuConfig,
    device_id: i64,
    final_timestamp: i64,
    verification_results: &mut VerificationResults,
    tx: &steward::StewardTransactionGuard<'_>,
) -> Result<()> {
    // 1. Find device config
    let device = config.devices.iter()
        .find(|d| d.id == device_id)
        .ok_or_else(|| anyhow::anyhow!("Device {device_id} not found in configuration"))?;
    
    let device_name = &device.name;
    info!("Verifying device {device_id} ({device_name})");
    
    // 2. Query stored data from SeriesTable
    let stored_records = query_stored_device_data(series_table, device_id, final_timestamp, tx, config).await?;
    let stored_count = stored_records.len();
    info!("Found {stored_count} stored records for device {device_id}");
    
    if stored_records.is_empty() {
        info!("No stored data found for device {device_id}, skipping verification");
        verification_results.record_device_success(0, 0);
        return Ok(());
    }
    
    // 3. Make fresh API calls with random chunking
    let api_records = fetch_fresh_api_data(client, device, final_timestamp).await?;
    let api_count = api_records.len();
    info!("Retrieved {api_count} fresh API records for device {device_id}");
    
    // 4. Compare the datasets
    let (matched, mismatched) = compare_datasets(&stored_records, &api_records, device_id)?;
    
    // 5. Record results
    verification_results.record_device_success(matched, mismatched);
    
    if mismatched > 0 {
        let total = matched + mismatched;
        warn!("Device {device_id}: {mismatched} mismatched records out of {total}");
    } else {
        info!("Device {device_id}: All {matched} records matched perfectly");
    }
    
    Ok(())
}

/// Query stored device data from SeriesTable using SQL
async fn query_stored_device_data(
    _series_table: &tlogfs::query::SeriesTable,
    device_id: i64,
    _final_timestamp: i64,
    tx: &steward::StewardTransactionGuard<'_>,
    config: &HydroVuConfig,
) -> Result<Vec<WideRecord>> {
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::TableReference;
    use std::sync::Arc;
    use arrow::array::Array;  // For is_null method
    
    debug!("Querying stored data for device {device_id}");
    
    // Get TinyFS root from the transaction
    let fs = &**tx;  // Deref StewardTransactionGuard to get FS
    let tinyfs_root = fs.root().await
        .map_err(|e| anyhow::anyhow!("Failed to get filesystem root: {e}"))?;
    
    // Get the data persistence layer from the transaction guard
    let data_persistence = tx.data_persistence()
        .map_err(|e| anyhow::anyhow!("Failed to access data persistence: {e}"))?;
    
    // Create MetadataTable using the DeltaTable from persistence
    let metadata_table = tlogfs::query::MetadataTable::new(data_persistence.table().clone());
    
    // Create DataFusion session context
    let ctx = SessionContext::new();
    
    // Use the actual series path from Phase 1 data storage  
    let device_series_path = format!("{}/devices/{device_id}/readings.series", config.hydrovu_path);
    
    debug!("Looking for series data at path: {device_series_path}");
    
    // Get the actual node_id by looking up the path in TinyFS
    let node_path = tinyfs_root.get_node_path(&device_series_path).await
        .map_err(|e| anyhow::anyhow!("Failed to resolve series path to node_id: {e}"))?;
    let node_id = node_path.node.id().await.to_hex_string();
    
    debug!("Found node_id {node_id} for device {device_id} at path {device_series_path}");
    
    // Create UnifiedTableProvider for series data
    let mut provider = tlogfs::query::UnifiedTableProvider::create_series_table_with_tinyfs_and_node_id(
        device_series_path,
        node_id,
        metadata_table,
        Arc::new(tinyfs_root)
    );
    
    // Load the schema from the actual Parquet files
    provider.load_schema_from_data().await
        .map_err(|e| anyhow::anyhow!("Failed to load schema from data: {e}"))?;
    
    // Register the provider with DataFusion
    ctx.register_table(TableReference::bare("series"), Arc::new(provider))
        .map_err(|e| anyhow::anyhow!("Failed to register table provider: {e}"))?;
    
    // Execute SQL query to get all records for this device
    let sql_query = format!("SELECT * FROM series WHERE location_id = {device_id} ORDER BY timestamp");
    
    let df = ctx.sql(&sql_query).await
        .map_err(|e| anyhow::anyhow!("Failed to execute SQL query: {e}"))?;
    
    let record_batches = df.collect().await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {e}"))?;
    
    // Convert Arrow RecordBatch back to Vec<WideRecord>
    let mut records = Vec::new();
    
    for batch in &record_batches {
        let num_rows = batch.num_rows();
        
        // Get the timestamp and location_id columns
        let timestamp_array = batch.column_by_name("timestamp")
            .ok_or_else(|| anyhow::anyhow!("Missing timestamp column"))?;
        let location_id_array = batch.column_by_name("location_id")
            .ok_or_else(|| anyhow::anyhow!("Missing location_id column"))?;
        
        // Extract timestamps and location_ids
        let timestamps = timestamp_array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column"))?;
        let location_ids = location_id_array.as_any().downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to cast location_id column"))?;
        
        // Create WideRecord for each row
        for row_idx in 0..num_rows {
            // Convert timestamp
            let timestamp_micros = timestamps.value(row_idx);
            let timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp(
                timestamp_micros / 1_000_000,
                ((timestamp_micros % 1_000_000) * 1000) as u32
            ).ok_or_else(|| anyhow::anyhow!("Invalid timestamp value"))?;
            
            let location_id = location_ids.value(row_idx);
            
            // Extract all parameter columns (skip timestamp and location_id)
            let mut parameters = std::collections::BTreeMap::new();
            
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let col_name = field.name();
                if col_name == "timestamp" || col_name == "location_id" {
                    continue;
                }
                
                let column = batch.column(col_idx);
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

/// Fetch fresh API data with random chunking
async fn fetch_fresh_api_data(
    client: &Client,
    device: &HydroVuDevice,
    _final_timestamp: i64,
) -> Result<Vec<WideRecord>> {
    let device_id = device.id;
    let device_name = &device.name;
    
    debug!("Fetching fresh API data for device {device_id} ({device_name})");
    
    // Generate random chunk size per request (between 100-1000 records)
    let chunk_size = {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen_range(100..=1000)
    };
    
    debug!("Using random chunk size {chunk_size} for device {device_id}");
    
    // Fetch names data needed for conversion
    let names = client.fetch_names().await?;
    
    // Make API call with random chunking
    let location_readings = client.fetch_location_data(device_id, 0, chunk_size).await?;
    
    // Convert LocationReadings to WideRecord using existing logic
    let records = WideRecord::from_location_readings(
        &location_readings,
        &names.units,
        &names.parameters,
        device,
    )?;
    
    let count = records.len();
    info!("Fetched {count} fresh records for device {device_id} with chunk size {chunk_size}");
    Ok(records)
}

/// Compare stored and API datasets record by record
fn compare_datasets(
    stored_records: &[WideRecord],
    api_records: &[WideRecord],
    device_id: i64,
) -> Result<(usize, usize)> {
    debug!("Comparing datasets for device {device_id}");
    
    // Create hashmaps indexed by timestamp for efficient comparison
    let stored_map: HashMap<DateTime<chrono::Utc>, &WideRecord> = stored_records.iter()
        .map(|r| (r.timestamp, r))
        .collect();
    
    let api_map: HashMap<DateTime<chrono::Utc>, &WideRecord> = api_records.iter()
        .map(|r| (r.timestamp, r))
        .collect();
    
    let mut matched = 0;
    let mut mismatched = 0;
    
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
        }
    }
    
    let stored_count = stored_records.len();
    let api_count = api_records.len();
    debug!("Comparison complete for device {device_id}: {matched} matched, {mismatched} mismatched ({stored_count} stored, {api_count} API)");
    
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
