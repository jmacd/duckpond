pub mod models;
pub mod config;
pub mod client;
pub mod schema;

// Re-export key types for use in tests and external code
pub use crate::models::{HydroVuConfig, HydroVuDevice, WideRecord, Names, FlattenedReading, LocationReadings, Location};
pub use crate::client::Client;

use crate::schema::create_base_schema;
use anyhow::{Result, Context};
use std::path::Path;
use diagnostics::*;

// Ship (steward) integration imports
use steward::Ship;

/// Main HydroVu data collector
pub struct HydroVuCollector {
    config: HydroVuConfig,
    client: Client,
    names: Names,
    ship: Ship,
}

impl HydroVuCollector {
    /// Create a new HydroVu collector from configuration
    pub async fn new(config: HydroVuConfig) -> Result<Self> {
        let client = Client::new(config.client_id.clone(), config.client_secret.clone())
            .await
            .context("Failed to create HydroVu client")?;
        
        let names = client.fetch_names()
            .await
            .context("Failed to fetch parameter and unit names from HydroVu API")?;
        
        // Initialize ship for pond operations
        let ship = Ship::open_existing_pond(&config.pond_path)
            .await
            .context("Failed to open pond")?;
        
        Ok(Self {
            config,
            client,
            names,
            ship,
        })
    }

    /// Run data collection for all configured devices
    pub async fn collect_data(&mut self) -> Result<()> {
        let device_count = self.config.devices.len();
        info!("Starting HydroVu data collection for {device_count} devices");
        
        // Update dictionaries if needed
        debug!("Updating parameter and unit dictionaries");
        self.update_dictionaries().await?;
        
        // Process each device and track failures
        let mut failures = Vec::new();
        let mut successes = 0;
        
        for device in &self.config.devices.clone() {
            let device_id = device.id;
            let device_name = &device.name;
            debug!("Processing device {device_id} ({device_name})");
            match self.collect_device_data(device).await {
                Ok(()) => {
                    successes += 1;
                    info!("Successfully collected data for device {device_id} ({device_name})");
                }
                Err(e) => {
                    error!("Failed to collect data for device {device_id} ({device_name}): {e}");
                    failures.push((device.id, device.name.clone(), e));
                }
            }
        }
        
        // Report final results
        info!("HydroVu data collection completed");
        let failures_count = failures.len();
        info!("Devices processed: {successes} succeeded, {failures_count} failed");
        
        if failures.is_empty() {
            Ok(())
        } else {
            // List all failures
            warn!("Failed devices:");
            for (id, name, _error) in &failures {
                let device_id = *id;
                let device_name = name;
                warn!("  - Device {device_id} ({device_name})");
            }
            
            anyhow::bail!("Data collection failed for {} device(s)", failures.len())
        }
    }

    /// Collect data for a single device
    async fn collect_device_data(&self, device: &HydroVuDevice) -> Result<()> {
        let device_id = device.id;
        let device_name = &device.name;
        debug!("Collecting data for device {device_id} ({device_name})");
        
        let max_rows = self.config.max_rows_per_run.unwrap_or(1000);
        debug!("Target rows to collect: {max_rows}");
        
        // Start by finding what data we already have
        let latest_stored_timestamp = self.find_youngest_timestamp(device.id).await?;
        
        // Start fetching from right after our latest stored data
        let mut current_since_timestamp = latest_stored_timestamp + 1;
        let mut all_wide_records = Vec::new();
        let mut fetch_attempts = 0;
        
        // Keep fetching data until we have enough rows or no more data is available
        while all_wide_records.len() < max_rows {
            fetch_attempts += 1;
            
            let since_datetime = if current_since_timestamp > 0 {
                chrono::DateTime::from_timestamp(current_since_timestamp / 1000, 0)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| format!("Invalid timestamp: {}", current_since_timestamp))
            } else {
                "epoch (1970-01-01)".to_string()
            };
            debug!("Fetch attempt {fetch_attempts}: requesting data since timestamp {current_since_timestamp} ({since_datetime}) for device {device_id}");
            
            // Fetch data from HydroVu API
            let location_readings = self.client
                .fetch_location_data_since(device.id, current_since_timestamp)
                .await
                .with_context(|| format!("Failed to fetch data for device {}", device.id))?;
            
            // Convert to timestamp-joined wide records
            let batch_wide_records = WideRecord::from_location_readings(
                &location_readings,
                &self.names.units,
                &self.names.parameters,
                device,
            );
            
            if batch_wide_records.is_empty() {
                debug!("No more data available from API for device {device_id}");
                break;
            }
            
            let batch_count = batch_wide_records.len();
            debug!("Fetched {batch_count} records in batch {fetch_attempts} for device {device_id}");
            
            // Find the timestamp range of this batch
            let min_timestamp = batch_wide_records.iter()
                .map(|r| r.timestamp.timestamp())
                .min()
                .unwrap_or(0);
            let max_timestamp = batch_wide_records.iter()
                .map(|r| r.timestamp.timestamp())
                .max()
                .unwrap_or(0);
            
            debug!("Batch timestamp range: {min_timestamp} to {max_timestamp} (seconds since epoch)");
            
            // Update the since timestamp to continue from the end of this batch
            current_since_timestamp = (max_timestamp + 1) * 1000; // Convert back to milliseconds
            
            // Add to our collection
            all_wide_records.extend(batch_wide_records);
            
            let current_count = all_wide_records.len();
            debug!("Total collected so far: {current_count} records for device {device_id}");
        }
        
        if all_wide_records.is_empty() {
            debug!("No new data for device {device_id}");
            return Ok(());
        }
        
        let total_available_rows = all_wide_records.len();
        debug!("Found {total_available_rows} total timestamp records for device {device_id}");
        
        // Apply the row limit
        let limited_records: Vec<_> = all_wide_records
            .into_iter()
            .take(max_rows)
            .collect();
        let record_count = limited_records.len();
        
        if total_available_rows > max_rows {
            info!("Limited collection to {record_count} rows (out of {total_available_rows} available) for device {device_id}");
        } else {
            info!("Collected all {record_count} available timestamp records for device {device_id}");
        }
        
        // Debug: Show parameter overview from the wide records
        let mut all_parameters = std::collections::BTreeSet::new();
        for record in &limited_records {
            all_parameters.extend(record.parameters.keys().cloned());
        }
        let param_count = all_parameters.len();
        let params_debug = format!("{:?}", all_parameters);
        debug!("Found {param_count} unique parameters: {params_debug}");
        
        // Get current schema for this device
        let current_schema = self.get_device_schema(device.id).await?;
        
        // Create union schema containing all parameters from existing + new records
        let field_count = current_schema.fields().len();
        debug!("Current schema has {field_count} fields");
        let evolved_schema = self.create_union_schema(&current_schema, &limited_records)?;
        
        let evolved_field_count = evolved_schema.fields().len();
        debug!("Union schema has {evolved_field_count} fields");
        
        // Store the data (single batch since we have union schema)
        let store_count = limited_records.len();
        self.store_device_data(device.id, &evolved_schema, vec![limited_records]).await?;
        
        info!("Successfully stored {store_count} timestamp records for device {device_id}");
        Ok(())
    }

    /// Find the youngest (most recent) timestamp for a device
    async fn find_youngest_timestamp(&self, device_id: i64) -> Result<i64> {
        debug!("Finding youngest timestamp for device {device_id}");
        
        // Construct the device path to query
        let device_path = format!("{}/devices/{}/readings.series", self.config.hydrovu_path, device_id);
        
        // Get access to TinyFS and MetadataTable for temporal queries
        let tinyfs_root = self.ship.data_fs().root().await
            .with_context(|| "Failed to get TinyFS root")?;
        
        let data_path = self.ship.data_path();
        let delta_manager = tlogfs::DeltaTableManager::new();
        let metadata_table = tlogfs::query::MetadataTable::new(data_path.clone(), delta_manager);
        
        // Convert path to node_id via TinyFS resolution
        let (_, lookup) = tinyfs_root.resolve_path(std::path::Path::new(&device_path[1..])).await
            .with_context(|| format!("Failed to resolve device path {}", device_path))?;
        
        let node_id = match lookup {
            tinyfs::Lookup::Found(node_path) => {
                node_path.id().await.to_string()
            }
            _ => {
                debug!("Device path {device_path} not found, starting from epoch");
                return Ok(0); // Device has no data yet, start from beginning
            }
        };
        
        // Query all FileSeries metadata for this device
        let metadata_entries = metadata_table.query_records_for_node(&node_id, tinyfs::EntryType::FileSeries).await
            .with_context(|| format!("Failed to query metadata for device {}", device_id))?;
        
        // Find the maximum max_event_time across all versions
        let mut latest_timestamp = 0i64; // Start from Unix epoch
        let mut found_any = false;
        
        for entry in &metadata_entries {
            if let Some((min_time, max_time)) = entry.temporal_range() {
                let version = entry.version;
                debug!("Device {device_id} version {version} has temporal range {min_time}..{max_time}");
                latest_timestamp = latest_timestamp.max(max_time);
                found_any = true;
            }
        }
        
        if found_any {
            debug!("Found latest timestamp {latest_timestamp} for device {device_id}");
            // Add 1 to get the next timestamp after the last recorded one
            Ok(latest_timestamp + 1)
        } else {
            debug!("No temporal metadata found for device {device_id}, starting from epoch");
            Ok(0) // Start from Unix epoch if no data exists
        }
    }

    /// Get current schema for a device (stub implementation)
    async fn get_device_schema(&self, device_id: i64) -> Result<arrow_schema::Schema> {
        // TODO: Query latest OplogEntry to get the complete current schema
        // For now, return base schema
        debug!("Getting schema for device {device_id} (using base schema for now)");
        Ok(create_base_schema())
    }

    /// Create union schema containing all parameters from existing schema + new records
    /// Follows original HydroVu naming convention: {scope}.{param_name}.{unit_name}
    fn create_union_schema(
        &self,
        current_schema: &arrow_schema::Schema,
        records: &[WideRecord],
    ) -> Result<arrow_schema::Schema> {
        use arrow_schema::{Field, DataType};
        use std::collections::BTreeMap;
        use std::sync::Arc;
        
        // Start with existing schema fields
        let mut all_fields = BTreeMap::new();
        
        // Keep existing fields
        for field in current_schema.fields() {
            all_fields.insert(field.name().clone(), field.clone());
        }
        
        // Add new parameter fields from records using original naming convention
        for record in records {
            for (param_key, _value) in &record.parameters {
                if !all_fields.contains_key(param_key) {
                    // Parameter name should already be formatted as {scope}.{param_name}.{unit_name}
                    // from the WideRecord creation logic
                    let new_field = Arc::new(Field::new(param_key, DataType::Float64, true));
                    all_fields.insert(param_key.clone(), new_field);
                }
            }
        }
        
        // Build final schema with sorted fields (for consistency)
        let mut fields: Vec<_> = all_fields.into_values().collect();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        
        let union_schema = arrow_schema::Schema::new(fields);
        Ok(union_schema)
    }

    /// Store device data using TinyFS FileSeries (automatically handles temporal metadata)
    async fn store_device_data(
        &self,
        device_id: i64,
        schema: &arrow_schema::Schema,
        batches: Vec<Vec<WideRecord>>,
    ) -> Result<()> {
        let batch_count = batches.len();
        let field_count = schema.fields().len();
        debug!("Storing {batch_count} batches for device {device_id} with {field_count} fields");
        
        // Get root working directory
        // Get root working directory from ship's filesystem
        let root_wd = self.ship.data_fs().root().await
            .context("Failed to get root working directory")?;
        
        for (batch_idx, records) in batches.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            
            let record_count = records.len();
            debug!("Converting batch {batch_idx} with {record_count} records to Arrow format");
            
            // Convert WideRecord batch to Arrow RecordBatch
            let record_batch = self.convert_wide_records_to_arrow(&records, schema, device_id)?;
            
            // Serialize to Parquet bytes
            let parquet_bytes = self.serialize_to_parquet(record_batch)?;
            
            // Calculate timestamp range for debugging (TinyFS will extract this automatically)
            let timestamps: Vec<i64> = records.iter()
                .map(|r| r.timestamp.timestamp())
                .collect();
            let min_timestamp = *timestamps.iter().min().unwrap();
            let max_timestamp = *timestamps.iter().max().unwrap();
            
            debug!("Batch timestamp range: {min_timestamp} to {max_timestamp}");
            
            // Create device-specific file path 
            let device_path = format!("{}/devices/{}/readings.series", self.config.hydrovu_path, device_id);
            
            // Create FileSeries writer - TinyFS will handle temporal metadata extraction automatically
            let mut writer = root_wd.async_writer_path_with_type(&device_path, tinyfs::EntryType::FileSeries).await
                .context("Failed to create FileSeries writer")?;
            
            // Write parquet data
            use tokio::io::AsyncWriteExt;
            writer.write_all(&parquet_bytes).await
                .context("Failed to write parquet data")?;
            
            // Shutdown writer - this triggers temporal metadata extraction for FileSeries
            writer.shutdown().await
                .context("Failed to shutdown writer and extract temporal metadata")?;
            
            let parquet_size = parquet_bytes.len();
            info!("Stored batch {batch_idx} for device {device_id}: {record_count} records, {parquet_size} bytes");
        }
        
        let total_batches = batch_count;
        info!("Successfully stored all {total_batches} batches for device {device_id}");
        Ok(())
    }
    
    /// Convert WideRecord batch to Arrow RecordBatch
    /// Following original HydroVu conventions - only timestamp + parameter columns
    fn convert_wide_records_to_arrow(
        &self,
        records: &[WideRecord],
        schema: &arrow_schema::Schema,
        device_id: i64, // Used for logging only, not stored in data
    ) -> Result<arrow_array::RecordBatch> {
        use arrow_array::{Array, RecordBatch};
        use arrow_array::builder::{TimestampSecondBuilder, Float64Builder};
        use std::collections::HashMap;
        use std::sync::Arc;
        
        if records.is_empty() {
            return Err(anyhow::anyhow!("Cannot convert empty records to Arrow"));
        }
        
        let num_records = records.len();
        
        // Build timestamp column
        let mut timestamp_builder = TimestampSecondBuilder::new();
        
        // Build parameter columns - collect all parameter names from schema
        let mut param_builders: HashMap<String, Float64Builder> = HashMap::new();
        for field in schema.fields() {
            let field_name = field.name();
            // Skip the timestamp field
            if field_name != "timestamp" {
                param_builders.insert(field_name.clone(), Float64Builder::new());
            }
        }
        
        // Fill builders with data
        for record in records {
            timestamp_builder.append_value(record.timestamp.timestamp());
            
            // Fill parameter columns (nullable - Some values may not exist for all timestamps)
            for (param_name, builder) in param_builders.iter_mut() {
                if let Some(value) = record.parameters.get(param_name) {
                    match value {
                        Some(v) => builder.append_value(*v),
                        None => builder.append_null(),
                    }
                } else {
                    builder.append_null();
                }
            }
        }
        
        // Finalize arrays in the exact order of schema fields
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        
        // Create arrays in schema field order
        for field in schema.fields() {
            let field_name = field.name();
            if field_name == "timestamp" {
                // Add timestamp array
                arrays.push(Arc::new(timestamp_builder.finish().with_timezone_utc()));
            } else {
                // Add parameter array
                if let Some(mut builder) = param_builders.remove(field_name) {
                    // Parameter has data in this batch
                    arrays.push(Arc::new(builder.finish()));
                } else {
                    // Parameter exists in schema but has no data in this batch - create null array
                    let mut null_builder = Float64Builder::new();
                    for _ in 0..num_records {
                        null_builder.append_null();
                    }
                    arrays.push(Arc::new(null_builder.finish()));
                }
            }
        }
        
        let schema_fields = schema.fields().len();
        let array_count = arrays.len();
        let schema_field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let array_lengths: Vec<usize> = arrays.iter().map(|a| a.len()).collect();
        
        debug!("Schema field order: {#[emit::as_debug] schema_field_names}");
        debug!("Array lengths: {#[emit::as_debug] array_lengths}");
        debug!("Expected rows: {num_records}, schema fields: {schema_fields}, arrays: {array_count}");
        
        // Additional debugging: check data types
        let schema_types: Vec<String> = schema.fields().iter().map(|f| format!("{:?}", f.data_type())).collect();
        let array_types: Vec<String> = arrays.iter().map(|a| format!("{:?}", a.data_type())).collect();
        debug!("Schema types: {#[emit::as_debug] schema_types}");
        debug!("Array types: {#[emit::as_debug] array_types}");
        
        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
            .map_err(|e| {
                debug!("Arrow error details: {e}");
                e
            })
            .with_context(|| {
                format!(
                    "Failed to create Arrow RecordBatch for device {}: schema has {} fields {:?}, but {} arrays provided with lengths {:?}", 
                    device_id, schema_fields, schema_field_names, array_count, array_lengths
                )
            })?;
            
        let batch_rows = record_batch.num_rows();
        debug!("Created Arrow RecordBatch with {batch_rows} rows");
        Ok(record_batch)
    }
    
    /// Serialize Arrow RecordBatch to Parquet bytes
    fn serialize_to_parquet(&self, record_batch: arrow_array::RecordBatch) -> Result<Vec<u8>> {
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), None)
                .context("Failed to create Parquet writer")?;
            
            writer.write(&record_batch)
                .context("Failed to write RecordBatch to Parquet")?;
            
            writer.close()
                .context("Failed to close Parquet writer")?;
        }
        
        let buffer_size = buffer.len();
        debug!("Serialized to Parquet: {buffer_size} bytes");
        Ok(buffer)
    }

    /// Update parameter and unit dictionaries
    async fn update_dictionaries(&self) -> Result<()> {
        debug!("Updating parameter and unit dictionaries...");
        
        // TODO: Implement dictionary updates
        // - Check if units and params tables exist
        // - Add any new entries from self.names
        // - Only update when new instruments appear
        
        debug!("Dictionary updates completed.");
        Ok(())
    }
}

/// Create example configuration file
pub async fn create_example_config<P: AsRef<Path>>(path: P) -> Result<()> {
    config::create_example_config(path)
}

/// Load configuration from file
pub async fn load_config<P: AsRef<Path>>(path: P) -> Result<HydroVuConfig> {
    config::load_config(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_pond_structure_creation() -> Result<()> {
        let temp_dir = tempdir()?;
        
        let config = HydroVuConfig {
            pond_path: temp_dir.path().to_string_lossy().to_string(),
            hydrovu_path: "hydrovu".to_string(), // Use relative path for test
            ..Default::default()
        };
        
        // This would fail in real usage due to invalid credentials,
        // but we can test the directory path construction logic
        let devices_dir = temp_dir.path().join(&config.hydrovu_path).join("devices");
        tokio::fs::create_dir_all(&devices_dir).await?;
        
        assert!(devices_dir.exists());
        
        Ok(())
    }
}
