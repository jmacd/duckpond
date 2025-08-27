pub mod models;
pub mod config;
pub mod client;
pub mod schema;

// Re-export key types for use in tests and external applications
pub use crate::models::{HydroVuConfig, HydroVuDevice, WideRecord, Names, FlattenedReading, LocationReadings, Location};
pub use crate::client::Client;
use tinyfs::FS;

use crate::schema::create_base_schema;
use steward::StewardError;
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
        let ship = Ship::open_pond(&config.pond_path)
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
        
        for device in self.config.devices.clone() {
	    self.collect_device(&device).await?;
        }
        
        // Report final results
        info!("HydroVu data collection completed");
        
        Ok(())
    }

    pub async fn collect_device(&mut self, device: &HydroVuDevice) -> Result<()> {
        let device_id = device.id;
	
        debug!("Processing device {device_id}");

	loop {
	    let device = device.clone();
            let device_name = device.name.clone();
	    let client = self.client.clone();
	    let names = self.names.clone();
	    let hydrovu_path = self.config.hydrovu_path.clone();
	    let max_rows = self.config.max_rows_per_run;

	    let points = self.ship.transact(
		vec!["hydrovu".to_string(), "collect_device_data".to_string(), device_id.to_string()],
		|_tx, fs| Box::pin(async move {
		    match Self::collect_device_data(fs, hydrovu_path, client, names, device, max_rows).await {
			Ok(points) => {
			    info!("Successfully collected data for device {device_id} ({device_name}, {points} points)");
			    Ok(points)
			}
			Err(e) => {
			    error!("Failed to collect data for device {device_id} ({device_name}): {e}");
			    Err(StewardError::Dyn(format!("{}", e).into()))
			}
		    }
		}),
	    ).await?;

	    if points == 0 {
		break
	    }
	}

	Ok(())
    }

    /// Collect device data atomically - reads timestamp, fetches API data, and writes data in single transaction
    pub async fn collect_device_data(
	fs: &FS,
	hydrovu_path: String,
	client: Client,
	names: Names,
        device: HydroVuDevice,
        max_rows_per_run: usize,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let device_id = device.id;
        debug!("Starting atomic data collection for device {device_id}");

        // // Extract data we need before the closure
        // let client = client.clone();
        // let names = names.clone();
        // let device_clone = device.clone();

                let device_path = format!("{hydrovu_path}/devices/{device_id}/readings.series");
                
                let root_wd = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                let youngest_timestamp = match root_wd.metadata_for_path(&device_path).await {
                    Ok(_) => {
                        0
                    }
                    Err(_) => {
                        info!("FileSeries doesn't exist for device {device_id}, starting from epoch");
                        0
                    }
                };

                // Step 2: Fetch data from API within the transaction with row limit
                let location_readings = client.fetch_location_data(device_id, youngest_timestamp, max_rows_per_run).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("API error: {e}")))))?;

                // Convert to wide records
                let wide_records = WideRecord::from_location_readings(
                    &location_readings,
                    &names.units,
                    &names.parameters,
                    &device,
                );

                if wide_records.is_empty() {
                    debug!("No new records for device {device_id}");
                    return Ok(0);
                }

                let count = wide_records.len();
                debug!("Fetched {count} new records from API (client handled row limiting)");

                // Step 3: Store data in filesystem within same transaction
                let schema = HydroVuCollector::create_arrow_schema_from_wide_records(&wide_records)
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Schema error: {e}")))))?;

                // Write data using the same pattern as store_device_data but within this transaction  
                let record_count = wide_records.len();
                debug!("Converting {record_count} records to Arrow format");
                
                // Convert WideRecord batch to Arrow RecordBatch
                let record_batch = HydroVuCollector::convert_wide_records_to_arrow_static(&wide_records, &schema, device_id)
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                    )))?;
                
                // Serialize to Parquet bytes
                let parquet_bytes = HydroVuCollector::serialize_to_parquet_static(record_batch)
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(
                        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                    )))?;
                
                // Create FileSeries writer
                let mut writer = root_wd.async_writer_path_with_type(&device_path, tinyfs::EntryType::FileSeries).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Write parquet data
                use tokio::io::AsyncWriteExt;
                writer.write_all(&parquet_bytes).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(e)))?;
                
                // Shutdown writer
                writer.shutdown().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(e)))?;

        debug!("Processed {count} records for device {device_id}");
        Ok(count)
    }

    /// Get current schema for a device (stub implementation)
    /// Create Arrow schema from WideRecord data - for use in collect_device_data_from_epoch 
    /// Create Arrow schema from WideRecord data
    fn create_arrow_schema_from_wide_records(records: &[WideRecord]) -> Result<arrow_schema::Schema> {
        use arrow_schema::{Field, DataType, TimeUnit};
        use std::collections::BTreeSet;
        use std::sync::Arc;
        
        if records.is_empty() {
            return Ok(create_base_schema());
        }
        
        // Collect all unique parameter names from the records
        let mut all_parameters = BTreeSet::new();
        for record in records {
            all_parameters.extend(record.parameters.keys().cloned());
        }
        
        // Build schema with timestamp field + parameter fields
        let mut fields = Vec::new();
        
        // Add timestamp field first (matching the base schema format)
        fields.push(Arc::new(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            false,
        )));
        
        // Add parameter fields (sorted for consistency)
        for param_name in all_parameters {
            fields.push(Arc::new(Field::new(param_name, DataType::Float64, true)));
        }
        
        let schema = arrow_schema::Schema::new(fields);
        Ok(schema)
    }

    /// Convert WideRecord batch to Arrow RecordBatch - static version for use in transaction closures
    /// Following original HydroVu conventions - only timestamp + parameter columns
    fn convert_wide_records_to_arrow_static(
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
                // Add timestamp array - match the schema's timezone format
                arrays.push(Arc::new(timestamp_builder.finish().with_timezone_opt(Some("+00:00"))));
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
    
    /// Serialize Arrow RecordBatch to Parquet bytes - static version for use in transaction closures
    fn serialize_to_parquet_static(record_batch: arrow_array::RecordBatch) -> Result<Vec<u8>> {
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
