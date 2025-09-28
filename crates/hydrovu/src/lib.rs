mod client;
mod config;
mod models;

pub use crate::client::Client;
pub use crate::models::{HydroVuConfig, HydroVuDevice};
pub use config::load_config;

        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        use tokio::io::AsyncWriteExt;
        use arrow_schema::{DataType, Field, TimeUnit};
        use std::collections::BTreeSet;
        use std::sync::Arc;
//            use arrow_schema::{DataType, Field, TimeUnit};
        use arrow_array::builder::{Float64Builder, TimestampSecondBuilder};
        use arrow_array::{Array, RecordBatch};
        use std::collections::HashMap;

use crate::models::{Names, WideRecord};
use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat};
use log::{debug, error, info};
use std::collections::BTreeMap;
use tinyfs::FS;
use steward::Ship;

/// Result of data collection operation
pub struct CollectionResult {
    pub records_collected: usize,
    pub final_timestamps: HashMap<i64, i64>,
}

/// Result of single device collection
struct DeviceCollectionResult {
    records_collected: usize,
    final_timestamp: Option<i64>,
}

/// Convert Unix timestamp (seconds since epoch) to RFC3339 date string
pub fn utc2date(utc: i64) -> Result<String> {
    Ok(DateTime::from_timestamp(utc, 0)
        .ok_or_else(|| anyhow::anyhow!("cannot convert timestamp {} to date", utc))?
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// Main HydroVu data collector
pub struct HydroVuCollector {
    config: HydroVuConfig,
    client: Client,
    names: Names,
    ship: Ship,
}

impl HydroVuCollector {
    /// Create a new HydroVu collector with provided Ship
    pub async fn new(config: HydroVuConfig, ship: Ship) -> Result<Self> {
        info!("Creating HydroVu collector");

        // Create HydroVu API client
        let client = Client::new(config.client_id.clone(), config.client_secret.clone())
            .await
            .with_context(|| "Failed to create HydroVu API client")?;

        // Initialize empty names dictionary (will be updated on first collection)
        let names = Names {
            parameters: BTreeMap::new(),
            units: BTreeMap::new(),
        };

        Ok(Self {
            config,
            client,
            names,
            ship,
        })
    }

    /// Core data collection function with configurable options
    pub async fn collect_data(&mut self) -> Result<CollectionResult> {
        let device_count = self.config.devices.len();
        info!("Starting HydroVu data collection for {device_count} devices");

        // Update dictionaries if needed
        debug!("Updating parameter and unit dictionaries");
        self.update_dictionaries().await?;

        let mut total_records = 0;
        let mut final_timestamps = HashMap::new();

        for device in self.config.devices.clone() {
            let result = self.collect_device(&device).await?;
            total_records += result.records_collected;

            if let Some(final_ts) = result.final_timestamp {
                if final_ts > 0 {
                    final_timestamps.insert(device.id, final_ts);
                }
            }
        }

        // Report final results
        info!("HydroVu data collection completed");

        Ok(CollectionResult {
            records_collected: total_records,
            final_timestamps,
        })
    }

    async fn collect_device(
        &mut self,
        device: &HydroVuDevice,
    ) -> Result<DeviceCollectionResult> {
        let device_id = device.id;
        debug!("Processing device {device_id}");

        let mut total_records = 0;
        let mut final_timestamp = None;

        loop {
            let device = device.clone();
            let device_name = device.name.clone();
            let client = self.client.clone();
            let names = self.names.clone();
            let hydrovu_path = self.config.hydrovu_path.clone();
            let max_points = self.config.max_points_per_run;

            let result = self.ship.transact(
                    vec!["hydrovu".to_string(), "collect".to_string(), device_id.to_string()],
                    |tx, fs| Box::pin(async move {
                        match Self::collect_device_data_internal(tx, fs, hydrovu_path, client, names, device, max_points).await {
                            Ok((records, last_timestamp)) => {
                                if records > 0 {
                                    info!("Successfully collected data for device {device_id} ({device_name}, {records} records)");
                                }
                                Ok((records, last_timestamp))
                            }
                            Err(e) => {
                                error!("Failed to collect data for device {device_id} ({device_name}): {e}");
                                Err(steward::StewardError::DataInit(tlogfs::TLogFSError::Io(
                                    std::io::Error::new(std::io::ErrorKind::Other, format!("{e}"))
                                )))
                            }
                        }
                    })
                ).await?;

            total_records += result.0;
            if result.1 > 0 {
                final_timestamp = Some(result.1);
            }

            if result.0 == 0 {
                break;
            }
        }

        Ok(DeviceCollectionResult {
            records_collected: total_records,
            final_timestamp,
        })
    }

    /// Update parameter and unit dictionaries from HydroVu API
    /// This is called automatically before each collection run
    async fn update_dictionaries(&mut self) -> Result<()> {
        info!("Updating HydroVu parameter and unit dictionaries");
        let names = self
            .client
            .fetch_names()
            .await
            .with_context(|| "Failed to update parameter and unit dictionaries")?;
        self.names = names;
        info!("Successfully updated dictionaries");
        Ok(())
    }

    /// Find the youngest (most recent) timestamp for a device using SQL over FileSeries metadata
    async fn find_youngest_timestamp_in_transaction(
        fs: &FS,
        tx: &steward::StewardTransactionGuard<'_>,
        hydrovu_path: &str,
        device_id: i64,
        device_name: &str,
    ) -> Result<i64, steward::StewardError> {
        let device_path = format!(
            "{}/devices/{}/{}.series",
            hydrovu_path, device_id, device_name
        );

        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Check if file exists and get node_id and part_id using resolve_path
        let (node_id, part_id) = match root_wd.resolve_path(&device_path).await {
            Ok((parent_wd, lookup)) => match lookup {
                tinyfs::Lookup::Found(found_node) => {
                    let node_guard = found_node.borrow().await;
                    let node_id = node_guard.id();
                    let part_id = parent_wd.node_path().id().await;
                    drop(node_guard);
                    debug!(
                        "Found existing FileSeries for device {device_id} with node_id: {node_id}, part_id: {part_id}"
                    );
                    (node_id, part_id)
                }
                _ => {
                    debug!("FileSeries doesn't exist for device {device_id}: path not found");
                    return Ok(0);
                }
            },
            Err(e) => {
                let err_str = format!("{:?}", e);
                debug!("FileSeries doesn't exist for device {device_id}: {err_str}");
                return Ok(0);
            }
        };

        // Get data persistence from transaction guard
        let data_persistence = tx
            .data_persistence()
            .map_err(|e| steward::StewardError::DataInit(e))?;

        // Create NodeTable for queries
        let metadata_table = tlogfs::query::NodeTable::new(data_persistence.table().clone());

        // Use the direct query method instead of DataFusion SQL
        let records = metadata_table
            .query_records_for_node(&node_id, &part_id, tinyfs::EntryType::FileSeries)
            .await
            .map_err(|e| {
                steward::StewardError::Dyn(
                    format!("Failed to query metadata records: {}", e).into(),
                )
            })?;

        let record_count = records.len();
        debug!("Found {record_count} metadata records for device {device_id} FileSeries");

        // Find the maximum max_event_time across all versions
        let mut max_timestamp: Option<i64> = None;
        for (i, record) in records.iter().enumerate() {
            if let Some((min_time, max_time)) = record.temporal_range() {
                debug!("Record {i}: temporal range {min_time}..{max_time}");
                max_timestamp =
                    Some(max_timestamp.map_or(max_time, |current| current.max(max_time)));
            } else {
                debug!("Record {i}: no temporal range");
            }
        }

        match max_timestamp {
            Some(timestamp) => {
                let next_timestamp = timestamp + 1;
                debug!(
                    "Found youngest timestamp {timestamp} for device {device_id}, will continue from {next_timestamp}"
                );
                Ok(next_timestamp)
            }
            None => {
                debug!("No temporal data found for FileSeries {device_path}, starting from epoch");
                Ok(0)
            }
        }
    }

    /// Core device data collection function - handles both counting and timestamp tracking
    async fn collect_device_data_internal(
        tx: &steward::StewardTransactionGuard<'_>,
        fs: &FS,
        hydrovu_path: String,
        client: Client,
        names: Names,
        device: HydroVuDevice,
        max_points_per_run: usize,
    ) -> Result<(usize, i64), Box<dyn std::error::Error + Send + Sync>> {
        let device_id = device.id;
        debug!("Starting data collection for device {device_id}");

        // Step 1: Find the youngest timestamp using SQL query over FileSeries metadata
        let youngest_timestamp = Self::find_youngest_timestamp_in_transaction(
            fs,
            tx,
            &hydrovu_path,
            device_id,
            &device.name,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let start_date =
            utc2date(youngest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
        debug!(
            "Device {device_id} collection starting from timestamp: {youngest_timestamp} ({start_date})"
        );

        // Get root working directory for file operations
        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Step 2: Fetch data from API within the transaction with row limit
        let location_readings = client
            .fetch_location_data(device_id, youngest_timestamp, max_points_per_run)
            .await
            .map_err(|e| {
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("API error: {e}"),
                )))
            })?;

        // Convert to wide records
        let wide_records = WideRecord::from_location_readings(
            &location_readings,
            &names.units,
            &names.parameters,
            &device,
        )
        .map_err(|e| {
            steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid timestamp in API response: {e}"),
            )))
        })?;

        if wide_records.is_empty() {
            debug!("No new records for device {device_id}");
            return Ok((0, youngest_timestamp));
        }

        let count = wide_records.len();
        debug!("Fetched {count} new records from API (client handled row limiting)");

        // Track final timestamp if needed
        let mut final_timestamp = youngest_timestamp;
        if !wide_records.is_empty() {
            let oldest_timestamp = wide_records
                .iter()
                .map(|r| r.timestamp.timestamp())
                .min()
                .unwrap_or(0);
            let newest_timestamp = wide_records
                .iter()
                .map(|r| r.timestamp.timestamp())
                .max()
                .unwrap_or(0);
            let oldest_date =
                utc2date(oldest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            let newest_date =
                utc2date(newest_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            info!(
                "Device {device_id} collected data from {oldest_timestamp} ({oldest_date}) to {newest_timestamp} ({newest_date}) ({count} records)"
            );
            final_timestamp = newest_timestamp;
        }

        // Step 3: Store data in filesystem within same transaction
        let schema = HydroVuCollector::create_arrow_schema_from_wide_records(&wide_records)
            .map_err(|e| {
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Schema error: {e}"),
                )))
            })?;

        let record_count = wide_records.len();
        debug!("Converting {record_count} records to Arrow format");

        // Convert WideRecord batch to Arrow RecordBatch
        let record_batch =
            HydroVuCollector::convert_wide_records_to_arrow(&wide_records, &schema, device_id)
                .map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )))
                })?;

        // Serialize to Parquet bytes
        let parquet_bytes = HydroVuCollector::serialize_to_parquet(record_batch).map_err(|e| {
            steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )))
        })?;

        // Create FileSeries writer
        let device_path = format!("{hydrovu_path}/devices/{device_id}/{}.series", device.name);
        let mut writer = root_wd
            .async_writer_path_with_type(&device_path, tinyfs::EntryType::FileSeries)
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Write parquet data
        writer
            .write_all(&parquet_bytes)
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(e)))?;

        // Shutdown writer
        writer
            .shutdown()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::Io(e)))?;

        debug!("Processed {count} records for device {device_id}");

        // Log completion with timestamp range for next run
        if !wide_records.is_empty() {
            let next_start_timestamp = final_timestamp + 1;
            let next_date =
                utc2date(next_start_timestamp).unwrap_or_else(|_| "invalid date".to_string());
            debug!(
                "Device {device_id} collection completed. Next run will start from timestamp: {next_start_timestamp} ({next_date})"
            );
        }

        Ok((count, final_timestamp))
    }

    /// Create Arrow schema from WideRecord data
    fn create_arrow_schema_from_wide_records(
        records: &[WideRecord],
    ) -> Result<arrow_schema::Schema> {

        if records.is_empty() {
            // Return empty schema with just timestamp field
            let fields = vec![Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
                false,
            ))];
            return Ok(arrow_schema::Schema::new(fields));
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

    /// Convert WideRecord batch to Arrow RecordBatch
    fn convert_wide_records_to_arrow(
        records: &[WideRecord],
        schema: &arrow_schema::Schema,
        device_id: i64, // Used for logging only, not stored in data
    ) -> Result<arrow_array::RecordBatch> {
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
                arrays.push(Arc::new(
                    timestamp_builder.finish().with_timezone_opt(Some("+00:00")),
                ));
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
        let schema_field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        let array_lengths: Vec<usize> = arrays.iter().map(|a| a.len()).collect();

        log::debug!("Schema field order: {schema_field_names:?}");
        log::debug!("Array lengths: {array_lengths:?}");
        log::debug!(
            "Expected rows: {num_records}, schema fields: {schema_fields}, arrays: {array_count}"
        );

        // Additional debugging: check data types
        let schema_types: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| format!("{:?}", f.data_type()))
            .collect();
        let array_types: Vec<String> = arrays
            .iter()
            .map(|a| format!("{:?}", a.data_type()))
            .collect();
        log::debug!("Schema types: {schema_types:?}");
        log::debug!("Array types: {array_types:?}");

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
    fn serialize_to_parquet(record_batch: arrow_array::RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), None)
                .context("Failed to create Parquet writer")?;

            writer
                .write(&record_batch)
                .context("Failed to write RecordBatch to Parquet")?;

            writer.close().context("Failed to close Parquet writer")?;
        }

        let buffer_size = buffer.len();
        debug!("Serialized to Parquet: {buffer_size} bytes");
        Ok(buffer)
    }
}
