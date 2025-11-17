#![allow(missing_docs)]

mod client;
mod models;

// Factory registration for TLogFS dynamic nodes
pub mod factory;

pub use crate::client::Client;
pub use crate::models::{HydroVuConfig, HydroVuDevice};

use crate::models::{Names, WideRecord};
use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{Float64Builder, TimestampSecondBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, SecondsFormat};
use log::debug;
use parquet::arrow::ArrowWriter;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::env;
use std::io::Cursor;
use std::sync::Arc;
use tinyfs::FS;
use tokio::io::AsyncWriteExt;

/// Summary of collection for a single device
#[derive(Clone, Debug)]
pub struct DeviceSummary {
    pub device_id: i64,
    pub device_name: String,
    pub records_collected: usize,
    pub start_timestamp: i64,
    pub final_timestamp: i64,
}

/// Result of data collection operation
pub struct CollectionResult {
    pub records_collected: usize,
    pub final_timestamps: HashMap<i64, i64>,
    pub device_summaries: Vec<DeviceSummary>,
}

/// Result of single device collection
struct DeviceCollectionResult {
    records_collected: usize,
    start_timestamp: i64,
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
}

fn getenv(name: &str) -> Result<String> {
    env::var(name).map_err(|e| anyhow!("missing env {}: {}", name, e))
}

pub fn get_key() -> Result<(String, String)> {
    let key_id = getenv("HYDRO_KEY_ID")?;
    let key_val = getenv("HYDRO_KEY_VALUE")?;
    Ok((key_id, key_val))
}

impl HydroVuCollector {
    /// Create a new HydroVu collector
    pub async fn new(config: HydroVuConfig) -> Result<Self> {
        debug!("Creating HydroVu collector");

        let (key_id, key_val) = get_key()?;

        // Create HydroVu API client
        let client = Client::new(key_id, key_val)
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
        })
    }

    /// Core data collection function
    /// Works within a single transaction - caller manages transaction lifecycle
    pub async fn collect_data(
        &mut self,
        state: &tlogfs::persistence::State,
        fs: &FS,
    ) -> Result<CollectionResult> {
        let device_count = self.config.devices.len();
        debug!("Starting HydroVu data collection for {device_count} devices");

        // Update dictionaries if needed
        debug!("Updating parameter and unit dictionaries");
        self.update_dictionaries().await?;

        let mut total_records = 0;
        let mut final_timestamps = HashMap::new();
        let mut device_summaries = Vec::new();

        for device in self.config.devices.clone() {
            let result = self.collect_device(&device, state, fs).await?;
            total_records += result.records_collected;

            if let Some(final_ts) = result.final_timestamp
                && final_ts > 0
            {
                _ = final_timestamps.insert(device.id, final_ts);
            }

            // Create summary for this device
            device_summaries.push(DeviceSummary {
                device_id: device.id,
                device_name: device.name.clone(),
                records_collected: result.records_collected,
                start_timestamp: result.start_timestamp,
                final_timestamp: result.final_timestamp.unwrap_or(result.start_timestamp),
            });
        }

        // Report final results
        debug!("HydroVu data collection completed");

        Ok(CollectionResult {
            records_collected: total_records,
            final_timestamps,
            device_summaries,
        })
    }

    async fn collect_device(
        &mut self,
        device: &HydroVuDevice,
        _state: &tlogfs::persistence::State,
        fs: &FS,
    ) -> Result<DeviceCollectionResult> {
        let device_id = device.id;
        let device_name = device.name.clone();
        debug!("Processing device {device_id}");

        let device = device.clone();
        let client = self.client.clone();
        let names = self.names.clone();
        let hydrovu_path = self.config.hydrovu_path.clone();
        let max_points = self.config.max_points_per_run;

        // Call internal collection function directly - no sub-transaction needed
        // We're already running within the caller's single transaction
        // Only one fetch per run (max_points_per_run enforced per run)
        let result =
            Self::collect_device_data_internal(fs, hydrovu_path, client, names, device, max_points)
                .await
                .map_err(|e| {
                    anyhow!(
                        "Failed to collect data for device '{}' (ID: {}): {}",
                        device_name,
                        device_id,
                        e
                    )
                })?;

        let total_records = result.0;
        let start_timestamp = result.1;
        let final_timestamp = if result.2 > 0 { Some(result.2) } else { None };

        if total_records == 0 {
            debug!("No more data available for device {device_id}");
        }

        Ok(DeviceCollectionResult {
            records_collected: total_records,
            start_timestamp,
            final_timestamp,
        })
    }

    /// Update parameter and unit dictionaries from HydroVu API
    /// This is called automatically before each collection run
    async fn update_dictionaries(&mut self) -> Result<()> {
        debug!("Updating HydroVu parameter and unit dictionaries");
        let names = self
            .client
            .fetch_names()
            .await
            .with_context(|| "Failed to update parameter and unit dictionaries")?;
        self.names = names;
        debug!("Successfully updated dictionaries");
        Ok(())
    }

    /// Find the youngest (most recent) timestamp for a device using TinyFS metadata API
    async fn find_youngest_timestamp(
        fs: &FS,
        hydrovu_path: &str,
        device_id: i64,
        device_name: &str,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let device_path = format!(
            "{}/devices/{}/{}.series",
            hydrovu_path, device_id, device_name
        );

        let root_wd = fs
            .root()
            .await
            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

        // Check if file exists before querying versions
        match root_wd.resolve_path(&device_path).await {
            Ok((_, lookup)) => match lookup {
                tinyfs::Lookup::Found(_) => {
                    debug!("Found existing FileSeries for device {device_id}");
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

        // Use TinyFS API for structured metadata access with temporal ranges
        // FAIL-FAST: Propagate errors instead of silent fallback to epoch
        let version_infos = root_wd
            .list_file_versions(&device_path)
            .await
            .map_err(|e| {
                steward::StewardError::Dyn(
                    format!(
                        "Failed to query file versions for device {device_id}: {}",
                        e
                    )
                    .into(),
                )
            })?;

        let version_count = version_infos.len();
        debug!("Found {version_count} file versions for device {device_id}");

        // Find the maximum max_event_time across all versions using extended_metadata
        let mut max_timestamp: Option<i64> = None;
        for (i, version_info) in version_infos.iter().enumerate() {
            if let Some(metadata) = &version_info.extended_metadata {
                if let (Some(min_str), Some(max_str)) = (
                    metadata.get("min_event_time"),
                    metadata.get("max_event_time"),
                ) && let (Ok(min_time), Ok(max_time)) =
                    (min_str.parse::<i64>(), max_str.parse::<i64>())
                {
                    debug!("Version {i}: temporal range {min_time}..{max_time}");
                    max_timestamp =
                        Some(max_timestamp.map_or(max_time, |current| current.max(max_time)));
                }
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
                debug!(
                    "New device {device_id}: no existing temporal data found, starting fresh collection from epoch"
                );
                Ok(0)
            }
        }
    }

    /// Core device data collection function - handles both counting and timestamp tracking
    /// Works within caller's transaction - no sub-transactions
    /// Returns (records_collected, start_timestamp, final_timestamp)
    async fn collect_device_data_internal(
        fs: &FS,
        hydrovu_path: String,
        client: Client,
        names: Names,
        device: HydroVuDevice,
        max_points_per_run: usize,
    ) -> Result<(usize, i64, i64), Box<dyn std::error::Error + Send + Sync>> {
        let device_id = device.id;
        debug!("Starting data collection for device {device_id}");

        // Step 1: Find the youngest timestamp using TinyFS metadata API
        let youngest_timestamp =
            Self::find_youngest_timestamp(fs, &hydrovu_path, device_id, &device.name).await?;

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
                // Concise error - detailed diagnostics already printed to stderr by client
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                    format!(
                        "HydroVu API failed for device '{}' (location {}): {}",
                        device.name, device_id, e
                    ),
                )))
            })?;

        // Convert to wide records
        let wide_records = WideRecord::from_location_readings(
            &location_readings,
            &names.units,
            &names.parameters,
        )
        .map_err(|e| {
            steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid timestamp in API response: {e}"),
            )))
        })?;

        if wide_records.is_empty() {
            debug!("No new records for device {device_id}");
            return Ok((0, youngest_timestamp, youngest_timestamp));
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
            debug!(
                "Device {device_id} collected data from {oldest_timestamp} ({oldest_date}) to {newest_timestamp} ({newest_date}) ({count} records)"
            );
            final_timestamp = newest_timestamp;
        }

        // Step 3: Store data in filesystem within same transaction
        let schema = HydroVuCollector::create_arrow_schema_from_wide_records(&wide_records)
            .map_err(|e| {
                steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                    format!("Schema error: {e}"),
                )))
            })?;

        let record_count = wide_records.len();
        debug!("Converting {record_count} records to Arrow format");

        // Convert WideRecord batch to Arrow RecordBatch
        let record_batch =
            HydroVuCollector::convert_wide_records_to_arrow(&wide_records, &schema, device_id)
                .map_err(|e| {
                    steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                        e.to_string(),
                    )))
                })?;

        // Serialize to Parquet bytes
        let parquet_bytes = HydroVuCollector::serialize_to_parquet(record_batch).map_err(|e| {
            steward::StewardError::DataInit(tlogfs::TLogFSError::Io(std::io::Error::other(
                e.to_string(),
            )))
        })?;

        // Create FileSeries writer
        let device_path = format!("{hydrovu_path}/devices/{device_id}/{}.series", device.name);
        let mut writer = root_wd
            .async_writer_path_with_type(&device_path, tinyfs::EntryType::FileSeriesPhysical)
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

        Ok((count, youngest_timestamp, final_timestamp))
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
    ) -> Result<RecordBatch> {
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
                _ = param_builders.insert(field_name.clone(), Float64Builder::new());
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
    fn serialize_to_parquet(record_batch: RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), None)
                .context("Failed to create Parquet writer")?;

            writer
                .write(&record_batch)
                .context("Failed to write RecordBatch to Parquet")?;

            _ = writer.close().context("Failed to close Parquet writer")?;
        }

        let buffer_size = buffer.len();
        debug!("Serialized to Parquet: {buffer_size} bytes");
        Ok(buffer)
    }
}
