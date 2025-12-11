//! OpenTelemetry JSON Format Provider
//!
//! Reads OpenTelemetry JSON Lines files containing metrics data.
//! This implementation:
//! - Focuses ONLY on gauge data points from the metrics signal
//! - Performs two-pass reading: first pass discovers schema (all metric names + timestamps)
//! - Creates a wide schema with timestamp column + one nullable Float64 column per metric
//! - Discards metric attributes (only interested in metric names and values)
//! - Uses official opentelemetry-proto crate for type-safe parsing
//!
//! Data format: JSON Lines, each line is an OTLP ExportMetricsServiceRequest with structure
//! matching the OpenTelemetry Protocol specification.

use crate::format::FormatProvider;
use crate::{Error, Result, Url};
use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// OtelJson format options parsed from query parameters
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OtelJsonOptions {
    /// Batch size for reading (default: 8192)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    8192
}

impl Default for OtelJsonOptions {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
        }
    }
}

/// OpenTelemetry JSON format provider
pub struct OtelJsonProvider;

impl OtelJsonProvider {
    /// Create a new OtelJson provider
    pub fn new() -> Self {
        Self
    }
}

impl Default for OtelJsonProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a single observation: timestamp + metric name + value
#[derive(Debug, Clone)]
struct Observation {
    timestamp_ns: i64,
    metric_name: String,
    value: f64,
}

/// Schema discovery result
#[derive(Debug)]
struct SchemaInfo {
    /// All unique metric names found (sorted for consistent column ordering)
    metric_names: Vec<String>,
    /// All unique timestamps found (sorted)
    timestamps: Vec<i64>,
}

/// Parse a single JSON line using official OTLP types and extract gauge observations
fn parse_line(line: &str) -> Result<Vec<Observation>> {
    let request: ExportMetricsServiceRequest = serde_json::from_str(line)
        .map_err(|e| Error::Arrow(format!("Failed to parse OTLP JSON: {}", e)))?;

    let mut observations = Vec::new();

    for resource_metric in request.resource_metrics {
        for scope_metric in resource_metric.scope_metrics {
            for metric in scope_metric.metrics {
                // Only process gauge metrics - check the data field
                if let Some(Data::Gauge(gauge)) = metric.data {
                    for data_point in gauge.data_points {
                        // Extract the value - gauge data points have various value types
                        let value = match data_point.value {
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v)) => v,
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => v as f64,
                            None => continue, // Skip data points without values
                        };

                        observations.push(Observation {
                            timestamp_ns: data_point.time_unix_nano as i64,
                            metric_name: metric.name.clone(),
                            value,
                        });
                    }
                }
            }
        }
    }

    Ok(observations)
}

/// Discover schema by reading entire file
async fn discover_schema(reader: Pin<Box<dyn AsyncRead + Send>>) -> Result<SchemaInfo> {
    let mut reader = BufReader::new(reader);
    let mut metric_names_set = BTreeSet::new();
    let mut timestamps_set = BTreeSet::new();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .await
            .map_err(|e| Error::Io(e))?;

        if bytes_read == 0 {
            break; // EOF
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let observations = parse_line(trimmed)?;
        for obs in observations {
            let _ = metric_names_set.insert(obs.metric_name);
            let _ = timestamps_set.insert(obs.timestamp_ns);
        }
    }

    Ok(SchemaInfo {
        metric_names: metric_names_set.into_iter().collect(),
        timestamps: timestamps_set.into_iter().collect(),
    })
}

/// Create Arrow schema from discovered metric names
fn create_arrow_schema(metric_names: &[String]) -> SchemaRef {
    let mut fields = vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )];

    for metric_name in metric_names {
        fields.push(Field::new(metric_name, DataType::Float64, true));
    }

    Arc::new(Schema::new(fields))
}

/// Build a single RecordBatch from observations
/// Each row represents one unique timestamp, with columns for each metric
fn build_record_batch(
    schema: SchemaRef,
    observations: &[Observation],
    metric_names: &[String],
) -> Result<RecordBatch> {
    // Group observations by timestamp
    let mut timestamp_map: BTreeMap<i64, BTreeMap<String, f64>> = BTreeMap::new();

    for obs in observations {
        let _ = timestamp_map
            .entry(obs.timestamp_ns)
            .or_insert_with(BTreeMap::new)
            .insert(obs.metric_name.clone(), obs.value);
    }

    // Build timestamp array
    let timestamps: Vec<i64> = timestamp_map.keys().copied().collect();
    let timestamp_array = TimestampNanosecondArray::from(timestamps.clone());

    // Build metric value arrays
    let mut metric_arrays = Vec::new();
    for metric_name in metric_names {
        let values: Vec<Option<f64>> = timestamps
            .iter()
            .map(|ts| timestamp_map.get(ts).and_then(|m| m.get(metric_name)).copied())
            .collect();
        metric_arrays.push(Arc::new(Float64Array::from(values)) as arrow::array::ArrayRef);
    }

    // Combine into columns
    let mut columns = vec![Arc::new(timestamp_array) as arrow::array::ArrayRef];
    columns.extend(metric_arrays);

    RecordBatch::try_new(schema, columns).map_err(|e| Error::Arrow(e.to_string()))
}

/// Stream that reads all data and yields a single RecordBatch
struct OtelJsonStream {
    batch: Option<RecordBatch>,
}

impl Stream for OtelJsonStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.batch.take().map(Ok))
    }
}

#[async_trait]
impl FormatProvider for OtelJsonProvider {
    fn name(&self) -> &str {
        "oteljson"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<SchemaRef> {
        let schema_info = discover_schema(reader).await?;
        Ok(create_arrow_schema(&schema_info.metric_names))
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        _url: &Url,
    ) -> Result<(SchemaRef, Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>)> {
        // Read entire file and build schema + data
        let mut reader = BufReader::new(reader);
        let mut metric_names_set = BTreeSet::new();
        let mut all_observations = Vec::new();
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(|e| Error::Io(e))?;

            if bytes_read == 0 {
                break; // EOF
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let observations = parse_line(trimmed)?;
            for obs in &observations {
                let _ = metric_names_set.insert(obs.metric_name.clone());
            }
            all_observations.extend(observations);
        }

        let metric_names: Vec<String> = metric_names_set.into_iter().collect();
        let schema = create_arrow_schema(&metric_names);

        // Build single batch
        let batch = build_record_batch(schema.clone(), &all_observations, &metric_names)?;

        let stream = Box::pin(OtelJsonStream { batch: Some(batch) });

        Ok((schema, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_parse_gauge_data() {
        // JSON structure using OTLP format with proper field names
        let json = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"test_metric","gauge":{"dataPoints":[{"timeUnixNano":"1750424338211000000","asDouble":123.45}]}}]}]}]}"#;

        let observations = parse_line(json).unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].metric_name, "test_metric");
        assert_eq!(observations[0].value, 123.45);
        assert_eq!(observations[0].timestamp_ns, 1750424338211000000);
    }

    #[tokio::test]
    async fn test_schema_discovery() {
        let data = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"metric_a","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":1.0}]}}]}]}]}
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"metric_b","gauge":{"dataPoints":[{"timeUnixNano":"2000000000","asDouble":2.0}]}}]}]}]}"#;

        let cursor = Cursor::new(data);
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(cursor);

        let schema_info = discover_schema(reader).await.unwrap();
        assert_eq!(schema_info.metric_names.len(), 2);
        assert!(schema_info.metric_names.contains(&"metric_a".to_string()));
        assert!(schema_info.metric_names.contains(&"metric_b".to_string()));
        assert_eq!(schema_info.timestamps.len(), 2);
    }

    #[tokio::test]
    async fn test_oteljson_provider() {
        let data = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"temp","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":20.5}]}}]}]}]}
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"humidity","gauge":{"dataPoints":[{"timeUnixNano":"1000000000","asDouble":65.0}]}},{"name":"temp","gauge":{"dataPoints":[{"timeUnixNano":"2000000000","asDouble":21.0}]}}]}]}]}"#;

        let cursor = Cursor::new(data);
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(cursor);
        let url = Url::parse("oteljson:///test.json").unwrap();

        let provider = OtelJsonProvider::new();
        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();

        // Verify schema: timestamp + humidity + temp (alphabetically sorted)
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "humidity");
        assert_eq!(schema.field(2).name(), "temp");

        // Verify we get one batch
        use futures::StreamExt;
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2); // Two unique timestamps
        assert_eq!(batch.num_columns(), 3);
    }
}

// Register the OtelJson provider
crate::register_format_provider!(scheme: "oteljson", provider: OtelJsonProvider::new);
