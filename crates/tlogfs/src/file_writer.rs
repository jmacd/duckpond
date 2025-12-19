// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::TLogFSError;
use log::debug;
use tokio::io::{AsyncRead, AsyncSeek};

/// Content reference for transaction storage - either inline or external
pub enum ContentRef {
    /// Small content stored inline in Delta Lake
    Small(Vec<u8>),
    /// Large content stored externally with SHA256 hash and size
    Large(String, u64), // SHA256, size
}

/// File-type specific metadata extracted during finish()
#[derive(Debug, Clone)]
pub enum FileMetadata {
    /// Raw file data, no special metadata
    Data,
    /// Structured table with schema information
    Table {
        schema: String, // JSON schema representation
    },
    /// Time-series data with temporal range
    Series {
        min_timestamp: i64,
        max_timestamp: i64,
        timestamp_column: String,
    },
}

/// Processor for FileSeries content - extracts temporal metadata from Parquet
pub struct SeriesProcessor;

impl SeriesProcessor {
    /// Extract temporal metadata from Parquet content using AsyncRead + AsyncSeek
    pub async fn extract_temporal_metadata<R>(reader: R) -> Result<FileMetadata, TLogFSError>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        debug!("Extracting temporal metadata from FileSeries content");

        // Read entire content into memory for parquet processing
        // Note: For truly massive files, we'd want streaming parquet readers,
        // but for Phase 3 we'll handle the common case of reasonable-sized parquet files
        let mut buffer = Vec::new();
        let mut reader = reader;
        let bytes_read = tokio::io::copy(&mut reader, &mut buffer)
            .await
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to read content for analysis: {}", e))
            })?;

        debug!("Read {bytes_read} bytes for temporal metadata extraction");

        if buffer.is_empty() {
            debug!("Empty content - returning default temporal metadata");
            return Ok(FileMetadata::Series {
                min_timestamp: 0,
                max_timestamp: 0,
                timestamp_column: "timestamp".to_string(),
            });
        }

        // Create parquet reader from buffer
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(buffer);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {}", e))
        })?;

        let reader = reader_builder.build().map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {}", e))
        })?;

        let mut all_batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                TLogFSError::ArrowMessage(format!("Failed to read Parquet batch: {}", e))
            })?;
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            debug!("No data batches in Parquet file");
            return Ok(FileMetadata::Series {
                min_timestamp: 0,
                max_timestamp: 0,
                timestamp_column: "timestamp".to_string(),
            });
        }

        let schema = all_batches[0].schema();

        // Auto-detect timestamp column
        let timestamp_column = crate::schema::detect_timestamp_column(&schema).map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to detect timestamp column: {}", e))
        })?;

        debug!("Detected timestamp column: {timestamp_column}");

        // Extract temporal range from all batches
        let mut global_min = i64::MAX;
        let mut global_max = i64::MIN;

        for batch in &all_batches {
            let (batch_min, batch_max) =
                crate::schema::extract_temporal_range_from_batch(batch, &timestamp_column)?;
            global_min = global_min.min(batch_min);
            global_max = global_max.max(batch_max);
        }

        // Handle edge case where no valid timestamps were found
        if global_min == i64::MAX {
            global_min = 0;
        }
        if global_max == i64::MIN {
            global_max = 0;
        }

        debug!("Extracted temporal range: {global_min} to {global_max}");

        Ok(FileMetadata::Series {
            min_timestamp: global_min,
            max_timestamp: global_max,
            timestamp_column,
        })
    }
}

/// Processor for FileTable content - validates schema and structure
pub struct TableProcessor;

impl TableProcessor {
    /// Validate schema for structured table data
    pub async fn validate_schema<R>(reader: R) -> Result<FileMetadata, TLogFSError>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        debug!("Schema validation for FileTable");

        // Read content for schema analysis
        let mut buffer = Vec::new();
        let mut reader = reader;
        let bytes_read = tokio::io::copy(&mut reader, &mut buffer)
            .await
            .map_err(|e| {
                TLogFSError::ArrowMessage(format!(
                    "Failed to read content for schema validation: {}",
                    e
                ))
            })?;

        debug!("Read {bytes_read} bytes for schema validation");

        if buffer.is_empty() {
            debug!("Empty content - returning default schema");
            return Ok(FileMetadata::Table {
                schema: r#"{"type": "struct", "fields": []}"#.to_string(),
            });
        }

        // Try to parse as Parquet for schema extraction
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tokio_util::bytes::Bytes;

        let bytes = Bytes::from(buffer.clone());
        match ParquetRecordBatchReaderBuilder::try_new(bytes) {
            Ok(reader_builder) => {
                // Successfully parsed as Parquet - extract schema
                let parquet_schema = reader_builder.schema();

                let field_count = parquet_schema.fields().len();
                debug!("Extracted Parquet schema with {field_count} fields");

                // Convert Arrow schema to JSON representation
                let schema_json = Self::arrow_schema_to_json(parquet_schema)?;

                Ok(FileMetadata::Table {
                    schema: schema_json,
                })
            }
            Err(_) => {
                // Not valid Parquet - could be CSV, JSON, or other table format
                // For Phase 3, we'll provide basic validation
                debug!("Content is not valid Parquet - treating as generic table data");

                // Basic content validation - ensure it's reasonable for a table
                let content_str = String::from_utf8_lossy(&buffer);

                // Rough heuristics for table-like content
                let line_count = content_str.lines().count();
                let has_headers = content_str
                    .lines()
                    .next()
                    .map(|line| line.contains(',') || line.contains('\t') || line.contains('|'))
                    .unwrap_or_else(|| {
                        // EXPLICIT: Empty content has no headers by definition
                        debug!("No first line found in content - treating as no headers");
                        false
                    });

                if line_count == 0 {
                    return Ok(FileMetadata::Table {
                        schema: r#"{"type": "struct", "fields": []}"#.to_string(),
                    });
                }

                let detected_format = if has_headers && content_str.contains(',') {
                    "csv"
                } else if content_str.trim_start().starts_with('{')
                    || content_str.trim_start().starts_with('[')
                {
                    "json"
                } else {
                    "unknown"
                };

                debug!("Detected table format: {detected_format}, lines: {line_count}");

                // Generic schema representation
                let schema = format!(
                    r#"{{"type": "struct", "format": "{}", "rows": {}, "validated": false}}"#,
                    detected_format, line_count
                );

                Ok(FileMetadata::Table { schema })
            }
        }
    }

    /// Convert Arrow schema to JSON representation
    fn arrow_schema_to_json(schema: &arrow::datatypes::Schema) -> Result<String, TLogFSError> {
        use serde_json::json;

        let mut fields = Vec::new();
        for field in schema.fields() {
            let field_json = json!({
                "name": field.name(),
                "type": format!("{:?}", field.data_type()),
                "nullable": field.is_nullable()
            });
            fields.push(field_json);
        }

        let schema_json = json!({
            "type": "struct",
            "fields": fields
        });

        serde_json::to_string(&schema_json).map_err(|e| {
            TLogFSError::ArrowMessage(format!("Failed to serialize schema to JSON: {}", e))
        })
    }
}

// #[cfg(test)]
// mod tests {
//     include!("file_writer/tests.rs");
// }

// #[cfg(test)]
// mod phase2_tests {
//     include!("file_writer/phase2_tests.rs");
// }

// #[cfg(test)]
// mod phase3_tests {
//     include!("file_writer/phase3_tests.rs");
// }
