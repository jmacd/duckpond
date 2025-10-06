use crate::error::TLogFSError;
use crate::large_files;
use crate::transaction_guard::TransactionGuard;
use tinyfs::{NodeID, EntryType};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWriteExt};
use std::io::{Cursor, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use log::{debug, info};

/// Main file writer that supports both small and large files with automatic promotion
pub struct FileWriter<'tx> {
    pub(crate) node_id: NodeID,
    pub(crate) part_id: NodeID,
    pub(crate) file_type: EntryType,
    pub(crate) storage: WriterStorage,
    pub(crate) total_written: u64,
    pub(crate) transaction: &'tx TransactionGuard<'tx>,
}

/// Storage strategy that automatically promotes from memory to external file
pub enum WriterStorage {
    /// Small files buffered in memory
    Small(Vec<u8>),
    /// Large files streamed to external storage
    Large(large_files::HybridWriter),
}

/// Content reference for transaction storage - either inline or external
pub enum ContentRef {
    /// Small content stored inline in Delta Lake
    Small(Vec<u8>),
    /// Large content stored externally with SHA256 hash and size
    Large(String, u64), // SHA256, size
}

/// Result of file write operation
pub struct WriteResult {
    pub size: u64,
    pub metadata: FileMetadata,
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

/// AsyncRead + AsyncSeek interface for content analysis regardless of storage type
pub enum ContentReader {
    /// In-memory content reader
    Memory(Cursor<Vec<u8>>),
    /// File-based content reader
    File(tokio::fs::File),
}

impl<'tx> FileWriter<'tx> {
    // /// Create a new file writer tied to a transaction
    // pub(crate) fn new(
    //     node_id: NodeID,
    //     part_id: NodeID,
    //     file_type: EntryType,
    //     transaction: &'tx TransactionGuard<'tx>,
    // ) -> Self {
    //     let node_hex = node_id.to_hex_string();
    //     debug!("Creating FileWriter for node {node_hex}");
        
    //     Self {
    //         node_id,
    //         part_id,
    //         file_type,
    //         transaction,
    //         storage: WriterStorage::Small(Vec::new()),
    //         total_written: 0,
    //     }
    // }
    
    /// Write data to the file, automatically promoting to large storage if needed
    pub async fn write(&mut self, data: &[u8]) -> Result<(), TLogFSError> {
        match &mut self.storage {
            WriterStorage::Small(buffer) => {
                // Check if we need to promote to large file storage
                let new_size = buffer.len() + data.len();
                if new_size >= large_files::LARGE_FILE_THRESHOLD {
                    debug!("Promoting to large file storage at {new_size} bytes");
                    self.promote_to_large_storage().await?;
                    
                    // Now write to the large storage
                    if let WriterStorage::Large(writer) = &mut self.storage {
                        writer.write_all(data).await
                            .map_err(|e| TLogFSError::Io(e))?;
                    }
                } else {
                    buffer.extend_from_slice(data);
                }
            }
            WriterStorage::Large(writer) => {
                writer.write_all(data).await
                    .map_err(|e| TLogFSError::Io(e))?;
            }
        }
        
        self.total_written += data.len() as u64;
        Ok(())
    }
    
    /// Finalize the write operation with content analysis and transaction storage
    pub async fn finish(mut self) -> Result<WriteResult, TLogFSError> {
        let node_hex = self.node_id.to_hex_string();
        let total_written = self.total_written;
        let node_id = self.node_id;
        let part_id = self.part_id;
        let file_type = self.file_type;

        debug!("Finalizing FileWriter for node {node_hex}, total written: {total_written} bytes");
        
        // Create AsyncRead + AsyncSeek interface for content analysis
        let content_reader = self.create_content_reader().await?;
        
        // Extract metadata based on file type
        let metadata = match file_type {
            EntryType::FileSeries => {
                SeriesProcessor::extract_temporal_metadata(content_reader).await?
            }
            EntryType::FileTable => {
                TableProcessor::validate_schema(content_reader).await?
            }
            EntryType::FileData => {
                FileMetadata::Data // No special processing needed
            }
            _ => {
                return Err(TLogFSError::Transaction {
                    message: format!("Unsupported file type for writer: {:?}", file_type)
                });
            }
        };
        let mut state = self.transaction.state()?;
	    
        // Finalize storage and get content reference
        let content_ref = self.finalize_storage().await?;
        
        // Store in transaction (replaces any existing pending version)
        state.store_file_content_ref(
            node_id,
            part_id,
            content_ref,
            file_type,
            metadata.clone(),
        ).await?;
        
        let node_hex = node_id.to_hex_string();
        let size = total_written;
        info!("Successfully wrote file node {node_hex}, size: {size}");
        
        Ok(WriteResult {
            size: total_written,
            metadata,
        })
    }
    
    /// Promote from small (memory) to large (external) storage
    async fn promote_to_large_storage(&mut self) -> Result<(), TLogFSError> {
        if let WriterStorage::Small(buffer) = &self.storage {
            let mut hybrid_writer = large_files::HybridWriter::new(
                self.transaction.store_path(),
            );
            
            // Write existing buffer content to large storage
            if !buffer.is_empty() {
                hybrid_writer.write_all(buffer).await
                    .map_err(|e| TLogFSError::Io(e))?;
            }
            
            self.storage = WriterStorage::Large(hybrid_writer);
            debug!("Successfully promoted to large file storage");
        }
        Ok(())
    }
    
    /// Create AsyncRead + AsyncSeek interface for content analysis
    async fn create_content_reader(&mut self) -> Result<ContentReader, TLogFSError> {
        match &self.storage {
            WriterStorage::Small(buffer) => {
                // For small files, create AsyncRead from Vec<u8>
                Ok(ContentReader::Memory(Cursor::new(buffer.clone())))
            }
            WriterStorage::Large(_writer) => {
                // For large files, we can't provide a reader without finalizing,
                // and we can't finalize twice. For now, skip content analysis
                // and return a placeholder reader.
                // TODO: Implement proper streaming analysis
                Ok(ContentReader::Memory(Cursor::new(Vec::new())))
            }
        }
    }
    
    /// Finalize storage and return content reference for transaction
    async fn finalize_storage(mut self) -> Result<ContentRef, TLogFSError> {
        match &mut self.storage {
            WriterStorage::Small(buffer) => {
                Ok(ContentRef::Small(buffer.clone()))
            }
            WriterStorage::Large(writer) => {
                // For large files, shutdown and then finalize
                let mut writer = std::mem::take(writer);
                writer.shutdown().await
                    .map_err(|e| TLogFSError::Io(e))?;
                let result = writer.finalize().await
                    .map_err(|e| TLogFSError::Io(e))?;
                Ok(ContentRef::Large(result.sha256, result.size as u64))
            }
        }
    }
}

// Implement AsyncRead + AsyncSeek for ContentReader
impl AsyncRead for ContentReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            ContentReader::Memory(cursor) => Pin::new(cursor).poll_read(cx, buf),
            ContentReader::File(file) => Pin::new(file).poll_read(cx, buf),
        }
    }
}

impl AsyncSeek for ContentReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        match &mut *self {
            ContentReader::Memory(cursor) => Pin::new(cursor).start_seek(position),
            ContentReader::File(file) => Pin::new(file).start_seek(position),
        }
    }
    
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        match &mut *self {
            ContentReader::Memory(cursor) => Pin::new(cursor).poll_complete(cx),
            ContentReader::File(file) => Pin::new(file).poll_complete(cx),
        }
    }
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
        let bytes_read = tokio::io::copy(&mut reader, &mut buffer).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read content for analysis: {}", e)))?;
        
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
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet reader: {}", e)))?;
            
        let reader = reader_builder.build()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet reader: {}", e)))?;
        
        let mut all_batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read Parquet batch: {}", e)))?;
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
        let timestamp_column = crate::schema::detect_timestamp_column(&schema)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to detect timestamp column: {}", e)))?;
        
        debug!("Detected timestamp column: {timestamp_column}");
        
        // Extract temporal range from all batches
        let mut global_min = i64::MAX;
        let mut global_max = i64::MIN;
        
        for batch in &all_batches {
            let (batch_min, batch_max) = crate::schema::extract_temporal_range_from_batch(batch, &timestamp_column)?;
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
        let bytes_read = tokio::io::copy(&mut reader, &mut buffer).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read content for schema validation: {}", e)))?;
        
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
                let schema_json = Self::arrow_schema_to_json(&parquet_schema)?;
                
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
                let has_headers = content_str.lines().next()
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
                } else if content_str.trim_start().starts_with('{') || content_str.trim_start().starts_with('[') {
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
                
                Ok(FileMetadata::Table {
                    schema,
                })
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
        
        serde_json::to_string(&schema_json)
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to serialize schema to JSON: {}", e)))
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
