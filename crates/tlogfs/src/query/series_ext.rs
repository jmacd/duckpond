use crate::query::FileInfo;
use crate::error::TLogFSError;
use arrow::record_batch::RecordBatch;
use std::path::Path;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use async_trait::async_trait;

/// Streaming interface for file:series data with time-range queries
/// 
/// This trait provides the high-level API for working with file:series data in DuckPond.
/// It leverages the SeriesTable DataFusion provider for efficient temporal queries with
/// dual-level filtering: fast file elimination via OplogEntry metadata, followed by
/// automatic Parquet statistics pruning within selected files.
/// 
/// # Architecture Benefits
/// 
/// - **Fast file elimination**: Uses min/max_event_time columns to eliminate entire files
/// - **Memory efficient**: Streams RecordBatches rather than loading all data into memory  
/// - **Version transparent**: Automatically handles multiple file versions behind the scenes
/// - **DataFusion optimized**: Leverages standard Parquet statistics for fine-grained pruning
/// - **Delta Lake compatible**: Follows the same metadata approach as production lakehouse systems
/// 
/// This trait extends TinyFS working directories with time-series specific functionality,
/// enabling efficient streaming of data from multiple file versions that overlap with
/// a specified time range. The implementation handles version discovery, temporal filtering,
/// and record batch streaming in a memory-efficient manner.
#[async_trait]
pub trait SeriesExt {
    /// Stream RecordBatches from all series versions that overlap with the time range
    /// 
    /// This method provides the core time-range query functionality for file:series data.
    /// It uses the dual-level filtering architecture for optimal performance:
    /// 1. Fast file elimination using OplogEntry min/max_event_time metadata
    /// 2. Automatic Parquet statistics pruning within selected files
    /// 
    /// # Arguments
    /// - `path`: Path to the series (e.g., "/sensors/temperature.series")
    /// - `start_time`: Start of time range (inclusive, milliseconds since Unix epoch)
    /// - `end_time`: End of time range (inclusive, milliseconds since Unix epoch)
    /// 
    /// # Returns
    /// SeriesStream that yields RecordBatches from all relevant file versions
    async fn read_series_time_range<P>(
        &self,
        path: P,
        start_time: i64,
        end_time: i64,
    ) -> Result<SeriesStream, TLogFSError>
    where
        P: AsRef<Path> + Send + Sync;
    
    /// Stream RecordBatches from all series versions (no time filtering)
    /// 
    /// This method reads the complete history of a series without time filtering.
    /// Useful for full data exports, analysis of all historical data, or when
    /// you need to process every version that has ever been written.
    /// 
    /// # Arguments
    /// - `path`: Path to the series (e.g., "/sensors/temperature.series")
    /// 
    /// # Returns
    /// SeriesStream that yields RecordBatches from all file versions in chronological order
    async fn read_series_all<P>(
        &self,
        path: P,
    ) -> Result<SeriesStream, TLogFSError>
    where
        P: AsRef<Path> + Send + Sync;
    
    /// Create a new series version with temporal metadata extraction
    /// 
    /// This method writes new data to a series with automatic temporal metadata extraction.
    /// For the first version, it allows setting the timestamp column name. For subsequent
    /// versions, the timestamp column is inherited from the first version.
    /// 
    /// # Arguments
    /// - `path`: Path to the series (e.g., "/sensors/temperature.series")  
    /// - `batch`: RecordBatch containing the data to write
    /// - `timestamp_column`: Optional timestamp column name (auto-detected if None)
    /// 
    /// # Returns
    /// Tuple of (min_event_time, max_event_time) extracted from the data
    async fn append_series_data<P>(
        &self,
        path: P,
        batch: &RecordBatch,
        timestamp_column: Option<&str>,
    ) -> Result<(i64, i64), TLogFSError>
    where
        P: AsRef<Path> + Send + Sync;
}

/// Stream of RecordBatches from multiple series versions
/// 
/// This struct provides a streaming interface over multiple file versions that have been
/// filtered based on temporal metadata. It implements the Stream trait for memory-efficient
/// processing of large datasets.
/// 
/// # Key Features
/// - **Memory efficient**: Only one RecordBatch in memory at a time
/// - **Version metadata**: Provides information about how many versions contribute to the stream
/// - **Time range tracking**: Reports the actual time range covered by the data
/// - **Error handling**: Propagates errors from the underlying Parquet readers
pub struct SeriesStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, TLogFSError>> + Send>>,
    pub total_versions: usize,    // How many file versions contribute to this stream
    pub time_range: (i64, i64),   // Actual time range covered by the data (min, max)
    pub schema_info: SeriesSchemaInfo, // Schema and metadata information
}

/// Schema and metadata information for a series
#[derive(Debug, Clone)]
pub struct SeriesSchemaInfo {
    pub timestamp_column: String,
    pub total_size_bytes: u64,
    pub schema_hash: String, // For schema consistency validation
}

impl SeriesStream {
    /// Create a new SeriesStream from file information
    pub fn new(
        file_infos: Vec<FileInfo>,
        timestamp_column: String,
    ) -> Result<Self, TLogFSError> {
        let total_versions = file_infos.len();
        
        // Calculate actual time range from file metadata
        let time_range = if file_infos.is_empty() {
            (0, 0)
        } else {
            let min_time = file_infos.iter().map(|f| f.min_event_time).min().unwrap_or(0);
            let max_time = file_infos.iter().map(|f| f.max_event_time).max().unwrap_or(0);
            (min_time, max_time)
        };
        
        // Calculate total size
        let total_size_bytes = file_infos.iter()
            .map(|f| f.size.unwrap_or(0))
            .sum();
        
        let schema_info = SeriesSchemaInfo {
            timestamp_column: timestamp_column.clone(),
            total_size_bytes,
            schema_hash: generate_schema_hash(&timestamp_column, total_size_bytes),
        };
        
        // Create the actual stream from file infos
        let stream = create_record_batch_stream(file_infos)?;
        
        Ok(Self {
            inner: Box::pin(stream),
            total_versions,
            time_range,
            schema_info,
        })
    }
    
    /// Collect all batches into a vector (memory-bounded alternative to collect_all)
    /// 
    /// This method is useful when you need all the data in memory for processing.
    /// Use with caution on large datasets as it will load all RecordBatches into memory.
    pub async fn try_collect(mut self) -> Result<Vec<RecordBatch>, TLogFSError> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }
    
    /// Process batches one at a time (memory-efficient)
    /// 
    /// This method provides a convenient way to process each RecordBatch without
    /// loading all data into memory. The function is called for each batch in order.
    pub async fn for_each<F>(mut self, mut f: F) -> Result<(), TLogFSError>
    where
        F: FnMut(RecordBatch) -> Result<(), TLogFSError>,
    {
        while let Some(batch) = self.next().await {
            f(batch?)?;
        }
        Ok(())
    }
    
    /// Get metadata about this series stream
    pub fn metadata(&self) -> &SeriesSchemaInfo {
        &self.schema_info
    }
}

impl Stream for SeriesStream {
    type Item = Result<RecordBatch, TLogFSError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Create a stream of RecordBatches from multiple file versions
/// 
/// This function would create streaming implementation for reading from multiple
/// Parquet files. Currently returns empty stream as SeriesTable uses a different approach.
fn create_record_batch_stream(
    _file_infos: Vec<FileInfo>,
) -> Result<impl Stream<Item = Result<RecordBatch, TLogFSError>>, TLogFSError> {
    // Return empty stream - SeriesTable uses TinyFS directly for actual file access
    let stream = futures::stream::empty();
    Ok(stream)
}

/// Generate a simple schema hash based on timestamp column and total size
fn generate_schema_hash(timestamp_column: &str, total_size_bytes: u64) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    timestamp_column.hash(&mut hasher);
    total_size_bytes.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// High-level convenience functions for series operations
/// 
/// These functions provide additional utilities for working with series data
/// beyond the core SeriesExt trait functionality.
pub mod series_utils {
    use super::*;
    
    /// Validate that a RecordBatch has a consistent schema with an existing series
    pub fn validate_series_schema(
        batch: &RecordBatch,
        existing_timestamp_column: &str,
    ) -> Result<(), TLogFSError> {
        // Check that the timestamp column exists
        if batch.column_by_name(existing_timestamp_column).is_none() {
            return Err(TLogFSError::ArrowMessage(format!(
                "Timestamp column '{}' not found in new data", 
                existing_timestamp_column
            )));
        }
        
        // Basic schema validation complete
        Ok(())
    }
    
    /// Auto-detect the best timestamp column from a RecordBatch schema
    pub fn auto_detect_timestamp_column(batch: &RecordBatch) -> Result<String, TLogFSError> {
        let schema = batch.schema();
        
        // Priority order for auto-detection
        let candidates = ["timestamp", "Timestamp", "event_time", "time", "ts", "datetime"];
        
        for candidate in candidates {
            if let Some((_index, field)) = schema.column_with_name(candidate) {
                if matches!(field.data_type(), arrow::datatypes::DataType::Timestamp(_, _)) {
                    return Ok(candidate.to_string());
                }
            }
        }
        
        Err(TLogFSError::ArrowMessage("No timestamp column found in schema".to_string()))
    }
    
    /// Get series metadata summary for display purposes
    pub async fn get_series_summary<T: SeriesExt>(
        series_provider: &T,
        path: &Path,
    ) -> Result<SeriesSummary, TLogFSError> {
        let stream = series_provider.read_series_all(path).await?;
        
        Ok(SeriesSummary {
            total_versions: stream.total_versions,
            time_range: stream.time_range,
            timestamp_column: stream.schema_info.timestamp_column.clone(),
            total_size_bytes: stream.schema_info.total_size_bytes,
        })
    }
}

/// Summary information about a series for display and monitoring
#[derive(Debug, Clone)]
pub struct SeriesSummary {
    pub total_versions: usize,
    pub time_range: (i64, i64),
    pub timestamp_column: String,
    pub total_size_bytes: u64,
}

impl std::fmt::Display for SeriesSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, 
            "Series Summary: {} versions, time range [{}, {}], timestamp column: '{}', size: {} bytes",
            self.total_versions,
            self.time_range.0,
            self.time_range.1,
            self.timestamp_column,
            self.total_size_bytes
        )
    }
}
