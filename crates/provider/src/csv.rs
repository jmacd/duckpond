//! CSV Format Provider
//!
//! Implements FormatProvider for CSV files using arrow_csv's async Decoder.
//! Supports customization via URL query parameters (delimiter, header, etc.)
//!
//! Uses the async pattern documented in arrow-csv for true async streaming.

use crate::format::FormatProvider;
use crate::{Error, Result, Url};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::{Decoder, Format};
use arrow_csv::ReaderBuilder;
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

/// CSV format options parsed from query parameters
///
/// Based on arrow_csv::reader::Format and ReaderBuilder options.
/// All fields have sensible defaults matching arrow_csv defaults.
///
/// # URL Examples
///
/// - `csv:///data/file.csv` - Default settings (comma delimiter, has header)
/// - `csv:///data/file.csv?delimiter=;` - Semicolon delimiter
/// - `csv:///data/file.csv?delimiter=;&has_header=false` - No header row
/// - `csv:///data/file.csv?batch_size=16384` - Larger batch size
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CsvOptions {
    /// Field delimiter (default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,

    /// Whether the file has a header row (default: true)
    #[serde(default = "default_has_header")]
    pub has_header: bool,

    /// Number of records per batch (default: 8192)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Quote character (default: '"')
    #[serde(default = "default_quote")]
    pub quote: char,

    /// Escape character (default: None)
    pub escape: Option<char>,

    /// Number of rows to sample for schema inference (default: 100)
    #[serde(default = "default_schema_infer_max_records")]
    pub schema_infer_max_records: usize,
}

fn default_delimiter() -> char {
    ','
}
fn default_has_header() -> bool {
    true
}
fn default_batch_size() -> usize {
    8192
}
fn default_quote() -> char {
    '"'
}
fn default_schema_infer_max_records() -> usize {
    100
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            has_header: default_has_header(),
            batch_size: default_batch_size(),
            quote: default_quote(),
            escape: None,
            schema_infer_max_records: default_schema_infer_max_records(),
        }
    }
}

/// CSV format provider using arrow_csv's async Decoder
pub struct CsvProvider;

impl CsvProvider {
    /// Create a new CSV provider
    pub fn new() -> Self {
        Self
    }

    /// Create arrow_csv Format from options
    fn build_format(options: &CsvOptions) -> Format {
        let mut format = Format::default()
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_quote(options.quote as u8);

        if let Some(escape) = options.escape {
            format = format.with_escape(escape as u8);
        }

        format
    }
}

impl Default for CsvProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FormatProvider for CsvProvider {
    fn name(&self) -> &str {
        "csv"
    }

    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<SchemaRef> {
        let options: CsvOptions = url.query_params().unwrap_or_default();
        let format = Self::build_format(&options);

        // Read entire file into memory for schema inference
        // This is acceptable - schema inference typically needs to see the full dataset anyway
        let mut bytes = Vec::new();
        let mut reader = reader;
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| Error::Io(e))?;

        // Use arrow_csv's infer_schema
        let cursor = std::io::Cursor::new(&bytes);
        let (schema, _) = format
            .infer_schema(cursor, Some(options.schema_infer_max_records))
            .map_err(|e| Error::Arrow(e.to_string()))?;

        Ok(Arc::new(schema))
    }

    async fn open_stream(
        &self,
        reader: Pin<Box<dyn AsyncRead + Send>>,
        url: &Url,
    ) -> Result<(SchemaRef, Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>)> {
        let options: CsvOptions = url.query_params().unwrap_or_default();

        // Read entire file into memory for schema inference
        // We'll reuse this buffer for streaming, avoiding re-opening
        let mut bytes = Vec::new();
        let mut reader = reader;
        let _ = reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| Error::Io(e))?;

        // Infer schema from the buffered data
        let format = Self::build_format(&options);
        let cursor = std::io::Cursor::new(&bytes);
        let (schema, _) = format
            .infer_schema(cursor, Some(options.schema_infer_max_records))
            .map_err(|e| Error::Arrow(e.to_string()))?;
        let schema = Arc::new(schema);

        // Create decoder with the inferred schema
        let mut builder = ReaderBuilder::new(schema.clone())
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_batch_size(options.batch_size)
            .with_quote(options.quote as u8);
        
        if let Some(escape) = options.escape {
            builder = builder.with_escape(escape as u8);
        }
        
        let decoder = builder.build_decoder();

        // Create a cursor over the buffered bytes for streaming
        // This positions us at the start of the data
        let cursor = std::io::Cursor::new(bytes);
        let buf_reader = tokio::io::BufReader::new(cursor);

        // Create the stream using our helper
        let stream = decode_csv_stream(decoder, buf_reader);

        Ok((schema, stream))
    }
}

/// Create a stream from a CSV Decoder and AsyncBufRead
///
/// This follows the pattern from arrow-csv documentation for async streaming.
#[allow(dead_code)] // Will be used once we implement file re-opening
fn decode_csv_stream<R: AsyncBufRead + Unpin + Send + 'static>(
    mut decoder: Decoder,
    mut reader: R,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> {
    Box::pin(futures::stream::poll_fn(move |cx| {
        loop {
            let buf = match futures::ready!(Pin::new(&mut reader).poll_fill_buf(cx)) {
                Ok(b) if b.is_empty() => break,
                Ok(b) => b,
                Err(e) => return Poll::Ready(Some(Err(Error::Io(e)))),
            };

            let decoded = match decoder.decode(buf) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(Error::Arrow(e.to_string())))),
            };

            Pin::new(&mut reader).consume(decoded);

            // Check if decoder is ready to flush a batch
            if decoded == 0 || decoder.capacity() == 0 {
                break;
            }
        }

        // Flush accumulated records to a RecordBatch
        match decoder.flush() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(Error::Arrow(e.to_string())))),
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_options_default() {
        let options = CsvOptions::default();
        assert_eq!(options.delimiter, ',');
        assert_eq!(options.has_header, true);
        assert_eq!(options.batch_size, 8192);
        assert_eq!(options.quote, '"');
        assert_eq!(options.escape, None);
    }

    #[test]
    fn test_csv_options_parse() {
        let url = Url::parse("csv:///file.csv?delimiter=;&has_header=false&batch_size=1024")
            .unwrap();
        let options: CsvOptions = url.query_params().unwrap();
        assert_eq!(options.delimiter, ';');
        assert_eq!(options.has_header, false);
        assert_eq!(options.batch_size, 1024);
    }

    #[test]
    fn test_csv_provider_name() {
        let provider = CsvProvider::new();
        assert_eq!(provider.name(), "csv");
    }

    #[tokio::test]
    async fn test_csv_schema_inference() {
        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv").unwrap();

        let schema = provider.infer_schema(reader, &url).await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
    }

    #[tokio::test]
    async fn test_csv_full_stream() {
        use futures::StreamExt;

        let csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv?batch_size=2").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        
        // Verify schema
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");

        // Read first batch (2 rows due to batch_size=2)
        let batch1 = stream.next().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);
        assert_eq!(batch1.num_columns(), 3);

        // Read second batch (1 remaining row)
        let batch2 = stream.next().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 1);
        assert_eq!(batch2.num_columns(), 3);

        // Stream should be exhausted
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_csv_custom_delimiter() {
        use futures::StreamExt;

        let csv_data = b"name;age;city\nAlice;30;NYC\nBob;25;LA\n";
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(&csv_data[..]);

        let provider = CsvProvider::new();
        let url = Url::parse("csv:///test.csv?delimiter=;").unwrap();

        let (schema, mut stream) = provider.open_stream(reader, &url).await.unwrap();
        
        // Verify schema parsed with semicolon delimiter
        assert_eq!(schema.fields().len(), 3);
        
        // Read all data
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
