# Provider Crate: URL-Based File Format Abstraction

## Implementation Status (Updated 2025-01-23)

### ✅ COMPLETE: Phase 1 Foundation (Layers 1-3)

**Layer 1: Compression Module**
- ✅ `crates/provider/src/compression.rs` - Standalone decompression utilities
- ✅ Supports zstd, gzip, bzip2 via async-compression
- ✅ Works with any AsyncRead (binary, text, config files)
- ✅ Integration tests: `tests/compression_tests.rs`

**Layer 2: Format Providers**
- ✅ `crates/provider/src/format.rs` - FormatProvider trait
- ✅ `crates/provider/src/csv.rs` - CSV provider with schema inference
- ✅ `crates/provider/src/excelhtml.rs` - **ExcelHTML provider for HydroVu exports**
  - ✅ Uses `tl` crate for HTML parsing
  - ✅ Extracts data from `<table id="isi-report">`
  - ✅ Parses dataHeader rows for column names
  - ✅ Streams data rows as Arrow RecordBatches
  - ✅ **Test result: 5728 rows parsed from sample file**
  - ✅ Registered as `excelhtml:///` scheme
- ✅ `crates/provider/src/sync_bridge.rs` - AsyncToSyncReader bridge
- ✅ Infinite CSV streaming test (100K rows, 391 batches)
- ✅ Query parameter support (delimiter, batch_size, etc.)
- ✅ Integration tests: `tests/csv_integration_tests.rs`, `tests/excel_html_integration_tests.rs`

**Layer 3: Provider API**
- ✅ `crates/provider/src/provider_api.rs` - URL → TableProvider conversion
- ✅ `crates/provider/src/format_registry.rs` - Linkme-based format registration
- ✅ `crates/provider/src/url.rs` - URL validation and parsing
- ✅ CSV provider registration via `register_format_provider!` macro
- ✅ **Glob pattern expansion** - Uses TinyFS `collect_matches()` for multi-file unions
- ✅ Schema validation across multi-file unions
- ✅ Full integration test: URL → FormatProvider → TableProvider → SQL
- ✅ Multi-file test: 3 files × 5 rows = 15 total (glob `csv:///data*.csv`)
- ✅ Tests passing: `cargo test --package provider --lib provider_api`

**Key Files**:
```
crates/provider/src/
├── compression.rs          # Layer 1: decompress() utility
├── format.rs              # Layer 2: FormatProvider trait
├── csv.rs                 # Layer 2: CSV implementation + registration
├── sync_bridge.rs         # Layer 2: Async→Sync bridge
├── provider_api.rs        # Layer 3: Provider API
├── format_registry.rs     # Layer 3: Linkme registry + macro
├── url.rs                 # Layer 3: URL validation
└── lib.rs                 # Public API exports
```

### ✅ COMPLETE: URL Pattern Migration (Phase 1.5)

**All Factory Configs Updated to Use Url Type**
- ✅ Created `crate::Url` type wrapper with Serialize/Deserialize/Display/PartialEq traits
- ✅ URLs serialize as strings, deserialize with automatic validation
- ✅ **sql-derived** - `patterns: HashMap<String, Url>` 
- ✅ **timeseries-join** - `pattern: Url` in each TimeseriesInput
- ✅ **timeseries-pivot** - `pattern: Url`
- ✅ **temporal-reduce** - `in_pattern: Url`, `out_pattern: String` (naming template)
- ✅ **template** - `in_pattern: Url`, `template_file: Url`, `out_pattern: String`
- ✅ All 83 provider tests passing
- ✅ All 85 cmd tests passing (updated test configs to use URL schemes)
- ✅ 370+ tests passing across entire workspace

**Key Design Decisions**:
- Pattern matching fields use `crate::Url` for validation and scheme detection
- Naming templates (`out_pattern`) remain `String` (not URLs, just substitution patterns)
- Path extraction via `.path()` method for filesystem operations
- URL schemes: `series://`, `table://`, `csv://`, `file://`, etc.

### 🚧 TODO: Remaining Implementation

**Next Steps** (in priority order):

1. **OtelJson Format Provider** (Layer 2 extension) ✅ **PRIORITY**
   - Implement `crates/provider/src/oteljson.rs`
   - Register with `register_format_provider!(scheme: "oteljson", provider: OtelJsonProvider::new)`
   - Test with actual OpenTelemetry JSON logs

2. **Factory Integration** (Phase 2) - **Enable gradual migration**
   - ✅ **DONE**: All factories now use Url type for patterns
   - Update factories to detect URL schemes and use Provider API for format conversion
   - Maintain backward compatibility with existing `series://` and `table://` schemes
   - Example migration path:
     ```yaml
     # Existing (still works)
     pattern: "series:///data/sensors/*.series"
     
     # New CSV format support (via Provider API)
     pattern: "csv:///data/sensors/*.csv"
     
     # With compression
     pattern: "csv://gzip/data/sensors/*.csv.gz"
     ```

3. **CLI Integration** (Phase 3)
   - Add `pond cat csv:///pattern` support
   - SQL queries over CSV files

## Executive Summary

The `provider` crate will create a URL-based abstraction layer for accessing files in TLogFS with automatic format conversion. It simplifies the current factory system by using URL schemes to indicate format converters, enabling straightforward data pipeline configuration.

**Core Concept**: `scheme://DECODER/path/pattern` where:
- `decoder` = URL authority, a decompressor (optional)
- `scheme` = format converter (e.g., `oteljson`, `csv`)
- `path/pattern` = TinyFS pattern expansion (e.g., `/data/**/*.json`)

## Provider Crate Design

### Goals

1. **Layered utilities**: Standalone decompression, format decoding, and TableProvider creation
2. **TinyFS integration**: Works with TinyFS FileSystem for file access and pattern expansion
3. **Maintain streaming**: Arrow RecordBatch streaming for large datasets
4. **Enable composition**: Build on provider URLs in higher-level APIs
5. **Backward compatibility**: Coexist with existing factory system during migration

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application / TLogFS                   │
│  pond cat oteljson:///logs/**/*.json --sql "SELECT ..."    │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│            Provider Crate (LAYERED APIs)                    │
│                                                             │
│  Layer 3 (HIGH): TableProvider Creation                    │
│    • TinyFS-aware pattern matching                         │
│    • Multi-file union handling                             │
│    • DataFusion integration                                │
│                                                             │
│  Layer 2 (MID): Format Decoding                           │
│    • FormatProvider trait (CSV, JSON, OtelJson)           │
│    • AsyncRead → RecordBatches                            │
│    • Schema inference with buffering                       │
│                                                             │
│  Layer 1 (LOW): Compression Utilities                     │
│    • decompress(reader, "zstd") → AsyncRead               │
│    • Standalone, format-agnostic                           │
│    • Used by TinyFS for any file type                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
              ┌──────────────┐
              │   TinyFS     │
              │ • FileSystem │
              │ • AsyncRead  │
              │ • Glob match │
              └──────────────┘
```

**Key Abstractions**: 

**Layer 1 (Compression)**: Standalone utilities usable by TinyFS for any file
- `decompress()` wraps AsyncRead with decompression (zstd, gzip, etc.)
- Format-agnostic, works for binary files, config files, anything
- TinyFS can decompress on-the-fly without knowing about formats

**Layer 2 (Format Decoding)**: Converts files to Arrow RecordBatches
- FormatProvider trait for streaming formats (CSV, JSON, OtelJson)
- Takes AsyncRead, returns schema + RecordBatchReader
- Composes with Layer 1 for compressed formats

**Layer 3 (TableProvider Creation)**: High-level TinyFS integration
- Takes TinyFS FileSystem + ProviderUrl
- Handles pattern expansion, multi-file scenarios
- Produces ready-to-use DataFusion TableProviders

### Core Types

```rust
// crates/provider/src/lib.rs

//============================================================================
// LAYER 1: Compression Utilities (Format-agnostic)
//============================================================================

/// Wrap an AsyncRead with decompression based on compression type.
/// This is a standalone utility usable by any code (including TinyFS)
/// for decompressing files of any type (binary, text, config, etc.)
/// 
/// Supported compressions: "zstd", "gzip", "bzip2"
/// Returns the same reader if compression is "none" or None
pub fn decompress(
    reader: Pin<Box<dyn AsyncRead + Send>>,
    compression: Option<&str>,
) -> Result<Pin<Box<dyn AsyncRead + Send>>, ProviderError> {
    match compression {
        None | Some("none") => Ok(reader),
        Some("zstd") => {
            use async_compression::tokio::bufread::ZstdDecoder;
            let buf_reader = tokio::io::BufReader::new(reader);
            Ok(Box::pin(ZstdDecoder::new(buf_reader)))
        }
        Some("gzip") => {
            use async_compression::tokio::bufread::GzipDecoder;
            let buf_reader = tokio::io::BufReader::new(reader);
            Ok(Box::pin(GzipDecoder::new(buf_reader)))
        }
        Some(other) => Err(ProviderError::UnsupportedCompression(other.to_string())),
    }
}

//============================================================================
// LAYER 2: Format Decoding (AsyncRead → RecordBatches)
//============================================================================

/// URL for accessing files with format conversion
/// Format: scheme://[compression]/path/pattern?query_params
/// Examples:
///   - csv:///data/file.csv?delimiter=;
///   - oteljson://zstd/logs/file.json.zstd
///   - csv://gzip/data/**/*.csv.gz
/// 
/// Uses standard `url` crate (url::Url) with validation:
/// - scheme: format name (csv, oteljson, etc.)
/// - host: optional compression (zstd, gzip, etc.)
/// - path: TinyFS path or pattern
/// - query: format-specific options
/// - Rejects: fragment, port, username, password
#[derive(Debug, Clone)]
pub struct ProviderUrl {
    inner: url::Url,
}

impl ProviderUrl {
    /// Parse from string using standard url crate
    /// Returns error if fragment, port, username, or password are present
    pub fn parse(url: &str) -> Result<Self, ProviderError> {
        let parsed = url::Url::parse(url)
            .map_err(|e| ProviderError::InvalidUrl(e.to_string()))?;
        
        // Validate: reject unexpected URL components
        if parsed.fragment().is_some() {
            return Err(ProviderError::InvalidUrl("fragment not allowed".into()));
        }
        if parsed.port().is_some() {
            return Err(ProviderError::InvalidUrl("port not allowed".into()));
        }
        if !parsed.username().is_empty() {
            return Err(ProviderError::InvalidUrl("username not allowed".into()));
        }
        if parsed.password().is_some() {
            return Err(ProviderError::InvalidUrl("password not allowed".into()));
        }
        
        Ok(Self { inner: parsed })
    }
    
    /// Get format scheme (e.g., "csv", "oteljson")
    pub fn scheme(&self) -> &str {
        self.inner.scheme()
    }
    
    /// Get optional compression from host (e.g., "zstd", "gzip")
    pub fn compression(&self) -> Option<&str> {
        self.inner.host_str().filter(|h| !h.is_empty())
    }
    
    /// Get TinyFS path or pattern
    pub fn path(&self) -> &str {
        self.inner.path()
    }
    
    /// Parse query parameters into strongly-typed struct using serde_qs
    pub fn query_params<T: serde::de::DeserializeOwned>(&self) -> Result<T, ProviderError> {
        match self.inner.query() {
            Some(q) => serde_qs::from_str(q)
                .map_err(|e| ProviderError::InvalidQuery(e.to_string())),
            None => serde_qs::from_str("")
                .map_err(|e| ProviderError::InvalidQuery(e.to_string())),
        }
    }
}

/// Format provider trait - converts file data to Arrow RecordBatches
/// For STREAMING formats only (CSV, JSON, etc.) - formats that don't require seek
/// Parquet and other random-access formats use existing TinyFS ObjectStore path
#[async_trait]
pub trait FormatProvider: Send + Sync {
    /// Provider name (matches URL scheme)
    fn name(&self) -> &str;
    
    /// Open file and infer schema, returning both schema and a reader positioned
    /// to continue reading. This allows DataFusion TableProvider to:
    /// 1. Get schema for table metadata
    /// 2. Immediately scan without re-opening/re-decompressing
    /// 
    /// The returned reader may have buffered data from schema inference.
    async fn open_with_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<(SchemaRef, Box<dyn RecordBatchReader>), ProviderError>;
    
    /// Alternative: Just infer schema if already known or for separate operations
    /// This reads the beginning of the stream but discards the reader
    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<SchemaRef, ProviderError> {
        let (schema, _reader) = self.open_with_schema(reader, url).await?;
        Ok(schema)
    }
}

/// Synchronous RecordBatch reader that can be used with DataFusion
/// Wraps the async reading with buffering from schema inference
pub trait RecordBatchReader: Send {
    /// Get the schema
    fn schema(&self) -> SchemaRef;
    
    /// Read next batch (blocking, but internally uses buffered data)
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ProviderError>;
}

/// Format registry - maps schemes to providers
pub struct FormatRegistry {
    providers: HashMap<String, Arc<dyn FormatProvider>>,
}

impl FormatRegistry {
    /// Register a format provider
    pub fn register(&mut self, provider: Arc<dyn FormatProvider>);
    
    /// Get provider by scheme name
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn FormatProvider>>;
    
    /// Global registry instance
    pub fn global() -> &'static RwLock<FormatRegistry>;
}

//============================================================================
// LAYER 3: TableProvider Creation (TinyFS integration)
//============================================================================

/// Main provider API - converts ProviderUrl + TinyFS FileSystem to DataFusion TableProvider
pub struct Provider {
    registry: Arc<RwLock<FormatRegistry>>,
}

impl Provider {
    /// HIGH-LEVEL: Create TableProvider from URL pattern
    /// This is the main entry point - handles everything:
    /// 1. Parse URL and extract pattern
    /// 2. Expand pattern via TinyFS FileSystem glob
    /// 3. Capture matched files into ResolvedFile structs
    /// 4. Determine schema from first file (with decompression/decoding)
    /// 5. Create multi-file TableProvider with lazy file opening
    /// 6. Each file scan uses open_with_schema() for efficiency
    /// 
    /// Returns ready-to-use TableProvider for DataFusion
    pub async fn create_table_provider(
        &self,
        url: &ProviderUrl,
        fs: &impl tinyfs::FileSystem,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;
    
    /// MID-LEVEL: Pattern capture with typed results
    /// Expands URL pattern and captures matches into structured form.
    /// This is useful when you need file metadata before creating TableProvider.
    /// 
    /// Returns:
    /// - PatternMatch with schema and list of resolved files
    /// - Each ResolvedFile has path and metadata
    /// - Can be passed to create_table_provider_from_match() for custom scenarios
    pub async fn capture_pattern(
        &self,
        url: &ProviderUrl,
        fs: &impl tinyfs::FileSystem,
    ) -> Result<PatternMatch, ProviderError>;
    
    /// Create TableProvider from captured pattern (advanced usage)
    /// Most users should use create_table_provider() directly.
    pub async fn create_table_provider_from_match(
        &self,
        pattern_match: PatternMatch,
        fs: &impl tinyfs::FileSystem,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;
}

/// Result of pattern capture - higher-level than raw file list
pub struct PatternMatch {
    /// The URL that was matched
    pub url: ProviderUrl,
    /// Schema inferred from first file
    pub schema: SchemaRef,
    /// All files matching the pattern
    pub files: Vec<ResolvedFile>,
    /// Format provider to use for decoding
    pub provider: Arc<dyn FormatProvider>,
}

/// Individual resolved file after pattern expansion
pub struct ResolvedFile {
    pub path: String,
    pub metadata: tinyfs::FileMetadata,
}
```

### Usage Examples by Layer

```rust
// LAYER 1: Just decompress a file (works for any file type)
let compressed_file = fs.open("/data/config.json.zst").await?;
let decompressed = provider::decompress(compressed_file, Some("zstd"))?;
// Use decompressed AsyncRead for any purpose (read config, binary data, etc.)

// LAYER 2: Decode format to RecordBatches (tabular data only)
let csv_file = fs.open("/data/file.csv.gz").await?;
let decompressed = provider::decompress(csv_file, Some("gzip"))?;
let url = ProviderUrl::parse("csv:///data/file.csv.gz?delimiter=,")?;
let (schema, batch_reader) = csv_provider.open_with_schema(decompressed, &url).await?;
while let Some(batch) = batch_reader.next_batch()? {
    // Process RecordBatch
}

// LAYER 3: Full TableProvider (highest convenience)
let url = ProviderUrl::parse("csv://gzip/data/**/*.csv.gz?delimiter=,")?;
let table = provider.create_table_provider(&url, &fs).await?;
ctx.register_table("data", table)?;
let df = ctx.sql("SELECT * FROM data").await?;
```

### Format Provider Implementations

#### 1. CSV Provider (Initial Implementation)

```rust
// crates/provider/src/csv.rs
use arrow_csv::ReaderBuilder;
use serde::{Deserialize, Serialize};

/// CSV format options parsed from query parameters
/// Based on arrow_csv::reader::Format and ReaderBuilder options
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CsvOptions {
    /// Field delimiter (default: ',')
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    
    /// Has header row (default: true)
    #[serde(default = "default_has_header")]
    pub has_header: bool,
    
    /// Batch size for reading (default: 8192)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    /// Quote character (default: '"')
    #[serde(default = "default_quote")]
    pub quote: char,
    
    /// Escape character (default: None)
    #[serde(default)]
    pub escape: Option<char>,
    
    /// Terminator character (default: '\n')
    #[serde(default = "default_terminator")]
    pub terminator: char,
}

fn default_delimiter() -> char { ',' }
fn default_has_header() -> bool { true }
fn default_batch_size() -> usize { 8192 }
fn default_quote() -> char { '"' }
fn default_terminator() -> char { '\n' }

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            has_header: default_has_header(),
            batch_size: default_batch_size(),
            quote: default_quote(),
            escape: None,
            terminator: default_terminator(),
        }
    }
}

pub struct CsvProvider;

#[async_trait]
impl FormatProvider for CsvProvider {
    fn name(&self) -> &str { "csv" }
    
    async fn open_with_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<(SchemaRef, Box<dyn RecordBatchReader>), ProviderError> {
        let options: CsvOptions = url.query_params()
            .unwrap_or_default();
        
        // Wrap AsyncRead with decompression if needed
        // Note: async-compression decoders take AsyncBufRead, return AsyncRead
        let decompressed = if let Some(compression) = url.compression() {
            let buf_reader = BufReader::new(reader);
            decompress(buf_reader, compression)?
        } else {
            reader
        };
        
        // Convert to sync reader with buffering
        // This is where we'll read the header and infer schema
        let sync_reader = AsyncToSyncReader::new(decompressed);
        let mut buf_reader = BufReader::new(sync_reader);
        
        // Infer schema using arrow_csv (reads header + sample rows)
        let (schema, bytes_read) = infer_csv_schema(
            &mut buf_reader,
            options.delimiter as u8,
            options.has_header,
        )?;
        
        // Create CSV reader that will continue from buffered position
        // The BufReader contains any data we read during inference
        let csv_reader = ReaderBuilder::new(schema.clone())
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_batch_size(options.batch_size)
            .build(buf_reader)?;
        
        // Wrap in our RecordBatchReader trait
        let batch_reader = Box::new(CsvBatchReader {
            inner: csv_reader,
            schema: schema.clone(),
        });
        
        Ok((schema, batch_reader))
    }
}

struct CsvBatchReader {
    inner: arrow_csv::Reader<BufReader<AsyncToSyncReader>>,
    schema: SchemaRef,
}

impl RecordBatchReader for CsvBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ProviderError> {
        self.inner.next()
            .transpose()
            .map_err(|e| ProviderError::ReadError(e.to_string()))
    }
}
```

#### 2. OtelJson Provider

```rust
// crates/provider/src/oteljson.rs
use serde::{Deserialize, Serialize};

/// OtelJson format options parsed from query parameters
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct OtelJsonOptions {
    /// Batch size for reading (default: 1024)
    #[serde(default = "default_otel_batch_size")]
    pub batch_size: usize,
}

fn default_otel_batch_size() -> usize { 1024 }

pub struct OtelJsonProvider;

#[async_trait]
impl FormatProvider for OtelJsonProvider {
    fn name(&self) -> &str { "oteljson" }
    
    async fn open_with_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<(SchemaRef, Box<dyn RecordBatchReader>), ProviderError> {
        let options: OtelJsonOptions = url.query_params()
            .unwrap_or_default();
        
        // OtelJson has well-defined schema from spec - no inference needed
        let schema = Arc::new(create_otel_schema());
        
        // Wrap AsyncRead with decompression if needed
        // Note: async-compression decoders take AsyncBufRead, return AsyncRead
        let decompressed = if let Some(compression) = url.compression() {
            let buf_reader = BufReader::new(reader);
            decompress(buf_reader, compression)?
        } else {
            reader
        };
        
        // Create streaming JSON line reader
        // Since schema is known, we can start reading immediately
        let batch_reader = Box::new(OtelJsonBatchReader::new(
            decompressed,
            schema.clone(),
            options.batch_size,
        ));
        
        Ok((schema, batch_reader))
    }
}

struct OtelJsonBatchReader {
    reader: Pin<Box<dyn AsyncRead>>,
    schema: SchemaRef,
    batch_size: usize,
    buffer: Vec<u8>,
}

impl OtelJsonBatchReader {
    fn new(reader: Pin<Box<dyn AsyncRead>>, schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            reader,
            schema,
            batch_size,
            buffer: Vec::new(),
        }
    }
}

impl RecordBatchReader for OtelJsonBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ProviderError> {
        // Read batch_size lines of JSON
        // Parse into Arrow RecordBatch matching OTel schema
        // This would use tokio::runtime::Handle::current().block_on()
        // to bridge async read in sync context
        todo!("implement JSON line reading and parsing")
    }
}
```

#### 3. Parquet - NOT in Provider Crate

**Important**: Parquet is NOT a streaming format and requires random access (AsyncReadSeek) for:
- Reading file metadata from footer
- Accessing column chunks at arbitrary positions
- Efficient predicate pushdown via row group pruning

**Parquet remains in the existing TinyFS ObjectStore path** which already handles:
- `tinyfs://` URLs with Parquet format
- Random access via AsyncReadSeek
- Integration with DataFusion's ParquetFormat and ListingTable

The provider crate is **only for streaming formats** that work with AsyncRead:
- CSV (line-by-line streaming)
- JSON/NDJSON (line-by-line streaming)  
- OtelJson (streaming JSON lines)
- Any custom streaming format

This separation is architectural - don't try to shoehorn Parquet into the streaming provider model.

### Integration Points

#### Phase 1: Standalone Development

```rust
// Test provider crate independently
#[tokio::test]
async fn test_csv_provider() {
    let provider = Provider::new();
    let url = ProviderUrl::parse("csv:///test/data.csv").unwrap();
    
    // Mock state or use test TLogFS
    let state = create_test_state().await;
    
    let table = provider.table_provider(&url, &state).await.unwrap();
    
    // Query with DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("data", table).unwrap();
    let df = ctx.sql("SELECT * FROM data").await.unwrap();
    // ...
}
```

#### Phase 2: SQL-Derived Factory Integration

Current sql-derived factory:
```rust
SqlDerivedConfig {
    patterns: {"source": "/raw/data.parquet"},
    query: "SELECT * FROM source WHERE value > 10",
}
```

With provider URLs (HIGH-LEVEL API):
```rust
// Provider crate handles pattern capture → TableProvider pipeline
let provider = Provider::new();

// For each pattern in config:
let logs_url = ProviderUrl::parse("oteljson://zstd/logs/**/*.json.zstd")?;
let metrics_url = ProviderUrl::parse("csv:///metrics/**/*.csv")?;

// HIGH-LEVEL: Pattern → TableProvider in one call
let logs_table = provider.table_provider(&logs_url, &state).await?;
let metrics_table = provider.table_provider(&metrics_url, &state).await?;

// Register in DataFusion context
ctx.register_table("logs", logs_table)?;
ctx.register_table("metrics", metrics_table)?;

// Execute SQL query
let df = ctx.sql(&config.query).await?;

// Or use capture_pattern() for more control:
let logs_match = provider.capture_pattern(&logs_url, &state).await?;
println!("Found {} log files with schema: {:?}", 
         logs_match.files.len(), 
         logs_match.schema);
let logs_table = provider.create_table_provider(logs_match, &state).await?;
```

#### Phase 3: Direct CLI Integration

```bash
# Query CSV files directly (streaming format)
pond cat csv:///data/measurements.csv

# Query with SQL filter (streaming format with compression)
pond cat oteljson://zstd/logs/**/*.json.zstd \
  --sql "SELECT timestamp, severity, body WHERE severity = 'ERROR'"

# Combine streaming and Parquet formats
pond cat --sql "
  SELECT l.timestamp, m.value 
  FROM oteljson://zstd/logs/**/*.json.zstd l
  JOIN /metrics/data.parquet m
  ON l.trace_id = m.trace_id
"
# Note: Parquet uses plain path (no csv:// or parquet:// scheme)
#       Provider URLs only for streaming formats
```

## Implementation Plan

### Phase 1: Provider Crate Foundation (Week 1-2)

**Goal**: Working provider crate with CSV implementation

**Tasks**:
1. Create `crates/provider/` structure with dependencies:
   - `url` - URL parsing (standard url crate with validation)
   - `serde_qs` - Query string deserialization
   - `arrow`, `arrow-csv`, `arrow-schema` - CSV reading
   - `async-trait`, `tokio` - Async support
   - `async-compression` - Compression support (zstd, gzip)
2. Define core types (`ProviderUrl`, `FormatProvider`, `FormatRegistry`, `Provider`)
3. Implement URL parsing: wrap url::Url with validation (reject fragment, port, username, password)
4. Implement CSV provider using arrow-csv with CsvOptions
5. Implement compression wrapper for AsyncRead (zstd, gzip)
6. Write unit tests with in-memory data
7. Integration tests with mock TLogFS state

**Deliverables**:
- `crates/provider/src/lib.rs` - Core API (3 layers: decompress, FormatProvider, Provider)
- `crates/provider/src/url.rs` - ProviderUrl (wraps url::Url with validation)
- `crates/provider/src/compression.rs` - Layer 1: decompress() function (standalone utility)
- `crates/provider/src/format.rs` - Layer 2: FormatProvider, RecordBatchReader traits
- `crates/provider/src/csv.rs` - CSV provider with buffered schema inference
- `crates/provider/src/sync_bridge.rs` - AsyncToSyncReader for arrow_csv
- `crates/provider/src/table.rs` - Layer 3: Provider, PatternMatch, TableProvider creation
- `crates/provider/src/error.rs` - Error types
- `crates/provider/tests/compression_tests.rs` - Layer 1 tests (decompress any file type)
- `crates/provider/tests/csv_tests.rs` - Layer 2 tests (format decoding)
- `crates/provider/tests/table_tests.rs` - Layer 3 tests (TableProvider creation)

**Success Criteria**:
- Parse `csv:///path/file.csv?delimiter=;` URLs
- Parse `csv://gzip/path/file.csv.gz` URLs
- Query parameters deserialize to CsvOptions
- Convert CSV AsyncRead to RecordBatch stream with options
- Compression transparent to format providers
- Schema inference working
- Tests passing

### Phase 2: TLogFS Integration (Week 3)

**Goal**: Provider URLs work with actual TLogFS pattern expansion

**Tasks**:
1. Implement `Provider::expand_pattern()` using TinyFS glob
2. Handle multi-file union (multiple files → single TableProvider)
3. Integrate with TLogFS State for file access via AsyncRead
4. Ensure Parquet files still use existing `tinyfs://` URLs (no changes needed)

**Deliverables**:
- Pattern expansion implementation
- Multi-file TableProvider wrapper for streaming formats
- Integration tests with real TLogFS pond
- Documentation clarifying provider (streaming) vs tinyfs (random-access) URLs

**Success Criteria**:
- `csv:///data/**/*.csv` expands to all matching files
- `oteljson://zstd/logs/**/*.json.zstd` works with compression
- Multiple streaming files unioned correctly
- Parquet files still work with existing `tinyfs://` URLs
- Works with actual pond filesystem

### Phase 3: SQL-Derived Factory Upgrade (Week 4)

**Goal**: sql-derived factory uses provider crate's HIGH-LEVEL pattern capture API

**Tasks**:
1. Replace low-level TinyFS pattern matching with provider.capture_pattern()
2. For provider URLs (csv://, oteljson://): use provider.table_provider()
3. For plain paths: continue using existing TinyFS ObjectStore (Parquet)
4. Leverage PatternMatch structure for multi-file scenarios
5. Maintain backward compatibility

**Deliverables**:
- Updated `sql_derived.rs` with provider URL support
- Migration path for existing configurations
- Tests for provider URLs (streaming), plain paths (Parquet), and mixed

**Success Criteria**:
- Existing Parquet configs still work (plain paths → tinyfs://)
- CSV provider URLs work (csv:// → provider crate)
- OtelJson URLs work (oteljson:// → provider crate)
- Mixed queries work (CSV + Parquet in same SQL)
- No breaking changes to factory API

### Phase 4: Additional Formats (Week 5-6)

**Goal**: Implement OtelJson and your second format

**Tasks**:
1. Implement OtelJson provider
2. Implement second format provider
3. Document format provider implementation guide
4. Add provider registry CLI (`pond list-providers`)

**Deliverables**:
- `crates/provider/src/oteljson.rs`
- `crates/provider/src/FORMAT2.rs`
- `docs/provider-format-guide.md`
- CLI command for listing registered providers

#### Phase 5: CLI Integration (Week 7)

**Goal**: Use provider URLs directly in pond commands

**Tasks**:
1. Update `pond cat` to detect and handle provider URLs (streaming formats)
2. Keep existing `tinyfs://` path for Parquet (no changes)
3. Update `pond describe` for provider URL schema
4. Add examples to CLI help showing both URL types

**Deliverables**:
- Updated `crates/cmd/src/commands/cat.rs`
- Updated `crates/cmd/src/commands/describe.rs`
- CLI examples showing provider URLs (csv, oteljson) vs plain paths (parquet)
- Documentation on when to use each

### Phase 6: Derivative Factories (Week 8+)

**Goal**: Upgrade timeseries_join and timeseries_pivot

**Tasks**:
1. Update timeseries_join to use provider URLs
2. Update timeseries_pivot to use provider URLs
3. Verify all existing factory tests pass
4. Document migration patterns

## API Examples

### Layer 1: Standalone Decompression

```rust
use provider::decompress;

// Decompress any file type (config, binary, text, etc.)
let compressed = fs.open("/config/app.json.zst").await?;
let decompressed = decompress(compressed, Some("zstd"))?;

// Read the decompressed content
let mut content = String::new();
decompressed.read_to_string(&mut content).await?;
let config: AppConfig = serde_json::from_str(&content)?;
```

### Layer 2: Format Decoding

```rust
use provider::{CsvProvider, FormatProvider, decompress};

let csv_provider = CsvProvider;
let url = ProviderUrl::parse("csv:///data/file.csv?delimiter=;")?;

// Open file and optionally decompress
let file = fs.open("/data/file.csv.gz").await?;
let decompressed = decompress(file, Some("gzip"))?;

// Decode to RecordBatches
let (schema, mut batch_reader) = csv_provider
    .open_with_schema(decompressed, &url)
    .await?;

while let Some(batch) = batch_reader.next_batch()? {
    // Process Arrow RecordBatch
}
```

### Layer 3: Full TableProvider

```rust
use provider::{Provider, ProviderUrl, FormatRegistry};

// Register formats (typically done at startup)
let mut registry = FormatRegistry::global().write();
registry.register(Arc::new(CsvProvider));
registry.register(Arc::new(OtelJsonProvider));

// Create provider instance
let provider = Provider::new();

// Parse URL with query parameters
let url = ProviderUrl::parse("csv:///data/measurements.csv?delimiter=;&has_header=true")?;

// Get TableProvider (handles file opening, decompression, format decoding)
let table = provider
    .create_table_provider(&url, &fs)
    .await?;

// Use with DataFusion
let ctx = SessionContext::new();
ctx.register_table("measurements", table)?;
let df = ctx.sql("SELECT * FROM measurements WHERE temp > 25").await?;
```

### With Compression

```rust
// Compressed OtelJson file
let url = ProviderUrl::parse("oteljson://zstd/logs/traces.json.zstd?batch_size=2048")?;

// Provider handles decompression automatically based on url.compression()
let table = provider.create_table_provider(&url, &fs).await?;
```

### Pattern Matching (Multi-File)

```rust
// Pattern matching across multiple files
let url = ProviderUrl::parse("oteljson://zstd/logs/**/*.json.zstd?batch_size=1024")?;
let pattern_match = provider.capture_pattern(&url, &fs).await?;

println!("Found {} files matching pattern", pattern_match.files.len());
println!("Schema: {:?}", pattern_match.schema);
for file in &pattern_match.files {
    println!("  {}: {} bytes", file.path, file.metadata.size);
}

// Create TableProvider from captured pattern (advanced)
let table = provider.create_table_provider_from_match(pattern_match, &fs).await?;

// Or use the simple API (does capture + create internally)
let table = provider.create_table_provider(&url, &fs).await?;

// All files will be:
// - Opened via TinyFS FileSystem
// - Decompressed with zstd automatically (Layer 1)
// - Decoded with OtelJson format (Layer 2)
// - Unioned into single table with consistent schema (Layer 3)
// - Query parameters (batch_size=1024) applied to all
```

### Multi-Pattern SQL

```rust
// Multiple provider URLs in one query with format options
let config = SqlDerivedConfig {
    patterns: hashmap! {
        "logs" => "oteljson://zstd/logs/**/*.json.zstd?batch_size=2048",
        "metrics" => "csv:///metrics/data.csv?delimiter=,&has_header=true",
    },
    query: Some("
        SELECT 
            l.timestamp,
            l.trace_id,
            m.cpu_usage
        FROM logs l
        JOIN metrics m ON l.timestamp = m.timestamp
        WHERE l.severity = 'ERROR'
    ".into()),
    scope_prefixes: None,
};

let table = provider
    .table_provider_from_config(&config, &state)
    .await?;
```

## Design Decisions

### Why URL-Based?

**Pros**:
- Clear format indication (scheme)
- Familiar pattern (HTTP URLs, S3 URLs)
- Composable (can pass URLs as strings)
- Extensible (new schemes easily added)

**Cons**:
- URL parsing overhead (minimal)
- Scheme namespace management (document conventions)

**Decision**: URLs provide clarity and composability that outweigh minimal parsing cost.

### Why Separate Crate?

**Pros**:
- Clear dependency boundaries
- Can be used independently of TLogFS factories
- Easier to test in isolation
- Simpler mental model (one thing: format conversion)

**Cons**:
- Another crate to maintain
- Coordination needed for changes

**Decision**: Separation of concerns worth the extra crate.

### Why Not Replace Factories?

**Pros of keeping factories**:
- Existing configurations work
- Complex composition still useful (dynamic-dir)
- Gradual migration path

**Cons**:
- Two systems to maintain temporarily

**Decision**: Coexist during transition, evaluate removal later.

### Schema Handling

**Two approaches**:

1. **Infer from data** (CSV, JSON)
   - Read file, detect types
   - May be expensive for large files
   - Necessary for schemaless formats

2. **Fixed schema** (OtelJson)
   - Known schema defined by format spec
   - Fast, no inference needed
   - Better for well-defined formats

**Decision**: Support both via `schema_hint` parameter.

## Testing Strategy

### Unit Tests

```rust
// Test URL parsing
#[test]
fn test_url_parse() {
    let url = ProviderUrl::parse("csv:///data/file.csv?delimiter=;").unwrap();
    assert_eq!(url.scheme(), "csv");
    assert_eq!(url.path(), "/data/file.csv");
    assert_eq!(url.compression(), None);
}

#[test]
fn test_url_parse_with_compression() {
    let url = ProviderUrl::parse("oteljson://zstd/logs/file.json.zstd?batch_size=1024").unwrap();
    assert_eq!(url.scheme(), "oteljson");
    assert_eq!(url.compression(), Some("zstd"));
    assert_eq!(url.path(), "/logs/file.json.zstd");
}

#[test]
fn test_url_reject_fragment() {
    let result = ProviderUrl::parse("csv:///data/file.csv#fragment");
    assert!(result.is_err());
}

#[test]
fn test_url_reject_port() {
    let result = ProviderUrl::parse("csv://host:8080/data/file.csv");
    assert!(result.is_err());
}

#[test]
fn test_query_params() {
    let url = ProviderUrl::parse("csv:///file.csv?delimiter=;&has_header=false").unwrap();
    let options: CsvOptions = url.query_params().unwrap();
    assert_eq!(options.delimiter, ';');
    assert_eq!(options.has_header, false);
}

// Test format provider
#[tokio::test]
async fn test_csv_read() {
    let provider = CsvProvider;
    let data = "a,b,c\n1,2,3\n4,5,6\n";
    let reader = Cursor::new(data);
    let url = ProviderUrl::parse("csv:///test.csv").unwrap();
    let stream = provider.read_batches(Box::pin(reader), &url, None).await.unwrap();
    // Verify batches...
}

#[tokio::test]
async fn test_csv_with_options() {
    let provider = CsvProvider;
    let data = "a;b;c\n1;2;3\n";
    let reader = Cursor::new(data);
    let url = ProviderUrl::parse("csv:///test.csv?delimiter=;").unwrap();
    let stream = provider.read_batches(Box::pin(reader), &url, None).await.unwrap();
    // Verify delimiter honored...
}
```

### Integration Tests

```rust
// Test with real TLogFS
#[tokio::test]
async fn test_provider_with_tlogfs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ship = Ship::create_pond(temp_dir.path()).await.unwrap();
    
    // Write CSV file to pond
    let mut tx = ship.begin_write(&meta).await.unwrap();
    let root = tx.root();
    root.write_file("/data/test.csv", "a,b\n1,2\n").await.unwrap();
    tx.commit(None).await.unwrap();
    
    // Read via provider
    let provider = Provider::new();
    let url = ProviderUrl::parse("csv:///data/test.csv").unwrap();
    let table = provider.table_provider(&url, &state).await.unwrap();
    
    // Query
    // ...
}
```

### Performance Tests

```rust
#[tokio::test]
async fn bench_large_csv() {
    // 100MB CSV file
    // Measure: parse time, memory usage, batch throughput
}
```

## Migration Path

### For Users

**Phase 1** (Now): Existing factory configs work as-is

**Phase 2** (After provider integration): Optional provider URL support
```yaml
# Old style - still works
patterns: {"data": "/raw/file.parquet"}

# New style - also works
patterns: {"data": "parquet:///raw/file.parquet"}
```

**Phase 3** (Future): Direct URL usage
```bash
pond cat csv:///data/file.csv
```

### For Developers

**Adding new formats**:

1. Implement `FormatProvider` trait
2. Register in global registry
3. Write tests
4. Document format spec

**Migration checklist**:
- [ ] Core provider crate working
- [ ] CSV provider complete
- [ ] TLogFS integration tested
- [ ] sql-derived factory accepts URLs
- [ ] Backward compatibility verified
- [ ] Documentation updated
- [ ] Additional formats implemented
- [ ] CLI commands updated
- [ ] Derivative factories upgraded

## Design Decisions (Finalized)

### 1. Query Parameters - **DECIDED: Use serde_qs with strongly-typed structs**

Format-specific options passed via query parameters and deserialized:
```rust
// URL: csv:///data/file.csv?delimiter=;&has_header=false
#[derive(Deserialize)]
struct CsvOptions {
    delimiter: char,
    has_header: bool,
}
let options: CsvOptions = url.query_params()?;
```

Each FormatProvider defines its own options struct based on underlying library capabilities (e.g., arrow_csv::ReaderBuilder options).

### 2. Compression - **DECIDED: Use URL hostname as compression indicator**

Compression specified as "hostname" in URL:
```
oteljson://zstd/path/to/file.json.zstd
csv://gzip/data/file.csv.gz
parquet:///path/to/file.parquet  (no compression)
```

Compression handling layers:
1. ProviderUrl extracts compression from hostname
2. Provider wraps AsyncRead with decompression before format parsing
3. Supports: zstd, gzip, none (extensible)

### 3. Error Handling - **DECIDED: Fail fast on parse errors**

Pattern matching (TinyFS) is separate from file reading (FormatProvider):
1. Pattern expansion finds files (may find 0, that's OK)
2. Reading each file can fail (parse error, I/O error)
3. Errors propagate up, transaction aborts
4. Working as intended - no special handling needed

### 4. Schema Inference with Buffering - **DECIDED: Single-pass with open_with_schema()**

Avoid double-reading by combining schema inference and reading:
- `open_with_schema()` returns both schema AND positioned reader
- CSV: Infer schema from header+sample, continue reading from buffer
- OtelJson: Known schema, start reading immediately
- DataFusion TableProvider gets schema without re-opening file

Implementation:
```rust
// Provider opens file once
let (schema, reader) = provider.open_with_schema(file, url).await?;

// DataFusion uses schema for metadata
let table = StreamingTable::new(schema.clone());

// Then scans using the same reader (already positioned, buffered)
while let Some(batch) = reader.next_batch()? {
    // Process batch
}
```

Key benefit: Compressed files only decompressed once, buffered during schema inference.

### 5. Memory Management

Handled by existing DataFusion streaming:
- Configurable batch_size in format options
- RecordBatchStream yields incrementally
- No full-file loads required

## Success Metrics

- [ ] CSV provider converts files to RecordBatches correctly
- [ ] Pattern expansion finds all matching files
- [ ] Multi-file union produces correct schema
- [ ] SQL queries over provider URLs work
- [ ] Existing sql-derived configs still work
- [ ] Performance comparable to current system
- [ ] OtelJson format working with real data
- [ ] Documentation complete and clear

## Next Steps

1. **Review this plan** - Get feedback on architecture
2. **Create crates/provider skeleton** - Basic structure
3. **Implement core types** - ProviderUrl, FormatProvider, Registry
4. **Build CSV provider** - First concrete implementation
5. **Write tests** - Prove it works in isolation
6. **Plan TLogFS integration** - Design State interaction

---

**Document Status**: Draft for review  
**Last Updated**: November 15, 2025  
**Author**: System analysis based on codebase review
