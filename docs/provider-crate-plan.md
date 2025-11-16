# Provider Crate: URL-Based File Format Abstraction

## Executive Summary

The `provider` crate will create a URL-based abstraction layer for accessing files in TLogFS with automatic format conversion. It simplifies the current factory system by using URL schemes to indicate format converters, enabling straightforward data pipeline configuration.

**Core Concept**: `scheme://path/pattern` where:
- `scheme` = format converter (e.g., `oteljson`, `csv`, `parquet`)
- `path/pattern` = TinyFS pattern expansion (e.g., `/data/**/*.json`)

## Current System Analysis

### Existing Architecture

```rust
// Current: Complex factory configuration
SqlDerivedConfig {
    patterns: {"source": "/raw/data.parquet"},
    query: "SELECT * FROM source WHERE value > 10",
}

// Current: TinyFS ObjectStore with fixed parquet format
TinyFsObjectStore::new(state)
// Hard-coded to read Parquet via tinyfs:// URLs
```

### Key Components Today

1. **TinyFS ObjectStore** (`tinyfs_object_store.rs`)
   - Implements `object_store::ObjectStore` trait
   - Path format: `tinyfs:///part/{part_id}/node/{node_id}/version/{version}.parquet`
   - Hard-coded to Parquet format via DataFusion's `ParquetFormat`
   - Used by ListingTable for file-based queries

2. **SQL-Derived Factory** (`sql_derived.rs`)
   - Pattern expansion via TinyFS
   - Creates ListingTable with TinyFS ObjectStore
   - Executes SQL queries over discovered files
   - Basis for timeseries_join and timeseries_pivot

3. **Factory System** (`factory.rs`)
   - YAML configuration with patterns and queries
   - Complex composition via dynamic-dir
   - Runtime registration via `linkme`

## Provider Crate Design

### Goals

1. **Simplify format conversion**: URL scheme specifies converter
2. **Preserve pattern expansion**: Leverage existing TinyFS glob matching
3. **Maintain streaming**: Arrow RecordBatch streaming for large datasets
4. **Enable composition**: Build on provider URLs in higher-level APIs
5. **Backward compatibility**: Coexist with existing factory system during migration

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
│  pond cat oteljson:///logs/**/*.json --sql "SELECT ..."    │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                  Provider Crate                             │
│  • URL parsing (scheme + path/pattern)                     │
│  • Format registry (scheme → FormatProvider)               │
│  • Pattern expansion delegation to TinyFS                  │
│  • RecordBatch streaming via format providers              │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┴─────────────┐
        │                          │
┌───────▼──────┐          ┌────────▼─────────┐
│ TinyFS       │          │ Format Providers │
│ • Glob       │          │ • oteljson       │
│ • File access│          │ • csv            │
└──────────────┘          │ • Custom formats │
                          └──────────────────┘
```

### Core Types

```rust
// crates/provider/src/lib.rs

/// URL for accessing files with format conversion
/// Format: scheme://[compression]/path/pattern?query_params
/// Examples:
///   - csv:///data/file.csv?delimiter=;
///   - oteljson://zstd/logs/file.json.zstd
///   - parquet:///measurements/**/*.parquet
#[derive(Debug, Clone)]
pub struct ProviderUrl {
    /// Format scheme (e.g., "oteljson", "csv", "parquet")
    pub scheme: String,
    /// Optional compression (e.g., "zstd", "gzip") - from URL "hostname"
    pub compression: Option<String>,
    /// TinyFS path or pattern (e.g., "/logs/**/*.json")
    pub path: String,
    /// Query parameters as string for format-specific parsing
    pub query: Option<String>,
}

impl ProviderUrl {
    /// Parse from string: "scheme://[compression]/path/pattern?query"
    /// Uses url crate for parsing, extracts compression from hostname
    pub fn parse(url: &str) -> Result<Self, ProviderError>;
    
    /// Create from components
    pub fn new(
        scheme: impl Into<String>, 
        compression: Option<String>,
        path: impl Into<String>,
        query: Option<String>,
    ) -> Self;
    
    /// Parse query parameters into strongly-typed struct using serde_qs
    pub fn query_params<T: serde::de::DeserializeOwned>(&self) -> Result<T, ProviderError>;
}

/// Format provider trait - converts file data to Arrow RecordBatches
/// For STREAMING formats only (CSV, JSON, etc.) - formats that don't require seek
/// Parquet and other random-access formats use existing TinyFS ObjectStore path
#[async_trait]
pub trait FormatProvider: Send + Sync {
    /// Provider name (matches URL scheme)
    fn name(&self) -> &str;
    
    /// Convert async reader to RecordBatch stream
    /// Query parameters should be parsed from url.query_params() by implementation
    /// Reader is AsyncRead only - no seek required for streaming formats
    async fn read_batches(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
        schema_hint: Option<SchemaRef>,
    ) -> Result<RecordBatchStream, ProviderError>;
    
    /// Infer schema from file (for schema discovery)
    /// Query parameters should be parsed from url.query_params() by implementation
    /// For streaming formats, this reads the beginning of the stream
    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<SchemaRef, ProviderError>;
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

/// Main provider API - converts ProviderUrl to DataFusion TableProvider
pub struct Provider {
    registry: Arc<RwLock<FormatRegistry>>,
}

impl Provider {
    /// Create TableProvider from URL
    /// Handles pattern expansion, multi-file union, format conversion
    pub async fn table_provider(
        &self,
        url: &ProviderUrl,
        state: &tlogfs::persistence::State,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;
    
    /// Create TableProvider with SQL transformation
    pub async fn table_provider_with_sql(
        &self,
        url: &ProviderUrl,
        sql: &str,
        state: &tlogfs::persistence::State,
    ) -> Result<Arc<dyn TableProvider>, ProviderError>;
    
    /// Expand URL pattern to list of files
    pub async fn expand_pattern(
        &self,
        url: &ProviderUrl,
        state: &tlogfs::persistence::State,
    ) -> Result<Vec<ResolvedFile>, ProviderError>;
}

/// Resolved file after pattern expansion
pub struct ResolvedFile {
    pub node_id: tinyfs::NodeID,
    pub part_id: tinyfs::NodeID,
    pub path: String,
    pub metadata: tinyfs::NodeMetadata,
}
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
    
    async fn read_batches(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
        schema_hint: Option<SchemaRef>,
    ) -> Result<RecordBatchStream, ProviderError> {
        // Parse options from URL query parameters
        let options: CsvOptions = url.query_params()
            .unwrap_or_default();
        
        // Build arrow_csv ReaderBuilder with options
        let mut builder = ReaderBuilder::new(Arc::new(schema_hint.unwrap_or_else(|| {
            // Infer schema if not provided
            // Will need to peek at reader first
            todo!("schema inference")
        })))
            .with_delimiter(options.delimiter as u8)
            .with_header(options.has_header)
            .with_batch_size(options.batch_size);
        
        if let Some(escape) = options.escape {
            builder = builder.with_escape(escape as u8);
        }
        
        // Convert AsyncRead to sync reader (requires buffering)
        // arrow_csv expects sync Read trait
        // Use tokio::io::BufReader and sync wrapper
        todo!("async to sync conversion + create stream")
    }
    
    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<SchemaRef, ProviderError> {
        let options: CsvOptions = url.query_params()
            .unwrap_or_default();
        
        // Use arrow_csv::reader::infer_schema
        // Need to read sample of file
        todo!("use arrow_csv::reader::infer_file_schema")
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
    
    async fn read_batches(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
        schema_hint: Option<SchemaRef>,
    ) -> Result<RecordBatchStream, ProviderError> {
        let options: OtelJsonOptions = url.query_params()
            .unwrap_or_default();
        
        // Parse JSON lines
        // Convert to Arrow records matching OTEL schema
        // Yield RecordBatch stream
        todo!()
    }
    
    async fn infer_schema(
        &self,
        reader: Pin<Box<dyn AsyncRead>>,
        url: &ProviderUrl,
    ) -> Result<SchemaRef, ProviderError> {
        // OtelJson has well-defined schema from spec
        // Return fixed schema (reader not needed for fixed schemas)
        Ok(Arc::new(create_otel_schema()))
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

With provider URLs:
```rust
SqlDerivedConfig {
    // NEW: Support provider URLs in patterns
    patterns: {"source": "csv:///raw/data.csv"},
    query: "SELECT * FROM source WHERE value > 10",
}

// Or for OTEL JSON:
SqlDerivedConfig {
    patterns: {
        "logs": "oteljson:///logs/**/*.json",
        "metrics": "oteljson:///metrics/**/*.json",
    },
    query: "SELECT * FROM logs JOIN metrics ON logs.trace_id = metrics.trace_id",
}
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
   - `url` - URL parsing
   - `serde_qs` - Query string deserialization
   - `arrow`, `arrow-csv`, `arrow-schema` - CSV reading
   - `async-trait`, `tokio` - Async support
   - `async-compression` - Compression support (zstd, gzip)
2. Define core types (`ProviderUrl`, `FormatProvider`, `FormatRegistry`, `Provider`)
3. Implement URL parsing with compression extraction from hostname
4. Implement CSV provider using arrow-csv with CsvOptions
5. Implement compression wrapper for AsyncRead (zstd, gzip)
6. Write unit tests with in-memory data
7. Integration tests with mock TLogFS state

**Deliverables**:
- `crates/provider/src/lib.rs` - Core API
- `crates/provider/src/url.rs` - ProviderUrl with compression
- `crates/provider/src/csv.rs` - CSV provider with options
- `crates/provider/src/compression.rs` - Decompression wrapper
- `crates/provider/src/error.rs` - Error types
- `crates/provider/tests/csv_tests.rs` - Test suite
- `crates/provider/tests/compression_tests.rs` - Compression tests

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

**Goal**: sql-derived factory accepts provider URLs for streaming formats

**Tasks**:
1. Detect provider URL (has scheme://) vs plain path in patterns
2. Route provider URLs (csv://, oteljson://) through provider crate
3. Route plain paths and tinyfs:// through existing TinyFS ObjectStore (Parquet)
4. Maintain backward compatibility

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

### Basic Usage

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

// Get TableProvider
let table = provider
    .table_provider(&url, &state)
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

// Provider handles decompression automatically based on url.compression
let table = provider.table_provider(&url, &state).await?;
```

### Pattern Expansion

```rust
// Expand glob pattern with compression
let url = ProviderUrl::parse("oteljson://zstd/logs/**/*.json.zstd?batch_size=1024")?;
let files = provider.expand_pattern(&url, &state).await?;

println!("Found {} files", files.len());
for file in files {
    println!("  {}: {} bytes", file.path, file.metadata.size);
}

// All files will be decompressed with zstd during reading
// Query parameters apply to all matched files
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
    assert_eq!(url.scheme, "csv");
    assert_eq!(url.path, "/data/file.csv");
    assert_eq!(url.compression, None);
    assert!(url.query.is_some());
}

#[test]
fn test_url_parse_with_compression() {
    let url = ProviderUrl::parse("oteljson://zstd/logs/file.json.zstd?batch_size=1024").unwrap();
    assert_eq!(url.scheme, "oteljson");
    assert_eq!(url.compression, Some("zstd".to_string()));
    assert_eq!(url.path, "/logs/file.json.zstd");
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

### 4. Schema Caching - **DECIDED: Not needed initially**

Schema inference cost is acceptable:
- CSV: Quick peek at first N rows
- OtelJson: Fixed schema from spec (no inference)
- Parquet: Schema in metadata (fast)

Future optimization if needed, but not a priority.

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
