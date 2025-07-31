# Dynamic File Support Integration Plan

**Project**: DuckPond TLogFS Dynamic File Support  
**Created**: July 25, 2025  
**Status**: Planning Phase Complete - Ready for Implementation  
**Priority**: Medium - Advanced feature with solid foundation  

## 📋 **Executive Summary**

This document outlines a minimum viable plan to integrate dynamic file support into TLogFS, focusing on a single dynamic node type: the hostmount dynamic directory. The initial implementation will provide a CLI `mknod` command for creating a dynamic directory that mounts a host directory into the pond, managed by a simple `hostmount` factory. Advanced dynamic nodes for translating CSV and SQL queries are deferred for future work.

## 🔍 **Research Findings**

The original duckpond implementation and TinyFS provide a foundation for dynamic file integration. For the minimum viable solution, we will leverage the existing TinyFS `Directory` trait and TLogFS `OplogEntry` schema to support a single dynamic directory type: hostmount. This type will expose a host directory as a read-only dynamic directory in the pond, with configuration stored as metadata.

## 🏗️ **Architecture Design**

### **Schema Extension**
Add a `factory` string column to the OplogEntry schema to distinguish between static and dynamic content. For hostmount dynamic directories, the factory will be set to `hostmount` and the content field will store the configuration metadata (YAML or JSON encoded).

### **Hostmount Dynamic Directory**
Implement a single dynamic directory type, `hostmount`, which exposes a host directory as a read-only dynamic directory in the pond. The configuration consists of a single field:
```yaml
directory: /host/directory/path
```
This configuration is stored in the content field of the OplogEntry.

### **CLI Integration**
Add a `mknod` command to the pond CLI:
```
pond mknod --factory hostmount PATH CONFIG
```
Where `PATH` is the target path in the pond, and `CONFIG` is a YAML file containing the configuration above. The CLI will validate the config and create a dynamic directory entry in the pond, managed by the hostmount factory.

### **Behavior**
- The dynamic directory is strictly read-only and reflects the current state of the host directory.
- No mutation operations are permitted through TinyFS.
- Node names in TinyFS correspond to the names of the entries in the host directory.
- Node IDs for these entries are ephemeral and scoped to the dynamic directory context; they are not persisted in the Oplog.
- The directory is refreshed on each access, reflecting the current state of the host directory.
pub struct SqlDerivedSeriesFactory {
    datafusion_ctx: Arc<SessionContext>,
    tinyfs_root: Arc<WD>,
}

#[derive(Serialize, Deserialize)]
pub struct SqlDerivedSeriesMetadata {
    source_series_path: String,   // Path to the source file:series
    sql_query: String,            // SQL query to transform the data
    refresh_interval: Option<Duration>, // Optional refresh interval
}

impl DynamicNodeFactory for SqlDerivedSeriesFactory {
    type Metadata = SqlDerivedSeriesMetadata;
    
    fn factory_type(&self) -> &'static str { "sql_derived_series" }
    
    fn entry_type(&self) -> EntryType { EntryType::FileSeries }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Register the source file:series as a table in DataFusion
        let table_name = "source_series";
        let series_provider = SeriesTableProvider::new(
            &self.tinyfs_root, 
            &metadata.source_series_path
        ).await?;
        
        self.datafusion_ctx.register_table(table_name, Arc::new(series_provider))?;
        
        // Execute the SQL query
        let df = self.datafusion_ctx.sql(&metadata.sql_query).await?;
        let batches = df.collect().await?;
        
        // Convert result to Parquet format (same as regular file:series)
        let mut buffer = Vec::new();
        let schema = batches[0].schema();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;
        
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        
        Ok(buffer)
    }
    
    fn validate_metadata(&self, metadata: &Self::Metadata) -> Result<()> {
        // Validate that source path exists and is a file:series
        let source_exists = self.tinyfs_root.path_exists(&metadata.source_series_path).await?;
        if !source_exists {
            return Err(TLogFSError::SourceSeriesNotFound(metadata.source_series_path.clone()));
        }
        
        // Basic SQL syntax validation could be added here
        Ok(())
    }
}
```

#### **Example Usage Scenarios**

**1. Simple Aggregation**:
```rust
// Create a daily summary series from hourly data
let metadata = SqlDerivedSeriesMetadata {
    source_series_path: "/data/hourly_sensors.series".to_string(),
    sql_query: "SELECT DATE(timestamp) as date, AVG(temperature) as avg_temp, MAX(humidity) as max_humidity FROM source_series GROUP BY DATE(timestamp)".to_string(),
    refresh_interval: Some(Duration::from_hours(1)),
};
```

**2. Filtered Views**:
```rust
// Create a high-priority alerts series from all events
let metadata = SqlDerivedSeriesMetadata {
    source_series_path: "/logs/all_events.series".to_string(),
    sql_query: "SELECT * FROM source_series WHERE severity = 'HIGH' AND timestamp > NOW() - INTERVAL '24 hours'".to_string(),
    refresh_interval: Some(Duration::from_minutes(5)),
};
```

**3. Recursive Derivation**:
```rust
// Create a trending alerts series from the filtered alerts series
let metadata = SqlDerivedSeriesMetadata {
    source_series_path: "/logs/high_priority_alerts.series".to_string(), // This is itself dynamic!
    sql_query: "SELECT hour, COUNT(*) as alert_count FROM (SELECT HOUR(timestamp) as hour FROM source_series) GROUP BY hour ORDER BY alert_count DESC".to_string(),
    refresh_interval: Some(Duration::from_minutes(10)),
};
```

#### **DataFusion Integration Benefits**

**Predicate Pushdown**: DataFusion can push predicates down through the chain:
- Query on derived series: `SELECT * FROM trending_alerts WHERE alert_count > 10`
- Pushes down to: high_priority_alerts.series (dynamic)
- Which pushes down to: all_events.series (source)
- Uses TLogFS temporal metadata and Parquet statistics for efficient filtering

**Recursive Resolution**: The factory system handles recursive dependencies:
```
trending_alerts.series (dynamic) 
    → high_priority_alerts.series (dynamic)
        → all_events.series (static source)
```

**Transparent Caching**: Each level can be cached independently with appropriate refresh intervals.

#### **CsvDirectoryFactory**
Create dynamic directories that discover CSV files and present them as converted Parquet file:table entries:

```rust
pub struct CsvDirectoryFactory {
    tinyfs_root: Arc<WD>,
    materialization_cache: Arc<MaterializationCache>,
}

#[derive(Serialize, Deserialize)]
pub struct CsvDirectoryMetadata {
    source_pattern: String,        // Glob pattern like "/data/imports/*.csv" 
    target_schema: Option<String>, // Optional schema override for CSV parsing
    target_type: EntryType,        // FileTable or FileSeries for converted files
    temporal_column: Option<String>, // For FileSeries: which column contains timestamps
    cache_ttl: Option<Duration>,   // How long to cache materialized Parquet files
}

impl DynamicNodeFactory for CsvDirectoryFactory {
    type Metadata = CsvDirectoryMetadata;
    
    fn factory_type(&self) -> &'static str { "csv_directory" }
    
    fn entry_type(&self) -> EntryType { EntryType::Directory }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // This factory creates a directory, so content is IPC-encoded directory listing
        let csv_files = self.discover_csv_files(&metadata.source_pattern).await?;
        
        let mut directory_entries = Vec::new();
        for csv_path in csv_files {
            // Create transient dynamic file entries for each CSV
            let file_extension = match metadata.target_type {
                EntryType::FileSeries => ".series",
                EntryType::FileTable => ".table",
                _ => ".table", // Default to table
            };
            let file_name = csv_path.file_stem().unwrap().to_string() + file_extension;
            
            let csv_metadata = CsvToParquetMetadata {
                source_csv_path: csv_path.to_string(),
                target_schema: metadata.target_schema.clone(),
                target_type: Some(metadata.target_type.clone()),
                temporal_column: metadata.temporal_column.clone(),
            };
            
            // Create a transient dynamic file entry (not persisted to TLogFS)
            let dynamic_entry = TransientDynamicEntry {
                factory_type: "csv_to_parquet".to_string(),
                metadata: serde_json::to_vec(&csv_metadata)?,
                entry_type: metadata.target_type.clone(),
            };
            
            directory_entries.push((file_name, dynamic_entry));
        }
        
        // Encode directory listing as IPC (same format as regular directories)
        let directory_ipc = encode_directory_listing(directory_entries)?;
        Ok(directory_ipc)
    }
    
    async fn discover_csv_files(&self, pattern: &str) -> Result<Vec<PathBuf>> {
        // Use TinyFS path discovery to find matching CSV files
        let glob_pattern = glob::Pattern::new(pattern)?;
        let mut csv_files = Vec::new();
        
        // Walk the filesystem looking for matching CSV files
        self.tinyfs_root.walk_paths(|path| {
            if glob_pattern.matches_path(path) {
                // Verify it's actually a file:data entry with CSV content
                if let Some(entry) = self.tinyfs_root.get_entry(path).await? {
                    if entry.factory == "tlogfs" && 
                       entry.entry_type == EntryType::File &&
                       path.extension() == Some("csv") {
                        csv_files.push(path.to_path_buf());
                    }
                }
            }
            Ok(())
        }).await?;
        
        Ok(csv_files)
    }
}

/// Factory for individual CSV-to-Parquet conversion (transient, not persisted)
pub struct CsvToParquetFactory {
    tinyfs_root: Arc<WD>,
    materialization_cache: Arc<MaterializationCache>,
}

#[derive(Serialize, Deserialize)]
pub struct CsvToParquetMetadata {
    source_csv_path: String,
    target_schema: Option<String>,
    target_type: Option<EntryType>,    // FileTable or FileSeries
    temporal_column: Option<String>,   // For FileSeries: which column contains timestamps
}

impl DynamicNodeFactory for CsvToParquetFactory {
    type Metadata = CsvToParquetMetadata;
    
    fn factory_type(&self) -> &'static str { "csv_to_parquet" }
    
    fn entry_type(&self) -> EntryType { 
        // Can be either FileTable or FileSeries based on metadata configuration
        match &self.metadata.target_type {
            Some(target) => target.clone(),
            None => EntryType::FileTable, // Default to table
        }
    }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Check materialization cache first - include temporal considerations
        let cache_key = self.build_cache_key(metadata);
        if let Some(cached_parquet) = self.materialization_cache.get(&cache_key).await? {
            return Ok(cached_parquet);
        }
        
        // Load source CSV file:data
        let csv_content = self.tinyfs_root.read_file_content(&metadata.source_csv_path).await?;
        let csv_reader = Cursor::new(csv_content);
        
        // Convert CSV to Arrow RecordBatch using existing CLI logic
        let schema = if let Some(schema_json) = &metadata.target_schema {
            serde_json::from_str(schema_json)?
        } else {
            // Infer schema from CSV headers
            infer_csv_schema(csv_reader.clone())?
        };
        
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_reader);
            
        let record_batches = csv_to_arrow_batches(&mut csv_reader, &schema)?;
        
        // For FileSeries, add temporal metadata to Parquet file
        let parquet_buffer = if metadata.target_type == Some(EntryType::FileSeries) {
            self.create_series_parquet(record_batches, &schema, metadata).await?
        } else {
            self.create_table_parquet(record_batches, &schema).await?
        };
        
        // Cache the result with appropriate TTL based on type
        let cache_ttl = self.determine_cache_ttl(metadata);
        self.materialization_cache.put(cache_key, parquet_buffer.clone(), cache_ttl).await?;
        
        Ok(parquet_buffer)
    }
    
    fn build_cache_key(&self, metadata: &CsvToParquetMetadata) -> String {
        // For FileSeries, include temporal information in cache key
        match &metadata.target_type {
            Some(EntryType::FileSeries) => {
                // Include source file modification time and temporal column info
                format!("csv_to_series:{}:{}:{}", 
                    metadata.source_csv_path,
                    metadata.temporal_column.as_ref().unwrap_or(&"timestamp".to_string()),
                    self.get_source_mtime(&metadata.source_csv_path).unwrap_or(0)
                )
            },
            _ => {
                // Standard cache key for FileTable
                format!("csv_to_table:{}", metadata.source_csv_path)
            }
        }
    }
    
    async fn create_series_parquet(&self, batches: Vec<RecordBatch>, schema: &Schema, metadata: &CsvToParquetMetadata) -> Result<Vec<u8>> {
        // For FileSeries, we need to add temporal metadata and ensure proper partitioning
        let temporal_col = metadata.temporal_column.as_ref().unwrap_or(&"timestamp".to_string());
        
        // Validate temporal column exists
        if schema.field_with_name(temporal_col).is_err() {
            return Err(TLogFSError::TemporalColumnMissing(temporal_col.clone()));
        }
        
        // Create SeriesWriter with temporal metadata (same logic as existing file:series creation)
        let mut parquet_buffer = Vec::new();
        let mut series_writer = SeriesWriter::try_new(&mut parquet_buffer, schema.clone())?;
        
        // Add temporal range metadata to Parquet file metadata
        if let Some((min_time, max_time)) = self.extract_temporal_range(&batches, temporal_col)? {
            series_writer.add_temporal_metadata(min_time, max_time)?;
        }
        
        for batch in batches {
            series_writer.write(&batch)?;
        }
        series_writer.close()?;
        
        Ok(parquet_buffer)
    }
    
    async fn create_table_parquet(&self, batches: Vec<RecordBatch>, schema: &Schema) -> Result<Vec<u8>> {
        // Standard Parquet creation for FileTable (existing logic)
        let mut parquet_buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_buffer, schema.clone(), None)?;
        
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        
        Ok(parquet_buffer)
    }
    
    fn determine_cache_ttl(&self, metadata: &CsvToParquetMetadata) -> Duration {
        match &metadata.target_type {
            Some(EntryType::FileSeries) => {
                // FileSeries cache more conservatively due to temporal sensitivity
                Duration::from_hours(6)
            },
            _ => {
                // FileTable can cache longer as they're typically more static
                Duration::from_hours(24)
            }
        }
    }
}

/// Materialization cache for dynamic files that can't use predicate pushdown
/// Handles both FileTable and FileSeries with temporal considerations
pub struct MaterializationCache {
    cache_dir: PathBuf,
    memory_cache: Arc<RwLock<HashMap<String, (Vec<u8>, Instant)>>>,
    temporal_index: Arc<RwLock<HashMap<String, TemporalRange>>>, // Track time ranges for series
}

#[derive(Clone)]
pub struct TemporalRange {
    min_timestamp: i64,
    max_timestamp: i64,
    cached_at: Instant,
}

impl MaterializationCache {
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // For FileSeries cache keys, check temporal validity
        if key.contains("csv_to_series:") {
            if let Some(temporal_range) = self.get_temporal_range(key).await? {
                // Check if cached data is still temporally valid
                if !self.is_temporally_valid(&temporal_range) {
                    return Ok(None); // Force regeneration
                }
            }
        }
        
        // Check memory cache first
        {
            let cache = self.memory_cache.read().await;
            if let Some((data, timestamp)) = cache.get(key) {
                if timestamp.elapsed() < Duration::from_minutes(30) {
                    return Ok(Some(data.clone()));
                }
            }
        }
        
        // Check disk cache
        let cache_path = self.cache_dir.join(format!("{}.parquet", blake3::hash(key.as_bytes())));
        if cache_path.exists() {
            let data = tokio::fs::read(&cache_path).await?;
            
            // Update memory cache
            let mut cache = self.memory_cache.write().await;
            cache.insert(key.to_string(), (data.clone(), Instant::now()));
            
            return Ok(Some(data));
        }
        
        Ok(None)
    }
    
    pub async fn put(&self, key: String, data: Vec<u8>, ttl: Duration) -> Result<()> {
        // For FileSeries, extract and store temporal range information
        if key.contains("csv_to_series:") {
            if let Ok(temporal_range) = self.extract_parquet_temporal_range(&data) {
                let mut temporal_index = self.temporal_index.write().await;
                temporal_index.insert(key.clone(), temporal_range);
            }
        }
        
        // Store in memory cache
        {
            let mut cache = self.memory_cache.write().await;
            cache.insert(key.clone(), (data.clone(), Instant::now()));
        }
        
        // Store in disk cache
        let cache_path = self.cache_dir.join(format!("{}.parquet", blake3::hash(key.as_bytes())));
        tokio::fs::write(&cache_path, &data).await?;
        
        Ok(())
    }
    
    async fn get_temporal_range(&self, key: &str) -> Result<Option<TemporalRange>> {
        let temporal_index = self.temporal_index.read().await;
        Ok(temporal_index.get(key).cloned())
    }
    
    fn is_temporally_valid(&self, range: &TemporalRange) -> bool {
        // For FileSeries, consider cache invalid if:
        // 1. It's too old (cached_at + some duration)
        // 2. The temporal range might have changed (more conservative TTL)
        range.cached_at.elapsed() < Duration::from_hours(2)
    }
    
    fn extract_parquet_temporal_range(&self, parquet_data: &[u8]) -> Result<TemporalRange> {
        // Read temporal metadata from Parquet file footer
        // This would use the same logic as SeriesTableProvider to read temporal range
        let reader = SerializedFileReader::new(Cursor::new(parquet_data))?;
        let metadata = reader.metadata();
        
        // Extract temporal range from custom metadata (added by SeriesWriter)
        let key_value_metadata = metadata.file_metadata().key_value_metadata();
        if let Some(kv_metadata) = key_value_metadata {
            for kv in kv_metadata {
                if kv.key == "temporal_range_min" {
                    let min_timestamp = kv.value.as_ref().unwrap().parse::<i64>()?;
                    // Find corresponding max...
                    for kv2 in kv_metadata {
                        if kv2.key == "temporal_range_max" {
                            let max_timestamp = kv2.value.as_ref().unwrap().parse::<i64>()?;
                            return Ok(TemporalRange {
                                min_timestamp,
                                max_timestamp,
                                cached_at: Instant::now(),
                            });
                        }
                    }
                }
            }
        }
        
        Err(TLogFSError::TemporalMetadataMissing)
    }
}
```

#### **Dynamic Directory Architecture**

**Key Architectural Elements**:

1. **Transient Dynamic Entries**: CsvDirectoryFactory creates dynamic file entries that exist only in memory, not persisted to TLogFS
2. **Pattern-Based Discovery**: Uses glob patterns to discover source CSV files at access time
3. **Materialization Caching**: Since CSV files don't support predicate pushdown, cache converted Parquet for performance
4. **Factory Composition**: One factory creates directories containing files from another factory
5. **Type Conversion**: Converts file:data (CSV) to file:table (Parquet) transparently

**Usage Example**:
```rust
// Create dynamic directory that converts CSV files to FileSeries
let csv_series_metadata = CsvDirectoryMetadata {
    source_pattern: "/sensor_logs/*.csv".to_string(),
    target_schema: None, // Auto-infer schema
    target_type: EntryType::FileSeries,
    temporal_column: Some("timestamp".to_string()),
    cache_ttl: Some(Duration::from_hours(6)), // Shorter TTL for temporal data
};

// Create dynamic directory that converts CSV files to FileTable  
let csv_table_metadata = CsvDirectoryMetadata {
    source_pattern: "/imports/*.csv".to_string(),
    target_schema: None, // Auto-infer schema
    target_type: EntryType::FileTable,
    temporal_column: None, // No temporal column for tables
    cache_ttl: Some(Duration::from_hours(12)),
};

// Store the dynamic directories (only these entries persisted)
pond.create_dynamic_directory("/processed/sensor_series", "csv_directory", &csv_series_metadata).await?;
pond.create_dynamic_directory("/processed/csv_tables", "csv_directory", &csv_table_metadata).await?;

// When accessed, directories dynamically contain:
// /processed/sensor_series/temperature.series (from /sensor_logs/temperature.csv)
// /processed/csv_tables/sales_data.table (from /imports/sales_data.csv)

// SQL queries work transparently with temporal awareness for series:
pond.sql("SELECT * FROM '/processed/sensor_series/temperature.series' WHERE timestamp > '2025-01-01'").await?;
pond.sql("SELECT * FROM '/processed/csv_tables/sales_data.table' WHERE amount > 1000").await?;
```

#### **Performance Characteristics**

**No Predicate Pushdown**: CSV files can't benefit from predicate pushdown like Parquet, so:
- First access materializes full CSV to Parquet and caches result
- Subsequent accesses use cached Parquet with full predicate pushdown capability
- Cache provides best of both worlds: preserve original CSV, get Parquet performance

**Temporal Considerations for FileSeries**:
- **Temporal Metadata**: FileSeries cache entries include temporal range metadata
- **Cache Invalidation**: More conservative TTL for temporal data due to time-sensitive nature
- **Temporal Range Tracking**: Cache tracks min/max timestamps for efficient temporal queries
- **Schema Validation**: Ensures temporal column exists and is properly typed

**Cache Strategy**:
- **Memory Cache**: Recent conversions kept in memory for immediate access
- **Disk Cache**: Longer-term storage for frequently accessed conversions
- **Temporal Index**: Separate index tracking temporal ranges for FileSeries
- **TTL Management**: Different cache lifetimes for FileTable (24h) vs FileSeries (6h)
- **Cache Invalidation**: Source CSV modification time and temporal validity checks

#### **CLI Integration Example**

```bash
# Create a derived series using the CLI
pond create-derived-series "/analytics/daily_summary.series" \
  --source="/data/hourly_sensors.series" \
  --query="SELECT DATE(timestamp) as date, AVG(temperature) as avg_temp FROM source_series GROUP BY DATE(timestamp)" \
  --refresh="1h"

# Create a dynamic CSV directory for time series data
pond create-csv-directory "/processed/sensor_series" \
  --pattern="/sensor_logs/*.csv" \
  --type="series" \
  --temporal-column="timestamp" \
  --cache-ttl="6h"

# Create a dynamic CSV directory for table data
pond create-csv-directory "/processed/csv_tables" \
  --pattern="/raw_data/*.csv" \
  --type="table" \
  --cache-ttl="12h"

# Query the derived series - looks exactly like a regular file:series
pond cat "/analytics/daily_summary.series" --sql="SELECT * WHERE avg_temp > 25.0"

# Query converted CSV files - works for both tables and series
pond cat "/processed/csv_tables/sales_data.table" --sql="SELECT * WHERE amount > 1000"
pond cat "/processed/sensor_series/temperature.series" --sql="SELECT * WHERE timestamp > '2025-01-01'"

# DataFusion automatically:
# 1. Resolves the dynamic series/directories
# 2. Uses materialization cache for CSV conversions
# 3. Pushes down predicates where possible
# 4. Uses temporal metadata for efficient scanning
```

### **Phase 4: TinyFS Integration**
Integrate both dynamic files and dynamic directories seamlessly into TinyFS:

```rust
pub struct DynamicTinyFSFile {
    factory_registry: Arc<DynamicFactoryRegistry>,
    factory_type: String,
    content_metadata: Vec<u8>,  // The metadata stored in content field
    cached_content: Arc<Mutex<Option<Vec<u8>>>>,
    cache_timeout: Option<Duration>,
    last_generated: Arc<Mutex<Option<Instant>>>,
}

pub struct DynamicTinyFSDirectory {
    factory_registry: Arc<DynamicFactoryRegistry>,
    factory_type: String,
    content_metadata: Vec<u8>,
    transient_entries: Arc<RwLock<HashMap<String, TransientDynamicEntry>>>,
    last_discovery: Arc<Mutex<Option<Instant>>>,
    discovery_ttl: Duration,
}

#[derive(Clone)]
pub struct TransientDynamicEntry {
    factory_type: String,
    metadata: Vec<u8>,
    entry_type: EntryType,
}

impl tinyfs::Directory for DynamicTinyFSDirectory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>> {
        // Refresh transient entries if needed
        self.refresh_if_needed().await?;
        
        let entries = self.transient_entries.read().await;
        if let Some(transient) = entries.get(name) {
            // Create dynamic file on-demand from transient entry
            let dynamic_file = DynamicTinyFSFile {
                factory_registry: self.factory_registry.clone(),
                factory_type: transient.factory_type.clone(),
                content_metadata: transient.metadata.clone(),
                cached_content: Arc::new(Mutex::new(None)),
                cache_timeout: Some(Duration::from_hours(1)),
                last_generated: Arc::new(Mutex::new(None)),
            };
            
            Ok(Some(NodeRef::File(Box::new(dynamic_file))))
        } else {
            Ok(None)
        }
    }
    
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>> {
        // Refresh and return all transient entries
        self.refresh_if_needed().await?;
        
        let entries = self.transient_entries.read().await;
        let iter = entries.iter().map(|(name, transient)| {
            let dynamic_file = DynamicTinyFSFile {
                factory_registry: self.factory_registry.clone(),
                factory_type: transient.factory_type.clone(),
                content_metadata: transient.metadata.clone(),
                cached_content: Arc::new(Mutex::new(None)),
                cache_timeout: Some(Duration::from_hours(1)),
                last_generated: Arc::new(Mutex::new(None)),
            };
            
            (name.clone(), NodeRef::File(Box::new(dynamic_file)))
        });
        
        Ok(Box::new(iter))
    }
    
    async fn refresh_if_needed(&self) -> Result<()> {
        let should_refresh = {
            let last = self.last_discovery.lock().await;
            last.map_or(true, |t| t.elapsed() > self.discovery_ttl)
        };
        
        if should_refresh {
            // Regenerate directory content using factory
            let content = self.factory_registry
                .materialize(&self.factory_type, &self.content_metadata)?;
                
            // Decode directory listing and update transient entries
            let new_entries = decode_directory_listing(&content)?;
            
            let mut entries = self.transient_entries.write().await;
            entries.clear();
            entries.extend(new_entries);
            
            *self.last_discovery.lock().await = Some(Instant::now());
        }
        
        Ok(())
    }
}

impl tinyfs::File for DynamicTinyFSFile {
    async fn content(&self) -> TinyFSResult<Vec<u8>> {
        // Check cache first
        if let Some(cached) = self.check_cache().await? {
            return Ok(cached);
        }
        
        // Generate new content using factory
        let content = self.factory_registry
            .materialize(&self.factory_type, &self.content_metadata)
            .map_err(|e| TinyFSError::DynamicFileError(e.to_string()))?;
        
        // Update cache
        self.update_cache(content.clone()).await?;
        
        Ok(content)
    }
    
    async fn size(&self) -> TinyFSResult<u64> {
        // For dynamic files, we need to generate content to get size
        let content = self.content().await?;
        Ok(content.len() as u64)
    }
    
    async fn async_reader(&self) -> TinyFSResult<Pin<Box<dyn AsyncReadSeek>>> {
        // Dynamic files always generate content first, then provide reader
        let content = self.content().await?;
        Ok(Box::pin(Cursor::new(content)))
    }
}
```

### **Phase 5: Persistence Layer Integration**
Extend OpLogPersistence to handle dynamic file materialization:

```rust
impl OpLogPersistence {
    pub async fn materialize_dynamic_file(&self, entry: &OplogEntry) -> Result<Vec<u8>> {
        match entry.factory.as_str() {
            "tlogfs" => {
                // Static content - use existing TLogFS logic
                if let Some(content) = &entry.content {
                    Ok(content.clone())
                } else {
                    // Large file - use existing external content loading via sha256
                    self.load_external_content(&entry.sha256).await
                }
            },
            factory_type => {
                // Dynamic content - use factory with content field as metadata
                let content = entry.content.as_ref()
                    .ok_or(TLogFSError::ContentMissing)?;
                    
                self.factory_registry
                    .materialize(factory_type, content)
                    .map_err(|e| TLogFSError::DynamicFileError(e.to_string()))
            }
        }
    }
    
    pub async fn create_dynamic_file<T: Serialize>(&self, 
        path: &NodePath, 
        factory_type: &str, 
        metadata: &T,
        entry_type: EntryType) -> Result<NodePath> {
        
        // Serialize metadata to content field
        let content = serde_json::to_vec(metadata)
            .map_err(|e| TLogFSError::SerializationError(e.to_string()))?;
        
        // Create OplogEntry with dynamic factory
        let entry = OplogEntry {
            factory: factory_type.to_string(),
            content: Some(content), // Factory metadata in content field
            sha256: String::new(),   // No SHA256 for dynamic files
            // ... other fields
        };
        
        // Store in Delta Lake
        let node_path = self.store_oplog_entry(entry).await?;
        Ok(node_path)
    }
}
```

## ✅ **Architecture Benefits**

### **API Transparency**
- **Seamless Integration**: Dynamic files work exactly like static files to TinyFS consumers
- **No Client Changes**: Existing code continues to work without modification
- **Standard Interfaces**: All files implement the same `File` trait regardless of type
- **Transparent Caching**: Performance optimizations hidden from clients

### **Extensibility**
- **Plugin Architecture**: New factory types can be added without core system changes
- **Registry Pattern**: Factories can be registered at runtime
- **Metadata Driven**: All configuration stored in JSON metadata for flexibility
- **Version Evolution**: Factory implementations can evolve while maintaining compatibility

### **Performance Optimization**
- **On-Demand Generation**: Content only generated when accessed
- **Intelligent Caching**: Expensive operations cached with configurable timeouts
- **Streaming Support**: Large dynamic content can be streamed without memory loading
- **Lazy Evaluation**: Dynamic files only materialized when actually read

### **Persistence Integration**
- **Durable Configuration**: All factory metadata stored in OplogEntry for persistence
- **Transaction Safety**: Dynamic file creation participates in ACID transactions
- **Version Control**: Dynamic file configurations versioned with filesystem operations
- **Backup Compatible**: Factory configurations included in filesystem backups

## 📅 **Implementation Roadmap**

### **Phase 1: Minimum Viable Dynamic Directory (Weeks 1-2)**
**Objective**: Implement hostmount dynamic directory and CLI mknod command

**Deliverables**:
- [ ] Add `factory` string column to OplogEntry schema
- [ ] Implement hostmount dynamic directory type
- [ ] Add CLI `mknod` command for creating hostmount dynamic directories
- [ ] Validate and store configuration metadata in content field
- [ ] Ensure read-only behavior and correct host directory mapping

**Success Criteria**:
- Hostmount dynamic directories can be created via CLI
- Directory contents reflect host directory state
- No mutation operations permitted
- Configuration is validated and stored
- All existing static file operations remain unchanged

## 🎯 **Success Criteria**

### **Functional Requirements**
- **✅ Backward Compatibility**: All existing static file operations unchanged
- **✅ Hostmount Dynamic Directory**: Can be created and managed via CLI
- **✅ Read-only Behavior**: No mutation operations permitted
- **✅ Persistence**: Configuration survives filesystem restarts
- **✅ Error Handling**: Graceful degradation for invalid config or missing host directory

### **Quality Requirements**
- **✅ Testing**: Coverage for hostmount dynamic directory and CLI
- **✅ Documentation**: Clear usage examples for CLI and hostmount
- **✅ Reliability**: System remains stable with malformed or invalid dynamic metadata

### **Integration Requirements**
- **✅ TinyFS Compatibility**: All existing TinyFS clients work unchanged
- **✅ CLI Compatibility**: Command-line tools work transparently with hostmount dynamic directories

## 📊 **Risk Assessment**

### **Low Risks** ✅
- **Architecture Compatibility**: TinyFS already supports custom Directory implementations
- **Schema Evolution**: Delta Lake handles schema changes gracefully
- **Incremental Implementation**: Each phase can be developed and tested independently

### **Medium Risks** ⚠️
- **Security Concerns**: Host directory mounting must be strictly read-only
- **Metadata Validation**: Invalid or missing config must be handled gracefully

### **Managed Risks** 🛡️
- **Migration Complexity**: Existing installations need smooth upgrade path
  - **Mitigation**: Backward compatible schema, migration tools

## 🚀 **Next Steps**

### **Immediate Actions**
1. **Review and Refine Plan**: Stakeholder review of minimum viable hostmount plan
2. **Schema Design Review**: Finalize OplogEntry schema and migration strategy
3. **Test Strategy**: Design comprehensive testing approach for hostmount dynamic directory and CLI
4. **Documentation Plan**: Outline documentation requirements for CLI and hostmount usage

---

**Document Status**: ✅ Minimum Viable Hostmount Plan - Ready for Review and Implementation
**Last Updated**: July 30, 2025
**Next Review**: Before Phase 1 implementation begins
