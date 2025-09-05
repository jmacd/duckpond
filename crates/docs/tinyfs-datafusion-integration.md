# TinyFS-DataFusion File-Table Duality Integration

**Version**: 1.0  
**Date**: September 4, 2025  
**Status**: Design Phase  

## Executive Summary

This document outlines a comprehensive redesign of the TinyFS-DataFusion integration to eliminate the current impedance mismatch between file-oriented and table-oriented interfaces. The core insight is that structured files in TinyFS are fundamentally **both files AND tables**, and should expose native interfaces for both paradigms.

## Current Architecture Problems

### 1. Performance Issues
- **4x computation overhead** for SqlDerivedFile due to unnecessary Parquet serialization
- **Schema loading** requires reading file data instead of metadata analysis
- **Intermediate materialization** for dynamic content that could stream directly

### 2. Architectural Impedance Mismatch
```
TinyFS Layer:    File interface (async_reader)
                      ↓ (forced conversion)
DataFusion Layer:    Table interface (RecordBatch streams)
```

### 3. Special Case Handling
- **SqlDerivedFile pretends to be a static file** but is actually dynamic
- **UnifiedTableProvider searches metadata table** for files that don't exist there
- **Virtual files forced through static file discovery** mechanisms

## Proposed Solution: File-Table Duality

### Core Principle
Every structured file in TinyFS should be **natively both a file and a table**:
- **File semantics**: Raw bytes, streaming I/O, filesystem operations
- **Table semantics**: Structured records, schema, SQL queries

## Detailed Design

### 1. FileTable Trait Definition

```rust
/// Represents a file that contains structured tabular data.
/// Provides both file-oriented and table-oriented access patterns.
#[async_trait]
pub trait FileTable: File + Send + Sync {
    // Table Interface (Primary for DataFusion)
    
    /// Get a stream of RecordBatches from this table
    /// This is the primary interface for DataFusion integration
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError>;
    
    /// Get the schema of this table without reading data
    /// Should be efficient and cacheable
    async fn schema(&self) -> Result<SchemaRef, TLogFSError>;
    
    /// Get table statistics for query optimization (optional)
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        Ok(Statistics::default())
    }
    
    /// Create a DataFusion TableProvider for this file
    /// Default implementation wraps the FileTable in a provider
    fn as_table_provider(self: Arc<Self>) -> Arc<dyn TableProvider> {
        Arc::new(FileTableProvider::new(self))
    }
    
    // File Interface (Inherited from File trait)
    // async fn async_reader(&self) -> Pin<Box<dyn AsyncReadSeek>>;
    // async fn async_writer(&self) -> Pin<Box<dyn AsyncWrite + Send>>;
    // async fn metadata(&self) -> NodeMetadata;
}
```

### 2. Implementation for Static Files

#### OpLogFile (FileTable/FileSeries)
```rust
#[async_trait]
impl FileTable for OpLogFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        // Direct Parquet → RecordBatch streaming (no intermediate steps)
        let reader = self.async_reader().await?;
        let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet stream: {}", e)))?;
            
        let stream = stream_builder.build()
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to build Parquet stream: {}", e)))?;
            
        Ok(Box::pin(stream))
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // Extract schema from Parquet metadata (no data reading required)
        let reader = self.async_reader().await?;
        let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to read Parquet metadata: {}", e)))?;
            
        Ok(stream_builder.schema())
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        // Extract row count and other stats from Parquet metadata
        let reader = self.async_reader().await?;
        let stream_builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader).await?;
        let metadata = stream_builder.metadata();
        
        let num_rows = metadata.file_metadata().num_rows() as usize;
        Ok(Statistics {
            num_rows: Some(num_rows),
            total_byte_size: None, // Could extract from file metadata
            column_statistics: None, // Could extract from Parquet column stats
            is_exact: true,
        })
    }
}
```

### 3. Implementation for Dynamic Files

#### SqlDerivedFile
```rust
#[async_trait]
impl FileTable for SqlDerivedFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        // Direct SQL execution → RecordBatch streaming (no Parquet intermediate)
        let ctx = self.create_datafusion_context().await?;
        
        // Register source tables
        self.register_source_tables(&ctx).await?;
        
        // Execute SQL and return stream directly
        let dataframe = ctx.sql(&self.effective_sql()).await
            .map_err(|e| TLogFSError::DataFusionError(e))?;
            
        let stream = dataframe.execute_stream().await
            .map_err(|e| TLogFSError::DataFusionError(e))?;
            
        Ok(stream)
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // Get schema from SQL logical plan (no execution required)
        let ctx = self.create_datafusion_context().await?;
        
        // Register source tables (needed for schema inference)
        self.register_source_tables(&ctx).await?;
        
        // Parse SQL to get logical plan
        let logical_plan = ctx.sql(&self.effective_sql()).await
            .map_err(|e| TLogFSError::DataFusionError(e))?
            .logical_plan()
            .clone();
            
        Ok(logical_plan.schema().as_ref().clone())
    }
    
    async fn statistics(&self) -> Result<Statistics, TLogFSError> {
        // For dynamic content, statistics are unknown
        Ok(Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        })
    }
    
    // File interface implementation (fallback for tools that need file access)
    async fn async_reader(&self) -> Result<Pin<Box<dyn AsyncReadSeek>>, TLogFSError> {
        // Materialize to Parquet only when file access is specifically requested
        let stream = self.record_batch_stream().await?;
        let parquet_data = self.stream_to_parquet_bytes(stream).await?;
        Ok(Box::pin(std::io::Cursor::new(parquet_data)))
    }
}
```

### 4. Helper Methods for SqlDerivedFile

```rust
impl SqlDerivedFile {
    /// Create DataFusion context with registered tables
    async fn create_datafusion_context(&self) -> Result<SessionContext, TLogFSError> {
        let ctx = SessionContext::new();
        // Configure with appropriate settings
        Ok(ctx)
    }
    
    /// Register source tables from patterns in context
    async fn register_source_tables(&self, ctx: &SessionContext) -> Result<(), TLogFSError> {
        for (table_name, pattern_info) in &self.config.patterns {
            // Resolve pattern to actual table
            let table_provider = self.resolve_pattern_to_table_provider(pattern_info).await?;
            ctx.register_table(table_name, table_provider)?;
        }
        Ok(())
    }
    
    /// Convert RecordBatch stream to Parquet bytes (for file interface fallback)
    async fn stream_to_parquet_bytes(&self, stream: SendableRecordBatchStream) -> Result<Vec<u8>, TLogFSError> {
        use parquet::arrow::ArrowWriter;
        use futures::stream::StreamExt;
        
        let mut buffer = Vec::new();
        let schema = stream.schema();
        
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to create Parquet writer: {}", e)))?;
                
            let mut stream = stream;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Error reading batch: {}", e)))?;
                writer.write(&batch)
                    .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to write batch: {}", e)))?;
            }
            
            writer.close()
                .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to finalize Parquet: {}", e)))?;
        }
        
        Ok(buffer)
    }
}
```

### 5. Simplified DataFusion Integration

#### FileTableProvider (Generic Wrapper)
```rust
/// Generic TableProvider that wraps any FileTable
pub struct FileTableProvider {
    file_table: Arc<dyn FileTable>,
    schema: SchemaRef,
}

impl FileTableProvider {
    pub fn new(file_table: Arc<dyn FileTable>) -> Self {
        // Schema will be loaded lazily or cached
        Self {
            file_table,
            schema: Arc::new(Schema::empty()), // Placeholder
        }
    }
    
    pub async fn load_schema(&mut self) -> Result<(), TLogFSError> {
        self.schema = self.file_table.schema().await?;
        Ok(())
    }
}

#[async_trait]
impl TableProvider for FileTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Get stream from FileTable
        let stream = self.file_table.record_batch_stream().await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            
        // Apply projection, filters, limit at execution plan level
        let exec_plan = StreamExecutionPlan::new(stream, self.schema.clone());
        
        // TODO: Push down projection and filters when possible
        Ok(Arc::new(exec_plan))
    }
    
    async fn statistics(&self) -> Option<Statistics> {
        self.file_table.statistics().await.ok()
    }
}
```

#### StreamExecutionPlan (for Direct Streaming)
```rust
/// ExecutionPlan that wraps a RecordBatch stream
#[derive(Debug)]
pub struct StreamExecutionPlan {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl StreamExecutionPlan {
    pub fn new(stream: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl ExecutionPlan for StreamExecutionPlan {
    fn as_any(&self) -> &dyn Any { self }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }
    
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }
    
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition == 0 {
            // Return the wrapped stream
            Ok(self.stream.clone()) // Note: This requires the stream to be cloneable
        } else {
            Err(DataFusionError::Internal(format!("Invalid partition: {}", partition)))
        }
    }
    
    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::default())
    }
}
```

### 6. TinyFS Integration

#### Path Resolution Enhancement
```rust
impl WD {
    /// Resolve a path to a FileTable (for structured data access)
    pub async fn resolve_file_table<P: AsRef<Path>>(&self, path: P) -> Result<Arc<dyn FileTable>, Error> {
        let (_, lookup) = self.resolve_path(path).await?;
        match lookup {
            Lookup::Found(node) => {
                let node_guard = node.borrow().await;
                if let Ok(file) = node_guard.as_file() {
                    // Check if this file implements FileTable
                    if let Some(file_table) = file.as_file_table() {
                        return Ok(file_table);
                    }
                }
                Err(Error::Other("File does not contain structured data".to_string()))
            },
            Lookup::NotFound(full_path, _) => Err(Error::not_found(&full_path)),
            Lookup::Empty(_) => Err(Error::empty_path()),
        }
    }
    
    /// Check if a path contains structured data
    pub async fn is_table<P: AsRef<Path>>(&self, path: P) -> bool {
        self.resolve_file_table(path).await.is_ok()
    }
}
```

#### File Trait Extension
```rust
pub trait File: Metadata + Send + Sync {
    // Existing methods...
    async fn async_reader(&self) -> error::Result<Pin<Box<dyn AsyncReadSeek>>>;
    async fn async_writer(&self) -> error::Result<Pin<Box<dyn AsyncWrite + Send>>>;
    
    // New method for FileTable detection
    fn as_file_table(&self) -> Option<Arc<dyn FileTable>> {
        None // Default implementation - only structured files override this
    }
}
```

### 7. Updated UnifiedTableProvider

#### Simplified Implementation
```rust
pub struct UnifiedTableProvider {
    file_table: Arc<dyn FileTable>,
    cached_schema: Option<SchemaRef>,
}

impl UnifiedTableProvider {
    /// Create from any FileTable
    pub async fn from_file_table(file_table: Arc<dyn FileTable>) -> Result<Self, TLogFSError> {
        let schema = file_table.schema().await?;
        Ok(Self {
            file_table,
            cached_schema: Some(schema),
        })
    }
    
    /// Create from TinyFS path (unified for all file types)
    pub async fn from_path(tinyfs_root: &WD, path: &str) -> Result<Self, TLogFSError> {
        let file_table = tinyfs_root.resolve_file_table(path).await
            .map_err(|e| TLogFSError::ArrowMessage(format!("Failed to resolve table at {}: {}", path, e)))?;
        Self::from_file_table(file_table).await
    }
}

#[async_trait]
impl TableProvider for UnifiedTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    
    fn schema(&self) -> SchemaRef {
        self.cached_schema.as_ref().unwrap().clone()
    }
    
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Delegate to FileTable
        let stream = self.file_table.record_batch_stream().await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            
        Ok(Arc::new(StreamExecutionPlan::new(stream, self.schema())))
    }
    
    async fn statistics(&self) -> Option<Statistics> {
        self.file_table.statistics().await.ok()
    }
}
```

### 8. Command Integration (pond cat)

#### Simplified Query Execution
```rust
// In pond cat command
async fn execute_sql_query(tinyfs_root: &WD, path: &str, sql: Option<&str>) -> Result<(), Error> {
    // Create DataFusion context
    let ctx = SessionContext::new();
    
    // Register the table directly from path
    let table_provider = UnifiedTableProvider::from_path(tinyfs_root, path).await?;
    ctx.register_table("data", Arc::new(table_provider))?;
    
    // Execute query
    let query = sql.unwrap_or("SELECT * FROM data");
    let dataframe = ctx.sql(query).await?;
    
    // Stream results
    let mut stream = dataframe.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        // Output batch...
    }
    
    Ok(())
}
```

## Implementation Phases

### Phase 1: Core Trait Definition and Static Files
**Deliverables:**
- [ ] Define `FileTable` trait
- [ ] Implement `FileTable` for `OpLogFile`
- [ ] Create `FileTableProvider` wrapper
- [ ] Update TinyFS path resolution to support `resolve_file_table()`

**Success Criteria:**
- Static files (FileTable/FileSeries) work through new interface
- Existing functionality preserved through File trait
- Performance improvement for schema loading (no data reading)

### Phase 2: Dynamic File Integration
**Deliverables:**
- [ ] Implement `FileTable` for `SqlDerivedFile`
- [ ] Direct RecordBatch streaming from SQL execution
- [ ] Schema extraction from logical plans
- [ ] Fallback Parquet materialization for file interface

**Success Criteria:**
- SqlDerivedFile works without Parquet intermediate step
- 4x performance improvement demonstrated
- Schema loading works without execution

### Phase 3: DataFusion Integration Cleanup
**Deliverables:**
- [ ] Replace old `UnifiedTableProvider` implementation
- [ ] Update `pond cat` to use new integration
- [ ] Remove metadata table dependency for file discovery
- [ ] Performance optimization and caching

**Success Criteria:**
- All file types work through unified interface
- No special cases for different file types
- Command-line tools work seamlessly

### Phase 4: Advanced Features
**Deliverables:**
- [ ] Predicate pushdown for supported file types
- [ ] Projection pushdown optimization
- [ ] Enhanced statistics for query optimization
- [ ] Parallel execution for large files

**Success Criteria:**
- Query optimization works across file boundaries
- Performance competitive with native DataFusion sources
- Extensible for future file types

## Performance Impact Analysis

### Before (Current Architecture)
1. **SqlDerivedFile.async_reader()**
   - Execute SQL query → RecordBatches
   - Serialize RecordBatches → Parquet bytes
   - Store Parquet bytes in memory
   - Return Cursor over Parquet bytes

2. **UnifiedTableProvider.scan()**
   - Read Parquet bytes from Cursor
   - Deserialize Parquet → RecordBatches
   - Stream RecordBatches to DataFusion

**Total: SQL → RecordBatch → Parquet → RecordBatch (2x conversion overhead)**

### After (FileTable Architecture)
1. **SqlDerivedFile.record_batch_stream()**
   - Execute SQL query → RecordBatches
   - Stream RecordBatches directly

2. **UnifiedTableProvider.scan()**
   - Get RecordBatch stream from FileTable
   - Stream directly to DataFusion

**Total: SQL → RecordBatch (direct streaming, no conversions)**

### Expected Performance Improvements
- **Eliminate 4x computation** from unnecessary serialization/deserialization
- **Reduce memory usage** by avoiding Parquet materialization
- **Faster schema discovery** through logical plan analysis
- **Better query optimization** through native DataFusion integration

## Extensibility

### Future File Types
The FileTable trait makes it easy to add new structured file types:

```rust
// Example: JsonLinesFile
impl FileTable for JsonLinesFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        // Parse JSON Lines and stream as RecordBatches
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // Infer schema from JSON structure
    }
}

// Example: DeltaTableFile  
impl FileTable for DeltaTableFile {
    async fn record_batch_stream(&self) -> Result<SendableRecordBatchStream, TLogFSError> {
        // Use Delta Lake's native streaming
    }
    
    async fn schema(&self) -> Result<SchemaRef, TLogFSError> {
        // Get schema from Delta table metadata
    }
}
```

### DataFusion Extensions
The architecture supports advanced DataFusion features:

```rust
impl FileTable for AdvancedFile {
    async fn scan_with_pushdown(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, TLogFSError> {
        // Implement predicate and projection pushdown
    }
}
```

## Migration Strategy

### Backward Compatibility
- **File trait preserved**: Existing code using `async_reader()` continues to work
- **Gradual migration**: Components can adopt FileTable interface incrementally
- **Fallback implementation**: FileTable provides `async_reader()` for compatibility

### Testing Strategy
- **Parallel implementation**: Keep old code until new code is proven
- **Feature flags**: Allow switching between implementations
- **Performance benchmarks**: Measure improvements at each phase
- **Integration tests**: Ensure all command-line tools work correctly

## Success Metrics

### Performance Metrics
- [ ] **4x reduction** in SqlDerivedFile query execution time
- [ ] **Schema loading time** reduced by 90% (no data reading)
- [ ] **Memory usage** reduced for large query results
- [ ] **Query optimization** improvements measurable

### Code Quality Metrics
- [ ] **Lines of code reduction** in DataFusion integration
- [ ] **Elimination of special cases** for different file types
- [ ] **Test coverage** maintained or improved
- [ ] **Documentation coverage** for new interfaces

### User Experience Metrics
- [ ] **Command response time** improvements
- [ ] **Error message clarity** for table/file type mismatches
- [ ] **Feature parity** with existing functionality
- [ ] **Tool compatibility** preserved

## Risk Mitigation

### Technical Risks
- **Complexity increase**: Mitigated by clear trait boundaries and documentation
- **Performance regression**: Mitigated by benchmarking and gradual rollout
- **Breaking changes**: Mitigated by maintaining File trait compatibility

### Implementation Risks
- **Scope creep**: Mitigated by clear phase boundaries and success criteria
- **DataFusion coupling**: Mitigated by keeping File trait independent
- **Testing coverage**: Mitigated by comprehensive test plan for each phase

## Conclusion

The FileTable trait design provides a clean, performant, and extensible solution to the TinyFS-DataFusion integration challenges. By recognizing that structured files are naturally both files and tables, we can provide native interfaces for both use cases without architectural compromises.

The phased implementation approach allows us to deliver value incrementally while maintaining backward compatibility and reducing implementation risk. The expected performance improvements and architectural simplifications justify the development investment.

This design positions DuckPond for future enhancements in query optimization, data source integration, and performance scaling while maintaining the clean abstractions that make the codebase maintainable.
