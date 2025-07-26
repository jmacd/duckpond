# Dynamic File Support Integration Plan

**Project**: DuckPond TLogFS Dynamic File Support  
**Created**: July 25, 2025  
**Status**: Planning Phase Complete - Ready for Implementation  
**Priority**: Medium - Advanced feature with solid foundation  

## 📋 **Executive Summary**

This document outlines the comprehensive plan to integrate dynamic file support into TLogFS, bringing the sophisticated virtual file capabilities from the original duckpond implementation into the modern TinyFS/TLogFS architecture. The plan leverages a factory pattern approach to maintain API transparency while enabling powerful dynamic content generation capabilities.

## 🔍 **Research Findings**

### **Original DuckPond Dynamic File System**
The original duckpond implementation provides a sophisticated dynamic file system through the `TreeLike` trait architecture:

**Core Dynamic File Types**:
- **Template files**: Generate content from Tera templates with variable substitution
- **Derived files**: SQL query results materialized as virtual files  
- **Combine files**: Union of multiple files with identical schemas
- **Scribble files**: Synthetic data generators for testing

**Key Implementation Traits**:
```rust
// TreeLike trait enables dynamic file generation
impl TreeLike for Collection {
    fn copy_version_to(&mut self, pond: &mut Pond, prefix: &str, 
                      _numf: i32, _ext: &str, mut to: Box<dyn Write + Send + 'a>) -> Result<()> {
        // Generate content on-demand
        let rendered = self.tera.render(&self.name, &ctx).unwrap();
        to.write(rendered.as_bytes())?;
        Ok(())
    }
}
```

**Architecture Elements**:
- **`ForPond` trait**: Identifies data structures that can be materialized as dynamic content
- **`Deriver` trait**: Creates virtual files based on patterns and queries
- **`TreeLike` trait**: Enables on-demand content generation
- **Template system**: Tera template engine integration
- **Synthetic directories**: `SynTree` for virtual directory structures

### **TinyFS Dynamic File Infrastructure**
TinyFS provides excellent foundation for dynamic file integration:

**Architecture Advantages**:
- **API Transparency**: Dynamic and static files indistinguishable to consumers
- **Custom Directory Trait**: `Directory` trait allows virtual filesystem implementations
- **Reference Implementations**: `ReverseDirectory`, `VisitDirectory` demonstrate patterns

**Directory Trait Structure**:
```rust
pub trait Directory {
    fn get(&self, name: &str) -> Result<Option<NodeRef>>;
    fn insert(&mut self, name: String, id: NodeRef) -> Result<()>;
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (String, NodeRef)> + 'a>>;
}
```

**Proven Extension Points**:
- **ReverseDirectory**: Demonstrates Directory trait customization with file list reversal
- **VisitDirectory**: Shows tracking visitor pattern for filesystem traversal
- **API Compatibility**: All examples work transparently with existing TinyFS clients

### **TLogFS Integration Foundation**
Current TLogFS `OplogEntry` structure provides solid foundation for dynamic file support:

**Existing Infrastructure**:
```rust
pub struct OplogEntry {
    // ... existing fields
    pub content: Option<Vec<u8>>,                // Current static content storage
    pub sha256: String,                          // Content addressing for large files
    pub extended_attributes: Option<String>,     // JSON metadata system already exists
}
```

**Ready for Extension**: The schema already supports extended attributes and external content references, providing natural extension points for dynamic file metadata.

## 🏗️ **Architecture Design**

### **Phase 1: Schema Extension**
Add factory column to distinguish between static and dynamic content:

```rust
pub struct OplogEntry {
    // ... existing fields (unchanged)
    pub content: Option<Vec<u8>>,            // Directory: IPC encoded; Symlink: path string; 
                                             // File: inline data OR dynamic factory metadata
    pub sha256: String,                      // Optional - only for physical files, not dynamic
    pub extended_attributes: Option<String>, // General metadata (unchanged)
    
    // NEW: Factory identification
    pub factory: String,                     // "tlogfs" for builtin files, factory type for dynamic
}
```

**Design Rationale**:
- **Reuses Existing Content Field**: Dynamic file metadata stored in existing `content` field
- **Simple Factory Column**: `factory = "tlogfs"` for static content, factory type string for dynamic content
- **No SHA256 for Dynamic**: Dynamic files don't use SHA256 field (deterministic but may change with bug fixes)
- **Type-Safe Metadata**: Each factory defines its own metadata type `T: Serialize + Deserialize`
- **Backward Compatible**: Existing entries get `factory = "tlogfs"` during migration

**Content Field Usage**:
- **Directory nodes**: IPC encoded directory contents (unchanged)
- **Symlink nodes**: Path string (unchanged)  
- **File nodes with `factory = "tlogfs"`**: Inline file data (unchanged)
- **File nodes with dynamic factory**: Serialized factory metadata `T`

**Migration Strategy**:
- **Backward Compatible**: Existing entries default to `factory = "tlogfs"`
- **Schema Versioning**: Use Delta Lake's natural schema evolution  
- **Content Interpretation**: Factory determines how to interpret content field

### **Phase 2: Factory Registry Pattern**
Implement extensible factory system for dynamic content generation:

```rust
pub trait DynamicNodeFactory: Send + Sync {
    /// Unique identifier for this factory type
    fn factory_type(&self) -> &'static str;
    
    /// Metadata type for this factory
    type Metadata: Serialize + Deserialize;
    
    /// Generate node content from typed metadata
    /// Returns the raw content that would be stored in the content field
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>>;
    
    /// What type of entry this factory creates (File, Directory, Symlink, FileTable, FileSeries)
    fn entry_type(&self) -> EntryType;
    
    /// Validate metadata without generating content
    fn validate_metadata(&self, metadata: &Self::Metadata) -> Result<()>;
    
    /// Deserialize metadata from content field bytes
    fn deserialize_metadata(&self, content: &[u8]) -> Result<Self::Metadata> {
        serde_json::from_slice(content).map_err(Into::into)
    }
    
    /// Serialize metadata to content field bytes  
    fn serialize_metadata(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        serde_json::to_vec(metadata).map_err(Into::into)
    }
}

pub struct DynamicFactoryRegistry {
    factories: HashMap<String, Box<dyn DynamicNodeFactory>>,
}

impl DynamicFactoryRegistry {
    pub fn new() -> Self {
        Self { factories: HashMap::new() }
    }
    
    pub fn register<F: DynamicNodeFactory + 'static>(&mut self, factory: F) {
        self.factories.insert(factory.factory_type().to_string(), Box::new(factory));
    }
    
    pub fn materialize(&self, factory_type: &str, content: &[u8]) -> Result<Vec<u8>> {
        let factory = self.factories.get(factory_type)
            .ok_or_else(|| TLogFSError::UnknownFactoryType(factory_type.to_string()))?;
        
        // Each factory deserializes its own metadata type from content field
        let metadata = factory.deserialize_metadata(content)?;
        factory.create_content(&metadata)
    }
    
    pub fn get_entry_info(&self, factory_type: &str) -> Option<EntryType> {
        self.factories.get(factory_type)
            .map(|f| f.entry_type())
    }
}
```

### **Phase 3: Core Factory Implementations**
Implement the four core factory types from original duckpond:

#### **1. SqlQueryFactory**
Execute SQL queries and return Arrow IPC bytes:

```rust
pub struct SqlQueryFactory {
    datafusion_ctx: Arc<SessionContext>,
}

#[derive(Serialize, Deserialize)]
pub struct SqlQueryMetadata {
    query: String,
    timeout_ms: Option<u64>,
}

impl DynamicNodeFactory for SqlQueryFactory {
    type Metadata = SqlQueryMetadata;
    
    fn factory_type(&self) -> &'static str { "sql_query" }
    
    fn entry_type(&self) -> EntryType { EntryType::FileTable }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Execute SQL query using DataFusion
        let df = self.datafusion_ctx.sql(&metadata.query).await?;
        let batches = df.collect().await?;
        
        // Convert to Arrow IPC format
        let schema = batches[0].schema();
        let mut buffer = Vec::new();
        let mut writer = FileWriter::try_new(&mut buffer, &schema)?;
        
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        
        Ok(buffer)
    }
}
```

#### **2. TemplateFactory**
Tera template engine integration with variable substitution:

```rust
pub struct TemplateFactory {
    tera: Arc<Tera>,
}

#[derive(Serialize, Deserialize)]
pub struct TemplateMetadata {
    template_name: String,
    variables: HashMap<String, serde_json::Value>,
    output_format: Option<String>, // "text", "json", "csv"
}

impl DynamicNodeFactory for TemplateFactory {
    type Metadata = TemplateMetadata;
    
    fn factory_type(&self) -> &'static str { "template" }
    
    fn entry_type(&self) -> EntryType { 
        // Could be FileTable, FileSeries, File, Directory, or Symlink depending on template output
        EntryType::File // Default, but template metadata could override this
    }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Create Tera context from variables
        let mut context = Context::new();
        for (key, value) in &metadata.variables {
            context.insert(key, value);
        }
        
        // Render template
        let rendered = self.tera.render(&metadata.template_name, &context)?;
        
        Ok(rendered.into_bytes())
    }
}
```

#### **3. CombineFactory**
Union multiple files with identical schemas into Parquet output:

```rust
pub struct CombineFactory {
    tinyfs_root: Arc<WD>,
}

#[derive(Serialize, Deserialize)]
pub struct CombineMetadata {
    file_patterns: Vec<String>,
    output_schema: Option<String>, // JSON schema override
    deduplication: Option<bool>,
    output_type: EntryType,        // FileTable or FileSeries
}

impl DynamicNodeFactory for CombineFactory {
    type Metadata = CombineMetadata;
    
    fn factory_type(&self) -> &'static str { "combine" }
    
    fn entry_type(&self) -> EntryType { 
        // Will be determined by metadata.output_type at runtime
        EntryType::FileTable // Default
    }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Collect all files matching patterns
        let mut all_batches = Vec::new();
        for pattern in &metadata.file_patterns {
            let files = self.find_files_by_pattern(pattern).await?;
            for file_path in files {
                let content = self.tinyfs_root.read_file(&file_path).await?;
                let batches = self.parquet_to_batches(&content)?;
                all_batches.extend(batches);
            }
        }
        
        // Combine into single Parquet file
        let combined_buffer = self.batches_to_parquet(&all_batches)?;
        Ok(combined_buffer)
    }
}
```

#### **4. ScribbleFactory**
Synthetic data generation for testing and development:

```rust
pub struct ScribbleFactory;

#[derive(Serialize, Deserialize)]
pub struct ScribbleMetadata {
    schema: String,           // JSON schema definition
    row_count: usize,
    seed: Option<u64>,        // For reproducible generation
    distribution: Option<String>, // "uniform", "normal", "exponential"
    output_type: EntryType,   // FileTable or FileSeries
}

impl DynamicNodeFactory for ScribbleFactory {
    type Metadata = ScribbleMetadata;
    
    fn factory_type(&self) -> &'static str { "scribble" }
    
    fn entry_type(&self) -> EntryType { 
        // Will be determined by metadata.output_type at runtime
        EntryType::FileTable // Default
    }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Parse schema and generate synthetic data
        let schema = self.parse_schema(&metadata.schema)?;
        let mut rng = metadata.seed.map(StdRng::seed_from_u64)
            .unwrap_or_else(StdRng::from_entropy);
        
        let batches = self.generate_batches(&schema, metadata.row_count, &mut rng)?;
        let parquet_buffer = self.batches_to_parquet(&batches)?;
        
        Ok(parquet_buffer)
    }
}

#### **5. DirectoryFactory**
Generate dynamic directories with computed entries:

```rust
pub struct DirectoryFactory {
    tinyfs_root: Arc<WD>,
}

#[derive(Serialize, Deserialize)]
pub struct DirectoryMetadata {
    pattern: String,          // Pattern for finding files to include
    filter: Option<String>,   // Optional filter expression
    virtual_structure: bool,  // Whether to create virtual subdirectories
}

impl DynamicNodeFactory for DirectoryFactory {
    type Metadata = DirectoryMetadata;
    
    fn factory_type(&self) -> &'static str { "directory" }
    
    fn entry_type(&self) -> EntryType { EntryType::Directory }
    
    fn create_content(&self, metadata: &Self::Metadata) -> Result<Vec<u8>> {
        // Find files matching pattern
        let files = self.find_files_by_pattern(&metadata.pattern).await?;
        
        // Create VersionedDirectoryEntry records for each file
        let mut entries = Vec::new();
        for file_path in files {
            let entry = VersionedDirectoryEntry {
                name: file_path.file_name().unwrap().to_string(),
                child_node_id: self.get_node_id_for_path(&file_path)?,
                operation_type: "Insert".to_string(),
                node_type: "File".to_string(),
            };
            entries.push(entry);
        }
        
        // Serialize to Arrow IPC format (same as regular directories)
        let batches = entries_to_record_batches(&entries)?;
        let mut buffer = Vec::new();
        let mut writer = FileWriter::try_new(&mut buffer, &batches[0].schema())?;
        
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        
        Ok(buffer)
    }
}
```

### **Phase 4: TinyFS Integration**
Integrate dynamic files seamlessly into TinyFS File trait:

```rust
pub struct DynamicTinyFSFile {
    factory_registry: Arc<DynamicFactoryRegistry>,
    factory_type: String,
    content_metadata: Vec<u8>,  // The metadata stored in content field
    cached_content: Arc<Mutex<Option<Vec<u8>>>>,
    cache_timeout: Option<Duration>,
    last_generated: Arc<Mutex<Option<Instant>>>,
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

### **Phase 1: Foundation (Weeks 1-2)**
**Objective**: Extend OplogEntry schema and basic factory infrastructure

**Deliverables**:
- [ ] Add `factory` string column to OplogEntry schema
- [ ] Implement `DynamicFactoryRegistry` core with type-safe metadata
- [ ] Create factory trait with associated `Metadata` type
- [ ] Add backward compatibility (default `factory = "tlogfs"` for existing entries)
- [ ] Update Delta Lake schema evolution
- [ ] Clarify content field usage for different node types

**Success Criteria**: 
- All existing tests pass with new schema
- New dynamic entries can be stored and retrieved
- Schema migration works correctly

### **Phase 2: Core Factory Implementation (Weeks 3-4)**
**Objective**: Implement SqlQueryFactory and basic registry

**Deliverables**:
- [ ] Complete `SqlQueryFactory` with DataFusion integration
- [ ] Implement factory registry registration system
- [ ] Add metadata validation and error handling
- [ ] Create basic factory testing infrastructure
- [ ] Add configuration management for DataFusion context

**Success Criteria**:
- SQL queries can be stored and executed as dynamic files
- Factory registry can register and invoke factories
- Comprehensive error handling for invalid metadata

### **Phase 3: Extended Factory Types (Weeks 5-6)**
**Objective**: Implement Template, Combine, and Scribble factories

**Deliverables**:
- [ ] `TemplateFactory` with Tera integration
- [ ] `CombineFactory` for file pattern unions
- [ ] `ScribbleFactory` for synthetic data generation
- [ ] Advanced metadata schemas for each factory type
- [ ] Factory-specific testing and validation

**Success Criteria**:
- All four core factory types operational
- Complex metadata configurations working
- Performance benchmarks for each factory type

### **Phase 4: TinyFS Integration (Weeks 7-8)**
**Objective**: Seamless integration with TinyFS File trait

**Deliverables**:
- [ ] `DynamicTinyFSFile` implementation
- [ ] Caching system with configurable timeouts
- [ ] Streaming support for large dynamic content
- [ ] Integration with existing TinyFS operations
- [ ] End-to-end testing with existing clients

**Success Criteria**:
- Dynamic files indistinguishable from static files to clients
- Performance equivalent to static files for cached content
- All existing TinyFS tests pass with dynamic file support

### **Phase 5: Production Hardening (Week 9)**
**Objective**: Production readiness and comprehensive testing

**Deliverables**:
- [ ] Comprehensive error handling and recovery
- [ ] Performance optimization and benchmarking
- [ ] Security review of dynamic code execution
- [ ] Documentation and usage examples
- [ ] Migration tools for existing installations

**Success Criteria**:
- Production-grade error handling and logging
- Performance benchmarks meet or exceed static file performance
- Security review completed with any issues addressed
- Complete documentation for all factory types

## 🎯 **Success Criteria**

### **Functional Requirements**
- **✅ Backward Compatibility**: All existing static file operations unchanged
- **✅ Factory Extensibility**: New factory types can be added without core changes
- **✅ Performance**: Dynamic content generation does not impact static file performance
- **✅ Persistence**: Factory configurations survive filesystem restarts
- **✅ Error Handling**: Graceful degradation when factories unavailable
- **✅ Security**: Dynamic code execution properly sandboxed

### **Quality Requirements**
- **✅ Testing**: Comprehensive test coverage for all factory types (>90%)
- **✅ Documentation**: Clear examples for custom factory implementation
- **✅ Performance**: Cached dynamic content performs within 10% of static files
- **✅ Memory**: No memory leaks during long-running dynamic file operations
- **✅ Reliability**: System remains stable with malformed or invalid dynamic metadata

### **Integration Requirements**
- **✅ TinyFS Compatibility**: All existing TinyFS clients work unchanged
- **✅ DataFusion Integration**: SQL factories integrate seamlessly with query engine
- **✅ Delta Lake Persistence**: Dynamic file metadata properly versioned and backed up
- **✅ CLI Compatibility**: Command-line tools work transparently with dynamic files

## 📊 **Risk Assessment**

### **Low Risks** ✅
- **Architecture Compatibility**: TinyFS already supports custom File implementations
- **Schema Evolution**: Delta Lake handles schema changes gracefully
- **Factory Pattern**: Well-established pattern with clear interfaces
- **Incremental Implementation**: Each phase can be developed and tested independently

### **Medium Risks** ⚠️
- **Performance Impact**: Dynamic content generation could be slow for complex operations
  - **Mitigation**: Comprehensive caching and streaming support
- **Security Concerns**: Dynamic code execution needs proper sandboxing
  - **Mitigation**: Careful factory interface design, input validation
- **Metadata Complexity**: JSON metadata could become unwieldy for complex configurations
  - **Mitigation**: Strong typing with serde, comprehensive validation

### **Managed Risks** 🛡️
- **Factory Dependencies**: External dependencies (DataFusion, Tera) could create compatibility issues
  - **Mitigation**: Version pinning, compatibility testing
- **Migration Complexity**: Existing installations need smooth upgrade path
  - **Mitigation**: Backward compatible schema, migration tools

## 🚀 **Next Steps**

### **Immediate Actions**
1. **Review and Refine Plan**: Stakeholder review of this comprehensive plan
2. **Architecture Validation**: Technical review of proposed interfaces and patterns
3. **Dependency Analysis**: Evaluate external dependencies (Tera, additional DataFusion features)
4. **Timeline Confirmation**: Validate 8-week timeline with development resources

### **Phase 1 Kickoff Preparation**
1. **Schema Design Review**: Finalize ContentType enum and migration strategy
2. **Factory Interface Design**: Validate DynamicNodeFactory trait design
3. **Test Strategy**: Design comprehensive testing approach for dynamic files
4. **Documentation Plan**: Outline documentation requirements for each phase

### **Long-term Considerations**
1. **Factory Ecosystem**: Consider external factory development and distribution
2. **Performance Optimization**: Plan for advanced caching and optimization strategies
3. **Enterprise Features**: Consider multi-tenant factory isolation
4. **Integration Opportunities**: Evaluate integration with external data sources

---

**Document Status**: ✅ Complete - Ready for Review and Implementation
**Last Updated**: July 25, 2025
**Next Review**: Before Phase 1 implementation begins
