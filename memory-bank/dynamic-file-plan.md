# Dynamic File Support Integration Plan

**Project**: DuckPond TLogFS Dynamic File Support
**Created**: July 25, 2025
**Status**: Planning Phase Complete - Ready for Implementation
**Priority**: Medium - Advanced feature with solid foundation

## 📋 **Executive Summary**

This document outlines a minimum viable plan to integrate dynamic file support into TLogFS, focusing on a single dynamic node type: the hostmount dynamic directory. The initial implementation will provide a CLI `mknod` command for creating a dynamic directory that mounts a host directory into the pond, managed by a simple `hostmount` factory. Advanced dynamic nodes for translating CSV and SQL queries are deferred for future work.

**Persistence Model Note**: When a dynamic node is created and
persisted, its configuration (YAML) is stored in the `content` field
of the associated OplogEntry. To create a dynamic node, you need its
configuration YAML, which is then stored in the content field. This
ensures dynamic node configuration is persisted and versioned in the
same way as static file content.

## 🔍 **Research Findings**

The original duckpond implementation and TinyFS provide a foundation for dynamic file integration. For the minimum viable solution, we will leverage the existing TinyFS `Directory` trait and TLogFS `OplogEntry` schema to support a single dynamic directory type: hostmount. This type will expose a host directory as a read-only dynamic directory in the pond, with configuration stored as metadata.

## 🏗️ **Architecture Design**

### **Schema Extension**
Add a `factory` string column to the OplogEntry schema to distinguish between static and dynamic content. For hostmount dynamic directories, the factory will be set to `hostmount` and the content field will store the configuration metadata (YAML or JSON encoded).

### **Hostmount Dynamic Directory Architecture**

Hostmount is the first example of a dynamic node.

Implement a complete dynamic directory type, `hostmount`, which exposes a host directory as a read-only dynamic directory in the pond with full traversal support. The configuration consists of a single field:
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

### **Complete Traversal Behavior**
- **Dynamic Files**: Host files are exposed as `HostmountFile` nodes that read actual host file content on demand
- **Dynamic Subdirectories**: Host subdirectories are exposed as nested `HostmountDirectory` nodes with the same traversal capabilities
- **Read-Only Access**: All dynamic nodes are strictly read-only and reflect the current state of the host filesystem
- **Ephemeral Node IDs**: Node IDs are generated dynamically and are not persisted in the Oplog
- **Live Reflection**: Directory contents and file data are refreshed on each access

### **Dynamic Node Types**

#### **HostmountFile** 

This is a dynamic implementation of File that reads the underlying host file system.

#### **HostmountDirectory**

This is a dynamic implementation of File that reads the underlying host file system.

When accessed, it will return HostmountFile and HostmountDirectory
objects. Ignore host symlinks.

**August 2, 2025** 
Status: HostmountDirectory is implemented.


#### SqlDerivedSeriesFactory

This is an example of a dynamic node that derives a new series from an existing
file:series node.

We expect this to use datafusion.

##### **Example Usage Scenarios**

**1. Simple Aggregation**:
**2. Filtered Views**:
**3. Recursive Derivation**:

##### **DataFusion Integration Benefits**

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

**Support tables and series**: Tinyfs file:table and file:series data should both be supported.

This case can use time-ranges to restrict access to a series; caching
mechanisms must be time-range aware for file:series.

#### **CsvDirectoryFactory**

Create dynamic directories that discover CSV files and present them as
converted Parquet file:table entries. These will use tinyfs paths to 
refer to other files in the pond. 

The existing CLI feature which converts CSV to Parquet on import will 
removed in favor of this approach: store original CSV files as file:data
nodes, convert them using dynamic derivation.

#### **Dynamic Directory Architecture**

**Key Architectural Elements**:

1. **Transient Dynamic Entries**: CsvDirectoryFactory creates dynamic file entries that exist only in memory, not persisted to TLogFS
2. **Pattern-Based Discovery**: Uses glob patterns to discover source CSV files at access time
3. **Materialization Caching**: Since CSV files don't support predicate pushdown, cache converted Parquet for performance
4. **Factory Composition**: One factory creates directories containing files from another factory
5. **Type Conversion**: Converts file:data (CSV) to file:table (Parquet) transparently

##### **Performance Characteristics**

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

### **Phase 1: Minimum Viable Dynamic Directory (COMPLETED ✅)**
**Objective**: Implement hostmount dynamic directory and CLI mknod command

**Deliverables**:
- [x] Add `factory` string column to OplogEntry schema
- [x] Implement basic hostmount dynamic directory type  
- [x] Add CLI `mknod` command for creating hostmount dynamic directories
- [x] Validate and store configuration metadata in content field
- [x] Basic directory listing shows hostmount entries

**Status**: ✅ COMPLETED - `./test.sh` works and lists hostmount dynamic directory


### **Phase 1.5: Complete Hostmount Traversal (COMPLETED ✅)**
**Objective**: Enable full file reading and subdirectory traversal within hostmount directories

**Status**: ✅ All hostmount traversal steps completed. Host files and subdirectories are now fully accessible and reflect the live host filesystem. All mutation operations are rejected and error handling is in place.

**Critical Deliverables**:
- [x] Implement `HostmountFile` struct with actual host file reading capabilities
- [x] Update `HostmountDirectory.get()` to create `HostmountFile` nodes for files
- [x] Update `HostmountDirectory.get()` to create nested `HostmountDirectory` nodes for subdirectories  
- [x] Update `HostmountDirectory.entries()` with same dynamic node creation
- [x] Add comprehensive tests for file reading and subdirectory traversal
- [x] Ensure read-only behavior for all dynamic nodes

**Success Criteria**:
- Can read actual content from files within hostmount directory
- Can traverse subdirectories recursively
- All host filesystem changes reflected immediately
- All mutation operations properly rejected
- Error handling for missing/inaccessible host paths


### **Phase 2: SQL-Derived Tables and Series (NEXT MILESTONE)**
**Objective**: Implement SQL-derived tables and series using DataFusion and dynamic node factories

**Deliverables**:
- [ ] SqlDerivedSeriesFactory for SQL-derived time series
- [ ] SqlDerivedTableFactory for SQL-derived tables
- [ ] CsvDirectoryFactory for automatic CSV to Parquet conversion
- [ ] Materialization cache for performance optimization
- [ ] DataFusion integration for predicate pushdown

### **Phase 3: Dynamic Directory Factories (FUTURE ⏳)**  
**Objective**: Implement CsvDirectory and CsvFile dynamic node types

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


## 🚀 **Immediate Next Steps (Phase 2: SQL-Derived Tables and Series)**

With hostmount traversal fully implemented and tested, the next actionable milestone is SQL-derived tables and series. This phase will introduce dynamic nodes that leverage DataFusion for SQL queries, support for derived time series and tables, and CSV-to-Parquet conversion with materialization caching.

**Key Next Steps:**
- Design and implement `SqlDerivedSeriesFactory` and `SqlDerivedTableFactory` for dynamic SQL-based nodes
- Integrate DataFusion for query execution and predicate pushdown
- Implement `CsvDirectoryFactory` for automatic CSV-to-Parquet conversion
- Add materialization cache for performance
- Develop comprehensive tests for SQL-derived node creation, query, and caching

**Document Status**: ✅ Hostmount phase complete; SQL-derived milestone is next
**Last Updated**: August 2, 2025
**Approval Note**: This plan is approved. Implementation may begin immediately.

**Checklist: Steps to Ensure Complete Hostmount Traversal**
1. ✅ Create hostmount dynamic directory entry in TLogFS (COMPLETED)
2. ✅ Implement basic HostmountDirectory for directory listings (COMPLETED)
3. ✅ Implement HostmountFile for reading actual host file content (COMPLETED)
4. ✅ Update HostmountDirectory.get() to return proper dynamic nodes (COMPLETED)
5. ✅ Update HostmountDirectory.entries() to return proper dynamic nodes (COMPLETED)
6. ✅ Ensure nested subdirectories create new HostmountDirectory instances (COMPLETED)
7. ✅ Add error handling for missing/inaccessible host files and directories
8. ✅ Add tests for file reading, subdirectory traversal, and error cases
9. ✅ Document complete hostmount traversal capabilities

---

**Next Milestone: SQL-Derived Tables and Series**
The next phase will focus on implementing SQL-derived dynamic nodes, including SQL-derived tables and series, leveraging DataFusion for query support and predicate pushdown. CsvDirectoryFactory and materialization caching will also be introduced.
