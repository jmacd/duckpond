# SQL-Derived Dynamic Node Factories - Design Document

This document contains the detailed design and architectural considerations for the SQL-derived dynamic node factories in TLogFS.

## Architecture Overview

Both factories execute arbitrary SQL queries against TLogFS data sources and present the results as Parquet files. The key difference is in their data model expectations and pattern matching behavior.

## Current Implementation

Each SQL-derived node operates independently:
1. Creates a fresh DataFusion `SessionContext`
2. Loads source data into `MemTable` instances  
3. Executes the configured SQL query
4. Materializes results as Parquet bytes

## Predicate Pushdown Limitations

**Problem**: DataFusion cannot optimize across SQL-derived node boundaries.

Consider a chain: `Source → SQL Node A → SQL Node B → Query`

### What Happens Now
- DataFusion sees each node as an independent `MemTable`
- No filter pushdown through `MemTable` (returns `Unsupported`)
- Full materialization at each stage
- Each stage processes all intermediate data

### What Could Happen with True Pushdown
```
Query: SELECT * FROM node_b WHERE final_value > 1000

Rewrite chain:
Level 2: WHERE (adjusted_value * 2) > 1000 -> WHERE adjusted_value > 500
Level 1: WHERE (value + 50) > 500 -> WHERE value > 450  
Level 0: Parquet scan with WHERE value > 450

Result: Only qualifying rows read from storage, ~90% I/O reduction possible
```

## Performance Characteristics

### Current (Materialized) Approach
- ✅ **Predictable**: Each transformation is explicit and debuggable
- ✅ **Isolated**: Node failures don't cascade through complex optimizer chains
- ✅ **Cacheable**: Intermediate results can be inspected and reused
- ❌ **I/O Heavy**: Reads all source data regardless of final predicates
- ❌ **Memory Heavy**: Must hold full intermediate results in MemTable
- ❌ **Storage Heavy**: Intermediate results consume disk space

### Hypothetical Optimized Approach  
- ✅ **Efficient**: Minimal I/O through predicate pushdown
- ✅ **Scalable**: Memory usage proportional to final result size
- ❌ **Complex**: Sophisticated query rewriting and error handling
- ❌ **Fragile**: Optimization failures could break entire chains
- ❌ **Opaque**: Harder to debug when optimizations go wrong

# Implementation Strategy for Predicate Pushdown

## Phase 1: Foundation (4-6 weeks)

### Custom TableProvider with Basic Pushdown
```rust
struct SqlDerivedTableProvider {
    config: SqlDerivedConfig,
    upstream: Arc<dyn TableProvider>,
}

impl TableProvider for SqlDerivedTableProvider {
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Analyze each filter for pushdown compatibility
        filters.iter().map(|filter| {
            if self.can_push_through_query(filter) {
                Ok(TableProviderFilterPushDown::Exact)
            } else {
                Ok(TableProviderFilterPushDown::Inexact) // Apply after scan
            }
        }).collect()
    }
    
    async fn scan(&self, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
        let rewritten_filters = self.rewrite_filters_for_upstream(filters)?;
        let upstream_plan = self.upstream.scan(projection, &rewritten_filters, limit).await?;
        Ok(Arc::new(SqlDerivedExecutionPlan::new(upstream_plan, &self.config.query)))
    }
}
```

### Simple Predicate Rewriting
Handle basic cases first:
- Column renaming: `WHERE new_col > 5` → `WHERE old_col > 5`
- Arithmetic inverse: `WHERE (col + 10) > 50` → `WHERE col > 40`
- Multiplication inverse: `WHERE (col * 2) > 100` → `WHERE col > 50`

## Phase 2: Advanced Rewriting (8-12 weeks)

### Complex Expression Analysis
```rust
trait PredicateRewriter {
    fn can_push_predicate(&self, predicate: &Expr, query: &str) -> bool;
    fn rewrite_predicate(&self, predicate: &Expr, query: &str) -> Result<Option<Expr>>;
}

struct ArithmeticRewriter;  // Handle +, -, *, /
struct ProjectionRewriter;  // Handle column selection and renaming
struct FilterRewriter;      // Handle WHERE clause combination
struct LimitRewriter;       // Handle LIMIT pushdown
```

### Query Parsing and Analysis
- Parse SQL queries to identify transformations
- Build dependency graphs between input and output columns
- Determine which predicates can be safely pushed down
- Handle aggregations, joins, window functions (non-pushable cases)

## Phase 3: Integration and Optimization (4-6 weeks)

### Unified Session Management
- Share `SessionContext` across related SQL-derived nodes
- Maintain logical plan trees for optimization
- Implement lazy evaluation with materialization fallback

### Performance Optimization
- Cache predicate rewriting results
- Optimize for common query patterns
- Implement cost-based decisions for materialization vs. pushdown

## Alternative Approaches

### 1. View-Based Implementation (Lower Complexity)
Register SQL-derived nodes as DataFusion views:
```rust
ctx.register_table("source", upstream_provider)?;
ctx.create_view("derived", &config.query)?;
// Let DataFusion handle optimization automatically
```
**Trade-off**: Less control, potential memory issues with large datasets

### 2. Streaming Pipeline (Medium Complexity)  
Process data in batches without full materialization:
```rust
struct StreamingSqlNode {
    upstream: SendableRecordBatchStream,
    query_executor: Arc<dyn BatchProcessor>,
}
```
**Trade-off**: More complex transaction and error handling

## Compatibility Analysis

The current materialized approach aligns well with DuckPond's philosophy:
- **Explicit transformations**: Each step is clearly defined and inspectable
- **Fail-fast behavior**: Errors are localized to individual nodes
- **Debuggability**: Intermediate results can be examined and validated

Adding predicate pushdown would require careful design to preserve these benefits while providing significant performance improvements for query-heavy workloads.

**Recommendation**: Start with Phase 1 for simple cases, then evaluate based on real-world usage patterns whether the complexity of advanced optimization is justified.

## MemTable Design Rationale

### Why MemTable is Used

MemTable is the simplest way to get Parquet data into DataFusion:
- **Easy API**: `MemTable::try_new(schema, batches)` - just pass Arrow RecordBatches
- **No file dependencies**: Data is loaded into memory, no external file coordination needed
- **Immediate availability**: No I/O during query execution after initial load
- **DataFusion native**: Built-in TableProvider, well-tested and reliable

### The Critical Limitation

**MemTable prevents predicate pushdown optimization!**

MemTable uses the default `TableProvider::supports_filters_pushdown()` implementation:
```rust
fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
}
```

This means:
- **No filter pushdown**: All filters are applied AFTER loading all data into memory
- **Memory bloat**: Even if query only needs 1% of data, 100% gets loaded
- **I/O waste**: All source data is read regardless of final predicates
- **Chain optimization blocked**: Cannot push predicates across SQL-derived node boundaries

### Alternative Approaches

#### 1. Direct Parquet TableProvider (Better for large datasets)
```rust
// Instead of: MemTable::try_new(schema, batches)
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
let listing_table = ListingTable::try_new(config)?; // Supports filter pushdown!
```

#### 2. Custom TableProvider with Pushdown Support
```rust
struct TinyFsTableProvider {
    file_path: String,
    tinyfs_context: FactoryContext,
}

impl TableProvider for TinyFsTableProvider {
    fn supports_filters_pushdown(&self, filters: &[&Expr]) 
        -> Result<Vec<TableProviderFilterPushDown>> {
        // Analyze filters and return Exact/Inexact as appropriate
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
    
    async fn scan(&self, projection: Option<&Vec<usize>>, 
                  filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        // Read TinyFS file with pushed-down filters applied
        // Only load data that survives the predicates
    }
}
```

### Why We Still Use MemTable (For Now)

Despite the limitations, MemTable aligns with DuckPond's current philosophy:
- **Explicit materialization**: Each transformation step is clearly defined
- **Debuggable**: Intermediate results can be inspected
- **Simple**: No complex optimization chains that could fail subtly
- **Works**: Handles small-to-medium datasets reliably

The performance cost is predictable and acceptable for many use cases.
