# Query Records Performance Analysis

## Problem

The `query_records()` function in `crates/tlogfs/src/persistence.rs` was being called over 12,000 times during a single export operation - 1-2 orders of magnitude more than the number of files/versions. This indicates excessive repetition and missing caching opportunities.

## Instrumentation

The code now includes automatic tracing that logs:

1. **QUERY_TRACE** - Every query_records call with:
   - Call number (sequential counter)
   - Caller function name (extracted from backtrace)
   - part_id being queried
   - node_id being queried

2. **QUERY_PERF** - Performance metrics for each call:
   - Number of committed records from Delta Lake
   - Number of pending records from memory
   - Total records returned
   - Query time in milliseconds
   - Memory scan time in microseconds

## Usage

### 1. Run your operation and capture output:

```bash
# Example: Export operation
cargo run --bin your_binary -- your_args 2>&1 | tee OUT
```

### 2. Analyze the patterns:

```bash
./analyze_query_patterns.sh OUT
```

This will show:

- **Total calls** - How many times query_records was invoked
- **Top callers** - Which functions call query_records most
- **Most queried part_ids** - Which files/parts are queried repeatedly
- **Duplicate query patterns** - Same part_id+node_id queried multiple times
- **Sequential duplicates** - Back-to-back identical queries (immediate caching opportunity)
- **Temporal clustering** - Queries repeated within N calls (batching opportunity)
- **Performance metrics** - Query times and result sizes
- **Call depth distribution** - Stack depth patterns

### 3. Example Analysis Output:

```
Total query_records calls: 12168

=== Top Callers ===
   8456 load_node
   2134 exists_node
    892 metadata
    686 list_file_versions

=== Most Queried part_id Values ===
    245 abc123...  
    198 def456...
    142 ghi789...

Sequential duplicates: 3421 (28.1% of total)
Queries repeated within 10 calls: 5892 (48.4% of total)
```

## Optimization Strategies

Based on the analysis, consider:

### 1. **Add Per-Request Caching**
If you see high sequential duplicates (>10%), add a simple HashMap cache:

```rust
// In the request/transaction scope
let mut query_cache: HashMap<(NodeID, NodeID), Vec<OplogEntry>> = HashMap::new();

// In query_records or wrapper
if let Some(cached) = query_cache.get(&(part_id, node_id)) {
    return Ok(cached.clone());
}
let result = self.query_records(part_id, node_id).await?;
query_cache.insert((part_id, node_id), result.clone());
```

### 2. **Batch Related Queries**
If temporal clustering is high (>30%), batch queries:

```rust
async fn query_records_batch(
    &self,
    queries: Vec<(NodeID, NodeID)>,
) -> Result<HashMap<(NodeID, NodeID), Vec<OplogEntry>>> {
    // Single SQL query with IN clause or UNION
    let sql = format!(
        "SELECT * FROM delta_table WHERE (part_id, node_id) IN (...) ORDER BY timestamp DESC"
    );
    // ...
}
```

### 3. **Investigate Top Callers**
If a single function dominates (>50%), optimize that specific function:
- Does it need to call query_records for every item in a loop?
- Can it query once and filter/partition in memory?
- Is it missing internal caching?

### 4. **Add Lazy Loading**
If many queries return 0 results (check QUERY_PERF):
- Add exists_node_fast() that just checks COUNT(*)
- Don't load full records until needed

### 5. **Profile the Queries**
If average query time is high (>10ms):
- Add indexes on (part_id, node_id, timestamp)
- Check if ORDER BY is needed for all callers
- Consider materialized views for hot paths

## Disabling Tracing

The tracing output goes to stderr and has minimal performance impact, but to disable it:

1. Comment out the `eprintln!` lines in `query_records()`
2. Or redirect stderr: `cargo run ... 2>/dev/null`

## Future Work

Consider adding:
- Configurable cache sizes and eviction policies
- Query result streaming for large result sets
- Prefetching for predictable access patterns
- Read-through cache with TTL for hot data
