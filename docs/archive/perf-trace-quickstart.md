# Performance Tracing Quick Start

## Collect Performance Traces

Run your tests with stderr redirected to capture traces:

```bash
# Run all tests
cargo test 2> perf_trace.log

# Or run specific test
cargo test test_query_directory_entry 2> perf_trace.log

# Or run with RUST_LOG for additional context
RUST_LOG=debug cargo test 2> perf_trace.log 1> test_output.log
```

## Analyze the Traces

```bash
./analyze_perf_traces.sh perf_trace.log
```

This will show:
- Top functions by call count
- Slowest calls
- Call patterns (which functions call which)
- Potential redundant queries (same params called multiple times)
- Performance breakdown for query operations
- Cache hit opportunities

## Example Output

```
===================================
Performance Analysis Report
===================================
Total traces: 1523

=== Top 10 Functions by Call Count ===
    856 query_records
    234 query_single_directory_entry
    145 load_node
     89 query_directory_entries

=== Potential Redundant Calls ===
  12 calls: query_records|caller=query_directory_entries|part_id=abc123|node_id=abc123
   8 calls: query_single_directory_entry|caller=load_directory_entries|part_id=root|entry_name=data
   
=== Cache Hit Opportunities ===
Signatures called 3+ times (cache candidates):
  12 calls: query_records|caller=query_directory_entries|part_id=abc123|node_id=abc123
      Call numbers: #42 #43 #44 #45 #56 #57 #58 #67 #68 #69 #70 #71
```

## Common Issues to Look For

### 1. Redundant Queries
If you see the same function called multiple times with identical parameters:
```
5 calls: query_records|part_id=abc|node_id=abc
```
**Fix**: Add transaction-level caching

### 2. Sequential Duplicates
If call numbers are consecutive (#42, #43, #44):
```
Call numbers: #42 #43 #44
```
**Fix**: The caller should cache the result instead of re-querying

### 3. Directory Entry Lookups
Many `query_single_directory_entry` calls:
```
234 query_single_directory_entry
```
**Fix**: Consider batch loading directory entries or caching

## Real-World Example

After finding this pattern:
```
PERF_TRACE|42|query_single_directory_entry|caller=exists_node|part_id=root|entry_name=data|elapsed_ms=15|query_ms=12|found=1
PERF_TRACE|43|query_single_directory_entry|caller=load_node|part_id=root|entry_name=data|elapsed_ms=14|query_ms=12|found=1
```

We discovered `exists_node` and `load_node` were both querying for the same entry. The fix was to check existence as part of the load operation rather than as a separate query.

## Viewing Raw Traces

Extract just the performance traces:
```bash
grep '^PERF_TRACE' perf_trace.log > traces.txt
```

View a specific call:
```bash
grep 'PERF_TRACE|42|' perf_trace.log
```

Find all calls from a specific caller:
```bash
grep 'caller=load_node' traces.txt
```

## Next Steps

See [performance-tracing.md](performance-tracing.md) for:
- Detailed API documentation
- Custom analysis scripts
- Integration with existing code
- Performance impact analysis
