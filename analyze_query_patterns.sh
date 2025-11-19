#!/bin/bash
# Analyze query_records call patterns to identify optimization opportunities
#
# Usage: cargo run ... 2>&1 | tee OUT
#        ./analyze_query_patterns.sh OUT

if [ -z "$1" ]; then
    echo "Usage: $0 <log_file>"
    echo "Example: cargo run ... 2>&1 | tee OUT && ./analyze_query_patterns.sh OUT"
    exit 1
fi

LOG_FILE="$1"

echo "========================================="
echo "QUERY_RECORDS OPTIMIZATION ANALYSIS"
echo "========================================="
echo

# Extract QUERY_TRACE lines
TRACE_FILE="/tmp/query_trace.$$"
grep "^QUERY_TRACE" "$LOG_FILE" > "$TRACE_FILE"

PERF_FILE="/tmp/query_perf.$$"
grep "^QUERY_PERF" "$LOG_FILE" > "$PERF_FILE"

TOTAL_CALLS=$(wc -l < "$TRACE_FILE" | tr -d ' ')
echo "Total query_records calls: $TOTAL_CALLS"
echo

# 1. Calls by caller function
echo "=== Top Callers ==="
awk -F'|' '{print $3}' "$TRACE_FILE" | sort | uniq -c | sort -rn | head -20
echo

# 2. Most queried part_id values
echo "=== Most Queried part_id Values (Top 20) ==="
awk -F'|' '{print $4}' "$TRACE_FILE" | sort | uniq -c | sort -rn | head -20
echo

# 3. Unique part_id count
UNIQUE_PARTS=$(awk -F'|' '{print $4}' "$TRACE_FILE" | sort -u | wc -l | tr -d ' ')
echo "Unique part_id values: $UNIQUE_PARTS"
echo "Average queries per part_id: $(echo "scale=2; $TOTAL_CALLS / $UNIQUE_PARTS" | bc)"
echo

# 4. Duplicate queries (same part_id + node_id combinations)
echo "=== Duplicate Query Patterns (part_id + node_id combos) ==="
awk -F'|' '{print $4 "|" $5}' "$TRACE_FILE" | sort | uniq -c | sort -rn | head -20
echo

# 5. Performance analysis
if [ -s "$PERF_FILE" ]; then
    echo "=== Performance Metrics ==="
    echo "Average query time (ms):"
    awk -F'|' '{sum+=$6; count++} END {printf "  %.2f ms\n", sum/count}' "$PERF_FILE"
    
    echo "Queries by result count:"
    awk -F'|' '{
        if ($5 == 0) empty++;
        else if ($5 <= 10) small++;
        else if ($5 <= 100) medium++;
        else large++;
    } END {
        printf "  Empty (0): %d\n", empty;
        printf "  Small (1-10): %d\n", small;
        printf "  Medium (11-100): %d\n", medium;
        printf "  Large (>100): %d\n", large;
    }' "$PERF_FILE"
    echo
fi

# 6. Sequential duplicate detection (queries immediately repeated)
echo "=== Sequential Duplicate Queries (same part_id+node_id back-to-back) ==="
awk -F'|' '
{
    key = $4 "|" $5
    if (key == prev_key) {
        dups++
    }
    prev_key = key
} 
END {
    printf "  Sequential duplicates: %d (%.1f%% of total)\n", dups, (dups/NR)*100
}' "$TRACE_FILE"
echo

# 7. Temporal clustering (within N calls of each other)
WINDOW=10
echo "=== Temporal Clustering (same query within $WINDOW calls) ==="
awk -F'|' -v window=$WINDOW '
{
    key = $4 "|" $5
    call_num = $2
    
    # Check if this key was seen recently
    if (key in last_seen) {
        gap = call_num - last_seen[key]
        if (gap <= window) {
            clustered++
        }
    }
    last_seen[key] = call_num
} 
END {
    printf "  Queries repeated within %d calls: %d (%.1f%% of total)\n", window, clustered, (clustered/NR)*100
}' "$TRACE_FILE"
echo

# 8. Call depth analysis
echo "=== Call Stack Depth Distribution ==="
awk -F'|' '{print $6}' "$TRACE_FILE" | sort -n | uniq -c | sort -rn | head -10
echo

# Cleanup
rm -f "$TRACE_FILE" "$PERF_FILE"

echo
echo "========================================="
echo "OPTIMIZATION RECOMMENDATIONS"
echo "========================================="
echo
echo "Based on the analysis above, consider:"
echo "1. Add caching for frequently queried part_id+node_id combinations"
echo "2. Batch queries that occur close together in time"
echo "3. Investigate why certain part_ids are queried so many times"
echo "4. Check if sequential duplicates indicate missing caching in callers"
echo "5. Profile the top caller functions for optimization opportunities"
echo
