#!/bin/bash
# EXPERIMENT: Hostmount overlay -- mount a dynamic-dir factory onto host filesystem
# EXPECTED: `--hostmount /reduced=host+dyndir:///reduce.yaml` mounts a dynamic-dir
#           at /reduced in the host TinyFS namespace. The factory reads host CSV data,
#           and its computed children are visible via `pond list` and queryable via
#           `pond cat`. No pond required.
set -e
source check.sh

echo "=== Experiment: Hostmount Overlay with Dynamic Dir ==="

HOST_ROOT="/tmp/overlay-test"

# Clean up from any prior run
rm -rf "${HOST_ROOT}"
mkdir -p "${HOST_ROOT}"

# ==============================================================================
# Step 1: Create CSV source data on host filesystem
# ==============================================================================

echo ""
echo "--- Step 1: Create CSV source data ---"

# Simple sensor data: 12 rows of hourly temperature readings
cat > "${HOST_ROOT}/sensor.csv" << 'CSV'
timestamp,temperature,pressure
2024-06-15T00:00:00Z,20.0,1013.0
2024-06-15T01:00:00Z,18.5,1013.2
2024-06-15T02:00:00Z,17.0,1013.1
2024-06-15T03:00:00Z,16.0,1012.9
2024-06-15T04:00:00Z,15.5,1013.0
2024-06-15T05:00:00Z,16.0,1013.3
2024-06-15T06:00:00Z,17.5,1013.5
2024-06-15T07:00:00Z,19.0,1013.4
2024-06-15T08:00:00Z,21.0,1013.2
2024-06-15T09:00:00Z,23.0,1013.0
2024-06-15T10:00:00Z,24.5,1012.8
2024-06-15T11:00:00Z,25.0,1012.9
CSV

echo "Source CSV created: $(wc -l < "${HOST_ROOT}/sensor.csv") lines"

# ==============================================================================
# Step 2: Create dynamic-dir config (reads host CSV via format provider)
# ==============================================================================

echo ""
echo "--- Step 2: Create dynamic-dir config ---"

# The dynamic-dir wraps a sql-derived-table that reads the host CSV
# and computes summary statistics.
cat > "${HOST_ROOT}/reduce.yaml" << 'YAML'
entries:
  - name: "summary"
    factory: "sql-derived-table"
    config:
      patterns:
        source: "csv:///sensor.csv"
      query: >-
        SELECT
          MIN(temperature) AS temp_min,
          MAX(temperature) AS temp_max,
          AVG(temperature) AS temp_avg,
          COUNT(*) AS row_count
        FROM source
  - name: "recent"
    factory: "sql-derived-table"
    config:
      patterns:
        source: "csv:///sensor.csv"
      query: >-
        SELECT timestamp, temperature, pressure
        FROM source
        ORDER BY timestamp DESC
        LIMIT 3
YAML

echo "Dynamic-dir config created"
cat "${HOST_ROOT}/reduce.yaml"

# ==============================================================================
# Step 3: Verify host files are visible without overlay
# ==============================================================================

echo ""
echo "--- Step 3: Verify host files without overlay ---"

unset POND
LIST_PLAIN=$(pond list -d "${HOST_ROOT}" "host+file:///*" 2>&1)
echo "$LIST_PLAIN"

# ==============================================================================
# Step 4: List with overlay mount -- factory children should appear
# ==============================================================================

echo ""
echo "--- Step 4: List with --hostmount overlay ---"

LIST_OVERLAY=$(pond list -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+file:///*" 2>&1)
echo "Root listing:"
echo "$LIST_OVERLAY"

# List the overlay directory contents
echo ""
echo "Children of /computed:"
LIST_CHILDREN=$(pond list -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+file:///computed/*" 2>&1)
echo "$LIST_CHILDREN"

# ==============================================================================
# Step 5: Query the factory-generated files through the overlay
# ==============================================================================

echo ""
echo "--- Step 5: Query overlay factory files ---"

echo ""
echo "Summary (aggregated from host CSV):"
SUMMARY=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+file:///computed/summary" \
  --format=table 2>&1)
echo "$SUMMARY"

echo ""
echo "Recent rows (last 3 from host CSV):"
RECENT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+file:///computed/recent" \
  --format=table 2>&1)
echo "$RECENT"

# ==============================================================================
# Step 6: Query with SQL through the overlay
# ==============================================================================

echo ""
echo "--- Step 6: SQL query through overlay ---"

SQL_RESULT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+file:///computed/summary" \
  --format=table \
  --sql "SELECT temp_min, temp_max, row_count FROM source" 2>&1)
echo "$SQL_RESULT"

# ==============================================================================
# Step 7: Verify original host files still accessible through overlay
# ==============================================================================

echo ""
echo "--- Step 7: Host files still accessible through overlay ---"

HOST_CAT=$(pond cat -d "${HOST_ROOT}" \
  --hostmount "/computed=host+dyndir:///reduce.yaml" \
  "host+csv:///sensor.csv" \
  --format=table \
  --sql "SELECT COUNT(*) AS cnt FROM source" 2>&1)
echo "$HOST_CAT"

# ==============================================================================
# Verification
# ==============================================================================

echo ""
echo "--- Verification ---"

# Host files visible
check 'echo "$LIST_PLAIN" | grep -q "sensor.csv"'      "host file sensor.csv visible without overlay"
check 'echo "$LIST_PLAIN" | grep -q "reduce.yaml"'     "host file reduce.yaml visible without overlay"

# Overlay directory visible
check 'echo "$LIST_OVERLAY" | grep -q "computed"'       "overlay directory /computed appears in listing"
check 'echo "$LIST_OVERLAY" | grep -q "sensor.csv"'     "host files still visible with overlay"

# Factory children visible
check 'echo "$LIST_CHILDREN" | grep -q "summary"'       "factory child 'summary' visible in /computed"
check 'echo "$LIST_CHILDREN" | grep -q "recent"'        "factory child 'recent' visible in /computed"

# Summary query results
check 'echo "$SUMMARY" | grep -q "temp_min"'            "summary output contains temp_min column"
check 'echo "$SUMMARY" | grep -q "15.5"'                "summary temp_min is 15.5"
check 'echo "$SUMMARY" | grep -q "25.0"'                "summary temp_max is 25.0 (or 25)"
check 'echo "$SUMMARY" | grep -q "12"'                  "summary row_count is 12"

# Recent query results
check 'echo "$RECENT" | grep -q "temperature"'          "recent output contains temperature column"
check 'echo "$RECENT" | grep -q "2024-06-15T11"'        "recent shows latest timestamp"

# SQL query works
check 'echo "$SQL_RESULT" | grep -q "15.5"'             "SQL query returns temp_min"
check 'echo "$SQL_RESULT" | grep -q "12"'               "SQL query returns row_count"

# Host files accessible through overlay
check 'echo "$HOST_CAT" | grep -q "12"'                 "host CSV still queryable with overlay active"

# Cleanup
rm -rf "${HOST_ROOT}"

check_finish
