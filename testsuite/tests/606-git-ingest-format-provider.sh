#!/bin/bash
# EXPERIMENT: Git-ingest files readable by format providers (csv://)
# DESCRIPTION: Reproduce the bug where gitpond's dynamic FileDynamic nodes
#              could not be read by format providers like csv:// or excelhtml://.
#              The fix is in ensure_url_cached which now handles dynamic files
#              by reading via async_reader instead of querying the persistence
#              layer for version records.
# EXPECTED: After git-ingest pull, a CSV file served by gitpond can be read
#           via cat --sql with csv:// URL, producing query results.
set -e

echo "=== Experiment: Git-Ingest + CSV Format Provider ==="

# --- Create a local git repo with CSV data -----------------------------------
REPO_DIR=/tmp/test-csv-repo
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR/data"
cd "$REPO_DIR"
git init -b main
git config user.email "test@example.com"
git config user.name "Test User"

cat > data/sensors.csv << 'CSV'
timestamp,temperature,humidity
2024-01-01T00:00:00Z,20.5,45.0
2024-01-01T01:00:00Z,21.0,44.5
2024-01-01T02:00:00Z,19.8,46.2
2024-01-01T03:00:00Z,22.1,43.0
2024-01-01T04:00:00Z,20.0,47.8
CSV

git add -A
git commit -m "Add sensor CSV data"
echo "Created test repo at $REPO_DIR"
cd /

# --- Initialize pond and create git-ingest factory ---------------------------
pond init

cat > /tmp/git-csv.yaml << EOF
url: file://${REPO_DIR}
ref: main
prefix: data
EOF

pond mknod git-ingest /sensors --config-path /tmp/git-csv.yaml
echo "Created git-ingest factory node at /sensors"

# --- Pull from git -----------------------------------------------------------
RUST_LOG=info pond run /sensors pull
echo "Pull complete"

# --- Verify the CSV file is listed -------------------------------------------
echo ""
echo "=== Listing /sensors/ ==="
pond list /sensors/

# --- Read raw content to verify it's accessible ------------------------------
echo ""
echo "=== Raw content of /sensors/sensors.csv ==="
RAW=$(pond cat /sensors/sensors.csv)
echo "$RAW"

if echo "$RAW" | grep -q "timestamp,temperature,humidity"; then
    echo "Raw CSV content: CORRECT"
else
    echo "Raw CSV content: MISSING HEADER"
    exit 1
fi

# --- Read CSV through format provider (the bug we fixed) ---------------------
echo ""
echo "=== SQL query via csv:// format provider ==="
RESULT=$(pond cat --sql "SELECT temperature, humidity FROM source ORDER BY temperature LIMIT 3" csv:///sensors/sensors.csv)
echo "$RESULT"

# Verify we got data back (not "No versions found" error)
if echo "$RESULT" | grep -q "19.8"; then
    echo "CSV format provider query: CORRECT (found 19.8)"
else
    echo "CSV format provider query: FAILED (expected temperature 19.8)"
    exit 1
fi

if echo "$RESULT" | grep -q "20.0"; then
    echo "CSV format provider query: CORRECT (found 20.0)"
else
    echo "CSV format provider query: FAILED (expected temperature 20.0)"
    exit 1
fi

echo ""
echo "=== Experiment Complete ==="
