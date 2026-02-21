#!/bin/bash
# EXPERIMENT: Copy files OUT of pond to host filesystem via hostmount
# DESCRIPTION: Tests pond copy <pond-path> host:///output for both raw data files
#   and queryable series (parquet). Exercises the dual-steward copy_out path:
#   host steward (write) + pond steward (read), AsyncArrowWriter for parquet export,
#   directory structure preservation, and full roundtrip (out then back in).
# EXPECTED: All exports succeed, parquet files are valid, roundtrip preserves data.
set -e
source check.sh

echo "=== Experiment: Copy Out via Hostmount ==="
echo ""

pond init

# ---------------------------------------------------------------------------
# Setup: create test data in the pond
# ---------------------------------------------------------------------------

# 1. Raw data file
echo "Hello from inside the pond" > /tmp/raw-content.txt
pond copy host:///tmp/raw-content.txt /raw-file.txt

# 2. Synthetic timeseries (creates a queryable dynamic series)
cat > /tmp/synth.yaml << 'YAML'
start: "2024-01-01T00:00:00Z"
end: "2024-01-01T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "6h"
        offset: 20.0
  - name: "pressure"
    components:
      - type: line
        slope: 0.0
        offset: 1013.0
YAML

pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/weather --config-path /tmp/synth.yaml

# Materialize the dynamic series into a physical series file
# by querying and writing it back
pond cat /sensors/weather --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

# 3. Another raw data file in a nested directory
pond mkdir /docs
pond mkdir /docs/readme
echo "# Nested Document" > /tmp/nested.md
pond copy host:///tmp/nested.md /docs/readme/index.md

echo "Setup complete."
echo ""

# ---------------------------------------------------------------------------
# Test 1: Copy out a raw data file
# ---------------------------------------------------------------------------
echo "--- Test 1: Copy out raw data file ---"
OUTPUT_DIR=/tmp/copy-out-test1
mkdir -p "$OUTPUT_DIR"

pond copy /raw-file.txt "host://$OUTPUT_DIR"

check 'test -f "$OUTPUT_DIR/raw-file.txt"' "raw file exists on host"
check 'grep -q "Hello from inside the pond" "$OUTPUT_DIR/raw-file.txt"' "raw file content matches"

echo ""

# ---------------------------------------------------------------------------
# Test 2: Copy out a queryable series (dynamic -> parquet export)
# ---------------------------------------------------------------------------
echo "--- Test 2: Copy out queryable series as parquet ---"
OUTPUT_DIR=/tmp/copy-out-test2
mkdir -p "$OUTPUT_DIR"

pond copy '/sensors/weather' "host://$OUTPUT_DIR"

check 'test -f "$OUTPUT_DIR/sensors/weather"' "series file exists on host"

# Verify it's valid parquet (PAR1 magic bytes at start)
MAGIC=$(head -c4 "$OUTPUT_DIR/sensors/weather" | od -A n -t x1 | tr -d ' ')
check '[ "$MAGIC" = "50415231" ]' "exported file has PAR1 magic (valid parquet)"

# Verify parquet is valid by checking file size (should be meaningful, not empty)
FILE_SIZE=$(stat -c %s "$OUTPUT_DIR/sensors/weather" 2>/dev/null || stat -f %z "$OUTPUT_DIR/sensors/weather")
check '[ "$FILE_SIZE" -gt 100 ]' "exported parquet has non-trivial size ($FILE_SIZE bytes)"

echo ""

# ---------------------------------------------------------------------------
# Test 3: Copy out with nested directory structure preserved
# ---------------------------------------------------------------------------
echo "--- Test 3: Directory structure preservation ---"
OUTPUT_DIR=/tmp/copy-out-test3
mkdir -p "$OUTPUT_DIR"

pond copy '/docs/**/*' "host://$OUTPUT_DIR"

check 'test -f "$OUTPUT_DIR/docs/readme/index.md"' "nested file preserves directory structure"
check 'grep -q "# Nested Document" "$OUTPUT_DIR/docs/readme/index.md"' "nested file content matches"

echo ""

# ---------------------------------------------------------------------------
# Test 4: Copy out multiple files with glob
# ---------------------------------------------------------------------------
echo "--- Test 4: Glob pattern copies multiple files ---"
OUTPUT_DIR=/tmp/copy-out-test4
mkdir -p "$OUTPUT_DIR"

# Copy everything
pond copy '/**/*' "host://$OUTPUT_DIR"

check 'test -f "$OUTPUT_DIR/raw-file.txt"' "glob: raw file exported"
check 'test -f "$OUTPUT_DIR/docs/readme/index.md"' "glob: nested file exported"

echo ""

# ---------------------------------------------------------------------------
# Test 5: Roundtrip — copy out then copy back in, verify data preserved
# ---------------------------------------------------------------------------
echo "--- Test 5: Roundtrip (out then back in) ---"
OUTPUT_DIR=/tmp/copy-out-roundtrip
mkdir -p "$OUTPUT_DIR"

# Export the raw file
pond copy /raw-file.txt "host://$OUTPUT_DIR"

# Re-import into a different pond path
pond mkdir /reimported
pond copy "host://$OUTPUT_DIR/raw-file.txt" /reimported/raw-file.txt

# Verify content matches
ORIGINAL=$(pond cat /raw-file.txt)
REIMPORTED=$(pond cat /reimported/raw-file.txt)
check '[ "$ORIGINAL" = "$REIMPORTED" ]' "roundtrip: raw file content identical"

# Export the series, re-import as a series, verify row count matches
pond copy /sensors/weather "host://$OUTPUT_DIR"

pond copy "host+series:///$OUTPUT_DIR/sensors/weather" /reimported/weather.series

ORIG_COUNT=$(pond cat /sensors/weather \
  --format=table --sql "SELECT COUNT(*) AS cnt FROM source")
REIMP_COUNT=$(pond cat /reimported/weather.series \
  --format=table --sql "SELECT COUNT(*) AS cnt FROM source")
check '[ "$ORIG_COUNT" = "$REIMP_COUNT" ]' "roundtrip: series row count identical"

# Verify specific values survived the roundtrip
ORIG_VALS=$(pond cat /sensors/weather \
  --format=table --sql "SELECT temperature, pressure FROM source ORDER BY timestamp LIMIT 3")
REIMP_VALS=$(pond cat /reimported/weather.series \
  --format=table --sql "SELECT temperature, pressure FROM source ORDER BY timestamp LIMIT 3")
check '[ "$ORIG_VALS" = "$REIMP_VALS" ]' "roundtrip: series values identical"

echo ""

# ---------------------------------------------------------------------------
# Test 6: Copy out with -d (host root override)
# ---------------------------------------------------------------------------
echo "--- Test 6: Copy out with -d flag ---"
OUTPUT_DIR=/tmp/copy-out-test6
mkdir -p "$OUTPUT_DIR"

# Use -d to set host root, so host:///output resolves to $OUTPUT_DIR/output
pond -d "$OUTPUT_DIR" copy /raw-file.txt "host:///output"

check 'test -f "$OUTPUT_DIR/output/raw-file.txt"' "-d flag: file exported under host root"
check 'grep -q "Hello from inside the pond" "$OUTPUT_DIR/output/raw-file.txt"' "-d flag: content matches"

check_finish
