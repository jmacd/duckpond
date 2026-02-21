#!/bin/bash
# EXPERIMENT: Host cat with format, compression, and entry type variations
# EXPECTED: pond cat reads host files via hostmount with proper format/compression handling
set -e
source check.sh

echo "=== Experiment: Hostmount cat variations ==="

# Install zstd for compression test (not in base image)
apt-get update -qq > /dev/null 2>&1
apt-get install -y --no-install-recommends zstd > /dev/null 2>&1

# Create test data directory
TEST_DIR=/tmp/hostmount-cat-test
mkdir -p "$TEST_DIR"

# Raw text file
echo "Hello from host filesystem" > "$TEST_DIR/readme.txt"

# CSV file (for table and series tests)
cat > "$TEST_DIR/data.csv" << 'EOF'
name,value,timestamp
alpha,10,2024-01-01T00:00:00Z
beta,20,2024-01-02T00:00:00Z
gamma,30,2024-01-03T00:00:00Z
delta,40,2024-01-04T00:00:00Z
EOF

# Zstd-compressed CSV
zstd -q "$TEST_DIR/data.csv" -o "$TEST_DIR/data.csv.zst"

echo ""
echo "--- Executing tests ---"

OUT_RAW=$(pond cat "host+file:///$TEST_DIR/readme.txt" 2>&1)
echo "raw: $OUT_RAW"

OUT_CSV=$(pond cat --format table "host+csv:///$TEST_DIR/data.csv" 2>&1)
echo "csv table:"
echo "$OUT_CSV"

OUT_SQL=$(pond cat --sql "SELECT name, value FROM source WHERE value > 15" --format table "host+csv:///$TEST_DIR/data.csv" 2>&1)
echo "csv+sql:"
echo "$OUT_SQL"

OUT_SERIES=$(pond cat --sql "SELECT name, value FROM source ORDER BY timestamp" --format table "host+csv+series:///$TEST_DIR/data.csv" 2>&1)
echo "csv+series:"
echo "$OUT_SERIES"

OUT_ZSTD=$(pond cat --sql "SELECT name, value FROM source WHERE value >= 30" --format table "host+csv+zstd:///$TEST_DIR/data.csv.zst" 2>&1)
echo "csv+zstd:"
echo "$OUT_ZSTD"

unset POND
OUT_NOPOND=$(pond cat "host+file:///$TEST_DIR/readme.txt" 2>&1)

# Cleanup
rm -rf "$TEST_DIR"

echo ""
echo "--- Verification ---"

check 'echo "$OUT_RAW" | grep -q "Hello from host filesystem"'  "host+file:// streams raw bytes"

check 'echo "$OUT_CSV" | grep -q "alpha"'  "host+csv:// contains alpha"
check 'echo "$OUT_CSV" | grep -q "delta"'  "host+csv:// contains delta"

check 'echo "$OUT_SQL" | grep -q "beta"'   "host+csv:// SQL includes beta (20)"
check 'echo "$OUT_SQL" | grep -q "gamma"'  "host+csv:// SQL includes gamma (30)"
check '! echo "$OUT_SQL" | grep -q "alpha"' "host+csv:// SQL excludes alpha (10)"

check 'echo "$OUT_SERIES" | grep -q "alpha"'  "host+csv+series:// contains alpha"
check 'echo "$OUT_SERIES" | grep -q "delta"'  "host+csv+series:// contains delta"

check 'echo "$OUT_ZSTD" | grep -q "gamma"'    "host+csv+zstd:// contains gamma (30)"
check 'echo "$OUT_ZSTD" | grep -q "delta"'    "host+csv+zstd:// contains delta (40)"
check '! echo "$OUT_ZSTD" | grep -q "alpha"'  "host+csv+zstd:// excludes alpha"

check 'echo "$OUT_NOPOND" | grep -q "Hello from host filesystem"'  "host cat works without POND env var"

check_finish
