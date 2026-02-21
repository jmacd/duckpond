#!/bin/bash
# EXPERIMENT: Host cat with format, compression, and entry type variations
# EXPECTED: pond cat reads host files via hostmount with proper format/compression handling
set -e

echo "=== TEST: Hostmount cat variations ==="

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
echo "--- Test 1: Raw file via host+file:// ---"
OUTPUT=$(pond cat "host+file:///$TEST_DIR/readme.txt" 2>&1)
echo "$OUTPUT"
echo "$OUTPUT" | grep -q "Hello from host filesystem" || { echo "FAIL: raw file content mismatch"; exit 1; }
echo "PASS: host+file:// streams raw bytes correctly"

echo ""
echo "--- Test 2: CSV as table via host+csv:// ---"
OUTPUT=$(pond cat --format table "host+csv:///$TEST_DIR/data.csv" 2>&1)
echo "$OUTPUT"
echo "$OUTPUT" | grep -q "alpha" || { echo "FAIL: CSV table should contain alpha"; exit 1; }
echo "$OUTPUT" | grep -q "delta" || { echo "FAIL: CSV table should contain delta"; exit 1; }
echo "PASS: host+csv:// displays CSV as table"

echo ""
echo "--- Test 3: CSV as table with --sql filter ---"
OUTPUT=$(pond cat --sql "SELECT name, value FROM source WHERE value > 15" --format table "host+csv:///$TEST_DIR/data.csv" 2>&1)
echo "$OUTPUT"
echo "$OUTPUT" | grep -q "beta" || { echo "FAIL: SQL WHERE should include beta (20)"; exit 1; }
echo "$OUTPUT" | grep -q "gamma" || { echo "FAIL: SQL WHERE should include gamma (30)"; exit 1; }
if echo "$OUTPUT" | grep -q "alpha"; then
    echo "FAIL: SQL WHERE should exclude alpha (10)"
    exit 1
fi
echo "PASS: host+csv:// with --sql filters correctly"

echo ""
echo "--- Test 4: CSV as series via host+csv+series:// ---"
OUTPUT=$(pond cat --sql "SELECT name, value FROM source ORDER BY timestamp" --format table "host+csv+series:///$TEST_DIR/data.csv" 2>&1)
echo "$OUTPUT"
echo "$OUTPUT" | grep -q "alpha" || { echo "FAIL: series should contain alpha"; exit 1; }
echo "$OUTPUT" | grep -q "delta" || { echo "FAIL: series should contain delta"; exit 1; }
echo "PASS: host+csv+series:// queries series data with SQL"

echo ""
echo "--- Test 5: Zstd-compressed CSV via host+csv+zstd:// ---"
OUTPUT=$(pond cat --sql "SELECT name, value FROM source WHERE value >= 30" --format table "host+csv+zstd:///$TEST_DIR/data.csv.zst" 2>&1)
echo "$OUTPUT"
echo "$OUTPUT" | grep -q "gamma" || { echo "FAIL: zstd CSV should contain gamma (30)"; exit 1; }
echo "$OUTPUT" | grep -q "delta" || { echo "FAIL: zstd CSV should contain delta (40)"; exit 1; }
if echo "$OUTPUT" | grep -q "alpha"; then
    echo "FAIL: SQL WHERE should exclude alpha from zstd CSV"
    exit 1
fi
echo "PASS: host+csv+zstd:// decompresses and queries correctly"

echo ""
echo "--- Test 6: No POND required for host cat ---"
unset POND
OUTPUT=$(pond cat "host+file:///$TEST_DIR/readme.txt" 2>&1)
echo "$OUTPUT" | grep -q "Hello from host filesystem" || { echo "FAIL: host cat should work without POND"; exit 1; }
echo "PASS: host cat works without POND env var"

# Cleanup
rm -rf "$TEST_DIR"

echo ""
echo "=== ALL HOSTMOUNT CAT TESTS PASSED ==="
