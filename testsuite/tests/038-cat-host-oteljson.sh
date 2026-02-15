#!/bin/bash
# EXPERIMENT: Cat host+oteljson:// without a pond
# DESCRIPTION: Verify that pond cat host+oteljson:///path works without
#   POND env or pond init. This reads local files directly through the
#   oteljson format provider and DataFusion, enabling quick ad-hoc
#   queries of OTLP JSON Lines files from the host filesystem.
#
set -e

echo "=== Experiment: Cat Host+OtelJSON Without Pond ==="
echo ""

# Explicitly unset POND to prove no pond is needed
unset POND

#############################
# CREATE SAMPLE OTELJSON DATA
#############################

echo "=== Setting up OtelJSON test data ==="

mkdir -p /tmp/oteljson-test

cat > /tmp/oteljson-test/metrics.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":5.25}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":0.0}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":12.5}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":5.30}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":0.0}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":12.3}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":0.0}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":6.10}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":11.8}]}}]}]}]}
OTELJSON

echo "  ✓ Created 3-line OtelJSON file"

#############################
# TEST 1: Basic cat --format=table
#############################

echo ""
echo "=== Test 1: pond cat host+oteljson:// --format=table ==="

OUTPUT=$(pond cat "host+oteljson:///tmp/oteljson-test/metrics.json" --format=table 2>&1)
echo "$OUTPUT"

# Verify we got rows
ROW_COUNT=$(echo "$OUTPUT" | grep -c '|' || true)
if [ "$ROW_COUNT" -gt 2 ]; then
    echo "  ✓ Got table output with $ROW_COUNT lines"
else
    echo "  ✗ Expected table output, got $ROW_COUNT lines"
    exit 1
fi

# Verify column names are present (oteljson pivots metrics into columns)
if echo "$OUTPUT" | grep -q "pump1_amps"; then
    echo "  ✓ Found pump1_amps column"
else
    echo "  ✗ Missing pump1_amps column"
    exit 1
fi

if echo "$OUTPUT" | grep -q "timestamp"; then
    echo "  ✓ Found timestamp column"
else
    echo "  ✗ Missing timestamp column"
    exit 1
fi

#############################
# TEST 2: SQL query filtering
#############################

echo ""
echo "=== Test 2: pond cat host+oteljson:// --sql ==="

OUTPUT=$(pond cat "host+oteljson:///tmp/oteljson-test/metrics.json" \
    --sql "SELECT timestamp, tank_level FROM source ORDER BY timestamp" \
    --format=table 2>&1)
echo "$OUTPUT"

# Should have exactly 3 data rows
DATA_ROWS=$(echo "$OUTPUT" | grep -cE '^\|.*202[0-9]' || true)
if [ "$DATA_ROWS" -eq 3 ]; then
    echo "  ✓ Got 3 data rows"
else
    echo "  ✗ Expected 3 data rows, got $DATA_ROWS"
    exit 1
fi

#############################
# TEST 3: Aggregation query
#############################

echo ""
echo "=== Test 3: Aggregation query ==="

OUTPUT=$(pond cat "host+oteljson:///tmp/oteljson-test/metrics.json" \
    --sql "SELECT COUNT(*) as cnt, AVG(pump1_amps) as avg_pump1, AVG(tank_level) as avg_tank FROM source" \
    --format=table 2>&1)
echo "$OUTPUT"

if echo "$OUTPUT" | grep -q "avg_pump1"; then
    echo "  ✓ Aggregation includes avg_pump1"
else
    echo "  ✗ Missing avg_pump1 in aggregation"
    exit 1
fi

#############################
# TEST 4: No POND env needed
#############################

echo ""
echo "=== Test 4: Verify no POND env is set ==="

if [ -z "${POND}" ]; then
    echo "  ✓ POND env is unset (no pond needed)"
else
    echo "  ✗ POND env is unexpectedly set: ${POND}"
    exit 1
fi

echo ""
echo "=== Results: All host+oteljson tests passed ==="
