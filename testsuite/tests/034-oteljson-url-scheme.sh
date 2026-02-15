#!/bin/bash
# EXPERIMENT: OtelJSON URL scheme for querying ingested logfiles
# DESCRIPTION: Verify that oteljson:// URL scheme works with pond cat
#   to query OpenTelemetry JSON Lines files stored as raw data.
#   This is the foundation for the septic station pipeline:
#     logfile-ingest → oteljson:// query → timeseries-pivot → sitegen
#
set -e

echo "=== Experiment: OtelJSON URL Scheme ==="
echo ""

pond init

#############################
# CREATE SAMPLE OTELJSON DATA
#############################

echo "=== Setting up OtelJSON test data ==="

# Simulate what the otel collector writes: one JSON line per export.
# These are OTLP ExportMetricsServiceRequest objects with gauge metrics
# from modbus registers (like the septic station produces).

mkdir -p /var/log/testapp

cat > /var/log/testapp/metrics.json << 'OTELJSON'
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":5.25}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":0.0}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000000000000000","asDouble":12.5}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":5.30}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":0.0}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000060000000000","asDouble":12.3}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"pump1_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":0.0}]}},{"name":"pump2_amps","unit":"amps","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":6.10}]}},{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"1700000120000000000","asDouble":11.8}]}}]}]}]}
OTELJSON

echo "✓ Created 3-line OtelJSON file with pump1_amps, pump2_amps, tank_level"

#############################
# COPY INTO POND AS RAW DATA
#############################

echo ""
echo "=== Copying OtelJSON file into pond as raw data ==="

pond mkdir -p /ingest
pond copy host:///var/log/testapp/metrics.json /ingest/metrics.json --format=data

echo "✓ Stored as data entry type"
pond describe /ingest/metrics.json

#############################
# QUERY VIA oteljson:// SCHEME
#############################

# --format=table for human-readable output; --sql controls the query independently

echo ""
echo "=== Query 1: Schema discovery (SELECT * LIMIT 1) ==="
pond cat oteljson:///ingest/metrics.json --format=table --sql "SELECT * FROM source LIMIT 1"

echo ""
echo "=== Query 2: All rows ==="
pond cat oteljson:///ingest/metrics.json --format=table --sql "SELECT * FROM source ORDER BY timestamp"

echo ""
echo "=== Query 3: Filter for active pump (amps > 0) ==="
pond cat oteljson:///ingest/metrics.json --format=table --sql "SELECT timestamp, pump1_amps, pump2_amps FROM source WHERE pump1_amps > 0 OR pump2_amps > 0 ORDER BY timestamp"

echo ""
echo "=== Query 4: Aggregation ==="
pond cat oteljson:///ingest/metrics.json --format=table --sql "SELECT COUNT(*) as readings, AVG(tank_level) as avg_level, MAX(pump1_amps) as max_p1, MAX(pump2_amps) as max_p2 FROM source"

echo ""
echo "=== Query 5: Parquet output (default --format=raw) ==="
pond cat oteljson:///ingest/metrics.json --sql "SELECT * FROM source ORDER BY timestamp" > /tmp/q5.parquet
ls -la /tmp/q5.parquet
file /tmp/q5.parquet
duckdb -c "SELECT * FROM '/tmp/q5.parquet'"

#############################
# TEST WITH LOGFILE-INGEST
#############################

echo ""
echo "=== Logfile-ingest + oteljson:// end-to-end ==="

# Create ingest config
cat > /tmp/ingest.yaml << 'EOF'
archived_pattern: /var/log/testapp/metrics.json.*
active_pattern: /var/log/testapp/metrics.json
pond_path: /logs/septic
EOF

pond mkdir -p /etc/system.d
pond mkdir -p /logs/septic

pond mknod logfile-ingest /etc/system.d/10-ingest --config-path /tmp/ingest.yaml

echo "✓ Created logfile-ingest factory"

# Run ingest
RUST_LOG=info pond run /etc/system.d/10-ingest 2>&1 | grep -E "complete|error" || true

echo ""
echo "=== Verify ingested files ==="
pond list '/logs/septic/*'

echo ""
echo "=== Query ingested file via oteljson:// ==="
pond cat oteljson:///logs/septic/metrics.json --format=table --sql "SELECT * FROM source ORDER BY timestamp"

echo ""
echo "=== Experiment Complete ==="
