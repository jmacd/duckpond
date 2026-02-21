#!/bin/bash
# EXPERIMENT: End-to-end oteljson → temporal-reduce pipeline
# DESCRIPTION: Reproduce the septic station pipeline locally:
#   1. Ingest sample OTelJSON as raw data
#   2. Create temporal-reduce node with oteljson:// in_pattern
#   3. Query the reduced dynamic series
#   This reproduces the failure seen on BeaglePlay where temporal-reduce
#   produced "Corrupt footer" when reading oteljson:// input from large file storage.
#
# REQUIRES: testsuite/testdata/septic-sample.json (auto-mounted at /data/)
set -e

echo "=== Experiment: Septic Temporal-Reduce Pipeline ==="
echo ""

SAMPLE=/data/septic-sample.json
if [ ! -f "$SAMPLE" ]; then
    echo "SKIP: septic-sample.json not found at $SAMPLE"
    exit 0
fi
echo "Using sample: ${SAMPLE} ($(wc -l < "$SAMPLE") lines, $(wc -c < "$SAMPLE") bytes)"

pond init

#############################
# STEP 1: INGEST RAW DATA
#############################

echo ""
echo "=== Step 1: Copy OtelJSON into pond ==="
pond mkdir -p /ingest
pond copy "host://${SAMPLE}" /ingest/septicstation.json
pond describe /ingest/septicstation.json
echo "✓ Ingested as raw data"

#############################
# STEP 2: VERIFY oteljson:// WORKS
#############################

echo ""
echo "=== Step 2: Verify oteljson:// reads the data ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "SELECT COUNT(*) AS row_count FROM source"
echo "✓ oteljson:// URL scheme works"

#############################
# STEP 3: CREATE TEMPORAL-REDUCE
#############################

echo ""
echo "=== Step 3: Create temporal-reduce factory ==="

# Write reduce config
cat > /tmp/reduce.yaml << 'EOF'
in_pattern: "oteljson:///ingest/septicstation.json"
out_pattern: "septic"
time_column: "timestamp"
resolutions: [1h, 6h, 1d]
aggregations:
  - type: "avg"
    columns:
      - "septicstation_temperature"
      - "septicstation_pressure"
      - "septicstation_humidity"
      - "orenco_RT_Pump1_Amps"
      - "orenco_RT_Pump2_Amps"
  - type: "min"
    columns:
      - "septicstation_temperature"
  - type: "max"
    columns:
      - "septicstation_temperature"
EOF

# temporal-reduce mknod target IS the output directory.
# Dynamic children (septic/res=1h.series, etc.) appear under it.
pond mknod temporal-reduce /reduced --config-path /tmp/reduce.yaml

echo "✓ Created temporal-reduce node"

#############################
# STEP 4: VERIFY DYNAMIC NODES EXIST
#############################

echo ""
echo "=== Step 4: List reduced directory ==="
pond list '/reduced/**/*'

#############################
# STEP 5: QUERY REDUCED DATA
#############################

echo ""
echo "=== Step 5: Query reduced 1h series ==="
pond cat /reduced/septic/res=1h.series --format=table --sql "SELECT * FROM source ORDER BY timestamp LIMIT 5"

echo ""
echo "=== Step 5b: Query reduced 1d series ==="
pond cat /reduced/septic/res=1d.series --format=table --sql "SELECT * FROM source ORDER BY timestamp LIMIT 5"

echo ""
echo "=== Step 5c: Row counts at each resolution ==="
pond cat /reduced/septic/res=1h.series --format=table --sql "SELECT COUNT(*) AS hour_count FROM source"
pond cat /reduced/septic/res=6h.series --format=table --sql "SELECT COUNT(*) AS sixhour_count FROM source"
pond cat /reduced/septic/res=1d.series --format=table --sql "SELECT COUNT(*) AS day_count FROM source"

echo ""
echo "=== Step 5d: Schema of reduced data ==="
pond describe /reduced/septic/res=1h.series

#############################
# STEP 6: EXPORT AS PARQUET
#############################

echo ""
echo "=== Step 6: Export reduced data as Parquet ==="
pond cat /reduced/septic/res=1d.series --sql "SELECT * FROM source ORDER BY timestamp" > /tmp/reduced-1d.parquet
ls -la /tmp/reduced-1d.parquet
duckdb -c "SELECT * FROM '/tmp/reduced-1d.parquet'" 2>&1 | head -20

echo ""
echo "=== Experiment Complete ==="
