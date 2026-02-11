#!/bin/bash
# EXPERIMENT: Explore septic station OtelJSON data — full schema discovery
# DESCRIPTION: Copy the real septicstation-sample.json into a pond and use
#   pond describe + pond cat with the oteljson:// URL scheme to discover
#   ALL metrics: modbus (Orenco controller), BME280 (environment sensor),
#   and anything else the OTel collector is exporting.
#
#   This test is exploratory — its purpose is to learn the complete schema
#   so we can design the timeseries-pivot and sitegen pipeline correctly.
#
# REQUIRES: testsuite/testdata/septic-sample.json (auto-mounted at /data/)
set -e

echo "=== Experiment: Septic Station Full Schema Discovery ==="
echo ""

# testdata/ is auto-mounted at /data/ by run-test.sh
SAMPLE=/data/septic-sample.json
if [ ! -f "$SAMPLE" ]; then
    echo "SKIP: septic-sample.json not found at $SAMPLE"
    echo "  Run with: ./run-test.sh 035"
    exit 0
fi
echo "Using sample: ${SAMPLE} ($(wc -l < "$SAMPLE") lines)"

pond init

#############################
# INGEST THE SAMPLE DATA
#############################

echo "=== Ingesting septicstation-sample.json ==="
pond mkdir -p /ingest
pond copy "host://${SAMPLE}" /ingest/septicstation.json --format=data

echo ""
echo "=== File info ==="
pond describe /ingest/septicstation.json

echo ""
echo "=== Line count ==="
pond cat /ingest/septicstation.json | wc -l

#############################
# SCHEMA DISCOVERY
#############################

echo ""
echo "=== Full OtelJSON schema (pond describe with oteljson URL) ==="
pond describe oteljson:///ingest/septicstation.json

echo ""
echo "=== Column names via information_schema ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT column_name
  FROM information_schema.columns
  WHERE table_name = 'source'
  ORDER BY column_name
"

echo ""
echo "=== Sample rows (first 5 readings) ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

echo ""
echo "=== Row count (total readings) ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT COUNT(*) AS total_readings FROM source
"

echo ""
echo "=== Time range ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT
    MIN(timestamp) AS earliest,
    MAX(timestamp) AS latest
  FROM source
"

#############################
# METRIC SUMMARIES
#############################

# Don't guess column names — pond describe already showed us the schema.
# Just query what's there.

echo ""
echo "=== BME280 environment metrics — summary stats ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT
    COUNT(septicstation_temperature) AS temp_count,
    MIN(septicstation_temperature) AS temp_min,
    MAX(septicstation_temperature) AS temp_max,
    AVG(septicstation_temperature) AS temp_avg,
    COUNT(septicstation_humidity) AS humid_count,
    MIN(septicstation_humidity) AS humid_min,
    MAX(septicstation_humidity) AS humid_max,
    COUNT(septicstation_pressure) AS press_count,
    MIN(septicstation_pressure) AS press_min,
    MAX(septicstation_pressure) AS press_max
  FROM source
"

echo ""
echo "=== Last 5 rows (most complete metric set) ==="
pond cat oteljson:///ingest/septicstation.json --format=table --sql "
  SELECT * FROM source ORDER BY timestamp DESC LIMIT 5
"

echo ""
echo "=== Experiment Complete ==="
