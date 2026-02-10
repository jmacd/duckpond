#!/bin/bash
# EXPERIMENT: Timeseries pivot extracts single columns from a timeseries-join
# EXPECTED: Two synthetic stations joined, then pivot extracts individual
#           measurements across both stations into separate views with
#           scoped column names (station_a.temperature, station_b.temperature).
set -e

echo "=== Experiment: Timeseries Pivot with Synthetic Data ==="

pond init

# --- Build the full pipeline in a single dynamic-dir --------------------------
# Layer 1: two synthetic-timeseries (station_a, station_b)
# Layer 2: one timeseries-join combining them with scope prefixes
# Layer 3: two timeseries-pivot nodes extracting individual columns
#
# station_a: Jan 1–20, sine temperature (offset 20), sine pressure (offset 1013)
# station_b: Jan 5–25, triangle temperature (offset 30), square pressure (offset 1010)
# Overlap:   Jan 5–20

cat > /tmp/pivot_test.yaml << 'YAML'
entries:
  - name: "station_a"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-20T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: sine
              amplitude: 5.0
              period: "24h"
              offset: 20.0
        - name: "pressure"
          components:
            - type: sine
              amplitude: 3.0
              period: "12h"
              offset: 1013.0

  - name: "station_b"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-05T00:00:00Z"
      end:   "2024-01-25T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: triangle
              amplitude: 8.0
              period: "24h"
              offset: 30.0
        - name: "pressure"
          components:
            - type: square
              amplitude: 2.0
              period: "6h"
              offset: 1010.0

  - name: "combined"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/station_a"
          scope: "station_a"
        - pattern: "series:///sensors/station_b"
          scope: "station_b"

  - name: "pivot_temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///sensors/combined"
      columns:
        - "station_a.temperature"
        - "station_b.temperature"

  - name: "pivot_pressure"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///sensors/combined"
      columns:
        - "station_a.pressure"
        - "station_b.pressure"
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/pivot_test.yaml

echo ""
echo "=== 1. Directory listing ==="
pond list /sensors

echo ""
echo "=== 2. Verify individual stations ==="
echo "--- station_a row count ---"
pond cat /sensors/station_a --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo "--- station_b row count ---"
pond cat /sensors/station_b --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo ""
echo "=== 3. Verify the combined join ==="
echo "--- combined schema ---"
pond describe /sensors/combined

echo "--- combined row count and time span ---"
pond cat /sensors/combined --format=table --sql "
  SELECT COUNT(*) AS cnt,
         MIN(timestamp) AS t_min,
         MAX(timestamp) AS t_max
  FROM source
"

echo "--- combined: check scoped column names exist ---"
pond cat /sensors/combined --format=table --sql "
  SELECT
    \"station_a.temperature\",
    \"station_a.pressure\",
    \"station_b.temperature\",
    \"station_b.pressure\"
  FROM source
  ORDER BY timestamp
  LIMIT 3
"

echo ""
echo "=== 4. Verify pivot_temperature ==="
echo "--- pivot_temperature schema ---"
pond describe /sensors/pivot_temperature

echo "--- pivot_temperature row count ---"
pond cat /sensors/pivot_temperature --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo "--- pivot_temperature: first 5 rows ---"
pond cat /sensors/pivot_temperature --format=table --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

echo "--- pivot_temperature: last 5 rows ---"
pond cat /sensors/pivot_temperature --format=table --sql "
  SELECT * FROM source ORDER BY timestamp DESC LIMIT 5
"

echo "--- pivot_temperature: only station_a data (before Jan 5) ---"
pond cat /sensors/pivot_temperature --format=table --sql "
  SELECT timestamp,
         \"combined.station_a.temperature\",
         \"combined.station_b.temperature\"
  FROM source
  WHERE timestamp < '2024-01-05T00:00:00Z'
  ORDER BY timestamp
  LIMIT 5
"

echo "--- pivot_temperature: overlap region (both non-null) ---"
pond cat /sensors/pivot_temperature --format=table --sql "
  SELECT COUNT(*) AS overlap_rows
  FROM source
  WHERE \"combined.station_a.temperature\" IS NOT NULL
    AND \"combined.station_b.temperature\" IS NOT NULL
"

echo "--- pivot_temperature: only station_b data (after Jan 20) ---"
pond cat /sensors/pivot_temperature --format=table --sql "
  SELECT timestamp,
         \"combined.station_a.temperature\",
         \"combined.station_b.temperature\"
  FROM source
  WHERE timestamp >= '2024-01-20T00:00:00Z'
  ORDER BY timestamp
  LIMIT 5
"

echo ""
echo "=== 5. Verify pivot_pressure ==="
echo "--- pivot_pressure schema ---"
pond describe /sensors/pivot_pressure

echo "--- pivot_pressure row count ---"
pond cat /sensors/pivot_pressure --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo "--- pivot_pressure: value ranges ---"
pond cat /sensors/pivot_pressure --format=table --sql "
  SELECT
    MIN(\"combined.station_a.pressure\") AS a_min,
    MAX(\"combined.station_a.pressure\") AS a_max,
    MIN(\"combined.station_b.pressure\") AS b_min,
    MAX(\"combined.station_b.pressure\") AS b_max
  FROM source
"

echo "--- pivot_pressure: first 5 rows ---"
pond cat /sensors/pivot_pressure --format=table --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

echo ""
echo "=== ALL PIVOT TESTS PASSED ==="
