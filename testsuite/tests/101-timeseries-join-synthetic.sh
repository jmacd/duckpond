#!/bin/bash
# EXPERIMENT: Timeseries join of two synthetic series
# EXPECTED: Two synthetic series with overlapping time ranges are combined via
#           timeseries-join, producing a single logical series spanning the full
#           range with scoped column names and NULLs in non-overlapping regions.
set -e

echo "=== Experiment: Timeseries Join with Synthetic Data ==="

pond init

# --- Create the dynamic directory config inline ------------------------------
# station_a: Jan 1–15  sine temperature (offset 20), sine pressure (offset 1013)
# station_b: Jan 10–25 triangle temperature (offset 30), square pressure (offset 1010)
# Overlap:   Jan 10–15 (both stations have data)
cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "station_a"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-15T00:00:00Z"
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
      start: "2024-01-10T00:00:00Z"
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
              amplitude: 4.0
              period: "6h"
              offset: 1010.0

  - name: "combined"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "/sensors/station_a"
          scope: StationA
        - pattern: "/sensors/station_b"
          scope: StationB
YAML

# --- Create the dynamic directory ---------------------------------------------
pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ mknod dynamic-dir succeeded"

# --- Verify directory structure -----------------------------------------------
echo ""
echo "=== Directory listing ==="
pond list /sensors/

# --- Verify station_a individually --------------------------------------------
echo ""
echo "=== Station A: row count & time range ==="
pond cat /sensors/station_a --sql "
  SELECT
    COUNT(*)       AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts,
    MIN(temperature) AS temp_min,
    MAX(temperature) AS temp_max
  FROM source
"

# --- Verify station_b individually --------------------------------------------
echo ""
echo "=== Station B: row count & time range ==="
pond cat /sensors/station_b --sql "
  SELECT
    COUNT(*)       AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts,
    MIN(temperature) AS temp_min,
    MAX(temperature) AS temp_max
  FROM source
"

# --- Verify the combined join -------------------------------------------------
echo ""
echo "=== Combined: row count & full time range ==="
pond cat /sensors/combined --sql "
  SELECT
    COUNT(*)       AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts
  FROM source
"

# --- Verify scoped column names exist -----------------------------------------
echo ""
echo "=== Combined: scoped value ranges ==="
pond cat /sensors/combined --sql "
  SELECT
    MIN(\"StationA.temperature\") AS a_temp_min,
    MAX(\"StationA.temperature\") AS a_temp_max,
    MIN(\"StationB.temperature\") AS b_temp_min,
    MAX(\"StationB.temperature\") AS b_temp_max,
    MIN(\"StationA.pressure\")    AS a_pres_min,
    MAX(\"StationA.pressure\")    AS a_pres_max,
    MIN(\"StationB.pressure\")    AS b_pres_min,
    MAX(\"StationB.pressure\")    AS b_pres_max
  FROM source
"

# --- Verify overlap region (Jan 10–15) has both stations' data ----------------
echo ""
echo "=== Overlap region (Jan 10–15): both stations present ==="
pond cat /sensors/combined --sql "
  SELECT COUNT(*) AS overlap_rows,
    COUNT(\"StationA.temperature\") AS a_present,
    COUNT(\"StationB.temperature\") AS b_present
  FROM source
  WHERE timestamp >= '2024-01-10T00:00:00Z'
    AND timestamp <= '2024-01-15T00:00:00Z'
"

# --- Verify exclusive regions have NULLs for the absent station ---------------
echo ""
echo "=== Station A exclusive (Jan 1–9): B columns should be NULL ==="
pond cat /sensors/combined --sql "
  SELECT COUNT(*) AS rows,
    COUNT(\"StationA.temperature\") AS a_present,
    COUNT(\"StationB.temperature\") AS b_present
  FROM source
  WHERE timestamp < '2024-01-10T00:00:00Z'
"

echo ""
echo "=== Station B exclusive (Jan 16–25): A columns should be NULL ==="
pond cat /sensors/combined --sql "
  SELECT COUNT(*) AS rows,
    COUNT(\"StationA.temperature\") AS a_present,
    COUNT(\"StationB.temperature\") AS b_present
  FROM source
  WHERE timestamp > '2024-01-15T00:00:00Z'
"

# --- Sample rows from each region --------------------------------------------
echo ""
echo "=== Sample: first 3 rows (Station A only) ==="
pond cat /sensors/combined --sql "
  SELECT timestamp,
    \"StationA.temperature\",
    \"StationB.temperature\"
  FROM source
  ORDER BY timestamp
  LIMIT 3
"

echo ""
echo "=== Sample: 3 rows from overlap (Jan 12 noon) ==="
pond cat /sensors/combined --sql "
  SELECT timestamp,
    \"StationA.temperature\",
    \"StationB.temperature\"
  FROM source
  WHERE timestamp >= '2024-01-12T00:00:00Z'
  ORDER BY timestamp
  LIMIT 3
"

echo ""
echo "=== Experiment Complete ==="
