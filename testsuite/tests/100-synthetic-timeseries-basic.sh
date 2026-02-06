#!/bin/bash
# EXPERIMENT: Synthetic timeseries factory — create and inspect
# EXPECTED: Factory creates queryable dynamic file; SQL shows expected timestamp
#           range and value ranges for sine/triangle/square/line waveforms
set -e

echo "=== Experiment: Synthetic Timeseries Factory ==="

pond init

# --- Create the factory config inside the container --------------------------
cat > /tmp/synth.yaml << 'EOF'
start: "2024-01-01T00:00:00Z"
end: "2024-01-02T00:00:00Z"
interval: "15m"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "24h"
        offset: 20.0
      - type: line
        slope: 0.0002

  - name: "pressure"
    components:
      - type: sine
        amplitude: 5.0
        period: "12h"
        offset: 1013.0
      - type: square
        amplitude: 2.0
        period: "6h"

  - name: "humidity"
    components:
      - type: triangle
        amplitude: 15.0
        period: "8h"
        offset: 60.0
      - type: sine
        amplitude: 3.0
        period: "3h"
        phase: 1.57
EOF

# --- Create parent directory and factory node ---------------------------------
pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/synth --config-path /tmp/synth.yaml
echo "✓ mknod succeeded"

# --- Verify node exists -------------------------------------------------------
echo ""
echo "=== Listing /sensors/ ==="
pond list /sensors/

# --- Inspect with SQL: row count & time range ---------------------------------
echo ""
echo "=== Row count and time range ==="
pond cat /sensors/synth --sql "
  SELECT
    COUNT(*)          AS row_count,
    MIN(timestamp)    AS first_ts,
    MAX(timestamp)    AS last_ts
  FROM source
"

# --- Inspect with SQL: value ranges per column --------------------------------
echo ""
echo "=== Value ranges ==="
pond cat /sensors/synth --sql "
  SELECT
    MIN(temperature)  AS temp_min,
    MAX(temperature)  AS temp_max,
    MIN(pressure)     AS pres_min,
    MAX(pressure)     AS pres_max,
    MIN(humidity)     AS hum_min,
    MAX(humidity)     AS hum_max
  FROM source
"

# --- Inspect with SQL: first 5 rows ------------------------------------------
echo ""
echo "=== First 5 rows ==="
pond cat /sensors/synth --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 5
"

# --- Inspect with SQL: last 5 rows -------------------------------------------
echo ""
echo "=== Last 5 rows ==="
pond cat /sensors/synth --sql "
  SELECT * FROM source ORDER BY timestamp DESC LIMIT 5
"

echo ""
echo "=== Experiment Complete ==="
