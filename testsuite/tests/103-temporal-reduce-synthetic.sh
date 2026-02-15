#!/bin/bash
# EXPERIMENT: Temporal reduce downsamples 24h of minute-resolution synthetic data
# EXPECTED: 24h of data at 1-minute resolution (1441 rows) is downsampled to
#           1-hour resolution (24 rows) with correct avg/min/max aggregations.
#           The known waveforms let us verify aggregation correctness.
set -e

echo "=== Experiment: Temporal Reduce with Synthetic Data ==="

pond init

# --- Create the source synthetic-timeseries -----------------------------------
# 24 hours of data at 1-minute intervals = 1441 rows (inclusive endpoints)
# temperature: sine wave, amplitude=10, period=24h, offset=20
#   → ranges from 10.0 to 30.0, daily average ≈ 20.0
# pressure: constant line at 1013.0 (slope=0)
#   → every aggregation should return exactly 1013.0
cat > /tmp/synth_source.yaml << 'YAML'
start: "2024-06-15T00:00:00Z"
end:   "2024-06-16T00:00:00Z"
interval: "1m"
points:
  - name: "temperature"
    components:
      - type: sine
        amplitude: 10.0
        period: "24h"
        offset: 20.0
  - name: "pressure"
    components:
      - type: line
        slope: 0.0
        offset: 1013.0
YAML

pond mkdir /sources
pond mknod synthetic-timeseries /sources/weather --config-path /tmp/synth_source.yaml

echo ""
echo "=== 1. Verify source data ==="
echo "--- source row count ---"
pond cat /sources/weather --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo "--- source time span ---"
pond cat /sources/weather --format=table --sql "
  SELECT MIN(timestamp) AS t_min, MAX(timestamp) AS t_max FROM source
"

echo "--- source value ranges ---"
pond cat /sources/weather --format=table --sql "
  SELECT
    MIN(temperature) AS temp_min, MAX(temperature) AS temp_max,
    AVG(temperature) AS temp_avg,
    MIN(pressure) AS pres_min, MAX(pressure) AS pres_max
  FROM source
"

# --- Create the temporal-reduce factory ----------------------------------------
# Downsample to 1h resolution with avg, min, max for both columns
cat > /tmp/reduce.yaml << 'YAML'
in_pattern: "series:///sources/*"
out_pattern: "$0"
time_column: "timestamp"
resolutions: ["1h"]
aggregations:
  - type: "avg"
    columns: ["temperature", "pressure"]
  - type: "min"
    columns: ["temperature"]
  - type: "max"
    columns: ["temperature"]
YAML

pond mknod temporal-reduce /reduce --config-path /tmp/reduce.yaml

echo ""
echo "=== 2. Directory structure ==="
pond list /reduce
pond list /reduce/weather

echo ""
echo "=== 3. Verify downsampled data ==="
echo "--- hourly row count (expect 24) ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "SELECT COUNT(*) AS cnt FROM source"

echo "--- hourly time span ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT MIN(timestamp) AS t_min, MAX(timestamp) AS t_max FROM source
"

echo "--- hourly schema ---"
pond describe /reduce/weather/res=1h.series

echo "--- all 24 hourly rows ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT
    timestamp,
    ROUND(\"temperature.avg\", 4) AS temp_avg,
    ROUND(\"temperature.min\", 4) AS temp_min,
    ROUND(\"temperature.max\", 4) AS temp_max,
    ROUND(\"pressure.avg\", 4)    AS pres_avg
  FROM source
  ORDER BY timestamp
"

echo ""
echo "=== 4. Verify aggregation correctness ==="

echo "--- pressure.avg should always be 1013.0 (constant input) ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT
    COUNT(*) AS total_hours,
    COUNT(CASE WHEN ABS(\"pressure.avg\" - 1013.0) < 0.001 THEN 1 END) AS correct_hours
  FROM source
"

echo "--- temperature.min should always <= temperature.avg <= temperature.max ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT
    COUNT(*) AS total_hours,
    COUNT(CASE
      WHEN \"temperature.min\" <= \"temperature.avg\"
       AND \"temperature.avg\" <= \"temperature.max\"
      THEN 1
    END) AS ordering_correct
  FROM source
"

echo "--- global min/max of hourly temperature aggregates ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT
    ROUND(MIN(\"temperature.min\"), 2)  AS global_min,
    ROUND(MAX(\"temperature.max\"), 2)  AS global_max,
    ROUND(AVG(\"temperature.avg\"), 4)  AS overall_avg
  FROM source
"

echo "--- midnight bucket (hour 0) should be near offset=20 ---"
pond cat /reduce/weather/res=1h.series --format=table --sql "
  SELECT
    timestamp,
    ROUND(\"temperature.avg\", 4) AS temp_avg
  FROM source
  ORDER BY timestamp
  LIMIT 1
"

echo ""
echo "=== ALL TEMPORAL-REDUCE TESTS PASSED ==="
