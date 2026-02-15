#!/bin/bash
# EXPERIMENT: Temporal-reduce factory — verify downsampling actually reduces rows
# EXPECTED: Each resolution should produce fewer rows than the source data.
#           res=1h  → same as source (8760 rows/year at 1h input)
#           res=2h  → ~4380
#           res=4h  → ~2190
#           res=12h → ~730
#           res=24h → ~365
#           At coarser resolutions, min/max spread should be > 0.
#
# This test isolates temporal-reduce from sitegen. It creates the same
# synthetic pipeline as test 201, then inspects the reduced series
# directly with `pond cat --sql`.
set -e

echo "=== Experiment: Temporal-Reduce Factory Isolation ==="

pond init

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create synthetic sensor data (same as test 201)
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create synthetic sensors ---"

cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "north_temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "temperature.C"
          components:
            - type: sine
              amplitude: 4.0
              period: "24h"
              offset: 14.0
            - type: sine
              amplitude: 6.0
              period: "8760h"
              phase: -1.5708

  - name: "north_do"
    factory: "synthetic-timeseries"
    config:
      start: "2025-01-01T00:00:00Z"
      end:   "2025-12-31T23:00:00Z"
      interval: "1h"
      points:
        - name: "do.mgL"
          components:
            - type: sine
              amplitude: 1.5
              period: "24h"
              offset: 8.0
              phase: 3.1416
            - type: sine
              amplitude: 2.0
              period: "8760h"
              phase: 1.5708
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "✓ /sensors created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create per-site join
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Create per-site join ---"

cat > /tmp/combined.yaml << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe
YAML

pond mknod dynamic-dir /combined --config-path /tmp/combined.yaml
echo "✓ /combined created"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Create temporal reduce
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 3: Create temporal reduce ---"

cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_site"
    factory: "temporal-reduce"
    config:
      in_pattern: "/combined/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions: ["1h", "2h", "4h", "12h", "24h"]
      aggregations:
        - type: "avg"
          columns: ["*"]
        - type: "min"
          columns: ["*"]
        - type: "max"
          columns: ["*"]
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "✓ /reduced created"

echo ""
echo "Reduced tree:"
pond list /reduced/**

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify row counts per resolution
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "=== VERIFICATION ==="

PASS=0
FAIL=0

check_row_count() {
  local series="$1"
  local label="$2"
  local expected_min="$3"
  local expected_max="$4"

  local output
  output=$(pond cat "$series" --format=table --sql "SELECT COUNT(*) as c FROM source" 2>&1)

  # Parse the row count from the table output (the numeric value in the data row)
  local count
  count=$(echo "$output" | grep -E '^\| [0-9]' | head -1 | grep -oE '[0-9]+' | head -1)

  if [ -z "$count" ]; then
    echo "  ✗ $label — could not read series"
    echo "    output: $output"
    FAIL=$((FAIL + 1))
    return
  fi

  if [ "$count" -ge "$expected_min" ] && [ "$count" -le "$expected_max" ]; then
    echo "  ✓ $label: $count rows (expected $expected_min–$expected_max)"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $label: $count rows (expected $expected_min–$expected_max)"
    FAIL=$((FAIL + 1))
  fi
}

echo ""
echo "--- Row counts (NorthDock, 1 year of 1h source data = 8760 rows) ---"

check_row_count "/reduced/single_site/NorthDock/res=1h.series"  "res=1h  (expect ~8760)" 8700 8800
check_row_count "/reduced/single_site/NorthDock/res=2h.series"  "res=2h  (expect ~4380)" 4300 4400
check_row_count "/reduced/single_site/NorthDock/res=4h.series"  "res=4h  (expect ~2190)" 2100 2200
check_row_count "/reduced/single_site/NorthDock/res=12h.series" "res=12h (expect ~730)"  700  750
check_row_count "/reduced/single_site/NorthDock/res=24h.series" "res=24h (expect ~365)"  360  370

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Verify min/max spread at coarser resolutions
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Min/max spread (should be > 0 at coarser resolutions) ---"

check_spread() {
  local series="$1"
  local label="$2"
  local expect_nonzero="$3"  # "yes" or "no"

  # avg(max - min) for the temperature column
  local output
  output=$(pond cat "$series" --format=table --sql "
    SELECT ROUND(AVG(\"TempProbe.temperature.C.max\" - \"TempProbe.temperature.C.min\"), 4) as spread
    FROM source
  " 2>&1)
  local spread
  spread=$(echo "$output" | grep -E '^\|' | grep -v 'spread' | grep -v '+-' | grep -oE '[0-9]+\.[0-9]+' | head -1)

  if [ -z "$spread" ]; then
    echo "  ✗ $label — could not compute spread"
    FAIL=$((FAIL + 1))
    return
  fi

  local is_zero
  is_zero=$(echo "$spread" | awk '{print ($1 == 0) ? "yes" : "no"}')

  if [ "$expect_nonzero" = "yes" ] && [ "$is_zero" = "no" ]; then
    echo "  ✓ $label: avg spread = $spread (nonzero, good)"
    PASS=$((PASS + 1))
  elif [ "$expect_nonzero" = "no" ] && [ "$is_zero" = "yes" ]; then
    echo "  ✓ $label: avg spread = $spread (zero expected for 1:1 buckets)"
    PASS=$((PASS + 1))
  elif [ "$expect_nonzero" = "yes" ] && [ "$is_zero" = "yes" ]; then
    echo "  ✗ $label: avg spread = $spread (EXPECTED NONZERO — not downsampling?)"
    FAIL=$((FAIL + 1))
  else
    echo "  ✗ $label: avg spread = $spread (unexpected)"
    FAIL=$((FAIL + 1))
  fi
}

check_spread "/reduced/single_site/NorthDock/res=1h.series"  "res=1h  spread" "no"
check_spread "/reduced/single_site/NorthDock/res=2h.series"  "res=2h  spread" "yes"
check_spread "/reduced/single_site/NorthDock/res=4h.series"  "res=4h  spread" "yes"
check_spread "/reduced/single_site/NorthDock/res=12h.series" "res=12h spread" "yes"
check_spread "/reduced/single_site/NorthDock/res=24h.series" "res=24h spread" "yes"

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Show sample data at each resolution for debugging
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Sample rows (first 3 per resolution) ---"

for res in 1h 2h 4h 12h 24h; do
  echo ""
  echo "  res=$res:"
  pond cat "/reduced/single_site/NorthDock/res=${res}.series" --format=table --sql "
    SELECT timestamp,
           ROUND(\"TempProbe.temperature.C.avg\", 2) as avg,
           ROUND(\"TempProbe.temperature.C.min\", 2) as min,
           ROUND(\"TempProbe.temperature.C.max\", 2) as max
    FROM source ORDER BY timestamp LIMIT 3
  " 2>&1 | head -10
done

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="

if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi

echo ""
echo "=== Test 203 PASSED ==="
