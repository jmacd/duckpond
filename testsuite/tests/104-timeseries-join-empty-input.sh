#!/bin/bash
# EXPERIMENT: Timeseries join where one input glob matches zero nodes
# EXPECTED: A timeseries-join input whose pattern resolves to no nodes is
#           treated as an empty set (contributes no rows) instead of aborting
#           the query with "table 'inputN' not found".  This mirrors the noyo
#           combine, where a decommissioned instrument has archive data but no
#           live `/hydrovu/...` series, leaving the live input empty.
set -e

echo "=== Experiment: Timeseries Join with a Zero-Match Input ==="

pond init

# --- Dynamic directory config ------------------------------------------------
# station_a (scope StationA) and station_b (scope StationB) both exist.
# The `combined` join also lists a SAME-SCOPE-as-StationA input whose glob
# (/sensors/missing_station_*) matches NO node.  Before the empty-placeholder
# fix this produced "table 'input1' not found" and failed the whole join.
cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "station_a"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-10T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: sine
              amplitude: 5.0
              period: "24h"
              offset: 20.0

  - name: "station_b"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-05T00:00:00Z"
      end:   "2024-01-15T00:00:00Z"
      interval: "1h"
      points:
        - name: "temperature"
          components:
            - type: triangle
              amplitude: 8.0
              period: "24h"
              offset: 30.0

  - name: "combined"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "/sensors/station_a"
          scope: StationA
        # Zero-match input, SAME scope as station_a (decommissioned-instrument
        # live counterpart that never produced a node).
        - pattern: "/sensors/missing_station_*"
          scope: StationA
        - pattern: "/sensors/station_b"
          scope: StationB
YAML

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "[OK] mknod dynamic-dir succeeded"

echo ""
echo "=== Directory listing ==="
pond list /sensors/

# --- The combined join must build despite the zero-match input ---------------
echo ""
echo "=== Combined: row count & full time range (must NOT error) ==="
pond cat /sensors/combined --format=table --sql "
  SELECT
    COUNT(*)       AS rows,
    MIN(timestamp) AS first_ts,
    MAX(timestamp) AS last_ts
  FROM source
"

# --- Both real scopes' data survive the empty same-scope sibling -------------
echo ""
echo "=== Combined: both scopes present ==="
pond cat /sensors/combined --format=table --sql "
  SELECT
    COUNT(\"StationA.temperature\") AS a_present,
    COUNT(\"StationB.temperature\") AS b_present
  FROM source
"

# --- Verification ------------------------------------------------------------
# The core regression is simply that `pond cat /sensors/combined` plans at all
# (before the fix it errored "table 'input1' not found").  Beyond that, we
# assert the resolved StationA data was NOT wiped by the empty same-scope
# sibling, and that StationB (the other scope) is present.  We grep the ASCII
# table output rather than parse it, and rely on `set -e` so any query error
# fails the test.
echo ""
echo "=== VERIFICATION: StationA data survived the empty same-scope sibling ==="
A_OUT=$(pond cat /sensors/combined --format=table --sql "
  SELECT timestamp, \"StationA.temperature\" AS a
  FROM source
  WHERE \"StationA.temperature\" IS NOT NULL
  ORDER BY timestamp
  LIMIT 1
")
echo "${A_OUT}"
if echo "${A_OUT}" | grep -q "No data found"; then
  echo "FAIL: resolved StationA input was wiped by the empty same-scope sibling"
  exit 1
fi
echo "${A_OUT}" | grep -q "2024-01-01" || {
  echo "FAIL: StationA earliest row missing"
  exit 1
}

echo ""
echo "=== VERIFICATION: StationB scope present ==="
B_OUT=$(pond cat /sensors/combined --format=table --sql "
  SELECT timestamp, \"StationB.temperature\" AS b
  FROM source
  WHERE \"StationB.temperature\" IS NOT NULL
  ORDER BY timestamp
  LIMIT 1
")
echo "${B_OUT}"
if echo "${B_OUT}" | grep -q "No data found"; then
  echo "FAIL: StationB scope missing"
  exit 1
fi

echo ""
echo "[OK] Zero-match input tolerated; both resolved scopes intact."
echo "=== Experiment Complete ==="
