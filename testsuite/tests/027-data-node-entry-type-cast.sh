#!/bin/bash
# EXPERIMENT: URL entry-type cast of a data file to Parquet (series + table)
# DESCRIPTION: A data-archetype node that holds raw Parquet bytes but is not
#   natively queryable (Format: "Raw data") can be reinterpreted as a queryable
#   series/table via a `series://` / `table://` URL cast (the pond analogue of
#   the host parquet cast). This covers BOTH archetypes:
#     - FilePhysicalVersion: a parquet file copied in raw via host:// .
#     - FileDynamic:         a parquet file exposed by git-ingest.
#   Cross BOTH cast entry types (series and table) = 4 combinations.
#   See docs/url-entry-type-casting.md. The same byte-cast powers HydroVu
#   resume (find_youngest reading archive max(timestamp) over series://).
# EXPECTED: All 4 casts query cleanly; a non-Parquet data file casts to a clear
#   Parquet error rather than an opaque "not queryable" error.
set -e
source check.sh

echo "=== Experiment: data-file -> series/table entry-type cast ==="

pond init --birthplace test-host

# ---- Setup: produce a real Parquet file (no external tooling) ----
# A synthetic-timeseries node is queryable; copy it OUT to the host to obtain
# valid Parquet bytes (7 rows), which we then re-ingest as raw data nodes.
cat > /tmp/synth.yaml << 'YAML'
start: "2024-01-01T00:00:00Z"
end: "2024-01-01T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: 20.0
YAML

pond mkdir /sensors
pond mknod synthetic-timeseries /sensors/weather --config-path /tmp/synth.yaml

mkdir -p /tmp/cast-export
pond copy /sensors/weather "host:///tmp/cast-export"
cp /tmp/cast-export/sensors/weather /tmp/weather.parquet
check '[ "$(head -c4 /tmp/weather.parquet | od -A n -t x1 | tr -d " ")" = "50415231" ]' \
    "exported file has PAR1 magic (valid parquet)"

# ---- Archetype 1: FilePhysicalVersion (raw parquet copied into the pond) ----
# host:// (no +table/+series) stores the bytes verbatim as a physical data file.
pond copy "host:///tmp/weather.parquet" /phys.parquet
pond describe /phys.parquet > /tmp/desc-phys.txt 2>&1
check_contains /tmp/desc-phys.txt "physical node is FilePhysicalVersion" 'FilePhysicalVersion'
check_contains /tmp/desc-phys.txt "physical node is raw data (needs cast)" 'Raw data'

# ---- Archetype 2: FileDynamic (parquet exposed via git-ingest) ----
REPO_DIR=/tmp/cast-repo
rm -rf "$REPO_DIR"
mkdir -p "$REPO_DIR/archive"
cp /tmp/weather.parquet "$REPO_DIR/archive/weather.parquet"
( cd "$REPO_DIR" && git init -b main >/dev/null && \
  git config user.email test@example.com && git config user.name "Test User" && \
  git add -A && git commit -m "parquet archive" >/dev/null )

cat > /tmp/git-arch.yaml << EOF
url: file://${REPO_DIR}
ref: main
prefix: archive
EOF
pond mknod git-ingest /gitarch --config-path /tmp/git-arch.yaml
pond run /gitarch pull
pond describe /gitarch/weather.parquet > /tmp/desc-dyn.txt 2>&1
check_contains /tmp/desc-dyn.txt "dynamic node is FileDynamic" 'FileDynamic'
check_contains /tmp/desc-dyn.txt "dynamic node is raw data (needs cast)" 'Raw data'

echo ""
echo "--- Verification: 4 cast combinations all query cleanly ---"

# Physical + series
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    "series:///phys.parquet" > /tmp/q-phys-series.txt 2>&1
check_contains /tmp/q-phys-series.txt "physical + series:// casts and counts 7 rows" '| 7'

# Physical + table
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    "table:///phys.parquet" > /tmp/q-phys-table.txt 2>&1
check_contains /tmp/q-phys-table.txt "physical + table:// casts and counts 7 rows" '| 7'

# Dynamic + series
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    "series:///gitarch/weather.parquet" > /tmp/q-dyn-series.txt 2>&1
check_contains /tmp/q-dyn-series.txt "dynamic + series:// casts and counts 7 rows" '| 7'

# Dynamic + table
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    "table:///gitarch/weather.parquet" > /tmp/q-dyn-table.txt 2>&1
check_contains /tmp/q-dyn-table.txt "dynamic + table:// casts and counts 7 rows" '| 7'

# The cast must decode the real schema, not just succeed: a typed column reads.
pond cat --sql "SELECT temperature FROM source LIMIT 1" --format table \
    "series:///gitarch/weather.parquet" > /tmp/q-schema.txt 2>&1
check_contains /tmp/q-schema.txt "cast preserves column schema (temperature)" 'temperature'
check_contains /tmp/q-schema.txt "cast preserves column type (Float64)" 'Float64'

echo ""
echo "--- Verification: non-Parquet data file gives a clear Parquet error ---"
echo "this is not parquet" > /tmp/plain.txt
pond copy "host:///tmp/plain.txt" /plain.txt
pond cat --sql "SELECT 1 FROM source" --format table \
    "series:///plain.txt" > /tmp/q-bad.txt 2>&1 || true
check 'grep -qiE "parquet" /tmp/q-bad.txt' "non-parquet cast yields a Parquet error (not 'not queryable')"
check '! grep -qi "not queryable" /tmp/q-bad.txt' "error is not the opaque 'not queryable' message"

check_finish
