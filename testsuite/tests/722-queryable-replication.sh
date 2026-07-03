#!/bin/bash
# EXPERIMENT: Queryable entry types survive push/pull and are SQL-readable on
#             the consumer (dynamic recipe rebuild + table:series parquet).
#
# DESCRIPTION:
#   The existing file:// replication tests (711 raw data, 717 data:series)
#   only ever read replicated content back as RAW BYTES via `pond cat`.  None
#   verify that a QUERYABLE entry type survives content-addressed replication
#   and remains SQL-readable on the consumer.  This test covers the two
#   queryable archetypes over a self-contained file:// remote (no S3/MinIO):
#
#     (a) DYNAMIC node (D4 recipe rebuild): a `synthetic-timeseries` factory
#         node stores no data -- only its recipe (factory + config).  After a
#         cross-pond pull the consumer must rebuild the recipe and RECOMPUTE
#         the same rows on read.  (533 replicates a dynamic node but uses
#         `SELECT 1` and only checks it is NOT auto-run; it never verifies the
#         node recomputes correct data on the consumer.)
#
#     (b) PHYSICAL table:series (parquet blob transfer): a `TablePhysicalSeries`
#         entry created from a real parquet file.  After the pull the consumer
#         must (1) preserve the entry type and (2) answer SQL queries against
#         the replicated parquet through its own DataFusion session.
#
# EXPECTED:
#   - Consumer recomputes the dynamic node (7 rows, temperature=20.0).
#   - Consumer preserves the table:series entry type and queries it (7 rows).
#   - Row counts and a sentinel value match the producer exactly.
#
# History:
#   Added on jmacd/65 to cover queryable-entry replication, a gap left by 711
#   (raw data), 717 (data:series read as bytes), and 533 (dynamic node
#   non-execution only, S3-only).
set -e
source check.sh

echo "=== Experiment: queryable-entry replication (file://) ==="

P1=/tmp/722-p1
P2=/tmp/722-p2
REMOTE=/tmp/722-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# A deterministic queryable source with no external tooling: temperature is a
# flat line at 20.0 over 7 hourly points.
cat > /tmp/722-synth.yaml << 'YAML'
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

echo "--- Step 1: producer builds a dynamic node + a physical table:series ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /sensors >/dev/null
pond mkdir /data >/dev/null

# (a) dynamic synthetic-timeseries node (recipe only, no stored data)
pond mknod synthetic-timeseries /sensors/weather --config-path /tmp/722-synth.yaml >/dev/null
pond cat --sql "SELECT count(*) AS n FROM source" --format table /sensors/weather \
    > /tmp/722-p1-dyn-count.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/722-p1-dyn-count.txt' "producer dynamic node yields 7 rows"

# (b) export the dynamic node to a real parquet file, re-ingest as table:series
mkdir -p /tmp/722-exp
pond copy /sensors/weather "host:///tmp/722-exp" >/dev/null 2>&1
cp /tmp/722-exp/sensors/weather /tmp/722-weather.parquet
check '[ "$(head -c4 /tmp/722-weather.parquet | od -A n -t x1 | tr -d " ")" = "50415231" ]' \
    "exported file has PAR1 magic (valid parquet)"
pond copy "host+series:///tmp/722-weather.parquet" /data/weather.series >/dev/null 2>&1
pond describe /data/weather.series > /tmp/722-p1-desc.txt 2>&1
check_contains /tmp/722-p1-desc.txt "producer entry is TablePhysicalSeries" "TablePhysicalSeries"

echo "--- Step 2: backup add (auto-push) ---"
pond backup add origin "file://${REMOTE}" > /tmp/722-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/722-backup.log' "backup add origin succeeded"

echo "--- Step 3: consumer cross-pond import + pull ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/722-pull.log 2>&1
check 'grep -q "pull upstream complete" /tmp/722-pull.log' "consumer completed cross-pond import"

echo "--- Step 4: dynamic node RECOMPUTES on the consumer (D4 recipe rebuild) ---"
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    /imports/up/sensors/weather > /tmp/722-p2-dyn-count.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/722-p2-dyn-count.txt' "consumer dynamic node recomputes 7 rows"
pond cat --sql "SELECT MIN(temperature) AS t, MAX(temperature) AS x FROM source" --format table \
    /imports/up/sensors/weather > /tmp/722-p2-dyn-val.txt 2>/dev/null
check 'grep -qE "\| 20" /tmp/722-p2-dyn-val.txt' "consumer dynamic node value matches producer (temperature=20)"

echo "--- Step 5: physical table:series is queryable + type-preserved on consumer ---"
pond describe /imports/up/data/weather.series > /tmp/722-p2-desc.txt 2>&1
check_contains /tmp/722-p2-desc.txt "consumer preserves TablePhysicalSeries entry type" "TablePhysicalSeries"
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    /imports/up/data/weather.series > /tmp/722-p2-ser-count.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/722-p2-ser-count.txt' "consumer queries replicated table:series (7 rows)"
pond cat --sql "SELECT MIN(temperature) AS t FROM source" --format table \
    /imports/up/data/weather.series > /tmp/722-p2-ser-val.txt 2>/dev/null
check 'grep -qE "\| 20" /tmp/722-p2-ser-val.txt' "consumer table:series value matches producer (temperature=20)"

check_finish
