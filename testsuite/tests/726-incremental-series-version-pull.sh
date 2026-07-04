#!/bin/bash
# EXPERIMENT: A second series VERSION appended upstream propagates to a consumer
#             via an incremental pull (the plan_series_versions append path).
#
# DESCRIPTION:
#   Existing replication tests only ever pull a series ONCE, as a fresh
#   consumer that fetches the whole history in a single shot:
#     - 717 collapses a data:series then a FRESH consumer pulls the collapsed
#       history (one pull, no incremental version delta).
#     - 722 replicates a table:series but the consumer pulls exactly once.
#     - 715 exercises incremental IMPORT but only appends a new FILE (a new
#       node_id), never a new VERSION of an EXISTING series node.
#
#   None drive the incremental series-VERSION diff: pull once, then append a
#   NEW version to an EXISTING series upstream, then pull again and prove the
#   consumer applies only the new version and can read BOTH. That path is
#   `content_pull.rs::plan_series_versions` (append-prefix reuse) feeding off
#   `content_tree.rs::build_target_state_for_pond`'s per-pond series map -- the
#   exact code a jmacd/65 review found dropped its pond_id filter and fixed
#   (BACKLOG: CA-REVIEW-FIXES item 1). This test is the end-to-end regression
#   guard for that fix: it is the only case where the consumer already holds
#   version N of a series and must correctly extend it to N+1 on re-pull.
#
# EXPECTED:
#   - Pull #1: consumer sees version 1 only (7 rows, day 1 present, day 2 not).
#   - Producer appends version 2 (a distinct day) to the SAME series; auto-push.
#   - Pull #2: incremental diff applies; consumer sees BOTH versions merged
#     (14 rows, day 1 AND day 2 present) -- version 1 is not lost or doubled.
#   - Consumer content TIP HASH equals the producer's pushed tip after pull #2.
#
# History:
#   Added on jmacd/65 to close the incremental series-version coverage gap left
#   by 717/722 (single pull) and 715 (new file, not new version).
set -e
source check.sh

echo "=== Experiment: incremental series-version replication (file://) ==="

P1=/tmp/726-p1
P2=/tmp/726-p2
REMOTE=/tmp/726-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# Two deterministic single-column series, one hourly point each, on two
# DISTINCT days so the two versions never overlap and row counts add cleanly.
cat > /tmp/726-day1.yaml << 'YAML'
start: "2024-01-01T00:00:00Z"
end: "2024-01-01T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: 10.0
YAML
cat > /tmp/726-day2.yaml << 'YAML'
start: "2024-01-02T00:00:00Z"
end: "2024-01-02T06:00:00Z"
interval: "1h"
time_column: "timestamp"
points:
  - name: "temperature"
    components:
      - type: line
        slope: 0.0
        offset: 20.0
YAML

echo "--- Step 1: producer builds two parquet series (distinct days) ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /gen >/dev/null
pond mkdir /data >/dev/null
mkdir -p /tmp/726-exp

# Export each synthetic node to a real parquet file on the host.
pond mknod synthetic-timeseries /gen/day1 --config-path /tmp/726-day1.yaml >/dev/null
pond mknod synthetic-timeseries /gen/day2 --config-path /tmp/726-day2.yaml >/dev/null
pond copy /gen/day1 "host:///tmp/726-exp" >/dev/null 2>&1
pond copy /gen/day2 "host:///tmp/726-exp" >/dev/null 2>&1
cp /tmp/726-exp/gen/day1 /tmp/726-v1.parquet
cp /tmp/726-exp/gen/day2 /tmp/726-v2.parquet
check '[ "$(head -c4 /tmp/726-v1.parquet | od -A n -t x1 | tr -d " ")" = "50415231" ]' \
    "exported v1 has PAR1 magic (valid parquet)"
check '[ "$(head -c4 /tmp/726-v2.parquet | od -A n -t x1 | tr -d " ")" = "50415231" ]' \
    "exported v2 has PAR1 magic (valid parquet)"

echo "--- Step 2: ingest version 1 as a table:series, backup add (auto-push) ---"
pond copy "host+series:///tmp/726-v1.parquet" /data/temps.series >/dev/null 2>&1
pond cat --sql "SELECT count(*) AS n FROM source" --format table /data/temps.series \
    > /tmp/726-p1-v1-count.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/726-p1-v1-count.txt' "producer series v1 has 7 rows"
pond backup add origin "file://${REMOTE}" > /tmp/726-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/726-backup.log' "backup add origin succeeded"

echo "--- Step 3: consumer cross-pond import + first pull ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/726-pull1.log 2>&1
check 'grep -q "pull upstream complete" /tmp/726-pull1.log' "consumer completed first pull"
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    /imports/up/data/temps.series > /tmp/726-p2-v1-count.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/726-p2-v1-count.txt' "consumer sees v1 only (7 rows)"
pond cat --sql "SELECT count(*) AS n FROM source WHERE temperature = 20.0" --format table \
    /imports/up/data/temps.series > /tmp/726-p2-v1-day2.txt 2>/dev/null
check 'grep -qE "\| 0 " /tmp/726-p2-v1-day2.txt' "consumer does NOT yet see v2 rows (day2)"

echo "--- Step 4: producer appends VERSION 2 to the SAME series (auto-push) ---"
export POND="$P1"
# host+series onto an EXISTING series path appends a new version (create-or-append).
pond copy "host+series:///tmp/726-v2.parquet" /data/temps.series > /tmp/726-append.log 2>&1
pond cat --sql "SELECT count(*) AS n FROM source" --format table /data/temps.series \
    > /tmp/726-p1-v2-count.txt 2>/dev/null
check 'grep -qE "\| 14 " /tmp/726-p1-v2-count.txt' "producer series now has 14 rows (2 versions merged)"

echo "--- Step 5: consumer re-pull applies the incremental version diff ---"
export POND="$P2"
pond pull upstream > /tmp/726-pull2.log 2>&1
check 'grep -q "pull upstream complete" /tmp/726-pull2.log' "consumer completed second pull"
pond cat --sql "SELECT count(*) AS n FROM source" --format table \
    /imports/up/data/temps.series > /tmp/726-p2-v2-count.txt 2>/dev/null
check 'grep -qE "\| 14 " /tmp/726-p2-v2-count.txt' "consumer sees BOTH versions merged (14 rows)"
pond cat --sql "SELECT count(*) AS n FROM source WHERE temperature = 10.0" --format table \
    /imports/up/data/temps.series > /tmp/726-p2-day1.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/726-p2-day1.txt' "consumer still has v1 rows (day1, 7 rows -- not lost)"
pond cat --sql "SELECT count(*) AS n FROM source WHERE temperature = 20.0" --format table \
    /imports/up/data/temps.series > /tmp/726-p2-day2.txt 2>/dev/null
check 'grep -qE "\| 7 " /tmp/726-p2-day2.txt' "consumer now has v2 rows (day2, 7 rows -- applied)"

echo "--- Step 6: consumer pulled-tip converges to the producer's pushed tip ---"
# short_tip (first 16 hex) is shared by PUSHED_TIP (producer) and PULLED_TIP
# (consumer), so they compare directly.  In cross-pond IMPORT mode the
# consumer's own commit tip differs from the producer's, but the recorded
# per-remote pulled tip must equal the producer's pushed tip.
PUSHED_TIP=$(POND="$P1" pond remote list 2>/dev/null | awk '$1=="origin" {print $5; exit}')
PULLED_TIP=$(POND="$P2" pond remote list 2>/dev/null | awk '$1=="upstream" {print $NF; exit}')
check '[ -n "'"${PUSHED_TIP}"'" ] && [ "'"${PUSHED_TIP}"'" != "-" ] && [ "'"${PUSHED_TIP}"'" = "'"${PULLED_TIP}"'" ]' \
    "consumer pulled tip equals producer pushed tip after incremental version pull"

check_finish
