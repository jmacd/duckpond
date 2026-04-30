#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Recursive cross-pond import advances watermark incrementally
# DESCRIPTION:
#   Reproduces a production bandwidth bug observed on the cloud
#   sitegen pond: every tick of pond@site-prod re-walked transactions
#   1..=N of every foreign pond's backup, even though the local store
#   already had every file.  Logs showed:
#       Watermark: 0 (will import transactions > 0)
#       679 new transaction(s) to import: "1..=679"
#       [OK] Import complete: 5 partition(s), 0 file(s) downloaded
#   on every tick, sustained -- 25 Mb/s inbound for 2 h with zero
#   useful bytes downloaded.  See ./remote-bandwidth-bug.md (in the
#   caspar.water repo) for the full investigation.
#
#   The watermark is supposed to be persisted into the local pond's
#   import metadata after each successful import (factory.rs:1707-1720,
#   `state.add_import_metadata(...)`) and read back on the next tick
#   into `FactoryContext.import_partitions` (factory.rs:1496-1507).
#   Either the write isn't surviving or the read isn't seeing it.
#
# EXPECTED:
#   - First pull starts at watermark 0 and imports the full txn range.
#   - Second pull (no upstream changes) reports "Already up to date"
#     OR starts at a non-zero watermark.  Specifically it must NOT
#     report `Watermark: 0` again, because that means the persisted
#     watermark was lost between ticks.
#   - Third pull, after a single new commit on the producer, reports
#     a small new-txn range (e.g. "N..=N+1"), NOT a full re-walk.
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Import watermark advances incrementally ==="
echo ""

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="import-watermark-test"

#############################
# CONFIGURE MINIO
#############################

echo "=== Configuring MinIO ==="
for i in $(seq 1 30); do
    if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
        echo "MinIO ready"
        break
    fi
    sleep 1
done

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true
mc mb "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# POND1 -- PRODUCER
#############################

echo ""
echo "=== Setting up Pond1 (Producer) ==="

export POND=/pond1
pond init

pond mkdir /data
pond mkdir /data/sensors
pond mkdir /data/logs

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
2024-01-01T01:00:00Z,roof,4.8
EOF

cat > /tmp/events.csv << 'EOF'
timestamp,level,message
2024-01-01T00:00:00Z,INFO,system started
EOF

pond copy host:///tmp/temps.csv  /data/sensors/temps.csv
pond copy host:///tmp/events.csv /data/logs/events.csv

pond mkdir /system
pond mkdir /system/run

cat > /tmp/producer-backup.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF

pond mknod remote /system/run/10-backup --config-path /tmp/producer-backup.yaml
pond run /system/run/10-backup push
echo "Pond1 pushed initial backup"

#############################
# POND2 -- CONSUMER (recursive import, /**)
#############################

echo ""
echo "=== Setting up Pond2 (Consumer) ==="

export POND=/pond2
pond init
pond mkdir /system
pond mkdir /system/etc

cat > /tmp/import.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/data/**"
  local_path: "/sources/producer"
EOF

pond mknod remote /system/etc/10-import --config-path /tmp/import.yaml

#############################
# PULL 1 -- initial import
#############################

echo ""
echo "=== Pull #1 (initial import) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull1.log

check 'grep -q "Watermark: 0 (will import transactions > 0)" /tmp/pull1.log' \
      "pull #1 starts at watermark 0 (expected for first import)"
check 'grep -q "Updated watermark to" /tmp/pull1.log' \
      "pull #1 logs that it persisted a new watermark"

# Confirm the imported data is actually queryable.
POND=/pond2 pond list '/sources/producer/**' > /tmp/pull1.list 2>&1 || true
check 'grep -q "temps.csv"  /tmp/pull1.list' "pull #1: temps.csv imported"
check 'grep -q "events.csv" /tmp/pull1.list' "pull #1: events.csv imported"

POND=/pond2 pond cat /sources/producer/sensors/temps.csv > /tmp/pull1.temps 2>/dev/null || true
check 'grep -q "roof" /tmp/pull1.temps' "pull #1: temps content matches"

# Capture the watermark value the first pull says it persisted.  This
# is the value the *second* pull must come back to, otherwise the
# write was lost.
PULL1_WATERMARK=$(grep -oE "Updated watermark to [0-9]+" /tmp/pull1.log \
    | head -1 | awk '{print $4}')
echo "Pull #1 persisted watermark: ${PULL1_WATERMARK}"
check '[ -n "${PULL1_WATERMARK}" ] && [ "${PULL1_WATERMARK}" -gt 0 ]' \
      "pull #1 advanced watermark above 0"

#############################
# PULL 2 -- no upstream changes (the bug)
#############################

echo ""
echo "=== Pull #2 (no upstream changes -- watermark must persist) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull2.log

# The two acceptable behaviours on the second pull are:
#   (a) "Already up to date (no transactions after N)" -- early exit
#   (b) "Watermark: N (will import transactions > N)" with N == PULL1_WATERMARK
# Either proves the persisted watermark survived.
#
# What we MUST NOT see:
#   "Watermark: 0 (will import transactions > 0)"
# That is the production bug -- the watermark was forgotten and the
# whole foreign txn range is being walked again, every tick, forever.
check '! grep -q "Watermark: 0 (will import transactions > 0)" /tmp/pull2.log' \
      "pull #2 does NOT restart at watermark 0 (the bandwidth-bleeding bug)"
check 'grep -qE "Already up to date|Watermark: [1-9]" /tmp/pull2.log' \
      "pull #2 either skips early or starts above watermark 0"

# Belt-and-suspenders: also assert no full-range walk message.
check '! grep -qE "[0-9]+ new transaction.s. to import: \"1\.\.=" /tmp/pull2.log' \
      "pull #2 does not report a full-range '1..=N' re-walk"

#############################
# PULL 3 -- one new commit upstream (incremental delta)
#############################

echo ""
echo "=== Producer adds one new file, pushes ==="

cat > /tmp/extra.csv << 'EOF'
timestamp,location,temperature
2024-01-02T00:00:00Z,roof,3.1
EOF

POND=/pond1 pond copy host:///tmp/extra.csv /data/sensors/extra.csv
POND=/pond1 pond run /system/run/10-backup push

echo ""
echo "=== Pull #3 (one upstream txn -- must do an incremental delta) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull3.log

# After pull #3, we must (a) have the new file, and (b) have walked
# only the small delta -- not transactions 1..=N+1 again.
POND=/pond2 pond cat /sources/producer/sensors/extra.csv > /tmp/pull3.extra 2>/dev/null || true
check 'grep -q "2024-01-02" /tmp/pull3.extra' \
      "pull #3 imported the new file"

check '! grep -q "Watermark: 0 (will import transactions > 0)" /tmp/pull3.log' \
      "pull #3 does not restart at watermark 0"
check "grep -q \"Watermark: ${PULL1_WATERMARK}\" /tmp/pull3.log" \
      "pull #3 resumes from pull #1's persisted watermark"
check '! grep -qE "[0-9]+ new transaction.s. to import: \"1\.\.=" /tmp/pull3.log' \
      "pull #3 does not full-range re-walk"

check_finish
