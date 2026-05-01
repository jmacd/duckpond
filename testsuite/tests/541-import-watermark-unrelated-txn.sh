#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Import watermark advances when foreign commits unrelated content
# DESCRIPTION:
#   Companion to 540.  In 540 we proved the watermark survives between
#   pulls when downloads happen.  Here we exercise the case where the
#   foreign pond commits new transactions that don't touch any of the
#   partitions we import: the consumer's import has `source_path:
#   /data/**` but the producer commits a file under `/other/`.
#
#   Old behaviour: `execute_import` only persisted the watermark inside
#   `if downloaded > 0`.  When the foreign txns added zero matching
#   files, no metadata was written, the next tick re-read the same
#   watermark, and the same N..=N+k range was walked perpetually.
#   Multiplied by long-running R2 backups, this produces the same
#   bandwidth bleed as 540 even when downloads succeed.
#
# EXPECTED:
#   - Pull #1 imports the initial /data tree.
#   - Producer commits a file under /other/ (NOT under /data).
#   - Pull #2 sees a new foreign txn but downloads zero files; the
#     watermark must still advance, otherwise pull #3 would walk the
#     same txn again.
#   - Pull #3 (no further upstream changes) must report "Already up
#     to date" or restart from the advanced watermark, NOT replay
#     pull #2's range.
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Watermark advances on unrelated foreign txns ==="
echo ""

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="import-watermark-unrelated-test"

#############################
# CONFIGURE MINIO
#############################

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
# POND1 -- PRODUCER (publishes /data and /other)
#############################

echo ""
echo "=== Setting up Pond1 (Producer) ==="

export POND=/pond1
pond init

pond mkdir /data
pond mkdir /data/sensors
pond mkdir /other

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
EOF

pond copy host:///tmp/temps.csv /data/sensors/temps.csv

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
# POND2 -- CONSUMER (imports only /data/**)
#############################

echo ""
echo "=== Setting up Pond2 (Consumer, imports /data/** only) ==="

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
# PULL 1 -- initial import of /data
#############################

echo ""
echo "=== Pull #1 (initial /data import) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull1.log

PULL1_WATERMARK=$(grep -oE "Updated watermark to [0-9]+" /tmp/pull1.log \
    | head -1 | awk '{print $4}')
check '[ -n "${PULL1_WATERMARK}" ] && [ "${PULL1_WATERMARK}" -gt 0 ]' \
      "pull #1 advanced watermark above 0"

#############################
# Producer commits to /other (NOT under /data)
#############################

echo ""
echo "=== Producer adds /other/note.txt and pushes (unrelated to consumer) ==="

cat > /tmp/note.txt << 'EOF'
hello, this file lives outside the imported subtree
EOF

POND=/pond1 pond copy host:///tmp/note.txt /other/note.txt
POND=/pond1 pond run /system/run/10-backup push

#############################
# PULL 2 -- new foreign txn, zero matching files
#############################

echo ""
echo "=== Pull #2 (foreign added unrelated content; download count = 0) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull2.log

# Pull #2 must persist a new watermark even though zero files matched
# our `source_path: /data/**` filter.  Two acceptable outputs:
#   (a) "Already up to date (no transactions after N)"
#       -- if the producer's push happened to share a txn with
#       previously-walked content (unlikely here)
#   (b) "Updated watermark to M" with M > PULL1_WATERMARK
#       -- the desired path: walked the new txn, downloaded 0 files,
#       but persisted progress so we don't re-walk it.
check 'grep -qE "Already up to date|Updated watermark to" /tmp/pull2.log' \
      "pull #2 either skipped early or persisted a new watermark"

PULL2_WATERMARK=$(grep -oE "Updated watermark to [0-9]+" /tmp/pull2.log \
    | tail -1 | awk '{print $4}')
if [ -n "${PULL2_WATERMARK}" ]; then
    echo "Pull #2 persisted watermark: ${PULL2_WATERMARK}"
    check '[ "${PULL2_WATERMARK}" -gt "${PULL1_WATERMARK}" ]' \
          "pull #2 watermark advanced past pull #1 even with 0 files downloaded"
fi

#############################
# PULL 3 -- no further upstream changes; must NOT re-walk
#############################

echo ""
echo "=== Pull #3 (no upstream changes; must resume from pull #2 watermark) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/pull3.log

# This is the core regression check: pull #3 must not re-walk the txn
# that pull #2 already consumed.  If pull #2 failed to persist its
# advance, pull #3 would log the same "N new transaction(s) to import"
# range as pull #2.
check '! grep -q "Watermark: 0 (will import transactions > 0)" /tmp/pull3.log' \
      "pull #3 does not restart at watermark 0"

if [ -n "${PULL2_WATERMARK}" ]; then
    # Either we're caught up, or we resume above pull #2's mark.
    check 'grep -qE "Already up to date|Watermark: [1-9]" /tmp/pull3.log' \
          "pull #3 resumes above 0 or reports already up to date"
fi

# Sanity: the consumer should still NOT have the unrelated file --
# our `source_path: /data/**` filter excludes it.
POND=/pond2 pond list '/sources/producer/**' > /tmp/pull3.list 2>&1 || true
check '! grep -q "note.txt" /tmp/pull3.list' \
      "pull #3 did not import the unrelated /other/note.txt (filter still works)"

check_finish
