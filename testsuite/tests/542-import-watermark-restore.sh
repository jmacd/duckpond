#!/bin/bash
# DISABLED-D4: cross-pond import was implemented by the legacy `remote` factory (deleted in D4.5); D5 will reintroduce it via row-level `pond_id` partitioning of tlogfs.
# EXPERIMENT: Replica reconstructs import watermark on first pull
# DESCRIPTION:
#   When a consumer pond is rebuilt from its own backup (the way
#   the cloud sitegen pond is rebuilt by terraform teardown/setup),
#   the steward control table is REPLICA-SPECIFIC -- it is created
#   fresh by create_pond_for_restoration and does not inherit the
#   source pond's audit log, factory modes, or import partition
#   records.
#
#   The replica must therefore "reconstruct" the import watermark
#   on its first pull by walking the foreign txn range once and
#   persisting the high-water mark into its own control table.
#   Subsequent pulls (which are the steady state) must resume from
#   that persisted mark and NOT re-walk from zero.
#
#   This wires up two MinIO buckets:
#     foreign-bucket   -- Pond1 (producer) backs up here, Pond2
#                         imports from here.
#     consumer-bucket  -- Pond2 backs up to here.  Pond3 restores
#                         from here.
#
# EXPECTED:
#   - Pond3 pull #1: walks foreign 1..=N, persists watermark = N.
#     Allowed to start at "Watermark: 0".
#   - Pond3 pull #2: must resume from N (or report up to date),
#     NEVER restart at "Watermark: 0".  This is the regression
#     guard for the cloud-sitegen bandwidth-bleed scenario after
#     terraform recreates the pond.
set -e

# DISABLED-D4 skip block (see header comment)
echo "SKIP: this test is DISABLED-D4 (see header for details)" >&2
exit 0


source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Import watermark survives pond restore ==="
echo ""

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
FOREIGN_BUCKET="watermark-restore-foreign"
CONSUMER_BUCKET="watermark-restore-consumer"

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
for b in "${FOREIGN_BUCKET}" "${CONSUMER_BUCKET}"; do
    mc rb --force "local/${b}" 2>/dev/null || true
    mc mb "local/${b}" 2>/dev/null || true
done

#############################
# POND1 -- PRODUCER -> foreign-bucket
#############################

echo ""
echo "=== Setting up Pond1 (Producer) ==="

export POND=/pond1
pond init

pond mkdir /data
pond mkdir /data/sensors

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
2024-01-01T01:00:00Z,roof,4.8
EOF

pond copy host:///tmp/temps.csv /data/sensors/temps.csv

pond mkdir /system
pond mkdir /system/run

cat > /tmp/producer-backup.yaml << EOF
url: "s3://${FOREIGN_BUCKET}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF

pond mknod remote /system/run/10-backup --config-path /tmp/producer-backup.yaml
pond run /system/run/10-backup push

#############################
# POND2 -- CONSUMER imports foreign-bucket, backs up to consumer-bucket
#############################

echo ""
echo "=== Setting up Pond2 (Consumer) ==="

export POND=/pond2
pond init
pond mkdir /system
pond mkdir /system/etc
pond mkdir /system/run

cat > /tmp/import.yaml << EOF
url: "s3://${FOREIGN_BUCKET}"
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

cat > /tmp/consumer-backup.yaml << EOF
url: "s3://${CONSUMER_BUCKET}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF
pond mknod remote /system/run/20-backup --config-path /tmp/consumer-backup.yaml

#############################
# Pond2 pull #1 -- imports from foreign
#############################

echo ""
echo "=== Pond2 pull #1 (imports from foreign-bucket) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/p2pull1.log

POND2_WATERMARK=$(grep -oE "Updated watermark to [0-9]+" /tmp/p2pull1.log \
    | head -1 | awk '{print $4}')
check '[ -n "${POND2_WATERMARK}" ] && [ "${POND2_WATERMARK}" -gt 0 ]' \
      "Pond2 pull #1 advanced watermark above 0"
echo "Pond2 watermark after pull #1: ${POND2_WATERMARK}"

#############################
# Pond2 pushes to consumer-bucket
#############################

echo ""
echo "=== Pond2 pushes to consumer-bucket ==="
pond run /system/run/20-backup push

#############################
# POND3 -- restore from consumer-bucket
#############################

echo ""
echo "=== Pond3: pond init --config=<base64> against consumer-bucket ==="

# Generate the base64 replication config (carries source pond identity)
POND=/pond2 pond run /system/run/20-backup replicate > /tmp/replicate.out 2>&1
INIT_CMD=$(grep -oE "pond init --config=[^[:space:]]+" /tmp/replicate.out | head -1)
check '[ -n "${INIT_CMD}" ]' "Replicate produced init command"

# Extract just the encoded portion
INIT_CONFIG=$(echo "${INIT_CMD}" | sed 's/^pond init --config=//')

export POND=/pond3
pond init --config="${INIT_CONFIG}" 2>&1 | tee /tmp/p3init.log
check 'grep -q "Pond initialized from backup successfully" /tmp/p3init.log' \
      "Pond3 init from backup completed"

# Confirm Pond3 has the imported data already
pond list '/sources/producer/**' > /tmp/p3list.log 2>&1 || true
check 'grep -q "temps.csv" /tmp/p3list.log' \
      "Pond3 inherited the imported file from Pond2"

#############################
# Pond3 runs the import factory -- the regression check
#############################

echo ""
echo "=== Pond3 pull #1 (constructs watermark from scratch by walking foreign) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/p3pull1.log

# Pull #1 on a fresh replica is permitted to start at watermark 0:
# the steward control table is replica-specific and was created
# empty by create_pond_for_restoration.  This pull "constructs"
# the watermark by walking the foreign txn range once.  That is
# the agreed cost of restore.
P3_WATERMARK=$(grep -oE "Updated watermark to [0-9]+" /tmp/p3pull1.log \
    | head -1 | awk '{print $4}')
check '[ -n "${P3_WATERMARK}" ] && [ "${P3_WATERMARK}" -gt 0 ]' \
      "Pond3 pull #1 constructed and persisted a watermark"

# Sanity: the imported file is queryable on Pond3 (it should already
# be there from the data-Delta restore; pull #1 just establishes the
# watermark and would only re-download anything missing).
pond list '/sources/producer/**' > /tmp/p3list.log 2>&1 || true
check 'grep -q "temps.csv" /tmp/p3list.log' \
      "Pond3 has the imported file"

echo ""
echo "=== Pond3 pull #2 (the regression check: must NOT re-walk) ==="

pond run /system/etc/10-import pull 2>&1 | tee /tmp/p3pull2.log

# THIS is the assertion that matters.  After pull #1 has constructed
# and persisted the watermark, pull #2 must resume from it.  If the
# steward control table didn't survive the restore (the original
# concern), pull #1 would persist into a transient table and pull #2
# would still see "Watermark: 0".
check '! grep -q "Watermark: 0 (will import transactions > 0)" /tmp/p3pull2.log' \
      "Pond3 pull #2 does NOT restart at watermark 0"

# Specifically: pull #2 should resume from pull #1's watermark, OR
# report already up to date.
check 'grep -qE "Already up to date|Watermark: [1-9]" /tmp/p3pull2.log' \
      "Pond3 pull #2 resumed above 0 or reports up to date"

# Belt-and-suspenders: no full-range re-walk.
check '! grep -qE "[0-9]+ new transaction.s. to import: \"1\.\.=" /tmp/p3pull2.log' \
      "Pond3 pull #2 did not full-range re-walk"

check_finish
