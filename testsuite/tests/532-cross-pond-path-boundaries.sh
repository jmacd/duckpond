#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond path resolution boundaries
# DESCRIPTION:
#   Tests that absolute paths in an imported pond resolve within the
#   imported root, not the importing pond's root. This is the foundation
#   for running factories (e.g., sitegen) from foreign ponds where
#   site.yaml contains absolute paths like "/site/index.md".
#
#   Phase 1: Import a foreign pond by root (source_path: "/")
#   Phase 2: Verify absolute paths resolve within the import mount
#   Phase 3: Verify that paths which exist in the consumer but not the
#            producer are NOT resolvable from the imported context
#
#   This test exercises cross-pond path isolation: the imported
#   filesystem is a self-contained unit where "/" means the foreign
#   root, not the local root.
#
# EXPECTED:
#   - source_path: "/" imports the foreign root partition
#   - Imported content is accessible at the local_path
#   - Absolute paths from the producer resolve within the import mount
#   - Paths unique to the consumer pond are not visible in the import
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Cross-Pond Path Resolution Boundaries ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="path-boundary-test"

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
mc mb "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# POND1 — PRODUCER
# Has a self-contained site with absolute paths
#############################

echo ""
echo "=== Setting up Pond1 (Producer with site content) ==="

export POND=/pond1
pond init

PRODUCER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Producer pond_id: ${PRODUCER_POND_ID}"

# Create a site structure that uses absolute paths
pond mkdir /site
pond mkdir /content
pond mkdir /data

cat > /tmp/index.md << 'EOF'
---
title: Producer Site
---
# Welcome to the producer site
EOF

cat > /tmp/page.md << 'EOF'
---
title: About
---
# About this site
Content from the producer pond.
EOF

cat > /tmp/readings.csv << 'EOF'
timestamp,sensor,value
2024-01-01T00:00:00Z,temp,22.5
2024-01-01T01:00:00Z,temp,23.1
EOF

pond copy host:///tmp/index.md /site/index.md
pond copy host:///tmp/page.md /content/about.md
pond copy host:///tmp/readings.csv /data/readings.csv

echo "Producer site structure created:"
pond list '/**'

# Push backup to MinIO
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
echo "Producer backup pushed"

#############################
# POND2 — CONSUMER
# Imports producer by root, has its own content too
#############################

echo ""
echo "=== Setting up Pond2 (Consumer with own content) ==="

export POND=/pond2
pond init

CONSUMER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Consumer pond_id: ${CONSUMER_POND_ID}"

# Create content unique to the consumer
pond mkdir /local-only
cat > /tmp/consumer-file.txt << 'EOF'
This file only exists in the consumer pond
EOF
pond copy host:///tmp/consumer-file.txt /local-only/consumer.txt

# Also create a /site dir in the consumer (different content!)
pond mkdir /site
cat > /tmp/consumer-index.md << 'EOF'
---
title: Consumer Site
---
# This is the CONSUMER site, not the producer
EOF
pond copy host:///tmp/consumer-index.md /site/index.md

echo "Consumer own content created:"
pond list '/**'

#############################
# IMPORT PRODUCER BY ROOT
#############################

echo ""
echo "=== Importing producer pond by root ==="

pond mkdir /system
pond mkdir /system/etc

# Import with source_path: "/" — the entire foreign root
cat > /tmp/import-config.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/**"
  local_path: "/imports/producer"
EOF

pond mknod remote /system/etc/10-producer --config-path /tmp/import-config.yaml
echo "Import factory created"

pond run /system/etc/10-producer pull
echo "Import pull complete"

echo ""
echo "--- Consumer full listing after import ---"
POND=/pond2 pond list '/**' 2>&1 || true

#############################
# POSITIVE TESTS: Imported content accessible
#############################

echo ""
echo "=== Positive tests: imported content accessible ==="

# Verify imported files are visible at the mount point
POND=/pond2 pond list '/imports/producer/**' > /tmp/imported-listing.txt 2>&1 || true
cat /tmp/imported-listing.txt

check 'grep -q "index.md" /tmp/imported-listing.txt' "imported /site/index.md visible"
check 'grep -q "about.md" /tmp/imported-listing.txt' "imported /content/about.md visible"
check 'grep -q "readings.csv" /tmp/imported-listing.txt' "imported /data/readings.csv visible"

# Verify data integrity
POND=/pond2 pond cat /imports/producer/site/index.md > /tmp/imported-index.txt 2>/dev/null || true
check 'grep -q "Producer Site" /tmp/imported-index.txt' "imported index.md has producer content"
check '! grep -q "Consumer" /tmp/imported-index.txt' "imported index.md does NOT have consumer content"

POND=/pond2 pond cat /imports/producer/content/about.md > /tmp/imported-about.txt 2>/dev/null || true
check 'grep -q "producer pond" /tmp/imported-about.txt' "imported about.md has producer content"

POND=/pond2 pond cat /imports/producer/data/readings.csv > /tmp/imported-readings.txt 2>/dev/null || true
POND=/pond1 pond cat /data/readings.csv > /tmp/original-readings.txt 2>/dev/null || true
ORIG_HASH=$(md5sum /tmp/original-readings.txt | cut -d' ' -f1)
IMP_HASH=$(md5sum /tmp/imported-readings.txt | cut -d' ' -f1)
check '[ "${ORIG_HASH}" = "${IMP_HASH}" ]' "imported readings.csv matches original byte-for-byte"

#############################
# NEGATIVE TESTS: Path isolation
#############################

echo ""
echo "=== Negative tests: path isolation ==="

# Consumer's /local-only/consumer.txt must NOT be visible inside /imports/producer/
POND=/pond2 pond cat /imports/producer/local-only/consumer.txt > /tmp/escape-test.txt 2>&1 || true
check 'grep -qi "not found\|error" /tmp/escape-test.txt' "consumer-only path not resolvable in import"

# Consumer's /site/index.md is different from producer's — the imported
# version at /imports/producer/site/index.md must have producer content
POND=/pond2 pond cat /site/index.md > /tmp/consumer-site.txt 2>/dev/null || true
POND=/pond2 pond cat /imports/producer/site/index.md > /tmp/producer-site.txt 2>/dev/null || true
check 'grep -q "Consumer" /tmp/consumer-site.txt' "consumer /site/index.md has consumer content"
check 'grep -q "Producer" /tmp/producer-site.txt' "imported /site/index.md has producer content"
check '! grep -q "Consumer" /tmp/producer-site.txt' "imported /site/index.md does not leak consumer content"

# The producer's /system/ directory should NOT be imported (or if imported,
# its backup config should not interfere with the consumer's system)
POND=/pond2 pond list '/imports/producer/system/**' > /tmp/imported-system.txt 2>&1 || true
echo "Imported system listing: $(cat /tmp/imported-system.txt)"

#############################
# PROVENANCE
#############################

echo ""
echo "=== Provenance check ==="
echo "Producer pond_id: ${PRODUCER_POND_ID}"
echo "Consumer pond_id: ${CONSUMER_POND_ID}"
check '[ "${PRODUCER_POND_ID}" != "${CONSUMER_POND_ID}" ]' "producer and consumer have different pond_ids"

#############################
# RESULTS
#############################

check_finish
