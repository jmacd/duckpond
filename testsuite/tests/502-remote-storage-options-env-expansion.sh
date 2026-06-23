#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Remote S3 storage options with ${env:VAR} credential expansion
# DESCRIPTION:
#   The remote attachment YAML at /sys/remotes/<name> is an oplog row that
#   `pond push` replicates to every backup, so credentials must be stored as
#   ${env:VAR} references, never literal secrets.  to_storage_options() then
#   resolves those references against the LOCAL process environment at use
#   time, so the secret feeds S3 auth without ever being persisted.
#
#   This test exercises that path end-to-end against MinIO:
#     Phase 1: `pond backup add` rejects a literal secret_access_key.
#     Phase 2: an unset ${env:VAR} reference fails fast with a clear error
#              (no silent fallback, never forwarded to S3).
#     Phase 3: a ${env:VAR} secret resolves and a real push to MinIO succeeds.
#     Phase 4: the stored config holds the ${env:...} reference, NOT the
#              resolved secret -- proving nothing sensitive is persisted.
#     Phase 5: with the env var unset, `pond verify` fails (the secret is
#              read from the live environment on every use, not baked in);
#              re-exporting it makes verify clean again.
#
# EXPECTED:
#   - Literal secrets are rejected; unset refs error; env-ref secrets work.
#   - The on-disk config never contains the literal password.
set -e
source check.sh

echo "=== Experiment: remote storage options + env-var expansion ==="

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="duckpond-envexp"

#############################
# MINIO AVAILABILITY
#############################

echo "--- Checking MinIO availability ---"
if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
    echo "[OK] MinIO running at ${MINIO_ENDPOINT}"
else
    echo "[FAIL] MinIO not available -- this test requires MinIO (compose)"
    exit 1
fi

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc mb --ignore-existing "local/${BUCKET_NAME}" >/dev/null
echo "[OK] bucket ready"

#############################
# SETUP POND
#############################

export POND=/tmp/502-pond
rm -rf "$POND"
pond init --birthplace test-host >/dev/null
pond mkdir /data >/dev/null
printf 'timestamp,sensor_id,value\n2024-01-01T00:00:00Z,s1,1.0\n2024-01-01T01:00:00Z,s1,2.0\n' > /tmp/502-data.csv
pond copy host:///tmp/502-data.csv /data/data.csv >/dev/null
echo "[OK] pond initialized with data"

#############################
# PHASE 1: reject literal secret
#############################

echo ""
echo "=== Phase 1: literal secret is rejected at add time ==="
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http > /tmp/502-literal.log 2>&1 || true
check_contains /tmp/502-literal.log "literal secret rejected" "must be an environment reference"
check '! pond backup list | grep -q "^origin "' "no attachment created after rejection"

#############################
# PHASE 2: unset env ref errors
#############################

echo ""
echo "=== Phase 2: unset \${env:VAR} reference fails fast ==="
unset POND_502_MISSING_SECRET
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:POND_502_MISSING_SECRET}' \
    --allow-http > /tmp/502-unset.log 2>&1 || true
check_contains /tmp/502-unset.log "unset env ref surfaces an error" "environment variable substitution failed"
check '! pond backup list | grep -q "^origin "' "no attachment created on unset env ref"

#############################
# PHASE 3: env ref resolves -> push succeeds
#############################

echo ""
echo "=== Phase 3: \${env:MINIO_ROOT_PASSWORD} resolves -> push works ==="
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http > /tmp/502-add.log 2>&1
check 'pond backup list | grep -q "^origin "' "origin attached with env-ref secret"

pond push origin > /tmp/502-push.log 2>&1
mc ls --recursive "local/${BUCKET_NAME}/" > /tmp/502-mc.txt 2>&1
check 'grep -q "_delta_log/" /tmp/502-mc.txt' "push populated the bucket (env secret fed S3 auth)"
check 'grep -q "\.parquet" /tmp/502-mc.txt'   "bucket contains parquet data"

#############################
# PHASE 4: stored config holds the ref, not the secret
#############################

echo ""
echo "=== Phase 4: persisted config carries the reference, not the secret ==="
pond cat /sys/remotes/origin > /tmp/502-config.yaml 2>/dev/null
# Scope the secret-leak assertion to the secret_access_key line: the
# default MinIO user and password are both "minioadmin", so a whole-file
# search would false-match on the access_key_id line.
grep '^secret_access_key:' /tmp/502-config.yaml > /tmp/502-secretline.txt || true
check_contains /tmp/502-secretline.txt "secret stored as an env reference" '${env:MINIO_ROOT_PASSWORD}'
check_not_contains /tmp/502-secretline.txt "secret line does NOT leak the resolved value" "${MINIO_ROOT_PASSWORD}"

#############################
# PHASE 5: secret resolved per-use from the live environment
#############################

echo ""
echo "=== Phase 5: secret comes from the live env on every use ==="
SAVED_SECRET="${MINIO_ROOT_PASSWORD}"
unset MINIO_ROOT_PASSWORD
pond verify origin > /tmp/502-verify-unset.log 2>&1 || true
check_contains /tmp/502-verify-unset.log "verify fails when the env var is unset" \
    "environment variable substitution failed"

export MINIO_ROOT_PASSWORD="${SAVED_SECRET}"
pond verify origin > /tmp/502-verify-set.log 2>&1
check_contains /tmp/502-verify-set.log "verify clean once the env var is restored" \
    "live data matches remote"

check_finish
