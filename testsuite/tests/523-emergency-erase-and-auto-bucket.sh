#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Emergency erase-bucket + automatic Delta-table re-init on attach
#
# DESCRIPTION:
#   End-to-end coverage of the "lose the bucket, reattach a fresh pond"
#   recovery path on top of the D5.7b backup CLI:
#     1. Pond1: `pond backup add origin s3://BUCKET` (auto-inits an empty
#        bucket as a Delta table tagged with Pond1's pond_id) and push.
#     2. Confirm the bucket has the expected Delta layout.
#     3. `pond emergency erase-bucket s3://BUCKET --dangerous` wipes every
#        object (the bucket itself stays).
#     4. Pond2: fresh `pond init`; `pond backup add origin SAME-URL` now
#        sees an empty bucket and auto-inits with Pond2's pond_id; push
#        succeeds (proves the empty bucket was reusable).
#     5. Pond3: fresh `pond init`; `pond backup add origin SAME-URL`
#        without erasing the bucket again -- must be REFUSED with a
#        store_id / foreign-pond mismatch (proves erase is required to
#        repurpose a populated bucket).
#
# EXPECTED:
#   - Step 1 attach + push succeed and populate the bucket.
#   - Step 3 erase leaves the bucket existing but empty.
#   - Step 4 attach + push succeed against the empty bucket.
#   - Step 5 attach is rejected with a mismatch error mentioning
#     both pond ids.
#
# History:
#   Migrated D5.8.2: rewritten on top of the D5.7b backup CLI
#   (`pond backup add`, `pond push`, `pond emergency erase-bucket`)
#   after the legacy `remote` factory + `pond apply` configuration
#   flow was removed.  The original test relied on auto-push from
#   `pond run /system/run/10-remote` and the legacy YAML config.
set -e

echo "=== Experiment: Emergency Erase & Auto-Bucket Re-init ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="watertown-erase-test"
BUCKET_URL="s3://${BUCKET_NAME}"

CHECKS_TOTAL=0
CHECKS_FAILED=0

check() {
    local condition="$1"
    local description="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${condition}"; then
        echo "  ✓ ${description}"
    else
        echo "  ✗ ${description}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

check_eq() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" == "${expected}" ]]; then
        echo "  ✓ ${description} (${actual})"
    else
        echo "  ✗ ${description}: got ${actual}, expected ${expected}"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

# Helper to invoke `pond backup add origin` against the test bucket.
# Usage: attach_origin
attach_origin() {
    pond backup add origin "${BUCKET_URL}" \
        --region us-east-1 \
        --endpoint "${MINIO_ENDPOINT}" \
        --access-key-id "${MINIO_ROOT_USER}" \
        --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
        --allow-http
}

#############################
# CHECK MINIO AVAILABILITY
#############################

echo "=== Checking MinIO availability ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    echo "       This test must run via 'run-test.sh --compose 523'."
    exit 1
fi
echo "[OK] MinIO reachable at ${MINIO_ENDPOINT}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true   # ensure clean start
mc mb --ignore-existing "local/${BUCKET_NAME}"
echo "[OK] Bucket ${BUCKET_NAME} ready"

#############################
# PHASE 1: POND1 ATTACH + PUSH
#############################

echo ""
echo "=== Phase 1: Pond1 attach + push ==="

export POND=/pond1
rm -rf "${POND}"
pond init --birthplace test-host

POND1_ID=$(pond config | grep "Pond ID" | awk '{print $NF}')
echo "Pond1 ID: ${POND1_ID}"

attach_origin
echo "[OK] Pond1 attached origin (auto-initialized bucket Delta table)"

pond mkdir -p /data
echo "hello from pond1" > /tmp/pond1.txt
pond copy "host:///tmp/pond1.txt" /data/pond1.txt
rm -f /tmp/pond1.txt

pond push origin
echo "[OK] Pond1 push complete"

# Sanity check: the bucket now has Delta-table content.
OBJ_COUNT_AFTER_PUSH=$(mc ls --recursive "local/${BUCKET_NAME}" | wc -l | tr -d ' ')
echo "Bucket objects after Pond1 push: ${OBJ_COUNT_AFTER_PUSH}"
check '[ "${OBJ_COUNT_AFTER_PUSH}" -gt 0 ]' "bucket non-empty after push"
check 'mc ls --recursive "local/${BUCKET_NAME}" | grep -q "_delta_log/"' "bucket has _delta_log/"

#############################
# PHASE 2: ERASE BUCKET
#############################

echo ""
echo "=== Phase 2: Erase bucket via 'pond emergency erase-bucket' ==="

# Refuses without --dangerous (safety guard).
REFUSE_OUT=$(pond emergency erase-bucket "${BUCKET_URL}" \
    --endpoint "${MINIO_ENDPOINT}" \
    --region us-east-1 \
    --access-key "${MINIO_ROOT_USER}" \
    --secret-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http 2>&1 || true)
echo "${REFUSE_OUT}"
check 'echo "${REFUSE_OUT}" | grep -qi "dangerous"' "erase-bucket refuses without --dangerous"

# Bucket is still populated.
OBJ_COUNT_AFTER_REFUSAL=$(mc ls --recursive "local/${BUCKET_NAME}" | wc -l | tr -d ' ')
check_eq "bucket untouched after refusal" "${OBJ_COUNT_AFTER_REFUSAL}" "${OBJ_COUNT_AFTER_PUSH}"

# Actually erase.
pond emergency erase-bucket "${BUCKET_URL}" \
    --endpoint "${MINIO_ENDPOINT}" \
    --region us-east-1 \
    --access-key "${MINIO_ROOT_USER}" \
    --secret-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http \
    --dangerous
echo "[OK] Bucket erased"

OBJ_COUNT_AFTER_ERASE=$(mc ls --recursive "local/${BUCKET_NAME}" | wc -l | tr -d ' ')
check_eq "bucket empty after erase" "${OBJ_COUNT_AFTER_ERASE}" "0"

# The bucket itself must still exist (erase-bucket only deletes objects).
check 'mc ls "local/${BUCKET_NAME}" >/dev/null 2>&1' "bucket itself still exists"

#############################
# PHASE 3: POND2 REUSES ERASED BUCKET
#############################

echo ""
echo "=== Phase 3: Pond2 attaches same URL, bucket auto-inits ==="

export POND=/pond2
rm -rf "${POND}"
pond init --birthplace test-host

POND2_ID=$(pond config | grep "Pond ID" | awk '{print $NF}')
echo "Pond2 ID: ${POND2_ID}"
check '[ "${POND1_ID}" != "${POND2_ID}" ]' "Pond1 and Pond2 have distinct ids"

attach_origin
echo "[OK] Pond2 attached origin (bucket re-initialized with Pond2 id)"

pond mkdir -p /data
echo "hello from pond2" > /tmp/pond2.txt
pond copy "host:///tmp/pond2.txt" /data/pond2.txt
rm -f /tmp/pond2.txt

pond push origin
echo "[OK] Pond2 push complete"

OBJ_COUNT_POND2=$(mc ls --recursive "local/${BUCKET_NAME}" | wc -l | tr -d ' ')
check '[ "${OBJ_COUNT_POND2}" -gt 0 ]' "bucket non-empty after Pond2 push"

#############################
# PHASE 4: POND3 ATTACH WITHOUT ERASE MUST FAIL
#############################

echo ""
echo "=== Phase 4: Pond3 attach to occupied bucket should fail ==="

export POND=/pond3
rm -rf "${POND}"
pond init --birthplace test-host

POND3_ID=$(pond config | grep "Pond ID" | awk '{print $NF}')
echo "Pond3 ID: ${POND3_ID}"
check '[ "${POND2_ID}" != "${POND3_ID}" ]' "Pond2 and Pond3 have distinct ids"

# Bucket currently holds Pond2's pond_id; attach must be rejected.
ATTACH_OUT=$(attach_origin 2>&1 || true)
echo "${ATTACH_OUT}"

check 'echo "${ATTACH_OUT}" | grep -qi "does not match\|mismatch\|foreign pond"' \
    "Pond3 attach refused with mismatch error"

# Pond3 should NOT have committed the backup attachment.
LIST_OUT=$(pond backup list 2>&1 || true)
check '! echo "${LIST_OUT}" | grep -q "origin"' \
    "Pond3 has no 'origin' backup attached after refused init"

#############################
# CLEANUP
#############################

rm -rf /pond1 /pond2 /pond3
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# SUMMARY
#############################

echo ""
echo "=== VERIFICATION ==="
echo "Total checks: ${CHECKS_TOTAL}, failed: ${CHECKS_FAILED}"

if [ "${CHECKS_FAILED}" -eq 0 ]; then
    echo "[OK] Test passed"
    exit 0
else
    echo "[FAIL] ${CHECKS_FAILED} check(s) failed"
    exit 1
fi
