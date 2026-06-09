#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond import watermark advances on ANY foreign txn
#             (not just file-content commits) (D5.4 + D5.7b)
#
# DESCRIPTION:
#   Companion to 540.  Where 540 proved the watermark survives no-op
#   pulls, this test proves the watermark advances even for foreign
#   transactions that don't introduce new file content -- e.g.
#   mkdir-only commits.
#
#   The legacy `remote` factory (deleted in D4.5) had a known bug here:
#   `execute_import` only persisted the watermark `if downloaded > 0`,
#   so foreign txns under filtered-out subpaths produced 0 file
#   downloads and the watermark never advanced.  Multiplied by a long-
#   running ingest, this re-walked the same N..=N+k range forever
#   (production bandwidth bleed observed on pond@site-prod).
#
#   D5.7b cross-pond import replicates the entire foreign data table
#   per Delta version (no source_path filter).  Whether the commit
#   adds file content, an empty directory, or a rename, it still
#   produces one bundle and the watermark must advance by exactly one
#   per commit.  This test guards that property.
#
# TEST SHAPE:
#   * Pond A:  init, one file commit, push.
#   * Pond B:  init, cross-pond-import A at /imports/A.  Pull #1.
#              Capture W1.
#   * Pond A:  TWO mkdir-only commits (no file blobs).  Push.
#   * Pond B:  Pull #2.  applied >= 2.  Watermark advances by >= 2.
#              `/imports/A/<new-dirs>` visible.
#   * Pond B:  Pull #3 (no-op).  applied == 0.  Watermark unchanged.
#              (Regression guard from 540 -- but after a mkdir-only
#              sequence, which exercises a different bundle shape.)
#   * Pond A:  one more file commit.  Push.
#   * Pond B:  Pull #4.  applied >= 1.  Watermark advances.  File
#              visible.
#
# EXPECTED:
#   - Mkdir-only foreign commits still produce bundles.
#   - LAST_PULLED_SEQ advances by exactly the producer-side commit
#     count, regardless of whether file content was involved.
#   - A no-op pull after mkdir-only history still reports
#     `applied 0 bundle(s)` (the 540 regression doesn't reappear).
#
# History:
#   Revived D5.8.7 from a DISABLED-D4 stub.  Original tested a now-
#   removed filter-induced bug in the `remote` factory; D5.7b cross-pond
#   import has no filter, so the test was re-cast as a generic
#   bundle-shape-independence guard around the same persisted-watermark
#   surface (`last_pulled_seq:<url>` + `pond remote list`).
set -e

echo "=== Experiment: Watermark advances on ANY foreign txn ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-541"

CHECKS_TOTAL=0
CHECKS_FAILED=0

check() {
    local description="$1"
    local cmd="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${cmd}" >/dev/null 2>&1; then
        echo "  ✓ ${description}"
    else
        echo "  ✗ ${description} (cmd: ${cmd})"
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
        echo "  ✗ ${description}: got '${actual}', expected '${expected}'"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

remote_last_pulled_seq() {
    local name="$1"
    pond remote list 2>/dev/null | awk -v n="${name}" '$1 == n {print $NF; exit}'
}

applied_bundles() {
    local logfile="$1"
    grep -oE "applied [0-9]+ bundle" "${logfile}" | awk '{print $2}' | head -1
}

#############################
# MINIO READINESS
#############################

echo "=== Checking MinIO availability ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    exit 1
fi

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null 2>&1

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A — init, populate one file, push ==="
export POND=/tmp/pond-a-541
rm -rf "${POND}"
pond init >/dev/null
pond mkdir -p /data/sensors >/dev/null

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
EOF
pond copy host:///tmp/temps.csv /data/sensors/temps.csv >/dev/null

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

#############################
# POND B — CONSUMER
#############################

echo ""
echo "=== Phase 2: Pond B — init, attach A as /imports/A, pull #1 ==="
export POND=/tmp/pond-b-541
rm -rf "${POND}"
pond init >/dev/null

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

pond pull upstream 2>&1 | tee /tmp/pull1.log
WM1=$(remote_last_pulled_seq upstream)
APPLIED1=$(applied_bundles /tmp/pull1.log)
echo "Pull #1: applied=${APPLIED1}  LAST_PULLED_SEQ=${WM1}"

check "pull #1: LAST_PULLED_SEQ is a positive integer" \
    "[[ \"${WM1}\" =~ ^[0-9]+$ ]] && [ \"${WM1}\" -gt 0 ]"
check "pull #1: applied a positive number of bundles" \
    "[[ \"${APPLIED1}\" =~ ^[0-9]+$ ]] && [ \"${APPLIED1}\" -gt 0 ]"

#############################
# PHASE 3 — Producer mkdir-only commits
#############################

echo ""
echo "=== Phase 3: Producer — TWO mkdir-only commits, push ==="
export POND=/tmp/pond-a-541
pond mkdir /private >/dev/null
pond mkdir /private/inner >/dev/null
pond push origin >/dev/null

#############################
# PULL #2 — bundles must exist even with no new file blobs
#############################

echo ""
echo "=== Phase 4: Pull #2 (foreign added mkdir-only txns) ==="
export POND=/tmp/pond-b-541
pond pull upstream 2>&1 | tee /tmp/pull2.log
WM2=$(remote_last_pulled_seq upstream)
APPLIED2=$(applied_bundles /tmp/pull2.log)
echo "Pull #2: applied=${APPLIED2}  LAST_PULLED_SEQ=${WM2}"

check "pull #2: at least 2 bundles applied (one per mkdir commit)" \
    "[[ \"${APPLIED2}\" =~ ^[0-9]+$ ]] && [ \"${APPLIED2}\" -ge 2 ]"
check "pull #2: LAST_PULLED_SEQ advanced from pull #1 by >= 2" \
    "[ \"${WM2}\" -ge \$((${WM1} + 2)) ]"

# Mkdir-only commits really did replicate -- the foreign directories
# are now visible through the mount.
check "pull #2: foreign /private/ visible at /imports/A/private/" \
    "pond list /imports/A/private/ 2>/dev/null | grep -q ."
check "pull #2: foreign /private/inner/ visible at /imports/A/private/inner/" \
    "pond list /imports/A/private/ 2>/dev/null | grep -qw inner"

#############################
# PULL #3 — no-op after mkdir-only history (regression guard from 540)
#############################

echo ""
echo "=== Phase 5: Pull #3 (no upstream changes after mkdir history) ==="
pond pull upstream 2>&1 | tee /tmp/pull3.log
WM3=$(remote_last_pulled_seq upstream)
APPLIED3=$(applied_bundles /tmp/pull3.log)
echo "Pull #3: applied=${APPLIED3}  LAST_PULLED_SEQ=${WM3}"

check_eq "pull #3: applied 0 bundles (no re-walk after mkdir-only history)" \
    "${APPLIED3}" "0"
check_eq "pull #3: LAST_PULLED_SEQ unchanged" "${WM3}" "${WM2}"

#############################
# PHASE 6 — Producer adds a new file
#############################

echo ""
echo "=== Phase 6: Producer adds one more file, push ==="
export POND=/tmp/pond-a-541
cat > /tmp/extra.csv << 'EOF'
timestamp,location,temperature
2024-01-02T00:00:00Z,basement,18.1
EOF
pond copy host:///tmp/extra.csv /data/sensors/extra.csv >/dev/null
pond push origin >/dev/null

echo ""
echo "=== Phase 7: Pull #4 (one new file commit) ==="
export POND=/tmp/pond-b-541
pond pull upstream 2>&1 | tee /tmp/pull4.log
WM4=$(remote_last_pulled_seq upstream)
APPLIED4=$(applied_bundles /tmp/pull4.log)
echo "Pull #4: applied=${APPLIED4}  LAST_PULLED_SEQ=${WM4}"

check "pull #4: at least 1 bundle applied" \
    "[[ \"${APPLIED4}\" =~ ^[0-9]+$ ]] && [ \"${APPLIED4}\" -ge 1 ]"
check "pull #4: LAST_PULLED_SEQ advanced past pull #3" \
    "[ \"${WM4}\" -gt \"${WM3}\" ]"

EXTRA_HASH=$(pond cat /imports/A/data/sensors/extra.csv 2>/dev/null | md5sum | cut -d' ' -f1)
EXTRA_EXP=$(md5sum /tmp/extra.csv | cut -d' ' -f1)
check_eq "pull #4: /imports/A/data/sensors/extra.csv matches producer" \
    "${EXTRA_HASH}" "${EXTRA_EXP}"

# The earlier mkdir-only dirs are still there.
check "pull #4: /imports/A/private/inner/ still present after later file pull" \
    "pond list /imports/A/private/ 2>/dev/null | grep -qw inner"

#############################
# RESULTS
#############################

echo ""
echo "=== Results ==="
echo "Checks: ${CHECKS_TOTAL}, Failed: ${CHECKS_FAILED}"
if [[ "${CHECKS_FAILED}" -ne 0 ]]; then
    echo "[FAIL] one or more checks failed"
    exit 1
fi
echo "[OK] all checks passed"
