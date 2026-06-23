#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond import watermark advances incrementally (D5.4 + D5.7b)
#
# DESCRIPTION:
#   Reproduces a production bandwidth bug observed on the cloud sitegen
#   pond: every tick of `pond pull` re-walked transactions 1..=N of every
#   foreign pond, even though the local store already had every file.
#   Logs showed:
#       Watermark: 0 (will import transactions > 0)
#       679 new transaction(s) to import: "1..=679"
#       [OK] Import complete: 5 partition(s), 0 file(s) downloaded
#   on every tick, sustained -- 25 Mb/s inbound for 2 h with zero useful
#   bytes downloaded.
#
#   The fix moved the watermark from the deleted `remote` factory's
#   import-metadata state into the steward control table as
#   `last_pulled_seq:<url>` (crates/cmd/src/commands/pull.rs, with
#   first-pull bootstrap at line 97-106).  It is now surfaced via
#   `pond remote list`'s `LAST_PULLED_SEQ` column.
#
# TEST SHAPE:
#   * Pond A:  init, populate /data/sensors/temps.csv +
#              /data/logs/events.csv, attach `pond backup add origin`,
#              push.
#   * Pond B:  init, cross-pond-import A at /imports/A.
#   * Pull #1 (initial): bundles_applied > 0; LAST_PULLED_SEQ advances
#              from "-" (or "1") to some value > 1; data is queryable.
#   * Pull #2 (no upstream changes): bundles_applied == 0;
#              LAST_PULLED_SEQ UNCHANGED.  This is the bandwidth-bug
#              regression guard.
#   * Pond A:  push one more file (/data/sensors/extra.csv).
#   * Pull #3 (one new upstream txn): bundles_applied == 1 (or a small
#              delta); LAST_PULLED_SEQ advances by exactly the delta;
#              new file is now visible via the mount.
#
# EXPECTED:
#   - Pull #1 advances LAST_PULLED_SEQ above its bootstrap floor.
#   - Pull #2 reports `applied 0 bundle(s)` and LAST_PULLED_SEQ is
#     unchanged (no full re-walk).
#   - Pull #3 reports `applied N bundle(s)` with N > 0 and small;
#     LAST_PULLED_SEQ advances; the new file is queryable.
#
# History:
#   Revived D5.8.7 from a DISABLED-D4 stub that drove the long-removed
#   `remote` factory with an `import:` YAML.  Watermark verification now
#   uses `pond remote list` and pull's log output, not the factory's
#   `Watermark: N` log lines.
set -e

echo "=== Experiment: Import watermark advances incrementally ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-540"

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

# Extract LAST_PULLED_SEQ for a given remote name from `pond remote list`.
# Returns "-" if not yet pulled, or the integer string otherwise.
remote_last_pulled_seq() {
    local name="$1"
    pond remote list 2>/dev/null | awk -v n="${name}" '
        $1 == n {print $NF; exit}
    '
}

# Count "applied N bundle(s)" in a pull log, returning N (or empty if absent).
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
echo "MinIO is reachable at ${MINIO_ENDPOINT}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null 2>&1

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A — init, populate, push ==="
export POND=/tmp/pond-a-540
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null

pond mkdir -p /data/sensors >/dev/null
pond mkdir -p /data/logs >/dev/null

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
2024-01-01T01:00:00Z,roof,4.8
EOF
cat > /tmp/events.csv << 'EOF'
timestamp,level,message
2024-01-01T00:00:00Z,INFO,system started
EOF
pond copy host:///tmp/temps.csv  /data/sensors/temps.csv >/dev/null
pond copy host:///tmp/events.csv /data/logs/events.csv >/dev/null

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
echo "=== Phase 2: Pond B — init, attach A as /imports/A ==="
export POND=/tmp/pond-b-540
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

WM_BEFORE=$(remote_last_pulled_seq upstream)
echo "LAST_PULLED_SEQ before first pull: ${WM_BEFORE}"

#############################
# PULL #1 — initial import
#############################

echo ""
echo "=== Phase 3: Pull #1 (initial import) ==="
pond pull upstream 2>&1 | tee /tmp/pull1.log
WM_AFTER1=$(remote_last_pulled_seq upstream)
APPLIED1=$(applied_bundles /tmp/pull1.log)
echo "LAST_PULLED_SEQ after pull #1: ${WM_AFTER1}"
echo "Bundles applied in pull #1:    ${APPLIED1}"

check "LAST_PULLED_SEQ after pull #1 is a positive integer" \
    "[[ \"${WM_AFTER1}\" =~ ^[0-9]+$ ]] && [ \"${WM_AFTER1}\" -gt 0 ]"
check "pull #1 reports a positive number of applied bundles" \
    "[[ \"${APPLIED1}\" =~ ^[0-9]+$ ]] && [ \"${APPLIED1}\" -gt 0 ]"

# Sanity: foreign data is queryable via the mount.
TEMPS_HASH=$(pond cat /imports/A/data/sensors/temps.csv 2>/dev/null | md5sum | cut -d' ' -f1)
TEMPS_EXP=$(md5sum /tmp/temps.csv | cut -d' ' -f1)
check_eq "pull #1: /imports/A/data/sensors/temps.csv readable and matches A" \
    "${TEMPS_HASH}" "${TEMPS_EXP}"

EVENTS_HASH=$(pond cat /imports/A/data/logs/events.csv 2>/dev/null | md5sum | cut -d' ' -f1)
EVENTS_EXP=$(md5sum /tmp/events.csv | cut -d' ' -f1)
check_eq "pull #1: /imports/A/data/logs/events.csv readable and matches A" \
    "${EVENTS_HASH}" "${EVENTS_EXP}"

#############################
# PULL #2 — no upstream changes (the bug)
#############################

echo ""
echo "=== Phase 4: Pull #2 (no upstream changes — watermark must persist) ==="
pond pull upstream 2>&1 | tee /tmp/pull2.log
WM_AFTER2=$(remote_last_pulled_seq upstream)
APPLIED2=$(applied_bundles /tmp/pull2.log)
echo "LAST_PULLED_SEQ after pull #2: ${WM_AFTER2}"
echo "Bundles applied in pull #2:    ${APPLIED2}"

check_eq "pull #2 reports 0 bundles applied (no re-walk)" \
    "${APPLIED2}" "0"
check_eq "pull #2 LAST_PULLED_SEQ unchanged from pull #1 (no watermark reset)" \
    "${WM_AFTER2}" "${WM_AFTER1}"

#############################
# PULL #3 — one new upstream commit
#############################

echo ""
echo "=== Phase 5: Producer adds one new file, pushes ==="
export POND=/tmp/pond-a-540
cat > /tmp/extra.csv << 'EOF'
timestamp,location,temperature
2024-01-02T00:00:00Z,roof,3.1
EOF
pond copy host:///tmp/extra.csv /data/sensors/extra.csv >/dev/null
# Explicit push (also exercises that backup add's post-commit auto-push
# is what landed the previous data without a second `pond push`).
pond push origin >/dev/null

echo ""
echo "=== Phase 6: Pull #3 (one new upstream txn — incremental delta) ==="
export POND=/tmp/pond-b-540
pond pull upstream 2>&1 | tee /tmp/pull3.log
WM_AFTER3=$(remote_last_pulled_seq upstream)
APPLIED3=$(applied_bundles /tmp/pull3.log)
echo "LAST_PULLED_SEQ after pull #3: ${WM_AFTER3}"
echo "Bundles applied in pull #3:    ${APPLIED3}"

check "pull #3 reports a positive but small number of applied bundles" \
    "[[ \"${APPLIED3}\" =~ ^[0-9]+$ ]] && [ \"${APPLIED3}\" -gt 0 ] && [ \"${APPLIED3}\" -le 2 ]"
check "pull #3 advances LAST_PULLED_SEQ above pull #2's value" \
    "[ \"${WM_AFTER3}\" -gt \"${WM_AFTER2}\" ]"

# The new file is now queryable through the mount.
EXTRA_HASH=$(pond cat /imports/A/data/sensors/extra.csv 2>/dev/null | md5sum | cut -d' ' -f1)
EXTRA_EXP=$(md5sum /tmp/extra.csv | cut -d' ' -f1)
check_eq "pull #3: /imports/A/data/sensors/extra.csv readable and matches A" \
    "${EXTRA_HASH}" "${EXTRA_EXP}"

# And the original files are still there with intact content.
TEMPS_HASH_AFTER=$(pond cat /imports/A/data/sensors/temps.csv 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "pull #3: /imports/A/data/sensors/temps.csv content unchanged" \
    "${TEMPS_HASH_AFTER}" "${TEMPS_EXP}"

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
