#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Synth-Logs -> Logfile-Ingest -> S3 Cross-Pond Replication Cycle
#
# DESCRIPTION:
#   End-to-end test of the data pipeline using the D5.7b cross-pond import
#   model (mirror-restart bootstrap CLI is still deferred per d5.8-resume.md
#   section 6; cross-pond import covers this scenario today):
#     1. Generate verifiable CSV content (row_number,row_squared)
#     2. Pond1: logfile-ingest pulls the CSV into /data/metrics
#     3. Pond1: `pond backup add origin` (push-mode) + `pond push origin`
#     4. Pond2: fresh `pond init`, then `pond remote add origin URL /imports/origin`
#        (cross-pond pull-mode mount); `pond pull origin` materializes the mount
#     5. Verify row counts and md5 hash of Pond1 /data/metrics/metrics.log
#        against Pond2 /imports/origin/data/metrics/metrics.log
#     6. Append more rows on the host, re-ingest, push, pull, verify (rounds 2-3)
#
# VERIFIABLE DATA:
#   Each line is `row_number,row_number_squared` so any drift surfaces both
#   as a row-count mismatch and as a hash mismatch.
#
# EXPECTED:
#   - Row counts match at each stage on both ponds
#   - md5 hashes of the replicated stream match exactly
#   - Incremental appends propagate correctly via the cross-pond mount
#
# History:
#   Migrated D5.8: rewritten on top of the D5.7b CLI verbs (`pond backup add`,
#   `pond remote add NAME URL /imports/<name>`, `pond push`, `pond pull`) after
#   the legacy chunked-parquet `remote` factory + `replicate` config flow was
#   removed.  The two-pond mirror-restart half of the old test (Pond2 takes on
#   Pond1's pond_id) is deferred until the mirror-restart CLI surface lands;
#   cross-pond import gives equivalent verification coverage today.
set -e

echo "=== Experiment: Synth-Logs Replication Cycle (cross-pond) ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="duckpond-replication-test"

# Test data parameters.  6000 rows of ~20 bytes each ~= 120KB, which
# crosses the 64KB large-file threshold so the replication cycle
# exercises the externalized `_large_files/blake3=…parquet` path on
# both the push and the pull sides (regression coverage for
# P1-BUG-LF-REPLICATION, fixed in D5.9).  APPEND_ROWS keeps subsequent
# rounds modest so we exercise multi-version appends without rerunning
# the heavy generator each phase.
INITIAL_ROWS=6000
APPEND_ROWS=250

SYNTH_LOGS_DIR="/var/log/synthapp"

# Track per-check pass/fail so run-test.sh's tail summary picks them up.
CHECKS_TOTAL=0
CHECKS_FAILED=0

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

#############################
# CHECK MINIO AVAILABILITY
#############################

echo "=== Checking MinIO availability ==="

if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    echo "       This test must run via 'run-test.sh --compose 510'."
    exit 1
fi
echo "[OK] MinIO reachable at ${MINIO_ENDPOINT}"

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null
mc rb --force "local/${BUCKET_NAME}" 2>/dev/null || true   # ensure clean start
mc mb --ignore-existing "local/${BUCKET_NAME}"
echo "[OK] Bucket ${BUCKET_NAME} ready"

#############################
# HELPERS (need MinIO + pond)
#############################

# Generate verifiable CSV rows.  Format: row_number,row_number_squared.
generate_csv_rows() {
    local output_file="$1"
    local start_row="$2"
    local num_rows="$3"
    local end_row=$((start_row + num_rows - 1))
    for row in $(seq "$start_row" "$end_row"); do
        echo "${row},$((row * row))"
    done >> "$output_file"
}

#############################
# PHASE 1: GENERATE INITIAL DATA
#############################

echo ""
echo "=== Phase 1: Generate Initial Data (${INITIAL_ROWS} rows) ==="

mkdir -p "${SYNTH_LOGS_DIR}"
generate_csv_rows "${SYNTH_LOGS_DIR}/metrics.log" 1 "${INITIAL_ROWS}"

ls -la "${SYNTH_LOGS_DIR}/"
HOST_LINES=$(wc -l < "${SYNTH_LOGS_DIR}/metrics.log" | tr -d ' ')
echo "Total lines on host: ${HOST_LINES}"

#############################
# PHASE 2: SETUP POND1 (PRIMARY)
#############################

echo ""
echo "=== Phase 2: Setup Pond1 (Primary) ==="

export POND=/pond1
pond init
pond mkdir -p /data/metrics
pond mkdir -p /system/run

cat > /tmp/metrics-ingest.yaml << EOF
archived_pattern: ${SYNTH_LOGS_DIR}/metrics.log.*
active_pattern: ${SYNTH_LOGS_DIR}/metrics.log
pond_path: /data/metrics
EOF

pond mknod logfile-ingest /system/run/20-metrics-ingest --config-path /tmp/metrics-ingest.yaml
echo "[OK] Pond1 initialized; logfile-ingest factory created"

#############################
# PHASE 3: ATTACH BACKUP + INGEST ROUND 1
#############################

echo ""
echo "=== Phase 3: Attach Backup + Ingest (Round 1) ==="

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http

echo "[OK] origin backup attached"

# Run ingest; this commits a write tx, which triggers post-commit auto-push.
pond run /system/run/20-metrics-ingest

# Explicit push as well -- belt-and-suspenders against any race where the
# auto-push view of the world differed from what we just committed.
pond push origin
echo "[OK] Pond1 ingest + push complete"

POND1_ROWS_R1=$(pond cat /data/metrics/metrics.log | wc -l | tr -d ' ')
echo "Pond1 row count: ${POND1_ROWS_R1}"

#############################
# PHASE 4: SETUP POND2 (CROSS-POND REPLICA)
#############################

echo ""
echo "=== Phase 4: Setup Pond2 (Cross-Pond Replica) ==="

export POND=/pond2
pond init

# Cross-pond import: store_id from the remote MUST differ from Pond2's
# fresh pond_id; first pull materializes the /imports/origin mount entry.
pond remote add origin "s3://${BUCKET_NAME}" /imports/origin \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key "${MINIO_ROOT_PASSWORD}" \
    --allow-http

echo "[OK] Pond2 attached origin as cross-pond import"

pond pull origin
echo "[OK] Pond2 pull complete"

#############################
# PHASE 5: VERIFY ROUND 1
#############################

echo ""
echo "=== Phase 5: Verify Round 1 ==="

POND2_ROWS_R1=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | wc -l | tr -d ' ')
echo "Pond1 rows: ${POND1_ROWS_R1}, Pond2 (via /imports/origin) rows: ${POND2_ROWS_R1}"

check_eq "Round 1 host vs Pond1 row count"    "${POND1_ROWS_R1}" "${HOST_LINES}"
check_eq "Round 1 Pond1 vs Pond2 row count"   "${POND2_ROWS_R1}" "${POND1_ROWS_R1}"

POND1_HASH_R1=$(POND=/pond1 pond cat /data/metrics/metrics.log | md5sum | cut -d' ' -f1)
POND2_HASH_R1=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | md5sum | cut -d' ' -f1)
check_eq "Round 1 content hash" "${POND2_HASH_R1}" "${POND1_HASH_R1}"

#############################
# PHASE 6: APPEND ROUND 2
#############################

echo ""
echo "=== Phase 6: Append Round 2 (${APPEND_ROWS} rows) ==="

NEXT_START_R2=$((INITIAL_ROWS + 1))
generate_csv_rows "${SYNTH_LOGS_DIR}/metrics.log" "${NEXT_START_R2}" "${APPEND_ROWS}"
EXPECTED_R2=$((INITIAL_ROWS + APPEND_ROWS))

export POND=/pond1
pond run /system/run/20-metrics-ingest
pond push origin

export POND=/pond2
pond pull origin

POND1_ROWS_R2=$(POND=/pond1 pond cat /data/metrics/metrics.log | wc -l | tr -d ' ')
POND2_ROWS_R2=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | wc -l | tr -d ' ')

check_eq "Round 2 Pond1 row count"           "${POND1_ROWS_R2}" "${EXPECTED_R2}"
check_eq "Round 2 Pond1 vs Pond2 row count"  "${POND2_ROWS_R2}" "${POND1_ROWS_R2}"

POND1_HASH_R2=$(POND=/pond1 pond cat /data/metrics/metrics.log | md5sum | cut -d' ' -f1)
POND2_HASH_R2=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | md5sum | cut -d' ' -f1)
check_eq "Round 2 content hash" "${POND2_HASH_R2}" "${POND1_HASH_R2}"

#############################
# PHASE 7: APPEND ROUND 3
#############################

echo ""
echo "=== Phase 7: Append Round 3 (${APPEND_ROWS} rows) ==="

NEXT_START_R3=$((INITIAL_ROWS + APPEND_ROWS + 1))
generate_csv_rows "${SYNTH_LOGS_DIR}/metrics.log" "${NEXT_START_R3}" "${APPEND_ROWS}"
EXPECTED_R3=$((INITIAL_ROWS + APPEND_ROWS + APPEND_ROWS))

export POND=/pond1
pond run /system/run/20-metrics-ingest
pond push origin

export POND=/pond2
pond pull origin

POND1_ROWS_R3=$(POND=/pond1 pond cat /data/metrics/metrics.log | wc -l | tr -d ' ')
POND2_ROWS_R3=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | wc -l | tr -d ' ')

check_eq "Round 3 Pond1 row count"           "${POND1_ROWS_R3}" "${EXPECTED_R3}"
check_eq "Round 3 Pond1 vs Pond2 row count"  "${POND2_ROWS_R3}" "${POND1_ROWS_R3}"

POND1_HASH_R3=$(POND=/pond1 pond cat /data/metrics/metrics.log | md5sum | cut -d' ' -f1)
POND2_HASH_R3=$(POND=/pond2 pond cat /imports/origin/data/metrics/metrics.log | md5sum | cut -d' ' -f1)
check_eq "Round 3 content hash" "${POND2_HASH_R3}" "${POND1_HASH_R3}"

#############################
# PHASE 8: WRITE-PROTECTION INVARIANT (D5.7b.3)
#############################

echo ""
echo "=== Phase 8: Foreign mount is read-only ==="

# Per d5.8-resume.md section 3 invariant 1, writes inside /imports/<name>
# must be refused with ReadOnlyImport.  Use a known-bad write to confirm.
export POND=/pond2
if pond mkdir /imports/origin/should-not-exist 2>/tmp/mkdir-err; then
    echo "  ✗ Expected mkdir under /imports/origin to fail (ReadOnlyImport)"
    CHECKS_FAILED=$((CHECKS_FAILED + 1))
else
    if grep -qi 'ReadOnlyImport\|read-only\|read only' /tmp/mkdir-err; then
        echo "  ✓ Write under /imports/origin refused (ReadOnlyImport)"
    else
        echo "  ✗ Write refused but for wrong reason:"
        cat /tmp/mkdir-err | sed 's/^/      /'
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
fi
CHECKS_TOTAL=$((CHECKS_TOTAL + 1))

#############################
# SUMMARY
#############################

echo ""
echo "=== Results: ${CHECKS_FAILED} failed / ${CHECKS_TOTAL} total ==="
echo ""
echo "Pipeline tested:"
echo "  1. synth-logs        -> verifiable CSV (row,row^2) on host"
echo "  2. logfile-ingest    -> /data/metrics on Pond1"
echo "  3. pond backup add   -> origin attached on Pond1 (push mode)"
echo "  4. pond push origin  -> Delta bundle into MinIO bucket"
echo "  5. pond remote add   -> origin attached on Pond2 at /imports/origin"
echo "  6. pond pull origin  -> Pond2 materializes the mount + pulls bundles"
echo "  7. Cross-pond verify -> row count + md5 match across 3 incremental rounds"
echo ""
echo "Round 1: ${POND1_ROWS_R1} rows (hash ${POND1_HASH_R1:0:12}...)"
echo "Round 2: ${POND1_ROWS_R2} rows (hash ${POND1_HASH_R2:0:12}...)"
echo "Round 3: ${POND1_ROWS_R3} rows (hash ${POND1_HASH_R3:0:12}...)"

if [[ "${CHECKS_FAILED}" -ne 0 ]]; then
    exit 1
fi
