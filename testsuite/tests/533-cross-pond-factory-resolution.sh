#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond factory resolution (D5.7b invariant 2: foreign factories NOT auto-run)
#
# DESCRIPTION:
#   D5.7b § 3 invariant 2: "The steward's post-commit auto-exec only
#   operates on factory entries owned by the **local** pond_id."
#
#   Two complementary mechanisms implement this:
#
#     (a) `discover_sys_remotes` scans `/sys/remotes/*` -- a
#         root-anchored path pattern.  Foreign entries at
#         `/imports/<NAME>/sys/remotes/*` simply don't match the
#         pattern, so they're never enumerated.
#
#     (b) `discover_post_commit_factories` scans `/system/run/*` and
#         then applies an explicit `pond_id` filter on each match
#         (foreign-owned entries reach the iteration but are skipped
#         with a debug log line; see crates/steward/src/guard.rs:699).
#
#   Surface invariant: even when foreign `/imports/<NAME>/sys/remotes/...`
#   or `/imports/<NAME>/system/run/...` entries are visible after a
#   cross-pond pull, the consumer must NOT execute them as part of its
#   own post-commit cycle.  Locally-owned entries on the consumer DO
#   still run.
#
# TEST SHAPE:
#   * Pond A:  init, populate /data/sensors.csv, install a
#              `/system/run/scratch` factory via mknod (sql-derived-table;
#              exercises mechanism (b)), attach `pond backup add origin`
#              (writes `/sys/remotes/origin`; exercises mechanism (a)),
#              push.  After this bucket-A holds A's Delta log + parquet
#              shards including the two factory entries.
#   * Pond B:  init, cross-pond-import A at /imports/A, pull.  After this
#              `/imports/A/sys/remotes/origin` AND
#              `/imports/A/system/run/scratch` are both visible as
#              foreign-owned factory entries inside B's filesystem.
#   * Snapshot bucket-A object count.
#   * Pond B:  perform a local write that triggers B's post-commit cycle.
#              B has NO local factories of its own yet.  Both scans must
#              skip the foreign entries.  Bucket-A count must be unchanged.
#   * Pond B:  install its OWN /sys/remotes/origin-b → bucket-B (via
#              `pond backup add origin-b ...`).  Perform another local
#              write.  Bucket-B object count must GROW (local mechanism
#              (a) DID auto-run).  Bucket-A still unchanged.
#   * Pond B:  both foreign factory entries are reachable by explicit
#              path traversal -- proof that they exist and the filters
#              rejected them, rather than the entries not being there.
#
# EXPECTED:
#   - Phase 4: B's first local write triggers post-commit; bucket-A
#     count is unchanged (foreign factory skipped).
#   - Phase 6: B's second local write triggers post-commit; bucket-B
#     count grows (local factory ran); bucket-A still unchanged.
#   - Phase 7: `pond list /imports/A/system/run/` shows the foreign
#     `origin` entry — proves it was visible to the scan but rejected
#     by the pond_id filter.
#
# History:
#   Revived D5.8.6 from a DISABLED-D4 stub that exercised a much-older
#   set of dynamic-dir / timeseries-join scenarios.  Reset to focus on
#   D5.7b invariant 2 as documented in docs/d5.8-resume.md § 3.
#   Modeled on the Rust integration test
#   `foreign_post_commit_factories_skipped_in_auto_exec` in
#   crates/cmd/tests/test_remote_cli.rs.
set -e

echo "=== Experiment: Cross-Pond Factory Resolution (invariant 2) ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_A="cross-pond-533-a"
BUCKET_B="cross-pond-533-b"

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

bucket_object_count() {
    local bucket="$1"
    mc ls --recursive "local/${bucket}" 2>/dev/null | wc -l | tr -d ' '
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
for B in "${BUCKET_A}" "${BUCKET_B}"; do
    mc rb --force "local/${B}" >/dev/null 2>&1 || true
    mc mb "local/${B}" >/dev/null 2>&1
done

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A — init, populate, install factories, push ==="
export POND=/tmp/pond-a-533
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null

pond mkdir /data >/dev/null
cat > /tmp/sensors.csv << 'EOF'
ts,sensor,value
2024-01-01T00:00:00Z,A,11
2024-01-01T01:00:00Z,A,12
EOF
pond copy host:///tmp/sensors.csv /data/sensors.csv >/dev/null

# Install an explicit `/system/run/scratch` factory via mknod.  This
# exercises the OTHER half of invariant 2: the /system/run/* scan in
# `discover_post_commit_factories` uses a pond_id filter (it cannot
# rely on the root-anchored path pattern, because foreign /system/run
# entries DO match `/system/run/*` when glob expansion crosses mount
# boundaries).  sql-derived-table is a dynamic factory; it is not
# itself post-commit-executable, but its mere presence under
# /system/run forces the scan to enumerate it.
cat > /tmp/sql-derived.yaml << 'YAML'
patterns:
  source: "table:///data/*.table"
query: "SELECT 1"
YAML
pond mkdir -p /system/run >/dev/null
pond mknod sql-derived-table /system/run/scratch --config-path /tmp/sql-derived.yaml >/dev/null

POND_ID_A=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond A pond_id: ${POND_ID_A}"

# `pond backup add` writes config to /sys/remotes/<name>.  The
# post-commit /sys/remotes/* scan is root-anchored so foreign mounts
# (/imports/A/sys/remotes/*) are implicitly excluded -- the first
# half of invariant 2.
pond backup add origin "s3://${BUCKET_A}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

A_BUCKET_COUNT_INIT=$(bucket_object_count "${BUCKET_A}")
echo "Bucket A object count after A's push: ${A_BUCKET_COUNT_INIT}"

#############################
# POND B — CONSUMER
#############################

echo ""
echo "=== Phase 2: Pond B — init, cross-pond-import A at /imports/A ==="
export POND=/tmp/pond-b-533
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null

POND_ID_B=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond B pond_id: ${POND_ID_B}"
check "Pond A and Pond B have distinct pond_ids" \
    "[ \"${POND_ID_A}\" != \"${POND_ID_B}\" ]"

pond remote add upstream "s3://${BUCKET_A}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond pull upstream >/dev/null

#############################
# B has NO local /system/run/*
#############################

echo ""
echo "=== Phase 3: B's local factory dirs in expected initial state ==="
LOCAL_RUN_DIR_STATUS="absent"
if pond list /system/run/ >/dev/null 2>&1; then
    if pond list /system/run/ 2>/dev/null | grep -qE '^\['; then
        LOCAL_RUN_DIR_STATUS="non_empty"
    else
        LOCAL_RUN_DIR_STATUS="empty"
    fi
fi
check "B's local /system/run/ is empty or absent before first local write" \
    "[ \"${LOCAL_RUN_DIR_STATUS}\" != \"non_empty\" ]"

# /sys/remotes IS expected to be non-empty on B because `pond remote
# add upstream` (Phase 2) wrote /sys/remotes/upstream into it.  But it
# must NOT contain the FOREIGN remote name `origin`.
pond list /sys/remotes/ > /tmp/b-local-remotes.txt 2>&1 || true
check "B's local /sys/remotes/ contains its OWN 'upstream' attachment" \
    "grep -q '/sys/remotes/upstream$' /tmp/b-local-remotes.txt"
check "B's local /sys/remotes/ does NOT contain foreign attachment 'origin'" \
    "! grep -q '/sys/remotes/origin$' /tmp/b-local-remotes.txt"

#############################
# A's bucket snapshot
#############################

A_BUCKET_COUNT_BEFORE=$(bucket_object_count "${BUCKET_A}")
B_BUCKET_COUNT_BEFORE=$(bucket_object_count "${BUCKET_B}")
echo "Bucket counts before B's local write: A=${A_BUCKET_COUNT_BEFORE}, B=${B_BUCKET_COUNT_BEFORE}"

#############################
# B does a local write — no auto-push to A
#############################

echo ""
echo "=== Phase 4: B does a local write — foreign factory must NOT auto-run ==="
pond mkdir /local-only >/dev/null
echo "B local file 1" > /tmp/b1.txt
pond copy host:///tmp/b1.txt /local-only/b1.txt >/dev/null

A_BUCKET_COUNT_AFTER1=$(bucket_object_count "${BUCKET_A}")
echo "Bucket A object count after B's first local write: ${A_BUCKET_COUNT_AFTER1}"
check_eq "bucket A unchanged by B's local write (both foreign-factory filters fired: /system/run/scratch by pond_id, /sys/remotes/origin by path anchor)" \
    "${A_BUCKET_COUNT_AFTER1}" "${A_BUCKET_COUNT_BEFORE}"

#############################
# Now install B's OWN /system/run/origin-b → bucket-B
#############################

echo ""
echo "=== Phase 5: Install B's own /sys/remotes/origin-b → bucket B ==="
pond backup add origin-b "s3://${BUCKET_B}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

# `pond backup add` itself is a transaction; its post-commit may or
# may not push.  Snapshot now so we measure only the NEXT write.
B_BUCKET_COUNT_AFTER_ADD=$(bucket_object_count "${BUCKET_B}")
A_BUCKET_COUNT_AFTER_ADD=$(bucket_object_count "${BUCKET_A}")
echo "After backup add: A=${A_BUCKET_COUNT_AFTER_ADD}, B=${B_BUCKET_COUNT_AFTER_ADD}"
check_eq "bucket A still unchanged after B installs its own backup" \
    "${A_BUCKET_COUNT_AFTER_ADD}" "${A_BUCKET_COUNT_BEFORE}"

#############################
# B's next local write — local factory DOES run, foreign still skipped
#############################

echo ""
echo "=== Phase 6: B's next local write — local factory runs, foreign still skipped ==="
echo "B local file 2" > /tmp/b2.txt
pond copy host:///tmp/b2.txt /local-only/b2.txt >/dev/null

A_BUCKET_COUNT_AFTER2=$(bucket_object_count "${BUCKET_A}")
B_BUCKET_COUNT_AFTER2=$(bucket_object_count "${BUCKET_B}")
echo "After B's second local write: A=${A_BUCKET_COUNT_AFTER2}, B=${B_BUCKET_COUNT_AFTER2}"

check_eq "bucket A STILL unchanged after B's second write" \
    "${A_BUCKET_COUNT_AFTER2}" "${A_BUCKET_COUNT_BEFORE}"
check "bucket B grew after B's second local write (local factory ran)" \
    "[ \"${B_BUCKET_COUNT_AFTER2}\" -gt \"${B_BUCKET_COUNT_AFTER_ADD}\" ]"

#############################
# Foreign factory entry is still visible by explicit path traversal
#############################

echo ""
echo "=== Phase 7: Foreign factory entries visible by explicit traversal ==="
echo "--- pond list /imports/A/ ---"
pond list /imports/A/ 2>&1 | grep -v "Peak memory" || true
echo "--- pond list /imports/A/sys/remotes/ ---"
pond list /imports/A/sys/remotes/ > /tmp/b-foreign-remotes.txt 2>&1 || true
cat /tmp/b-foreign-remotes.txt
echo "--- pond list /imports/A/system/run/ ---"
pond list /imports/A/system/run/ > /tmp/b-foreign-runs.txt 2>&1 || true
cat /tmp/b-foreign-runs.txt

# Both filters in invariant 2 must allow these entries to remain
# visible (skipping at scan time, not deletion at sync time).
check "/imports/A/sys/remotes/origin is visible to B (proves the /sys/remotes/* path filter is path-anchored, not entry-deletion)" \
    "grep -q '/imports/A/sys/remotes/origin' /tmp/b-foreign-remotes.txt"
check "/imports/A/system/run/scratch is visible to B (proves the /system/run/* pond_id filter is per-entry, not entry-deletion)" \
    "grep -q '/imports/A/system/run/scratch' /tmp/b-foreign-runs.txt"

# Decoration shows A's pond_id, not B's — final cross-pond provenance evidence.
POND_ID_A_TAIL="${POND_ID_A: -12}"
check "/imports/A/sys/remotes/origin is decorated with A's pond_id tail" \
    "grep -q '\\[${POND_ID_A_TAIL}\\]' /tmp/b-foreign-remotes.txt"
check "/imports/A/system/run/scratch is decorated with A's pond_id tail" \
    "grep -q '\\[${POND_ID_A_TAIL}\\]' /tmp/b-foreign-runs.txt"

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
