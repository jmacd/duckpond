#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Consumer watermark is replica-specific and reconstructs
#             cleanly on a fresh pond (D5.4 + D5.7b)
#
# DESCRIPTION:
#   In the legacy `remote` factory (pre-D4), the import watermark lived
#   in the factory's own metadata partition.  When the consumer pond
#   was rebuilt from a backup (the production "terraform destroy +
#   recreate" scenario), the new pond's control table was created
#   fresh and didn't inherit the old factory state.  The watermark
#   therefore had to be "reconstructed" by walking the foreign txn
#   range once.  The regression: if reconstruction silently failed,
#   the rebuilt consumer would re-walk forever on every pull, burning
#   bandwidth.
#
#   D5 deleted the `--from-backup` / `pond init --config` flag (see
#   crates/cmd/src/commands/init.rs line 15).  The equivalent recovery
#   path is now: `pond init` + `pond remote add` + `pond pull`, which
#   makes the regression's blast radius narrower but still real -- a
#   `pond` reinstall, a bind-mount swap on a server, or a deliberate
#   rebuild all reset the consumer's control table to empty.
#
#   This test exercises the explicit "blow away and rebuild" path:
#     1. Consumer-A imports producer, captures W_initial.
#     2. Wipe consumer-A entirely (rm -rf), making no backup of it.
#     3. Consumer-A' (fresh pond, same URL, same mount) attaches the
#        same producer bucket.  Pre-pull, `last_pulled_seq` is unset.
#     4. Pull #1 on the fresh consumer reaches W_initial (same value
#        as the original) -- the foreign bundles are still in the
#        bucket and re-applied in order.
#     5. Pull #2 on the fresh consumer is a no-op (applied=0, watermark
#        unchanged): the rebuilt control table really did persist the
#        watermark.  This is the regression guard against the legacy
#        "reconstruction silently fails" bug.
#
# EXPECTED:
#   - Pre-pull on consumer-A':  LAST_PULLED_SEQ == "-".
#   - Pull #1 on consumer-A':   applied > 0; LAST_PULLED_SEQ ==
#                               W_initial (same as original consumer).
#   - Pull #2 on consumer-A':   applied == 0; LAST_PULLED_SEQ
#                               unchanged (no re-walk).
#
# History:
#   Revived D5.8.7 from a DISABLED-D4 stub that tested the now-removed
#   `pond init --from-backup` flow.  Recast as a fresh-replica bootstrap
#   guard.  Mirror-restart restore (foreign==local pond_id) is covered
#   by the existing Rust integration tests in test_remote_cli.rs; this
#   shell test focuses on cross-pond import-side bootstrap.
set -e

echo "=== Experiment: Watermark survives consumer rebuild ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-542"

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

remote_last_pulled_tip() {
    local name="$1"
    pond remote list 2>/dev/null | awk -v n="${name}" '$1 == n {print $NF; exit}'
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
# POND A — PRODUCER (commits a multi-txn history)
#############################

echo ""
echo "=== Phase 1: Pond A — multi-txn history, push ==="
export POND=/tmp/pond-a-542
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null
pond mkdir -p /data/sensors >/dev/null
pond mkdir -p /data/logs >/dev/null

cat > /tmp/temps.csv << 'EOF'
timestamp,location,temperature
2024-01-01T00:00:00Z,roof,5.2
EOF
cat > /tmp/events.csv << 'EOF'
timestamp,level,message
2024-01-01T00:00:00Z,INFO,started
EOF
cat > /tmp/extra.csv << 'EOF'
timestamp,location,temperature
2024-01-02T00:00:00Z,roof,3.1
EOF
pond copy host:///tmp/temps.csv  /data/sensors/temps.csv  >/dev/null
pond copy host:///tmp/events.csv /data/logs/events.csv    >/dev/null
pond copy host:///tmp/extra.csv  /data/sensors/extra.csv  >/dev/null

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

#############################
# POND B (ORIGINAL) — first incarnation of consumer
#############################

echo ""
echo "=== Phase 2: Pond B (original) — attach + first pull ==="
export POND=/tmp/pond-b-542
rm -rf "${POND}"
pond init --birthplace test-host >/dev/null

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

pond pull upstream 2>&1 | tee /tmp/p1pull1.log
W_INITIAL=$(remote_last_pulled_tip upstream)
echo "Original consumer PULLED_TIP: ${W_INITIAL}"

check "original consumer: pull #1 PULLED_TIP is a non-empty short hash" \
    "[ -n \"${W_INITIAL}\" ] && [ \"${W_INITIAL}\" != \"-\" ]"
check "original consumer: pull #1 ran the cross-pond import" \
    "grep -qE 'pull upstream complete' /tmp/p1pull1.log"

#############################
# DESTROY POND B — terraform-destroy analog
#############################

echo ""
echo "=== Phase 3: Destroy Pond B entirely (no consumer-side backup) ==="
rm -rf /tmp/pond-b-542
check "Pond B physically removed" "[ ! -d /tmp/pond-b-542 ]"

#############################
# POND B' (FRESH) — rebuilt consumer, same URL, same mount
#############################

echo ""
echo "=== Phase 4: Pond B' (fresh) — re-init, re-attach, pre-pull state ==="
pond init --birthplace test-host >/dev/null

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null

W_PRE=$(remote_last_pulled_tip upstream)
echo "Fresh consumer PULLED_TIP before any pull: '${W_PRE}'"
check_eq "fresh consumer: PULLED_TIP is unset before first pull" \
    "${W_PRE}" "-"

#############################
# PULL #1 — bootstrap on the rebuilt consumer
#############################

echo ""
echo "=== Phase 5: Fresh consumer pull #1 (bootstrap reconstruction) ==="
pond pull upstream 2>&1 | tee /tmp/p2pull1.log
W_REBUILT=$(remote_last_pulled_tip upstream)
echo "Fresh consumer PULLED_TIP after bootstrap: ${W_REBUILT}"

check "fresh consumer: pull #1 ran the cross-pond import" \
    "grep -qE 'pull upstream complete' /tmp/p2pull1.log"
check_eq "fresh consumer: bootstrap PULLED_TIP matches original (deterministic tip from same producer state)" \
    "${W_REBUILT}" "${W_INITIAL}"

# Sanity: all foreign data is queryable on the rebuilt consumer.
TEMPS_HASH=$(pond cat /imports/A/data/sensors/temps.csv 2>/dev/null | md5sum | cut -d' ' -f1)
TEMPS_EXP=$(md5sum /tmp/temps.csv | cut -d' ' -f1)
check_eq "fresh consumer: /imports/A/data/sensors/temps.csv content correct" \
    "${TEMPS_HASH}" "${TEMPS_EXP}"

EXTRA_HASH=$(pond cat /imports/A/data/sensors/extra.csv 2>/dev/null | md5sum | cut -d' ' -f1)
EXTRA_EXP=$(md5sum /tmp/extra.csv | cut -d' ' -f1)
check_eq "fresh consumer: /imports/A/data/sensors/extra.csv content correct" \
    "${EXTRA_HASH}" "${EXTRA_EXP}"

#############################
# PULL #2 — regression guard: no re-walk after reconstruction
#############################

echo ""
echo "=== Phase 6: Fresh consumer pull #2 (no-op; the regression guard) ==="
pond pull upstream 2>&1 | tee /tmp/p2pull2.log
W_REBUILT2=$(remote_last_pulled_tip upstream)
echo "Fresh consumer pull #2: PULLED_TIP=${W_REBUILT2}"

check "fresh consumer: pull #2 short-circuits as already up to date (no re-walk)" \
    "grep -qE 'already up to date' /tmp/p2pull2.log"
check_eq "fresh consumer: pull #2 PULLED_TIP unchanged from bootstrap" \
    "${W_REBUILT2}" "${W_REBUILT}"

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
