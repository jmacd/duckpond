#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Format cache persistence for cross-pond (read-through) imports
#
# DESCRIPTION:
#   Investigates remote-followup.md item C2: "Site rebuild re-derives the
#   full ingest history every build."  The site (consumer) pond mounts
#   producer ponds read-through and reads their raw oteljson ingest files
#   through a format provider on every sitegen build.  The format cache
#   (docs/format-cache-design.md) is supposed to make this incremental:
#   each immutable file version is parsed once, cached as Parquet under
#   {POND}/cache/, and reused on later reads.
#
#   The open question (and @jmacd's hypothesis): does that cache actually
#   persist and hit across builds when the underlying files are FOREIGN,
#   reached only through a cross-pond read-through mount?  If the consumer
#   re-parses the full history on every build, cross-pond sites pay an
#   O(history) recompute each tick -- a real scaling concern.
#
#   This test isolates the variable: the same physical oteljson files are
#   read TWICE through the mount, in two separate `pond` processes (the
#   "across builds" scenario, since the cache lives on disk in
#   {POND}/cache/).  We watch the format-cache log lines:
#
#     [SAVE] Format cache: writing N of M versions ...   <- cache MISS (parse)
#     [GO]   Format cache: all N versions cached ...      <- cache HIT  (reuse)
#
# TEST SHAPE:
#   Pond A (producer): init, write 3 raw oteljson files under /ingest,
#       `pond backup add` + `pond push` to MinIO.
#   Pond B (consumer): init, `pond remote add upstream ... /imports/A`,
#       `pond pull`.
#   Build #1 on B: read each /imports/A/ingest/*.json via oteljson:// .
#       Expect a cold cache: one [SAVE] per file, and Parquet files
#       materialized under B's {POND}/cache/.
#   Build #2 on B: read the SAME files again (fresh process).
#       Expect a warm cache: ZERO [SAVE] lines, one [GO] per file, and
#       byte-identical query results.
#
# EXPECTED (cache working as designed -> C2 is a one-time cold cost):
#   - Build #1: >= 3 [SAVE] lines; cache/ holds >= 3 oteljson Parquet files.
#   - Build #2: 0 [SAVE] lines; >= 3 [GO] lines; results identical.
#
# FAILURE INTERPRETATION (confirms @jmacd's hypothesis / a real C2 bug):
#   - Build #2 still emits [SAVE] lines  ==> the format cache does NOT
#     persist/hit for cross-pond read-through imports; every consumer
#     build re-derives the full foreign history.  Next step would be to
#     chase node_id / blake3 stability (or is_dynamic() classification)
#     for foreign nodes resolved through the mount in
#     provider/src/provider_api.rs::ensure_url_cached.
set -e

echo "=== Experiment: Cross-Pond Format Cache Persistence (C2) ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-534"

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

check_ge() {
    local description="$1"
    local actual="$2"
    local floor="$3"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if [[ "${actual}" -ge "${floor}" ]]; then
        echo "  ✓ ${description} (${actual} >= ${floor})"
    else
        echo "  ✗ ${description}: got '${actual}', expected >= '${floor}'"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    fi
}

# Number of [SAVE]/[GO] format-cache lines for the oteljson scheme in a log.
save_count() { grep -c '\[SAVE\] Format cache: writing' "$1" 2>/dev/null || true; }
go_count()   { grep -c '\[GO\] Format cache: all'        "$1" 2>/dev/null || true; }

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
echo "=== Phase 1: Pond A (producer) — raw oteljson ingest files ==="

export POND=/tmp/pond-a-534
rm -rf "${POND}"
pond init >/dev/null
pond mkdir -p /ingest >/dev/null

# Three independent raw oteljson files, mirroring the per-file ingest
# history that the Caspar Water site re-derives in C2.  Each is a
# physical `data` entry (entry type is NOT derived from the .json name).
make_otel() {
    local file="$1" t0="$2" t1="$3" v0="$4" v1="$5"
    cat > "${file}" << EOF
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"${t0}","asDouble":${v0}}]}}]}]}]}
{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{"name":"modbus"},"metrics":[{"name":"tank_level","unit":"inches","gauge":{"dataPoints":[{"timeUnixNano":"${t1}","asDouble":${v1}}]}}]}]}]}
EOF
}

make_otel /tmp/m1.json 1700000000000000000 1700000060000000000 12.5 12.3
make_otel /tmp/m2.json 1700000120000000000 1700000180000000000 11.8 11.6
make_otel /tmp/m3.json 1700000240000000000 1700000300000000000 10.9 10.7

for n in 1 2 3; do
    pond copy "host:///tmp/m${n}.json" "/ingest/m${n}.json" >/dev/null
done
echo "Wrote 3 oteljson files (2 rows each) under /ingest."

POND_ID_A=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond A pond_id: ${POND_ID_A}"

echo ""
echo "=== Phase 2: Pond A — backup add + push ==="
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null
echo "Push complete."

#############################
# POND B — CONSUMER
#############################

echo ""
echo "=== Phase 3: Pond B (consumer) — init + cross-pond pull ==="
export POND=/tmp/pond-b-534
rm -rf "${POND}"
pond init >/dev/null

POND_ID_B=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
check "Pond A and Pond B have distinct pond_ids" \
    "[ \"${POND_ID_A}\" != \"${POND_ID_B}\" ]"

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond pull upstream >/dev/null
echo "Pull complete."

check "imported oteljson files are visible through the mount" \
    "pond list /imports/A/ingest/ 2>/dev/null | grep -q 'm1.json'"

# Helper: read all three imported files via oteljson:// through the mount,
# capturing query results to $1 (stdout) and logs to $2 (stderr).
read_all_through_mount() {
    local results="$1" logfile="$2" rustlog="$3"
    : > "${results}"
    : > "${logfile}"
    for n in 1 2 3; do
        RUST_LOG="${rustlog}" pond cat "oteljson:///imports/A/ingest/m${n}.json" \
            --format=table \
            --sql "SELECT COUNT(*) AS n, MAX(tank_level) AS mx FROM source" \
            >> "${results}" 2>> "${logfile}"
    done
}

#############################
# BUILD #1 — cold cache expected
#############################

echo ""
echo "=== Phase 4: Build #1 (cold cache expected) ==="
read_all_through_mount /tmp/b-results1.txt /tmp/b-build1.log "info"

SAVE_1=$(save_count /tmp/b-build1.log)
echo "Build #1 [SAVE] count: ${SAVE_1}"
grep '\[SAVE\] Format cache' /tmp/b-build1.log || true
check_ge "Build #1 parses each foreign file once (>=3 [SAVE])" "${SAVE_1}" 3

# The cache lives in B's own pond directory, never in A's.
CACHE_DIR="/tmp/pond-b-534/cache"
CACHE_PARQUET_1=$(find "${CACHE_DIR}" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
echo "Cached Parquet files under ${CACHE_DIR}: ${CACHE_PARQUET_1}"
find "${CACHE_DIR}" -name '*.parquet' 2>/dev/null || true
check_ge "Build #1 materialized cache Parquet for foreign files (>=3)" \
    "${CACHE_PARQUET_1}" 3

#############################
# BUILD #2 — warm cache is the whole point
#############################

echo ""
echo "=== Phase 5: Build #2 (warm cache expected — the C2 question) ==="
# provider=debug surfaces the [GO] cache-hit line (logged at debug);
# info still surfaces any [SAVE] cache-miss line.
read_all_through_mount /tmp/b-results2.txt /tmp/b-build2.log "info,provider=debug"

SAVE_2=$(save_count /tmp/b-build2.log)
GO_2=$(go_count /tmp/b-build2.log)
echo "Build #2 [SAVE] count: ${SAVE_2}   [GO] count: ${GO_2}"
echo "--- build #2 format-cache log lines ---"
grep -E '\[SAVE\]|\[GO\] Format cache' /tmp/b-build2.log || true

# THE key assertion for C2 / @jmacd's hypothesis:
check_eq "Build #2 re-parses NOTHING (0 [SAVE] — cache persisted across builds)" \
    "${SAVE_2}" "0"
check_ge "Build #2 reuses every cached foreign file (>=3 [GO])" "${GO_2}" 3

#############################
# CORRECTNESS — cache must not change results
#############################

echo ""
echo "=== Phase 6: Cached reads return identical results ==="
if diff -u /tmp/b-results1.txt /tmp/b-results2.txt >/dev/null 2>&1; then
    echo "  ✓ Build #1 and Build #2 query results are byte-identical"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
else
    echo "  ✗ Build #1 and Build #2 query results differ:"
    diff -u /tmp/b-results1.txt /tmp/b-results2.txt || true
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    CHECKS_FAILED=$((CHECKS_FAILED + 1))
fi

#############################
# RESULTS
#############################

echo ""
echo "=== Results ==="
echo "Checks: ${CHECKS_TOTAL}, Failed: ${CHECKS_FAILED}"
if [[ "${CHECKS_FAILED}" -ne 0 ]]; then
    echo "[FAIL] one or more checks failed"
    echo ""
    echo "If Build #2 still shows [SAVE] lines, this REPRODUCES C2: the"
    echo "format cache does not persist/hit for cross-pond read-through"
    echo "imports, so consumer (site) rebuilds re-derive the full foreign"
    echo "history every build."
    exit 1
fi
echo "[OK] all checks passed — format cache persists across cross-pond builds"
