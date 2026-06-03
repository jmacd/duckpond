#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: 3-deep cross-pond chain pins non-transitive import invariant
#             (D5.7b push filters foreign-pond rows from outbound bundles)
#
# DESCRIPTION:
#   Three ponds A -> B -> C connected by two S3 buckets:
#
#     Pond A (producer):       writes /data/a.txt;     pushes to bucket-A
#     Pond B (middle):         mounts A at /imports/A; pulls A;
#                              writes /data/b.txt;     pushes to bucket-B
#     Pond C (downstream):     mounts B at /imports/B; pulls B
#
#   Inside B, /imports/A is a foreign mount whose underlying rows are
#   partition-tagged with A's pond_id, not B's.  The push pipeline in
#   `steward::remote_adapter::actions_at_version` filters outbound
#   bundles to rows where `partition_values["pond_id"] == local pond's
#   uuid` -- foreign rows are intentionally excluded.  Cross-pond import
#   is therefore NOT transitive: C sees B's local content through the
#   mount but does NOT see A's content via /imports/B/imports/A/<...>.
#
#   This test pins that contract.  If anyone weakens the pond_id filter
#   in actions_at_version without a deliberate design change, this test
#   breaks loudly.
#
# EXPECTED:
#   - C reads B's local /imports/B/data/b.txt and /imports/B/sys/*.
#   - C's /imports/B/imports/ exists as a B-owned directory entry
#     (B materialized it via create_dir_all when attaching upstreamA),
#     but its body is empty/unresolvable on C because the mount-entry
#     row was filtered out of B's push.
#   - C cannot read /imports/B/imports/A/data/a.txt.
#   - A and B remain fully functional: B still reads A through its
#     local /imports/A mount.
#
# History:
#   The legacy DISABLED-D4 stub exercised the deleted `remote` factory's
#   flat-vs-recursive `source_path` filter (/data vs /data/**); that
#   surface no longer exists -- in D5.7b cross-pond mounts the whole
#   foreign pond root.  Rewritten 2026-06 to cover the truly recursive
#   case (3-deep transitive replication) per docs/d5.8-resume.md step 7.
set -e

echo "=== Experiment: 3-deep cross-pond non-transitivity ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_A="cross-pond-531-a"
BUCKET_B="cross-pond-531-b"

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

# Run a pond CLI command that we EXPECT to fail; report success only if
# it actually exits non-zero.
check_fails() {
    local description="$1"
    local cmd="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    if eval "${cmd}" >/dev/null 2>&1; then
        echo "  ✗ ${description}: command unexpectedly SUCCEEDED (cmd: ${cmd})"
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
    else
        echo "  ✓ ${description}"
    fi
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
for bucket in "${BUCKET_A}" "${BUCKET_B}"; do
    mc rb --force "local/${bucket}" >/dev/null 2>&1 || true
    mc mb "local/${bucket}" >/dev/null 2>&1
done

# Shared S3 args so the three ponds use consistent credentials.
s3_args=(--region us-east-1
         --endpoint "${MINIO_ENDPOINT}"
         --access-key-id "${MINIO_ROOT_USER}"
         --secret-access-key "${MINIO_ROOT_PASSWORD}"
         --allow-http)

#############################
# POND A — UPSTREAM
#############################

echo ""
echo "=== Phase 1: Pond A — init, write /data/a.txt, push to bucket-A ==="

export POND=/tmp/pond-a-531
rm -rf "${POND}"
pond init >/dev/null

pond mkdir /data >/dev/null
echo "from pond A" > /tmp/a-531.txt
pond copy host:///tmp/a-531.txt /data/a.txt >/dev/null

pond backup add originA "s3://${BUCKET_A}" \
    "${s3_args[@]}" --overwrite >/dev/null
pond push originA >/dev/null

POND_ID_A=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
A_TAIL="${POND_ID_A##*-}"
A_HASH=$(pond cat /data/a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
echo "Pond A pond_id: ${POND_ID_A}  (tail=${A_TAIL})"
echo "Pond A /data/a.txt hash: ${A_HASH}"

#############################
# POND B — MIDDLE NODE
#############################

echo ""
echo "=== Phase 2: Pond B — mount A, pull, add own data, push to bucket-B ==="

export POND=/tmp/pond-b-531
rm -rf "${POND}"
pond init >/dev/null

pond remote add upstreamA "s3://${BUCKET_A}" /imports/A \
    "${s3_args[@]}" --overwrite >/dev/null
pond pull upstreamA >/dev/null

# Sanity: B can read A through its own /imports/A mount.
B_VIEW_OF_A=$(pond cat /imports/A/data/a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "B reads A through /imports/A (2-deep works)" "${B_VIEW_OF_A}" "${A_HASH}"

pond mkdir /data >/dev/null
echo "from pond B" > /tmp/b-531.txt
pond copy host:///tmp/b-531.txt /data/b.txt >/dev/null

pond backup add originB "s3://${BUCKET_B}" \
    "${s3_args[@]}" --overwrite >/dev/null
pond push originB >/dev/null

POND_ID_B=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
B_TAIL="${POND_ID_B##*-}"
B_HASH=$(pond cat /data/b.txt 2>/dev/null | md5sum | cut -d' ' -f1)
echo "Pond B pond_id: ${POND_ID_B}  (tail=${B_TAIL})"
echo "Pond B /data/b.txt hash: ${B_HASH}"

check "A and B have distinct pond_ids" "[ \"${POND_ID_A}\" != \"${POND_ID_B}\" ]"

#############################
# POND C — DOWNSTREAM
#############################

echo ""
echo "=== Phase 3: Pond C — mount B, pull ==="

export POND=/tmp/pond-c-531
rm -rf "${POND}"
pond init >/dev/null

pond remote add upstreamB "s3://${BUCKET_B}" /imports/B \
    "${s3_args[@]}" --overwrite >/dev/null
pond pull upstreamB >/dev/null

POND_ID_C=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
C_TAIL="${POND_ID_C##*-}"
echo "Pond C pond_id: ${POND_ID_C}  (tail=${C_TAIL})"

check "A, B, C all have distinct pond_ids" \
    "[ \"${POND_ID_A}\" != \"${POND_ID_C}\" ] && [ \"${POND_ID_B}\" != \"${POND_ID_C}\" ]"

#############################
# VERIFICATION (the actual invariants)
#############################

echo ""
echo "=== Phase 4: C reads B's local content through /imports/B (works) ==="

pond list /imports/B/ > /tmp/c-list-b.txt 2>&1 || true
cat /tmp/c-list-b.txt

check "C's /imports/B/ listing includes 'data'" \
    "grep -q '/imports/B/data$' /tmp/c-list-b.txt"
check "C's /imports/B/ listing includes 'imports'" \
    "grep -q '/imports/B/imports$' /tmp/c-list-b.txt"
check "C's /imports/B/ listing includes 'sys'" \
    "grep -q '/imports/B/sys$' /tmp/c-list-b.txt"
check "all B-tree children carry B's pond_id tail [${B_TAIL}]" \
    "[ \"\$(grep -cE '/imports/B/(data|imports|sys)\$' /tmp/c-list-b.txt)\" = \"\$(grep -cE '\[${B_TAIL}\].*/imports/B/(data|imports|sys)\$' /tmp/c-list-b.txt)\" ]"

C_VIEW_OF_B=$(pond cat /imports/B/data/b.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "C reads B's /data/b.txt through /imports/B (B's local content replicates)" \
    "${C_VIEW_OF_B}" "${B_HASH}"

echo ""
echo "=== Phase 5: 3-deep transitivity is BLOCKED by the pond_id push filter ==="
# B's push contains only rows where partition_values["pond_id"] == B.
# The /imports/A mount entry that B inserted has A's pond_id and is
# therefore filtered out of bucket-B.  C sees /imports/B/imports as a
# child entry (B-owned dir created by `create_dir_all`) but its body
# is empty/unresolvable on C.

# The /imports/B/imports parent entry exists at C ...
check "/imports/B/imports appears in C's listing (B-owned mkdir parent)" \
    "grep -q '/imports/B/imports$' /tmp/c-list-b.txt"

# ... but listing its CHILDREN must NOT succeed (the foreign mount-entry
# row for 'A' was filtered out of B's push, so /imports/B/imports has
# no resolvable body in C's view).
check_fails "listing /imports/B/imports/ on C fails (no foreign rows)" \
    "pond list /imports/B/imports/"

# Direct access through the would-be transitive path must fail too.
check_fails "reading /imports/B/imports/A/data/a.txt on C fails (3-deep blocked)" \
    "pond cat /imports/B/imports/A/data/a.txt"

# A's pond_id tail must not surface anywhere in C's pond.
pond list / > /tmp/c-list-root.txt 2>&1 || true
check "A's pond_id tail [${A_TAIL}] never surfaces in C" \
    "! grep -q '\[${A_TAIL}\]' /tmp/c-list-root.txt"

echo ""
echo "=== Phase 6: A and B remain fully usable in their own right ==="
# Sanity that the test didn't corrupt the middle node: B can still
# read its own data AND still read A through its mount.
export POND=/tmp/pond-b-531
B_OWN=$(pond cat /data/b.txt 2>/dev/null | md5sum | cut -d' ' -f1)
B_OF_A=$(pond cat /imports/A/data/a.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "B still reads /data/b.txt after C pulls" "${B_OWN}" "${B_HASH}"
check_eq "B still reads /imports/A/data/a.txt after C pulls" "${B_OF_A}" "${A_HASH}"

#############################
# CLEANUP
#############################

mc rb --force "local/${BUCKET_A}" >/dev/null 2>&1 || true
mc rb --force "local/${BUCKET_B}" >/dev/null 2>&1 || true

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
