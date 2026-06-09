#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Cross-pond path boundaries (D5.7b invariant 1: read-only foreign mount)
#
# DESCRIPTION:
#   D5.7b § 3 invariant 1: "A foreign pond_id reachable through a mount
#   path is strictly read-only." This is enforced in tinyfs at
#   `WD::insert_node::check_writable` and surfaces to callers as
#   `tinyfs::Error::ReadOnlyImport`.
#
#   At the CLI layer, every writing verb that targets a path under the
#   mount root must fail.  This test exercises the major write surfaces
#   end-to-end through MinIO:
#
#     Phase 1 — Pond A: init + populate + push to MinIO.
#     Phase 2 — Pond B: init + cross-pond-import A at /imports/A + pull.
#     Phase 3 — Sanity: reads through the mount work and bytes match A.
#     Phase 4 — Writes anywhere inside /imports/A/* are refused:
#               * `pond copy host:///file /imports/A/new.txt`
#               * `pond mkdir /imports/A/newdir`
#               * `pond mkdir /imports/A/data/newsub`
#               * `pond mknod sql-derived-table /imports/A/factory ...`
#     Phase 5 — After every refused write, foreign content is intact.
#     Phase 6 — Local writes OUTSIDE the mount on Pond B still succeed.
#     Phase 7 — Path-namespace isolation: Pond B has its OWN /data/foo.txt
#               that coexists with Pond A's /data/foo.txt visible via
#               /imports/A/data/foo.txt; they are distinct entries with
#               distinct content.
#
# EXPECTED:
#   - Every write-into-mount command exits non-zero with a message that
#     mentions read-only / ReadOnlyImport / cannot modify.
#   - After all failed writes, /imports/A/data/sensors.csv still reads
#     bit-for-bit identical to Pond A's original.
#   - Local writes on Pond B at non-mount paths still succeed.
#   - Pond B's /data/foo.txt content != Pond A's /data/foo.txt content,
#     proving that '/data' inside the mount resolves into the foreign
#     tree rather than aliasing Pond B's root.
#
# History:
#   Revived D5.8.6 from a `DISABLED-D4` stub that drove the long-removed
#   `remote` factory with an `import:` YAML.  Modeled on the existing
#   Rust integration test `foreign_mount_writes_are_refused` in
#   crates/cmd/tests/test_remote_cli.rs (D5.7b.3).
set -e

echo "=== Experiment: Cross-Pond Path Boundaries (invariant 1) ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-532"

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

# Run a command that we EXPECT to fail with a read-only-import-style
# error.  Pass iff (a) command exits non-zero AND (b) stderr matches a
# read-only signature.
check_refused() {
    local description="$1"
    local cmd="$2"
    CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
    local out
    set +e
    out=$(eval "${cmd}" 2>&1)
    local rc=$?
    set -e
    if [[ "${rc}" -ne 0 ]] && \
       echo "${out}" | grep -qiE "read.?only|readonlyimport|cannot modify|imported foreign"; then
        echo "  ✓ ${description} (rc=${rc})"
    else
        echo "  ✗ ${description}: rc=${rc}, output:"
        echo "${out}" | sed 's/^/      /'
        CHECKS_FAILED=$((CHECKS_FAILED + 1))
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
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null 2>&1

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A — init, populate, push ==="
export POND=/tmp/pond-a
rm -rf "${POND}"
pond init >/dev/null

pond mkdir /data >/dev/null

cat > /tmp/sensors.csv << 'EOF'
ts,sensor,value
2024-01-01T00:00:00Z,A,11
2024-01-01T01:00:00Z,A,12
2024-01-01T02:00:00Z,A,13
EOF
pond copy host:///tmp/sensors.csv /data/sensors.csv >/dev/null

# A file at the same logical path Pond B will use locally; the two
# must coexist as distinct entries under their owners' pond_ids.
echo "PRODUCER A: this is /data/foo.txt in pond A" > /tmp/foo-a.txt
pond copy host:///tmp/foo-a.txt /data/foo.txt >/dev/null

POND_ID_A=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond A pond_id: ${POND_ID_A}"

pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

SENSORS_HASH_A=$(pond cat /data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
FOO_HASH_A=$(pond cat /data/foo.txt 2>/dev/null | md5sum | cut -d' ' -f1)
echo "  /data/sensors.csv md5 on A: ${SENSORS_HASH_A}"
echo "  /data/foo.txt     md5 on A: ${FOO_HASH_A}"

#############################
# POND B — CONSUMER
#############################

echo ""
echo "=== Phase 2: Pond B — init, attach + pull A as /imports/A ==="
export POND=/tmp/pond-b
rm -rf "${POND}"
pond init >/dev/null

POND_ID_B=$(pond config 2>/dev/null | awk '/^Pond ID:/ {print $NF; exit}')
echo "Pond B pond_id: ${POND_ID_B}"

# B has its OWN /data/foo.txt with different content; this proves
# /data inside the mount resolves to the foreign tree, not aliasing
# B's root.
pond mkdir /data >/dev/null
echo "CONSUMER B: this is /data/foo.txt in pond B" > /tmp/foo-b.txt
pond copy host:///tmp/foo-b.txt /data/foo.txt >/dev/null
FOO_HASH_B_BEFORE=$(pond cat /data/foo.txt 2>/dev/null | md5sum | cut -d' ' -f1)

pond remote add upstream "s3://${BUCKET_NAME}" /imports/A \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond pull upstream >/dev/null

#############################
# SANITY
#############################

echo ""
echo "=== Phase 3: Sanity — reads through the mount ==="
SENSORS_HASH_B=$(pond cat /imports/A/data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "/imports/A/data/sensors.csv readable and matches A" \
    "${SENSORS_HASH_B}" "${SENSORS_HASH_A}"

FOO_HASH_B_VIA_MOUNT=$(pond cat /imports/A/data/foo.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "/imports/A/data/foo.txt routes to A's content" \
    "${FOO_HASH_B_VIA_MOUNT}" "${FOO_HASH_A}"

#############################
# READ-ONLY ENFORCEMENT
#############################

echo ""
echo "=== Phase 4: Writes anywhere inside /imports/A/* are refused ==="

# 4a. pond copy host://... /imports/A/<NEW>
echo "blocked file content" > /tmp/blocked.txt
check_refused "pond copy host:///tmp/blocked.txt /imports/A/blocked.txt" \
    "pond copy host:///tmp/blocked.txt /imports/A/blocked.txt"

# 4b. pond mkdir at the mount root
check_refused "pond mkdir /imports/A/newdir" \
    "pond mkdir /imports/A/newdir"

# 4c. pond mkdir inside a foreign subdir
check_refused "pond mkdir /imports/A/data/newsub" \
    "pond mkdir /imports/A/data/newsub"

# 4d. pond mknod (dynamic factory) inside the mount
cat > /tmp/sql-derived.yaml << 'YAML'
patterns:
  source: "table:///data/*.table"
query: "SELECT 1"
YAML
check_refused "pond mknod sql-derived-table /imports/A/factory ..." \
    "pond mknod sql-derived-table /imports/A/factory --config-path /tmp/sql-derived.yaml"

#############################
# POST-CONDITION
#############################

echo ""
echo "=== Phase 5: Foreign content remains intact after refused writes ==="
SENSORS_HASH_B_AFTER=$(pond cat /imports/A/data/sensors.csv 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "/imports/A/data/sensors.csv unchanged after refused writes" \
    "${SENSORS_HASH_B_AFTER}" "${SENSORS_HASH_A}"

# /imports/A should NOT have a blocked.txt or newdir/newsub/factory.
pond list /imports/A/ > /tmp/b-mount-list.txt 2>&1 || true
cat /tmp/b-mount-list.txt
check "no 'blocked.txt' leaked into the mount" \
    "! grep -q 'blocked.txt' /tmp/b-mount-list.txt"
check "no 'newdir' leaked into the mount" \
    "! grep -q 'newdir' /tmp/b-mount-list.txt"
check "no 'factory' leaked into the mount" \
    "! grep -q '/imports/A/factory$' /tmp/b-mount-list.txt"
pond list /imports/A/data/ > /tmp/b-mount-data-list.txt 2>&1 || true
check "no 'newsub' leaked into /imports/A/data/" \
    "! grep -q 'newsub' /tmp/b-mount-data-list.txt"

#############################
# LOCAL WRITES STILL WORK
#############################

echo ""
echo "=== Phase 6: Local writes OUTSIDE the mount still succeed on B ==="
echo "still works" > /tmp/post-test.txt
pond copy host:///tmp/post-test.txt /data/post-test.txt >/dev/null
POST_HASH=$(pond cat /data/post-test.txt 2>/dev/null | md5sum | cut -d' ' -f1)
EXP_HASH=$(md5sum /tmp/post-test.txt | cut -d' ' -f1)
check_eq "Pond B can still write /data/post-test.txt locally" \
    "${POST_HASH}" "${EXP_HASH}"

#############################
# PATH-NAMESPACE ISOLATION
#############################

echo ""
echo "=== Phase 7: Path-namespace isolation /data vs /imports/A/data ==="
FOO_HASH_B_AFTER=$(pond cat /data/foo.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check_eq "Pond B's local /data/foo.txt still has B's own content" \
    "${FOO_HASH_B_AFTER}" "${FOO_HASH_B_BEFORE}"
check "Pond A's and Pond B's /data/foo.txt have distinct content" \
    "[ \"${FOO_HASH_A}\" != \"${FOO_HASH_B_BEFORE}\" ]"
check_eq "/imports/A/data/foo.txt still routes to A's content (not B's)" \
    "${FOO_HASH_B_VIA_MOUNT}" "${FOO_HASH_A}"

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
