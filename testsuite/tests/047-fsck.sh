#!/bin/bash
# EXPERIMENT: `pond fsck` -- local filesystem-check.
# DESCRIPTION: fsck computes a single deterministic root checksum over every
#   row in the pond and re-validates content checksums (inline + external
#   large-file blobs).  This test verifies:
#     1. fsck prints a stable 64-hex root checksum on a clean pond.
#     2. The root is deterministic (re-running yields the same value) and
#        independent of the content-rehash pass (--quick matches default).
#     3. The root CHANGES when data changes.
#     4. --verbose reports per-partition digests and content stats.
#     5. Corrupting a large-file blob is DETECTED (default fsck fails) while
#        the structural-only --quick pass still passes -- exactly the gap
#        the content pass closes.
# EXPECTED: All checks pass; corruption produces a non-zero exit and an error
#   naming the mismatching blake3.
set -e
source check.sh

echo "=== Experiment: pond fsck ==="

pond init --birthplace test-host

echo "hello duckpond" > /tmp/small.txt
head -c 200000 /dev/urandom > /tmp/big.bin
pond copy host:///tmp/small.txt /small.txt
pond copy host:///tmp/big.bin /big.bin

echo "=== Clean fsck ==="
ROOT1=$(pond fsck 2>/dev/null)
echo "root1=$ROOT1"
check "[ \${#ROOT1} -eq 64 ]" "root checksum is 64 hex chars"

ROOT2=$(pond fsck 2>/dev/null)
check "[ \"$ROOT1\" = \"$ROOT2\" ]" "root is deterministic across runs"

ROOT_QUICK=$(pond fsck --quick 2>/dev/null)
check "[ \"$ROOT1\" = \"$ROOT_QUICK\" ]" "root identical with --quick (content pass does not affect root)"

pond fsck --verbose > /tmp/fsck_verbose.txt 2>/dev/null
check_contains /tmp/fsck_verbose.txt "verbose shows root checksum" "Root checksum: $ROOT1"
check_contains /tmp/fsck_verbose.txt "verbose reports inline verified" "Inline verified:"
check_contains /tmp/fsck_verbose.txt "verbose reports blobs verified" "Blobs verified:"
check_contains /tmp/fsck_verbose.txt "verbose reports OK result" "Result:          OK"

echo "=== Root changes when data changes ==="
echo "another file" > /tmp/another.txt
pond copy host:///tmp/another.txt /another.txt
ROOT3=$(pond fsck 2>/dev/null)
check "[ \"$ROOT1\" != \"$ROOT3\" ]" "root changes after adding a file"

echo "=== Corruption detection ==="
BLOB=$(ls "$POND"/data/_large_files/blake3=*.parquet | head -1)
echo "corrupting blob: $BLOB"
# Flip 16 bytes near the middle of the blob's data region.
python3 - "$BLOB" <<'PY'
import sys
p = sys.argv[1]
d = bytearray(open(p, 'rb').read())
for i in range(len(d)//2, len(d)//2 + 16):
    d[i] ^= 0xFF
open(p, 'wb').write(d)
PY

set +e
pond fsck > /tmp/fsck_corrupt.txt 2>&1
CORRUPT_EXIT=$?
pond fsck --quick > /dev/null 2>&1
QUICK_EXIT=$?
set -e

check "[ $CORRUPT_EXIT -ne 0 ]" "fsck fails (non-zero exit) on corrupted blob"
check_contains /tmp/fsck_corrupt.txt "fsck reports content error" "does not match recorded blake3"
check "[ $QUICK_EXIT -eq 0 ]" "--quick still passes (structural-only; corruption invisible to row Merkle)"

check_finish
