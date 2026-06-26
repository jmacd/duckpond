#!/bin/bash
# EXPERIMENT: `pond fsck` as a replica-comparison fingerprint.
#
# DESCRIPTION:
#   fsck's headline use case is "verify two replicas are identical."  The
#   root checksum commits to every OplogEntry row, keyed by pond_id/part_id,
#   so it is a faithful fingerprint of a pond's identity-preserving content.
#   This test pins down exactly what "comparable" means -- and the scope
#   limit baked into the design.  Two parts, both self-contained (the
#   replication half uses a file:// remote, no compose):
#
#   PART A -- byte replica, TOP-LEVEL root equality:
#     1. Build a pond with an inline file and an incompressible large-file
#        blob; record its fsck root.
#     2. `cp -r` the entire pond to a new location and fsck the copy: the
#        root is IDENTICAL (location-independent; a true replica matches by
#        root alone, which is the documented compare-by-root workflow).
#     3. --quick roots also match (the root is structural, content pass does
#        not move it).
#     4. Diverge the copy (add a file) -> roots now DIFFER, so the root
#        detects replica divergence.
#     5. Corrupt the copy's blob -> the copy's default fsck FAILS while its
#        --quick (structural) pass still passes -- the content pass is what
#        closes that gap, even on a replica.
#
#   PART B -- replication-produced replica, PARTITION-level equality:
#     6. Producer pushes to a file:// remote; a consumer cross-pond imports
#        and pulls.  The consumer keeps its OWN pond_id and mounts the
#        producer's data, so the two TOP-LEVEL roots legitimately differ
#        (the consumer has extra partitions) -- a content-only fingerprint
#        this is NOT.
#     7. But every one of the producer's per-partition digests
#        (pond_id/part_id rows=N checksum) reappears VERBATIM in the
#        consumer's `fsck --verbose` output: replication preserved pond_id,
#        part_id, and content checksums byte-for-byte.
#     8. Mutate the producer + re-push/re-pull -> the producer's partition
#        digests change AND the consumer re-converges to the new digests.
#
# EXPECTED:
#   - Byte-replica fsck roots match exactly and track divergence.
#   - Corruption on a replica is caught by the content pass, invisible to
#     --quick.
#   - A replication-produced consumer reproduces every producer partition
#     digest exactly, and stays converged across incremental pushes, even
#     though the consumer's top-level root differs by design.
#
# History:
#   Added on jmacd/65 alongside `pond fsck` to give the replica-comparison
#   guarantee end-to-end coverage (047-fsck.sh only exercises a single pond).
#   Uses a file:// remote so it runs in the base container without compose.
set -e
source check.sh

echo "=== Experiment: fsck replica equality ==="

P1=/tmp/718-p1
P1_COPY=/tmp/718-p1-copy
P2=/tmp/718-p2
REMOTE=/tmp/718-remote
rm -rf "$P1" "$P1_COPY" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

# All fsck partition digest lines: "<pond_id>/<part_id>  rows=N  <hex>".
fsck_partitions() {  # POND_DIR
    POND="$1" pond fsck --verbose 2>/dev/null | grep -E '^  [0-9a-f]'
}

echo "--- Part A: byte replica, top-level root equality ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
echo "hello duckpond" > /tmp/718-small.txt
head -c 200000 /dev/urandom > /tmp/718-big.bin   # incompressible -> real blob
pond copy host:///tmp/718-small.txt /small.txt >/dev/null 2>&1
pond copy host:///tmp/718-big.bin   /big.bin    >/dev/null 2>&1

ROOT_P1=$(pond fsck 2>/dev/null)
check '[ ${#ROOT_P1} -eq 64 ]' "producer root is 64 hex chars"

# A true replica: bit-for-bit copy of the whole pond directory.
cp -r "$P1" "$P1_COPY"
ROOT_COPY=$(POND="$P1_COPY" pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" = "'"$ROOT_COPY"'" ]' "byte-replica root equals original (location-independent)"

ROOT_P1_Q=$(pond fsck --quick 2>/dev/null)
ROOT_COPY_Q=$(POND="$P1_COPY" pond fsck --quick 2>/dev/null)
check '[ "'"$ROOT_P1_Q"'" = "'"$ROOT_COPY_Q"'" ]' "--quick roots also match across replicas"

echo "--- Part A: root detects divergence ---"
echo "diverge" > /tmp/718-extra.txt
POND="$P1_COPY" pond copy host:///tmp/718-extra.txt /extra.txt >/dev/null 2>&1
ROOT_COPY_DIV=$(POND="$P1_COPY" pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" != "'"$ROOT_COPY_DIV"'" ]' "root diverges after replica gains a file"

echo "--- Part A: content pass catches blob corruption on a replica ---"
BLOB=$(ls "$P1_COPY"/data/_large_files/blake3=*.parquet | head -1)
python3 - "$BLOB" <<'PY'
import sys
p = sys.argv[1]
d = bytearray(open(p, 'rb').read())
for i in range(len(d)//2, len(d)//2 + 16):
    d[i] ^= 0xFF
open(p, 'wb').write(d)
PY
set +e
POND="$P1_COPY" pond fsck > /tmp/718-corrupt.txt 2>&1
CORRUPT_EXIT=$?
POND="$P1_COPY" pond fsck --quick > /dev/null 2>&1
QUICK_EXIT=$?
set -e
check '[ '"$CORRUPT_EXIT"' -ne 0 ]' "default fsck fails on corrupted replica blob"
check_contains /tmp/718-corrupt.txt "fsck names the mismatching blake3" "does not match recorded blake3"
check '[ '"$QUICK_EXIT"' -eq 0 ]' "--quick still passes (structural-only; corruption invisible)"

echo "--- Part B: replication-produced replica, partition-level equality ---"
export POND="$P1"
pond backup add origin "file://${REMOTE}" >/dev/null 2>&1   # auto-push

# Capture the producer's partition digests (all share the producer pond_id).
fsck_partitions "$P1" | sort > /tmp/718-p1-parts.txt
P1ID=$(awk -F/ '{print $1}' /tmp/718-p1-parts.txt | tr -d ' ' | head -1)
check '[ -n "'"$P1ID"'" ]' "producer pond_id extracted"

export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/718-pull.log 2>&1
check 'grep -qE "applied [1-9][0-9]* bundle" /tmp/718-pull.log' "consumer pulled producer bundles"

# Top-level roots differ by design (consumer has its own pond_id partitions).
ROOT_P2=$(pond fsck 2>/dev/null)
check '[ "'"$ROOT_P1"'" != "'"$ROOT_P2"'" ]' "consumer top-level root differs (own pond_id present)"

# Every producer partition digest must reappear verbatim in the consumer.
fsck_partitions "$P2" | grep "^  ${P1ID}/" | sort > /tmp/718-p2-foreign.txt
check 'diff -q /tmp/718-p1-parts.txt /tmp/718-p2-foreign.txt' \
    "consumer reproduces ALL producer partition digests exactly (replica intact)"

# The consumer also carries its own (different) pond_id partitions.
check '[ -n "$(fsck_partitions "'"$P2"'" | grep -v "^  '"$P1ID"'/")" ]' \
    "consumer has its own pond_id partitions (not a pure mirror)"

echo "--- Part B: convergence across an incremental push ---"
export POND="$P1"
echo "second-gen" > /tmp/718-gen2.txt
pond copy host:///tmp/718-gen2.txt /gen2.txt >/dev/null 2>&1   # auto-pushed
fsck_partitions "$P1" | sort > /tmp/718-p1-parts2.txt
check '! diff -q /tmp/718-p1-parts.txt /tmp/718-p1-parts2.txt' \
    "producer partition digests change after new data"

export POND="$P2"
pond pull upstream > /tmp/718-pull2.log 2>&1
fsck_partitions "$P2" | grep "^  ${P1ID}/" | sort > /tmp/718-p2-foreign2.txt
check 'diff -q /tmp/718-p1-parts2.txt /tmp/718-p2-foreign2.txt' \
    "consumer re-converges to producer's new partition digests after pull"

check_finish
