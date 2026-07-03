#!/bin/bash
# EXPERIMENT: A large external blob (>64KB) survives content-addressed
#             replication and is READABLE BYTE-FOR-BYTE on the consumer.
#
# DESCRIPTION:
#   Files larger than LARGE_FILE_THRESHOLD (64KB) are not inlined as object
#   rows -- they are externalized to `data/_large_files/blake3=*` and transfer
#   by hash (D7).  On pull, the consumer streams the external blob back into
#   its own `_large_files/` store (commit f90a5aa6, "D7: consumer-side
#   streaming rebuild for large external blobs").
#
#   718 already creates a 200KB blob and proves the producer and consumer
#   converge on the same content TIP HASH, and that producer-side fsck catches
#   blob corruption.  But 718 only reads the small INLINE file (`/a.txt`) back
#   through the consumer mount -- it never reads the LARGE BLOB bytes on the
#   consumer.  Tip equality proves the recorded blob HASH matches; it does not
#   prove the consumer materialized the blob bytes so they are readable.  A bug
#   in the consumer's streaming rebuild that stored wrong/truncated bytes under
#   the right hash key would pass 718's tip check and only surface on a read.
#
#   This test closes that gap: it reads the >64KB blob back through the
#   consumer mount and asserts an exact md5 match against the producer, and
#   confirms the blob really landed in the consumer's `_large_files/` store
#   (i.e. it was transferred externally, not inlined).
#
# EXPECTED:
#   - Producer stores the blob in `data/_large_files/`.
#   - Consumer pull completes; consumer also has the blob in `_large_files/`.
#   - `pond cat` of the blob on the consumer md5-matches the producer exactly.
#   - A small inline sibling file also round-trips (control).
#
# History:
#   Added on jmacd/65 to cover byte-level readback of a D7 external large blob
#   on the consumer, a gap left by 718 (tip equality + inline-file readback).
set -e
source check.sh

echo "=== Experiment: large external blob (>64KB) replication readback ==="

P1=/tmp/723-p1
P2=/tmp/723-p2
REMOTE=/tmp/723-remote
rm -rf "$P1" "$P2" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer creates a >64KB incompressible blob + a small file ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
# 200000 bytes of /dev/urandom -> incompressible, comfortably over the 64KB
# threshold so it is externalized rather than inlined.
head -c 200000 /dev/urandom > /tmp/723-big.bin
echo "small inline sibling" > /tmp/723-small.txt
pond copy host:///tmp/723-big.bin   /big.bin   >/dev/null 2>&1
pond copy host:///tmp/723-small.txt /small.txt >/dev/null 2>&1

BIG_MD5=$(md5sum /tmp/723-big.bin | cut -d' ' -f1)
SMALL_MD5=$(md5sum /tmp/723-small.txt | cut -d' ' -f1)

check 'ls "$P1"/data/_large_files/blake3=*.parquet >/dev/null 2>&1' \
    "producer externalized the >64KB blob into _large_files/"

echo "--- Step 2: backup add (auto-push) to a file:// remote ---"
pond backup add origin "file://${REMOTE}" > /tmp/723-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/723-backup.log' "backup add origin succeeded"

echo "--- Step 3: consumer cross-pond import + pull ---"
export POND="$P2"
pond init --birthplace test-host >/dev/null
pond remote add upstream "file://${REMOTE}" /imports/up >/dev/null 2>&1
pond pull upstream > /tmp/723-pull.log 2>&1
check 'grep -q "pull upstream complete" /tmp/723-pull.log' "consumer completed cross-pond import"

echo "--- Step 4: consumer transferred the blob EXTERNALLY (not inlined) ---"
check 'ls "$P2"/data/_large_files/blake3=*.parquet >/dev/null 2>&1' \
    "consumer materialized the blob into its own _large_files/ store"

echo "--- Step 5: consumer reads the LARGE BLOB back byte-for-byte ---"
P2_BIG_MD5=$(pond cat /imports/up/big.bin 2>/dev/null | md5sum | cut -d' ' -f1)
check '[ "'"$P2_BIG_MD5"'" = "'"$BIG_MD5"'" ]' \
    "consumer /imports/up/big.bin md5 matches producer (D7 streaming rebuild)"

# Control: the small inline sibling must also round-trip.
P2_SMALL_MD5=$(pond cat /imports/up/small.txt 2>/dev/null | md5sum | cut -d' ' -f1)
check '[ "'"$P2_SMALL_MD5"'" = "'"$SMALL_MD5"'" ]' \
    "consumer /imports/up/small.txt md5 matches producer"

echo "--- Step 6: consumer fsck validates the materialized blob content ---"
check 'POND="$P2" pond fsck >/dev/null 2>&1' \
    "consumer fsck passes (materialized blob hashes to its recorded blake3)"

check_finish
