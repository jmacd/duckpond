#!/bin/bash
# EXPERIMENT: `pond verify` reports the commit-graph relationship between the
#             local tip and a remote's published tip (UpToDate / RemoteBehind).
#
# DESCRIPTION:
#   The content-addressed remote holds one tip ref per pond; `pond verify`
#   walks the local commit spine to place the remote tip and reports:
#     * UpToDate     -- remote tip == local tip.
#     * RemoteBehind -- remote tip is an ANCESTOR of the local tip (local has
#                       unpushed commits); reports the unpushed count.
#     * Diverged     -- remote tip is not in local history (real mismatch).
#
#   712 pushes a compaction commit and then asserts verify is clean (UpToDate),
#   but it never observes the RemoteBehind state.  RemoteBehind is awkward to
#   reach because a normal write auto-pushes to backup remotes -- EXCEPT
#   `pond maintain --compact`, which advances the local tip via a Compact
#   commit WITHOUT triggering the post-commit auto-push (this is exactly the
#   bug fixed on jmacd/65: "pond push after maintain --compact failed with no
#   commit spine recorded").  This test drives that window: after compaction
#   the local tip is ahead of the remote by one commit, so verify must report
#   RemoteBehind by 1; a follow-up `pond push` restores UpToDate.
#
#   This exercises `content_verify::find_in_local_history` (the ancestor walk)
#   and the `local_unpushed` count end-to-end from the CLI, which the Rust unit
#   tests cover but no testsuite case did.
#
# EXPECTED:
#   - After backup add (auto-push): verify reports UpToDate.
#   - After maintain --compact (no push): verify reports RemoteBehind by 1.
#   - After push: verify reports UpToDate again.
#
# History:
#   Added on jmacd/65 to cover the RemoteBehind verify state over file://.
set -e
source check.sh

echo "=== Experiment: pond verify commit-graph states (UpToDate / RemoteBehind) ==="

P1=/tmp/725-p1
REMOTE=/tmp/725-remote
rm -rf "$P1" "$REMOTE"
mkdir -p "$REMOTE"

echo "--- Step 1: producer ingests data + attaches a backup (auto-push) ---"
export POND="$P1"
pond init --birthplace test-host >/dev/null
pond mkdir /data >/dev/null
for i in 1 2 3 4; do
    printf 'verify-row-%d\n' "$i" > /tmp/725-f$i.txt
    pond copy host:///tmp/725-f$i.txt /data/f$i.txt >/dev/null 2>&1
done
pond backup add origin "file://${REMOTE}" > /tmp/725-backup.log 2>&1
check 'grep -q "added remote origin" /tmp/725-backup.log' "backup add origin succeeded"

echo "--- Step 2: verify is UpToDate right after auto-push ---"
pond verify origin > /tmp/725-verify1.log 2>&1
check_contains /tmp/725-verify1.log "verify clean after auto-push (UpToDate)" \
    "live data matches remote tip"

echo "--- Step 3: maintain --compact advances the local tip WITHOUT pushing ---"
pond maintain --compact > /tmp/725-compact.log 2>&1
check_contains /tmp/725-compact.log "compaction recorded as a commit" "Compaction committed"

echo "--- Step 4: verify now reports RemoteBehind (local ahead by 1 commit) ---"
set +e
pond verify origin > /tmp/725-verify2.log 2>&1
VERIFY2_EXIT=$?
set -e
# verify exits non-zero on any non-ok state, but RemoteBehind is a healthy
# "you have unpushed work" report (report.ok == true), so it must exit 0.
check '[ '"$VERIFY2_EXIT"' -eq 0 ]' "verify exit is success for the RemoteBehind (healthy lag) state"
check_contains /tmp/725-verify2.log "verify reports the remote is behind local" "is behind local by"
check_contains /tmp/725-verify2.log "verify reports exactly one unpushed commit" "behind local by 1 commit"

echo "--- Step 5: push the compaction; verify returns to UpToDate ---"
pond push origin > /tmp/725-push.log 2>&1
check 'grep -qE "push origin complete" /tmp/725-push.log' "compact push completed"
pond verify origin > /tmp/725-verify3.log 2>&1
check_contains /tmp/725-verify3.log "verify clean again after pushing the compaction" \
    "live data matches remote tip"

check_finish
