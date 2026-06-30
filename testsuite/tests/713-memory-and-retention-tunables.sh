#!/bin/bash
# EXPERIMENT: Operator-tunable memory pool + delta-log retention
#
# DESCRIPTION:
#   Confirms the delta-log retention knobs added for low-RAM (BeaglePlay 2GB)
#   operation persist via `pond config set` and survive across opens
#   (maintenance.data_log_retention_minutes / control_log_retention_minutes).
#   The companion POND_MEMORY_LIMIT_MB pool knob is covered by unit-level smoke.
#   Both are documented in docs/configurable-settings.md.
#
# EXPECTED:
#   - Both retention settings round-trip through `pond config`.
#   - maintain still succeeds with the data retention set.
set -e
source check.sh

P=/tmp/713-pond
rm -rf "$P"
export POND="$P"

pond init --birthplace test-host >/dev/null 2>&1

echo "--- retention settings persist ---"
pond config set maintenance.data_log_retention_minutes 1440 >/dev/null 2>&1
pond config set maintenance.control_log_retention_minutes 1 >/dev/null 2>&1
pond config > /tmp/713-config.log 2>&1
check_contains /tmp/713-config.log "data retention persisted" "maintenance.data_log_retention_minutes: 1440"
check_contains /tmp/713-config.log "control retention persisted" "maintenance.control_log_retention_minutes: 1"

echo "--- maintain succeeds with data retention set ---"
pond maintain > /tmp/713-maintain.log 2>&1
check "test $? -eq 0" "maintain ran cleanly"

check_finish
