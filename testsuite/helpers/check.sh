#!/bin/bash
# Shared test assertion helpers for DuckPond testsuite.
# Source this at the top of your test:  source check.sh
#
# Usage:
#   check 'grep -q "hello" /tmp/out.txt'  "output contains hello"
#   check_contains /tmp/out.html          "page has nav"  'class="nav"'
#   check_not_contains /tmp/out.html      "no debug output"  'DEBUG:'
#
# At the end of your test:
#   check_finish
#
# The runner (run-test.sh) counts "  ✓" and "  ✗" lines from the log
# to produce the summary.  These functions produce that format.

_CHECK_PASS=0
_CHECK_FAIL=0

# check COMMAND DESCRIPTION
#   Runs COMMAND (via eval); prints ✓ or ✗ with DESCRIPTION.
check() {
  if eval "$1" > /dev/null 2>&1; then
    echo "  ✓ $2"
    _CHECK_PASS=$((_CHECK_PASS + 1))
  else
    echo "  ✗ $2"
    _CHECK_FAIL=$((_CHECK_FAIL + 1))
  fi
}

# check_contains FILE DESCRIPTION PATTERN
#   Passes if PATTERN (fixed string) appears in FILE.
check_contains() {
  if grep -qF "$3" "$1" 2>/dev/null; then
    echo "  ✓ $2"
    _CHECK_PASS=$((_CHECK_PASS + 1))
  else
    echo "  ✗ $2 — expected: '$3'"
    _CHECK_FAIL=$((_CHECK_FAIL + 1))
  fi
}

# check_not_contains FILE DESCRIPTION PATTERN
#   Passes if PATTERN (fixed string) does NOT appear in FILE.
check_not_contains() {
  if grep -qF "$3" "$1" 2>/dev/null; then
    echo "  ✗ $2 — found unwanted: '$3'"
    _CHECK_FAIL=$((_CHECK_FAIL + 1))
  else
    echo "  ✓ $2"
    _CHECK_PASS=$((_CHECK_PASS + 1))
  fi
}

# check_finish
#   Prints results summary and exits non-zero if any check failed.
#   Call this as the LAST line of your test.
check_finish() {
  echo ""
  echo "=== Results: ${_CHECK_PASS} passed, ${_CHECK_FAIL} failed ==="
  if [ "${_CHECK_FAIL}" -gt 0 ]; then
    exit 1
  fi
}
