#!/bin/bash
# EXPERIMENT: Sitegen browser validation — Puppeteer checks on rendered site
# EXPECTED: All pages load, styles applied, sidebar links work, no JS errors
# REQUIRES: host
#
# This test delegates to testsuite/browser/test.sh which:
#   1. Generates the site via test 201 (Docker)
#   2. Serves it with Vite
#   3. Validates with Puppeteer
#   4. Cleans up
#
# run-all.sh detects '# REQUIRES: host' and runs this directly (not in Docker).
#
# For manual inspection instead, run:
#   cd testsuite/browser && SITE_ROOT=/tmp/test-output npm run serve
#
# Prerequisites: Node.js >=18, Docker
# First-time:    cd testsuite/browser && npm install
set -e

# Safety net: if somehow run inside Docker, fail loudly.
if [[ -f /.dockerenv ]] || [[ "$(dirname "${BASH_SOURCE[0]}")" == "/test" ]]; then
    echo "ERROR: Test 202 requires a browser and cannot run inside Docker."
    echo "Use run-all.sh (which detects REQUIRES: host) or run directly:"
    echo "  bash testsuite/tests/202-sitegen-browser-validation.sh"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROWSER_DIR="$(cd "${SCRIPT_DIR}/../browser" && pwd)"

exec bash "${BROWSER_DIR}/test.sh" "$@"
