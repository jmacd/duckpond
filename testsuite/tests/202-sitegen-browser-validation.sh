#!/bin/bash
# EXPERIMENT: Sitegen browser validation — Puppeteer checks on rendered site
# EXPECTED: All pages load, styles applied, sidebar links work, no JS errors
#
# This test delegates to testsuite/browser/test.sh which:
#   1. Generates the site via test 201 (Docker)
#   2. Serves it with Vite
#   3. Validates with Puppeteer
#   4. Cleans up
#
# For manual inspection instead, run:
#   cd testsuite/browser && ./serve.sh
#
# Prerequisites: Node.js >=18, Docker
# First-time:    cd testsuite/browser && npm install
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BROWSER_DIR="$(cd "${SCRIPT_DIR}/../browser" && pwd)"

exec bash "${BROWSER_DIR}/test.sh" "$@"
