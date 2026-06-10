#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Recursive sitegen via cross-pond import (D5.4 + D5.7b)
#
# DESCRIPTION:
#   A consumer pond cross-pond-imports a producer pond.  The producer
#   carries its own sitegen factory config and templates.  The
#   consumer's top-level sitegen references the producer's sitegen
#   as a `subsites:` entry, so the combined `pond run /etc/site
#   build` produces:
#     - top-level pages from the consumer's config
#     - sub-site pages from the producer's config, rendered under a
#       sub-directory (e.g. /producer/)
#     - shared build assets (style.css, chart.js, overlay.js) at the
#       top level ONLY (not duplicated per subsite)
#     - per-site `theme.css` overrides at top level AND in each subsite
#
#   This is the build-time analog of cross-pond import: the producer
#   author writes a self-contained sitegen YAML; an aggregator
#   composes multiple producers' sites into one combined deploy.
#
# TEST SHAPE:
#   Pond A:  init, synthetic timeseries + temporal-reduce + sitegen
#            config + page templates.  Standalone build succeeds.
#            backup add + push.
#   Pond B:  init, remote add upstream URL /sources/producer + pull.
#            Verify imported tree under /sources/producer/* matches
#            producer.  Install top-level sitegen with subsites:
#            entry pointing at /sources/producer.  pond run build.
#   Verify:  Top-level outputs, subsite outputs, shared assets, themes.
#
# EXPECTED:
#   - Producer standalone sitegen build still works.
#   - After cross-pond pull, /sources/producer/etc/site is readable as
#     the sub-site's sitegen config.
#   - Combined build emits both top-level pages and subsite pages, with
#     shared assets at top level and per-site theme overrides.
#
# History:
#   Renumbered from 540-recursive-sitegen.sh in D5.8.7 to resolve a
#   numeric prefix collision with the new 540 watermark cluster.
#   Revived D5.8.8 from a DISABLED-D4 stub that used the deleted
#   `remote` factory; D5.7b cross-pond plumbing (`pond backup add` +
#   `pond push` on the producer, `pond remote add NAME URL MOUNT_PATH`
#   + `pond pull` on the consumer) replaces the legacy import path.
set -e

echo "=== Experiment: Recursive sitegen via cross-pond import ==="
echo ""

#############################
# CONFIGURATION
#############################

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
BUCKET_NAME="cross-pond-550"

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

#############################
# MINIO READINESS
#############################

echo "=== Checking MinIO availability ==="
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    echo "[FAIL] MinIO not reachable at ${MINIO_ENDPOINT}"
    exit 1
fi

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1
mc rb --force "local/${BUCKET_NAME}" >/dev/null 2>&1 || true
mc mb "local/${BUCKET_NAME}" >/dev/null 2>&1

#############################
# POND A — PRODUCER
#############################

echo ""
echo "=== Phase 1: Pond A — data + sitegen factory + standalone build ==="
export POND=/tmp/pond-a-550
rm -rf "${POND}"
pond init >/dev/null

# Synthetic sensors (parents of dynamic-dir)
cat > /tmp/sensors.yaml << 'YAML'
entries:
  - name: "temperature"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-10T00:00:00Z"
      interval: "1h"
      points:
        - name: "value"
          components:
            - type: sine
              amplitude: 5.0
              period: "24h"
              offset: 20.0

  - name: "pressure"
    factory: "synthetic-timeseries"
    config:
      start: "2024-01-01T00:00:00Z"
      end:   "2024-01-10T00:00:00Z"
      interval: "1h"
      points:
        - name: "value"
          components:
            - type: sine
              amplitude: 3.0
              period: "12h"
              offset: 1013.0
YAML
pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml >/dev/null

# Temporal-reduce wraps the raw series into queryable .series files
cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "single_param"
    factory: "temporal-reduce"
    config:
      in_pattern: "series:///sensors/*"
      out_pattern: "$0"
      time_column: "timestamp"
      resolutions:
        - "1h"
      aggregations:
        - type: "avg"
          columns: ["*"]
YAML
pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml >/dev/null

# Sanity: the producer's own reduced output is queryable.
pond cat /reduced/single_param/temperature/res=1h.series \
    --format=table \
    --sql "SELECT COUNT(*) AS rows FROM source" > /tmp/producer-temp.txt 2>&1
check "producer: /reduced/.../temperature is queryable" \
    "grep -q rows /tmp/producer-temp.txt"

# Page templates + sitegen factory config
pond mkdir -p /etc  >/dev/null
pond mkdir -p /site >/dev/null

cat > /tmp/producer-index.md << 'EOF'
---
title: Producer Site
layout: default
---

# Producer Monitoring

This is the producer's index page.
EOF
pond copy host:///tmp/producer-index.md /site/index.md >/dev/null

cat > /tmp/producer-data.md << 'EOF'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ chart /}}
EOF
pond copy host:///tmp/producer-data.md /site/data.md >/dev/null

cat > /tmp/producer-sidebar.md << 'EOF'
{{ content_nav /}}
EOF
pond copy host:///tmp/producer-sidebar.md /site/sidebar.md >/dev/null

cat > /tmp/producer-site.yaml << 'YAML'
site:
  title: "Producer Monitoring"
  base_url: "/"

exports:
  - name: "metrics"
    pattern: "/reduced/single_param/*/*.series"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
    routes:
      - name: "detail"
        type: template
        slug: "$0"
        page: "/site/data.md"
        export: "metrics"

partials:
  sidebar: "/site/sidebar.md"

sidebar:
  - label: "Home"
    href: "/"
  - label: "Metrics"
    children:
      - label: "Temperature"
        href: "/temperature.html"
      - label: "Pressure"
        href: "/pressure.html"

theme:
  accent: "#2d5016"
  accent-light: "#4a7c2e"
YAML
pond mknod sitegen /etc/site --config-path /tmp/producer-site.yaml >/dev/null

# Producer standalone build (sanity, before cross-pond)
rm -rf /tmp/producer-export
pond run /etc/site build /tmp/producer-export 2>&1 | tail -3
check "producer standalone: index.html exists" \
    "[ -f /tmp/producer-export/index.html ]"
check "producer standalone: temperature.html exists" \
    "[ -f /tmp/producer-export/temperature.html ]"
check "producer standalone: pressure.html exists" \
    "[ -f /tmp/producer-export/pressure.html ]"

# Backup the producer to MinIO
pond backup add origin "s3://${BUCKET_NAME}" \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond push origin >/dev/null

#############################
# POND B — CONSUMER (recursive sitegen)
#############################

echo ""
echo "=== Phase 2: Pond B — attach producer, install top-level sitegen ==="
export POND=/tmp/pond-b-550
rm -rf "${POND}"
pond init >/dev/null

pond remote add upstream "s3://${BUCKET_NAME}" /sources/producer \
    --region us-east-1 \
    --endpoint "${MINIO_ENDPOINT}" \
    --access-key-id "${MINIO_ROOT_USER}" \
    --secret-access-key '${env:MINIO_ROOT_PASSWORD}' \
    --allow-http \
    --overwrite >/dev/null
pond pull upstream 2>&1 | tail -3

# Verify imported tree.
pond list /sources/producer/ > /tmp/import-listing.txt 2>&1 || true
check "consumer: imported /sources/producer/sensors visible" \
    "grep -qw sensors /tmp/import-listing.txt"
check "consumer: imported /sources/producer/reduced visible" \
    "grep -qw reduced /tmp/import-listing.txt"
check "consumer: imported /sources/producer/site visible" \
    "grep -qw site /tmp/import-listing.txt"
check "consumer: imported /sources/producer/etc/site (sitegen factory) visible" \
    "pond list /sources/producer/etc/ 2>/dev/null | grep -qw site"

# Consumer's portal page + sidebar
pond mkdir -p /system/etc  >/dev/null
pond mkdir -p /system/site >/dev/null

cat > /tmp/portal-index.md << 'EOF'
---
title: Combined Dashboard
layout: default
---

# Combined Dashboard

Portal page with links to sub-sites.

- [Producer](/producer/)
EOF
pond copy host:///tmp/portal-index.md /system/site/index.md >/dev/null

cat > /tmp/portal-sidebar.md << 'EOF'
{{ content_nav /}}
EOF
pond copy host:///tmp/portal-sidebar.md /system/site/sidebar.md >/dev/null

cat > /tmp/consumer-site.yaml << 'YAML'
site:
  title: "Combined Dashboard"
  base_url: "/"

subsites:
  - name: "producer"
    path: "/sources/producer"
    config: "/etc/site"
    base_url: "/producer/"

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/system/site/index.md"

partials:
  sidebar: "/system/site/sidebar.md"

sidebar:
  - label: "Home"
    href: "/"
  - label: "Producer"
    href: "/producer/"

theme:
  accent: "#1a365d"
YAML
pond mknod sitegen /system/etc/90-sitegen --config-path /tmp/consumer-site.yaml >/dev/null

#############################
# BUILD COMBINED SITE
#############################

echo ""
echo "=== Phase 3: Combined build ==="
COMBINED_OUT=/tmp/combined-export-550
rm -rf "${COMBINED_OUT}"
mkdir -p "${COMBINED_OUT}"
pond run /system/etc/90-sitegen build "${COMBINED_OUT}" 2>&1 | tail -5

#############################
# VERIFY OUTPUT STRUCTURE
#############################

echo ""
echo "=== Phase 4: Verify combined output ==="

# Top-level pages + assets
check "top: index.html exists" \
    "[ -f \"${COMBINED_OUT}/index.html\" ]"
check "top: shared style.css at top level" \
    "[ -f \"${COMBINED_OUT}/style.css\" ]"
check "top: shared chart.js at top level" \
    "[ -f \"${COMBINED_OUT}/chart.js\" ]"
check "top: theme.css exists" \
    "[ -f \"${COMBINED_OUT}/theme.css\" ]"

# Sub-site pages
check "subsite: producer/index.html exists" \
    "[ -f \"${COMBINED_OUT}/producer/index.html\" ]"
check "subsite: producer/temperature.html exists" \
    "[ -f \"${COMBINED_OUT}/producer/temperature.html\" ]"
check "subsite: producer/pressure.html exists" \
    "[ -f \"${COMBINED_OUT}/producer/pressure.html\" ]"
check "subsite: producer/theme.css exists" \
    "[ -f \"${COMBINED_OUT}/producer/theme.css\" ]"

# Content correctness
check "top: index references the dashboard title" \
    "grep -q 'Combined Dashboard' \"${COMBINED_OUT}/index.html\""
check "subsite: index references the producer title" \
    "grep -q 'Producer Monitoring' \"${COMBINED_OUT}/producer/index.html\""

# Per-site theme accents
check "top: theme has consumer accent (#1a365d)" \
    "grep -q '1a365d' \"${COMBINED_OUT}/theme.css\""
check "subsite: theme has producer accent (#2d5016)" \
    "grep -q '2d5016' \"${COMBINED_OUT}/producer/theme.css\""

# Shared assets NOT duplicated under subsite
check "no dup: producer/style.css absent" \
    "[ ! -f \"${COMBINED_OUT}/producer/style.css\" ]"
check "no dup: producer/chart.js absent" \
    "[ ! -f \"${COMBINED_OUT}/producer/chart.js\" ]"

echo ""
echo "=== Output tree (top 30) ==="
find "${COMBINED_OUT}" -type f | sort | head -30

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
