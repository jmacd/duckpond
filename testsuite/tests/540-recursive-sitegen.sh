#!/bin/bash
# REQUIRES: compose
# EXPERIMENT: Recursive sitegen via cross-pond import
# DESCRIPTION:
#   Tests that a top-level sitegen with subsites: can recursively generate
#   a sub-site from an imported pond's own sitegen config.
#
#   Producer pond has:
#     - synthetic timeseries data
#     - temporal-reduce output
#     - a sitegen config with exports and routes
#     - page templates
#
#   Consumer imports the producer, installs a top-level sitegen with a
#   subsites: directive, and builds the combined site.
#
# EXPECTED:
#   - Top-level index.html generated
#   - Sub-site pages generated in subdirectory
#   - Sub-site data files exported
#   - Shared assets (style.css, chart.js) at top level
#   - Per-site theme.css in each directory
set -e

source /usr/local/bin/check.sh 2>/dev/null || source check.sh 2>/dev/null || true

echo "=== Experiment: Recursive Sitegen via Cross-Pond Import ==="
echo ""

# MinIO configuration
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="recursive-sitegen-test"

#############################
# CONFIGURE MINIO
#############################

echo "=== Configuring MinIO ==="
for i in $(seq 1 30); do
    if curl -s "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; then
        echo "MinIO ready"
        break
    fi
    sleep 1
done

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null || true
mc mb "local/${BUCKET_NAME}" 2>/dev/null || true

#############################
# POND1 -- PRODUCER
# Has data + sitegen config
#############################

echo ""
echo "=== Setting up Pond1 (Producer with data + sitegen) ==="

export POND=/pond1
pond init

PRODUCER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Producer pond_id: ${PRODUCER_POND_ID}"

# Create a dynamic-dir with synthetic timeseries
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

pond mknod dynamic-dir /sensors --config-path /tmp/sensors.yaml
echo "Producer sensors created"

# Create a temporal-reduce to make series files
cat > /tmp/reduce.yaml << 'YAML'
entries:
  - name: "temperature"
    factory: "temporal-reduce"
    config:
      source_patterns:
        - "/sensors/temperature"
      resolutions:
        - "1h"
      aggregation: "mean"
      timestamp_column: "timestamp"

  - name: "pressure"
    factory: "temporal-reduce"
    config:
      source_patterns:
        - "/sensors/pressure"
      resolutions:
        - "1h"
      aggregation: "mean"
      timestamp_column: "timestamp"
YAML

pond mknod dynamic-dir /reduced --config-path /tmp/reduce.yaml
echo "Producer reduce created"

# Verify the series are accessible
pond cat /reduced/temperature --format=table --sql "SELECT COUNT(*) AS rows FROM source" > /tmp/producer-temp.txt 2>&1
cat /tmp/producer-temp.txt
check 'grep -q "rows" /tmp/producer-temp.txt' "producer temperature has rows"

# Create site templates in the pond
pond mkdir -p /etc
pond mkdir -p /site

# Create producer index page template
cat > /tmp/producer-index.md << 'EOF'
---
title: Producer Site
layout: default
---

# Producer Monitoring

This is the producer's index page.
EOF
pond copy host:///tmp/producer-index.md /site/index.md

# Create producer data page template
cat > /tmp/producer-data.md << 'EOF'
---
title: "{{ $0 }}"
layout: data
---

# {{ $0 }}

{{ chart }}
EOF
pond copy host:///tmp/producer-data.md /site/data.md

# Create producer sidebar
cat > /tmp/producer-sidebar.md << 'EOF'
{{ content_nav }}
EOF
pond copy host:///tmp/producer-sidebar.md /site/sidebar.md

# Create producer sitegen config
cat > /tmp/producer-site.yaml << 'YAML'
factory: sitegen

site:
  title: "Producer Monitoring"
  base_url: "/"

exports:
  - name: "metrics"
    pattern: "/reduced/*"

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

pond mknod sitegen /etc/site.yaml --config-path /tmp/producer-site.yaml
echo "Producer sitegen created"

# Verify producer can build its own site standalone
POND=/pond1 pond run /etc/site.yaml build /tmp/producer-export
echo "Producer standalone build complete"

check '[ -f /tmp/producer-export/index.html ]' "producer standalone: index.html exists"
check '[ -f /tmp/producer-export/temperature.html ]' "producer standalone: temperature.html exists"
check '[ -f /tmp/producer-export/pressure.html ]' "producer standalone: pressure.html exists"
check '[ -d /tmp/producer-export/data ]' "producer standalone: data/ directory exists"

#############################
# PUSH BACKUP
#############################

echo ""
echo "=== Pushing producer backup to MinIO ==="

pond mkdir -p /system/run

cat > /tmp/producer-backup.yaml << EOF
url: "s3://${BUCKET_NAME}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
EOF

pond mknod remote /system/run/10-backup --config-path /tmp/producer-backup.yaml
pond run /system/run/10-backup push
echo "Push complete"

#############################
# POND2 -- CONSUMER
# Imports the producer, adds top-level sitegen with subsites
#############################

echo ""
echo "=== Setting up Pond2 (Consumer with subsites) ==="

export POND=/pond2
pond init

CONSUMER_POND_ID=$(pond config 2>/dev/null | grep "Pond ID" | awk '{print $NF}')
echo "Consumer pond_id: ${CONSUMER_POND_ID}"

pond mkdir -p /system/etc
pond mkdir -p /sources

# Import the producer's entire tree
cat > /tmp/import-config.yaml << EOF
url: "s3://${BUCKET_NAME}/pond-${PRODUCER_POND_ID}"
endpoint: "${MINIO_ENDPOINT}"
region: "us-east-1"
access_key_id: "${MINIO_ROOT_USER}"
secret_access_key: "${MINIO_ROOT_PASSWORD}"
allow_http: true
import:
  source_path: "/**"
  local_path: "/sources/producer"
EOF

pond mknod remote /system/etc/10-producer --config-path /tmp/import-config.yaml
echo "Import factory created"

pond run /system/etc/10-producer pull
echo "Import pull complete"

# Verify imported tree
echo ""
echo "=== Verify imported structure ==="
POND=/pond2 pond list '/sources/producer/*' > /tmp/import-listing.txt 2>&1 || true
cat /tmp/import-listing.txt

check 'grep -q "sensors" /tmp/import-listing.txt' "imported /sensors visible"
check 'grep -q "reduced" /tmp/import-listing.txt' "imported /reduced visible"
check 'grep -q "site" /tmp/import-listing.txt' "imported /site visible"

# Create consumer's portal page
cat > /tmp/portal-index.md << 'EOF'
---
title: Combined Dashboard
layout: default
---

# Combined Dashboard

Portal page with links to sub-sites.

- [Producer](/producer/)
EOF
pond copy host:///tmp/portal-index.md /system/site/index.md

cat > /tmp/portal-sidebar.md << 'EOF'
{{ content_nav }}
EOF
pond copy host:///tmp/portal-sidebar.md /system/site/sidebar.md

# Create consumer sitegen with subsites
cat > /tmp/consumer-site.yaml << 'YAML'
factory: sitegen

site:
  title: "Combined Dashboard"
  base_url: "/"

subsites:
  - name: "producer"
    path: "/sources/producer"
    config: "/etc/site.yaml"
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

pond mknod sitegen /system/etc/90-sitegen --config-path /tmp/consumer-site.yaml
echo "Consumer sitegen created"

#############################
# BUILD COMBINED SITE
#############################

echo ""
echo "=== Building combined site ==="

COMBINED_OUT=/tmp/combined-export
rm -rf "${COMBINED_OUT}"
mkdir -p "${COMBINED_OUT}"

POND=/pond2 pond run /system/etc/90-sitegen build "${COMBINED_OUT}"
echo "Combined build complete"

#############################
# VERIFY OUTPUT STRUCTURE
#############################

echo ""
echo "=== Verify combined output ==="

# Top-level assets
check '[ -f "${COMBINED_OUT}/index.html" ]' "top-level index.html exists"
check '[ -f "${COMBINED_OUT}/style.css" ]' "shared style.css at top level"
check '[ -f "${COMBINED_OUT}/chart.js" ]' "shared chart.js at top level"
check '[ -f "${COMBINED_OUT}/overlay.js" ]' "shared overlay.js at top level"
check '[ -f "${COMBINED_OUT}/theme.css" ]' "top-level theme.css exists"

# Sub-site pages
check '[ -f "${COMBINED_OUT}/producer/index.html" ]' "subsite index.html exists"
check '[ -f "${COMBINED_OUT}/producer/temperature.html" ]' "subsite temperature.html exists"
check '[ -f "${COMBINED_OUT}/producer/pressure.html" ]' "subsite pressure.html exists"
check '[ -f "${COMBINED_OUT}/producer/theme.css" ]' "subsite theme.css exists"

# Sub-site data
check '[ -d "${COMBINED_OUT}/producer/data" ]' "subsite data/ directory exists"

# Verify content
echo ""
echo "=== Verify HTML content ==="

# Top-level page should have the portal content
check 'grep -q "Combined Dashboard" "${COMBINED_OUT}/index.html"' "top-level has dashboard title"
check 'grep -q "style.css" "${COMBINED_OUT}/index.html"' "top-level references style.css"
check 'grep -q "theme.css" "${COMBINED_OUT}/index.html"' "top-level references theme.css"

# Sub-site page should have sub-site content
check 'grep -q "Producer Monitoring" "${COMBINED_OUT}/producer/index.html"' "subsite has producer title"
check 'grep -q "style.css" "${COMBINED_OUT}/producer/index.html"' "subsite references style.css"
check 'grep -q "theme.css" "${COMBINED_OUT}/producer/index.html"' "subsite references theme.css"

# Verify per-site theme CSS
check 'grep -q "1a365d" "${COMBINED_OUT}/theme.css"' "top-level theme has steel blue accent"
check 'grep -q "2d5016" "${COMBINED_OUT}/producer/theme.css"' "producer theme has green accent"

# No shared assets duplicated in subsite
check '[ ! -f "${COMBINED_OUT}/producer/style.css" ]' "style.css not duplicated in subsite"
check '[ ! -f "${COMBINED_OUT}/producer/chart.js" ]' "chart.js not duplicated in subsite"

echo ""
echo "=== Output tree ==="
find "${COMBINED_OUT}" -type f | sort | head -30

#############################
# RESULTS
#############################

check_finish
