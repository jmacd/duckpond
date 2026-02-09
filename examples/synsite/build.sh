#!/bin/bash
# build.sh — End-to-end synthetic example: build pond + export site
#
# This script exercises the full DuckPond pipeline:
#   synthetic-timeseries → timeseries-join → timeseries-pivot
#   → temporal-reduce → template → pond export
#
# Usage:
#   ./build.sh                    # uses $POND or /pond, and `pond` from PATH
#   POND=/tmp/mypond ./build.sh   # custom pond location
#
# Output: ./dist/ directory with HTML + Parquet files, ready to serve
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="${SCRIPT_DIR}/dist"

echo "=== DuckPond Synthetic Example Build ==="
echo "POND=${POND}"
echo "Output: ${OUTDIR}"
echo ""

# ── 1. Initialize pond ──────────────────────────────────────────────────────

echo "--- Step 1: Initialize pond ---"
pond init
echo "✓ pond initialized"

# ── 2. Create directory structure ─────────────────────────────────────────────

echo ""
echo "--- Step 2: Create directories ---"
pond mkdir /etc
echo "✓ directories created"

# ── 3. Copy templates and static assets into pond ─────────────────────────────

echo ""
echo "--- Step 3: Load templates into pond ---"
pond copy "host://${SCRIPT_DIR}/templates/data.html.tmpl"  /etc/data.html.tmpl
pond copy "host://${SCRIPT_DIR}/templates/index.html.tmpl" /etc/index.html.tmpl
pond copy "host://${SCRIPT_DIR}/templates/page.html.tmpl"  /etc/page.html.tmpl
echo "✓ templates loaded"

# ── 4. Create synthetic sensors (dynamic-dir with join + pivot) ───────────────

echo ""
echo "--- Step 4: Create synthetic sensors + joins + pivots ---"
pond mknod dynamic-dir /sensors --config-path "${SCRIPT_DIR}/configs/sensors.yaml"
echo "✓ /sensors created"

echo ""
echo "Sensor listing:"
pond list /sensors

# Verify a synthetic source is queryable
echo ""
echo "NorthDock temperature sample (first 3 rows):"
pond cat /sensors/north_temperature --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 3
"

# ── 5. Create per-site joined views in /combined ──────────────────────────────

echo ""
echo "--- Step 5: Link joined views to /combined ---"
# The joins are already in /sensors/ (NorthDock, SouthDock).
# We need them also accessible at /combined/* for the reduce and pivot inputs.
# Use symbolic references: copy the join nodes to /combined/
pond mknod dynamic-dir /combined --config-path <(cat << 'YAML'
entries:
  - name: "NorthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/north_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/north_do"
          scope: DOProbe

  - name: "SouthDock"
    factory: "timeseries-join"
    config:
      inputs:
        - pattern: "series:///sensors/south_temperature"
          scope: TempProbe
        - pattern: "series:///sensors/south_do"
          scope: DOProbe
YAML
)
echo "✓ /combined created"

echo ""
echo "Combined listing:"
pond list /combined

echo ""
echo "NorthDock combined schema:"
pond describe /combined/NorthDock

# ── 6. Create per-param pivoted views in /singled ─────────────────────────────

echo ""
echo "--- Step 6: Create pivoted per-param views ---"
pond mknod dynamic-dir /singled --config-path <(cat << 'YAML'
entries:
  - name: "Temperature"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "TempProbe.temperature.C"

  - name: "DO"
    factory: "timeseries-pivot"
    config:
      pattern: "series:///combined/*"
      columns:
        - "DOProbe.do.mgL"
YAML
)
echo "✓ /singled created"

echo ""
echo "Singled listing:"
pond list /singled

echo ""
echo "Temperature pivot schema:"
pond describe /singled/Temperature

echo ""
echo "Temperature pivot sample (first 3 rows):"
pond cat /singled/Temperature --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 3
"

# ── 7. Create temporal reduce ─────────────────────────────────────────────────

echo ""
echo "--- Step 7: Create temporal reduce (1h avg/min/max) ---"
pond mknod dynamic-dir /reduced --config-path "${SCRIPT_DIR}/configs/reduce.yaml"
echo "✓ /reduced created"

echo ""
echo "Reduced listing:"
pond list /reduced/**

echo ""
echo "Reduced single_param/Temperature sample:"
pond cat /reduced/single_param/Temperature/res=1h.series --sql "
  SELECT * FROM source ORDER BY timestamp LIMIT 3
"

# ── 8. Create template factory ────────────────────────────────────────────────

echo ""
echo "--- Step 8: Create templates ---"
pond mknod dynamic-dir /templates --config-path "${SCRIPT_DIR}/configs/template.yaml"
echo "✓ /templates created"

echo ""
echo "Template listing:"
pond list /templates/**

# ── 9. Export everything to dist/ ─────────────────────────────────────────────

echo ""
echo "--- Step 9: Export to ${OUTDIR} ---"
rm -rf "${OUTDIR}"

# Export per-param data (parquet + HTML)
pond export \
  --pattern '/reduced/single_param/*/*.series' \
  --pattern '/templates/params/*' \
  --dir "${OUTDIR}" \
  --temporal "year,month"

# Export per-site data (parquet + HTML)
pond export \
  --pattern '/reduced/single_site/*/*.series' \
  --pattern '/templates/sites/*' \
  --dir "${OUTDIR}" \
  --temporal "year,month"

# Export index page
pond export \
  --pattern '/templates/index/*' \
  --dir "${OUTDIR}"

echo "✓ export complete"

# ── 10. Verify output ────────────────────────────────────────────────────────

echo ""
echo "=== Verification ==="
echo ""
echo "Output directory structure:"
find "${OUTDIR}" -type f | head -40
echo ""
echo "File counts:"
echo "  HTML files: $(find "${OUTDIR}" -name '*.html' | wc -l)"
echo "  Parquet files: $(find "${OUTDIR}" -name '*.parquet' | wc -l)"
echo ""

# Check expected files exist
for f in index.html; do
  if [ -f "${OUTDIR}/${f}" ]; then
    echo "  ✓ ${f}"
  else
    echo "  ✗ MISSING: ${f}"
    exit 1
  fi
done

for name in Temperature DO; do
  if [ -f "${OUTDIR}/params/${name}.html" ] || [ -f "${OUTDIR}/${name}.html" ]; then
    echo "  ✓ param page: ${name}"
  else
    echo "  ✗ MISSING param page: ${name}"
  fi
done

for name in NorthDock SouthDock; do
  if [ -f "${OUTDIR}/sites/${name}.html" ] || [ -f "${OUTDIR}/${name}.html" ]; then
    echo "  ✓ site page: ${name}"
  else
    echo "  ✗ MISSING site page: ${name}"
  fi
done

echo ""
echo "=== Build Complete ==="
echo ""
echo "To view the site:"
echo "  npx serve ${OUTDIR}"
echo "  # or: python3 -m http.server -d ${OUTDIR} 8080"
