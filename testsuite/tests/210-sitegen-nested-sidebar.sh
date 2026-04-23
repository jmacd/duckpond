#!/bin/bash
# EXPERIMENT: Sitegen -- nested sidebar children (recursive sub-navigation)
# EXPECTED: A SidebarChild may itself have `children`, producing a recursive
#           sub-sub-nav. A parent-only child (no href, only children) renders
#           as a nav-heading linking to the first descendant leaf. Active
#           state propagates up through multiple levels of nesting so that
#           all ancestor subnavs expand when a deeply-nested page is active.
set -e
source check.sh

echo "=== Experiment: Nested Sidebar Children ==="

SITE_ROOT="/tmp/host-nested-sidebar"
OUTDIR="${OUTPUT:-/output}"

rm -rf "${SITE_ROOT}"
rm -rf "${OUTDIR:?}"/* 2>/dev/null || true
mkdir -p "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Create a site with a three-level-deep sidebar structure
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 1: Create site structure ---"

mkdir -p "${SITE_ROOT}/site"

cat > "${SITE_ROOT}/site/index.md" << 'MD'
---
title: Home
layout: default
---

# Home
MD

cat > "${SITE_ROOT}/site/page.md" << 'MD'
---
title: "{{ $0 }}"
layout: page
---

# {{ $0 }}
MD

cat > "${SITE_ROOT}/site/sidebar.md" << 'MD'
{{ content_nav /}}
MD

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Build site with nested sidebar config
#         Monitoring > Septic > {Pump Amps, Cycle Times}
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Step 2: Build site ---"

cat > "${SITE_ROOT}/site.yaml" << 'YAML'

site:
  title: "Nested Test"
  base_url: "/"

exports: []

routes:
  - name: "home"
    type: static
    slug: ""
    page: "/site/index.md"
  - name: "well"
    type: static
    slug: "data/well"
    page: "/site/page.md"
  - name: "pumps"
    type: static
    slug: "septic/pumps"
    page: "/site/page.md"
  - name: "cycles"
    type: static
    slug: "septic/cycles"
    page: "/site/page.md"

partials:
  sidebar: "/site/sidebar.md"

sidebar:
  - label: "Monitoring"
    children:
      - label: "Well Depth"
        href: "/data/well/index.html"
      - label: "Septic"
        children:
          - label: "Pump Amps"
            href: "/septic/pumps/index.html"
          - label: "Cycle Times"
            href: "/septic/cycles/index.html"
YAML

pond run -d "${SITE_ROOT}" host+sitegen:///site.yaml build "${OUTDIR}"

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Verify nested sidebar structure on home page
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Home page (nothing active) ---"

check '[ -f "${OUTDIR}/index.html" ]'      "index.html exists"

# Top-level and all three levels visible
check_contains     "${OUTDIR}/index.html"  "Monitoring label present"      '>Monitoring<'
check_contains     "${OUTDIR}/index.html"  "Well Depth child present"      'Well Depth'
check_contains     "${OUTDIR}/index.html"  "Septic nested parent present"  '>Septic<'
check_contains     "${OUTDIR}/index.html"  "Pump Amps grandchild present"  'Pump Amps'
check_contains     "${OUTDIR}/index.html"  "Cycle Times grandchild"        'Cycle Times'

# Septic is a parent-only child (no href): must render as nav-heading linked
# to its first descendant leaf (/septic/pumps/index.html).
check_contains     "${OUTDIR}/index.html"  "Septic links to first leaf" \
    'href="/septic/pumps/index.html" class="nav-heading">Septic<'

# No section is active on the home page -- nothing should be expanded.
check_not_contains "${OUTDIR}/index.html"  "no subnav expanded on home"    'subnav expanded'

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Verify active-state propagation to the deeply nested page
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Deep page (Pump Amps active) ---"

check '[ -f "${OUTDIR}/septic/pumps/index.html" ]'  "deep page generated"

# On the Pump Amps page, BOTH levels of subnav should be expanded:
# the outer Monitoring subnav AND the inner Septic subnav.
PUMPS_HTML="${OUTDIR}/septic/pumps/index.html"

# Count expanded subnavs -- must be at least 2 (Monitoring + Septic)
EXPANDED_COUNT=$(grep -c 'subnav expanded' "${PUMPS_HTML}" || true)
check '[ "${EXPANDED_COUNT}" -ge 2 ]'   "two levels of subnav expanded"

# Pump Amps itself must be marked active with aria-current
check_contains     "${PUMPS_HTML}"       "leaf marked active"  \
    '<li class="active"><a href="/septic/pumps/index.html" aria-current="page">Pump Amps</a>'

# Septic (parent-only) should also be active because a descendant is active
check_contains     "${PUMPS_HTML}"       "parent-only child active" \
    '<li class="active"><a href="/septic/pumps/index.html" class="nav-heading">Septic</a>'

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Sibling leaf at deeper level should NOT mark Pump Amps active
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Sibling page (Cycle Times active) ---"

CYCLES_HTML="${OUTDIR}/septic/cycles/index.html"
check '[ -f "${CYCLES_HTML}" ]'         "cycles page generated"

check_contains     "${CYCLES_HTML}"     "Cycle Times marked active" \
    '<li class="active"><a href="/septic/cycles/index.html" aria-current="page">Cycle Times</a>'
check_not_contains "${CYCLES_HTML}"     "Pump Amps not active here" \
    '<li class="active"><a href="/septic/pumps/index.html" aria-current="page">Pump Amps</a>'

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: A sibling under Monitoring (Well Depth) does NOT expand Septic
# ══════════════════════════════════════════════════════════════════════════════

echo ""
echo "--- Verification: Sibling branch (Well Depth active) ---"

WELL_HTML="${OUTDIR}/data/well/index.html"
check '[ -f "${WELL_HTML}" ]'           "well page generated"

# On Well Depth page, Monitoring subnav expands but Septic subnav does not.
WELL_EXPANDED=$(grep -c 'subnav expanded' "${WELL_HTML}" || true)
check '[ "${WELL_EXPANDED}" -eq 1 ]'   "only outer subnav expanded"
check_not_contains "${WELL_HTML}"       "Septic not active on sibling branch" \
    '<li class="active"><a href="/septic/pumps/index.html" class="nav-heading">Septic</a>'

check_finish
