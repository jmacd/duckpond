# Lazy Parquet Loading & Data-Driven Resolution Picker

## Summary

This document describes the changes made to the sitegen chart viewer
(`chart.js`) and the supporting test infrastructure to enforce a key
design invariant:

> **For any view window, at most 2 parquet files are loaded.**

Sitegen exports time-series data as Hive-partitioned parquet files,
organized by resolution (`res=1h`, `res=6h`, `res=1d`, …) and temporal
partition (`year=2025`, `year=2025/quarter=Q1`, etc.).  The temporal
partition size is automatically computed per resolution from the
`target_points` setting so that each resolution tier tiles the time axis
with partitions wide enough that a typical screen view spans at most one
partition boundary — meaning at most 2 files are needed per render.

Previously, `chart.js` loaded **all** parquet files for **all**
resolutions eagerly at page load.  A site with 5 resolutions and monthly
partitions over a year could load 60+ files before the chart even
appeared.  The changes below replace that with demand-driven loading
that respects the tiling design.

---

## Problem Statement

### Before (eager loading)

```
Page load → fetch ALL parquet files → register ALL in DuckDB-WASM → query
```

For the test-201 site (1 year, 5 resolutions, monthly partitions), this
meant ~60 `.parquet` fetches on every page load — most of which were
never queried.  In production with longer time spans and finer
resolutions, the number grows proportionally.

### Design intent

The sitegen export pipeline creates multiple resolution tiers
specifically so that the client can pick the right one for the current
zoom level.  Each tier is temporally partitioned so that:

- The **finest** resolution (e.g., `1h`) has many small files — useful
  for narrow zoom windows.
- The **coarsest** resolution (e.g., `24h`) has few large files —
  useful for wide "show me everything" views.
- For any view width, there exists at least one resolution where **at
  most 2 files** cover the visible time range.

The client should load only those 2 files.

---

## Changes Made

### 1. Lazy file loading (`ensureFile()`)

**File:** `crates/sitegen/assets/chart.js`

Replaced the eager "fetch all files at startup" pattern with a lazy
cache:

```javascript
const registeredNames = new Map();  // url → DuckDB name (or null on failure)

async function ensureFile(url) {
  if (registeredNames.has(url)) return registeredNames.get(url);
  const duckdbName = `f${fileIdx++}.parquet`;
  const resp = await fetch(url);
  const buf = await resp.arrayBuffer();
  await db.registerFileBuffer(duckdbName, new Uint8Array(buf));
  registeredNames.set(url, duckdbName);
  return duckdbName;
}
```

Files are fetched **only** when `queryData()` determines they overlap
the current view window.  Once fetched, they are cached — switching back
to a previously-viewed window does not re-fetch.

### 2. Resolution indexing

**File:** `crates/sitegen/assets/chart.js`

The manifest entries are grouped by resolution at startup (no network
I/O, just indexing the JSON manifest that's already in the HTML):

```javascript
function extractResolution(captures) {
  for (const c of captures) {
    if (typeof c === "string" && c.startsWith("res=")) return c;
  }
  return null;
}

const byResolution = new Map();   // "res=1h" → [{url, start_time, end_time}, …]
for (const m of manifest) {
  const res = extractResolution(m.captures) || "res=1h";
  byResolution.get(res).push({ url: m.file, start_time: m.start_time, end_time: m.end_time });
}

const sortedResolutions = [...byResolution.keys()].sort(/* finest first */);
```

The `extractResolution()` function dynamically finds the `res=` capture
in the manifest rather than hardcoding a capture index — this is robust
to changes in the glob pattern.

### 3. Data-driven resolution picker (`pickResolution()`)

**File:** `crates/sitegen/assets/chart.js`

Replaced the old hardcoded duration-threshold approach with a
data-driven picker that directly counts overlapping files:

```javascript
function countOverlapping(entries, beginMs, endMs) {
  let count = 0;
  for (const f of entries) {
    if (f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs))
      count++;
  }
  return count;
}

function pickResolution(beginMs, endMs) {
  // Try finest-first — pick the finest resolution with ≤ 2 overlapping files.
  for (const res of sortedResolutions) {
    if (countOverlapping(byResolution.get(res), beginMs, endMs) <= 2)
      return res;
  }
  // No resolution fits — use coarsest and warn.
  console.warn("chart.js: no resolution fits within 2 files for this view …");
  return sortedResolutions[sortedResolutions.length - 1];
}
```

This approach is self-tuning:

- It works regardless of how many resolutions exist or how the temporal
  partitions are sized.
- It always picks the **finest** resolution that satisfies the ≤ 2 file
  constraint, giving maximum detail for the current zoom level.
- If no resolution can satisfy the constraint (misconfigured partitions),
  it falls back to the coarsest resolution and emits a `console.warn` —
  which the Puppeteer test detects as a failure.

### 4. Query pipeline

**File:** `crates/sitegen/assets/chart.js`

The `queryData(beginMs, endMs)` function ties it all together:

1. Call `pickResolution(beginMs, endMs)` → get the resolution key.
2. Filter that resolution's manifest entries to those overlapping the
   window (at most 2).
3. Call `ensureFile(url)` on each overlapping file (parallel, cached).
4. Build a `UNION ALL BY NAME` SQL query across the loaded files with a
   `WHERE epoch_ms(timestamp) >= ... AND ... <= ...` filter.
5. Return the query results for rendering.

Every user interaction — duration button click, brush-to-zoom drag,
reset — goes through `renderChart()` → `queryData()`, ensuring the
invariant is checked on every view change.

---

## Test Infrastructure

### Test 210 — Page structure validation

**File:** `testsuite/browser/tests/210-browser-page-validation.mjs`

Puppeteer test that validates every generated HTML page:

- Layout classes applied (`hero` for index, `data-page` for data pages)
- Sidebar present with navigation links using correct `BASE_PATH` prefix
- CSS styles loaded (sidebar has non-zero width)
- Chart container present on data pages
- DuckDB-WASM loads and renders Observable Plot SVGs
- Duration buttons rendered (≥ 5)
- Plot marks present (paths/lines/circles)
- No JavaScript errors
- Navigation between pages works (click sidebar → correct URL)

### Test 211 — Parquet loading invariant

**File:** `testsuite/browser/tests/211-browser-parquet-loading.mjs`

Puppeteer test that **directly verifies the ≤ 2 file property**:

1. **Discover all data pages** — walks the site directory tree to find
   every HTML file that contains a chart (not just index.html).

2. **Network interception** — hooks Puppeteer's response events to count
   `.parquet` fetches per render cycle.

3. **Initial load check** — loads each data page and asserts that the
   initial render fetched ≤ 2 parquet files.

4. **Duration button sweep** — clicks every duration button (1W, 2W, 1M,
   3M, 6M, 1Y, 2Y) and verifies no `console.warn` about exceeding
   2 files.  The test hooks `console.warn` in the browser context to
   detect the picker's fallback warning.

5. **Brush-to-zoom** — simulates a mouse drag on the brush overlay
   (20%–40% of the plot width), verifying the zoomed view also satisfies
   the invariant.

6. **Reset-zoom** — clicks the reset button and verifies the restored
   view is clean.

7. **JS error check** — asserts zero JavaScript errors per page.

Each page produces 8+ individual checks. The test reports a summary
line per test file (e.g., `=== Results: 48 passed, 0 failed ===`).

### Test 202 — Browser validation orchestrator

**File:** `testsuite/tests/202-sitegen-browser-validation.sh`  
**File:** `testsuite/browser/test.sh`

Shell script that orchestrates the full browser test pipeline in 4
stages:

| Stage | Action | Site config |
|-------|--------|-------------|
| 1 | Generate root site via test 201 in Docker | `base_url: /` |
| 2 | Serve at `/` with Vite, run tests 210 + 211 | Root deployment |
| 3 | Generate subdir site via test 205 in Docker | `base_url: /myapp/` |
| 4 | Serve at `/myapp/` with Vite, run tests 210 + 211 | Subdir deployment |

The orchestrator:

- Auto-discovers all `NNN-*.mjs` test files in `browser/tests/`
- Manages Vite lifecycle with robust port cleanup:
  `lsof -ti:${PORT} 2>/dev/null | xargs kill -9 2>/dev/null; true`
- Runs as a `# REQUIRES: host` test (not in Docker — needs a real
  browser via Puppeteer)
- Traps EXIT for cleanup

**Final results (35 tests, all passing):**

| Stage | Test | Checks |
|-------|------|--------|
| Stage 2 (root `/`) | 210 page validation | 73 passed |
| Stage 2 (root `/`) | 211 parquet loading | 48 passed |
| Stage 4 (subdir `/myapp/`) | 210 page validation | 43 passed |
| Stage 4 (subdir `/myapp/`) | 211 parquet loading | 24 passed |
| **Total** | | **188 passed, 0 failed** |

---

## Temporal Partitioning Alignment

### The constraint

The ≤ 2 file invariant places a requirement on temporal partitioning:

> For each resolution and the widest expected view window, at
> most 2 temporal partition files must cover the window.

### Automatic partitioning

Temporal partitions are now **computed automatically** per resolution
based on the `target_points` setting (default: 1500). The algorithm:

1. For each resolution $R$: $\text{max\_viewport} = R \times \text{target\_points}$
2. $\text{min\_partition\_width} = \text{max\_viewport} / 2$ (ensures ≤ 2 files)
3. Quantize upward to the nearest temporal level (minute → hour → day → month → quarter → year)

This means fine resolutions get finer partitions and coarse resolutions
get coarser partitions — automatically satisfying the ≤ 2 file invariant
at each resolution's handoff boundary.

### Example: test 201

Test 201 generates 1 year of synthetic data (2025-01-15 through
2026-01-14) with resolutions `[1h, 2h, 4h, 12h, 24h]` and
`target_points: 1500`. The auto-computed partitions:

| Resolution | Partition | Max pts/file | Rationale |
|-----------|-----------|-------------|-----------|
| `1h` | `["year", "quarter"]` | 2160 | 90-day quarters ≈ 2160 hourly points |
| `2h` | `["year", "quarter"]` | 1080 | 90-day quarters ≈ 1080 bi-hourly points |
| `4h` | `["year"]` | 2190 | 365-day year ≈ 2190 4-hourly points |
| `12h` | `["year"]` | 730 | 365-day year ≈ 730 12-hourly points |
| `24h` | `["year"]` | 365 | 365-day year ≈ 365 daily points |

### Configuration

```yaml
exports:
  - name: "params"
    pattern: "/reduced/single_param/*/*.series"
    target_points: 1500    # default; controls partition granularity
```

No manual `temporal` field is needed. The system logs a warning if the
coarsest resolution's partition cannot cover `max_width` in ≤ 2 files,
suggesting a coarser resolution be added.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  User interaction                                                   │
│  (duration button click, brush-to-zoom, page load)                  │
└─────────┬───────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  renderChart()                                                      │
│  Computes [beginMs, endMs] from activeDays or zoomDomain            │
└─────────┬───────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  queryData(beginMs, endMs)                                          │
│                                                                     │
│  1. pickResolution(beginMs, endMs)                                  │
│     └─ finest-first scan of sortedResolutions                       │
│     └─ countOverlapping(entries, beginMs, endMs) ≤ 2 → pick it     │
│     └─ fallback: coarsest + console.warn                            │
│                                                                     │
│  2. Filter to overlapping files (≤ 2)                               │
│                                                                     │
│  3. ensureFile(url) for each  (parallel, cached)                    │
│     └─ fetch() → registerFileBuffer() → DuckDB-WASM                │
│                                                                     │
│  4. SQL: UNION ALL BY NAME + WHERE timestamp filter                 │
│     └─ DuckDB-WASM query → results                                 │
└─────────┬───────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Observable Plot renders SVG                                        │
│  + brush overlay for zoom interaction                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Files Changed

| File | Change |
|------|--------|
| `crates/sitegen/assets/chart.js` | Lazy loading, resolution indexing, data-driven picker |
| `crates/sitegen/src/partitions.rs` | New — auto-computes temporal partitions per resolution from `target_points` |
| `crates/sitegen/src/config.rs` | `ExportStage`: replaced `temporal` with `target_points: u64` (default 1500) |
| `crates/sitegen/src/factory.rs` | Export stages auto-partition per resolution using `compute_partitions()` |
| `testsuite/browser/tests/210-browser-page-validation.mjs` | Moved from `validate.mjs`, renumbered to 200-series |
| `testsuite/browser/tests/211-browser-parquet-loading.mjs` | New — ≤ 2 file invariant test |
| `testsuite/browser/test.sh` | Rewritten: auto-discovers `NNN-*.mjs` tests, robust port cleanup |
| `testsuite/browser/package.json` | Updated scripts for `test:210`, `test:211` |
| `testsuite/tests/201-sitegen-markdown-maudit.sh` | Uses `target_points: 1500` (auto-partitioning) |
| `testsuite/tests/205-sitegen-base-url.sh` | Uses `target_points: 1500` (auto-partitioning) |
