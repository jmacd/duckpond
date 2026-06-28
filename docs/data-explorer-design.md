# Browser Data Explorer Design Document

## Overview

This document proposes a **Datasette-style interactive data explorer** for
DuckPond sitegen sites: a way for a visitor to a published static site (e.g.
the Noyo Harbor monitoring pages) to *dive into the underlying data* — open a
query, experiment with SQL, view results, and download them — entirely in the
browser, with no server.

The motivation comes directly from how DuckPond presents data today. The charts
a visitor sees are not "raw data": they are the output of a chain of DataFusion
factories (join → pivot → temporal-reduce) materialized at a chosen display
resolution. There is, in a real sense, *no single raw table behind the
display* — only the upstream sources that were reduced to produce it. The goal
is to give visitors principled access to that data: the reduced tiers they are
already looking at, and (optionally) the raw/intermediate datasets that produced
them.

This is explicitly a **design**; no implementation is proposed to land with this
document.

## Why this is feasible

The published site already ships every ingredient an explorer needs. Three facts
make this almost entirely a *surfacing* exercise rather than new infrastructure:

1. **Static columnar data is already exported.** The sitegen export stage runs
   `provider::export::export_series_to_parquet` /
   `export_table_provider_to_parquet` — the same DataFusion
   `COPY TO … PARTITIONED BY` pipeline used by `pond export` — to materialize
   matched pond series into **static Hive-partitioned parquet** under the site's
   `data/` tree. See `crates/sitegen/src/factory.rs::run_export_stages`.

2. **A manifest already describes those files.** Each exported file is recorded
   as an `ExportedFile { path, file, captures, temporal, start_time, end_time }`
   (`crates/sitegen/src/shortcodes.rs`), emitted into the page as a
   `script.chart-data` JSON block. Partition planning (`partitions.rs`) records
   the `resolution` per file.

3. **A SQL engine already runs in the browser.** `crates/sitegen/assets/chart.js`
   loads **DuckDB-WASM**, fetches parquet on demand, registers each file with
   `db.registerFileBuffer(name, bytes)`, and runs ordinary SQL —
   `SELECT * FROM read_parquet('<name>') WHERE … ORDER BY timestamp` — returning
   Arrow rows. The chart's brush-zoom, resolution selection, and the recently
   added CSV export all sit on top of this.

In other words, the site is *already a tiny Datasette* — static columnar files
plus an in-browser query engine. The explorer turns that latent capability into
a first-class, user-facing surface.

## The one real constraint

The factory SQL that produces the display — the joins, pivots, temporal
reductions, and any custom table providers — runs **server-side**, inside the
pond, on DataFusion. Those views and providers **cannot execute in the browser**;
DuckDB-WASM only knows how to read the static parquet that was shipped.

This is the same constraint Datasette embraces: it does not expose a live
application database; it publishes an immutable snapshot and lets users query
*that*. The DuckPond analogue is:

> Choose which pipeline stage(s) to expose, **materialize each as static
> parquet**, register them in the browser as named DuckDB tables, and give the
> user a SQL playground over those tables.

"Raw data access" is therefore a *configuration choice about which stages to
export*, not a request to re-run factories in the browser. We can expose any
slice of the dataflow:

| Dataset                | Pond source (noyo example)              | Meaning                              |
|------------------------|------------------------------------------|--------------------------------------|
| Source readings        | `/hydrovu/devices/**/...series`          | Closest to "raw" instrument data     |
| Combined               | `/combined/*` (timeseries-join output)   | Sensors merged per logical station   |
| Singled                | `/singled/*` (timeseries-pivot output)   | One parameter per file               |
| Reduced (shown today)  | `/reduced/single_param/*/*.series`       | avg/min/max at 1h…24h tiers          |

Each becomes a queryable table set with its own manifest.

## Proposed architecture

```
┌──────────────────────────── Pond (server-side, at build) ─────────────────────────┐
│  Factories (join/pivot/reduce) ─► export stages ─► static parquet + manifests       │
└───────────────────────────────────────────┬────────────────────────────────────────┘
                                            │  (shipped with the static site)
                                            ▼
┌──────────────────────────── Browser (DuckDB-WASM) ────────────────────────────────┐
│  Explore page:                                                                     │
│    • dataset picker (reads dataset manifests)                                       │
│    • registers parquet for the chosen dataset as a DuckDB table/view                │
│    • SQL editor  ─►  conn.query(sql)  ─►  results grid                              │
│    • download results (CSV / parquet / JSON)                                        │
│    • shareable query URL (#sql=…&dataset=…)                                          │
└────────────────────────────────────────────────────────────────────────────────────┘
```

### Server-side (build time)

- **Reuse the export stage** to materialize each dataset the operator wants to
  expose. This is already what produces the chart parquet; exposing additional
  stages is additional `exports:` entries pointed at different patterns (e.g.
  `/combined/*`, `/singled/*`, source `.series`).
- **Emit a "datasets" manifest** describing each exposed dataset: a stable table
  name, a human label, the list of parquet file URLs (with `start_time` /
  `end_time` / `resolution` from the existing `ExportedFile`), and the column
  schema. The schema can be captured from the export's Arrow schema so the
  explorer can show columns without fetching data.
- **Render an Explore page** (a new sitegen layout, analogous to `data_layout`)
  that loads an `explore.js` asset and carries the datasets manifest in a
  `script` block, mirroring how chart pages carry `chart-data`.

### Browser-side (`explore.js`)

- **DuckDB-WASM init and file registration are lifted from `chart.js`**
  (`ensureFile` → `fetch` → `registerFileBuffer`). Factor the shared init into a
  small module both assets import, to avoid divergence.
- **Register the selected dataset.** For a dataset spanning multiple partition
  files, register them and expose a single logical table via a view:
  `CREATE VIEW <table> AS SELECT * FROM read_parquet(['f1','f2',…])`. Lazy
  fetching can be preserved (only load files overlapping a referenced time range)
  but the simplest first cut registers all files for the chosen dataset.
- **SQL editor → results grid.** A `<textarea>` (or a small code editor) feeds
  `conn.query(sql)`; render the returned Arrow rows into a paged HTML table.
  Guardrails: wrap user SQL so an implicit `LIMIT` protects against accidental
  multi-million-row renders; surface DuckDB errors inline.
- **Download results.** Reuse the CSV serializer already added to `chart.js`
  (lift it into the shared module); additionally offer parquet/JSON, both of
  which DuckDB-WASM can produce (`COPY (…) TO 'out.parquet'` then read the
  virtual file back as bytes).
- **Shareable query URLs.** Extend the pattern already prototyped for charts
  (`#t=<begin>,<end>` absolute ranges) to `#dataset=<name>&sql=<encoded>`. On
  load, decode and run, so a query is fully reproducible from a link — the
  Datasette "copy link to this query" behavior.

## Relationship to existing chart features

The explorer is a natural generalization of work already shipped on chart pages:

- **CSV download** (per-window export of the queried rows) — the explorer
  generalizes this to arbitrary queries and result sets.
- **Shareable absolute-range links** (`#t=`) — the explorer generalizes the URL
  state to encode a dataset + SQL.
- **Full-screen view** — the explorer benefits from the same overlay treatment
  for a roomy editor + results pane.

A nice closing of the loop (Stage 3 S3.4): a "view as chart" affordance that
hands a query's time/value columns to the Vega-Lite renderer, and conversely an
"explore this data" link from a chart that opens the explorer pre-pointed at the
chart's dataset and window (the cross-link shipped in Stage 2).

## Staging

- **Stage 0 — PoC over existing reduced parquet.** One Explore page per site.
  Reuse DuckDB-WASM init + file registration, register the already-shipped
  reduced tiers as a table, provide a SQL editor, a results grid, CSV download,
  and a shareable `#sql=` URL. No new export stages, no new server code beyond
  the page/layout and manifest wiring. Proves the end-to-end UX cheaply.
- **Stage 1 — expose raw/intermediate datasets.** Add export stages so source
  `.series`, `/combined`, and `/singled` ship as static parquet with their own
  manifests, selectable in the explorer. This is what realizes genuine "raw
  data" access. Mind data volume: raw source series are larger than reduced
  tiers, so this is opt-in per site and benefits from the existing temporal
  partitioning so the browser fetches only what a query touches.
- **Stage 2 — ergonomics.** Schema/column browser, canned example queries,
  result paging and row counts, parquet/JSON export, and the chart ↔ explorer
  cross-links above.
- **Stage 3 — Vega-Lite visualization.** Migrate the chart-rendering path from
  Observable Plot to Vega-Lite, factor a single rendering module shared by chart
  pages and the explorer, expose an editable Vega-Lite spec in the explorer, and
  round-trip query + spec through the shareable URL and the chart ↔ explorer
  cross-links.

## Implementation status

Tracked on branch `jmacd/66`. Coverage is two-tiered. The Docker testsuite
`testsuite/tests/212-sitegen-explore-multi.sh` is asset-ships level: it asserts
the emitted assets contain expected substrings; it does not exercise in-browser
runtime behavior. The Puppeteer harness `testsuite/browser/` does exercise real
in-browser behavior headlessly; `tests/213-browser-vega-render.mjs` drives the
shipped `vega-shared.js` through both the single-series and multi-series (fold)
spec paths and asserts the vendored vega-embed bundle renders SVG with no JS
errors -- the de-risking step S3.3 calls for, now satisfied. (Lazy fetch and
full-screen resize remain unexercised headlessly.) Client assets live in
`crates/sitegen/assets/` and embed into the binary via `include_str!` in
`crates/sitegen/src/factory.rs::write_shared_assets`.

Done:

- **Stage 0 / Stage 1 / Stage 2** — complete. Dataset picker over multiple
  selectable datasets; deferred + window-pruned per-partition registration with a
  window control; column-chip browser; canned examples; result paging + row
  counts; CSV / JSON / Parquet export; Table/Chart view switcher; full-screen
  toggle; chart → explorer "Explore this data" cross-link (ad-hoc `chart_data`
  dataset); shareable `#dataset=&sql=` (+ `&files=&label=`) URL.
- **Stage 3 S3.1** — Vega-Lite chart-view spike in the explorer.
- **Stage 3 S3.2** — `assets/vega-shared.js` shared module (`loadVega`,
  `detectKind`, `inferEncoding`, `buildLineSpec` returning a data-less spec,
  `sanitizeRows`); explore.js imports it; the Chart view has an editable
  Vega-Lite spec textarea with "Apply spec" / "Reset to auto" (data is injected
  from the live query at render time).
- **Stage 3 S3.4 (shareable URL round-trip)** — the explorer hash now carries
  `&view=chart` and, when the spec is hand-edited, `&spec=<encoded>`; on load
  both are restored before the first run so a shared link reopens the same
  visualization (auto-inferred specs are omitted to keep links short). View
  toggles and Apply/Reset spec rewrite the hash live.
- **Stage 3 S3.3 prerequisite (headless render verification)** — a Puppeteer
  test (`testsuite/browser/tests/213-browser-vega-render.mjs`) proves the
  in-browser Vega path renders: the vendored vega-embed bundle imports, a
  `buildLineSpec` spec compiles, and both single-series and folded multi-series
  specs draw SVG line paths with no JS errors. This removes the "untested
  headlessly" blocker noted below for the chart.js migration.

Remaining:

- **Stage 3 S3.3** — port `chart.js` + `overlay.js` line/area/dot marks to
  Vega-Lite specs (via `vega-shared.js`), preserving the auto-resolution +
  lazy-fetch data layer. Largest migration. The in-browser Vega render path is
  now verified headlessly (test 213), so the migration can proceed; the work
  itself (mapping the existing marks, brush-zoom, hover crosshair, and
  counter-rate layer onto Vega-Lite specs) is still outstanding.
- **Stage 3 S3.4 (chart cross-link spec)** — the URL round-trip above is done;
  what remains is for the chart pages' "Explore this data" link to hand over a
  Vega-Lite spec (and "view as chart" to emit one back to the chart), which is
  blocked on the S3.3 chart.js Vega migration since chart.js still renders with
  Observable Plot and has no spec to pass.
- **Optional embedded SQL editor** (syntax highlight / completion) — explicitly
  deferred; the plain `<textarea>` stands.

## Open questions

1. **Data volume / cost.** Exposing raw source series multiplies the static
   payload. Do we cap exposed history, downsample a "raw-ish" tier, or rely on
   partition-aware lazy fetching and accept large totals for power users?
2. **Schema stability.** `oteljson`-derived schemas can vary across history when
   metrics are added/renamed (see `temporal-reduce` notes). The datasets
   manifest should record a per-file schema, and the explorer should present a
   unioned schema with absent-column semantics, matching how reductions already
   handle this.
3. **Editor scope.** Plain `<textarea>` for Stage 0 vs. a small embedded SQL
   editor (syntax highlight, completion) later. Keep Stage 0 dependency-free.
4. **Guardrails.** Default `LIMIT`, query timeouts, and memory ceilings in
   DuckDB-WASM to keep a careless query from freezing the tab.
5. **Discoverability.** Where the Explore entry point lives (a sidebar item, a
   per-chart "explore" link, or both) and whether it is enabled per site via
   config.

## Summary

A browser-based, Datasette-style data explorer is well within reach because the
published site already ships static parquet, a manifest, and an in-browser SQL
engine. The only firm constraint — that factory/provider SQL runs server-side
and cannot execute in the browser — is handled exactly as Datasette handles it:
publish static snapshots of the chosen pipeline stages and let users query those.
"Raw data access" becomes a configuration decision about which stages to export,
and the user-facing explorer is a generalization of the CSV-export and
shareable-link features already present on chart pages.
