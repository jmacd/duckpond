// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen -- vega-shared.js
//
// Shared Vega-Lite rendering helpers used by the explorer (explore.js) and,
// in a later stage, the chart pages (chart.js). Centralizing the lazy vendor
// import, encoding inference, spec construction, and row sanitization keeps the
// two render paths from diverging.

let vegaEmbedFn = null;

// Lazily import the vendored vega-embed bundle, resolved relative to this
// module so any asset that imports it picks up the same vendor copy. The bundle
// is large, so callers should invoke this only when a chart is first shown.
export async function loadVega() {
  if (vegaEmbedFn) return vegaEmbedFn;
  const mod = await import(
    /* @vite-ignore */ new URL("./vendor/vega-bundle.mjs", import.meta.url).href
  );
  vegaEmbedFn = mod.vegaEmbed;
  return vegaEmbedFn;
}

// Detect whether a column is temporal / quantitative / nominal by sampling its
// first non-null value across the materialized rows.
export function detectKind(field, rows) {
  for (const r of rows) {
    const v = r[field];
    if (v == null) continue;
    if (v instanceof Date) return "temporal";
    if (typeof v === "number" || typeof v === "bigint") return "quantitative";
    return "nominal";
  }
  return "nominal";
}

// Choose an x column (first temporal, else first column) and the numeric y
// columns (all quantitative columns except x).
export function inferEncoding(fields, rows) {
  const kinds = {};
  for (const f of fields) kinds[f] = detectKind(f, rows);
  let xCol = fields.find((f) => kinds[f] === "temporal");
  if (!xCol) xCol = fields[0];
  const yCols = fields.filter((f) => f !== xCol && kinds[f] === "quantitative");
  return { xCol, xKind: kinds[xCol], yCols };
}

// Build a data-less Vega-Lite line-chart spec from result fields/rows. The data
// is injected by the caller at render time so the same spec can be reused as the
// query result changes and edited independently of the data. Returns
// `{ spec }` on success or `{ error }` when there is nothing chartable.
export function buildLineSpec(fields, rows, opts = {}) {
  if (!rows || !rows.length || !fields || !fields.length) {
    return { error: "Run a query to chart its result." };
  }
  const { xCol, xKind, yCols } = inferEncoding(fields, rows);
  if (!yCols.length) {
    return {
      error:
        "No numeric column to plot. The chart view needs at least one numeric column.",
    };
  }
  const multi = yCols.length > 1;
  const spec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    width: "container",
    height: opts.height || 340,
    transform: multi ? [{ fold: yCols, as: ["series", "value"] }] : [],
    mark: { type: "line", clip: true, tooltip: true },
    encoding: {
      x: {
        field: xCol,
        type: xKind === "temporal" ? "temporal" : "quantitative",
        title: xCol,
      },
      y: multi
        ? { field: "value", type: "quantitative" }
        : { field: yCols[0], type: "quantitative", title: yCols[0] },
      ...(multi ? { color: { field: "series", type: "nominal", title: null } } : {}),
    },
    // Interval selection bound to scales gives pan + zoom out of the box --
    // brush-zoom parity with the Observable Plot chart path.
    params: [{ name: "grid", select: "interval", bind: "scales" }],
  };
  return { spec };
}

// Coerce rows to Vega-friendly plain objects: BigInt -> Number, keep Date. All
// fields are included so a hand-edited spec can reference any result column.
export function sanitizeRows(fields, rows) {
  return rows.map((r) => {
    const o = {};
    for (const c of fields) {
      const v = r[c];
      o[c] = typeof v === "bigint" ? Number(v) : v;
    }
    return o;
  });
}
