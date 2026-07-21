// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Watertown sitegen -- overlay.js
// Pump/well cycle analysis: two SERVER-SIDE aggregated charts, one band per
// calendar month.
//
//   * Drawdown  -- drawdown (static - depth) vs elapsed seconds since pump-on.
//   * Horner    -- residual drawdown vs log10((t_p + dt) / dt) during recovery.
//
// Each dataset already carries per-(calendar-month, x-bucket) P10 / P50 / P90,
// computed in the water pond (see config/water.yaml: /analysis
// drawdown-by-month and horner-by-month).  The browser therefore only draws
// ~28 median lines + on-demand bands, so the page loads instantly -- the old
// per-cycle overlay downloaded ~150k rows and drew ~3,900 lines.
//
// Datasets are recognized by their columns:
//   drawdown-by-month: month, elapsed_s, s_p10, s_p50, s_p90, n
//   horner-by-month:   month, log_ratio, s_p10, s_p50, s_p90, n

import { loadVega, sanitizeRows, buildBandSpec, monthShade } from "./vega-shared.js";

(async function () {
  "use strict";

  try {
    await renderOverlay();
  } catch (e) {
    console.error("overlay.js fatal:", e);
    const c = document.getElementById("overlay-chart");
    if (c) c.innerHTML = '<div class="empty-state">Render error: ' +
      e.message + '<br><pre>' + e.stack + '</pre></div>';
  }

  async function renderOverlay() {

  const manifestEl = document.querySelector(
    'script.overlay-data[type="application/json"]'
  );
  if (!manifestEl) return;

  const container = document.getElementById("overlay-chart");
  if (!container) return;
  const exploreUrl = container.dataset.exploreUrl || "";
  // Optional pre-selected month-of-year from a shared "Copy link" URL (?month=NN).
  const initMonth = (function () {
    const m = new URL(location.href).searchParams.get("month");
    return m && /^(0[1-9]|1[0-2])$/.test(m) ? m : null;
  })();

  let manifest;
  try {
    manifest = JSON.parse(manifestEl.textContent);
  } catch (e) {
    return;
  }
  if (!manifest || manifest.length === 0) return;

  container.innerHTML =
    '<div class="empty-state">Loading pump cycle data...</div>';

  // -- DuckDB-WASM init -------------------------------------------------------

  let db, conn;
  try {
    const duckdb = await import(
      /* @vite-ignore */ "./vendor/duckdb-browser.mjs"
    );
    const bundle = {
      mainModule: new URL("./vendor/duckdb-eh.wasm", import.meta.url).href,
      mainWorker: new URL("./vendor/duckdb-browser-eh.worker.js", import.meta.url).href,
    };
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
  } catch (e) {
    container.innerHTML =
      '<div class="empty-state">DuckDB-WASM failed: ' + e.message + '</div>';
    return;
  }

  // -- Register + classify parquet files --------------------------------------

  const registeredNames = new Map();
  let fileIdx = 0;

  async function ensureFile(entry) {
    const key = entry.file;
    if (registeredNames.has(key)) return registeredNames.get(key);
    const url = new URL(entry.file, window.location.href);
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("Failed to fetch " + key);
    const buf = new Uint8Array(await resp.arrayBuffer());
    const name = "f" + fileIdx++ + ".parquet";
    await db.registerFileBuffer(name, buf);
    registeredNames.set(key, name);
    return name;
  }

  const drawdownTables = [];
  const hornerTables = [];
  // Original manifest file URLs per chart, handed to the explorer verbatim.
  const drawdownFiles = [];
  const hornerFiles = [];

  for (const entry of manifest) {
    try {
      const name = await ensureFile(entry);
      const probe = await conn.query(
        "SELECT * FROM read_parquet('" + name + "') LIMIT 1"
      );
      const row = probe.toArray()[0];
      if (!row) continue;
      const cols = Object.keys(row);
      if (cols.includes("elapsed_s") && cols.includes("s_p50")) {
        drawdownTables.push(name);
        drawdownFiles.push(entry.file);
      } else if (cols.includes("log_ratio") && cols.includes("s_p50")) {
        hornerTables.push(name);
        hornerFiles.push(entry.file);
      }
    } catch (e) {
      console.warn("overlay.js: failed to load", entry.file, e);
    }
  }

  if (drawdownTables.length === 0 && hornerTables.length === 0) {
    container.innerHTML =
      '<div class="empty-state">Unrecognized data shape for pump analysis.</div>';
    return;
  }

  // Resolve theme colors from CSS custom properties (Vega cannot read them).
  function resolveTheme() {
    const cs = getComputedStyle(document.body);
    const fg = (cs.getPropertyValue("--fg") || "").trim() || "#333333";
    return { fg, grid: "rgba(128,128,128,0.2)" };
  }
  const chartTheme = resolveTheme();

  async function embedVega(targetEl, spec, rows, fields) {
    try {
      targetEl.style.width = "100%";
      const embed = await loadVega();
      const renderSpec = { ...spec, data: { values: sanitizeRows(fields, rows) } };
      const res = await embed(targetEl, renderSpec, { actions: false, renderer: "svg" });
      refitView(res && res.view, targetEl);
      return (res && res.view) || null;
    } catch (e) {
      targetEl.innerHTML =
        '<div class="empty-state">Chart render failed: ' +
        String((e && e.message) || e) +
        "</div>";
      return null;
    }
  }

  // Re-fit container width once layout/fonts settle (see vega width:"container").
  function refitView(view, targetEl) {
    if (!view) return;
    const refit = () => {
      const w = targetEl.clientWidth ||
        (targetEl.parentElement && targetEl.parentElement.clientWidth) || 700;
      view.width(w).resize().runAsync();
    };
    requestAnimationFrame(refit);
    new ResizeObserver(refit).observe(targetEl);
  }

  // -- Load aggregated rows ---------------------------------------------------

  async function loadRows(tables, xCol) {
    if (tables.length === 0) return [];
    const unionSql = tables
      .map(function (t) { return "SELECT * FROM read_parquet('" + t + "')"; })
      .join(" UNION ALL BY NAME ");
    const result = await conn.query(
      "SELECT month, " + xCol + ", s_p10, s_p50, s_p90, n " +
      "FROM (" + unionSql + ") ORDER BY month, " + xCol
    );
    return result.toArray();
  }

  const num = function (v) { return typeof v === "bigint" ? Number(v) : Number(v); };

  // Add month-of-year, year, and a per-year perceptual shade of the month hue
  // (darker = older, lighter = newer) so a selected month's years are distinct.
  function decorateMonths(rows) {
    if (rows.length === 0) return rows;
    let minY = Infinity;
    let maxY = -Infinity;
    for (const r of rows) {
      const y = parseInt(r.month.slice(0, 4), 10);
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
    const span = maxY > minY ? maxY - minY : 1;
    for (const r of rows) {
      const moy = r.month.slice(5, 7);
      const t = (parseInt(r.month.slice(0, 4), 10) - minY) / span; // 0 old..1 new
      r.moy = moy;
      r.year = r.month.slice(0, 4);
      r.shade = monthShade(parseInt(moy, 10) - 1, t);
    }
    return rows;
  }

  let drawdownRows = [];
  let hornerRows = [];
  try {
    const raw = await loadRows(drawdownTables, "elapsed_s");
    drawdownRows = raw.map(function (d) {
      return {
        month: String(d.month),
        elapsed_min: num(d.elapsed_s) / 60.0,
        s_p10: num(d.s_p10),
        s_p50: num(d.s_p50),
        s_p90: num(d.s_p90),
        n: num(d.n),
      };
    });
  } catch (e) {
    console.error("drawdown query failed:", e);
  }
  try {
    const raw = await loadRows(hornerTables, "log_ratio");
    hornerRows = raw.map(function (d) {
      return {
        month: String(d.month),
        log_ratio: num(d.log_ratio),
        s_p10: num(d.s_p10),
        s_p50: num(d.s_p50),
        s_p90: num(d.s_p90),
        n: num(d.n),
      };
    });
  } catch (e) {
    console.error("horner query failed:", e);
  }

  // -- DOM layout -------------------------------------------------------------

  container.innerHTML = "";

  function section(title, blurb) {
    const wrap = document.createElement("section");
    wrap.className = "analysis-block";
    const head = document.createElement("div");
    head.className = "overlay-header";
    const h = document.createElement("h3");
    h.textContent = title;
    head.appendChild(h);
    if (blurb) {
      const p = document.createElement("p");
      p.className = "chart-caption";
      p.textContent = blurb;
      head.appendChild(p);
    }
    wrap.appendChild(head);
    const chart = document.createElement("div");
    chart.className = "chart-host overlay-vega";
    wrap.appendChild(chart);
    container.appendChild(wrap);
    return { wrap: wrap, chart: chart };
  }

  // Default explorer visualization for an analysis dataset: one median line per
  // calendar month over the aggregated x column. The explorer injects the query
  // result as a view named `chart_data`, so this mirrors the parquet columns.
  function buildExploreSpec(xCol, xTitle, yTitle) {
    return {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      width: "container",
      height: 340,
      config: { scale: { zero: false } },
      mark: { type: "line", clip: true, tooltip: true },
      encoding: {
        x: { field: xCol, type: "quantitative", title: xTitle },
        y: { field: "s_p50", type: "quantitative", title: yTitle },
        color: { field: "month", type: "nominal", title: "Month" },
      },
    };
  }

  // Read the currently legend-selected month-of-year ('01'..'12') from a Vega
  // view's point-selection store, or null when nothing is selected.
  function selectedMonth(view) {
    try {
      const store = view.data("monthSel_store");
      if (store && store.length && store[0].values && store[0].values.length) {
        return String(store[0].values[0]);
      }
    } catch (e) { /* view not ready */ }
    return null;
  }

  const MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  // Per-chart toolbar: a full-screen toggle (expands just this chart to fill the
  // viewport; the Vega view's ResizeObserver refits width, and we bump its
  // height), a "Copy link" button that captures the selected month (if any) so
  // the link reopens with it highlighted, and, when the site emitted an explorer
  // URL, an "Explore this data" cross-link handing this chart's parquet files +
  // a default query/spec to the explorer -- mirroring chart.js.
  function addChartControls(block, host, getView, opts) {
    const bar = document.createElement("div");
    bar.className = "analysis-toolbar";

    const fsBtn = document.createElement("button");
    fsBtn.type = "button";
    fsBtn.className = "fullscreen-toggle";
    const syncFs = function () {
      const on = block.classList.contains("block-fullscreen");
      fsBtn.innerHTML = on
        ? '<span class="fs-icon">\u00d7</span> Close'
        : 'Full screen <span class="fs-icon">\u2192</span>';
      fsBtn.setAttribute("aria-pressed", on ? "true" : "false");
      fsBtn.title = on ? "Exit full screen (Esc)" : "Expand chart to full screen";
    };
    const setFs = function (on) {
      block.classList.toggle("block-fullscreen", on);
      document.body.classList.toggle("analysis-fullscreen", on);
      syncFs();
      const view = getView();
      if (view) {
        const h = on ? Math.max(360, window.innerHeight - 220) : opts.baseHeight;
        const w = host.clientWidth || 700;
        view.width(w).height(h).resize().runAsync();
      }
    };
    fsBtn.onclick = function () {
      setFs(!block.classList.contains("block-fullscreen"));
    };
    syncFs();
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && block.classList.contains("block-fullscreen")) {
        setFs(false);
      }
    });
    bar.appendChild(fsBtn);

    // Copy a link to this page that restores the currently selected month.
    const copyBtn = document.createElement("button");
    copyBtn.type = "button";
    copyBtn.className = "copy-link";
    copyBtn.textContent = "Copy link";
    copyBtn.onclick = function () {
      const view = getView();
      const m = view ? selectedMonth(view) : null;
      const u = new URL(location.href);
      if (m) u.searchParams.set("month", m);
      else u.searchParams.delete("month");
      const done = function () {
        const label = m ? "Copied " + MONTH_NAMES[parseInt(m, 10) - 1] + " link" : "Copied link";
        const orig = "Copy link";
        copyBtn.textContent = label;
        setTimeout(function () { copyBtn.textContent = orig; }, 1600);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(u.href).then(done, done);
      } else {
        const ta = document.createElement("textarea");
        ta.value = u.href;
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand("copy"); } catch (e) { /* ignore */ }
        ta.remove();
        done();
      }
    };
    bar.appendChild(copyBtn);

    if (exploreUrl && opts.files && opts.files.length > 0) {
      const exBtn = document.createElement("button");
      exBtn.type = "button";
      exBtn.className = "explore-data";
      exBtn.textContent = "Explore this data";
      exBtn.onclick = function () {
        const urls = opts.files
          .map(function (f) { return new URL(f, location.href).href; })
          .filter(Boolean);
        if (urls.length === 0) return;
        const params = new URLSearchParams();
        params.set("label", opts.label);
        params.set("files", urls.join(","));
        params.set("sql", "SELECT * FROM chart_data ORDER BY month, " + opts.xCol);
        params.set("view", "chart");
        params.set("spec", JSON.stringify(
          buildExploreSpec(opts.xCol, opts.xTitle, opts.yTitle)));
        location.assign(exploreUrl + "#" + params.toString());
      };
      bar.appendChild(exBtn);
    }

    block.appendChild(bar);
  }

  const bandFields = ["month", "s_p10", "s_p50", "s_p90", "n", "moy", "year", "shade"];

  if (drawdownRows.length > 0) {
    decorateMonths(drawdownRows);
    const sec = section(
      "Drawdown by month",
      "Median drawdown (static level minus well depth) versus time since the " +
      "pump switched on, one line per calendar month. Click a month in the " +
      "legend to reveal its P10-P90 band."
    );
    const spec = buildBandSpec({
      xField: "elapsed_min",
      xTitle: "Minutes since pump-on",
      yTitle: "Drawdown (m)",
      theme: chartTheme,
      initMonth: initMonth,
    });
    const view = await embedVega(sec.chart, spec, drawdownRows,
      bandFields.concat(["elapsed_min"]));
    addChartControls(sec.wrap, sec.chart, function () { return view; }, {
      files: drawdownFiles,
      label: "Drawdown by month",
      xCol: "elapsed_s",
      xTitle: "Seconds since pump-on",
      yTitle: "Drawdown (m)",
      baseHeight: 380,
    });
  }

  if (hornerRows.length > 0) {
    decorateMonths(hornerRows);
    const sec = section(
      "Horner recovery by month",
      "Median residual drawdown versus the log Horner ratio (t_p + dt) / dt " +
      "during recovery, one line per calendar month. Larger x is earlier in " +
      "recovery; the curve heads toward 0 as the well returns to static. " +
      "Click a month in the legend to reveal its P10-P90 band."
    );
    const spec = buildBandSpec({
      xField: "log_ratio",
      xTitle: "log10( (t_p + dt) / dt )",
      yTitle: "Residual drawdown (m)",
      theme: chartTheme,
      initMonth: initMonth,
    });
    const view = await embedVega(sec.chart, spec, hornerRows,
      bandFields.concat(["log_ratio"]));
    addChartControls(sec.wrap, sec.chart, function () { return view; }, {
      files: hornerFiles,
      label: "Horner recovery by month",
      xCol: "log_ratio",
      xTitle: "log10( (t_p + dt) / dt )",
      yTitle: "Residual drawdown (m)",
      baseHeight: 380,
    });
  }

  if (drawdownRows.length === 0 && hornerRows.length === 0) {
    container.innerHTML =
      '<div class="empty-state">No pump cycle data found.</div>';
  }

  } // end renderOverlay
})();
