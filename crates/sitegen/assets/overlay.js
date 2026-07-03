// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen -- overlay.js
// Interactive pump cycle analysis with coordinated brush+filter.
//
// Top: 4-year well-depth overview with a Vega-Lite interval selection for
// time-range selection. Below: analysis charts (overlay, Horner, drawdown,
// recovery, summary) that re-render when the selection changes.
//
// All charts render with Vega-Lite via the shared vega module (the Stage 3
// S3.3 migration -- Observable Plot and D3 are no longer used here).
//
// Data sources:
//   pump-cycles: pump_event_id, month, elapsed_s, depth, static_depth, phase
//   cycle-summary: pump_event_id, timestamp, month, draw_duration_s, ...

import {
  loadVega,
  sanitizeRows,
  buildMultiLineSpec,
  buildDotLineSpec,
  buildBrushOverviewSpec,
} from "./vega-shared.js";

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

  // -- Load parquet files -----------------------------------------------------

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

  const pumpCyclesTables = [];
  const cycleSummaryTables = [];
  // Absolute URLs + column lists per category, captured for the "Explore this
  // data" cross-link so the explorer can register each schema as its own view.
  const pumpCyclesFiles = [];
  const cycleSummaryFiles = [];
  let pumpCyclesCols = [];
  let cycleSummaryCols = [];

  for (const entry of manifest) {
    try {
      const name = await ensureFile(entry);
      const probe = await conn.query(
        "SELECT * FROM read_parquet('" + name + "') LIMIT 1"
      );
      const row = probe.toArray()[0];
      if (!row) continue;
      const cols = Object.keys(row);
      const absUrl = new URL(entry.file, window.location.href).href;
      if (cols.includes("elapsed_s") && cols.includes("pump_event_id")) {
        pumpCyclesTables.push(name);
        pumpCyclesFiles.push(absUrl);
        if (pumpCyclesCols.length === 0) pumpCyclesCols = cols;
      } else if (cols.includes("draw_duration_s") && cols.includes("pump_event_id")) {
        cycleSummaryTables.push(name);
        cycleSummaryFiles.push(absUrl);
        if (cycleSummaryCols.length === 0) cycleSummaryCols = cols;
      }
    } catch (e) {
      console.warn("overlay.js: failed to load", entry.file, e);
    }
  }

  const hasPumpCycles = pumpCyclesTables.length > 0;
  const hasCycleSummary = cycleSummaryTables.length > 0;

  if (!hasPumpCycles && !hasCycleSummary) {
    container.innerHTML =
      '<div class="empty-state">Unrecognized data shape for overlay chart.</div>';
    return;
  }

  // Resolve the page theme's foreground and a faint grid color from CSS custom
  // properties so the Vega charts match the surrounding light/dark styling
  // (Vega cannot read CSS variables itself; we pass resolved colors in).
  function resolveTheme() {
    const cs = getComputedStyle(document.body);
    const fg = (cs.getPropertyValue("--fg") || "").trim() || "#333333";
    return { fg, grid: "rgba(128,128,128,0.2)" };
  }
  const chartTheme = resolveTheme();

  // Render a data-less Vega-Lite spec into `targetEl` with the supplied rows
  // injected as data. The wrapper is appended synchronously by the caller so
  // chart order is preserved; this async embed fills it in when ready. Errors
  // surface inline rather than throwing out of the (unawaited) render path.
  async function embedVega(targetEl, spec, rows, fields) {
    try {
      targetEl.style.width = "100%";
      const embed = await loadVega();
      const renderSpec = { ...spec, data: { values: sanitizeRows(fields, rows) } };
      const res = await embed(targetEl, renderSpec, { actions: false, renderer: "svg" });
      refitView(res && res.view, targetEl);
    } catch (e) {
      targetEl.innerHTML =
        '<div class="empty-state">Chart render failed: ' +
        String((e && e.message) || e) +
        "</div>";
    }
  }

  // Vega's `width: "container"` reads the element's clientWidth at embed time.
  // When layout/fonts have not settled the element can measure zero width, which
  // collapses every datum to x=0 and leaves the plot blank. Re-fit to the real
  // width once the layout settles and on every resize so lines span the plot.
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

  // -- Constants ---------------------------------------------------------------

  const monthNames = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];

  const monthColors = [
    "#2166ac", "#4393c3", "#92c5de", "#d1e5f0",
    "#fddbc7", "#f4a582", "#d6604d", "#b2182b",
    "#8856a7", "#810f7c", "#1b7837", "#762a83",
  ];

  function monthColor(m) {
    return monthColors[(m - 1) % 12];
  }

  function arrayMax(arr, fn) {
    let max = -Infinity;
    for (const v of arr) {
      const x = fn(v);
      if (x > max) max = x;
    }
    return max;
  }

  // -- Query all data upfront -------------------------------------------------

  let allPumpRows = [];
  let allSummaryRows = [];

  if (hasPumpCycles) {
    const unionSql = pumpCyclesTables
      .map(function (t) { return "SELECT * FROM read_parquet('" + t + "')"; })
      .join(" UNION ALL BY NAME ");
    try {
      const result = await conn.query(
        "SELECT pump_event_id, month, elapsed_s, depth, static_depth, phase " +
        "FROM (" + unionSql + ") WHERE depth > 30 AND depth < 50 " +
        "ORDER BY pump_event_id, elapsed_s"
      );
      const data = result.toArray();
      console.log("overlay.js: loaded", data.length, "pump cycle rows");

      const seenEvents = new Set();
      for (const d of data) {
        const event = Number(d.pump_event_id);
        const month = Number(d.month);
        const color = monthColor(month);
        const staticDepth = Number(d.static_depth);

        if (!seenEvents.has(event) && staticDepth > 0) {
          seenEvents.add(event);
          allPumpRows.push({
            event, month, color,
            elapsed_min: -1,
            depth: staticDepth,
            drawdown: 0,
            phase: "static",
          });
        }

        allPumpRows.push({
          event, month, color,
          elapsed_min: Number(d.elapsed_s) / 60.0,
          depth: Number(d.depth),
          drawdown: staticDepth - Number(d.depth),
          phase: String(d.phase),
        });
      }
    } catch (e) {
      console.error("Pump cycle query failed:", e);
    }
  }

  if (hasCycleSummary) {
    const unionSql = cycleSummaryTables
      .map(function (t) { return "SELECT * FROM read_parquet('" + t + "')"; })
      .join(" UNION ALL BY NAME ");
    try {
      const result = await conn.query(
        "SELECT * FROM (" + unionSql + ") ORDER BY timestamp"
      );
      const data = result.toArray();
      console.log("overlay.js: loaded", data.length, "cycle summary rows");

      for (const d of data) {
        allSummaryRows.push({
          pump_event_id: Number(d.pump_event_id),
          timestamp: new Date(
            Number(
              typeof d.timestamp === "bigint"
                ? d.timestamp / 1000000n
                : d.timestamp
            )
          ),
          month: Number(d.month),
          day_of_year: Number(d.day_of_year),
          draw_duration_min: Number(d.draw_duration_s) / 60.0,
          recovery_duration_min: Number(d.recovery_duration_s) / 60.0,
          total_duration_min: Number(d.total_duration_s) / 60.0,
          max_drawdown: Number(d.max_drawdown),
          depth_at_start: Number(d.depth_at_start),
          static_depth: d.static_depth != null ? Number(d.static_depth) : null,
          min_depth: Number(d.min_depth),
          num_points: Number(d.num_points),
          duty_cycle: d.duty_cycle != null ? Number(d.duty_cycle) : null,
          inter_pump_min: d.inter_pump_s != null ? Number(d.inter_pump_s) / 60.0 : null,
        });
      }
    } catch (e) {
      console.error("Cycle summary query failed:", e);
    }
  }

  if (allPumpRows.length === 0 && allSummaryRows.length === 0) {
    container.innerHTML =
      '<div class="empty-state">No pump cycle data found.</div>';
    return;
  }

  // -- DOM layout -------------------------------------------------------------

  container.innerHTML = "";

  const overviewSection = document.createElement("div");
  overviewSection.className = "overview-section";

  const statusBar = document.createElement("div");
  statusBar.className = "brush-status";
  statusBar.style.cssText =
    "text-align:center;padding:6px 0;font-size:14px;color:var(--fg-muted,#6b7280)";

  const analysisSection = document.createElement("div");
  analysisSection.className = "analysis-section";

  container.appendChild(overviewSection);
  container.appendChild(statusBar);
  container.appendChild(analysisSection);

  // -- "Explore this data" cross-link -----------------------------------------
  // When sitegen emitted an explorer URL (data-explore-url), offer a pivot that
  // hands this page's underlying parquet to the data explorer. The pump-cycles
  // and cycle-summary series have distinct schemas, so each is handed as its own
  // named dataset (a picker) rather than union-ed into one table. The visitor
  // lands on the raw pump-cycles rows and can query/download either dataset.
  const exploreUrl = container.dataset.exploreUrl || "";
  if (exploreUrl && (pumpCyclesFiles.length || cycleSummaryFiles.length)) {
    const handoff = [];
    if (pumpCyclesFiles.length) {
      handoff.push({
        table: "pump_cycles",
        label: "Pump cycles (raw samples)",
        files: pumpCyclesFiles,
        columns: pumpCyclesCols,
      });
    }
    if (cycleSummaryFiles.length) {
      handoff.push({
        table: "cycle_summary",
        label: "Cycle summary (per event)",
        files: cycleSummaryFiles,
        columns: cycleSummaryCols,
      });
    }

    const bar = document.createElement("div");
    bar.className = "overlay-explore-bar";
    bar.style.cssText = "text-align:right;margin:0 0 8px";
    const exploreBtn = document.createElement("button");
    exploreBtn.type = "button";
    exploreBtn.className = "explore-data";
    exploreBtn.textContent = "Explore this data";
    exploreBtn.addEventListener("click", () => {
      const first = handoff[0];
      const params = new URLSearchParams();
      params.set("datasets", JSON.stringify(handoff));
      params.set("dataset", first.table);
      params.set("sql", `SELECT * FROM ${first.table} ORDER BY timestamp`);
      location.assign(`${exploreUrl}#${params.toString()}`);
    });
    bar.appendChild(exploreBtn);
    container.insertBefore(bar, overviewSection);
  }

  // -- Build event ID lookup --------------------------------------------------

  const allEventIds = new Set();
  for (const r of allPumpRows) allEventIds.add(r.event);
  for (const r of allSummaryRows) allEventIds.add(r.pump_event_id);

  // -- Overview chart with Vega interval selection ----------------------------

  // Month legend (always shows all 12 months)
  const legendDiv = document.createElement("div");
  legendDiv.className = "overlay-header";
  legendDiv.innerHTML = buildMonthLegend([1,2,3,4,5,6,7,8,9,10,11,12]);
  overviewSection.appendChild(legendDiv);

  // The Vega view backing the overview; rebuilt when zooming into a selection.
  let overviewView = null;

  if (allSummaryRows.length > 0) {
    const ovHeader = document.createElement("div");
    ovHeader.className = "overlay-header";
    ovHeader.innerHTML =
      "<h3>Well Depth Timeline</h3>" +
      '<p class="chart-subtitle">Brush to select a time range, then ' +
      "release to zoom in. All charts below update to the selection.</p>";
    overviewSection.appendChild(ovHeader);

    // Reset button
    const resetBtn = document.createElement("button");
    resetBtn.textContent = "Reset to full range";
    resetBtn.style.cssText =
      "margin:0 0 8px 60px;padding:4px 12px;font-size:13px;" +
      "cursor:pointer;border:1px solid #999;border-radius:4px;" +
      "background:var(--bg,#fff);color:var(--fg,#333)";
    resetBtn.addEventListener("click", function () {
      embedOverview(null);
      renderAnalysis(null);
    });
    overviewSection.appendChild(resetBtn);

    const ovHolder = document.createElement("div");
    ovHolder.className = "overlay-vega";
    overviewSection.appendChild(ovHolder);

    // Project the summary rows once for the overview: a temporal timestamp
    // (epoch-ms), the per-row month color used verbatim, and the stem top
    // (static depth, falling back to the cycle's start depth).
    const overviewRows = allSummaryRows.map(function (d) {
      return {
        timestamp: +d.timestamp,
        min_depth: d.min_depth,
        static_depth: d.static_depth,
        y_top: d.static_depth != null ? d.static_depth : d.depth_at_start,
        color: monthColor(d.month),
      };
    });

    // (Re)render the overview chart, optionally zoomed to an [t0, t1] epoch-ms
    // window. Each embed replaces the holder's content, so the pointerup
    // listener is attached once to the holder (below), not per embed.
    async function embedOverview(domain) {
      const spec = buildBrushOverviewSpec({
        yDomain: [34, 46],
        height: 160,
        theme: chartTheme,
      });
      if (domain) spec.encoding.x.scale = { domain: domain };
      try {
        const embed = await loadVega();
        if (overviewView) {
          try { overviewView.finalize(); } catch (e) { /* already gone */ }
          overviewView = null;
        }
        ovHolder.style.width = "100%";
        const res = await embed(
          ovHolder,
          { ...spec, data: { values: overviewRows } },
          { actions: false, renderer: "svg" }
        );
        overviewView = res.view;
        refitView(overviewView, ovHolder);
      } catch (e) {
        ovHolder.innerHTML =
          '<div class="empty-state">Overview render failed: ' +
          String((e && e.message) || e) + "</div>";
      }
    }

    // The Vega interval selection updates continuously while dragging; act on
    // release. Read the `brush` signal's x extent, drive the analysis charts,
    // and re-render the overview zoomed into the selection (mirrors the old
    // D3 brush-then-zoom behaviour). A plain click (empty selection) is a no-op.
    let releaseTimer = null;
    ovHolder.addEventListener("pointerup", function () {
      if (releaseTimer) clearTimeout(releaseTimer);
      releaseTimer = setTimeout(function () {
        const sel = overviewView && overviewView.signal("brush");
        const ext = sel && sel.timestamp;
        if (ext && ext.length === 2 && ext[0] !== ext[1]) {
          renderAnalysis([new Date(ext[0]), new Date(ext[1])]);
          embedOverview([ext[0], ext[1]]);
        }
      }, 80);
    });

    await embedOverview(null);
  }

  // -- Chart render functions -------------------------------------------------

  function buildMonthLegend(monthsPresent) {
    return '<div class="overlay-legend">' +
      monthsPresent.map(function (m) {
        return '<span class="legend-item">' +
          '<span class="legend-swatch" style="background:' +
          monthColor(m) + '"></span>' + monthNames[m - 1] + "</span>";
      }).join("") + "</div>";
  }

  function getMonthsPresent(rows) {
    const seen = new Set();
    for (const r of rows) seen.add(r.month);
    return [...seen].sort(function (a, b) { return a - b; });
  }

  function renderPumpOverlay(target, rows) {
    const numEvents = new Set(rows.map(function (r) { return r.event; })).size;
    const monthsPresent = getMonthsPresent(rows);

    const maxMin = Math.min(90, arrayMax(rows, function (r) {
      return r.elapsed_min;
    }));

    const header = document.createElement("div");
    header.className = "overlay-header";
    header.innerHTML =
      "<h3>Pump Cycle Overlay (" + numEvents + " cycles)</h3>" +
      '<p class="chart-subtitle">Each line starts at its pre-pump ' +
      "static water level. Seasonal variation visible in starting depth.</p>";

    const plot = document.createElement("div");
    plot.className = "overlay-vega";

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    target.appendChild(wrapper);

    const spec = buildMultiLineSpec({
      xField: "elapsed_min",
      yField: "depth",
      xTitle: "Elapsed time (minutes)",
      yTitle: "Well depth (m)",
      xDomain: [-2, maxMin],
      yDomain: [30, 45],
      height: 400,
      theme: chartTheme,
    });
    embedVega(plot, spec, rows, ["elapsed_min", "depth", "event", "color"]);
  }

  function computeDrawStats(rows) {
    const stats = new Map();
    for (const r of rows) {
      if (r.phase !== "draw") continue;
      const s = stats.get(r.event);
      if (!s) {
        stats.set(r.event, {
          count: 1,
          lastElapsed: r.elapsed_min,
          maxDrawdown: r.drawdown,
        });
      } else {
        s.count++;
        s.lastElapsed = r.elapsed_min;
        if (r.drawdown > s.maxDrawdown) s.maxDrawdown = r.drawdown;
      }
    }
    return stats;
  }

  function renderHorner(target, rows, drawStats) {
    const hornerRows = [];
    for (const r of rows) {
      if (r.phase !== "recovery") continue;
      const s = drawStats.get(r.event);
      if (!s || s.count < 3) continue;
      const dtPrime = r.elapsed_min - s.lastElapsed;
      if (dtPrime <= 0) continue;
      const hornerRatio = (s.lastElapsed + dtPrime) / dtPrime;
      if (hornerRatio <= 1) continue;
      hornerRows.push({
        event: r.event,
        color: r.color,
        horner_time: Math.log10(hornerRatio),
        residual_drawdown: s.maxDrawdown - r.drawdown,
      });
    }

    if (hornerRows.length === 0) return;

    const header = document.createElement("div");
    header.className = "overlay-header";
    header.innerHTML =
      "<h3>Horner Plot - Recovery Analysis</h3>" +
      '<p class="chart-subtitle">Residual drawdown vs ' +
      "log\u2081\u2080((t\u209A + \u0394t') / \u0394t'). " +
      "Straight-line slope indicates transmissivity.</p>";

    const plot = document.createElement("div");
    plot.className = "overlay-vega";

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    target.appendChild(wrapper);

    const spec = buildMultiLineSpec({
      xField: "horner_time",
      yField: "residual_drawdown",
      xTitle: "log\u2081\u2080(Horner time ratio)",
      yTitle: "Residual drawdown (m)",
      height: 400,
      theme: chartTheme,
    });
    embedVega(plot, spec, hornerRows, [
      "horner_time",
      "residual_drawdown",
      "event",
      "color",
    ]);
  }

  function renderDrawdownDetail(target, rows) {
    const drawOnly = rows.filter(function (r) {
      return r.phase === "draw" && r.elapsed_min <= 60;
    });
    if (drawOnly.length === 0) return;

    const numEvents = new Set(
      drawOnly.map(function (r) { return r.event; })
    ).size;

    const header = document.createElement("div");
    header.className = "overlay-header";
    header.innerHTML =
      "<h3>Drawdown Phase Detail (" + numEvents + " cycles)</h3>" +
      '<p class="chart-subtitle">Pump-on phase only. Seasonal variation ' +
      "in drawdown rate reflects aquifer recharge state.</p>";

    const plot = document.createElement("div");
    plot.className = "overlay-vega";

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    target.appendChild(wrapper);

    const spec = buildMultiLineSpec({
      xField: "elapsed_min",
      yField: "drawdown",
      xTitle: "Elapsed time (minutes)",
      yTitle: "Drawdown (m)",
      xDomain: [0, Math.min(60, arrayMax(drawOnly, function (r) {
        return r.elapsed_min;
      }))],
      height: 350,
      theme: chartTheme,
    });
    embedVega(plot, spec, drawOnly, ["elapsed_min", "drawdown", "event", "color"]);
  }

  function renderRecoveryDetail(target, rows, drawStats) {
    const recoveryRows = [];
    for (const r of rows) {
      if (r.phase !== "recovery") continue;
      const s = drawStats.get(r.event);
      if (!s || s.count < 3) continue;
      const dtMin = r.elapsed_min - s.lastElapsed;
      if (dtMin <= 0) continue;
      recoveryRows.push({
        event: r.event,
        color: r.color,
        elapsed_min: dtMin,
        recovery: s.maxDrawdown - r.drawdown,
      });
    }

    if (recoveryRows.length === 0) return;

    const header = document.createElement("div");
    header.className = "overlay-header";
    header.innerHTML =
      "<h3>Recovery Phase Detail</h3>" +
      '<p class="chart-subtitle">Water level recovery after pump shutoff. ' +
      "Aligned at pump-off, showing meters recovered vs elapsed time.</p>";

    const plot = document.createElement("div");
    plot.className = "overlay-vega";

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    target.appendChild(wrapper);

    const spec = buildMultiLineSpec({
      xField: "elapsed_min",
      yField: "recovery",
      xTitle: "Minutes since pump off",
      yTitle: "Recovery (m)",
      height: 350,
      theme: chartTheme,
    });
    embedVega(plot, spec, recoveryRows, [
      "elapsed_min",
      "recovery",
      "event",
      "color",
    ]);
  }

  function renderSummaryCharts(target, sRows) {
    if (sRows.length === 0) return;

    // Append a time-series dot+line chart (filled points colored by month over a
    // faint connecting line). Rows carry a per-row `color` so the points use the
    // month palette via the spec's identity color scale.
    function addDotLineChart(headerHtml, rows, yField, yTitle, yDomain) {
      const header = document.createElement("div");
      header.className = "overlay-header";
      header.innerHTML = headerHtml;

      const plot = document.createElement("div");
      plot.className = "overlay-vega";

      const wrapper = document.createElement("div");
      wrapper.className = "chart overlay-chart";
      wrapper.appendChild(header);
      wrapper.appendChild(plot);
      target.appendChild(wrapper);

      const colored = rows.map(function (d) {
        return { timestamp: d.timestamp, color: monthColor(d.month), [yField]: d[yField] };
      });
      const spec = buildDotLineSpec({
        yField: yField,
        yTitle: yTitle,
        yDomain: yDomain,
        height: 300,
        theme: chartTheme,
      });
      embedVega(plot, spec, colored, ["timestamp", yField, "color"]);
    }

    // Duty cycle (draw_duration / inter_pump_interval)
    addDotLineChart(
      "<h3>Pump Duty Cycle</h3>" +
        '<p class="chart-subtitle">Draw duration / time to next pump start. ' +
        "Rising duty cycle indicates increasing demand or a leak.</p>",
      sRows.filter(function (d) { return d.duty_cycle != null; }),
      "duty_cycle",
      "Duty cycle (draw / interval)",
      [0, 1]
    );

    // Max drawdown
    addDotLineChart(
      "<h3>Maximum Drawdown per Cycle</h3>" +
        '<p class="chart-subtitle">Peak drawdown reached during each pump ' +
        "cycle. Deeper drawdown at same duty cycle suggests declining aquifer.</p>",
      sRows,
      "max_drawdown",
      "Max drawdown (m)"
    );

    // Pump duration
    addDotLineChart(
      "<h3>Pump Duration per Cycle</h3>" +
        '<p class="chart-subtitle">Minutes spent pumping per cycle. ' +
        "Longer pump times at same consumption indicate reduced well yield.</p>",
      sRows,
      "draw_duration_min",
      "Draw duration (min)"
    );
  }

  // -- Coordinated render -----------------------------------------------------

  function renderAnalysis(dateRange) {
    let filteredSummary = allSummaryRows;
    let filteredPump = allPumpRows;

    if (dateRange && allSummaryRows.length > 0) {
      const t0 = dateRange[0];
      const t1 = dateRange[1];
      filteredSummary = allSummaryRows.filter(function (r) {
        return r.timestamp >= t0 && r.timestamp <= t1;
      });
      const ids = new Set(
        filteredSummary.map(function (r) { return r.pump_event_id; })
      );
      filteredPump = allPumpRows.filter(function (r) {
        return ids.has(r.event);
      });
    }

    const nCycles = filteredSummary.length ||
      new Set(filteredPump.map(function (r) { return r.event; })).size;
    const totalCycles = allSummaryRows.length || allEventIds.size;

    if (dateRange) {
      const fmt = function (d) {
        return d.toLocaleDateString("en-US", {
          year: "numeric", month: "short",
        });
      };
      statusBar.textContent =
        nCycles + " of " + totalCycles + " cycles selected (" +
        fmt(dateRange[0]) + " \u2013 " + fmt(dateRange[1]) + ")";
    } else {
      statusBar.textContent = "All " + totalCycles + " cycles";
    }

    analysisSection.innerHTML = "";

    if (filteredPump.length > 0) {
      const drawStats = computeDrawStats(filteredPump);
      renderPumpOverlay(analysisSection, filteredPump);
      renderHorner(analysisSection, filteredPump, drawStats);
      renderDrawdownDetail(analysisSection, filteredPump);
      renderRecoveryDetail(analysisSection, filteredPump, drawStats);
    }

    if (filteredSummary.length > 0) {
      renderSummaryCharts(analysisSection, filteredSummary);
    }
  }

  // -- Initial render ---------------------------------------------------------

  renderAnalysis(null);

  } // end renderOverlay
})();
