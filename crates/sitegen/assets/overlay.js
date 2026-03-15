// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen -- overlay.js
// Renders pump cycle overlay charts: thousands of aligned drawdown+recovery
// curves plotted against elapsed time, colored by month of year.
//
// Data source: sql-derived-series producing columns:
//   pump_event_id (int), month (int 1-12), elapsed_s (float),
//   drawdown (float), depth (float), phase (string: draw/recovery)
//
// Also supports cycle summary data for duty cycle and statistics charts.

(async function () {
  "use strict";

  try {
    await renderOverlay();
  } catch (e) {
    console.error("overlay.js fatal:", e);
    const c = document.getElementById("overlay-chart");
    if (c) c.innerHTML = `<div class="empty-state">Render error: ${e.message}<br><pre>${e.stack}</pre></div>`;
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
      "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm"
    );
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(
      URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], {
          type: "text/javascript",
        })
      )
    );
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
  } catch (e) {
    container.innerHTML = `<div class="empty-state">DuckDB-WASM failed: ${e.message}</div>`;
    return;
  }

  // -- Load parquet files -----------------------------------------------------

  const registeredNames = new Map();
  let fileIdx = 0;

  async function ensureFile(url) {
    if (registeredNames.has(url)) return registeredNames.get(url);
    const duckdbName = `f${fileIdx++}.parquet`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const buf = await resp.arrayBuffer();
      await db.registerFileBuffer(duckdbName, new Uint8Array(buf));
      registeredNames.set(url, duckdbName);
      return duckdbName;
    } catch (e) {
      console.error("overlay.js: failed to load", url, e);
      registeredNames.set(url, null);
      return null;
    }
  }

  // Load all parquet files (overlay data is typically a single file)
  const fileUrls = manifest.map((m) => m.file).filter(Boolean);
  if (fileUrls.length === 0) {
    container.innerHTML =
      '<div class="empty-state">No parquet files in manifest.</div>';
    return;
  }

  const loaded = await Promise.all(fileUrls.map(ensureFile));
  const tableNames = loaded.filter(Boolean);
  if (tableNames.length === 0) {
    container.innerHTML =
      '<div class="empty-state">Failed to load data files.</div>';
    return;
  }

  const unionSql = tableNames
    .map((t) => `SELECT * FROM read_parquet('${t}')`)
    .join(" UNION ALL BY NAME ");

  // -- Detect data shape ------------------------------------------------------

  let columns;
  try {
    const schemaResult = await conn.query(
      `SELECT * FROM (${unionSql}) LIMIT 1`
    );
    const row = schemaResult.toArray()[0];
    columns = row ? Object.keys(row) : [];
  } catch (e) {
    container.innerHTML = `<div class="empty-state">Query error: ${e.message}</div>`;
    return;
  }

  const hasPumpCycles =
    columns.includes("pump_event_id") && columns.includes("elapsed_s");
  const hasCycleSummary =
    columns.includes("pump_event_id") && columns.includes("draw_duration_s");

  if (!hasPumpCycles && !hasCycleSummary) {
    container.innerHTML =
      '<div class="empty-state">Unrecognized data shape for overlay chart.</div>';
    return;
  }

  // -- Observable Plot --------------------------------------------------------

  const Plot = await import(
    "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm"
  );

  // Month colors: a perceptually distinct palette for 12 months.
  // Designed so adjacent months are distinguishable and the seasonal
  // progression (winter=blue, spring=green, summer=warm, fall=brown) is
  // intuitive.
  const monthColors = [
    "#1e3a5f", // Jan - deep blue (winter)
    "#2563eb", // Feb - bright blue
    "#0891b2", // Mar - teal
    "#059669", // Apr - green (spring)
    "#16a34a", // May - bright green
    "#65a30d", // Jun - lime
    "#eab308", // Jul - yellow (summer)
    "#f97316", // Aug - orange
    "#dc2626", // Sep - red
    "#9333ea", // Oct - purple (fall)
    "#7c3aed", // Nov - violet
    "#4338ca", // Dec - indigo (winter)
  ];

  const monthNames = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  function monthColor(m) {
    return monthColors[(m - 1) % 12];
  }

  // Safe max for large arrays (avoids call stack overflow with spread)
  function arrayMax(arr, fn) {
    let max = -Infinity;
    for (const v of arr) {
      const x = fn(v);
      if (x > max) max = x;
    }
    return max;
  }

  // -- Render pump cycle overlay chart ----------------------------------------

  if (hasPumpCycles) {
    // Query all pump cycle data
    let data;
    try {
      const result = await conn.query(`
        SELECT pump_event_id, month, elapsed_s, depth, static_depth, phase
        FROM (${unionSql})
        WHERE depth > 30 AND depth < 50
        ORDER BY pump_event_id, elapsed_s
      `);
      data = result.toArray();
    } catch (e) {
      container.innerHTML = `<div class="empty-state">Query error: ${e.message}</div>`;
      return;
    }

    if (data.length === 0) {
      container.innerHTML =
        '<div class="empty-state">No pump cycle data found.</div>';
      return;
    }

    console.log("overlay.js: loaded", data.length, "pump cycle rows");
    container.innerHTML = "";

    // Convert data types and add color column for Plot z-channel rendering.
    // Pre-computing color avoids per-event loops -- O(n) instead of O(n*m).
    const rows = [];
    const seenEvents = new Set();
    for (const d of data) {
      const event = Number(d.pump_event_id);
      const month = Number(d.month);
      const color = monthColor(month);
      const staticDepth = Number(d.static_depth);

      // Insert a synthetic pre-pump point at the static water level
      if (!seenEvents.has(event) && staticDepth > 0) {
        seenEvents.add(event);
        rows.push({
          event,
          month,
          color,
          elapsed_min: -1,
          depth: staticDepth,
          drawdown: 0,
          phase: "static",
        });
      }

      rows.push({
        event,
        month,
        color,
        elapsed_min: Number(d.elapsed_s) / 60.0,
        depth: Number(d.depth),
        drawdown: staticDepth - Number(d.depth),
        phase: String(d.phase),
      });
    }

    // Find unique events and their months
    const eventMonths = new Map();
    for (const r of rows) {
      if (!eventMonths.has(r.event)) eventMonths.set(r.event, r.month);
    }
    const numEvents = eventMonths.size;

    // Find months present for legend
    const monthsPresent = [...new Set(eventMonths.values())].sort(
      (a, b) => a - b
    );

    // -- Combined drawdown + recovery overlay ---------------------------------

    const width = container.clientWidth - 32;
    const marginLeft = 60;

    // Cap x-axis at 90 minutes for readability (most cycles complete by then)
    const maxMin = Math.min(
      90,
      arrayMax(rows, (r) => r.elapsed_min)
    );

    // Header with legend
    const header = document.createElement("div");
    header.className = "overlay-header";
    header.innerHTML =
      `<h3>Pump Cycle Overlay (${numEvents} cycles)</h3>` +
      `<p class="chart-subtitle">Each line starts at its pre-pump static water level. ` +
      `Seasonal variation visible in starting depth.</p>` +
      `<div class="overlay-legend">` +
      monthsPresent
        .map(
          (m) =>
            `<span class="legend-item"><span class="legend-swatch" style="background:${monthColor(m)}"></span>${monthNames[m - 1]}</span>`
        )
        .join("") +
      `</div>`;

    // Single Plot.line with z-channel grouping: one SVG path per event,
    // colored by pre-computed month color.  This is O(n) instead of O(n*m).
    const plot = Plot.plot({
      width,
      height: 400,
      marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: {
        label: "Elapsed time (minutes)",
        domain: [-2, maxMin],
        grid: true,
      },
      y: {
        label: "Well depth (m)",
        domain: [30, 45],
        grid: true,
      },
      color: { type: "identity" },
      marks: [
        Plot.line(rows, {
          x: "elapsed_min",
          y: "depth",
          z: "event",
          stroke: "color",
          strokeWidth: 0.8,
          strokeOpacity: 0.4,
        }),
      ],
    });

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    container.appendChild(wrapper);

    // -- Horner plot ----------------------------------------------------------
    // For recovery phase only: x = log10((t_pump + dt') / dt'), y = residual drawdown
    // where t_pump = draw duration, dt' = time since pump stopped

    // Pre-compute per-event draw stats in a single pass
    const eventDrawStats = new Map();
    for (const r of rows) {
      if (r.phase !== "draw") continue;
      const s = eventDrawStats.get(r.event);
      if (!s) {
        eventDrawStats.set(r.event, {
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

    // Build Horner rows in a single pass over recovery data
    const hornerRows = [];
    for (const r of rows) {
      if (r.phase !== "recovery") continue;
      const s = eventDrawStats.get(r.event);
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

    if (hornerRows.length > 0) {
      const hornerHeader = document.createElement("div");
      hornerHeader.className = "overlay-header";
      hornerHeader.innerHTML =
        `<h3>Horner Plot - Recovery Analysis</h3>` +
        `<p class="chart-subtitle">Residual drawdown vs log<sub>10</sub>((t<sub>p</sub> + ` +
        `\u0394t') / \u0394t'). Straight-line slope indicates transmissivity.</p>`;

      const hornerPlot = Plot.plot({
        width,
        height: 400,
        marginLeft,
        style: { background: "transparent", color: "var(--fg)" },
        x: {
          label: "log\u2081\u2080(Horner time ratio)",
          grid: true,
        },
        y: {
          label: "Residual drawdown (m)",
          grid: true,
        },
        color: { type: "identity" },
        marks: [
          Plot.line(hornerRows, {
            x: "horner_time",
            y: "residual_drawdown",
            z: "event",
            stroke: "color",
            strokeWidth: 0.8,
            strokeOpacity: 0.4,
          }),
        ],
      });

      const hornerWrapper = document.createElement("div");
      hornerWrapper.className = "chart overlay-chart";
      hornerWrapper.appendChild(hornerHeader);
      hornerWrapper.appendChild(hornerPlot);
      container.appendChild(hornerWrapper);
    }

    // -- Drawdown-only detail -------------------------------------------------

    const drawOnlyRows = rows.filter(
      (r) => r.phase === "draw" && r.elapsed_min <= 60
    );
    if (drawOnlyRows.length > 0) {
      const drawHeader = document.createElement("div");
      drawHeader.className = "overlay-header";
      drawHeader.innerHTML =
        `<h3>Drawdown Phase Detail (${numEvents} cycles)</h3>` +
        `<p class="chart-subtitle">Pump-on phase only. Seasonal variation in drawdown rate ` +
        `reflects aquifer recharge state.</p>`;

      const drawPlot = Plot.plot({
        width,
        height: 350,
        marginLeft,
        style: { background: "transparent", color: "var(--fg)" },
        x: {
          label: "Elapsed time (minutes)",
          domain: [0, Math.min(60, arrayMax(drawOnlyRows, (r) => r.elapsed_min))],
          grid: true,
        },
        y: {
          label: "Drawdown (m)",
          grid: true,
        },
        color: { type: "identity" },
        marks: [
          Plot.line(drawOnlyRows, {
            x: "elapsed_min",
            y: "drawdown",
            z: "event",
            stroke: "color",
            strokeWidth: 0.8,
            strokeOpacity: 0.4,
          }),
        ],
      });

      const drawWrapper = document.createElement("div");
      drawWrapper.className = "chart overlay-chart";
      drawWrapper.appendChild(drawHeader);
      drawWrapper.appendChild(drawPlot);
      container.appendChild(drawWrapper);
    }

    // -- Recovery-only detail -------------------------------------------------

    // Build recovery-aligned rows in a single pass
    const recoveryOnlyRows = [];
    for (const r of rows) {
      if (r.phase !== "recovery") continue;
      const s = eventDrawStats.get(r.event);
      if (!s || s.count < 3) continue;
      const dtMin = r.elapsed_min - s.lastElapsed;
      if (dtMin <= 0) continue;
      recoveryOnlyRows.push({
        event: r.event,
        color: r.color,
        elapsed_min: dtMin,
        recovery: s.maxDrawdown - r.drawdown,
      });
    }

    if (recoveryOnlyRows.length > 0) {
      const recHeader = document.createElement("div");
      recHeader.className = "overlay-header";
      recHeader.innerHTML =
        `<h3>Recovery Phase Detail</h3>` +
        `<p class="chart-subtitle">Water level recovery after pump shutoff. ` +
        `Aligned at pump-off, showing meters recovered vs elapsed time.</p>`;

      const recPlot = Plot.plot({
        width,
        height: 350,
        marginLeft,
        style: { background: "transparent", color: "var(--fg)" },
        x: {
          label: "Minutes since pump off",
          grid: true,
        },
        y: {
          label: "Recovery (m)",
          grid: true,
        },
        color: { type: "identity" },
        marks: [
          Plot.line(recoveryOnlyRows, {
            x: "elapsed_min",
            y: "recovery",
            z: "event",
            stroke: "color",
            strokeWidth: 0.8,
            strokeOpacity: 0.4,
          }),
        ],
      });

      const recWrapper = document.createElement("div");
      recWrapper.className = "chart overlay-chart";
      recWrapper.appendChild(recHeader);
      recWrapper.appendChild(recPlot);
      container.appendChild(recWrapper);
    }
  }

  // -- Cycle summary charts ---------------------------------------------------

  if (hasCycleSummary) {
    let summaryData;
    try {
      const result = await conn.query(`
        SELECT * FROM (${unionSql}) ORDER BY timestamp
      `);
      summaryData = result.toArray();
    } catch (e) {
      console.error("Cycle summary query failed:", e);
      return;
    }

    if (summaryData.length === 0) return;

    container.innerHTML = "";

    const summaryRows = summaryData.map((d) => ({
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
      min_depth: Number(d.min_depth),
      duty_cycle:
        Number(d.draw_duration_s) /
        (Number(d.draw_duration_s) + Number(d.recovery_duration_s)),
    }));

    const width = container.clientWidth - 32;
    const marginLeft = 60;

    // -- Duty cycle over time -------------------------------------------------

    const dutyHeader = document.createElement("div");
    dutyHeader.className = "overlay-header";
    dutyHeader.innerHTML =
      `<h3>Pump Duty Cycle</h3>` +
      `<p class="chart-subtitle">Fraction of each pump event spent drawing. ` +
      `Rising duty cycle may indicate increasing demand or a leak.</p>`;

    const dutyPlot = Plot.plot({
      width,
      height: 300,
      marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: {
        label: "Duty cycle (draw / total)",
        domain: [0, 1],
        grid: true,
      },
      marks: [
        Plot.dot(summaryRows, {
          x: "timestamp",
          y: "duty_cycle",
          fill: (d) => monthColor(d.month),
          r: 4,
        }),
        Plot.line(summaryRows, {
          x: "timestamp",
          y: "duty_cycle",
          stroke: "#6b7280",
          strokeWidth: 1,
          strokeOpacity: 0.5,
        }),
      ],
    });

    const dutyWrapper = document.createElement("div");
    dutyWrapper.className = "chart overlay-chart";
    dutyWrapper.appendChild(dutyHeader);
    dutyWrapper.appendChild(dutyPlot);
    container.appendChild(dutyWrapper);

    // -- Max drawdown over time -----------------------------------------------

    const ddHeader = document.createElement("div");
    ddHeader.className = "overlay-header";
    ddHeader.innerHTML =
      `<h3>Maximum Drawdown per Cycle</h3>` +
      `<p class="chart-subtitle">Peak drawdown reached during each pump cycle. ` +
      `Deeper drawdown at the same duty cycle suggests declining aquifer.</p>`;

    const ddPlot = Plot.plot({
      width,
      height: 300,
      marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: { label: "Max drawdown (m)", grid: true },
      marks: [
        Plot.dot(summaryRows, {
          x: "timestamp",
          y: "max_drawdown",
          fill: (d) => monthColor(d.month),
          r: 4,
        }),
        Plot.line(summaryRows, {
          x: "timestamp",
          y: "max_drawdown",
          stroke: "#6b7280",
          strokeWidth: 1,
          strokeOpacity: 0.5,
        }),
      ],
    });

    const ddWrapper = document.createElement("div");
    ddWrapper.className = "chart overlay-chart";
    ddWrapper.appendChild(ddHeader);
    ddWrapper.appendChild(ddPlot);
    container.appendChild(ddWrapper);

    // -- Draw duration over time ----------------------------------------------

    const durHeader = document.createElement("div");
    durHeader.className = "overlay-header";
    durHeader.innerHTML =
      `<h3>Pump Duration per Cycle</h3>` +
      `<p class="chart-subtitle">Minutes spent pumping per cycle. ` +
      `Longer pump times at same consumption indicate reduced well yield.</p>`;

    const durPlot = Plot.plot({
      width,
      height: 300,
      marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: { label: "Draw duration (min)", grid: true },
      marks: [
        Plot.dot(summaryRows, {
          x: "timestamp",
          y: "draw_duration_min",
          fill: (d) => monthColor(d.month),
          r: 4,
        }),
        Plot.line(summaryRows, {
          x: "timestamp",
          y: "draw_duration_min",
          stroke: "#6b7280",
          strokeWidth: 1,
          strokeOpacity: 0.5,
        }),
      ],
    });

    const durWrapper = document.createElement("div");
    durWrapper.className = "chart overlay-chart";
    durWrapper.appendChild(durHeader);
    durWrapper.appendChild(durPlot);
    container.appendChild(durWrapper);
  }

  } // end renderOverlay
})();
