// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen -- overlay.js
// Interactive pump cycle analysis with coordinated brush+filter.
//
// Top: 4-year well-depth overview with D3 brush for time selection.
// Below: analysis charts (overlay, Horner, drawdown, recovery, summary)
// that re-render when the brush selection changes.
//
// Data sources:
//   pump-cycles: pump_event_id, month, elapsed_s, depth, static_depth, phase
//   cycle-summary: pump_event_id, timestamp, month, draw_duration_s, ...

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
      "./vendor/duckdb-browser.mjs"
    );
    const bundle = {
      mainModule: new URL("./vendor/duckdb-eh.wasm", import.meta.url).href,
      mainWorker: new URL("./vendor/duckdb-browser-eh.worker.js", import.meta.url).href,
    };
    const worker = new Worker(
      URL.createObjectURL(
        new Blob(['importScripts("' + bundle.mainWorker + '");'], {
          type: "text/javascript",
        })
      )
    );
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

  for (const entry of manifest) {
    try {
      const name = await ensureFile(entry);
      const probe = await conn.query(
        "SELECT * FROM read_parquet('" + name + "') LIMIT 1"
      );
      const row = probe.toArray()[0];
      if (!row) continue;
      const cols = Object.keys(row);
      if (cols.includes("elapsed_s") && cols.includes("pump_event_id")) {
        pumpCyclesTables.push(name);
      } else if (cols.includes("draw_duration_s") && cols.includes("pump_event_id")) {
        cycleSummaryTables.push(name);
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

  // -- Import Plot + D3 -------------------------------------------------------

  const { Plot, d3 } = await import("./vendor/plot-d3-bundle.mjs");

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
  const width = container.clientWidth - 32;
  const marginLeft = 60;

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

  // -- Build event ID lookup --------------------------------------------------

  const allEventIds = new Set();
  for (const r of allPumpRows) allEventIds.add(r.event);
  for (const r of allSummaryRows) allEventIds.add(r.pump_event_id);

  // -- Overview chart with D3 brush -------------------------------------------

  // Month legend (always shows all 12 months)
  const legendDiv = document.createElement("div");
  legendDiv.className = "overlay-header";
  legendDiv.innerHTML = buildMonthLegend([1,2,3,4,5,6,7,8,9,10,11,12]);
  overviewSection.appendChild(legendDiv);

  let brushGroup = null;
  let overviewX = null;
  let overviewXAxis = null;
  let overviewLines = null;
  let overviewDots = null;

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
      overviewX.domain(xDomainFull);
      updateOverviewView();
      brushGroup.call(brush.move, null);
      renderAnalysis(null);
    });
    overviewSection.appendChild(resetBtn);

    const ovHeight = 160;
    const margin = { top: 15, right: 20, bottom: 30, left: marginLeft };

    const xDomainFull = d3.extent(allSummaryRows, function (d) {
      return d.timestamp;
    });
    overviewX = d3.scaleTime()
      .domain(xDomainFull.slice())
      .range([margin.left, width - margin.right]);
    const x = overviewX;

    const y = d3.scaleLinear()
      .domain([34, 46])
      .range([ovHeight - margin.bottom, margin.top]);

    const svg = d3.select(overviewSection)
      .append("svg")
      .attr("width", width)
      .attr("height", ovHeight)
      .style("background", "transparent");

    // Clip path for zoomed view
    svg.append("defs").append("clipPath")
      .attr("id", "overview-clip")
      .append("rect")
      .attr("x", margin.left)
      .attr("y", margin.top)
      .attr("width", width - margin.left - margin.right)
      .attr("height", ovHeight - margin.top - margin.bottom);

    const chartArea = svg.append("g")
      .attr("clip-path", "url(#overview-clip)");

    // Vertical lines: static_depth to min_depth per cycle (pump envelope)
    overviewLines = chartArea.selectAll(".range-line")
      .data(allSummaryRows)
      .join("line")
      .attr("x1", function (d) { return x(d.timestamp); })
      .attr("x2", function (d) { return x(d.timestamp); })
      .attr("y1", function (d) {
        return y(d.static_depth != null ? d.static_depth : d.depth_at_start);
      })
      .attr("y2", function (d) { return y(d.min_depth); })
      .attr("stroke", function (d) { return monthColor(d.month); })
      .attr("stroke-width", 1)
      .attr("opacity", 0.4);

    // Static depth dots (water table level)
    overviewDots = chartArea.selectAll(".static-dot")
      .data(allSummaryRows.filter(function (d) {
        return d.static_depth != null;
      }))
      .join("circle")
      .attr("cx", function (d) { return x(d.timestamp); })
      .attr("cy", function (d) { return y(d.static_depth); })
      .attr("r", 1.5)
      .attr("fill", function (d) { return monthColor(d.month); })
      .attr("opacity", 0.7);

    // Axes
    overviewXAxis = svg.append("g")
      .attr("transform", "translate(0," + (ovHeight - margin.bottom) + ")")
      .call(d3.axisBottom(x).ticks(width > 600 ? 10 : 5));

    svg.append("g")
      .attr("transform", "translate(" + margin.left + ",0)")
      .call(d3.axisLeft(y).ticks(5));

    svg.append("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -(ovHeight / 2))
      .attr("y", 14)
      .attr("text-anchor", "middle")
      .attr("fill", "var(--fg,#333)")
      .attr("font-size", "12px")
      .text("Depth (m)");

    function updateOverviewView() {
      overviewLines
        .attr("x1", function (d) { return x(d.timestamp); })
        .attr("x2", function (d) { return x(d.timestamp); });
      overviewDots
        .attr("cx", function (d) { return x(d.timestamp); });
      overviewXAxis.call(d3.axisBottom(x).ticks(width > 600 ? 10 : 5));
    }

    // D3 brush
    let debounceTimer = null;
    let suppressBrushReset = false;
    const brush = d3.brushX()
      .extent([
        [margin.left, margin.top],
        [width - margin.right, ovHeight - margin.bottom],
      ])
      .on("end", function (event) {
        if (debounceTimer) clearTimeout(debounceTimer);
        if (suppressBrushReset) {
          suppressBrushReset = false;
          return;
        }
        debounceTimer = setTimeout(function () {
          if (!event.selection) {
            renderAnalysis(null);
            return;
          }
          const t0 = x.invert(event.selection[0]);
          const t1 = x.invert(event.selection[1]);

          // Zoom the overview to the selection
          overviewX.domain([t0, t1]);
          updateOverviewView();
          suppressBrushReset = true;
          brushGroup.call(brush.move, null);

          renderAnalysis([t0, t1]);
        }, 100);
      });

    brushGroup = svg.append("g").attr("class", "brush").call(brush);

    svg.selectAll(".brush .selection")
      .attr("fill", "steelblue")
      .attr("fill-opacity", 0.15)
      .attr("stroke", "steelblue");
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

    const plot = Plot.plot({
      width: width,
      height: 400,
      marginLeft: marginLeft,
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
    target.appendChild(wrapper);
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

    const plot = Plot.plot({
      width: width,
      height: 400,
      marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { label: "log\u2081\u2080(Horner time ratio)", grid: true },
      y: { label: "Residual drawdown (m)", grid: true },
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

    const wrapper = document.createElement("div");
    wrapper.className = "chart overlay-chart";
    wrapper.appendChild(header);
    wrapper.appendChild(plot);
    target.appendChild(wrapper);
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

    const plot = Plot.plot({
      width: width,
      height: 350,
      marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: {
        label: "Elapsed time (minutes)",
        domain: [0, Math.min(60, arrayMax(drawOnly, function (r) {
          return r.elapsed_min;
        }))],
        grid: true,
      },
      y: { label: "Drawdown (m)", grid: true },
      color: { type: "identity" },
      marks: [
        Plot.line(drawOnly, {
          x: "elapsed_min",
          y: "drawdown",
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
    target.appendChild(wrapper);
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

    const plot = Plot.plot({
      width: width,
      height: 350,
      marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { label: "Minutes since pump off", grid: true },
      y: { label: "Recovery (m)", grid: true },
      color: { type: "identity" },
      marks: [
        Plot.line(recoveryRows, {
          x: "elapsed_min",
          y: "recovery",
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
    target.appendChild(wrapper);
  }

  function renderSummaryCharts(target, sRows) {
    if (sRows.length === 0) return;

    // Duty cycle (draw_duration / inter_pump_interval)
    const dutyRows = sRows.filter(function (d) { return d.duty_cycle != null; });
    const dutyHeader = document.createElement("div");
    dutyHeader.className = "overlay-header";
    dutyHeader.innerHTML =
      "<h3>Pump Duty Cycle</h3>" +
      '<p class="chart-subtitle">Draw duration / time to next pump start. ' +
      "Rising duty cycle indicates increasing demand or a leak.</p>";

    const dutyPlot = Plot.plot({
      width: width, height: 300, marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: { label: "Duty cycle (draw / interval)", domain: [0, 1], grid: true },
      marks: [
        Plot.dot(dutyRows, {
          x: "timestamp", y: "duty_cycle",
          fill: function (d) { return monthColor(d.month); }, r: 4,
        }),
        Plot.line(dutyRows, {
          x: "timestamp", y: "duty_cycle",
          stroke: "#6b7280", strokeWidth: 1, strokeOpacity: 0.5,
        }),
      ],
    });

    const dutyW = document.createElement("div");
    dutyW.className = "chart overlay-chart";
    dutyW.appendChild(dutyHeader);
    dutyW.appendChild(dutyPlot);
    target.appendChild(dutyW);

    // Max drawdown
    const ddHeader = document.createElement("div");
    ddHeader.className = "overlay-header";
    ddHeader.innerHTML =
      "<h3>Maximum Drawdown per Cycle</h3>" +
      '<p class="chart-subtitle">Peak drawdown reached during each pump ' +
      "cycle. Deeper drawdown at same duty cycle suggests declining aquifer.</p>";

    const ddPlot = Plot.plot({
      width: width, height: 300, marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: { label: "Max drawdown (m)", grid: true },
      marks: [
        Plot.dot(sRows, {
          x: "timestamp", y: "max_drawdown",
          fill: function (d) { return monthColor(d.month); }, r: 4,
        }),
        Plot.line(sRows, {
          x: "timestamp", y: "max_drawdown",
          stroke: "#6b7280", strokeWidth: 1, strokeOpacity: 0.5,
        }),
      ],
    });

    const ddW = document.createElement("div");
    ddW.className = "chart overlay-chart";
    ddW.appendChild(ddHeader);
    ddW.appendChild(ddPlot);
    target.appendChild(ddW);

    // Pump duration
    const durHeader = document.createElement("div");
    durHeader.className = "overlay-header";
    durHeader.innerHTML =
      "<h3>Pump Duration per Cycle</h3>" +
      '<p class="chart-subtitle">Minutes spent pumping per cycle. ' +
      "Longer pump times at same consumption indicate reduced well yield.</p>";

    const durPlot = Plot.plot({
      width: width, height: 300, marginLeft: marginLeft,
      style: { background: "transparent", color: "var(--fg)" },
      x: { type: "time", label: "Date", grid: true },
      y: { label: "Draw duration (min)", grid: true },
      marks: [
        Plot.dot(sRows, {
          x: "timestamp", y: "draw_duration_min",
          fill: function (d) { return monthColor(d.month); }, r: 4,
        }),
        Plot.line(sRows, {
          x: "timestamp", y: "draw_duration_min",
          stroke: "#6b7280", strokeWidth: 1, strokeOpacity: 0.5,
        }),
      ],
    });

    const durW = document.createElement("div");
    durW.className = "chart overlay-chart";
    durW.appendChild(durHeader);
    durW.appendChild(durPlot);
    target.appendChild(durW);
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
