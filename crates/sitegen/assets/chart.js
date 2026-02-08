// DuckPond sitegen — chart.js
// Reads the chart-data JSON manifest, loads .parquet files via DuckDB-WASM,
// renders time-series line charts with Observable Plot.
// Supports duration buttons (1W … 1Y) and click-drag brush-to-zoom.

(async function () {
  "use strict";

  const manifestEl = document.querySelector('script.chart-data[type="application/json"]');
  if (!manifestEl) return;

  const container = document.getElementById("chart");
  if (!container) return;

  let manifest;
  try {
    manifest = JSON.parse(manifestEl.textContent);
  } catch (e) {
    return;
  }
  if (!manifest || manifest.length === 0) return;

  // ── State ──────────────────────────────────────────────────────────────────

  // Time range options (label, days)
  const ranges = [
    ["1W", 7], ["2W", 14], ["1M", 30], ["3M", 90],
    ["6M", 180], ["1Y", 365],
  ];
  let activeDays = 90;  // Default: 3 months

  // Custom zoom range (set by brush, cleared by Reset or duration button)
  // When non-null, overrides the duration-button window.
  let zoomDomain = null;  // [beginMs, endMs] or null

  // ── Duration button bar ────────────────────────────────────────────────────

  const toolbar = document.createElement("div");
  toolbar.className = "chart-toolbar";
  container.before(toolbar);

  const btnBar = document.createElement("div");
  btnBar.className = "duration-buttons";
  toolbar.appendChild(btnBar);

  ranges.forEach(([text, days]) => {
    const btn = document.createElement("button");
    btn.textContent = text;
    btn.dataset.days = days;
    if (days === activeDays) btn.classList.add("active");
    btn.onclick = () => {
      zoomDomain = null;
      hideResetBtn();
      btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeDays = days;
      renderChart();
    };
    btnBar.appendChild(btn);
  });

  // Reset-zoom button (hidden until a brush selection is made)
  const resetBtn = document.createElement("button");
  resetBtn.className = "reset-zoom";
  resetBtn.textContent = "Reset zoom";
  resetBtn.style.display = "none";
  resetBtn.onclick = () => {
    zoomDomain = null;
    hideResetBtn();
    // Re-activate the current duration button
    btnBar.querySelectorAll("button").forEach(b => {
      b.classList.toggle("active", parseInt(b.dataset.days) === activeDays);
    });
    renderChart();
  };
  toolbar.appendChild(resetBtn);

  function showResetBtn() { resetBtn.style.display = ""; }
  function hideResetBtn() { resetBtn.style.display = "none"; }

  // Status message while loading
  container.innerHTML = '<div class="empty-state">Loading chart data…</div>';

  // ── DuckDB-WASM init ──────────────────────────────────────────────────────

  let db, conn;
  try {
    const duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm");
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(
      URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }))
    );
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
  } catch (e) {
    container.innerHTML = `<div class="empty-state">DuckDB-WASM failed to load: ${e.message}</div>`;
    return;
  }

  // Register each parquet file
  const fileUrls = manifest.map(m => m.file).filter(Boolean);
  if (fileUrls.length === 0) {
    container.innerHTML = '<div class="empty-state">No exported parquet files in manifest.</div>';
    return;
  }

  for (const url of fileUrls) {
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const buf = await resp.arrayBuffer();
      const name = url.replace(/[^a-zA-Z0-9]/g, "_");
      await db.registerFileBuffer(name + ".parquet", new Uint8Array(buf));
    } catch (e) {
      console.warn("Failed to load", url, e);
    }
  }

  // Group manifest entries by resolution (captures[1], e.g. "res=1h").
  const byResolution = new Map();
  for (const m of manifest) {
    const res = (m.captures && m.captures[1]) || "res=1h";
    if (!byResolution.has(res)) byResolution.set(res, []);
    const name = m.file.replace(/[^a-zA-Z0-9]/g, "_") + ".parquet";
    byResolution.get(res).push({
      name,
      start_time: m.start_time || 0,
      end_time: m.end_time || 0,
    });
  }

  // Pick resolution based on the time window (fewer points for wider windows).
  function pickResolution(days) {
    const available = [...byResolution.keys()];
    const prefer = [
      [30, "res=1h"], [60, "res=2h"], [90, "res=4h"],
      [180, "res=12h"], [Infinity, "res=24h"],
    ];
    for (const [maxDays, res] of prefer) {
      if (days <= maxDays && available.includes(res)) return res;
    }
    return available[available.length - 1] || available[0];
  }

  // ── Data query ─────────────────────────────────────────────────────────────

  // For monitoring: the time axis always extends to now.
  const nowMs = Date.now();

  // Query data for a time window [beginMs, endMs].
  // Resolution is chosen automatically based on window width.
  async function queryData(beginMs, endMs) {
    const windowDays = (endMs - beginMs) / 86400000;
    const res = pickResolution(windowDays);
    const entries = byResolution.get(res) || [];

    const filtered = entries.filter(f =>
      f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs)
    );
    const tableNames = filtered.map(f => f.name);
    if (tableNames.length === 0) return [];

    const parts = tableNames.map(t =>
      `SELECT * FROM read_parquet('${t}') WHERE epoch_ms(timestamp) >= ${beginMs} AND epoch_ms(timestamp) <= ${endMs}`
    );
    const sql = parts.join(" UNION ALL BY NAME ") + " ORDER BY timestamp ASC";

    try {
      const result = await conn.query(sql);
      return result.toArray();
    } catch (e) {
      console.error("Query failed:", e);
      return [];
    }
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  const Plot = await import("https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm");

  function toDate(v) {
    return typeof v === "bigint" ? new Date(Number(v / 1000000n)) : new Date(v);
  }

  function legendLabel(base) {
    const parts = base.split(".");
    return parts.length >= 2 ? parts.slice(0, 2).join(" ") : base;
  }

  const PARTITION_COLS = new Set(["year", "month", "day", "hour", "minute"]);

  function groupColumns(row) {
    const stats = new Map();
    for (const col of Object.keys(row)) {
      if (col === "timestamp" || PARTITION_COLS.has(col)) continue;
      if (typeof row[col] !== "number" && typeof row[col] !== "bigint") continue;
      for (const suffix of [".avg", ".min", ".max"]) {
        if (col.endsWith(suffix)) {
          const base = col.slice(0, -suffix.length);
          if (!stats.has(base)) stats.set(base, {});
          stats.get(base)[suffix.slice(1)] = col;
          break;
        }
      }
      if (![".avg", ".min", ".max"].some(s => col.endsWith(s))) {
        stats.set(col, { avg: col });
      }
    }

    const charts = new Map();
    for (const [base, s] of stats) {
      const parts = base.split(".");
      const chartKey = parts.length >= 2 ? parts.slice(-2).join(".") : base;
      if (!charts.has(chartKey)) charts.set(chartKey, []);
      charts.get(chartKey).push({ base, ...s });
    }
    return charts;
  }

  const palette = [
    "var(--accent-color)", "#f472b6", "#34d399", "#fbbf24",
    "#a78bfa", "#fb923c", "#22d3ee", "#e879f9",
  ];

  // ── Brush-to-zoom ─────────────────────────────────────────────────────────

  // Attach a brush overlay to a chart wrapper.  The overlay sits on top of
  // the Plot SVG and translates pixel drag → time domain → re-render.
  //
  // `domainBegin` / `domainEnd` are epoch-ms values that correspond to the
  // left/right edges of the plot area (the SVG viewBox minus margins).
  function attachBrush(wrapper, plotEl, domainBegin, domainEnd, marginLeft, plotWidth) {
    const overlay = document.createElement("div");
    overlay.className = "brush-overlay";
    // Position over just the plot area (inside margins)
    overlay.style.left = marginLeft + "px";
    overlay.style.width = plotWidth + "px";
    wrapper.style.position = "relative";
    wrapper.appendChild(overlay);

    const rect = document.createElement("div");
    rect.className = "brush-rect";
    overlay.appendChild(rect);

    let startX = null;

    overlay.addEventListener("mousedown", e => {
      e.preventDefault();
      startX = e.offsetX;
      rect.style.left = startX + "px";
      rect.style.width = "0";
      rect.style.display = "block";
    });

    overlay.addEventListener("mousemove", e => {
      if (startX === null) return;
      const curX = Math.max(0, Math.min(e.offsetX, plotWidth));
      const left = Math.min(startX, curX);
      const width = Math.abs(curX - startX);
      rect.style.left = left + "px";
      rect.style.width = width + "px";
    });

    const finish = e => {
      if (startX === null) return;
      const endX = Math.max(0, Math.min(e.offsetX, plotWidth));
      const left = Math.min(startX, endX);
      const right = Math.max(startX, endX);
      startX = null;
      rect.style.display = "none";

      // Ignore tiny drags (< 5 px)
      if (right - left < 5) return;

      // Map pixel range → time domain
      const t0 = domainBegin + (left / plotWidth) * (domainEnd - domainBegin);
      const t1 = domainBegin + (right / plotWidth) * (domainEnd - domainBegin);

      zoomDomain = [t0, t1];
      // Deactivate duration buttons, show reset
      btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      showResetBtn();
      renderChart();
    };

    overlay.addEventListener("mouseup", finish);
    overlay.addEventListener("mouseleave", e => {
      if (startX !== null) finish(e);
    });
  }

  // ── Render ─────────────────────────────────────────────────────────────────

  async function renderChart() {
    // Determine the visible x-domain first — this drives both the query
    // (resolution + time filter) and the chart axis.
    const domainEnd = zoomDomain ? zoomDomain[1] : nowMs;
    const domainBegin = zoomDomain ? zoomDomain[0] : (nowMs - activeDays * 86400000);

    const data = await queryData(domainBegin, domainEnd);

    container.innerHTML = "";

    if (data.length === 0) {
      container.innerHTML = '<div class="empty-state">No data for selected time range</div>';
      return;
    }

    const charts = groupColumns(data[0]);

    const width = container.clientWidth - 32;
    const marginLeft = 60;
    const plotAreaWidth = width - marginLeft - 20; // 20 = right margin

    for (const [chartKey, series] of charts) {
      const marks = [];
      series.forEach((s, i) => {
        const color = palette[i % palette.length];
        const label = legendLabel(s.base);

        if (s.min && s.max) {
          marks.push(
            Plot.areaY(data, {
              x: d => toDate(d.timestamp),
              y1: d => Number(d[s.min]),
              y2: d => Number(d[s.max]),
              fill: color,
              fillOpacity: 0.15,
            })
          );
        }

        if (s.avg) {
          marks.push(
            Plot.line(data, {
              x: d => toDate(d.timestamp),
              y: d => Number(d[s.avg]),
              stroke: color,
              strokeWidth: 1.5,
              title: () => label,
            })
          );
        }
      });

      const keyParts = chartKey.split(".");
      const title = keyParts.length >= 2
        ? `${keyParts[0].charAt(0).toUpperCase() + keyParts[0].slice(1)} (${keyParts[1]})`
        : chartKey;

      const legendItems = series.map((s, i) => ({
        label: legendLabel(s.base),
        color: palette[i % palette.length],
      }));

      const header = document.createElement("div");
      header.style.cssText = "display:flex; align-items:baseline; gap:1rem; margin-bottom:0.5rem;";
      header.innerHTML =
        `<h3 style="margin:0; font-size:1.1rem; color:var(--fg-primary)">${title}</h3>` +
        `<span style="font-size:0.8rem; color:var(--fg-muted)">${legendItems.map(l =>
          `<span style="color:${l.color}">●</span> ${l.label}`
        ).join("&emsp;")}</span>`;

      const plot = Plot.plot({
        width,
        height: 300,
        marginLeft,
        style: { background: "transparent", color: "var(--fg-primary)" },
        x: {
          type: "time",
          label: "Date",
          grid: true,
          domain: [domainBegin, domainEnd],
        },
        y: { label: keyParts[keyParts.length - 1] || "", grid: true },
        marks,
      });

      const wrapper = document.createElement("div");
      wrapper.className = "chart";
      wrapper.appendChild(header);
      wrapper.appendChild(plot);
      container.appendChild(wrapper);

      // Attach brush-to-zoom on the rendered SVG
      attachBrush(wrapper, plot, domainBegin, domainEnd, marginLeft, plotAreaWidth);
    }
  }

  await renderChart();
})();
