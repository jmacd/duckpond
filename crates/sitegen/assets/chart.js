// DuckPond sitegen — chart.js
// Reads the chart-data JSON manifest, loads .parquet files via DuckDB-WASM,
// renders time-series line charts with Observable Plot.

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

  // Time range options (label, days)
  const ranges = [
    ["1W", 7], ["2W", 14], ["1M", 30], ["3M", 90],
    ["6M", 180], ["1Y", 365], ["All", 9999],
  ];
  let activeDays = 9999;  // Default: show all data

  // Duration button bar — Noyo-style: window = now minus display width.
  // Built directly inside the chart container, no server-rendered element needed.
  const btnBar = document.createElement("div");
  btnBar.className = "duration-buttons";
  container.before(btnBar);

  ranges.forEach(([text, days]) => {
    const btn = document.createElement("button");
    btn.textContent = text;
    btn.dataset.days = days;
    if (days === activeDays) btn.classList.add("active");
    btn.onclick = () => {
      btnBar.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeDays = days;
      renderChart();
    };
    btnBar.appendChild(btn);
  });

  // Status message while loading
  container.innerHTML = '<div class="empty-state">Loading chart data…</div>';

  // Initialize DuckDB-WASM
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
  // Each entry carries its DuckDB file name and time bounds for filtering.
  const byResolution = new Map();
  for (const m of manifest) {
    const res = (m.captures && m.captures[1]) || "res=1h";
    if (!byResolution.has(res)) byResolution.set(res, []);
    const name = m.file.replace(/[^a-zA-Z0-9]/g, "_") + ".parquet";
    byResolution.get(res).push({
      name,
      start_time: m.start_time || 0,  // epoch seconds
      end_time: m.end_time || 0,
    });
  }

  // Pick resolution based on the time window (fewer points for wider windows).
  // Matches the DuckPond temporal-reduce output resolutions.
  function pickResolution(days) {
    const available = [...byResolution.keys()];
    const prefer = [
      [30, "res=1h"], [60, "res=2h"], [90, "res=4h"],
      [180, "res=12h"], [9999, "res=24h"],
    ];
    for (const [maxDays, res] of prefer) {
      if (days <= maxDays && available.includes(res)) return res;
    }
    // Fall back to coarsest available, then finest
    return available[available.length - 1] || available[0];
  }

  // Compute the latest data timestamp from the manifest (epoch ms).
  // Duration windows anchor to this, not Date.now(), so historical data works.
  let dataEndMs = 0;
  for (const entries of byResolution.values()) {
    for (const f of entries) {
      if (f.end_time > 0) dataEndMs = Math.max(dataEndMs, f.end_time * 1000);
    }
  }
  if (dataEndMs === 0) dataEndMs = Date.now();

  async function queryData(days) {
    const end = dataEndMs;
    const begin = days >= 9999 ? 0 : end - days * 86400000;
    const res = pickResolution(days);
    const entries = byResolution.get(res) || [];

    // Filter to files whose time range overlaps the requested window.
    // start_time/end_time are epoch seconds; end/begin are epoch milliseconds.
    const filtered = entries.filter(f =>
      f.start_time === 0 || (f.start_time * 1000 <= end && f.end_time * 1000 >= begin)
    );
    const tableNames = filtered.map(f => f.name);
    if (tableNames.length === 0) return [];

    const parts = tableNames.map(t =>
      begin > 0
        ? `SELECT * FROM read_parquet('${t}') WHERE epoch_ms(timestamp) >= ${begin}`
        : `SELECT * FROM read_parquet('${t}')`
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

  // Import Observable Plot
  const Plot = await import("https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm");

  // Convert a timestamp value (possibly BigInt nanoseconds) to a JS Date.
  function toDate(v) {
    return typeof v === "bigint" ? new Date(Number(v / 1000000n)) : new Date(v);
  }

  // Pretty-print a column base name: "NorthDock.TempProbe.temperature.C" → "NorthDock TempProbe"
  function legendLabel(base) {
    // The base is "instrument.probe.param.unit" — show the first two parts.
    const parts = base.split(".");
    return parts.length >= 2 ? parts.slice(0, 2).join(" ") : base;
  }

  // Columns injected by Hive partitioning — skip these when grouping.
  const PARTITION_COLS = new Set(["year", "month", "day", "hour", "minute"]);

  // Group columns by measurement.
  //
  // Temporal-reduce outputs columns like:
  //   NorthDock.TempProbe.temperature.C.avg
  //   NorthDock.TempProbe.temperature.C.min
  //   NorthDock.TempProbe.temperature.C.max
  //
  // We group by the base name (everything before the last ".avg"/".min"/".max")
  // and collect the stat suffixes present for each base. Then we group bases
  // that share the same parameter+unit into one chart (e.g., all Temperature.C
  // instruments on one chart).
  function groupColumns(row) {
    const stats = new Map(); // base → { avg?, min?, max? }
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
      // Columns without a stat suffix get their own entry
      if (![".avg", ".min", ".max"].some(s => col.endsWith(s))) {
        stats.set(col, { avg: col });
      }
    }

    // Group bases by param+unit (last two dot-separated parts of the base).
    // e.g., "NorthDock.TempProbe.temperature.C" → key "temperature.C"
    const charts = new Map(); // chartKey → [{ base, avg, min, max }]
    for (const [base, s] of stats) {
      const parts = base.split(".");
      const chartKey = parts.length >= 2 ? parts.slice(-2).join(".") : base;
      if (!charts.has(chartKey)) charts.set(chartKey, []);
      charts.get(chartKey).push({ base, ...s });
    }
    return charts;
  }

  // Palette for multiple series on one chart.
  const palette = [
    "var(--accent-color)", "#f472b6", "#34d399", "#fbbf24",
    "#a78bfa", "#fb923c", "#22d3ee", "#e879f9",
  ];

  async function renderChart() {
    const data = await queryData(activeDays);

    container.innerHTML = "";

    if (data.length === 0) {
      container.innerHTML = '<div class="empty-state">No data for selected time range</div>';
      return;
    }

    const charts = groupColumns(data[0]);

    // Compute actual data time bounds (not Date.now — data may be historical).
    const firstTs = toDate(data[0].timestamp).getTime();
    const lastTs = toDate(data[data.length - 1].timestamp).getTime();
    const dataEnd = lastTs;
    const begin = activeDays >= 9999 ? undefined : dataEnd - activeDays * 86400000;
    const width = container.clientWidth - 32;

    for (const [chartKey, series] of charts) {
      // Build marks: for each series, area band (min–max) + avg line
      const marks = [];
      series.forEach((s, i) => {
        const color = palette[i % palette.length];
        const label = legendLabel(s.base);

        // Min/max shaded band (if both present)
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

        // Avg line (or the only column if no stats)
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

      // Chart title from the key, e.g. "temperature.C" → "Temperature (C)"
      const keyParts = chartKey.split(".");
      const title = keyParts.length >= 2
        ? `${keyParts[0].charAt(0).toUpperCase() + keyParts[0].slice(1)} (${keyParts[1]})`
        : chartKey;

      // Legend entries
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
        marginLeft: 60,
        style: { background: "transparent", color: "var(--fg-primary)" },
        x: {
          type: "time",
          label: "Date",
          grid: true,
          domain: begin ? [begin, dataEnd] : undefined,
        },
        y: { label: keyParts[keyParts.length - 1] || "", grid: true },
        marks,
      });

      const wrapper = document.createElement("div");
      wrapper.className = "chart";
      wrapper.appendChild(header);
      wrapper.appendChild(plot);
      container.appendChild(wrapper);
    }
  }

  await renderChart();
})();
