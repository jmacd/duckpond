// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen — chart.js
// Reads the chart-data JSON manifest, loads .parquet files via DuckDB-WASM,
// renders time-series line charts with Observable Plot.
// Supports duration buttons (1W … 1Y) and click-drag brush-to-zoom.
//
// LAZY LOADING: Parquet files are fetched on demand — only the files needed
// for the current resolution and visible time window are loaded. Each
// resolution tier is designed so that a typical screen view spans at most
// one partition boundary, meaning at most 2 files are fetched per render.

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

  // Time range options (label, days).
  // Buttons are filtered at render time — only ranges that fit the actual
  // data span are shown.
  const ranges = [
    ["1W", 7], ["2W", 14], ["1M", 30], ["3M", 90],
    ["6M", 180], ["1Y", 365], ["2Y", 730], ["5Y", 1826],
  ];
  let activeDays = 90;  // Default: 3 months

  // Custom zoom range (set by brush, cleared by Reset or duration button)
  // When non-null, overrides the duration-button window.
  let zoomDomain = null;  // [beginMs, endMs] or null

  // Compute the data time span from the manifest so we can hide buttons
  // for ranges wider than the available data.
  let dataStartMs = Infinity;
  let dataEndMs = 0;
  for (const m of manifest) {
    if (m.start_time > 0) dataStartMs = Math.min(dataStartMs, m.start_time * 1000);
    if (m.end_time > 0) dataEndMs = Math.max(dataEndMs, m.end_time * 1000);
  }
  if (dataStartMs === Infinity) dataStartMs = 0;
  const dataSpanDays = dataEndMs > 0 ? (Date.now() - dataStartMs) / 86400000 : Infinity;

  // ── Duration button bar ────────────────────────────────────────────────────

  const toolbar = document.createElement("div");
  toolbar.className = "chart-toolbar";
  container.before(toolbar);

  const btnBar = document.createElement("div");
  btnBar.className = "duration-buttons";
  toolbar.appendChild(btnBar);

  // Only show buttons whose window fits within the data span (with a small
  // margin — show the button if data covers at least 50% of the window).
  const visibleRanges = ranges.filter(([, days]) => days <= dataSpanDays * 2);

  // If the current default doesn't appear in the visible set, pick the largest.
  if (!visibleRanges.some(([, days]) => days === activeDays) && visibleRanges.length > 0) {
    activeDays = visibleRanges[visibleRanges.length - 1][1];
  }

  visibleRanges.forEach(([text, days]) => {
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

  // Reset-zoom button (disabled until a brush selection is made)
  const resetBtn = document.createElement("button");
  resetBtn.className = "reset-zoom";
  resetBtn.textContent = "Reset zoom";
  resetBtn.disabled = true;
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

  function showResetBtn() { resetBtn.disabled = false; }
  function hideResetBtn() { resetBtn.disabled = true; }

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

  // ── Lazy file loading ─────────────────────────────────────────────────────
  //
  // Files are NOT loaded eagerly. Instead, we index the manifest by resolution
  // and only fetch + register files when they are needed for a query. A cache
  // ensures each file is fetched at most once.

  // Verify manifest has loadable files
  const fileUrls = manifest.map(m => m.file).filter(Boolean);
  if (fileUrls.length === 0) {
    container.innerHTML = '<div class="empty-state">No exported parquet files in manifest.</div>';
    return;
  }

  // Cache: file URL → DuckDB registered name (populated lazily)
  const registeredNames = new Map();
  let fileIdx = 0;

  // Fetch a single parquet file, register it with DuckDB, return its name.
  // Returns null if the fetch fails. Results are cached.
  async function ensureFile(url) {
    if (registeredNames.has(url)) return registeredNames.get(url);
    const duckdbName = `f${fileIdx++}.parquet`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
      const buf = await resp.arrayBuffer();
      await db.registerFileBuffer(duckdbName, new Uint8Array(buf));
      registeredNames.set(url, duckdbName);
      return duckdbName;
    } catch (e) {
      console.error("chart.js: failed to load parquet file", url, e);
      registeredNames.set(url, null); // cache failure to avoid retries
      return null;
    }
  }

  // ── Resolution detection ────────────────────────────────────────────────
  //
  // Find the resolution from captures — it's the capture that starts with
  // "res=".  The index varies by glob pattern, so we detect it dynamically.

  function extractResolution(captures) {
    if (!captures) return null;
    for (const c of captures) {
      if (typeof c === "string" && c.startsWith("res=")) return c;
    }
    return null;
  }

  // Parse a duration string like "1h", "6h", "1d" to seconds.
  function parseDurationSecs(durStr) {
    const m = durStr.match(/^(\d+)([smhd])$/);
    if (!m) return Infinity;
    const n = parseInt(m[1]);
    switch (m[2]) {
      case "s": return n;
      case "m": return n * 60;
      case "h": return n * 3600;
      case "d": return n * 86400;
      default: return Infinity;
    }
  }

  // Group manifest entries by resolution.
  // Entries keep their file URL — the actual fetch happens in queryData().
  const byResolution = new Map();
  for (const m of manifest) {
    const res = extractResolution(m.captures) || "res=1h";
    if (!byResolution.has(res)) byResolution.set(res, []);
    if (!m.file) continue;
    byResolution.get(res).push({
      url: m.file,
      start_time: m.start_time || 0,
      end_time: m.end_time || 0,
    });
  }

  // Sort resolutions finest-first by parsing the duration value.
  const sortedResolutions = [...byResolution.keys()].sort((a, b) => {
    const da = parseDurationSecs(a.replace("res=", ""));
    const db = parseDurationSecs(b.replace("res=", ""));
    return da - db;
  });

  // ── Data-driven resolution picker ─────────────────────────────────────────
  //
  // For a given time window, pick the finest resolution where at most 2
  // manifest files overlap the window. This guarantees at most 2 fetches
  // per render, matching the tiling design.
  //
  // If NO resolution satisfies ≤ 2 files, fall back to the coarsest and
  // warn — this means the data needs a coarser resolution tier or wider
  // partitions.

  function countOverlapping(entries, beginMs, endMs) {
    let count = 0;
    for (const f of entries) {
      if (f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs)) {
        count++;
      }
    }
    return count;
  }

  function pickResolution(beginMs, endMs) {
    // Try finest resolution first — pick the finest with ≤ 2 overlapping files.
    for (const res of sortedResolutions) {
      const entries = byResolution.get(res) || [];
      if (countOverlapping(entries, beginMs, endMs) <= 2) return res;
    }
    // No resolution fits in 2 files — use coarsest and warn.
    const coarsest = sortedResolutions[sortedResolutions.length - 1];
    const entries = byResolution.get(coarsest) || [];
    const n = countOverlapping(entries, beginMs, endMs);
    console.warn(
      `chart.js: no resolution fits within 2 files for this view ` +
      `(coarsest ${coarsest} needs ${n} files). ` +
      `Consider adding a coarser resolution tier or wider partitions.`
    );
    return coarsest;
  }

  // ── Data query ─────────────────────────────────────────────────────────────

  // For monitoring: the time axis always extends to now.
  const nowMs = Date.now();

  // Query data for a time window [beginMs, endMs].
  // Resolution is chosen automatically based on window width.
  // Only the parquet files that overlap the window are fetched (lazy).
  async function queryData(beginMs, endMs) {
    const res = pickResolution(beginMs, endMs);
    const entries = byResolution.get(res) || [];

    // Filter to files whose time range overlaps the query window.
    const overlapping = entries.filter(f =>
      f.start_time === 0 || (f.start_time * 1000 <= endMs && f.end_time * 1000 >= beginMs)
    );

    // Lazily fetch only the overlapping files (parallel).
    const loaded = await Promise.all(overlapping.map(f => ensureFile(f.url)));
    const tableNames = loaded.filter(Boolean);
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

  function groupColumns(data) {
    // Discover numeric columns across ALL rows.  UNION ALL BY NAME fills
    // missing instruments with NULL; typeof null !== "number", so checking
    // only the first row misses series whose data starts later.
    const numericCols = new Set();
    const candidates = new Set();

    for (const col of Object.keys(data[0])) {
      if (col === "timestamp" || PARTITION_COLS.has(col)) continue;
      const v = data[0][col];
      if (typeof v === "number" || typeof v === "bigint") {
        numericCols.add(col);
      } else if (v == null) {
        candidates.add(col);  // null — may be numeric in later rows
      }
    }

    // Resolve remaining candidates by scanning subsequent rows
    for (let i = 1; i < data.length && candidates.size > 0; i++) {
      for (const col of candidates) {
        const v = data[i][col];
        if (v != null) {
          if (typeof v === "number" || typeof v === "bigint") {
            numericCols.add(col);
          }
          candidates.delete(col);
        }
      }
    }

    const stats = new Map();
    for (const col of numericCols) {
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

    const charts = groupColumns(data);

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
              y1: d => d[s.min] != null ? Number(d[s.min]) : NaN,
              y2: d => d[s.max] != null ? Number(d[s.max]) : NaN,
              defined: d => d[s.min] != null && d[s.max] != null,
              fill: color,
              fillOpacity: 0.15,
            })
          );
        }

        if (s.avg) {
          marks.push(
            Plot.line(data, {
              x: d => toDate(d.timestamp),
              y: d => d[s.avg] != null ? Number(d[s.avg]) : NaN,
              defined: d => d[s.avg] != null,
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
