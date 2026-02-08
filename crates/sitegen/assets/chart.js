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
  let activeDays = 90;

  // Build the time picker
  const picker = document.querySelector(".time-picker");
  if (picker) {
    const label = document.createElement("label");
    label.textContent = "Time Range";
    picker.appendChild(label);

    const opts = document.createElement("div");
    opts.className = "time-picker-options";
    ranges.forEach(([text, days]) => {
      const btn = document.createElement("button");
      btn.textContent = text;
      btn.dataset.days = days;
      if (days === activeDays) btn.classList.add("active");
      btn.onclick = () => {
        opts.querySelectorAll("button").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        activeDays = days;
        renderChart();
      };
      opts.appendChild(btn);
    });
    picker.appendChild(opts);
  }

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

  // Build a UNION ALL query over all registered files
  const tableNames = fileUrls.map(u => u.replace(/[^a-zA-Z0-9]/g, "_") + ".parquet");

  async function queryData(days) {
    const now = Date.now();
    const begin = days >= 9999 ? 0 : now - days * 86400000;

    const parts = tableNames.map(t =>
      `SELECT * FROM read_parquet('${t}') WHERE epoch_ms(timestamp) >= ${begin}`
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

  async function renderChart() {
    const data = await queryData(activeDays);

    container.innerHTML = "";

    if (data.length === 0) {
      container.innerHTML = '<div class="empty-state">No data for selected time range</div>';
      return;
    }

    // Get all numeric columns (exclude timestamp)
    const cols = Object.keys(data[0]).filter(k => k !== "timestamp" && typeof data[0][k] === "number");

    const now = Date.now();
    const begin = activeDays >= 9999 ? undefined : now - activeDays * 86400000;
    const width = container.clientWidth - 32;

    // One chart per numeric column
    for (const col of cols) {
      const plot = Plot.plot({
        width,
        height: 300,
        marginLeft: 60,
        style: { background: "transparent", color: "var(--fg-primary)" },
        x: {
          type: "time",
          label: "Date",
          grid: true,
          domain: begin ? [begin, now] : undefined,
        },
        y: { label: col, grid: true },
        marks: [
          Plot.line(data, {
            x: d => {
              const v = d.timestamp;
              return typeof v === "bigint" ? new Date(Number(v / 1000000n)) : new Date(v);
            },
            y: d => Number(d[col]),  // ensure numeric even if BigInt
            stroke: "var(--accent-color)",
            strokeWidth: 1.5,
          }),
        ],
      });
      container.appendChild(plot);
    }
  }

  await renderChart();
})();
