// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// DuckPond sitegen -- explore.js
//
// A Datasette-style browser data explorer. Reads a `script.datasets` JSON
// manifest emitted by sitegen, registers each dataset's static parquet files
// with DuckDB-WASM as a named view, and gives the visitor a SQL playground
// over those views: a dataset picker, a SQL editor, a paged results grid, a
// CSV download, and a shareable `#dataset=...&sql=...` URL.
//
// DuckDB-WASM init, parquet registration, and CSV serialization are shared
// with chart.js via duckdb-shared.js so the two assets do not diverge.

import { initDuckdb, createFileRegistry, rowsToCsv, rowsToJson } from "./duckdb-shared.js";

(async function () {
  "use strict";

  const container = document.getElementById("explore");
  if (!container) return;

  const manifestEl = document.querySelector('script.datasets[type="application/json"]');
  if (!manifestEl) {
    container.innerHTML = '<div class="empty-state">No datasets manifest on this page.</div>';
    return;
  }

  let datasets;
  try {
    datasets = JSON.parse(manifestEl.textContent);
  } catch (e) {
    container.innerHTML = `<div class="empty-state">Datasets manifest is not valid JSON: ${e.message}</div>`;
    return;
  }
  if (!Array.isArray(datasets) || datasets.length === 0) {
    container.innerHTML = '<div class="empty-state">Datasets manifest is empty.</div>';
    return;
  }

  // ── Ad-hoc dataset handed over by a chart "Explore this data" link ───────────
  // chart.js navigates here with `#label=...&files=<url1,url2,...>&sql=...`. We
  // register those exact parquet files as a `chart_data` view and prepend it to
  // the picker so the visitor lands on the chart's data + window, then can edit
  // the SQL or switch to a configured dataset.
  let adhocSql = null;
  {
    const params = new URLSearchParams(location.hash.replace(/^#/, ""));
    const filesParam = params.get("files");
    if (filesParam) {
      const urls = filesParam.split(",").map((u) => u.trim()).filter(Boolean);
      if (urls.length > 0) {
        datasets.unshift({
          table: "chart_data",
          label: params.get("label") || "Chart data",
          files: urls.map((u) => ({ url: u })),
          columns: [],
        });
        adhocSql = params.get("sql") || "";
      }
    }
  }

  // Default row cap applied when a query has no explicit LIMIT, to protect the
  // tab from accidentally materializing a multi-million-row result.
  const DEFAULT_LIMIT = 1000;

  // Result grid page size: large results are materialized once (bounded by
  // DEFAULT_LIMIT) and paged client-side so the DOM never holds thousands of
  // rows at once.
  const PAGE_SIZE = 100;

  // ── DuckDB-WASM init ────────────────────────────────────────────────────────

  container.innerHTML = '<div class="empty-state">Loading DuckDB-WASM…</div>';
  let db, conn;
  try {
    ({ db, conn } = await initDuckdb());
  } catch (e) {
    container.innerHTML = `<div class="empty-state">DuckDB-WASM failed to load: ${e.message}</div>`;
    return;
  }
  const { ensureFile } = createFileRegistry(db);

  // ── UI scaffold ─────────────────────────────────────────────────────────────

  container.innerHTML = "";

  const controls = document.createElement("div");
  controls.className = "explore-controls";

  const datasetLabel = document.createElement("label");
  datasetLabel.textContent = "Dataset ";
  const datasetSelect = document.createElement("select");
  datasetSelect.className = "explore-dataset";
  datasets.forEach((d, i) => {
    const opt = document.createElement("option");
    opt.value = String(i);
    opt.textContent = d.label || d.table || `dataset ${i}`;
    datasetSelect.appendChild(opt);
  });
  datasetLabel.appendChild(datasetSelect);

  const schemaInfo = document.createElement("div");
  schemaInfo.className = "explore-schema";

  controls.append(datasetLabel, schemaInfo);

  // Clickable column browser: chips are populated per dataset and insert the
  // column name into the SQL editor at the cursor when clicked.
  const columnBar = document.createElement("div");
  columnBar.className = "explore-columns";

  // Canned example queries for the current dataset (Preview / Count / Schema).
  const exampleBar = document.createElement("div");
  exampleBar.className = "explore-examples";

  const editor = document.createElement("textarea");
  editor.className = "explore-sql";
  editor.rows = 6;
  editor.spellcheck = false;
  editor.setAttribute("aria-label", "SQL query");

  const buttonBar = document.createElement("div");
  buttonBar.className = "explore-buttons";

  const runBtn = document.createElement("button");
  runBtn.type = "button";
  runBtn.textContent = "Run";
  runBtn.className = "explore-run";

  const downloadBtn = document.createElement("button");
  downloadBtn.type = "button";
  downloadBtn.textContent = "Download CSV";
  downloadBtn.className = "explore-download";
  downloadBtn.disabled = true;

  const downloadJsonBtn = document.createElement("button");
  downloadJsonBtn.type = "button";
  downloadJsonBtn.textContent = "Download JSON";
  downloadJsonBtn.className = "explore-download-json";
  downloadJsonBtn.disabled = true;

  const downloadParquetBtn = document.createElement("button");
  downloadParquetBtn.type = "button";
  downloadParquetBtn.textContent = "Download Parquet";
  downloadParquetBtn.className = "explore-download-parquet";
  downloadParquetBtn.disabled = true;

  const status = document.createElement("span");
  status.className = "explore-status";

  buttonBar.append(runBtn, downloadBtn, downloadJsonBtn, downloadParquetBtn, status);

  // View switcher: Table (grid) vs Chart (Vega-Lite). The chart view is part of
  // the Stage 3 Vega-Lite spike and lazy-loads the vega bundle on first use.
  const viewBar = document.createElement("div");
  viewBar.className = "explore-views";
  const tableViewBtn = document.createElement("button");
  tableViewBtn.type = "button";
  tableViewBtn.textContent = "Table";
  tableViewBtn.className = "explore-view-table";
  const chartViewBtn = document.createElement("button");
  chartViewBtn.type = "button";
  chartViewBtn.textContent = "Chart";
  chartViewBtn.className = "explore-view-chart";
  viewBar.append(tableViewBtn, chartViewBtn);
  viewBar.hidden = true;

  const results = document.createElement("div");
  results.className = "explore-results";

  // Vega-Lite render target; shown only in chart view.
  const chartContainer = document.createElement("div");
  chartContainer.className = "explore-chart";
  chartContainer.hidden = true;

  // Pager: prev/next + "rows X–Y of N" for the materialized result set.
  const pager = document.createElement("div");
  pager.className = "explore-pager";
  pager.hidden = true;
  const prevBtn = document.createElement("button");
  prevBtn.type = "button";
  prevBtn.textContent = "‹ Prev";
  prevBtn.className = "explore-prev";
  const nextBtn = document.createElement("button");
  nextBtn.type = "button";
  nextBtn.textContent = "Next ›";
  nextBtn.className = "explore-next";
  const pageInfo = document.createElement("span");
  pageInfo.className = "explore-page-info";
  pager.append(prevBtn, pageInfo, nextBtn);

  container.append(controls, columnBar, exampleBar, editor, buttonBar, viewBar, results, chartContainer, pager);

  // ── Dataset registration ────────────────────────────────────────────────────

  // Tracks which dataset indices have had their files registered + view created.
  const registeredDatasets = new Set();
  let lastRows = null;
  let lastFields = null;
  let pageIndex = 0;
  // Wrapped SQL of the last successful run, replayed by the Parquet export so
  // the downloaded file matches the displayed result set.
  let lastSql = null;
  // Current result view: "table" or "chart".
  let viewMode = "table";
  // Lazily-imported vega-embed function (loaded on first chart render).
  let vegaEmbedFn = null;

  function datasetTable(d, i) {
    // Prefer the manifest-provided table name; fall back to a safe identifier.
    const raw = (d.table || `dataset_${i}`).replace(/[^A-Za-z0-9_]/g, "_");
    return /^[A-Za-z_]/.test(raw) ? raw : `t_${raw}`;
  }

  // Register every parquet file for a dataset and (re)create its view. Returns
  // the view's table name, or throws if no files could be loaded.
  async function ensureDataset(i) {
    const d = datasets[i];
    const table = datasetTable(d, i);
    if (registeredDatasets.has(i)) return table;

    const files = Array.isArray(d.files) ? d.files : [];
    const urls = files.map((f) => f.url).filter(Boolean);
    if (urls.length === 0) throw new Error("dataset has no exported parquet files");

    const names = (await Promise.all(urls.map((u) => ensureFile(u)))).filter(Boolean);
    if (names.length === 0) throw new Error("failed to fetch any parquet file for this dataset");

    // union_by_name tolerates per-file schema drift (added/renamed columns
    // across history) by filling absent columns with NULL.
    const list = names.map((n) => `'${n}'`).join(", ");
    await conn.query(
      `CREATE OR REPLACE VIEW ${table} AS ` +
        `SELECT * FROM read_parquet([${list}], union_by_name=true)`
    );
    registeredDatasets.add(i);
    return table;
  }

  // Show the column list for a dataset's view via DESCRIBE. Renders the typed
  // schema text and refreshes the clickable column chips with type tooltips.
  async function showSchema(table) {
    try {
      const res = await conn.query(`DESCRIBE ${table}`);
      const rows = res.toArray();
      const cols = rows.map((r) => `${r.column_name} ${r.column_type}`);
      schemaInfo.textContent = `${table}: ${cols.join(", ")}`;
      renderColumnChips(
        rows.map((r) => ({ name: r.column_name, type: r.column_type }))
      );
    } catch (e) {
      schemaInfo.textContent = "";
    }
  }

  // Show the dataset's columns from the manifest immediately, without a query
  // round-trip. DESCRIBE later refines this with column types once the view is
  // registered. No-op when the manifest carries no column list.
  function showManifestSchema(d, table) {
    const cols = Array.isArray(d.columns) ? d.columns : [];
    if (cols.length === 0) {
      schemaInfo.textContent = "";
      renderColumnChips([]);
      return;
    }
    schemaInfo.textContent = `${table}: ${cols.join(", ")}`;
    renderColumnChips(cols.map((name) => ({ name, type: null })));
  }

  // Render the clickable column browser. Each chip inserts its column name into
  // the SQL editor at the cursor, so a visitor can compose a query by clicking.
  function renderColumnChips(columns) {
    columnBar.innerHTML = "";
    if (!columns.length) {
      columnBar.hidden = true;
      return;
    }
    columnBar.hidden = false;
    for (const c of columns) {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "explore-col-chip";
      chip.textContent = c.name;
      if (c.type) chip.title = `${c.name} ${c.type}`;
      chip.addEventListener("click", () => insertAtCursor(editor, quoteIdent(c.name)));
      columnBar.appendChild(chip);
    }
  }

  // Quote a column identifier for SQL only when it is not a plain identifier
  // (DuckDB accepts double-quoted identifiers for names with dots/spaces).
  function quoteIdent(name) {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
  }

  // Insert text at the textarea's caret, preserving selection semantics.
  function insertAtCursor(ta, text) {
    const start = ta.selectionStart ?? ta.value.length;
    const end = ta.selectionEnd ?? ta.value.length;
    ta.value = ta.value.slice(0, start) + text + ta.value.slice(end);
    const caret = start + text.length;
    ta.selectionStart = ta.selectionEnd = caret;
    ta.focus();
  }

  // Populate the canned example-query bar for the given table name.
  function renderExamples(table) {
    exampleBar.innerHTML = "";
    if (!table) {
      exampleBar.hidden = true;
      return;
    }
    exampleBar.hidden = false;
    const examples = [
      ["Preview", `SELECT * FROM ${table} LIMIT 100`],
      ["Row count", `SELECT count(*) AS rows FROM ${table}`],
      ["Schema", `DESCRIBE ${table}`],
    ];
    for (const [labelText, sql] of examples) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "explore-example";
      btn.textContent = labelText;
      btn.addEventListener("click", () => {
        editor.value = sql;
        runQuery();
      });
      exampleBar.appendChild(btn);
    }
  }

  // ── Query execution ─────────────────────────────────────────────────────────

  // Wrap a query in a default LIMIT if it has none, to bound result size.
  function withDefaultLimit(sql) {
    const trimmed = sql.trim().replace(/;\s*$/, "");
    if (/\blimit\s+\d+/i.test(trimmed)) return trimmed;
    return `SELECT * FROM (\n${trimmed}\n) AS _explore_q LIMIT ${DEFAULT_LIMIT}`;
  }

  // Build a results table element for a slice of rows.
  function buildTable(fields, rows) {
    const table = document.createElement("table");
    table.className = "explore-grid";
    const thead = document.createElement("thead");
    const htr = document.createElement("tr");
    for (const f of fields) {
      const th = document.createElement("th");
      th.textContent = f;
      htr.appendChild(th);
    }
    thead.appendChild(htr);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    for (const r of rows) {
      const tr = document.createElement("tr");
      for (const f of fields) {
        const td = document.createElement("td");
        const v = r[f];
        td.textContent = v == null ? "" : typeof v === "bigint" ? v.toString() : String(v);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    return table;
  }

  // Render the current page of `lastRows` into the results grid and update the
  // pager controls. Hides the pager when the result fits on a single page.
  function renderPage() {
    if (!lastRows || !lastFields) {
      results.innerHTML = "";
      pager.hidden = true;
      return;
    }
    const total = lastRows.length;
    const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));
    pageIndex = Math.min(Math.max(0, pageIndex), pageCount - 1);
    const start = pageIndex * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, total);

    results.innerHTML = "";
    results.appendChild(buildTable(lastFields, lastRows.slice(start, end)));

    if (total > PAGE_SIZE) {
      pager.hidden = false;
      pageInfo.textContent = `rows ${start + 1}–${end} of ${total}`;
      prevBtn.disabled = pageIndex === 0;
      nextBtn.disabled = pageIndex >= pageCount - 1;
    } else {
      pager.hidden = true;
    }
  }

  let running = false;
  async function runQuery() {
    if (running) return;
    const raw = editor.value;
    if (!raw.trim()) return;
    running = true;
    runBtn.disabled = true;
    downloadBtn.disabled = true;
    downloadJsonBtn.disabled = true;
    downloadParquetBtn.disabled = true;
    status.textContent = "Running…";
    results.innerHTML = "";

    try {
      const sql = withDefaultLimit(raw);
      const result = await conn.query(sql);
      lastFields = result.schema.fields.map((f) => f.name);
      lastRows = result.toArray();
      lastSql = sql;
      pageIndex = 0;
      const hasRows = lastRows.length > 0;
      viewBar.hidden = !hasRows;
      updateViewButtons();
      renderCurrentView();
      status.textContent = `${lastRows.length} row${lastRows.length === 1 ? "" : "s"}`;
      downloadBtn.disabled = !hasRows;
      downloadJsonBtn.disabled = !hasRows;
      downloadParquetBtn.disabled = !hasRows;
      writeHash();
    } catch (e) {
      results.innerHTML = `<div class="explore-error">${escapeHtml(String(e.message || e))}</div>`;
      status.textContent = "Error";
      lastRows = null;
      lastFields = null;
      lastSql = null;
      pager.hidden = true;
      viewBar.hidden = true;
      chartContainer.hidden = true;
    } finally {
      running = false;
      runBtn.disabled = false;
    }
  }

  // ── Result views (table / Vega-Lite chart) ───────────────────────────────────

  function updateViewButtons() {
    tableViewBtn.classList.toggle("active", viewMode === "table");
    chartViewBtn.classList.toggle("active", viewMode === "chart");
  }

  function renderCurrentView() {
    if (viewMode === "chart") {
      results.hidden = true;
      pager.hidden = true;
      chartContainer.hidden = false;
      renderChart();
    } else {
      chartContainer.hidden = true;
      results.hidden = false;
      renderPage();
    }
  }

  tableViewBtn.addEventListener("click", () => {
    if (viewMode === "table") return;
    viewMode = "table";
    updateViewButtons();
    renderCurrentView();
  });
  chartViewBtn.addEventListener("click", () => {
    if (viewMode === "chart") return;
    viewMode = "chart";
    updateViewButtons();
    renderCurrentView();
  });

  // Lazily import the vendored vega-embed bundle (only when first needed).
  async function ensureVega() {
    if (vegaEmbedFn) return vegaEmbedFn;
    const mod = await import(/* @vite-ignore */ new URL("./vendor/vega-bundle.mjs", import.meta.url).href);
    vegaEmbedFn = mod.vegaEmbed;
    return vegaEmbedFn;
  }

  // Detect whether a column is numeric / temporal by sampling its first
  // non-null value across the materialized rows.
  function detectKind(field) {
    for (const r of lastRows) {
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
  function inferChartEncoding() {
    const kinds = {};
    for (const f of lastFields) kinds[f] = detectKind(f);
    let xCol = lastFields.find((f) => kinds[f] === "temporal");
    if (!xCol) xCol = lastFields[0];
    const yCols = lastFields.filter((f) => f !== xCol && kinds[f] === "quantitative");
    return { xCol, xKind: kinds[xCol], yCols };
  }

  // Coerce rows to Vega-friendly plain objects: BigInt -> Number, keep Date.
  function sanitizeChartRows(cols) {
    return lastRows.map((r) => {
      const o = {};
      for (const c of cols) {
        const v = r[c];
        o[c] = typeof v === "bigint" ? Number(v) : v;
      }
      return o;
    });
  }

  async function renderChart() {
    if (!lastRows || !lastFields || !lastRows.length) {
      chartContainer.innerHTML = '<div class="empty-state">Run a query to chart its result.</div>';
      return;
    }
    const { xCol, xKind, yCols } = inferChartEncoding();
    if (!yCols.length) {
      chartContainer.innerHTML =
        '<div class="empty-state">No numeric column to plot. The chart view needs at least one numeric column.</div>';
      return;
    }
    chartContainer.innerHTML = '<div class="empty-state">Rendering chart…</div>';
    let embed;
    try {
      embed = await ensureVega();
    } catch (e) {
      chartContainer.innerHTML = `<div class="explore-error">Vega failed to load: ${escapeHtml(String(e.message || e))}</div>`;
      return;
    }

    const cols = [xCol, ...yCols];
    const values = sanitizeChartRows(cols);
    const multi = yCols.length > 1;
    const spec = {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      width: "container",
      height: 340,
      data: { values },
      transform: multi ? [{ fold: yCols, as: ["series", "value"] }] : [],
      mark: { type: "line", clip: true, tooltip: true },
      encoding: {
        x: { field: xCol, type: xKind === "temporal" ? "temporal" : "quantitative", title: xCol },
        y: multi
          ? { field: "value", type: "quantitative" }
          : { field: yCols[0], type: "quantitative", title: yCols[0] },
        ...(multi ? { color: { field: "series", type: "nominal", title: null } } : {}),
      },
      // Interval selection bound to scales gives pan + zoom (brush-zoom parity
      // with the Observable Plot chart path -- a key spike comparison point).
      params: [{ name: "grid", select: "interval", bind: "scales" }],
    };

    try {
      await embed(chartContainer, spec, { actions: false, renderer: "canvas" });
    } catch (e) {
      chartContainer.innerHTML = `<div class="explore-error">Chart render failed: ${escapeHtml(String(e.message || e))}</div>`;
    }
  }

  prevBtn.addEventListener("click", () => {
    pageIndex -= 1;
    renderPage();
  });
  nextBtn.addEventListener("click", () => {
    pageIndex += 1;
    renderPage();
  });

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
    );
  }

  // ── Downloads (CSV / JSON / Parquet) ─────────────────────────────────────────

  function triggerDownload(content, mime, ext) {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `query_${Date.now()}.${ext}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  downloadBtn.addEventListener("click", () => {
    if (!lastRows || !lastRows.length) return;
    triggerDownload(rowsToCsv(lastRows), "text/csv;charset=utf-8", "csv");
  });

  downloadJsonBtn.addEventListener("click", () => {
    if (!lastRows || !lastRows.length) return;
    triggerDownload(rowsToJson(lastRows), "application/json;charset=utf-8", "json");
  });

  // Parquet export replays the last successful query through DuckDB's native
  // COPY ... TO writer into a virtual file, then hands the bytes back to the
  // browser. This preserves true column types (unlike the CSV/JSON text paths)
  // and reuses the exact wrapped SQL so the file matches the displayed rows.
  let exportingParquet = false;
  downloadParquetBtn.addEventListener("click", async () => {
    if (exportingParquet || !lastSql) return;
    exportingParquet = true;
    downloadParquetBtn.disabled = true;
    const prevStatus = status.textContent;
    status.textContent = "Exporting Parquet…";
    const vname = `explore_export_${Date.now()}.parquet`;
    try {
      await conn.query(
        `COPY (${lastSql}) TO '${vname}' (FORMAT PARQUET)`
      );
      const buf = await db.copyFileToBuffer(vname);
      triggerDownload(buf, "application/vnd.apache.parquet", "parquet");
      status.textContent = prevStatus;
    } catch (e) {
      status.textContent = `Parquet export failed: ${e.message || e}`;
    } finally {
      try {
        await db.dropFile(vname);
      } catch (e) {
        // Best-effort cleanup; a leftover virtual file is harmless.
      }
      exportingParquet = false;
      downloadParquetBtn.disabled = !lastRows || !lastRows.length;
    }
  });

  // ── Shareable URL state (#dataset=<table>&sql=<encoded>) ─────────────────────

  function writeHash() {
    const i = Number(datasetSelect.value);
    const d = datasets[i];
    const params = new URLSearchParams();
    params.set("dataset", datasetTable(d, i));
    params.set("sql", editor.value);
    // For the ad-hoc chart dataset, also keep its file list so the link
    // survives a reload or share (the view is registered from these URLs).
    if (adhocSql !== null && i === 0) {
      const urls = (d.files || []).map((f) => f.url).filter(Boolean);
      if (urls.length > 0) {
        params.set("label", d.label || "Chart data");
        params.set("files", urls.join(","));
      }
    }
    history.replaceState(null, "", `#${params.toString()}`);
  }

  function readHash() {
    const h = location.hash.replace(/^#/, "");
    if (!h) return null;
    const params = new URLSearchParams(h);
    return { dataset: params.get("dataset"), sql: params.get("sql") };
  }

  // ── Wiring ──────────────────────────────────────────────────────────────────

  async function selectDataset(i, { run = false } = {}) {
    datasetSelect.value = String(i);
    status.textContent = "Loading dataset…";
    const tableName = datasetTable(datasets[i], i);
    // Show the manifest column list immediately so the schema is visible while
    // the parquet files download; DESCRIBE refines it with types afterward.
    showManifestSchema(datasets[i], tableName);
    renderExamples(tableName);
    try {
      const table = await ensureDataset(i);
      await showSchema(table);
      renderExamples(table);
      if (!editor.value.trim() || !run) {
        if (!editor.value.trim()) editor.value = `SELECT * FROM ${table} LIMIT 100`;
      }
      status.textContent = "";
    } catch (e) {
      schemaInfo.textContent = "";
      status.textContent = `Dataset error: ${e.message}`;
    }
    if (run) await runQuery();
  }

  datasetSelect.addEventListener("change", () => selectDataset(Number(datasetSelect.value)));
  runBtn.addEventListener("click", runQuery);
  editor.addEventListener("keydown", (e) => {
    // Ctrl/Cmd+Enter runs the query.
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
  });

  // Initial state: an ad-hoc chart dataset (index 0) wins; else honor a
  // shareable hash; else select the first dataset.
  if (adhocSql !== null) {
    if (adhocSql) editor.value = adhocSql;
    await selectDataset(0, { run: Boolean(adhocSql) });
  } else {
    const hash = readHash();
    if (hash && hash.dataset) {
      const idx = datasets.findIndex((d, i) => datasetTable(d, i) === hash.dataset);
      const i = idx >= 0 ? idx : 0;
      if (hash.sql) editor.value = hash.sql;
      await selectDataset(i, { run: Boolean(hash.sql) });
    } else {
      await selectDataset(0);
    }
  }
})();
