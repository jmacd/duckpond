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
import { loadVega, buildLineSpec, sanitizeRows } from "./vega-shared.js";

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

  // Time-window control for per-partition lazy fetch. Shown only for temporal
  // datasets (files carry start_time/end_time). Narrowing the window means only
  // the parquet partitions overlapping it are fetched and scanned on the next
  // run, so browsing a large dataset never downloads every partition up front.
  const windowBar = document.createElement("div");
  windowBar.className = "explore-window";
  windowBar.hidden = true;
  const windowLabel = document.createElement("span");
  windowLabel.className = "explore-window-label";
  windowLabel.textContent = "Window";
  const startInput = document.createElement("input");
  startInput.type = "datetime-local";
  startInput.className = "explore-window-input";
  startInput.setAttribute("aria-label", "Window start");
  const windowArrow = document.createElement("span");
  windowArrow.textContent = "→";
  const endInput = document.createElement("input");
  endInput.type = "datetime-local";
  endInput.className = "explore-window-input";
  endInput.setAttribute("aria-label", "Window end");
  const fullRangeBtn = document.createElement("button");
  fullRangeBtn.type = "button";
  fullRangeBtn.className = "explore-window-full";
  fullRangeBtn.textContent = "Full range";
  const windowHint = document.createElement("span");
  windowHint.className = "explore-window-hint";
  windowBar.append(windowLabel, startInput, windowArrow, endInput, fullRangeBtn, windowHint);

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

  // Editable Vega-Lite spec, shown beneath the chart in chart view. The textarea
  // holds the data-less spec; the current query result is injected as the data
  // at render time. "Apply spec" re-renders the edited spec; "Reset to auto"
  // regenerates the spec inferred from the result columns.
  const specPanel = document.createElement("div");
  specPanel.className = "explore-spec";
  specPanel.hidden = true;
  const specLabel = document.createElement("div");
  specLabel.className = "explore-spec-label";
  specLabel.textContent = "Vega-Lite spec";
  const specEditor = document.createElement("textarea");
  specEditor.className = "explore-spec-editor";
  specEditor.spellcheck = false;
  specEditor.rows = 10;
  specEditor.setAttribute("aria-label", "Vega-Lite spec");
  const specButtons = document.createElement("div");
  specButtons.className = "explore-spec-buttons";
  const specApplyBtn = document.createElement("button");
  specApplyBtn.type = "button";
  specApplyBtn.textContent = "Apply spec";
  specApplyBtn.className = "explore-spec-apply";
  const specResetBtn = document.createElement("button");
  specResetBtn.type = "button";
  specResetBtn.textContent = "Reset to auto";
  specResetBtn.className = "explore-spec-reset";
  const specError = document.createElement("div");
  specError.className = "explore-error";
  specError.hidden = true;
  specButtons.append(specApplyBtn, specResetBtn);
  specPanel.append(specLabel, specEditor, specButtons, specError);

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

  container.append(controls, windowBar, columnBar, exampleBar, editor, buttonBar, viewBar, results, chartContainer, specPanel, pager);

  // ── Dataset registration ────────────────────────────────────────────────────

  // Tracks the file-set key currently registered as each dataset's view, so a
  // re-run with an unchanged window can skip rebuilding the view.
  const registeredKey = new Map();
  let lastRows = null;
  let lastFields = null;
  let pageIndex = 0;
  // Wrapped SQL of the last successful run, replayed by the Parquet export so
  // the downloaded file matches the displayed result set.
  let lastSql = null;
  // Current result view: "table" or "chart".
  let viewMode = "table";
  // Whether the user has hand-edited the Vega-Lite spec. While true, the editor
  // contents are used verbatim; while false, the spec is auto-inferred from the
  // result columns on each render.
  let specEdited = false;

  function datasetTable(d, i) {
    // Prefer the manifest-provided table name; fall back to a safe identifier.
    const raw = (d.table || `dataset_${i}`).replace(/[^A-Za-z0-9_]/g, "_");
    return /^[A-Za-z_]/.test(raw) ? raw : `t_${raw}`;
  }

  // A dataset is temporal if any of its files declares a real time range. Files
  // with start_time === 0 are treated as "spans everything" (always included),
  // matching the chart's overlap rule.
  function datasetFiles(d) {
    return Array.isArray(d.files) ? d.files : [];
  }
  function datasetIsTemporal(d) {
    return datasetFiles(d).some((f) => (f.start_time || 0) > 0);
  }
  // Full data span [minStartMs, maxEndMs] across a dataset's temporal files,
  // or null when the dataset carries no time ranges.
  function datasetSpanMs(d) {
    let lo = Infinity;
    let hi = 0;
    for (const f of datasetFiles(d)) {
      if ((f.start_time || 0) > 0) lo = Math.min(lo, f.start_time * 1000);
      if ((f.end_time || 0) > 0) hi = Math.max(hi, f.end_time * 1000);
    }
    return lo === Infinity ? null : { lo, hi: hi || Date.now() };
  }
  // Files overlapping a window [beginMs, endMs]; start_time === 0 always
  // overlaps. With no window, every file is returned.
  function overlappingFiles(d, win) {
    const files = datasetFiles(d);
    if (!win) return files;
    return files.filter(
      (f) =>
        (f.start_time || 0) === 0 ||
        (f.start_time * 1000 <= win.endMs && f.end_time * 1000 >= win.beginMs)
    );
  }

  function parseLocalInput(v) {
    const ms = new Date(v).getTime();
    return Number.isFinite(ms) ? ms : null;
  }
  function pad2(n) {
    return String(n).padStart(2, "0");
  }
  function toLocalInput(ms) {
    const d = new Date(ms);
    return (
      `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}` +
      `T${pad2(d.getHours())}:${pad2(d.getMinutes())}`
    );
  }

  // The window the user has set for the selected dataset, or null for full
  // range. A window that covers the dataset's whole span is treated as null
  // (no pruning), which also avoids dropping boundary partitions to the
  // minute-resolution datetime inputs.
  function currentWindow() {
    if (windowBar.hidden) return null;
    const beginMs = parseLocalInput(startInput.value);
    const endRaw = parseLocalInput(endInput.value);
    if (beginMs == null || endRaw == null || beginMs >= endRaw) return null;
    // Inputs are minute-resolution; extend the end to cover the whole minute.
    const endMs = endRaw + 60000;
    const span = datasetSpanMs(datasets[Number(datasetSelect.value)]);
    if (span && beginMs <= span.lo && endMs >= span.hi) return null;
    return { beginMs, endMs };
  }

  // Register the parquet files for a dataset that overlap the current window and
  // (re)create its view over exactly those files. Fetches are cached, so a
  // wider window only fetches the newly-needed partitions. Returns the view's
  // table name, or throws if no file could be loaded.
  async function ensureDataset(i) {
    const d = datasets[i];
    const table = datasetTable(d, i);
    const win = currentWindow();
    const selected = overlappingFiles(d, win);
    const urls = selected.map((f) => f.url).filter(Boolean);
    const total = datasetFiles(d).length;
    if (urls.length === 0) {
      throw new Error(
        win
          ? "no parquet partitions overlap the selected window"
          : "dataset has no exported parquet files"
      );
    }

    // Skip the rebuild when the exact same file set is already the view.
    const key = urls.slice().sort().join("|");
    if (registeredKey.get(i) === key) {
      updateWindowHint(urls.length, total);
      return table;
    }

    const names = (await Promise.all(urls.map((u) => ensureFile(u)))).filter(Boolean);
    if (names.length === 0) throw new Error("failed to fetch any parquet file for this dataset");

    // union_by_name tolerates per-file schema drift (added/renamed columns
    // across history) by filling absent columns with NULL.
    const list = names.map((n) => `'${n}'`).join(", ");
    await conn.query(
      `CREATE OR REPLACE VIEW ${table} AS ` +
        `SELECT * FROM read_parquet([${list}], union_by_name=true)`
    );
    registeredKey.set(i, key);
    updateWindowHint(names.length, total);
    return table;
  }

  function updateWindowHint(loaded, total) {
    if (windowBar.hidden) {
      windowHint.textContent = "";
      return;
    }
    windowHint.textContent = `Loaded ${loaded} of ${total} partition${total === 1 ? "" : "s"}`;
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
    status.textContent = "Loading data…";
    results.innerHTML = "";

    try {
      // Register the selected dataset's partitions overlapping the current
      // window and refine the typed schema before querying.
      const i = Number(datasetSelect.value);
      const table = await ensureDataset(i);
      await showSchema(table);
      status.textContent = "Running…";
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
      specPanel.hidden = false;
      renderChart();
    } else {
      chartContainer.hidden = true;
      specPanel.hidden = true;
      results.hidden = false;
      renderPage();
    }
  }

  tableViewBtn.addEventListener("click", () => {
    if (viewMode === "table") return;
    viewMode = "table";
    updateViewButtons();
    renderCurrentView();
    writeHash();
  });
  chartViewBtn.addEventListener("click", () => {
    if (viewMode === "chart") return;
    viewMode = "chart";
    updateViewButtons();
    renderCurrentView();
    writeHash();
  });

  // ── Full-screen toggle ───────────────────────────────────────────────────────
  // Mirror chart.js: inject a control into the top bar that expands the explorer
  // to fill the viewport (the explore layout shares .content-page/.blog-post, so
  // the same fixed-overlay CSS applies via the `explore-fullscreen` body class).
  // Re-render the current view on toggle so the Vega chart resizes to fit.
  const topBar = document.querySelector(".top-bar");
  if (topBar) {
    const fsToggle = document.createElement("button");
    fsToggle.type = "button";
    fsToggle.className = "fullscreen-toggle";

    const syncFsLabel = () => {
      const on = document.body.classList.contains("explore-fullscreen");
      fsToggle.innerHTML = on
        ? '<span class="fs-icon">\u00d7</span> Close'
        : 'Full screen <span class="fs-icon">\u2192</span>';
      fsToggle.setAttribute("aria-pressed", on ? "true" : "false");
      fsToggle.title = on ? "Exit full screen (Esc)" : "Expand the explorer to full screen";
    };

    const setFullscreen = (on) => {
      document.body.classList.toggle("explore-fullscreen", on);
      syncFsLabel();
      // The Vega chart sizes to its container width but a fixed height; re-render
      // the current view so the chart picks up the taller full-screen height.
      // The results table reflows via CSS, so re-rendering it is harmless.
      if (lastRows && lastRows.length) renderCurrentView();
    };

    fsToggle.onclick = () =>
      setFullscreen(!document.body.classList.contains("explore-fullscreen"));
    syncFsLabel();

    const backLink = topBar.querySelector(".top-bar-back");
    if (backLink) backLink.insertAdjacentElement("afterend", fsToggle);
    else topBar.insertAdjacentElement("afterbegin", fsToggle);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && document.body.classList.contains("explore-fullscreen")) {
        setFullscreen(false);
      }
    });
  }

  // Compute the inline / full-screen chart height.
  function chartHeight() {
    const fullscreen = document.body.classList.contains("explore-fullscreen");
    return fullscreen ? Math.max(340, window.innerHeight - 320) : 340;
  }

  // Render the current query result as a Vega-Lite chart. When the spec has not
  // been hand-edited, it is auto-inferred from the result columns and the editor
  // is refreshed to show it; otherwise the editor contents are parsed and used
  // verbatim. In both cases the current result rows are injected as the spec's
  // data so the visualization always reflects the latest query.
  async function renderChart() {
    if (!lastRows || !lastFields || !lastRows.length) {
      chartContainer.innerHTML =
        '<div class="empty-state">Run a query to chart its result.</div>';
      return;
    }

    let spec;
    if (specEdited) {
      try {
        spec = JSON.parse(specEditor.value);
      } catch (e) {
        showSpecError(`Spec is not valid JSON: ${e.message || e}`);
        chartContainer.innerHTML =
          '<div class="empty-state">Fix the spec to render the chart.</div>';
        return;
      }
    } else {
      const built = buildLineSpec(lastFields, lastRows, { height: chartHeight() });
      if (built.error) {
        chartContainer.innerHTML = `<div class="empty-state">${escapeHtml(built.error)}</div>`;
        specEditor.value = "";
        return;
      }
      spec = built.spec;
      // Reflect the auto spec in the editor (data-less, so it stays readable),
      // unless the user is actively editing it.
      if (document.activeElement !== specEditor) {
        specEditor.value = JSON.stringify(spec, null, 2);
      }
    }
    clearSpecError();

    chartContainer.innerHTML = '<div class="empty-state">Rendering chart…</div>';
    let embed;
    try {
      embed = await loadVega();
    } catch (e) {
      chartContainer.innerHTML = `<div class="explore-error">Vega failed to load: ${escapeHtml(String(e.message || e))}</div>`;
      return;
    }

    // Inject the current result and force a responsive width. The data is kept
    // out of the editor, so it is supplied here for both the auto and edited
    // spec paths.
    const renderSpec = {
      ...spec,
      width: "container",
      data: { values: sanitizeRows(lastFields, lastRows) },
    };

    try {
      await embed(chartContainer, renderSpec, { actions: false, renderer: "canvas" });
    } catch (e) {
      chartContainer.innerHTML = `<div class="explore-error">Chart render failed: ${escapeHtml(String(e.message || e))}</div>`;
    }
  }

  function showSpecError(msg) {
    specError.textContent = msg;
    specError.hidden = false;
  }
  function clearSpecError() {
    specError.hidden = true;
    specError.textContent = "";
  }

  specApplyBtn.addEventListener("click", () => {
    specEdited = true;
    renderChart();
    // Persist the hand-edited spec so the shared link reproduces it (S3.4).
    writeHash();
  });
  specResetBtn.addEventListener("click", () => {
    specEdited = false;
    clearSpecError();
    renderChart();
    // Drop the spec from the link; it reverts to the auto-inferred spec.
    writeHash();
  });

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
    // Persist the chart view and, when the spec has been hand-edited, the
    // Vega-Lite spec itself, so a shared link reopens the same visualization
    // (the Stage 3 S3.4 round-trip). The auto-inferred spec is not stored: it
    // is regenerated from the result columns, so omitting it keeps links short.
    if (viewMode === "chart") {
      params.set("view", "chart");
      if (specEdited && specEditor.value.trim()) {
        params.set("spec", specEditor.value);
      }
    }
    history.replaceState(null, "", `#${params.toString()}`);
  }

  function readHash() {
    const h = location.hash.replace(/^#/, "");
    if (!h) return null;
    const params = new URLSearchParams(h);
    return {
      dataset: params.get("dataset"),
      sql: params.get("sql"),
      view: params.get("view"),
      spec: params.get("spec"),
    };
  }

  // ── Wiring ──────────────────────────────────────────────────────────────────

  async function selectDataset(i, { run = false } = {}) {
    datasetSelect.value = String(i);
    const d = datasets[i];
    const tableName = datasetTable(d, i);
    // Show the manifest column list immediately; types are refined by a DESCRIBE
    // after the first run, once the dataset's view exists. No parquet is fetched
    // here — registration is deferred to run time and pruned to the window, so
    // browsing datasets never downloads every partition up front.
    showManifestSchema(d, tableName);
    renderExamples(tableName);

    // Configure the time-window control for temporal datasets; hide it for
    // non-temporal ones (the whole dataset is registered on run).
    const span = datasetSpanMs(d);
    if (datasetIsTemporal(d) && span) {
      windowBar.hidden = false;
      startInput.min = toLocalInput(span.lo);
      startInput.max = toLocalInput(span.hi);
      endInput.min = toLocalInput(span.lo);
      endInput.max = toLocalInput(span.hi);
      // Default to the full range so a first query returns all data; narrowing
      // the window then prunes which partitions are fetched.
      startInput.value = toLocalInput(span.lo);
      endInput.value = toLocalInput(span.hi);
      windowHint.textContent = "";
    } else {
      windowBar.hidden = true;
      windowHint.textContent = "";
    }

    if (!editor.value.trim()) editor.value = `SELECT * FROM ${tableName} LIMIT 100`;
    status.textContent = "";
    if (run) await runQuery();
  }

  datasetSelect.addEventListener("change", () => selectDataset(Number(datasetSelect.value)));
  runBtn.addEventListener("click", runQuery);

  // Narrowing/widening the window re-runs the current query over the pruned set
  // of partitions; "Full range" restores the dataset's whole span.
  function onWindowChange() {
    if (lastRows !== null || editor.value.trim()) runQuery();
  }
  startInput.addEventListener("change", onWindowChange);
  endInput.addEventListener("change", onWindowChange);
  fullRangeBtn.addEventListener("click", () => {
    const span = datasetSpanMs(datasets[Number(datasetSelect.value)]);
    if (span) {
      startInput.value = toLocalInput(span.lo);
      endInput.value = toLocalInput(span.hi);
    }
    onWindowChange();
  });

  editor.addEventListener("keydown", (e) => {
    // Ctrl/Cmd+Enter runs the query.
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
  });

  // Initial state: an ad-hoc chart dataset (index 0) wins; else honor a
  // shareable hash; else select the first dataset. A persisted chart view and
  // hand-edited Vega-Lite spec (S3.4) are restored before the first run so the
  // result renders straight into the shared visualization.
  const initHash = readHash();
  if (initHash && initHash.view === "chart") {
    viewMode = "chart";
    if (initHash.spec) {
      specEditor.value = initHash.spec;
      specEdited = true;
    }
    updateViewButtons();
  }
  if (adhocSql !== null) {
    if (adhocSql) editor.value = adhocSql;
    await selectDataset(0, { run: Boolean(adhocSql) });
  } else {
    const hash = initHash;
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
