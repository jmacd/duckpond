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

  const status = document.createElement("span");
  status.className = "explore-status";

  buttonBar.append(runBtn, downloadBtn, downloadJsonBtn, status);

  const results = document.createElement("div");
  results.className = "explore-results";

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

  container.append(controls, columnBar, exampleBar, editor, buttonBar, results, pager);

  // ── Dataset registration ────────────────────────────────────────────────────

  // Tracks which dataset indices have had their files registered + view created.
  const registeredDatasets = new Set();
  let lastRows = null;
  let lastFields = null;
  let pageIndex = 0;

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
    status.textContent = "Running…";
    results.innerHTML = "";

    try {
      const sql = withDefaultLimit(raw);
      const result = await conn.query(sql);
      lastFields = result.schema.fields.map((f) => f.name);
      lastRows = result.toArray();
      pageIndex = 0;
      renderPage();
      status.textContent = `${lastRows.length} row${lastRows.length === 1 ? "" : "s"}`;
      downloadBtn.disabled = lastRows.length === 0;
      downloadJsonBtn.disabled = lastRows.length === 0;
      writeHash();
    } catch (e) {
      results.innerHTML = `<div class="explore-error">${escapeHtml(String(e.message || e))}</div>`;
      status.textContent = "Error";
      lastRows = null;
      lastFields = null;
      pager.hidden = true;
    } finally {
      running = false;
      runBtn.disabled = false;
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

  // ── Downloads (CSV / JSON) ───────────────────────────────────────────────────

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

  // ── Shareable URL state (#dataset=<table>&sql=<encoded>) ─────────────────────

  function writeHash() {
    const table = datasets[Number(datasetSelect.value)];
    const params = new URLSearchParams();
    params.set("dataset", datasetTable(table, Number(datasetSelect.value)));
    params.set("sql", editor.value);
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

  // Initial state: honor a shareable hash, else select the first dataset.
  const hash = readHash();
  if (hash && hash.dataset) {
    const idx = datasets.findIndex((d, i) => datasetTable(d, i) === hash.dataset);
    const i = idx >= 0 ? idx : 0;
    if (hash.sql) editor.value = hash.sql;
    await selectDataset(i, { run: Boolean(hash.sql) });
  } else {
    await selectDataset(0);
  }
})();
