// DuckPond sitegen -- log-viewer.js
// Reads the log-data JSON manifest, loads .parquet files via DuckDB-WASM,
// and renders a scrollable log viewer with unit filtering.
//
// The page must contain:
//   <div id="log-viewer">
//     <script type="application/json" class="log-data">[...]</script>
//   </div>

const PAGE_SIZE = 200;

async function main() {
  const container = document.getElementById("log-viewer");
  if (!container) return;

  const manifestEl = container.querySelector(
    'script.log-data[type="application/json"]'
  );
  if (!manifestEl) return;

  const manifest = JSON.parse(manifestEl.textContent);
  if (!manifest.length) {
    container.innerHTML = "<p>No log files.</p>";
    return;
  }

  // Show loading state
  container.innerHTML = '<p class="log-status">Loading logs...</p>';

  // Load DuckDB-WASM from local vendor files (offline-capable)
  const duckdb = await import(/* @vite-ignore */ "./vendor/duckdb-browser.mjs");
  const bundle = {
    mainModule: new URL("./vendor/duckdb-eh.wasm", import.meta.url).href,
    mainWorker: new URL("./vendor/duckdb-browser-eh.worker.js", import.meta.url).href,
  };
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();

  // Register all parquet files
  for (let i = 0; i < manifest.length; i++) {
    const entry = manifest[i];
    const resp = await fetch(entry.file);
    const buf = await resp.arrayBuffer();
    const fname = `log_${i}.parquet`;
    await db.registerFileBuffer(fname, new Uint8Array(buf));
    await conn.query(
      `CREATE OR REPLACE VIEW log_${i} AS SELECT * FROM read_parquet('${fname}')`
    );
  }

  // Create unified view across all log files
  const viewParts = manifest.map((_, i) => `SELECT * FROM log_${i}`);
  await conn.query(
    `CREATE OR REPLACE VIEW all_logs AS ${viewParts.join(" UNION ALL ")}`
  );

  // Discover columns
  const schemaResult = await conn.query(
    "SELECT column_name FROM information_schema.columns WHERE table_name='all_logs'"
  );
  const columns = [];
  for (let r = 0; r < schemaResult.numRows; r++) {
    columns.push(schemaResult.getChildAt(0).get(r));
  }

  // Discover unique units (look for common unit columns)
  const unitCol = columns.find(
    (c) =>
      c === "_SYSTEMD_UNIT" ||
      c === "SYSLOG_IDENTIFIER" ||
      c === "unit" ||
      c === "_COMM"
  );

  let units = [];
  if (unitCol) {
    const unitResult = await conn.query(
      `SELECT DISTINCT "${unitCol}" AS u FROM all_logs WHERE "${unitCol}" IS NOT NULL ORDER BY u`
    );
    for (let r = 0; r < unitResult.numRows; r++) {
      const val = unitResult.getChildAt(0).get(r);
      if (val) units.push(val);
    }
  }

  // Detect MESSAGE column
  const messageCol = columns.find(
    (c) => c === "MESSAGE" || c === "message" || c === "msg" || c === "body"
  );

  // Detect timestamp column (journald uses __REALTIME_TIMESTAMP in microseconds)
  const tsCol = columns.find(
    (c) =>
      c === "__REALTIME_TIMESTAMP" ||
      c === "timestamp" ||
      c === "Timestamp" ||
      c === "time"
  );

  // Detect priority column
  const priorityCol = columns.find(
    (c) => c === "PRIORITY" || c === "priority" || c === "level"
  );

  // Build UI
  container.innerHTML = "";

  // Controls bar
  const controls = document.createElement("div");
  controls.className = "log-controls";

  // Unit filter pills
  if (units.length > 0) {
    const unitBar = document.createElement("div");
    unitBar.className = "log-unit-bar";

    const allBtn = document.createElement("button");
    allBtn.textContent = "All";
    allBtn.className = "log-unit-pill active";
    allBtn.addEventListener("click", () => {
      for (const b of unitBar.querySelectorAll(".log-unit-pill"))
        b.classList.remove("active");
      allBtn.classList.add("active");
      state.unit = null;
      state.offset = 0;
      loadPage();
    });
    unitBar.appendChild(allBtn);

    for (const unit of units) {
      const btn = document.createElement("button");
      btn.textContent = unit;
      btn.className = "log-unit-pill";
      btn.addEventListener("click", () => {
        for (const b of unitBar.querySelectorAll(".log-unit-pill"))
          b.classList.remove("active");
        btn.classList.add("active");
        state.unit = unit;
        state.offset = 0;
        loadPage();
      });
      unitBar.appendChild(btn);
    }
    controls.appendChild(unitBar);
  }

  // Navigation buttons
  const nav = document.createElement("div");
  nav.className = "log-nav";

  const newerBtn = document.createElement("button");
  newerBtn.textContent = "Newer";
  newerBtn.className = "log-nav-btn";
  newerBtn.addEventListener("click", () => {
    if (state.offset >= PAGE_SIZE) {
      state.offset -= PAGE_SIZE;
      loadPage();
    }
  });

  const olderBtn = document.createElement("button");
  olderBtn.textContent = "Older";
  olderBtn.className = "log-nav-btn";
  olderBtn.addEventListener("click", () => {
    if (state.offset + PAGE_SIZE < state.totalRows) {
      state.offset += PAGE_SIZE;
      loadPage();
    }
  });

  const latestBtn = document.createElement("button");
  latestBtn.textContent = "Latest";
  latestBtn.className = "log-nav-btn";
  latestBtn.addEventListener("click", () => {
    state.offset = 0;
    loadPage();
  });

  const status = document.createElement("span");
  status.className = "log-status";

  nav.appendChild(newerBtn);
  nav.appendChild(olderBtn);
  nav.appendChild(latestBtn);
  nav.appendChild(status);
  controls.appendChild(nav);

  container.appendChild(controls);

  // Log table
  const tableWrap = document.createElement("div");
  tableWrap.className = "log-table-wrap";
  container.appendChild(tableWrap);

  // State
  const state = {
    unit: null,
    offset: 0,
    totalRows: 0,
  };

  // Priority labels and CSS classes
  const priorityClass = {
    0: "log-emerg",
    1: "log-alert",
    2: "log-crit",
    3: "log-err",
    4: "log-warn",
    5: "log-notice",
    6: "log-info",
    7: "log-debug",
  };

  async function loadPage() {
    const where = state.unit
      ? `WHERE "${unitCol}" = '${state.unit.replace(/'/g, "''")}'`
      : "";

    // Get total count
    const countResult = await conn.query(
      `SELECT COUNT(*) AS cnt FROM all_logs ${where}`
    );
    state.totalRows = Number(countResult.getChildAt(0).get(0));

    // Build ORDER BY: most recent first
    const orderBy = tsCol ? `ORDER BY "${tsCol}" DESC` : "";

    const sql = `SELECT * FROM all_logs ${where} ${orderBy} LIMIT ${PAGE_SIZE} OFFSET ${state.offset}`;
    const result = await conn.query(sql);

    // Update status
    const from = state.totalRows - state.offset - result.numRows + 1;
    const to = state.totalRows - state.offset;
    status.textContent = `${Math.max(1, from)}-${to} of ${state.totalRows}`;

    // Update button states
    newerBtn.disabled = state.offset === 0;
    olderBtn.disabled = state.offset + PAGE_SIZE >= state.totalRows;

    // Build table
    const table = document.createElement("table");
    table.className = "log-table";

    // Pick display columns
    const displayCols = [];
    if (tsCol) displayCols.push(tsCol);
    if (unitCol) displayCols.push(unitCol);
    if (priorityCol) displayCols.push(priorityCol);
    if (messageCol) displayCols.push(messageCol);

    // If no recognized columns, show all
    if (displayCols.length === 0) {
      displayCols.push(...columns);
    }

    // Header
    const thead = document.createElement("thead");
    const hrow = document.createElement("tr");
    for (const col of displayCols) {
      const th = document.createElement("th");
      th.textContent = col;
      hrow.appendChild(th);
    }
    thead.appendChild(hrow);
    table.appendChild(thead);

    // Body
    const tbody = document.createElement("tbody");
    const colIndices = displayCols.map((c) => columns.indexOf(c));

    for (let r = 0; r < result.numRows; r++) {
      const tr = document.createElement("tr");

      // Apply priority coloring
      if (priorityCol) {
        const pIdx = columns.indexOf(priorityCol);
        const pVal = result.getChildAt(pIdx).get(r);
        const cls = priorityClass[pVal] || priorityClass[String(pVal)];
        if (cls) tr.className = cls;
      }

      for (const ci of colIndices) {
        const td = document.createElement("td");
        let val = result.getChildAt(ci).get(r);

        // Format timestamp: microseconds since epoch -> ISO string
        if (columns[ci] === tsCol && val) {
          try {
            const us = BigInt(val);
            const ms = Number(us / 1000n);
            val = new Date(ms).toISOString().replace("T", " ").replace("Z", "");
          } catch {
            // leave as-is
          }
        }

        td.textContent = val != null ? String(val) : "";
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);

    tableWrap.innerHTML = "";
    tableWrap.appendChild(table);
  }

  await loadPage();
}

main().catch(console.error);
