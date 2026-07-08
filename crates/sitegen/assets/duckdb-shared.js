// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

// Watertown sitegen -- duckdb-shared.js
//
// Shared DuckDB-WASM helpers used by both chart.js and explore.js. Factoring
// the init, file registration, and CSV serialization here keeps the two assets
// from diverging. All vendor URLs resolve relative to this module's location,
// which is the same assets/ directory as chart.js, so the relative paths are
// identical to the originals lifted out of chart.js.

// Initialize a DuckDB-WASM instance and open a connection.
//
// Returns { db, conn }. Throws on failure so the caller can render its own
// error UI (chart.js and explore.js show different empty states).
export async function initDuckdb() {
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
  return { db, conn };
}

// Create a lazy parquet file registrar bound to a DuckDB instance.
//
// Returns { ensureFile }. `ensureFile(url)` fetches a parquet file once,
// registers it with DuckDB under a generated name, and returns that name.
// Results (including failures) are cached so each URL is fetched at most once.
// On failure it returns null and logs to the console.
export function createFileRegistry(db) {
  const registeredNames = new Map();
  let fileIdx = 0;

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
      console.error("duckdb-shared: failed to load parquet file", url, e);
      registeredNames.set(url, null); // cache failure to avoid retries
      return null;
    }
  }

  return { ensureFile };
}

// Serialize an array of row objects to CSV.
//
// Options:
//   excludeCols   - array of column names to omit (e.g. partition columns).
//   timestampCol  - if set, this column is emitted first; remaining columns
//                   follow in sorted order. If unset, all non-excluded columns
//                   are emitted in sorted order.
//   timestampToIso- optional formatter applied to the timestampCol value.
//
// Values are stringified with null -> "" and bigint -> decimal string. Cells
// containing commas, quotes, or newlines are quoted with doubled quotes.
export function rowsToCsv(rows, opts = {}) {
  if (!rows || !rows.length) return "";
  const { timestampCol = null, excludeCols = [], timestampToIso = null } = opts;
  const exclude = new Set(excludeCols);
  if (timestampCol) exclude.add(timestampCol);

  const cols = new Set();
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (exclude.has(k)) continue;
      cols.add(k);
    }
  }
  const valueCols = Array.from(cols).sort();
  const header = timestampCol ? [timestampCol, ...valueCols] : valueCols;

  const esc = (s) => {
    const str = String(s);
    return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };
  const fmt = (v) => (v == null ? "" : typeof v === "bigint" ? v.toString() : String(v));

  const lines = [header.map(esc).join(",")];
  for (const r of rows) {
    const cells = [];
    if (timestampCol) {
      const tv = r[timestampCol];
      cells.push(timestampToIso ? timestampToIso(tv) : fmt(tv));
    }
    for (const c of valueCols) cells.push(fmt(r[c]));
    lines.push(cells.map(esc).join(","));
  }
  return lines.join("\n");
}

// Serialize an array of row objects to a pretty-printed JSON array string.
//
// DuckDB returns 64-bit integers as JS BigInt, which `JSON.stringify` cannot
// serialize. We coerce BigInt to a JS number when it is exactly representable
// (<= 2^53) and to a decimal string otherwise, so large counts/ids survive as
// strings rather than throwing or losing precision silently.
export function rowsToJson(rows) {
  if (!rows || !rows.length) return "[]";
  const replacer = (_key, value) => {
    if (typeof value === "bigint") {
      return value >= -9007199254740991n && value <= 9007199254740991n
        ? Number(value)
        : value.toString();
    }
    return value;
  };
  return JSON.stringify(rows, replacer, 2);
}
