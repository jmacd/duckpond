// Noyo Harbor - Shared JavaScript Library
import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28/+esm";

// Re-export Plot for use in pages
export { Plot };

// Time range options
export const timelist = [
  ["1 Week", 7],
  ["2 Weeks", 14],
  ["1 Month", 30],
  ["2 Months", 60],
  ["3 Months", 90],
  ["6 Months", 180],
  ["12 Months", 365],
  ["18 Months", 550],
];

// Simple reactive state
class State {
  constructor(initial) {
    this.value = initial;
    this.subs = new Set();
  }
  
  set(v) {
    this.value = v;
    this.subs.forEach(fn => fn(v));
  }
  
  subscribe(fn) {
    this.subs.add(fn);
    fn(this.value); // Call immediately with current value
    return () => this.subs.delete(fn);
  }
}

// Global time range state
export const timeState = new State(30);

// Create time picker UI
export function createTimePicker(container) {
  const picker = document.createElement("div");
  picker.className = "time-picker";
  picker.innerHTML = `
    <label>Time Range</label>
    <div class="time-picker-options">
      ${timelist.map(([label, days]) => 
        `<button data-days="${days}"${days === timeState.value ? ' class="active"' : ''}>${label}</button>`
      ).join("")}
    </div>
  `;
  
  picker.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", () => {
      picker.querySelectorAll("button").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      timeState.set(parseInt(btn.dataset.days));
    });
  });
  
  container.appendChild(picker);
  return picker;
}

// Get resolution based on time range
export function timerange(days) {
  if (days <= 30) return "1h";
  if (days <= 60) return "2h";
  if (days <= 90) return "4h";
  if (days <= 180) return "12h";
  return "24h";
}

// DuckDB singleton
let duckInstance = null;

export async function getDuckDB() {
  if (!duckInstance) {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
    );
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    duckInstance = await db.connect();
  }
  return duckInstance;
}

// Query parquet files for a time range
export async function queryData(datafiles, days) {
  const res = timerange(days);
  const now = Date.now();
  const begin = now - days * 24 * 3600 * 1000;
  
  console.log(`queryData: days=${days}, res=${res}, now=${now}, begin=${begin}`);
  console.log(`Available resolutions:`, Object.keys(datafiles));
  
  const files = datafiles[`res=${res}`] || [];
  console.log(`Files for res=${res}:`, files.length);
  
  const filtered = files.filter(f => f.start_time * 1000 <= now && f.end_time * 1000 >= begin);
  console.log(`After time filter:`, filtered.length);
  
  const urls = filtered.map(f => f.file);
  
  if (urls.length === 0) {
    console.log('No files match time range');
    return [];
  }
  
  console.log(`Loading ${urls.length} files:`, urls.slice(0, 3));
  
  const duck = await getDuckDB();
  const result = await duck.query(`
    SELECT epoch_us(timestamp)/1000.0::DOUBLE as timestamp, * EXCLUDE timestamp 
    FROM read_parquet(${JSON.stringify(urls)}) 
    WHERE timestamp >= epoch_ms(${begin}) 
    ORDER BY timestamp ASC
  `);
  
  const arr = result.toArray();
  console.log(`Query returned ${arr.length} rows`);
  return arr;
}

// Helper functions
export function legendName(i, n, u) {
  return i.split(".").join(" ");
}

export function plotName(n) {
  if (n === "DO") return "Dissolved Oxygen";
  return n;
}

// Create a chart that updates on time change
export function createChart(container, datafiles, chartConfig) {
  const chartDiv = document.createElement("div");
  chartDiv.className = "chart";
  container.appendChild(chartDiv);
  
  async function render(days) {
    const data = await queryData(datafiles, days);
    chartDiv.innerHTML = "";
    
    if (data.length === 0) {
      chartDiv.innerHTML = `<div class="empty-state">No data for selected time range</div>`;
      return;
    }
    
    const now = Date.now();
    const begin = now - days * 24 * 3600 * 1000;
    const width = chartDiv.clientWidth - 32;
    
    const plot = Plot.plot({
      width,
      ...chartConfig,
      x: { grid: true, type: "time", label: "Date", domain: [begin, now] },
    });
    
    chartDiv.appendChild(plot);
  }
  
  timeState.subscribe(render);
  return chartDiv;
}
