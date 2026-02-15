/**
 * Parquet loading invariant test — verifies the ≤2 file property.
 *
 * For any view window, the resolution picker in chart.js should select a
 * resolution where at most 2 parquet files overlap. This test verifies
 * that property by:
 *
 *   1. Loading a data page with network interception enabled
 *   2. Checking that the initial render loads ≤ 2 parquet files
 *   3. Clicking each duration button and verifying ≤ 2 parquet files per render
 *   4. Simulating a brush-zoom drag and verifying ≤ 2 parquet files
 *
 * Usage:
 *   BASE_URL=http://localhost:4174 SITE_ROOT=/tmp/test-output node validate-parquet-loading.mjs
 */
import puppeteer from "puppeteer";
import { readdirSync, statSync, realpathSync } from "node:fs";
import { join, relative } from "node:path";

const BASE_URL = process.env.BASE_URL || "http://localhost:4174";
const BASE_PATH = process.env.BASE_PATH || "/";
const SITE_ROOT = realpathSync(process.env.SITE_ROOT || "/tmp/test-output");

// Find first data page (non-index HTML)
// Find data pages — HTML files that are NOT index.html.
// Walks the site root recursively, skipping build artifacts but NOT the
// data/ directory (which contains both HTML pages and parquet subdirs).
function findDataPages(siteRoot) {
  const results = [];
  function walk(dir) {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      if (statSync(full).isDirectory()) {
        if (["node_modules", ".vite", "assets"].includes(entry)) continue;
        walk(full);
      } else if (entry.endsWith(".html") && entry !== "index.html") {
        results.push(relative(siteRoot, full));
      }
    }
  }
  walk(siteRoot);
  return results;
}

const dataPages = findDataPages(SITE_ROOT);
if (dataPages.length === 0) {
  console.error("No data page found in", SITE_ROOT);
  process.exit(1);
}

console.log(`Discovered ${dataPages.length} data pages in ${SITE_ROOT}:`);
dataPages.forEach((p) => console.log(`  ${p}`));

let pass = 0;
let fail = 0;

function ok(msg) { console.log(`  ✅ ${msg}`); pass++; }
function nok(msg) { console.log(`  ❌ ${msg}`); fail++; }
function check(cond, msg) { cond ? ok(msg) : nok(msg); }

// ── Per-page test ──────────────────────────────────────────────────────

async function testPage(browser, pagePath) {
  const url = `${BASE_URL}${BASE_PATH}${pagePath}`;
  console.log(`\n========== ${pagePath} ==========`);
  console.log(`URL: ${url}`);

  const tab = await browser.newPage();

  // Track .parquet fetches per render cycle
  let renderParquetFetches = [];
  tab.on("response", (res) => {
    const u = res.url();
    if (u.endsWith(".parquet") && res.status() < 400) {
      renderParquetFetches.push(u);
    }
  });

  const jsErrors = [];
  tab.on("pageerror", (err) => jsErrors.push(err.message));

  // ── Load page and wait for chart ──────────────────────────────────

  renderParquetFetches = [];

  await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });

  const chartRendered = await waitForChart(tab);
  check(chartRendered, "chart rendered on initial load");
  if (!chartRendered) {
    console.log("  Skipping — chart did not render");
    await tab.close();
    return;
  }

  await sleep(500);

  console.log(`\n  --- Initial render ---`);
  console.log(`  Parquet files fetched: ${renderParquetFetches.length}`);
  renderParquetFetches.forEach((u) => console.log(`    ${shortUrl(u)}`));
  check(
    renderParquetFetches.length <= 2,
    `initial render loaded ≤ 2 parquet files (got ${renderParquetFetches.length})`
  );

  // ── Duration buttons ──────────────────────────────────────────────

  const buttons = await tab.evaluate(() =>
    [...document.querySelectorAll(".duration-buttons button")]
      .map((b) => ({ text: b.textContent, days: b.dataset.days }))
  );

  console.log(`\n  --- Duration buttons: ${buttons.map((b) => b.text).join(", ")} ---`);

  // Hook console.warn to detect "no resolution fits" violations
  await tab.evaluate(() => {
    window.__parquetWarnings = [];
    const origWarn = console.warn;
    console.warn = function (...args) {
      const msg = args.join(" ");
      if (msg.includes("no resolution fits within 2 files")) {
        window.__parquetWarnings.push(msg);
      }
      origWarn.apply(console, args);
    };
  });

  for (const btn of buttons) {
    renderParquetFetches = [];
    await tab.evaluate(() => { window.__parquetWarnings = []; });

    await tab.evaluate((days) => {
      const b = document.querySelector(`.duration-buttons button[data-days="${days}"]`);
      if (b) b.click();
    }, btn.days);

    await waitForChart(tab);
    await sleep(500);

    const warnings = await tab.evaluate(() => window.__parquetWarnings);

    console.log(`\n  --- ${btn.text} (${btn.days} days) ---`);
    console.log(`  New parquet fetches: ${renderParquetFetches.length}`);
    renderParquetFetches.forEach((u) => console.log(`    ${shortUrl(u)}`));
    check(
      warnings.length === 0,
      `${btn.text}: resolution picker found ≤ 2 file resolution (no warnings)`
    );
    if (warnings.length > 0) {
      warnings.forEach((w) => console.log(`    WARN: ${w}`));
    }
  }

  // ── Brush-to-zoom ─────────────────────────────────────────────────

  console.log(`\n  --- Brush-to-zoom ---`);

  renderParquetFetches = [];
  await tab.evaluate(() => { window.__parquetWarnings = []; });

  const hasBrush = await tab.evaluate(
    () => document.querySelector(".brush-overlay") !== null
  );

  if (hasBrush) {
    const box = await tab.evaluate(() => {
      const el = document.querySelector(".brush-overlay");
      const rect = el.getBoundingClientRect();
      return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
    });

    const startX = box.x + box.width * 0.2;
    const endX = box.x + box.width * 0.4;
    const midY = box.y + box.height / 2;

    await tab.mouse.move(startX, midY);
    await tab.mouse.down();
    await tab.mouse.move(endX, midY, { steps: 10 });
    await tab.mouse.up();

    await waitForChart(tab);
    await sleep(500);

    const warnings = await tab.evaluate(() => window.__parquetWarnings);

    console.log(`  New parquet fetches after zoom: ${renderParquetFetches.length}`);
    renderParquetFetches.forEach((u) => console.log(`    ${shortUrl(u)}`));
    check(
      warnings.length === 0,
      `brush-zoom: resolution picker found ≤ 2 file resolution`
    );
    if (warnings.length > 0) {
      warnings.forEach((w) => console.log(`    WARN: ${w}`));
    }

    // Reset zoom
    renderParquetFetches = [];
    await tab.evaluate(() => { window.__parquetWarnings = []; });

    await tab.evaluate(() => {
      const btn = document.querySelector(".reset-zoom");
      if (btn && !btn.disabled) btn.click();
    });

    await waitForChart(tab);
    await sleep(500);

    const resetWarnings = await tab.evaluate(() => window.__parquetWarnings);
    check(
      resetWarnings.length === 0,
      `reset-zoom: resolution picker found ≤ 2 file resolution`
    );
  } else {
    console.log("  No brush overlay found — skipping zoom test");
  }

  // ── JS errors ─────────────────────────────────────────────────────

  check(jsErrors.length === 0, `no JS errors (got ${jsErrors.length})`);
  jsErrors.forEach((e) => console.log(`    ERROR: ${e}`));

  await tab.close();
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n211 — Parquet loading invariant test`);
  console.log(`Testing ${dataPages.length} data pages\n`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--disable-dev-shm-usage",  // Use /tmp instead of /dev/shm (prevents OOM)
      "--disable-gpu",            // Reduce memory pressure from WASM-heavy pages
      "--no-sandbox",
    ],
  });

  try {
    for (const page of dataPages) {
      try {
        await testPage(browser, page);
      } catch (err) {
        nok(`page crashed: ${page} — ${err.message}`);
      }
    }
  } finally {
    await browser.close();
  }

  console.log(`\n=== Results: ${pass} passed, ${fail} failed ===`);

  if (fail > 0) {
    console.log("\nFAILED");
    process.exit(1);
  }

  console.log("\n=== 211 — Parquet loading invariant PASSED ===");
}

// ── Helpers ────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function shortUrl(url) {
  try {
    return new URL(url).pathname;
  } catch {
    return url;
  }
}

// Wait for chart to finish rendering (SVG exists)
async function waitForChart(tab) {
  return tab.evaluate(() => {
    return new Promise((resolve) => {
      let tries = 0;
      const poll = () => {
        if (document.querySelectorAll(".chart-container svg").length > 0) return resolve(true);
        if (++tries > 60) return resolve(false);
        setTimeout(poll, 500);
      };
      poll();
    });
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
