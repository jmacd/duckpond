/**
 * Overlay chart rendering test -- verifies overlay.js renders pump cycle
 * charts without errors.
 *
 * Looks for pages with an #overlay-chart container, loads them in Puppeteer,
 * and verifies:
 *   1. No JavaScript errors
 *   2. Parquet files are fetched
 *   3. SVG chart elements appear in the DOM
 *   4. "Loading..." message is replaced with actual content
 *
 * Usage:
 *   SITE_ROOT=/tmp/test-output node tests/305-browser-overlay-chart.mjs
 */
import puppeteer from "puppeteer";
import { readdirSync, readFileSync, statSync, realpathSync } from "node:fs";
import { join, relative } from "node:path";

const BASE_URL = process.env.BASE_URL || "http://localhost:4174";
const BASE_PATH = process.env.BASE_PATH || "/";
const SITE_ROOT = realpathSync(process.env.SITE_ROOT || "/tmp/test-output");

// Find pages that contain overlay-chart
function findOverlayPages(siteRoot) {
  const results = [];
  function walk(dir) {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      if (statSync(full).isDirectory()) {
        if (["node_modules", ".vite", "assets"].includes(entry)) continue;
        walk(full);
      } else if (entry.endsWith(".html") && entry !== "index.html") {
        const html = readFileSync(full, "utf8");
        if (html.includes('id="overlay-chart"')) {
          results.push(relative(siteRoot, full));
        }
      }
    }
  }
  walk(siteRoot);
  return results;
}

const overlayPages = findOverlayPages(SITE_ROOT);
if (overlayPages.length === 0) {
  console.log("No overlay chart pages found in", SITE_ROOT, "— skipping");
  process.exit(0);
}

console.log(
  `Discovered ${overlayPages.length} overlay page(s) in ${SITE_ROOT}:`
);
overlayPages.forEach((p) => console.log(`  ${p}`));

let pass = 0;
let fail = 0;

function ok(msg) {
  console.log(`  [PASS] ${msg}`);
  pass++;
}
function nok(msg) {
  console.log(`  [FAIL] ${msg}`);
  fail++;
}
function check(cond, msg) {
  cond ? ok(msg) : nok(msg);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function testPage(browser, pagePath) {
  const url = `${BASE_URL}${BASE_PATH}${pagePath}`;
  console.log(`\n========== ${pagePath} ==========`);
  console.log(`URL: ${url}`);

  const tab = await browser.newPage();

  const parquetFetches = [];
  tab.on("response", (res) => {
    const u = res.url();
    if (u.endsWith(".parquet") && res.status() < 400) {
      parquetFetches.push(u);
    }
  });

  const jsErrors = [];
  tab.on("pageerror", (err) => jsErrors.push(err.message));

  const consoleLogs = [];
  tab.on("console", (msg) => consoleLogs.push(msg.text()));

  await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

  // Wait for overlay.js to finish -- either SVG appears or error shows.
  // DuckDB-WASM loads from CDN so allow generous timeout.
  const rendered = await tab
    .waitForSelector("#overlay-chart svg, #overlay-chart .empty-state", {
      timeout: 120000,
    })
    .then(() => true)
    .catch(() => false);

  await sleep(2000);

  check(jsErrors.length === 0, `no JavaScript errors (got ${jsErrors.length})`);
  if (jsErrors.length > 0) {
    jsErrors.forEach((e) => console.log(`    JS error: ${e}`));
  }

  check(parquetFetches.length > 0, `parquet files loaded (${parquetFetches.length})`);

  // Check for error state
  const errorText = await tab.evaluate(() => {
    const el = document.querySelector("#overlay-chart .empty-state");
    return el ? el.textContent : null;
  });
  check(!errorText, `no error state displayed`);
  if (errorText) {
    console.log(`    Error: ${errorText.substring(0, 200)}`);
  }

  // Check SVG charts rendered
  const svgCount = await tab.evaluate(
    () => document.querySelectorAll("#overlay-chart svg").length
  );
  check(svgCount > 0, `SVG charts rendered (${svgCount})`);

  // Check that "Loading..." is gone
  const loadingVisible = await tab.evaluate(() => {
    const el = document.getElementById("overlay-chart");
    return el && el.textContent.includes("Loading");
  });
  check(!loadingVisible, `loading message cleared`);

  // Check for overlay headers (chart titles)
  const headerCount = await tab.evaluate(
    () => document.querySelectorAll(".overlay-header").length
  );
  check(headerCount > 0, `chart headers present (${headerCount})`);

  // Log console output for debugging
  const overlayLogs = consoleLogs.filter((l) => l.includes("overlay.js"));
  if (overlayLogs.length > 0) {
    console.log(`  Console: ${overlayLogs.join("; ")}`);
  }

  await tab.close();
}

// ── Main ──────────────────────────────────────────────────────────────

const browser = await puppeteer.launch({
  headless: true,
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});

for (const page of overlayPages) {
  await testPage(browser, page);
}

await browser.close();

console.log(`\n========== Summary ==========`);
console.log(`${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
