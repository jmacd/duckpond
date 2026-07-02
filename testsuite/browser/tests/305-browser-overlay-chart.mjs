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
  console.error("No overlay chart pages found in", SITE_ROOT);
  process.exit(1);
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

  // Wait for overlay.js to finish -- either SVG/headers appear (success)
  // or DuckDB initialization fails (error .empty-state).
  // DuckDB-WASM loads from CDN so allow generous timeout.
  //
  // IMPORTANT: We must NOT match .empty-state in the initial selector
  // because the loading placeholder ("Loading pump cycle data...") also
  // uses .empty-state and would match immediately before DuckDB finishes.
  // Instead we wait for success indicators, with a fallback timeout that
  // then checks for error state.
  const rendered = await tab
    .waitForSelector("#overlay-chart svg, #overlay-chart .overlay-header", {
      timeout: 120000,
    })
    .then(() => true)
    .catch(() => false);

  if (!rendered) {
    // Timed out waiting for charts. Check if there's a DuckDB error.
    const earlyError = await tab.evaluate(() => {
      const el = document.querySelector("#overlay-chart .empty-state");
      return el ? el.textContent : null;
    });
    console.log(`  [INFO] chart render timed out after 120s`);
    if (earlyError) {
      console.log(`  [INFO] page shows: ${earlyError.substring(0, 200)}`);
    }
  }

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

  // The analysis charts migrated from Observable Plot to Vega-Lite; Vega's SVG
  // renderer marks its root <svg> with class "marks". Assert at least one is
  // present so the migration can't silently regress to a non-Vega path.
  const vegaSvgCount = await tab.evaluate(
    () => document.querySelectorAll("#overlay-chart .overlay-vega svg.marks").length
  );
  check(vegaSvgCount > 0, `Vega-Lite charts rendered (${vegaSvgCount})`);

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

  // ── "Explore this data" pivot ─────────────────────────────────────────────
  // The overlay chart offers a cross-link to the data explorer when sitegen
  // emitted a data-explore-url. Verify the button exists, then click it and
  // confirm the explorer registers this page's two schemas as separate
  // datasets (pump_cycles + cycle_summary) rather than a single merged view.
  const hasExploreUrl = await tab.evaluate(() => {
    const el = document.getElementById("overlay-chart");
    return Boolean(el && el.dataset.exploreUrl);
  });
  if (hasExploreUrl) {
    const exploreBtn = await tab.$("#overlay-chart button.explore-data");
    check(Boolean(exploreBtn), `"Explore this data" button present`);
    if (exploreBtn) {
      await Promise.all([
        tab.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 30000 }),
        exploreBtn.click(),
      ]);
      // Each overlay page is scoped to its own capture ($0), so it hands the
      // explorer exactly the dataset(s) it loaded: pump_cycles or cycle_summary
      // (the multi-dataset handoff supports 1..N). Confirm the handoff arrived
      // via the URL hash and that the explorer prepended it to the picker as
      // the selected dataset.
      const handed = await tab.evaluate(() => {
        const p = new URLSearchParams(location.hash.replace(/^#/, ""));
        try {
          return JSON.parse(p.get("datasets") || "[]");
        } catch (e) {
          return [];
        }
      });
      check(
        Array.isArray(handed) &&
          handed.length > 0 &&
          handed.every((d) => ["pump_cycles", "cycle_summary"].includes(d.table)),
        `handoff carries page dataset(s) (${handed.map((d) => d.table).join(", ")})`
      );
      await tab
        .waitForSelector("select.explore-dataset option", { timeout: 120000 })
        .catch(() => null);
      const picker = await tab.evaluate(() => {
        const sel = document.querySelector("select.explore-dataset");
        const options = Array.from(sel ? sel.options : []).map((o) => o.textContent);
        const selected = sel ? sel.options[sel.selectedIndex]?.textContent : null;
        return { options, selected };
      });
      check(
        /pump cycles|cycle summary/i.test(picker.selected || ""),
        `explorer selects the handed dataset (${picker.selected})`
      );
    }
  } else {
    console.log("  [INFO] no data-explore-url on overlay chart; skipping pivot");
  }

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
