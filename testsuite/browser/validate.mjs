/**
 * Sitegen browser validation — Puppeteer tests for rendered site.
 *
 * Checks that every page loads, styles are applied, sidebar links work,
 * charts are present on data pages, and no JS errors occur.
 *
 * Usage:
 *   # Start vite first (from the dist directory):
 *   npx vite --port 4174
 *
 *   # Then run validation:
 *   node validate.mjs                              # default localhost:4174
 *   BASE_URL=http://localhost:3000 node validate.mjs  # custom URL
 */
import puppeteer from "puppeteer";

const BASE_URL = process.env.BASE_URL || "http://localhost:4174";

// Pages to test — path is relative to BASE_URL
const PAGES = [
  { path: "/index.html", title: "Synthetic Example", layout: "hero" },
  { path: "/params/Temperature.html", title: "Temperature", layout: "data-page" },
  { path: "/params/DO.html", title: "DO", layout: "data-page" },
  { path: "/sites/NorthDock.html", title: "NorthDock", layout: "data-page" },
  { path: "/sites/SouthDock.html", title: "SouthDock", layout: "data-page" },
];

let pass = 0;
let fail = 0;

function ok(msg) {
  console.log(`  ✅ ${msg}`);
  pass++;
}
function nok(msg) {
  console.log(`  ❌ ${msg}`);
  fail++;
}
function check(cond, msg) {
  cond ? ok(msg) : nok(msg);
}

// ─── Per-page checks ────────────────────────────────────────

async function testPage(browser, page) {
  const url = `${BASE_URL}${page.path}`;
  const errors = [];
  const tab = await browser.newPage();

  // Collect JS errors and non-trivial HTTP failures
  tab.on("pageerror", (err) => errors.push(err.message));
  tab.on("response", (res) => {
    const u = res.url();
    if (
      res.status() >= 400 &&
      !u.includes("favicon") &&
      !u.includes("cdn.jsdelivr")
    ) {
      errors.push(`HTTP ${res.status()}: ${u}`);
    }
  });

  try {
    await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
    await new Promise((r) => setTimeout(r, 1000));

    console.log(`\n=== ${page.path} ===`);

    // Non-data pages: check JS errors immediately (data pages check after chart loads)
    if (page.layout !== "data-page") {
      check(errors.length === 0, `no JS errors (got ${errors.length})`);
      errors.forEach((e) => console.log(`    ERROR: ${e}`));
    }

    // Title present
    const title = await tab.title();
    check(
      title.includes(page.title),
      `title contains "${page.title}" (got "${title}")`
    );

    // Layout class applied
    const hasLayout = await tab.evaluate(
      (cls) => document.querySelector(`main.${cls}`) !== null,
      page.layout
    );
    check(hasLayout, `layout class "${page.layout}" present`);

    // Sidebar exists
    const hasSidebar = await tab.evaluate(
      () => document.querySelector("nav.sidebar") !== null
    );
    check(hasSidebar, "sidebar present");

    // Sidebar has navigation links
    const navLinks = await tab.evaluate(
      () => document.querySelectorAll("nav.sidebar .nav-list a").length
    );
    check(navLinks >= 4, `sidebar has nav links (${navLinks})`);

    // Styles loaded (sidebar has non-zero width)
    const sidebarWidth = await tab.evaluate(() => {
      const el = document.querySelector("nav.sidebar");
      return el ? el.getBoundingClientRect().width : 0;
    });
    check(
      sidebarWidth > 100,
      `styles applied — sidebar width ${sidebarWidth}px`
    );

    // Main content not empty
    const mainText = await tab.evaluate(() => {
      const el = document.querySelector("main");
      return el ? el.textContent.trim().length : 0;
    });
    check(mainText > 10, `main content not empty (${mainText} chars)`);

    // Data pages: chart container and manifest
    if (page.layout === "data-page") {
      const hasChart = await tab.evaluate(
        () => document.querySelector(".chart-container") !== null
      );
      check(hasChart, "chart container present");

      const hasManifest = await tab.evaluate(
        () =>
          document.querySelector(
            'script.chart-data[type="application/json"]'
          ) !== null
      );
      check(hasManifest, "chart data manifest present");

      // Manifest has file entries with /data/ prefix
      const manifestFiles = await tab.evaluate(() => {
        const el = document.querySelector('script.chart-data[type="application/json"]');
        if (!el) return [];
        try {
          return JSON.parse(el.textContent).map(m => m.file);
        } catch { return []; }
      });
      check(
        manifestFiles.length > 0 && manifestFiles.every(f => f.startsWith("/data/")),
        `manifest has ${manifestFiles.length} files, all with /data/ prefix`
      );

      // Duration buttons rendered by chart.js
      const btnCount = await tab.evaluate(
        () => document.querySelectorAll(".duration-buttons button").length
      );
      check(btnCount >= 5, `duration buttons present (${btnCount})`);

      // Wait for DuckDB-WASM to load and chart to render (up to 30s)
      const chartRendered = await tab.evaluate(() => {
        return new Promise((resolve) => {
          let tries = 0;
          const check = () => {
            // Observable Plot renders SVG elements inside the chart container
            const svgs = document.querySelectorAll(".chart-container svg");
            if (svgs.length > 0) return resolve(true);
            if (++tries > 60) return resolve(false);
            setTimeout(check, 500);
          };
          check();
        });
      });
      check(chartRendered, "chart rendered SVG (DuckDB-WASM + Observable Plot)");

      if (chartRendered) {
        // Check SVG has actual plot content (paths/lines, not just empty axes)
        const plotMarks = await tab.evaluate(() => {
          const paths = document.querySelectorAll(
            ".chart-container svg path, .chart-container svg line, .chart-container svg circle"
          );
          return paths.length;
        });
        check(plotMarks > 0, `chart has plot marks (${plotMarks} path/line/circle elements)`);

        // Check for multiple chart sections (min/max/avg grouping)
        const chartDivs = await tab.evaluate(
          () => document.querySelectorAll(".chart-container .chart").length
        );
        check(chartDivs >= 1, `chart sections rendered (${chartDivs})`);
      }

      // Verify no errors accumulated during chart loading
      check(
        errors.length === 0,
        `no errors after chart load (got ${errors.length})`
      );
      errors.forEach((e) => console.log(`    ERROR: ${e}`));
    }
  } catch (err) {
    console.log(`\n=== ${page.path} ===`);
    nok(`page load failed: ${err.message}`);
  }

  await tab.close();
}

// ─── Navigation test ────────────────────────────────────────

async function testNavigation(browser) {
  console.log("\n=== Navigation ===");
  const tab = await browser.newPage();

  await tab.goto(`${BASE_URL}/index.html`, {
    waitUntil: "domcontentloaded",
    timeout: 10000,
  });
  await new Promise((r) => setTimeout(r, 500));

  // Click a sidebar link
  const link = await tab.evaluate(() => {
    const a = document.querySelector(
      'nav.sidebar .nav-list a[href*="Temperature"]'
    );
    if (a) {
      a.click();
      return a.href;
    }
    return null;
  });
  check(link !== null, "found Temperature link in sidebar");

  await new Promise((r) => setTimeout(r, 1000));
  const url = tab.url();
  check(url.includes("Temperature"), `navigated to Temperature page`);

  const title = await tab.title();
  check(title.includes("Temperature"), "Temperature page title correct");

  await tab.close();
}

// ─── Main ───────────────────────────────────────────────────

async function main() {
  console.log(`Validating site at: ${BASE_URL}\n`);

  const browser = await puppeteer.launch({ headless: true });

  for (const page of PAGES) {
    await testPage(browser, page);
  }
  await testNavigation(browser);

  await browser.close();

  console.log(`\n=== Results: ${pass} passed, ${fail} failed ===`);

  if (fail > 0) {
    console.log("\nFAILED");
    process.exit(1);
  }

  console.log("\n=== Browser validation PASSED ===");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
