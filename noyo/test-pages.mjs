// Test script to check for JavaScript errors in the dashboard
// Run: node test-pages.mjs

import puppeteer from 'puppeteer';

const BASE_URL = 'http://localhost:5173';
const PAGES = ['index.html', 'FieldStation.html', 'Temperature.html', 'DO.html'];

async function testPage(browser, page) {
  const url = `${BASE_URL}/${page}`;
  const errors = [];
  
  const tab = await browser.newPage();
  
  tab.on('console', msg => {
    if (msg.type() === 'error') {
      const text = msg.text();
      const location = msg.location();
      // Ignore external resource 404s (map tiles, etc)
      if (text.includes('404') && (text.includes('openstreetmap') || text.includes('tile'))) return;
      // Ignore generic "Failed to load resource" with no specific info
      if (text === 'Failed to load resource: the server responded with a status of 404 (Not Found)') return;
      errors.push(`Console: ${text} @ ${location?.url || 'unknown'}`);
    }
  });
  
  tab.on('pageerror', err => {
    errors.push(`JS Error: ${err.message}`);
  });
  
  tab.on('response', res => {
    if (res.status() >= 400 && !res.url().includes('favicon') && !res.url().includes('openstreetmap')) {
      errors.push(`HTTP ${res.status()}: ${res.url()}`);
    }
  });
  
  tab.on('requestfailed', req => {
    errors.push(`Failed: ${req.url()} - ${req.failure()?.errorText || 'unknown'}`);
  });
  
  try {
    await tab.goto(url, { waitUntil: 'domcontentloaded', timeout: 10000 });
    
    // Simple wait
    await new Promise(r => setTimeout(r, 3000));
    
    // Check for chart SVGs on data pages
    const hasCharts = await tab.evaluate(() => document.querySelectorAll('.chart svg').length);
    const hasEmpty = await tab.evaluate(() => document.querySelectorAll('.empty-state').length);
    
    console.log(`\n=== ${page} ===`);
    if (errors.length === 0) {
      if (page !== 'index.html') {
        if (hasCharts > 0) {
          console.log(`✅ No errors, ${hasCharts} chart(s) rendered`);
        } else if (hasEmpty > 0) {
          console.log(`⚠️  No errors, but no data loaded (empty state shown)`);
        } else {
          console.log(`⚠️  No errors, but no charts or empty state found`);
        }
      } else {
        console.log('✅ No errors');
      }
    } else {
      errors.forEach(e => console.log(`❌ ${e}`));
    }
  } catch (err) {
    console.log(`\n=== ${page} ===`);
    console.log(`❌ Load failed: ${err.message}`);
  }
  
  await tab.close();
}

async function main() {
  console.log(`Testing: ${BASE_URL}`);
  
  const browser = await puppeteer.launch({ headless: true });
  
  for (const page of PAGES) {
    await testPage(browser, page);
  }
  
  // Test 18-month time range specifically
  console.log('\n=== Testing 18-month time range ===');
  const tab = await browser.newPage();
  await tab.goto(`${BASE_URL}/FieldStation.html`, { waitUntil: 'domcontentloaded', timeout: 10000 });
  await new Promise(r => setTimeout(r, 2000));
  await tab.click('button[data-days="550"]');
  await new Promise(r => setTimeout(r, 5000));
  const hasCharts = await tab.evaluate(() => document.querySelectorAll('.chart svg').length);
  const hasEmpty = await tab.evaluate(() => document.querySelectorAll('.empty-state').length);
  if (hasCharts > 0) {
    console.log(`✅ 18-month: ${hasCharts} chart(s) rendered`);
  } else if (hasEmpty > 0) {
    console.log(`❌ 18-month: empty state shown (no data)`);
  } else {
    console.log(`❌ 18-month: no charts or empty state`);
  }
  await tab.close();
  
  await browser.close();
  console.log('\nDone.');
}

main().catch(e => { console.error(e); process.exit(1); });
