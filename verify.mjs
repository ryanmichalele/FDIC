import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright-core';
import { load } from 'cheerio';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SITE_DIR = path.join(__dirname, 'site');
const MANIFEST_PATH = path.join(__dirname, 'manifest.json');
const REPORT_PATH = path.join(__dirname, 'verify-report.json');
const PORT = Number(process.env.VERIFY_PORT || 8099);
const BASE = `http://127.0.0.1:${PORT}`;
const MAX_PAGES = Number(process.env.VERIFY_PAGES || 25);

const manifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf8'));
const pageUrls = Object.keys(manifest.pages || {});

function collectTestPaths() {
  const out = new Set(['/']);
  try {
    const html = fs.readFileSync(path.join(SITE_DIR, 'index.html'), 'utf8');
    const $ = load(html);
    $('nav a[href], header a[href]').each((_, el) => {
      const href = $(el).attr('href');
      if (!href) return;
      try {
        const u = new URL(href, 'https://www.fdic.gov/');
        if (u.hostname !== 'www.fdic.gov' && u.hostname !== 'fdic.gov') return;
        const local = u.pathname + u.search;
        const key = 'https://www.fdic.gov' + local;
        if (manifest.pages[key] || manifest.pages[key.replace(/\/$/, '')]) out.add(local);
      } catch {}
    });
  } catch {}
  for (const u of pageUrls) {
    if (out.size >= MAX_PAGES) break;
    try {
      const x = new URL(u);
      const depth = x.pathname.split('/').filter(Boolean).length;
      if (depth === 1) out.add(x.pathname + x.search);
    } catch {}
  }
  return [...out].slice(0, MAX_PAGES);
}

async function waitForServer() {
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(BASE + '/', { signal: AbortSignal.timeout(2000) });
      if (r.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error('Server did not start');
}

const server = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
  env: { ...process.env, PORT: String(PORT) },
  stdio: ['ignore', 'pipe', 'pipe'],
});
server.stdout.on('data', (d) => process.stdout.write('[server] ' + d));
server.stderr.on('data', (d) => process.stderr.write('[server] ' + d));

const pages = collectTestPaths();
const report = {
  startedAt: new Date().toISOString(),
  base: BASE,
  pagesTested: pages,
  results: [],
  summary: {
    pages: pages.length,
    localAssetFailures: 0,
    localHttpErrors: 0,
    consoleErrors: 0,
    pageErrors: 0,
    externalRequestsBlocked: 0,
  },
};

let exitCode = 0;
let browser;
try {
  await waitForServer();
  browser = await chromium.launch({ channel: 'chrome', headless: true });
  const context = await browser.newContext({ ignoreHTTPSErrors: true });

  if (process.env.VERIFY_OFFLINE === '1') {
    await context.route('**/*', (route) => {
      const url = route.request().url();
      if (url.startsWith(BASE)) route.continue();
      else route.abort('blockedbyclient');
    });
  }

  for (const pagePath of pages) {
    const page = await context.newPage();
    const errors = [];
    const localFailures = [];
    const httpErrors = [];
    const external = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
    });
    page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
    page.on('requestfailed', (req) => {
      const url = req.url();
      if (url.startsWith(BASE)) localFailures.push(`${req.method()} ${url} :: ${req.failure()?.errorText}`);
      else external.push(url);
    });
    page.on('response', (res) => {
      const url = res.url();
      if (url.startsWith(BASE) && res.status() >= 400) httpErrors.push(`${res.status()} ${url}`);
    });
    const url = BASE + pagePath;
    let navOk = true;
    try {
      await page.goto(url, { waitUntil: 'load', timeout: 30000 });
      await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(() => {});
      await page.waitForTimeout(1500);
    } catch (e) {
      navOk = false;
      errors.push('navigation: ' + e.message);
    }
    const title = await page.title().catch(() => '');
    const result = {
      url,
      title,
      navOk,
      consoleErrors: errors,
      localFailures,
      httpErrors,
      externalRequests: [...new Set(external)].slice(0, 20),
    };
    report.results.push(result);
    report.summary.consoleErrors += errors.length;
    report.summary.localAssetFailures += localFailures.length;
    report.summary.localHttpErrors += httpErrors.length;
    report.summary.pageErrors += errors.filter((e) => e.startsWith('pageerror:')).length;
    report.summary.externalRequestsBlocked += external.length;
    const flag =
      errors.length || localFailures.length || httpErrors.length || !navOk ? 'FAIL' : 'PASS';
    if (flag === 'FAIL') exitCode = 1;
    console.log(
      `${flag} ${pagePath} title="${title.slice(0, 60)}" console=${errors.length} local404=${localFailures.length} http>=400=${httpErrors.length} external=${external.length}`
    );
    for (const e of errors.slice(0, 5)) console.log('   ' + e);
    for (const e of localFailures.slice(0, 5)) console.log('   ' + e);
    for (const e of httpErrors.slice(0, 5)) console.log('   ' + e);
    await page.close();
  }
  await context.close();
  report.finishedAt = new Date().toISOString();
  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
  console.log('\nSummary:', JSON.stringify(report.summary));
  console.log('Report:', REPORT_PATH);
} catch (e) {
  console.error('VERIFY FAILED:', e);
  exitCode = 1;
} finally {
  if (browser) await browser.close().catch(() => {});
  server.kill();
}
process.exit(exitCode);
