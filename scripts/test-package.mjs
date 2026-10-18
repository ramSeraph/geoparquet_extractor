import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, webkit } from 'playwright';
import { build } from 'vite';

const root = fileURLToPath(new URL('../', import.meta.url));
const artifacts = path.join(root, '.artifacts');
await mkdir(artifacts, { recursive: true });
const [packed] = JSON.parse(execFileSync('npm', ['pack', '--ignore-scripts', '--json', '--pack-destination', artifacts], {
  cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'],
}));
const tarball = path.join(artifacts, packed.filename);
const fixture = await realpath(await mkdtemp(path.join(tmpdir(), 'geoparquet-consumer-')));
let server, browser;
try {
  await writeFile(path.join(fixture, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', tarball], {
    cwd: fixture, stdio: 'inherit',
  });
  const lock = JSON.parse(await readFile(path.join(fixture, 'package-lock.json')));
  assert(!Object.keys(lock.packages).some(name => /duckdb-wasm/.test(name)), 'Consumer must not install a separate DuckDB package');
  await writeFile(path.join(fixture, 'index.html'), '<!doctype html><script type="module" src="/main.js"></script>');
  await writeFile(path.join(fixture, 'main.js'), `
import { initDuckDB } from 'geoparquet-extractor';
import * as duckdb from 'geoparquet-extractor/duckdb';
globalThis.checkPackage = async (allVariants = true) => {
  const results = {};
  globalThis.packageTestStage = 'default runtime instantiation';
  const client = await initDuckDB();
  const db = client.db;
  const name = 'packed-' + crypto.randomUUID();
  try {
    globalThis.packageTestStage = 'default runtime OPFS spill';
    const conn = await db.connect();
    await db.registerOPFSTempDir('opfs://' + name);
    await conn.query("SET threads=1; SET temp_directory='opfs://" + name + "'; SET memory_limit='4MB'; SET max_temp_directory_size='100MiB'");
    results.spill = String((await conn.query('SELECT COUNT(*) FROM (SELECT i FROM range(0, 800000) i ORDER BY hash(i) LIMIT 100000)')).getChildAt(0).get(0));
    await conn.close();
    await db.reset();
    await db.registerOPFSTempDir('opfs://' + name);
    const next = await db.connect();
    await next.query("SET threads=1; SET temp_directory='opfs://" + name + "'; SET memory_limit='4MB'; SET max_temp_directory_size='100MiB'");
    results.afterReset = String((await next.query('SELECT COUNT(*) FROM (SELECT i FROM range(0, 800000) i ORDER BY hash(i) LIMIT 100000)')).getChildAt(0).get(0));
    for (let i = 0; i < 3; i++) {
      const repeated = String((await next.query('SELECT COUNT(*) FROM (SELECT i FROM range(0, 800000) i ORDER BY hash(i) LIMIT 100000)')).getChildAt(0).get(0));
      if (repeated !== '100000') throw new Error('Repeated spill returned ' + repeated);
    }
    await next.close();
    await db.reset();
    await (await navigator.storage.getDirectory()).removeEntry(name, { recursive: true });
  } finally { client.terminate(); }
  for (const [variant, bundle] of Object.entries(allVariants ? duckdb.getLocalBundles() : {})) {
    globalThis.packageTestStage = variant + ' worker';
    const worker = new Worker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR), worker);
    try {
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      await db.open({ maximumThreads: 1 });
      const conn = await db.connect();
      results[variant] = (await conn.query('SELECT 42 AS n')).getChildAt(0).get(0);
      await conn.close();
      await db.reset();
    } finally { worker.terminate(); }
  }
  results.package = duckdb.PACKAGE_NAME;
  return results;
};
`);
  // Type-check the published DuckDB API in the consumer, including Arrow types.
  await writeFile(path.join(fixture, 'check.ts'), `
import { initDuckDB } from 'geoparquet-extractor';
import { AsyncDuckDB, ConsoleLogger, getLocalBundles } from 'geoparquet-extractor/duckdb';
const db = new AsyncDuckDB(new ConsoleLogger(), new Worker(getLocalBundles().mvp.mainWorker));
db.registerOPFSTempDir('opfs://tmp');
initDuckDB({ memoryLimitMB: 64 });
`);
  execFileSync(process.execPath, [path.join(root, 'node_modules/typescript/bin/tsc'), '--noEmit', '--skipLibCheck',
    '--target', 'esnext', '--module', 'esnext', '--moduleResolution', 'bundler', 'check.ts'], { cwd: fixture, stdio: 'inherit' });
  await build({ root: fixture, configFile: false, logLevel: 'warn', build: { assetsInlineLimit: 0 } });
  const dist = path.join(fixture, 'dist');
  server = createServer(async (req, res) => {
    try {
      const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
      const file = path.resolve(dist, '.' + (pathname === '/' ? '/index.html' : pathname));
      if (!file.startsWith(dist + path.sep)) throw new Error('Invalid path');
      res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
      res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
      res.setHeader('Content-Type', file.endsWith('.wasm') ? 'application/wasm' : file.endsWith('.html') ? 'text/html' : 'text/javascript');
      res.end(await readFile(file));
    } catch { res.writeHead(404); res.end(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const channel = process.env.CHROME_CHANNEL || (process.argv.includes('--chrome') ? 'chrome' : undefined);
  browser = await chromium.launch({ headless: true, ...(channel ? { channel } : {}) });
  async function checkBrowser(allVariants) {
    const page = await browser.newPage();
    page.on('pageerror', error => console.error('Consumer page:', error.message));
    page.on('requestfailed', request => console.error('Consumer request:', request.url(), request.failure()?.errorText));
    await page.goto(`http://127.0.0.1:${server.address().port}/`);
    await page.waitForFunction(() => typeof globalThis.checkPackage === 'function');
    return page.evaluate(allVariants => Promise.race([
      globalThis.checkPackage(allVariants),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Package smoke test timed out: ' + globalThis.packageTestStage)), 60000)),
    ]), allVariants);
  }
  assert.deepEqual(await checkBrowser(true), { spill: '100000', afterReset: '100000', mvp: 42, eh: 42, coi: 42, package: 'geoparquet-extractor' });
  await browser.close();
  // OPFS is unavailable in Playwright WebKit's ephemeral/private contexts.
  // https://github.com/microsoft/playwright/blob/main/docs/src/api/class-browsercontext.md
  browser = await webkit.launchPersistentContext(path.join(fixture, 'webkit-profile'), { headless: true });
  assert.deepEqual(await checkBrowser(false), { spill: '100000', afterReset: '100000', package: 'geoparquet-extractor' });
  console.log(`Verified ${packed.filename}: clean install, TypeScript, Vite, Chrome/WebKit OPFS spill and reset, MVP/EH/COI workers`);
} finally {
  await browser?.close();
  if (server) await new Promise(resolve => server.close(resolve));
  await rm(fixture, { recursive: true, force: true });
}
