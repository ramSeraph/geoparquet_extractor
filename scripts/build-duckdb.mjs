import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const upstream = path.join(root, 'node_modules/@duckdb/duckdb-wasm');
const output = path.join(root, 'dist/duckdb');
const pin = JSON.parse(await readFile(new URL('../vendor/duckdb/upstream.json', import.meta.url)));
const installed = JSON.parse(await readFile(path.join(upstream, 'package.json')));
const pkg = JSON.parse(await readFile(path.join(root, 'package.json')));
if (installed.version !== pin.version) throw new Error(`DuckDB build requires exactly ${pin.version}`);
const lock = JSON.parse(await readFile(path.join(root, 'package-lock.json')));
if (lock.packages['node_modules/@duckdb/duckdb-wasm'].integrity !== pin.integrity) {
  throw new Error('DuckDB lockfile integrity differs from vendor/duckdb/upstream.json');
}

const scratch = await mkdtemp(path.join(tmpdir(), 'geoparquet-duckdb-'));
try {
  const sources = new Map();
  // The pinned npm release includes original TS and generated Emscripten JS in
  // sourcesContent. Rebuild these sources, never edit the minified workers.
  for (const file of (await readdir(path.join(upstream, 'dist'))).filter(name => name.endsWith('.map')).sort()) {
    const map = JSON.parse(await readFile(path.join(upstream, 'dist', file)));
    for (let i = 0; i < map.sources.length; i++) {
      const name = map.sources[i];
      if (!name.startsWith('../src/') || map.sourcesContent[i] == null) continue;
      const relative = name.slice(3);
      if (relative.split('/').includes('..')) throw new Error(`Unexpected source path: ${name}`);
      if (sources.has(relative) && sources.get(relative) !== map.sourcesContent[i]) {
        throw new Error(`Conflicting upstream source: ${name}`);
      }
      sources.set(relative, map.sourcesContent[i]);
    }
  }
  // Tree-shaken barrels and interfaces do not appear in source maps. Their
  // declarations contain the same exports/types and no required runtime bodies.
  async function addDeclarations(directory, relative = 'src') {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const source = path.join(directory, entry.name);
      if (entry.isDirectory()) await addDeclarations(source, `${relative}/${entry.name}`);
      else if (entry.name.endsWith('.d.ts')) {
        const target = `${relative}/${entry.name.replace(/\.d\.ts$/, '.ts')}`;
        if (!sources.has(target)) sources.set(target, await readFile(source, 'utf8'));
      }
    }
  }
  await addDeclarations(path.join(upstream, 'dist/types/src'));
  // npm omits the MVP browser source map; its worker entry is identical to EH.
  sources.set('src/targets/duckdb-browser-mvp.worker.ts',
    sources.get('src/targets/duckdb-browser-eh.worker.ts').replaceAll('bindings_browser_eh', 'bindings_browser_mvp'));
  // The corresponding binding also has an identical implementation.
  sources.set('src/bindings/bindings_browser_mvp.ts',
    sources.get('src/bindings/bindings_browser_eh.ts').replaceAll('duckdb-eh.js', 'duckdb-mvp.js'));
  for (const [name, content] of sources) {
    const target = path.join(scratch, name);
    await mkdir(path.dirname(target), { recursive: true });
    await writeFile(target, content);
  }
  // Final package identity must be present before bundling version.ts.
  await writeFile(path.join(scratch, 'package.json'), JSON.stringify({ name: pkg.name, version: pkg.version }));
  const patchPath = path.join(root, 'vendor/duckdb/opfs.patch');
  execFileSync('git', ['apply', '--check', patchPath], { cwd: scratch, stdio: 'inherit' });
  execFileSync('git', ['apply', patchPath], { cwd: scratch, stdio: 'inherit' });
  await cp(path.join(root, 'vendor/duckdb/opfs_pool.js'), path.join(scratch, 'src/utils/opfs_pool.js'));

  await mkdir(output, { recursive: true });
  const common = {
    bundle: true, platform: 'browser', target: 'es2020', minify: true,
    nodePaths: [path.join(root, 'node_modules')],
    define: { 'process.release.name': '"browser"', 'process.env.NODE_ENV': '"production"' },
    // Emscripten's Node-only branches are unreachable in these browser workers.
    external: ['module', 'fs', 'path', 'os', 'crypto', 'worker_threads', 'perf_hooks', 'child_process', 'vm'],
    legalComments: 'eof',
  };
  await build({ ...common, format: 'esm',
    entryPoints: [path.join(scratch, 'src/targets/duckdb.ts')],
    outfile: path.join(output, 'duckdb-browser.mjs'),
    // Upstream createWorker uses web-worker solely as a browser Worker alias.
    plugins: [{ name: 'browser-worker', setup(build) {
      build.onResolve({ filter: /^web-worker$/ }, () => ({ path: 'worker', namespace: 'browser-worker' }));
      build.onLoad({ filter: /.*/, namespace: 'browser-worker' }, () => ({ contents: 'export default globalThis.Worker;' }));
    } }],
  });
  for (const variant of ['mvp', 'eh', 'coi', 'coi.pthread']) {
    await build({ ...common, format: 'iife', globalName: 'duckdb',
      entryPoints: [path.join(scratch, `src/targets/duckdb-browser-${variant}.worker.ts`)],
      outfile: path.join(output, `duckdb-browser-${variant}.worker.js`),
    });
  }
  for (const variant of ['mvp', 'eh', 'coi']) {
    await cp(path.join(upstream, `dist/duckdb-${variant}.wasm`), path.join(output, `duckdb-${variant}.wasm`));
  }
  await cp(path.join(upstream, 'dist/types'), path.join(output, 'types'), { recursive: true });
  await cp(path.join(upstream, 'dist/duckdb-browser.d.ts'), path.join(output, 'duckdb-browser.d.ts'));
  const bindingsType = path.join(output, 'types/src/parallel/async_bindings.d.ts');
  const types = await readFile(bindingsType, 'utf8');
  await writeFile(bindingsType, types.replace('    registerOPFSFileName(',
    '    registerOPFSTempDir(path?: string, maxUnused?: number, minUnused?: number): Promise<void>;\n    registerOPFSFileName('));
  await cp(path.join(root, 'src/duckdb.js'), path.join(root, 'dist/duckdb.js'));
  await writeFile(path.join(root, 'dist/duckdb.d.ts'), `export * from './duckdb/duckdb-browser';
import * as duckdb from './duckdb/duckdb-browser';
export declare function getLocalBundles(): duckdb.DuckDBBundles;
export declare function loadBundledDuckDB(): Promise<{ duckdb: typeof duckdb; bundle: duckdb.DuckDBBundle }>;
`);
  await cp(path.join(root, 'vendor/duckdb/LICENSE'), path.join(output, 'LICENSE'));
  await cp(path.join(root, 'vendor/duckdb/NOTICE.txt'), path.join(output, 'NOTICE.txt'));
  await cp(path.join(root, 'vendor/duckdb/upstream.json'), path.join(output, 'upstream.json'));
  const notices = path.join(output, 'licenses');
  await mkdir(notices, { recursive: true });
  for (const dependency of ['apache-arrow', 'flatbuffers', 'tslib', 'js-sha256', 'wasm-feature-detect']) {
    const directory = path.join(root, 'node_modules', dependency);
    for (const name of (await readdir(directory)).filter(name => /^(LICENSE|NOTICE)/i.test(name))) {
      await cp(path.join(directory, name), path.join(notices, `${dependency}-${name}`));
    }
  }
  const hash = createHash('sha256');
  hash.update(await readFile(patchPath));
  hash.update(await readFile(path.join(root, 'vendor/duckdb/opfs_pool.js')));
  await writeFile(path.join(output, 'build.json'), JSON.stringify({
    package: pkg.name, version: pkg.version, upstream: pin, patchSha256: hash.digest('hex'),
  }, null, 2) + '\n');
  console.log(`Built packaged DuckDB from ${pin.package}@${pin.version}`);
} finally {
  await rm(scratch, { recursive: true, force: true });
}
