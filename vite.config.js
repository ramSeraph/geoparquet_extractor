import { defineConfig } from 'vite';
import { resolve } from 'path';
import { copyFileSync, existsSync } from 'fs';

// Two-phase build: main entry then worker (BUILD_TARGET=worker).
// Each entry is self-contained — no shared chunks — so the worker
// can be loaded independently from any origin.
const isWorkerBuild = process.env.BUILD_TARGET === 'worker';

export default defineConfig({
  resolve: {
    alias: [{ find: './duckdb.js', replacement: resolve(__dirname, 'dist/duckdb.js') }],
  },
  assetsInclude: ['**/*.parquet'],
  optimizeDeps: {
    include: [
      'flatbuffers',
      'flatgeobuf',
      'flatgeobuf/lib/mjs/constants.js',
      'flatgeobuf/lib/mjs/generic/feature.js',
      'flatgeobuf/lib/mjs/generic/featurecollection.js',
      'flatgeobuf/lib/mjs/geojson/geometry.js',
      'flatgeobuf/lib/mjs/flat-geobuf/column.js',
      'flatgeobuf/lib/mjs/flat-geobuf/column-type.js',
      'flatgeobuf/lib/mjs/flat-geobuf/crs.js',
      'flatgeobuf/lib/mjs/flat-geobuf/geometry-type.js',
      'flatgeobuf/lib/mjs/flat-geobuf/header.js',
      'wa-sqlite-rtree/dist/wa-sqlite-async.mjs',
      'wa-sqlite-rtree/src/sqlite-api.js',
      'wa-sqlite-rtree/src/examples/OPFSAdaptiveVFS.js',
      'wa-sqlite-rtree/src/examples/OPFSAnyContextVFS.js',
    ],
  },
  test: {
    environment: 'jsdom',
    // Browser suites share an origin and some replace its storage API with a
    // memory harness. Keep real-OPFS and harness suites in separate runs.
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 60000,
    browser: {
      enabled: true,
      provider: 'playwright',
      headless: true,
      screenshotFailures: false,
      instances: [
        { browser: 'chromium', ...(process.env.CHROME_CHANNEL ? { launch: { channel: process.env.CHROME_CHANNEL } } : {}) },
        ...(process.env.CHROME_CHANNEL ? [] : [{
          browser: 'webkit',
          // Playwright WebKit's ephemeral contexts do not support OPFS.
          // test:package covers real OPFS in a persistent WebKit context.
          exclude: ['test/duckdb_runtime.browser.test.js'],
        }]),
      ],
    },
  },
  build: {
    lib: {
      entry: isWorkerBuild
        ? resolve(__dirname, 'src/workers/gpkg_worker.js')
        : resolve(__dirname, 'src/index.js'),
      formats: ['es'],
      fileName: isWorkerBuild ? 'gpkg_worker' : 'geoparquet-extractor',
    },
    rollupOptions: {
      // Emitted separately with static worker/Wasm URLs for downstream bundlers.
      external: isWorkerBuild ? [] : ['./duckdb.js'],
    },
    outDir: 'dist',
    emptyOutDir: !isWorkerBuild && !process.env.KEEP_RUNTIME,
    target: 'esnext',
    minify: false,
    sourcemap: true,
  },
  plugins: isWorkerBuild ? [{
    name: 'copy-wa-sqlite-wasm',
    closeBundle() {
      const src = resolve(__dirname, 'node_modules/wa-sqlite-rtree/dist/wa-sqlite-async.wasm');
      const dest = resolve(__dirname, 'dist/wa-sqlite-async.wasm');
      if (existsSync(src)) {
        copyFileSync(src, dest);
        console.log('Copied wa-sqlite-async.wasm to dist/');
      } else {
        console.warn('Warning: wa-sqlite-async.wasm not found in node_modules/wa-sqlite-rtree/dist/');
      }
    },
  }] : [],
});
