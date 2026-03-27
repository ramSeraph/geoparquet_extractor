import { defineConfig } from 'vite';
import { resolve } from 'path';
import { copyFileSync, existsSync } from 'fs';

// Two-phase build: main entry then worker (BUILD_TARGET=worker).
// Each entry is self-contained — no shared chunks — so the worker
// can be loaded independently from any origin.
const isWorkerBuild = process.env.BUILD_TARGET === 'worker';

export default defineConfig({
  assetsInclude: ['**/*.parquet'],
  optimizeDeps: {
    include: [
      'wa-sqlite-rtree/dist/wa-sqlite-async.mjs',
      'wa-sqlite-rtree/src/sqlite-api.js',
      'wa-sqlite-rtree/src/examples/OPFSAdaptiveVFS.js',
      'wa-sqlite-rtree/src/examples/OPFSAnyContextVFS.js',
    ],
  },
  test: {
    environment: 'jsdom',
    testTimeout: 60000,
    hookTimeout: 60000,
    browser: {
      enabled: true,
      provider: 'playwright',
      headless: true,
      screenshotFailures: false,
      instances: [
        { browser: 'webkit' },
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
      external: isWorkerBuild ? [] : ['duckdb-wasm-opfs-tempdir'],
    },
    outDir: 'dist',
    emptyOutDir: !isWorkerBuild,
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
