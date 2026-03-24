import { defineConfig } from 'vite';
import { resolve } from 'path';
import { copyFileSync, existsSync } from 'fs';

export default defineConfig({
  test: {
    environment: 'jsdom',
  },
  build: {
    lib: {
      entry: {
        'geoparquet-extractor': resolve(__dirname, 'src/index.js'),
        'gpkg_worker': resolve(__dirname, 'src/workers/gpkg_worker.js'),
      },
      formats: ['es'],
    },
    rollupOptions: {
      external: [
        'duckdb-wasm-opfs-tempdir',
      ],
    },
    target: 'esnext',
    minify: false,
    sourcemap: true,
  },
  plugins: [{
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
  }],
});
