// Copied beside the main bundle. Static URLs let application bundlers emit the
// worker/Wasm assets, while direct browser/CDN imports resolve them in place.
import * as duckdb from './duckdb/duckdb-browser.mjs';
export * from './duckdb/duckdb-browser.mjs';

export function getLocalBundles() {
  return {
    mvp: {
      mainModule: new URL('./duckdb/duckdb-mvp.wasm', import.meta.url).href,
      mainWorker: new URL('./duckdb/duckdb-browser-mvp.worker.js', import.meta.url).href,
    },
    eh: {
      mainModule: new URL('./duckdb/duckdb-eh.wasm', import.meta.url).href,
      mainWorker: new URL('./duckdb/duckdb-browser-eh.worker.js', import.meta.url).href,
    },
    coi: {
      mainModule: new URL('./duckdb/duckdb-coi.wasm', import.meta.url).href,
      mainWorker: new URL('./duckdb/duckdb-browser-coi.worker.js', import.meta.url).href,
      pthreadWorker: new URL('./duckdb/duckdb-browser-coi.pthread.worker.js', import.meta.url).href,
    },
  };
}

export async function loadBundledDuckDB() {
  // Spatial extensions require the non-threaded runtime, even on isolated pages.
  const { mvp, eh } = getLocalBundles();
  return { duckdb, bundle: await duckdb.selectBundle({ mvp, eh }) };
}
