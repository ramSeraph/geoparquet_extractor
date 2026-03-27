// Web Worker for writing GeoPackage files to OPFS.
// Reads an intermediate parquet from OPFS via hyparquet,
// writes GPKG via wa-sqlite-rtree's OPFSAdaptiveVFS.

import SQLiteESMFactory from 'wa-sqlite-rtree/dist/wa-sqlite-async.mjs';
import * as SQLite from 'wa-sqlite-rtree/src/sqlite-api.js';
import { OPFSAdaptiveVFS } from 'wa-sqlite-rtree/src/examples/OPFSAdaptiveVFS.js';
import {
  registerGpkgWorkerMessageHandler as registerCoreGpkgWorkerMessageHandler,
  writeFromParquet as writeFromParquetCore,
} from './gpkg_core.js';
import { createSqliteRuntime as createSharedSqliteRuntime } from './gpkg_runtime.js';

const WASM_URL = new URL(/* @vite-ignore */ 'wa-sqlite-async.wasm', import.meta.url).href;

function createDatabaseRuntime(dbPath) {
  return createSharedSqliteRuntime({
    dbPath,
    wasmUrl: WASM_URL,
    createSqliteModule: (opts) => SQLiteESMFactory(opts),
    sqliteApi: SQLite,
    createVfs: (module) => OPFSAdaptiveVFS.create('opfs-adaptive', module),
  });
}

function createWorkerEnv(workerScope) {
  return {
    getRoot: () => navigator.storage.getDirectory(),
    postMessage: (payload) => workerScope.postMessage(payload),
    createDatabaseRuntime,
    getTimestamp: () => new Date(),
  };
}

export function writeFromParquet(args, msgId, workerScope = self) {
  return writeFromParquetCore(args, msgId, createWorkerEnv(workerScope));
}

export function registerGpkgWorkerMessageHandler(workerScope = self) {
  return registerCoreGpkgWorkerMessageHandler(workerScope, {
    writeFromParquet: (args, msgId) => writeFromParquet(args, msgId, workerScope),
  });
}

if (
  typeof self !== 'undefined' &&
  typeof DedicatedWorkerGlobalScope !== 'undefined' &&
  self instanceof DedicatedWorkerGlobalScope
) {
  registerGpkgWorkerMessageHandler(self);
}
