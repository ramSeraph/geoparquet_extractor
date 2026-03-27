import {
  createProgressReporter,
  formatGpkgTimestamp,
  registerGpkgWorkerMessageHandler as registerCoreGpkgWorkerMessageHandler,
  serializeWorkerError,
  writeFromParquet as writeFromParquetCore,
} from '../../src/workers/gpkg_core.js';
import { createSqliteRuntime as createSharedSqliteRuntime } from '../../src/workers/gpkg_runtime.js';

export { createProgressReporter, formatGpkgTimestamp, serializeWorkerError };

async function defaultCreateSqliteModule(opts) {
  const { default: SQLiteESMFactory } = await import('wa-sqlite-rtree/dist/wa-sqlite-async.mjs');
  return SQLiteESMFactory(opts);
}

async function defaultCreateVfs(module) {
  const { OPFSAnyContextVFS } = await import('wa-sqlite-rtree/src/examples/OPFSAnyContextVFS.js');
  const vfsName = `gpkg-test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  return OPFSAnyContextVFS.create(vfsName, module);
}

async function defaultSqliteApi() {
  return import('wa-sqlite-rtree/src/sqlite-api.js');
}

export function createSqliteRuntime({
  dbPath,
  getWasmUrl,
  createSqliteModule = defaultCreateSqliteModule,
  sqliteApi,
  createVfs = defaultCreateVfs,
} = {}) {
  if (typeof getWasmUrl !== 'function') {
    throw new Error('createSqliteRuntime requires getWasmUrl');
  }
  return (async () => createSharedSqliteRuntime({
    dbPath,
    wasmUrl: getWasmUrl(),
    createSqliteModule,
    sqliteApi: sqliteApi ?? await defaultSqliteApi(),
    createVfs,
  }))();
}

function createTestEnv({
  getRoot = () => navigator.storage.getDirectory(),
  postMessage = () => {},
  createDatabaseRuntime,
  getTimestamp = () => new Date(),
  getWasmUrl,
  createSqliteModule,
  sqliteApi,
  createVfs,
} = {}) {
  return {
    getRoot,
    postMessage,
    createDatabaseRuntime: createDatabaseRuntime ?? ((dbPath) => createSqliteRuntime({
      dbPath,
      getWasmUrl,
      createSqliteModule,
      sqliteApi,
      createVfs,
    })),
    getTimestamp,
  };
}

export function writeFromParquet(args, msgId, options = {}) {
  return writeFromParquetCore(args, msgId, createTestEnv(options));
}

export function registerGpkgWorkerMessageHandler(workerScope, options = {}) {
  return registerCoreGpkgWorkerMessageHandler(workerScope, {
    writeFromParquet: (args, msgId) => writeFromParquet(args, msgId, {
      ...options,
      postMessage: (payload) => workerScope.postMessage(payload),
    }),
  });
}

function createDuckdbBackedRoot(duckdbClient) {
  return async () => {
    const root = await navigator.storage.getDirectory();
    return {
      ...root,
      async getFileHandle(name, options = {}) {
        try {
          return await root.getFileHandle(name, options);
        } catch (error) {
          if (options.create || error?.name !== 'NotFoundError') {
            throw error;
          }
          const bytes = await duckdbClient.db.copyFileToBuffer(name);
          const handle = await root.getFileHandle(name, { create: true });
          const writable = await handle.createWritable();
          await writable.write(bytes);
          await writable.close();
          return handle;
        }
      },
    };
  };
}

export function createMockGpkgWorker({ duckdbClient, wasmUrl }) {
  let terminated = false;
  const worker = {
    onmessage: null,
    onerror: null,
    terminate() {
      terminated = true;
    },
    postMessage(message) {
      queueMicrotask(async () => {
        if (terminated) return;
        const { id, method, args } = message;

        try {
          if (method !== 'writeFromParquet') {
            throw new Error(`Unknown method: ${method}`);
          }

          const result = await writeFromParquet(args, id, {
            getWasmUrl: () => wasmUrl,
            getRoot: createDuckdbBackedRoot(duckdbClient),
            postMessage: (payload) => {
              if (!terminated) worker.onmessage?.({ data: payload });
            },
          });

          if (!terminated) {
            worker.onmessage?.({ data: { id, result } });
          }
        } catch (error) {
          if (!terminated) {
            worker.onmessage?.({
              data: {
                id,
                error: error?.message || String(error),
              },
            });
          }
        }
      });
    },
  };

  return worker;
}
