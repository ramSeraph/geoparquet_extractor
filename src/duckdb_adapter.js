/**
 * @module duckdb_adapter
 * Thin adapter that wraps a DuckDB-WASM connection into the DuckDBClient
 * interface expected by this library. Consumers can also implement the
 * interface directly if they have a non-standard DuckDB setup.
 */

/**
 * @typedef {Object} DuckDBClient
 * @property {(sql: string) => Promise<{ toArray: () => any[] }>} query
 *   Execute a SQL query and return a result with .toArray().
 * @property {() => Promise<void>} init
 *   Initialize the client (extensions, settings). Idempotent.
 * @property {() => void} terminate
 *   Kill the DuckDB worker. After this, init() can reinitialize.
 * @property {object} conn
 *   The underlying DuckDB connection (for direct access when needed).
 * @property {object} db
 *   The underlying DuckDB database instance.
 */

/**
 * Create a DuckDBClient by loading DuckDB-WASM from a distribution URL.
 * Handles bundle selection, worker creation, and WASM instantiation.
 *
 * @param {string} duckdbDist - Base URL of the duckdb-wasm distribution
 *   (e.g. 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir')
 * @param {object} [options]
 * @param {string} [options.tempDirectory] - OPFS temp directory path
 * @param {number} [options.memoryLimitMB] - DuckDB memory limit in MB; a runtime tuning knob for browser/Wasm memory pressure, not something derived from temp-directory page sizes
 * @returns {Promise<DuckDBClient>}
 */
// Cache bundle resolution per distribution URL
const _bundleCache = new Map();
const OPFS_TEMP_POOL_MAX = 128;
const OPFS_TEMP_POOL_MIN = 32;

/**
 * Register an OPFS temp directory with DuckDB before setting temp_directory.
 * Explicit registration is needed for the default in-memory DuckDB setup.
 *
 * @param {object} db - AsyncDuckDB instance
 * @param {(sql: string) => Promise<any>} query - Query executor
 * @param {string | undefined} tempDirectory - Temp directory path
 * @returns {Promise<void>}
 */
export async function configureDuckDBTempDirectory(db, query, tempDirectory) {
  if (!tempDirectory) {
    await query("RESET temp_directory;");
    return;
  }
  await db.registerOPFSTempDir(tempDirectory, OPFS_TEMP_POOL_MAX, OPFS_TEMP_POOL_MIN);
  await query(`SET temp_directory = '${tempDirectory.replace(/'/g, "''")}';`);
}

export async function initDuckDB(duckdbDist, options = {}) {
  const base = duckdbDist.replace(/\/$/, '');

  if (!_bundleCache.has(base)) {
    _bundleCache.set(base, (async () => {
      const duckdb = await import(`${base}/duckdb-browser.mjs`);
      const bundles = {
        mvp: {
          mainModule: `${base}/duckdb-mvp.wasm`,
          mainWorker: `${base}/duckdb-browser-mvp.worker.js`,
        },
        eh: {
          mainModule: `${base}/duckdb-eh.wasm`,
          mainWorker: `${base}/duckdb-browser-eh.worker.js`,
        },
        coi: {
          mainModule: `${base}/duckdb-coi.wasm`,
          mainWorker: `${base}/duckdb-browser-coi.worker.js`,
          pthreadWorker: `${base}/duckdb-browser-coi.pthread.worker.js`,
        },
      };
      const bundle = await duckdb.selectBundle(bundles);
      return { duckdb, bundle };
    })());
  }

  const { duckdb, bundle } = await _bundleCache.get(base);

  async function bootstrap() {
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    try {
      const worker = new Worker(workerUrl);
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      return { db, worker };
    } finally {
      URL.revokeObjectURL(workerUrl);
    }
  }

  const initial = await bootstrap();
  return createDuckDBClient(initial.db, {
    ...options, _worker: initial.worker, _bootstrap: bootstrap,
  });
}

/**
 * Create a DuckDBClient from a pre-initialized AsyncDuckDB instance.
 *
 * The caller is responsible for:
 * 1. Creating the DuckDB worker, logger, and AsyncDuckDB instance
 * 2. Calling db.instantiate() with the WASM module
 * 3. Passing the db instance here
 *
 * This adapter will handle connection creation and extension loading.
 *
 * @param {object} db - An AsyncDuckDB instance from duckdb-wasm-opfs-tempdir
 * @param {object} [options]
 * @param {string} [options.tempDirectory] - OPFS temp directory path (e.g., 'opfs://tmpdir_xxx')
 * @param {number} [options.memoryLimitMB] - DuckDB memory limit in MB; a runtime tuning knob for browser/Wasm memory pressure, not something derived from temp-directory page sizes
 * @param {Worker} [options._worker] - Internal: worker reference for terminate()
 * @returns {DuckDBClient}
 */
export function createDuckDBClient(db, options = {}) {
  let _db = db;
  let _conn = null;
  let _initialized = false;
  let _worker = null;
  const _bootstrap = options._bootstrap || null;

  if (options._worker) _worker = options._worker;
  else if (db._worker) _worker = db._worker;

  const client = {
    get db() {
      if (!_db) throw new DOMException('DuckDB client unavailable', 'AbortError');
      return _db;
    },

    get conn() {
      if (!_conn) throw new DOMException('DuckDB client unavailable', 'AbortError');
      return _conn;
    },

    async init() {
      if (_initialized) return;

      // Re-bootstrap after terminate() if a bootstrap function was provided
      if (!_db && _bootstrap) {
        const fresh = await _bootstrap();
        _db = fresh.db;
        _worker = fresh.worker;
      }
      if (!_db) throw new DOMException('DuckDB client unavailable', 'AbortError');

      _conn = await _db.connect();
      await _conn.query('INSTALL spatial; LOAD spatial;');
      await _conn.query('SET builtin_httpfs = false;');
      await _conn.query('INSTALL httpfs; LOAD httpfs;');
      await _conn.query('SET arrow_large_buffer_size=true;');

      if (options.tempDirectory) {
        await configureDuckDBTempDirectory(_db, sql => _conn.query(sql), options.tempDirectory);
      }
      // memory_limit is a high-level execution cap; temp_directory only affects
      // where spill files are written after that cap is exceeded.
      if (options.memoryLimitMB) {
        await _conn.query(`SET memory_limit = '${options.memoryLimitMB}MB';`);
      }

      _initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    },

    async query(sql) {
      return client.conn.query(sql);
    },

    terminate() {
      if (_worker) {
        _worker.terminate();
        console.log('[DuckDB] Worker terminated');
      }
      _worker = null;
      _conn = null;
      _db = null;
      _initialized = false;
    },
  };

  return client;
}
