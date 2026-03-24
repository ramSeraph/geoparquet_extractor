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
 * Create a DuckDBClient from a pre-initialized duckdb-wasm-opfs-tempdir setup.
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
 * @param {number} [options.memoryLimitMB] - DuckDB memory limit in MB
 * @returns {DuckDBClient}
 */
export function createDuckDBClient(db, options = {}) {
  let conn = null;
  let initialized = false;
  let worker = null;

  // Try to get the worker reference from the db for terminate()
  if (db._worker) worker = db._worker;

  const client = {
    get db() {
      if (!db) throw new DOMException('DuckDB client unavailable', 'AbortError');
      return db;
    },

    get conn() {
      if (!conn) throw new DOMException('DuckDB client unavailable', 'AbortError');
      return conn;
    },

    async init() {
      if (initialized) return;

      conn = await db.connect();
      await conn.query('INSTALL spatial; LOAD spatial;');
      await conn.query('SET builtin_httpfs = false;');
      await conn.query('INSTALL httpfs; LOAD httpfs;');
      await conn.query('SET arrow_large_buffer_size=true;');

      if (options.tempDirectory) {
        await conn.query(`SET temp_directory = '${options.tempDirectory}';`);
      }
      if (options.memoryLimitMB) {
        await conn.query(`SET memory_limit = '${options.memoryLimitMB}MB';`);
      }

      initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    },

    async query(sql) {
      if (!initialized) await client.init();
      return conn.query(sql);
    },

    terminate() {
      if (worker) {
        worker.terminate();
        console.log('[DuckDB] Worker terminated');
      }
      worker = null;
      conn = null;
      db = null;
      initialized = false;
    },
  };

  return client;
}
