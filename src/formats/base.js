/**
 * @module formats/base
 * Base class for partial download format handlers.
 * Manages OPFS file lifecycle, DuckDB queries, bbox filtering, and progress tracking.
 */

import { getStorageEstimate, formatSize } from '../utils.js';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Parse top-level field names from a DuckDB STRUCT type string.
 * E.g. "STRUCT(a INTEGER, b VARCHAR)" → ["a", "b"]
 * Handles nested types like "STRUCT(a STRUCT(x INT, y INT), b VARCHAR)"
 * @param {string} structType
 * @returns {string[]}
 */
function parseStructFieldNames(structType) {
  const openParen = structType.indexOf('(');
  const inner = structType.slice(openParen + 1, structType.lastIndexOf(')'));
  const fields = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    else if (ch === ',' && depth === 0) {
      fields.push(extractFieldName(inner.slice(start, i).trim()));
      start = i + 1;
    }
  }
  const last = inner.slice(start).trim();
  if (last) fields.push(extractFieldName(last));
  return fields;
}

/** @param {string} fieldDef e.g. 'a INTEGER' or '"my field" VARCHAR' */
function extractFieldName(fieldDef) {
  if (fieldDef.startsWith('"')) {
    const endQuote = fieldDef.indexOf('"', 1);
    return fieldDef.slice(1, endQuote);
  }
  return fieldDef.split(/\s+/)[0];
}

/**
 * @typedef {Object} FormatHandlerOptions
 * @property {string} sessionId - Unique session/tab ID for OPFS scoping
 * @property {import('../duckdb_adapter.js').DuckDBClient} duckdb
 * @property {string[]} urls - Proxied parquet URLs to read from
 * @property {{ west: number, south: number, east: number, north: number }} bbox
 * @property {number | null} [estimatedBytes] - Estimated output size for progress
 * @property {boolean} [flattenStructs] - Flatten STRUCT columns into separate columns
 */

export { parseStructFieldNames };

export class FormatHandler {
  /** @param {FormatHandlerOptions} opts */
  constructor({ sessionId, duckdb, urls, bbox, estimatedBytes, flattenStructs } = {}) {
    this.sessionId = sessionId;
    this.duckdb = duckdb;
    this.urls = urls || [];
    this.bbox = bbox;
    this.estimatedBytes = estimatedBytes ?? null;
    this.flattenStructs = flattenStructs ?? false;
    this._duckdbRegisteredPaths = new Set();
    this._prepared = false;
    this._downloadedFiles = new Set();
    this._activeTrackers = [];
    this.cancelled = false;
  }

  /** @returns {number} Expected peak OPFS usage in bytes */
  getExpectedBrowserStorageUsage() { throw new Error('Not implemented'); }

  /** @returns {number} Total expected disk usage including downloads */
  getTotalExpectedDiskUsage() { throw new Error('Not implemented'); }

  /**
   * Return a format-specific warning or error before download starts, or null.
   * @returns {{ message: string, isBlocking: boolean } | null}
   */
  getFormatWarning() { return null; }

  cancel() {
    this.cancelled = true;
  }

  throwIfCancelled() {
    if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');
  }

  /**
   * Start a background interval that polls disk usage and reports progress
   * based on estimated output size. Returns a stop function.
   */
  startDiskProgressTracker(onProgress, onStatus, messagePrefix, expectedBytes, intervalMs = 5000) {
    if (!expectedBytes || expectedBytes <= 0 || this.cancelled) return () => {};

    let baselineUsage = null;
    let stopped = false;
    let lastPct = 0;

    const poll = async () => {
      if (stopped) return;
      const { usage: currentUsage } = await getStorageEstimate();
      if (typeof currentUsage !== 'number') return;
      if (baselineUsage === null) {
        baselineUsage = currentUsage;
        return;
      }
      const written = Math.max(0, currentUsage - baselineUsage);
      const pct = Math.min(100, (written / expectedBytes) * 100);
      if (pct > lastPct) lastPct = pct;
      onProgress?.(lastPct);
      onStatus?.(`${messagePrefix} ~${formatSize(written)} / ~${formatSize(expectedBytes)}`);
    };

    poll();
    const id = setInterval(poll, intervalMs);

    const stop = () => {
      stopped = true;
      clearInterval(id);
      this._activeTrackers = this._activeTrackers.filter(s => s !== stop);
    };
    this._activeTrackers.push(stop);
    return stop;
  }

  /** DuckDB read_parquet() expression for the source URLs */
  get parquetSource() {
    const urlList = this.urls.map(u => `'${u}'`).join(', ');
    return `read_parquet([${urlList}], union_by_name=true)`;
  }

  /**
   * Create and register an OPFS file path for DuckDB to write to.
   * @param {string} prefix - OPFS prefix (e.g. OPFS_PREFIX_TMP)
   * @param {string} ext - File extension
   * @returns {Promise<string>} opfs:// path
   */
  async createDuckdbOpfsFile(prefix, ext) {
    const path = `opfs://${prefix}${this.sessionId}_${Date.now()}.${ext}`;
    await this.duckdb.db.registerOPFSFileName(path);
    await sleep(5);
    this._duckdbRegisteredPaths.add(path);
    return path;
  }

  async _getRoot() {
    if (!this._opfsRoot) this._opfsRoot = await navigator.storage.getDirectory();
    return this._opfsRoot;
  }

  async removeOpfsFile(opfsFileName) {
    try {
      const root = await this._getRoot();
      await root.removeEntry(opfsFileName);
    } catch (e) { /* may already be cleaned up */ } // eslint-disable-line no-unused-vars
  }

  async releaseDuckdbOpfsFile(opfsFileName) {
    try {
      await this.duckdb.db.dropFile(opfsFileName);
    } catch (e) { /* db terminated or file already gone */ } // eslint-disable-line no-unused-vars
    this._duckdbRegisteredPaths.delete(opfsFileName);
  }

  async releaseDuckdbOpfsFiles() {
    const paths = Array.from(this._duckdbRegisteredPaths);
    for (const path of paths) {
      await this.releaseDuckdbOpfsFile(path);
    }
  }

  async getOpfsHandle(name, { create = false } = {}) {
    const root = await this._getRoot();
    return root.getFileHandle(name.replace('opfs://', ''), { create });
  }

  async getOpfsFile(opfsPath) {
    const handle = await this.getOpfsHandle(opfsPath);
    return handle.getFile();
  }

  /**
   * Build a SELECT column expression, optionally flattening STRUCT columns.
   * @param {string[]} [excludeCols] - Columns to exclude
   * @returns {Promise<string>} SQL column expression
   */
  async buildColumnSelect(excludeCols = ['geometry', 'bbox']) {
    if (!this.flattenStructs) {
      return `* EXCLUDE (${excludeCols.join(', ')})`;
    }

    const result = await this.duckdb.conn.query(
      `SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM ${this.parquetSource})`
    );
    const parts = [];
    for (let i = 0; i < result.numRows; i++) {
      const name = result.getChildAt(0).get(i);
      const type = result.getChildAt(1).get(i);
      if (excludeCols.includes(name)) continue;
      if (type.startsWith('STRUCT')) {
        const fieldNames = parseStructFieldNames(type);
        for (const field of fieldNames) {
          parts.push(`"${name}"."${field}" AS "${name}.${field}"`);
        }
      } else {
        parts.push(`"${name}"`);
      }
    }
    return parts.join(', ');
  }

  /**
   * Create an intermediate parquet file on OPFS with WKB geometry from the remote source.
   * @param {Object} opts
   * @param {string} opts.prefix - OPFS filename prefix
   * @param {string[]} [opts.extraColumns] - Additional SQL expressions for the SELECT
   * @param {Function} opts.onProgress - Progress callback (0–100)
   * @param {Function} opts.onStatus - Status message callback
   * @returns {Promise<string>} opfs:// path to the intermediate parquet file
   */
  async createIntermediateParquet({ prefix, extraColumns, onProgress, onStatus }) {
    onStatus?.('Filtering data...');
    const tempPath = await this.createDuckdbOpfsFile(prefix, 'parquet');

    const stopTracker = this.startDiskProgressTracker(
      onProgress, onStatus, 'Filtering data:', this.estimatedBytes
    );
    const extraSelect = extraColumns?.length
      ? extraColumns.join(', ') + ', '
      : '';
    const columnSelect = await this.buildColumnSelect();
    try {
      await this.duckdb.conn.query(`
        COPY (
          SELECT
            hex(ST_AsWKB(geometry)::BLOB) AS geom_wkb,
            ${extraSelect}${columnSelect}
          FROM ${this.parquetSource}
          WHERE ${this.bboxFilter}
        ) TO '${tempPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
      `);
    } finally {
      stopTracker();
    }

    this.throwIfCancelled();
    return tempPath;
  }

  /**
   * Discover attribute columns from an intermediate parquet file via DuckDB DESCRIBE.
   * @param {string} parquetPath - opfs:// path
   * @param {Set<string>} internalCols - Columns to exclude
   * @returns {Promise<Array<{name: string, type: string}>>}
   */
  async describeColumns(parquetPath, internalCols) {
    const result = await this.duckdb.conn.query(
      `SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '${parquetPath}')`
    );
    const columns = [];
    for (let i = 0; i < result.numRows; i++) {
      const name = result.getChildAt(0).get(i);
      const type = result.getChildAt(1).get(i);
      if (internalCols.has(name)) continue;
      columns.push({ name, type });
    }
    return columns;
  }

  /**
   * Return a list of { downloadName, blobParts } for triggerDownload.
   * @param {string} baseName
   * @returns {Promise<Array<{downloadName: string, blobParts: (Blob|File|Uint8Array)[]}>>}
   */
  async getDownloadMap(_baseName) {
    throw new Error('Not implemented');
  }

  /** WKT polygon for the current bbox */
  get bboxWkt() {
    return `POLYGON((${this.bbox.west} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.south}))`;
  }

  /** DuckDB WHERE clause for bbox + geometry intersection */
  get bboxFilter() {
    const { west, south, east, north } = this.bbox;
    const bboxRowGroupFilter = `bbox.xmin <= ${east} AND bbox.xmax >= ${west} AND bbox.ymin <= ${north} AND bbox.ymax >= ${south}`;
    return `${bboxRowGroupFilter} AND ST_Intersects(geometry, ST_GeomFromText('${this.bboxWkt}'))`;
  }

  /**
   * Trigger browser download(s) from the format handler's output files.
   * @param {string} baseName - Base filename (without extension)
   * @param {number} cleanupDelayMs - Delay before cleaning up OPFS files
   */
  async triggerDownload(baseName, cleanupDelayMs) {
    const entries = await this.getDownloadMap(baseName);

    for (const { downloadName, blobParts } of entries) {
      const blob = new Blob(blobParts);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = downloadName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      const opfsFiles = blobParts.filter(p => p instanceof File).map(f => f.name);
      for (const f of opfsFiles) this._downloadedFiles.add(f);

      setTimeout(async () => {
        URL.revokeObjectURL(url);
        const root = await this._getRoot();
        for (const name of opfsFiles) {
          try { await root.removeEntry(name); } catch (e) { /* ignore */ } // eslint-disable-line no-unused-vars
        }
      }, cleanupDelayMs);

      if (entries.length > 1) await new Promise(r => setTimeout(r, 200));
    }
  }

  /**
   * Run the format handler's write pipeline.
   * @param {{ onProgress?: (pct: number) => void, onStatus?: (msg: string) => void }} callbacks
   */
  async write(callbacks) {
    try {
      await this._write(callbacks);
    } finally {
      await this.releaseDuckdbOpfsFiles();
    }
  }

  /** @protected - Override in subclass */
  async _write(_callbacks) {
    throw new Error('Not implemented');
  }

  /** Clean up all OPFS files belonging to this session */
  async cleanup() {
    for (const stop of [...this._activeTrackers]) stop();

    const root = await this._getRoot();
    for await (const [name] of root) {
      if (!name.includes(this.sessionId)) continue;
      if (this._downloadedFiles.has(name)) continue;
      try {
        await root.removeEntry(name, { recursive: true });
      } catch (e) { /* may already be cleaned up */ } // eslint-disable-line no-unused-vars
    }
  }
}
