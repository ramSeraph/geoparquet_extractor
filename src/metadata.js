/**
 * @module metadata
 * Default metadata provider that reads GeoParquet metadata via DuckDB.
 * Subclass and override methods to customize for your data source
 * (e.g., partition-aware meta.json resolution).
 */

import { proxyUrl } from './proxy.js';

/**
 * @typedef {[number, number, number, number]} Bbox
 * [minx, miny, maxx, maxy] in WGS84.
 */

/**
 * Metadata provider for GeoParquet sources.
 * Reads parquet metadata via DuckDB queries (kv_metadata, parquet_metadata).
 * Override getExtents/getParquetUrls for partition-aware sources.
 */
export class MetadataProvider {
  constructor() {
    /** @type {Map<string, Bbox | null>} */
    this._bboxCache = new Map();
    /** @type {Map<string, object | null>} */
    this._rgBboxCache = new Map();
  }

  /**
   * Transform a source URL to a single parquet URL.
   * Default: returns sourceUrl as-is (assumes it is already a parquet URL).
   * Override to handle custom URL schemes (e.g., .pmtiles → .parquet).
   * @param {string} sourceUrl
   * @returns {string}
   */
  getParquetUrl(sourceUrl) {
    return sourceUrl;
  }

  /**
   * Get bounding boxes for each partition file.
   * Default: returns null. Override for partition-aware sources.
   * @param {string} sourceUrl
   * @returns {Promise<Object<string, Bbox> | null>} { filename: [minx,miny,maxx,maxy] } or null
   */
  async getExtents(sourceUrl) {
    return null;
  }

  /**
   * Get the list of parquet URLs to query for a source, filtered by bbox.
   * Default: returns single URL from getParquetUrl(). Override for partition filtering.
   * @param {string} sourceUrl
   * @param {boolean} [partitioned]
   * @param {Bbox} [bbox]
   * @returns {Promise<string[]>}
   */
  async getParquetUrls(sourceUrl, partitioned, bbox) {
    return [this.getParquetUrl(sourceUrl)];
  }

  /**
   * Get the overall bounding box for a single parquet source via DuckDB kv_metadata.
   * @param {string} parquetUrl
   * @param {import('./duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Bbox | null>}
   */
  async getBbox(parquetUrl, duckdb) {
    if (this._bboxCache.has(parquetUrl)) return this._bboxCache.get(parquetUrl);
    await duckdb.init();

    try {
      const safeUrl = proxyUrl(parquetUrl).replace(/'/g, "''");
      const geoMeta = await this._getGeoMetadata(safeUrl, duckdb);
      if (!geoMeta) { this._bboxCache.set(parquetUrl, null); return null; }

      const primaryCol = geoMeta.primary_column || 'geometry';
      const colMeta = geoMeta.columns?.[primaryCol];
      if (!colMeta?.bbox || colMeta.bbox.length < 4) { this._bboxCache.set(parquetUrl, null); return null; }

      const [minx, miny, maxx, maxy] = colMeta.bbox;
      if (!this._isValidWgs84Bbox(minx, miny, maxx, maxy)) {
        console.warn('[MetadataProvider] Parquet bbox outside WGS84 range:', colMeta.bbox);
        this._bboxCache.set(parquetUrl, null);
        return null;
      }

      this._bboxCache.set(parquetUrl, colMeta.bbox);
      return colMeta.bbox;
    } catch (error) {
      console.error('[MetadataProvider] Failed to read parquet bbox:', error);
      this._bboxCache.set(parquetUrl, null);
      return null;
    }
  }

  /**
   * Get per-row-group bounding boxes for a single parquet file.
   * @param {string} parquetUrl
   * @param {import('./duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Object<string, Bbox> | null>} { rg_N: [minx,miny,maxx,maxy] } or null
   */
  async getRowGroupBboxes(parquetUrl, duckdb) {
    const result = await this.getRowGroupBboxesMulti([parquetUrl], duckdb);
    if (!result) return null;
    const firstKey = Object.keys(result)[0];
    return firstKey ? result[firstKey] : null;
  }

  /**
   * Get per-row-group bounding boxes for multiple parquet files in one call.
   * @param {string[]} parquetUrls
   * @param {import('./duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Object<string, Object<string, Bbox>> | null>}
   */
  async getRowGroupBboxesMulti(parquetUrls, duckdb) {
    if (!parquetUrls?.length) return null;

    const cacheKey = parquetUrls.join('\n');
    if (this._rgBboxCache.has(cacheKey)) return this._rgBboxCache.get(cacheKey);

    await duckdb.init();

    try {
      const proxyUrls = parquetUrls.map(u => proxyUrl(u));
      const proxyToFilename = {};
      for (let i = 0; i < parquetUrls.length; i++) {
        proxyToFilename[proxyUrls[i]] = parquetUrls[i].split('/').pop();
      }

      const firstSafeUrl = proxyUrls[0].replace(/'/g, "''");
      const coveringPaths = await this._getCoveringBboxPaths(firstSafeUrl, duckdb);
      if (!coveringPaths) { this._rgBboxCache.set(cacheKey, null); return null; }

      const { xminPath, yminPath, xmaxPath, ymaxPath } = coveringPaths;
      const allPaths = [xminPath, yminPath, xmaxPath, ymaxPath];
      const urlList = proxyUrls.map(u => `'${u.replace(/'/g, "''")}'`).join(',');

      const queryResult = await duckdb.conn.query(
        `SELECT file_name, row_group_id, path_in_schema, stats_min, stats_max
         FROM parquet_metadata([${urlList}])
         WHERE path_in_schema IN (${allPaths.map(p => `'${p}'`).join(',')})
         ORDER BY file_name, row_group_id, path_in_schema`
      );

      const rows = queryResult.toArray();
      if (rows.length === 0) { this._rgBboxCache.set(cacheKey, null); return null; }

      const fileGroups = {};
      for (const row of rows) {
        const fileName = proxyToFilename[row.file_name] || row.file_name;
        if (!fileGroups[fileName]) fileGroups[fileName] = {};
        const rgId = Number(row.row_group_id);
        if (!fileGroups[fileName][rgId]) fileGroups[fileName][rgId] = {};
        const path = row.path_in_schema;
        if (path === xminPath) fileGroups[fileName][rgId].xmin = Number(row.stats_min);
        if (path === yminPath) fileGroups[fileName][rgId].ymin = Number(row.stats_min);
        if (path === xmaxPath) fileGroups[fileName][rgId].xmax = Number(row.stats_max);
        if (path === ymaxPath) fileGroups[fileName][rgId].ymax = Number(row.stats_max);
      }

      const allExtents = {};
      for (const [fileName, groups] of Object.entries(fileGroups)) {
        const extents = {};
        for (const [rgId, g] of Object.entries(groups)) {
          if (g.xmin == null || g.ymin == null || g.xmax == null || g.ymax == null) continue;
          if (!this._isValidWgs84Bbox(g.xmin, g.ymin, g.xmax, g.ymax)) continue;
          extents[`rg_${rgId}`] = [g.xmin, g.ymin, g.xmax, g.ymax];
        }
        if (Object.keys(extents).length > 0) {
          allExtents[fileName] = extents;
        }
      }

      const result = Object.keys(allExtents).length > 0 ? allExtents : null;
      this._rgBboxCache.set(cacheKey, result);
      return result;
    } catch (error) {
      console.error('[MetadataProvider] Failed to read row group bboxes:', error);
      this._rgBboxCache.set(cacheKey, null);
      return null;
    }
  }

  // --- Internal helpers ---

  async _getGeoMetadata(safeUrl, duckdb) {
    const result = await duckdb.conn.query(
      `SELECT value FROM parquet_kv_metadata('${safeUrl}') WHERE key='geo'`
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return this._parseKvBlob(rows[0].value);
  }

  async _getCoveringBboxPaths(safeUrl, duckdb) {
    const geoMeta = await this._getGeoMetadata(safeUrl, duckdb);
    if (!geoMeta) return null;

    const primaryCol = geoMeta.primary_column || 'geometry';
    const covering = geoMeta.columns?.[primaryCol]?.covering?.bbox;
    if (!covering) return null;

    return {
      xminPath: covering.xmin?.join(', ') || 'bbox, xmin',
      yminPath: covering.ymin?.join(', ') || 'bbox, ymin',
      xmaxPath: covering.xmax?.join(', ') || 'bbox, xmax',
      ymaxPath: covering.ymax?.join(', ') || 'bbox, ymax',
    };
  }

  _parseKvBlob(raw) {
    if (raw instanceof Uint8Array || raw instanceof ArrayBuffer) {
      raw = new TextDecoder().decode(raw);
    } else if (typeof raw !== 'string') {
      raw = String(raw);
    }
    return JSON.parse(raw);
  }

  _isValidWgs84Bbox(minx, miny, maxx, maxy) {
    return Math.abs(minx) <= 180 && Math.abs(maxx) <= 180 &&
           Math.abs(miny) <= 90 && Math.abs(maxy) <= 90;
  }
}
