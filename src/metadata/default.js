/**
 * @module metadata/default
 * Default metadata provider that reads GeoParquet kv_metadata via DuckDB.
 * Supports partitioned datasets via meta.json files and row-group bbox extraction.
 */

import { MetadataProvider } from './provider.js';
import { proxyUrl } from '../utils.js';

/**
 * Default implementation of MetadataProvider.
 * Reads parquet metadata via DuckDB queries (kv_metadata, parquet_metadata).
 * For partitioned sources, fetches a meta.json file listing partitions + extents.
 */
export class DefaultMetadataProvider extends MetadataProvider {
  constructor() {
    super();
    /** @type {Map<string, object>} */
    this._metaJsonCache = new Map();
    /** @type {Map<string, string[]>} */
    this._partitionCache = new Map();
    /** @type {Map<string, import('./provider.js').Bbox | null>} */
    this._bboxCache = new Map();
    /** @type {Map<string, object | null>} */
    this._rgBboxCache = new Map();
  }

  /**
   * Get the meta.json URL for a source.
   * Default: appends '.meta.json' to the parquet URL from getParquetUrl().
   * Override this to change how meta.json URLs are resolved.
   * @param {string} sourceUrl
   * @returns {string}
   */
  getMetaJsonUrl(sourceUrl) {
    return this.getParquetUrl(sourceUrl) + '.meta.json';
  }

  /** @override */
  async getPartitions(sourceUrl) {
    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    if (this._partitionCache.has(metaUrl)) {
      return this._partitionCache.get(metaUrl);
    }
    const metaJson = await this._fetchMetaJson(metaUrl);
    if (!metaJson) return null;
    return this._partitionCache.get(metaUrl);
  }

  /** @override */
  async getExtents(sourceUrl) {
    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    const metaJson = await this._fetchMetaJson(metaUrl);
    return metaJson?.extents ?? null;
  }

  /** @override */
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

  /** @override */
  async getRowGroupBboxes(parquetUrl, duckdb) {
    const result = await this.getRowGroupBboxesMulti([parquetUrl], duckdb);
    if (!result) return null;
    const firstKey = Object.keys(result)[0];
    return firstKey ? result[firstKey] : null;
  }

  /** @override */
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

  /** @override */
  async getParquetUrls(sourceUrl, partitioned, bbox) {
    if (!partitioned) {
      return [this.getParquetUrl(sourceUrl)];
    }

    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    const metaJson = await this._fetchMetaJson(metaUrl);
    if (!metaJson) return [this.getParquetUrl(sourceUrl)];

    const baseUrl = this.getBaseUrl(sourceUrl);
    const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];

    if (!bbox || !metaJson.extents) {
      return partitions.map(p => baseUrl + p);
    }

    // Filter partitions by bbox overlap
    const [west, south, east, north] = bbox;
    return partitions
      .filter(p => {
        const ext = metaJson.extents[p];
        if (!ext || ext.length < 4) return true; // include if no extent info
        const [pMinx, pMiny, pMaxx, pMaxy] = ext;
        return pMinx <= east && pMaxx >= west && pMiny <= north && pMaxy >= south;
      })
      .map(p => baseUrl + p);
  }

  // --- Internal helpers ---

  async _fetchMetaJson(metaUrl) {
    if (this._metaJsonCache.has(metaUrl)) {
      return this._metaJsonCache.get(metaUrl);
    }

    try {
      const proxied = proxyUrl(metaUrl);
      const response = await fetch(proxied);
      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }

      const metaJson = await response.json();
      this._metaJsonCache.set(metaUrl, metaJson);
      const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];
      this._partitionCache.set(metaUrl, partitions);
      return metaJson;
    } catch (error) {
      console.error('Error fetching partition metadata:', error);
      return null;
    }
  }

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
