import { asyncBufferFromUrl, parquetMetadataAsync } from 'hyparquet';
import { proxyUrl } from './proxy.js';

/**
 * @typedef {[number, number, number, number]} Bbox
 * [minx, miny, maxx, maxy] in WGS84.
 */

/**
 * Reads GeoParquet bbox metadata using hyparquet over HTTP range requests.
 * This is a drop-in alternative to ParquetBboxReader for environments where
 * reading footer metadata directly is preferable to querying DuckDB metadata tables.
 * For row groups it prefers GeoParquet 1.1 covering-column statistics and only
 * falls back to geospatial statistics when they are available.
 */
export class HyparquetBboxReader {
  /**
   * @param {{
   *   batchSize?: number,
   *   fetch?: typeof globalThis.fetch,
   *   requestInit?: RequestInit,
   *   initialFetchSize?: number,
   *   metadataLoader?: (url: string) => Promise<any>
   * }} [options]
   */
  constructor(options = {}) {
    const {
      batchSize = 10,
      fetch: customFetch,
      requestInit,
      initialFetchSize,
      metadataLoader,
    } = options;

    this._batchSize = Math.max(1, Number(batchSize) || 10);
    this._fetch = customFetch;
    this._requestInit = requestInit;
    this._initialFetchSize = initialFetchSize;
    this._metadataLoader = metadataLoader || this._loadMetadataFromUrl.bind(this);

    /** @type {Map<string, Promise<any | null>>} */
    this._metadataCache = new Map();
    /** @type {Map<string, Bbox | null>} */
    this._bboxCache = new Map();
    /** @type {Map<string, Record<string, Bbox> | null>} */
    this._rowGroupCache = new Map();
    this._cancelGeneration = 0;
  }

  cancel() {
    this._cancelGeneration += 1;
  }

  /**
   * @param {{ id: string, url: string }[]} files
   * @param {unknown} _duckdb
   * @param {{ signal?: AbortSignal }} [options]
   * @returns {Promise<Record<string, Bbox> | null>}
   */
  async getFileBboxes(files, _duckdb, options = {}) {
    if (!files?.length) return null;

    const runGeneration = this._cancelGeneration;
    const uncachedFiles = files.filter(file => !this._bboxCache.has(file.url));
    await this._processInBatches(uncachedFiles, async (file) => {
      const metadata = await this._getMetadata(file.url);
      this._throwIfCancelled(runGeneration, options.signal);
      this._bboxCache.set(file.url, this._extractFileBbox(metadata));
    }, runGeneration, options.signal);

    const result = {};
    for (const file of files) {
      const bbox = this._bboxCache.get(file.url);
      if (bbox) result[file.id] = bbox;
    }

    return Object.keys(result).length ? result : null;
  }

  /**
   * @param {{ id: string, url: string }[]} files
   * @param {unknown} _duckdb
   * @param {{ bboxColumn?: string, signal?: AbortSignal }} [options]
   * @returns {Promise<Record<string, Record<string, Bbox>> | null>}
   */
  async getRowGroupBboxes(files, _duckdb, options = {}) {
    if (!files?.length) return null;

    const runGeneration = this._cancelGeneration;
    const cacheKeyFor = (url) => `${url}::${options?.bboxColumn || ''}`;
    const uncachedFiles = files.filter(file => !this._rowGroupCache.has(cacheKeyFor(file.url)));

    await this._processInBatches(uncachedFiles, async (file) => {
      const metadata = await this._getMetadata(file.url);
      this._throwIfCancelled(runGeneration, options.signal);
      const extents = this._extractRowGroupBboxes(metadata, options?.bboxColumn);
      this._rowGroupCache.set(cacheKeyFor(file.url), extents);
    }, runGeneration, options.signal);

    const result = {};
    for (const file of files) {
      const rowGroups = this._rowGroupCache.get(cacheKeyFor(file.url));
      if (rowGroups && Object.keys(rowGroups).length) {
        result[file.id] = rowGroups;
      }
    }

    return Object.keys(result).length ? result : null;
  }

  async _processInBatches(items, worker, runGeneration, signal) {
    for (let i = 0; i < items.length; i += this._batchSize) {
      this._throwIfCancelled(runGeneration, signal);
      const batch = items.slice(i, i + this._batchSize);
      await Promise.all(batch.map(worker));
    }
  }

  async _getMetadata(url) {
    if (!this._metadataCache.has(url)) {
      const pending = this._metadataLoader(url).catch((error) => {
        console.error('[HyparquetBboxReader] Failed to load parquet metadata:', error);
        return null;
      });
      this._metadataCache.set(url, pending);
    }

    return this._metadataCache.get(url);
  }

  async _loadMetadataFromUrl(url) {
    const asyncBuffer = await asyncBufferFromUrl({
      url: proxyUrl(url),
      fetch: this._fetch,
      requestInit: this._requestInit,
    });

    return parquetMetadataAsync(asyncBuffer, {
      initialFetchSize: this._initialFetchSize,
    });
  }

  _extractFileBbox(metadata) {
    const geoMeta = this._getGeoMetadata(metadata);
    const primaryMeta = this._getPrimaryColumnMetadata(geoMeta);
    const bbox = primaryMeta?.bbox;
    if (!Array.isArray(bbox) || bbox.length < 4) return null;

    const candidate = bbox.slice(0, 4).map(value => Number(value));
    if (!this._isValidWgs84Bbox(...candidate)) return null;
    return candidate;
  }

  _extractRowGroupBboxes(metadata, bboxColumn) {
    if (!metadata?.row_groups?.length) return null;

    const coveringPaths = this._getCoveringBboxPaths(metadata, bboxColumn);
    const extents = {};

    metadata.row_groups.forEach((rowGroup, index) => {
      const bbox = this._getRowGroupStatsBbox(rowGroup, coveringPaths)
        || this._getRowGroupGeospatialBbox(rowGroup);
      if (bbox) {
        extents[`rg_${index}`] = bbox;
      }
    });

    return Object.keys(extents).length ? extents : null;
  }

  _getGeoMetadata(metadata) {
    const rawGeo = metadata?.key_value_metadata?.find(entry => entry.key === 'geo')?.value;
    if (!rawGeo) return null;

    try {
      return typeof rawGeo === 'string' ? JSON.parse(rawGeo) : rawGeo;
    } catch (error) {
      console.warn('[HyparquetBboxReader] Failed to parse GeoParquet metadata:', error);
      return null;
    }
  }

  _getPrimaryColumnMetadata(geoMeta) {
    const columns = geoMeta?.columns;
    if (!columns || typeof columns !== 'object') return null;

    const primaryColumn = geoMeta.primary_column || 'geometry';
    return columns[primaryColumn] || Object.values(columns)[0] || null;
  }

  _getCoveringBboxPaths(metadata, bboxColumn) {
    const primaryMeta = this._getPrimaryColumnMetadata(this._getGeoMetadata(metadata));
    const covering = primaryMeta?.covering?.bbox;
    if (covering) {
      return {
        xminPath: this._normalizePath(covering.xmin, ['bbox', 'xmin']),
        yminPath: this._normalizePath(covering.ymin, ['bbox', 'ymin']),
        xmaxPath: this._normalizePath(covering.xmax, ['bbox', 'xmax']),
        ymaxPath: this._normalizePath(covering.ymax, ['bbox', 'ymax']),
      };
    }

    if (bboxColumn) {
      return {
        xminPath: [bboxColumn, 'xmin'],
        yminPath: [bboxColumn, 'ymin'],
        xmaxPath: [bboxColumn, 'xmax'],
        ymaxPath: [bboxColumn, 'ymax'],
      };
    }

    return null;
  }

  _normalizePath(path, fallback) {
    if (Array.isArray(path)) return path.map(part => String(part));
    if (typeof path === 'string') {
      return path.split(',').map(part => part.trim()).filter(Boolean);
    }
    return fallback;
  }

  _getRowGroupGeospatialBbox(rowGroup) {
    for (const column of rowGroup?.columns || []) {
      const bbox = column?.meta_data?.geospatial_statistics?.bbox;
      if (!bbox) continue;

      const candidate = [
        Number(bbox.xmin),
        Number(bbox.ymin),
        Number(bbox.xmax),
        Number(bbox.ymax),
      ];
      if (this._isValidWgs84Bbox(...candidate)) return candidate;
    }

    return null;
  }

  _getRowGroupStatsBbox(rowGroup, coveringPaths) {
    if (!coveringPaths) return null;

    const values = {};
    for (const column of rowGroup?.columns || []) {
      const path = column?.meta_data?.path_in_schema;
      const stats = column?.meta_data?.statistics;
      if (!path || !stats) continue;

      if (this._pathsEqual(path, coveringPaths.xminPath)) values.xmin = this._toFiniteNumber(stats.min);
      if (this._pathsEqual(path, coveringPaths.yminPath)) values.ymin = this._toFiniteNumber(stats.min);
      if (this._pathsEqual(path, coveringPaths.xmaxPath)) values.xmax = this._toFiniteNumber(stats.max);
      if (this._pathsEqual(path, coveringPaths.ymaxPath)) values.ymax = this._toFiniteNumber(stats.max);
    }

    const candidate = [values.xmin, values.ymin, values.xmax, values.ymax];
    return this._isValidWgs84Bbox(...candidate) ? candidate : null;
  }

  _pathsEqual(left, right) {
    const normalizedLeft = this._normalizePath(left, []);
    const normalizedRight = this._normalizePath(right, []);
    return normalizedLeft.length === normalizedRight.length
      && normalizedLeft.every((part, index) => part === normalizedRight[index]);
  }

  _toFiniteNumber(value) {
    if (value == null) return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  _throwIfCancelled(runGeneration, signal) {
    if (signal?.aborted || this._cancelGeneration !== runGeneration) {
      throw new DOMException('BBox extraction cancelled', 'AbortError');
    }
  }

  _isValidWgs84Bbox(minx, miny, maxx, maxy) {
    return Number.isFinite(minx) && Number.isFinite(miny) &&
      Number.isFinite(maxx) && Number.isFinite(maxy) &&
      Math.abs(minx) <= 180 && Math.abs(maxx) <= 180 &&
      Math.abs(miny) <= 90 && Math.abs(maxy) <= 90;
  }
}
