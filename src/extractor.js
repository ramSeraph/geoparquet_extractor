// GeoParquetExtractor — main orchestrator for partial downloads.
// Headless (no DOM/UI dependencies), designed for library consumers.

import { proxyUrl, OPFS_PREFIX_TMPDIR, getOpfsPrefixes, ScopedProgress } from './utils.js';
import { CsvFormatHandler } from './formats/csv.js';
import { GeoJsonFormatHandler } from './formats/geojson.js';
import { GeoParquetFormatHandler } from './formats/geoparquet.js';
import { GeoPackageFormatHandler } from './formats/geopackage.js';
import { ShapefileFormatHandler } from './formats/shapefile.js';
import { KmlFormatHandler } from './formats/kml.js';
import { DxfFormatHandler } from './formats/dxf.js';

export const FORMAT_OPTIONS = [
  { value: 'geopackage', label: 'GeoPackage (.gpkg)' },
  { value: 'geojson', label: 'GeoJSON' },
  { value: 'geojsonseq', label: 'GeoJSONSeq (.geojsonl)' },
  { value: 'geoparquet', label: 'GeoParquet (v1.1)' },
  { value: 'geoparquet2', label: 'GeoParquet (v2.0)' },
  { value: 'csv', label: 'CSV (WKT geometry)' },
  { value: 'shapefile', label: 'Shapefile (.shp)' },
  { value: 'kml', label: 'KML (.kml)' },
  { value: 'dxf', label: 'DXF (.dxf)' },
];

/** Normalize extent (array or object) to [minx, miny, maxx, maxy]. */
function extentBounds(extent) {
  return Array.isArray(extent)
    ? extent
    : [extent.minx, extent.miny, extent.maxx, extent.maxy];
}

/** Returns the fraction of extent overlapped by bbox (0–1), or null if degenerate. */
function bboxOverlapRatio(extent, bbox) {
  if (!extent) return null;
  const [minx, miny, maxx, maxy] = extentBounds(extent);
  const area = (maxx - minx) * (maxy - miny);
  if (area <= 0) return null;
  const ix = Math.max(0, Math.min(bbox[2], maxx) - Math.max(bbox[0], minx));
  const iy = Math.max(0, Math.min(bbox[3], maxy) - Math.max(bbox[1], miny));
  return Math.min(1, (ix * iy) / area);
}

/**
 * @param {string} format
 * @param {object} opts
 * @returns {import('./formats/base.js').FormatHandler}
 */
function getFormatHandler(format, opts) {
  switch (format) {
    case 'csv': return new CsvFormatHandler(opts);
    case 'geojson': return new GeoJsonFormatHandler({ commaSeparated: true, ...opts });
    case 'geojsonseq': return new GeoJsonFormatHandler({ commaSeparated: false, ...opts });
    case 'geoparquet': return new GeoParquetFormatHandler({ version: '1.1', ...opts });
    case 'geoparquet2': return new GeoParquetFormatHandler({ version: '2.0', ...opts });
    case 'geopackage': return new GeoPackageFormatHandler(opts);
    case 'shapefile': return new ShapefileFormatHandler(opts);
    case 'kml': return new KmlFormatHandler(opts);
    case 'dxf': return new DxfFormatHandler(opts);
    default: throw new Error(`Unsupported format: ${format}`);
  }
}

const DOWNLOAD_CLEANUP_DELAY_MS = 120_000;
const PROGRESS_WRITE_START = 5;
const PROGRESS_WRITE_END = 90;

// Web Lock constants for session-scoped OPFS cleanup
const SESSION_LOCK_PREFIX = 'gpqe_session_';

const OPFS_PREFIXES = getOpfsPrefixes();

function extractSessionId(name) {
  for (const prefix of OPFS_PREFIXES) {
    if (name.startsWith(prefix)) return name.slice(prefix.length).split('_')[0];
  }
  return null;
}

// Memory config
const MEMORY_STEP = 128;
const MEMORY_MIN_MB = 512;

export function getDeviceMaxMemoryMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  return Math.max(MEMORY_MIN_MB, Math.floor(deviceMemGB * 1024 * 0.75 / MEMORY_STEP) * MEMORY_STEP);
}

export function getDefaultMemoryLimitMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  const halfMB = Math.floor(deviceMemGB * 1024 * 0.5 / MEMORY_STEP) * MEMORY_STEP;
  return Math.max(MEMORY_MIN_MB, Math.min(halfMB, getDeviceMaxMemoryMB()));
}

export { MEMORY_STEP, MEMORY_MIN_MB };

/**
 * @typedef {Object} GeoParquetExtractorOptions
 * @property {import('./duckdb_adapter.js').DuckDBClient} duckdb - Pre-initialized DuckDB client
 * @property {import('./metadata/provider.js').MetadataProvider} [metadataProvider] - Optional metadata provider
 * @property {string} [gpkgWorkerUrl] - URL for GeoPackage worker (or Worker instance)
 * @property {Worker} [gpkgWorker] - Pre-constructed GeoPackage Worker instance
 * @property {number} [memoryLimitMB] - DuckDB memory limit in MB
 */

/**
 * @typedef {Object} ExtractOptions
 * @property {string[]} [urls] - Direct parquet URLs (skip metadata resolution)
 * @property {string} [sourceUrl] - Source URL for metadata resolution
 * @property {boolean} [partitioned] - Whether the source is partitioned
 * @property {number[]} bbox - [west, south, east, north] bounding box
 * @property {string} format - Output format (csv, geojson, geojsonseq, geoparquet, geoparquet2, geopackage, shapefile, kml, dxf)
 * @property {string} [baseName] - Base filename for download (without extension)
 * @property {(pct: number) => void} [onProgress] - Progress callback (0–100)
 * @property {(msg: string) => void} [onStatus] - Status message callback
 */

export class GeoParquetExtractor {
  /**
   * @param {GeoParquetExtractorOptions} options
   */
  constructor(options) {
    const { duckdb, metadataProvider, gpkgWorkerUrl, gpkgWorker, memoryLimitMB } = options;
    if (!duckdb) throw new Error('duckdb is required');

    this._duckdb = duckdb;
    this._metadataProvider = metadataProvider || null;
    this._gpkgWorkerUrl = gpkgWorkerUrl || null;
    this._gpkgWorker = gpkgWorker || null;
    this._memoryLimitMB = memoryLimitMB || null;
    this._formatHandler = null;
    this._cancelled = false;
    this._rejectCancel = null;
    this._cancelPromise = null;
    this._initialized = false;

    // Generate a unique session ID for OPFS scoping
    this._sessionId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    // Hold a Web Lock for this session's lifetime
    this._lockName = `${SESSION_LOCK_PREFIX}${this._sessionId}`;
    this._lockReady = new Promise(resolve => {
      navigator.locks.request(this._lockName, () => {
        resolve();
        return new Promise(() => {});
      });
    });
  }

  /**
   * Initialize DuckDB temp directory and memory settings.
   * Called automatically by prepare(), but can be called early.
   */
  async init(onStatus) {
    if (this._initialized) return;

    await this._lockReady;

    onStatus?.('Configuring DuckDB...');
    await this._duckdb.query(`SET temp_directory = 'opfs://${OPFS_PREFIX_TMPDIR}${this._sessionId}'`);

    if (this._memoryLimitMB) {
      await this._duckdb.query(`SET memory_limit = '${this._memoryLimitMB}MB'`);
    }

    this._initialized = true;
  }

  /**
   * Resolve parquet URLs from metadata provider for a given source + bbox.
   * @param {string} sourceUrl
   * @param {boolean} partitioned
   * @param {number[]} bbox - [west, south, east, north]
   * @param {(msg: string) => void} [onStatus]
   * @returns {Promise<string[]>}
   */
  async resolveUrls(sourceUrl, partitioned, bbox, onStatus) {
    if (!this._metadataProvider) {
      throw new Error('MetadataProvider required to resolve URLs from sourceUrl');
    }

    if (!partitioned) {
      const urls = await this._metadataProvider.getParquetUrls(sourceUrl);
      return urls;
    }

    const extents = await this._metadataProvider.getExtents(sourceUrl);
    if (!extents || Object.keys(extents).length === 0) {
      throw new Error('Could not load partition metadata');
    }

    const filtered = [];
    for (const [filename, extent] of Object.entries(extents)) {
      const [minx, miny, maxx, maxy] = extentBounds(extent);
      if (!(bbox[2] < minx || bbox[0] > maxx || bbox[3] < miny || bbox[1] > maxy)) {
        filtered.push(filename);
      }
    }

    if (filtered.length === 0) {
      throw new Error('No data found in current bbox');
    }

    onStatus?.(`Found ${filtered.length} partition(s) in bbox...`);

    // Resolve filenames to full URLs
    const allUrls = await this._metadataProvider.getParquetUrls(sourceUrl);
    const baseUrl = allUrls[0]?.replace(/[^/]+$/, '') || '';
    return filtered.map(f => baseUrl + f);
  }

  /**
   * Estimate download size based on file sizes and bbox overlap.
   * @param {string[]} parquetUrls
   * @param {number[]} bbox - [west, south, east, north]
   * @param {string} [sourceUrl] - For getting extents
   * @param {boolean} [partitioned]
   * @returns {Promise<number>}
   */
  async estimateSize(parquetUrls, bbox, sourceUrl, partitioned) {
    // Get file sizes via HEAD requests
    const sizes = await Promise.all(
      parquetUrls.map(url => this._getFileSize(url))
    );

    let fileExtents = {};
    if (this._metadataProvider && sourceUrl) {
      if (partitioned) {
        fileExtents = await this._metadataProvider.getExtents(sourceUrl) || {};
      } else {
        const overallBbox = await this._metadataProvider.getBbox(sourceUrl, this._duckdb);
        if (overallBbox) {
          fileExtents = { [parquetUrls[0]]: overallBbox };
        }
      }
    }

    this._throwIfCancelled();

    let totalEstimate = 0;
    for (let i = 0; i < parquetUrls.length; i++) {
      const fileSize = sizes[i];
      if (!fileSize) continue;

      const filename = parquetUrls[i].split('/').pop();
      const extent = fileExtents[filename] || fileExtents[parquetUrls[i]];
      const ratio = bboxOverlapRatio(extent, bbox);
      totalEstimate += fileSize * (ratio ?? 1);
    }

    return Math.round(totalEstimate);
  }

  /**
   * Prepare for download: resolve URLs, estimate size, create format handler.
   * Returns the format handler so the caller can inspect warnings/estimates.
   * @param {ExtractOptions} options
   * @returns {Promise<import('./formats/base.js').FormatHandler>}
   */
  async prepare(options) {
    const { urls, sourceUrl, partitioned, bbox, format, onProgress, onStatus, memoryLimitMB } = options;

    this._cancelled = false;
    this._cancelPromise = new Promise((_, reject) => {
      this._rejectCancel = reject;
    });

    if (memoryLimitMB) {
      this._memoryLimitMB = memoryLimitMB;
    }

    await Promise.race([this.init(onStatus), this._cancelPromise]);
    this._throwIfCancelled();

    if (this._memoryLimitMB) {
      await this._duckdb.query(`SET memory_limit = '${this._memoryLimitMB}MB'`);
    }

    onProgress?.(0);

    // Resolve parquet URLs
    let parquetUrls;
    if (urls && urls.length > 0) {
      parquetUrls = urls;
    } else if (sourceUrl) {
      parquetUrls = await this.resolveUrls(sourceUrl, !!partitioned, bbox, onStatus);
    } else {
      throw new Error('Either urls or sourceUrl is required');
    }

    this._throwIfCancelled();

    onStatus?.(`Estimating download size from ${parquetUrls.length} file(s)...`);
    const estimatedBytes = await this.estimateSize(parquetUrls, bbox, sourceUrl, partitioned);

    this._throwIfCancelled();
    onProgress?.(PROGRESS_WRITE_START);

    // Apply proxy URL to all parquet URLs
    const resolvedUrls = parquetUrls.map(u => proxyUrl(u));

    // Format handlers expect bbox as { west, south, east, north } object
    const bboxObj = Array.isArray(bbox)
      ? { west: bbox[0], south: bbox[1], east: bbox[2], north: bbox[3] }
      : bbox;

    const handlerOpts = {
      sessionId: this._sessionId,
      duckdb: this._duckdb,
      urls: resolvedUrls,
      bbox: bboxObj,
      estimatedBytes,
    };

    // Pass gpkg worker config through if needed
    if (format === 'geopackage') {
      handlerOpts.gpkgWorker = this._gpkgWorker || this._gpkgWorkerUrl;
    }

    this._formatHandler = getFormatHandler(format, handlerOpts);
    return this._formatHandler;
  }

  /**
   * Execute the download using a previously prepared format handler.
   * @param {import('./formats/base.js').FormatHandler} formatHandler
   * @param {object} options
   * @param {string} options.baseName - Base filename for download
   * @param {(pct: number) => void} [options.onProgress]
   * @param {(msg: string) => void} [options.onStatus]
   * @returns {Promise<boolean>}
   */
  async download(formatHandler, { baseName, onProgress, onStatus }) {
    try {
      const writeProgress = new ScopedProgress(onProgress, PROGRESS_WRITE_START, PROGRESS_WRITE_END);

      await Promise.race([
        formatHandler.write({
          onProgress: writeProgress.callback,
          onStatus,
        }),
        this._cancelPromise,
      ]);

      this._throwIfCancelled();
      onProgress?.(PROGRESS_WRITE_END);

      onStatus?.('Saving file...');
      await formatHandler.triggerDownload(baseName, DOWNLOAD_CLEANUP_DELAY_MS);

      onProgress?.(100);
      return true;

    } catch (e) {
      this._throwIfCancelled();
      throw e;
    } finally {
      this._rejectCancel = null;
      this._cancelPromise = null;
      this._formatHandler = null;
      await formatHandler.cleanup();
    }
  }

  /**
   * Convenience: prepare + download in one call.
   * @param {ExtractOptions & { baseName: string }} options
   * @returns {Promise<boolean>}
   */
  async extract(options) {
    const { baseName, onProgress, onStatus, ...prepareOpts } = options;
    const handler = await this.prepare({ ...prepareOpts, onProgress, onStatus });

    const warning = handler.getFormatWarning?.();
    if (warning?.isBlocking) {
      throw new Error(warning.message);
    }

    return this.download(handler, { baseName, onProgress, onStatus });
  }

  /** Cancel any in-flight download. */
  cancel() {
    this._cancelled = true;
    this._formatHandler?.cancel();

    setTimeout(() => {
      if (this._rejectCancel) {
        this._rejectCancel(new DOMException('Download cancelled', 'AbortError'));
        this._rejectCancel = null;
      }
    }, 1000);
  }

  /** Generate a suggested base filename. */
  static getDownloadBaseName(sourceName, bbox) {
    const coordStr = bbox
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    return `${sourceName.replace(/\s+/g, '_')}.${coordStr}`;
  }

  /**
   * Clean up orphaned OPFS entries from dead sessions.
   * Call periodically or on app startup.
   */
  static async cleanupOrphanedFiles() {
    try {
      const { held } = await navigator.locks.query();
      const aliveSessions = new Set(
        held.filter(l => l.name.startsWith(SESSION_LOCK_PREFIX))
          .map(l => l.name.slice(SESSION_LOCK_PREFIX.length))
      );

      const root = await navigator.storage.getDirectory();
      let count = 0;
      for await (const [name, handle] of root) {
        const sessionId = extractSessionId(name);
        if (sessionId && !aliveSessions.has(sessionId)) {
          try {
            await root.removeEntry(name, { recursive: handle.kind === 'directory' });
            count++;
          } catch (e) { /* may be locked or already removed */ }
        }
      }

      if (count > 0) console.log(`[GeoParquetExtractor] Cleaned up ${count} orphaned OPFS entries`);
    } catch (e) {
      console.warn('[GeoParquetExtractor] OPFS orphan cleanup failed:', e);
    }
  }

  _throwIfCancelled() {
    if (this._cancelled) throw new DOMException('Download cancelled', 'AbortError');
  }

  async _getFileSize(url) {
    try {
      const resp = await fetch(proxyUrl(url), { method: 'HEAD' });
      if (!resp.ok) return null;
      const cl = resp.headers.get('content-length');
      return cl ? parseInt(cl, 10) : null;
    } catch {
      return null;
    }
  }
}
