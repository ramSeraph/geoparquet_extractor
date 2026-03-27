// GeoParquetExtractor — main orchestrator for partial downloads.
// Headless (no DOM/UI dependencies), designed for library consumers.

import { proxyUrl } from './proxy.js';
import { OPFS_PREFIX_TMPDIR, getOpfsPrefixes } from './utils.js';
import { ScopedProgress } from './scoped_progress.js';
import { SizeGetter } from './size_getter.js';
import { CsvFormatHandler } from './formats/csv.js';
import { GeoJsonFormatHandler } from './formats/geojson.js';
import { GeoParquetFormatHandler } from './formats/geoparquet.js';
import { GeoPackageFormatHandler } from './formats/geopackage.js';
import { ShapefileFormatHandler } from './formats/shapefile.js';
import { KmlFormatHandler } from './formats/kml.js';
import { DxfFormatHandler } from './formats/dxf.js';
import { SourceResolver } from './source_resolver.js';
import { ParquetBboxReader } from './parquet_bbox_reader.js';

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

/**
 * @typedef {Object} GeoParquetExtractorOptions
 * @property {import('./duckdb_adapter.js').DuckDBClient} duckdb - Pre-initialized DuckDB client
 * @property {import('./source_resolver.js').SourceResolver} [sourceResolver] - Optional source resolver
 * @property {import('./parquet_bbox_reader.js').ParquetBboxReader} [bboxReader] - Optional parquet bbox reader
 * @property {string} [gpkgWorkerUrl] - URL for GeoPackage worker (or Worker instance)
 * @property {Worker} [gpkgWorker] - Pre-constructed GeoPackage Worker instance
 * @property {number} [memoryLimitMB] - DuckDB memory limit in MB
 */

/**
 * @typedef {Object} ExtractOptions
 * @property {string[]} [urls] - Direct parquet URLs (skip source resolution)
 * @property {string} [sourceUrl] - Source URL for source resolution
 * @property {number[]} bbox - [west, south, east, north] bounding box
 * @property {string} format - Output format (csv, geojson, geojsonseq, geoparquet, geoparquet2, geopackage, shapefile, kml, dxf)
 * @property {string} [baseName] - Base filename for download (without extension)
 * @property {boolean} [flattenStructs] - Flatten STRUCT columns into separate columns
 * @property {(pct: number) => void} [onProgress] - Progress callback (0–100)
 * @property {(msg: string) => void} [onStatus] - Status message callback
 */

export class GeoParquetExtractor {
  /**
   * @param {GeoParquetExtractorOptions} options
   */
  constructor(options) {
    const { duckdb, sourceResolver, bboxReader, gpkgWorkerUrl, gpkgWorker, memoryLimitMB } = options;
    if (!duckdb) throw new Error('duckdb is required');

    this._duckdb = duckdb;
    this._sourceResolver = sourceResolver || new SourceResolver();
    this._bboxReader = bboxReader || new ParquetBboxReader();
    this._gpkgWorkerUrl = gpkgWorkerUrl || null;
    this._gpkgWorker = gpkgWorker || null;
    this._memoryLimitMB = memoryLimitMB || null;
    this._sizeGetter = new SizeGetter();
    this._formatHandler = null;
    this._cancelled = false;
    this._rejectCancel = null;
    this._cancelPromise = null;
    this._initialized = false;

    this._sessionId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    this._lockName = `${SESSION_LOCK_PREFIX}${this._sessionId}`;
    this._lockReady = new Promise(resolve => {
      navigator.locks.request(this._lockName, () => {
        resolve();
        return new Promise(() => {});
      });
    });
  }

  async init(onStatus) {
    if (this._initialized) return;

    await this._lockReady;

    onStatus?.('Configuring DuckDB...');
    await this._duckdb.init();
    await this._duckdb.query(`SET temp_directory = 'opfs://${OPFS_PREFIX_TMPDIR}${this._sessionId}'`);

    if (this._memoryLimitMB) {
      await this._duckdb.query(`SET memory_limit = '${this._memoryLimitMB}MB'`);
    }

    this._initialized = true;
  }

  /**
   * Resolve parquet files from source resolver for a given source + bbox.
   * @param {string} sourceUrl
   * @param {number[]} bbox - [west, south, east, north]
   * @param {(msg: string) => void} [onStatus]
   * @returns {Promise<{ id: string, url: string, bbox?: number[] | null }[]>}
   */
  async resolveFiles(sourceUrl, bbox, onStatus) {
    const { files } = await this._sourceResolver.resolve(sourceUrl, { bbox });
    if (!files?.length) {
      throw new Error('No data found in current bbox');
    }

    onStatus?.(`Found ${files.length} file(s) in bbox...`);
    return files;
  }

  /**
   * Estimate download size based on file sizes and bbox overlap.
   * @param {{ id: string, url: string, bbox?: number[] | null }[]} files
   * @param {number[]} bbox - [west, south, east, north]
   * @returns {Promise<number>}
   */
  async estimateSize(files, bbox) {
    const sizes = await Promise.all(files.map(file => this._sizeGetter.getSizeBytes(file.url)));

    let fallbackBboxes = null;
    if (files.some(file => !file.bbox)) {
      fallbackBboxes = await this._bboxReader.getFileBboxes(
        files.filter(file => !file.bbox),
        this._duckdb,
      );
    }

    this._throwIfCancelled();

    let totalEstimate = 0;
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const fileSize = sizes[i];
      if (!fileSize) continue;

      const extent = file.bbox || fallbackBboxes?.[file.id] || null;
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
    const {
      urls,
      sourceUrl,
      bbox,
      format,
      flattenStructs = false,
      onProgress,
      onStatus,
      memoryLimitMB,
    } = options;

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

    let files;
    if (urls && urls.length > 0) {
      files = urls.map(url => ({
        id: url.substring(url.lastIndexOf('/') + 1) || url,
        url,
        bbox: null,
      }));
    } else if (sourceUrl) {
      files = await this.resolveFiles(sourceUrl, bbox, onStatus);
    } else {
      throw new Error('Either urls or sourceUrl is required');
    }

    const parquetUrls = files.map(file => file.url);

    this._throwIfCancelled();

    onStatus?.(`Estimating download size from ${parquetUrls.length} file(s)...`);
    const estimatedBytes = await this.estimateSize(files, bbox);

    this._throwIfCancelled();
    onProgress?.(PROGRESS_WRITE_START);

    const resolvedUrls = parquetUrls.map(u => proxyUrl(u));
    const bboxObj = Array.isArray(bbox)
      ? { west: bbox[0], south: bbox[1], east: bbox[2], north: bbox[3] }
      : bbox;

    const handlerOpts = {
      sessionId: this._sessionId,
      duckdb: this._duckdb,
      urls: resolvedUrls,
      bbox: bboxObj,
      estimatedBytes,
      flattenStructs,
    };

    if (format === 'geopackage') {
      handlerOpts.gpkgWorker = this._gpkgWorker || this._gpkgWorkerUrl;
    }

    this._formatHandler = getFormatHandler(format, handlerOpts);
    return this._formatHandler;
  }

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

  async extract(options) {
    const { baseName, onProgress, onStatus, ...prepareOpts } = options;
    const handler = await this.prepare({ ...prepareOpts, onProgress, onStatus });

    const warning = handler.getFormatWarning?.();
    if (warning?.isBlocking) {
      throw new Error(warning.message);
    }

    return this.download(handler, { baseName, onProgress, onStatus });
  }

  cancel() {
    this._cancelled = true;
    this._formatHandler?.cancel();
    this._duckdb.terminate();
    this._initialized = false;

    setTimeout(() => {
      if (this._rejectCancel) {
        this._rejectCancel(new DOMException('Download cancelled', 'AbortError'));
        this._rejectCancel = null;
      }
    }, 1000);
  }

  static getDownloadBaseName(sourceName, bbox) {
    const coordStr = bbox
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    return `${sourceName.replace(/\s+/g, '_')}.${coordStr}`;
  }

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
          } catch { /* may be locked or already removed */ }
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
}
