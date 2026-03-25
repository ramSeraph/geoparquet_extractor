// GeoPackage format handler for partial downloads
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// gpkg_worker reads parquet + writes GPKG entirely inside the worker

import { FormatHandler } from './base.js';
import { OPFS_PREFIX_GPKG_TMP, OPFS_PREFIX_GPKG } from '../utils.js';
import { ScopedProgress } from '../scoped_progress.js';

export class GeoPackageFormatHandler extends FormatHandler {
  /**
   * @param {object} opts
   * @param {string | URL | Worker} [opts.gpkgWorker] - Worker URL, URL object, or Worker instance for GeoPackage writing
   */
  constructor(opts = {}) {
    super(opts);
    this.gpkgFileName = null;
    this.extension = 'gpkg';
    this._worker = null;
    this._gpkgWorkerConfig = opts.gpkgWorker ?? null;
  }

  cancel() {
    super.cancel();
    this._worker?.terminate();
    this._worker = null;
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * (1 + 1.5); }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * (1.5 + 1.5); }

  async _write({ onProgress, onStatus }) {
    if (!this._gpkgWorkerConfig) {
      throw new Error(
        'GeoPackage format requires a gpkgWorker. Pass a Worker instance, URL, or URL string ' +
        'when constructing GeoPackageFormatHandler or GeoParquetExtractor.'
      );
    }

    // Stage 1 (0–70%): Write intermediate parquet to OPFS (remote data fetch)
    const stage1 = new ScopedProgress(onProgress, 0, 70);
    const tempParquetPath = await this.createIntermediateParquet({
      prefix: OPFS_PREFIX_GPKG_TMP,
      extraColumns: [
        "ST_GeometryType(geometry) AS _geom_type",
        "ST_XMin(geometry) AS _bbox_minx", "ST_YMin(geometry) AS _bbox_miny",
        "ST_XMax(geometry) AS _bbox_maxx", "ST_YMax(geometry) AS _bbox_maxy",
      ],
      onProgress: stage1.callback, onStatus,
    });

    await this.releaseDuckdbOpfsFile(tempParquetPath);

    this.throwIfCancelled();

    // Stage 2 (70–100%): Worker reads parquet + writes GPKG
    onStatus?.('Writing GeoPackage...');
    const stage2 = new ScopedProgress(onProgress, 70, 100);
    this.gpkgFileName = `${OPFS_PREFIX_GPKG}${this.sessionId}_${Date.now()}.gpkg`;

    const stopTracker2 = this.startDiskProgressTracker(
      stage2.callback, onStatus, 'Writing GeoPackage:', this.estimatedBytes * 1.5
    );

    // Create worker from config
    if (this._gpkgWorkerConfig instanceof Worker) {
      this._worker = this._gpkgWorkerConfig;
    } else {
      this._worker = new Worker(this._gpkgWorkerConfig, { type: 'module' });
    }

    try {
      await new Promise((resolve, reject) => {
        const msgId = 1;
        this._worker.onmessage = (e) => {
          const { id, result, error, progress, status } = e.data;
          if (id !== msgId) return;
          if (progress) {
            onStatus?.(status);
            return;
          }
          if (error) reject(new Error(error));
          else resolve(result);
        };
        this._worker.onerror = (e) => {
          reject(new Error(e.message));
        };
        this._worker.postMessage({
          id: msgId,
          method: 'writeFromParquet',
          args: { parquetFileName: tempParquetPath.replace('opfs://', ''), gpkgFileName: this.gpkgFileName },
        });
      });
    } finally {
      stopTracker2();
      this._worker?.terminate();
      this._worker = null;
      await this.removeOpfsFile(tempParquetPath.replace('opfs://', ''));
    }

    onProgress?.(100);
  }

  async getDownloadMap(baseName) {
    const file = await this.getOpfsFile(this.gpkgFileName);
    return [{ downloadName: `${baseName}.${this.extension}`, blobParts: [file] }];
  }
}
