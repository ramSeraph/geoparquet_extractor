// ExtentData — fetches file-level and row-group-level bounding boxes.
// Returns raw extent data; callers handle presentation (GeoJSON, labels, etc.).

import { ParquetBboxReader } from './parquet_bbox_reader.js';

/**
 * @typedef {Object} ExtentDataOptions
 * @property {import('./source_resolver.js').SourceResolver} sourceResolver
 * @property {import('./parquet_bbox_reader.js').ParquetBboxReader} [bboxReader]
 * @property {import('./duckdb_adapter.js').DuckDBClient} [duckdb] - Required for parquet bbox queries
 */

export class ExtentData {
  /**
   * @param {ExtentDataOptions} options
   */
  constructor({ sourceResolver, bboxReader, duckdb }) {
    if (!sourceResolver) throw new Error('sourceResolver is required');
    this._sourceResolver = sourceResolver;
    this._bboxReader = bboxReader || new ParquetBboxReader();
    this._duckdb = duckdb || null;
    this._cancelGeneration = 0;
  }

  /**
   * Attach or replace the DuckDB client used for parquet metadata fallback.
   * @param {import('./duckdb_adapter.js').DuckDBClient | null} duckdb
   */
  setDuckDB(duckdb) {
    this._duckdb = duckdb || null;
  }

  cancel() {
    this._cancelGeneration += 1;
    this._bboxReader?.cancel?.();
    this._duckdb?.terminate?.();
    this._duckdb = null;
  }

  /**
   * Fetch file-level and row-group-level extent data for a source.
   *
   * @param {object} options
   * @param {string} options.sourceUrl - Source route URL
    * @param {boolean} [options.includeRowGroups=true] - Whether to fetch row-group bboxes
    * @param {string} [options.bboxColumn] - Explicit bbox struct column name (e.g. 'bbox')
    *   to use for row-group stats when GeoParquet covering metadata is absent.
    * @param {(msg: string) => void} [options.onStatus] - Status callback
    * @param {AbortSignal} [options.signal] - Abort signal for cancelling extent loading.
    * @returns {Promise<{
    *   dataExtents: Object<string, number[]> | null,
    *   rgExtents: Object<string, Object<string, number[]>> | null
    * }>}
    */
  async fetchExtents({ sourceUrl, includeRowGroups = true, bboxColumn, onStatus, signal }) {
    const runGeneration = this._cancelGeneration;
    const abortHandler = () => this.cancel();
    signal?.addEventListener('abort', abortHandler, { once: true });

    try {
      this._throwIfCancelled(runGeneration, signal);

      const { files } = await this._sourceResolver.resolve(sourceUrl);
      this._throwIfCancelled(runGeneration, signal);
      if (!files?.length) return { dataExtents: null, rgExtents: null };

      let dataExtents = {};
      for (const file of files) {
        if (file.bbox) dataExtents[file.id] = file.bbox;
      }

      if (Object.keys(dataExtents).length !== files.length && this._duckdb) {
        const fallbackBboxes = await this._bboxReader.getFileBboxes(
          files.filter(file => !file.bbox),
          this._duckdb,
          { signal },
        );
        this._throwIfCancelled(runGeneration, signal);
        if (fallbackBboxes) dataExtents = { ...dataExtents, ...fallbackBboxes };
      }

      dataExtents = Object.keys(dataExtents).length ? dataExtents : null;

      let rgExtents = null;
      if (includeRowGroups && this._duckdb) {
        onStatus?.('Loading row groups...');
        rgExtents = await this._bboxReader.getRowGroupBboxes(files, this._duckdb, { bboxColumn, signal });
        this._throwIfCancelled(runGeneration, signal);
      }

      return { dataExtents, rgExtents };
    } catch (error) {
      if (this._isCancelled(runGeneration, signal)) {
        throw new DOMException('Extent loading cancelled', 'AbortError');
      }
      throw error;
    } finally {
      signal?.removeEventListener('abort', abortHandler);
    }
  }

  _isCancelled(runGeneration, signal) {
    return signal?.aborted || this._cancelGeneration !== runGeneration;
  }

  _throwIfCancelled(runGeneration, signal) {
    if (this._isCancelled(runGeneration, signal)) {
      throw new DOMException('Extent loading cancelled', 'AbortError');
    }
  }
}
