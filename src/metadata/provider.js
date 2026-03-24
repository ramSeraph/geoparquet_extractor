/**
 * @module metadata/provider
 * Abstract base class for metadata providers.
 * Consumers subclass this to customize how partition URLs and bboxes are resolved.
 */

/**
 * @typedef {[number, number, number, number]} Bbox
 * [minx, miny, maxx, maxy] in WGS84.
 */

/**
 * Abstract metadata provider. Override methods to customize metadata resolution
 * for your data source (e.g., reading from a custom meta.json file).
 */
export class MetadataProvider {
  /**
   * Transform a source URL to a single parquet URL.
   * Default: replace .mosaic.json or .pmtiles extension with .parquet.
   * @param {string} sourceUrl
   * @returns {string}
   */
  getParquetUrl(sourceUrl) {
    return sourceUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  /**
   * Get the base directory URL (everything up to and including the last /).
   * @param {string} sourceUrl
   * @returns {string}
   */
  getBaseUrl(sourceUrl) {
    const lastSlash = sourceUrl.lastIndexOf('/');
    return sourceUrl.substring(0, lastSlash + 1);
  }

  /**
   * Get the list of partition filenames for a partitioned source.
   * @param {string} sourceUrl - The original source URL
   * @returns {Promise<string[] | null>} Array of partition filenames or null
   */
  async getPartitions(sourceUrl) {
    throw new Error('MetadataProvider.getPartitions() not implemented');
  }

  /**
   * Get bounding boxes for each partition file.
   * @param {string} sourceUrl
   * @returns {Promise<Object<string, Bbox> | null>} { filename: [minx,miny,maxx,maxy] } or null
   */
  async getExtents(sourceUrl) {
    throw new Error('MetadataProvider.getExtents() not implemented');
  }

  /**
   * Get the overall bounding box for a single (non-partitioned) parquet source.
   * @param {string} parquetUrl - Direct URL to the parquet file
   * @param {import('../duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Bbox | null>}
   */
  async getBbox(parquetUrl, duckdb) {
    throw new Error('MetadataProvider.getBbox() not implemented');
  }

  /**
   * Get per-row-group bounding boxes for a single parquet file.
   * @param {string} parquetUrl
   * @param {import('../duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Object<string, Bbox> | null>} { rg_N: [minx,miny,maxx,maxy] } or null
   */
  async getRowGroupBboxes(parquetUrl, duckdb) {
    throw new Error('MetadataProvider.getRowGroupBboxes() not implemented');
  }

  /**
   * Get per-row-group bounding boxes for multiple parquet files in one call.
   * @param {string[]} parquetUrls
   * @param {import('../duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Object<string, Object<string, Bbox>> | null>}
   *   { filename: { rg_N: [minx,miny,maxx,maxy] } } or null
   */
  async getRowGroupBboxesMulti(parquetUrls, duckdb) {
    throw new Error('MetadataProvider.getRowGroupBboxesMulti() not implemented');
  }

  /**
   * Get the list of parquet URLs to query for a source, filtered by bbox.
   * For partitioned sources, returns only partitions that overlap the bbox.
   * For single-file sources, returns the single parquet URL.
   * @param {string} sourceUrl
   * @param {boolean} [partitioned] - Whether the source is partitioned
   * @param {Bbox} [bbox] - Optional bbox to filter partitions
   * @returns {Promise<string[]>}
   */
  async getParquetUrls(sourceUrl, partitioned = false, bbox) {
    throw new Error('MetadataProvider.getParquetUrls() not implemented');
  }
}
