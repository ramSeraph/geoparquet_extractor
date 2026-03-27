/**
 * @typedef {[number, number, number, number]} Bbox
 * [minx, miny, maxx, maxy] in WGS84.
 */

/**
 * @typedef {Object} ResolvedFile
 * @property {string} id
 * @property {string} url
 * @property {Bbox | null} [bbox]
 */

/**
 * Base source resolver.
 * Resolves an app-level source URL to concrete parquet files with optional file bboxes.
 */
export class SourceResolver {
  /**
   * @param {string} sourceUrl
   * @param {{ bbox?: Bbox, signal?: AbortSignal, onStatus?: (msg: string) => void }} [options]
   * @returns {Promise<{ files: ResolvedFile[] }>}
   */
  async resolve(sourceUrl, _options = {}) {
    const filename = sourceUrl.substring(sourceUrl.lastIndexOf('/') + 1) || sourceUrl;
    return {
      files: [{ id: filename, url: sourceUrl, bbox: null }],
    };
  }
}
