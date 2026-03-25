// ExtentData — fetches partition-level and row-group-level bounding boxes.
// Returns raw extent data; callers handle presentation (GeoJSON, labels, etc.).

/**
 * @typedef {Object} ExtentDataOptions
 * @property {import('./metadata.js').MetadataProvider} metadataProvider
 * @property {import('./duckdb_adapter.js').DuckDBClient} [duckdb] - Required for row-group bbox queries
 */

export class ExtentData {
  /**
   * @param {ExtentDataOptions} options
   */
  constructor({ metadataProvider, duckdb }) {
    if (!metadataProvider) throw new Error('metadataProvider is required');
    this._metadataProvider = metadataProvider;
    this._duckdb = duckdb || null;
  }

  /**
   * Fetch partition-level and row-group-level extent data for a source.
   *
   * @param {object} options
   * @param {string} options.sourceUrl - Source route URL
   * @param {boolean} [options.partitioned=false] - Whether the source is partitioned
   * @param {boolean} [options.includeRowGroups=true] - Whether to fetch row-group bboxes
   * @param {string} [options.bboxColumn] - Explicit bbox struct column name (e.g. 'bbox')
   *   to use for row-group stats when GeoParquet covering metadata is absent.
   * @param {(msg: string) => void} [options.onStatus] - Status callback
   * @returns {Promise<{
   *   dataExtents: Object<string, number[]> | null,
   *   rgExtents: Object<string, Object<string, number[]>> | null
   * }>}
   *   dataExtents: { filename: [minx,miny,maxx,maxy] }
   *   rgExtents: { filename: { rg_N: [minx,miny,maxx,maxy] } }
   */
  async fetchExtents({ sourceUrl, partitioned = false, includeRowGroups = true, bboxColumn, onStatus }) {
    if (partitioned) {
      return this._fetchPartitioned(sourceUrl, includeRowGroups, bboxColumn, onStatus);
    } else {
      return this._fetchSingle(sourceUrl, includeRowGroups, bboxColumn, onStatus);
    }
  }

  // --- Private ---

  async _fetchPartitioned(sourceUrl, includeRowGroups, bboxColumn, onStatus) {
    const extents = await this._metadataProvider.getExtents(sourceUrl);
    const dataExtents = extents && Object.keys(extents).length ? extents : null;

    let rgExtents = null;
    if (includeRowGroups && this._duckdb) {
      const parquetUrls = await this._metadataProvider.getParquetUrls(sourceUrl);
      if (parquetUrls?.length) {
        onStatus?.('Loading row groups...');
        rgExtents = await this._metadataProvider.getRowGroupBboxesMulti(
          parquetUrls, this._duckdb, { bboxColumn }
        );
      }
    }

    return { dataExtents, rgExtents };
  }

  async _fetchSingle(sourceUrl, includeRowGroups, bboxColumn, onStatus) {
    const parquetUrls = await this._metadataProvider.getParquetUrls(sourceUrl);
    const parquetUrl = parquetUrls?.[0];
    if (!parquetUrl) return { dataExtents: null, rgExtents: null };

    const bbox = await this._metadataProvider.getBbox(parquetUrl, this._duckdb);

    let dataExtents = null;
    if (bbox) {
      const filename = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
      dataExtents = { [filename]: bbox };
    }

    let rgExtents = null;
    if (includeRowGroups && this._duckdb) {
      onStatus?.('Loading row groups...');
      const rgBboxes = await this._metadataProvider.getRowGroupBboxes(
        parquetUrl, this._duckdb, { bboxColumn }
      );
      if (rgBboxes) {
        const filename = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
        rgExtents = { [filename]: rgBboxes };
      }
    }

    return { dataExtents, rgExtents };
  }
}
