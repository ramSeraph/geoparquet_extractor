// ExtentData — data-fetching and GeoJSON generation for partition/row-group bboxes.
// This is the headless (no UI/map) portion extracted from extent_handler.js.
// Consumers use this class to fetch extent data and convert it to GeoJSON
// for visualization in their own map library.

/**
 * @typedef {Object} ExtentDataOptions
 * @property {import('./metadata/provider.js').MetadataProvider} metadataProvider
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
   * @param {(msg: string) => void} [options.onStatus] - Status callback
   * @returns {Promise<{ dataExtents: Object|null, rgExtents: Object|null }>}
   */
  async fetchExtents({ sourceUrl, partitioned = false, includeRowGroups = true, onStatus }) {
    if (partitioned) {
      return this._fetchPartitioned(sourceUrl, includeRowGroups, onStatus);
    } else {
      return this._fetchSingle(sourceUrl, includeRowGroups, onStatus);
    }
  }

  /**
   * Convert an extents map { name: [minx,miny,maxx,maxy] } to GeoJSON.
   * Returns a FeatureCollection with Polygon features (bbox rectangles)
   * and Point features (label anchors at top-left corner).
   *
   * @param {Object<string, number[]>} extents
   * @returns {{ polygons: object, labelPoints: object }}
   */
  toGeoJSON(extents) {
    if (!extents) return { polygons: emptyFC(), labelPoints: emptyFC() };

    const polyFeatures = [];
    const labelFeatures = [];

    for (const [name, bbox] of Object.entries(extents)) {
      const [minx, miny, maxx, maxy] = normalizeBbox(bbox);
      const label = extractLabel(name) ?? '';

      polyFeatures.push({
        type: 'Feature',
        properties: { name, label },
        geometry: {
          type: 'Polygon',
          coordinates: [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]],
        },
      });

      if (label) {
        labelFeatures.push({
          type: 'Feature',
          properties: { label },
          geometry: { type: 'Point', coordinates: [minx, maxy] },
        });
      }
    }

    return {
      polygons: { type: 'FeatureCollection', features: polyFeatures },
      labelPoints: { type: 'FeatureCollection', features: labelFeatures },
    };
  }

  // --- Private ---

  async _fetchPartitioned(sourceUrl, includeRowGroups, onStatus) {
    const extents = await this._metadataProvider.getExtents(sourceUrl);
    const dataExtents = extents && Object.keys(extents).length ? extents : null;

    let rgExtents = null;
    if (includeRowGroups && this._duckdb) {
      const parquetUrls = await this._metadataProvider.getParquetUrls(sourceUrl);
      if (parquetUrls?.length) {
        onStatus?.('Loading row groups...');
        const allRgBboxes = await this._metadataProvider.getRowGroupBboxesMulti(
          parquetUrls, this._duckdb
        );
        if (allRgBboxes) {
          rgExtents = {};
          for (const [filename, rgGroups] of Object.entries(allRgBboxes)) {
            const partLabel = extractLabel(filename);
            for (const [rgKey, bbox] of Object.entries(rgGroups)) {
              rgExtents[partLabel ? `${partLabel}.${rgKey.replace('rg_', '')}` : rgKey] = bbox;
            }
          }
          if (!Object.keys(rgExtents).length) rgExtents = null;
        }
      }
    }

    return { dataExtents, rgExtents };
  }

  async _fetchSingle(sourceUrl, includeRowGroups, onStatus) {
    const parquetUrls = await this._metadataProvider.getParquetUrls(sourceUrl);
    const parquetUrl = parquetUrls?.[0];
    if (!parquetUrl) return { dataExtents: null, rgExtents: null };

    const bbox = await this._metadataProvider.getBbox(sourceUrl, this._duckdb);

    let dataExtents = null;
    if (bbox) {
      const filename = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
      dataExtents = { [filename]: bbox };
    }

    let rgExtents = null;
    if (includeRowGroups && this._duckdb) {
      onStatus?.('Loading row groups...');
      rgExtents = await this._metadataProvider.getRowGroupBboxes(parquetUrl, this._duckdb);
    }

    return { dataExtents, rgExtents };
  }
}

// --- Helpers ---

function normalizeBbox(bbox) {
  return Array.isArray(bbox)
    ? bbox
    : [bbox.minx, bbox.miny, bbox.maxx, bbox.maxy];
}

/**
 * Extract a human-readable label from a partition filename or row-group key.
 * @param {string} name
 * @returns {string|null}
 */
export function extractLabel(name) {
  const clean = name.replace(/\.parquet$/, '');
  const dotMatch = clean.match(/\.(\d+)$/);
  if (dotMatch) return dotMatch[1];
  const rgMatch = clean.match(/^rg_(\d+)$/);
  if (rgMatch) return rgMatch[1];
  return null;
}

function emptyFC() {
  return { type: 'FeatureCollection', features: [] };
}
