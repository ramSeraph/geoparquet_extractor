import { proxyUrl } from './proxy.js';

/**
 * @typedef {[number, number, number, number]} Bbox
 * [minx, miny, maxx, maxy] in WGS84.
 */

/**
 * Reads bbox metadata directly from GeoParquet files using DuckDB.
 * Handles file-level bbox fallback and row-group bbox extraction.
 */
export class ParquetBboxReader {
  constructor() {
    /** @type {Map<string, Bbox | null>} */
    this._bboxCache = new Map();
    /** @type {Map<string, Record<string, Bbox> | null>} */
    this._rowGroupCache = new Map();
  }

  /**
   * @param {{ id: string, url: string }[]} files
   * @param {import('./duckdb_adapter.js').DuckDBClient} duckdb
   * @returns {Promise<Record<string, Bbox> | null>}
   */
  async getFileBboxes(files, duckdb) {
    if (!files?.length || !duckdb) return null;

    const uncachedFiles = files.filter(file => !this._bboxCache.has(file.url));
    if (uncachedFiles.length > 0) {
      await duckdb.init();

      try {
        const proxyUrls = uncachedFiles.map(file => proxyUrl(file.url));
        const safeUrls = proxyUrls.map(url => `'${url.replace(/'/g, "''")}'`);
        const proxyToFile = Object.fromEntries(
          proxyUrls.map((url, index) => [url, uncachedFiles[index]])
        );

        const result = await duckdb.conn.query(
          `SELECT file_name, value
           FROM parquet_kv_metadata([${safeUrls.join(',')}])
           WHERE key='geo'`
        );

        for (const file of uncachedFiles) {
          this._bboxCache.set(file.url, null);
        }

        for (const row of result.toArray()) {
          const file = proxyToFile[row.file_name];
          if (!file) continue;

          const geoMeta = this._parseKvBlob(row.value);
          const primaryCol = geoMeta.primary_column || 'geometry';
          const colMeta = geoMeta.columns?.[primaryCol];
          if (!colMeta?.bbox || colMeta.bbox.length < 4) continue;

          const [minx, miny, maxx, maxy] = colMeta.bbox;
          if (!this._isValidWgs84Bbox(minx, miny, maxx, maxy)) {
            console.warn('[ParquetBboxReader] Parquet bbox outside WGS84 range:', colMeta.bbox);
            continue;
          }

          this._bboxCache.set(file.url, colMeta.bbox);
        }
      } catch (error) {
        console.error('[ParquetBboxReader] Failed to read parquet bboxes:', error);
        for (const file of uncachedFiles) {
          this._bboxCache.set(file.url, null);
        }
      }
    }

    const result = {};
    for (const file of files) {
      const bbox = this._bboxCache.get(file.url);
      if (bbox) result[file.id] = bbox;
    }

    return Object.keys(result).length ? result : null;
  }

  /**
   * @param {{ id: string, url: string }[]} files
   * @param {import('./duckdb_adapter.js').DuckDBClient} duckdb
   * @param {{ bboxColumn?: string }} [options]
   * @returns {Promise<Record<string, Record<string, Bbox>> | null>}
   */
  async getRowGroupBboxes(files, duckdb, options = {}) {
    if (!files?.length || !duckdb) return null;

    const uncachedFiles = files.filter(file => !this._rowGroupCache.has(file.url));
    if (uncachedFiles.length > 0) {
      await duckdb.init();

      try {
        const proxyUrls = uncachedFiles.map(file => proxyUrl(file.url));
        const safeUrls = proxyUrls.map(url => `'${url.replace(/'/g, "''")}'`);
        const proxyToFile = Object.fromEntries(
          proxyUrls.map((url, index) => [url, uncachedFiles[index]])
        );

        const firstSafeUrl = proxyUrls[0].replace(/'/g, "''");
        const coveringPaths = await this._getCoveringBboxPaths(firstSafeUrl, duckdb, options?.bboxColumn);
        if (!coveringPaths) {
          for (const file of uncachedFiles) {
            this._rowGroupCache.set(file.url, null);
          }
        } else {
          const { xminPath, yminPath, xmaxPath, ymaxPath } = coveringPaths;
          const pathToField = {
            [xminPath]: ['xmin', 'stats_min'],
            [yminPath]: ['ymin', 'stats_min'],
            [xmaxPath]: ['xmax', 'stats_max'],
            [ymaxPath]: ['ymax', 'stats_max'],
          };

          const queryResult = await duckdb.conn.query(
            `SELECT file_name, row_group_id, path_in_schema, stats_min, stats_max
             FROM parquet_metadata([${safeUrls.join(',')}])
             WHERE path_in_schema IN ('${xminPath}', '${yminPath}', '${xmaxPath}', '${ymaxPath}')
             ORDER BY file_name, row_group_id, path_in_schema`
          );

          const fileGroups = {};
          for (const row of queryResult.toArray()) {
            const file = proxyToFile[row.file_name];
            if (!file) continue;

            if (!fileGroups[file.url]) fileGroups[file.url] = {};
            const rgId = Number(row.row_group_id);
            if (!fileGroups[file.url][rgId]) fileGroups[file.url][rgId] = {};

            const mapping = pathToField[row.path_in_schema];
            if (mapping) {
              fileGroups[file.url][rgId][mapping[0]] = Number(row[mapping[1]]);
            }
          }

          for (const file of uncachedFiles) {
            const groups = fileGroups[file.url] || {};
            const extents = {};
            for (const [rgId, group] of Object.entries(groups)) {
              if (group.xmin == null || group.ymin == null || group.xmax == null || group.ymax == null) continue;
              if (!this._isValidWgs84Bbox(group.xmin, group.ymin, group.xmax, group.ymax)) continue;
              extents[`rg_${rgId}`] = [group.xmin, group.ymin, group.xmax, group.ymax];
            }
            this._rowGroupCache.set(file.url, Object.keys(extents).length ? extents : null);
          }
        }
      } catch (error) {
        console.error('[ParquetBboxReader] Failed to read row group bboxes:', error);
        for (const file of uncachedFiles) {
          this._rowGroupCache.set(file.url, null);
        }
      }
    }

    const result = {};
    for (const file of files) {
      const rowGroups = this._rowGroupCache.get(file.url);
      if (rowGroups && Object.keys(rowGroups).length) {
        result[file.id] = rowGroups;
      }
    }

    return Object.keys(result).length ? result : null;
  }

  async _getGeoMetadata(safeUrl, duckdb) {
    const result = await duckdb.conn.query(
      `SELECT value FROM parquet_kv_metadata('${safeUrl}') WHERE key='geo'`
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return this._parseKvBlob(rows[0].value);
  }

  async _getCoveringBboxPaths(safeUrl, duckdb, bboxColumn) {
    const geoMeta = await this._getGeoMetadata(safeUrl, duckdb);

    if (geoMeta) {
      const primaryCol = geoMeta.primary_column || 'geometry';
      const covering = geoMeta.columns?.[primaryCol]?.covering?.bbox;
      if (covering) {
        return {
          xminPath: covering.xmin?.join(', ') || 'bbox, xmin',
          yminPath: covering.ymin?.join(', ') || 'bbox, ymin',
          xmaxPath: covering.xmax?.join(', ') || 'bbox, xmax',
          ymaxPath: covering.ymax?.join(', ') || 'bbox, ymax',
        };
      }
    }

    if (bboxColumn) {
      return {
        xminPath: `${bboxColumn}, xmin`,
        yminPath: `${bboxColumn}, ymin`,
        xmaxPath: `${bboxColumn}, xmax`,
        ymaxPath: `${bboxColumn}, ymax`,
      };
    }

    return null;
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
