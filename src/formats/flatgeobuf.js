// FlatGeobuf format handler for partial downloads.
// Pipeline: DuckDB filters + Hilbert-orders remote parquet → intermediate parquet on OPFS →
// hyparquet reads row-group-by-row-group → features stream into a temporary FlatGeobuf →
// packed R-tree levels are built into OPFS scratch files → final indexed FlatGeobuf assembled on OPFS.
//
// FlatGeobuf natively supports mixed geometry types: the header's geometry_type
// is set to Unknown when features have different types, and each feature stores
// its own type. No promotion (e.g. Point → MultiPoint) is needed.

import { FormatHandler } from './base.js';
import { OPFS_PREFIX_FGB_TMP, OPFS_PREFIX_OUTPUT, fileToAsyncBuffer } from '../utils.js';
import { ScopedProgress } from '../scoped_progress.js';
import { parseWkbHex } from '../wkb.js';
import { parquetRead, parquetMetadataAsync, parquetSchema } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { buildColumnLookup, buildRowProperties } from './streaming_helpers.js';
import {
  FGB_DEFAULT_INDEX_NODE_SIZE,
  FGB_NODE_ITEM_BYTE_LEN,
  buildFgbHeader,
  buildPackedRTreeLevels,
  copyBlobToWriter,
  createNodeItem,
  expandNodeItem,
  writeNodeItem,
} from './fgb_packed_rtree.js';

import { buildFeature } from 'flatgeobuf/lib/mjs/generic/feature.js';
import { parseGeometry, parseGC } from 'flatgeobuf/lib/mjs/geojson/geometry.js';
import { magicbytes } from 'flatgeobuf/lib/mjs/constants.js';
import { GeometryType } from 'flatgeobuf/lib/mjs/flat-geobuf/geometry-type.js';
import { ColumnType } from 'flatgeobuf/lib/mjs/flat-geobuf/column-type.js';

/** Map DuckDB ST_GeometryType names → FlatGeobuf GeometryType */
const DUCKDB_TO_FGB_GEOM = {
  'POINT': GeometryType.Point,
  'LINESTRING': GeometryType.LineString,
  'POLYGON': GeometryType.Polygon,
  'MULTIPOINT': GeometryType.MultiPoint,
  'MULTILINESTRING': GeometryType.MultiLineString,
  'MULTIPOLYGON': GeometryType.MultiPolygon,
  'GEOMETRYCOLLECTION': GeometryType.GeometryCollection,
};

const INTERNAL_COLS = new Set(['geom_wkb', '_geom_type']);
const LEAF_BATCH_RECORDS = 1024;

function expandPointNode(nodeItem, point) {
  if (!Array.isArray(point) || point.length < 2) return;
  const [x, y] = point;
  if (!Number.isFinite(x) || !Number.isFinite(y)) return;
  if (x < nodeItem.minX) nodeItem.minX = x;
  if (y < nodeItem.minY) nodeItem.minY = y;
  if (x > nodeItem.maxX) nodeItem.maxX = x;
  if (y > nodeItem.maxY) nodeItem.maxY = y;
}

function expandCoordinatesNode(nodeItem, coordinates) {
  if (!Array.isArray(coordinates)) return;
  if (coordinates.length >= 2 && typeof coordinates[0] === 'number') {
    expandPointNode(nodeItem, coordinates);
    return;
  }
  for (const child of coordinates) {
    expandCoordinatesNode(nodeItem, child);
  }
}

function hasFiniteBounds(nodeItem) {
  return Number.isFinite(nodeItem.minX) &&
    Number.isFinite(nodeItem.minY) &&
    Number.isFinite(nodeItem.maxX) &&
    Number.isFinite(nodeItem.maxY);
}

/**
 * Convert a GeoJSON-style geometry into a FlatGeobuf node item / bbox.
 * Returns null for empty geometries, which cannot be spatially indexed.
 * @param {{ type: string, coordinates?: any, geometries?: any[] }} geom
 * @param {number} offset
 * @returns {{ minX: number, minY: number, maxX: number, maxY: number, offset: number } | null}
 */
function geometryToNodeItem(geom, offset) {
  const nodeItem = createNodeItem(offset);
  if (geom.type === 'GeometryCollection') {
    for (const child of geom.geometries || []) {
      const childNode = geometryToNodeItem(child, 0);
      if (childNode) expandNodeItem(nodeItem, childNode);
    }
  } else {
    expandCoordinatesNode(nodeItem, geom.coordinates);
  }
  return hasFiniteBounds(nodeItem) ? nodeItem : null;
}

/**
 * Resolve a set of DuckDB geometry type names to a single FlatGeobuf GeometryType.
 * Returns the common type if all features share it, otherwise Unknown.
 * @param {Set<string>} duckdbTypes
 * @returns {GeometryType}
 */
export function resolveFgbGeometryType(duckdbTypes) {
  let resolved;
  for (const typeName of duckdbTypes) {
    const mapped = DUCKDB_TO_FGB_GEOM[typeName];
    if (mapped === undefined) return GeometryType.Unknown;
    if (resolved === undefined) resolved = mapped;
    else if (resolved !== mapped) return GeometryType.Unknown;
  }
  return resolved ?? GeometryType.Unknown;
}

/**
 * Map a DuckDB column type string to a FlatGeobuf ColumnType.
 * @param {string} duckdbType
 * @returns {ColumnType}
 */
export function duckdbTypeToColumnType(duckdbType) {
  const typeName = duckdbType.toUpperCase();
  if (typeName === 'BOOLEAN') return ColumnType.Bool;
  if (typeName === 'TINYINT' || typeName === 'SMALLINT') return ColumnType.Short;
  if (typeName === 'UTINYINT' || typeName === 'USMALLINT') return ColumnType.UShort;
  if (typeName === 'INTEGER') return ColumnType.Int;
  if (typeName === 'UINTEGER') return ColumnType.UInt;
  if (typeName === 'BIGINT' || typeName === 'HUGEINT') return ColumnType.Long;
  if (typeName === 'UBIGINT') return ColumnType.ULong;
  if (typeName === 'FLOAT') return ColumnType.Float;
  if (typeName === 'DOUBLE' || typeName.startsWith('DECIMAL')) return ColumnType.Double;
  if (typeName === 'DATE' || typeName.startsWith('TIMESTAMP') || typeName.startsWith('TIME')) return ColumnType.DateTime;
  if (typeName === 'BLOB') return ColumnType.Binary;
  if (typeName.startsWith('STRUCT') || typeName.startsWith('MAP') || typeName.startsWith('LIST')) return ColumnType.Json;
  return ColumnType.String;
}

/**
 * Build a FlatGeobuf ColumnMeta array from DuckDB column descriptors.
 * @param {Array<{name: string, type: string}>} columns
 * @returns {import('flatgeobuf/lib/mjs/column-meta.js').ColumnMeta[]}
 */
export function buildFgbColumns(columns) {
  return columns.map(({ name, type }) => ({
    name,
    type: duckdbTypeToColumnType(type),
    title: null,
    description: null,
    width: -1,
    precision: -1,
    scale: -1,
    nullable: true,
    unique: false,
    primary_key: false,
  }));
}

export class FlatGeobufFormatHandler extends FormatHandler {
  constructor(opts = {}) {
    super(opts);
    this.extension = 'fgb';
    this._fgbFileName = null;
    this._tempCounter = 0;
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 4; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 5; }

  _makeTempName(suffix) {
    this._tempCounter += 1;
    return `${OPFS_PREFIX_FGB_TMP}${this.sessionId}_${Date.now()}_${this._tempCounter}${suffix}`;
  }

  _makeOutputName() {
    this._tempCounter += 1;
    return `${OPFS_PREFIX_OUTPUT}${this.sessionId}_${Date.now()}_${this._tempCounter}.${this.extension}`;
  }

  async _write({ onProgress, onStatus }) {
    const tempNames = new Set();
    let tempParquetPath = null;
    let tempParquetName = null;
    let tempFgbName = null;
    /** @type {{ write: (chunk: Uint8Array) => Promise<void>, close: () => Promise<void> } | null} */
    let tempFgbWriter = null;
    /** @type {{ write: (chunk: Uint8Array) => Promise<void>, close: () => Promise<void> } | null} */
    let leafWriter = null;
    let writeSucceeded = false;

    try {
      const stage1 = new ScopedProgress(onProgress, 0, 45);
      tempParquetPath = await this.createIntermediateParquet({
        prefix: OPFS_PREFIX_FGB_TMP,
        extraColumns: ['ST_GeometryType(geometry) AS _geom_type'],
        hilbertOrder: true,
        onProgress: stage1.callback,
        onStatus,
      });
      tempParquetName = tempParquetPath.replace('opfs://', '');
      tempNames.add(tempParquetName);

      onStatus?.('Reading schema...');
      const columns = await this.describeColumns(tempParquetPath, INTERNAL_COLS);
      const attrColumns = columns.map(column => ({ originalName: column.name }));

      const typesResult = await this.duckdb.conn.query(
        `SELECT DISTINCT _geom_type FROM '${tempParquetPath}' WHERE _geom_type IS NOT NULL`
      );
      const allGeomTypes = new Set();
      for (let i = 0; i < typesResult.numRows; i++) {
        allGeomTypes.add(typesResult.getChildAt(0).get(i));
      }

      await this.releaseDuckdbOpfsFile(tempParquetPath);
      this.throwIfCancelled();

      const tempParquetFile = await this.getOpfsFile(tempParquetPath);
      const asyncBuffer = fileToAsyncBuffer(tempParquetFile);
      const metadata = await parquetMetadataAsync(asyncBuffer);
      const hSchema = parquetSchema(metadata);
      const totalRows = metadata.row_groups.reduce(
        (sum, rowGroup) => sum + Number(rowGroup.num_rows), 0
      );

      const headerMeta = {
        geometryType: resolveFgbGeometryType(allGeomTypes),
        columns: buildFgbColumns(columns),
        envelope: null,
        featuresCount: totalRows,
        indexNodeSize: 0,
        crs: null,
        title: null,
        description: null,
        metadata: null,
      };

      if (totalRows === 0) {
        onStatus?.('Writing FlatGeobuf...');
        const headerBytes = buildFgbHeader(headerMeta, 4326);
        this._fgbFileName = this._makeOutputName();
        const outputHandle = await this.getOpfsHandle(this._fgbFileName, { create: true });
        const outputWriter = await outputHandle.createWritable();
        try {
          await outputWriter.write(magicbytes);
          await outputWriter.write(headerBytes);
        } finally {
          await outputWriter.close();
        }

        await this.removeOpfsFile(tempParquetName);
        writeSucceeded = true;
        onProgress?.(100);
        onStatus?.('FlatGeobuf complete');
        return;
      }

      const tempHeaderBytes = buildFgbHeader(headerMeta, 4326);
      const tempFeatureStartOffset = magicbytes.length + tempHeaderBytes.length;
      tempFgbName = this._makeTempName('.raw.fgb');
      tempNames.add(tempFgbName);
      const leafFileName = this._makeTempName('.idx.l0.bin');
      tempNames.add(leafFileName);

      const tempFgbHandle = await this.getOpfsHandle(tempFgbName, { create: true });
      const leafHandle = await this.getOpfsHandle(leafFileName, { create: true });
      tempFgbWriter = await tempFgbHandle.createWritable();
      leafWriter = await leafHandle.createWritable();

      await tempFgbWriter.write(magicbytes);
      await tempFgbWriter.write(tempHeaderBytes);

      const { colIndex, iWkb, attrIndices, attrNorms } = buildColumnLookup(hSchema, attrColumns);
      const iGeomType = colIndex['_geom_type'];

      onStatus?.('Writing FlatGeobuf features...');
      const stage2 = new ScopedProgress(onProgress, 50, 80);
      const leafBatch = new Uint8Array(LEAF_BATCH_RECORDS * FGB_NODE_ITEM_BYTE_LEN);
      let leafBatchCount = 0;
      let processedRows = 0;
      let rowOffset = 0;
      let featureOffset = 0;
      const extent = createNodeItem(0);

      const flushLeafBatch = async () => {
        if (!leafBatchCount) return;
        await leafWriter.write(leafBatch.subarray(0, leafBatchCount * FGB_NODE_ITEM_BYTE_LEN));
        leafBatchCount = 0;
      };

      try {
        for (const rowGroup of metadata.row_groups) {
          this.throwIfCancelled();

          const rowEnd = rowOffset + Number(rowGroup.num_rows);
          let rows;
          await parquetRead({
            file: asyncBuffer,
            compressors,
            rowStart: rowOffset,
            rowEnd,
            onComplete: (data) => { rows = data; },
          });

          for (const row of rows) {
            const wkbHex = row[iWkb];
            const duckdbType = row[iGeomType];
            if (!wkbHex || !duckdbType) {
              throw new Error('FlatGeobuf export requires non-null geometries in the filtered dataset');
            }

            const geom = parseWkbHex(wkbHex);
            const nodeItem = geometryToNodeItem(geom, featureOffset);
            if (!nodeItem) {
              throw new Error('FlatGeobuf spatial index requires non-empty geometries');
            }

            expandNodeItem(extent, nodeItem);
            writeNodeItem(leafBatch, leafBatchCount * FGB_NODE_ITEM_BYTE_LEN, nodeItem);
            leafBatchCount++;
            if (leafBatchCount === LEAF_BATCH_RECORDS) {
              await flushLeafBatch();
            }

            const parsedGeom = geom.type === 'GeometryCollection'
              ? parseGC(geom)
              : parseGeometry(geom);
            const props = buildRowProperties(row, attrIndices, attrNorms, attrColumns);
            const cleanProps = {};
            for (const key in props) {
              if (props[key] != null) cleanProps[key] = props[key];
            }

            const featureBytes = buildFeature(parsedGeom, cleanProps, headerMeta);
            await tempFgbWriter.write(featureBytes);
            featureOffset += featureBytes.length;
          }

          processedRows += rows.length;
          stage2.report((processedRows / totalRows) * 100);
          onStatus?.(`Writing FlatGeobuf features... (${processedRows}/${totalRows})`);
          rowOffset = rowEnd;
        }

        await flushLeafBatch();
      } finally {
        try {
          await leafWriter.close();
        } finally {
          leafWriter = null;
        }
        try {
          await tempFgbWriter.close();
        } finally {
          tempFgbWriter = null;
        }
      }

      if (!hasFiniteBounds(extent)) {
        throw new Error('FlatGeobuf spatial index requires at least one non-empty geometry');
      }

      await this.removeOpfsFile(tempParquetName);
      tempNames.delete(tempParquetName);
      tempParquetPath = null;
      tempParquetName = null;

      this.throwIfCancelled();
      onStatus?.('Building FlatGeobuf spatial index...');
      const stage3 = new ScopedProgress(onProgress, 80, 90);
      const { levelFiles, treeByteSize } = await buildPackedRTreeLevels({
        numItems: totalRows,
        leafFileName,
        nodeSize: FGB_DEFAULT_INDEX_NODE_SIZE,
        createScratchFile: async (levelIndex) => {
          const name = this._makeTempName(`.idx.l${levelIndex}.bin`);
          tempNames.add(name);
          const handle = await this.getOpfsHandle(name, { create: true });
          return { name, writer: await handle.createWritable() };
        },
        getScratchFile: (name) => this.getOpfsFile(name),
        onProgress: stage3.callback,
      });

      this.throwIfCancelled();

      const finalHeaderBytes = buildFgbHeader({
        ...headerMeta,
        envelope: [extent.minX, extent.minY, extent.maxX, extent.maxY],
        indexNodeSize: FGB_DEFAULT_INDEX_NODE_SIZE,
      }, 4326);

      this._fgbFileName = this._makeOutputName();
      const outputHandle = await this.getOpfsHandle(this._fgbFileName, { create: true });
      const outputWriter = await outputHandle.createWritable();
      const tempFgbFile = await this.getOpfsFile(tempFgbName);
      const featureBytesToCopy = tempFgbFile.size - tempFeatureStartOffset;
      const totalAssemblyBytes = magicbytes.length + finalHeaderBytes.length + treeByteSize + featureBytesToCopy;
      const stage4 = new ScopedProgress(onProgress, 90, 100);
      let assembledBytes = 0;
      const reportAssembly = (byteCount) => {
        assembledBytes += byteCount;
        stage4.report((assembledBytes / totalAssemblyBytes) * 100);
      };

      try {
        await outputWriter.write(magicbytes);
        reportAssembly(magicbytes.length);

        await outputWriter.write(finalHeaderBytes);
        reportAssembly(finalHeaderBytes.length);

        for (let i = levelFiles.length - 1; i >= 0; i--) {
          const levelFile = await this.getOpfsFile(levelFiles[i]);
          await copyBlobToWriter(levelFile, outputWriter, reportAssembly);
        }

        await copyBlobToWriter(tempFgbFile.slice(tempFeatureStartOffset), outputWriter, reportAssembly);
      } finally {
        await outputWriter.close();
      }

      for (const name of tempNames) {
        await this.removeOpfsFile(name);
      }
      tempNames.clear();

      writeSucceeded = true;
      onProgress?.(100);
      onStatus?.('FlatGeobuf complete');
    } finally {
      if (leafWriter) {
        try { await leafWriter.close(); } catch { /* already closed */ }
      }
      if (tempFgbWriter) {
        try { await tempFgbWriter.close(); } catch { /* already closed */ }
      }

      if (!writeSucceeded) {
        for (const name of tempNames) {
          await this.removeOpfsFile(name);
        }
        if (tempParquetName) {
          await this.removeOpfsFile(tempParquetName);
        }
        if (this._fgbFileName) {
          await this.removeOpfsFile(this._fgbFileName);
          this._fgbFileName = null;
        }
      }
    }
  }

  async getDownloadMap(baseName) {
    const file = await this.getOpfsFile(this._fgbFileName);
    return [{ downloadName: `${baseName}.${this.extension}`, blobParts: [file] }];
  }
}
