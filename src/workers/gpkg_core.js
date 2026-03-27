import { parquetRead, parquetMetadataAsync, parquetSchema } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { buildNormalizer } from '../normalizer.js';

function fileToAsyncBuffer(file) {
  return {
    byteLength: file.size,
    async slice(start, end) {
      const blob = file.slice(start, end);
      return await blob.arrayBuffer();
    },
  };
}

export function createProgressReporter(msgId, postMessage) {
  return (status) => postMessage({ id: msgId, progress: true, status });
}

export function formatGpkgTimestamp(date) {
  return date.toISOString().replace('Z', '').replace(/\.\d{3}$/, '') + 'Z';
}

export function serializeWorkerError(error, stage = null) {
  return {
    message: error?.message || String(error),
    name: error?.name || 'Error',
    stack: error?.stack || '',
    stage,
  };
}

const GP_HEADER = new Uint8Array([0x47, 0x50, 0x00, 0x01, 0xE6, 0x10, 0x00, 0x00]);

const WGS84_WKT = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]';

function esc(s) {
  return s.replace(/'/g, "''");
}

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
  }
  return bytes;
}

const SINGLE_TO_MULTI_WKB = { 1: 4, 2: 5, 3: 6 };

function promoteToMulti(wkb) {
  if (wkb.length < 5) return wkb;
  const littleEndian = wkb[0] === 1;
  const typeVal = littleEndian
    ? wkb[1] | (wkb[2] << 8) | (wkb[3] << 16) | (wkb[4] << 24)
    : (wkb[1] << 24) | (wkb[2] << 16) | (wkb[3] << 8) | wkb[4];
  const baseType = typeVal % 1000;
  const flags = typeVal - baseType;
  const multiType = SINGLE_TO_MULTI_WKB[baseType];
  if (!multiType) return wkb;

  const result = new Uint8Array(9 + wkb.length);
  result[0] = wkb[0];
  const newType = multiType + flags;
  if (littleEndian) {
    result[1] = newType & 0xFF;
    result[2] = (newType >> 8) & 0xFF;
    result[3] = (newType >> 16) & 0xFF;
    result[4] = (newType >> 24) & 0xFF;
    result[5] = 1;
    result[6] = 0;
    result[7] = 0;
    result[8] = 0;
  } else {
    result[1] = (newType >> 24) & 0xFF;
    result[2] = (newType >> 16) & 0xFF;
    result[3] = (newType >> 8) & 0xFF;
    result[4] = newType & 0xFF;
    result[5] = 0;
    result[6] = 0;
    result[7] = 0;
    result[8] = 1;
  }
  result.set(wkb, 9);
  return result;
}

function buildGpkgGeom(wkbHex, needsPromote) {
  if (!wkbHex) return null;
  let wkb = hexToBytes(wkbHex);
  if (needsPromote) wkb = promoteToMulti(wkb);
  const result = new Uint8Array(GP_HEADER.length + wkb.length);
  result.set(GP_HEADER, 0);
  result.set(wkb, GP_HEADER.length);
  return result;
}

const MULTI_MAP = {
  POINT: 'MULTIPOINT',
  LINESTRING: 'MULTILINESTRING',
  POLYGON: 'MULTIPOLYGON',
};

function resolveGeomTypeName(geomTypes) {
  if (geomTypes.size === 1) return [...geomTypes][0];
  const bases = new Set([...geomTypes].map((type) => type.replace('MULTI', '')));
  if (bases.size === 1) {
    const base = [...bases][0];
    return MULTI_MAP[base] || 'MULTI' + base;
  }
  return 'GEOMETRY';
}

function parquetTypeToSqlite(element) {
  const logical = element.logicalType;
  if (logical) {
    if (logical.type === 'STRING' || logical.type === 'UTF8' || logical.type === 'ENUM' || logical.type === 'JSON') return 'TEXT';
    if (logical.type === 'DATE' || logical.type === 'TIME' || logical.type === 'TIMESTAMP') return 'TEXT';
    if (logical.type === 'DECIMAL') return 'REAL';
    if (logical.type === 'INTEGER' || logical.type === 'INT') return 'INTEGER';
  }
  const phys = element.type;
  if (phys === 'INT32' || phys === 'INT64' || phys === 'BOOLEAN') return 'INTEGER';
  if (phys === 'FLOAT' || phys === 'DOUBLE') return 'REAL';
  if (phys === 'BYTE_ARRAY' || phys === 'FIXED_LEN_BYTE_ARRAY') return 'TEXT';
  return 'TEXT';
}

const INTERNAL_COLS = new Set(['geom_wkb', '_geom_type', '_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy', 'bbox']);

export async function writeFromParquet({ parquetFileName, gpkgFileName }, msgId, env) {
  const progress = createProgressReporter(msgId, env.postMessage);

  const root = await env.getRoot();
  const fileHandle = await root.getFileHandle(parquetFileName);
  const file = await fileHandle.getFile();
  const asyncBuffer = fileToAsyncBuffer(file);

  const metadata = await parquetMetadataAsync(asyncBuffer);
  const schema = parquetSchema(metadata);

  const columns = [];
  for (const child of schema.children) {
    if (INTERNAL_COLS.has(child.element.name)) continue;
    const isNested = child.children?.length > 0;
    columns.push({
      name: child.element.name,
      sqliteType: isNested ? 'TEXT' : parquetTypeToSqlite(child.element),
      jsonSerialize: isNested,
      normalize: buildNormalizer(child),
    });
  }

  progress('Scanning metadata...');
  const geomTypes = new Set();
  let xmin = Infinity;
  let ymin = Infinity;
  let xmax = -Infinity;
  let ymax = -Infinity;

  let rowOffset = 0;
  for (const rg of metadata.row_groups) {
    const rgEnd = rowOffset + Number(rg.num_rows);
    let rows;
    await parquetRead({
      file: asyncBuffer,
      compressors,
      columns: ['_geom_type', '_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy'],
      rowStart: rowOffset,
      rowEnd: rgEnd,
      onComplete: (data) => {
        rows = data;
      },
    });
    for (const row of rows) {
      if (row[0]) geomTypes.add(row[0]);
      if (row[1] != null && row[1] < xmin) xmin = row[1];
      if (row[2] != null && row[2] < ymin) ymin = row[2];
      if (row[3] != null && row[3] > xmax) xmax = row[3];
      if (row[4] != null && row[4] > ymax) ymax = row[4];
    }
    rowOffset = rgEnd;
  }

  const bbox = {
    xmin: xmin === Infinity ? 0 : xmin,
    ymin: ymin === Infinity ? 0 : ymin,
    xmax: xmax === -Infinity ? 0 : xmax,
    ymax: ymax === -Infinity ? 0 : ymax,
  };
  const geomTypeName = resolveGeomTypeName(geomTypes);
  const promoteTypes = new Set(
    [...geomTypes].filter((type) => MULTI_MAP[type] && MULTI_MAP[type] === geomTypeName)
  );

  progress('Initializing GeoPackage writer...');
  const dbRuntime = await env.createDatabaseRuntime(gpkgFileName);

  try {
    await dbRuntime.exec('PRAGMA application_id = 0x47503130');
    await dbRuntime.exec('PRAGMA user_version = 10400');
    await dbRuntime.exec('PRAGMA journal_mode = MEMORY');
    await dbRuntime.exec('PRAGMA synchronous = OFF');

    progress('Creating GeoPackage schema...');
    await dbRuntime.exec(`
      CREATE TABLE gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT
      )
    `);

    await dbRuntime.exec(`
      INSERT INTO gpkg_spatial_ref_sys VALUES
        ('Undefined Cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined Cartesian coordinate reference system'),
        ('Undefined Geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'),
        ('WGS 84 geodetic', 4326, 'EPSG', 4326, '${esc(WGS84_WKT)}', 'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid')
    `);

    await dbRuntime.exec(`
      CREATE TABLE gpkg_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        data_type TEXT NOT NULL,
        identifier TEXT UNIQUE,
        description TEXT DEFAULT '',
        last_change TEXT NOT NULL,
        min_x DOUBLE,
        min_y DOUBLE,
        max_x DOUBLE,
        max_y DOUBLE,
        srs_id INTEGER,
        CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      )
    `);

    await dbRuntime.exec(`
      CREATE TABLE gpkg_geometry_columns (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        geometry_type_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL,
        z TINYINT NOT NULL,
        m TINYINT NOT NULL,
        CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
        CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
        CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      )
    `);

    const colDefs = columns.map((column) => `"${column.name}" ${column.sqliteType}`).join(', ');
    await dbRuntime.exec(`
      CREATE TABLE features (
        fid INTEGER PRIMARY KEY AUTOINCREMENT,
        geom BLOB,
        _bbox_minx REAL, _bbox_miny REAL, _bbox_maxx REAL, _bbox_maxy REAL
        ${columns.length > 0 ? ', ' + colDefs : ''}
      )
    `);

    progress('Writing features...');
    const bboxCols = '_bbox_minx, _bbox_miny, _bbox_maxx, _bbox_maxy';
    const placeholders = ['?', '?', '?', '?', '?', ...columns.map(() => '?')].join(', ');
    const attrCols = columns.length > 0 ? ', ' + columns.map((column) => `"${column.name}"`).join(', ') : '';
    const insertSql = `INSERT INTO features (geom, ${bboxCols}${attrCols}) VALUES (${placeholders})`;

    const colIndex = {};
    schema.children.forEach((child, index) => {
      colIndex[child.element.name] = index;
    });
    const iWkb = colIndex.geom_wkb;
    const iGT = colIndex._geom_type;
    const iMinX = colIndex._bbox_minx;
    const iMinY = colIndex._bbox_miny;
    const iMaxX = colIndex._bbox_maxx;
    const iMaxY = colIndex._bbox_maxy;
    const attrIndices = columns.map((column) => colIndex[column.name]);

    let rowCount = 0;
    await dbRuntime.exec('BEGIN TRANSACTION');

    rowOffset = 0;
    for (const rg of metadata.row_groups) {
      const rgEnd = rowOffset + Number(rg.num_rows);
      let rows;
      await parquetRead({
        file: asyncBuffer,
        compressors,
        rowStart: rowOffset,
        rowEnd: rgEnd,
        onComplete: (data) => {
          rows = data;
        },
      });

      const paramSets = [];
      for (const row of rows) {
        const needsPromote = promoteTypes.has(row[iGT]);
        const params = [
          buildGpkgGeom(row[iWkb], needsPromote),
          row[iMinX],
          row[iMinY],
          row[iMaxX],
          row[iMaxY],
        ];
        for (let index = 0; index < attrIndices.length; index++) {
          const value = columns[index].normalize(row[attrIndices[index]]);
          if (columns[index].jsonSerialize && value != null) {
            params.push(JSON.stringify(value));
          } else {
            params.push(value);
          }
        }
        paramSets.push(params);
      }
      await dbRuntime.insertBatch(insertSql, paramSets);
      rowCount += paramSets.length;
      progress(`Writing features... (${rowCount} rows)`);

      rowOffset = rgEnd;
    }

    await dbRuntime.exec('COMMIT');

    progress('Building spatial index...');
    await dbRuntime.exec(`
      CREATE TABLE gpkg_extensions (
        table_name TEXT,
        column_name TEXT,
        extension_name TEXT NOT NULL,
        definition TEXT NOT NULL,
        scope TEXT NOT NULL,
        CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name)
      )
    `);

    await dbRuntime.exec(`
      INSERT INTO gpkg_extensions VALUES (
        'features', 'geom', 'gpkg_rtree_index',
        'http://www.geopackage.org/spec120/#extension_rtree',
        'write-only'
      )
    `);

    await dbRuntime.exec('CREATE VIRTUAL TABLE rtree_features_geom USING rtree(id, minx, maxx, miny, maxy)');
    await dbRuntime.exec('INSERT INTO rtree_features_geom (id, minx, maxx, miny, maxy) SELECT fid, _bbox_minx, _bbox_maxx, _bbox_miny, _bbox_maxy FROM features');

    for (const column of ['_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy']) {
      await dbRuntime.exec(`ALTER TABLE features DROP COLUMN ${column}`);
    }

    progress('Finalizing metadata...');
    const now = formatGpkgTimestamp(env.getTimestamp());

    await dbRuntime.exec(`
      INSERT INTO gpkg_contents VALUES (
        'features', 'features', 'features', '',
        '${now}',
        ${bbox.xmin}, ${bbox.ymin}, ${bbox.xmax}, ${bbox.ymax},
        4326
      )
    `);

    await dbRuntime.exec(`
      INSERT INTO gpkg_geometry_columns VALUES (
        'features', 'geom', '${geomTypeName}', 4326, 0, 0
      )
    `);

    progress(`GeoPackage complete (${rowCount} features)`);
    return { rowCount };
  } finally {
    await dbRuntime.close();
  }
}

export function registerGpkgWorkerMessageHandler(workerScope, { writeFromParquet: handleWriteFromParquet }) {
  if (typeof handleWriteFromParquet !== 'function') {
    throw new Error('registerGpkgWorkerMessageHandler requires a writeFromParquet handler');
  }

  workerScope.onmessage = async (event) => {
    const { id, method, args } = event.data;
    try {
      let result;
      switch (method) {
        case 'writeFromParquet':
          result = await handleWriteFromParquet(args, id);
          break;
        default:
          throw new Error(`Unknown method: ${method}`);
      }
      workerScope.postMessage({ id, result });
    } catch (error) {
      workerScope.postMessage({
        id,
        error: error.message || String(error),
        errorDetails: serializeWorkerError(error),
      });
    }
  };
}
