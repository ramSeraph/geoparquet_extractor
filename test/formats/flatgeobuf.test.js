import { describe, it, expect } from 'vitest';
import * as flatbuffers from 'flatbuffers';
import {
  resolveFgbGeometryType,
  duckdbTypeToColumnType,
  buildFgbColumns,
} from '../../src/formats/flatgeobuf.js';
import {
  buildFgbHeader,
  writeNodeItem,
} from '../../src/formats/fgb_packed_rtree.js';
import { geojson as fgbGeojson } from 'flatgeobuf';
import { buildHeader } from 'flatgeobuf/lib/mjs/generic/featurecollection.js';
import { buildFeature } from 'flatgeobuf/lib/mjs/generic/feature.js';
import { parseGeometry, parseGC } from 'flatgeobuf/lib/mjs/geojson/geometry.js';
import { magicbytes } from 'flatgeobuf/lib/mjs/constants.js';
import { Header as FgbHeader } from 'flatgeobuf/lib/mjs/flat-geobuf/header.js';
import { GeometryType } from 'flatgeobuf/lib/mjs/flat-geobuf/geometry-type.js';
import { ColumnType } from 'flatgeobuf/lib/mjs/flat-geobuf/column-type.js';

// --- Helper: simulate the streaming write (magic + header + per-feature) ---

function buildFgbBytes(headerMeta, features) {
  const headerBytes = buildHeader(headerMeta, 4326);
  const featureBuffers = features.map(({ geom, props }) => {
    const parsed = geom.type === 'GeometryCollection' ? parseGC(geom) : parseGeometry(geom);
    return buildFeature(parsed, props, headerMeta);
  });
  const totalLen = magicbytes.length + headerBytes.length
    + featureBuffers.reduce((s, f) => s + f.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  result.set(magicbytes, offset); offset += magicbytes.length;
  result.set(headerBytes, offset); offset += headerBytes.length;
  for (const fb of featureBuffers) { result.set(fb, offset); offset += fb.length; }
  return result;
}

async function readFgbFeatures(bytes) {
  const features = [];
  for await (const f of fgbGeojson.deserialize(bytes)) features.push(f);
  return features;
}

function readHeader(bytes, { withMagic = false } = {}) {
  const bb = new flatbuffers.ByteBuffer(bytes);
  if (withMagic) bb.setPosition(magicbytes.length);
  return FgbHeader.getSizePrefixedRootAsHeader(bb);
}

function col(name, type) {
  return { name, type, title: null, description: null, width: -1, precision: -1, scale: -1, nullable: true, unique: false, primary_key: false };
}

// --- Unit tests for helpers ---

describe('resolveFgbGeometryType', () => {
  it('returns the type when all features share a single geometry type', () => {
    expect(resolveFgbGeometryType(new Set(['POINT']))).toBe(GeometryType.Point);
    expect(resolveFgbGeometryType(new Set(['MULTIPOLYGON']))).toBe(GeometryType.MultiPolygon);
  });

  it('returns Unknown for mixed types (no promotion)', () => {
    expect(resolveFgbGeometryType(new Set(['POINT', 'MULTIPOINT']))).toBe(GeometryType.Unknown);
  });

  it('returns Unknown for completely different types', () => {
    expect(resolveFgbGeometryType(new Set(['POINT', 'POLYGON']))).toBe(GeometryType.Unknown);
  });

  it('returns Unknown for empty set', () => {
    expect(resolveFgbGeometryType(new Set())).toBe(GeometryType.Unknown);
  });

  it('returns Unknown for unrecognized DuckDB type names', () => {
    expect(resolveFgbGeometryType(new Set(['CURVE']))).toBe(GeometryType.Unknown);
  });

  it('maps all standard DuckDB geometry types correctly', () => {
    expect(resolveFgbGeometryType(new Set(['LINESTRING']))).toBe(GeometryType.LineString);
    expect(resolveFgbGeometryType(new Set(['POLYGON']))).toBe(GeometryType.Polygon);
    expect(resolveFgbGeometryType(new Set(['MULTIPOINT']))).toBe(GeometryType.MultiPoint);
    expect(resolveFgbGeometryType(new Set(['MULTILINESTRING']))).toBe(GeometryType.MultiLineString);
    expect(resolveFgbGeometryType(new Set(['GEOMETRYCOLLECTION']))).toBe(GeometryType.GeometryCollection);
  });
});

describe('duckdbTypeToColumnType', () => {
  it('maps BOOLEAN to Bool', () => {
    expect(duckdbTypeToColumnType('BOOLEAN')).toBe(ColumnType.Bool);
  });

  it('maps integer types', () => {
    expect(duckdbTypeToColumnType('TINYINT')).toBe(ColumnType.Short);
    expect(duckdbTypeToColumnType('SMALLINT')).toBe(ColumnType.Short);
    expect(duckdbTypeToColumnType('INTEGER')).toBe(ColumnType.Int);
    expect(duckdbTypeToColumnType('BIGINT')).toBe(ColumnType.Long);
    expect(duckdbTypeToColumnType('HUGEINT')).toBe(ColumnType.Long);
  });

  it('maps unsigned integer types', () => {
    expect(duckdbTypeToColumnType('UTINYINT')).toBe(ColumnType.UShort);
    expect(duckdbTypeToColumnType('USMALLINT')).toBe(ColumnType.UShort);
    expect(duckdbTypeToColumnType('UINTEGER')).toBe(ColumnType.UInt);
    expect(duckdbTypeToColumnType('UBIGINT')).toBe(ColumnType.ULong);
  });

  it('maps floating point types', () => {
    expect(duckdbTypeToColumnType('FLOAT')).toBe(ColumnType.Float);
    expect(duckdbTypeToColumnType('DOUBLE')).toBe(ColumnType.Double);
    expect(duckdbTypeToColumnType('DECIMAL(10,2)')).toBe(ColumnType.Double);
  });

  it('maps temporal types to DateTime', () => {
    expect(duckdbTypeToColumnType('DATE')).toBe(ColumnType.DateTime);
    expect(duckdbTypeToColumnType('TIMESTAMP')).toBe(ColumnType.DateTime);
    expect(duckdbTypeToColumnType('TIMESTAMP WITH TIME ZONE')).toBe(ColumnType.DateTime);
    expect(duckdbTypeToColumnType('TIME')).toBe(ColumnType.DateTime);
  });

  it('maps BLOB to Binary', () => {
    expect(duckdbTypeToColumnType('BLOB')).toBe(ColumnType.Binary);
  });

  it('maps complex types to Json', () => {
    expect(duckdbTypeToColumnType('STRUCT(a INT, b TEXT)')).toBe(ColumnType.Json);
    expect(duckdbTypeToColumnType('MAP(VARCHAR, INT)')).toBe(ColumnType.Json);
    expect(duckdbTypeToColumnType('LIST(INT)')).toBe(ColumnType.Json);
  });

  it('maps VARCHAR and unknown types to String', () => {
    expect(duckdbTypeToColumnType('VARCHAR')).toBe(ColumnType.String);
    expect(duckdbTypeToColumnType('TEXT')).toBe(ColumnType.String);
    expect(duckdbTypeToColumnType('ENUM')).toBe(ColumnType.String);
  });

  it('is case-insensitive', () => {
    expect(duckdbTypeToColumnType('boolean')).toBe(ColumnType.Bool);
    expect(duckdbTypeToColumnType('Integer')).toBe(ColumnType.Int);
  });
});

describe('buildFgbColumns', () => {
  it('builds ColumnMeta array from DuckDB descriptors', () => {
    const result = buildFgbColumns([
      { name: 'name', type: 'VARCHAR' },
      { name: 'count', type: 'INTEGER' },
      { name: 'active', type: 'BOOLEAN' },
    ]);

    expect(result).toHaveLength(3);
    expect(result[0]).toEqual(col('name', ColumnType.String));
    expect(result[1].name).toBe('count');
    expect(result[1].type).toBe(ColumnType.Int);
    expect(result[2].name).toBe('active');
    expect(result[2].type).toBe(ColumnType.Bool);
  });

  it('returns empty array for no columns', () => {
    expect(buildFgbColumns([])).toEqual([]);
  });
});

// --- Round-trip tests: streaming write → flatgeobuf library read ---

describe('FlatGeobuf streaming round-trip', () => {
  it('writes and reads back Point features with attributes', async () => {
    const headerMeta = {
      geometryType: GeometryType.Point,
      columns: [col('name', ColumnType.String), col('value', ColumnType.Double)],
      envelope: null, featuresCount: 2, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    const bytes = buildFgbBytes(headerMeta, [
      { geom: { type: 'Point', coordinates: [1.5, 2.5] }, props: { name: 'hello', value: 42 } },
      { geom: { type: 'Point', coordinates: [3.0, 4.0] }, props: { name: 'world', value: 99.5 } },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(2);

    expect(features[0].geometry).toEqual({ type: 'Point', coordinates: [1.5, 2.5] });
    expect(features[0].properties.name).toBe('hello');
    expect(features[0].properties.value).toBe(42);

    expect(features[1].geometry).toEqual({ type: 'Point', coordinates: [3, 4] });
    expect(features[1].properties.name).toBe('world');
    expect(features[1].properties.value).toBe(99.5);
  });

  it('writes and reads back Polygon features', async () => {
    const ring = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]];
    const headerMeta = {
      geometryType: GeometryType.Polygon,
      columns: [col('id', ColumnType.Int)],
      envelope: null, featuresCount: 1, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    const bytes = buildFgbBytes(headerMeta, [
      { geom: { type: 'Polygon', coordinates: [ring] }, props: { id: 1 } },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(1);
    expect(features[0].geometry.type).toBe('Polygon');
    expect(features[0].geometry.coordinates[0]).toEqual(ring);
    expect(features[0].properties.id).toBe(1);
  });

  it('writes and reads back MultiPolygon features', async () => {
    const poly1 = [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]];
    const poly2 = [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]];
    const headerMeta = {
      geometryType: GeometryType.MultiPolygon,
      columns: [],
      envelope: null, featuresCount: 1, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    const bytes = buildFgbBytes(headerMeta, [
      { geom: { type: 'MultiPolygon', coordinates: [poly1, poly2] }, props: {} },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(1);
    expect(features[0].geometry.type).toBe('MultiPolygon');
    expect(features[0].geometry.coordinates).toHaveLength(2);
  });

  it('handles mixed geometry types with Unknown header', async () => {
    const headerMeta = {
      geometryType: GeometryType.Unknown,
      columns: [col('label', ColumnType.String)],
      envelope: null, featuresCount: 3, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    const bytes = buildFgbBytes(headerMeta, [
      { geom: { type: 'Point', coordinates: [1, 2] }, props: { label: 'pt' } },
      { geom: { type: 'LineString', coordinates: [[0, 0], [1, 1]] }, props: { label: 'ls' } },
      { geom: { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] }, props: { label: 'pg' } },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(3);
    expect(features[0].geometry.type).toBe('Point');
    expect(features[1].geometry.type).toBe('LineString');
    expect(features[2].geometry.type).toBe('Polygon');
    expect(features.map(f => f.properties.label)).toEqual(['pt', 'ls', 'pg']);
  });

  it('handles GeometryCollection via parseGC', async () => {
    const headerMeta = {
      geometryType: GeometryType.GeometryCollection,
      columns: [],
      envelope: null, featuresCount: 1, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    const bytes = buildFgbBytes(headerMeta, [
      {
        geom: {
          type: 'GeometryCollection',
          geometries: [
            { type: 'Point', coordinates: [1, 2] },
            { type: 'LineString', coordinates: [[3, 4], [5, 6]] },
          ],
        },
        props: {},
      },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(1);
    expect(features[0].geometry.type).toBe('GeometryCollection');
    expect(features[0].geometry.geometries).toHaveLength(2);
    expect(features[0].geometry.geometries[0].type).toBe('Point');
    expect(features[0].geometry.geometries[1].type).toBe('LineString');
  });

  it('handles features with null/undefined properties (filtered out)', async () => {
    const headerMeta = {
      geometryType: GeometryType.Point,
      columns: [col('a', ColumnType.String), col('b', ColumnType.Double)],
      envelope: null, featuresCount: 1, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };

    // Only pass 'a', omit 'b' — simulates the null-filtering in the handler
    const bytes = buildFgbBytes(headerMeta, [
      { geom: { type: 'Point', coordinates: [0, 0] }, props: { a: 'yes' } },
    ]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(1);
    expect(features[0].properties.a).toBe('yes');
    // FlatGeobuf schema always includes declared columns; omitted Double reads back as NaN
    expect(features[0].properties.b).toBeNaN();
  });

  it('produces valid magic bytes at file start', () => {
    const headerMeta = {
      geometryType: GeometryType.Point, columns: [],
      envelope: null, featuresCount: 0, indexNodeSize: 0,
      crs: null, title: null, description: null, metadata: null,
    };
    const bytes = buildFgbBytes(headerMeta, []);
    // Magic bytes: "fgb" version 3, "fgb" 0
    expect(bytes[0]).toBe(0x66); // 'f'
    expect(bytes[1]).toBe(0x67); // 'g'
    expect(bytes[2]).toBe(0x62); // 'b'
    expect(bytes[3]).toBe(0x03); // version
    expect(bytes[4]).toBe(0x66); // 'f'
    expect(bytes[5]).toBe(0x67); // 'g'
    expect(bytes[6]).toBe(0x62); // 'b'
    expect(bytes[7]).toBe(0x00);
  });
});

describe('FlatGeobuf indexed layout', () => {
  it('builds indexed headers with envelope and node size', () => {
    const headerBytes = buildFgbHeader({
      geometryType: GeometryType.Point,
      columns: [col('name', ColumnType.String)],
      envelope: [1, 2, 3, 4],
      featuresCount: 2,
      indexNodeSize: 16,
      crs: null,
      title: null,
      description: null,
      metadata: null,
    }, 4326);

    const header = readHeader(headerBytes);
    expect(header.indexNodeSize()).toBe(16);
    expect(Array.from(header.envelopeArray())).toEqual([1, 2, 3, 4]);
    expect(Number(header.featuresCount())).toBe(2);
    expect(header.crs().code()).toBe(4326);
  });

  it('writes and reads back indexed FlatGeobuf files', async () => {
    const headerMeta = {
      geometryType: GeometryType.Point,
      columns: [col('name', ColumnType.String)],
      envelope: [1, 2, 3, 4],
      featuresCount: 2,
      indexNodeSize: 2,
      crs: null,
      title: null,
      description: null,
      metadata: null,
    };

    const feature1 = buildFeature(
      parseGeometry({ type: 'Point', coordinates: [1, 2] }),
      { name: 'alpha' },
      headerMeta
    );
    const feature2 = buildFeature(
      parseGeometry({ type: 'Point', coordinates: [3, 4] }),
      { name: 'beta' },
      headerMeta
    );

    const indexBytes = new Uint8Array(3 * 40);
    writeNodeItem(indexBytes, 0, { minX: 1, minY: 2, maxX: 3, maxY: 4, offset: 1 });
    writeNodeItem(indexBytes, 40, { minX: 1, minY: 2, maxX: 1, maxY: 2, offset: 0 });
    writeNodeItem(indexBytes, 80, { minX: 3, minY: 4, maxX: 3, maxY: 4, offset: feature1.length });

    const headerBytes = buildFgbHeader(headerMeta, 4326);
    const totalLen = magicbytes.length + headerBytes.length + indexBytes.length + feature1.length + feature2.length;
    const bytes = new Uint8Array(totalLen);
    let offset = 0;
    bytes.set(magicbytes, offset); offset += magicbytes.length;
    bytes.set(headerBytes, offset); offset += headerBytes.length;
    bytes.set(indexBytes, offset); offset += indexBytes.length;
    bytes.set(feature1, offset); offset += feature1.length;
    bytes.set(feature2, offset);

    const header = readHeader(bytes, { withMagic: true });
    expect(header.indexNodeSize()).toBe(2);
    expect(Array.from(header.envelopeArray())).toEqual([1, 2, 3, 4]);

    const features = await readFgbFeatures(bytes);
    expect(features).toHaveLength(2);
    expect(features[0].properties.name).toBe('alpha');
    expect(features[1].properties.name).toBe('beta');
    expect(features[0].geometry.coordinates).toEqual([1, 2]);
    expect(features[1].geometry.coordinates).toEqual([3, 4]);
  });
});
