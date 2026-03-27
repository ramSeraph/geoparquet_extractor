import { beforeAll, describe, expect, it, vi } from 'vitest';

const {
  parquetReadMock,
  parquetMetadataAsyncMock,
  parquetSchemaMock,
  buildNormalizerMock,
} = vi.hoisted(() => ({
  parquetReadMock: vi.fn(),
  parquetMetadataAsyncMock: vi.fn(),
  parquetSchemaMock: vi.fn(),
  buildNormalizerMock: vi.fn(),
}));

vi.mock('hyparquet', () => ({
  parquetRead: parquetReadMock,
  parquetMetadataAsync: parquetMetadataAsyncMock,
  parquetSchema: parquetSchemaMock,
}));

vi.mock('hyparquet-compressors', () => ({
  compressors: {},
}));

vi.mock('../src/normalizer.js', () => ({
  buildNormalizer: buildNormalizerMock,
}));

let gpkgWorker;

beforeAll(async () => {
  gpkgWorker = await import('./helpers/gpkg_test_worker.js');
});

describe('gpkg_worker test seams', () => {
  it('createSqliteRuntime wires injected sqlite and VFS dependencies', async () => {
    const calls = [];
    const fakeStmt = {};
    const createSqliteModule = vi.fn(async (opts) => ({ opts, module: true }));
    const fakeVfs = { close: vi.fn(() => calls.push(['vfs-close'])) };
    const createVfs = vi.fn(async () => fakeVfs);
    const fakeSqlite3 = {
      vfs_register: vi.fn((vfs, makeDefault) => calls.push(['vfs_register', vfs, makeDefault])),
      open_v2: vi.fn(async (path) => {
        calls.push(['open_v2', path]);
        return 'db-handle';
      }),
      exec: vi.fn(async (db, sql) => calls.push(['exec', db, sql])),
      statements: vi.fn(async function* (_db, sql) {
        calls.push(['statements', sql]);
        yield fakeStmt;
      }),
      reset: vi.fn(async (stmt) => calls.push(['reset', stmt])),
      bind_collection: vi.fn((stmt, params) => calls.push(['bind', stmt, params])),
      step: vi.fn(async (stmt) => calls.push(['step', stmt])),
      close: vi.fn(async (db) => calls.push(['close', db])),
    };

    const runtime = await gpkgWorker.createSqliteRuntime({
      dbPath: 'out.gpkg',
      getWasmUrl: () => 'https://example.test/wa-sqlite-async.wasm',
      createSqliteModule,
      sqliteApi: { Factory: vi.fn(() => fakeSqlite3) },
      createVfs,
    });
    await runtime.exec('SELECT 1');
    await runtime.insertBatch('INSERT INTO t VALUES (?)', [[1], [2]]);
    await runtime.close();

    expect(createSqliteModule).toHaveBeenCalledOnce();
    expect(createSqliteModule.mock.calls[0][0].locateFile('wa-sqlite-async.wasm'))
      .toBe('https://example.test/wa-sqlite-async.wasm');
    expect(createVfs).toHaveBeenCalledOnce();
    expect(calls).toEqual(expect.arrayContaining([
      ['vfs_register', fakeVfs, true],
      ['open_v2', 'out.gpkg'],
      ['exec', 'db-handle', 'SELECT 1'],
      ['statements', 'INSERT INTO t VALUES (?)'],
      ['bind', fakeStmt, [1]],
      ['bind', fakeStmt, [2]],
      ['close', 'db-handle'],
      ['vfs-close'],
    ]));
  });

  it('writeFromParquet supports injected parquet/root/db dependencies', async () => {
    const posted = [];
    const executedSql = [];
    const inserted = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = {
      row_groups: [{ num_rows: 2 }],
    };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
        { element: { name: 'name', type: 'BYTE_ARRAY', logicalType: { type: 'STRING' } } },
        {
          element: { name: 'attrs', type: 'BYTE_ARRAY' },
          children: [{ element: { name: 'code' } }],
        },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['POINT', 77.1, 12.1, 77.1, 12.1],
          ['POINT', 77.2, 12.2, 77.2, 12.2],
        ]);
        return;
      }
      onComplete([
        ['010100000033333333334353409a99999999992840', 'POINT', 77.1, 12.1, 77.1, 12.1, 'alpha', { code: 'A1' }],
        ['0101000000cdcccccccc4c53406666666666662840', 'POINT', 77.2, 12.2, 77.2, 12.2, 'bravo', { code: 'B2' }],
      ]);
    });

    const result = await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      7,
      {
        postMessage: (payload) => posted.push(payload),
        getRoot: async () => ({
          getFileHandle: async (name) => {
            expect(name).toBe('input.parquet');
            return { getFile: async () => fakeFile };
          },
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async (sql, paramSets) => { inserted.push({ sql, paramSets }); },
          close: async () => { executedSql.push('CLOSE'); },
        }),
        getTimestamp: () => new Date('2026-03-27T00:00:00.000Z'),
      }
    );

    expect(result).toEqual({ rowCount: 2 });
    expect(posted.map(msg => msg.status)).toEqual(expect.arrayContaining([
      'Scanning metadata...',
      'Initializing GeoPackage writer...',
      'Creating GeoPackage schema...',
      'Writing features...',
      'GeoPackage complete (2 features)',
    ]));
    expect(inserted).toHaveLength(1);
    expect(inserted[0].paramSets).toHaveLength(2);
    expect(inserted[0].paramSets[0][0]).toBeInstanceOf(Uint8Array);
    expect(inserted[0].paramSets[0][5]).toBe('alpha');
    expect(inserted[0].paramSets[0][6]).toBe(JSON.stringify({ code: 'A1' }));
    expect(executedSql.some(sql => sql.includes('CREATE TABLE features'))).toBe(true);
    expect(executedSql.some(sql => sql.includes("'2026-03-27T00:00:00Z'"))).toBe(true);
    expect(executedSql.at(-1)).toBe('CLOSE');
  });

  it('writeFromParquet promotes mixed Point and MultiPoint metadata to MULTIPOINT', async () => {
    const executedSql = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = { row_groups: [{ num_rows: 2 }] };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
        { element: { name: 'name', type: 'BYTE_ARRAY', logicalType: { type: 'STRING' } } },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['POINT', 77.1, 12.1, 77.1, 12.1],
          ['MULTIPOINT', 77.2, 12.2, 77.2, 12.2],
        ]);
        return;
      }
      onComplete([
        ['010100000033333333334353409a99999999992840', 'POINT', 77.1, 12.1, 77.1, 12.1, 'alpha'],
        ['0104000000010000000101000000cdcccccccc4c53406666666666662840', 'MULTIPOINT', 77.2, 12.2, 77.2, 12.2, 'bravo'],
      ]);
    });

    await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      8,
      {
        postMessage: () => {},
        getRoot: async () => ({
          getFileHandle: async () => ({ getFile: async () => fakeFile }),
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async () => {},
          close: async () => {},
        }),
      }
    );

    expect(executedSql.some(sql => sql.includes("'features', 'geom', 'MULTIPOINT', 4326, 0, 0"))).toBe(true);
  });

  it('writeFromParquet promotes multipolygon metadata to MULTIPOLYGON', async () => {
    const executedSql = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = { row_groups: [{ num_rows: 2 }] };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['MULTIPOLYGON', 77.0, 12.0, 77.5, 12.5],
          ['MULTIPOLYGON', 77.5, 12.5, 78.0, 13.0],
        ]);
        return;
      }
      onComplete([
        ['010600000000000000', 'MULTIPOLYGON', 77.0, 12.0, 77.5, 12.5],
        ['010600000000000000', 'MULTIPOLYGON', 77.5, 12.5, 78.0, 13.0],
      ]);
    });

    await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      9,
      {
        postMessage: () => {},
        getRoot: async () => ({
          getFileHandle: async () => ({ getFile: async () => fakeFile }),
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async () => {},
          close: async () => {},
        }),
      }
    );

    expect(executedSql.some(sql => sql.includes("'features', 'geom', 'MULTIPOLYGON', 4326, 0, 0"))).toBe(true);
  });

  it('writeFromParquet promotes mixed LineString and MultiLineString metadata to MULTILINESTRING', async () => {
    const executedSql = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = { row_groups: [{ num_rows: 2 }] };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['LINESTRING', 77.0, 12.0, 77.5, 12.5],
          ['MULTILINESTRING', 77.5, 12.5, 78.0, 13.0],
        ]);
        return;
      }
      onComplete([
        ['010200000000000000', 'LINESTRING', 77.0, 12.0, 77.5, 12.5],
        ['010500000000000000', 'MULTILINESTRING', 77.5, 12.5, 78.0, 13.0],
      ]);
    });

    await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      10,
      {
        postMessage: () => {},
        getRoot: async () => ({
          getFileHandle: async () => ({ getFile: async () => fakeFile }),
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async () => {},
          close: async () => {},
        }),
      }
    );

    expect(executedSql.some(sql => sql.includes("'features', 'geom', 'MULTILINESTRING', 4326, 0, 0"))).toBe(true);
  });

  it('writeFromParquet promotes mixed Polygon and MultiPolygon metadata to MULTIPOLYGON', async () => {
    const executedSql = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = { row_groups: [{ num_rows: 2 }] };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['POLYGON', 77.0, 12.0, 77.5, 12.5],
          ['MULTIPOLYGON', 77.5, 12.5, 78.0, 13.0],
        ]);
        return;
      }
      onComplete([
        ['010300000000000000', 'POLYGON', 77.0, 12.0, 77.5, 12.5],
        ['010600000000000000', 'MULTIPOLYGON', 77.5, 12.5, 78.0, 13.0],
      ]);
    });

    await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      11,
      {
        postMessage: () => {},
        getRoot: async () => ({
          getFileHandle: async () => ({ getFile: async () => fakeFile }),
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async () => {},
          close: async () => {},
        }),
      }
    );

    expect(executedSql.some(sql => sql.includes("'features', 'geom', 'MULTIPOLYGON', 4326, 0, 0"))).toBe(true);
  });

  it('writeFromParquet falls back to GEOMETRY for mixed Point and Polygon metadata', async () => {
    const executedSql = [];
    const fakeFile = {
      size: 16,
      slice: () => ({ arrayBuffer: async () => new ArrayBuffer(0) }),
    };
    const metadata = { row_groups: [{ num_rows: 2 }] };
    const schema = {
      children: [
        { element: { name: 'geom_wkb', type: 'BYTE_ARRAY' } },
        { element: { name: '_geom_type', type: 'BYTE_ARRAY' } },
        { element: { name: '_bbox_minx', type: 'DOUBLE' } },
        { element: { name: '_bbox_miny', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxx', type: 'DOUBLE' } },
        { element: { name: '_bbox_maxy', type: 'DOUBLE' } },
      ],
    };
    parquetMetadataAsyncMock.mockResolvedValue(metadata);
    parquetSchemaMock.mockReturnValue(schema);
    buildNormalizerMock.mockReturnValue((val) => val);
    parquetReadMock.mockImplementation(async ({ columns, onComplete }) => {
      if (columns) {
        onComplete([
          ['POINT', 77.1, 12.1, 77.1, 12.1],
          ['POLYGON', 77.0, 12.0, 78.0, 13.0],
        ]);
        return;
      }
      onComplete([
        ['010100000033333333334353409a99999999992840', 'POINT', 77.1, 12.1, 77.1, 12.1],
        ['010300000000000000', 'POLYGON', 77.0, 12.0, 78.0, 13.0],
      ]);
    });

    await gpkgWorker.writeFromParquet(
      { parquetFileName: 'input.parquet', gpkgFileName: 'out.gpkg' },
      12,
      {
        postMessage: () => {},
        getRoot: async () => ({
          getFileHandle: async () => ({ getFile: async () => fakeFile }),
        }),
        createDatabaseRuntime: async () => ({
          exec: async (sql) => { executedSql.push(sql); },
          insertBatch: async () => {},
          close: async () => {},
        }),
      }
    );

    expect(executedSql.some(sql => sql.includes("'features', 'geom', 'GEOMETRY', 4326, 0, 0"))).toBe(true);
  });

  it('registerGpkgWorkerMessageHandler returns structured error details', async () => {
    const messages = [];
    const workerScope = {
      postMessage: (payload) => messages.push(payload),
      onmessage: null,
    };

    gpkgWorker.registerGpkgWorkerMessageHandler(workerScope);
    await workerScope.onmessage({ data: { id: 9, method: 'nope', args: {} } });

    expect(messages).toHaveLength(1);
    expect(messages[0].error).toContain('Unknown method');
    expect(messages[0].errorDetails).toMatchObject({
      name: 'Error',
      message: 'Unknown method: nope',
    });
  });

  it('exposes small helper utilities for tests', () => {
    const payloads = [];
    const progress = gpkgWorker.createProgressReporter(3, (payload) => payloads.push(payload));
    progress('hello');

    expect(payloads).toEqual([{ id: 3, progress: true, status: 'hello' }]);
    expect(gpkgWorker.formatGpkgTimestamp(new Date('2026-03-27T01:02:03.456Z'))).toBe('2026-03-27T01:02:03Z');
    expect(gpkgWorker.serializeWorkerError(new TypeError('boom'), 'writing-features')).toMatchObject({
      name: 'TypeError',
      message: 'boom',
      stage: 'writing-features',
    });
  });
});
