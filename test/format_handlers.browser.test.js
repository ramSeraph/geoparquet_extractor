import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import * as duckdb from 'duckdb-wasm-opfs-tempdir';
import { parquetMetadataAsync, parquetSchema } from 'hyparquet';
import {
  GeoParquetExtractor,
  createDuckDBClient,
} from '../src/index.js';
import { FormatHandler } from '../src/formats/base.js';
import { createMockGpkgWorker } from './helpers/gpkg_test_worker.js';
import mvpWasmUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-mvp.wasm?url';
import mvpWorkerUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-browser-mvp.worker.js?url';
import ehWasmUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-eh.wasm?url';
import ehWorkerUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-browser-eh.worker.js?url';
import coiWasmUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-coi.wasm?url';
import coiWorkerUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-browser-coi.worker.js?url';
import coiPthreadWorkerUrl from 'duckdb-wasm-opfs-tempdir/dist/duckdb-browser-coi.pthread.worker.js?url';
import waSqliteWasmUrl from 'wa-sqlite-rtree/dist/wa-sqlite-async.wasm?url';
import sampleParquetUrl from './fixtures/sample-gp11.parquet?url';
import pointParquetUrl from './fixtures/sample-point.parquet?url';
import multipointParquetUrl from './fixtures/sample-multipoint.parquet?url';
import linestringParquetUrl from './fixtures/sample-linestring.parquet?url';
import multilinestringParquetUrl from './fixtures/sample-multilinestring.parquet?url';
import polygonParquetUrl from './fixtures/sample-polygon.parquet?url';
import multipolygonParquetUrl from './fixtures/sample-multipolygon.parquet?url';
import mixedPointMultipointParquetUrl from './fixtures/sample-mixed-point-multipoint.parquet?url';
import mixedLineMultilineParquetUrl from './fixtures/sample-mixed-line-multiline.parquet?url';
import mixedPolygonMultipolygonParquetUrl from './fixtures/sample-mixed-polygon-multipolygon.parquet?url';
import mixedPointPolygonParquetUrl from './fixtures/sample-mixed-point-polygon.parquet?url';
import nestedAttrsParquetUrl from './fixtures/sample-nested-attrs.parquet?url';
import nestedListStructParquetUrl from './fixtures/sample-nested-list-struct.parquet?url';
import longFieldNamesParquetUrl from './fixtures/sample-long-fieldnames.parquet?url';

const TEST_BBOX = [77.0, 12.0, 78.0, 13.0];
const DXF_TEST_BBOX = [77.0, 12.0, 77.9, 13.0];
const TEST_TRACE_LIMIT = 200;
const toAbsFixtureUrl = (fixtureUrl) => new URL(fixtureUrl, window.location.href).href;
const WA_SQLITE_WASM_URL = toAbsFixtureUrl(waSqliteWasmUrl);
const FIXTURE_URLS = {
  sample: toAbsFixtureUrl(sampleParquetUrl),
  point: toAbsFixtureUrl(pointParquetUrl),
  multipoint: toAbsFixtureUrl(multipointParquetUrl),
  linestring: toAbsFixtureUrl(linestringParquetUrl),
  multilinestring: toAbsFixtureUrl(multilinestringParquetUrl),
  polygon: toAbsFixtureUrl(polygonParquetUrl),
  multipolygon: toAbsFixtureUrl(multipolygonParquetUrl),
  mixedPointMultipoint: toAbsFixtureUrl(mixedPointMultipointParquetUrl),
  mixedLineMultiline: toAbsFixtureUrl(mixedLineMultilineParquetUrl),
  mixedPolygonMultipolygon: toAbsFixtureUrl(mixedPolygonMultipolygonParquetUrl),
  mixedPointPolygon: toAbsFixtureUrl(mixedPointPolygonParquetUrl),
  nestedAttrs: toAbsFixtureUrl(nestedAttrsParquetUrl),
  nestedListStruct: toAbsFixtureUrl(nestedListStructParquetUrl),
  longFieldNames: toAbsFixtureUrl(longFieldNamesParquetUrl),
};

const GEOJSON_GEOMETRY_CASES = [
  { label: 'Point', fixtureUrl: FIXTURE_URLS.point, expectedTypes: ['Point'] },
  { label: 'MultiPoint', fixtureUrl: FIXTURE_URLS.multipoint, expectedTypes: ['MultiPoint'] },
  { label: 'LineString', fixtureUrl: FIXTURE_URLS.linestring, expectedTypes: ['LineString'] },
  { label: 'MultiLineString', fixtureUrl: FIXTURE_URLS.multilinestring, expectedTypes: ['MultiLineString'] },
  { label: 'Polygon', fixtureUrl: FIXTURE_URLS.polygon, expectedTypes: ['Polygon'] },
  { label: 'MultiPolygon', fixtureUrl: FIXTURE_URLS.multipolygon, expectedTypes: ['MultiPolygon'] },
  {
    label: 'mixed Point and MultiPoint',
    fixtureUrl: FIXTURE_URLS.mixedPointMultipoint,
    expectedTypes: ['MultiPoint', 'Point'],
  },
  {
    label: 'mixed LineString and MultiLineString',
    fixtureUrl: FIXTURE_URLS.mixedLineMultiline,
    expectedTypes: ['LineString', 'MultiLineString'],
  },
  {
    label: 'mixed Polygon and MultiPolygon',
    fixtureUrl: FIXTURE_URLS.mixedPolygonMultipolygon,
    expectedTypes: ['MultiPolygon', 'Polygon'],
  },
  {
    label: 'mixed Point and Polygon',
    fixtureUrl: FIXTURE_URLS.mixedPointPolygon,
    expectedTypes: ['Point', 'Polygon'],
  },
];

const CSV_GEOMETRY_CASES = [
  { label: 'Point', fixtureUrl: FIXTURE_URLS.point, expectedFragments: ['POINT'] },
  { label: 'MultiPoint', fixtureUrl: FIXTURE_URLS.multipoint, expectedFragments: ['MULTIPOINT'] },
  { label: 'LineString', fixtureUrl: FIXTURE_URLS.linestring, expectedFragments: ['LINESTRING'] },
  { label: 'MultiLineString', fixtureUrl: FIXTURE_URLS.multilinestring, expectedFragments: ['MULTILINESTRING'] },
  { label: 'Polygon', fixtureUrl: FIXTURE_URLS.polygon, expectedFragments: ['POLYGON'] },
  { label: 'MultiPolygon', fixtureUrl: FIXTURE_URLS.multipolygon, expectedFragments: ['MULTIPOLYGON'] },
  {
    label: 'mixed Point and MultiPoint',
    fixtureUrl: FIXTURE_URLS.mixedPointMultipoint,
    expectedFragments: ['POINT', 'MULTIPOINT'],
  },
  {
    label: 'mixed LineString and MultiLineString',
    fixtureUrl: FIXTURE_URLS.mixedLineMultiline,
    expectedFragments: ['LINESTRING', 'MULTILINESTRING'],
  },
  {
    label: 'mixed Polygon and MultiPolygon',
    fixtureUrl: FIXTURE_URLS.mixedPolygonMultipolygon,
    expectedFragments: ['POLYGON', 'MULTIPOLYGON'],
  },
  {
    label: 'mixed Point and Polygon',
    fixtureUrl: FIXTURE_URLS.mixedPointPolygon,
    expectedFragments: ['POINT', 'POLYGON'],
  },
];

const KML_GEOMETRY_CASES = [
  { label: 'Point', fixtureUrl: FIXTURE_URLS.point, expectedTags: ['<Point>'] },
  { label: 'MultiPoint', fixtureUrl: FIXTURE_URLS.multipoint, expectedTags: ['<MultiGeometry>', '<Point>'] },
  { label: 'LineString', fixtureUrl: FIXTURE_URLS.linestring, expectedTags: ['<LineString>'] },
  { label: 'MultiLineString', fixtureUrl: FIXTURE_URLS.multilinestring, expectedTags: ['<MultiGeometry>', '<LineString>'] },
  { label: 'Polygon', fixtureUrl: FIXTURE_URLS.polygon, expectedTags: ['<Polygon>', '<outerBoundaryIs>'] },
  { label: 'MultiPolygon', fixtureUrl: FIXTURE_URLS.multipolygon, expectedTags: ['<MultiGeometry>', '<Polygon>'] },
  {
    label: 'mixed Point and Polygon',
    fixtureUrl: FIXTURE_URLS.mixedPointPolygon,
    expectedTags: ['<Point>', '<Polygon>'],
  },
];

const DXF_GEOMETRY_CASES = [
  { label: 'Point', fixtureUrl: FIXTURE_URLS.point, expectedFragments: ['0\nPOINT\n'] },
  { label: 'MultiPoint', fixtureUrl: FIXTURE_URLS.multipoint, expectedFragments: ['0\nPOINT\n'] },
  { label: 'LineString', fixtureUrl: FIXTURE_URLS.linestring, expectedFragments: ['0\nPOLYLINE\n', '70\n0\n'] },
  { label: 'MultiLineString', fixtureUrl: FIXTURE_URLS.multilinestring, expectedFragments: ['0\nPOLYLINE\n', '70\n0\n'] },
  { label: 'Polygon', fixtureUrl: FIXTURE_URLS.polygon, expectedFragments: ['0\nPOLYLINE\n', '70\n1\n'] },
  { label: 'MultiPolygon', fixtureUrl: FIXTURE_URLS.multipolygon, expectedFragments: ['0\nPOLYLINE\n', '70\n1\n'] },
  {
    label: 'mixed Point and Polygon',
    fixtureUrl: FIXTURE_URLS.mixedPointPolygon,
    expectedFragments: ['0\nPOINT\n', '0\nPOLYLINE\n'],
  },
];

const GEOPARQUET_GEOMETRY_CASES = GEOJSON_GEOMETRY_CASES;
const GPKG_GEOMETRY_CASES = [
  { label: 'Point', fixtureUrl: FIXTURE_URLS.point, expectedGeometryType: 'POINT' },
  { label: 'MultiPoint', fixtureUrl: FIXTURE_URLS.multipoint, expectedGeometryType: 'MULTIPOINT' },
  { label: 'LineString', fixtureUrl: FIXTURE_URLS.linestring, expectedGeometryType: 'LINESTRING' },
  { label: 'MultiLineString', fixtureUrl: FIXTURE_URLS.multilinestring, expectedGeometryType: 'MULTILINESTRING' },
  { label: 'Polygon', fixtureUrl: FIXTURE_URLS.polygon, expectedGeometryType: 'POLYGON' },
];

let currentTestTrace = null;

function serializeTraceError(error) {
  if (!error) return null;
  return { name: error.name, message: error.message, stack: error.stack };
}

function pushTestTrace(level, message, details) {
  if (!currentTestTrace) return;
  currentTestTrace.push({ at: new Date().toISOString(), level, message, details });
  if (currentTestTrace.length > TEST_TRACE_LIMIT) {
    currentTestTrace.shift();
  }
}

function logTrace(message, details) {
  pushTestTrace('log', message, details);
}

function errorTrace(message, details) {
  pushTestTrace('error', message, details);
}

function flushTestTrace(testName, traceEntries) {
  if (!traceEntries?.length) return;
  console.error(`--- Buffered trace for failed test: ${testName} ---`);
  for (const entry of traceEntries) {
    const prefix = `[${entry.at}] ${entry.level}`;
    if (entry.details === undefined) {
      console.error(`${prefix} ${entry.message}`);
    } else {
      console.error(`${prefix} ${entry.message}`, entry.details);
    }
  }
  console.error('--- End trace ---');
}

function installWindowFailureLogging() {
  const onError = (event) => {
    errorTrace('window:error', {
      message: event.message,
      filename: event.filename,
      lineno: event.lineno,
      colno: event.colno,
      error: serializeTraceError(event.error),
    });
  };
  const onUnhandledRejection = (event) => {
    errorTrace('window:unhandledrejection', {
      reason: event.reason instanceof Error ? serializeTraceError(event.reason) : event.reason,
    });
  };
  window.addEventListener('error', onError);
  window.addEventListener('unhandledrejection', onUnhandledRejection);
  return () => {
    window.removeEventListener('error', onError);
    window.removeEventListener('unhandledrejection', onUnhandledRejection);
  };
}

function createMemoryFileRoot() {
  const files = new Map();

  function cloneBytes(bytes) {
    return new Uint8Array(bytes);
  }

  async function toBytes(part) {
    if (part instanceof Uint8Array) return cloneBytes(part);
    if (part instanceof ArrayBuffer) return new Uint8Array(part);
    if (typeof part === 'string') return new TextEncoder().encode(part);
    if (part?.buffer instanceof ArrayBuffer) {
      return new Uint8Array(part.buffer, part.byteOffset ?? 0, part.byteLength);
    }
    return new Uint8Array(await new Blob([part]).arrayBuffer());
  }

  function ensureSize(bytes, size) {
    if (bytes.length >= size) return bytes;
    const next = new Uint8Array(size);
    next.set(bytes);
    return next;
  }

  class MemoryFileHandle {
    constructor(name) {
      this.kind = 'file';
      this.name = name;
    }

    async createWritable({ keepExistingData = false } = {}) {
      let bytes = keepExistingData && files.has(this.name)
        ? cloneBytes(files.get(this.name))
        : new Uint8Array(0);
      let position = 0;
      const writeBytes = async (data) => {
        const chunk = await toBytes(data);
        bytes = ensureSize(bytes, position + chunk.length);
        bytes.set(chunk, position);
        position += chunk.length;
      };

      return {
        write: async (part) => {
          if (part?.type === 'write') {
            if (typeof part.position === 'number') position = part.position;
            await writeBytes(part.data);
            return;
          }
          if (part?.type === 'seek') {
            position = part.position;
            return;
          }
          if (part?.type === 'truncate') {
            bytes = ensureSize(bytes, part.size).slice(0, part.size);
            position = Math.min(position, part.size);
            return;
          }

          await writeBytes(part);
        },
        seek: async (offset) => {
          position = offset;
        },
        truncate: async (size) => {
          bytes = ensureSize(bytes, size).slice(0, size);
          position = Math.min(position, size);
        },
        close: async () => {
          files.set(this.name, cloneBytes(bytes));
        },
      };
    }

    async getFile() {
      const bytes = files.get(this.name) || new Uint8Array(0);
      return new File([bytes], this.name, { type: 'application/octet-stream' });
    }
  }

  const root = {
    async getFileHandle(name, { create = false } = {}) {
      if (!files.has(name) && !create) {
        throw new DOMException(`File not found: ${name}`, 'NotFoundError');
      }
      if (!files.has(name) && create) {
        files.set(name, new Uint8Array(0));
      }
      return new MemoryFileHandle(name);
    },
    async getDirectoryHandle(name, { create = false } = {}) {
      if (!create) {
        throw new DOMException(`Directory not found: ${name}`, 'NotFoundError');
      }
      return root;
    },
    async removeEntry(name) {
      files.delete(name);
    },
    async *[Symbol.asyncIterator]() {
      for (const name of files.keys()) {
        yield [name, { kind: 'file', name }];
      }
    },
  };

  return root;
}

async function queryGpkgBlob(blob, sql) {
  const root = await navigator.storage.getDirectory();
  const fileName = `gpkg-query-${Date.now()}-${Math.random().toString(36).slice(2)}.gpkg`;
  logTrace('queryGpkgBlob:start', { fileName, size: blob.size, sql });
  const handle = await root.getFileHandle(fileName, { create: true });
  const writable = await handle.createWritable();
  await writable.write(new Uint8Array(await blob.arrayBuffer()));
  await writable.close();

  const { SQLite, OPFSAnyContextVFS, module } = await getGpkgQueryRuntime();
  logTrace('queryGpkgBlob:runtime-ready', { fileName });
  const sqlite3 = SQLite.Factory(module);
  const vfs = await OPFSAnyContextVFS.create(`gpkg-query-vfs-${Math.random().toString(36).slice(2)}`, module);
  sqlite3.vfs_register(vfs, true);

  const db = await sqlite3.open_v2(fileName);
  const rows = [];
  try {
    await sqlite3.exec(db, sql, (row, columns) => {
      rows.push(Object.fromEntries(columns.map((column, index) => [column, row[index]])));
    });
    logTrace('queryGpkgBlob:query-complete', { fileName, rowCount: rows.length });
  } finally {
    await sqlite3.close(db);
    vfs.close?.();
    await root.removeEntry(fileName);
    logTrace('queryGpkgBlob:cleanup', { fileName });
  }

  return rows;
}

let gpkgQueryRuntimePromise = null;

async function getGpkgQueryRuntime() {
  if (!gpkgQueryRuntimePromise) {
    logTrace('getGpkgQueryRuntime:init');
    gpkgQueryRuntimePromise = (async () => {
      const [{ default: SQLiteESMFactory }, SQLite, { OPFSAnyContextVFS }] = await Promise.all([
        import('wa-sqlite-rtree/dist/wa-sqlite-async.mjs'),
        import('wa-sqlite-rtree/src/sqlite-api.js'),
        import('wa-sqlite-rtree/src/examples/OPFSAnyContextVFS.js'),
      ]);

      const module = await SQLiteESMFactory({
        locateFile: (file) => file.endsWith('.wasm') ? WA_SQLITE_WASM_URL : file,
      });

      logTrace('getGpkgQueryRuntime:ready');
      return { SQLite, OPFSAnyContextVFS, module };
    })();
  } else {
    logTrace('getGpkgQueryRuntime:reuse');
  }

  return gpkgQueryRuntimePromise;
}

function installMemoryFsHarness(duckdbClient) {
  const root = createMemoryFileRoot();
  const hadStorage = 'storage' in navigator;
  const originalStorage = navigator.storage;
  const storage = originalStorage ?? {};
  const originalGetDirectory = storage.getDirectory?.bind(storage);
  const originalCreateDuckdbOpfsFile = FormatHandler.prototype.createDuckdbOpfsFile;
  const originalGetOpfsFile = FormatHandler.prototype.getOpfsFile;
  const originalGetOpfsHandle = FormatHandler.prototype.getOpfsHandle;
  const originalRemoveOpfsFile = FormatHandler.prototype.removeOpfsFile;
  const originalReleaseDuckdbOpfsFile = FormatHandler.prototype.releaseDuckdbOpfsFile;

  if (!hadStorage) {
    Object.defineProperty(navigator, 'storage', {
      configurable: true,
      writable: true,
      value: storage,
    });
  }

  storage.getDirectory = async () => root;

  FormatHandler.prototype.createDuckdbOpfsFile = async function(prefix, ext) {
    const path = `${prefix}${this.sessionId}_${Date.now()}.${ext}`;
    this._duckdbRegisteredPaths.add(path);
    return path;
  };

  FormatHandler.prototype.getOpfsHandle = async function(name, { create = false } = {}) {
    return root.getFileHandle(name.replace('opfs://', ''), { create });
  };

  FormatHandler.prototype.getOpfsFile = async function(opfsPath) {
    const path = opfsPath.replace('opfs://', '');
    try {
      const handle = await root.getFileHandle(path, { create: false });
      return handle.getFile();
    } catch {
      // Fall through to DuckDB virtual files.
    }
    try {
      const bytes = await duckdbClient.db.copyFileToBuffer(path);
      return new File([bytes], path, { type: 'application/octet-stream' });
    }
    catch {
      const handle = await root.getFileHandle(path, { create: false });
      return handle.getFile();
    }
  };

  FormatHandler.prototype.removeOpfsFile = async function(opfsFileName) {
    const path = opfsFileName.replace('opfs://', '');
    try { await duckdbClient.db.dropFile(path); } catch {} // eslint-disable-line no-empty
    try { await root.removeEntry(path); } catch {} // eslint-disable-line no-empty
  };

  FormatHandler.prototype.releaseDuckdbOpfsFile = async function(opfsFileName) {
    this._duckdbRegisteredPaths.delete(opfsFileName);
  };

  return () => {
    if (originalGetDirectory) {
      storage.getDirectory = originalGetDirectory;
    } else {
      delete storage.getDirectory;
    }
    if (!hadStorage) {
      delete navigator.storage;
    }
    FormatHandler.prototype.createDuckdbOpfsFile = originalCreateDuckdbOpfsFile;
    FormatHandler.prototype.getOpfsFile = originalGetOpfsFile;
    FormatHandler.prototype.getOpfsHandle = originalGetOpfsHandle;
    FormatHandler.prototype.removeOpfsFile = originalRemoveOpfsFile;
    FormatHandler.prototype.releaseDuckdbOpfsFile = originalReleaseDuckdbOpfsFile;
  };
}

async function createBrowserDuckDBClient() {
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: mvpWasmUrl,
      mainWorker: mvpWorkerUrl,
    },
    eh: {
      mainModule: ehWasmUrl,
      mainWorker: ehWorkerUrl,
    },
    coi: {
      mainModule: coiWasmUrl,
      mainWorker: coiWorkerUrl,
      pthreadWorker: coiPthreadWorkerUrl,
    },
  });

  try {
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return createDuckDBClient(db, { _worker: worker });
  } catch (error) {
    errorTrace('DuckDB browser bootstrap failed', {
      bundle,
      userAgent: navigator.userAgent,
      error: serializeTraceError(error),
    });
    throw error;
  }
}

async function materializeDownloads(entries) {
  return Promise.all(entries.map(async ({ downloadName, blobParts }) => ({
    downloadName,
    blob: new Blob(blobParts),
  })));
}

function findDownload(downloads, suffix) {
  const match = downloads.find(entry => entry.downloadName.endsWith(suffix));
  expect(match, `Expected download ending with ${suffix}`).toBeTruthy();
  return match;
}

function findDownloads(downloads, suffix) {
  return downloads.filter(entry => entry.downloadName.endsWith(suffix));
}

function blobToAsyncBuffer(blob) {
  return {
    byteLength: blob.size,
    async slice(start, end) {
      return await blob.slice(start, end).arrayBuffer();
    },
  };
}

async function getGeoParquetMetadata(blob) {
  const metadata = await parquetMetadataAsync(blobToAsyncBuffer(blob));
  const geoEntry = metadata.key_value_metadata?.find(entry => entry.key === 'geo');
  return geoEntry ? JSON.parse(geoEntry.value) : null;
}

async function getParquetTopLevelFieldNames(blob) {
  const metadata = await parquetMetadataAsync(blobToAsyncBuffer(blob));
  const schema = parquetSchema(metadata);
  return schema.children.map(child => child.element.name);
}

function parseDbfHeader(buffer) {
  const view = new DataView(buffer);
  const recordCount = view.getUint32(4, true);
  const headerLength = view.getUint16(8, true);
  const fields = [];
  for (let offset = 32; offset < headerLength - 1; offset += 32) {
    const nameBytes = new Uint8Array(buffer, offset, 11);
    const name = new TextDecoder().decode(nameBytes).replace(/\0+$/, '').trim();
    if (!name) continue;
    fields.push(name);
  }
  return { recordCount, fields };
}

function parseShpHeader(buffer) {
  const view = new DataView(buffer);
  return {
    shapeType: view.getInt32(32, true),
    firstRecordShapeType: view.getInt32(108, true),
    firstPoint: [
      view.getFloat64(112, true),
      view.getFloat64(120, true),
    ],
  };
}

describe('format handler end-to-end', () => {
  /** @type {import('../src/duckdb_adapter.js').DuckDBClient | null} */
  let duckdbClient = null;
  let restoreFsHarness = null;
  let restoreWindowFailureLogging = null;

  beforeEach((ctx) => {
    const traceBuffer = [];
    currentTestTrace = traceBuffer;
    ctx.onTestFinished(({ task }) => {
      if (task?.result?.state === 'fail') {
        flushTestTrace(task.meta.name, traceBuffer);
      }
      if (currentTestTrace === traceBuffer) {
        currentTestTrace = null;
      }
    });
  });

  beforeAll(async () => {
    restoreWindowFailureLogging = installWindowFailureLogging();
    duckdbClient = await createBrowserDuckDBClient();
    restoreFsHarness = installMemoryFsHarness(duckdbClient);
  });

  afterAll(() => {
    restoreFsHarness?.();
    restoreWindowFailureLogging?.();
    duckdbClient?.terminate();
  });

  it('has a working memory filesystem harness in WebKit', async () => {
    const root = await navigator.storage.getDirectory();
    const handle = await root.getFileHandle('vitest-memoryfs-smoke.txt', { create: true });
    const writable = await handle.createWritable();
    await writable.write('ok');
    await writable.close();
    const file = await handle.getFile();
    expect(await file.text()).toBe('ok');
    await root.removeEntry('vitest-memoryfs-smoke.txt');
  });

  async function renderFormat(
    format,
    {
      sourceUrl = FIXTURE_URLS.sample,
      baseName = 'sample',
      extractorOptions = {},
      bbox = TEST_BBOX,
      flattenStructs = false,
    } = {}
  ) {
    const effectiveExtractorOptions = { ...extractorOptions };
    if (format === 'geopackage' && !effectiveExtractorOptions.gpkgWorker && !effectiveExtractorOptions.gpkgWorkerUrl) {
      effectiveExtractorOptions.gpkgWorker = createMockGpkgWorker({
        duckdbClient,
        wasmUrl: WA_SQLITE_WASM_URL,
      });
    }

    const extractor = new GeoParquetExtractor({ duckdb: duckdbClient, ...effectiveExtractorOptions });
    logTrace('renderFormat:start', { format, url: sourceUrl });
    const handler = await extractor.prepare({
      urls: [sourceUrl],
      bbox,
      format,
      flattenStructs,
      onProgress: () => {},
      onStatus: (msg) => logTrace('renderFormat:status', { format, msg }),
    });
    logTrace('renderFormat:prepared', { format, handler: handler.constructor.name });

    try {
      await handler.write({
        onProgress: (pct) => logTrace('renderFormat:progress', { format, pct }),
        onStatus: (msg) => logTrace('renderFormat:write-status', { format, msg }),
      });
      logTrace('renderFormat:wrote', { format });
      const downloads = await materializeDownloads(await handler.getDownloadMap(baseName));
      logTrace('renderFormat:downloads', {
        format,
        files: downloads.map(entry => ({ name: entry.downloadName, size: entry.blob.size })),
      });
      return downloads;
    } catch (error) {
      errorTrace('renderFormat:failed', {
        format,
        error: serializeTraceError(error),
      });
      throw error;
    } finally {
      logTrace('renderFormat:cleanup:start', { format });
      await handler.cleanup();
      logTrace('renderFormat:cleanup:done', { format });
    }
  }

  it('writes CSV output from a GeoParquet 1.1 source', async () => {
    const downloads = await renderFormat('csv');
    const csv = await findDownload(downloads, '.csv').blob.text();
    const lines = csv.trim().split('\n');

    expect(lines[0]).toContain('name');
    expect(lines[0]).toContain('value');
    expect(lines[0]).toContain('geometry_wkt');
    expect(lines).toHaveLength(3);
    expect(csv).toContain('alpha');
    expect(csv).toContain('POINT');
  });

  it('handles flattenStructs in CSV by flattening only top-level structs', async () => {
    const downloads = await renderFormat('csv', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-csv',
      flattenStructs: true,
    });
    const csv = await findDownload(downloads, '.csv').blob.text();
    const [headerLine, ...rows] = csv.trim().split('\n');
    const headers = headerLine.split(',');

    expect(headers).toContain('info.code');
    expect(headers).toContain('info.score');
    expect(headers).toContain('info.nest');
    expect(headers).toContain('dims.h');
    expect(headers).toContain('dims.w');
    expect(headers).toContain('dims.deep');
    expect(headers).toContain('items');
    expect(headers).not.toContain('info');
    expect(headers).not.toContain('dims');
    expect(rows.join('\n')).toContain('north');
    expect(rows.join('\n')).toContain('red');
  });

  for (const { label, fixtureUrl, expectedFragments } of CSV_GEOMETRY_CASES) {
    it(`writes ${label} geometry markers to CSV output`, async () => {
      const downloads = await renderFormat('csv', {
        sourceUrl: fixtureUrl,
        baseName: `csv-${label.toLowerCase().replaceAll(/\s+/g, '-')}`,
      });
      const csv = await findDownload(downloads, '.csv').blob.text();

      expect(csv).toContain('geometry_wkt');
      for (const fragment of expectedFragments) {
        expect(csv).toContain(fragment);
      }
    });
  }

  it('writes GeoJSON output from a GeoParquet 1.1 source', async () => {
    const downloads = await renderFormat('geojson');
    const geojson = JSON.parse(await findDownload(downloads, '.geojson').blob.text());

    expect(geojson.type).toBe('FeatureCollection');
    expect(geojson.features).toHaveLength(2);
    expect(geojson.features[0].geometry.type).toBe('Point');
    expect(geojson.features[0].properties.name).toBe('alpha');
    expect(geojson.features[1].properties.value).toBe(2);
  });

  it('currently leaves GeoJSON properties unflattened even when flattenStructs is true', async () => {
    const downloads = await renderFormat('geojson', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-geojson',
      flattenStructs: true,
    });
    const geojson = JSON.parse(await findDownload(downloads, '.geojson').blob.text());

    expect(geojson.features[0].properties.info.code).toBe('A1');
    expect(geojson.features[0].properties.info.nest.rank).toBe('high');
    expect(geojson.features[0].properties.items[0]).toBe('red');
    expect(geojson.features[0].properties['info.code']).toBeUndefined();
  });

  it('still passes LIST<STRUCT> values through GeoJSON when flattenStructs is true', async () => {
    const downloads = await renderFormat('geojson', {
      sourceUrl: FIXTURE_URLS.nestedListStruct,
      baseName: 'nested-list-struct-geojson',
      flattenStructs: true,
    });
    const geojson = JSON.parse(await findDownload(downloads, '.geojson').blob.text());

    expect(geojson.features[0].properties.items[0].id).toBe(101);
    expect(geojson.features[0].properties.items[0].label).toBe('red');
    expect(geojson.features[0].properties['items.id']).toBeUndefined();
  });

  for (const { label, fixtureUrl, expectedTypes } of GEOJSON_GEOMETRY_CASES) {
    it(`preserves ${label} geometries in GeoJSON output`, async () => {
      const downloads = await renderFormat('geojson', {
        sourceUrl: fixtureUrl,
        baseName: label.toLowerCase().replaceAll(/\s+/g, '-'),
      });
      const geojson = JSON.parse(await findDownload(downloads, '.geojson').blob.text());
      const actualTypes = [...new Set(geojson.features.map(feature => feature.geometry?.type))].sort();

      expect(geojson.type).toBe('FeatureCollection');
      expect(geojson.features).toHaveLength(2);
      expect(actualTypes).toEqual([...expectedTypes].sort());
    });
  }

  it('writes KML output from a GeoParquet 1.1 source', async () => {
    const downloads = await renderFormat('kml');
    const kml = await findDownload(downloads, '.kml').blob.text();

    expect(kml).toContain('<kml');
    expect((kml.match(/<Placemark>/g) || [])).toHaveLength(2);
    expect(kml).toContain('<name>alpha</name>');
    expect(kml).toContain('<coordinates>77.1,12.1</coordinates>');
  });

  it('handles flattenStructs in KML by flattening top-level structs into ExtendedData names', async () => {
    const downloads = await renderFormat('kml', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-kml',
      flattenStructs: true,
    });
    const kml = await findDownload(downloads, '.kml').blob.text();

    expect(kml).toContain('Data name="info.code"');
    expect(kml).toContain('Data name="info.score"');
    expect(kml).toContain('Data name="dims.h"');
    expect(kml).toContain('Data name="dims.deep"');
    expect(kml).toContain('Data name="items"');
    expect(kml).not.toContain('Data name="info"');
    expect(kml).toContain('north');
    expect(kml).toContain('red');
  });

  for (const { label, fixtureUrl, expectedTags } of KML_GEOMETRY_CASES) {
    it(`writes ${label} geometry markers to KML output`, async () => {
      const downloads = await renderFormat('kml', {
        sourceUrl: fixtureUrl,
        baseName: `kml-${label.toLowerCase().replaceAll(/\s+/g, '-')}`,
      });
      const kml = await findDownload(downloads, '.kml').blob.text();

      expect((kml.match(/<Placemark>/g) || [])).toHaveLength(2);
      for (const tag of expectedTags) {
        expect(kml).toContain(tag);
      }
    });
  }

  for (const { label, fixtureUrl, expectedFragments } of DXF_GEOMETRY_CASES) {
    it(`writes ${label} geometry markers to DXF output`, async () => {
      const downloads = await renderFormat('dxf', {
        sourceUrl: fixtureUrl,
        baseName: `dxf-${label.toLowerCase().replaceAll(/\s+/g, '-')}`,
        bbox: DXF_TEST_BBOX,
      });
      const dxf = await findDownload(downloads, '.dxf').blob.text();

      expect(dxf).toContain('0\nSECTION\n2\nENTITIES\n');
      expect(dxf).toContain('0\nEOF\n');
      for (const fragment of expectedFragments) {
        expect(dxf).toContain(fragment);
      }
    });
  }

  it('handles flattenStructs in DXF by flattening top-level struct keys in XDATA', async () => {
    const downloads = await renderFormat('dxf', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-dxf',
      bbox: DXF_TEST_BBOX,
      flattenStructs: true,
    });
    const dxf = await findDownload(downloads, '.dxf').blob.text();

    expect(dxf).toContain('1000\ninfo.code\n');
    expect(dxf).toContain('1000\ninfo.score\n');
    expect(dxf).toContain('1000\ndims.h\n');
    expect(dxf).toContain('1000\ndims.deep\n');
    expect(dxf).toContain('1000\nitems\n');
    expect(dxf).not.toContain('1000\ninfo\n');
    expect(dxf).toContain('north');
    expect(dxf).toContain('red');
  });

  for (const { label, fixtureUrl, expectedTypes } of GEOPARQUET_GEOMETRY_CASES) {
    it(`writes ${label} geometry metadata to GeoParquet output`, async () => {
      const downloads = await renderFormat('geoparquet', {
        sourceUrl: fixtureUrl,
        baseName: `geoparquet-${label.toLowerCase().replaceAll(/\s+/g, '-')}`,
      });
      const blob = findDownload(downloads, '.parquet').blob;
      const geo = await getGeoParquetMetadata(blob);

      expect(geo.version).toBe('1.1.0');
      expect(geo.primary_column).toBe('geometry');
      expect(geo.columns.geometry.encoding).toBe('WKB');
      expect(geo.columns.geometry.covering.bbox).toBeTruthy();
      expect([...geo.columns.geometry.geometry_types].sort()).toEqual([...expectedTypes].sort());
    });
  }

  // GeoPackage browser tests disabled — covered by gpkg_worker seam tests instead.
  // it('writes GeoPackage output from a GeoParquet 1.1 source', async () => {
  //   const downloads = await renderFormat('geopackage');
  //   const gpkg = findDownload(downloads, '.gpkg').blob;
  //   const featureRows = await queryGpkgBlob(gpkg, 'SELECT COUNT(*) AS count FROM features');
  //   const metadataRows = await queryGpkgBlob(gpkg, 'SELECT geometry_type_name, srs_id FROM gpkg_geometry_columns');
  //
  //   expect(featureRows[0].count).toBe(2);
  //   expect(metadataRows).toEqual([{ geometry_type_name: 'POINT', srs_id: 4326 }]);
  // });

  // it('handles flattenStructs in GeoPackage by flattening top-level structs into SQLite columns', async () => {
  //   const downloads = await renderFormat('geopackage', {
  //     sourceUrl: FIXTURE_URLS.nestedAttrs,
  //     baseName: 'nested-gpkg',
  //     flattenStructs: true,
  //   });
  //   const gpkg = findDownload(downloads, '.gpkg').blob;
  //   const columns = await queryGpkgBlob(gpkg, 'PRAGMA table_info(features)');
  //   const columnNames = columns.map(column => column.name);
  //
  //   expect(columnNames).toContain('geom');
  //   expect(columnNames).toContain('info.code');
  //   expect(columnNames).toContain('info.score');
  //   expect(columnNames).toContain('info.nest');
  //   expect(columnNames).toContain('dims.h');
  //   expect(columnNames).toContain('dims.w');
  //   expect(columnNames).toContain('dims.deep');
  //   expect(columnNames).toContain('items');
  //   expect(columnNames).not.toContain('info');
  //   expect(columnNames).not.toContain('_bbox_minx');
  // });

  // for (const { label, fixtureUrl, expectedGeometryType } of GPKG_GEOMETRY_CASES) {
  //   it(`writes ${label} geometry metadata to GeoPackage output`, async () => {
  //     const downloads = await renderFormat('geopackage', {
  //       sourceUrl: fixtureUrl,
  //       baseName: `gpkg-${label.toLowerCase().replaceAll(/\s+/g, '-')}`,
  //     });
  //     const gpkg = findDownload(downloads, '.gpkg').blob;
  //     const metadataRows = await queryGpkgBlob(gpkg, 'SELECT geometry_type_name FROM gpkg_geometry_columns');
  //
  //     expect(metadataRows[0].geometry_type_name).toBe(expectedGeometryType);
  //   }, 60000);
  // }

  it('currently leaves GeoParquet schema unflattened even when flattenStructs is true', async () => {
    const downloads = await renderFormat('geoparquet', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-geoparquet',
      flattenStructs: true,
    });
    const fields = await getParquetTopLevelFieldNames(findDownload(downloads, '.parquet').blob);

    expect(fields).toContain('info');
    expect(fields).toContain('dims');
    expect(fields).toContain('items');
    expect(fields).not.toContain('info.code');
    expect(fields).not.toContain('dims.h');
  });

  it('still leaves LIST<STRUCT> columns untouched in GeoParquet when flattenStructs is true', async () => {
    const downloads = await renderFormat('geoparquet', {
      sourceUrl: FIXTURE_URLS.nestedListStruct,
      baseName: 'nested-list-struct-geoparquet',
      flattenStructs: true,
    });
    const fields = await getParquetTopLevelFieldNames(findDownload(downloads, '.parquet').blob);

    expect(fields).toContain('items');
    expect(fields).not.toContain('items.id');
  });

  it('writes shapefile components from a GeoParquet 1.1 source', async () => {
    const downloads = await renderFormat('shapefile');
    const shp = findDownload(downloads, '.shp');
    const shx = findDownload(downloads, '.shx');
    const dbf = findDownload(downloads, '.dbf');
    const prj = findDownload(downloads, '.prj');

    const shpHeader = parseShpHeader(await shp.blob.arrayBuffer());
    const dbfHeader = parseDbfHeader(await dbf.blob.arrayBuffer());
    const prjText = await prj.blob.text();

    expect(shpHeader.shapeType).toBe(1);
    expect(shpHeader.firstRecordShapeType).toBe(1);
    expect(shpHeader.firstPoint[0]).toBeCloseTo(77.1, 6);
    expect(shpHeader.firstPoint[1]).toBeCloseTo(12.1, 6);
    expect(dbfHeader.recordCount).toBe(2);
    expect(dbfHeader.fields).toContain('name');
    expect(dbfHeader.fields).toContain('value');
    expect((await shx.blob.arrayBuffer()).byteLength).toBeGreaterThan(100);
    expect(prjText).toContain('WGS_1984');
  });

  it('handles flattenStructs in Shapefile by flattening top-level structs into DBF fields', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.nestedAttrs,
      baseName: 'nested-shp',
      flattenStructs: true,
    });
    const dbf = findDownload(downloads, '.dbf');
    const dbfHeader = parseDbfHeader(await dbf.blob.arrayBuffer());

    expect(dbfHeader.fields).toContain('info.code');
    expect(dbfHeader.fields).toContain('info.score');
    expect(dbfHeader.fields).toContain('info.nest');
    expect(dbfHeader.fields).toContain('dims.h');
    expect(dbfHeader.fields).toContain('dims.w');
    expect(dbfHeader.fields).toContain('dims.deep');
    expect(dbfHeader.fields).toContain('items');
    expect(dbfHeader.fields).not.toContain('info');
  });

  it('truncates long flattened Shapefile field names to unique 10-char DBF names', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.longFieldNames,
      baseName: 'long-fieldnames-shp',
      flattenStructs: true,
    });
    const dbfHeader = parseDbfHeader(await findDownload(downloads, '.dbf').blob.arrayBuffer());

    expect(dbfHeader.recordCount).toBe(2);
    expect(dbfHeader.fields).toEqual(expect.arrayContaining([
      'name',
      'transport_',
      'transport1',
      'transport2',
      'administra',
    ]));
    expect(new Set(dbfHeader.fields).size).toBe(dbfHeader.fields.length);
    expect(dbfHeader.fields.every(name => name.length <= 10)).toBe(true);
  });

  for (const format of ['csv', 'kml', 'dxf', 'shapefile', 'geopackage']) {
    it(`currently errors on flattenStructs with top-level LIST<STRUCT> in ${format.toUpperCase()}`, async () => {
      await expect(renderFormat(format, {
        sourceUrl: FIXTURE_URLS.nestedListStruct,
        baseName: `nested-list-struct-${format}`,
        flattenStructs: true,
        bbox: format === 'dxf' ? DXF_TEST_BBOX : TEST_BBOX,
      })).rejects.toThrow(/Cannot extract field 'id' from expression "items"/);
    });
  }

  it('promotes mixed Point and MultiPoint input into one multipoint shapefile', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.mixedPointMultipoint,
      baseName: 'mixed-point-multipoint',
    });
    const shpDownloads = findDownloads(downloads, '.shp');
    const dbfDownloads = findDownloads(downloads, '.dbf');
    const shpHeader = parseShpHeader(await shpDownloads[0].blob.arrayBuffer());
    const dbfHeader = parseDbfHeader(await dbfDownloads[0].blob.arrayBuffer());

    expect(shpDownloads).toHaveLength(1);
    expect(dbfDownloads).toHaveLength(1);
    expect(shpDownloads[0].downloadName).toBe('mixed-point-multipoint.shp');
    expect(shpHeader.shapeType).toBe(8);
    expect(dbfHeader.recordCount).toBe(2);
  });

  it('keeps mixed LineString and MultiLineString input in one polyline shapefile', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.mixedLineMultiline,
      baseName: 'mixed-line-multiline',
    });
    const shpDownloads = findDownloads(downloads, '.shp');
    const shpHeader = parseShpHeader(await shpDownloads[0].blob.arrayBuffer());

    expect(shpDownloads).toHaveLength(1);
    expect(shpDownloads[0].downloadName).toBe('mixed-line-multiline.shp');
    expect(shpHeader.shapeType).toBe(3);
  });

  it('keeps mixed Polygon and MultiPolygon input in one polygon shapefile', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.mixedPolygonMultipolygon,
      baseName: 'mixed-polygon-multipolygon',
    });
    const shpDownloads = findDownloads(downloads, '.shp');
    const shpHeader = parseShpHeader(await shpDownloads[0].blob.arrayBuffer());

    expect(shpDownloads).toHaveLength(1);
    expect(shpDownloads[0].downloadName).toBe('mixed-polygon-multipolygon.shp');
    expect(shpHeader.shapeType).toBe(5);
  });

  it('splits mixed Point and Polygon input into separate shapefile bundles', async () => {
    const downloads = await renderFormat('shapefile', {
      sourceUrl: FIXTURE_URLS.mixedPointPolygon,
      baseName: 'mixed-point-polygon',
    });
    const shpDownloads = findDownloads(downloads, '.shp').sort((a, b) => a.downloadName.localeCompare(b.downloadName));
    const dbfDownloads = findDownloads(downloads, '.dbf').sort((a, b) => a.downloadName.localeCompare(b.downloadName));

    expect(shpDownloads).toHaveLength(2);
    expect(dbfDownloads).toHaveLength(2);
    expect(shpDownloads.map(entry => entry.downloadName)).toEqual([
      'mixed-point-polygon_point.shp',
      'mixed-point-polygon_polygon.shp',
    ]);

    const pointShpHeader = parseShpHeader(await shpDownloads[0].blob.arrayBuffer());
    const polygonShpHeader = parseShpHeader(await shpDownloads[1].blob.arrayBuffer());
    const pointDbfHeader = parseDbfHeader(await dbfDownloads[0].blob.arrayBuffer());
    const polygonDbfHeader = parseDbfHeader(await dbfDownloads[1].blob.arrayBuffer());

    expect(pointShpHeader.shapeType).toBe(1);
    expect(polygonShpHeader.shapeType).toBe(5);
    expect(pointDbfHeader.recordCount).toBe(1);
    expect(polygonDbfHeader.recordCount).toBe(1);
  });
});
