# geoparquet-extractor

Extract and convert spatial data from remote GeoParquet files in the browser. Supports bbox filtering, multiple output formats, and pluggable metadata providers.

> **Browser-only** — requires Origin Private File System (OPFS), Web Workers, and Web Locks APIs.

## Installation

```bash
npm install geoparquet-extractor
```

## Quick Start

```javascript
import { GeoParquetExtractor, createDuckDBClient } from 'geoparquet-extractor';

// You initialize DuckDB yourself
import * as duckdb from 'duckdb-wasm-opfs-tempdir';

const db = /* your initialized AsyncDuckDB instance */;
const client = await createDuckDBClient(db, {
  extensions: ['spatial', 'httpfs'],
});

const extractor = new GeoParquetExtractor({ duckdb: client });

await extractor.extract({
  urls: ['https://example.com/data.parquet'],
  bbox: [77.5, 12.9, 77.7, 13.1],
  format: 'geoparquet',
  baseName: 'my-data',
  onProgress: (pct) => console.log(`${pct}%`),
  onStatus: (msg) => console.log(msg),
});
```

## Features

- **9 output formats**: GeoParquet (v1.1 & v2.0), GeoPackage, Shapefile, CSV, GeoJSON, GeoJSONSeq, KML, DXF
- **Spatial filtering**: Bbox intersection with per-partition and per-row-group optimization
- **Pluggable metadata**: Override how partition URLs and bboxes are resolved
- **Extent visualization data**: Fetch partition/row-group bboxes as GeoJSON for map display
- **DuckDB-powered**: Spatial SQL queries via DuckDB WASM (you provide the instance)
- **Self-contained GeoPackage worker**: wa-sqlite-rtree bundled into the worker — no CDN needed

## Formats

| Format | Value | Extension | Notes |
|--------|-------|-----------|-------|
| GeoPackage | `geopackage` | `.gpkg` | Requires GeoPackage worker |
| GeoJSON | `geojson` | `.geojson` | FeatureCollection |
| GeoJSONSeq | `geojsonseq` | `.geojsonl` | Newline-delimited |
| GeoParquet v1.1 | `geoparquet` | `.parquet` | With Hilbert spatial sort |
| GeoParquet v2.0 | `geoparquet2` | `.parquet` | Native geometry encoding |
| CSV | `csv` | `.csv` | WKT geometry column |
| Shapefile | `shapefile` | `.shp` | 2 GB limit per component |
| KML | `kml` | `.kml` | XML format |
| DXF | `dxf` | `.dxf` | AutoCAD R14, UTM projection |

## DuckDB Setup

The library does NOT bundle DuckDB WASM. You initialize it yourself and pass it in:

```javascript
import { createDuckDBClient } from 'geoparquet-extractor';
import * as duckdb from 'duckdb-wasm-opfs-tempdir';

// Standard duckdb-wasm-opfs-tempdir init
const MANUAL_BUNDLES = { /* your bundle config */ };
const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
const worker = new Worker(bundle.mainWorker);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

// Wrap it for the library
const client = await createDuckDBClient(db, {
  extensions: ['spatial', 'httpfs'],
});
```

### Custom DuckDB Builds

The `duckdb-wasm-opfs-tempdir` package supports `SET temp_directory = 'opfs://...'` for large downloads that exceed browser memory limits. The library's `createDuckDBClient` adapter works with any DuckDB WASM build that provides `AsyncDuckDB`.

## GeoPackage Worker

The GeoPackage format requires a Web Worker for wa-sqlite. The library ships a self-contained worker with wa-sqlite-rtree bundled in:

```javascript
// Option 1: URL to hosted worker
const extractor = new GeoParquetExtractor({
  duckdb: client,
  gpkgWorkerUrl: '/workers/gpkg_worker.js',
});

// Option 2: Worker instance (import.meta.url resolves to dist/gpkg_worker.js)
const worker = new Worker(new URL('geoparquet-extractor/gpkg-worker', import.meta.url), { type: 'module' });
const extractor = new GeoParquetExtractor({
  duckdb: client,
  gpkgWorker: worker,
});
```

> **Note**: The worker requires `wa-sqlite-async.wasm` to be served from the same directory as `gpkg_worker.js`. Both files are included in the `dist/` directory.

## Custom Metadata Provider

Override how partition URLs and bboxes are resolved:

```javascript
import { MetadataProvider, GeoParquetExtractor } from 'geoparquet-extractor';

class MyMetadataProvider extends MetadataProvider {
  async getParquetUrls(sourceUrl) {
    const meta = await fetch(sourceUrl + '.meta.json').then(r => r.json());
    const baseUrl = sourceUrl.replace(/[^/]+$/, '');
    return Object.keys(meta.extents).map(f => baseUrl + f);
  }

  async getExtents(sourceUrl) {
    const meta = await fetch(sourceUrl + '.meta.json').then(r => r.json());
    return meta.extents; // { "file.parquet": [minx, miny, maxx, maxy] }
  }

  async getBbox(sourceUrl, duckdb) {
    const extents = await this.getExtents(sourceUrl);
    // Compute overall bbox from all partition extents
    let bbox = [Infinity, Infinity, -Infinity, -Infinity];
    for (const ext of Object.values(extents)) {
      bbox[0] = Math.min(bbox[0], ext[0]);
      bbox[1] = Math.min(bbox[1], ext[1]);
      bbox[2] = Math.max(bbox[2], ext[2]);
      bbox[3] = Math.max(bbox[3], ext[3]);
    }
    return bbox;
  }
}

const extractor = new GeoParquetExtractor({
  duckdb: client,
  metadataProvider: new MyMetadataProvider(),
});
```

## Extent Visualization

Fetch partition and row-group bboxes as GeoJSON for map display:

```javascript
import { ExtentData, DefaultMetadataProvider } from 'geoparquet-extractor';

const extentData = new ExtentData({
  metadataProvider: new DefaultMetadataProvider(),
  duckdb: client,
});

const { dataExtents, rgExtents } = await extentData.fetchExtents({
  sourceUrl: 'https://example.com/data.mosaic.json',
  partitioned: true,
});

// Convert to GeoJSON for map rendering
const { polygons, labelPoints } = extentData.toGeoJSON(dataExtents);
// polygons: FeatureCollection of bbox rectangles
// labelPoints: FeatureCollection of label anchor points
```

## API

### `GeoParquetExtractor`

Main orchestrator class.

- `constructor({ duckdb, metadataProvider?, gpkgWorkerUrl?, gpkgWorker?, memoryLimitMB? })`
- `prepare(options)` → Returns format handler for inspection before download
- `download(handler, { baseName, onProgress?, onStatus? })` → Execute download
- `extract(options)` → Convenience: prepare + download in one call
- `cancel()` → Cancel in-flight download
- `static cleanupOrphanedFiles()` → Clean up OPFS files from dead sessions
- `static getDownloadBaseName(sourceName, bbox)` → Generate suggested filename

### `ExtentData`

Data-fetching for partition/row-group bboxes.

- `constructor({ metadataProvider, duckdb? })`
- `fetchExtents({ sourceUrl, partitioned?, includeRowGroups?, onStatus? })` → `{ dataExtents, rgExtents }`
- `toGeoJSON(extents)` → `{ polygons, labelPoints }`

### `MetadataProvider`

Abstract base class. Override to customize metadata resolution.

- `getParquetUrls(sourceUrl)` → `string[]`
- `getExtents(sourceUrl)` → `{ filename: [minx, miny, maxx, maxy] }`
- `getBbox(sourceUrl, duckdb)` → `[minx, miny, maxx, maxy]`
- `getRowGroupBboxes(parquetUrl, duckdb)` → `{ rg_N: bbox }`
- `getRowGroupBboxesMulti(urls, duckdb)` → `{ filename: { rg_N: bbox } }`

### `createDuckDBClient(db, options?)`

Wraps an `AsyncDuckDB` instance into the library's DuckDBClient interface.

## CORS Proxy

If your parquet files need a CORS proxy:

```javascript
import { setProxyUrl } from 'geoparquet-extractor';

// Set a custom proxy URL transformer
setProxyUrl((url) => `/proxy?url=${encodeURIComponent(url)}`);
```

## License

[Unlicense](LICENSE) — public domain.
