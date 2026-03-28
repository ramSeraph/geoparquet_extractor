# geoparquet-extractor

[![npm version](https://img.shields.io/npm/v/geoparquet-extractor)](https://www.npmjs.com/package/geoparquet-extractor)
[![GitHub release](https://img.shields.io/github/v/release/ramSeraph/geoparquet_extractor)](https://github.com/ramSeraph/geoparquet_extractor/releases/latest)

Extract and convert spatial data from remote GeoParquet files entirely in the browser, using OPFS (Origin Private File System) for temporary storage and spill-over. Supports bbox filtering, multiple output formats, and pluggable metadata providers.

> **Browser-only** — requires Origin Private File System (OPFS), Web Workers, and Web Locks APIs.

## How It Works

The library queries remote GeoParquet files using HTTP range requests — only the data matching the requested bounding box is transferred. All processing happens client-side with no backend involved.

```
Browser
  ├─ DuckDB-WASM
  │   ├─ HTTP range requests → remote GeoParquet
  │   ├─ Spatial filtering by bounding box
  │   ├─ OPFS temp directory for spill-over
  │   └─ COPY TO OPFS (intermediate or final output)
  └─ GeoPackage Worker (for .gpkg only)
      ├─ hyparquet (reads intermediate parquet from OPFS)
      └─ wa-sqlite/sqwab (writes .gpkg with R-tree index to OPFS)
```

### Key Internal Dependencies

- **DuckDB-WASM with OPFS temp directory** — uses [duckdb-wasm-opfs-tempdir](https://www.npmjs.com/package/duckdb-wasm-opfs-tempdir) which supports `SET temp_directory = 'opfs://...'` for processing datasets larger than available memory. DuckDB's `spatial` extension is loaded at runtime for geometry operations (`ST_AsWKB`, `ST_AsGeoJSON`, `ST_Hilbert`, etc.).
- **[sqwab](https://github.com/ramSeraph/sqwab)** — wa-sqlite with R-tree support for GeoPackage output. Runs in a dedicated Web Worker using `OPFSAdaptiveVFS` for file I/O.
- **[hyparquet](https://github.com/hyparam/hyparquet)** — pure-JS parquet reader used in the GeoPackage worker to read intermediate files from OPFS.
- **[Apache Arrow](https://arrow.apache.org/)** — columnar data handling.

## Used By

- **[Indian Open Maps](https://indianopenmaps.com/)** — extract Indian geospatial datasets
- **[OSM Layercake Extract](https://ramseraph.github.io/osm-layercake-extract/)** — extract OpenStreetMap data from OSMUS Layercake datasets
- **[Overture Maps Extract](https://ramseraph.github.io/overturemaps-extract/)** — extract Overture Maps Foundation datasets

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
import { ExtentData, MetadataProvider } from 'geoparquet-extractor';

const extentData = new ExtentData({
  metadataProvider: new MetadataProvider(),
  duckdb: client,
});

const { dataExtents, rgExtents } = await extentData.fetchExtents({
  sourceUrl: 'https://example.com/data.mosaic.json',
  partitioned: true,
});
// dataExtents: { filename: [minx, miny, maxx, maxy] } or null
// rgExtents: { filename: { rg_N: [minx, miny, maxx, maxy] } } or null
```

## API

### `GeoParquetExtractor`

Main orchestrator class.

- `constructor({ duckdb, metadataProvider?, gpkgWorkerUrl?, gpkgWorker?, memoryLimitMB? })`
- `async prepare(options)` → Returns format handler for inspection before download
- `async download(handler, { baseName, onProgress?, onStatus? })` → Execute download, returns `boolean`
- `async extract(options)` → Convenience: prepare + download in one call
- `cancel()` → Cancel in-flight download
- `static async cleanupOrphanedFiles()` → Clean up OPFS files from dead sessions
- `static getDownloadBaseName(sourceName, bbox)` → Generate suggested filename

### `ExtentData`

Data-fetching for partition/row-group bboxes.

- `constructor({ metadataProvider, duckdb? })`
- `async fetchExtents({ sourceUrl, partitioned?, includeRowGroups?, onStatus? })` → `{ dataExtents, rgExtents }`

### `MetadataProvider`

Base class with working defaults. Override to customize metadata resolution.

- `getParquetUrl(sourceUrl)` → `string` — resolve source URL to parquet URL (default: identity)
- `async getParquetUrls(sourceUrl)` → `string[]`
- `async getExtents(sourceUrl)` → `{ filename: [minx, miny, maxx, maxy] }` or `null`
- `async getBbox(parquetUrl, duckdb)` → `[minx, miny, maxx, maxy]` or `null`
- `async getRowGroupBboxes(parquetUrl, duckdb)` → `{ rg_N: bbox }` or `null`
- `async getRowGroupBboxesMulti(urls, duckdb)` → `{ filename: { rg_N: bbox } }` or `null`

### `createDuckDBClient(db, options?)`

Wraps an `AsyncDuckDB` instance into the library's DuckDBClient interface.

### `initDuckDB(duckdbDist, options?)`

Creates a DuckDBClient by loading DuckDB-WASM from a distribution URL. Handles bundle selection, worker creation, and WASM instantiation.

### `proxyUrl(url)`

Returns the proxied version of a URL (complement to `setProxyUrl`).

### Format Handlers

Base class and per-format subclasses for advanced usage and subclassing. Normally you don't need these directly — `GeoParquetExtractor.prepare()` creates them for you.

- **`FormatHandler`** — Base class. Manages OPFS file lifecycle, DuckDB queries, bbox filtering, and progress tracking.
  - `getExpectedBrowserStorageUsage()` → Expected peak OPFS usage in bytes
  - `getTotalExpectedDiskUsage()` → Total expected disk usage including downloads
  - `getFormatWarning()` → Format-specific warning string, or `null`
  - `getDownloadMap(baseName)` → List of downloadable files
  - `async write(callbacks)` → Run the format handler's write pipeline
  - `async triggerDownload(baseName, cleanupDelayMs?)` → Trigger browser download(s)
  - `async cleanup()` → Clean up all OPFS files belonging to this session
  - `cancel()` → Cancel the operation
- **`CsvFormatHandler`** — CSV with WKT geometry column
- **`GeoJsonFormatHandler`** — GeoJSON / GeoJSONSeq (`{ commaSeparated? }`)
- **`GeoParquetFormatHandler`** — GeoParquet v1.1 or v2.0 (`{ version? }`)
- **`GeoPackageFormatHandler`** — GeoPackage via wa-sqlite worker (`{ gpkgWorker? }`)
- **`ShapefileFormatHandler`** — Shapefile (.shp/.dbf/.shx/.prj)
- **`KmlFormatHandler`** — KML (Keyhole Markup Language)
- **`DxfFormatHandler`** — DXF (AutoCAD R14, UTM projection)

### Utilities

- **`formatSize(bytes)`** → Human-readable string (e.g., `"1.5 MB"`)
- **`async getStorageEstimate()`** → Browser storage quota and usage via `navigator.storage`
- **`SizeGetter`** — Fetches and caches file sizes via HEAD requests through the configured proxy

## CORS Proxy

If your parquet files need a CORS proxy:

```javascript
import { setProxyUrl } from 'geoparquet-extractor';

// Set a custom proxy URL transformer
setProxyUrl((url) => `/proxy?url=${encodeURIComponent(url)}`);
```

## License

[Unlicense](LICENSE) — public domain.
