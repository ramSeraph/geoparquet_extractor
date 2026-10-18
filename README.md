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

- **DuckDB-WASM with OPFS temp directory** — ships a patched, pinned upstream DuckDB runtime alongside the extractor. It supports `SET temp_directory = 'opfs://...'` for processing datasets larger than available memory. DuckDB's `spatial` extension is loaded at runtime for geometry operations (`ST_AsWKB`, `ST_AsGeoJSON`, `ST_Hilbert`, etc.).
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
import { GeoParquetExtractor, initDuckDB } from 'geoparquet-extractor';

// Loads the DuckDB client, workers and Wasm shipped with this package.
const client = await initDuckDB({ memoryLimitMB: 256 });

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

- **9 output formats**: GeoParquet (v1.1 & v2.0), FlatGeobuf, GeoPackage, Shapefile, CSV, GeoJSON, GeoJSONSeq, KML, DXF
- **Spatial filtering**: Bbox intersection with per-partition and per-row-group optimization
- **Pluggable metadata**: Override how partition URLs and bboxes are resolved
- **Extent visualization data**: Fetch partition/row-group bboxes as GeoJSON for map display
- **DuckDB-powered**: Packaged DuckDB WASM with OPFS spilling; custom instances remain supported
- **Self-contained GeoPackage worker**: wa-sqlite-rtree bundled into the worker — no CDN needed

## Formats

| Format | Value | Extension | Notes |
|--------|-------|-----------|-------|
| GeoPackage | `geopackage` | `.gpkg` | Requires GeoPackage worker |
| GeoJSON | `geojson` | `.geojson` | FeatureCollection |
| GeoJSONSeq | `geojsonseq` | `.geojsonl` | Newline-delimited |
| GeoParquet v1.1 | `geoparquet` | `.parquet` | With Hilbert spatial sort |
| GeoParquet v2.0 | `geoparquet2` | `.parquet` | Native geometry encoding |
| FlatGeobuf | `flatgeobuf` | `.fgb` | Packed Hilbert R-tree spatial index |
| CSV | `csv` | `.csv` | WKT geometry column |
| Shapefile | `shapefile` | `.shp` | 2 GB limit per component |
| KML | `kml` | `.kml` | XML format |
| DXF | `dxf` | `.dxf` | AutoCAD R14, UTM projection |

## DuckDB Setup

DuckDB's patched client, worker scripts and matching Wasm files are included in this package. There is no separate DuckDB package to install. `initDuckDB()` loads these assets lazily; Vite application builds copy the referenced assets into their output. For direct browser imports, serve the package's `dist/` tree intact.

```javascript
import { initDuckDB } from 'geoparquet-extractor';

const client = await initDuckDB({ memoryLimitMB: 256 });
await client.init(); // Connect and load spatial/httpfs; extractors also call this.
```

DuckDB extensions still load from DuckDB's extension server at runtime.

### Custom DuckDB Builds

The existing `initDuckDB(distributionUrl, options)` overload and `createDuckDBClient(db, options)` remain available. An externally supplied runtime must expose `registerOPFSTempDir` if you use OPFS spilling. Use the packaged API for manual initialization:

```javascript
import * as duckdb from 'geoparquet-extractor/duckdb';
import { createDuckDBClient } from 'geoparquet-extractor';

const bundle = await duckdb.selectBundle(duckdb.getLocalBundles());
const worker = new Worker(bundle.mainWorker);
const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
const client = createDuckDBClient(db, { memoryLimitMB: 256 });
```

When configuring a temp directory, the adapter explicitly registers its OPFS handle pool before setting `temp_directory`. Pool sizes count spare file handles, not memory pages or megabytes. Re-registering the same pool is safe. Reset DuckDB before replacing a pool with open files; reset removes its native file registrations, closes its handles and deletes its private scratch subdirectory. Pre-existing files in the parent directory are preserved.

The packaged runtime defaults to `useDirectIO: true` and `maximumThreads: 1` on initialization, open and reset. Direct I/O makes spill files bypass DuckDB-Wasm's native file-page cache, where repeated low-memory sorts otherwise stalled in browser regression tests. One thread also avoids reconfiguring an already-running COI thread pool during startup. These settings do not change `memory_limit`. Custom runtimes should be opened with `await db.open({ useDirectIO: true, maximumThreads: 1 })` before creating connections if they use this OPFS patch. The COI build remains experimental; multi-threaded spilling is not covered by these checks.

### Maintaining the DuckDB patch

`vendor/duckdb/upstream.json` pins upstream npm version `1.33.1-dev4.0` and its integrity. This is the nearest published upstream build after the former fork's `v1.33.0` base; its runtime source differs only in an S3 helper. `npm run build:duckdb` reconstructs original source and generated Emscripten JavaScript from the pinned package's source maps, applies `vendor/duckdb/opfs.patch`, adds `opfs_pool.js`, and rebuilds the browser client and workers. Matching upstream Wasm files are copied unchanged. The build requires Git, but does not clone the fork, compile C++, or publish another package.

Keep the upstream pin, lockfile and patch together when upgrading. Missing source inputs or a patch that no longer applies fail the build. Final extractor package metadata is embedded before bundling; `dist/duckdb/build.json` records the upstream pin and patch hash.

Run `npm test` for unit and browser integration tests, then `npm run test:package` to install the tarball in a clean consumer and verify TypeScript, Vite asset emission, real OPFS spilling in Chrome and WebKit, and MVP/EH/COI workers in Chrome. WebKit's OPFS checks use a persistent context because Playwright's private contexts do not support OPFS. Install Playwright Chromium and WebKit first (`npx playwright install chromium webkit`). To use installed Chrome locally, set `CHROME_CHANNEL=chrome`; the package check still requires Playwright WebKit. The release workflow publishes that verified tarball. Consumers do not run patching or compilation during installation.

> **Choosing `memory_limit`:** Treat browser-side defaults as heuristics. They are **not** derived from DuckDB temp-directory page or block sizes. In DuckDB, `memory_limit` controls how much data stays in memory before spilling, while `temp_directory` only controls where spill files are written. For DuckDB-Wasm, conservative defaults are usually driven by browser/WebAssembly memory limits instead.

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

### `initDuckDB(options?)` / `initDuckDB(duckdbDist, options?)`

Creates a DuckDBClient using the packaged runtime by default, or an explicit distribution URL. Options include `memoryLimitMB` and `tempDirectory`. Handles bundle selection, worker creation, and Wasm instantiation; `client.init()` connects and loads extensions.

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
- **`FlatGeobufFormatHandler`** — FlatGeobuf with packed spatial index
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
