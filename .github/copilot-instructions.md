# Copilot Instructions for geoparquet-extractor

## Project Overview

Browser-only library for extracting and converting spatial data from remote GeoParquet files. Uses DuckDB-WASM for SQL queries and supports 9 output formats. Requires OPFS, Web Workers, and Web Locks APIs.

## Architecture

- **`src/extractor.js`** — Main orchestrator (`GeoParquetExtractor`). Coordinates URL resolution, size estimation, format handling, and OPFS lifecycle.
- **`src/metadata.js`** — `MetadataProvider` base class. Reads GeoParquet metadata via DuckDB (`parquet_kv_metadata`, `parquet_metadata`). Designed to be subclassed for custom data sources.
- **`src/extent_data.js`** — `ExtentData` class. Fetches partition-level and row-group-level bounding boxes and metadata for map visualization.
- **`src/formats/`** — Format handlers. Each extends `FormatHandler` (in `base.js`). Shapefile/KML/DXF use `hyparquet` for streaming row-group-by-row-group reads.
- **`src/duckdb_adapter.js`** — DuckDB-WASM client adapter.
- **`src/proxy.js`** — URL proxy configuration.
- **`src/wkb.js`** — WKB geometry parsing.

## Key Patterns

- **Two metadata systems**: DuckDB queries for statistical metadata (bboxes, row group info), hyparquet for streaming reads (row-by-row processing in format handlers).
- **Row group data** is keyed as `rg_N` (e.g., `rg_0`, `rg_1`) and includes `{ bbox, num_rows, compressed_size }`.
- **Caching**: `MetadataProvider` caches bbox and row group info results in `Map` instances.
- **OPFS scoping**: Each extractor session gets a unique ID; Web Locks detect orphaned sessions for cleanup.
- **Proxy URLs**: All remote URLs go through `proxyUrl()` before DuckDB queries.

## Conventions

- ES modules (`"type": "module"` in package.json).
- No TypeScript — plain JS with JSDoc type annotations and `@typedef` for complex types.
- Private methods/fields prefixed with `_`.
- Tests use Vitest (`npm test`). Test files mirror source structure under `test/`.
- Build with Vite (`npm run build`). Separate worker build for GeoPackage.
- Lint with ESLint (`npm run lint`).

## Dependencies

- **Runtime**: `hyparquet`, `hyparquet-compressors`, `wa-sqlite-rtree`
- **Peer**: `duckdb-wasm-opfs-tempdir` (optional — consumer provides DuckDB)
- **Dev**: `vite`, `vitest`, `typescript` (for declaration generation only), `jsdom`

## When Making Changes

- Run `npm test` to validate — all tests should pass.
- Update JSDoc types when changing data structures.
- Update `README.md` API docs if public interfaces change.
- Format handlers that stream via hyparquet (Shapefile, KML, DXF) iterate `metadata.row_groups` and use `num_rows` for row offset calculations — be careful changing row group structures there.
