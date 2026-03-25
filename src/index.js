// geoparquet-extractor — main entry point.
// Public API surface for the library.

// Core
export { GeoParquetExtractor } from './extractor.js';
export { ExtentData } from './extent_data.js';

// DuckDB adapter
export { createDuckDBClient, initDuckDB } from './duckdb_adapter.js';

// Metadata provider
export { MetadataProvider } from './metadata.js';

// Format handlers (for advanced usage / subclassing)
export { FormatHandler } from './formats/base.js';
export { CsvFormatHandler } from './formats/csv.js';
export { GeoJsonFormatHandler } from './formats/geojson.js';
export { GeoParquetFormatHandler } from './formats/geoparquet.js';
export { GeoPackageFormatHandler } from './formats/geopackage.js';
export { ShapefileFormatHandler } from './formats/shapefile.js';
export { KmlFormatHandler } from './formats/kml.js';
export { DxfFormatHandler } from './formats/dxf.js';

// Utilities
export {
  setProxyUrl, proxyUrl,
} from './proxy.js';
export {
  formatSize, getUtmZone, bboxUtmZone,
  getStorageEstimate,
  OPFS_PREFIX_TMPDIR,
} from './utils.js';
export { ScopedProgress } from './scoped_progress.js';
export { SizeGetter } from './size_getter.js';
export { parseWkbHex } from './wkb.js';
