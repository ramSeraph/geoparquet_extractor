// geoparquet-extractor — main entry point.
// Public API surface for the library.

// Core
export { GeoParquetExtractor } from './extractor.js';
export { ExtentData } from './extent_data.js';

// DuckDB adapter
export { createDuckDBClient } from './duckdb_adapter.js';

// Metadata providers
export { MetadataProvider } from './metadata/provider.js';
export { DefaultMetadataProvider } from './metadata/default.js';

// Format handlers (for advanced usage / subclassing)
export { FormatHandler } from './formats/base.js';
export { CsvFormatHandler } from './formats/csv.js';
export { GeoJsonFormatHandler } from './formats/geojson.js';
export { GeoParquetFormatHandler } from './formats/geoparquet.js';
export { GeoPackageFormatHandler } from './formats/geopackage.js';
export { ShapefileFormatHandler } from './formats/shapefile.js';
export { KmlFormatHandler } from './formats/kml.js';
export { DxfFormatHandler } from './formats/dxf.js';

// Utilities (for advanced usage)
export {
  setProxyUrl, proxyUrl,
  formatSize, ScopedProgress,
  parseWkbHex, getUtmZone, bboxUtmZone,
  OPFS_PREFIX_TMPDIR,
} from './utils.js';
