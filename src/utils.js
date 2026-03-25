/**
 * Format byte count as human-readable string.
 * @param {number} bytes
 * @returns {string}
 */
export function formatSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// OPFS file/dir name prefixes — each followed by session ID.
// Longest prefixes first so extractSessionId matches greedily.
const OPFS_PREFIXES = [];
function opfsPrefix(value) {
  OPFS_PREFIXES.push(value);
  return value;
}

export const OPFS_PREFIX_GPKG_TMP = opfsPrefix('dl_gpkg_tmp_');
export const OPFS_PREFIX_GPKG = opfsPrefix('dl_gpkg_');
export const OPFS_PREFIX_OUTPUT = opfsPrefix('dl_output_');
export const OPFS_PREFIX_TMP = opfsPrefix('dl_tmp_');
export const OPFS_PREFIX_SHP_TMP = opfsPrefix('dl_shp_tmp_');
export const OPFS_PREFIX_KML_TMP = opfsPrefix('dl_kml_tmp_');
export const OPFS_PREFIX_DXF_TMP = opfsPrefix('dl_dxf_tmp_');
export const OPFS_PREFIX_TMPDIR = opfsPrefix('tmpdir_');

/**
 * Get the ordered list of all OPFS prefixes (longest first).
 * @returns {string[]}
 */
export function getOpfsPrefixes() {
  return OPFS_PREFIXES;
}

/** Return the UTM zone number (1–60) for a given longitude. */
export function getUtmZone(lon) {
  return Math.floor((lon + 180) / 6) + 1;
}

/**
 * Check whether a bbox fits in a single UTM zone.
 * @param {{ west: number, south: number, east: number, north: number }} bbox
 * @returns {{ zone: number, hemisphere: string } | null} null if it spans multiple zones.
 */
export function bboxUtmZone(bbox) {
  const westZone = getUtmZone(bbox.west);
  const eastZone = getUtmZone(bbox.east);
  if (westZone !== eastZone) return null;
  const hemisphere = ((bbox.south + bbox.north) / 2) >= 0 ? 'N' : 'S';
  return { zone: westZone, hemisphere };
}

/**
 * Get browser storage estimate.
 * @returns {Promise<{ usage: number | undefined, quota: number | undefined }>}
 */
export async function getStorageEstimate() {
  const { usage, quota } = await navigator.storage.estimate();
  return { usage, quota };
}

/**
 * Wrap an OPFS File handle into an async buffer interface for hyparquet.
 * @param {File} file
 * @returns {{ byteLength: number, slice: (start: number, end: number) => Promise<ArrayBuffer> }}
 */
export function fileToAsyncBuffer(file) {
  return {
    byteLength: file.size,
    slice(start, end) { return file.slice(start, end).arrayBuffer(); }
  };
}


