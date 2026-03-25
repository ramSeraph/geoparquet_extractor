// WKB hex → GeoJSON-style geometry objects
// Only XY coordinates are extracted; Z and M dimensions are present in some WKB
// variants (PointZ=1001, PointM=2001, PointZM=3001, etc.) but are skipped during
// reading since downstream formats (Shapefile, KML, DXF) only use 2D coordinates.

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2)
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  return bytes;
}

function readCoord(view, offset, le, coordSize) {
  const x = view.getFloat64(offset, le);
  const y = view.getFloat64(offset + 8, le);
  return { coord: [x, y], offset: offset + coordSize * 8 };
}

function readPoint(view, offset, le, coordSize) {
  const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
  const empty = isNaN(coord[0]) && isNaN(coord[1]);
  return { geom: { type: 'Point', coordinates: empty ? [] : coord }, offset: newOffset };
}

function readLineString(view, offset, le, coordSize) {
  const numPoints = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numPoints; i++) {
    const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
    coords.push(coord);
    offset = newOffset;
  }
  return { geom: { type: 'LineString', coordinates: coords }, offset };
}

function readPolygon(view, offset, le, coordSize) {
  const numRings = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const rings = [];
  for (let r = 0; r < numRings; r++) {
    const numPoints = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
    offset += 4;
    const ring = [];
    for (let i = 0; i < numPoints; i++) {
      const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
      ring.push(coord);
      offset = newOffset;
    }
    rings.push(ring);
  }
  return { geom: { type: 'Polygon', coordinates: rings }, offset };
}

function readMultiPoint(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiPoint', coordinates: coords }, offset };
}

function readMultiLineString(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiLineString', coordinates: coords }, offset };
}

function readMultiPolygon(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiPolygon', coordinates: coords }, offset };
}

function readGeometryCollection(view, offset, le) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const geometries = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    geometries.push(geom);
    offset = newOffset;
  }
  return { geom: { type: 'GeometryCollection', geometries }, offset };
}

function readGeometry(view, offset) {
  const le = view.getUint8(offset) === 1;
  offset += 1;
  const rawType = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;

  // ISO WKB type codes: base + 1000 (Z), + 2000 (M), + 3000 (ZM)
  const baseType = rawType % 1000;
  const hasZ = (rawType >= 1000 && rawType < 2000) || rawType >= 3000;
  const hasM = rawType >= 2000;
  const coordSize = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

  switch (baseType) {
    case 1: return readPoint(view, offset, le, coordSize);
    case 2: return readLineString(view, offset, le, coordSize);
    case 3: return readPolygon(view, offset, le, coordSize);
    case 4: return readMultiPoint(view, offset, le, coordSize);
    case 5: return readMultiLineString(view, offset, le, coordSize);
    case 6: return readMultiPolygon(view, offset, le, coordSize);
    case 7: return readGeometryCollection(view, offset, le);
    default: throw new Error('Unsupported WKB geometry type: ' + rawType);
  }
}

/**
 * Parse a WKB geometry (as hex string) into { type, coordinates }.
 * Supports Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
 * and GeometryCollection. Z and M coordinates are skipped (only XY output).
 * @param {string} hex
 * @returns {{ type: string, coordinates: any }}
 */
export function parseWkbHex(hex) {
  const bytes = hexToBytes(hex);
  const view = new DataView(bytes.buffer);
  const result = readGeometry(view, 0);
  return result.geom;
}
