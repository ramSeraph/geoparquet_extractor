// Shared helpers for streaming format handlers (KML, DXF, Shapefile).
// These handlers read row-group-by-row-group via hyparquet and share
// identical column-index-building and property-normalization logic.

import { buildNormalizer } from '../normalizer.js';

/**
 * Build a column lookup from a hyparquet schema, including normalizers.
 * @param {{ children: Array<{ element: { name: string } }> }} hSchema
 * @param {Array<{ originalName: string }>} attrColumns
 * @returns {{ colIndex: Record<string, number>, iWkb: number, attrIndices: number[], attrNorms: Function[] }}
 */
export function buildColumnLookup(hSchema, attrColumns) {
  const colIndex = {};
  hSchema.children.forEach((child, i) => { colIndex[child.element.name] = i; });
  return {
    colIndex,
    iWkb: colIndex['geom_wkb'],
    attrIndices: attrColumns.map(c => colIndex[c.originalName]),
    attrNorms: attrColumns.map(c => buildNormalizer(hSchema.children[colIndex[c.originalName]])),
  };
}

/**
 * Build a properties object from a row using pre-computed column indices.
 * @param {any[]} row
 * @param {number[]} attrIndices
 * @param {Function[]} attrNorms
 * @param {Array<{ originalName: string }>} fieldNames - Objects with .originalName for property keys
 * @returns {Record<string, any>}
 */
export function buildRowProperties(row, attrIndices, attrNorms, fieldNames) {
  const props = {};
  for (let ci = 0; ci < attrIndices.length; ci++) {
    const val = attrNorms[ci](row[attrIndices[ci]]);
    props[fieldNames[ci].originalName] = val != null && typeof val === 'object'
      ? JSON.stringify(val) : val;
  }
  return props;
}
