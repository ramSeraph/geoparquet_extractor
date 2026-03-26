/**
 * Schema-aware value normalizer for hyparquet row data.
 *
 * Builds a per-column normalizer from the hyparquet SchemaTree that:
 * - Converts BigInt → Number (SQLite / JSON compatibility)
 * - Converts empty {} → [] for LIST columns at any nesting depth
 *   (hyparquet's assembleMaps returns {} for empty lists-within-structs)
 * - Recursively normalizes MAP values and STRUCT fields
 */

/**
 * @param {*} val
 * @returns {*}
 */
function normalizeScalar(val) {
  if (typeof val === 'bigint') return Number(val);
  return val;
}

/** @param {*} val */
function isEmptyObject(val) {
  return val !== null && typeof val === 'object' && !Array.isArray(val) && Object.keys(val).length === 0;
}

/** @param {{ element: { converted_type?: string, logical_type?: { type?: string } } }} node */
function isListNode(node) {
  const ct = node.element.converted_type;
  const lt = node.element.logical_type?.type;
  return ct === 'LIST' || lt === 'LIST';
}

/** @param {{ element: { converted_type?: string, logical_type?: { type?: string } } }} node */
function isMapNode(node) {
  const ct = node.element.converted_type;
  const lt = node.element.logical_type?.type;
  return ct === 'MAP' || lt === 'MAP';
}

/**
 * Build a normalizer function for a hyparquet SchemaTree node.
 * The returned function converts a single row value for that column.
 *
 * @param {{ element: object, children?: Array }} schemaNode - hyparquet SchemaTree node
 * @returns {(val: any) => any}
 */
export function buildNormalizer(schemaNode) {
  if (isListNode(schemaNode)) {
    // LIST schema: node → repeated-group → element
    const elementChild = schemaNode.children?.[0]?.children?.[0];
    const itemNorm = elementChild?.children?.length
      ? buildNormalizer(elementChild)
      : normalizeScalar;
    return (val) => {
      if (val == null) return null;
      if (isEmptyObject(val)) return [];
      if (Array.isArray(val)) return val.map(itemNorm);
      return val;
    };
  }

  if (isMapNode(schemaNode)) {
    // MAP schema: node → key_value (repeated) → key, value
    const kvChild = schemaNode.children?.[0];
    const valueChild = kvChild?.children?.find(c => c.element.name === 'value');
    const valueNorm = valueChild?.children?.length
      ? buildNormalizer(valueChild)
      : normalizeScalar;
    return (val) => {
      if (val == null) return null;
      if (typeof val !== 'object' || Array.isArray(val)) return val;
      const out = {};
      for (const [k, v] of Object.entries(val)) out[k] = valueNorm(v);
      return out;
    };
  }

  // STRUCT: has children but not LIST/MAP
  if (schemaNode.children?.length > 0) {
    /** @type {Record<string, (val: any) => any>} */
    const childNorms = {};
    for (const child of schemaNode.children) {
      childNorms[child.element.name] = buildNormalizer(child);
    }
    return (val) => {
      if (val == null) return null;
      if (typeof val !== 'object' || Array.isArray(val)) return normalizeScalar(val);
      const out = {};
      for (const [k, v] of Object.entries(val)) {
        out[k] = childNorms[k] ? childNorms[k](v) : normalizeScalar(v);
      }
      return out;
    };
  }

  // Leaf / scalar
  return normalizeScalar;
}
