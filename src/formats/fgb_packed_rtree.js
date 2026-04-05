import * as flatbuffers from 'flatbuffers';

import { Column } from 'flatgeobuf/lib/mjs/flat-geobuf/column.js';
import { Crs } from 'flatgeobuf/lib/mjs/flat-geobuf/crs.js';
import { Header } from 'flatgeobuf/lib/mjs/flat-geobuf/header.js';

export const FGB_NODE_ITEM_BYTE_LEN = 40;
export const FGB_DEFAULT_INDEX_NODE_SIZE = 16;

const FGB_MIN_NODE_SIZE = 2;
const FGB_MAX_NODE_SIZE = 65535;
const NODE_ITEM_BATCH_SIZE = 1024;

function clampNodeSize(nodeSize) {
  return Math.min(Math.max(Number(nodeSize) || FGB_DEFAULT_INDEX_NODE_SIZE, FGB_MIN_NODE_SIZE), FGB_MAX_NODE_SIZE);
}

function toDataView(bytes) {
  return bytes instanceof DataView
    ? bytes
    : new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

/**
 * Create an empty FlatGeobuf node item / bbox accumulator.
 * @param {number} [offset=0]
 * @returns {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }}
 */
export function createNodeItem(offset = 0) {
  return {
    minX: Infinity,
    minY: Infinity,
    maxX: -Infinity,
    maxY: -Infinity,
    offset,
  };
}

/**
 * Expand a node item to include another node item / bbox.
 * @param {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }} target
 * @param {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }} source
 * @returns {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }}
 */
export function expandNodeItem(target, source) {
  if (source.minX < target.minX) target.minX = source.minX;
  if (source.minY < target.minY) target.minY = source.minY;
  if (source.maxX > target.maxX) target.maxX = source.maxX;
  if (source.maxY > target.maxY) target.maxY = source.maxY;
  return target;
}

/**
 * Encode a node item into the FlatGeobuf packed R-tree binary layout.
 * @param {Uint8Array|DataView} target
 * @param {number} byteOffset
 * @param {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }} nodeItem
 */
export function writeNodeItem(target, byteOffset, nodeItem) {
  const view = toDataView(target);
  view.setFloat64(byteOffset + 0, nodeItem.minX, true);
  view.setFloat64(byteOffset + 8, nodeItem.minY, true);
  view.setFloat64(byteOffset + 16, nodeItem.maxX, true);
  view.setFloat64(byteOffset + 24, nodeItem.maxY, true);
  view.setBigUint64(byteOffset + 32, BigInt(nodeItem.offset), true);
}

/**
 * Decode a node item from the FlatGeobuf packed R-tree binary layout.
 * @param {Uint8Array|DataView} source
 * @param {number} [byteOffset=0]
 * @returns {{ minX: number, minY: number, maxX: number, maxY: number, offset: number }}
 */
export function readNodeItem(source, byteOffset = 0) {
  const view = toDataView(source);
  return {
    minX: view.getFloat64(byteOffset + 0, true),
    minY: view.getFloat64(byteOffset + 8, true),
    maxX: view.getFloat64(byteOffset + 16, true),
    maxY: view.getFloat64(byteOffset + 24, true),
    offset: Number(view.getBigUint64(byteOffset + 32, true)),
  };
}

/**
 * Calculate the total packed R-tree size in bytes for a feature count.
 * Returns 0 for empty datasets.
 * @param {number} numItems
 * @param {number} [nodeSize=FGB_DEFAULT_INDEX_NODE_SIZE]
 * @returns {number}
 */
export function calcTreeSize(numItems, nodeSize = FGB_DEFAULT_INDEX_NODE_SIZE) {
  if (numItems <= 0) return 0;
  const width = clampNodeSize(nodeSize);
  let levelCount = numItems;
  let totalCount = levelCount;
  do {
    levelCount = Math.ceil(levelCount / width);
    totalCount += levelCount;
  } while (levelCount !== 1);
  return totalCount * FGB_NODE_ITEM_BYTE_LEN;
}

/**
 * Generate packed R-tree level bounds in the FlatGeobuf / flatbush layout.
 * The returned array is bottom-up: index 0 is the leaf level, last is the root.
 * Returns [] for empty datasets.
 * @param {number} numItems
 * @param {number} [nodeSize=FGB_DEFAULT_INDEX_NODE_SIZE]
 * @returns {Array<[number, number]>}
 */
export function generateLevelBounds(numItems, nodeSize = FGB_DEFAULT_INDEX_NODE_SIZE) {
  if (numItems <= 0) return [];

  const width = clampNodeSize(nodeSize);
  const levelSizes = [numItems];
  let levelCount = numItems;
  let totalCount = levelCount;

  do {
    levelCount = Math.ceil(levelCount / width);
    totalCount += levelCount;
    levelSizes.push(levelCount);
  } while (levelCount !== 1);

  const levelOffsets = [];
  let offset = totalCount;
  for (const size of levelSizes) {
    offset -= size;
    levelOffsets.push(offset);
  }

  return levelSizes.map((size, i) => [levelOffsets[i], levelOffsets[i] + size]);
}

function levelSize(levelBounds, levelIndex) {
  return levelBounds[levelIndex][1] - levelBounds[levelIndex][0];
}

async function writeNodeItemBatch(writer, batch, count) {
  if (!count) return;
  await writer.write(batch.subarray(0, count * FGB_NODE_ITEM_BYTE_LEN));
}

async function buildParentLevel({
  childFile,
  childCount,
  childLevelStart,
  nodeSize,
  writer,
}) {
  const reader = childFile.stream().getReader();
  const batch = new Uint8Array(NODE_ITEM_BATCH_SIZE * FGB_NODE_ITEM_BYTE_LEN);
  let batchCount = 0;
  let leftover = new Uint8Array(0);
  let processedChildren = 0;
  let groupCount = 0;
  let parentCount = 0;
  /** @type {{ minX: number, minY: number, maxX: number, maxY: number, offset: number } | null} */
  let parentNode = null;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      let chunk = value;
      if (leftover.length) {
        const merged = new Uint8Array(leftover.length + chunk.length);
        merged.set(leftover);
        merged.set(chunk, leftover.length);
        chunk = merged;
        leftover = new Uint8Array(0);
      }

      const usableBytes = chunk.length - (chunk.length % FGB_NODE_ITEM_BYTE_LEN);
      if (usableBytes !== chunk.length) {
        leftover = chunk.slice(usableBytes);
        chunk = chunk.subarray(0, usableBytes);
      }

      const view = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength);
      for (let byteOffset = 0; byteOffset < chunk.byteLength; byteOffset += FGB_NODE_ITEM_BYTE_LEN) {
        if (processedChildren >= childCount) {
          break;
        }

        if (groupCount === 0) {
          parentNode = createNodeItem(childLevelStart + processedChildren);
        }

        expandNodeItem(parentNode, readNodeItem(view, byteOffset));
        groupCount++;
        processedChildren++;

        if (groupCount === nodeSize) {
          writeNodeItem(batch, batchCount * FGB_NODE_ITEM_BYTE_LEN, parentNode);
          batchCount++;
          parentCount++;
          groupCount = 0;
          parentNode = null;

          if (batchCount === NODE_ITEM_BATCH_SIZE) {
            await writeNodeItemBatch(writer, batch, batchCount);
            batchCount = 0;
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  if (leftover.length) {
    throw new Error('Packed R-tree scratch file ended mid-record');
  }
  if (processedChildren !== childCount) {
    throw new Error(`Packed R-tree scratch file had ${processedChildren} child nodes, expected ${childCount}`);
  }

  if (groupCount > 0) {
    writeNodeItem(batch, batchCount * FGB_NODE_ITEM_BYTE_LEN, parentNode);
    batchCount++;
    parentCount++;
  }

  await writeNodeItemBatch(writer, batch, batchCount);
  return parentCount;
}

/**
 * Build all packed R-tree levels above an existing leaf-node scratch file.
 * Returns the scratch files bottom-up: [leaves, parents, ..., root].
 * @param {Object} opts
 * @param {number} opts.numItems
 * @param {string} opts.leafFileName
 * @param {Function} opts.createScratchFile - async (levelIndex) => { name, writer }
 * @param {Function} opts.getScratchFile - async (name) => File
 * @param {number} [opts.nodeSize=FGB_DEFAULT_INDEX_NODE_SIZE]
 * @param {(progress: number) => void} [opts.onProgress]
 * @returns {Promise<{ levelFiles: string[], levelBounds: Array<[number, number]>, treeByteSize: number }>}
 */
export async function buildPackedRTreeLevels({
  numItems,
  leafFileName,
  createScratchFile,
  getScratchFile,
  nodeSize = FGB_DEFAULT_INDEX_NODE_SIZE,
  onProgress,
}) {
  if (numItems <= 0) {
    return { levelFiles: [], levelBounds: [], treeByteSize: 0 };
  }

  const width = clampNodeSize(nodeSize);
  const levelBounds = generateLevelBounds(numItems, width);
  const levelFiles = [leafFileName];

  if (levelBounds.length === 1) {
    onProgress?.(100);
    return {
      levelFiles,
      levelBounds,
      treeByteSize: calcTreeSize(numItems, width),
    };
  }

  for (let levelIndex = 1; levelIndex < levelBounds.length; levelIndex++) {
    const childLevelIndex = levelIndex - 1;
    const childFile = await getScratchFile(levelFiles[childLevelIndex]);
    const { name, writer } = await createScratchFile(levelIndex);

    try {
      const parentCount = await buildParentLevel({
        childFile,
        childCount: levelSize(levelBounds, childLevelIndex),
        childLevelStart: levelBounds[childLevelIndex][0],
        nodeSize: width,
        writer,
      });
      if (parentCount !== levelSize(levelBounds, levelIndex)) {
        throw new Error(`Packed R-tree parent level ${levelIndex} wrote ${parentCount} nodes, expected ${levelSize(levelBounds, levelIndex)}`);
      }
    } finally {
      await writer.close();
    }

    levelFiles.push(name);
    onProgress?.((levelIndex / (levelBounds.length - 1)) * 100);
  }

  return {
    levelFiles,
    levelBounds,
    treeByteSize: calcTreeSize(numItems, width),
  };
}

/**
 * Build a FlatGeobuf header that can include an envelope and non-zero index node size.
 * @param {import('flatgeobuf/lib/mjs/header-meta.js').HeaderMeta} headerMeta
 * @param {number} [crsCode=0]
 * @returns {Uint8Array}
 */
export function buildFgbHeader(headerMeta, crsCode = 0) {
  const builder = new flatbuffers.Builder();

  let columnsOffset = 0;
  if (headerMeta.columns?.length) {
    const columnOffsets = headerMeta.columns.map((column) => {
      const nameOffset = builder.createString(column.name);
      Column.startColumn(builder);
      Column.addName(builder, nameOffset);
      Column.addType(builder, column.type);
      return Column.endColumn(builder);
    });
    columnsOffset = Header.createColumnsVector(builder, columnOffsets);
  }

  let envelopeOffset = 0;
  if (headerMeta.envelope?.length) {
    envelopeOffset = Header.createEnvelopeVector(builder, headerMeta.envelope);
  }

  let crsOffset = 0;
  if (crsCode) {
    Crs.startCrs(builder);
    Crs.addCode(builder, crsCode);
    crsOffset = Crs.endCrs(builder);
  }

  const nameOffset = builder.createString('L1');

  Header.startHeader(builder);
  Header.addName(builder, nameOffset);
  if (envelopeOffset) Header.addEnvelope(builder, envelopeOffset);
  Header.addGeometryType(builder, headerMeta.geometryType);
  Header.addFeaturesCount(builder, BigInt(headerMeta.featuresCount));
  Header.addIndexNodeSize(builder, headerMeta.indexNodeSize ?? 0);
  if (columnsOffset) Header.addColumns(builder, columnsOffset);
  if (crsOffset) Header.addCrs(builder, crsOffset);
  const headerOffset = Header.endHeader(builder);
  Header.finishSizePrefixedHeaderBuffer(builder, headerOffset);
  return builder.asUint8Array();
}

/**
 * Copy a Blob/File to a writable stream without buffering the whole payload in JS.
 * @param {Blob} blob
 * @param {{ write: (chunk: Uint8Array) => Promise<void> }} writer
 * @param {(bytesWritten: number) => void} [onProgress]
 */
export async function copyBlobToWriter(blob, writer, onProgress) {
  const reader = blob.stream().getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      await writer.write(value);
      onProgress?.(value.byteLength);
    }
  } finally {
    reader.releaseLock();
  }
}
