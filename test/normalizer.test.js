import { describe, it, expect } from 'vitest';
import { buildNormalizer } from '../src/normalizer.js';

// Helper to build minimal schema tree nodes matching hyparquet's SchemaTree shape

function leaf(name, opts = {}) {
  return {
    element: { name, repetition_type: 'OPTIONAL', ...opts },
    children: [],
  };
}

function listNode(name, itemNode) {
  return {
    element: { name, converted_type: 'LIST', repetition_type: 'OPTIONAL' },
    children: [{
      element: { name: 'list', repetition_type: 'REPEATED' },
      children: [itemNode],
    }],
  };
}

function mapNode(name, valueNode) {
  return {
    element: { name, converted_type: 'MAP', repetition_type: 'OPTIONAL' },
    children: [{
      element: { name: 'key_value', repetition_type: 'REPEATED' },
      children: [
        leaf('key'),
        { ...valueNode, element: { ...valueNode.element, name: 'value' } },
      ],
    }],
  };
}

function structNode(name, children) {
  return {
    element: { name, repetition_type: 'OPTIONAL' },
    children,
  };
}

describe('buildNormalizer', () => {
  describe('scalar columns', () => {
    it('converts BigInt to Number', () => {
      const norm = buildNormalizer(leaf('id'));
      expect(norm(42n)).toBe(42);
    });

    it('passes through regular values', () => {
      const norm = buildNormalizer(leaf('name'));
      expect(norm('hello')).toBe('hello');
      expect(norm(3.14)).toBe(3.14);
      expect(norm(null)).toBe(null);
      expect(norm(undefined)).toBe(undefined);
    });
  });

  describe('LIST columns', () => {
    it('normalizes empty {} to []', () => {
      const norm = buildNormalizer(listNode('tags', leaf('element')));
      expect(norm({})).toEqual([]);
    });

    it('passes through normal arrays', () => {
      const norm = buildNormalizer(listNode('tags', leaf('element')));
      expect(norm(['a', 'b'])).toEqual(['a', 'b']);
    });

    it('converts BigInts inside arrays', () => {
      const norm = buildNormalizer(listNode('ids', leaf('element')));
      expect(norm([1n, 2n, 3n])).toEqual([1, 2, 3]);
    });

    it('returns null for null', () => {
      const norm = buildNormalizer(listNode('tags', leaf('element')));
      expect(norm(null)).toBe(null);
    });

    it('handles nested list of structs', () => {
      const innerStruct = structNode('element', [
        leaf('value'),
        leaf('language'),
      ]);
      const norm = buildNormalizer(listNode('names', innerStruct));

      expect(norm({})).toEqual([]);
      expect(norm([{ value: 'NYC', language: 'en' }])).toEqual([{ value: 'NYC', language: 'en' }]);
    });
  });

  describe('MAP columns', () => {
    it('preserves empty {} for empty maps', () => {
      const norm = buildNormalizer(mapNode('props', leaf('value')));
      expect(norm({})).toEqual({});
    });

    it('normalizes map values', () => {
      const norm = buildNormalizer(mapNode('counts', leaf('value')));
      expect(norm({ a: 1n, b: 2n })).toEqual({ a: 1, b: 2 });
    });

    it('returns null for null', () => {
      const norm = buildNormalizer(mapNode('props', leaf('value')));
      expect(norm(null)).toBe(null);
    });

    it('handles map with LIST values', () => {
      const norm = buildNormalizer(mapNode('tag_lists', listNode('value', leaf('element'))));
      expect(norm({ colors: ['red', 'blue'] })).toEqual({ colors: ['red', 'blue'] });
      expect(norm({ colors: {} })).toEqual({ colors: [] });
    });
  });

  describe('STRUCT columns', () => {
    it('normalizes nested LIST fields (the tags.short_names case)', () => {
      const schema = structNode('tags', [
        leaf('primary'),
        listNode('short_names', leaf('element')),
        listNode('long_names', leaf('element')),
      ]);
      const norm = buildNormalizer(schema);

      const result = norm({
        primary: 'Main St',
        short_names: {},
        long_names: ['Long Street'],
      });
      expect(result).toEqual({
        primary: 'Main St',
        short_names: [],
        long_names: ['Long Street'],
      });
    });

    it('converts BigInts in struct fields', () => {
      const schema = structNode('stats', [
        leaf('count'),
        leaf('total'),
      ]);
      const norm = buildNormalizer(schema);
      expect(norm({ count: 10n, total: 999n })).toEqual({ count: 10, total: 999 });
    });

    it('returns null for null struct', () => {
      const schema = structNode('tags', [leaf('primary')]);
      const norm = buildNormalizer(schema);
      expect(norm(null)).toBe(null);
    });

    it('handles deeply nested structs with lists and maps', () => {
      const schema = structNode('root', [
        structNode('names', [
          leaf('primary'),
          listNode('common', leaf('element')),
        ]),
        mapNode('properties', leaf('value')),
      ]);
      const norm = buildNormalizer(schema);

      const result = norm({
        names: { primary: 'Test', common: {} },
        properties: { key1: 'val1' },
      });
      expect(result).toEqual({
        names: { primary: 'Test', common: [] },
        properties: { key1: 'val1' },
      });
    });
  });

  describe('logical_type fallback', () => {
    it('detects LIST via logical_type when converted_type is absent', () => {
      const schema = {
        element: { name: 'items', logical_type: { type: 'LIST' }, repetition_type: 'OPTIONAL' },
        children: [{
          element: { name: 'list', repetition_type: 'REPEATED' },
          children: [leaf('element')],
        }],
      };
      const norm = buildNormalizer(schema);
      expect(norm({})).toEqual([]);
      expect(norm(['a'])).toEqual(['a']);
    });

    it('detects MAP via logical_type when converted_type is absent', () => {
      const schema = {
        element: { name: 'kv', logical_type: { type: 'MAP' }, repetition_type: 'OPTIONAL' },
        children: [{
          element: { name: 'key_value', repetition_type: 'REPEATED' },
          children: [leaf('key'), { ...leaf('value'), element: { ...leaf('value').element, name: 'value' } }],
        }],
      };
      const norm = buildNormalizer(schema);
      expect(norm({})).toEqual({});
      expect(norm({ a: 1n })).toEqual({ a: 1 });
    });
  });
});
