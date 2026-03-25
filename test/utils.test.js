import { describe, it, expect } from 'vitest';
import {
  formatSize,
  getUtmZone, bboxUtmZone,
  getOpfsPrefixes, OPFS_PREFIX_TMPDIR,
} from '../src/utils.js';
import { ScopedProgress } from '../src/scoped_progress.js';
import { parseWkbHex } from '../src/wkb.js';

describe('formatSize', () => {
  it('formats bytes', () => {
    expect(formatSize(0)).toBe('0 B');
    expect(formatSize(500)).toBe('500 B');
  });

  it('formats KB', () => {
    expect(formatSize(1024)).toBe('1 KB');
    expect(formatSize(1536)).toBe('1.5 KB');
  });

  it('formats MB', () => {
    expect(formatSize(1048576)).toBe('1 MB');
  });

  it('formats GB', () => {
    expect(formatSize(1073741824)).toBe('1 GB');
  });
});

describe('ScopedProgress', () => {
  it('maps 0-100 to a sub-range', () => {
    const values = [];
    const sp = new ScopedProgress((v) => values.push(v), 20, 80);

    sp.report(0);
    sp.report(50);
    sp.report(100);

    expect(values).toEqual([20, 50, 80]);
  });

  it('works with callback property', () => {
    const values = [];
    const sp = new ScopedProgress((v) => values.push(v), 0, 100);
    sp.callback(50);
    expect(values).toEqual([50]);
  });

  it('handles null callback gracefully', () => {
    const sp = new ScopedProgress(null, 0, 100);
    expect(() => sp.report(50)).not.toThrow();
  });
});

describe('parseWkbHex', () => {
  it('parses a WKB Point (little-endian)', () => {
    // Point(1.0, 2.0) in WKB little-endian hex
    const hex = '0101000000000000000000F03F0000000000000040';
    const geom = parseWkbHex(hex);
    expect(geom.type).toBe('Point');
    expect(geom.coordinates[0]).toBeCloseTo(1.0);
    expect(geom.coordinates[1]).toBeCloseTo(2.0);
  });

  it('parses a WKB Point (big-endian)', () => {
    // Point(1.0, 2.0) in WKB big-endian hex
    const hex = '00000000013FF00000000000004000000000000000';
    const geom = parseWkbHex(hex);
    expect(geom.type).toBe('Point');
    expect(geom.coordinates[0]).toBeCloseTo(1.0);
    expect(geom.coordinates[1]).toBeCloseTo(2.0);
  });
});

describe('getUtmZone', () => {
  it('returns correct zone for Delhi (77.2°E)', () => {
    expect(getUtmZone(77.2)).toBe(43);
  });

  it('returns correct zone for London (0°)', () => {
    expect(getUtmZone(0)).toBe(31);
  });
});

describe('bboxUtmZone', () => {
  it('returns zone for bbox within single UTM zone', () => {
    const result = bboxUtmZone({ west: 77.0, south: 12.0, east: 77.5, north: 13.0 });
    expect(result).not.toBeNull();
    expect(result.zone).toBe(43);
    expect(result.hemisphere).toBe('N');
  });

  it('returns null for bbox spanning multiple UTM zones', () => {
    const result = bboxUtmZone({ west: 74.0, south: 12.0, east: 84.0, north: 13.0 });
    expect(result).toBeNull();
  });

  it('detects southern hemisphere', () => {
    const result = bboxUtmZone({ west: 77.0, south: -2.0, east: 77.5, north: -1.0 });
    expect(result).not.toBeNull();
    expect(result.hemisphere).toBe('S');
  });
});

describe('getOpfsPrefixes', () => {
  it('returns an array of prefix strings', () => {
    const prefixes = getOpfsPrefixes();
    expect(Array.isArray(prefixes)).toBe(true);
    expect(prefixes.length).toBeGreaterThan(0);
  });

  it('includes the tmpdir prefix', () => {
    const prefixes = getOpfsPrefixes();
    expect(prefixes).toContain(OPFS_PREFIX_TMPDIR);
  });
});
