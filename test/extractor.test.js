import { describe, it, expect } from 'vitest';
import { GeoParquetExtractor, FORMAT_OPTIONS, getDefaultMemoryLimitMB, MEMORY_STEP, MEMORY_MIN_MB } from '../src/extractor.js';

describe('FORMAT_OPTIONS', () => {
  it('contains 9 format options', () => {
    expect(FORMAT_OPTIONS).toHaveLength(9);
  });

  it('each has value and label', () => {
    for (const opt of FORMAT_OPTIONS) {
      expect(opt.value).toBeTruthy();
      expect(opt.label).toBeTruthy();
    }
  });

  it('includes all expected formats', () => {
    const values = FORMAT_OPTIONS.map(o => o.value);
    expect(values).toContain('geopackage');
    expect(values).toContain('geojson');
    expect(values).toContain('geojsonseq');
    expect(values).toContain('geoparquet');
    expect(values).toContain('geoparquet2');
    expect(values).toContain('csv');
    expect(values).toContain('shapefile');
    expect(values).toContain('kml');
    expect(values).toContain('dxf');
  });
});

describe('GeoParquetExtractor', () => {
  it('requires duckdb in constructor', () => {
    expect(() => new GeoParquetExtractor({})).toThrow('duckdb is required');
  });

  it('getDownloadBaseName generates correct filename', () => {
    const baseName = GeoParquetExtractor.getDownloadBaseName('My Source', [77.5, 12.9, 77.7, 13.1]);
    expect(baseName).toContain('My_Source');
    expect(baseName).toContain('77-5000');
  });
});

describe('memory helpers', () => {
  it('MEMORY_STEP is 128', () => {
    expect(MEMORY_STEP).toBe(128);
  });

  it('MEMORY_MIN_MB is 512', () => {
    expect(MEMORY_MIN_MB).toBe(512);
  });

  it('getDefaultMemoryLimitMB returns at least MIN', () => {
    const limit = getDefaultMemoryLimitMB();
    expect(limit).toBeGreaterThanOrEqual(MEMORY_MIN_MB);
  });
});
