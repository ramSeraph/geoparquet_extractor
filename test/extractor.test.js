import { describe, it, expect } from 'vitest';
import { GeoParquetExtractor } from '../src/extractor.js';

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
