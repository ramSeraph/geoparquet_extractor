import { describe, it, expect } from 'vitest';
import { MetadataProvider } from '../src/metadata.js';

describe('MetadataProvider', () => {
  it('getParquetUrl returns sourceUrl as-is by default', () => {
    const mp = new MetadataProvider();
    expect(mp.getParquetUrl('https://example.com/data.parquet'))
      .toBe('https://example.com/data.parquet');
  });

  it('getParquetUrl preserves non-parquet URLs (override needed)', () => {
    const mp = new MetadataProvider();
    expect(mp.getParquetUrl('https://example.com/data.pmtiles'))
      .toBe('https://example.com/data.pmtiles');
  });

  it('getBaseUrl returns directory portion', () => {
    const mp = new MetadataProvider();
    expect(mp.getBaseUrl('https://example.com/path/to/data.parquet'))
      .toBe('https://example.com/path/to/');
  });

  it('default implementations return sensible values', async () => {
    const mp = new MetadataProvider();
    expect(await mp.getPartitions('url')).toBeNull();
    expect(await mp.getExtents('url')).toBeNull();
    expect(await mp.getParquetUrls('https://example.com/data.parquet'))
      .toEqual(['https://example.com/data.parquet']);
  });
});
