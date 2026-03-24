import { describe, it, expect } from 'vitest';
import { MetadataProvider } from '../src/metadata/provider.js';

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

  it('abstract methods throw', async () => {
    const mp = new MetadataProvider();
    await expect(mp.getPartitions('url')).rejects.toThrow('not implemented');
    await expect(mp.getExtents('url')).rejects.toThrow('not implemented');
    await expect(mp.getBbox('url', {})).rejects.toThrow('not implemented');
    await expect(mp.getRowGroupBboxes('url', {})).rejects.toThrow('not implemented');
    await expect(mp.getRowGroupBboxesMulti([], {})).rejects.toThrow('not implemented');
    await expect(mp.getParquetUrls('url')).rejects.toThrow('not implemented');
  });
});
