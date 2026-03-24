import { describe, it, expect } from 'vitest';
import { ExtentData } from '../src/extent_data.js';

describe('ExtentData', () => {
  it('requires metadataProvider', () => {
    expect(() => new ExtentData({})).toThrow('metadataProvider is required');
  });

  it('fetchExtents single returns raw data', async () => {
    const ed = new ExtentData({
      metadataProvider: {
        getParquetUrls: async () => ['https://example.com/data.parquet'],
        getBbox: async () => [77.0, 12.0, 78.0, 13.0],
        getRowGroupBboxes: async () => ({ rg_0: [77.0, 12.0, 77.5, 12.5], rg_1: [77.5, 12.5, 78.0, 13.0] }),
      },
      duckdb: {},
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toEqual({ 'data.parquet': [77.0, 12.0, 78.0, 13.0] });
    expect(rgExtents).toEqual({ 'data.parquet': { rg_0: [77.0, 12.0, 77.5, 12.5], rg_1: [77.5, 12.5, 78.0, 13.0] } });
  });

  it('fetchExtents partitioned returns raw data', async () => {
    const ed = new ExtentData({
      metadataProvider: {
        getExtents: async () => ({ 'data.0.parquet': [77, 12, 78, 13] }),
        getParquetUrls: async () => ['https://example.com/data.0.parquet'],
        getRowGroupBboxesMulti: async () => ({ 'data.0.parquet': { rg_0: [77, 12, 77.5, 12.5] } }),
      },
      duckdb: {},
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test', partitioned: true });

    expect(dataExtents).toEqual({ 'data.0.parquet': [77, 12, 78, 13] });
    expect(rgExtents).toEqual({ 'data.0.parquet': { rg_0: [77, 12, 77.5, 12.5] } });
  });

  it('fetchExtents without duckdb skips row groups', async () => {
    const ed = new ExtentData({
      metadataProvider: {
        getParquetUrls: async () => ['https://example.com/data.parquet'],
        getBbox: async () => [77, 12, 78, 13],
      },
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toEqual({ 'data.parquet': [77, 12, 78, 13] });
    expect(rgExtents).toBeNull();
  });
});
