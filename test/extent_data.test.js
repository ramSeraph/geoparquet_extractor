import { describe, it, expect } from 'vitest';
import { ExtentData } from '../src/extent_data.js';

describe('ExtentData', () => {
  it('requires sourceResolver', () => {
    expect(() => new ExtentData({})).toThrow('sourceResolver is required');
  });

  it('uses resolver-provided file bboxes and row groups', async () => {
    const ed = new ExtentData({
      sourceResolver: {
        resolve: async () => ({
          files: [
            { id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: [77, 12, 78, 13] },
          ],
        }),
      },
      bboxReader: {
        getRowGroupBboxes: async () => ({
          'data.parquet': { rg_0: [77, 12, 77.5, 12.5], rg_1: [77.5, 12.5, 78, 13] },
        }),
      },
      duckdb: {},
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toEqual({ 'data.parquet': [77, 12, 78, 13] });
    expect(rgExtents).toEqual({
      'data.parquet': { rg_0: [77, 12, 77.5, 12.5], rg_1: [77.5, 12.5, 78, 13] },
    });
  });

  it('falls back to parquet-derived file bboxes when resolver omits them', async () => {
    const ed = new ExtentData({
      sourceResolver: {
        resolve: async () => ({
          files: [{ id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: null }],
        }),
      },
      bboxReader: {
        getFileBboxes: async () => ({ 'data.parquet': [77, 12, 78, 13] }),
        getRowGroupBboxes: async () => ({ 'data.parquet': { rg_0: [77, 12, 77.5, 12.5] } }),
      },
      duckdb: {},
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toEqual({ 'data.parquet': [77, 12, 78, 13] });
    expect(rgExtents).toEqual({ 'data.parquet': { rg_0: [77, 12, 77.5, 12.5] } });
  });

  it('without duckdb skips parquet fallback and row groups', async () => {
    const ed = new ExtentData({
      sourceResolver: {
        resolve: async () => ({
          files: [{ id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: null }],
        }),
      },
    });

    const { dataExtents, rgExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toBeNull();
    expect(rgExtents).toBeNull();
  });

  it('cancels extent loading and terminates duckdb', async () => {
    let cancelled = false;
    let terminated = false;

    const ed = new ExtentData({
      sourceResolver: {
        resolve: async () => ({
          files: [{ id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: null }],
        }),
      },
      bboxReader: {
        cancel: () => {
          cancelled = true;
        },
        getFileBboxes: async () => {
          await Promise.resolve();
          if (cancelled) {
            throw new DOMException('cancelled', 'AbortError');
          }
          return null;
        },
      },
      duckdb: {
        terminate: () => {
          terminated = true;
        },
      },
    });

    const pending = ed.fetchExtents({ sourceUrl: 'test' });
    ed.cancel();

    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(cancelled).toBe(true);
    expect(terminated).toBe(true);
  });

  it('can reuse the same instance with a new duckdb after cancellation', async () => {
    let calls = 0;
    const bboxReader = {
      cancel: () => {},
      getFileBboxes: async () => {
        calls += 1;
        return { 'data.parquet': [77, 12, 78, 13] };
      },
      getRowGroupBboxes: async () => null,
    };

    const ed = new ExtentData({
      sourceResolver: {
        resolve: async () => ({
          files: [{ id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: null }],
        }),
      },
      bboxReader,
      duckdb: { terminate: () => {} },
    });

    ed.cancel();
    ed.setDuckDB({ terminate: () => {} });

    const { dataExtents } = await ed.fetchExtents({ sourceUrl: 'test' });

    expect(dataExtents).toEqual({ 'data.parquet': [77, 12, 78, 13] });
    expect(calls).toBe(1);
  });
});
