import { describe, it, expect } from 'vitest';
import { HyparquetBboxReader } from '../src/hyparquet_bbox_reader.js';

describe('HyparquetBboxReader', () => {
  it('reads file bboxes from GeoParquet metadata', async () => {
    const reader = new HyparquetBboxReader({
      metadataLoader: async (url) => ({
        key_value_metadata: [
          {
            key: 'geo',
            value: JSON.stringify({
              primary_column: 'geometry',
              columns: {
                geometry: {
                  bbox: url.includes('a.parquet') ? [77, 12, 78, 13] : [78, 13, 79, 14],
                },
              },
            }),
          },
        ],
        row_groups: [],
      }),
    });

    const result = await reader.getFileBboxes([
      { id: 'a', url: 'https://example.com/a.parquet' },
      { id: 'b', url: 'https://example.com/b.parquet' },
    ]);

    expect(result).toEqual({
      a: [77, 12, 78, 13],
      b: [78, 13, 79, 14],
    });
  });

  it('reads row group bboxes from covering stats and geospatial stats fallback', async () => {
    const reader = new HyparquetBboxReader({
      metadataLoader: async () => ({
        key_value_metadata: [
          {
            key: 'geo',
            value: JSON.stringify({
              primary_column: 'geometry',
              columns: {
                geometry: {
                  covering: {
                    bbox: {
                      xmin: ['bbox', 'xmin'],
                      ymin: ['bbox', 'ymin'],
                      xmax: ['bbox', 'xmax'],
                      ymax: ['bbox', 'ymax'],
                    },
                  },
                },
              },
            }),
          },
        ],
        row_groups: [
          {
            columns: [
              {
                meta_data: {
                  path_in_schema: ['bbox', 'xmin'],
                  statistics: { min: 77 },
                },
              },
              { meta_data: { path_in_schema: ['bbox', 'ymin'], statistics: { min: 12 } } },
              { meta_data: { path_in_schema: ['bbox', 'xmax'], statistics: { max: 77.5 } } },
              { meta_data: { path_in_schema: ['bbox', 'ymax'], statistics: { max: 12.5 } } },
            ],
          },
          {
            columns: [
              {
                meta_data: {
                  geospatial_statistics: {
                    bbox: { xmin: 77.5, ymin: 12.5, xmax: 78, ymax: 13 },
                  },
                },
              },
            ],
          },
        ],
      }),
    });

    const result = await reader.getRowGroupBboxes([
      { id: 'data', url: 'https://example.com/data.parquet' },
    ]);

    expect(result).toEqual({
      data: {
        rg_0: [77, 12, 77.5, 12.5],
        rg_1: [77.5, 12.5, 78, 13],
      },
    });
  });

  it('uses bboxColumn fallback when covering metadata is absent', async () => {
    const reader = new HyparquetBboxReader({
      metadataLoader: async () => ({
        key_value_metadata: [
          {
            key: 'geo',
            value: JSON.stringify({
              primary_column: 'geometry',
              columns: { geometry: {} },
            }),
          },
        ],
        row_groups: [
          {
            columns: [
              { meta_data: { path_in_schema: ['bounds', 'xmin'], statistics: { min: 77 } } },
              { meta_data: { path_in_schema: ['bounds', 'ymin'], statistics: { min: 12 } } },
              { meta_data: { path_in_schema: ['bounds', 'xmax'], statistics: { max: 78 } } },
              { meta_data: { path_in_schema: ['bounds', 'ymax'], statistics: { max: 13 } } },
            ],
          },
        ],
      }),
    });

    const result = await reader.getRowGroupBboxes(
      [{ id: 'data', url: 'https://example.com/data.parquet' }],
      null,
      { bboxColumn: 'bounds' },
    );

    expect(result).toEqual({
      data: {
        rg_0: [77, 12, 78, 13],
      },
    });
  });

  it('loads metadata in batches and reuses cached metadata across calls', async () => {
    let active = 0;
    let maxActive = 0;
    let calls = 0;

    const reader = new HyparquetBboxReader({
      batchSize: 10,
      metadataLoader: async (url) => {
        calls += 1;
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise(resolve => setTimeout(resolve, 1));
        active -= 1;

        return {
          key_value_metadata: [
            {
              key: 'geo',
              value: JSON.stringify({
                primary_column: 'geometry',
                columns: { geometry: { bbox: [77, 12, 78, 13] } },
              }),
            },
          ],
          row_groups: [
            {
              columns: [
                {
                  meta_data: {
                    geospatial_statistics: {
                      bbox: { xmin: 77, ymin: 12, xmax: 78, ymax: 13 },
                    },
                  },
                },
              ],
            },
          ],
          url,
        };
      },
    });

    const files = Array.from({ length: 25 }, (_, index) => ({
      id: `f${index}`,
      url: `https://example.com/${index}.parquet`,
    }));

    await reader.getFileBboxes(files);
    await reader.getRowGroupBboxes(files);

    expect(calls).toBe(25);
    expect(maxActive).toBe(10);
  });

  it('stops before starting the next batch after cancellation', async () => {
    let calls = 0;
    const reader = new HyparquetBboxReader({
      batchSize: 2,
      metadataLoader: async () => {
        calls += 1;
        if (calls === 1) {
          reader.cancel();
        }
        return {
          key_value_metadata: [
            {
              key: 'geo',
              value: JSON.stringify({
                primary_column: 'geometry',
                columns: { geometry: { bbox: [77, 12, 78, 13] } },
              }),
            },
          ],
          row_groups: [],
        };
      },
    });

    const files = Array.from({ length: 4 }, (_, index) => ({
      id: `f${index}`,
      url: `https://example.com/${index}.parquet`,
    }));

    await expect(reader.getFileBboxes(files)).rejects.toMatchObject({ name: 'AbortError' });
    expect(calls).toBe(2);
  });
});
