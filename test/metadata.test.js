import { describe, it, expect } from 'vitest';
import { SourceResolver } from '../src/source_resolver.js';

describe('SourceResolver', () => {
  it('default resolver returns the source URL as a single file', async () => {
    const resolver = new SourceResolver();
    const { files } = await resolver.resolve('https://example.com/data.parquet');

    expect(files).toEqual([
      { id: 'data.parquet', url: 'https://example.com/data.parquet', bbox: null },
    ]);
  });

  it('default resolver preserves non-parquet URLs as-is', async () => {
    const resolver = new SourceResolver();
    const { files } = await resolver.resolve('https://example.com/data.pmtiles');

    expect(files).toEqual([
      { id: 'data.pmtiles', url: 'https://example.com/data.pmtiles', bbox: null },
    ]);
  });
});
