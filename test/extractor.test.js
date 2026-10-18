import { afterEach, describe, it, expect, vi } from 'vitest';
import { GeoParquetExtractor } from '../src/extractor.js';
import { OPFS_PREFIX_TMPDIR } from '../src/utils.js';

describe('GeoParquetExtractor', () => {
  const originalLocks = navigator.locks;

  afterEach(() => {
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: originalLocks,
    });
  });

  it('requires duckdb in constructor', () => {
    expect(() => new GeoParquetExtractor({})).toThrow('duckdb is required');
  });

  it('getDownloadBaseName generates correct filename', () => {
    const baseName = GeoParquetExtractor.getDownloadBaseName('My Source', [77.5, 12.9, 77.7, 13.1]);
    expect(baseName).toContain('My_Source');
    expect(baseName).toContain('77-5000');
  });

  it('registers the session temp directory before setting temp_directory', async () => {
    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: {
        request(_name, callback) {
          callback();
          return Promise.resolve();
        },
      },
    });

    const events = [];
    const duckdb = {
      init: vi.fn(async () => {
        events.push('init');
      }),
      query: vi.fn(async (sql) => {
        events.push(sql);
        return { toArray: () => [] };
      }),
      db: {
        registerOPFSTempDir: vi.fn(async (path) => {
          events.push(`register:${path}`);
        }),
      },
    };

    const extractor = new GeoParquetExtractor({ duckdb });
    await extractor.init();

    const registerEvent = events.find(event => event.startsWith('register:'));
    const setTempDirectoryEvent = events.find(
      event => event.startsWith("SET temp_directory = 'opfs://")
    );
    expect(registerEvent).toBeTruthy();
    expect(setTempDirectoryEvent).toBeTruthy();
    expect(registerEvent).toContain(`opfs://${OPFS_PREFIX_TMPDIR}`);
    expect(events.indexOf(registerEvent)).toBeLessThan(events.indexOf(setTempDirectoryEvent));
  });
});
