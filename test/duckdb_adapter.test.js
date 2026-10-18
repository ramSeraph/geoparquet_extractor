import { describe, expect, it, vi } from 'vitest';
import { createDuckDBClient, configureDuckDBTempDirectory } from '../src/duckdb_adapter.js';

describe('createDuckDBClient', () => {
  it('registers OPFS temp directories before setting temp_directory', async () => {
    const events = [];
    const conn = {
      query: vi.fn(async (sql) => {
        events.push(sql);
        return { toArray: () => [] };
      }),
    };
    const db = {
      connect: vi.fn(async () => conn),
      registerOPFSTempDir: vi.fn(async (path) => {
        events.push(`register:${path}`);
      }),
    };

    const client = createDuckDBClient(db, {
      tempDirectory: 'opfs://tmpdir_test',
    });
    await client.init();

    expect(db.registerOPFSTempDir).toHaveBeenCalledWith('opfs://tmpdir_test', 128, 32);
    expect(events.indexOf('register:opfs://tmpdir_test')).toBeLessThan(
      events.indexOf("SET temp_directory = 'opfs://tmpdir_test';")
    );
  });
});

describe('configureDuckDBTempDirectory', () => {
  it('resets temp_directory when called with empty/null path', async () => {
    const queries = [];
    const query = vi.fn(async (sql) => { queries.push(sql); });
    const db = { registerOPFSTempDir: vi.fn() };

    await configureDuckDBTempDirectory(db, query, '');
    await configureDuckDBTempDirectory(db, query, null);
    await configureDuckDBTempDirectory(db, query, undefined);

    expect(db.registerOPFSTempDir).not.toHaveBeenCalled();
    expect(queries).toEqual([
      'RESET temp_directory;',
      'RESET temp_directory;',
      'RESET temp_directory;',
    ]);
  });
});
