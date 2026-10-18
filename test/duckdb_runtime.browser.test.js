import { afterEach, describe, expect, it } from 'vitest';
import { GeoParquetExtractor, initDuckDB } from '../dist/geoparquet-extractor.js';
import * as duckdb from '../dist/duckdb.js';

const spillSQL = 'SELECT COUNT(*) AS n FROM (SELECT i FROM range(0, 800000) i ORDER BY hash(i) LIMIT 100000)';
const clients = [];
const directories = new Set();

async function storedBytes(directory) {
  let bytes = 0;
  for await (const entry of directory.values()) {
    bytes += entry.kind === 'directory' ? await storedBytes(entry) : (await entry.getFile()).size;
  }
  return bytes;
}

afterEach(async () => {
  for (const client of clients.splice(0)) client.terminate();
  const root = await navigator.storage.getDirectory();
  // Worker termination releases sync access handles asynchronously.
  for (const name of directories) {
    await expect.poll(async () => {
      try { await root.removeEntry(name, { recursive: true }); return true; }
      catch (error) { if (error.name === 'NotFoundError') return true; return false; }
    }).toBe(true);
  }
  directories.clear();
});

describe('packaged DuckDB in the extractor workflow', () => {
  it('uses the extractor package identity and asset URLs', async () => {
    expect(duckdb.PACKAGE_NAME).toBe('geoparquet-extractor');
    expect(duckdb.getJsDelivrBundles().eh.mainWorker).toContain('/geoparquet-extractor@');
    expect(duckdb.getJsDelivrBundles().eh.mainWorker).toContain('/dist/duckdb/');
    const client = await initDuckDB();
    clients.push(client);
    const conn = await client.db.connect();
    expect((await conn.query('SELECT 42 AS n')).getChildAt(0).get(0)).toBe(42);
    await conn.close();
  });

  it('spills repeatedly and restarts after extractor cancellation', async () => {
    const client = await initDuckDB();
    clients.push(client);
    const extractor = new GeoParquetExtractor({ duckdb: client, memoryLimitMB: 4 });
    await extractor.init();
    const firstDB = client.db;
    const path = (await client.query("SELECT current_setting('temp_directory')")).getChildAt(0).get(0);
    directories.add(path.slice('opfs://'.length));
    await client.query("SET max_temp_directory_size='100MiB'");
    for (let i = 0; i < 3; i++) {
      await extractor.init();
      expect(client.db).toBe(firstDB);
      expect(String((await client.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
    }
    const root = await navigator.storage.getDirectory();
    expect(await storedBytes(await root.getDirectoryHandle(path.slice('opfs://'.length)))).toBeGreaterThan(0);

    extractor.cancel();
    await extractor.init();
    expect(client.db).not.toBe(firstDB);
    await client.query("SET max_temp_directory_size='100MiB'");
    for (let i = 0; i < 3; i++) {
      expect(String((await client.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
    }
  });

  it('does not alias filenames when a temp handle is reused', async () => {
    const client = await initDuckDB();
    clients.push(client);
    const name = `gpqe-test-${crypto.randomUUID()}`;
    directories.add(name);
    const path = `opfs://${name}`;
    const { db } = client;
    const conn = await db.connect();
    await db.registerOPFSTempDir(path, 4, 0);
    await conn.query(`COPY (SELECT 111 AS n) TO '${path}/a.csv' (FORMAT CSV)`);
    await db.dropFile(`${path}/a.csv`);
    await conn.query(`COPY (SELECT 222 AS n) TO '${path}/b.csv' (FORMAT CSV)`);
    await conn.query(`COPY (SELECT 333 AS n) TO '${path}/a.csv' (FORMAT CSV)`);
    expect(String((await conn.query(`SELECT * FROM read_csv('${path}/b.csv')`)).getChildAt(0).get(0))).toBe('222');
    expect(String((await conn.query(`SELECT * FROM read_csv('${path}/a.csv')`)).getChildAt(0).get(0))).toBe('333');
    await conn.close();
  });
});
