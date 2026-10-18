import { afterEach, describe, expect, it } from 'vitest';
import { initDuckDB } from '../dist/geoparquet-extractor.js';
import * as duckdb from '../dist/duckdb.js';

const spillSQL = 'SELECT COUNT(*) AS n FROM (SELECT i FROM range(0, 800000) i ORDER BY hash(i) LIMIT 100000)';
const clients = [];
const directories = [];

async function setup() {
  const client = await initDuckDB();
  clients.push(client);
  const name = `gpqe-test-${crypto.randomUUID()}`;
  directories.push(name);
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(name, { create: true });
  const conn = await client.db.connect();
  return { db: client.db, conn, directory, path: `opfs://${name}` };
}

async function spill(db, conn, path) {
  await db.registerOPFSTempDir(path, 10, 2);
  await conn.query(`SET threads=1; SET temp_directory='${path}'; SET memory_limit='4MB'; SET max_temp_directory_size='100MiB'`);
  expect(String((await conn.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
}

async function names(directory) {
  const result = [];
  for await (const name of directory.keys()) result.push(name);
  return result.sort();
}

async function storedBytes(directory) {
  let bytes = 0;
  for await (const entry of directory.values()) {
    bytes += entry.kind === 'directory' ? await storedBytes(entry) : (await entry.getFile()).size;
  }
  return bytes;
}

afterEach(async () => {
  for (const client of clients.splice(0)) {
    try { await client.db.reset(); } finally { client.terminate(); }
  }
  const root = await navigator.storage.getDirectory();
  for (const name of directories.splice(0)) await root.removeEntry(name, { recursive: true });
});

describe('packaged DuckDB with real OPFS', () => {
  it('uses the extractor package identity and asset URLs', async () => {
    expect(duckdb.PACKAGE_NAME).toBe('geoparquet-extractor');
    expect(duckdb.getJsDelivrBundles().eh.mainWorker).toContain('/geoparquet-extractor@');
    expect(duckdb.getJsDelivrBundles().eh.mainWorker).toContain('/dist/duckdb/');
    const { conn } = await setup();
    expect((await conn.query('SELECT 42 AS n')).getChildAt(0).get(0)).toBe(42);
    await conn.close();
  });

  it('spills in memory databases and preserves a pool when registered again', async () => {
    const { db, conn, path, directory } = await setup();
    await spill(db, conn, path);
    expect(await storedBytes(directory)).toBeGreaterThan(0);
    const owners = await names(directory);
    await db.registerOPFSTempDir(path, 10, 2);
    expect(await names(directory)).toEqual(owners);
    expect(String((await conn.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
    await expect(db.registerOPFSTempDir(`${path}/replacement`)).rejects.toThrow('Reset DuckDB');
    await conn.close();
  });

  it('releases checked-out handles on reset and can spill again', async () => {
    const { db, conn, path, directory } = await setup();
    await spill(db, conn, path);
    await conn.close();
    await db.reset();
    expect(await names(directory)).toEqual([]);
    const next = await db.connect();
    await spill(db, next, path);
    for (let i = 0; i < 3; i++) {
      expect(String((await next.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
    }
    await next.close();
  });

  it('does not alias two filenames when a pooled handle is reused', async () => {
    const { db, conn, path } = await setup();
    await db.registerOPFSTempDir(path, 4, 0);
    await conn.query(`COPY (SELECT 111 AS n) TO '${path}/a.csv' (FORMAT CSV)`);
    await db.dropFile(`${path}/a.csv`);
    await conn.query(`COPY (SELECT 222 AS n) TO '${path}/b.csv' (FORMAT CSV)`);
    await conn.query(`COPY (SELECT 333 AS n) TO '${path}/a.csv' (FORMAT CSV)`);
    expect(String((await conn.query(`SELECT * FROM read_csv('${path}/b.csv')`)).getChildAt(0).get(0))).toBe('222');
    expect(String((await conn.query(`SELECT * FROM read_csv('${path}/a.csv')`)).getChildAt(0).get(0))).toBe('333');
    await conn.close();
  });

  it('keeps existing temp-directory files and supports spaces in paths', async () => {
    const { db, conn, path, directory } = await setup();
    const file = await directory.getFileHandle('tmp1', { create: true });
    const writer = await file.createWritable();
    await writer.write('keep me');
    await writer.close();
    await db.registerOPFSTempDir(path, 4, 0);
    await db.registerOPFSTempDir();
    expect(await (await file.getFile()).text()).toBe('keep me');
    await spill(db, conn, `${path}/with space`);
    await conn.close();
  });

  it('isolates simultaneous databases using the same temp directory', async () => {
    const first = await setup();
    const second = await setup();
    await Promise.all([
      spill(first.db, first.conn, first.path),
      spill(second.db, second.conn, first.path),
    ]);
    expect(await names(first.directory)).toHaveLength(2);
    await first.conn.close();
    await first.db.reset();
    expect(await names(first.directory)).toHaveLength(1);
    expect((await second.conn.query('SELECT 42 AS n')).getChildAt(0).get(0)).toBe(42);
    for (let i = 0; i < 3; i++) {
      expect(String((await second.conn.query(spillSQL)).getChildAt(0).get(0))).toBe('100000');
    }
    await second.conn.close();
  });
});
