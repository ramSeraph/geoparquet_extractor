import { describe, expect, it } from 'vitest';
import { OPFSTempPool } from '../vendor/duckdb/opfs_pool.js';

function fakeDirectory() {
  const locks = new Set();
  return {
    locks,
    gate: null,
    async getFileHandle(name) {
      if (this.gate) await this.gate;
      return { async createSyncAccessHandle() {
        const handle = { close() { locks.delete(handle); }, truncate() {} };
        locks.add(handle);
        return handle;
      } };
    },
    async removeEntry() {},
  };
}

describe('OPFS temp pool lifecycle', () => {
  it('closes handles created by an in-flight refill during teardown', async () => {
    const directory = fakeDirectory();
    const pool = new OPFSTempPool('opfs://tmp', directory, 'owner', 2, 1);
    await pool.refill();
    let release;
    directory.gate = new Promise(resolve => { release = resolve; });
    pool.openFile('opfs://tmp/a');
    pool.openFile('opfs://tmp/b');
    const destroying = pool.destroy();
    release();
    await destroying;
    expect(directory.locks.size).toBe(0);
    expect(pool.unused).toHaveLength(0);
    expect(() => pool.openFile('opfs://tmp/c')).toThrow('destroyed');
  });

  it('matches path components, not similarly named sibling directories', () => {
    const pool = new OPFSTempPool('opfs://tmp', fakeDirectory(), 'owner', 2, 1);
    expect(pool.matches('opfs://tmp/a')).toBe(true);
    expect(pool.matches('opfs://tmp_backup/a')).toBe(false);
    expect(pool.matches('opfs://tmp2/a')).toBe(false);
  });

  it('reports refill failure without leaking the handles it already owns', async () => {
    const directory = fakeDirectory();
    const pool = new OPFSTempPool('opfs://tmp', directory, 'owner', 2, 1);
    await pool.refill();
    directory.getFileHandle = async () => { throw new Error('quota exceeded'); };
    pool.openFile('opfs://tmp/a');
    pool.openFile('opfs://tmp/b');
    await pool.refilling;
    expect(() => pool.openFile('opfs://tmp/c')).toThrow('quota exceeded');
    await pool.destroy();
    expect(directory.locks.size).toBe(0);
  });
});
