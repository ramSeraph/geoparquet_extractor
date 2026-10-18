// OPFS handles must be opened asynchronously before DuckDB's synchronous I/O.
// Each pool owns a private subdirectory; existing files in tempPath are untouched.
export class OPFSTempPool {
  constructor(path, directory, ownerName, maxUnused, minUnused) {
    this.path = path;
    this.directory = directory;
    this.ownerName = ownerName;
    this.maxUnused = maxUnused;
    this.minUnused = minUnused;
    this.unused = [];
    this.open = new Map();
    this.nextId = 0;
    this.destroyed = false;
    this.refilling = null;
    this.deleting = new Set();
    this.refillError = null;
  }

  static async create(path, maxUnused = 10, minUnused = 2) {
    path = canonicalTempPath(path);
    if (!Number.isInteger(maxUnused) || maxUnused < 1 ||
        !Number.isInteger(minUnused) || minUnused < 0 || minUnused >= maxUnused) {
      throw new Error('OPFS pool sizes must be integers with 0 <= minUnused < maxUnused');
    }
    let parent = await navigator.storage.getDirectory();
    for (const part of path.slice('opfs://'.length).split('/')) {
      parent = await parent.getDirectoryHandle(part, { create: true });
    }
    const ownerName = `.geoparquet-tmp-${crypto.randomUUID()}`;
    const directory = await parent.getDirectoryHandle(ownerName, { create: true });
    const pool = new OPFSTempPool(path, directory, ownerName, maxUnused, minUnused);
    pool.parent = parent;
    try {
      await pool.refill();
      return pool;
    } catch (error) {
      await pool.destroy();
      throw error;
    }
  }

  matches(path) {
    return path === this.path || path.startsWith(`${this.path}/`);
  }

  owns(path) {
    return this.open.has(path);
  }

  openFile(path) {
    if (this.destroyed) throw new Error('OPFS temp pool has been destroyed');
    if (this.open.has(path)) return this.open.get(path).handle;
    if (!this.unused.length) {
      throw new Error(`OPFS temp pool exhausted${this.refillError ? `: ${this.refillError.message}` : ''}`);
    }
    const entry = this.unused.pop();
    this.open.set(path, entry);
    if (this.unused.length < this.minUnused && !this.refilling) {
      this.refilling = this.refill().catch(error => {
        this.refillError = error;
      }).finally(() => { this.refilling = null; });
    }
    return entry.handle;
  }

  dropFile(path) {
    const entry = this.open.get(path);
    if (!entry) return;
    this.open.delete(path);
    if (this.destroyed || this.unused.length >= this.maxUnused) {
      const deleting = this.deleteEntry(entry).catch(error => {
        this.refillError = error;
      }).finally(() => this.deleting.delete(deleting));
      this.deleting.add(deleting);
    } else {
      entry.handle.truncate(0);
      this.unused.push(entry);
    }
  }

  async refill() {
    while (!this.destroyed && this.unused.length < this.maxUnused) {
      const name = `tmp${this.nextId++}`;
      const file = await this.directory.getFileHandle(name, { create: true });
      const handle = await file.createSyncAccessHandle();
      const entry = { name, handle };
      if (this.destroyed) await this.deleteEntry(entry);
      else this.unused.push(entry);
    }
  }

  async deleteEntry(entry) {
    entry.handle.close();
    await this.directory.removeEntry(entry.name);
  }

  async destroy() {
    this.destroyed = true;
    // A pending createSyncAccessHandle must finish and close before removal.
    await this.refilling;
    const entries = [...this.unused.splice(0), ...this.open.values()];
    this.open.clear();
    await Promise.all([...entries.map(entry => this.deleteEntry(entry)), ...this.deleting]);
    if (this.parent) await this.parent.removeEntry(this.ownerName, { recursive: true });
  }
}

export function canonicalTempPath(path) {
  if (typeof path !== 'string' || !path.startsWith('opfs://')) {
    throw new Error('Temporary directories must use opfs://');
  }
  const parts = path.slice('opfs://'.length).replace(/\/+$/, '').split('/');
  if (parts.some(part => !part || part === '.' || part === '..' || part.includes('\\'))) {
    throw new Error('An OPFS temp directory must be a nonempty path without dot segments');
  }
  return `opfs://${parts.join('/')}`;
}
