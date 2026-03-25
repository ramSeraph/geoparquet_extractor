// Fetches and caches file sizes via HEAD requests through the configured proxy.

import { proxyUrl } from './proxy.js';
import { formatSize } from './utils.js';

export class SizeGetter {
  constructor() {
    this._cache = new Map();
  }

  async getSizeBytes(url) {
    if (this._cache.has(url)) return this._cache.get(url);
    try {
      const resp = await fetch(proxyUrl(url), { method: 'HEAD' });
      if (resp.ok) {
        const cl = resp.headers.get('content-length');
        if (cl) {
          const bytes = parseInt(cl, 10);
          this._cache.set(url, bytes);
          return bytes;
        }
      }
    } catch { /* ignore */ }
    return null;
  }

  async getSize(url) {
    const bytes = await this.getSizeBytes(url);
    return bytes != null ? formatSize(bytes) : null;
  }
}
