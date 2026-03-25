/**
 * Configurable URL proxy function.
 * Consumers can override this to provide their own CORS proxy.
 * @type {(url: string) => string}
 */
let _proxyUrlFn = (url) => url;

/**
 * Set the proxy URL function used throughout the library.
 * @param {(url: string) => string} fn
 */
export function setProxyUrl(fn) {
  _proxyUrlFn = fn;
}

/**
 * Get the proxied version of a URL.
 * @param {string} url
 * @returns {string}
 */
export function proxyUrl(url) {
  return _proxyUrlFn(url);
}
