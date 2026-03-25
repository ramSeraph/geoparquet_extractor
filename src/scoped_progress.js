/**
 * Maps 0–100 progress to a sub-range of a parent progress handler.
 * Supports nesting: a ScopedProgress can wrap another ScopedProgress.
 */
export class ScopedProgress {
  /**
   * @param {((pct: number) => void) | undefined} onProgress
   * @param {number} start - Start of the mapped range (0–100)
   * @param {number} end - End of the mapped range (0–100)
   */
  constructor(onProgress, start, end) {
    this._onProgress = onProgress;
    this._start = start;
    this._end = end;
    this.callback = this.report.bind(this);
  }

  /** @param {number} pct - Progress 0–100 within this scope */
  report(pct) {
    const clamped = Math.max(0, Math.min(100, pct));
    const mapped = this._start + (clamped / 100) * (this._end - this._start);
    this._onProgress?.(Math.round(mapped));
  }
}
