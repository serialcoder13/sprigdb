import { describe, expect, it } from 'vitest';
import { seriesBuffer } from './Fingerprints';

describe('Workload.Series', () => {
  it('becomes a chartable buffer: time first, numbers after', () => {
    const b = seriesBuffer({
      id: 'ab',
      text: 'SELECT ?',
      days: 7,
      buckets: [
        { start: 1_794_268_800, runs: 3, errors: 0, p50_ms: 1.5, p95_ms: 4, total_ms: 6 },
        { start: 1_794_269_100, runs: 1, errors: 1, p50_ms: 2, p95_ms: 2, total_ms: 2 },
      ],
    });
    expect(b.rowCount).toBe(2);
    expect(b.cell(0, 0)).toBe('2026-11-10 00:00');
    expect(b.numeric(2)).toBe(true);
    expect(b.cell(1, 3)).toBe('1');
  });
});
