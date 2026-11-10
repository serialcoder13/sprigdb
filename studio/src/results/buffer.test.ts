import { describe, expect, it } from 'vitest';
import { ResultBuffer } from './buffer';

const cols = [
  { name: 'a', type: 'LONG', mysql_type: 3 },
  { name: 'b', type: 'VAR_STRING', mysql_type: 253 },
];

describe('ResultBuffer', () => {
  it('stores columns and stops at the row cap', () => {
    const b = new ResultBuffer(cols, 3, 1e9);
    expect(b.push([['1', 'x'], ['2', null]])).toBe(true);
    expect(b.push([['3', 'y'], ['4', 'z']])).toBe(false);
    expect(b.rowCount).toBe(3);
    expect(b.truncated).toBe(true);
    expect(b.row(1)).toEqual(['2', null]);
    expect(b.cell(2, 1)).toBe('y');
    expect(b.numeric(0)).toBe(true);
    expect(b.numeric(1)).toBe(false);
  });
  it('stops at the byte cap', () => {
    const b = new ResultBuffer(cols, 1e6, 200);
    let more = true;
    let n = 0;
    while (more && n < 100) {
      more = b.push([[String(n), 'x'.repeat(20)]]);
      n++;
    }
    expect(b.bytes).toBeGreaterThanOrEqual(200);
    expect(b.rowCount).toBeLessThan(100);
  });
});
