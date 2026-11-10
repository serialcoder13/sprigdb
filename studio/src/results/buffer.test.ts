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
  it('slides a window of maxRows rows for a cursor', () => {
    const b = new ResultBuffer(cols, 4, 1e9);
    b.push([['1', 'a'], ['2', 'b'], ['3', 'c'], ['4', 'd']]);
    b.slide([['5', 'e'], ['6', 'f']]);
    expect(b.rowCount).toBe(4);
    expect(b.offset).toBe(2);
    expect(b.cell(0, 0)).toBe('3');
    expect(b.cell(3, 1)).toBe('f');
    const bytes = b.bytes;
    b.slide([['7', 'g']]);
    expect(b.bytes).toBe(bytes);
    expect(b.offset).toBe(3);
  });
  it('drops rows from the front to stay under the byte cap', () => {
    const b = new ResultBuffer(cols, 1000, 400);
    for (let i = 0; i < 20; i++) b.slide([[String(i), 'x'.repeat(20)]]);
    expect(b.bytes).toBeLessThan(400);
    expect(b.offset + b.rowCount).toBe(20);
    expect(b.cell(b.rowCount - 1, 0)).toBe('19');
  });
});
