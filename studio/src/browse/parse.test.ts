import { describe, expect, it } from 'vitest';
import { parseCsv, parseNdjson } from './parse';

describe('import parsing', () => {
  it('reads RFC 4180 CSV', () => {
    expect(parseCsv('a,b,c\r\n1,"x, ""y""",\n2,"",\\N\n')).toEqual([
      ['a', 'b', 'c'],
      ['1', 'x, "y"', null],
      ['2', '', null],
    ]);
    expect(parseCsv('a\n"multi\nline"')).toEqual([['a'], ['multi\nline']]);
  });
  it('reads NDJSON', () => {
    expect(parseNdjson('{"a":1,"b":"x"}\n{"a":2,"c":{"k":1}}\n')).toEqual({
      columns: ['a', 'b', 'c'],
      rows: [
        ['1', 'x', null],
        ['2', null, '{"k":1}'],
      ],
    });
  });
});
