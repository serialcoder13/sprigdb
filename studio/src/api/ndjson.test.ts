import { describe, expect, it } from 'vitest';
import { NdjsonDecoder } from './ndjson';

describe('NdjsonDecoder', () => {
  it('reassembles lines and UTF-8 split across chunks', () => {
    const d = new NdjsonDecoder();
    const bytes = new TextEncoder().encode('{"a":"é"}\n{"b":2}\n{"c"');
    // Split inside the two-byte é.
    const cut = 7;
    expect(d.push(bytes.slice(0, cut))).toEqual([]);
    expect(d.push(bytes.slice(cut))).toEqual([{ a: 'é' }, { b: 2 }]);
    expect(d.push(new TextEncoder().encode(':3}'))).toEqual([]);
    expect(d.end()).toEqual([{ c: 3 }]);
  });
});
