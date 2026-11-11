import { describe, expect, it } from 'vitest';
import { invalidScalar, numberType, parseDoc, setAt, toEjson, type BVal } from './ejson';
import { dotted, saveCommand } from './edit';

describe('Extended JSON with BSON types (step 20)', () => {
  it('keeps number types from their text', () => {
    expect(numberType('1')).toEqual({ t: 'int32', v: '1' });
    expect(numberType('2147483648')).toEqual({ t: 'int64', v: '2147483648' });
    expect(numberType('9007199254740993')).toEqual({ t: 'int64', v: '9007199254740993' });
    expect(numberType('1.0')).toEqual({ t: 'double', v: '1.0' });
    expect(numberType('1e3')).toEqual({ t: 'double', v: '1e3' });
  });

  it('reads relaxed and canonical wrappers', () => {
    const d = parseDoc(
      '{"_id":{"$oid":"64b7f0c2a1b2c3d4e5f60718"},"n":9007199254740993,"l":{"$numberLong":"5"},"x":2.50,"dec":{"$numberDecimal":"1.10"},"at":{"$date":"2026-11-10T00:00:00Z"},"b":{"$binary":{"base64":"AQI=","subType":"00"}},"t":{"$timestamp":{"t":5,"i":1}},"r":{"$regularExpression":{"pattern":"a","options":"i"}},"s":"text","ok":true,"none":null,"arr":[1,{"k":"v"}]}',
    );
    expect(d.t).toBe('object');
    const types = (d as Extract<BVal, { t: 'object' }>).v.map(([k, v]) => `${k}:${v.t}`);
    expect(types).toEqual([
      '_id:objectId', 'n:int64', 'l:int64', 'x:double', 'dec:decimal', 'at:date', 'b:binary', 't:timestamp', 'r:regex', 's:string', 'ok:bool', 'none:null', 'arr:array',
    ]);
  });

  it('every type survives parse → canonical Extended JSON → parse', () => {
    const src =
      '{"_id":{"$oid":"64b7f0c2a1b2c3d4e5f60718"},"i":7,"l":{"$numberLong":"9223372036854775807"},"d":{"$numberDouble":"1.0"},"dec":{"$numberDecimal":"-0.000"},"at":{"$date":{"$numberLong":"1762732800000"}},"b":{"$binary":{"base64":"AQI=","subType":"04"}},"t":{"$timestamp":{"t":5,"i":1}},"r":{"$regularExpression":{"pattern":"a/b","options":"im"}},"mn":{"$minKey":1},"mx":{"$maxKey":1},"nested":{"a":[{"$numberLong":"1"},2.5]}}';
    const a = parseDoc(src);
    const b = parseDoc(JSON.stringify(toEjson(a)));
    expect(b).toEqual(a);
    // An int64 stays an int64 after a save, whatever its value.
    expect(toEjson(parseDoc('{"l":{"$numberLong":"1"}}'))).toEqual({ l: { $numberLong: '1' } });
  });

  it('checks edited values against their badge', () => {
    expect(invalidScalar('int32', '2147483648')).not.toBeNull();
    expect(invalidScalar('int64', '2147483648')).toBeNull();
    expect(invalidScalar('objectId', 'zz')).not.toBeNull();
    expect(invalidScalar('date', '2026-11-10T00:00:00Z')).toBeNull();
    expect(invalidScalar('double', '1.5e3')).toBeNull();
    expect(invalidScalar('bool', 'yes')).not.toBeNull();
  });

  it('a save sets only the edited paths, if every field read is unchanged', () => {
    const read = parseDoc('{"_id":{"$oid":"64b7f0c2a1b2c3d4e5f60718"},"n":{"$numberLong":"5"},"small":7,"sub":{"a":1},"tags":["x","y"]}');
    let mine = setAt(read, ['n'], { t: 'int64', v: '6' });
    mine = setAt(mine, ['tags', 1], { t: 'string', v: 'z' });
    expect(saveCommand('users', read, mine, [['n'], ['tags', 1]])).toEqual({
      update: 'users',
      updates: [
        {
          q: {
            _id: { $oid: '64b7f0c2a1b2c3d4e5f60718' },
            n: { $numberLong: '5' },
            small: { $numberInt: '7' },
            sub: { $eq: { a: { $numberInt: '1' } } },
            tags: { $eq: ['x', 'y'] },
          },
          // `small` is untouched, so a relaxed 7 never becomes int32 on disk.
          u: { $set: { n: { $numberLong: '6' }, 'tags.1': 'z' } },
          multi: false,
          upsert: false,
        },
      ],
    });
    expect(dotted(['a.b'])).toBeNull();
    expect(dotted(['$x'])).toBeNull();
    expect(dotted(['a', 0, 'b'])).toBe('a.0.b');
  });
});
