// Documents with their BSON types (chapter 44 step 20, "JSON tree"). The
// gateway sends each document as relaxed Extended JSON (chapter 37,
// "Languages"), so this parser keeps every number's text: an integer within
// 32 bits is int32, a larger one int64, one with a fraction or exponent a
// double; `$oid`, `$date`, `$numberLong`, `$numberDecimal`, `$binary`,
// `$timestamp`, `$regularExpression`, `$minKey` and `$maxKey` wrappers keep
// their types. Values convert back to canonical Extended JSON, so saving a
// document never turns an int64 into a double.

export type BsonType =
  | 'object'
  | 'array'
  | 'string'
  | 'int32'
  | 'int64'
  | 'double'
  | 'decimal'
  | 'bool'
  | 'null'
  | 'objectId'
  | 'date'
  | 'binary'
  | 'timestamp'
  | 'regex'
  | 'minKey'
  | 'maxKey';

export type BVal =
  | { t: 'object'; v: [string, BVal][] }
  | { t: 'array'; v: BVal[] }
  | { t: Exclude<BsonType, 'object' | 'array'>; v: string };

class Reader {
  i = 0;
  constructor(readonly s: string) {}
  ws() {
    while (this.i < this.s.length && ' \t\n\r'.includes(this.s[this.i]!)) this.i++;
  }
  err(msg: string): never {
    throw new SyntaxError(`${msg} at ${this.i}`);
  }
  str(): string {
    // JSON.parse handles escapes; find the closing quote first.
    const start = this.i;
    this.i++;
    while (this.i < this.s.length && this.s[this.i] !== '"') {
      if (this.s[this.i] === '\\') this.i++;
      this.i++;
    }
    if (this.i >= this.s.length) this.err('unterminated string');
    this.i++;
    return JSON.parse(this.s.slice(start, this.i)) as string;
  }
  value(): BVal {
    this.ws();
    const c = this.s[this.i];
    if (c === '{') {
      this.i++;
      const v: [string, BVal][] = [];
      this.ws();
      if (this.s[this.i] === '}') {
        this.i++;
        return { t: 'object', v };
      }
      for (;;) {
        this.ws();
        if (this.s[this.i] !== '"') this.err('expected a key');
        const k = this.str();
        this.ws();
        if (this.s[this.i] !== ':') this.err('expected :');
        this.i++;
        v.push([k, this.value()]);
        this.ws();
        if (this.s[this.i] === ',') {
          this.i++;
          continue;
        }
        if (this.s[this.i] === '}') {
          this.i++;
          return wrapper(v) ?? { t: 'object', v };
        }
        this.err('expected , or }');
      }
    }
    if (c === '[') {
      this.i++;
      const v: BVal[] = [];
      this.ws();
      if (this.s[this.i] === ']') {
        this.i++;
        return { t: 'array', v };
      }
      for (;;) {
        v.push(this.value());
        this.ws();
        if (this.s[this.i] === ',') {
          this.i++;
          continue;
        }
        if (this.s[this.i] === ']') {
          this.i++;
          return { t: 'array', v };
        }
        this.err('expected , or ]');
      }
    }
    if (c === '"') return { t: 'string', v: this.str() };
    if (this.s.startsWith('true', this.i)) {
      this.i += 4;
      return { t: 'bool', v: 'true' };
    }
    if (this.s.startsWith('false', this.i)) {
      this.i += 5;
      return { t: 'bool', v: 'false' };
    }
    if (this.s.startsWith('null', this.i)) {
      this.i += 4;
      return { t: 'null', v: 'null' };
    }
    const m = /^-?\d+(\.\d+)?([eE][-+]?\d+)?/.exec(this.s.slice(this.i, this.i + 400));
    if (!m) this.err('unexpected character');
    this.i += m[0].length;
    return numberType(m[0]);
  }
}

/** The BSON type relaxed Extended JSON gives a number. */
export function numberType(text: string): BVal {
  if (/[.eE]/.test(text)) return { t: 'double', v: text };
  const n = BigInt(text);
  if (n >= -2147483648n && n <= 2147483647n) return { t: 'int32', v: text };
  return { t: 'int64', v: text };
}

const str = (b: BVal | undefined): string | null => (b && b.t === 'string' ? b.v : null);
const get = (o: [string, BVal][], k: string) => o.find(([x]) => x === k)?.[1];

/** An Extended JSON wrapper object as its typed value. */
function wrapper(o: [string, BVal][]): BVal | null {
  if (o.length === 0 || o.length > 2) return null;
  const [k0, v0] = o[0]!;
  if (o.length === 1) {
    switch (k0) {
      case '$oid':
        return str(v0) !== null ? { t: 'objectId', v: str(v0)! } : null;
      case '$numberLong':
        return str(v0) !== null ? { t: 'int64', v: str(v0)! } : null;
      case '$numberInt':
        return str(v0) !== null ? { t: 'int32', v: str(v0)! } : null;
      case '$numberDouble':
        return str(v0) !== null ? { t: 'double', v: str(v0)! } : null;
      case '$numberDecimal':
        return str(v0) !== null ? { t: 'decimal', v: str(v0)! } : null;
      case '$date': {
        if (str(v0) !== null) return { t: 'date', v: str(v0)! };
        if (v0.t === 'object') {
          const l = str(get(v0.v, '$numberLong'));
          if (l !== null) return { t: 'date', v: new Date(Number(l)).toISOString() };
        }
        if (v0.t === 'int32' || v0.t === 'int64' || v0.t === 'double') return { t: 'date', v: new Date(Number(v0.v)).toISOString() };
        return null;
      }
      case '$binary':
        if (v0.t === 'object') {
          const b = str(get(v0.v, 'base64'));
          const st = str(get(v0.v, 'subType'));
          if (b !== null && st !== null) return { t: 'binary', v: `${st}:${b}` };
        }
        return null;
      case '$timestamp':
        if (v0.t === 'object') {
          const t = get(v0.v, 't');
          const i = get(v0.v, 'i');
          if (t && i && t.t !== 'object' && t.t !== 'array' && i.t !== 'object' && i.t !== 'array') return { t: 'timestamp', v: `${t.v},${i.v}` };
        }
        return null;
      case '$regularExpression':
        if (v0.t === 'object') {
          const p = str(get(v0.v, 'pattern'));
          const f = str(get(v0.v, 'options'));
          if (p !== null && f !== null) return { t: 'regex', v: `/${p}/${f}` };
        }
        return null;
      case '$minKey':
        return { t: 'minKey', v: 'MinKey' };
      case '$maxKey':
        return { t: 'maxKey', v: 'MaxKey' };
      default:
        return null;
    }
  }
  return null;
}

/** Parses one document (relaxed or canonical Extended JSON). */
export function parseDoc(text: string): BVal {
  const r = new Reader(text);
  const v = r.value();
  r.ws();
  if (r.i !== text.length) r.err('trailing text');
  return v;
}

/** A value as canonical Extended JSON (what the gateway reads back exactly). */
export function toEjson(b: BVal): unknown {
  switch (b.t) {
    case 'object':
      return Object.fromEntries(b.v.map(([k, v]) => [k, toEjson(v)]));
    case 'array':
      return b.v.map(toEjson);
    case 'string':
      return b.v;
    case 'int32':
      return { $numberInt: b.v };
    case 'int64':
      return { $numberLong: b.v };
    case 'double':
      return { $numberDouble: b.v };
    case 'decimal':
      return { $numberDecimal: b.v };
    case 'bool':
      return b.v === 'true';
    case 'null':
      return null;
    case 'objectId':
      return { $oid: b.v };
    case 'date': {
      const ms = Date.parse(b.v);
      return { $date: { $numberLong: String(Number.isNaN(ms) ? 0 : ms) } };
    }
    case 'binary': {
      const [st, b64] = b.v.split(':');
      return { $binary: { base64: b64 ?? '', subType: st ?? '00' } };
    }
    case 'timestamp': {
      const [t, i] = b.v.split(',');
      return { $timestamp: { t: Number(t), i: Number(i) } };
    }
    case 'regex': {
      const m = /^\/(.*)\/([a-z]*)$/s.exec(b.v);
      return { $regularExpression: { pattern: m?.[1] ?? b.v, options: m?.[2] ?? '' } };
    }
    case 'minKey':
      return { $minKey: 1 };
    case 'maxKey':
      return { $maxKey: 1 };
  }
}

/** Checks a scalar's text for its type; null when valid, else why not. */
export function invalidScalar(t: BsonType, v: string): string | null {
  switch (t) {
    case 'int32': {
      if (!/^-?\d+$/.test(v)) return 'an int32 is a whole number';
      const n = BigInt(v);
      return n < -2147483648n || n > 2147483647n ? 'an int32 is between -2147483648 and 2147483647' : null;
    }
    case 'int64': {
      if (!/^-?\d+$/.test(v)) return 'an int64 is a whole number';
      const n = BigInt(v);
      return n < -9223372036854775808n || n > 9223372036854775807n ? 'out of the int64 range' : null;
    }
    case 'double':
      return /^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$|^-?Infinity$|^NaN$/.test(v) ? null : 'not a number';
    case 'decimal':
      return /^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(v) ? null : 'not a decimal number';
    case 'bool':
      return v === 'true' || v === 'false' ? null : 'true or false';
    case 'objectId':
      return /^[0-9a-f]{24}$/i.test(v) ? null : '24 hex digits';
    case 'date':
      return Number.isNaN(Date.parse(v)) ? 'an ISO-8601 date' : null;
    case 'regex':
      return /^\/.*\/[a-z]*$/s.test(v) ? null : '/pattern/options';
    case 'binary':
      return /^[0-9a-f]{2}:[A-Za-z0-9+/=]*$/i.test(v) ? null : 'subtype:base64';
    case 'timestamp':
      return /^\d+,\d+$/.test(v) ? null : 't,i';
    default:
      return null;
  }
}

/** The scalar types a user may pick for a value. */
export const SCALAR_TYPES: BsonType[] = ['string', 'int32', 'int64', 'double', 'decimal', 'bool', 'null', 'objectId', 'date', 'regex', 'binary', 'timestamp'];

/** Sets the value at `path` (keys and indexes) in a copy of `root`. */
export function setAt(root: BVal, path: (string | number)[], value: BVal): BVal {
  if (!path.length) return value;
  const [head, ...rest] = path;
  if (root.t === 'object') {
    return { t: 'object', v: root.v.map(([k, v]) => [k, k === head ? setAt(v, rest, value) : v] as [string, BVal]) };
  }
  if (root.t === 'array') {
    return { t: 'array', v: root.v.map((v, i) => (i === head ? setAt(v, rest, value) : v)) };
  }
  return root;
}

/** The document's `_id` as canonical Extended JSON, if it has one. */
export function idOf(doc: BVal): unknown {
  if (doc.t !== 'object') return undefined;
  const id = doc.v.find(([k]) => k === '_id')?.[1];
  return id ? toEjson(id) : undefined;
}
