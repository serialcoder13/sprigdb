// MongoDB's server-side JavaScript globals: the shell types documents
// cross as, and the helpers `$where`, `$function` and `$accumulator` code
// may call.
// Runs once per runtime, before any user code. `__bt` (non-enumerable)
// names the BSON type a value goes back as.
(() => {
  "use strict";
  const def = (o, k, v) =>
    Object.defineProperty(o, k, { value: v, enumerable: false, writable: true, configurable: true });
  const host = globalThis.__sprig_mongo_host;
  delete globalThis.__sprig_mongo_host;

  // --- ObjectId --------------------------------------------------------
  let oidCounter = Math.floor(Math.random() * 0xffffff);
  const oidRandom = Array.from({ length: 5 }, () =>
    Math.floor(Math.random() * 256).toString(16).padStart(2, "0")).join("");
  function ObjectId(h) {
    if (!(this instanceof ObjectId)) return new ObjectId(h);
    if (h === undefined) {
      const ts = Math.floor(Date.now() / 1000).toString(16).padStart(8, "0");
      oidCounter = (oidCounter + 1) % 0x1000000;
      h = ts + oidRandom + oidCounter.toString(16).padStart(6, "0");
    }
    if (h instanceof ObjectId) h = h.str;
    if (typeof h !== "string" || !/^[0-9a-fA-F]{24}$/.test(h))
      throw new Error("invalid object id: length");
    def(this, "str", h.toLowerCase());
  }
  ObjectId.prototype.toString = function () { return 'ObjectId("' + this.str + '")'; };
  ObjectId.prototype.tojson = ObjectId.prototype.toString;
  ObjectId.prototype.valueOf = function () { return this.str; };
  ObjectId.prototype.toJSON = function () { return { $oid: this.str }; };
  ObjectId.prototype.toHexString = function () { return this.str; };
  ObjectId.prototype.getTimestamp = function () {
    return new Date(parseInt(this.str.substring(0, 8), 16) * 1000);
  };
  ObjectId.prototype.equals = function (o) { return o instanceof ObjectId && o.str === this.str; };
  def(ObjectId.prototype, "__bt", "oid");

  // --- NumberLong, NumberInt, NumberDecimal ------------------------------
  const I64_MIN = -(2n ** 63n), I64_MAX = 2n ** 63n - 1n;
  function NumberLong(v) {
    if (!(this instanceof NumberLong)) return new NumberLong(v);
    let b;
    if (v === undefined) b = 0n;
    else if (v instanceof NumberLong) b = BigInt(v.__v);
    else if (typeof v === "string") {
      const s = v.trim();
      if (!/^[-+]?\d+$/.test(s)) throw new Error("could not convert \"" + v + "\" to NumberLong");
      b = BigInt(s);
    } else {
      const n = Number(v);
      if (!Number.isFinite(n)) throw new Error("could not convert " + String(v) + " to NumberLong");
      b = BigInt(Math.trunc(n));
    }
    if (b < I64_MIN || b > I64_MAX) throw new Error("NumberLong out of range");
    def(this, "__v", b.toString());
  }
  NumberLong.prototype.valueOf = function () { return Number(this.__v); };
  NumberLong.prototype.toNumber = NumberLong.prototype.valueOf;
  NumberLong.prototype.toString = function () {
    const n = Number(this.__v);
    return Number.isSafeInteger(n) ? "NumberLong(" + this.__v + ")" : 'NumberLong("' + this.__v + '")';
  };
  NumberLong.prototype.tojson = NumberLong.prototype.toString;
  NumberLong.prototype.toJSON = function () { return { $numberLong: this.__v }; };
  NumberLong.prototype.compare = function (o) {
    const a = BigInt(this.__v), b = BigInt(new NumberLong(o).__v);
    return a < b ? -1 : a > b ? 1 : 0;
  };
  Object.defineProperty(NumberLong.prototype, "floatApprox", { get() { return Number(this.__v); } });
  def(NumberLong.prototype, "__bt", "long");

  function NumberInt(v) {
    if (!(this instanceof NumberInt)) return new NumberInt(v);
    def(this, "__v", Number(v === undefined ? 0 : v) | 0);
  }
  NumberInt.prototype.valueOf = function () { return this.__v; };
  NumberInt.prototype.toNumber = NumberInt.prototype.valueOf;
  NumberInt.prototype.toString = function () { return "NumberInt(" + this.__v + ")"; };
  NumberInt.prototype.tojson = NumberInt.prototype.toString;
  NumberInt.prototype.toJSON = function () { return this.__v; };
  def(NumberInt.prototype, "__bt", "int");

  function NumberDecimal(v) {
    if (!(this instanceof NumberDecimal)) return new NumberDecimal(v);
    def(this, "__v", v === undefined ? "0" : v instanceof NumberDecimal ? v.__v : String(v));
  }
  NumberDecimal.prototype.valueOf = function () { return Number(this.__v); };
  NumberDecimal.prototype.toString = function () { return 'NumberDecimal("' + this.__v + '")'; };
  NumberDecimal.prototype.tojson = NumberDecimal.prototype.toString;
  NumberDecimal.prototype.toJSON = function () { return { $numberDecimal: this.__v }; };
  def(NumberDecimal.prototype, "__bt", "dec");

  // --- Timestamp, BinData, UUID, HexData -------------------------------
  function Timestamp(t, i) {
    if (!(this instanceof Timestamp)) return new Timestamp(t, i);
    if (t !== null && typeof t === "object") { i = t.i; t = t.t; }
    this.t = Number(t === undefined ? 0 : t) >>> 0;
    this.i = Number(i === undefined ? 0 : i) >>> 0;
  }
  Timestamp.prototype.getTime = function () { return this.t; };
  Timestamp.prototype.getInc = function () { return this.i; };
  Timestamp.prototype.toString = function () { return "Timestamp(" + this.t + ", " + this.i + ")"; };
  Timestamp.prototype.tojson = Timestamp.prototype.toString;
  Timestamp.prototype.toJSON = function () { return { $timestamp: { t: this.t, i: this.i } }; };
  def(Timestamp.prototype, "__bt", "ts");

  const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  function hexToB64(hex) {
    const bytes = [];
    for (let k = 0; k + 1 < hex.length; k += 2) bytes.push(parseInt(hex.substr(k, 2), 16));
    let out = "";
    for (let k = 0; k < bytes.length; k += 3) {
      const n = (bytes[k] << 16) | ((bytes[k + 1] || 0) << 8) | (bytes[k + 2] || 0);
      out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] +
        (k + 1 < bytes.length ? B64[(n >> 6) & 63] : "=") + (k + 2 < bytes.length ? B64[n & 63] : "=");
    }
    return out;
  }
  function b64ToHex(b64) {
    let bits = 0, acc = 0, out = "";
    for (const ch of b64.replace(/=+$/, "")) {
      const v = B64.indexOf(ch);
      if (v < 0) continue;
      acc = (acc << 6) | v; bits += 6;
      if (bits >= 8) { bits -= 8; out += ((acc >> bits) & 255).toString(16).padStart(2, "0"); }
    }
    return out;
  }
  function BinData(sub, b64) {
    if (!(this instanceof BinData)) return new BinData(sub, b64);
    def(this, "__sub", Number(sub) & 255);
    def(this, "__b64", String(b64 === undefined ? "" : b64));
  }
  BinData.prototype.subtype = function () { return this.__sub; };
  BinData.prototype.base64 = function () { return this.__b64; };
  BinData.prototype.hex = function () { return b64ToHex(this.__b64); };
  BinData.prototype.length = function () { return this.hex().length / 2; };
  BinData.prototype.toString = function () {
    if (this.__sub === 4) {
      const h = this.hex();
      return 'UUID("' + h.substr(0, 8) + "-" + h.substr(8, 4) + "-" + h.substr(12, 4) + "-" +
        h.substr(16, 4) + "-" + h.substr(20) + '")';
    }
    return "BinData(" + this.__sub + ',"' + this.__b64 + '")';
  };
  BinData.prototype.tojson = BinData.prototype.toString;
  BinData.prototype.toJSON = function () {
    return { $binary: { base64: this.__b64, subType: this.__sub.toString(16).padStart(2, "0") } };
  };
  def(BinData.prototype, "__bt", "bin");
  const HexData = (sub, hex) => new BinData(sub, hexToB64(String(hex)));
  // MongoDB's server-side `UUID()` builds subtype 3 (the legacy subtype).
  const UUID = (s) => new BinData(3, hexToB64(String(s === undefined ? "" : s).replace(/-/g, "")));
  const MD5 = (s) => new BinData(5, hexToB64(String(s)));

  // --- MinKey, MaxKey, Code, DBRef, ISODate ------------------------------
  function MinKey() { return MinKey; }
  def(MinKey, "__bt", "minkey");
  MinKey.tojson = () => '{ "$minKey" : 1 }';
  MinKey.toJSON = () => ({ $minKey: 1 });
  function MaxKey() { return MaxKey; }
  def(MaxKey, "__bt", "maxkey");
  MaxKey.tojson = () => '{ "$maxKey" : 1 }';
  MaxKey.toJSON = () => ({ $maxKey: 1 });
  function Code(code, scope) {
    if (!(this instanceof Code)) return new Code(code, scope);
    this.code = String(code);
    if (scope !== undefined) this.scope = scope;
  }
  Code.prototype.toString = function () { return 'Code("' + this.code + '")'; };
  def(Code.prototype, "__bt", "code");
  function DBRef(ns, id, db) {
    if (!(this instanceof DBRef)) return new DBRef(ns, id, db);
    this.$ref = ns; this.$id = id;
    if (db !== undefined) this.$db = db;
  }
  const ISODate = (s) => (s === undefined ? new Date() : new Date(s));

  def(Date.prototype, "__bt", "date");
  def(RegExp.prototype, "__bt", "re");
  Date.prototype.tojson = function () { return 'ISODate("' + this.toISOString() + '")'; };

  // --- helpers ---------------------------------------------------------
  const isNumber = (x) => typeof x === "number";
  const isString = (x) => typeof x === "string";
  const isObject = (x) => typeof x === "object" && x !== null;
  function tojson(x, indent) {
    if (x === undefined) return "undefined";
    if (x === null) return "null";
    if (typeof x === "string") return JSON.stringify(x);
    if (typeof x === "number" || typeof x === "boolean") return String(x);
    if (typeof x.tojson === "function") return x.tojson();
    if (typeof x === "function") return x.toString();
    if (Array.isArray(x)) return x.length === 0 ? "[ ]" : "[ " + x.map((v) => tojson(v)).join(", ") + " ]";
    const ks = Object.keys(x);
    if (ks.length === 0) return "{ }";
    return "{ " + ks.map((k) => JSON.stringify(k) + " : " + tojson(x[k])).join(", ") + " }";
  }
  const tojsononeline = tojson;
  const noop = () => {};
  function sleep(ms) {
    const end = Date.now() + Number(ms);
    while (Date.now() < end) { /* the budget's interrupt ends it */ }
  }
  const hex_md5 = (s) => host("md5", String(s));
  Object.bsonsize = (o) => Number(host("bsonsize", o));

  const g = globalThis;
  Object.assign(g, {
    ObjectId, NumberLong, NumberInt, NumberDecimal, Timestamp, BinData, HexData, UUID, MD5,
    MinKey, MaxKey, Code, DBRef, ISODate, isNumber, isString, isObject, tojson, tojsononeline,
    print: noop, printjson: noop, printjsononeline: noop, gc: noop, sleep, hex_md5,
  });
  // Typed values built by the host (Rust), without a JavaScript parse.
  g.__sprig_mk = Object.freeze({
    oid: (h) => new ObjectId(h),
    long: (s) => new NumberLong(s),
    dec: (s) => new NumberDecimal(s),
    date: (ms) => new Date(ms),
    ts: (t, i) => new Timestamp(t, i),
    bin: (sub, b64) => new BinData(sub, b64),
    re: (p, f) => {
      try { return new RegExp(p, f); } catch (e) { return new RegExp(p.replace(/[\\^$.*+?()[\]{}|/]/g, "\\$&"), f); }
    },
    code: (c) => new Code(c),
    minkey: MinKey,
    maxkey: MaxKey,
    undef: undefined,
    // A thrown value, for the error the client sees.
    thrown: (e) => {
      if (typeof e === "string") return [0, e, ""];
      if (e instanceof Error) return [1, String(e.name) + ": " + String(e.message), String(e.stack || "")];
      let s;
      try { s = String(e); } catch (x) { s = "[object]"; }
      return [2, s, ""];
    },
  });
})();
