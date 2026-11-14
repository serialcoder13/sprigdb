// The server-side JavaScript differential corpus: `$where`, `$function`
// and `$accumulator`.
//
// Run with mongosh against a reference MongoDB 8.0 on a private port:
//
//   docker cp gen_cases.js mongo4-ref:/tmp/gen_cases.js
//   docker exec mongo4-ref mongosh --quiet --host 127.0.0.1 --port 47417 \
//       /tmp/gen_cases.js > cases.json
//
// `where` cases run `find(filter)` on a collection holding `docs` and
// record the `_id`s in natural order; `expr` cases run
// `[{$documents: [doc]}, {$replaceWith: {r: expr}}]`; `group` cases run
// `[{$documents: docs}, {$group: group}, {$sort: {_id: 1}}]`. Answers are
// canonical Extended JSON with the server's own types (`promoteValues:
// false`); errors keep their code. `tests/serverjs_diff.rs` runs the same
// cases on SprigDB's engine and QuickJS and compares.

const I = (n) => NumberInt(n);
const L = (n) => NumberLong(String(n));
const D = (x) => Double(x);
const cases = [];
let seq = 0;
const W = (filter, docs) => cases.push({ name: `where#${seq++}`, kind: "where", docs: docs || DOCS, filter });
const E = (expr, doc) => cases.push({ name: `fn#${seq++}`, kind: "expr", doc: doc || DOC, expr });
const G = (group, docs) => cases.push({ name: `acc#${seq++}`, kind: "group", docs: docs || GDOCS, group });
const F = (body, args, extra) => ({ $function: Object.assign({ body, args: args || [], lang: "js" }, extra || {}) });
const A = (spec) => ({ $accumulator: Object.assign({ lang: "js" }, spec) });

const DOCS = [
  { _id: I(1), a: I(1), s: "x", l: L(5), d: D(1.5), arr: [I(1), I(2)], sub: { q: I(1) }, dt: ISODate("2020-01-01T00:00:00Z") },
  { _id: I(2), a: I(5), s: "yy", l: L(7), d: D(-2), arr: [], sub: { q: I(2) } },
  { _id: I(3), a: I(3), s: "", l: L(0), arr: [I(3)], o: ObjectId("5f0000000000000000000001") },
  { _id: I(4), s: "zzz", d: D(0) },
];
const DOC = { _id: I(1), a: I(3), b: D(2.5), l: L(9), s: "abc", arr: [I(1), I(2), I(3)], sub: { q: I(1), r: "t" },
  dt: ISODate("2020-01-01T00:00:00Z"), o: ObjectId("5f0000000000000000000001"), dec: NumberDecimal("1.5"), n: null, t: true };
const GDOCS = [
  { _id: I(1), k: "a", v: I(1) }, { _id: I(2), k: "a", v: I(2) }, { _id: I(3), k: "b", v: I(5) },
  { _id: I(4), k: "b" }, { _id: I(5), k: "c", v: D(2.5) },
];

// --- $where ----------------------------------------------------------------
W({ $where: "this.a > 2" });
W({ $where: "this.a < 2" });
W({ $where: "obj.a == 5" });
W({ $where: "function () { return this.a == 3 }" });
W({ $where: Code("this.s.length > 1") });
W({ $where: "var x = 2; this.a > x" });
W({ $where: "return this.a === 1" });
W({ $where: "this.l == 7" });
W({ $where: "this.l > 4" });
W({ $where: "this.d < 0" });
W({ $where: "this.arr.length > 0" });
W({ $where: "Array.isArray(this.arr) && this.arr.indexOf(3) >= 0" });
W({ $where: "this.sub && this.sub.q == 2" });
W({ $where: "this.dt instanceof Date && this.dt.getUTCFullYear() == 2020" });
W({ $where: "this.o && this.o.str == '5f0000000000000000000001'" });
W({ $where: "this.o instanceof ObjectId" });
W({ $where: "typeof this.l == 'object'" });
W({ $where: "this.s" });
W({ $where: "''" });
W({ $where: "5" });
W({ $where: "function () {}" });
W({ $where: "return {}" });
W({ $where: "this.a = 100; return true" });
W({ s: "yy", $where: "this.a > 0" });
W({ $or: [{ a: I(1) }, { $where: "this.a == 5" }] });
W({ $nor: [{ $where: "this.a == 5" }] });
W({ $and: [{ $where: "this.a > 0" }, { $where: "this.a < 4" }] });
W({ $where: "this.a == undefined" });
W({ $where: "this.missing === undefined" });
W({ $where: "tojson(this.sub) == '{ \"q\" : 1 }'" });
W({ $where: "isNumber(this.a)" });
W({ $where: "Object.keys(this).length > 6" });
W({ $where: "JSON.stringify(this.l) == '{\"$numberLong\":\"5\"}'" });
W({ $where: "hex_md5(this.s) == '9dd4e461268c8034f5c8564e155c67a6'" });
W({ $where: "NumberLong(this.l).toString() == 'NumberLong(7)'" });
// errors
W({ $where: "this.a >" });
W({ $where: "throw new Error('boom')" });
W({ $where: "throw 'x'" });
W({ $where: "null.x" });
W({ $where: "undefinedVar > 1" });
W({ $where: I(5) });
W({ $where: Code("this.a == q", { q: 1 }) });
W({ a: { $where: "true" } });
W({ arr: { $elemMatch: { $where: "true" } } });
W({ $where: "function ( {" });

// --- $function ---------------------------------------------------------------
E(F("function (a) { return a * 2 }", ["$a"]));
E(F("function (a) { return a }", ["$a"]));
E(F("function (b) { return b }", ["$b"]));
E(F("function (l) { return l }", ["$l"]));
E(F("function (l) { return l + 1 }", ["$l"]));
E(F("function (s) { return s.toUpperCase() }", ["$s"]));
E(F("function (arr) { return arr.map(function (x) { return x * x }) }", ["$arr"]));
E(F("function (arr) { return arr }", ["$arr"]));
E(F("function (sub) { return sub }", ["$sub"]));
E(F("function (sub) { sub.z = 2; return sub }", ["$sub"]));
E(F("function (sub) { sub.q = 7; return sub }", ["$sub"]));
E(F("function (sub) { sub.q = 7.5; return sub }", ["$sub"]));
E(F("function (sub) { return {q: sub.q} }", ["$sub"]));
E(F("function (dt) { return dt }", ["$dt"]));
E(F("function (dt) { return dt.getTime() }", ["$dt"]));
E(F("function (dt) { return new Date(dt.getTime() + 86400000) }", ["$dt"]));
E(F("function (o) { return o }", ["$o"]));
E(F("function (o) { return o.str }", ["$o"]));
E(F("function (o) { return o.getTimestamp() }", ["$o"]));
E(F("function (dec) { return dec }", ["$dec"]));
E(F("function (dec) { return dec.toString() }", ["$dec"]));
E(F("function (n, m) { return [n, m, typeof m] }", ["$n", "$missing"]));
E(F("function (t) { return !t }", ["$t"]));
E(F("function () { return NumberInt(7) }"));
E(F("function () { return NumberLong('9007199254740993') }"));
E(F("function () { return NumberDecimal('2.50') }"));
E(F("function () { return Timestamp(5, 2) }"));
E(F("function () { return MinKey }"));
E(F("function () { return MaxKey() }"));
E(F("function () { return BinData(0, 'AQID') }"));
E(F("function () { return [undefined, null, 1] }"));
E(F("function () { return {x: undefined, y: 'v'} }"));
E(F("function () { return new Date(NaN) }"));
E(F("function () { return -0 }"));
E(F("function () { return 2147483648 }"));
E(F("function () { return 1e300 * 10 }"));
E(F("function () { return 0 / 0 }"));
E(F("function () { return 'str' }"));
E(F("function () { return true }"));
E(F("function () { return undefined }"));
E(F("1 + 2"));
E(F("return 7"));
E(F("  function (a) { return a * 3 };  ", ["$a"]));
E(F(Code("function (a) { return a * 4 }"), ["$a"]));
E(F("function (a) { return a }", { $literal: [I(1)] }));
E(F("function () { return arguments.length }", ["$a", "$b", "$s"]));
E(F("function () { return typeof db }"));
E(F("function () { return Object.bsonsize({a: 1}) }"));
E(F("function () { return hex_md5('abc') }"));
E(F("function () { return tojson([1, 'x']) }"));
E(F("function () { return isNumber('1') }"));
E(F("function (a, b) { return Math.max(a, b) }", ["$a", "$b"]));
E({ $add: [F("function (a) { return a }", ["$a"]), I(1)] });
E({ $type: F("function (a) { return a }", ["$a"]) });
E({ $type: F("function () { return NumberInt(1) }") });
// errors
E(F("function () { throw new Error('boom') }"));
E(F("function () { throw new TypeError('bad') }"));
E(F("function () { throw 'x' }"));
E(F("function () { throw {a: 1} }"));
E(F("function () { return undefinedVar }"));
E(F("function ( {"));
E(F("function () { return 5n }"));
E(F("function () { return Symbol('x') }"));
E({ $function: "function () { return 1 }" });
E({ $function: { args: [], lang: "js" } });
E({ $function: { body: "function () {}", lang: "js" } });
E({ $function: { body: "function () {}", args: [] } });
E({ $function: { body: "function () {}", args: [], lang: "py" } });
E({ $function: { body: I(5), args: [], lang: "js" } });
E({ $function: { body: "function () {}", args: "$a", lang: "js" } });

// --- $accumulator ------------------------------------------------------------
const AVG = { init: "function () { return {c: 0, s: 0} }", accumulate: "function (st, v) { return {c: st.c + 1, s: st.s + (v || 0)} }",
  accumulateArgs: ["$v"], merge: "function (a, b) { return {c: a.c + b.c, s: a.s + b.s} }" };
G({ _id: "$k", r: A(Object.assign({}, AVG, { finalize: "function (st) { return st.s / st.c }" })) });
G({ _id: "$k", r: A(AVG) });
G({ _id: null, r: A(AVG), n: { $sum: I(1) } });
G({ _id: "$k", n: { $sum: I(1) }, r: A({ init: "function () { return 0 }", accumulate: "function (st) { return st + 1 }", accumulateArgs: [], merge: "function (a, b) { return a + b }" }), m: { $max: "$v" } });
G({ _id: { k: "$k" }, r: A({ init: "function (k) { return [k] }", initArgs: ["$k"], accumulate: "function (st, v) { st.push(v); return st }", accumulateArgs: ["$v"], merge: "function (a, b) { return a.concat(b) }" }) });
G({ _id: "$k", r: A({ init: "function (k) { return [k] }", initArgs: ["$k"], accumulate: "function (st, v) { st.push(v); return st }", accumulateArgs: ["$v"], merge: "function (a, b) { return a.concat(b) }" }) });
G({ _id: "$k", r: A({ init: "function (k) { return [k] }", initArgs: ["$$ROOT"], accumulate: "function (st) { return st }", accumulateArgs: ["$v"], merge: "function (a) { return a }" }) });
G({ _id: "$k", r: A({ init: "function (a, b) { return a + b }", initArgs: [I(1), I(2)], accumulate: "function (st, v) { return st + 1 }", accumulateArgs: ["$v"], merge: "function (a, b) { return a + b }" }) });
G({ _id: "$k", r: A({ init: "function () { return [] }", accumulate: "function (st, a, b) { st.push(typeof a + typeof b); return st }", accumulateArgs: ["$v", "$nope"], merge: "function (a, b) { return a.concat(b) }" }) });
G({ _id: "$k", r: A({ init: "function () { return {m: new Date(0), n: NumberLong(1)} }", accumulate: "function (st) { return {m: st.m, n: st.n, t: typeof st.n, d: st.m instanceof Date} }", accumulateArgs: ["$v"], merge: "function (a) { return a }" }) });
G({ _id: "$k", r: A({ init: "function () { return 0 }", accumulate: "function (st, v) { return st + 1 }", accumulateArgs: ["$v"], merge: "function (a, b) { return a + b }", finalize: "function (s) {}" }) });
G({ _id: "$k", r: A({ init: "1 + 1", accumulate: "function (st) { return st + 1 }", accumulateArgs: ["$v"], merge: "function (a, b) { return a + b }" }) });
G({ _id: "$k", r: { $accumulator: { init: "function () { return 0 }", accumulate: "function (st) { return st + 1 }", accumulateArgs: ["$v"], merge: "function (a, b) { return a + b }" } } });
G({ _id: "$k", r: A({ init: "function () { return {} }", accumulate: "function (st, v) { st[String(v)] = 1; return st }", accumulateArgs: ["$v"], merge: "function (a) { return a }" }) });
G({ _id: "$k", r: A({ init: "function () { return '' }", accumulate: "function (st, id) { return st + id }", accumulateArgs: [{ $toString: "$_id" }], merge: "function (a, b) { return a + b }" }) });
G({ _id: "$k", r: A({ init: "function () { return null }", accumulate: "function (st, v) { return st === null || v > st ? v : st }", accumulateArgs: ["$v"], merge: "function (a, b) { return a }" }) });
// errors
G({ _id: "$k", r: A(Object.assign({}, AVG, { accumulateArgs: "$v" })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { initArgs: "$k" })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { merge: undefined })) });
G({ _id: "$k", r: A({ accumulate: AVG.accumulate, accumulateArgs: ["$v"], merge: AVG.merge }) });
G({ _id: "$k", r: A({ init: AVG.init, accumulateArgs: ["$v"], merge: AVG.merge }) });
G({ _id: "$k", r: A({ init: AVG.init, accumulate: AVG.accumulate, merge: AVG.merge }) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { lang: "python" })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { lang: I(1) })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { foo: I(1) })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { init: I(5) })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { finalize: I(5) })) });
G({ _id: "$k", r: { $accumulator: I(1) } });
G({ _id: "$k", r: A(Object.assign({}, AVG, { accumulate: "function () { throw 'nope' }" })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { accumulate: "function () { throw new Error('nope') }" })) });
G({ _id: "$k", r: A(Object.assign({}, AVG, { init: "function ( {" })) });

// --- run them -----------------------------------------------------------------------
const tdb = db.getSiblingDB("m4diff");
const out = [];
for (const c of cases) {
  for (const k of Object.keys(c)) if (c[k] === undefined) delete c[k];
  if (c.group && c.group.r && c.group.r.$accumulator) {
    for (const k of Object.keys(c.group.r.$accumulator)) if (c.group.r.$accumulator[k] === undefined) delete c.group.r.$accumulator[k];
  }
  const rec = Object.assign({}, c);
  try {
    if (c.kind === "where") {
      tdb.w.drop();
      tdb.w.insertMany(c.docs);
      rec.expect = { ok: tdb.w.find(c.filter).toArray().map((d) => d._id) };
    } else {
      const pipeline = c.kind === "group"
        ? [{ $documents: c.docs }, { $group: c.group }, { $sort: { _id: 1 } }]
        : [{ $documents: [c.doc] }, { $replaceWith: { r: c.expr } }];
      rec.expect = { ok: db.getSiblingDB("admin").aggregate(pipeline, { promoteValues: false }).toArray() };
    }
  } catch (e) {
    rec.expect = { code: e.code === undefined ? -1 : e.code, errmsg: String(e.errmsg || e.message) };
  }
  out.push(rec);
}
tdb.dropDatabase();
print(EJSON.stringify(out, null, 1, { relaxed: false }));
