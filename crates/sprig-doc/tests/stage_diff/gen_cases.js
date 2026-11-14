// The aggregation-stage differential corpus: window functions,
// accumulators and stages.
//
// Run with mongosh against a reference MongoDB 8.0 on a private port:
//
//   docker cp gen_cases.js mongo2-ref:/tmp/gen_cases.js
//   docker exec mongo2-ref mongosh --quiet --host 127.0.0.1 --port 47217 \
//       /tmp/gen_cases.js > cases.json
//
// Each case is a pipeline over `docs`, run as
// `db.aggregate([{$documents: docs}, ...pipeline])` (a `db` case runs its
// pipeline as written, `$documents` included). A case with `colls` first
// fills those collections (for `$graphLookup`). The answer (documents, or
// the error's code and message) is recorded next to the case as canonical
// Extended JSON. `tests/stage_diff.rs` runs the same cases with SprigDB's
// in-gateway stages and compares.

const I = (n) => NumberInt(n);
const L = (n) => NumberLong(String(n));
const D = (x) => Double(x);
const DEC = (s) => NumberDecimal(s);
const DT = (s) => ISODate(s);

const cases = [];
let seq = 0;
function add(area, docs, pipeline, extra) {
  cases.push(Object.assign({ name: `${area}#${seq++}`, docs, pipeline }, extra || {}));
}

// --- data sets -----------------------------------------------------------

// Sales: partitions a/b/c, mixed numeric types, null and missing values.
const S = [
  { _id: I(1), g: "a", t: I(1), x: I(5), y: I(2), s: "p" },
  { _id: I(2), g: "a", t: I(2), x: I(3), y: I(4), s: "q" },
  { _id: I(3), g: "b", t: I(2), x: D(2.5), y: I(1), s: "r" },
  { _id: I(4), g: "a", t: I(4), x: L(7), y: D(0.5) },
  { _id: I(5), g: "b", t: I(5), x: null, y: I(3), s: "s" },
  { _id: I(6), g: "c", t: I(6), x: DEC("1.5"), y: I(9), s: "t" },
  { _id: I(7), g: "a", t: I(7), y: I(6), s: "u" },
  { _id: I(8), g: "b", t: I(9), x: I(-4), y: I(2), s: "v" },
  { _id: I(9), g: "c", t: I(10), x: I(11), y: L(8), s: "w" },
  { _id: I(10), g: "a", t: I(12), x: "str", y: I(1), s: "x" },
  { _id: I(11), g: "b", t: I(13), x: I(8), y: D(7.25), s: "y" },
  { _id: I(12), g: "a", t: I(15), x: I(5), y: I(5), s: "z" },
];
// Integers only (exact arithmetic everywhere).
const N = [];
for (let i = 1; i <= 10; i++) N.push({ _id: I(i), g: i % 3 === 0 ? "k" : "j", t: I(i * 2), x: I((i * 7) % 11), y: I(i) });
// Doubles that are not exact in binary.
const F = [];
for (let i = 1; i <= 8; i++) F.push({ _id: I(i), t: I(i), x: D(i / 10), y: D(i * 0.3) });
// Hourly time series with gaps and nulls.
const TS = [
  { _id: I(1), k: "s1", ts: DT("2024-03-01T00:00:00Z"), v: I(10) },
  { _id: I(2), k: "s1", ts: DT("2024-03-01T01:00:00Z"), v: null },
  { _id: I(3), k: "s1", ts: DT("2024-03-01T02:00:00Z"), v: I(16) },
  { _id: I(4), k: "s1", ts: DT("2024-03-01T05:00:00Z"), v: I(22) },
  { _id: I(5), k: "s2", ts: DT("2024-03-01T00:30:00Z"), v: D(1.5) },
  { _id: I(6), k: "s2", ts: DT("2024-03-01T03:30:00Z") },
  { _id: I(7), k: "s2", ts: DT("2024-03-01T04:30:00Z"), v: D(4.5) },
  { _id: I(8), k: "s1", ts: DT("2024-03-01T06:00:00Z"), v: null },
];
// Numbers on a line for $densify/$fill/$linearFill.
const LN = [
  { _id: I(1), p: "u", n: I(0), v: I(0) },
  { _id: I(2), p: "u", n: I(3), v: null },
  { _id: I(3), p: "u", n: I(4), v: I(8) },
  { _id: I(4), p: "w", n: I(1), v: D(1) },
  { _id: I(5), p: "w", n: I(7), v: null },
  { _id: I(6), p: "w", n: I(9) },
  { _id: I(7), p: "w", n: I(10), v: D(5) },
];
const sortT = { $sort: { _id: 1 } };

// --- $group accumulators ---------------------------------------------------
const G = (area, docs, acc, more) => add(area, docs, [{ $group: Object.assign({ _id: "$g" }, acc) }, sortT].concat(more || []));
for (const ds of [["S", S], ["N", N]]) {
  const [nm, docs] = ds;
  G(`group.top.${nm}`, docs, { r: { $top: { sortBy: { x: 1 }, output: "$_id" } } });
  G(`group.top.${nm}`, docs, { r: { $top: { sortBy: { x: -1, _id: 1 }, output: ["$x", "$_id"] } } });
  G(`group.bottom.${nm}`, docs, { r: { $bottom: { sortBy: { x: 1 }, output: "$_id" } } });
  G(`group.bottom.${nm}`, docs, { r: { $bottom: { sortBy: { y: -1 }, output: { x: "$x", y: "$y" } } } });
  G(`group.topN.${nm}`, docs, { r: { $topN: { n: 2, sortBy: { x: 1 }, output: "$_id" } } });
  G(`group.topN.${nm}`, docs, { r: { $topN: { n: 10, sortBy: { y: -1 }, output: "$x" } } });
  G(`group.bottomN.${nm}`, docs, { r: { $bottomN: { n: 3, sortBy: { x: 1, _id: -1 }, output: "$_id" } } });
  G(`group.firstN.${nm}`, docs, { r: { $firstN: { n: 2, input: "$x" } } });
  G(`group.lastN.${nm}`, docs, { r: { $lastN: { n: 3, input: "$x" } } });
  G(`group.maxN.${nm}`, docs, { r: { $maxN: { n: 2, input: "$x" } } });
  G(`group.minN.${nm}`, docs, { r: { $minN: { n: 3, input: "$x" } } });
  G(`group.median.${nm}`, docs, { r: { $median: { input: "$x", method: "approximate" } } });
  G(`group.percentile.${nm}`, docs, { r: { $percentile: { input: "$x", p: [0, 0.2, 0.5, 0.95, 1], method: "approximate" } } });
  G(`group.stddev.${nm}`, docs, { p: { $stdDevPop: "$x" }, s: { $stdDevSamp: "$x" } });
  G(`group.stddev.${nm}`, docs, { p: { $stdDevPop: "$y" }, s: { $stdDevSamp: "$y" } });
  G(`group.sumavg.${nm}`, docs, { s: { $sum: "$x" }, a: { $avg: "$x" }, sy: { $sum: "$y" }, ay: { $avg: "$y" } });
  G(`group.basic.${nm}`, docs, { f: { $first: "$x" }, l: { $last: "$x" }, mn: { $min: "$x" }, mx: { $max: "$x" }, c: { $count: {} } });
  G(`group.push.${nm}`, docs, { p: { $push: "$x" }, s: { $addToSet: "$y" } }, [{ $project: { p: 1, n: { $size: "$s" } } }]);
}
G("group.topN.nexpr", S, { r: { $topN: { n: { $cond: [{ $eq: ["$_id", "a"] }, 1, 3] }, sortBy: { t: -1 }, output: "$t" } } });
G("group.firstN.nexpr", S, { r: { $firstN: { n: { $cond: [{ $eq: ["$_id", "b"] }, 1, 2] }, input: "$t" } } });
G("group.maxN.mixed", S, { r: { $maxN: { n: 4, input: "$s" } } });
G("group.minN.arr", S, { r: { $minN: { n: 2, input: ["$x", "$y"] } } });
for (const n of [0, -1, 2.5, "x", null, L(2), D(2), DEC("2"), 1e10]) {
  G("group.topN.n", S, { r: { $topN: { n, sortBy: { x: 1 }, output: "$x" } } });
  G("group.firstN.n", S, { r: { $firstN: { n, input: "$x" } } });
}
G("group.firstN.nfield", S, { r: { $firstN: { n: "$t", input: "$x" } } });
G("group.top.err", S, { r: { $top: { output: "$x" } } });
G("group.top.err", S, { r: { $top: { sortBy: { x: 1 } } } });
G("group.top.err", S, { r: { $top: { n: 1, sortBy: { x: 1 }, output: "$x" } } });
G("group.top.err", S, { r: { $top: { sortBy: { x: 1 }, output: "$x", extra: 1 } } });
G("group.top.err", S, { r: { $top: "$x" } });
G("group.topN.err", S, { r: { $topN: { sortBy: { x: 1 }, output: "$x" } } });
G("group.topN.err", S, { r: { $topN: { n: 2, sortBy: { x: 2 }, output: "$x" } } });
G("group.firstN.err", S, { r: { $firstN: { input: "$x" } } });
G("group.firstN.err", S, { r: { $firstN: { n: 2 } } });
G("group.firstN.err", S, { r: { $firstN: [1, 2] } });
G("group.maxN.err", S, { r: { $maxN: { n: 2, input: "$x", z: 1 } } });
G("group.median.err", S, { r: { $median: { input: "$x" } } });
G("group.median.err", S, { r: { $median: { input: "$x", method: "exact" } } });
G("group.median.err", S, { r: { $median: "$x" } });
G("group.percentile.err", S, { r: { $percentile: { input: "$x", p: [1.5], method: "approximate" } } });
G("group.percentile.err", S, { r: { $percentile: { input: "$x", p: [], method: "approximate" } } });
G("group.percentile.err", S, { r: { $percentile: { input: "$x", p: 0.5, method: "approximate" } } });
G("group.percentile.err", S, { r: { $percentile: { input: "$x", p: ["a"], method: "approximate" } } });
G("group.percentile.err", S, { r: { $percentile: { input: "$x", method: "approximate" } } });
G("group.percentile.pexpr", S, { r: { $percentile: { input: "$y", p: { $literal: [0.25, 0.75] }, method: "approximate" } } });
G("group.percentile.arr", S, { r: { $percentile: { input: ["$x", "$y"], p: [0.5], method: "approximate" } } });
G("group.median.dec", [{ _id: 1, g: 1, x: DEC("1.25") }, { _id: 2, g: 1, x: DEC("3.5") }, { _id: 3, g: 1, x: L(9) }], { r: { $median: { input: "$x", method: "approximate" } } });
G("group.stddev.one", [{ _id: 1, g: 1, x: I(4) }], { p: { $stdDevPop: "$x" }, s: { $stdDevSamp: "$x" } });
G("group.stddev.none", [{ _id: 1, g: 1, x: "a" }], { p: { $stdDevPop: "$x" }, s: { $stdDevSamp: "$x" } });
G("group.stddev.dec", [{ _id: 1, g: 1, x: DEC("1.1") }, { _id: 2, g: 1, x: DEC("2.2") }, { _id: 3, g: 1, x: I(7) }], { p: { $stdDevPop: "$x" }, s: { $stdDevSamp: "$x" } });
G("group.stddev.inf", [{ _id: 1, g: 1, x: I(1) }, { _id: 2, g: 1, x: D(Infinity) }], { p: { $stdDevPop: "$x" }, s: { $stdDevSamp: "$x" } });
G("group.stddev.nan", [{ _id: 1, g: 1, x: I(1) }, { _id: 2, g: 1, x: D(NaN) }, { _id: 3, g: 1, x: I(3) }], { p: { $stdDevPop: "$x" } });
G("group.stddev.arr", S, { p: { $stdDevPop: ["$x", "$y"] } });
G("group.sum.f", F.map((d) => Object.assign({ g: 1 }, d)), { s: { $sum: "$x" }, a: { $avg: "$x" }, sy: { $sum: "$y" }, p: { $stdDevPop: "$y" } });
G("group.sum.overflow", [{ _id: 1, g: 1, x: I(2147483647) }, { _id: 2, g: 1, x: I(1) }], { s: { $sum: "$x" }, a: { $avg: "$x" } });
G("group.sum.overflow", [{ _id: 1, g: 1, x: L("9223372036854775807") }, { _id: 2, g: 1, x: I(1) }], { s: { $sum: "$x" }, a: { $avg: "$x" } });
G("group.sum.dec", [{ _id: 1, g: 1, x: DEC("0.1") }, { _id: 2, g: 1, x: D(0.2) }, { _id: 3, g: 1, x: I(3) }], { s: { $sum: "$x" }, a: { $avg: "$x" } });
G("group.sum.dec", [{ _id: 1, g: 1, x: DEC("1") }, { _id: 2, g: 1, x: DEC("2") }, { _id: 3, g: 1, x: DEC("4") }], { s: { $sum: "$x" }, a: { $avg: "$x" } });
G("group.mergeObjects", [{ _id: 1, g: 1, o: { a: 1, b: 2 } }, { _id: 2, g: 1, o: null }, { _id: 3, g: 1 }, { _id: 4, g: 1, o: { b: 3, c: { d: 1 } } }], { m: { $mergeObjects: "$o" } });
G("group.mergeObjects.err", [{ _id: 1, g: 1, o: { a: 1 } }, { _id: 2, g: 1, o: 5 }], { m: { $mergeObjects: "$o" } });
G("group.unknown", S, { m: { $foo: "$x" } });

// --- $setWindowFields --------------------------------------------------------
const W = (area, docs, spec, more) => add(area, docs, [{ $setWindowFields: spec }].concat(more || []));
const WINS = [
  undefined,
  { documents: ["unbounded", "current"] },
  { documents: ["unbounded", "unbounded"] },
  { documents: [-1, 1] },
  { documents: [-2, 0] },
  { documents: [0, 0] },
  { documents: [1, 3] },
  { documents: ["current", "unbounded"] },
  { documents: [-3, -2] },
  { range: [-2, 2] },
  { range: ["unbounded", 0] },
  { range: [0, 3] },
  { range: [-1, 1] },
  { range: ["current", "unbounded"] },
];
const ACCS = [
  ["sum", { $sum: "$x" }], ["avg", { $avg: "$x" }], ["min", { $min: "$x" }], ["max", { $max: "$x" }],
  ["first", { $first: "$x" }], ["last", { $last: "$x" }], ["push", { $push: "$x" }], ["count", { $count: {} }],
  ["addToSet", { $addToSet: "$y" }],
  ["stdDevPop", { $stdDevPop: "$x" }], ["stdDevSamp", { $stdDevSamp: "$x" }],
  ["topN", { $topN: { n: 2, sortBy: { y: 1 }, output: "$_id" } }], ["bottom", { $bottom: { sortBy: { y: 1 }, output: "$_id" } }],
  ["firstN", { $firstN: { n: 2, input: "$x" } }], ["lastN", { $lastN: { n: 2, input: "$x" } }],
  ["maxN", { $maxN: { n: 2, input: "$x" } }], ["minN", { $minN: { n: 2, input: "$x" } }],
  ["median", { $median: { input: "$x", method: "approximate" } }],
  ["percentile", { $percentile: { input: "$x", p: [0.1, 0.9], method: "approximate" } }],
];
// Every accumulator over three windows on mixed data, every window on two.
for (const [an, acc] of ACCS) {
  for (const w of [undefined, { documents: [-1, 1] }, { range: [-2, 0] }]) {
    const o = Object.assign({}, acc);
    if (w) o.window = w;
    W(`win.${an}`, S, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: o } });
  }
}
for (const w of WINS) {
  for (const [an, acc] of [["sum", { $sum: "$x" }], ["avg", { $avg: "$y" }], ["push", { $push: "$_id" }]]) {
    const o = Object.assign({}, acc);
    if (w) o.window = w;
    W(`win.bounds.${an}`, N, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: o } });
  }
}
W("win.sum.f", F, { sortBy: { t: 1 }, output: { a: { $sum: "$x", window: { documents: [-2, 0] } }, b: { $avg: "$y", window: { documents: [-1, 1] } }, c: { $stdDevSamp: "$y", window: { documents: [-3, 0] } } } });
W("win.sum.long", [{ _id: 1, t: 1, x: L(5) }, { _id: 2, t: 2, x: L(7) }, { _id: 3, t: 3, x: I(1) }], { sortBy: { t: 1 }, output: { a: { $sum: "$x", window: { documents: [-1, 0] } }, b: { $sum: "$x", window: { documents: ["unbounded", "current"] } }, c: { $sum: "$x" }, d: { $avg: "$x", window: { documents: [-1, 0] } } } });
W("win.sum.overflow", [{ _id: 1, t: 1, x: I(2147483647) }, { _id: 2, t: 2, x: I(5) }, { _id: 3, t: 3, x: L("9223372036854775807") }], { sortBy: { t: 1 }, output: { a: { $sum: "$x", window: { documents: [-1, 0] } }, b: { $sum: "$x", window: { documents: ["unbounded", "current"] } } } });
W("win.nopart", S, { sortBy: { t: -1 }, output: { r: { $sum: "$y", window: { documents: ["unbounded", "current"] } } } });
W("win.nosort", S, { partitionBy: "$g", output: { r: { $push: "$_id" } } });
W("win.nosort", S, { output: { r: { $sum: "$y" } } });
W("win.partexpr", S, { partitionBy: { $mod: ["$t", 3] }, sortBy: { _id: 1 }, output: { r: { $push: "$_id", window: { documents: [-1, 0] } } } });
W("win.partmissing", S, { partitionBy: "$s", sortBy: { _id: 1 }, output: { r: { $count: {} } } });
W("win.dotted", S, { partitionBy: "$g", sortBy: { t: 1 }, output: { "o.r": { $sum: "$y" }, "s": { $max: "$t" } } });
W("win.multi", S, { partitionBy: "$g", sortBy: { t: 1 }, output: { a: { $sum: "$y" }, b: { $rank: {} }, c: { $shift: { output: "$_id", by: 1 } } } });
W("win.sortdesc", N, { partitionBy: "$g", sortBy: { x: -1, _id: 1 }, output: { r: { $push: "$_id", window: { documents: [-1, 1] } } } });
W("win.sortmissing", S, { sortBy: { x: 1, _id: 1 }, output: { r: { $documentNumber: {} } } });
// Rank functions.
for (const [nm, fn] of [["rank", { $rank: {} }], ["denseRank", { $denseRank: {} }], ["documentNumber", { $documentNumber: {} }]]) {
  W(`win.${nm}`, S, { partitionBy: "$g", sortBy: { y: 1 }, output: { r: fn } }, [sortT]);
  W(`win.${nm}`, S, { sortBy: { x: -1 }, output: { r: fn } }, [sortT]);
  W(`win.${nm}.err`, S, { output: { r: fn } });
  W(`win.${nm}.err`, S, { sortBy: { x: 1, y: 1 }, output: { r: fn } });
  W(`win.${nm}.err`, S, { sortBy: { x: 1 }, output: { r: Object.assign({ window: { documents: [-1, 1] } }, fn) } });
}
W("win.rank.err", S, { sortBy: { x: 1 }, output: { r: { $rank: { a: 1 } } } });
W("win.rank.arr", [{ _id: 1, x: [1, 2] }, { _id: 2, x: I(1) }], { sortBy: { x: 1 }, output: { r: { $rank: {} } } });
W("win.rank.dates", TS, { sortBy: { ts: 1 }, output: { r: { $denseRank: {} } } });
// $shift.
for (const by of [1, -1, 0, 2, -5, 10]) W("win.shift", S, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $shift: { output: "$x", by } } } });
W("win.shift.default", S, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $shift: { output: "$s", by: 1, default: "none" } } } });
W("win.shift.expr", S, { sortBy: { t: 1 }, output: { r: { $shift: { output: { $add: ["$y", 1] }, by: -1, default: 0 } } } });
W("win.shift.err", S, { sortBy: { t: 1 }, output: { r: { $shift: { output: "$x" } } } });
W("win.shift.err", S, { sortBy: { t: 1 }, output: { r: { $shift: { by: 1 } } } });
W("win.shift.err", S, { output: { r: { $shift: { output: "$x", by: 1 } } } });
W("win.shift.err", S, { sortBy: { t: 1 }, output: { r: { $shift: { output: "$x", by: 1.5 } } } });
W("win.shift.err", S, { sortBy: { t: 1 }, output: { r: { $shift: { output: "$x", by: 1, default: "$y" } } } });
W("win.shift.err", S, { sortBy: { t: 1 }, output: { r: { $shift: { output: "$x", by: 1 }, window: { documents: [-1, 1] } } } });
// $derivative and $integral.
W("win.derivative", N, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $derivative: { input: "$x" }, window: { documents: [-1, 0] } } } });
W("win.derivative", N, { sortBy: { t: 1 }, output: { r: { $derivative: { input: "$x" }, window: { range: [-4, 0] } } } });
W("win.derivative", TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { r: { $derivative: { input: "$v", unit: "hour" }, window: { documents: [-1, 0] } } } });
W("win.derivative", TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { r: { $derivative: { input: "$v", unit: "minute" }, window: { range: [-2, 0], unit: "hour" } } } });
W("win.derivative.err", N, { sortBy: { t: 1 }, output: { r: { $derivative: { input: "$x" } } } });
W("win.derivative.err", TS, { sortBy: { ts: 1 }, output: { r: { $derivative: { input: "$v" }, window: { documents: [-1, 0] } } } });
W("win.derivative.err", N, { sortBy: { t: 1 }, output: { r: { $derivative: { input: "$x", unit: "hour" }, window: { documents: [-1, 0] } } } });
W("win.derivative.err", N, { output: { r: { $derivative: { input: "$x" }, window: { documents: [-1, 0] } } } });
W("win.derivative.err", N, { sortBy: { t: 1 }, output: { r: { $derivative: "$x", window: { documents: [-1, 0] } } } });
W("win.integral", N, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $integral: { input: "$x" }, window: { documents: ["unbounded", "current"] } } } });
W("win.integral", N, { sortBy: { t: 1 }, output: { r: { $integral: { input: "$y" }, window: { range: [-4, 0] } } } });
W("win.integral", N, { sortBy: { t: 1 }, output: { r: { $integral: { input: "$y" } } } });
W("win.integral", TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { r: { $integral: { input: "$v", unit: "hour" }, window: { documents: [-1, 0] } } } });
W("win.integral", TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { r: { $integral: { input: "$v", unit: "second" }, window: { range: [-3, 0], unit: "hour" } } } });
W("win.integral", F, { sortBy: { t: 1 }, output: { r: { $integral: { input: "$y" }, window: { documents: [-2, 0] } } } });
W("win.integral.err", TS, { sortBy: { ts: 1 }, output: { r: { $integral: { input: "$v" } } } });
W("win.integral.err", N, { sortBy: { t: 1, x: 1 }, output: { r: { $integral: { input: "$x" } } } });
// $expMovingAvg.
W("win.ema", N, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 2 } } } });
W("win.ema", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", alpha: 0.75 } } } });
W("win.ema", S, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 3 } } } });
W("win.ema", F, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$y", alpha: 0.3 } } } });
W("win.ema.dec", [{ _id: 1, t: 1, x: DEC("10") }, { _id: 2, t: 2, x: I(4) }, { _id: 3, t: 3, x: D(1.5) }], { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 2 } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 2, alpha: 0.5 } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x" } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 0 } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", alpha: 1 } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 2.5 } } } });
W("win.ema.err", N, { output: { r: { $expMovingAvg: { input: "$x", N: 2 } } } });
W("win.ema.err", N, { sortBy: { t: 1 }, output: { r: { $expMovingAvg: { input: "$x", N: 2 }, window: { documents: [-1, 0] } } } });
// Covariance.
for (const w of [undefined, { documents: [-2, 0] }, { range: [-4, 4] }, { documents: ["unbounded", "current"] }]) {
  for (const op of ["$covariancePop", "$covarianceSamp"]) {
    const o = { [op]: ["$x", "$y"] };
    if (w) o.window = w;
    W(`win.cov`, N, { partitionBy: "$g", sortBy: { t: 1 }, output: { r: o } });
    W(`win.cov.S`, S, { sortBy: { t: 1 }, output: { r: o } });
  }
}
W("win.cov.err", N, { sortBy: { t: 1 }, output: { r: { $covariancePop: ["$x"] } } });
W("win.cov.err", N, { sortBy: { t: 1 }, output: { r: { $covariancePop: "$x" } } });
W("win.cov.f", F, { sortBy: { t: 1 }, output: { r: { $covarianceSamp: ["$x", "$y"], window: { documents: [-2, 1] } } } });
// $linearFill and $locf.
W("win.linearFill", LN, { partitionBy: "$p", sortBy: { n: 1 }, output: { r: { $linearFill: "$v" } } });
W("win.linearFill", TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { v: { $linearFill: "$v" } } });
W("win.linearFill", [{ _id: 1, n: 1, v: null }, { _id: 2, n: 2, v: I(4) }, { _id: 3, n: 3 }, { _id: 4, n: 5, v: I(5) }, { _id: 5, n: 6 }], { sortBy: { n: 1 }, output: { v: { $linearFill: "$v" } } });
W("win.linearFill.dec", [{ _id: 1, n: 1, v: DEC("1") }, { _id: 2, n: 2 }, { _id: 3, n: 4, v: DEC("2") }], { sortBy: { n: 1 }, output: { v: { $linearFill: "$v" } } });
W("win.linearFill.err", LN, { output: { r: { $linearFill: "$v" } } });
W("win.linearFill.err", LN, { sortBy: { n: 1, _id: 1 }, output: { r: { $linearFill: "$v" } } });
W("win.linearFill.err", [{ _id: 1, n: 1, v: I(1) }, { _id: 2, n: 1 }, { _id: 3, n: 2, v: I(3) }], { sortBy: { n: 1 }, output: { r: { $linearFill: "$v" } } });
W("win.linearFill.err", [{ _id: 1, n: "a", v: I(1) }, { _id: 2, n: "b" }], { sortBy: { n: 1 }, output: { r: { $linearFill: "$v" } } });
W("win.linearFill.err", [{ _id: 1, n: 1, v: "a" }, { _id: 2, n: 2 }, { _id: 3, n: 3, v: I(3) }], { sortBy: { n: 1 }, output: { r: { $linearFill: "$v" } } });
W("win.linearFill.err", LN, { sortBy: { n: 1 }, output: { r: { $linearFill: "$v", window: { documents: [-1, 1] } } } });
W("win.locf", LN, { partitionBy: "$p", sortBy: { n: 1 }, output: { r: { $locf: "$v" } } });
W("win.locf", TS, { sortBy: { ts: 1 }, output: { v: { $locf: "$v" } } });
W("win.locf", S, { sortBy: { t: 1 }, output: { x: { $locf: "$x" } } });
W("win.locf.err", LN, { sortBy: { n: 1 }, output: { r: { $locf: "$v", window: { documents: [-1, 1] } } } });
W("win.locf.nosort", LN, { output: { r: { $locf: "$v" } } });
// Range windows on dates.
for (const unit of ["hour", "minute", "day", "second", "millisecond", "week"]) {
  W(`win.range.${unit}`, TS, { partitionBy: "$k", sortBy: { ts: 1 }, output: { r: { $push: "$_id", window: { range: [-2, 0], unit } } } });
}
W("win.range.month", TS, { sortBy: { ts: 1 }, output: { r: { $count: {}, window: { range: [-1, 0], unit: "month" } } } });
W("win.range.dateunbounded", TS, { sortBy: { ts: 1 }, output: { r: { $sum: "$v", window: { range: ["unbounded", "current"], unit: "hour" } } } });
W("win.range.err", TS, { sortBy: { ts: 1 }, output: { r: { $sum: "$v", window: { range: [-2, 0] } } } });
W("win.range.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { range: [-2, 0], unit: "hour" } } } });
W("win.range.err", N, { sortBy: { t: 1, x: 1 }, output: { r: { $sum: "$x", window: { range: [-2, 0] } } } });
W("win.range.err", N, { output: { r: { $sum: "$x", window: { range: [-2, 0] } } } });
W("win.range.err", S, { sortBy: { s: 1 }, output: { r: { $sum: "$x", window: { range: [-2, 0] } } } });
W("win.range.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { range: [2, 0] } } } });
W("win.range.dec", N, { sortBy: { t: 1 }, output: { r: { $push: "$_id", window: { range: [DEC("-2.5"), D(1.5)] } } } });
W("win.range.nonnum", [{ _id: 1, t: 1 }, { _id: 2, t: "a" }], { sortBy: { t: 1 }, output: { r: { $sum: 1, window: { range: [-1, 0] } } } });
W("win.range.null", [{ _id: 1, t: 1 }, { _id: 2, t: null }, { _id: 3 }], { sortBy: { t: 1 }, output: { r: { $sum: 1, window: { range: [-1, 0] } } } });
// Window spec errors.
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: [1, -1] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: [-1.5, 0] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: [-1] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: ["a", 0] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: [-1, 0], range: [-1, 0] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { documents: [-1, 0], unit: "hour" } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: { foo: 1 } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", window: {} } } });
W("win.err", N, { output: { r: { $sum: "$x", window: { documents: [-1, 0] } } } });
W("win.err", N, { output: { r: { $sum: "$x", window: { documents: ["unbounded", "unbounded"] } } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $foo: "$x" } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x", $avg: "$x" } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: 1 } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: {} } });
W("win.err", N, { sortBy: { t: 1 } });
W("win.err", N, { sortBy: { t: 1 }, output: 5 });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $sum: "$x" } }, foo: 1 });
W("win.err", N, { sortBy: { t: 2 }, output: { r: { $sum: "$x" } } });
W("win.err", N, { sortBy: "t", output: { r: { $sum: "$x" } } });
W("win.err", N, { sortBy: { t: 1 }, output: { "$r": { $sum: "$x" } } });
W("win.err", N, { sortBy: { t: 1 }, output: { r: { $add: ["$x", 1] } } });
add("win.err", N, [{ $setWindowFields: 5 }]);
W("win.partarr", [{ _id: 1, g: [1, 2] }], { partitionBy: "$g", output: { r: { $sum: 1 } } });
W("win.partobj", [{ _id: 1, g: { a: 1 } }, { _id: 2, g: { a: 1 } }, { _id: 3, g: { a: 2 } }], { partitionBy: "$g", output: { r: { $sum: 1 } } });
W("win.partnull", [{ _id: 1, g: null }, { _id: 2 }, { _id: 3, g: I(0) }], { partitionBy: "$g", output: { r: { $push: "$_id" } } });

// --- $bucket and $bucketAuto -------------------------------------------------
const B = (area, docs, spec, more) => add(area, docs, [{ $bucket: spec }].concat(more || []));
B("bucket", N, { groupBy: "$x", boundaries: [0, 3, 6, 11] });
B("bucket", N, { groupBy: "$x", boundaries: [0, 5, 20], output: { c: { $sum: 1 }, ids: { $push: "$_id" }, a: { $avg: "$y" } } });
B("bucket", S, { groupBy: "$x", boundaries: [0, 5, 10], default: "other" });
B("bucket", S, { groupBy: "$y", boundaries: [1, 2, 4, 8], default: I(100), output: { n: { $sum: 1 } } });
B("bucket", S, { groupBy: "$s", boundaries: ["p", "s", "w"], default: "zz" });
B("bucket", TS, { groupBy: "$ts", boundaries: [DT("2024-03-01T00:00:00Z"), DT("2024-03-01T03:00:00Z"), DT("2024-03-02T00:00:00Z")] });
B("bucket", N, { groupBy: { $multiply: ["$x", 2] }, boundaries: [0, 10, 25] });
B("bucket", S, { groupBy: "$x", boundaries: [L(0), D(4.5), DEC("12")], default: "d" });
B("bucket.err", S, { groupBy: "$x", boundaries: [0, 5, 10] });
B("bucket.err", N, { groupBy: "$x", boundaries: [5, 0] });
B("bucket.err", N, { groupBy: "$x", boundaries: [0] });
B("bucket.err", N, { groupBy: "$x", boundaries: [0, "a"] });
B("bucket.err", N, { groupBy: "$x", boundaries: [0, 5, 20], default: 3 });
B("bucket.err", N, { boundaries: [0, 5] });
B("bucket.err", N, { groupBy: "$x" });
B("bucket.err", N, { groupBy: "x", boundaries: [0, 5] });
B("bucket.err", N, { groupBy: "$x", boundaries: [0, 20], foo: 1 });
B("bucket.err", N, { groupBy: "$x", boundaries: [0, 20], output: { c: 1 } });
B("bucket.err", N, { groupBy: "$x", boundaries: [0, 0] });
const BA = (area, docs, spec, more) => add(area, docs, [{ $bucketAuto: spec }].concat(more || []));
for (const n of [1, 2, 3, 4, 7, 20]) BA("bucketAuto", N, { groupBy: "$x", buckets: n });
BA("bucketAuto", N, { groupBy: "$y", buckets: 3, output: { ids: { $push: "$_id" }, s: { $sum: "$x" } } });
BA("bucketAuto", S, { groupBy: "$x", buckets: 3 });
BA("bucketAuto", S, { groupBy: "$s", buckets: 4 });
BA("bucketAuto", TS, { groupBy: "$ts", buckets: 3 });
BA("bucketAuto.ties", [1, 1, 1, 1, 2, 2, 3, 4, 5, 5].map((x, i) => ({ _id: I(i), x: I(x) })), { groupBy: "$x", buckets: 4 });
for (const gr of ["R5", "R10", "R20", "R40", "R80", "1-2-5", "E6", "E12", "E24", "E48", "E96", "E192", "POWERSOF2"]) {
  BA(`bucketAuto.gran.${gr}`, N, { groupBy: { $multiply: ["$x", 13] }, buckets: 3, granularity: gr });
}
BA("bucketAuto.gran", F, { groupBy: "$x", buckets: 3, granularity: "R10" });
BA("bucketAuto.gran", [{ _id: 1, x: I(0) }, { _id: 2, x: I(5) }], { groupBy: "$x", buckets: 2, granularity: "POWERSOF2" });
BA("bucketAuto.err", N, { groupBy: "$x", buckets: 0 });
BA("bucketAuto.err", N, { groupBy: "$x", buckets: 2.5 });
BA("bucketAuto.err", N, { groupBy: "$x" });
BA("bucketAuto.err", N, { buckets: 2 });
BA("bucketAuto.err", N, { groupBy: "$x", buckets: 2, granularity: "R7" });
BA("bucketAuto.err", S, { groupBy: "$s", buckets: 2, granularity: "R5" });
BA("bucketAuto.err", [{ _id: 1, x: I(-1) }, { _id: 2, x: I(5) }], { groupBy: "$x", buckets: 2, granularity: "R5" });
BA("bucketAuto.err", N, { groupBy: "$x", buckets: 2, foo: 1 });
BA("bucketAuto.null", [{ _id: 1, x: null }, { _id: 2 }, { _id: 3, x: I(4) }], { groupBy: "$x", buckets: 2 });

// --- $densify ------------------------------------------------------------------
const DN = (area, docs, spec, more) => add(area, docs, [{ $densify: spec }].concat(more || []));
DN("densify", LN, { field: "n", range: { step: 1, bounds: "full" } });
DN("densify", LN, { field: "n", partitionByFields: ["p"], range: { step: 1, bounds: "partition" } });
DN("densify", LN, { field: "n", partitionByFields: ["p"], range: { step: 2, bounds: "full" } });
DN("densify", LN, { field: "n", range: { step: 3, bounds: [2, 12] } });
DN("densify", LN, { field: "n", partitionByFields: ["p"], range: { step: 2, bounds: [-1, 6] } });
DN("densify", LN, { field: "n", range: { step: D(1.5), bounds: "full" } });
DN("densify", TS, { field: "ts", range: { step: 1, unit: "hour", bounds: "full" } });
DN("densify", TS, { field: "ts", partitionByFields: ["k"], range: { step: 1, unit: "hour", bounds: "partition" } });
DN("densify", TS, { field: "ts", range: { step: 90, unit: "minute", bounds: [DT("2024-03-01T00:00:00Z"), DT("2024-03-01T06:00:00Z")] } });
DN("densify", TS, { field: "ts", range: { step: 1, unit: "day", bounds: [DT("2024-02-28T00:00:00Z"), DT("2024-03-02T00:00:00Z")] } });
DN("densify", [{ _id: 1, d: DT("2024-01-31T00:00:00Z") }, { _id: 2, d: DT("2024-06-15T00:00:00Z") }], { field: "d", range: { step: 1, unit: "month", bounds: "full" } });
DN("densify", [{ _id: 1, a: { n: I(1) } }, { _id: 2, a: { n: I(4) } }], { field: "a.n", range: { step: 1, bounds: "full" } });
DN("densify", [{ _id: 1, n: I(1) }, { _id: 2, n: null }, { _id: 3 }, { _id: 4, n: I(3) }], { field: "n", range: { step: 1, bounds: "full" } });
DN("densify", [{ _id: 1, n: L(1), q: "x" }, { _id: 2, n: DEC("3.5") }], { field: "n", range: { step: 1, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: 0, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: -1, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: 1, unit: "hour", bounds: "full" } });
DN("densify.err", TS, { field: "ts", range: { step: 1, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: 1, bounds: [5, 1] } });
DN("densify.err", LN, { field: "n", range: { step: 1, bounds: "foo" } });
DN("densify.err", LN, { field: "$n", range: { step: 1, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: 1 } });
DN("densify.err", LN, { field: "n" });
DN("densify.err", LN, { range: { step: 1, bounds: "full" } });
DN("densify.err", LN, { field: "n", range: { step: 1, bounds: "partition" } , partitionByFields: ["n"] });
DN("densify.err", [{ _id: 1, n: "a" }], { field: "n", range: { step: 1, bounds: "full" } });
DN("densify.err", [{ _id: 1, n: I(1) }, { _id: 2, n: DT("2024-01-01T00:00:00Z") }], { field: "n", range: { step: 1, bounds: "full" } });

// --- $fill -------------------------------------------------------------------------
const FL = (area, docs, spec, more) => add(area, docs, [{ $fill: spec }].concat(more || []));
FL("fill", LN, { sortBy: { n: 1 }, partitionBy: "$p", output: { v: { method: "linear" } } });
FL("fill", LN, { sortBy: { n: 1 }, partitionByFields: ["p"], output: { v: { method: "locf" } } });
FL("fill", LN, { output: { v: { value: I(-1) } } });
FL("fill", LN, { output: { v: { value: { $multiply: ["$n", 10] } }, w: { value: "new" } } });
FL("fill", TS, { sortBy: { ts: 1 }, partitionBy: { k: "$k" }, output: { v: { method: "linear" } } });
FL("fill", TS, { sortBy: { ts: -1 }, output: { v: { method: "locf" } } });
FL("fill", S, { sortBy: { t: 1 }, output: { x: { method: "locf" }, s: { value: "?" } } });
FL("fill.err", LN, { output: { v: { method: "linear" } } });
FL("fill.err", LN, { sortBy: { n: 1 }, output: { v: { method: "foo" } } });
FL("fill.err", LN, { sortBy: { n: 1 }, output: { v: { method: "locf", value: 1 } } });
FL("fill.err", LN, { sortBy: { n: 1 }, output: { v: {} } });
FL("fill.err", LN, { sortBy: { n: 1 } });
FL("fill.err", LN, { sortBy: { n: 1 }, output: { v: { method: "locf" } }, partitionBy: "$p", partitionByFields: ["p"] });
FL("fill.err", LN, { sortBy: { n: 1 }, output: { v: { method: "locf" } }, foo: 1 });
FL("fill.err", LN, { sortBy: { n: 1, _id: 1 }, output: { v: { method: "linear" } } });

// --- $redact -------------------------------------------------------------------------
const RD = [
  { _id: I(1), level: I(1), title: "top", sub: { level: I(3), secret: "x", inner: { level: I(1), ok: true } }, list: [{ level: I(1), a: 1 }, { level: I(5), a: 2 }, { a: 3 }, 7] },
  { _id: I(2), level: I(4), title: "hidden" },
  { _id: I(3), title: "nolevel", sub: { level: I(2) } },
];
const lvl = (n) => ({ $cond: { if: { $lte: [{ $ifNull: ["$level", 0] }, n] }, then: "$$DESCEND", else: "$$PRUNE" } });
add("redact", RD, [{ $redact: lvl(2) }]);
add("redact", RD, [{ $redact: lvl(3) }]);
add("redact", RD, [{ $redact: { $cond: [{ $eq: ["$level", 4] }, "$$KEEP", "$$DESCEND"] } }]);
add("redact", RD, [{ $redact: "$$KEEP" }]);
add("redact", RD, [{ $redact: "$$PRUNE" }]);
add("redact.err", RD, [{ $redact: "x" }]);
add("redact.err", RD, [{ $redact: { $literal: 1 } }]);
add("redact.err", RD, [{ $redact: "$$ROOT" }]);

// --- $documents ----------------------------------------------------------------------
const DB = (area, pipeline) => add(area, [], pipeline, { db: true });
DB("documents", [{ $documents: [{ a: 1 }, { a: 2, b: { c: 3 } }] }]);
DB("documents", [{ $documents: [] }]);
DB("documents", [{ $documents: { $concatArrays: [[{ a: 1 }], [{ a: 2 }]] } }, { $match: { a: 2 } }]);
DB("documents", [{ $documents: { $map: { input: [1, 2, 3], in: { x: "$$this" } } } }, { $group: { _id: null, s: { $sum: "$x" } } }]);
DB("documents", [{ $documents: [{ x: "$$NOW" }] }, { $project: { t: { $type: "$x" } } }]);
DB("documents.err", [{ $documents: [1, 2] }]);
DB("documents.err", [{ $documents: 5 }]);
DB("documents.err", [{ $documents: "$a" }]);
DB("documents.err", [{ $documents: [{ a: 1 }] }, { $documents: [{ a: 2 }] }]);
DB("documents.err", [{ $match: {} }, { $documents: [{ a: 2 }] }]);
DB("documents.err", [{ $documents: [{ a: 1 }, null] }]);

// --- $graphLookup ------------------------------------------------------------------
const EMP = [
  { _id: I(1), name: "Dev", reportsTo: null },
  { _id: I(2), name: "Eliot", reportsTo: "Dev" },
  { _id: I(3), name: "Ron", reportsTo: "Eliot" },
  { _id: I(4), name: "Andrew", reportsTo: "Eliot" },
  { _id: I(5), name: "Asya", reportsTo: "Ron" },
  { _id: I(6), name: "Dan", reportsTo: "Andrew", dept: "x" },
  { _id: I(7), name: "Loop", reportsTo: "Loop" },
];
const AIR = [
  { _id: I(0), airport: "JFK", connects: ["BOS", "ORD"] },
  { _id: I(1), airport: "BOS", connects: ["JFK", "PWM"] },
  { _id: I(2), airport: "ORD", connects: ["JFK"] },
  { _id: I(3), airport: "PWM", connects: ["BOS", "LHR"] },
  { _id: I(4), airport: "LHR", connects: ["PWM"] },
];
const GL = (area, docs, spec, more) => add(area, docs, [{ $graphLookup: spec }].concat(more || []), { colls: { emp: EMP, air: AIR } });
const sortHier = { $project: { name: 1, h: { $sortArray: { input: "$h", sortBy: { _id: 1 } } } } };
GL("graph", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h" }, [sortHier]);
GL("graph", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", maxDepth: 0 }, [sortHier]);
GL("graph", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", maxDepth: 1, depthField: "d" }, [sortHier]);
GL("graph", EMP, { from: "emp", startWith: "$name", connectFromField: "name", connectToField: "reportsTo", as: "h", depthField: "d" }, [sortHier]);
GL("graph", EMP, { from: "emp", startWith: "$name", connectFromField: "name", connectToField: "reportsTo", as: "h", restrictSearchWithMatch: { dept: { $ne: "x" } } }, [sortHier]);
GL("graph", [{ _id: 1, s: ["Ron", "Andrew"] }], { from: "emp", startWith: "$s", connectFromField: "reportsTo", connectToField: "name", as: "h", depthField: "lvl" }, [sortHier]);
GL("graph", [{ _id: 1, s: "JFK" }, { _id: 2, s: "LHR" }, { _id: 3 }], { from: "air", startWith: "$s", connectFromField: "connects", connectToField: "airport", as: "h", maxDepth: 2, depthField: "hops" }, [sortHier]);
GL("graph", [{ _id: 1, s: "JFK" }], { from: "air", startWith: { $concat: ["$s", ""] }, connectFromField: "connects", connectToField: "airport", as: "h" }, [{ $project: { n: { $size: "$h" } } }]);
GL("graph", [{ _id: 1 }], { from: "nosuch", startWith: "x", connectFromField: "a", connectToField: "b", as: "h" });
GL("graph", [{ _id: 1, s: null }], { from: "emp", startWith: "$s", connectFromField: "name", connectToField: "reportsTo", as: "h" }, [sortHier]);
GL("graph", [{ _id: 1, s: "Loop" }], { from: "emp", startWith: "$s", connectFromField: "reportsTo", connectToField: "name", as: "h", depthField: "d" }, [sortHier]);
GL("graph", [{ _id: 1, s: "Dev" }], { from: "emp", startWith: "$s", connectFromField: "name", connectToField: "reportsTo", as: "a.b", maxDepth: L(1) }, [{ $project: { n: { $size: "$a.b" } } }]);
GL("graph.err", EMP, { startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h" });
GL("graph.err", EMP, { from: "emp", connectFromField: "reportsTo", connectToField: "name", as: "h" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectToField: "name", as: "h" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", as: "h" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", maxDepth: -1 });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", maxDepth: 1.5 });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", depthField: 1 });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", foo: 1 });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "$reportsTo", connectToField: "name", as: "h" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "$h" });
GL("graph.err", EMP, { from: "emp", startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h", restrictSearchWithMatch: 5 });
GL("graph.err", EMP, { from: 5, startWith: "$reportsTo", connectFromField: "reportsTo", connectToField: "name", as: "h" });

// --- run -------------------------------------------------------------------------------
const db2 = db.getSiblingDB("m2_stage_diff");
const out = [];
for (const c of cases) {
  const rec = Object.assign({}, c);
  try {
    db2.dropDatabase();
    if (c.colls) for (const [k, v] of Object.entries(c.colls)) db2.getCollection(k).insertMany(v);
    const pipeline = c.db ? c.pipeline : [{ $documents: c.docs }].concat(c.pipeline);
    rec.expect = { ok: db2.aggregate(pipeline, { promoteValues: false }).toArray() };
  } catch (e) {
    rec.expect = { code: e.code === undefined ? -1 : NumberInt(e.code), errmsg: String(e.errmsg || e.message) };
  }
  out.push(rec);
}
db2.dropDatabase();
print(EJSON.stringify(out, null, 1, { relaxed: false }));
