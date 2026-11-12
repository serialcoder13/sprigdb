// The aggregation-expression differential corpus (M1, 12 Nov 2026).
//
// Run with mongosh against a reference MongoDB 8.0 on a private port:
//
//   docker cp gen_cases.js mongo1-ref:/tmp/gen_cases.js
//   docker exec mongo1-ref mongosh --quiet --host 127.0.0.1 --port 47117 \
//       /tmp/gen_cases.js > cases.json
//
// Each case is evaluated as `[{$documents: [doc]}, {$replaceWith: {r: expr}}]`
// (or `[{$documents: docs}, {$group: group}, {$sort: {_id: 1}}]`), and the
// answer (the documents, or the error's code and message) is written next
// to the case as canonical Extended JSON. `tests/expr_diff.rs` evaluates
// the same cases with SprigDB's engine and compares.

const I = (n) => NumberInt(n);
const L = (n) => NumberLong(String(n));
const D = (x) => Double(x);
const DEC = (s) => NumberDecimal(s);
const DT = (s) => ISODate(s);
const OID = (h) => ObjectId(h);

const cases = [];
let seq = 0;
function add(area, expr, doc) {
  cases.push({ name: `${area}#${seq++}`, doc: doc === undefined ? { _id: 1 } : doc, expr });
}
function grp(area, docs, group) {
  cases.push({ name: `${area}#${seq++}`, docs, group });
}

// A palette of argument values.
const NUMS = [I(4), I(-3), I(0), L(9), L(-1), D(2.5), D(-0.5), D(0), DEC("1.5"), DEC("-2")];
const ODD = [null, "$missing", "abc", true, [1, 2], { a: 1 }, DT("2020-01-01T00:00:00Z")];
const SPECIAL = [D(NaN), D(Infinity), D(-Infinity)];

// --- arithmetic and trigonometry -------------------------------------------
for (const op of ["$exp", "$ln", "$log10", "$sqrt"]) {
  for (const v of [...NUMS, ...SPECIAL, null, "$missing", "abc"]) add("math", { [op]: v });
}
for (const v of [I(8), L(100), D(0.5), DEC("1000"), I(0), I(-1), D(1)]) {
  for (const b of [I(2), D(10), L(3), DEC("2"), I(1), I(0), I(-2)]) add("math.log", { $log: [v, b] });
}
add("math.log", { $log: [null, 2] });
add("math.log", { $log: [8, "$missing"] });
add("math.log", { $log: ["x", 2] });
add("math.log", { $log: [8, "x"] });
add("math.log", { $log: [8] });
for (const [b, e] of [
  [I(2), I(10)], [I(2), I(31)], [I(2), I(32)], [I(-2), I(31)], [L(3), I(2)], [I(2), L(62)], [I(2), L(63)],
  [I(2), I(-1)], [I(0), I(-1)], [I(0), I(0)], [I(1), I(-5)], [I(-1), I(-3)], [I(-1), I(-4)], [I(2), D(0.5)],
  [D(2.5), I(2)], [DEC("2"), I(3)], [I(4), DEC("0.5")], [I(-8), D(1 / 3)], [I(10), I(-2)], [L(0), I(-2)],
  [null, I(2)], [I(2), null], [I(2), "$missing"], ["a", I(2)], [I(2), "a"], [D(NaN), I(0)], [I(0), D(-0.5)],
  [L("3037000500"), I(2)], [I(46341), I(2)], [I(-46341), I(2)],
]) add("math.pow", { $pow: [b, e] });
add("math.pow", { $pow: [2] });
const TRIG = ["$sin", "$cos", "$tan", "$asin", "$acos", "$atan", "$sinh", "$cosh", "$tanh", "$asinh", "$acosh", "$atanh",
  "$degreesToRadians", "$radiansToDegrees"];
for (const op of TRIG) {
  for (const v of [I(0), I(1), L(2), D(0.5), D(-0.75), DEC("0.5"), D(Infinity), D(NaN), null, "$missing", "abc", I(-2)]) {
    add("trig", { [op]: v });
  }
}
for (const op of ["$sin", "$acos", "$atanh"]) add("trig", { [op]: [D(0.5)] });
add("trig", { $sin: [1, 2] });
for (const [y, x] of [[I(1), I(1)], [D(1), D(-1)], [L(0), I(-1)], [DEC("1"), D(2)], [I(1), DEC("2")], [null, 1], [1, "$missing"], ["a", 1], [D(-0.0), D(-1)]]) {
  add("trig.atan2", { $atan2: [y, x] });
}

// --- bitwise ------------------------------------------------------------------
for (const args of [
  [I(12), I(10)], [I(12), L(10)], [L(-1), I(5)], [], [I(7)], [I(7), null], [I(7), "$missing"], [I(7), D(1)], [I(7), "a"],
  [I(-1), I(0), I(255)], [L("9223372036854775807"), I(1)],
]) {
  for (const op of ["$bitAnd", "$bitOr", "$bitXor"]) add("bit", { [op]: args });
}
for (const v of [I(5), L(5), I(-1), D(1.5), null, "$missing", "a", DEC("1")]) add("bit", { $bitNot: v });
add("bit", { $bitNot: [I(1), I(2)] });

// --- strings -----------------------------------------------------------------
for (const op of ["$trim", "$ltrim", "$rtrim"]) {
  for (const spec of [
    { input: "  hi there \t\n" }, { input: "xxhixx", chars: "x" }, { input: "abcHIcba", chars: "cab" }, { input: "" },
    { input: null }, { input: "$missing" }, { input: 5 }, { input: "hi", chars: 5 }, { input: "hi", chars: null },
    { input: "   é 　" }, { input: "ééaéé", chars: "é" }, { input: "hi", chars: "" }, { input: "hi", extra: 1 },
    { chars: "x" }, "hi",
  ]) add("str.trim", { [op]: spec });
}
for (const spec of [
  { input: "banana", regex: "an" }, { input: "banana", regex: /AN/i }, { input: "banana", regex: "(a)(n)?" },
  { input: "banana", regex: "x" }, { input: "a1b22c333", regex: "\\d+" }, { input: "héllo wörld", regex: "ö" },
  { input: "héllo wörld", regex: "(w)(ö)" }, { input: "abc", regex: "(x)?b" }, { input: null, regex: "a" },
  { input: "$missing", regex: "a" }, { input: 5, regex: "a" }, { input: "a", regex: 5 }, { input: "a", regex: null },
  { input: "aBc", regex: "b", options: "i" }, { input: "a\nb", regex: "^b", options: "m" }, { input: "a", regex: /a/i, options: "m" },
  { input: "abc", regex: "B", options: "q" }, { input: "", regex: "" }, { input: "aaa", regex: "" }, { input: "a b", regex: "a  b", options: "x" },
  { input: "a.b", regex: "a.b", options: "s" }, { regex: "a" }, { input: "a" },
]) {
  for (const op of ["$regexFind", "$regexFindAll", "$regexMatch"]) add("str.regex", { [op]: spec });
}
for (const op of ["$replaceOne", "$replaceAll"]) {
  for (const spec of [
    { input: "a.b.c", find: ".", replacement: "-" }, { input: "aaa", find: "aa", replacement: "b" }, { input: "abc", find: "", replacement: "-" },
    { input: "abc", find: "x", replacement: "-" }, { input: null, find: "a", replacement: "b" }, { input: "$missing", find: "a", replacement: "b" },
    { input: "abc", find: null, replacement: "b" }, { input: "abc", find: "a", replacement: null }, { input: 5, find: "a", replacement: "b" },
    { input: "abc", find: 5, replacement: "b" }, { input: "abc", find: "a", replacement: 5 }, { input: "abc", find: "a" },
    { input: "ééé", find: "é", replacement: "e" }, { input: 5, find: null, replacement: "b" }, { input: "", find: "", replacement: "x" },
  ]) add("str.replace", { [op]: spec });
}
for (const op of ["$indexOfBytes", "$indexOfCP"]) {
  for (const args of [
    ["cafétéria", "é"], ["cafétéria", "é", 4], ["cafétéria", "t", 2, 5], ["abc", ""], ["abc", "", 3], ["abc", "", 4],
    ["abc", "c", 5], ["abc", "b", 2, 1], [null, "a"], ["$missing", "a"], ["abc", null], [5, "a"], ["abc", 5],
    ["abc", "a", -1], ["abc", "a", D(1.5)], ["abc", "a", L(0), L(3)], ["abc", "a", D(0)], ["abc"], ["abc", "a", 0, 1, 2], ["abc", "a", "x"],
    ["abc", "c", 0, -1], ["∫aé", "é"],
  ]) add("str.indexOf", { [op]: args });
}
for (const args of [["a", "A"], ["abc", "abd"], ["b", "a"], ["", ""], [null, "a"], [null, null], ["$missing", ""], [5, "5"], [1, 2],
  [true, "true"], ["a"], ["ß", "SS"], ["é", "É"], [DT("2020-01-01T00:00:00Z"), "x"], [I(10), I(9)]]) {
  add("str.strcasecmp", { $strcasecmp: args });
}

// --- arrays and sets ------------------------------------------------------------
for (const args of [
  [[1, 2, 3, 2], 2], [[1, 2, 3, 2], 2, 2], [[1, 2, 3], 4], [[1, 2, 3], 1, 1, 2], [[1, I(2), 3], D(2)], [[[1], [2]], [2]],
  [null, 1], ["$missing", 1], [5, 1], [[1, 2], 1, -1], [[1, 2], 1, D(1.5)], [[1, 2], 1, null], [[1, 2], null], [[null], null],
  [[1, 2], 2, 0, 1], [[1, 2], 2, 5], [[1]], [[1], 1, 0, 1, 1], [["a", "b"], "b", L(1)],
]) add("arr.indexOfArray", { $indexOfArray: args });
for (const args of [
  [0, 5], [0, 10, 3], [10, 0, -3], [5, 5], [0, -5], [-2, 2], [L(0), I(3)], [D(0), D(3)], [D(0.5), 3], [0, 3, 0],
  [0, 3, D(1.5)], [null, 3], [0, null], ["a", 3], [0], [0, L("3000000000")], [0, 3, -1], [I(1), DEC("3")],
]) add("arr.range", { $range: args });
for (const spec of [
  { inputs: [[1, 2, 3], ["a", "b"]] }, { inputs: [[1, 2, 3], ["a", "b"]], useLongestLength: true },
  { inputs: [[1, 2, 3], ["a"]], useLongestLength: true, defaults: [0, "z"] }, { inputs: [[1], ["a"]], defaults: [0, "z"] },
  { inputs: [] }, { inputs: [[1, 2]] }, { inputs: [[1], null] }, { inputs: [[1], "$missing"] }, { inputs: [[1], 5] },
  { inputs: 5 }, { inputs: [[1], [2]], useLongestLength: 1 }, { inputs: [[1], [2]], useLongestLength: true, defaults: [1] },
  { inputs: [[1], [2]], useLongestLength: true, defaults: 5 }, { inputs: [[1], [2]], bad: 1 }, {}, [[1], [2]], { inputs: [[], [1, 2]], useLongestLength: true },
]) add("arr.zip", { $zip: spec });
const SA = [3, 1, "b", null, { a: 2 }, [0], I(2), D(1.5), L(-4), "a", true, DT("2020-01-01T00:00:00Z")];
for (const spec of [
  { input: SA, sortBy: 1 }, { input: SA, sortBy: -1 }, { input: [{ a: 2, b: 1 }, { a: 1, b: 2 }, { a: 2, b: 0 }, { b: 5 }], sortBy: { a: 1, b: -1 } },
  { input: [{ a: [3, 1] }, { a: [2] }, { a: 0 }], sortBy: { a: 1 } }, { input: [{ a: [3, 1] }, { a: [2] }, { a: 0 }], sortBy: { a: -1 } },
  { input: [{ x: { y: 2 } }, { x: { y: 1 } }], sortBy: { "x.y": 1 } }, { input: null, sortBy: 1 }, { input: "$missing", sortBy: 1 },
  { input: 5, sortBy: 1 }, { input: [3, 1], sortBy: 2 }, { input: [3, 1], sortBy: {} }, { input: [3, 1] }, { input: [3, 1], sortBy: 1, z: 1 },
  { input: [], sortBy: 1 }, { input: ["B", "a", "C"], sortBy: 1 }, { input: [3, 1], sortBy: "a" }, { input: [1, 2], sortBy: { a: 1 } },
]) add("arr.sortArray", { $sortArray: spec });
for (const op of ["$firstN", "$lastN", "$maxN", "$minN"]) {
  for (const spec of [
    { n: 2, input: [5, 1, null, 9, 3] }, { n: 10, input: [5, 1, 9] }, { n: L(1), input: [5, 1, 9] }, { n: D(2), input: [5, 1, 9] },
    { n: 0, input: [1] }, { n: -1, input: [1] }, { n: D(1.5), input: [1] }, { n: 2, input: null }, { n: 2, input: "$missing" },
    { n: 2, input: 5 }, { n: null, input: [1] }, { n: "a", input: [1] }, { n: 2 }, { input: [1] }, { n: 2, input: [1], x: 1 },
    { n: 2, input: [null, "$missing", "a", 1, [2]] }, { n: 3, input: ["b", "a", 1, { x: 1 }] }, [1, 2],
  ]) add("arr.nfuncs", { [op]: spec });
}
for (const v of [[true, 1, "a"], [true, 0], [], [null], [[false]], [[]], null, "$missing", 5, [D(NaN)], [undefined]]) {
  add("set.allAny", { $allElementsTrue: [v] });
  add("set.allAny", { $anyElementTrue: [v] });
}
add("set.allAny", { $allElementsTrue: [[1], [1]] });
add("set.allAny", { $anyElementTrue: [1, 2] });
add("set.allAny", { $allElementsTrue: [1, 2, 3] });
for (const [a, b] of [
  [[1, 2, 3], [2, 4]], [[1, 1, 2], [3]], [[1, I(2)], [D(2)]], [["a", "b"], []], [[], [1]], [null, [1]], [[1], null],
  ["$missing", [1]], [[1], 5], [5, [1]], [[[1], 2], [[1]]], [[{ a: 1 }], [{ a: 1 }]],
]) {
  add("set.diff", { $setDifference: [a, b] });
  add("set.subset", { $setIsSubset: [a, b] });
  add("set.equals", { $setEquals: [a, b] });
}
add("set.diff", { $setDifference: [[1]] });
add("set.equals", { $setEquals: [[1]] });
add("set.equals", { $setEquals: [[1, 2], [2, 1, 1], [2, 1]] });
add("set.equals", { $setEquals: [[1, 2], [2, 1], [3]] });
add("set.subset", { $setIsSubset: [[1], [1], [1]] });
// The existing expressions, with array index variables (8.3 adds IDX).
add("arr.map", { $map: { input: [1, 2, 3], in: { $multiply: ["$$this", 2] } } });
add("arr.map", { $map: { input: [1, 2], as: "x", in: { $add: ["$$x", 1] } } });
add("arr.map", { $map: { input: null, in: 1 } });
add("arr.map", { $map: { input: "$missing", in: 1 } });
add("arr.map", { $map: { input: 5, in: 1 } });
add("arr.filter", { $filter: { input: [1, 2, 3, 4], cond: { $gt: ["$$this", 2] } } });
add("arr.filter", { $filter: { input: [1, 2, 3, 4], cond: { $gt: ["$$this", 1] }, limit: 2 } });
add("arr.filter", { $filter: { input: [1, 2, 3, 4], cond: true, limit: 0 } });
add("arr.filter", { $filter: { input: 5, cond: true } });
add("arr.reduce", { $reduce: { input: [1, 2, 3], initialValue: 0, in: { $add: ["$$value", "$$this"] } } });
add("arr.reduce", { $reduce: { input: null, initialValue: 0, in: 1 } });
add("arr.reduce", { $reduce: { input: 5, initialValue: 0, in: 1 } });
add("arr.concatArrays", { $concatArrays: [[1], [2, [3]]] });
add("arr.concatArrays", { $concatArrays: [[1], null] });
add("arr.concatArrays", { $concatArrays: [[1], 5] });

// --- dates ------------------------------------------------------------------------
const T1 = DT("2021-03-14T06:30:15.250Z");
const T2 = DT("2020-12-31T23:59:59.999Z");
const T3 = DT("2024-02-29T12:00:00Z");
const T4 = DT("1969-12-31T23:59:59.500Z");
const DATES = [T1, T2, T3, T4, DT("2021-01-03T00:00:00Z"), DT("2021-01-04T00:00:00Z"), DT("2018-12-31T00:00:00Z"), DT("2027-01-01T00:00:00Z")];
for (const op of ["$week", "$isoWeek", "$isoWeekYear", "$isoDayOfWeek", "$dayOfWeek", "$dayOfYear", "$year", "$month",
  "$dayOfMonth", "$hour", "$minute", "$second", "$millisecond"]) {
  for (const d of DATES) add("date.parts", { [op]: d });
  add("date.parts", { [op]: { date: T1, timezone: "Asia/Kolkata" } });
  add("date.parts", { [op]: { date: T2, timezone: "-08:00" } });
}
for (const op of ["$week", "$isoWeek", "$isoDayOfWeek", "$hour"]) {
  for (const a of [null, "$missing", "x", 5, OID("5ab9cbfa31c2ab715d42129e"), Timestamp({ t: 1600000000, i: 1 }), [T1],
    { date: T1, timezone: "Bad/Zone" }, { date: T1, timezone: 5 }, { date: T1, timezone: null }, { date: null, timezone: "UTC" },
    { date: T1, timezone: "+0530" }, { date: T1, timezone: "EST" }, { date: T1, x: 1 }, { timezone: "UTC" }]) {
    add("date.parts.args", { [op]: a });
  }
}
for (const tz of [undefined, "UTC", "America/New_York", "+05:30", "-0300", "Europe/London"]) {
  for (const d of [T1, T4, DT("2021-11-07T05:30:00Z")]) {
    const s = { date: d };
    if (tz) s.timezone = tz;
    add("date.toParts", { $dateToParts: s });
    add("date.toParts", { $dateToParts: { ...s, iso8601: true } });
  }
}
for (const s of [{ date: null }, { date: "$missing" }, { date: 5 }, { date: T1, iso8601: 1 }, { date: T1, timezone: "Nope" }, {}, { date: T1, foo: 1 }]) {
  add("date.toParts", { $dateToParts: s });
}
for (const s of [
  { year: 2021, month: 3, day: 14, hour: 6 }, { year: 2021 }, { year: 2021, month: 14, day: 35, hour: 25, minute: 61, second: 61, millisecond: 1001 },
  { year: 2021, month: 0, day: 0 }, { year: 2021, month: -1, day: -1, hour: -1 }, { year: 2021, month: 3, day: 14, timezone: "America/New_York" },
  { year: 2021, month: 3, day: 14, hour: 2, minute: 30, timezone: "America/New_York" }, { year: 2021, month: 3, timezone: "+05:30" },
  { isoWeekYear: 2021, isoWeek: 1, isoDayOfWeek: 1 }, { isoWeekYear: 2021, isoWeek: 53, isoDayOfWeek: 8 }, { isoWeekYear: 2020 },
  { year: 2021, isoWeek: 1 }, { year: null }, { year: 2021, month: null }, { year: "$missing" }, { year: 2021, month: "$missing" },
  { year: "x" }, { year: D(2021.5) }, { year: D(2021) }, { year: L(2021), month: DEC("2") }, { year: 0 }, { year: 10000 }, { year: -1 },
  { year: 2021, month: 40000 }, { month: 1 }, {}, { year: 2021, bad: 1 }, { year: 2021, timezone: "X/Y" }, { year: 2021, millisecond: L("100000000000") },
]) add("date.fromParts", { $dateFromParts: s });
for (const s of [
  { dateString: "2017-02-08T12:10:40.787" }, { dateString: "2017-02-08T12:10:40.787Z" }, { dateString: "2017-02-08T12:10:40+05:30" },
  { dateString: "2017-02-08" }, { dateString: "2017-02-08", timezone: "America/New_York" }, { dateString: "2017-02-08T12:10:40Z", timezone: "Asia/Tokyo" },
  { dateString: "06-15-2018", format: "%m-%d-%Y" }, { dateString: "15/06/2018 10:20", format: "%d/%m/%Y %H:%M" },
  { dateString: "2018-W01-1", format: "%G-W%V-%u" }, { dateString: "2018-06-15T10:20:30.123", format: "%Y-%m-%dT%H:%M:%S.%L" },
  { dateString: "2018 166", format: "%Y %j" }, { dateString: "2018-06-15 +0530", format: "%Y-%m-%d %z" },
  { dateString: "2018-06-15 330", format: "%Y-%m-%d %Z" }, { dateString: "not a date" }, { dateString: "not a date", onError: "bad" },
  { dateString: null }, { dateString: null, onNull: "nul" }, { dateString: "$missing", onNull: "nul" }, { dateString: 5 },
  { dateString: 5, onError: "bad" }, { dateString: "2018-06-15", format: "%Y-%m" }, { dateString: "2018-06-15", format: 5 },
  { dateString: "2018-06-15", timezone: "Bad/Zone" }, { dateString: "2018-06-15", timezone: "Bad/Zone", onError: "x" },
  { dateString: "2018-02-30" }, { dateString: "2018-06-15T10:20:30Z", timezone: "+01:00" }, { dateString: "June 15, 2018" },
  { dateString: "2018-06-15", format: "%Y-%m-%d%%" }, { dateString: "2018-06-15", format: "%Q" }, {}, { dateString: "2018-06-15", z: 1 },
  { dateString: "2018-06-15T25:00:00" }, { dateString: "1960-01-01T00:00:00Z" },
]) add("date.fromString", { $dateFromString: s });
const DA = DT("2021-01-31T10:00:00Z");
for (const s of [
  { startDate: DA, unit: "month", amount: 1 }, { startDate: DA, unit: "month", amount: L(13) }, { startDate: DA, unit: "year", amount: -1 },
  { startDate: DT("2024-02-29T00:00:00Z"), unit: "year", amount: 1 }, { startDate: DA, unit: "quarter", amount: 1 },
  { startDate: DA, unit: "week", amount: 2 }, { startDate: DA, unit: "day", amount: -40 }, { startDate: DA, unit: "hour", amount: 30 },
  { startDate: DA, unit: "minute", amount: 90 }, { startDate: DA, unit: "second", amount: 3600 }, { startDate: DA, unit: "millisecond", amount: 1500 },
  { startDate: DT("2021-03-13T12:00:00Z"), unit: "day", amount: 1, timezone: "America/New_York" },
  { startDate: DT("2021-03-14T06:30:00Z"), unit: "hour", amount: 1, timezone: "America/New_York" },
  { startDate: DT("2021-03-13T12:00:00Z"), unit: "month", amount: 1, timezone: "+05:30" },
  { startDate: DA, unit: "day", amount: D(1.0) }, { startDate: DA, unit: "day", amount: D(1.5) }, { startDate: DA, unit: "day", amount: DEC("2") },
  { startDate: DA, unit: "days", amount: 1 }, { startDate: DA, unit: 5, amount: 1 }, { startDate: DA, unit: null, amount: 1 },
  { startDate: DA, unit: "day", amount: null }, { startDate: null, unit: "day", amount: 1 }, { startDate: "$missing", unit: "day", amount: 1 },
  { startDate: "x", unit: "day", amount: 1 }, { startDate: OID("5ab9cbfa31c2ab715d42129e"), unit: "day", amount: 1 },
  { startDate: Timestamp({ t: 1600000000, i: 1 }), unit: "day", amount: 1 }, { startDate: DA, unit: "day", amount: "x" },
  { startDate: DA, unit: "day" }, { startDate: DA, unit: "day", amount: 1, timezone: "Nope" }, { startDate: DA, unit: "day", amount: 1, zz: 1 },
  { startDate: DA, unit: "year", amount: L("100000000000") },
]) {
  add("date.add", { $dateAdd: s });
  add("date.subtract", { $dateSubtract: s });
}
const E1 = DT("2020-12-31T23:59:00Z");
const E2 = DT("2021-01-01T00:01:00Z");
for (const unit of ["year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"]) {
  add("date.diff", { $dateDiff: { startDate: E1, endDate: E2, unit } });
  add("date.diff", { $dateDiff: { startDate: E2, endDate: DT("2019-03-17T08:00:00Z"), unit } });
}
for (const s of [
  { startDate: E1, endDate: E2, unit: "day", timezone: "Asia/Tokyo" }, { startDate: E1, endDate: E2, unit: "year", timezone: "-05:00" },
  { startDate: DT("2021-01-02T00:00:00Z"), endDate: DT("2021-01-12T00:00:00Z"), unit: "week" },
  { startDate: DT("2021-01-02T00:00:00Z"), endDate: DT("2021-01-12T00:00:00Z"), unit: "week", startOfWeek: "saturday" },
  { startDate: DT("2021-01-02T00:00:00Z"), endDate: DT("2021-01-12T00:00:00Z"), unit: "week", startOfWeek: "MON" },
  { startDate: DT("2021-01-02T00:00:00Z"), endDate: DT("2021-01-12T00:00:00Z"), unit: "week", startOfWeek: "funday" },
  { startDate: DT("2021-01-02T00:00:00Z"), endDate: DT("2021-01-12T00:00:00Z"), unit: "day", startOfWeek: "funday" },
  { startDate: null, endDate: E2, unit: "day" }, { startDate: E1, endDate: "$missing", unit: "day" }, { startDate: E1, endDate: E2, unit: null },
  { startDate: E1, endDate: 5, unit: "day" }, { startDate: E1, endDate: E2, unit: "days" }, { startDate: E1, endDate: E2 },
  { startDate: OID("5ab9cbfa31c2ab715d42129e"), endDate: E2, unit: "day" }, { startDate: E1, endDate: E2, unit: "day", x: 1 },
]) add("date.diff", { $dateDiff: s });
const TR = DT("2021-07-15T17:43:21.123Z");
for (const s of [
  { date: TR, unit: "year" }, { date: TR, unit: "quarter" }, { date: TR, unit: "month" }, { date: TR, unit: "week" },
  { date: TR, unit: "week", startOfWeek: "monday" }, { date: TR, unit: "day" }, { date: TR, unit: "hour" }, { date: TR, unit: "minute" },
  { date: TR, unit: "second" }, { date: TR, unit: "millisecond" }, { date: TR, unit: "hour", binSize: 6 }, { date: TR, unit: "day", binSize: 10 },
  { date: TR, unit: "month", binSize: 5 }, { date: TR, unit: "minute", binSize: 7 }, { date: TR, unit: "day", timezone: "America/New_York" },
  { date: TR, unit: "day", timezone: "+05:30" }, { date: TR, unit: "hour", binSize: 6, timezone: "Asia/Kolkata" },
  { date: TR, unit: "week", binSize: 2, startOfWeek: "Tue" }, { date: T4, unit: "day" }, { date: T4, unit: "second", binSize: 7 },
  { date: OID("5ab9cbfa31c2ab715d42129e"), unit: "day" }, { date: Timestamp({ t: 1600000000, i: 1 }), unit: "hour" },
  { date: null, unit: "day" }, { date: TR, unit: null }, { date: TR, unit: "day", binSize: null }, { date: "$missing", unit: "day" },
  { date: "x", unit: "day" }, { date: TR, unit: "fortnight" }, { date: TR, unit: "day", binSize: 0 }, { date: TR, unit: "day", binSize: D(1.5) },
  { date: TR, unit: "day", binSize: L(2) }, { date: TR }, { date: TR, unit: "day", q: 1 }, { date: TR, unit: "week", startOfWeek: "x" },
]) add("date.trunc", { $dateTrunc: s });
for (const v of [T1, "2021-03-14T06:30:15Z", "2021-03-14", L("1600000000000"), D(1600000000000.5), DEC("1600000000000"), I(5),
  OID("5ab9cbfa31c2ab715d42129e"), Timestamp({ t: 1600000000, i: 1 }), null, "$missing", true, "garbage", [T1]]) {
  add("date.toDate", { $toDate: v });
}
for (const s of [
  { date: T1, format: "%Y-%m-%d %H:%M:%S.%L %j %w %u %U %V %G %z %Z %%" }, { date: T1, format: "%d/%m/%Y", timezone: "Asia/Kolkata" },
  { date: T1 }, { date: T1, timezone: "America/New_York" }, { date: null }, { date: null, onNull: "n" }, { date: "$missing" },
  { date: 5 }, { date: T1, format: "%Q" }, { date: T1, format: "%b %B %a %A" }, { date: OID("5ab9cbfa31c2ab715d42129e") },
  { date: Timestamp({ t: 1600000000, i: 1 }) }, { date: T4, format: "%Y %L" }, { date: DT("0099-01-01T00:00:00Z"), format: "%Y" },
]) add("date.toString", { $dateToString: s });

// --- types ------------------------------------------------------------------------
const CONV_IN = [I(5), L(-7), D(2.75), D(-0.5), DEC("12.5"), true, false, "42", " 42", "4.5e1", "0x1A", "abc", "", "-0",
  DT("2020-01-01T00:00:00Z"), OID("5ab9cbfa31c2ab715d42129e"), null, "$missing", [1], { a: 1 }, D(NaN), D(Infinity),
  L("9223372036854775807"), D(3e10), "2147483648", Timestamp({ t: 1600000000, i: 1 }), BinData(0, "AQID"),
  UUID("867dee52-c331-484e-92d1-c56479b8e67e"), /ab/i, MinKey(), "5ab9cbfa31c2ab715d42129e", "2020-06-01"];
const TARGETS = ["double", "string", "objectId", "bool", "date", "int", "long", "decimal"];
for (const v of CONV_IN) {
  for (const to of TARGETS) {
    // A date past JavaScript's range can't be printed by mongosh.
    if (to === "date" && v instanceof Long) continue;
    add("convert", { $convert: { input: v, to, onError: "ERR" } });
  }
}
for (const s of [
  { input: "abc", to: "int" }, { input: 5, to: "foo" }, { input: 5, to: 99 }, { input: 5, to: I(16) }, { input: 5, to: D(2) }, { input: 5, to: null },
  { input: 5, to: "$missing" }, { input: null, to: "int", onNull: "N" }, { input: "$missing", to: "int", onNull: "N" }, { input: null, to: "int" },
  { input: "x", to: "int", onError: "$missing" }, { input: 5 }, { to: "int" }, { input: 5, to: "int", z: 1 }, { input: "abc", to: "array" },
  { input: "[1]", to: "array", onError: "E" }, { input: 5, to: "minKey" }, { input: 5, to: "binData" }, { input: "AQID", to: "binData", onError: "E" },
  { input: "AQID", to: { type: "binData", subtype: 0 }, format: "base64" }, { input: BinData(0, "AQID"), to: "string", format: "hex" },
  { input: BinData(0, "AQID"), to: "string", format: "base64" }, { input: BinData(0, "AQID"), to: "string", format: "utf8" },
  { input: UUID("867dee52-c331-484e-92d1-c56479b8e67e"), to: "string", format: "uuid" },
  { input: "867dee52-c331-484e-92d1-c56479b8e67e", to: { type: "binData", subtype: 4 }, format: "uuid" }, { input: "ff", to: "int", base: 16 },
  { input: "5", to: "string", onError: { $concat: ["a", "b"] } }, { input: "5", to: "int", onNull: { $concat: ["a", "b"] } },
  { input: 1e300, to: "int", onError: "E" }, { input: DEC("1e40"), to: "long", onError: "E" }, { input: "1e400", to: "double", onError: "E" },
  { input: D(-2147483648.9), to: "int" }, { input: D(2147483647.9), to: "int" }, { input: D(-9223372036854775808), to: "long" },
]) add("convert.args", { $convert: s });
for (const op of ["$toInt", "$toLong", "$toDouble", "$toDecimal", "$toBool", "$toString", "$toObjectId"]) {
  for (const v of [I(7), D(7.9), "8", "8.5", "zz", null, "$missing", DEC("-3.7"), true, DT("2020-01-01T00:00:00Z"), [1], { a: 1 }, "5ab9cbfa31c2ab715d42129e", L(3)]) {
    add("convert.short", { [op]: v });
  }
}
add("convert.short", { $toInt: [I(1), I(2)] });
add("convert.short", { $toInt: [I(1)] });
for (const v of [I(5), L(5), D(5), DEC("5"), D(NaN), "5", null, "$missing", true, [1], { a: 1 }]) add("type.isNumber", { $isNumber: v });
add("type.isNumber", { $isNumber: [1, 2] });
for (const v of ["867dee52-c331-484e-92d1-c56479b8e67e", "867DEE52-C331-484E-92D1-C56479B8E67E", "nope", 5, null, "$missing",
  UUID("867dee52-c331-484e-92d1-c56479b8e67e")]) add("type.toUUID", { $convert: { input: v, to: { type: "binData", subtype: 4 }, format: "uuid", onError: "E" } });
for (const v of [BinData(0, "AQID"), BinData(4, "hn3uUsMxSE6S0cVkebjmfg=="), "héllo", "", null, "$missing", 5, [1]]) add("type.binarySize", { $binarySize: v });
for (const v of [{ a: 1 }, {}, { a: "xyz", b: [1, 2] }, null, "$missing", 5, "a", [1]]) add("type.bsonSize", { $bsonSize: v });
add("type.bsonSize", { $bsonSize: "$$ROOT" }, { _id: 1, x: "hello" });
for (const v of [Timestamp({ t: 1600000000, i: 7 }), Timestamp({ t: 0, i: 0 }), null, "$missing", 5, DT("2020-01-01T00:00:00Z"), L(5)]) {
  add("type.ts", { $tsIncrement: v });
  add("type.ts", { $tsSecond: v });
}
for (const r of [0, 1, D(1), D(0.5), D(-0.1), D(1.5), "a", null]) add("type.sampleRate", { $sampleRate: r });
for (const v of [I(1), L(1), D(1), DEC("1"), "s", true, null, "$missing", [1], { a: 1 }, OID("5ab9cbfa31c2ab715d42129e"), DT("2020-01-01T00:00:00Z"),
  BinData(0, "AQID"), /x/, Timestamp({ t: 1, i: 1 }), MinKey(), MaxKey()]) add("type.type", { $type: v });
for (const v of [I(1), L(-2), D(2.5), D(1e21), D(1e-7), D(0.1), D(-0.0), D(NaN), D(Infinity), DEC("1.50"), DEC("1E+3"), true, OID("5ab9cbfa31c2ab715d42129e"),
  DT("2020-01-01T00:00:00Z"), DT("1960-05-01T10:00:00.5Z"), BinData(0, "AQID"), UUID("867dee52-c331-484e-92d1-c56479b8e67e"), null, "$missing"]) {
  add("type.toString", { $toString: v });
}

// --- $group accumulators (existing ones, for regression) ---------------------------
const GD = [{ _id: 1, k: "a", v: I(1), arr: [1, 2] }, { _id: 2, k: "a", v: L(2), arr: [2, 3] }, { _id: 3, k: "b", v: D(1.5), arr: [] },
  { _id: 4, k: "b", arr: null }, { _id: 5, k: "a", v: "x", arr: [[4]] }];
for (const acc of ["$sum", "$avg", "$min", "$max", "$first", "$last", "$push", "$addToSet"]) {
  grp("group", GD, { _id: "$k", r: { [acc]: "$v" } });
}
grp("group", GD, { _id: "$k", r: { $count: {} } });
grp("group", GD, { _id: null, r: { $sum: 1 } });

// --- batch 2: generated dates, time zones, numbers and strings --------------
let seed = 12345;
function rnd() {
  seed = (seed * 1103515245 + 12345) % 2147483648;
  return seed / 2147483648;
}
const ZONES = ["America/New_York", "Europe/London", "Asia/Kolkata", "Australia/Lord_Howe", "Pacific/Chatham",
  "+05:45", "-09:30", "America/Sao_Paulo", "UTC", "Europe/Berlin"];
const UNITS = ["year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"];
const RD = [];
for (let i = 0; i < 40; i++) {
  // 1971–2029, with DST-change weekends over-represented.
  const base = Date.UTC(1971 + Math.floor(rnd() * 58), Math.floor(rnd() * 12), 1 + Math.floor(rnd() * 28));
  const ms = base + Math.floor(rnd() * 86400000);
  RD.push(new Date(ms));
}
for (const t of ["2021-03-14T06:59:59.999Z", "2021-03-14T07:00:00Z", "2021-11-07T05:30:00Z", "2021-11-07T06:30:00Z",
  "2021-03-28T00:59:59Z", "2021-03-28T01:00:00Z", "2021-10-31T00:30:00Z", "2021-10-31T01:30:00Z", "2021-04-04T15:00:00Z"]) {
  RD.push(new Date(t));
}
for (let i = 0; i < RD.length; i++) {
  const d = RD[i];
  const tz = ZONES[i % ZONES.length];
  const unit = UNITS[i % UNITS.length];
  add("gen.trunc", { $dateTrunc: { date: d, unit, timezone: tz } });
  add("gen.trunc", { $dateTrunc: { date: d, unit, binSize: 1 + (i % 5), timezone: tz, startOfWeek: ["mon", "sun", "fri"][i % 3] } });
  add("gen.diff", { $dateDiff: { startDate: d, endDate: RD[(i * 7 + 3) % RD.length], unit, timezone: tz } });
  add("gen.add", { $dateAdd: { startDate: d, unit, amount: (i % 7) - 3, timezone: tz } });
  add("gen.toParts", { $dateToParts: { date: d, timezone: tz, iso8601: i % 2 === 0 } });
  add("gen.toString", { $dateToString: { date: d, timezone: tz, format: "%Y-%m-%dT%H:%M:%S.%L %z %Z %j %U %V %G %u %w" } });
  add("gen.week", { $week: { date: d, timezone: tz } });
  add("gen.fromParts", { $dateFromParts: { year: 1971 + (i % 58), month: 1 + (i % 12), day: 1 + (i % 28), hour: i % 24, minute: (i * 7) % 60, timezone: tz } });
  add("gen.fromString", { $dateFromString: { dateString: d.toISOString().slice(0, 19), timezone: tz } });
}
const RN = [];
for (let i = 0; i < 30; i++) {
  const mag = Math.pow(10, Math.floor(rnd() * 40) - 20);
  RN.push(D((rnd() - 0.5) * mag));
}
RN.push(D(0.1 + 0.2), D(1 / 3), D(2 / 3), D(123456789.123456789), D(1e15), D(1e16), D(1e17), D(4.35), D(-1e-5), D(5e-324));
for (const n of RN) {
  add("gen.num", { $toString: n });
  add("gen.num", { $toDecimal: n });
  add("gen.num", { $toLong: n });
  add("gen.num", { $convert: { input: n, to: "int", onError: "E" } });
  add("gen.num", { $exp: n });
  add("gen.num", { $atan: n });
  add("gen.num", { $sin: n });
  add("gen.num", { $pow: [n, D(2)] });
}
for (const str of ["12", "-12", "+12", "012", "1e3", "1.5e3", "  7", "7 ", "1_000", "Infinity", "-Infinity", "NaN", "inf", "0.1",
  ".5", "5.", "-.5", "1e-400", "9223372036854775807", "9223372036854775808", "-9223372036854775808", "2147483647", "-2147483649",
  "1E+3", "0x10", "1,000", "١٢"]) {
  add("gen.str", { $toInt: str });
  add("gen.str", { $convert: { input: str, to: "long", onError: "E" } });
  add("gen.str", { $convert: { input: str, to: "double", onError: "E" } });
  add("gen.str", { $convert: { input: str, to: "decimal", onError: "E" } });
}
const WORDS = ["", "a", "abc", "ÀÉÎõü", "∑∫√", "日本語テキスト", "emoji 😀 here", "  padded  ", "a.b*c+d", "AaBbCc"];
for (let i = 0; i < WORDS.length; i++) {
  const w = WORDS[i];
  const t = WORDS[(i + 3) % WORDS.length].slice(0, 2);
  add("gen.text", { $indexOfCP: [w, t] });
  add("gen.text", { $indexOfBytes: [w, t] });
  add("gen.text", { $strcasecmp: [w, WORDS[(i + 1) % WORDS.length]] });
  add("gen.text", { $regexFindAll: { input: w, regex: "[a-zà-ü]", options: "i" } });
  add("gen.text", { $trim: { input: w, chars: " a" } });
  add("gen.text", { $replaceAll: { input: w, find: "a", replacement: "∆" } });
  add("gen.text", { $strLenCP: w });
}

// --- batch 3: free-form date strings ----------------------------------------
for (const str of ["2017-02-08 12:10:40", "2017-02-08T12:10", "2017-02-08T12", "02/08/2017", "2/8/2017", "Feb 8 2017",
  "8 Feb 2017", "8 February 2017", "February 8, 2017 10:20", "2017-02-08T12:10:40.1234567Z", "2017-02-08T12:10:40 +0530",
  "2017-02-08T12:10:40 GMT", "2017-02-08T12:10:40 UTC", "2017-02-08T12:10:40-08", "20170208", "2017-02-08T12:10:40.787z",
  "2017-W06-3", "2017-039", "Wed, 08 Feb 2017 12:10:40 GMT", "2017/02/08", "2017-2-8", "2017-02-08T24:00:00", "12:10",
  "2017-02-08T12:10:40 America/New_York", "2017-02-08 12:10:40 EST", "2017-02-08T12:10:40.787+05:30",
  "1999-12-31T23:59:59.999999Z", "0001-01-01", "9999-12-31T23:59:59Z", "2016-02-29", "2017-02-29", "2017-13-01",
  "Sept 3 2017", "2017-02-08T12:10:40.5", "2017-02-08T12:10:40 pdt"]) {
  add("date.free", { $dateFromString: { dateString: str } });
  add("date.free", { $dateFromString: { dateString: str, timezone: "Asia/Kolkata", onError: "E" } });
  add("date.free", { $toDate: str });
}

// --- batch 4: argument shapes and older operators the new modules answer --------
const T5 = DT("2021-03-14T06:30:15.250Z");
for (const op of ["$year", "$month", "$dayOfMonth", "$dayOfYear", "$dayOfWeek", "$millisecond", "$isoWeekYear"]) {
  add("shape.parts", { [op]: [T5, T5] });
  add("shape.parts", { [op]: [] });
  add("shape.parts", { [op]: { date: T5, timezone: "Pacific/Chatham" } });
  add("shape.parts", { [op]: { $dateFromParts: { year: 2020, month: 2, day: 29 } } });
}
for (const f of ["%b %B", "%Y%m%d", "%H%%%M", "%", "%Y-%", "abc", ""]) {
  add("shape.toString", { $dateToString: { date: T5, format: f } });
  add("shape.toString", { $dateToString: { date: T5, format: f, timezone: "+05:30" } });
}
add("shape.toString", { $dateToString: { date: T5, timezone: null } });
add("shape.toString", { $dateToString: { date: T5, format: null } });
add("shape.toString", { $dateToString: { date: "$missing", onNull: "$missing" } });
add("shape.toString", { $dateToString: { format: "%Y" } });
add("shape.toString", { $dateToString: { date: T5, x: 1 } });
add("shape.toString", { $dateToString: "x" });
for (const spec of [
  { input: [1, 2], as: "x", in: "$$x", extra: 1 }, { as: "x", in: 1 }, { input: [1] }, "x", { input: [1], as: "x" },
  { input: [[1, 2], [3]], in: { $size: "$$this" } }, { input: [1, 2], in: "$$nope" }, { input: [1, 2], as: "this", in: "$$this" },
  { input: { $range: [0, 3] }, in: { $multiply: ["$$this", "$$this"] } }, { input: [1, "$missing", 3], in: "$$this" },
  { input: [1, 2, 3], in: "$missingField" },
]) add("shape.map", { $map: spec });
for (const spec of [
  { input: [1, 2], cond: true, limit: null }, { input: [1, 2], cond: true, limit: D(1.0) }, { input: [1, 2], cond: true, limit: D(1.5) },
  { input: [1, 2], cond: true, limit: "a" }, { input: [1, 2], cond: true, limit: -1 }, { input: [1, 2], cond: true, limit: L(1) },
  { input: [1, 2] }, { cond: true }, { input: [1, 2], cond: true, z: 1 }, { input: null, cond: true },
  { input: [1, null, 0, "", [], false], cond: "$$this" }, { input: [1, 2, 3], as: "n", cond: { $gte: ["$$n", 2] } },
]) add("shape.filter", { $filter: spec });
for (const spec of [
  { input: [1, 2], initialValue: [], in: { $concatArrays: ["$$value", ["$$this"]] } }, { input: [1, 2], in: 1 },
  { input: [1, 2], initialValue: 0 }, { initialValue: 0, in: 1 }, { input: [], initialValue: "x", in: 1 },
  { input: "$missing", initialValue: 0, in: 1 }, { input: [1], initialValue: 0, in: 1, q: 1 },
  { input: [{ a: 1 }, { a: 2 }], initialValue: { s: 0 }, in: { s: { $add: ["$$value.s", "$$this.a"] } } },
]) add("shape.reduce", { $reduce: spec });
for (const spec of [
  { input: "abc", regex: "(" }, { input: "abc", regex: "[" }, { input: "aaa", regex: "a*" }, { input: "abcabc", regex: "(?<x>b)(c)" },
  { input: "AbC", regex: /b/, options: "i" }, { input: "line1\nline2", regex: "^line\\d$", options: "m" }, { input: "a", regex: "a", options: 5 },
  { input: "a", regex: "a", options: null }, { input: "ab", regex: "(a)|(b)" }, { input: "x", regex: /x/, options: "" },
]) {
  add("shape.regex", { $regexFindAll: spec });
  add("shape.regex", { $regexMatch: spec });
}
for (const s2 of [
  { input: BinData(0, "AQID"), to: "string", format: "base64url" }, { input: BinData(0, "+/8="), to: "string", format: "base64url" },
  { input: "-_8", to: { type: "binData", subtype: 0 }, format: "base64url" }, { input: "0a0B", to: "binData", format: "hex" },
  { input: "0a0", to: "binData", format: "hex", onError: "E" }, { input: "héllo", to: "binData", format: "utf8" },
  { input: BinData(0, "/w=="), to: "string", format: "utf8", onError: "E" }, { input: BinData(0, "AQID"), to: "string", format: "uuid", onError: "E" },
  { input: BinData(5, "AQID"), to: "binData" }, { input: BinData(5, "AQID"), to: { type: "binData", subtype: 5 } },
  { input: "AQID", to: "binData", format: "bogus" }, { input: 5, to: "string", format: "hex" },
  { input: "5ab9cbfa31c2ab715d42129z", to: "objectId", onError: "E" }, { input: "5AB9CBFA31C2AB715D42129E", to: "objectId" },
  { input: true, to: { type: "bool" } }, { input: 1, to: { type: 8 } }, { input: 1, to: { type: "int", subtype: 1 } },
]) add("shape.convert", { $convert: s2 });
for (const v of ["867dee52-c331-484e-92d1-c56479b8e67e", "867dee52c331484e92d1c56479b8e67e", "{867dee52-c331-484e-92d1-c56479b8e67e}", null, 5]) {
  add("shape.toUUID", { $toUUID: v });
}
for (const a of [[[1, 2, 3], 1, 0, 3], [[1, 2, 3], 3, L(1)], [[], 1], [[null, "$missing"], null], [[[1, 2]], [1, 2]], [[{ a: 1 }], { a: 1.0 }]]) {
  add("shape.indexOfArray", { $indexOfArray: a });
}
for (const a of [[0, 100, 25], [100, 0, -25], [I(-2147483648), I(-2147483646)], [0, 5, L(2)], [0, 5, DEC("2")], [L("2147483647"), L("2147483647")]]) {
  add("shape.range", { $range: a });
}

// --- run them -----------------------------------------------------------------------
const out = [];
for (const c of cases) {
  const pipeline = c.group
    ? [{ $documents: c.docs }, { $group: c.group }, { $sort: { _id: 1 } }]
    : [{ $documents: [c.doc] }, { $replaceWith: { r: c.expr } }];
  const rec = Object.assign({}, c);
  try {
    rec.expect = { ok: db.getSiblingDB("admin").aggregate(pipeline, { promoteValues: false }).toArray() };
  } catch (e) {
    rec.expect = { code: e.code === undefined ? -1 : e.code, errmsg: String(e.errmsg || e.message) };
  }
  out.push(rec);
}
print(EJSON.stringify(out, null, 1, { relaxed: false }));
