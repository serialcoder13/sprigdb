// The schema-validation and query-operator differential corpus (M3, 12 Nov 2026).
//
// Run with mongosh against a reference MongoDB 8.0 on a private port:
//
//   mongosh --quiet --host 127.0.0.1 --port 47317 gen_cases.js > cases.json
//
// A "validator" case creates a collection with the validator and inserts
// the document; the answer is ok, the error's code and `errInfo`, or the
// `create` error's code. A "filter" case inserts `docs` into a collection
// without a validator and runs `find(filter)` sorted by `_id`; the answer
// is the matching `_id`s or the error's code. `tests/validate_diff.rs`
// runs the same cases with SprigDB's engine and compares.

const I = (n) => NumberInt(n);
const L = (n) => NumberLong(String(n));
const D = (x) => Double(x);
const DEC = (s) => NumberDecimal(s);
const cases = [];
let seq = 0;
function v(area, validator, doc) {
  cases.push({ name: `${area}#${seq++}`, validator, doc: Object.assign({ _id: I(seq) }, doc) });
}
function js(area, schema, docs) {
  for (const d of docs) v(area, { $jsonSchema: schema }, d);
}
function f(area, filter, docs) {
  cases.push({ name: `${area}#${seq++}`, filter, docs });
}

// --- $jsonSchema: scalar keywords ----------------------------------------
const SCALARS = [{ a: I(2) }, { a: I(7) }, { a: D(2.5) }, { a: L(9) }, { a: DEC("3") }, { a: "q" }, { a: "xyzzy" }, { a: "héé" },
  { a: true }, { a: null }, { a: [1, 2] }, { a: { b: 1 } }, {}, { a: D(NaN) }];
js("js.min", { properties: { a: { minimum: I(5) } } }, SCALARS);
js("js.max", { properties: { a: { maximum: D(3) } } }, SCALARS);
js("js.exmin", { properties: { a: { minimum: I(7), exclusiveMinimum: true } } }, [{ a: I(7) }, { a: I(8) }, { a: D(7.0) }]);
js("js.exmax", { properties: { a: { maximum: I(7), exclusiveMaximum: true } } }, [{ a: I(7) }, { a: I(6) }, { a: L(7) }]);
js("js.multipleOf", { properties: { a: { multipleOf: I(3) } } }, [{ a: I(9) }, { a: I(10) }, { a: D(4.5) }, { a: L(12) }, { a: D(6) }]);
js("js.multipleOfD", { properties: { a: { multipleOf: D(0.5) } } }, [{ a: D(1.25) }, { a: D(1.5) }, { a: I(3) }]);
js("js.strlen", { properties: { a: { minLength: I(2), maxLength: I(3) } } }, SCALARS);
js("js.pattern", { properties: { a: { pattern: "^x" } } }, SCALARS);
js("js.allscalar", { properties: { a: { minimum: 5, maximum: 1, multipleOf: 3, minLength: 9, maxLength: 0, pattern: "z", enum: [9] } } },
  [{ a: I(2) }, { a: "q" }, { a: true }]);
// --- types --------------------------------------------------------------------
for (const t of ["int", "long", "double", "decimal", "number", "string", "object", "array", "bool", "null", "date", "objectId", "binData", "regex", "timestamp"]) {
  js("js.bsonType." + t, { properties: { a: { bsonType: t } } }, [{ a: I(1) }, { a: "s" }, { a: [1] }]);
}
js("js.bsonTypeArr", { properties: { a: { bsonType: ["string", "int"] } } }, [{ a: true }, { a: I(1) }, { a: L(1) }]);
for (const t of ["object", "array", "number", "string", "boolean", "null"]) {
  js("js.type." + t, { properties: { a: { type: t } } }, [{ a: I(1) }, { a: false }, { a: null }]);
}
js("js.typeArr", { properties: { a: { type: ["string", "number"] } } }, [{ a: true }, { a: D(1) }]);
js("js.typeFirst", { properties: { a: { bsonType: "string", enum: [9], minimum: 3 } } }, [{ a: I(2) }, { a: "x" }]);
js("js.typeLast", { properties: { a: { type: "string", enum: [9], allOf: [{ minimum: 5 }] } } }, [{ a: I(2) }]);
js("js.rootType", { bsonType: "object", required: ["x"] }, [{}, { x: 1 }]);
js("js.rootTitle", { title: "T", description: "D", required: ["x"] }, [{}]);
// --- arrays ---------------------------------------------------------------
const ARRS = [{ a: [] }, { a: [1, 1] }, { a: [1, "x", "y"] }, { a: [1, 2, 3, 4] }, { a: "notarr" }, { a: [[1], [1]] }, { a: [{ x: 1, y: 2 }, { y: 2, x: 1 }] },
  { a: [1, D(1), L(1)] }, { a: [null, null] }, {}];
js("js.items", { properties: { a: { items: { bsonType: "int" } } } }, ARRS);
js("js.minmaxItems", { properties: { a: { minItems: 1, maxItems: 2 } } }, ARRS);
js("js.unique", { properties: { a: { uniqueItems: true } } }, ARRS);
js("js.tuple", { properties: { a: { items: [{ bsonType: "int" }, { bsonType: "string" }] } } }, ARRS);
js("js.tupleAddF", { properties: { a: { items: [{ bsonType: "int" }], additionalItems: false } } }, ARRS);
js("js.tupleAddS", { properties: { a: { items: [{ bsonType: "int" }], additionalItems: { bsonType: "string" } } } }, ARRS);
js("js.addItemsNoTuple", { properties: { a: { additionalItems: false } } }, ARRS.slice(0, 3));
js("js.itemsTitle", { properties: { a: { items: { title: "IT", description: "ID", bsonType: "int" } } } }, [{ a: ["x"] }]);
js("js.arrayAll", { properties: { a: { minItems: 5, maxItems: 0, uniqueItems: true, items: { bsonType: "string" } } } }, [{ a: [1, 1] }]);
// --- objects ------------------------------------------------------------------
const OBJS = [{}, { a: 1 }, { a: 1, b: 2 }, { a: "x", b: "y", c: 3 }, { xa: 1 }, { xa: "s", ya: 2 }, { b: 1, c: 1, d: 1, e: 1 }];
js("js.required", { required: ["a", "b"] }, OBJS);
js("js.props", { properties: { b: { bsonType: "int" }, a: { bsonType: "int" } } }, OBJS);
js("js.addPropsF", { properties: { _id: {}, a: {} }, additionalProperties: false }, OBJS);
js("js.addPropsFnoid", { properties: { a: {} }, additionalProperties: false }, OBJS);
js("js.addPropsS", { properties: { _id: {} }, additionalProperties: { bsonType: "string" } }, OBJS);
js("js.patternProps", { patternProperties: { "^x": { bsonType: "string" }, "a$": { bsonType: "string" } } }, OBJS);
js("js.patternAddF", { properties: { _id: {} }, patternProperties: { "^x": {} }, additionalProperties: false }, OBJS);
js("js.minmaxProps", { minProperties: 3, maxProperties: 3 }, OBJS);
js("js.depsArr", { dependencies: { a: ["b"], c: ["d", "e"] } }, OBJS);
js("js.depsSchema", { dependencies: { a: { required: ["c"] }, b: { properties: { c: { bsonType: "string" } } } } }, OBJS);
js("js.nested", { properties: { o: { bsonType: "object", required: ["z"], properties: { z: { bsonType: "int", description: "zz" } } } } },
  [{ o: {} }, { o: { z: "s" } }, { o: { z: I(1) } }, { o: 5 }, { o: [] }]);
js("js.propTitle", { properties: { a: { title: "TT", description: "DD", bsonType: "int" } } }, [{ a: "x" }]);
js("js.objAll", { properties: { a: { minProperties: 5, maxProperties: 0, required: ["x"], properties: { y: { bsonType: "int" } }, patternProperties: { "^z": { bsonType: "int" } }, additionalProperties: false, dependencies: { y: ["w"] } } } },
  [{ a: { y: "s", zz: "s", q: 1 } }]);
// --- logical and enum ---------------------------------------------------------
const LOG = [{}, { a: I(1) }, { a: "x" }, { a: I(1), b: I(2) }, { a: [1] }, { a: { k: 1 } }];
js("js.allOf", { allOf: [{ required: ["a"] }, { properties: { a: { bsonType: "int" } } }] }, LOG);
js("js.anyOf", { anyOf: [{ required: ["b"] }, { properties: { a: { bsonType: "string" } }, required: ["a"] }] }, LOG);
js("js.oneOf", { oneOf: [{ required: ["a"] }, { required: ["b"] }] }, LOG);
js("js.not", { not: { required: ["b"] } }, LOG);
js("js.enumRoot", { properties: { a: { enum: [I(1), "x", { k: 1 }, [1], null] } } }, LOG.concat([{ a: D(1) }, { a: L(1) }, { a: null }, { a: { k: D(1) } }]));
js("js.enumOrder", { properties: { a: { enum: [{ x: 1, y: 2 }] } } }, [{ a: { y: 2, x: 1 } }, { a: { x: 1 } }]);
js("js.logicTitle", { allOf: [{ title: "AT", description: "AD", required: ["z"] }], anyOf: [{ title: "B", required: ["q"] }] }, [{}]);
js("js.subLogic", { properties: { a: { anyOf: [{ bsonType: "int" }, { minLength: 3 }], oneOf: [{ bsonType: "int" }, { minimum: 0 }], not: { bsonType: "bool" } } } },
  [{ a: "x" }, { a: I(1) }, { a: true }, { a: "long" }]);
js("js.everything", { allOf: [{ required: ["x"] }, { required: ["y"] }], anyOf: [{ required: ["p"] }, { required: ["q"] }], oneOf: [{ required: ["_id"] }, { properties: { _id: { bsonType: "int" } } }], not: { required: ["_id"] }, enum: [{ _id: 2 }] }, [{}]);
// --- schema parse errors (create fails) ---------------------------------------
for (const s of [{ foo: 1 }, { $ref: "x" }, { format: "date" }, { minimum: "x" }, { required: [] }, { required: ["a", "a"] }, { required: "a" }, { enum: [] },
  { enum: [1, 1] }, { bsonType: "bogus" }, { type: "int" }, { type: "integer" }, { type: "string", bsonType: "string" }, { minLength: -1 }, { minItems: 1.5 },
  { pattern: "(" }, { pattern: 5 }, { exclusiveMinimum: true }, { exclusiveMinimum: 1, minimum: 1 }, { items: 5 }, { uniqueItems: 1 }, { properties: 5 }, { properties: { a: 5 } },
  { allOf: [] }, { allOf: 5 }, { allOf: [5] }, { not: 5 }, { dependencies: { b: [] } }, { dependencies: { b: 5 } }, { multipleOf: 0 }, { multipleOf: -1 },
  { title: 5 }, { additionalProperties: 5 }, { additionalItems: 5, items: [{}] }, { maxProperties: "x" }]) {
  v("js.parse", { $jsonSchema: s }, {});
}
// --- query-operator validators --------------------------------------------------
const Q = [{ a: I(7) }, { a: "x" }, { a: [2, 3] }, { a: [] }, { a: [[1, 2], 3] }, { a: null }, {}, { a: { b: 2 } }, { a: [{ b: 2 }, { b: 3 }, { c: 1 }] }, { a: I(1) }];
for (const val of [
  { a: I(1) }, { a: { $eq: I(1) } }, { a: { $ne: I(1) } }, { a: { $gt: I(1), $lt: I(5) } }, { a: { $gte: I(5) } }, { a: { $lte: I(3) } },
  { a: { $in: [I(1), I(2)] } }, { a: { $nin: [I(1), I(2)] } }, { a: { $exists: true } }, { a: { $exists: false } }, { a: { $type: "string" } },
  { a: { $type: ["int", "string"] } }, { a: { $size: I(2) } }, { a: { $regex: "^x" } }, { a: /^x/ }, { a: { $all: [I(2)] } },
  { a: { $elemMatch: { $gt: I(5) } } }, { a: { $mod: [I(2), I(0)] } }, { a: { $not: { $gt: I(5) } } }, { a: { $not: /x/ } },
  { "a.b": I(1) }, { "a.0": I(1) }, { $or: [{ a: I(1) }, { b: I(2) }] }, { $and: [{ a: I(1) }] }, { $nor: [{ a: I(7) }, { a: "x" }] },
  { $expr: { $gt: ["$a", I(5)] } }, { a: { $gt: I(1) }, b: I(1), c: { $type: "int", $exists: true } }, { $or: [{ a: I(1), b: I(1) }, { c: I(1) }] },
  { a: { $not: { $gt: I(1), $lt: I(5) } } }, { a: { $ne: null } }, { a: { $bitsAllSet: I(3) } }, { a: { $bitsAnyClear: I(1) } },
  { a: { $elemMatch: { b: I(1) } } }, { a: { $gt: I(1) }, $jsonSchema: { required: ["z"] } }, { $and: [{ a: { $gt: I(1), $lt: I(5) } }] },
]) {
  for (const d of Q) v("q", val, d);
}
// --- query operators in find ----------------------------------------------------
const BD = [{ _id: 1, a: I(0) }, { _id: 2, a: I(1) }, { _id: 3, a: I(54) }, { _id: 4, a: I(-1) }, { _id: 5, a: L("9223372036854775807") }, { _id: 6, a: D(3) },
  { _id: 7, a: D(3.5) }, { _id: 8, a: "x" }, { _id: 9, a: BinData(0, "Ag==") }, { _id: 10, a: BinData(0, "AQID") }, { _id: 11, a: [I(1), I(4)] }, { _id: 12 },
  { _id: 13, a: DEC("6") }, { _id: 14, a: D(-0.0) }, { _id: 15, a: D(1e20) }];
for (const op of ["$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"]) {
  for (const m of [I(0), I(1), I(6), I(35), [1, 5], [0], [], [63], [64, 100], BinData(0, "Ag=="), BinData(0, "AwA="), D(2), I(-1), D(1.5), "x", [-1], ["a"], [D(1.5)], L(1)]) {
    f("bits", { a: { [op]: m } }, BD);
  }
}
const JD = [{ _id: 1, a: I(5), b: "xy" }, { _id: 2, a: I(1) }, { _id: 3, a: "q", b: "xyz" }, { _id: 4 }, { _id: 5, a: [1, 1] }, { _id: 6, a: { k: 1 } }];
for (const s of [{ required: ["a"] }, { properties: { a: { bsonType: "int", minimum: 3 } } }, { properties: { b: { maxLength: 2 } } }, { additionalProperties: false, properties: { _id: {}, a: {} } },
  { properties: { a: { uniqueItems: true } } }, { not: { required: ["b"] } }, { properties: { a: { enum: [1, "q"] } } }, { foo: 1 }]) {
  f("jsfind", { $jsonSchema: s }, JD);
  f("jsfind.and", { $jsonSchema: s, _id: { $gt: 2 } }, JD);
}

// --- run them -----------------------------------------------------------------------
const db3 = db.getSiblingDB("m3diff");
const out = [];
for (const c of cases) {
  const rec = Object.assign({}, c);
  db3.c.drop();
  if (c.validator) {
    try {
      db3.createCollection("c", { validator: c.validator });
    } catch (e) {
      rec.expect = { createCode: e.code === undefined ? -1 : e.code, errmsg: String(e.errmsg || e.message) };
      out.push(rec);
      continue;
    }
    try {
      db3.c.insertOne(c.doc);
      rec.expect = { ok: true };
    } catch (e) {
      rec.expect = { code: e.code === undefined ? -1 : e.code, errInfo: e.errInfo === undefined ? null : e.errInfo, errmsg: String(e.errmsg || e.message) };
    }
  } else {
    db3.c.insertMany(c.docs);
    try {
      rec.expect = { ids: db3.c.find(c.filter).sort({ _id: 1 }).toArray().map((d) => d._id) };
    } catch (e) {
      rec.expect = { code: e.code === undefined ? -1 : e.code, errmsg: String(e.errmsg || e.message) };
    }
  }
  out.push(rec);
}
print(EJSON.stringify(out, null, 1, { relaxed: false }));
