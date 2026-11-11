import { describe, expect, it } from 'vitest';
import { classifyCommand, mongoAt, MongoshError, splitMongo, toCommand } from './mongosh';

describe('MongoDB editor: parsed, never evaluated (step 19)', () => {
  it('turns call chains into command documents', () => {
    expect(toCommand('db.users.find({age: {$gt: 30}}, {name: 1}).sort({age: -1}).limit(10).skip(5)')).toMatchObject({
      command: { find: 'users', filter: { age: { $gt: 30 } }, projection: { name: 1 }, sort: { age: -1 }, limit: 10, skip: 5 },
      cls: 'read',
    });
    expect(toCommand('db.users.findOne({_id: ObjectId("64b7f0c2a1b2c3d4e5f60718")})').command).toEqual({
      find: 'users',
      filter: { _id: { $oid: '64b7f0c2a1b2c3d4e5f60718' } },
      limit: 1,
      singleBatch: true,
    });
    expect(toCommand('db.orders.aggregate([{$match: {s: "paid"}}, {$group: {_id: "$c", n: {$sum: 1}}}])').command).toEqual({
      aggregate: 'orders',
      pipeline: [{ $match: { s: 'paid' } }, { $group: { _id: '$c', n: { $sum: 1 } } }],
      cursor: {},
    });
    expect(toCommand("db.getCollection('my coll').countDocuments({a: 1})").command).toEqual({ count: 'my coll', query: { a: 1 } });
    expect(toCommand("db.getSiblingDB('other').x.find()")).toMatchObject({ db: 'other', command: { find: 'x' } });
    expect(toCommand('db.a.b.find()').command).toEqual({ find: 'a.b' });
    expect(toCommand('db["my-coll"].find()').command).toEqual({ find: 'my-coll' });
    expect(toCommand("db.getSiblingDB('d').getCollection('c').drop()")).toMatchObject({ db: 'd', command: { drop: 'c' } });
    expect(toCommand('show collections').command).toEqual({ listCollections: 1, nameOnly: true });
    expect(toCommand('{"find": "users", "limit": 2}').command).toEqual({ find: 'users', limit: 2 });
  });

  it('writes and DDL', () => {
    expect(toCommand('db.u.insertOne({a: NumberLong("9007199254740993"), d: NumberDecimal("1.10")})')).toMatchObject({
      command: { insert: 'u', documents: [{ a: { $numberLong: '9007199254740993' }, d: { $numberDecimal: '1.10' } }] },
      cls: 'write',
    });
    expect(toCommand('db.u.updateMany({a: 1}, {$set: {b: 2}}, {upsert: true})').command).toEqual({
      update: 'u',
      updates: [{ q: { a: 1 }, u: { $set: { b: 2 } }, multi: true, upsert: true }],
    });
    expect(toCommand('db.u.deleteOne({a: 1})').command).toEqual({ delete: 'u', deletes: [{ q: { a: 1 }, limit: 1 }] });
    expect(toCommand('db.u.createIndex({a: 1, b: -1})')).toMatchObject({
      command: { createIndexes: 'u', indexes: [{ key: { a: 1, b: -1 }, name: 'a_1_b_-1' }] },
      cls: 'ddl',
    });
    expect(toCommand('db.u.drop()').cls).toBe('ddl');
    expect(toCommand('db.u.aggregate([{$match: {}}, {$out: "x"}])').cls).toBe('write');
    expect(toCommand('db.u.find({}).explain()').command).toEqual({ explain: { find: 'u', filter: {} }, verbosity: 'queryPlanner' });
  });

  it('helpers become Extended JSON v2', () => {
    const c = toCommand(
      'db.t.insertOne({d: ISODate("2026-11-10T00:00:00Z"), i: NumberInt(7), u: UUID("0123456789abcdef0123456789abcdef"), r: /ab+c/i, ts: Timestamp(5, 1), m: MinKey, x: -Infinity, big: 12345678901234567890n})',
    ).command as { documents: Record<string, unknown>[] };
    expect(c.documents[0]).toEqual({
      d: { $date: { $numberLong: String(Date.parse('2026-11-10T00:00:00Z')) } },
      i: { $numberInt: '7' },
      u: { $binary: { base64: 'ASNFZ4mrze8BI0VniavN7w==', subType: '04' } },
      r: { $regularExpression: { pattern: 'ab+c', options: 'i' } },
      ts: { $timestamp: { t: 5, i: 1 } },
      m: { $minKey: 1 },
      x: { $numberDouble: '-Infinity' },
      big: { $numberLong: '12345678901234567890' },
    });
  });

  // The property chapter 44 asks for: nothing executable gets through.
  const code = [
    'db.u.find({$where: function() { return true }})',
    'db.u.find({a: (() => 1)()})',
    'var x = 1',
    'x = db.u.find()',
    'db.u.find({a: x})',
    'db.u.find({a: 1 + 1})',
    'db.u.find({a: `${process.env}`})',
    'db.u.find({...other})',
    'db.u.find({[k]: 1})',
    'db.u.find({get a() { return 1 }})',
    'for (;;) {}',
    'while (true) db.u.drop()',
    'db.u.find(); db.u.drop()',
    'eval("db.u.drop()")',
    'this.constructor.constructor("return 1")()',
    'db.u.find({a: new Function("x")})',
    'db.u.find({a: Math.random()})',
    'db.u.find({a: db.v.findOne()})',
    'require("fs")',
    'import("x")',
    'db.u.mapReduce(function(){}, function(){})',
    'db.u.find({a: 1}).forEach(printjson)',
    'db.u.find({a: typeof x})',
    'db.u.find({a: ObjectId()})',
    'db.u.find({a: new Date()})',
    'db[x].find()',
    'db.u.find().sort(x)',
    'db.getSiblingDB(name).u.find()',
  ];
  it.each(code)('refuses %s', (t) => {
    expect(() => toCommand(t)).toThrow(MongoshError);
  });

  it('errors carry the position of the offending part', () => {
    try {
      toCommand('db.users.find({a: x})');
      expect.unreachable();
    } catch (e) {
      expect(e).toBeInstanceOf(MongoshError);
      const m = e as MongoshError;
      expect('db.users.find({a: x})'.slice(m.start, m.end)).toBe('x');
    }
  });

  it('splits statements on ; and new lines between complete calls', () => {
    const t = 'db.a.find({x: ";"})\n  .limit(2)\ndb.b.find({\n  y: 1\n});db.c.drop()';
    expect(splitMongo(t).map((s) => s.text)).toEqual(['db.a.find({x: ";"})\n  .limit(2)', 'db.b.find({\n  y: 1\n})', 'db.c.drop()']);
    expect(mongoAt(t, t.indexOf('y:'))?.text).toBe('db.b.find({\n  y: 1\n})');
  });

  it('classifies command documents', () => {
    expect(classifyCommand({ find: 'a' })).toBe('read');
    expect(classifyCommand({ insert: 'a' })).toBe('write');
    expect(classifyCommand({ dropDatabase: 1 })).toBe('ddl');
  });
});
