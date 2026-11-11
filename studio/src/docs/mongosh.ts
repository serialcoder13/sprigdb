// The MongoDB editor's language (chapter 44 step 19): a mongosh subset that
// is parsed, never evaluated. acorn (MIT) produces an AST; Studio accepts
// only a call chain rooted at `db` (find, aggregate, insertOne, updateMany
// and similar), literals, and the helpers ObjectId, ISODate, NumberLong,
// NumberInt, NumberDecimal, Timestamp, UUID, BinData, MinKey and MaxKey,
// which become Extended JSON v2. Variables, functions, loops, operators and
// anything else are errors with their position, so pasted text can't run
// code. The result is one command document for Sql.Query with
// `language: mongo` (chapter 37, "Languages").

import { parse, type Node } from 'acorn';
import type { StatementClass } from '../lib/sql';

export class MongoshError extends Error {
  constructor(
    message: string,
    readonly start: number,
    readonly end: number,
  ) {
    super(message);
  }
}

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
export type Doc = { [k: string]: Json };

export interface MongoCommand {
  /** The command document (Extended JSON v2). */
  command: Doc;
  /** A database named in the text (`db.getSiblingDB('x')`), else the tab's. */
  db?: string;
  /** read, write or ddl, for studio.confirm_writes. */
  cls: StatementClass;
  /** The shell method, for messages ("find", "insertOne", ...). */
  method: string;
}

// acorn's ESTree nodes, loosely: Studio reads only the fields it needs.
interface N extends Node {
  [k: string]: unknown;
}

const fail = (n: { start: number; end: number }, msg: string): never => {
  throw new MongoshError(msg, n.start, n.end);
};

const node = (v: unknown): N => v as N;

function isDoc(v: Json): v is Doc {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

const HEX24 = /^[0-9a-fA-F]{24}$/;

function hexToBase64(hex: string): string {
  const bytes = hex.match(/../g)?.map((h) => parseInt(h, 16)) ?? [];
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

function strArg(call: N, i: number, what: string): string {
  const args = call.arguments as N[];
  const a = args[i];
  if (!a) return fail(call, `${what} needs an argument`);
  const v = literal(a);
  if (typeof v !== 'string') return fail(a, `${what} takes a string`);
  return v;
}

function numArg(call: N, i: number, what: string): number {
  const args = call.arguments as N[];
  const a = args[i];
  if (!a) return fail(call, `${what} needs an argument`);
  const v = literal(a);
  if (typeof v === 'number') return v;
  if (typeof v === 'string' && v.trim() !== '' && !Number.isNaN(Number(v))) return Number(v);
  return fail(a, `${what} takes a number`);
}

/** A helper call (ObjectId("..."), ISODate(), ...) as Extended JSON. */
function helper(n: N, name: string): Json {
  const args = n.arguments as N[];
  switch (name) {
    case 'ObjectId': {
      if (!args.length) return fail(n, 'ObjectId() needs its 24 hex digits here: Studio never makes ids itself');
      const h = strArg(n, 0, 'ObjectId');
      if (!HEX24.test(h)) return fail(args[0]!, 'ObjectId takes 24 hex digits');
      return { $oid: h.toLowerCase() };
    }
    case 'ISODate':
    case 'Date': {
      if (!args.length) return fail(n, `${name}() needs a date: Studio doesn't read the clock for you`);
      const a = literal(args[0]!);
      const ms = typeof a === 'number' ? a : typeof a === 'string' ? Date.parse(a) : NaN;
      if (Number.isNaN(ms)) return fail(args[0]!, `${name} takes an ISO-8601 date or milliseconds`);
      return { $date: { $numberLong: String(Math.trunc(ms)) } };
    }
    case 'NumberLong':
    case 'Long': {
      const a = literal(args[0] ?? fail(n, 'NumberLong needs a value'));
      const t = typeof a === 'number' || typeof a === 'string' ? String(a) : '';
      if (!/^-?\d+$/.test(t)) return fail(args[0]!, 'NumberLong takes an integer');
      return { $numberLong: t };
    }
    case 'NumberInt':
    case 'Int32': {
      const v = numArg(n, 0, 'NumberInt');
      if (!Number.isInteger(v) || v < -2147483648 || v > 2147483647) return fail(args[0]!, 'NumberInt takes a 32-bit integer');
      return { $numberInt: String(v) };
    }
    case 'NumberDecimal':
    case 'Decimal128': {
      const a = literal(args[0] ?? fail(n, 'NumberDecimal needs a value'));
      const t = typeof a === 'number' || typeof a === 'string' ? String(a) : '';
      if (!/^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$|^[-+]?(Infinity|NaN)$/.test(t))
        return fail(args[0]!, 'NumberDecimal takes a decimal number as a string');
      return { $numberDecimal: t };
    }
    case 'Double': {
      const v = numArg(n, 0, 'Double');
      return { $numberDouble: Number.isInteger(v) ? `${v}.0` : String(v) };
    }
    case 'Timestamp': {
      const t = numArg(n, 0, 'Timestamp');
      const i = args.length > 1 ? numArg(n, 1, 'Timestamp') : 0;
      return { $timestamp: { t, i } };
    }
    case 'UUID': {
      const s = strArg(n, 0, 'UUID').replace(/-/g, '');
      if (!/^[0-9a-fA-F]{32}$/.test(s)) return fail(args[0]!, 'UUID takes 32 hex digits');
      return { $binary: { base64: hexToBase64(s), subType: '04' } };
    }
    case 'BinData': {
      const sub = numArg(n, 0, 'BinData');
      const b64 = strArg(n, 1, 'BinData');
      return { $binary: { base64: b64, subType: sub.toString(16).padStart(2, '0') } };
    }
    case 'MinKey':
      return { $minKey: 1 };
    case 'MaxKey':
      return { $maxKey: 1 };
    default:
      return fail(n, `${name}() isn't allowed: Studio runs commands, never code`);
  }
}

/** A literal value: objects, arrays, strings, numbers, booleans, null,
 * regular expressions and the helpers. Everything else is refused. */
export function literal(n: N): Json {
  switch (n.type) {
    case 'Literal': {
      const regex = n.regex as { pattern: string; flags: string } | undefined;
      if (regex) return { $regularExpression: { pattern: regex.pattern, options: regex.flags.split('').sort().join('') } };
      const v = n.value;
      if (typeof v === 'bigint') return { $numberLong: v.toString() };
      if (v === null || typeof v === 'string' || typeof v === 'boolean') return v;
      if (typeof v === 'number') return v;
      return fail(n, 'this literal has no document form');
    }
    case 'TemplateLiteral': {
      if ((n.expressions as N[]).length) return fail(n, 'template strings with ${…} are code');
      return (n.quasis as N[]).map((q) => (q.value as { cooked: string }).cooked).join('');
    }
    case 'UnaryExpression': {
      const arg = node(n.argument);
      if ((n.operator === '-' || n.operator === '+') && arg.type === 'Literal' && typeof arg.value === 'number')
        return n.operator === '-' ? -arg.value : arg.value;
      if ((n.operator === '-' || n.operator === '+') && arg.type === 'Identifier' && arg.name === 'Infinity')
        return { $numberDouble: n.operator === '-' ? '-Infinity' : 'Infinity' };
      return fail(n, `the operator ${String(n.operator)} is code`);
    }
    case 'Identifier': {
      if (n.name === 'Infinity') return { $numberDouble: 'Infinity' };
      if (n.name === 'NaN') return { $numberDouble: 'NaN' };
      if (n.name === 'MinKey') return { $minKey: 1 };
      if (n.name === 'MaxKey') return { $maxKey: 1 };
      return fail(n, `'${String(n.name)}' is a variable: write the value itself`);
    }
    case 'ArrayExpression': {
      return (n.elements as (N | null)[]).map((e) => {
        if (!e) return fail(n, 'arrays need a value at every position');
        if (e.type === 'SpreadElement') return fail(e, 'spreading (...) is code');
        return literal(e);
      });
    }
    case 'ObjectExpression': {
      const out: Doc = {};
      for (const p of n.properties as N[]) {
        if (p.type !== 'Property') return fail(p, 'spreading (...) is code');
        if (p.computed) return fail(p, 'computed keys ([…]) are code');
        if (p.kind !== 'init' || p.method) return fail(p, 'methods and getters are code');
        if (p.shorthand) return fail(p, 'write the key and its value (key: value)');
        const k = node(p.key);
        const key = k.type === 'Identifier' ? String(k.name) : k.type === 'Literal' ? String(k.value) : fail(k, 'bad key');
        out[key] = literal(node(p.value));
      }
      return out;
    }
    case 'CallExpression':
    case 'NewExpression': {
      const callee = node(n.callee);
      if (callee.type !== 'Identifier') return fail(n, 'only the document helpers (ObjectId, ISODate, NumberLong, ...) may be called here');
      return helper(n, String(callee.name));
    }
    default:
      return fail(n, `${n.type.replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase()} is code: Studio runs commands, never code`);
  }
}

interface Step {
  name: string;
  args: N[];
  call: N;
}

/** Flattens `db.coll.m1(a).m2(b)` into its root and steps. */
function chain(expr: N): { root: N; coll: string | null; db: string | null; steps: Step[] } {
  const steps: Step[] = [];
  let cur = expr;
  for (;;) {
    if (cur.type === 'CallExpression') {
      const callee = node(cur.callee);
      if (callee.type !== 'MemberExpression') return fail(cur, 'a command starts with db.');
      if (callee.computed) {
        // db['my coll'].find() is fine; anything else in brackets is code.
        return fail(callee, 'use db.getCollection("name") for unusual names');
      }
      steps.unshift({ name: String(node(callee.property).name), args: cur.arguments as N[], call: cur });
      cur = node(callee.object);
      continue;
    }
    break;
  }
  // What's left: db, db.coll, db.getCollection('x'), db.getSiblingDB('d')...
  let db: string | null = null;
  let coll: string | null = null;
  const base = (e: N): void => {
    if (e.type === 'CallExpression') {
      // db.getSiblingDB('d').coll... or db.getCollection('c').find...
      const callee = node(e.callee);
      const name = callee.type === 'MemberExpression' && !callee.computed ? String(node(callee.property).name) : '';
      if (name === 'getSiblingDB' && coll === null) {
        base(node(callee.object));
        if (coll !== null) fail(e, 'getSiblingDB comes right after db');
        db = strArg(e, 0, 'getSiblingDB');
        return;
      }
      if (name === 'getCollection' && coll === null) {
        base(node(callee.object));
        if (coll !== null) fail(e, 'getCollection comes right after db');
        coll = strArg(e, 0, 'getCollection');
        return;
      }
      fail(e, 'only getSiblingDB and getCollection may come before the method');
    }
    if (e.type === 'Identifier') {
      if (e.name !== 'db') fail(e, `'${String(e.name)}' is a variable: commands start with db.`);
      return;
    }
    if (e.type === 'MemberExpression') {
      const obj = node(e.object);
      const prop = node(e.property);
      if (e.computed) {
        if (prop.type !== 'Literal' || typeof prop.value !== 'string') fail(prop, 'a collection name in brackets must be a string');
        base(obj);
        coll = String(prop.value);
        return;
      }
      base(obj);
      if (coll !== null) coll = `${coll}.${String(prop.name)}`;
      else coll = String(prop.name);
      return;
    }
    fail(e, 'a command starts with db.');
  };
  // getSiblingDB / getCollection come out of the steps as leading calls.
  base(cur);
  while (steps.length) {
    const s = steps[0]!;
    if (coll === null && s.name === 'getSiblingDB') {
      db = strArg(s.call, 0, 'getSiblingDB');
      steps.shift();
      continue;
    }
    if (coll === null && s.name === 'getCollection') {
      coll = strArg(s.call, 0, 'getCollection');
      steps.shift();
      continue;
    }
    break;
  }
  return { root: cur, coll, db, steps };
}

const arg = (s: Step, i: number): Json | undefined => {
  const a = s.args[i];
  return a ? literal(a) : undefined;
};

function docArg(s: Step, i: number, what: string): Doc | undefined {
  const v = arg(s, i);
  if (v === undefined) return undefined;
  if (!isDoc(v)) return fail(s.args[i]!, `${what} must be a document`);
  return v;
}

function indexName(keys: Doc): string {
  return Object.entries(keys)
    .map(([k, v]) => `${k}_${typeof v === 'object' ? JSON.stringify(v) : String(v)}`)
    .join('_');
}

const NOT_READ_STAGES = ['$out', '$merge'];

function cursorOptions(steps: Step[], cmd: Doc, allowed: string[]): boolean {
  let explain = false;
  for (const s of steps) {
    if (!allowed.includes(s.name)) {
      if (s.name === 'explain') {
        explain = true;
        continue;
      }
      if (s.name === 'toArray' || s.name === 'pretty') continue;
      return fail(s.call, `.${s.name}() isn't supported after this command`);
    }
    const v = arg(s, 0);
    switch (s.name) {
      case 'sort':
      case 'projection':
      case 'hint':
      case 'collation':
        if (v === undefined) fail(s.call, `.${s.name}() needs an argument`);
        cmd[s.name] = v!;
        break;
      case 'limit':
      case 'skip':
      case 'batchSize':
      case 'maxTimeMS':
        cmd[s.name] = numArg(s.call, 0, s.name);
        break;
      case 'comment':
        cmd.comment = v ?? null;
        break;
      default:
        break;
    }
  }
  return explain;
}

/** One statement of the editor as a command. */
export function toCommand(text: string): MongoCommand {
  const t = text.trim();
  if (!t) throw new MongoshError('nothing to run', 0, 0);
  // `show collections` / `show dbs` as mongosh has them.
  const show = /^show\s+(collections|tables|dbs|databases)\s*;?$/i.exec(t);
  if (show) {
    const what = show[1]!.toLowerCase();
    return what === 'collections' || what === 'tables'
      ? { command: { listCollections: 1, nameOnly: true }, cls: 'read', method: 'show collections' }
      : { command: { listDatabases: 1, nameOnly: true }, cls: 'read', method: 'show dbs' };
  }
  let program: N;
  // A bare command document would parse as a block: read it as an
  // expression, positions shifted back by the added parenthesis.
  const bare = t.startsWith('{');
  const shift = bare ? 1 : 0;
  try {
    program = parse(bare ? `(${text}\n)` : text, { ecmaVersion: 2023, sourceType: 'script', ranges: false }) as unknown as N;
  } catch (e) {
    const pos = Math.max(0, ((e as { pos?: number }).pos ?? 0) - shift);
    throw new MongoshError(e instanceof Error ? e.message.replace(/ \(\d+:\d+\)$/, '') : 'syntax error', pos, pos + 1);
  }
  if (bare) {
    try {
      return toCommandOf(program);
    } catch (e) {
      if (e instanceof MongoshError) throw new MongoshError(e.message, Math.max(0, e.start - 1), Math.max(0, e.end - 1));
      throw e;
    }
  }
  return toCommandOf(program);
}

function toCommandOf(program: N): MongoCommand {
  const body = program.body as N[];
  if (body.length !== 1) {
    const second = body[1];
    return fail(second ?? program, 'one command per statement: separate them with ; and run each');
  }
  const stmt = body[0]!;
  if (stmt.type !== 'ExpressionStatement') return fail(stmt, `${stmt.type} is code: Studio runs commands, never code`);
  const expr = node(stmt.expression);
  // A bare command document: {"find": "users", ...}.
  if (expr.type === 'ObjectExpression') {
    const command = literal(expr) as Doc;
    return { command, cls: classifyCommand(command), method: Object.keys(command)[0] ?? 'command' };
  }
  if (expr.type !== 'CallExpression') return fail(expr, 'a command is a call such as db.users.find({...})');
  const { coll, db, steps } = chain(expr);
  const first = steps[0];
  if (!first) return fail(expr, 'call a method, such as db.users.find()');
  const rest = steps.slice(1);
  const m = first.name;
  const done = (command: Doc, cls: StatementClass, explain = false): MongoCommand => {
    const c: Doc = explain ? { explain: command, verbosity: 'queryPlanner' } : command;
    return { command: c, cls: explain ? 'read' : cls, method: m, ...(db ? { db } : {}) };
  };
  // Database-level methods.
  if (coll === null) {
    if (rest.length) fail(rest[0]!.call, `.${rest[0]!.name}() isn't supported here`);
    switch (m) {
      case 'runCommand': {
        const c = docArg(first, 0, 'runCommand');
        if (!c) return fail(first.call, 'runCommand needs a command document');
        return done(c, classifyCommand(c));
      }
      case 'getCollectionNames':
      case 'getCollectionInfos':
        return done({ listCollections: 1, ...(m === 'getCollectionNames' ? { nameOnly: true } : {}), ...(docArg(first, 0, 'filter') ? { filter: docArg(first, 0, 'filter')! } : {}) }, 'read');
      case 'createCollection':
        return done({ create: strArg(first.call, 0, 'createCollection'), ...(docArg(first, 1, 'options') ?? {}) }, 'ddl');
      case 'stats':
        return done({ dbStats: 1 }, 'read');
      case 'dropDatabase':
        return done({ dropDatabase: 1 }, 'ddl');
      default:
        return fail(first.call, `db.${m}() isn't supported: use db.runCommand({...})`);
    }
  }
  switch (m) {
    case 'find':
    case 'findOne': {
      const cmd: Doc = { find: coll };
      const filter = docArg(first, 0, 'the filter');
      const projection = docArg(first, 1, 'the projection');
      if (filter) cmd.filter = filter;
      if (projection) cmd.projection = projection;
      if (m === 'findOne') {
        cmd.limit = 1;
        cmd.singleBatch = true;
      }
      const explain = cursorOptions(rest, cmd, ['sort', 'limit', 'skip', 'projection', 'hint', 'collation', 'batchSize', 'maxTimeMS', 'comment']);
      return done(cmd, 'read', explain);
    }
    case 'aggregate': {
      const p = arg(first, 0);
      if (!Array.isArray(p)) return fail(first.args[0] ?? first.call, 'aggregate takes a pipeline: an array of stages');
      const cmd: Doc = { aggregate: coll, pipeline: p, cursor: {}, ...(docArg(first, 1, 'options') ?? {}) };
      const explain = cursorOptions(rest, cmd, ['batchSize', 'maxTimeMS', 'comment', 'hint']);
      const writes = p.some((s) => isDoc(s) && NOT_READ_STAGES.some((k) => k in s));
      return done(cmd, writes ? 'write' : 'read', explain);
    }
    case 'countDocuments':
    case 'count':
      noRest(rest);
      return done({ count: coll, ...(docArg(first, 0, 'the filter') ? { query: docArg(first, 0, 'the filter')! } : {}) }, 'read');
    case 'estimatedDocumentCount':
      noRest(rest);
      return done({ count: coll }, 'read');
    case 'distinct': {
      noRest(rest);
      const key = strArg(first.call, 0, 'distinct');
      const q = docArg(first, 1, 'the filter');
      return done({ distinct: coll, key, ...(q ? { query: q } : {}) }, 'read');
    }
    case 'insertOne':
    case 'insert': {
      noRest(rest);
      const d = arg(first, 0);
      if (d === undefined) return fail(first.call, `${m} needs a document`);
      const docs = Array.isArray(d) ? d : [d];
      return done({ insert: coll, documents: docs }, 'write');
    }
    case 'insertMany': {
      noRest(rest);
      const d = arg(first, 0);
      if (!Array.isArray(d)) return fail(first.args[0] ?? first.call, 'insertMany takes an array of documents');
      const o = docArg(first, 1, 'options');
      return done({ insert: coll, documents: d, ...(o && 'ordered' in o ? { ordered: o.ordered! } : {}) }, 'write');
    }
    case 'updateOne':
    case 'updateMany':
    case 'replaceOne': {
      noRest(rest);
      const q = docArg(first, 0, 'the filter') ?? fail(first.call, `${m} needs a filter`);
      const u = arg(first, 1);
      if (u === undefined || !(isDoc(u) || Array.isArray(u))) return fail(first.call, `${m} needs ${m === 'replaceOne' ? 'a replacement document' : 'an update'}`);
      const o = docArg(first, 2, 'options') ?? {};
      const upd: Doc = { q, u, multi: m === 'updateMany' };
      for (const k of ['upsert', 'arrayFilters', 'hint', 'collation']) if (k in o) upd[k] = o[k]!;
      return done({ update: coll, updates: [upd] }, 'write');
    }
    case 'deleteOne':
    case 'deleteMany': {
      noRest(rest);
      const q = docArg(first, 0, 'the filter') ?? fail(first.call, `${m} needs a filter ({} for every document)`);
      return done({ delete: coll, deletes: [{ q, limit: m === 'deleteOne' ? 1 : 0 }] }, 'write');
    }
    case 'createIndex': {
      noRest(rest);
      const keys = docArg(first, 0, 'the keys') ?? fail(first.call, 'createIndex needs its keys');
      const o = docArg(first, 1, 'options') ?? {};
      return done({ createIndexes: coll, indexes: [{ key: keys, name: typeof o.name === 'string' ? o.name : indexName(keys), ...o }] }, 'ddl');
    }
    case 'getIndexes':
      noRest(rest);
      return done({ listIndexes: coll }, 'read');
    case 'dropIndex': {
      noRest(rest);
      const v = arg(first, 0);
      if (v === undefined) return fail(first.call, 'dropIndex needs a name or the keys');
      return done({ dropIndexes: coll, index: v }, 'ddl');
    }
    case 'drop':
      noRest(rest);
      return done({ drop: coll }, 'ddl');
    case 'stats':
      noRest(rest);
      return done({ collStats: coll }, 'read');
    default:
      return fail(first.call, `.${m}() isn't supported: use db.runCommand({...})`);
  }
}

function noRest(rest: Step[]): void {
  if (rest[0]) fail(rest[0].call, `.${rest[0].name}() isn't supported after this command`);
}

const READS = new Set(['find', 'aggregate', 'count', 'distinct', 'listCollections', 'listIndexes', 'listDatabases', 'dbStats', 'collStats', 'explain', 'ping', 'buildInfo', 'hello', 'isMaster', 'serverStatus', 'connectionStatus', 'getMore', 'killCursors']);
const WRITES = new Set(['insert', 'update', 'delete', 'findAndModify']);

/** read, write or ddl for a command document. */
export function classifyCommand(c: Doc): StatementClass {
  const name = Object.keys(c)[0] ?? '';
  if (name === 'aggregate') {
    const p = c.pipeline;
    return Array.isArray(p) && p.some((s) => isDoc(s) && NOT_READ_STAGES.some((k) => k in s)) ? 'write' : 'read';
  }
  if (READS.has(name)) return 'read';
  if (WRITES.has(name)) return 'write';
  return 'ddl';
}

/** Statements of the editor: split on `;` and on new lines between complete
 * top-level calls, outside strings, brackets and comments. */
export function splitMongo(text: string): { text: string; start: number; end: number }[] {
  const out: { text: string; start: number; end: number }[] = [];
  let depth = 0;
  let start = 0;
  let i = 0;
  const n = text.length;
  const push = (end: number) => {
    const raw = text.slice(start, end);
    const body = raw.trim();
    const lead = raw.length - raw.trimStart().length;
    if (body && !/^(\/\/[^\n]*\n?\s*|\/\*[\s\S]*?\*\/\s*)+$/.test(body)) out.push({ text: body, start: start + lead, end: start + lead + body.length });
  };
  while (i < n) {
    const c = text[i]!;
    const d = text[i + 1];
    if (c === '/' && d === '/') {
      const nl = text.indexOf('\n', i);
      i = nl < 0 ? n : nl;
      continue;
    }
    if (c === '/' && d === '*') {
      const e = text.indexOf('*/', i + 2);
      i = e < 0 ? n : e + 2;
      continue;
    }
    if (c === '"' || c === "'" || c === '`') {
      i++;
      while (i < n && text[i] !== c) {
        if (text[i] === '\\') i++;
        i++;
      }
      i++;
      continue;
    }
    if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') depth = Math.max(0, depth - 1);
    else if (c === ';' && depth === 0) {
      push(i);
      start = i + 1;
    } else if (c === '\n' && depth === 0) {
      // A new line ends a statement unless the next one continues the chain.
      const next = text.slice(i + 1).trimStart();
      if (!next.startsWith('.') && text.slice(start, i).trim()) {
        push(i);
        start = i + 1;
      }
    }
    i++;
  }
  push(n);
  return out;
}

/** The statement under offset `at`, or the last one before it. */
export function mongoAt(text: string, at: number): { text: string; start: number; end: number } | null {
  const all = splitMongo(text);
  let best = null;
  for (const s of all) {
    if (s.start <= at) best = s;
    if (at >= s.start && at <= s.end + 1) return s;
  }
  return best ?? all[0] ?? null;
}
