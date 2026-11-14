// The Redis console's language: one command per line,
// split as redis-cli splits it (spaces; double quotes with escapes; single
// quotes literal), a command table with arity for completion and a
// pre-check, and redis-cli's reply formatting. The gateway decides every
// right; the table only helps typing.

export class CommandError extends Error {
  constructor(
    message: string,
    readonly at: number,
  ) {
    super(message);
  }
}

/** Splits one line into arguments as redis-cli does. */
export function splitArgs(line: string): string[] {
  const out: string[] = [];
  let i = 0;
  const n = line.length;
  while (i < n) {
    while (i < n && /\s/.test(line[i]!)) i++;
    if (i >= n) break;
    let cur = '';
    const q = line[i];
    if (q === '"') {
      i++;
      let closed = false;
      while (i < n) {
        const c = line[i]!;
        if (c === '\\' && i + 1 < n) {
          const d = line[i + 1]!;
          if (d === 'x' && /^[0-9a-fA-F]{2}$/.test(line.slice(i + 2, i + 4))) {
            cur += String.fromCharCode(parseInt(line.slice(i + 2, i + 4), 16));
            i += 4;
            continue;
          }
          cur += { n: '\n', r: '\r', t: '\t', b: '\b', a: '\x07' }[d] ?? d;
          i += 2;
          continue;
        }
        if (c === '"') {
          closed = true;
          i++;
          break;
        }
        cur += c;
        i++;
      }
      if (!closed) throw new CommandError('unbalanced quotes', i);
      if (i < n && !/\s/.test(line[i]!)) throw new CommandError('a closing quote must be followed by a space', i);
    } else if (q === "'") {
      i++;
      let closed = false;
      while (i < n) {
        const c = line[i]!;
        if (c === '\\' && line[i + 1] === "'") {
          cur += "'";
          i += 2;
          continue;
        }
        if (c === "'") {
          closed = true;
          i++;
          break;
        }
        cur += c;
        i++;
      }
      if (!closed) throw new CommandError('unbalanced quotes', i);
      if (i < n && !/\s/.test(line[i]!)) throw new CommandError('a closing quote must be followed by a space', i);
    } else {
      while (i < n && !/\s/.test(line[i]!)) cur += line[i++]!;
    }
    out.push(cur);
  }
  return out;
}

/** A command's arity as Redis reports it: positive is exact, negative is
 * "at least" (both counting the command's name), and whether it writes. */
export interface CommandInfo {
  arity: number;
  write: boolean;
  args: string;
  /** Destructive enough to ask first whatever studio.confirm_writes says. */
  danger?: boolean;
}

const c = (arity: number, write: boolean, args: string, danger = false): CommandInfo => ({ arity, write, args, danger });

export const COMMANDS: Record<string, CommandInfo> = {
  GET: c(2, false, 'key'),
  SET: c(-3, true, 'key value [NX|XX] [EX s|PX ms|KEEPTTL] [GET]'),
  SETNX: c(3, true, 'key value'),
  SETEX: c(4, true, 'key seconds value'),
  PSETEX: c(4, true, 'key ms value'),
  MGET: c(-2, false, 'key [key ...]'),
  MSET: c(-3, true, 'key value [key value ...]'),
  GETSET: c(3, true, 'key value'),
  GETDEL: c(2, true, 'key'),
  GETEX: c(-2, true, 'key [EX s|PX ms|PERSIST]'),
  APPEND: c(3, true, 'key value'),
  STRLEN: c(2, false, 'key'),
  GETRANGE: c(4, false, 'key start end'),
  SETRANGE: c(4, true, 'key offset value'),
  INCR: c(2, true, 'key'),
  DECR: c(2, true, 'key'),
  INCRBY: c(3, true, 'key increment'),
  DECRBY: c(3, true, 'key decrement'),
  INCRBYFLOAT: c(3, true, 'key increment'),
  DEL: c(-2, true, 'key [key ...]'),
  UNLINK: c(-2, true, 'key [key ...]'),
  EXISTS: c(-2, false, 'key [key ...]'),
  TYPE: c(2, false, 'key'),
  TTL: c(2, false, 'key'),
  PTTL: c(2, false, 'key'),
  EXPIRE: c(-3, true, 'key seconds [NX|XX|GT|LT]'),
  PEXPIRE: c(-3, true, 'key ms [NX|XX|GT|LT]'),
  EXPIREAT: c(-3, true, 'key unix-seconds'),
  PERSIST: c(2, true, 'key'),
  RENAME: c(3, true, 'key newkey'),
  RENAMENX: c(3, true, 'key newkey'),
  KEYS: c(2, false, 'pattern'),
  SCAN: c(-2, false, 'cursor [MATCH pattern] [COUNT n] [TYPE type]'),
  DBSIZE: c(1, false, ''),
  RANDOMKEY: c(1, false, ''),
  FLUSHDB: c(-1, true, '[ASYNC|SYNC]', true),
  FLUSHALL: c(-1, true, '[ASYNC|SYNC]', true),
  SWAPDB: c(3, true, 'index1 index2', true),
  HGET: c(3, false, 'key field'),
  HSET: c(-4, true, 'key field value [field value ...]'),
  HSETNX: c(4, true, 'key field value'),
  HMGET: c(-3, false, 'key field [field ...]'),
  HMSET: c(-4, true, 'key field value [field value ...]'),
  HGETALL: c(2, false, 'key'),
  HDEL: c(-3, true, 'key field [field ...]'),
  HEXISTS: c(3, false, 'key field'),
  HKEYS: c(2, false, 'key'),
  HVALS: c(2, false, 'key'),
  HLEN: c(2, false, 'key'),
  HINCRBY: c(4, true, 'key field increment'),
  HSCAN: c(-3, false, 'key cursor [MATCH pattern] [COUNT n]'),
  LPUSH: c(-3, true, 'key element [element ...]'),
  RPUSH: c(-3, true, 'key element [element ...]'),
  LPOP: c(-2, true, 'key [count]'),
  RPOP: c(-2, true, 'key [count]'),
  LLEN: c(2, false, 'key'),
  LRANGE: c(4, false, 'key start stop'),
  LINDEX: c(3, false, 'key index'),
  LSET: c(4, true, 'key index element'),
  LREM: c(4, true, 'key count element'),
  LTRIM: c(4, true, 'key start stop'),
  SADD: c(-3, true, 'key member [member ...]'),
  SREM: c(-3, true, 'key member [member ...]'),
  SMEMBERS: c(2, false, 'key'),
  SISMEMBER: c(3, false, 'key member'),
  SCARD: c(2, false, 'key'),
  SSCAN: c(-3, false, 'key cursor [MATCH pattern] [COUNT n]'),
  SINTER: c(-2, false, 'key [key ...]'),
  SUNION: c(-2, false, 'key [key ...]'),
  ZADD: c(-4, true, 'key [NX|XX] [GT|LT] [CH] score member [score member ...]'),
  ZREM: c(-3, true, 'key member [member ...]'),
  ZRANGE: c(-4, false, 'key start stop [BYSCORE|BYLEX] [REV] [LIMIT o c] [WITHSCORES]'),
  ZREVRANGE: c(-4, false, 'key start stop [WITHSCORES]'),
  ZRANGEBYSCORE: c(-4, false, 'key min max [WITHSCORES] [LIMIT o c]'),
  ZSCORE: c(3, false, 'key member'),
  ZCARD: c(2, false, 'key'),
  ZCOUNT: c(4, false, 'key min max'),
  ZINCRBY: c(4, true, 'key increment member'),
  ZRANK: c(-3, false, 'key member [WITHSCORE]'),
  ZSCAN: c(-3, false, 'key cursor [MATCH pattern] [COUNT n]'),
  XADD: c(-5, true, 'key [NOMKSTREAM] [MAXLEN n] *|id field value [field value ...]'),
  XRANGE: c(-4, false, 'key start end [COUNT n]'),
  XLEN: c(2, false, 'key'),
  GEOADD: c(-5, true, 'key longitude latitude member [...]'),
  GEOPOS: c(-2, false, 'key member [member ...]'),
  GEODIST: c(-4, false, 'key member1 member2 [M|KM|FT|MI]'),
  GEOSEARCH: c(-7, false, 'key FROMMEMBER m|FROMLONLAT lon lat BYRADIUS r unit|BYBOX w h unit [ASC|DESC] [COUNT n]'),
  PING: c(-1, false, '[message]'),
  ECHO: c(2, false, 'message'),
  TIME: c(1, false, ''),
  INFO: c(-1, false, '[section]'),
  EVAL: c(-3, true, 'script numkeys [key ...] [arg ...]'),
  EVALSHA: c(-3, true, 'sha1 numkeys [key ...] [arg ...]'),
  FCALL: c(-3, true, 'function numkeys [key ...] [arg ...]'),
  OBJECT: c(-2, false, 'ENCODING|FREQ|IDLETIME|REFCOUNT key'),
  COMMAND: c(-1, false, '[COUNT|INFO|DOCS ...]'),
};

/** Checks a command against the table: the error to show, or null. Commands
 * the table doesn't know go to the gateway, which answers for itself. */
export function precheck(args: string[]): string | null {
  const name = args[0]?.toUpperCase();
  if (!name) return 'empty command';
  const info = COMMANDS[name];
  if (!info) return null;
  const n = args.length;
  if (info.arity > 0 ? n !== info.arity : n < -info.arity) {
    return `wrong number of arguments for '${name.toLowerCase()}': ${name} ${info.args}`;
  }
  return null;
}

/** 'read' or 'write' for studio.confirm_writes; 'danger' always asks. */
export function commandClass(args: string[]): 'read' | 'write' | 'danger' {
  const name = args[0]?.toUpperCase() ?? '';
  const info = COMMANDS[name];
  if (info?.danger) return 'danger';
  if (!info) return 'write';
  return info.write ? 'write' : 'read';
}

export type Reply = null | boolean | number | string | Reply[] | { error: string } | { base64: string } | { [k: string]: Reply };

/** A reply as redis-cli prints it. */
export function formatReply(r: Reply, indent = ''): string {
  if (r === null) return '(nil)';
  if (typeof r === 'boolean') return r ? '(true)' : '(false)';
  if (typeof r === 'number') return Number.isInteger(r) ? `(integer) ${r}` : `(double) ${r}`;
  if (typeof r === 'string') return JSON.stringify(r);
  if (Array.isArray(r)) {
    if (!r.length) return '(empty array)';
    const w = String(r.length).length;
    return r
      .map((x, i) => {
        const head = `${String(i + 1).padStart(w)}) `;
        const body = formatReply(x, indent + ' '.repeat(head.length));
        return (i ? indent : '') + head + body;
      })
      .join('\n');
  }
  if ('error' in r && typeof r.error === 'string' && Object.keys(r).length === 1) return `(error) ${r.error}`;
  if ('base64' in r && typeof r.base64 === 'string' && Object.keys(r).length === 1) return `(binary) ${r.base64}`;
  const entries = Object.entries(r);
  if (!entries.length) return '(empty map)';
  const w = String(entries.length).length;
  return entries
    .map(([k, v], i) => {
      const head = `${String(i + 1).padStart(w)}# ${JSON.stringify(k)} => `;
      return (i ? indent : '') + head + formatReply(v, indent + ' '.repeat(w + 2));
    })
    .join('\n');
}

/** Whether a reply is an error. */
export function isErrorReply(r: Reply): boolean {
  return typeof r === 'object' && r !== null && !Array.isArray(r) && 'error' in r && Object.keys(r).length === 1;
}

/** Namespaces of a database: the key-value store makes `<ns>__hash` views for each. */
export function namespacesOf(objects: { name: string; kind: string }[]): string[] {
  const out = new Set<string>();
  for (const o of objects) {
    const m = /^(.+)__hash$/.exec(o.name);
    if (m && o.kind === 'view') out.add(m[1]!);
  }
  return [...out].sort();
}
