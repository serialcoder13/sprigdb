// The statements the stream, queue, sink and inherited views run (chapter
// 44 steps 28–31), built from forms and shown before they run, as a SQL
// client would write them (chapters 45, 50, 51, 42).

import { ident, sqlString } from '../logic/declaration';
import type { GraphData } from '../graph/elements';

export { ident, sqlString };

/** Records of a partition from an offset (a read never commits an offset). */
export function readRecords(stream: string, partition: number, from: number, limit: number): string {
  const n = (x: number, what: string) => {
    if (!Number.isInteger(x) || x < 0) throw new Error(`${what} is a whole number`);
    return x;
  };
  return `SELECT partition_no, offset_no, ts, \`key\`, \`value\`, headers FROM ${ident(stream)} WHERE partition_no = ${n(partition, 'the partition')} AND offset_no >= ${n(from, 'the offset')} ORDER BY offset_no LIMIT ${Math.min(1000, Math.max(1, n(limit, 'the limit')))}`;
}

export function createStream(name: string, partitions: number, retention: string, cleanup: 'delete' | 'compact'): string {
  if (!Number.isInteger(partitions) || partitions < 1) throw new Error('partitions is a whole number above 0');
  const ret = retention.trim() ? ` RETENTION ${sqlString(retention.trim())}` : '';
  return `CREATE STREAM ${ident(name)} PARTITIONS ${partitions}${ret} CLEANUP ${cleanup}`;
}

export function dropStream(name: string): string {
  return `DROP STREAM ${ident(name)}`;
}

export function publishTest(exchange: string, routingKey: string, body: string, vhost?: string | null): string {
  return `CALL sprig.mq_publish(${sqlString(exchange)}, ${sqlString(routingKey)}, ${sqlString(body)}, '{}'${vhost && vhost !== '/' ? `, FALSE, ${sqlString(vhost)}` : ''})`;
}

export function getMessages(queue: string, max: number, vhost?: string | null): string {
  const m = Math.min(100, Math.max(1, Math.floor(max)));
  return `CALL sprig.mq_get(${sqlString(queue)}, ${m}, 30000${vhost && vhost !== '/' ? `, ${sqlString(vhost)}` : ''})`;
}

export function settle(token: string, requeue: boolean): string {
  return requeue ? `CALL sprig.mq_nack(${sqlString(token)}, TRUE)` : `CALL sprig.mq_ack(${sqlString(token)})`;
}

export type SinkAction = 'pause' | 'resume' | 'earliest' | 'latest';

export function alterSink(name: string, a: SinkAction): string {
  switch (a) {
    case 'pause':
      return `ALTER SINK ${ident(name)} PAUSE`;
    case 'resume':
      return `ALTER SINK ${ident(name)} RESUME`;
    case 'earliest':
      return `ALTER SINK ${ident(name)} RESET TO EARLIEST`;
    case 'latest':
      return `ALTER SINK ${ident(name)} RESET TO LATEST`;
  }
}

export type InheritedAction = 'disable' | 'enable' | 'reset' | 'detach';
const KINDS = ['STREAM', 'QUEUE', 'EXCHANGE', 'TRIGGER', 'SINK', 'CONNECTION', 'BINDING'];

/** `ALTER INHERITED kind name DISABLE | ENABLE | RESET | DETACH`. */
export function alterInherited(kind: string, name: string, a: InheritedAction): string {
  const k = kind.toUpperCase();
  if (!KINDS.includes(k)) throw new Error(`'${kind}' isn't an inherited kind`);
  return `ALTER INHERITED ${k} ${name.split('.').map(ident).join('.')} ${a.toUpperCase()}`;
}

export interface Topology {
  vhost: string | null;
  exchanges: { id: number; name: string; type: string; durable: boolean; internal: boolean; arguments: unknown }[];
  queues: { id: number; name: string; type: string; durable: boolean; arguments: unknown; lanes: number; state: string }[];
  bindings: { source: string | null; destination: string | null; destination_kind: string; binding_key: string; arguments: unknown }[];
}

/** The bindings graph: exchanges and queues as nodes, bindings as edges. */
export function bindingsGraph(t: Topology): GraphData {
  const g: GraphData = { nodes: new Map(), edges: new Map(), dropped: 0, dangling: 0 };
  for (const e of t.exchanges) g.nodes.set(`x:${e.name}`, { id: `x:${e.name}`, labels: ['Exchange'], props: { name: e.name || '(default)', type: e.type } });
  for (const q of t.queues) g.nodes.set(`q:${q.name}`, { id: `q:${q.name}`, labels: ['Queue'], props: { name: q.name, type: q.type } });
  t.bindings.forEach((b, i) => {
    const src = `x:${b.source ?? ''}`;
    const dst = `${b.destination_kind === 'queue' ? 'q' : 'x'}:${b.destination ?? ''}`;
    const key = b.binding_key || (b.arguments ? 'headers' : '#');
    g.edges.set(`b${i}`, { id: `b${i}`, type: key, start: src, end: dst, props: { binding_key: b.binding_key, arguments: b.arguments } });
  });
  return g;
}

// ---- Connections and script sinks (Studio round 5, chapter 51) ----

export type ConnKind = 'webhook' | 'kafka' | 's3' | 'mysql' | 'postgresql';
export const CONN_KINDS: readonly ConnKind[] = ['webhook', 'kafka', 's3', 'mysql', 'postgresql'];

/** The options each connection type needs (chapter 51; 39218 otherwise). */
export const CONN_REQUIRED: Record<ConnKind, readonly string[]> = {
  webhook: ['url'],
  kafka: ['bootstrap'],
  s3: ['endpoint', 'bucket'],
  mysql: ['host', 'port', 'database'],
  postgresql: ['host', 'port', 'database'],
};

/** The credentials each type takes (`sprig_sinks::secrets::allowed_credentials`). */
export const CONN_CREDENTIALS: Record<ConnKind, readonly string[]> = {
  webhook: ['token', 'username', 'password', 'header_value', 'client_id', 'client_secret', 'signing_secret', 'tls_cert', 'tls_key'],
  kafka: ['username', 'password', 'tls_cert', 'tls_key', 'client_secret'],
  s3: ['access_key_id', 'secret_access_key', 'session_token'],
  mysql: ['username', 'password', 'tls_cert', 'tls_key'],
  postgresql: ['username', 'password', 'tls_cert', 'tls_key'],
};

export interface Pair {
  key: string;
  value: string;
}

/** What a credential's value shows as in a statement on screen: values are
 * write-only, so the text shown before running never holds them. */
export const MASK = "'********'";

function key(k: string): string {
  const t = k.trim();
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(t)) throw new Error(`"${k}" isn't an option name (letters, digits and _)`);
  return t.toLowerCase();
}

function pairs(ps: readonly Pair[], mask: boolean): string {
  return ps.map((p) => `${key(p.key)} = ${mask ? MASK : sqlString(p.value)}`).join(', ');
}

/** Pairs with a key; credentials also need a value. */
export function filled(ps: readonly Pair[], needValue = false): Pair[] {
  return ps.filter((p) => p.key.trim() && (!needValue || p.value !== ''));
}

/**
 * `CREATE CONNECTION name TYPE kind WITH (...) CREDENTIALS (...)`. With
 * `mask`, credential values are replaced by {@link MASK} (the text shown);
 * the text sent has them.
 */
export function createConnection(name: string, kind: ConnKind, options: readonly Pair[], credentials: readonly Pair[], mask = false): string {
  const opts = filled(options);
  for (const r of CONN_REQUIRED[kind].filter((k) => k !== 'port')) {
    if (!opts.some((o) => o.key.trim().toLowerCase() === r && o.value.trim())) throw new Error(`a ${kind} connection needs ${r}`);
  }
  const creds = filled(credentials, true);
  const allowed = CONN_CREDENTIALS[kind];
  for (const c of creds) {
    if (!allowed.includes(key(c.key))) throw new Error(`a ${kind} connection takes no credential ${c.key} (${allowed.join(', ')})`);
  }
  const w = opts.length ? ` WITH (${pairs(opts, false)})` : '';
  const c = creds.length ? ` CREDENTIALS (${pairs(creds, mask)})` : '';
  return `CREATE CONNECTION ${ident(name)} TYPE ${kind}${w}${c}`;
}

/** `ALTER CONNECTION name SET CREDENTIALS (...)`, masked as {@link createConnection}. */
export function setCredentials(name: string, credentials: readonly Pair[], mask = false): string {
  const creds = filled(credentials, true);
  if (!creds.length) throw new Error('give at least one credential');
  return `ALTER CONNECTION ${ident(name)} SET CREDENTIALS (${pairs(creds, mask)})`;
}

export function dropConnection(name: string): string {
  return `DROP CONNECTION ${ident(name)}`;
}

export type ScriptLanguage = 'javascript' | 'lua' | 'kirun';
export type SinkSource = { kind: 'stream' | 'queue' | 'changes'; name: string };

/** `$$body$$`, or `$sprig$...$sprig$` (and so on) when the body holds `$$`. */
export function dollarQuote(body: string): string {
  for (let i = 0; ; i++) {
    const d = i === 0 ? '$$' : `$sprig${i === 1 ? '' : i}$`;
    if (!body.includes(d)) return `${d}${body}${d}`;
  }
}

function source(s: SinkSource): string {
  if (!s.name.trim()) throw new Error('name the source');
  switch (s.kind) {
    case 'stream':
      return `STREAM ${ident(s.name.trim())}`;
    case 'queue':
      return `QUEUE ${ident(s.name.trim())}`;
    case 'changes':
      return `CHANGES OF ${s.name
        .split(',')
        .map((t) => t.trim())
        .filter(Boolean)
        .map((t) => ident(t))
        .join(', ')}`;
  }
}

/** `CREATE SINK name FROM ... TO SCRIPT LANGUAGE l [USING CONNECTIONS (...)] AS $$...$$`. */
export function createScriptSink(name: string, from: SinkSource, language: ScriptLanguage, using: readonly string[], body: string): string {
  if (!body.trim()) throw new Error('write the deliver function');
  const u = using.map((c) => c.trim()).filter(Boolean);
  const conns = u.length ? ` USING CONNECTIONS (${u.map((c) => ident(c)).join(', ')})` : '';
  return `CREATE SINK ${ident(name)} FROM ${source(from)} TO SCRIPT LANGUAGE ${language}${conns} AS ${dollarQuote(body)}`;
}

/** `ALTER SINK name SET SCRIPT AS $$...$$`. */
export function setScript(name: string, body: string): string {
  if (!body.trim()) throw new Error('write the deliver function');
  return `ALTER SINK ${ident(name)} SET SCRIPT AS ${dollarQuote(body)}`;
}

/** A script sink's language and code from its `SHOW CREATE SINK` text; null
 * for other sinks and `TO SCRIPT FUNCTION`. */
export function scriptOf(create: string): { language: ScriptLanguage; body: string } | null {
  const m = /\bTO\s+SCRIPT\s+LANGUAGE\s+(\w+)/i.exec(create);
  if (!m) return null;
  const l = m[1]!.toLowerCase();
  const language: ScriptLanguage | null = l === 'js' || l === 'javascript' ? 'javascript' : l === 'lua' ? 'lua' : l === 'kirun' ? 'kirun' : null;
  if (!language) return null;
  const rest = create.slice(m.index + m[0].length);
  const d = /\bAS\s+(\$[A-Za-z0-9_]*\$)/i.exec(rest);
  if (!d) return null;
  const start = d.index + d[0].length;
  const end = rest.indexOf(d[1]!, start);
  if (end < 0) return null;
  // SHOW CREATE SINK writes `$$ body $$`: one space each side is its own.
  const raw = rest.slice(start, end);
  const body = raw.startsWith(' ') && raw.endsWith(' ') && raw.length >= 2 ? raw.slice(1, -1) : raw;
  return { language, body };
}

/** A starting point for each script language (chapter 51, "Script sinks"). */
export const SCRIPT_TEMPLATES: Record<ScriptLanguage, string> = {
  javascript: `// deliver(batch, ctx): batch = {id, attempt, records}.
// Return nothing or {ok: true} (all delivered), {retry: true}, {fail: reason}
// or {results: [...]} per record. ctx.fetch, ctx.sign, ctx.db (read only).
export function deliver(batch, ctx) {
  for (const r of batch.records) {
    // send r somewhere with ctx.fetch(url, {connection: 'name', method: 'POST', body: JSON.stringify(r)})
  }
  return { ok: true };
}
`,
  lua: `-- deliver(batch, ctx): batch = {id, attempt, records}.
-- Return true (all delivered), {retry = true}, {fail = reason} or {results = {...}}.
function deliver(batch, ctx)
  for _, r in ipairs(batch.records) do
    -- sprig.fetch(url, {connection = 'name', method = 'POST', body = cjson.encode(r)})
  end
  return true
end
`,
  kirun: `{
  "namespace": "Sink",
  "name": "Deliver",
  "parameters": {
    "batch": {"parameterName": "batch", "schema": {"type": ["OBJECT"]}},
    "ctx": {"parameterName": "ctx", "schema": {"type": ["OBJECT"]}}
  },
  "steps": {}
}
`,
};
