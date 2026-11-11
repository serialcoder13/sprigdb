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
