// Streams, queues, sinks and inherited messaging objects (chapter 44 steps
// 28–31), per logical database (for queues, its virtual host). Lists come
// from the statements a SQL client runs (SHOW STREAMS, SHOW SINKS, SHOW
// SINK STATUS, SHOW INHERITED ...), so the gateway shows only what the
// user may read; consumer groups with lag, the queue topology and sink
// dead letters come from the round's small REST routes. Every change is a
// statement shown before it runs.

import { lazy, Suspense, useState } from 'react';
import { useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { execute, rows as rowsOf } from '../api/sql';
import { useDatabases } from '../catalog/snapshot';
import { binaryText } from './body';
import { QueueCounts, QueueDetail } from './QueuePanel';
import { SinkDryRun } from './DryRuns';
import {
  alterInherited,
  alterSink,
  bindingsGraph,
  createStream,
  dropStream,
  publishTest,
  readRecords,
  type InheritedAction,
  type SinkAction,
  type Topology,
} from './statements';

const GraphView = lazy(() => import('../graph/GraphView'));
const SinkForms = lazy(() => import('./SinkForms'));
const ScriptEditor = lazy(() => import('./SinkForms').then((m) => ({ default: m.ScriptEditor })));

type Tab = 'streams' | 'queues' | 'sinks' | 'inherited';

export interface Table {
  columns: string[];
  rows: (string | null)[][];
}

export async function table(db: string | null, sql: string): Promise<Table> {
  const r = await rowsOf(db, sql);
  // Binary columns (a stream's key and value) arrive as base64: shown as
  // text when they are UTF-8, else as hex (chapter 44, "Bodies").
  const bin = r.columns.map((c) => !!c.binary);
  return { columns: r.columns.map((c) => c.name), rows: r.rows.map((row) => row.map((v, i) => (bin[i] ? binaryText(v) : v))) };
}

export function col(t: Table, row: (string | null)[], ...names: string[]): string | null {
  for (const n of names) {
    const i = t.columns.findIndex((c) => c.toLowerCase() === n.toLowerCase());
    if (i >= 0) return row[i] ?? null;
  }
  return null;
}

export function Generic({ t, testid, onPick, pickLabel }: Readonly<{ t: Table; testid: string; onPick?: (row: (string | null)[]) => void; pickLabel?: string }>) {
  return (
    <div className="table-scroll" tabIndex={0} role="region" aria-label={testid}>
      <table className="table" data-testid={testid}>
        <thead>
          <tr>
            {onPick && <th />}
            {t.columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {t.rows.map((r, i) => (
            <tr key={i}>
              {onPick && (
                <td>
                  <button onClick={() => onPick(r)}>{pickLabel ?? 'Open'}</button>
                </td>
              )}
              {r.map((c, j) => (
                <td key={j} className="mono">
                  {c ?? 'NULL'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {!t.rows.length && <p className="muted">None.</p>}
    </div>
  );
}

/** Runs a statement after showing it; `shown` is the text on screen when it
 * differs (credential values masked, round 5). `runTable` (round 6) also
 * shows a warning in the dialog and returns the statement's first result,
 * whose first row is the outcome line (`purged`, `moved`, ...). */
export function useStatement(db: string) {
  const [c, setC] = useState<{ sql: string; note?: string; resolve: (ok: boolean) => void } | null>(null);
  const [msg, setMsg] = useState<{ text: string; error: boolean } | null>(null);
  const ask = async (sql: string, note?: string): Promise<boolean> => {
    const ok = await new Promise<boolean>((resolve) => setC({ sql, note, resolve }));
    setC(null);
    return ok;
  };
  const run = async (sql: string, shown?: string): Promise<boolean> => {
    if (!(await ask(shown ?? sql))) return false;
    try {
      const r = await execute(db, sql);
      setMsg({ text: `Done${r.results[0]?.affected_rows ? `: ${r.results[0].affected_rows} affected` : ''}.`, error: false });
      return true;
    } catch (e) {
      setMsg({ text: errorText(e), error: true });
      return false;
    }
  };
  const runTable = async (sql: string, opts: { note?: string; done?: (t: Table) => string } = {}): Promise<Table | null> => {
    if (!(await ask(sql, opts.note))) return null;
    try {
      const r = await execute(db, sql);
      const first = r.results.find((x) => x.columns?.length);
      const t: Table = { columns: (first?.columns ?? []).map((x) => x.name), rows: first?.rows ?? [] };
      const line = t.rows[0] ? t.columns.map((name, i) => `${name} ${t.rows[0]![i] ?? 'NULL'}`).join(', ') : '';
      setMsg({ text: opts.done ? opts.done(t) : `Done${line ? `: ${line}` : ''}.`, error: false });
      return t;
    } catch (e) {
      setMsg({ text: errorText(e), error: true });
      return null;
    }
  };
  const node = (
    <>
      {msg && (
        <p role={msg.error ? 'alert' : 'status'} className={msg.error ? 'error' : 'muted'} data-testid="msg-outcome">
          {msg.text}
        </p>
      )}
      <dialog open={!!c} className="modal" aria-labelledby="msg-confirm-h">
        {c && (
          <div className="stack">
            <h2 id="msg-confirm-h">Run this statement?</h2>
            {c.note && (
              <p className="banner" data-testid="msg-confirm-note">
                {c.note}
              </p>
            )}
            <pre className="logic-stmt" data-testid="msg-confirm-sql">
              {c.sql}
            </pre>
            <div className="row">
              <button className="primary" onClick={() => c.resolve(true)} data-testid="msg-confirm">
                Run
              </button>
              <button autoFocus onClick={() => c.resolve(false)}>
                Cancel
              </button>
            </div>
          </div>
        )}
      </dialog>
    </>
  );
  return { run, runTable, ask, node };
}

export function MessagingPage() {
  const [params, setParams] = useSearchParams();
  const dbs = useDatabases();
  const db = params.get('db') ?? dbs.data?.[0] ?? '';
  const tab = (params.get('tab') as Tab | null) ?? 'streams';
  const set = (k: string, v: string) => {
    const p = new URLSearchParams(params);
    p.set(k, v);
    setParams(p, { replace: true });
  };
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Messaging</h1>
      <div className="row">
        <label className="row">
          Database
          <select value={db} onChange={(e) => set('db', e.target.value)} data-testid="msg-db">
            {(dbs.data ?? []).map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <div className="row" role="tablist" aria-label="Messaging views">
          {(['streams', 'queues', 'sinks', 'inherited'] as const).map((t) => (
            <button key={t} role="tab" aria-selected={tab === t} onClick={() => set('tab', t)} data-testid={`msg-tab-${t}`}>
              {t[0]!.toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>
      </div>
      {db && tab === 'streams' && <Streams key={db} db={db} />}
      {db && tab === 'queues' && <Queues key={db} db={db} />}
      {db && tab === 'sinks' && <Sinks key={db} db={db} />}
      {db && tab === 'inherited' && <Inherited key={db} db={db} />}
    </div>
  );
}

interface Groups {
  stream: string;
  kind: string;
  cleanup: string;
  partitions: { partition: number; log_start: number | null; high_watermark: number | null; records: number | null }[];
  groups: { group: string; generation: number | null; state: string | null; lag: number; partitions: { partition: number; committed: number; lag: number; committed_at: string }[] }[];
}

function Streams({ db }: Readonly<{ db: string }>) {
  const qc = useQueryClient();
  const list = useQuery({ queryKey: ['streams', db], queryFn: () => table(db, `SHOW STREAMS FROM \`${db.replace(/`/g, '``')}\``) });
  const [sel, setSel] = useState<string | null>(null);
  const st = useStatement(db);
  const [form, setForm] = useState({ name: '', partitions: '3', retention: '7 days', cleanup: 'delete' as 'delete' | 'compact' });
  const create = async () => {
    try {
      if (await st.run(createStream(form.name, Number(form.partitions), form.retention, form.cleanup))) await qc.invalidateQueries({ queryKey: ['streams', db] });
    } catch (e) {
      window.alert(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <div className="stack">
      {list.error && (
        <p className="error" role="alert">
          {errorText(list.error)}
        </p>
      )}
      {list.data && <Generic t={list.data} testid="msg-streams" onPick={(r) => setSel(col(list.data, r, 'Name', 'Stream', 'name') ?? r[0] ?? null)} />}
      <details>
        <summary>Create a stream</summary>
        <div className="row">
          <label className="row">
            Name <input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} data-testid="msg-stream-name" />
          </label>
          <label className="row">
            Partitions <input value={form.partitions} onChange={(e) => setForm({ ...form, partitions: e.target.value })} size={4} />
          </label>
          <label className="row">
            Retention <input value={form.retention} onChange={(e) => setForm({ ...form, retention: e.target.value })} size={10} />
          </label>
          <label className="row">
            Cleanup
            <select value={form.cleanup} onChange={(e) => setForm({ ...form, cleanup: e.target.value as 'delete' | 'compact' })}>
              <option>delete</option>
              <option>compact</option>
            </select>
          </label>
          <button onClick={() => void create()} disabled={!form.name} data-testid="msg-stream-create">
            Create…
          </button>
        </div>
      </details>
      {sel && <StreamDetail key={sel} db={db} name={sel} onDropped={() => setSel(null)} />}
      {st.node}
    </div>
  );
}

function StreamDetail({ db, name, onDropped }: Readonly<{ db: string; name: string; onDropped: () => void }>) {
  const qc = useQueryClient();
  const g = useQuery({
    queryKey: ['stream-groups', db, name],
    queryFn: () => json<Groups>(`/v1/databases/${encodeURIComponent(db)}/streams/${encodeURIComponent(name)}/groups`),
    refetchInterval: () => (document.visibilityState === 'visible' ? 5000 : false),
  });
  const [read, setRead] = useState({ partition: '0', from: '0', limit: '50' });
  const [records, setRecords] = useState<Table | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const st = useStatement(db);
  const doRead = async () => {
    setErr(null);
    try {
      setRecords(await table(db, readRecords(name, Number(read.partition), Number(read.from), Number(read.limit))));
    } catch (e) {
      setErr(e instanceof Error ? (errorText(e) ?? e.message) : String(e));
    }
  };
  return (
    <section className="card stack" aria-labelledby="stream-h" data-testid="msg-stream-detail">
      <h2 id="stream-h" style={{ margin: 0 }}>
        {name} <span className="muted">{g.data ? `${g.data.kind}, cleanup ${g.data.cleanup}` : ''}</span>
      </h2>
      {g.error && (
        <p className="error" role="alert">
          {errorText(g.error)}
        </p>
      )}
      {g.data && (
        <>
          <h3>Partitions</h3>
          <table className="table" data-testid="msg-partitions">
            <thead>
              <tr>
                <th>Partition</th>
                <th>Log start</th>
                <th>High watermark</th>
                <th>Records</th>
              </tr>
            </thead>
            <tbody>
              {g.data.partitions.map((p) => (
                <tr key={p.partition}>
                  <td>{p.partition}</td>
                  <td>{p.log_start ?? '–'}</td>
                  <td>{p.high_watermark ?? 0}</td>
                  <td>{p.records ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h3>Consumer groups</h3>
          <table className="table" data-testid="msg-groups">
            <thead>
              <tr>
                <th>Group</th>
                <th>State</th>
                <th>Generation</th>
                <th>Lag</th>
                <th>Committed (partition: offset, lag)</th>
              </tr>
            </thead>
            <tbody>
              {g.data.groups.map((x) => (
                <tr key={x.group}>
                  <td>{x.group}</td>
                  <td>{x.state ?? '–'}</td>
                  <td>{x.generation ?? '–'}</td>
                  <td data-testid={`msg-lag-${x.group}`}>{x.lag}</td>
                  <td className="mono">{x.partitions.map((p) => `${p.partition}: ${p.committed} (${p.lag})`).join(', ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!g.data.groups.length && <p className="muted">No consumer group has committed an offset.</p>}
        </>
      )}
      <h3>Read records</h3>
      <p className="muted">A SELECT on the stream: it never commits an offset.</p>
      <div className="row">
        <label className="row">
          Partition <input value={read.partition} onChange={(e) => setRead({ ...read, partition: e.target.value })} size={4} data-testid="msg-read-partition" />
        </label>
        <label className="row">
          From offset <input value={read.from} onChange={(e) => setRead({ ...read, from: e.target.value })} size={8} />
        </label>
        <label className="row">
          Limit <input value={read.limit} onChange={(e) => setRead({ ...read, limit: e.target.value })} size={5} />
        </label>
        <button onClick={() => void doRead()} data-testid="msg-read">
          Read
        </button>
        <button
          onClick={() =>
            void st.run(dropStream(name)).then(async (ok) => {
              if (ok) {
                await qc.invalidateQueries({ queryKey: ['streams', db] });
                onDropped();
              }
            })
          }
        >
          Drop stream…
        </button>
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {records && <Generic t={records} testid="msg-records" />}
      {st.node}
    </section>
  );
}

function dlx(args: unknown): string {
  const v = args && typeof args === 'object' ? (args as Record<string, unknown>)['x-dead-letter-exchange'] : undefined;
  return typeof v === 'string' ? v : '–';
}

function Queues({ db }: Readonly<{ db: string }>) {
  const topo = useQuery({
    queryKey: ['mq', db],
    queryFn: () => json<Topology>(`/v1/databases/${encodeURIComponent(db)}/mq`),
    refetchInterval: () => (document.visibilityState === 'visible' ? 5000 : false),
  });
  const st = useStatement(db);
  const [pub, setPub] = useState({ exchange: '', key: '', body: '{"test": true}' });
  const [open, setOpen] = useState<string | null>(null);
  const t = topo.data;
  // The database is its own vhost; gateways before round 6 sent null
  // when the vhost had no mq_vhosts row, even with queues.
  const vhost = t ? (t.vhost ?? (t.exchanges.length || t.queues.length ? db : null)) : null;
  return (
    <div className="stack">
      {topo.error && (
        <p className="error" role="alert">
          {errorText(topo.error)}
        </p>
      )}
      {t && !vhost && <p className="muted">This database has no virtual host yet: it gets one when AMQP or sprig.mq_publish first uses it.</p>}
      {t && vhost && (
        <>
          <p>
            Virtual host <strong className="mono">{vhost}</strong>
          </p>
          <h2>Bindings</h2>
          <Suspense fallback={<p className="muted">Loading graph…</p>}>
            <GraphView graph={bindingsGraph(t)} label="Bindings of exchanges and queues" />
          </Suspense>
          <h2>Exchanges</h2>
          <table className="table" data-testid="msg-exchanges">
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Durable</th>
                <th>Internal</th>
              </tr>
            </thead>
            <tbody>
              {t.exchanges.map((e) => (
                <tr key={e.id}>
                  <td className="mono">{e.name || '(default)'}</td>
                  <td>{e.type}</td>
                  <td>{e.durable ? 'yes' : 'no'}</td>
                  <td>{e.internal ? 'yes' : 'no'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h2>Queues</h2>
          <div className="table-scroll" tabIndex={0} role="region" aria-label="Queues">
            <table className="table" data-testid="msg-queues">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Lanes</th>
                  <th>Dead-letter exchange</th>
                  <th>State</th>
                  <th>Ready</th>
                  <th>Unacked</th>
                  <th>Consumers</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {t.queues.map((q) => (
                  <tr key={q.id} aria-selected={open === q.name || undefined}>
                    <td className="mono">{q.name}</td>
                    <td>{q.type}</td>
                    <td>{q.lanes}</td>
                    <td className="mono">{dlx(q.arguments)}</td>
                    <td>{q.state}</td>
                    <QueueCounts db={db} queue={q.name} vhost={vhost} />
                    <td>
                      <button onClick={() => setOpen(q.name)} data-testid={`mq-open-${q.name}`} aria-label={`Open queue ${q.name}`}>
                        Open
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {open && t.queues.some((q) => q.name === open) && (
            <QueueDetail key={open} db={db} queue={open} vhost={vhost} exchanges={['', ...t.exchanges.filter((e) => !e.internal && e.name).map((e) => e.name)]} />
          )}
          <details>
            <summary>Publish a test message</summary>
            <div className="row">
              <label className="row">
                Exchange
                <select value={pub.exchange} onChange={(e) => setPub({ ...pub, exchange: e.target.value })} data-testid="msg-pub-exchange">
                  {t.exchanges.map((e) => (
                    <option key={e.id} value={e.name}>
                      {e.name || '(default)'}
                    </option>
                  ))}
                </select>
              </label>
              <label className="row">
                Routing key <input value={pub.key} onChange={(e) => setPub({ ...pub, key: e.target.value })} data-testid="msg-pub-key" />
              </label>
              <label className="row">
                Body <input value={pub.body} onChange={(e) => setPub({ ...pub, body: e.target.value })} size={30} />
              </label>
              <button onClick={() => void st.run(publishTest(pub.exchange, pub.key, pub.body, vhost)).then(() => topo.refetch())} data-testid="msg-publish">
                Publish…
              </button>
            </div>
          </details>
        </>
      )}
      {st.node}
    </div>
  );
}

interface DeadRow {
  id: number;
  key: string;
  reason: string;
  class: string;
  attempts: number;
  state: string;
  at: string;
}

function Sinks({ db }: Readonly<{ db: string }>) {
  const list = useQuery({ queryKey: ['sinks', db], queryFn: () => table(db, `SHOW SINKS FROM \`${db.replace(/`/g, '``')}\``) });
  const [sel, setSel] = useState<string | null>(null);
  return (
    <div className="stack">
      <Suspense fallback={<p className="muted">Loading…</p>}>
        <SinkForms db={db} onCreated={() => void list.refetch()} />
      </Suspense>
      {list.error && (
        <p className="error" role="alert">
          {errorText(list.error)}
        </p>
      )}
      {list.data && <Generic t={list.data} testid="msg-sinks" onPick={(r) => setSel(col(list.data, r, 'Name', 'Sink', 'name') ?? r[0] ?? null)} />}
      {sel && <SinkDetail key={sel} db={db} name={sel} />}
    </div>
  );
}

function SinkDetail({ db, name }: Readonly<{ db: string; name: string }>) {
  const qn = `\`${db.replace(/`/g, '``')}\`.\`${name.replace(/`/g, '``')}\``;
  const status = useQuery({
    queryKey: ['sink-status', db, name],
    queryFn: () => table(db, `SHOW SINK STATUS ${qn}`),
    refetchInterval: () => (document.visibilityState === 'visible' ? 5000 : false),
  });
  const def = useQuery({ queryKey: ['sink-def', db, name], queryFn: () => table(db, `SHOW CREATE SINK ${qn}`) });
  const dead = useQuery({
    queryKey: ['sink-dead', db, name],
    queryFn: () => json<{ dead_letters: DeadRow[] }>(`/v1/databases/${encodeURIComponent(db)}/sinks/${encodeURIComponent(name)}/deadLetters`),
    retry: false,
  });
  const st = useStatement(db);
  const [msg, setMsg] = useState<string | null>(null);
  const act = async (a: SinkAction) => {
    if (await st.run(alterSink(name, a))) await status.refetch();
  };
  const settleDead = async (verb: 'replay' | 'discard') => {
    try {
      const r = await json<{ marked: number }>(`/v1/databases/${encodeURIComponent(db)}/sinks/${encodeURIComponent(name)}/deadLetters:${verb}`, { body: { all: true } });
      setMsg(`${r.marked} dead letters ${verb === 'replay' ? 'marked for replay' : 'discarded'}`);
      await dead.refetch();
    } catch (e) {
      setMsg(errorText(e));
    }
  };
  return (
    <section className="card stack" aria-labelledby="sink-h" data-testid="msg-sink-detail">
      <h2 id="sink-h" style={{ margin: 0 }}>
        {name}
      </h2>
      <div className="row">
        <button onClick={() => void act('pause')}>Pause…</button>
        <button onClick={() => void act('resume')}>Resume…</button>
        <button onClick={() => void act('earliest')}>Reset to earliest…</button>
        <button onClick={() => void act('latest')}>Reset to latest…</button>
      </div>
      {status.error && (
        <p className="error" role="alert">
          {errorText(status.error)}
        </p>
      )}
      {status.data && <Generic t={status.data} testid="msg-sink-status" />}
      <SinkDryRun db={db} name={name} />
      {def.data && (
        <details>
          <summary>Definition</summary>
          <pre className="logic-stmt">{def.data.rows.map((r) => r[r.length - 1]).join('\n')}</pre>
        </details>
      )}
      <h3>Dead letters</h3>
      {dead.error && <p className="muted">{errorText(dead.error)}</p>}
      {dead.data && (
        <>
          <div className="row">
            <button onClick={() => void settleDead('replay')} disabled={!dead.data.dead_letters.some((d) => d.state === 'dead')}>
              Replay all
            </button>
            <button onClick={() => void settleDead('discard')} disabled={!dead.data.dead_letters.some((d) => d.state === 'dead')}>
              Discard all
            </button>
          </div>
          <table className="table" data-testid="msg-sink-dead">
            <thead>
              <tr>
                <th>Id</th>
                <th>Key</th>
                <th>Class</th>
                <th>Attempts</th>
                <th>State</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {dead.data.dead_letters.map((d) => (
                <tr key={d.id}>
                  <td>{d.id}</td>
                  <td className="mono">{d.key}</td>
                  <td>{d.class}</td>
                  <td>{d.attempts}</td>
                  <td>{d.state}</td>
                  <td>{d.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      {msg && <p role="status">{msg}</p>}
      {def.data && (
        <Suspense fallback={null}>
          <ScriptEditor db={db} name={name} create={def.data.rows.map((r) => r[r.length - 1] ?? '').join('\n')} onSaved={() => void def.refetch()} />
        </Suspense>
      )}
      {st.node}
    </section>
  );
}

function Inherited({ db }: Readonly<{ db: string }>) {
  const defs = useQuery({ queryKey: ['inh-defs', db], queryFn: () => table(db, 'SHOW INHERITED DEFINITIONS') });
  const inst = useQuery({ queryKey: ['inh-inst', db], queryFn: () => table(db, 'SHOW INHERITED INSTANCES') });
  const over = useQuery({ queryKey: ['inh-over', db], queryFn: () => table(db, 'SHOW INHERITED OVERRIDES') });
  const st = useStatement(db);
  const act = async (row: (string | null)[], t: Table, a: InheritedAction) => {
    const kind = col(t, row, 'Kind', 'kind') ?? '';
    const name = col(t, row, 'Name', 'Object', 'name') ?? '';
    try {
      if (await st.run(alterInherited(kind, name, a))) await inst.refetch();
    } catch (e) {
      window.alert(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <div className="stack">
      <h2>Definitions declared here</h2>
      {defs.error ? <p className="muted">{errorText(defs.error)}</p> : defs.data && <Generic t={defs.data} testid="msg-inh-defs" />}
      <h2>Instances</h2>
      {inst.error ? (
        <p className="muted">{errorText(inst.error)}</p>
      ) : (
        inst.data && (
          <>
            <Generic t={inst.data} testid="msg-inh-instances" />
            {inst.data.rows.length > 0 && (
              <div className="stack">
                {inst.data.rows.map((r, i) => (
                  <div key={i} className="row">
                    <span className="mono">
                      {col(inst.data, r, 'Kind', 'kind')} {col(inst.data, r, 'Name', 'Object', 'name')}
                    </span>
                    {(['disable', 'enable', 'reset', 'detach'] as const).map((a) => (
                      <button key={a} onClick={() => void act(r, inst.data, a)}>
                        {a[0]!.toUpperCase() + a.slice(1)}…
                      </button>
                    ))}
                  </div>
                ))}
              </div>
            )}
          </>
        )
      )}
      <h2>Overrides by level</h2>
      {over.error ? <p className="muted">{errorText(over.error)}</p> : over.data && <Generic t={over.data} testid="msg-inh-overrides" />}
      {st.node}
    </div>
  );
}
