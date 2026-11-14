// A function's or procedure's staged rollout, and dropping a module with what
// still uses it. The stage and each version's numbers come from
// `GET .../rollout` every 5 s; moving, promoting and aborting are the
// statements a SQL client runs (`ALTER ... ROLLOUT TO n | PROMOTE | ABORT`),
// shown before they run. When the gateway rolled a candidate back by itself,
// the reason is the module's newest such log line.

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { errorText, isApiError } from '../api/errors';
import { execute } from '../api/sql';
import { autoRollback, getRollout, logs as fetchLogs, type VersionStats } from './api';
import { dependentsFromRows, dependentsOf, dropStatement, noDependents, rolloutStatement, showDependents, type Dependents, type LogicKind, type RolloutBy } from './declaration';

/** A statement shown before it runs, with an optional explanation. */
export function useAsk() {
  const [c, setC] = useState<{ sql: string; note?: string; resolve: (ok: boolean) => void } | null>(null);
  const ask = (sql: string, note?: string) =>
    new Promise<boolean>((resolve) => setC({ sql, note, resolve })).then((ok) => {
      setC(null);
      return ok;
    });
  const node = (
    <dialog open={!!c} className="modal" aria-labelledby="logic-ask-h">
      {c && (
        <div className="stack">
          <h2 id="logic-ask-h">Run this statement?</h2>
          {c.note && (
            <p className="banner" data-testid="logic-confirm-note">
              {c.note}
            </p>
          )}
          <pre className="logic-stmt" data-testid="logic-confirm-sql">
            {c.sql}
          </pre>
          <div className="row">
            <button className="primary" onClick={() => c.resolve(true)} data-testid="logic-confirm">
              Run
            </button>
            <button autoFocus onClick={() => c.resolve(false)}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </dialog>
  );
  return { ask, node };
}

function Histogram({ s, bounds }: Readonly<{ s: VersionStats; bounds: number[] }>) {
  const max = Math.max(1, ...s.histogram);
  const labels = [...bounds.map((b) => `≤${b} ms`), `>${bounds[bounds.length - 1] ?? 0} ms`];
  return (
    <span className="row" style={{ gap: 1, alignItems: 'flex-end', blockSize: 24 }} role="img" aria-label={`Latency of version ${s.version}: ${s.histogram.map((n, i) => `${labels[i]} ${n}`).join(', ')}`}>
      {s.histogram.map((n, i) => (
        <span
          key={i}
          title={`${labels[i]}: ${n}`}
          style={{ display: 'inline-block', inlineSize: 6, blockSize: Math.max(1, Math.round((n / max) * 24)), background: n ? 'var(--accent)' : 'var(--border)' }}
        />
      ))}
    </span>
  );
}

export function RolloutView({ db, kind, name, routines }: Readonly<{ db: string; kind: LogicKind; name: string; routines: boolean }>) {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ['logic-rollout', db, kind, name],
    queryFn: () => getRollout(db, kind, name),
    refetchInterval: () => (document.visibilityState === 'visible' ? 5000 : false),
  });
  const lg = useQuery({ queryKey: ['logic-rollback-log', db, kind, name], queryFn: () => fetchLogs(db, kind, name, 0), enabled: routines, retry: false });
  const [to, setTo] = useState('25');
  const [by, setBy] = useState<'' | RolloutBy>('');
  const [msg, setMsg] = useState<{ text: string; error: boolean } | null>(null);
  const confirm = useAsk();
  const act = async (a: Parameters<typeof rolloutStatement>[3]) => {
    setMsg(null);
    let sql: string;
    try {
      sql = rolloutStatement(db, kind, name, a);
    } catch (e) {
      setMsg({ text: e instanceof Error ? e.message : String(e), error: true });
      return;
    }
    const note =
      a === 'promote'
        ? 'The candidate becomes the active version for every caller (recompiled and redeployed as a rollback is).'
        : a === 'abort'
          ? 'Every caller goes back to the active version; the candidate is retired.'
          : undefined;
    if (!(await confirm.ask(sql, note))) return;
    try {
      await execute(db, sql);
      setMsg({ text: 'Done.', error: false });
      await qc.invalidateQueries({ queryKey: ['logic', db] });
      await q.refetch();
      await lg.refetch();
    } catch (e) {
      setMsg({ text: errorText(e), error: true });
    }
  };
  if (q.error)
    return (
      <p className="error" role="alert">
        {errorText(q.error)}
      </p>
    );
  if (!q.data) return <p className="muted">Loading…</p>;
  const v = q.data;
  const r = v.rollout;
  const back = lg.data ? autoRollback(lg.data.logs) : null;
  return (
    <section className="stack" aria-labelledby="rollout-h" data-testid="logic-rollout">
      <h3 id="rollout-h" style={{ margin: 0 }}>
        Rollout
      </h3>
      {r ? (
        <dl className="kv" data-testid="logic-rollout-stage">
          <dt>Active</dt>
          <dd>v{r.active_version}</dd>
          <dt>Candidate</dt>
          <dd>v{r.candidate_version}</dd>
          <dt>Share</dt>
          <dd data-testid="logic-rollout-pct">{r.pct}% of callers</dd>
          <dt>Picked by</dt>
          <dd>{r.by === 'tenant' ? 'tenant (each tenant gets one version)' : 'caller (each account keeps its version)'}</dd>
          <dt>Since</dt>
          <dd className="mono">{r.started_at}</dd>
        </dl>
      ) : (
        <p className="muted" data-testid="logic-rollout-none">
          No rollout: every call runs v{v.active_version ?? '–'}. Deploy with &quot;Deploy as a rollout&quot; to try a new version on a share of callers.
        </p>
      )}
      {back && (
        <p className="banner" role="status" data-testid="logic-rollout-auto">
          Rolled back by the gateway at {back.at}: {back.message}
        </p>
      )}
      {r && v.verdict && (
        <p className="error" role="alert" data-testid="logic-rollout-verdict">
          The candidate is failing its guard and will be rolled back: {v.verdict}.
        </p>
      )}
      {r && (
        <div className="row">
          <label className="row">
            Move to <input value={to} onChange={(e) => setTo(e.target.value)} size={4} inputMode="numeric" data-testid="logic-rollout-to" />%
          </label>
          <label className="row">
            Pick by
            <select value={by} onChange={(e) => setBy(e.target.value as '' | RolloutBy)}>
              <option value="">as now</option>
              <option value="user">caller</option>
              <option value="tenant">tenant</option>
            </select>
          </label>
          <button onClick={() => void act({ to: Number(to), by: by || undefined })} data-testid="logic-rollout-move">
            Move…
          </button>
          <button className="primary" onClick={() => void act('promote')} data-testid="logic-rollout-promote">
            Promote…
          </button>
          <button className="danger" onClick={() => void act('abort')} data-testid="logic-rollout-abort">
            Abort…
          </button>
        </div>
      )}
      {msg && (
        <p role={msg.error ? 'alert' : 'status'} className={msg.error ? 'error' : 'muted'}>
          {msg.text}
        </p>
      )}
      <h4 style={{ margin: 0 }}>Calls per version</h4>
      {v.stats.length ? (
        <div className="table-scroll" tabIndex={0} role="region" aria-label="Calls per version">
          <table className="table" data-testid="logic-rollout-stats">
            <thead>
              <tr>
                <th>Version</th>
                <th>Calls</th>
                <th>Errors</th>
                <th>Error rate</th>
                <th>Mean</th>
                <th>p95</th>
                <th>Latency</th>
                <th>Since</th>
              </tr>
            </thead>
            <tbody>
              {v.stats.map((s) => (
                <tr key={s.version}>
                  <td>
                    v{s.version}
                    {r && s.version === r.candidate_version ? ' (candidate)' : s.version === (r?.active_version ?? v.active_version) ? ' (active)' : ''}
                  </td>
                  <td className="num">{s.calls}</td>
                  <td className="num">{s.errors}</td>
                  <td className="num">{(s.errorRate * 100).toFixed(1)}%</td>
                  <td className="num">{s.meanMs.toFixed(2)} ms</td>
                  <td className="num">{s.p95Ms == null ? (s.calls ? '>5000 ms' : '–') : `≤${s.p95Ms} ms`}</td>
                  <td>
                    <Histogram s={s} bounds={v.buckets_ms} />
                  </td>
                  <td className="mono">{s.since}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="muted">No calls counted yet.</p>
      )}
      <p className="muted">
        The pick is a hash of the caller&apos;s account (or tenant) and the module, so an application that connects with one account gets all of one
        version or all of the other. The gateway rolls a candidate back by itself once it has {v.min_calls} calls and its error rate is above 1% and
        twice the active version&apos;s, or its p95 is twice the active one&apos;s.
      </p>
      {confirm.node}
    </section>
  );
}

const CASCADE_DOES: [keyof Dependents, string, string][] = [
  ['schedules', 'Schedules that call it', 'dropped too'],
  ['columns', 'Computed columns made from it', 'kept (their expression is a copy)'],
  ['callers', 'Code that calls it', 'kept; those calls then fail with 39860'],
  ['sinks', 'Sinks using it as TRANSFORM', 'never dropped: alter or drop the sink first'],
];

/**
 * What uses the module (`SHOW DEPENDENTS`), or `null` when nothing
 * can (not a function or procedure) or the gateway can't say (older ones:
 * the refused drop's 39875 text is read then).
 */
async function dependentsNow(db: string, kind: string, name: string): Promise<Dependents | null> {
  const sql = showDependents(db, kind, name);
  if (!sql) return null;
  try {
    const r = await execute(db, sql);
    const t = r.results.find((x) => x.columns?.length);
    if (!t) return null;
    return dependentsFromRows((t.columns ?? []).map((c) => c.name), t.rows ?? []);
  } catch {
    return null;
  }
}

/**
 * "Drop…": what still uses the module first (`SHOW DEPENDENTS`): if
 * something does, the table of it, with what `CASCADE` does to each and
 * "Drop with CASCADE…" when it may go ahead; otherwise `DROP ... RESTRICT`
 * after showing it. A refusal (39875) in between lists them the same way.
 */
export function DropModule({ db, kind, name, onDropped }: Readonly<{ db: string; kind: string; name: string; onDropped: () => void }>) {
  const confirm = useAsk();
  const [deps, setDeps] = useState<{ d: Dependents; message: string } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const drop = async (cascade: boolean) => {
    setErr(null);
    if (!cascade) {
      const d = await dependentsNow(db, kind, name);
      if (d && !noDependents(d)) {
        setDeps({ d, message: '' });
        return;
      }
    }
    let sql: string;
    try {
      sql = dropStatement(db, kind, name, cascade);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    const note = cascade
      ? 'CASCADE drops the schedules that call it, keeps computed columns, and goes ahead past code that calls it.'
      : `This removes ${name} and every version of it on every shard. If something still uses it, the drop is refused and lists them.`;
    if (!(await confirm.ask(sql, note))) return;
    try {
      await execute(db, sql);
      setDeps(null);
      onDropped();
    } catch (e) {
      const d = isApiError(e) && e.code === 39875 ? ((await dependentsNow(db, kind, name)) ?? dependentsOf(e.message)) : null;
      if (d && !noDependents(d)) setDeps({ d, message: e instanceof Error ? e.message : '' });
      else setErr(errorText(e));
    }
  };
  return (
    <div className="stack">
      <div>
        <button className="danger" onClick={() => void drop(false)} data-testid="logic-drop">
          Drop…
        </button>
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {deps && (
        <section className="card stack" aria-labelledby="logic-deps-h" data-testid="logic-dependents">
          <h3 id="logic-deps-h" style={{ margin: 0 }}>
            {name} is still in use: DROP … RESTRICT is refused (39875)
          </h3>
          <table className="table">
            <thead>
              <tr>
                <th>What uses it</th>
                <th>Names</th>
                <th>With CASCADE</th>
              </tr>
            </thead>
            <tbody>
              {CASCADE_DOES.filter(([k]) => deps.d[k].length).map(([k, label, does]) => (
                <tr key={k} data-testid={`logic-dep-${k}`}>
                  <td>{label}</td>
                  <td className="mono">{deps.d[k].join(', ')}</td>
                  <td>{does}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {deps.d.callers.length > 0 && <p className="muted">Callers are found by reading the code, so a name that only appears in a call never made is listed too.</p>}
          <div className="row">
            <button className="danger" disabled={deps.d.sinks.length > 0} onClick={() => void drop(true)} data-testid="logic-drop-cascade">
              Drop with CASCADE…
            </button>
            <button onClick={() => setDeps(null)}>Keep it</button>
          </div>
          {deps.d.sinks.length > 0 && <p className="muted">CASCADE never drops a sink: a sink without its transform would dead-letter every record.</p>}
        </section>
      )}
      {confirm.node}
    </div>
  );
}
