// The logic editors (chapter 44 step 26, "Logic editors"): KIRun functions
// in @fincity/kirun-ui, JavaScript and Lua in Monaco. "Check" is
// Logic.Compile, a dry run showing the tier and each reason where it
// applies (Monaco markers for script lines, the step's name for KIRun),
// and the generated PL/SQL for users with ALTER ROUTINE. Deploy, rollback
// and test runs are the statements a SQL client would run, shown before
// they run; a test run is a CALL inside a transaction handle that is rolled
// back unless the user keeps its changes. Logs poll Logic.TailLogs; dead
// letters are replayed or discarded.

import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useParams, useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { useDatabases } from '../catalog/snapshot';
import { errorText } from '../api/errors';
import { begin, commit, execute, rollback } from '../api/sql';
import type { MonacoHandle } from '../editor/Monaco';
import {
  compileDry,
  deadLetters,
  getModule,
  getVersion,
  listModules,
  logs as fetchLogs,
  settleDeadLetters,
  type CompileResult,
  type LogRow,
  type ModuleDetail,
} from './api';
import {
  callStatement,
  declaration,
  lineOf,
  paramsText,
  rollbackStatement,
  stepOf,
  template,
  type DeclForm,
  type LogicKind,
  type Runtime,
} from './declaration';

const Monaco = lazy(() => import('../editor/Monaco'));
const KirunEditor = lazy(() => import('./KirunEditor'));

/** A statement shown before it runs. */
function useConfirm() {
  const [c, setC] = useState<{ sql: string; resolve: (ok: boolean) => void } | null>(null);
  const ask = (sql: string) => new Promise<boolean>((resolve) => setC({ sql, resolve }));
  const node = (
    <dialog open={!!c} className="modal" aria-labelledby="logic-confirm-h">
      {c && (
        <div className="stack">
          <h2 id="logic-confirm-h">Run this statement?</h2>
          <pre className="logic-stmt" data-testid="logic-confirm-sql">
            {c.sql}
          </pre>
          <div className="row">
            <button
              className="primary"
              onClick={() => {
                c.resolve(true);
                setC(null);
              }}
              data-testid="logic-confirm"
            >
              Run
            </button>
            <button
              autoFocus
              onClick={() => {
                c.resolve(false);
                setC(null);
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </dialog>
  );
  return { ask, node };
}

export function LogicPage({ creating = false }: Readonly<{ creating?: boolean }>) {
  const { tenant = '', db: dbParam, kind, name } = useParams();
  const [params, setParams] = useSearchParams();
  const dbs = useDatabases();
  const db = dbParam ?? params.get('db') ?? dbs.data?.[0] ?? '';
  const base = `/t/${encodeURIComponent(tenant)}/logic`;
  const list = useQuery({ queryKey: ['logic', db], queryFn: () => listModules(db), enabled: !!db });
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Logic</h1>
      <div className="row">
        <label className="row">
          Database
          <select
            value={db}
            onChange={(e) => {
              const p = new URLSearchParams(params);
              p.set('db', e.target.value);
              setParams(p);
            }}
            data-testid="logic-db"
            disabled={!!dbParam}
          >
            {(dbs.data ?? []).map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <Link to={`${base}/${encodeURIComponent(db)}/new/function`}>New module</Link>
      </div>
      <div className="logic-layout">
        <nav aria-label="Modules" className="stack">
          {list.error && (
            <p className="error" role="alert">
              {errorText(list.error)}
            </p>
          )}
          <ul className="linklist" data-testid="logic-modules">
            {(list.data?.modules ?? []).map((m) => (
              <li key={m.module_id}>
                <Link to={`${base}/${encodeURIComponent(db)}/${m.kind}/${encodeURIComponent(m.name)}`} aria-current={m.name === name && m.kind === kind ? 'page' : undefined}>
                  {m.name}
                </Link>{' '}
                <span className="muted">
                  {m.kind}, {m.runtime}, v{m.active_version ?? '–'}
                </span>
              </li>
            ))}
          </ul>
          {list.data && !list.data.modules.length && <p className="muted">No logic in {db} yet.</p>}
        </nav>
        <section>
          {db && creating && kind ? (
            <Editor key={`new/${db}`} db={db} isNew newKind={kind as LogicKind} />
          ) : db && kind && name ? (
            <Module key={`${db}/${kind}/${name}`} db={db} kind={kind} name={name} />
          ) : (
            <p className="muted">Choose a module, or start a new one.</p>
          )}
        </section>
      </div>
    </div>
  );
}

function Module({ db, kind, name }: Readonly<{ db: string; kind: string; name: string }>) {
  const q = useQuery({ queryKey: ['logic', db, kind, name], queryFn: () => getModule(db, kind, name) });
  const [tab, setTab] = useState<'code' | 'versions' | 'test' | 'logs' | 'dead'>('code');
  if (q.error)
    return (
      <p className="error" role="alert">
        {errorText(q.error)}
      </p>
    );
  if (!q.data) return <p className="muted">Loading…</p>;
  const d = q.data;
  const m = d.module;
  const active = d.versions.find((v) => v.version === m.active_version) ?? d.versions[0];
  return (
    <div className="stack" data-testid="logic-module">
      <h2 style={{ margin: 0 }}>
        {m.name} <span className="muted">({m.kind})</span>
      </h2>
      <dl className="kv">
        <dt>Runtime</dt>
        <dd>{m.runtime}</dd>
        <dt>Active version</dt>
        <dd data-testid="logic-active">{m.active_version ?? 'none'}</dd>
        <dt>Tier</dt>
        <dd data-testid="logic-tier">{active?.tier ?? '–'}</dd>
        <dt>Security</dt>
        <dd>SQL SECURITY {m.security.toUpperCase()}</dd>
        <dt>Tier preference</dt>
        <dd>{m.tier_pref}</dd>
        {d.binding && (
          <>
            <dt>Binding</dt>
            <dd>
              {d.binding.timing} {d.binding.events} ({d.binding.state})
            </dd>
          </>
        )}
        {d.schedule && (
          <>
            <dt>Schedule</dt>
            <dd>
              {d.schedule.cron} {d.schedule.time_zone}, next {new Date(d.schedule.next_fire_ms).toISOString()}, last {d.schedule.last_outcome ?? '–'}
            </dd>
          </>
        )}
      </dl>
      <div className="row" role="tablist" aria-label="Module views">
        {(
          [
            ['code', 'Code'],
            ['versions', 'Versions'],
            ['test', 'Test run'],
            ['logs', 'Logs'],
            ['dead', 'Dead letters'],
          ] as const
        ).map(([k, label]) => (
          <button key={k} role="tab" aria-selected={tab === k} onClick={() => setTab(k)} data-testid={`logic-tab-${k}`}>
            {label}
          </button>
        ))}
      </div>
      {tab === 'code' &&
        (d.routines ? (
          <Editor db={db} detail={d} />
        ) : (
          <p className="muted">Viewing code needs ALTER ROUTINE, CREATE ROUTINE or ALL on {db}.</p>
        ))}
      {tab === 'versions' && <Versions db={db} detail={d} />}
      {tab === 'test' && <TestRun db={db} detail={d} />}
      {tab === 'logs' && <Logs db={db} kind={kind} name={name} />}
      {tab === 'dead' && <DeadLetters db={db} kind={kind} name={name} />}
    </div>
  );
}

function formOf(d: ModuleDetail): DeclForm {
  const v = d.versions.find((x) => x.version === (d.source_version ?? d.module.active_version)) ?? d.versions[0];
  return {
    kind: d.module.kind === 'procedure' ? 'procedure' : 'function',
    name: d.module.name,
    runtime: d.module.runtime === 'sql' ? 'javascript' : d.module.runtime,
    params: paramsText(v?.signature),
    returns: v?.signature?.returns ?? '',
    security: d.module.security,
    tier: d.module.tier_pref,
    body: d.source ?? '',
  };
}

function Editor({ db, detail, isNew, newKind }: Readonly<{ db: string; detail?: ModuleDetail; isNew?: boolean; newKind?: LogicKind }>) {
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  const qc = useQueryClient();
  const [form, setForm] = useState<DeclForm>(() =>
    detail
      ? formOf(detail)
      : { kind: newKind ?? 'function', name: '', runtime: 'javascript', security: 'invoker', tier: 'auto', ...template('javascript', newKind ?? 'function', 'f') },
  );
  const [checked, setChecked] = useState<CompileResult | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const editor = useRef<MonacoHandle | null>(null);
  const confirm = useConfirm();
  const set = (p: Partial<DeclForm>) => {
    setForm((f) => ({ ...f, ...p }));
    setChecked(null);
  };
  const stmt = useMemo(() => {
    try {
      return form.name ? declaration(db, form) : '';
    } catch (e) {
      return e instanceof Error ? `-- ${e.message}` : '';
    }
  }, [db, form]);

  // Reasons as Monaco markers on their lines (scripts).
  useEffect(() => {
    if (form.runtime === 'kirun') return;
    const ed = editor.current;
    if (!ed) return;
    const reasons = [...(checked?.placement?.reasons ?? []), ...(checked?.placement?.lints ?? [])];
    const lines = form.body.split('\n');
    const offsetOf = (line: number) => lines.slice(0, line - 1).reduce((n, l) => n + l.length + 1, 0);
    ed.setMarkers(
      reasons
        .map((r) => [r, lineOf(r)] as const)
        .filter(([, l]) => l !== null && l >= 1 && l <= lines.length)
        .map(([r, l]) => [offsetOf(l!), offsetOf(l!) + Math.max(1, lines[l! - 1]!.length), `${r.code}: ${r.detail}`, true] as [number, number, string, boolean]),
    );
  }, [checked, form.body, form.runtime]);

  const check = async () => {
    setErr(null);
    setBusy(true);
    try {
      setChecked(await compileDry(db, declaration(db, form)));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const deploy = async () => {
    setErr(null);
    setNotice(null);
    let text: string;
    try {
      text = declaration(db, form);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    if (!(await confirm.ask(text))) return;
    setBusy(true);
    try {
      const r = await execute(db, text);
      setNotice(`Deployed${r.results[0]?.warnings ? ` with ${r.results[0].warnings} notes` : ''}.`);
      await qc.invalidateQueries({ queryKey: ['logic', db] });
      if (isNew) void navigate(`/t/${encodeURIComponent(tenant)}/logic/${encodeURIComponent(db)}/${form.kind}/${encodeURIComponent(form.name)}`);
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const reasons = [...(checked?.placement?.reasons ?? [])];
  return (
    <div className="stack">
      {isNew && (
        <div className="row">
          <label className="row">
            Name
            <input value={form.name} onChange={(e) => set({ name: e.target.value.trim() })} data-testid="logic-name" required />
          </label>
          <label className="row">
            Kind
            <select value={form.kind} onChange={(e) => set({ kind: e.target.value as LogicKind })} data-testid="logic-kind">
              <option value="function">function</option>
              <option value="procedure">procedure</option>
            </select>
          </label>
          <label className="row">
            Language
            <select
              value={form.runtime}
              onChange={(e) => {
                const runtime = e.target.value as Runtime;
                set({ runtime, ...template(runtime, form.kind, form.name || 'f') });
              }}
              data-testid="logic-runtime"
            >
              <option value="javascript">JavaScript</option>
              <option value="lua">Lua</option>
              <option value="kirun">KIRun</option>
            </select>
          </label>
        </div>
      )}
      <div className="row">
        {form.runtime !== 'kirun' && (
          <>
            <label className="row">
              Parameters
              <input value={form.params} onChange={(e) => set({ params: e.target.value })} size={30} data-testid="logic-params" />
            </label>
            {form.kind === 'function' && (
              <label className="row">
                Returns
                <input value={form.returns} onChange={(e) => set({ returns: e.target.value })} size={12} data-testid="logic-returns" />
              </label>
            )}
          </>
        )}
        <label className="row">
          Security
          <select value={form.security} onChange={(e) => set({ security: e.target.value as DeclForm['security'] })}>
            <option value="invoker">INVOKER</option>
            <option value="definer">DEFINER</option>
          </select>
        </label>
        <label className="row">
          Tier
          <select value={form.tier} onChange={(e) => set({ tier: e.target.value as DeclForm['tier'] })} data-testid="logic-tier-pref">
            <option value="auto">auto</option>
            <option value="mariadb">mariadb</option>
            <option value="engine">engine</option>
          </select>
        </label>
      </div>
      <Suspense fallback={<div className="editor-host muted">Loading editor…</div>}>
        {form.runtime === 'kirun' ? (
          <>
            <KirunEditor value={form.body} onChange={(body) => set({ body })} />
            <details>
              <summary>Definition as JSON</summary>
              <Monaco
                value={form.body}
                onChange={(body) => set({ body })}
                onRun={() => void check()}
                completion={{ keywords: [], objects: () => [], columns: () => [] }}
                language="javascript"
                label="KIRun definition JSON"
                handle={() => undefined}
              />
            </details>
          </>
        ) : (
          <Monaco
            value={form.body}
            onChange={(body) => set({ body })}
            onRun={() => void check()}
            completion={{ keywords: [], objects: () => [], columns: () => [] }}
            language={form.runtime === 'lua' ? 'lua' : 'javascript'}
            label={`${form.runtime} source of ${form.name || 'the new module'}`}
            handle={(h) => {
              editor.current = h;
            }}
          />
        )}
      </Suspense>
      <div className="row">
        <button onClick={() => void check()} disabled={busy || !form.name} data-testid="logic-check" title="Ctrl/⌘ + Enter">
          Check
        </button>
        <button className="primary" onClick={() => void deploy()} disabled={busy || !form.name} data-testid="logic-deploy">
          Deploy
        </button>
      </div>
      {err && (
        <p className="error" role="alert" data-testid="logic-error">
          {err}
        </p>
      )}
      {notice && (
        <p role="status" data-testid="logic-notice">
          {notice}
        </p>
      )}
      {checked && (
        <section className="card stack" aria-label="Check result" data-testid="logic-check-result">
          {checked.diagnostics.map((d, i) => (
            <p key={i} className="error">
              {d.code} {d.name}: {d.message}
            </p>
          ))}
          {checked.tier && (
            <p>
              Tier: <strong data-testid="logic-check-tier">{checked.tier}</strong>
              {checked.placement && ` · cost ${checked.placement.cost} · reads ${checked.placement.reads.join(', ') || '–'} · writes ${checked.placement.writes.join(', ') || '–'}`}
            </p>
          )}
          {reasons.length > 0 && (
            <>
              <h3>Why not MariaDB</h3>
              <ul data-testid="logic-reasons">
                {reasons.map((r, i) => (
                  <li key={i}>
                    <span className="mono">{r.code}</span> at {stepOf(r) ? <strong>step {stepOf(r)}</strong> : r.at}: {r.detail}
                  </li>
                ))}
              </ul>
            </>
          )}
          {(checked.placement?.lints.length ?? 0) > 0 && (
            <ul>
              {checked.placement!.lints.map((r, i) => (
                <li key={i} className="muted">
                  {r.code} at {r.at}: {r.detail}
                </li>
              ))}
            </ul>
          )}
          {checked.plsql && (
            <details>
              <summary>Generated PL/SQL</summary>
              <pre data-testid="logic-plsql">{checked.plsql}</pre>
            </details>
          )}
          {checked.plsql_hidden && <p className="muted">The generated PL/SQL shows to users with ALTER ROUTINE.</p>}
        </section>
      )}
      <details>
        <summary>Statement</summary>
        <pre className="logic-stmt">{stmt}</pre>
      </details>
      {confirm.node}
    </div>
  );
}

function Versions({ db, detail }: Readonly<{ db: string; detail: ModuleDetail }>) {
  const qc = useQueryClient();
  const confirm = useConfirm();
  const [err, setErr] = useState<string | null>(null);
  const [shown, setShown] = useState<{ v: number; source: string; plsql: string | null } | null>(null);
  const m = detail.module;
  const kind: LogicKind = m.kind === 'procedure' ? 'procedure' : 'function';
  const roll = async (v: number) => {
    const text = rollbackStatement(db, kind, m.name, v);
    if (!(await confirm.ask(text))) return;
    try {
      await execute(db, text);
      await qc.invalidateQueries({ queryKey: ['logic', db] });
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <div className="stack">
      <table className="table" data-testid="logic-versions">
        <thead>
          <tr>
            <th>Version</th>
            <th>State</th>
            <th>Tier</th>
            <th>Reasons</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {detail.versions.map((v) => (
            <tr key={v.version}>
              <td>{v.version}</td>
              <td>{v.state}</td>
              <td>{v.tier}</td>
              <td>{(v.placement?.reasons ?? []).map((r) => r.code).join(', ') || '–'}</td>
              <td className="row">
                {detail.routines && (
                  <button
                    onClick={() =>
                      void getVersion(db, m.kind, m.name, v.version).then(
                        (x) => setShown({ v: v.version, source: x.source, plsql: x.plsql }),
                        (e: unknown) => setErr(errorText(e)),
                      )
                    }
                  >
                    Source
                  </button>
                )}
                {v.version !== m.active_version && ['active', 'superseded', 'retired', 'compiled', 'staged'].includes(v.state) && (
                  <button onClick={() => void roll(v.version)} data-testid={`logic-rollback-${v.version}`}>
                    Roll back to this
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        Rollout by percentage (candidate versions) isn&apos;t built in the logic layer yet; a deploy activates the new version, and rollback
        switches at once (chapter 41).
      </p>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {shown && (
        <div className="stack">
          <h3>Version {shown.v}</h3>
          <pre data-testid="logic-version-source">{shown.source}</pre>
          {shown.plsql && (
            <details>
              <summary>PL/SQL</summary>
              <pre>{shown.plsql}</pre>
            </details>
          )}
        </div>
      )}
      {confirm.node}
    </div>
  );
}

function TestRun({ db, detail }: Readonly<{ db: string; detail: ModuleDetail }>) {
  const m = detail.module;
  const v = detail.versions.find((x) => x.version === m.active_version) ?? detail.versions[0];
  const params = v?.signature?.params ?? [];
  const [values, setValues] = useState<Record<string, string>>({});
  const [keep, setKeep] = useState(false);
  const [out, setOut] = useState<{ columns: string[]; rows: (string | null)[][]; ms: number; kept: boolean } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const confirm = useConfirm();
  const me = useSession((s) => s.me);
  if (m.kind !== 'function' && m.kind !== 'procedure') return <p className="muted">Only functions and procedures run on their own.</p>;
  const run = async () => {
    setErr(null);
    setOut(null);
    let text: string;
    try {
      text = callStatement(db, m.name, params, values);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    if (keep && !(await confirm.ask(`${text}\n-- then COMMIT`))) return;
    setBusy(true);
    const t0 = performance.now();
    const tx = await begin(db).catch((e: unknown) => {
      setErr(errorText(e));
      return null;
    });
    if (!tx) {
      setBusy(false);
      return;
    }
    try {
      const r = await execute(db, text, { transaction: tx.name, timeoutMs: me?.studio.statement_timeout_ms });
      const first = r.results.find((x) => x.columns?.length);
      if (keep) await commit(tx);
      else await rollback(tx);
      setOut({
        columns: (first?.columns ?? []).map((c) => c.name),
        rows: first?.rows ?? [],
        ms: Math.round(performance.now() - t0),
        kept: keep,
      });
    } catch (e) {
      await rollback(tx).catch(() => undefined);
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="stack">
      {params.length === 0 && <p className="muted">No parameters.</p>}
      {params.map((p) => (
        <label key={p.name} className="row">
          <span className="mono">
            {p.name} ({p.sql ?? p.type})
          </span>
          <input value={values[p.name] ?? ''} onChange={(e) => setValues({ ...values, [p.name]: e.target.value })} data-testid={`logic-arg-${p.name}`} />
        </label>
      ))}
      <label className="row">
        <input type="checkbox" checked={keep} onChange={(e) => setKeep(e.target.checked)} />
        Keep its changes (commit); otherwise the run is rolled back
      </label>
      <div className="row">
        <button className="primary" onClick={() => void run()} disabled={busy} data-testid="logic-run">
          Run
        </button>
      </div>
      {err && (
        <p className="error" role="alert" data-testid="logic-run-error">
          {err}
        </p>
      )}
      {out && (
        <div className="stack" data-testid="logic-run-result">
          <p className="muted">
            {out.ms} ms, {out.kept ? 'committed' : 'rolled back'}
          </p>
          <table className="table">
            <thead>
              <tr>
                {out.columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {out.rows.map((r, i) => (
                <tr key={i}>
                  {r.map((c, j) => (
                    <td key={j} className="mono">
                      {c ?? 'NULL'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {confirm.node}
    </div>
  );
}

function Logs({ db, kind, name }: Readonly<{ db: string; kind: string; name: string }>) {
  const [rows, setRows] = useState<LogRow[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [live, setLive] = useState(true);
  const after = useRef(0);
  useEffect(() => {
    let stop = false;
    const tick = async () => {
      try {
        const r = await fetchLogs(db, kind, name, after.current);
        if (stop) return;
        if (r.logs.length) {
          after.current = Math.max(after.current, ...r.logs.map((l) => l.log_id));
          setRows((x) => [...r.logs.slice().reverse(), ...x].slice(0, 1000));
        }
        setErr(null);
      } catch (e) {
        if (!stop) setErr(errorText(e));
      }
    };
    void tick();
    if (!live) return () => void (stop = true);
    const t = setInterval(() => {
      if (document.visibilityState === 'visible') void tick();
    }, 2000);
    return () => {
      stop = true;
      clearInterval(t);
    };
  }, [db, kind, name, live]);
  return (
    <div className="stack">
      <label className="row">
        <input type="checkbox" checked={live} onChange={(e) => setLive(e.target.checked)} /> Follow (every 2 s)
      </label>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      <table className="table" data-testid="logic-logs">
        <thead>
          <tr>
            <th>At (UTC)</th>
            <th>Level</th>
            <th>Source</th>
            <th>Version</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((l) => (
            <tr key={l.log_id}>
              <td className="mono">{l.at}</td>
              <td>{l.level}</td>
              <td>{l.source}</td>
              <td>{l.version ?? '–'}</td>
              <td>{l.message}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && !err && <p className="muted">No log lines: triggers and schedules write them.</p>}
    </div>
  );
}

function DeadLetters({ db, kind, name }: Readonly<{ db: string; kind: string; name: string }>) {
  const [state, setState] = useState('dead');
  const q = useQuery({ queryKey: ['logic-dead', db, kind, name, state], queryFn: () => deadLetters(db, kind, name, state) });
  const [sel, setSel] = useState<number[]>([]);
  const [msg, setMsg] = useState<string | null>(null);
  const act = async (verb: 'replay' | 'discard') => {
    try {
      const r = await settleDeadLetters(db, kind, name, verb, sel.length ? sel : 'all');
      setMsg(verb === 'replay' ? `${r.replaying ?? 0} marked for replay, ${r.skipped ?? 0} skipped` : `${r.discarded ?? 0} discarded`);
      setSel([]);
      await q.refetch();
    } catch (e) {
      setMsg(errorText(e));
    }
  };
  if (q.error)
    return (
      <p className="error" role="alert">
        {errorText(q.error)}
      </p>
    );
  if (q.data && q.data.binding === null) return <p className="muted">This module has no binding, so no dead letters.</p>;
  return (
    <div className="stack">
      <div className="row">
        <label className="row">
          State
          <select value={state} onChange={(e) => setState(e.target.value)}>
            {['dead', 'replaying', 'replayed', 'discarded'].map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
        <button onClick={() => void act('replay')} disabled={state !== 'dead'}>
          Replay {sel.length ? 'selected' : 'all'}
        </button>
        <button onClick={() => void act('discard')} disabled={state !== 'dead'}>
          Discard {sel.length ? 'selected' : 'all'}
        </button>
      </div>
      {msg && <p role="status">{msg}</p>}
      <table className="table" data-testid="logic-dead">
        <thead>
          <tr>
            <th />
            <th>Id</th>
            <th>Event</th>
            <th>Attempts</th>
            <th>Error</th>
            <th>Last failed</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.dead_letters ?? []).map((d) => (
            <tr key={d.dl_id}>
              <td>
                <input
                  type="checkbox"
                  aria-label={`Select ${d.dl_id}`}
                  checked={sel.includes(d.dl_id)}
                  onChange={(e) => setSel((x) => (e.target.checked ? [...x, d.dl_id] : x.filter((i) => i !== d.dl_id)))}
                />
              </td>
              <td>{d.dl_id}</td>
              <td className="mono">{JSON.stringify(d.event).slice(0, 200)}</td>
              <td>{d.attempts}</td>
              <td>
                {d.error_code}: {d.error_message}
              </td>
              <td className="mono">{d.last_failed_at}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
