// Roles, the policy editor and the "view as this user" tester. Roles and
// policies are the statements a SQL client runs (CREATE ROLE, GRANT, CREATE
// POLICY ...), built by forms and shown before they run. The tester calls Policies.TestAs: one read-only
// statement run as another user of the tenant, never wider than the
// caller's own view.

import { lazy, Suspense, useMemo, useState } from 'react';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { execute, rows as rowsOf } from '../api/sql';
import { useDatabases, useSnapshot } from '../catalog/snapshot';
import { useSession } from '../auth/session';
import type { CompletionSource } from '../editor/Monaco';
import { KEYWORDS } from '../editor/keywords';
import { createPolicy, createRole, dropPolicy, grantRole, grantToRole, POLICY_FUNCTIONS, rowSecurity, type PolicyForm } from './policies';

const Monaco = lazy(() => import('../editor/Monaco'));

function useRun() {
  const [c, setC] = useState<{ sql: string; resolve: (ok: boolean) => void } | null>(null);
  const [msg, setMsg] = useState<{ text: string; error: boolean } | null>(null);
  const run = async (sql: string) => {
    const ok = await new Promise<boolean>((resolve) => setC({ sql, resolve }));
    setC(null);
    if (!ok) return;
    try {
      await execute('-', sql);
      setMsg({ text: 'Done.', error: false });
    } catch (e) {
      setMsg({ text: errorText(e), error: true });
    }
  };
  const node = (
    <>
      {msg && (
        <p role={msg.error ? 'alert' : 'status'} className={msg.error ? 'error' : 'muted'} data-testid="pol-outcome">
          {msg.text}
        </p>
      )}
      <dialog open={!!c} className="modal" aria-labelledby="pol-confirm-h">
        {c && (
          <div className="stack">
            <h2 id="pol-confirm-h">Run this statement?</h2>
            <pre className="logic-stmt" data-testid="pol-confirm-sql">
              {c.sql}
            </pre>
            <div className="row">
              <button className="primary" onClick={() => c.resolve(true)} data-testid="pol-confirm">
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
  return { run, node };
}

export function PoliciesPage() {
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Roles and policies</h1>
      <ViewAs />
      <Roles />
      <Policies />
    </div>
  );
}

function Roles() {
  const r = useRun();
  const [role, setRole] = useState('');
  const [priv, setPriv] = useState('SELECT');
  const [on, setOn] = useState('');
  const [user, setUser] = useState('');
  const [show, setShow] = useState<{ who: string; lines: string[]; err: string | null } | null>(null);
  const showGrants = async (who: string) => {
    try {
      const x = await rowsOf('-', `SHOW GRANTS FOR ${who}`);
      setShow({ who, lines: x.rows.map((row) => row[0] ?? ''), err: null });
    } catch (e) {
      setShow({ who, lines: [], err: errorText(e) });
    }
  };
  const safe = (f: () => string) => {
    try {
      void r.run(f());
    } catch (e) {
      window.alert(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <section className="card stack" aria-labelledby="roles-h">
      <h2 id="roles-h">Roles</h2>
      <div className="row">
        <label className="row">
          Role <input value={role} onChange={(e) => setRole(e.target.value)} data-testid="pol-role" />
        </label>
        <button onClick={() => safe(() => createRole(role))} disabled={!role}>
          Create role…
        </button>
        <button onClick={() => void showGrants(`'${role.replace(/'/g, "''")}'`)} disabled={!role}>
          Show its grants
        </button>
      </div>
      <div className="row">
        <label className="row">
          Privileges <input value={priv} onChange={(e) => setPriv(e.target.value)} size={18} />
        </label>
        <label className="row">
          On <input value={on} onChange={(e) => setOn(e.target.value)} placeholder="shop.* or shop.orders" size={18} data-testid="pol-on" />
        </label>
        <button onClick={() => safe(() => grantToRole(priv, on, role))} disabled={!role || !on}>
          Grant to the role…
        </button>
      </div>
      <div className="row">
        <label className="row">
          User <input value={user} onChange={(e) => setUser(e.target.value)} data-testid="pol-user" />
        </label>
        <button onClick={() => safe(() => grantRole(role, user))} disabled={!role || !user}>
          Grant the role to the user…
        </button>
      </div>
      {show && (
        <div className="stack">
          <h3 className="mono">{show.who}</h3>
          {show.err ? (
            <p className="error">{show.err}</p>
          ) : (
            <ul className="mono" data-testid="pol-grants">
              {show.lines.map((l, i) => (
                <li key={i}>{l}</li>
              ))}
            </ul>
          )}
        </div>
      )}
      {r.node}
    </section>
  );
}

function Policies() {
  const dbs = useDatabases();
  const [f, setF] = useState<PolicyForm>({ name: '', db: '', table: '', kind: 'PERMISSIVE', command: 'SELECT', to: 'PUBLIC', using: '', check: '' });
  const db = f.db || dbs.data?.[0] || '';
  const snap = useSnapshot(db || null);
  const r = useRun();
  const completion: CompletionSource = useMemo(
    () => ({
      keywords: [...KEYWORDS, ...POLICY_FUNCTIONS],
      objects: () => (snap.data?.objects ?? []).map((o) => ({ name: o.name, detail: o.kind })),
      columns: () => (snap.data?.columns[f.table] ?? []).map((c) => ({ name: c.name, detail: `${f.table}.${c.name} ${c.type}` })),
    }),
    [snap.data, f.table],
  );
  let stmt = '';
  let err: string | null = null;
  try {
    stmt = f.name && f.table ? createPolicy({ ...f, db }) : '';
  } catch (e) {
    err = e instanceof Error ? e.message : String(e);
  }
  return (
    <section className="card stack" aria-labelledby="policies-h">
      <h2 id="policies-h">Row-level security policies</h2>
      <div className="row">
        <label className="row">
          Name <input value={f.name} onChange={(e) => setF({ ...f, name: e.target.value })} data-testid="pol-name" />
        </label>
        <label className="row">
          Database
          <select value={db} onChange={(e) => setF({ ...f, db: e.target.value, table: '' })}>
            {(dbs.data ?? []).map((d) => (
              <option key={d}>{d}</option>
            ))}
          </select>
        </label>
        <label className="row">
          Table
          <select value={f.table} onChange={(e) => setF({ ...f, table: e.target.value })} data-testid="pol-table">
            <option value="">(choose)</option>
            {(snap.data?.objects ?? [])
              .filter((o) => o.kind === 'table')
              .map((o) => (
                <option key={o.name}>{o.name}</option>
              ))}
          </select>
        </label>
        <label className="row">
          As
          <select value={f.kind} onChange={(e) => setF({ ...f, kind: e.target.value as PolicyForm['kind'] })}>
            <option>PERMISSIVE</option>
            <option>RESTRICTIVE</option>
          </select>
        </label>
        <label className="row">
          For
          <select value={f.command} onChange={(e) => setF({ ...f, command: e.target.value as PolicyForm['command'] })}>
            {['ALL', 'SELECT', 'INSERT', 'UPDATE', 'DELETE'].map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </label>
        <label className="row">
          To <input value={f.to} onChange={(e) => setF({ ...f, to: e.target.value })} size={14} />
        </label>
      </div>
      <p className="muted">USING: which rows the policy admits. Session functions: {POLICY_FUNCTIONS.join(', ')}.</p>
      <Suspense fallback={<div className="editor-host muted">Loading editor…</div>}>
        <Monaco
          value={f.using}
          onChange={(using) => setF((x) => ({ ...x, using }))}
          onRun={() => undefined}
          completion={completion}
          label="Policy USING expression"
          handle={() => undefined}
        />
      </Suspense>
      {(f.command === 'INSERT' || f.command === 'UPDATE' || f.command === 'ALL') && (
        <label className="stack">
          <span>WITH CHECK (rows a write may produce; empty: the USING expression)</span>
          <input value={f.check} onChange={(e) => setF({ ...f, check: e.target.value })} />
        </label>
      )}
      {err && <p className="error">{err}</p>}
      {stmt && <pre className="logic-stmt" data-testid="pol-statement">{stmt}</pre>}
      <div className="row">
        <button className="primary" onClick={() => void r.run(stmt)} disabled={!stmt}>
          Create policy…
        </button>
        {(['ENABLE', 'FORCE', 'NO FORCE', 'DISABLE'] as const).map((m) => (
          <button key={m} onClick={() => void r.run(rowSecurity(db, f.table, m))} disabled={!f.table} data-testid={`pol-${m.replace(' ', '-').toLowerCase()}`}>
            {m.charAt(0) + m.slice(1).toLowerCase()} row-level security…
          </button>
        ))}
      </div>
      <p className="muted">
        A table&apos;s policies apply once its row-level security is enabled (then no policy means no rows). Users holding ALTER on the
        table manage it and aren&apos;t bound unless it is forced; BYPASS RLS frees a user. The tester runs as the user, with their
        policies.
      </p>
      <PolicyList db={db} onDrop={(name, table) => void r.run(dropPolicy(name, db, table))} />
      {r.node}
    </section>
  );
}

/** `SHOW POLICIES FROM db`: the database's policies and switches. */
function PolicyList({ db, onDrop }: { db: string; onDrop: (name: string, table: string) => void }) {
  const [list, setList] = useState<(string | null)[][] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const load = async () => {
    try {
      setErr(null);
      setList((await rowsOf('-', `SHOW POLICIES FROM \`${db.replace(/`/g, '``')}\``)).rows);
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <div className="stack">
      <div className="row">
        <button onClick={() => void load()} disabled={!db} data-testid="pol-list">
          Show policies in {db || '…'}
        </button>
      </div>
      {err && <p className="error">{err}</p>}
      {list && (
        <table className="grid" data-testid="pol-rows">
          <thead>
            <tr>
              {['Table', 'Policy', 'Mode', 'Commands', 'Roles', 'Using', 'With check', 'Enabled', 'Forced', ''].map((h) => (
                <th key={h}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {list.map((row, i) => (
              <tr key={i}>
                {row.slice(1).map((c, j) => (
                  <td key={j}>{c ?? ''}</td>
                ))}
                <td>{row[2] && <button onClick={() => onDrop(row[2] ?? '', row[1] ?? '')}>Drop…</button>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

interface TestAsResult {
  as: string;
  columns: string[];
  rows: (string | null)[][];
  truncated: boolean;
  max_rows: number;
  policies?: { table: string; policy: string; mode: string; predicate: string }[];
}

/** A JSON object from the text, or undefined when empty; throws on bad JSON. */
function jsonObject(text: string, what: string): Record<string, unknown> | undefined {
  if (!text.trim()) return undefined;
  const v: unknown = JSON.parse(text);
  if (typeof v !== 'object' || v === null || Array.isArray(v)) throw new Error(`${what} must be a JSON object`);
  return v as Record<string, unknown>;
}

function ViewAs() {
  const me = useSession((s) => s.me);
  const dbs = useDatabases();
  const [user, setUser] = useState('');
  const [tenant, setTenant] = useState('');
  const [db, setDb] = useState('');
  const [sql, setSql] = useState('');
  const [claims, setClaims] = useState('');
  const [attrs, setAttrs] = useState('');
  const [out, setOut] = useState<TestAsResult | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const run = async () => {
    setErr(null);
    setOut(null);
    setBusy(true);
    try {
      setOut(
        await json<TestAsResult>('/v1/policies:testAs', {
          body: {
            user,
            tenant: tenant || undefined,
            database: db || dbs.data?.[0] || '-',
            sql,
            claims: jsonObject(claims, 'Claims'),
            attributes: jsonObject(attrs, 'Attributes'),
          },
        }),
      );
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  if (!me?.privileges.tenant_admin) {
    return (
      <section className="card">
        <h2>View as a user</h2>
        <p className="muted">The tester is for tenant administrators (SPRIG_TENANT_ADMIN).</p>
      </section>
    );
  }
  return (
    <section className="card stack" aria-labelledby="viewas-h">
      <h2 id="viewas-h">View as a user</h2>
      <p className="muted">
        Runs one read-only statement as the user: their privileges and policies apply, capped at studio.max_result_rows. Audited as
        studio.view_as.
      </p>
      <div className="row">
        <label className="row">
          User <input value={user} onChange={(e) => setUser(e.target.value)} data-testid="viewas-user" />
        </label>
        <label className="row">
          Tenant
          <select value={tenant} onChange={(e) => setTenant(e.target.value)}>
            <option value="">{me.principal.tenant} (yours)</option>
            {me.tenants
              .filter((t) => !!t.name && t.name !== me.principal.tenant)
              .map((t) => (
                <option key={t.name ?? ''} value={t.name ?? ''}>
                  {t.path ?? t.name}
                </option>
              ))}
          </select>
        </label>
        <label className="row">
          Database
          <select value={db} onChange={(e) => setDb(e.target.value)} data-testid="viewas-db">
            {(dbs.data ?? []).map((d) => (
              <option key={d}>{d}</option>
            ))}
          </select>
        </label>
      </div>
      <textarea rows={3} aria-label="Statement" value={sql} onChange={(e) => setSql(e.target.value)} data-testid="viewas-sql" className="mono" />
      <div className="row">
        <label className="stack">
          Simulated claims (SPRIG_CLAIM), JSON
          <textarea rows={2} className="mono" value={claims} onChange={(e) => setClaims(e.target.value)} placeholder='{"sub": "u_123"}' data-testid="viewas-claims" />
        </label>
        <label className="stack">
          Simulated attributes (SPRIG_USER_ATTR), JSON
          <textarea rows={2} className="mono" value={attrs} onChange={(e) => setAttrs(e.target.value)} placeholder='{"org": "acme"}' data-testid="viewas-attrs" />
        </label>
      </div>
      <div className="row">
        <button className="primary" onClick={() => void run()} disabled={busy || !user || !sql.trim()} data-testid="viewas-run">
          Run as {user || 'the user'}
        </button>
      </div>
      {err && (
        <p className="error" role="alert" data-testid="viewas-error">
          {err}
        </p>
      )}
      {out && (
        <div className="stack" data-testid="viewas-result">
          <p className="muted">
            As {out.as}: {out.rows.length} rows{out.truncated ? ` (cut at ${out.max_rows})` : ''}
          </p>
          {(out.policies?.length ?? 0) > 0 && (
            <table className="table" data-testid="viewas-policies">
              <caption>Policies applied, with this run's values bound</caption>
              <thead>
                <tr>
                  <th>Table</th>
                  <th>Policy</th>
                  <th>Mode</th>
                  <th>Predicate</th>
                </tr>
              </thead>
              <tbody>
                {out.policies?.map((p) => (
                  <tr key={`${p.table}.${p.policy}.${p.predicate}`}>
                    <td>{p.table}</td>
                    <td>{p.policy}</td>
                    <td>{p.mode}</td>
                    <td className="mono">{p.predicate}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <div className="table-scroll" tabIndex={0} role="region" aria-label="Rows the user sees">
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
        </div>
      )}
    </section>
  );
}
