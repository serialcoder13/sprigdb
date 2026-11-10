import { useState } from 'react';
import { NavLink, useParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { execute, rows as fetchRows } from '../api/sql';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import { useDatabases } from '../catalog/snapshot';
import { createUser, dropUser, grant, lockUser, PRIVILEGES, revoke, showGrants, unlockUser } from './statements';

interface UserRow {
  user: string;
  host: string;
  kind: string;
  auth_methods: string;
  account_locked: boolean;
  password_expired: boolean;
  require_tls: string;
}

interface KeyRow {
  key_id: string;
  name: string;
  user: string | null;
  host: string | null;
  expires_at_ms: string | null;
  revoked_at_ms: string | null;
  created_at_ms: string;
}

/** A statement shown before it runs (chapter 44: no second path). */
function Pending({ sql, onDone, then }: { sql: string[]; onDone: (msg: string, ok: boolean) => void; then?: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  return (
    <div className="card stack" role="region" aria-label="Statements to run">
      <pre data-testid="access-statement">{sql.join(';\n')}</pre>
      <div className="row">
        <button
          className="primary"
          disabled={busy}
          onClick={() => {
            setBusy(true);
            void (async () => {
              try {
                for (const s of sql) await execute(null, s);
                if (then) await then();
                onDone('Done.', true);
              } catch (e) {
                onDone(errorText(e), false);
              } finally {
                setBusy(false);
              }
            })();
          }}
        >
          Run
        </button>
        <button onClick={() => onDone('', true)}>Cancel</button>
      </div>
    </div>
  );
}

function Users() {
  const me = useSession((s) => s.me);
  const qc = useQueryClient();
  const users = useQuery({ queryKey: ['users'], queryFn: () => json<{ users: UserRow[]; all: boolean }>('/v1/users') });
  const [pending, setPending] = useState<{ sql: string[]; then?: () => Promise<void> } | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [grants, setGrants] = useState<{ who: string; lines: string[] } | null>(null);
  const [nu, setNu] = useState({ user: '', host: '%', password: '' });
  const [pw, setPw] = useState<{ user: string; host: string; password: string } | null>(null);
  const done = (text: string, ok: boolean) => {
    setPending(null);
    if (text) setMsg({ ok, text });
    void qc.invalidateQueries({ queryKey: ['users'] });
  };
  const setPassword = (user: string, host: string, password: string) =>
    json('/v1/users:setPassword', { body: { user, host, password } });
  const canAdmin = me?.privileges.create_user || me?.privileges.tenant_admin;
  return (
    <div className="stack">
      {msg && (
        <p className={msg.ok ? 'banner' : 'error'} role={msg.ok ? 'status' : 'alert'} data-testid="access-message">
          {msg.text}
        </p>
      )}
      {pending && <Pending sql={pending.sql} then={pending.then} onDone={done} />}
      {users.error && <p className="error">{errorText(users.error)}</p>}
      <table data-testid="users-table">
        <thead>
          <tr>
            <th scope="col">User</th>
            <th scope="col">Host</th>
            <th scope="col">Kind</th>
            <th scope="col">Methods</th>
            <th scope="col">State</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {(users.data?.users ?? []).map((u) => (
            <tr key={`${u.user}@${u.host}`}>
              <td>{u.user}</td>
              <td className="mono">{u.host}</td>
              <td>{u.kind}</td>
              <td>{u.auth_methods}</td>
              <td>
                {u.account_locked ? 'locked' : 'open'}
                {u.password_expired ? ', password expired' : ''}
              </td>
              <td className="row">
                <button
                  onClick={() =>
                    void fetchRows(null, showGrants(u.user, u.host))
                      .then((r) => setGrants({ who: `${u.user}@${u.host}`, lines: r.rows.map((x) => x[0] ?? '') }))
                      .catch((e: unknown) => setMsg({ ok: false, text: errorText(e) }))
                  }
                >
                  Grants
                </button>
                <button onClick={() => setPw({ user: u.user, host: u.host, password: '' })}>Password</button>
                {canAdmin && (
                  <>
                    <button onClick={() => setPending({ sql: [u.account_locked ? unlockUser(u.user, u.host) : lockUser(u.user, u.host)] })}>
                      {u.account_locked ? 'Unlock' : 'Lock'}
                    </button>
                    <button
                      className="danger"
                      onClick={() => {
                        const typed = window.prompt(`Type ${u.user} to drop the account`);
                        if (typed === u.user) setPending({ sql: [dropUser(u.user, u.host)] });
                      }}
                    >
                      Drop
                    </button>
                  </>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {grants && (
        <section className="card" aria-label={`Grants of ${grants.who}`}>
          <h3>Grants of {grants.who}</h3>
          <pre>{grants.lines.join('\n')}</pre>
          <button onClick={() => setGrants(null)}>Close</button>
        </section>
      )}
      {pw && (
        <form
          className="card row"
          onSubmit={(e) => {
            e.preventDefault();
            void setPassword(pw.user, pw.host, pw.password)
              .then(() => setMsg({ ok: true, text: `Password of ${pw.user} changed.` }))
              .catch((err: unknown) => setMsg({ ok: false, text: errorText(err) }))
              .finally(() => setPw(null));
          }}
        >
          <label className="row">
            New password for {pw.user}@{pw.host}
            <input
              type="password"
              autoComplete="new-password"
              value={pw.password}
              onChange={(e) => setPw({ ...pw, password: e.target.value })}
              required
            />
          </label>
          <button className="primary" type="submit">
            Set password
          </button>
          <button type="button" onClick={() => setPw(null)}>
            Cancel
          </button>
        </form>
      )}
      {canAdmin && (
        <form
          className="card stack"
          aria-label="New user"
          onSubmit={(e) => {
            e.preventDefault();
            const { user, host, password } = nu;
            setPending({
              sql: createUser(user, host),
              then: async () => {
                await setPassword(user, host, password);
                await execute(null, unlockUser(user, host));
                setNu({ user: '', host: '%', password: '' });
              },
            });
          }}
        >
          <h3 style={{ margin: 0 }}>New user</h3>
          <div className="row">
            <label className="row">
              User <input required value={nu.user} onChange={(e) => setNu({ ...nu, user: e.target.value })} name="new-user" />
            </label>
            <label className="row">
              Host <input value={nu.host} onChange={(e) => setNu({ ...nu, host: e.target.value })} style={{ inlineSize: 100 }} />
            </label>
            <label className="row">
              Password{' '}
              <input
                type="password"
                required
                autoComplete="new-password"
                value={nu.password}
                onChange={(e) => setNu({ ...nu, password: e.target.value })}
              />
            </label>
            <button type="submit">Create…</button>
          </div>
          <p className="muted">
            The account is created locked; the password goes through Users.SetPassword (never in statement text), then it is unlocked.
          </p>
        </form>
      )}
    </div>
  );
}

function Grants() {
  const dbs = useDatabases();
  const [f, setF] = useState({ user: '', host: '%', db: '', table: '', privs: new Set<string>(['SELECT']), withGrant: false });
  const [pending, setPending] = useState<string[] | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const privs = [...f.privs];
  return (
    <div className="stack">
      {msg && <p className={msg.ok ? 'banner' : 'error'}>{msg.text}</p>}
      {pending && (
        <Pending
          sql={pending}
          onDone={(t, ok) => {
            setPending(null);
            if (t) setMsg({ ok, text: t });
          }}
        />
      )}
      <fieldset className="card stack">
        <legend>Grant or revoke</legend>
        <div className="row">
          <label className="row">
            User <input value={f.user} onChange={(e) => setF({ ...f, user: e.target.value })} />
          </label>
          <label className="row">
            Host <input value={f.host} onChange={(e) => setF({ ...f, host: e.target.value })} style={{ inlineSize: 100 }} />
          </label>
          <label className="row">
            On database
            <select value={f.db} onChange={(e) => setF({ ...f, db: e.target.value })}>
              <option value="">*.* (every database)</option>
              {(dbs.data ?? []).map((d) => (
                <option key={d}>{d}</option>
              ))}
            </select>
          </label>
          <label className="row">
            Table <input value={f.table} placeholder="* for all" onChange={(e) => setF({ ...f, table: e.target.value })} />
          </label>
        </div>
        <div className="row" role="group" aria-label="Privileges">
          {PRIVILEGES.map((p) => (
            <label key={p} className="row" style={{ gap: 4 }}>
              <input
                type="checkbox"
                checked={f.privs.has(p)}
                onChange={(e) => {
                  const s = new Set(f.privs);
                  if (e.target.checked) s.add(p);
                  else s.delete(p);
                  setF({ ...f, privs: s });
                }}
              />
              {p}
            </label>
          ))}
        </div>
        <label className="row">
          <input type="checkbox" checked={f.withGrant} onChange={(e) => setF({ ...f, withGrant: e.target.checked })} /> WITH GRANT OPTION
        </label>
        <div className="row">
          <button disabled={!f.user || !privs.length} onClick={() => setPending([grant(privs, f.db, f.table, f.user, f.host, f.withGrant)])}>
            Grant…
          </button>
          <button disabled={!f.user || !privs.length} onClick={() => setPending([revoke(privs, f.db, f.table, f.user, f.host)])}>
            Revoke…
          </button>
        </div>
      </fieldset>
    </div>
  );
}

function Keys() {
  const me = useSession((s) => s.me);
  const qc = useQueryClient();
  const keys = useQuery({ queryKey: ['keys'], queryFn: () => json<{ keys: KeyRow[]; all: boolean }>('/v1/api-keys') });
  const [form, setForm] = useState({ name: '', user: '', host: '%', days: '90' });
  const [created, setCreated] = useState<string | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const canAdmin = me?.privileges.create_user || me?.privileges.tenant_admin;
  const day = (ms: string | null) => (ms ? new Date(Number(ms)).toISOString().slice(0, 10) : '');
  return (
    <div className="stack">
      {msg && <p className={msg.ok ? 'banner' : 'error'}>{msg.text}</p>}
      {created && (
        <div className="card stack" role="alert" data-testid="new-key">
          <strong>Copy the key now: it is shown only once.</strong>
          <code style={{ userSelect: 'all', wordBreak: 'break-all' }}>{created}</code>
          <button onClick={() => setCreated(null)}>I have copied it</button>
        </div>
      )}
      {keys.error && <p className="error">{errorText(keys.error)}</p>}
      <table data-testid="keys-table">
        <thead>
          <tr>
            <th scope="col">Key id</th>
            <th scope="col">Name</th>
            <th scope="col">User</th>
            <th scope="col">Created</th>
            <th scope="col">Expires</th>
            <th scope="col">State</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {(keys.data?.keys ?? []).map((k) => (
            <tr key={k.key_id}>
              <td className="mono">{k.key_id}</td>
              <td>{k.name}</td>
              <td>
                {k.user}@{k.host}
              </td>
              <td>{day(k.created_at_ms)}</td>
              <td>{day(k.expires_at_ms) || 'never'}</td>
              <td>{k.revoked_at_ms ? `revoked ${day(k.revoked_at_ms)}` : 'active'}</td>
              <td>
                {!k.revoked_at_ms && (
                  <button
                    className="danger"
                    onClick={() =>
                      void json(`/v1/api-keys/${encodeURIComponent(k.key_id)}:revoke`, { body: {} })
                        .then(() => qc.invalidateQueries({ queryKey: ['keys'] }))
                        .catch((e: unknown) => setMsg({ ok: false, text: errorText(e) }))
                    }
                  >
                    Revoke
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <form
        className="card row"
        aria-label="New API key"
        onSubmit={(e) => {
          e.preventDefault();
          void json<{ key: string }>('/v1/api-keys', {
            body: {
              name: form.name,
              user: form.user || undefined,
              host: form.host,
              expires_days: Number(form.days) || undefined,
            },
          })
            .then((r) => {
              setCreated(r.key);
              return qc.invalidateQueries({ queryKey: ['keys'] });
            })
            .catch((err: unknown) => setMsg({ ok: false, text: errorText(err) }));
        }}
      >
        <label className="row">
          Name <input required value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
        </label>
        {canAdmin && (
          <label className="row">
            For user <input value={form.user} placeholder={me?.principal.user} onChange={(e) => setForm({ ...form, user: e.target.value })} />
          </label>
        )}
        <label className="row">
          Expires in days <input type="number" min={0} value={form.days} onChange={(e) => setForm({ ...form, days: e.target.value })} style={{ inlineSize: 80 }} />
        </label>
        <button type="submit">Create key</button>
      </form>
    </div>
  );
}

function Tenants() {
  const me = useSession((s) => s.me);
  return (
    <div className="stack">
      <table>
        <thead>
          <tr>
            <th scope="col">Tenant</th>
            <th scope="col">Path</th>
            <th scope="col">You are</th>
          </tr>
        </thead>
        <tbody>
          {(me?.tenants ?? []).map((t) => (
            <tr key={t.id ?? t.name}>
              <td>{t.name}</td>
              <td>{t.path}</td>
              <td>{t.relation === 'home' ? 'a user here' : 'administrator (SPRIG_TENANT_ADMIN above it)'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        Child tenants are created with <code>CREATE TENANT c UNDER b</code> (chapter 29), which the gateway does not run yet; use{' '}
        <code>sprigctl tenant create</code> until then.
      </p>
    </div>
  );
}

/** Users, grants, API keys and the tenant tree (chapter 44 step 13). */
export function AccessPage() {
  const { tenant = '', area = 'users' } = useParams();
  const base = `/t/${encodeURIComponent(tenant)}/access`;
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Users and access</h1>
      <nav className="tabs" aria-label="Access areas">
        <NavLink to={`${base}/users`}>Users</NavLink>
        <NavLink to={`${base}/grants`}>Grants</NavLink>
        <NavLink to={`${base}/keys`}>API keys</NavLink>
        <NavLink to={`${base}/tenants`}>Tenants</NavLink>
      </nav>
      {area === 'users' && <Users />}
      {area === 'grants' && <Grants />}
      {area === 'keys' && <Keys />}
      {area === 'tenants' && <Tenants />}
    </div>
  );
}
