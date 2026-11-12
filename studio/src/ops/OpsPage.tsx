import { useState, type ReactNode } from 'react';
import { Mascot } from '../app/Mascot';
import { NavLink, Navigate, useParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import { RecoveryCodes } from '../auth/Factors';
import { QrCode } from '../auth/QrCode';
import {
  confirmTyped,
  fmtMs,
  legacyOperatorMode,
  loginBody,
  moveLag,
  operatorHeaders,
  operatorSession,
  secretOf,
  sessionOf,
  setOperatorSession,
  type Dc,
  type Job,
  type MoverJob,
  type OperatorLogin,
  type OpsCluster,
  type OpsTenant,
  type Placement,
} from './ops';

const AREAS = ['dcs', 'clusters', 'tenants', 'moves', 'jobs', 'failovers', 'txns'] as const;
type Area = (typeof AREAS)[number];

function ops<T>(path: string, init?: { body: unknown }): Promise<T> {
  const headers = operatorHeaders();
  return json<T>(`/v1/ops/${path}`, init ? { body: init.body, headers } : { headers });
}

/** Signs a named operator in (chapter 14): password, then a TOTP code or
 * a recovery code in the same field; the first sign-in enrols the
 * authenticator (QR code, secret and link) and shows the recovery codes
 * once. */
function OperatorSignIn({ onSignedIn }: { onSignedIn: () => void }) {
  const intl = useIntl();
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [code, setCode] = useState('');
  const [needCode, setNeedCode] = useState(false);
  const [enrol, setEnrol] = useState<{ ticket: string; uri: string } | null>(null);
  const [codes, setCodes] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const done = (r: OperatorLogin) => {
    const s = sessionOf(r);
    if (!s) return;
    setOperatorSession(s);
    if (r.recovery_codes?.length) setCodes(r.recovery_codes);
    else onSignedIn();
  };
  const submit = async () => {
    setError(null);
    setBusy(true);
    try {
      if (enrol) {
        done(await json<OperatorLogin>('/v1/ops:enrol', { body: { name: name.trim(), ticket: enrol.ticket, code: code.trim() }, auth: false }));
        return;
      }
      const r = await json<OperatorLogin>('/v1/ops:login', { body: loginBody(name, password, needCode ? code : ''), auth: false });
      if (r.enrol_ticket) {
        setEnrol({ ticket: r.enrol_ticket, uri: r.otpauth_uri ?? '' });
        setCode('');
        return;
      }
      done(r);
    } catch (e: unknown) {
      if ((e as { code?: number }).code === 39311 && !needCode) setNeedCode(true);
      else setError(errorText(e));
      setCode('');
    } finally {
      setBusy(false);
    }
  };
  if (codes.length) {
    return <RecoveryCodes codes={codes} onDone={onSignedIn} />;
  }
  return (
    <form
      className="stack"
      aria-label="Operator sign-in"
      data-testid="ops-signin"
      style={{ maxWidth: '36rem' }}
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <p className="muted">Sign in as a platform or DC operator (your operator account, not a tenant login).</p>
      {!enrol && (
        <>
          <label className="stack">
            <span>Operator</span>
            <input value={name} required autoComplete="username" readOnly={needCode} onChange={(e) => setName(e.target.value)} data-testid="ops-name" />
          </label>
          <label className="stack">
            <span>Password</span>
            <input
              type="password"
              value={password}
              required
              autoComplete="current-password"
              readOnly={needCode}
              onChange={(e) => setPassword(e.target.value)}
              data-testid="ops-password"
            />
          </label>
        </>
      )}
      {enrol && (
        <>
          <h2>
            <FormattedMessage id="mfa.enrolTitle" />
          </h2>
          <p>
            <FormattedMessage id="mfa.enrolHint" />
          </p>
          <QrCode text={enrol.uri} label={intl.formatMessage({ id: 'mfa.qrLabel' })} />
          <dl className="kv">
            <dt>Secret</dt>
            <dd className="mono" data-testid="ops-secret">
              {secretOf(enrol.uri)}
            </dd>
            <dt>Link</dt>
            <dd className="mono" style={{ wordBreak: 'break-all' }} data-testid="ops-otpauth">
              {enrol.uri}
            </dd>
          </dl>
        </>
      )}
      {(needCode || enrol) && (
        <div className="stack">
          <label className="stack">
            <span>
              <FormattedMessage id="mfa.code" />
            </span>
            <input
              value={code}
              required
              name="one-time-code"
              autoComplete="one-time-code"
              aria-describedby={enrol ? undefined : 'ops-code-hint'}
              onChange={(e) => setCode(e.target.value)}
              data-testid="ops-code"
            />
          </label>
          {!enrol && (
            <span className="muted" id="ops-code-hint">
              <FormattedMessage id="mfa.hint" />
            </span>
          )}
        </div>
      )}
      <div className="row">
        <button className="primary" type="submit" disabled={busy} data-testid="ops-signin-submit">
          {enrol ? <FormattedMessage id="mfa.confirm" /> : 'Sign in'}
        </button>
        {(needCode || enrol) && (
          <button
            type="button"
            onClick={() => {
              setEnrol(null);
              setNeedCode(false);
              setCode('');
              setError(null);
            }}
          >
            <FormattedMessage id="mfa.restart" />
          </button>
        )}
      </div>
      {error && (
        <p className="error" role="alert" data-testid="ops-signin-error">
          {error}
        </p>
      )}
    </form>
  );
}

function Loading({ q, children }: { q: { isLoading: boolean; error: unknown }; children: ReactNode }) {
  if (q.isLoading) return <p className="muted">Loading…</p>;
  if (q.error) return <p className="error">{errorText(q.error)}</p>;
  return <>{children}</>;
}

function Dcs() {
  const dcs = useQuery({ queryKey: ['ops', 'dcs'], queryFn: () => ops<{ dcs: Dc[] }>('dcs') });
  const lat = useQuery({ queryKey: ['ops', 'lat'], queryFn: () => ops<{ rows: Record<string, unknown>[] }>('dcs:latency') });
  return (
    <Loading q={dcs}>
      <table className="table" aria-label="Data centers" data-testid="ops-dcs">
        <thead>
          <tr>
            <th scope="col">DC</th>
            <th scope="col">Kind</th>
            <th scope="col">State</th>
            <th scope="col">Epoch</th>
            <th scope="col">Global voter</th>
          </tr>
        </thead>
        <tbody>
          {(dcs.data?.dcs ?? []).map((d) => (
            <tr key={d.dc_id}>
              <td>{d.dc_id}</td>
              <td>{d.kind}</td>
              <td>{d.state}</td>
              <td>{d.dc_epoch}</td>
              <td>{d.global_voter ? 'yes' : 'no'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <h3>Latency</h3>
      {(lat.data?.rows ?? []).length === 0 ? (
        <p className="muted">No inter-DC latency measured (one DC).</p>
      ) : (
        <pre className="code" tabIndex={0}>
          {JSON.stringify(lat.data?.rows, null, 2)}
        </pre>
      )}
    </Loading>
  );
}

function Clusters() {
  const q = useQuery({ queryKey: ['ops', 'clusters'], queryFn: () => ops<{ clusters: OpsCluster[] }>('clusters') });
  return (
    <Loading q={q}>
      <table className="table" aria-label="Clusters" data-testid="ops-clusters">
        <thead>
          <tr>
            <th scope="col">Cluster</th>
            <th scope="col">Name</th>
            <th scope="col">DC</th>
            <th scope="col">Topology</th>
            <th scope="col">State</th>
            <th scope="col">Nodes</th>
            <th scope="col">Server</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.clusters ?? []).map((c) => (
            <tr key={c.cluster_id}>
              <td>{c.cluster_id}</td>
              <td>{c.name}</td>
              <td>{c.dc_id}</td>
              <td>{c.topology}</td>
              <td>{c.admin_state}</td>
              <td className="mono">{(c.nodes ?? []).map((n) => n.addr).join(', ')}</td>
              <td>{c.profile?.server?.version_text ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Loading>
  );
}

function Action({ label, target, run }: { label: string; target: string; run: (reason: string) => Promise<unknown> }) {
  const [msg, setMsg] = useState<string | null>(null);
  return (
    <>
      <button
        onClick={() => {
          const reason = confirmTyped(target, label);
          if (!reason) return;
          run(reason).then(
            () => setMsg('Done.'),
            (e: unknown) => setMsg(errorText(e)),
          );
        }}
      >
        {label}
      </button>
      {msg && (
        <span className="muted" role="status">
          {' '}
          {msg}
        </span>
      )}
    </>
  );
}

function Tenants() {
  const [open, setOpen] = useState<string | null>(null);
  const q = useQuery({ queryKey: ['ops', 'tenants'], queryFn: () => ops<{ tenants: OpsTenant[] }>('tenants') });
  const p = useQuery({
    queryKey: ['ops', 'placement', open],
    enabled: open !== null,
    queryFn: () => ops<Placement>(`tenants/${encodeURIComponent(open ?? '')}/placement`),
  });
  return (
    <Loading q={q}>
      <table className="table" aria-label="Tenants" data-testid="ops-tenants">
        <thead>
          <tr>
            <th scope="col">Tenant</th>
            <th scope="col">Id</th>
            <th scope="col">State</th>
            <th scope="col">Home DC</th>
            <th scope="col">Children</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.tenants ?? []).map((t) => (
            <tr key={t.tenant_id}>
              <td>
                <button aria-expanded={open === t.name} onClick={() => setOpen(open === t.name ? null : t.name)}>
                  {t.name}
                </button>
              </td>
              <td className="mono">{t.tenant_id}</td>
              <td>{t.state}</td>
              <td>{t.home_dc}</td>
              <td>{t.child_count}</td>
              <td>
                <Action
                  label="Run the sharding advisor"
                  target={t.name}
                  run={(reason) => ops(`tenants/${encodeURIComponent(t.name)}/advisor:run`, { body: { reason } })}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {open && (
        <section className="card stack" aria-label={`Placement of ${open}`} data-testid="ops-placement">
          <h3>Placement of {open}</h3>
          <Loading q={p}>
            <p>Mode: {p.data?.mode}</p>
            <table className="table" aria-label="Shards and their clusters">
              <thead>
                <tr>
                  <th scope="col">Shard</th>
                  <th scope="col">Cluster</th>
                  <th scope="col">Size (MB)</th>
                  <th scope="col">Busy ms/s</th>
                  <th scope="col">Pinned</th>
                </tr>
              </thead>
              <tbody>
                {(p.data?.shards ?? []).map((s) => (
                  <tr key={s.shard}>
                    <td className="mono">{s.shard}</td>
                    <td>{s.cluster}</td>
                    <td>{(s.size_bytes / 1e6).toFixed(1)}</td>
                    <td>{s.busy_ms_per_sec.toFixed(2)}</td>
                    <td>{s.pinned ? 'yes' : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <table className="table" aria-label="Clusters it could use">
              <thead>
                <tr>
                  <th scope="col">Cluster</th>
                  <th scope="col">State</th>
                  <th scope="col">Full</th>
                  <th scope="col">Bottleneck</th>
                  <th scope="col">Holds the tenant</th>
                </tr>
              </thead>
              <tbody>
                {(p.data?.clusters ?? []).map((c) => (
                  <tr key={c.cluster}>
                    <td>{c.cluster}</td>
                    <td>{c.state}</td>
                    <td>{(c.full * 100).toFixed(2)}%</td>
                    <td>{c.bottleneck}</td>
                    <td>{c.holds_tenant ? 'yes' : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Loading>
        </section>
      )}
    </Loading>
  );
}

function Moves() {
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ['ops', 'moves'],
    queryFn: () => ops<{ jobs: MoverJob[] }>('mover/jobs'),
    refetchInterval: 5_000,
  });
  return (
    <Loading q={q}>
      <table className="table" aria-label="Moves and splits" data-testid="ops-moves">
        <thead>
          <tr>
            <th scope="col">Job</th>
            <th scope="col">Kind</th>
            <th scope="col">Tenant</th>
            <th scope="col">State</th>
            <th scope="col">From → to</th>
            <th scope="col">Rows copied</th>
            <th scope="col">Catch-up lag</th>
            <th scope="col">Cutover holds</th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.jobs ?? []).map((j) => (
            <tr key={j.id}>
              <td className="mono">{j.id.slice(0, 13)}…</td>
              <td>{j.kind}</td>
              <td className="mono">{j.tenant}</td>
              <td>{j.state}</td>
              <td>
                {j.source} → {j.target}
              </td>
              <td>{j.progress?.rows_copied ?? ''}</td>
              <td>{moveLag(j)}</td>
              <td>{(j.progress?.holds_ms ?? []).map((h) => `${h} ms`).join(', ')}</td>
              <td>
                {!['done', 'retaining', 'cancelled', 'failed'].includes(j.state) && (
                  <Action
                    label="Cancel"
                    target={j.id}
                    run={(reason) =>
                      ops(`mover/jobs/${j.id}:cancel`, { body: { reason } }).then(() => qc.invalidateQueries({ queryKey: ['ops', 'moves'] }))
                    }
                  />
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Loading>
  );
}

function Raw({ path, label }: { path: string; label: string }) {
  const q = useQuery({ queryKey: ['ops', path], queryFn: () => ops<unknown>(path), refetchInterval: 10_000 });
  return (
    <Loading q={q}>
      <pre className="code" tabIndex={0} aria-label={label} data-testid={`ops-raw-${path}`}>
        {JSON.stringify(q.data, null, 2)}
      </pre>
    </Loading>
  );
}

function Jobs() {
  const q = useQuery({ queryKey: ['ops', 'jobs'], queryFn: () => ops<{ jobs: Job[] }>('jobs'), refetchInterval: 10_000 });
  return (
    <Loading q={q}>
      <table className="table" aria-label="Jobs" data-testid="ops-jobs">
        <thead>
          <tr>
            <th scope="col">Job</th>
            <th scope="col">Kind</th>
            <th scope="col">State</th>
            <th scope="col">Owner</th>
            <th scope="col">Attempts</th>
            <th scope="col">Started</th>
            <th scope="col">Error</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.jobs ?? []).map((j) => (
            <tr key={j.job_id}>
              <td className="mono">{j.job_id.slice(0, 13)}…</td>
              <td>{j.kind}</td>
              <td>{j.state}</td>
              <td>{j.owner?.worker_id ?? ''}</td>
              <td>{j.attempts}</td>
              <td>{fmtMs(j.started_ms)}</td>
              <td>{j.error ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Loading>
  );
}

/** Operator mode (chapter 44 step 17): data centers, clusters, tenants
 * with placement, moves and splits with catch-up lag and cutover holds,
 * jobs, failovers, in-doubt transactions. Shown only to platform
 * operators; destructive actions ask for the target's name and a reason. */
export function OpsPage() {
  const { area = 'clusters' } = useParams();
  const me = useSession((s) => s.me);
  const qc = useQueryClient();
  const [op, setOp] = useState(() => operatorSession());
  const entered = useQuery({
    queryKey: ['ops', 'enter', op?.token ?? ''],
    queryFn: () => ops<{ operator?: string; principal?: { name: string } }>(''),
    staleTime: Infinity,
    retry: false,
  });
  if (!AREAS.includes(area as Area)) return <Navigate to="/ops/clusters" replace />;
  const legacy = me?.platform_roles.includes('operator') ?? false;
  const signIn = !op && (!legacy || entered.isError);
  const signOut = async () => {
    try {
      await json('/v1/ops:logout', { body: {}, headers: operatorHeaders(op), auth: false });
    } catch {
      // The session may already have ended.
    }
    setOperatorSession(null);
    setOp(null);
    void qc.invalidateQueries({ queryKey: ['ops'] });
  };
  return (
    <div className="shell">
      <header className="topbar">
        <span className="brand">
          <Mascot pose="avatar" size={26} />
          SprigDB Studio · operator mode
        </span>
        <nav aria-label="Operator">
          {AREAS.map((a) => (
            <NavLink key={a} to={`/ops/${a}`}>
              <FormattedMessage id={`ops.${a}`} />
            </NavLink>
          ))}
        </nav>
        <NavLink to={`/t/${encodeURIComponent(me?.principal.tenant ?? '-')}`}>
          <FormattedMessage id="ops.leave" />
        </NavLink>
        <span className="muted" data-testid="whoami">
          {op ? `operator ${op.name}` : me?.principal.login}
        </span>
        {op && (
          <button onClick={() => void signOut()} data-testid="ops-signout">
            Sign out
          </button>
        )}
      </header>
      <main className="main" id="content" tabIndex={-1}>
        <div className="stack">
          <h1>
            <FormattedMessage id={`ops.${area}`} />
          </h1>
          {!signIn && legacyOperatorMode(me?.platform_roles, op) && (
            <p className="banner" role="status" data-testid="ops-legacy-banner">
              <FormattedMessage id="ops.legacyDeprecated" />
            </p>
          )}
          {signIn ? (
            <OperatorSignIn
              onSignedIn={() => {
                setOp(operatorSession());
                void qc.invalidateQueries({ queryKey: ['ops'] });
              }}
            />
          ) : entered.error ? (
            <p className="error" role="alert" data-testid="ops-refused">
              {errorText(entered.error)}
            </p>
          ) : (
            <>
              {area === 'dcs' && <Dcs />}
              {area === 'clusters' && <Clusters />}
              {area === 'tenants' && <Tenants />}
              {area === 'moves' && <Moves />}
              {area === 'jobs' && <Jobs />}
              {area === 'failovers' && <Raw path="failovers" label="Failovers" />}
              {area === 'txns' && <Raw path="txns" label="In-doubt transactions" />}
            </>
          )}
        </div>
      </main>
    </div>
  );
}
