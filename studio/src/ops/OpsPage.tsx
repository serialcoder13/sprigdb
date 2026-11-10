import { useState, type ReactNode } from 'react';
import { NavLink, Navigate, useParams } from 'react-router';
import { FormattedMessage } from 'react-intl';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import { confirmTyped, fmtMs, moveLag, type Dc, type Job, type MoverJob, type OpsCluster, type OpsTenant, type Placement } from './ops';

const AREAS = ['dcs', 'clusters', 'tenants', 'moves', 'jobs', 'failovers', 'txns'] as const;
type Area = (typeof AREAS)[number];

function ops<T>(path: string, init?: { body: unknown }): Promise<T> {
  return json<T>(`/v1/ops/${path}`, init ? { body: init.body } : {});
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
  const entered = useQuery({ queryKey: ['ops', 'enter'], queryFn: () => ops<{ operator: string }>(''), staleTime: Infinity });
  if (me && !me.platform_roles.includes('operator')) return <Navigate to="/" replace />;
  if (!AREAS.includes(area as Area)) return <Navigate to="/ops/clusters" replace />;
  return (
    <div className="shell">
      <header className="topbar">
        <span className="brand">SprigDB Studio · operator mode</span>
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
          {me?.principal.login}
        </span>
      </header>
      <main className="main" id="content" tabIndex={-1}>
        <div className="stack">
          <h1>
            <FormattedMessage id={`ops.${area}`} />
          </h1>
          {entered.error ? (
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
