import { useState } from 'react';
import { FormattedMessage } from 'react-intl';
import { useQuery } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText, isApiError } from '../api/errors';
import { keyText, rangeText, shardSummary, type ShardGroup } from './shards';

interface ShardsReply {
  groups: ShardGroup[];
  maps_error: string | null;
  all_databases: boolean;
}

interface AdvisorRun {
  run: string;
  cause: string;
  mode: string;
  status: string;
  verdict: string | null;
  units_sampled: string | null;
  current_score: number | null;
  best_score: number | null;
  runner_up_score: number | null;
  confidence: number | null;
  decision: string | null;
  approved_by: string | null;
  error: string | null;
  created_at: string | null;
  finished_at: string | null;
}

function Group({ g }: { g: ShardGroup }) {
  const s = shardSummary(g);
  return (
    <section className="card stack" aria-labelledby={`g-${g.group}`} data-testid={`group-${g.name}`}>
      <h3 id={`g-${g.group}`}>
        {g.name} <span className="muted">#{g.group}</span>
      </h3>
      <dl className="kv">
        <dt>Kind</dt>
        <dd>{g.kind}</dd>
        <dt>State</dt>
        <dd>{g.state}</dd>
        <dt>Key</dt>
        <dd data-testid="key">{keyText(g)}</dd>
        <dt>Origin</dt>
        <dd>
          {g.origin}
          {g.advisor_run ? ` (advisor run ${g.advisor_run})` : ''}
        </dd>
        <dt>Shards</dt>
        <dd data-testid="shard-count">
          {s.shards} on {s.clusters.join(', ') || '—'}
          {s.held ? `, ${s.held} held by a job` : ''}
          {g.map ? ` · map version ${g.map.version}` : ''}
        </dd>
      </dl>
      <table className="table" aria-label={`Members of ${g.name}`}>
        <thead>
          <tr>
            <th scope="col">Database</th>
            <th scope="col">Table</th>
            <th scope="col">Routing</th>
            <th scope="col">Key columns</th>
            <th scope="col">Parent</th>
          </tr>
        </thead>
        <tbody>
          {g.members.map((m) => (
            <tr key={`${m.db}.${m.table}`}>
              <td>{m.db}</td>
              <td>{m.table}</td>
              <td>{m.routing}</td>
              <td>{Array.isArray(m.key_columns) ? m.key_columns.join(', ') : ''}</td>
              <td>{m.parent ? `${m.parent}${m.parent_fk ? ` (${m.parent_fk})` : ''}` : ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {g.map && g.map.shards.length > 0 && (
        <table className="table" aria-label={`Shard map of ${g.name}`} data-testid="shard-map">
          <thead>
            <tr>
              <th scope="col">Shard</th>
              <th scope="col">Hash range</th>
              <th scope="col">Cluster</th>
              <th scope="col">State</th>
              <th scope="col">Epoch</th>
              <th scope="col">Job</th>
            </tr>
          </thead>
          <tbody>
            {g.map.shards.map((r) => (
              <tr key={r.shard}>
                <td>{r.shard}</td>
                <td className="mono">{rangeText(r)}</td>
                <td>{r.cluster}</td>
                <td>{r.state}</td>
                <td>{r.epoch}</td>
                <td className="mono">{r.job ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

function RunDetail({ run }: { run: string }) {
  const q = useQuery({
    queryKey: ['advisor-run', run],
    queryFn: () => json<Record<string, unknown>>(`/v1/shards/advisor/${encodeURIComponent(run)}`),
  });
  if (q.isLoading) return <p className="muted">Loading run {run}…</p>;
  if (q.error) return <p className="error">{errorText(q.error)}</p>;
  return (
    <section className="stack" aria-label={`Advisor run ${run}`} data-testid="run-detail">
      <h3>Run {run}</h3>
      <pre className="code" tabIndex={0} aria-label={`Run ${run} as JSON`}>
        {JSON.stringify(q.data, null, 2)}
      </pre>
    </section>
  );
}

function Advisor() {
  const [open, setOpen] = useState<string | null>(null);
  const q = useQuery({ queryKey: ['advisor-runs'], queryFn: () => json<{ runs: AdvisorRun[] }>('/v1/shards/advisor') });
  if (q.isLoading) return <p className="muted">Loading advisor runs…</p>;
  if (q.error) {
    if (isApiError(q.error) && q.error.code === 1227) {
      return (
        <p className="muted" data-testid="advisor-denied">
          <FormattedMessage id="shards.advisorDenied" />
        </p>
      );
    }
    return <p className="error">{errorText(q.error)}</p>;
  }
  const runs = q.data?.runs ?? [];
  return (
    <div className="stack">
      {runs.length === 0 ? (
        <p className="muted" data-testid="no-runs">
          <FormattedMessage id="shards.noRuns" />
        </p>
      ) : (
        <table className="table" aria-label="Advisor runs" data-testid="advisor-runs">
          <thead>
            <tr>
              <th scope="col">Run</th>
              <th scope="col">Cause</th>
              <th scope="col">Mode</th>
              <th scope="col">Status</th>
              <th scope="col">Verdict</th>
              <th scope="col">Best score</th>
              <th scope="col">Confidence</th>
              <th scope="col">Started</th>
              <th scope="col">Finished</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.run}>
                <td>
                  <button onClick={() => setOpen(open === r.run ? null : r.run)} aria-expanded={open === r.run}>
                    {r.run}
                  </button>
                </td>
                <td>{r.cause}</td>
                <td>{r.mode}</td>
                <td>{r.status}</td>
                <td>{r.verdict ?? ''}</td>
                <td>{r.best_score ?? ''}</td>
                <td>{r.confidence ?? ''}</td>
                <td>{r.created_at ?? ''}</td>
                <td>{r.finished_at ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {open && <RunDetail run={open} />}
    </div>
  );
}

/** The shards view (chapter 44 step 15; chapters 15 and 16): table groups
 * with their keys and members, each group's shard map, and the sharding
 * advisor's runs. Read-only: splits, merges, moves and applying a run are
 * operator actions. */
export function ShardsPage() {
  const [tab, setTab] = useState<'groups' | 'advisor'>('groups');
  const q = useQuery({ queryKey: ['shards'], queryFn: () => json<ShardsReply>('/v1/shards'), refetchInterval: 15_000 });
  return (
    <div className="stack">
      <h1>
        <FormattedMessage id="shards.title" />
      </h1>
      <div className="row" role="tablist" aria-label="Shards">
        <button role="tab" aria-selected={tab === 'groups'} onClick={() => setTab('groups')}>
          <FormattedMessage id="shards.groups" />
        </button>
        <button role="tab" aria-selected={tab === 'advisor'} onClick={() => setTab('advisor')}>
          <FormattedMessage id="shards.advisor" />
        </button>
      </div>
      {tab === 'advisor' ? (
        <Advisor />
      ) : q.isLoading ? (
        <p className="muted">Loading table groups…</p>
      ) : q.error ? (
        <p className="error">{errorText(q.error)}</p>
      ) : (
        <>
          {q.data?.maps_error && (
            <p className="banner" role="status" data-testid="maps-error">
              Shard maps unavailable: {q.data.maps_error}
            </p>
          )}
          {!q.data?.all_databases && (
            <p className="muted">
              <FormattedMessage id="shards.filtered" />
            </p>
          )}
          {(q.data?.groups ?? []).length === 0 ? (
            <p className="muted" data-testid="no-groups">
              <FormattedMessage id="shards.none" />
            </p>
          ) : (
            (q.data?.groups ?? []).map((g) => <Group key={g.group} g={g} />)
          )}
        </>
      )}
    </div>
  );
}
