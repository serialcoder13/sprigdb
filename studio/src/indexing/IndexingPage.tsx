import { Fingerprints } from './Fingerprints';
import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { useSession } from '../auth/session';
import type { Privileges } from '../api/types';

/** chapter 13's IndexView, as the controller returns it. */
interface IndexView {
  index_id: number;
  db: string;
  table: string;
  name: string;
  model: string;
  kind: string;
  columns: string[];
  ddl?: string | null;
  state: string;
  prior_state?: string | null;
  drop_reason?: string | null;
  est_benefit_ms_h: number;
  est_write_ms_h: number;
  est_net_ms_h: number;
  est_bytes: number;
  actual_bytes?: number | null;
  shards: Record<string, string>;
  fingerprints: { fingerprint?: string; [k: string]: unknown }[];
  observed?: unknown;
  candidate_id: number;
  validation_id?: number | null;
  last_used_at?: string | null;
  created_at: string;
}

interface IndexDetail {
  index: IndexView;
  candidate?: Record<string, unknown> | null;
  validations: Record<string, unknown>[];
  history: { at?: string; event?: string; detail?: string; [k: string]: unknown }[];
}

export function mayIndex(p: Privileges | undefined, db: string): boolean {
  if (!p) return false;
  return p.index_all || p.index_dbs.some((d) => d.toLowerCase() === db.toLowerCase());
}

function q(params: Record<string, string>): string {
  const s = new URLSearchParams(Object.entries(params).filter(([, v]) => v)).toString();
  return s ? `?${s}` : '';
}

const fmt = (n: number) => (Number.isFinite(n) ? n.toFixed(1) : '');

/** Automatic indexing (chapter 44 step 12; chapter 13): indexes by origin
 * and state, the evidence behind each, recommendations to approve, and the
 * explanation of a query fingerprint. Approving needs the privilege to run
 * the CREATE INDEX. */
export function IndexingPage() {
  const me = useSession((s) => s.me);
  const [params, setParams] = useSearchParams();
  const view = params.get('view') ?? 'indexes';
  const db = params.get('db') ?? '';
  const table = params.get('table') ?? '';
  const [showAll, setShowAll] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [fp, setFp] = useState('');
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ['indexing', view, db, table, showAll],
    enabled: view !== 'explain' && view !== 'fingerprints',
    queryFn: () =>
      json<{ indexes: IndexView[] }>(
        view === 'recommendations'
          ? `/v1/indexing/recommendations${q({ db, table })}`
          : `/v1/indexing/indexes${q({ db, table, all: showAll ? 'true' : '' })}`,
      ),
  });
  const detail = useQuery({
    queryKey: ['indexing', 'detail', selected],
    enabled: !!selected,
    queryFn: () => json<IndexDetail>(`/v1/indexing/indexes/${encodeURIComponent(selected ?? '')}`),
  });
  const explain = useQuery({
    queryKey: ['indexing', 'explain', fp],
    enabled: view === 'explain' && fp.length > 0,
    queryFn: () => json<Record<string, unknown>>(`/v1/indexing/queries/${encodeURIComponent(fp)}:explain`),
  });
  const act = async (ix: IndexView, action: string) => {
    const reason = window.prompt(`Why ${action} ${ix.name}? (recorded with the decision)`, '');
    if (reason === null) return;
    setMsg(null);
    try {
      const r = await json<{ message: string; from: string; to: string }>(`/v1/indexing/indexes/${ix.index_id}:${action}`, {
        body: { reason },
      });
      setMsg({ ok: true, text: `${ix.name}: ${r.from} → ${r.to}. ${r.message}` });
      await qc.invalidateQueries({ queryKey: ['indexing'] });
    } catch (e) {
      setMsg({ ok: false, text: errorText(e) });
    }
  };
  const tab = (v: string, label: string) => (
    <button role="tab" aria-selected={view === v} onClick={() => setParams({ view: v, ...(db ? { db } : {}), ...(table ? { table } : {}) })}>
      {label}
    </button>
  );
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Automatic indexing</h1>
      <p className="muted">
        Statements run from Studio are not meant to cause indexes (chapter 13, <code>auto_index.include_studio</code>).
      </p>
      <div className="tabs" role="tablist">
        {tab('indexes', 'Indexes')}
        {tab('recommendations', 'Recommendations')}
        {tab('fingerprints', 'Fingerprints')}
        {tab('explain', 'Explain a query')}
      </div>
      {(db || table) && (
        <p>
          Filtered to {db}
          {table ? `.${table}` : ''} <button onClick={() => setParams({ view })}>Show all</button>
        </p>
      )}
      {msg && (
        <p className={msg.ok ? 'banner' : 'error'} role={msg.ok ? 'status' : 'alert'}>
          {msg.text}
        </p>
      )}
      {view === 'fingerprints' ? (
        <Fingerprints />
      ) : view === 'explain' ? (
        <div className="stack">
          <label className="row">
            Fingerprint
            <input value={fp} onChange={(e) => setFp(e.target.value.trim())} placeholder="hex fingerprint" className="mono" />
          </label>
          {explain.error && <p className="error">{errorText(explain.error)}</p>}
          {explain.data && (
            <div className="stack">
              {Array.isArray(explain.data.summary) && (
                <ul>
                  {(explain.data.summary as string[]).map((s, i) => (
                    <li key={i}>{s}</li>
                  ))}
                </ul>
              )}
              <details>
                <summary>Candidates and policies</summary>
                <pre>{JSON.stringify(explain.data, null, 2)}</pre>
              </details>
            </div>
          )}
        </div>
      ) : (
        <div className="stack">
          {view === 'indexes' && (
            <label className="row">
              <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} /> Include dropped and rejected
              (history)
            </label>
          )}
          {list.isLoading && <p className="muted">Loading…</p>}
          {list.error && <p className="error">{errorText(list.error)}</p>}
          {list.data && list.data.indexes.length === 0 && <p className="muted">None.</p>}
          {list.data && list.data.indexes.length > 0 && (
            <table data-testid="indexes-table">
              <thead>
                <tr>
                  <th scope="col">Index</th>
                  <th scope="col">Object</th>
                  <th scope="col">Columns</th>
                  <th scope="col">State</th>
                  <th scope="col">Net ms/h</th>
                  <th scope="col">Size</th>
                  <th scope="col">Actions</th>
                </tr>
              </thead>
              <tbody>
                {list.data.indexes.map((ix) => {
                  const can = mayIndex(me?.privileges, ix.db);
                  return (
                    <tr key={ix.index_id}>
                      <td>
                        <button onClick={() => setSelected(String(ix.index_id))} className="mono">
                          {ix.name}
                        </button>
                      </td>
                      <td>
                        {ix.db}.{ix.table}
                      </td>
                      <td className="mono">{ix.columns.join(', ')}</td>
                      <td>
                        <span className="badge">{ix.state}</span>
                      </td>
                      <td>{fmt(ix.est_net_ms_h)}</td>
                      <td>{ix.actual_bytes ?? ix.est_bytes}</td>
                      <td className="row">
                        {can && ix.state === 'recommended' && (
                          <>
                            <button className="primary" onClick={() => void act(ix, 'approve')}>
                              Approve
                            </button>
                            <button onClick={() => void act(ix, 'reject')}>Dismiss</button>
                          </>
                        )}
                        {can && ix.state !== 'pinned' && ix.state !== 'recommended' && ix.state !== 'dropped' && (
                          <button onClick={() => void act(ix, 'pin')}>Freeze</button>
                        )}
                        {can && ix.state === 'pinned' && <button onClick={() => void act(ix, 'unpin')}>Unfreeze</button>}
                        {!can && <span className="muted">view only</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      )}
      {selected && (
        <section className="card stack" aria-label="Evidence">
          <div className="row">
            <h2 style={{ margin: 0, flex: 1 }}>Evidence: {detail.data?.index.name ?? selected}</h2>
            <button onClick={() => setSelected(null)}>Close</button>
          </div>
          {detail.error && <p className="error">{errorText(detail.error)}</p>}
          {detail.data && (
            <>
              {detail.data.index.ddl && <pre>{detail.data.index.ddl}</pre>}
              <p>
                Benefit {fmt(detail.data.index.est_benefit_ms_h)} ms/h, write cost {fmt(detail.data.index.est_write_ms_h)} ms/h, net{' '}
                {fmt(detail.data.index.est_net_ms_h)} ms/h.
              </p>
              <h3>Rollout per shard</h3>
              <ul>
                {Object.entries(detail.data.index.shards).map(([s, st]) => (
                  <li key={s}>
                    {s}: {st}
                  </li>
                ))}
              </ul>
              <h3>Target fingerprints (before and after)</h3>
              <pre>{JSON.stringify(detail.data.index.fingerprints, null, 2)}</pre>
              {detail.data.index.observed != null && <pre>{JSON.stringify(detail.data.index.observed, null, 2)}</pre>}
              <h3>Validations</h3>
              <pre>{JSON.stringify(detail.data.validations, null, 2)}</pre>
              <h3>History</h3>
              <ol>
                {detail.data.history.map((h, i) => (
                  <li key={i}>
                    {String(h.at ?? '')} {String(h.event ?? '')} {String(h.detail ?? '')}
                  </li>
                ))}
              </ol>
            </>
          )}
        </section>
      )}
    </div>
  );
}
