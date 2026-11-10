import { useState } from 'react';
import { useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';

/** Chapter 52's recommendation, as the controller returns it. */
export interface Recommendation {
  id: string;
  kind: string;
  level: string;
  advisory: boolean;
  subject: { tenant_name?: string; db_name?: string; object_name?: string; dc?: string; cluster?: string };
  target: string;
  state: string;
  summary: string;
  action: { text: string; machine?: unknown };
  evidence: Record<string, unknown>;
  score: number;
  mode_source: string;
  notify_level: string;
  escalations: number;
  escalate_at_us?: number | null;
  auto_apply_at_us?: number | null;
  window?: { start_us: number; end_us: number } | null;
  decided_by?: string | null;
  reason?: string | null;
  result?: string | null;
  refreshes: number;
  created_at_us: number;
  history: { at_us: number; from: string; to: string; by: string; note?: string }[];
  comments: { at_us: number; author: string; body: string }[];
  age_hours: number;
  may_act: boolean;
}

interface ListResponse {
  items: Recommendation[];
  open: number;
}

const KINDS = [
  'index',
  'vector',
  'shard',
  'advisor',
  'balancer',
  'dc',
  'standby',
  'failover',
  'graph',
];
const STATES = ['open', 'approved', 'applying', 'applied', 'failed', 'ignored', 'snoozed', 'superseded', 'expired'];
const LEVELS = ['table', 'database', 'tenant', 'dc', 'platform'];

export const when = (us?: number | null) => (us ? new Date(us / 1000).toISOString().replace('.000Z', 'Z') : '—');

function target(r: Recommendation): string {
  const s = r.subject;
  if (s.db_name && s.object_name) return `${s.db_name}.${s.object_name}`;
  if (s.db_name) return s.db_name;
  return s.tenant_name ?? s.cluster ?? r.target;
}

/** The inbox's unread count, for the navigation badge. */
export function useOpenCount(): number {
  const q = useQuery({
    queryKey: ['approvals', 'badge'],
    queryFn: () => json<ListResponse>('/v1/recommendations?state=open&limit=200'),
    refetchInterval: 60_000,
    retry: false,
  });
  return q.data?.open ?? 0;
}

/** Recommendations held for approval (chapter 52): filters, the item with
 * its evidence and history, and approve, approve in a window, ignore,
 * snooze, comment and bulk actions. What a user sees and may act on is
 * decided by the controller from their grants. */
export function InboxPage() {
  const [params, setParams] = useSearchParams();
  const kind = params.get('kind') ?? '';
  const state = params.get('state') ?? '';
  const level = params.get('level') ?? '';
  const all = params.get('all') === 'true';
  const [selected, setSelected] = useState<string | null>(params.get('id'));
  const [checked, setChecked] = useState<Set<string>>(new Set());
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const qc = useQueryClient();
  const qs = new URLSearchParams(
    Object.entries({ kind, state, level, all: all ? 'true' : '' }).filter(([, v]) => v),
  ).toString();
  const list = useQuery({
    queryKey: ['approvals', 'list', qs],
    queryFn: () => json<ListResponse>(`/v1/recommendations${qs ? `?${qs}` : ''}`),
  });
  const detail = useQuery({
    queryKey: ['approvals', 'item', selected],
    enabled: !!selected,
    queryFn: () => json<Recommendation>(`/v1/recommendations/${encodeURIComponent(selected ?? '')}`),
  });
  const set = (k: string, v: string) => {
    const p = new URLSearchParams(params);
    if (v) p.set(k, v);
    else p.delete(k);
    setParams(p);
  };
  const refresh = () => qc.invalidateQueries({ queryKey: ['approvals'] });
  const act = async (ids: string[], action: string, body: Record<string, unknown>) => {
    setMsg(null);
    try {
      if (ids.length === 1) {
        const r = await json<Recommendation>(`/v1/recommendations/${encodeURIComponent(ids[0] ?? '')}:${action}`, { body });
        setMsg({ ok: true, text: `${r.id} is ${r.state}` });
      } else {
        const r = await json<{ id: string; state?: string; error?: string }[]>('/v1/recommendations:bulk', {
          body: { ids, action, ...body },
        });
        const bad = r.filter((x) => x.error);
        setMsg({
          ok: bad.length === 0,
          text: `${r.length - bad.length} of ${r.length} done${bad.length ? `; ${bad.map((b) => `${b.id}: ${b.error}`).join('; ')}` : ''}`,
        });
        setChecked(new Set());
      }
      await refresh();
    } catch (e) {
      setMsg({ ok: false, text: errorText(e) });
    }
  };
  const ask = (q: string) => window.prompt(q, '');
  const approve = (ids: string[]) => {
    const reason = ask('Why approve? (recorded with the decision)');
    if (reason !== null) void act(ids, 'approve', { reason });
  };
  const approveInWindow = (ids: string[]) => {
    const start = ask('Apply from (UTC, e.g. 2026-11-11T20:00:00Z)');
    if (!start) return;
    const end = ask('Apply until (UTC)');
    if (!end) return;
    const s = Date.parse(start);
    const e = Date.parse(end);
    if (Number.isNaN(s) || Number.isNaN(e)) {
      setMsg({ ok: false, text: 'Times must be like 2026-11-11T20:00:00Z' });
      return;
    }
    void act(ids, 'approve', { reason: 'approved in a window', window: { start_us: s * 1000, end_us: e * 1000 } });
  };
  const ignore = (ids: string[]) => {
    const reason = ask('Why ignore?');
    if (reason === null) return;
    const how = ask('Ignore for how many days? (empty: once; "forever": forever)');
    if (how === null) return;
    const body: Record<string, unknown> = { reason };
    if (how.trim().toLowerCase() === 'forever') body.forever = true;
    else if (how.trim()) body.for_days = Number(how.trim());
    void act(ids, 'ignore', body);
  };
  const snooze = (ids: string[]) => {
    const h = ask('Snooze for how many hours?');
    if (h) void act(ids, 'snooze', { hours: Number(h) });
  };
  const comment = (id: string) => {
    const text = ask('Comment');
    if (text) void act([id], 'comment', { text });
  };
  const items = list.data?.items ?? [];
  const actable = items.filter((i) => i.may_act);
  const sel = [...checked];
  const select = (label: string, k: string, v: string, options: string[]) => (
    <label className="row">
      {label}
      <select aria-label={label} value={v} onChange={(e) => set(k, e.target.value)}>
        <option value="">all</option>
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </label>
  );
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Approvals</h1>
      <p className="muted">
        Automatic decisions held for approval or recorded as advice (<code>approve</code> and <code>recommend</code>{' '}
        modes). Approved changes run through the same machinery as automatic ones.
      </p>
      <div className="row" role="group" aria-label="Filters">
        {select('Kind', 'kind', kind, KINDS)}
        {select('State', 'state', state, STATES)}
        {select('Level', 'level', level, LEVELS)}
        <label className="row">
          <input type="checkbox" checked={all} onChange={(e) => set('all', e.target.checked ? 'true' : '')} /> Closed too
        </label>
      </div>
      {msg && (
        <p className={msg.ok ? 'banner' : 'error'} role={msg.ok ? 'status' : 'alert'}>
          {msg.text}
        </p>
      )}
      {list.isError && (
        <p className="error" role="alert">
          {errorText(list.error)}
        </p>
      )}
      {sel.length > 0 && (
        <div className="row" role="group" aria-label="Bulk actions">
          <span>{sel.length} selected</span>
          <button className="primary" onClick={() => approve(sel)}>
            Approve selected
          </button>
          <button onClick={() => ignore(sel)}>Ignore selected</button>
          <button onClick={() => snooze(sel)}>Snooze selected</button>
        </div>
      )}
      <table data-testid="inbox-table">
        <thead>
          <tr>
            <th>
              <input
                type="checkbox"
                aria-label="Select all"
                checked={actable.length > 0 && actable.every((i) => checked.has(i.id))}
                onChange={(e) => setChecked(new Set(e.target.checked ? actable.map((i) => i.id) : []))}
              />
            </th>
            <th>Id</th>
            <th>Kind</th>
            <th>Level</th>
            <th>State</th>
            <th>Target</th>
            <th>Age (h)</th>
            <th>Summary</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.id} aria-selected={selected === r.id}>
              <td>
                <input
                  type="checkbox"
                  aria-label={`Select ${r.id}`}
                  disabled={!r.may_act}
                  checked={checked.has(r.id)}
                  onChange={(e) => {
                    const n = new Set(checked);
                    if (e.target.checked) n.add(r.id);
                    else n.delete(r.id);
                    setChecked(n);
                  }}
                />
              </td>
              <td>
                <button className="link mono" onClick={() => setSelected(r.id)}>
                  {r.id}
                </button>
              </td>
              <td>{r.kind}</td>
              <td>{r.level}</td>
              <td>
                <span className="badge">{r.state}</span>
                {r.advisory ? ' advice' : ''}
              </td>
              <td>{target(r)}</td>
              <td>{r.age_hours.toFixed(1)}</td>
              <td>{r.summary}</td>
            </tr>
          ))}
          {items.length === 0 && !list.isLoading && (
            <tr>
              <td colSpan={8} className="muted">
                Nothing waiting.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      {selected && detail.data && (
        <section className="card stack" aria-label={`Recommendation ${detail.data.id}`} data-testid="inbox-item">
          <h2 style={{ margin: 0 }}>
            {detail.data.id} · {detail.data.kind} · {detail.data.state}
          </h2>
          <p>{detail.data.summary}</p>
          <pre className="mono">{detail.data.action.text}</pre>
          <dl>
            <dt>Level</dt>
            <dd>
              {detail.data.level} (mode from {detail.data.mode_source}); notifies {detail.data.notify_level}
            </dd>
            <dt>Raised</dt>
            <dd>
              {when(detail.data.created_at_us)}, proposed again {detail.data.refreshes} times
            </dd>
            <dt>Escalates</dt>
            <dd>{when(detail.data.escalate_at_us)}</dd>
            <dt>Auto-applies</dt>
            <dd>{when(detail.data.auto_apply_at_us)}</dd>
            {detail.data.result && (
              <>
                <dt>Result</dt>
                <dd>{detail.data.result}</dd>
              </>
            )}
          </dl>
          <h3>Evidence</h3>
          <table>
            <tbody>
              {Object.entries(detail.data.evidence ?? {}).map(([k, v]) => (
                <tr key={k}>
                  <th scope="row">{k}</th>
                  <td className="mono">{typeof v === 'string' ? v : JSON.stringify(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {detail.data.may_act && (
            <div className="row" role="group" aria-label="Actions">
              <button className="primary" onClick={() => approve([detail.data.id])}>
                Approve
              </button>
              <button onClick={() => approveInWindow([detail.data.id])}>Approve in a window</button>
              <button onClick={() => ignore([detail.data.id])}>Ignore</button>
              <button onClick={() => snooze([detail.data.id])}>Snooze</button>
              <button onClick={() => comment(detail.data.id)}>Comment</button>
            </div>
          )}
          <h3>History</h3>
          <ul>
            {detail.data.history.map((h, i) => (
              <li key={i}>
                {when(h.at_us)} {h.from} → {h.to} by {h.by} {h.note ?? ''}
              </li>
            ))}
          </ul>
          {detail.data.comments.length > 0 && (
            <>
              <h3>Comments</h3>
              <ul>
                {detail.data.comments.map((c, i) => (
                  <li key={i}>
                    {when(c.at_us)} {c.author}: {c.body}
                  </li>
                ))}
              </ul>
            </>
          )}
        </section>
      )}
    </div>
  );
}
