import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';

interface KindPolicy {
  kind: string;
  mode_key: string;
  mode: string;
  mode_source: string;
  level: string;
  escalate_after?: string | null;
  auto_apply_after?: string | null;
}

interface PolicyResponse {
  kinds: KindPolicy[];
  settings: Record<string, [unknown, string]>;
}

interface Subscription {
  user: string;
  email: string;
  delivery: string;
  kinds: string;
}

/** Approval policy at one level (chapter 52): every kind's mode, the level
 * a new item goes to and its timers, resolved at the tenant, a database or
 * a table, plus the user's own email subscription. Values are changed
 * with SQL (`ALTER ... SPRIG SET`) or on the Settings page; operators use
 * `sprigctl recommendations policy` and `sprigctl config set`. */
export function PolicyPage() {
  const [db, setDb] = useState('');
  const [object, setObject] = useState('');
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const qc = useQueryClient();
  const qs = new URLSearchParams(Object.entries({ db, object }).filter(([, v]) => v)).toString();
  const policy = useQuery({
    queryKey: ['approvals', 'policy', qs],
    queryFn: () => json<PolicyResponse>(`/v1/recommendations:policy${qs ? `?${qs}` : ''}`),
  });
  const subs = useQuery({
    queryKey: ['approvals', 'subs'],
    queryFn: () => json<Subscription[]>('/v1/recommendations:subscriptions'),
  });
  const [email, setEmail] = useState('');
  const [delivery, setDelivery] = useState('immediate');
  const save = async () => {
    setMsg(null);
    try {
      await json('/v1/recommendations:subscriptions', { body: { email, delivery, kinds: '*' } });
      setMsg({ ok: true, text: 'Saved' });
      await qc.invalidateQueries({ queryKey: ['approvals', 'subs'] });
    } catch (e) {
      setMsg({ ok: false, text: errorText(e) });
    }
  };
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Approval policy</h1>
      <div className="row" role="group" aria-label="Level">
        <label className="row">
          Database
          <input value={db} onChange={(e) => setDb(e.target.value.trim())} placeholder="(tenant)" />
        </label>
        <label className="row">
          Table
          <input value={object} onChange={(e) => setObject(e.target.value.trim())} placeholder="(database)" disabled={!db} />
        </label>
      </div>
      {policy.isError && (
        <p className="error" role="alert">
          {errorText(policy.error)}
        </p>
      )}
      <table data-testid="policy-table">
        <thead>
          <tr>
            <th>Kind</th>
            <th>Mode</th>
            <th>From</th>
            <th>Level</th>
            <th>Escalate after</th>
            <th>Auto-apply after</th>
          </tr>
        </thead>
        <tbody>
          {(policy.data?.kinds ?? []).map((k) => (
            <tr key={k.kind}>
              <td>{k.kind}</td>
              <td>
                <code>{k.mode_key}</code> = {k.mode}
              </td>
              <td>{k.mode_source}</td>
              <td>{k.level}</td>
              <td>{k.escalate_after ?? 'off'}</td>
              <td>{k.auto_apply_after ?? 'off'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        Change a mode with <code>ALTER {db ? (object ? 'TABLE' : 'DATABASE') : 'TENANT'} … SPRIG SET (auto_index.mode = &apos;approve&apos;)</code>; timers
        with <code>approvals.escalate_after</code> and <code>approvals.auto_apply_after</code> (for example{' '}
        <code>dc.move=off,index=24h,*=72h</code>).
      </p>
      <h2>Email</h2>
      {msg && (
        <p className={msg.ok ? 'banner' : 'error'} role={msg.ok ? 'status' : 'alert'}>
          {msg.text}
        </p>
      )}
      <ul>
        {(subs.data ?? []).map((s) => (
          <li key={s.user}>
            {s.user}: {s.email} ({s.delivery})
          </li>
        ))}
      </ul>
      <div className="row">
        <label className="row">
          My address
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} />
        </label>
        <label className="row">
          Delivery
          <select value={delivery} onChange={(e) => setDelivery(e.target.value)} aria-label="Delivery">
            <option value="immediate">immediate</option>
            <option value="digest">daily digest</option>
            <option value="off">off</option>
          </select>
        </label>
        <button className="primary" onClick={() => void save()} disabled={!email.includes('@')}>
          Save
        </button>
      </div>
    </div>
  );
}
