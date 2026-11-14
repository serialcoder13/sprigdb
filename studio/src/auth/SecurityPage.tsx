import { useState } from 'react';
import { FormattedMessage } from 'react-intl';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { EnrolForm } from './Factors';
import { logout, useSession } from './session';

interface FactorsReply {
  factors: { id: string; kind: string; state: string; created_ms: string }[];
  recovery_left: number;
  required: boolean;
}

/** One's own second factors. */
export function SecurityPage() {
  const qc = useQueryClient();
  const me = useSession((s) => s.me);
  const q = useQuery({ queryKey: ['factors'], queryFn: () => json<FactorsReply>('/v1/auth/factors') });
  const [enrolling, setEnrolling] = useState(false);
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const active = (q.data?.factors ?? []).filter((f) => f.kind === 'totp' && f.state === 'active');
  const remove = async () => {
    setError(null);
    try {
      await json('/v1/auth/factors:reset', { body: { code: code.trim() } });
      // Every session ended, this one too.
      await logout('Your second factor was removed and your sessions ended. Sign in again.');
    } catch (e) {
      setError(errorText(e));
    }
  };
  return (
    <div className="stack" style={{ maxInlineSize: 720 }}>
      <h1>
        <FormattedMessage id="security.title" />
      </h1>
      <dl className="kv">
        <dt>Signed in with</dt>
        <dd data-testid="amr">{(me?.amr ?? []).join(', ')}</dd>
        <dt>Second factor required</dt>
        <dd>{q.data ? (q.data.required ? 'yes (auth.mfa_required)' : 'no') : ''}</dd>
        <dt>Recovery codes left</dt>
        <dd data-testid="recovery-left">{q.data?.recovery_left ?? ''}</dd>
      </dl>
      {q.error && <p className="error">{errorText(q.error)}</p>}
      {active.length > 0 ? (
        <section className="card stack" aria-label="Authenticator app">
          <p data-testid="factor-active">
            Authenticator app (TOTP), active since {new Date(Number(active[0]?.created_ms ?? 0)).toLocaleString()}.
          </p>
          <p className="muted">
            <FormattedMessage id="security.removeHint" />
          </p>
          <label className="stack">
            <span>Code</span>
            <input autoComplete="one-time-code" value={code} onChange={(e) => setCode(e.target.value)} />
          </label>
          <button className="danger" onClick={() => void remove()} disabled={!code.trim()}>
            <FormattedMessage id="security.remove" />
          </button>
        </section>
      ) : enrolling ? (
        <section className="card" aria-label="Set up an authenticator app">
          <EnrolForm
            onDone={() => {
              setEnrolling(false);
              void qc.invalidateQueries({ queryKey: ['factors'] });
            }}
          />
        </section>
      ) : (
        <div className="stack">
          <p className="muted" data-testid="no-factor">
            <FormattedMessage id="security.none" />
          </p>
          <button className="primary" onClick={() => setEnrolling(true)}>
            <FormattedMessage id="security.add" />
          </button>
        </div>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
