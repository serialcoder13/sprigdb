import { useState, type FormEvent } from 'react';
import { Navigate, useSearchParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { insecureLoginBlocked, login, useSession, type LoginStep } from './session';
import { Enrol, SecondFactor } from './Factors';
import { errorText } from '../api/errors';

/** Sign-in (chapter 44, "Login and the session"). Paste and password
 * managers work (WCAG 3.3.8): plain inputs with autocomplete hints. */
export function LoginPage() {
  const intl = useIntl();
  const status = useSession((s) => s.status);
  const me = useSession((s) => s.me);
  const reason = useSession((s) => s.reason);
  const [params] = useSearchParams();
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [step, setStep] = useState<LoginStep>({ kind: 'done' });
  const blocked = insecureLoginBlocked();
  const next = params.get('next');
  if (status === 'active' && me && step.kind !== 'enrol') {
    const safeNext = next && next.startsWith('/t/') ? next : `/t/${encodeURIComponent(me.principal.tenant ?? '-')}`;
    return <Navigate to={safeNext} replace />;
  }
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const next = await login(name.trim(), password);
      setPassword('');
      setStep(next);
    } catch (err) {
      setError(errorText(err));
    }
  };
  const busy = status === 'authenticating';
  if (step.kind === 'challenge') {
    return <SecondFactor challenge={step.challenge} onRestart={() => setStep({ kind: 'done' })} />;
  }
  if (step.kind === 'enrol') {
    return <Enrol ticket={step.ticket} onDone={() => setStep({ kind: 'done' })} />;
  }
  return (
    <main className="main" style={{ maxInlineSize: 420, marginInline: 'auto', paddingBlockStart: 64 }}>
      <h1>
        <FormattedMessage id="login.title" />
      </h1>
      {reason && (
        <p className="banner" role="status">
          {reason}
        </p>
      )}
      {blocked && (
        <p className="banner" role="alert">
          <FormattedMessage id="login.insecure" />
        </p>
      )}
      <form className="stack" onSubmit={(e) => void submit(e)} aria-describedby={error ? 'login-error' : undefined}>
        <label className="stack">
          <span>
            <FormattedMessage id="login.login" />
          </span>
          <input
            name="username"
            autoComplete="username"
            required
            value={name}
            onChange={(e) => setName(e.target.value)}
            aria-describedby="login-hint"
          />
        </label>
        <span id="login-hint" className="muted">
          {intl.formatMessage({ id: 'login.loginHint' })}
        </span>
        <label className="stack">
          <span>
            <FormattedMessage id="login.password" />
          </span>
          <input
            name="password"
            type="password"
            autoComplete="current-password"
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        {error && (
          <p id="login-error" className="error" role="alert">
            {error}
          </p>
        )}
        <button className="primary" type="submit" disabled={busy || blocked}>
          {busy ? intl.formatMessage({ id: 'login.working' }) : intl.formatMessage({ id: 'login.submit' })}
        </button>
      </form>
    </main>
  );
}
