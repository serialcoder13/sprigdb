import { useEffect, useState, type FormEvent } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';
import { completeMfa, confirmEnrolment, startEnrolment, type Enrolment } from './session';
import { errorText } from '../api/errors';
import { QrCode } from './QrCode';

const page = { maxInlineSize: 480, marginInline: 'auto', paddingBlockStart: 64 } as const;

/** The second step of a login (chapter 14 step 13): an authenticator code
 * or a recovery code. */
export function SecondFactor({ challenge, onRestart }: Readonly<{ challenge: string; onRestart: () => void }>) {
  const intl = useIntl();
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      await completeMfa(challenge, code.trim());
    } catch (err) {
      setError(errorText(err));
      setBusy(false);
    }
  };
  return (
    <main className="main" style={page}>
      <h1>
        <FormattedMessage id="mfa.title" />
      </h1>
      <form className="stack" onSubmit={(e) => void submit(e)}>
        <label className="stack">
          <span>
            <FormattedMessage id="mfa.code" />
          </span>
          <input
            name="one-time-code"
            autoComplete="one-time-code"
            inputMode="text"
            required
            autoFocus
            value={code}
            onChange={(e) => setCode(e.target.value)}
            aria-describedby="mfa-hint"
          />
        </label>
        <span id="mfa-hint" className="muted">
          {intl.formatMessage({ id: 'mfa.hint' })}
        </span>
        {error && (
          <p className="error" role="alert" data-testid="mfa-error">
            {error}
          </p>
        )}
        <div className="row">
          <button className="primary" type="submit" disabled={busy}>
            <FormattedMessage id="mfa.verify" />
          </button>
          <button type="button" onClick={onRestart}>
            <FormattedMessage id="mfa.restart" />
          </button>
        </div>
      </form>
    </main>
  );
}

/** Recovery codes, shown once. */
export function RecoveryCodes({ codes, onDone }: Readonly<{ codes: string[]; onDone: () => void }>) {
  return (
    <section className="stack" aria-labelledby="rc-title">
      <h2 id="rc-title">
        <FormattedMessage id="mfa.recoveryTitle" />
      </h2>
      <p>
        <FormattedMessage id="mfa.recoveryHint" />
      </p>
      <ul className="mono" data-testid="recovery-codes" aria-label="Recovery codes">
        {codes.map((c) => (
          <li key={c}>{c}</li>
        ))}
      </ul>
      <button className="primary" onClick={onDone}>
        <FormattedMessage id="mfa.saved" />
      </button>
    </section>
  );
}

/** TOTP enrolment: the secret (shown once), then a code from the app. With
 * an enrol ticket (at login, when auth.mfa_required covers the user) the
 * confirmation also signs in. */
export function EnrolForm({ ticket, onDone }: Readonly<{ ticket?: string; onDone: () => void }>) {
  const intl = useIntl();
  const [e, setE] = useState<Enrolment | null>(null);
  const [code, setCode] = useState('');
  const [codes, setCodes] = useState<{ codes: string[]; finish: () => Promise<void> } | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    startEnrolment(ticket)
      .then((x) => live && setE(x))
      .catch((err: unknown) => live && setError(errorText(err)));
    return () => {
      live = false;
    };
  }, [ticket]);
  if (codes) {
    return (
      <RecoveryCodes
        codes={codes.codes}
        onDone={() => {
          void codes.finish().then(onDone, (err: unknown) => setError(errorText(err)));
        }}
      />
    );
  }
  const submit = async (ev: FormEvent) => {
    ev.preventDefault();
    setError(null);
    try {
      setCodes(await confirmEnrolment(code.trim(), ticket));
    } catch (err) {
      setError(errorText(err));
    }
  };
  return (
    <div className="stack">
      {e ? (
        <>
          <p>
            <FormattedMessage id="mfa.enrolHint" />
          </p>
          <QrCode text={e.otpauth_uri} label={intl.formatMessage({ id: 'mfa.qrLabel' })} />
          <dl className="kv">
            <dt>Secret</dt>
            <dd className="mono" data-testid="totp-secret">
              {e.secret}
            </dd>
            <dt>Link</dt>
            <dd className="mono" style={{ wordBreak: 'break-all' }} data-testid="totp-uri">
              {e.otpauth_uri}
            </dd>
          </dl>
          <form className="stack" onSubmit={(ev) => void submit(ev)}>
            <label className="stack">
              <span>
                <FormattedMessage id="mfa.code" />
              </span>
              <input
                name="one-time-code"
                autoComplete="one-time-code"
                inputMode="numeric"
                required
                value={code}
                onChange={(ev) => setCode(ev.target.value)}
              />
            </label>
            <button className="primary" type="submit">
              <FormattedMessage id="mfa.confirm" />
            </button>
          </form>
        </>
      ) : (
        !error && <p className="muted">Preparing…</p>
      )}
      {error && (
        <p className="error" role="alert" data-testid="mfa-error">
          {error}
        </p>
      )}
    </div>
  );
}

/** The enrolment a login requires (auth.mfa_required). */
export function Enrol({ ticket, onDone }: Readonly<{ ticket: string; onDone: () => void }>) {
  return (
    <main className="main" style={page}>
      <h1>
        <FormattedMessage id="mfa.enrolTitle" />
      </h1>
      <p className="banner" role="status">
        <FormattedMessage id="mfa.enrolRequired" />
      </p>
      <EnrolForm ticket={ticket} onDone={onDone} />
    </main>
  );
}
