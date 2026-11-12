// Dry runs for connections and sinks (chapter 44 step 30; chapter 51,
// "Testing connections and sinks"; Studio round 6, R9 items 104–105):
//
// - `TEST CONNECTION c` opens the destination as a sink would and answers
//   one row per step (credentials, options, connect, metadata, ...), each
//   `ok` or `failed` with the destination's own message and the time;
// - `TEST CONNECTION c WRITE [TO 'target']` also writes one marked test
//   record, so it is shown first, like any change;
// - a new connection is tested before it is saved under a scratch name:
//   CREATE (credentials masked on screen), TEST, DROP, whatever the test
//   found;
// - `TEST SINK s WITH SAMPLE n` runs the sink's WHERE, columns and
//   TRANSFORM over the source's latest records and shows each record's
//   outcome, delivering nothing and moving no position.

import { useEffect, useRef, useState } from 'react';
import { errorText } from '../api/errors';
import { execute } from '../api/sql';
import { Generic, table, useStatement, type Table } from './MessagingPage';
import { dropConnection, outcomes, stepRows, testConnection, testSink, type StepRow } from './statements';

export function StepTable({ steps, testid = 'conn-test-steps' }: Readonly<{ steps: StepRow[]; testid?: string }>) {
  const failed = steps.filter((s) => !s.ok).length;
  return (
    <div className="stack">
      <p role="status" className={failed ? 'error' : undefined} data-testid={`${testid}-summary`}>
        {failed ? `${failed} of ${steps.length} steps failed.` : `All ${steps.length} steps passed.`}
      </p>
      <div className="table-scroll" tabIndex={0} role="region" aria-label="Test steps">
        <table className="table" data-testid={testid}>
          <thead>
            <tr>
              <th>Step</th>
              <th>Result</th>
              <th>Detail</th>
              <th>Elapsed</th>
            </tr>
          </thead>
          <tbody>
            {steps.map((s, i) => (
              <tr key={i}>
                <td className="mono">{s.step}</td>
                <td>
                  <span className={`badge ${s.ok ? '' : 'error'}`}>{s.ok ? 'ok' : 'failed'}</span>
                </td>
                <td>{s.detail}</td>
                <td className="num">{s.ms == null ? '–' : `${s.ms} ms`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

async function steps(db: string, sql: string): Promise<StepRow[]> {
  const t = await table(db, sql);
  return stepRows(t.columns, t.rows);
}

/** "Test connection" and "Write a test record" on a saved connection. */
/** `autoRun` changing (after credentials were set) runs the plain test again. */
export function ConnectionTest({ db, name, autoRun = 0 }: Readonly<{ db: string; name: string; autoRun?: number }>) {
  const st = useStatement(db);
  const [out, setOut] = useState<{ steps: StepRow[]; write: boolean } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [to, setTo] = useState('');
  const run = async (write: boolean) => {
    setErr(null);
    setOut(null);
    const sql = testConnection(name, write ? { to } : null);
    if (write && !(await st.ask(sql, 'This writes one marked test record to the destination (a webhook request, a Kafka record, an S3 object deleted again).'))) return;
    setBusy(true);
    try {
      setOut({ steps: await steps(db, sql), write });
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const last = useRef(autoRun);
  useEffect(() => {
    if (autoRun !== last.current) {
      last.current = autoRun;
      void run(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRun]);
  return (
    <section className="stack" aria-label={`Test ${name}`} data-testid="conn-test">
      <div className="row">
        <button onClick={() => void run(false)} disabled={busy} data-testid="conn-test-run">
          Test connection
        </button>
        <label className="row">
          Write to <input value={to} onChange={(e) => setTo(e.target.value)} placeholder="topic, prefix or table" size={18} data-testid="conn-test-to" />
        </label>
        <button onClick={() => void run(true)} disabled={busy} data-testid="conn-test-write">
          Write a test record…
        </button>
        {busy && <span className="muted">Testing…</span>}
      </div>
      <p className="muted">A test opens the destination as a sink would (allowlist, residency, sealed credentials) and writes nothing.</p>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {out && <StepTable steps={out.steps} />}
      {st.node}
    </section>
  );
}

/**
 * Tests an unsaved connection: CREATE under a scratch name, TEST, DROP.
 * `create` is the statement sent, `shown` the one on screen (credentials
 * masked); both name `scratch`.
 */
export async function testUnsaved(db: string, scratch: string, create: string): Promise<{ steps: StepRow[]; dropped: boolean }> {
  await execute(db, create);
  let found: StepRow[] = [];
  let failure: Error | null = null;
  try {
    found = await steps(db, testConnection(scratch));
  } catch (e) {
    failure = e instanceof Error ? e : new Error(errorText(e));
  }
  let dropped = true;
  try {
    await execute(db, dropConnection(scratch));
  } catch {
    dropped = false;
  }
  if (failure) throw failure;
  return { steps: found, dropped };
}

const OUTCOME_ORDER = ['deliver', 'filtered', 'dropped', 'failed'];

/** `TEST SINK s WITH SAMPLE n`: what the sink would deliver now. */
export function SinkDryRun({ db, name }: Readonly<{ db: string; name: string }>) {
  const [sample, setSample] = useState('10');
  const [out, setOut] = useState<Table | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const run = async () => {
    setErr(null);
    setOut(null);
    setBusy(true);
    try {
      setOut(await table(db, testSink(name, Number(sample))));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const counts = out ? outcomes(out.columns, out.rows) : null;
  return (
    <section className="stack" aria-labelledby="sink-dry-h" data-testid="sink-dry-run">
      <h3 id="sink-dry-h" style={{ margin: 0 }}>
        Dry run
      </h3>
      <p className="muted">
        Runs the sink&apos;s WHERE, columns and TRANSFORM over the source&apos;s latest records (a stream&apos;s newest, a queue&apos;s next, browsed
        and not taken). Nothing is delivered and no position moves.
      </p>
      <div className="row">
        <label className="row">
          Records <input value={sample} onChange={(e) => setSample(e.target.value)} size={5} inputMode="numeric" data-testid="sink-dry-sample" />
        </label>
        <button onClick={() => void run()} disabled={busy} data-testid="sink-dry-run-go">
          Dry run
        </button>
        {busy && <span className="muted">Running…</span>}
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {counts && (
        <p role="status" data-testid="sink-dry-summary">
          {out!.rows.length
            ? [...OUTCOME_ORDER.filter((k) => counts[k]), ...Object.keys(counts).filter((k) => !OUTCOME_ORDER.includes(k))].map((k) => `${counts[k]} ${k}`).join(', ')
            : 'The source has no records to sample.'}
        </p>
      )}
      {out && out.rows.length > 0 && <Generic t={out} testid="sink-dry-rows" />}
    </section>
  );
}
