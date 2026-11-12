// Dry runs for connections and sinks (chapter 44 step 30; chapter 51,
// "Testing connections and sinks"; Studio rounds 6 and 7, R9 items 104–105):
//
// - a saved connection's test is `Connections.Test` (`POST .../connections/
//   {name}:test`, TEST CONNECTION as the caller): one row per step
//   (credentials, options, connect, metadata, ...), each `ok` or `failed`
//   with the destination's own message and the time;
// - with `write`, it also writes one marked test record, so it is shown
//   first, like any change;
// - a new connection is tested before it is saved with `TEST CREATE
//   CONNECTION ...` (ST68): the same steps, nothing created; shown first,
//   credentials masked;
// - a saved sink's dry run is `Sinks.Test` (`POST .../sinks/{name}:test`,
//   TEST SINK WITH SAMPLE n): the sink's WHERE, columns and TRANSFORM over
//   the source's latest records, each record's outcome, delivering nothing
//   and moving no position; a new sink's is `TEST CREATE SINK ... WITH
//   SAMPLE n` (ST69).

import { useEffect, useRef, useState } from 'react';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { Generic, table, useStatement, type Table } from './MessagingPage';
import { outcomes, stepRows, testConnection, testNewSink, type StepRow } from './statements';

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

export async function steps(db: string, sql: string): Promise<StepRow[]> {
  const t = await table(db, sql);
  return stepRows(t.columns, t.rows);
}

type Reply = { columns: string[]; rows: (string | null)[][] };
const base = (db: string, kind: 'sinks' | 'connections', name: string) =>
  `/v1/databases/${encodeURIComponent(db)}/${kind}/${encodeURIComponent(name)}:test`;

/** `Connections.Test` (r9last's route): TEST CONNECTION [WRITE [TO 'to']] as the caller. */
async function connectionTest(db: string, name: string, write: { to: string } | null): Promise<StepRow[]> {
  const body = write ? { write: true, ...(write.to.trim() ? { to: write.to.trim() } : {}) } : {};
  const r = await json<Reply>(base(db, 'connections', name), { body });
  return stepRows(r.columns, r.rows);
}

/** `Sinks.Test` (r9last's route): TEST SINK WITH SAMPLE n as the caller. */
async function sinkTest(db: string, name: string, sample: number): Promise<Table> {
  if (!Number.isInteger(sample) || sample < 1 || sample > 1000) throw new Error('sample 1 to 1,000 records');
  return json<Reply>(base(db, 'sinks', name), { body: { sample } });
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
    // The statement Connections.Test runs, shown before a write.
    const sql = testConnection(name, write ? { to } : null);
    if (write && !(await st.ask(sql, 'This writes one marked test record to the destination (a webhook request, a Kafka record, an S3 object deleted again).'))) return;
    setBusy(true);
    try {
      setOut({ steps: await connectionTest(db, name, write ? { to } : null), write });
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

const OUTCOME_ORDER = ['deliver', 'filtered', 'dropped', 'failed'];

function Outcomes({ out, tid }: Readonly<{ out: Table; tid: string }>) {
  const counts = outcomes(out.columns, out.rows);
  return (
    <>
      <p role="status" data-testid={`${tid}-summary`}>
        {out.rows.length
          ? [...OUTCOME_ORDER.filter((k) => counts[k]), ...Object.keys(counts).filter((k) => !OUTCOME_ORDER.includes(k))].map((k) => `${counts[k]} ${k}`).join(', ')
          : 'The source has no records to sample.'}
      </p>
      {out.rows.length > 0 && <Generic t={out} testid={`${tid}-rows`} />}
    </>
  );
}

/**
 * A dry run: a saved sink's (`Sinks.Test`), or with `create`, the `CREATE
 * SINK` statement of a sink not saved yet (`TEST CREATE SINK`, ST69).
 */
export function SinkDryRun({ db, name, create }: Readonly<{ db: string; name: string; create?: () => string }>) {
  const [sample, setSample] = useState('10');
  const [out, setOut] = useState<Table | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const run = async () => {
    setErr(null);
    setOut(null);
    setBusy(true);
    try {
      const n = Number(sample);
      setOut(create ? await table(db, testNewSink(create(), n)) : await sinkTest(db, name, n));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const tid = create ? 'sink-new-dry' : 'sink-dry';
  return (
    <section className="stack" aria-labelledby={`${tid}-h`} data-testid={`${tid}-run`}>
      <h3 id={`${tid}-h`} style={{ margin: 0 }}>
        {create ? 'Dry run before creating' : 'Dry run'}
      </h3>
      <p className="muted">
        Runs the sink&apos;s WHERE, columns and TRANSFORM over the source&apos;s latest records (a stream&apos;s newest, a queue&apos;s next, browsed
        and not taken). Nothing is delivered and no position moves{create ? '; nothing is created' : ''}.
      </p>
      <div className="row">
        <label className="row">
          Records <input value={sample} onChange={(e) => setSample(e.target.value)} size={5} inputMode="numeric" data-testid={`${tid}-sample`} />
        </label>
        <button type="button" onClick={() => void run()} disabled={busy} data-testid={`${tid}-run-go`}>
          Dry run
        </button>
        {busy && <span className="muted">Running…</span>}
      </div>
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
      {out && <Outcomes out={out} tid={tid} />}
    </section>
  );
}
