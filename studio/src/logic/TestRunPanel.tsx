// The test-run form and its report. Inputs: arguments by parameter, a
// trigger's or validation's row (old row and operation), a stream record or
// queue message, snapshot rows (the code sees exactly these rows of each table
// named) and answers for `fetch` (never sent). The report shows the event or
// the error, each statement with its kind, rows and time, the writes
// (always rolled back), logs, outbound calls, the snapshot tables after the
// run, budgets and timing.

import { useState } from 'react';
import { errorText } from '../api/errors';
import { argValue, budgetLines, jsonField, mocks, snapshotRows, testRun, type MockForm, type Param, type Report, type Stmt, type TestRunRequest } from './testrun';

export type Target =
  | { kind: string; name: string; versions: number[]; active: number | null }
  | { kind: string; draft: () => string };

const TARGET = { paddingBlock: 6, cursor: 'pointer' } as const;

export function TestRunPanel({ db, target, params, binding }: Readonly<{ db: string; target: Target; params: Param[]; binding?: string | null }>) {
  const [values, setValues] = useState<Record<string, string>>({});
  const [version, setVersion] = useState('');
  const [op, setOp] = useState('insert');
  const [row, setRow] = useState('');
  const [old, setOld] = useState('');
  const [event, setEvent] = useState('');
  const [snap, setSnap] = useState('');
  const [mockRows, setMockRows] = useState<MockForm[]>([]);
  const [report, setReport] = useState<Report | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const isRow = target.kind === 'trigger' || target.kind === 'validation';
  const onEvent = target.kind === 'trigger' && !!binding && /stream|queue|message/i.test(binding);
  const eventKey = binding && /queue|message/i.test(binding) ? 'message' : 'record';

  const run = async () => {
    setErr(null);
    setReport(null);
    let req: TestRunRequest;
    try {
      req = { kind: target.kind };
      if ('draft' in target) req.source = target.draft();
      else {
        req.name = target.name;
        if (version) req.version = Number(version);
      }
      if (!isRow) req.args = Object.fromEntries(params.map((p) => [p.name, argValue(p.type, values[p.name] ?? '')]));
      if (isRow && !onEvent) {
        req.op = op;
        req.row = jsonField('Row', row) ?? null;
        req.old = jsonField('Old row', old) ?? null;
      }
      if (onEvent) req[eventKey] = jsonField(eventKey === 'message' ? 'Message' : 'Record', event) ?? {};
      const rows = snapshotRows(snap);
      if (rows) req.rows = rows;
      const f = mocks(mockRows);
      if (f.length) req.fetch = f;
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      return;
    }
    setBusy(true);
    try {
      setReport(await testRun(db, req));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="stack" aria-label="Test run" data-testid="logic-testrun">
      <p className="muted">
        A test run runs the code on the engine tier with your rights, inside a transaction that is always rolled back: nothing it writes stays, and
        fetch calls are answered by the mocks below, never sent.
      </p>
      {!('draft' in target) && (
        <label className="row">
          Version
          <select value={version} onChange={(e) => setVersion(e.target.value)} data-testid="logic-test-version">
            <option value="">active{target.active ? ` (v${target.active})` : ''}</option>
            {target.versions.map((v) => (
              <option key={v} value={String(v)}>
                v{v}
              </option>
            ))}
          </select>
        </label>
      )}
      {!isRow && params.length === 0 && <p className="muted">No parameters.</p>}
      {!isRow &&
        params.map((p) => (
          <label key={p.name} className="row">
            <span className="mono">
              {p.name} ({p.type || 'any'})
            </span>
            <input value={values[p.name] ?? ''} onChange={(e) => setValues({ ...values, [p.name]: e.target.value })} data-testid={`logic-arg-${p.name}`} />
          </label>
        ))}
      {isRow && !onEvent && (
        <div className="stack">
          <label className="row">
            Operation
            <select value={op} onChange={(e) => setOp(e.target.value)} data-testid="logic-test-op">
              <option>insert</option>
              <option>update</option>
              <option>delete</option>
            </select>
          </label>
          <label className="stack">
            <span>Row after the write (JSON object)</span>
            <textarea rows={3} value={row} onChange={(e) => setRow(e.target.value)} spellCheck={false} className="mono" data-testid="logic-test-row" />
          </label>
          <label className="stack">
            <span>Row before it (JSON object; empty for an insert)</span>
            <textarea rows={2} value={old} onChange={(e) => setOld(e.target.value)} spellCheck={false} className="mono" />
          </label>
        </div>
      )}
      {onEvent && (
        <label className="stack">
          <span>{eventKey === 'message' ? 'The queue message (ctx.message, JSON)' : 'The stream record (ctx.record, JSON)'}</span>
          <textarea rows={3} value={event} onChange={(e) => setEvent(e.target.value)} spellCheck={false} className="mono" data-testid="logic-test-event" />
        </label>
      )}
      <details>
        <summary style={TARGET}>Snapshot rows{snap.trim() ? ' (set)' : ''}</summary>
        <label className="stack">
          <span className="muted">
            The code sees exactly these rows of each table named (a temporary table in your session; the real table is untouched), as{' '}
            <code>{'{"table": [{"column": value, ...}]}'}</code>.
          </span>
          <textarea rows={5} value={snap} onChange={(e) => setSnap(e.target.value)} spellCheck={false} className="mono" aria-label="Snapshot rows (JSON)" data-testid="logic-test-rows" />
        </label>
      </details>
      <details>
        <summary style={TARGET}>Mocks for fetch{mockRows.length ? ` (${mockRows.length})` : ''}</summary>
        <div className="stack">
          <p className="muted">The first mock whose method and URL match answers (a URL ending in * matches a prefix). Without one, the call fails.</p>
          {mockRows.map((m, i) => (
            <div key={i} className="row" data-testid={`logic-mock-${i}`}>
              <label className="row">
                Method
                <select value={m.method} onChange={(e) => setMockRows(mockRows.map((x, j) => (j === i ? { ...x, method: e.target.value } : x)))}>
                  {['any', 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map((x) => (
                    <option key={x}>{x}</option>
                  ))}
                </select>
              </label>
              <label className="row">
                URL <input value={m.url} onChange={(e) => setMockRows(mockRows.map((x, j) => (j === i ? { ...x, url: e.target.value } : x)))} size={28} data-testid={`logic-mock-url-${i}`} />
              </label>
              <label className="row">
                Status <input value={m.status} onChange={(e) => setMockRows(mockRows.map((x, j) => (j === i ? { ...x, status: e.target.value } : x)))} size={4} />
              </label>
              <label className="row">
                Body <input value={m.body} onChange={(e) => setMockRows(mockRows.map((x, j) => (j === i ? { ...x, body: e.target.value } : x)))} size={28} className="mono" data-testid={`logic-mock-body-${i}`} />
              </label>
              <button type="button" onClick={() => setMockRows(mockRows.filter((_, j) => j !== i))} aria-label={`Remove mock ${i + 1}`}>
                ✕
              </button>
            </div>
          ))}
          <div>
            <button type="button" onClick={() => setMockRows([...mockRows, { method: 'any', url: '', status: '200', body: '' }])} data-testid="logic-mock-add">
              Add a mock
            </button>
          </div>
        </div>
      </details>
      <div className="row">
        <button className="primary" onClick={() => void run()} disabled={busy} data-testid="logic-test-run">
          Run test
        </button>
        {busy && <span className="muted">Running…</span>}
      </div>
      {err && (
        <p className="error" role="alert" data-testid="logic-run-error">
          {err}
        </p>
      )}
      {report && <TestReport report={report} />}
    </section>
  );
}

function StmtTable({ list, testid }: Readonly<{ list: Stmt[]; testid: string }>) {
  return (
    <div className="table-scroll" tabIndex={0} role="region" aria-label={testid}>
      <table className="table" data-testid={testid}>
        <thead>
          <tr>
            <th>#</th>
            <th>Kind</th>
            <th>Statement</th>
            <th>Rows</th>
            <th>Time</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
          {list.map((s, i) => (
            <tr key={i}>
              <td>{i + 1}</td>
              <td>{s.kind}</td>
              <td className="mono">{s.sql}</td>
              <td className="num">{s.rows ?? (s.affected != null ? `${s.affected} affected` : '–')}</td>
              <td className="num">{s.ms.toFixed(2)} ms</td>
              <td className={s.error ? 'error' : undefined}>{s.error ? `${s.error.code ?? ''} ${s.error.message ?? ''}` : ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RowsTable({ name, rows }: Readonly<{ name: string; rows: Record<string, unknown>[] }>) {
  const cols = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  const cell = (v: unknown) => (v === null || v === undefined ? 'NULL' : typeof v === 'string' ? v : JSON.stringify(v));
  return (
    <div className="table-scroll" tabIndex={0} role="region" aria-label={`Table ${name} after the run`}>
      <table className="table" data-testid={`logic-report-table-${name}`}>
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td key={c} className="mono">
                  {cell(r[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TestReport({ report: r }: Readonly<{ report: Report }>) {
  const budget = budgetLines(r.budget);
  const t = r.timing ?? {};
  return (
    <section className="card stack" aria-label="Test run report" data-testid="logic-report">
      <p className="muted">
        {r.draft ? 'Draft' : `Version ${r.version ?? '–'}`} of {r.kind} {r.name}, {r.runtime}
        {r.deployedTier ? `; deployed on the ${r.deployedTier} tier, tested on the engine tier` : ''}.
      </p>
      {r.error ? (
        <p className="error" role="alert" data-testid="logic-report-error">
          Failed: {r.error.code ?? ''} {r.error.name ?? ''}: {r.error.message ?? ''}
        </p>
      ) : (
        <div className="stack">
          <p data-testid="logic-report-event">
            Event <strong>{r.event ?? 'output'}</strong>
          </p>
          <pre className="logic-stmt" data-testid="logic-report-result">
            {JSON.stringify(r.result, null, 2)}
          </pre>
        </div>
      )}
      <dl className="kv" data-testid="logic-report-timing">
        <dt>Total</dt>
        <dd>{t.totalMs ?? '–'} ms</dd>
        <dt>Setup</dt>
        <dd>{t.setupMs ?? '–'} ms</dd>
        <dt>Run</dt>
        <dd>{t.runMs ?? '–'} ms</dd>
        <dt>In statements</dt>
        <dd>{t.dbMs ?? '–'} ms</dd>
      </dl>
      <h3>Statements ({r.statements.length})</h3>
      {r.statements.length ? <StmtTable list={r.statements} testid="logic-report-statements" /> : <p className="muted">None.</p>}
      <h3>Writes, rolled back ({r.writes.length})</h3>
      {r.writes.length ? <StmtTable list={r.writes} testid="logic-report-writes" /> : <p className="muted">None.</p>}
      <h3>Logs ({r.logs.length})</h3>
      {r.logs.length ? (
        <pre className="logic-stmt" data-testid="logic-report-logs">
          {r.logs.join('\n')}
        </pre>
      ) : (
        <p className="muted">None.</p>
      )}
      <h3>Outbound calls, not sent ({r.fetches.length})</h3>
      {r.fetches.length ? (
        <table className="table" data-testid="logic-report-fetches">
          <thead>
            <tr>
              <th>Method</th>
              <th>URL</th>
              <th>Answered</th>
            </tr>
          </thead>
          <tbody>
            {r.fetches.map((f, i) => (
              <tr key={i}>
                <td>{f.method}</td>
                <td className="mono">{f.url}</td>
                <td className={f.error ? 'error' : undefined}>{f.error ?? f.status ?? '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="muted">None.</p>
      )}
      {Object.keys(r.tables).length > 0 && (
        <>
          <h3>Snapshot tables after the run</h3>
          {Object.entries(r.tables).map(([name, rows]) => (
            <div key={name} className="stack">
              <h4 style={{ margin: 0 }} className="mono">
                {name}
              </h4>
              <RowsTable name={name} rows={rows} />
            </div>
          ))}
        </>
      )}
      <h3>Budgets</h3>
      <table className="table" data-testid="logic-report-budget">
        <thead>
          <tr>
            <th>Budget</th>
            <th>Used</th>
            <th>Limit</th>
            <th>Share</th>
          </tr>
        </thead>
        <tbody>
          {budget.map((b) => (
            <tr key={b.key}>
              <td>{b.label}</td>
              <td className="num">{b.used ?? '–'}</td>
              <td className="num">{b.limit ?? '–'}</td>
              <td>
                {b.used != null && b.limit ? (
                  <meter min={0} max={b.limit} value={Math.min(b.used, b.limit)} high={b.limit * 0.8} aria-label={`${b.label}: ${b.used} of ${b.limit}`} />
                ) : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {r.notes.length > 0 && (
        <ul className="muted" data-testid="logic-report-notes">
          {r.notes.map((n, i) => (
            <li key={i}>{n}</li>
          ))}
        </ul>
      )}
    </section>
  );
}
