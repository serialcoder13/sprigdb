// The Fingerprints and Before-and-after views of automatic indexing: top
// fingerprints by total time from Workload.ListFingerprints, and one
// fingerprint's p95 per bucket from Workload.Series, charted with
// the grid as the accessible alternative.

import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { json } from '../api/client';
import { errorText } from '../api/errors';
import { ResultBuffer } from '../results/buffer';
import { Chart } from '../results/Chart';

export interface FingerprintRow {
  id: string;
  kind: string;
  stmt_type: string;
  db: string | null;
  text: string;
  caller: string | null;
  runs: number;
  errors: number;
  total_ms: number;
  p50_ms: number;
  p95_ms: number;
  max_ms: number;
  rows_returned: number;
  examined_per_returned: number | null;
  unsargable: unknown[];
}

interface Series {
  id: string;
  text: string;
  days: number;
  buckets: { start: number; runs: number; errors: number; p50_ms: number; p95_ms: number; total_ms: number }[];
}

const fmt = (n: number | null | undefined) => (n == null ? '—' : n.toLocaleString(undefined, { maximumFractionDigits: 2 }));

/** The series as a result buffer: time, p50, p95, runs. */
export function seriesBuffer(s: Series): ResultBuffer {
  const b = new ResultBuffer(
    [
      { name: 'bucket (UTC)', type: 'VARCHAR', mysql_type: 253 },
      { name: 'p50 ms', type: 'DOUBLE', mysql_type: 5 },
      { name: 'p95 ms', type: 'DOUBLE', mysql_type: 5 },
      { name: 'runs', type: 'BIGINT', mysql_type: 8 },
    ],
    100_000,
    64 << 20,
  );
  b.push(
    s.buckets.map((x) => [
      new Date(x.start * 1000).toISOString().slice(0, 16).replace('T', ' '),
      String(x.p50_ms),
      String(x.p95_ms),
      String(x.runs),
    ]),
  );
  return b;
}

export function Fingerprints() {
  const [hours, setHours] = useState(24);
  const [source, setSource] = useState('all');
  const [open, setOpen] = useState<string | null>(null);
  const list = useQuery({
    queryKey: ['workload', 'fingerprints', hours, source],
    queryFn: () =>
      json<{ fingerprints: FingerprintRow[] }>(`/v1/workload/fingerprints?hours=${hours}&limit=100&source=${source}`),
  });
  const series = useQuery({
    queryKey: ['workload', 'series', open],
    enabled: !!open,
    queryFn: () => json<Series>(`/v1/workload/fingerprints/${open ?? ''}/series?days=7`),
  });
  const buffer = useMemo(() => (series.data ? seriesBuffer(series.data) : null), [series.data]);
  return (
    <div className="stack" data-testid="fingerprints">
      <div className="row">
        <label className="row">
          Window
          <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
            <option value={1}>1 hour</option>
            <option value={24}>24 hours</option>
            <option value={168}>7 days</option>
          </select>
        </label>
        <label className="row">
          Source
          <select value={source} onChange={(e) => setSource(e.target.value)}>
            <option value="all">All</option>
            <option value="client">Applications</option>
            <option value="studio">Studio</option>
          </select>
        </label>
      </div>
      {list.error && <p className="error">{errorText(list.error)}</p>}
      {list.data && list.data.fingerprints.length === 0 && <p className="muted">No statements recorded in this window.</p>}
      {list.data && list.data.fingerprints.length > 0 && (
        <table className="table">
          <caption className="sr-only">Top fingerprints by total time</caption>
          <thead>
            <tr>
              <th scope="col">Statement</th>
              <th scope="col">Database</th>
              <th scope="col">Runs</th>
              <th scope="col">Total ms</th>
              <th scope="col">p50 ms</th>
              <th scope="col">p95 ms</th>
              <th scope="col">Examined per returned</th>
              <th scope="col">Unsargable</th>
              <th scope="col">Source</th>
              <th scope="col">Latency</th>
            </tr>
          </thead>
          <tbody>
            {list.data.fingerprints.map((f) => (
              <tr key={f.id}>
                <td className="mono" title={f.id}>
                  {f.text.length > 120 ? `${f.text.slice(0, 120)}…` : f.text}
                </td>
                <td>{f.db ?? '—'}</td>
                <td>{fmt(f.runs)}</td>
                <td>{fmt(f.total_ms)}</td>
                <td>{fmt(f.p50_ms)}</td>
                <td>{fmt(f.p95_ms)}</td>
                <td>{fmt(f.examined_per_returned)}</td>
                <td>{f.unsargable.length ? f.unsargable.length : '—'}</td>
                <td>{f.caller === 'studio' ? 'Studio' : f.caller ?? 'Application'}</td>
                <td>
                  <button onClick={() => setOpen(f.id)} aria-label={`Latency series of ${f.id}`}>
                    Series
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {open && (
        <section className="card stack" aria-label="Latency series">
          <div className="row">
            <h2 style={{ margin: 0, flex: 1 }}>p95 per bucket, last 7 days</h2>
            <button onClick={() => setOpen(null)}>Close</button>
          </div>
          {series.data && <pre className="mono">{series.data.text}</pre>}
          {series.error && <p className="error">{errorText(series.error)}</p>}
          {buffer && buffer.rowCount === 0 && <p className="muted">No buckets yet.</p>}
          {buffer && buffer.rowCount > 0 && (
            <>
              <Chart buffer={buffer} />
              <table className="table">
                <caption className="sr-only">The same series as a table</caption>
                <thead>
                  <tr>
                    {buffer.columns.map((c) => (
                      <th key={c.name} scope="col">
                        {c.name}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {Array.from({ length: buffer.rowCount }, (_, r) => (
                    <tr key={r}>
                      {buffer.columns.map((c, i) => (
                        <td key={c.name}>{buffer.cell(r, i)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </section>
      )}
    </div>
  );
}
