import { useEffect, useRef, useState } from 'react';
import { begin, commit, execute, rollback } from '../api/sql';
import { errorText } from '../api/errors';
import { ident, qualified, sqlValue } from '../lib/sql';
import { parseCsv, parseNdjson } from './parse';
import type { Cell } from '../api/types';

/** Import CSV or NDJSON: batches of
 * studio.import_batch_rows, each its own transaction, with progress,
 * rejected rows and a resume point; above studio.import_max_mb, sprigctl. */
export function ImportDialog({
  db,
  table,
  batchRows,
  maxMb,
  onClose,
}: {
  db: string;
  table: string;
  batchRows: number;
  maxMb: number;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [resume, setResume] = useState(0);
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const [rejected, setRejected] = useState<{ row: number; error: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  useEffect(() => {
    ref.current?.showModal?.();
  }, []);
  const tooBig = file ? file.size > maxMb * 1024 * 1024 : false;
  const start = async () => {
    if (!file || tooBig) return;
    setRunning(true);
    setError(null);
    setRejected([]);
    try {
      const text = await file.text();
      let columns: string[];
      let rows: Cell[][];
      if (/\.(ndjson|jsonl|json)$/i.test(file.name)) {
        ({ columns, rows } = parseNdjson(text));
      } else {
        const all = parseCsv(text);
        columns = (all[0] ?? []).map((c) => c ?? '');
        rows = all.slice(1);
      }
      const target = qualified(db, table);
      const cols = columns.map(ident).join(', ');
      const insert = (batch: Cell[][]) =>
        `INSERT INTO ${target} (${cols}) VALUES ${batch.map((r) => `(${columns.map((_, i) => sqlValue(r[i] ?? null)).join(', ')})`).join(', ')}`;
      const bad: { row: number; error: string }[] = [];
      for (let i = resume; i < rows.length; i += batchRows) {
        const batch = rows.slice(i, i + batchRows);
        const t = await begin(db);
        try {
          await execute(db, insert(batch), { transaction: t.name });
          await commit(t);
        } catch {
          await rollback(t).catch(() => undefined);
          // Find the rejected rows one by one; the others go in.
          for (let j = 0; j < batch.length; j++) {
            try {
              await execute(db, insert([batch[j]!]));
            } catch (e) {
              bad.push({ row: i + j + 1, error: errorText(e) });
            }
          }
        }
        setResume(Math.min(rows.length, i + batchRows));
        setProgress({ done: Math.min(rows.length, i + batchRows), total: rows.length });
        setRejected([...bad]);
      }
    } catch (e) {
      setError(errorText(e));
    } finally {
      setRunning(false);
    }
  };
  return (
    <dialog ref={ref} aria-labelledby="import-title" onClose={onClose}>
      <div className="stack">
        <h2 id="import-title">Import into {table}</h2>
        <label className="stack">
          CSV (header row) or NDJSON file
          <input type="file" accept=".csv,.ndjson,.jsonl,.json,text/csv" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
        </label>
        {tooBig && (
          <p className="banner">
            The file is over studio.import_max_mb ({maxMb} MB): use <code>sprigctl import</code>.
          </p>
        )}
        <label className="row">
          Resume at row
          <input type="number" min={0} value={resume} onChange={(e) => setResume(Number(e.target.value))} style={{ inlineSize: 120 }} />
        </label>
        {progress && (
          <p role="status">
            <progress value={progress.done} max={progress.total} /> {progress.done} / {progress.total} rows
          </p>
        )}
        {rejected.length > 0 && (
          <details open>
            <summary>{rejected.length} rejected row(s)</summary>
            <ul>
              {rejected.slice(0, 200).map((r) => (
                <li key={r.row}>
                  Row {r.row}: {r.error}
                </li>
              ))}
            </ul>
          </details>
        )}
        {error && <p className="error">{error}</p>}
        <div className="row">
          <button className="primary" disabled={!file || tooBig || running} onClick={() => void start()}>
            {running ? 'Importing…' : 'Import'}
          </button>
          <button onClick={() => ref.current?.close()}>Close</button>
        </div>
      </div>
    </dialog>
  );
}
