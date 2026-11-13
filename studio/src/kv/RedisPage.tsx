// The Redis console and key browser (chapter 44 step 22) over the key-value
// API's Exec and Scan (chapter 37). A namespace is chosen by database and
// name; as `sprig` does over REST (tui decision T11) the defaults are
// database `redis` and namespace `db0`. Each line of the console is one
// command, checked against the command table, confirmed when
// studio.confirm_writes covers it (FLUSHDB, FLUSHALL and SWAPDB always ask),
// and its reply printed as redis-cli prints it.

import { lazy, Suspense, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { useDatabases, useSnapshot } from '../catalog/snapshot';
import { errorText } from '../api/errors';
import type { CompletionSource, MonacoHandle } from '../editor/Monaco';
import { COMMANDS, CommandError, commandClass, formatReply, isErrorReply, namespacesOf, precheck, splitArgs, type Reply } from './commands';
import { del, exec, put, scan, viewCommand, type Entry } from './api';

const Monaco = lazy(() => import('../editor/Monaco'));

interface LogLine {
  id: number;
  cmd: string;
  out: string;
  error: boolean;
  ms?: number;
}

export function RedisPage() {
  const { tenant = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const me = useSession((s) => s.me);
  const dbs = useDatabases();
  const db = params.get('db') ?? (dbs.data?.includes('redis') ? 'redis' : (dbs.data?.[0] ?? 'redis'));
  const ns = params.get('ns') ?? 'db0';
  const snap = useSnapshot(db);
  const known = useMemo(() => namespacesOf(snap.data?.objects ?? []), [snap.data]);
  const set = (k: string, v: string) => {
    const p = new URLSearchParams(params);
    p.set(k, v);
    setParams(p, { replace: true });
  };
  const [text, setText] = useState('PING\n');
  const [log, setLog] = useState<LogLine[]>([]);
  const [busy, setBusy] = useState(false);
  const [confirm, setConfirm] = useState<{ cmd: string; resolve: (ok: boolean) => void } | null>(null);
  const editor = useRef<MonacoHandle | null>(null);
  const seq = useRef(0);
  const [browseKey, setBrowseKey] = useState(0);

  const completion: CompletionSource = useMemo(
    () => ({
      keywords: Object.keys(COMMANDS),
      objects: () => [],
      columns: () => [],
    }),
    [],
  );

  const append = (l: Omit<LogLine, 'id'>) => setLog((x) => [...x.slice(-499), { ...l, id: seq.current++ }]);

  const runLines = async (lines: string[]) => {
    if (!me) return;
    setBusy(true);
    try {
      for (const raw of lines) {
        const line = raw.trim();
        if (!line || line.startsWith('#')) continue;
        let args: string[];
        try {
          args = splitArgs(line);
        } catch (e) {
          append({ cmd: line, out: `(error) ${e instanceof CommandError ? e.message : String(e)}`, error: true });
          return;
        }
        const pre = precheck(args);
        if (pre) {
          append({ cmd: line, out: `(error) ${pre}`, error: true });
          return;
        }
        const cls = commandClass(args);
        const mode = me.studio.confirm_writes;
        if (cls === 'danger' || (cls === 'write' && mode === 'all')) {
          const ok = await new Promise<boolean>((resolve) => setConfirm({ cmd: line, resolve }));
          setConfirm(null);
          if (!ok) {
            append({ cmd: line, out: '(not run)', error: false });
            return;
          }
        }
        const t0 = performance.now();
        try {
          const r: Reply = await exec(db, ns, args);
          append({ cmd: line, out: formatReply(r), error: isErrorReply(r), ms: Math.round(performance.now() - t0) });
          if (isErrorReply(r)) return;
          if (cls !== 'read') setBrowseKey((k) => k + 1);
        } catch (e) {
          append({ cmd: line, out: `(error) ${errorText(e)}`, error: true });
          return;
        }
      }
    } finally {
      setBusy(false);
    }
  };

  const runCurrent = () => {
    const sel = editor.current?.selection() ?? '';
    if (sel.trim()) return void runLines(sel.split('\n'));
    const at = editor.current?.offset() ?? 0;
    const start = text.lastIndexOf('\n', Math.max(0, at - 1)) + 1;
    const end = text.indexOf('\n', at);
    void runLines([text.slice(start, end < 0 ? text.length : end)]);
  };

  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Redis</h1>
      <div className="row">
        <label className="row">
          Database
          <select value={db} onChange={(e) => set('db', e.target.value)} data-testid="kv-db">
            {!(dbs.data ?? []).includes(db) && <option value={db}>{db}</option>}
            {(dbs.data ?? []).map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <label className="row">
          Namespace
          <input
            list="kv-namespaces"
            value={ns}
            onChange={(e) => set('ns', e.target.value)}
            size={12}
            data-testid="kv-ns"
            aria-describedby="kv-ns-hint"
          />
          <datalist id="kv-namespaces">
            {known.map((n) => (
              <option key={n} value={n} />
            ))}
          </datalist>
        </label>
        <span id="kv-ns-hint" className="muted">
          Redis database n is namespace db{'<n>'} in database redis unless mapped. A namespace is created on first use.
        </span>
      </div>
      <div className="kv-layout">
        <section className="stack" aria-labelledby="console-h">
          <h2 id="console-h">Console</h2>
          <div className="row">
            <button className="primary" onClick={runCurrent} disabled={busy} title="Ctrl/⌘ + Enter" data-testid="kv-run">
              Run line
            </button>
            <button onClick={() => void runLines(text.split('\n'))} disabled={busy}>
              Run all
            </button>
            <button onClick={() => setLog([])}>Clear output</button>
          </div>
          <Suspense fallback={<div className="editor-host muted">Loading editor…</div>}>
            <Monaco
              value={text}
              onChange={setText}
              onRun={runCurrent}
              completion={completion}
              language="redis"
              label="Redis commands, one per line"
              handle={(h) => {
                editor.current = h;
              }}
            />
          </Suspense>
          <div className="kv-log mono" role="log" aria-live="polite" aria-label="Replies" data-testid="kv-log" tabIndex={0}>
            {log.map((l) => (
              <div key={l.id} className="kv-entry">
                <div className="kv-cmd">
                  {db}/{ns}&gt; {l.cmd}
                  {l.ms !== undefined && <span className="muted"> ({l.ms} ms)</span>}
                </div>
                <pre className={l.error ? 'error' : ''}>{l.out}</pre>
              </div>
            ))}
          </div>
        </section>
        <KeyBrowser db={db} ns={ns} refresh={browseKey} onOpen={(k) => void runLines([`TYPE ${quote(k)}`])} />
      </div>
      <dialog open={!!confirm} aria-labelledby="kv-confirm-h" className="modal">
        {confirm && (
          <div className="stack">
            <h2 id="kv-confirm-h">Run this command?</h2>
            <p>
              It changes data in {me?.principal.tenant} / {db}.{ns}.
            </p>
            <pre>{confirm.cmd}</pre>
            <div className="row">
              <button className="primary" onClick={() => confirm.resolve(true)} data-testid="kv-confirm">
                Run
              </button>
              <button autoFocus onClick={() => confirm.resolve(false)}>
                Cancel
              </button>
            </div>
          </div>
        )}
      </dialog>
      <p className="muted">
        <button className="link" onClick={() => void navigate(`/t/${encodeURIComponent(tenant)}`)}>
          Back to the explorer
        </button>
      </p>
    </div>
  );
}

/** A key as a console argument. */
export function quote(k: string): string {
  return /^[^\s"'\\]+$/.test(k) ? k : `"${k.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')}"`;
}

function ttlText(e: Entry): string {
  const at = Number(e.expires_at_ms);
  if (!at) return 'no TTL';
  const s = Math.max(0, Math.round((at - Date.now()) / 1000));
  return `${s} s`;
}

/** Keys by prefix, a page at a time, and the selected key's value. */
function KeyBrowser({ db, ns, refresh, onOpen }: Readonly<{ db: string; ns: string; refresh: number; onOpen: (k: string) => void }>) {
  const [prefix, setPrefix] = useState('');
  const [pages, setPages] = useState<string[]>(['']);
  const token = pages[pages.length - 1] ?? '';
  const q = useQuery({
    queryKey: ['kv-scan', db, ns, prefix, token, refresh],
    queryFn: () => scan(db, ns, prefix, token, 50),
    enabled: !!db && !!ns,
  });
  const [sel, setSel] = useState<Entry | null>(null);
  return (
    <section className="stack" aria-labelledby="keys-h">
      <h2 id="keys-h">Keys</h2>
      <label className="row">
        Prefix
        <input
          value={prefix}
          onChange={(e) => {
            setPrefix(e.target.value);
            setPages(['']);
          }}
          data-testid="kv-prefix"
        />
      </label>
      {q.error && (
        <p className="error" role="alert">
          {errorText(q.error)}
        </p>
      )}
      <table className="table" data-testid="kv-keys">
        <thead>
          <tr>
            <th>Key</th>
            <th>Type</th>
            <th>TTL</th>
          </tr>
        </thead>
        <tbody>
          {(q.data?.entries ?? []).map((e) => (
            <tr key={e.key} aria-selected={sel?.key === e.key}>
              <td>
                <button className="link mono" onClick={() => setSel(e)}>
                  {e.key}
                </button>
              </td>
              <td>{e.type}</td>
              <td>{ttlText(e)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="row">
        <button disabled={pages.length < 2} onClick={() => setPages((p) => p.slice(0, -1))}>
          Previous
        </button>
        <button disabled={!q.data?.next_page_token} onClick={() => setPages((p) => [...p, q.data!.next_page_token])}>
          Next
        </button>
      </div>
      {sel && <KeyValue key={`${sel.key}/${sel.version}/${refresh}`} db={db} ns={ns} entry={sel} onOpen={onOpen} onChanged={() => setSel(null)} />}
    </section>
  );
}

function KeyValue({ db, ns, entry, onOpen, onChanged }: Readonly<{ db: string; ns: string; entry: Entry; onOpen: (k: string) => void; onChanged: () => void }>) {
  const view = useQuery({
    queryKey: ['kv-view', db, ns, entry.key, entry.version],
    queryFn: () => exec(db, ns, viewCommand(entry.type, entry.key)),
  });
  const [edit, setEdit] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const save = async () => {
    if (edit === null) return;
    setErr(null);
    try {
      // Saved only if nobody changed the key since it was read (chapter 33's version condition).
      await put(db, ns, entry.key, edit, entry.version);
      setNotice('Saved.');
      onChanged();
    } catch (e) {
      setErr(errorText(e));
    }
  };
  const remove = async () => {
    setErr(null);
    try {
      const r = await del(db, ns, entry.key, entry.version);
      setNotice(r.deleted ? 'Deleted.' : 'Not deleted: it changed since it was read.');
      if (r.deleted) onChanged();
    } catch (e) {
      setErr(errorText(e));
    }
  };
  return (
    <div className="card stack" data-testid="kv-value">
      <h3 className="mono" style={{ margin: 0, wordBreak: 'break-all' }}>
        {entry.key}
      </h3>
      <dl className="kv">
        <dt>Type</dt>
        <dd>{entry.type}</dd>
        <dt>Version</dt>
        <dd className="mono">{entry.version}</dd>
        <dt>TTL</dt>
        <dd>{ttlText(entry)}</dd>
      </dl>
      {entry.type === 'string' && edit === null && (
        <pre data-testid="kv-string">{entry.text ?? `(binary, base64) ${entry.value ?? ''}`}</pre>
      )}
      {entry.type !== 'string' && <pre data-testid="kv-structure">{view.data !== undefined ? formatReply(view.data) : view.error ? errorText(view.error) : '…'}</pre>}
      {edit !== null && (
        <textarea aria-label="Value" rows={4} value={edit} onChange={(e) => setEdit(e.target.value)} data-testid="kv-edit" />
      )}
      <div className="row">
        {entry.type === 'string' && entry.text !== undefined && edit === null && <button onClick={() => setEdit(entry.text ?? '')}>Edit</button>}
        {edit !== null && (
          <>
            <button className="primary" onClick={() => void save()} data-testid="kv-save">
              Save if unchanged
            </button>
            <button onClick={() => setEdit(null)}>Cancel</button>
          </>
        )}
        <button onClick={() => onOpen(entry.key)}>TYPE in console</button>
        <button onClick={() => void remove()}>Delete if unchanged</button>
      </div>
      {notice && (
        <p role="status" className="muted">
          {notice}
        </p>
      )}
      {err && (
        <p role="alert" className="error">
          {err}
        </p>
      )}
    </div>
  );
}
