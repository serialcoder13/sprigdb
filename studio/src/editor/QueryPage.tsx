import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { useQueryClient } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { addTab, closeTab, loadTabs, updateTab, useTabs } from './tabs';
import { invalidateCatalog, useDatabases, useSnapshot } from '../catalog/snapshot';
import { classify, needsConfirmation, splitStatements, statementAt, type Statement } from '../lib/sql';
import { runStatement, type RunHandle } from './runner';
import { ResultView, type ResultItem } from '../results/ResultView';
import { errorText, isApiError } from '../api/errors';
import { check, rows as rowsOf } from '../api/sql';
import type { CompletionSource, MonacoHandle } from './Monaco';
import { KEYWORDS } from './keywords';

const Monaco = lazy(() => import('./Monaco'));

type Phase = 'idle' | 'checking' | 'streaming' | 'done' | 'cancelled' | 'failed';

interface Confirm {
  sql: string;
  kind: string;
  resolve(ok: boolean): void;
}

/** The SQL editor (chapter 44 step 8) with its results (step 9). */
export function QueryPage() {
  const intl = useIntl();
  const { tenant = '', tab: tabId = '' } = useParams();
  const navigate = useNavigate();
  const me = useSession((s) => s.me);
  const tabs = useTabs((s) => s.tabs);
  const tab = tabs.find((t) => t.id === tabId);
  const dbs = useDatabases();
  const snap = useSnapshot(tab?.db);
  const qc = useQueryClient();
  const editor = useRef<MonacoHandle | null>(null);
  const [phase, setPhase] = useState<Phase>('idle');
  const [items, setItems] = useState<ResultItem[]>([]);
  const [version, setVersion] = useState(0);
  const [error, setError] = useState<{ text: string; trace?: string } | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [lastSql, setLastSql] = useState('');
  const [explain, setExplain] = useState<string | null>(null);
  const [confirm, setConfirm] = useState<Confirm | null>(null);
  const running = useRef<RunHandle | null>(null);
  const stopAll = useRef(false);
  const dialog = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    if (me) void loadTabs(me.principal.tenant ?? '-', me.principal.user, me.studio.persist_drafts);
  }, [me]);
  useEffect(() => {
    if (!tab && tabs[0]) void navigate(`/t/${encodeURIComponent(tenant)}/q/${tabs[0].id}`, { replace: true });
  }, [tab, tabs, tenant, navigate]);
  useEffect(() => {
    const d = dialog.current;
    if (!d) return;
    if (confirm && !d.open) d.showModal?.();
    if (!confirm && d.open) d.close();
  }, [confirm]);

  const completion: CompletionSource = useMemo(
    () => ({
      keywords: KEYWORDS,
      objects: () => [
        ...(dbs.data ?? []).map((d) => ({ name: d, detail: 'database' })),
        ...(snap.data?.objects ?? []).map((o) => ({ name: o.name, detail: o.kind })),
      ],
      columns: () =>
        Object.entries(snap.data?.columns ?? {}).flatMap(([t, cols]) =>
          cols.map((c) => ({ name: c.name, detail: `${t}.${c.name} ${c.type}` })),
        ),
    }),
    [dbs.data, snap.data],
  );

  // Check markers: Sql.Check 500 ms after typing stops; the gateway parses
  // each statement (positions) and binds reads and writes as the user.
  const sql = tab?.sql ?? '';
  const db = tab?.db ?? null;
  useEffect(() => {
    if (!sql.trim()) {
      editor.current?.setMarkers([]);
      return;
    }
    const ctl = new AbortController();
    const t = setTimeout(() => {
      void (async () => {
        try {
          const checked = await check(db, sql, ctl.signal);
          if (ctl.signal.aborted) return;
          const marks: [number, number, string, boolean][] = checked.flatMap((st) =>
            st.diagnostics.map(
              (d) =>
                [
                  d.start,
                  Math.max(d.end, d.start + 1),
                  d.code ? `${d.code}${d.name ? ` ${d.name}` : ''}: ${d.message}` : d.message,
                  d.severity === 'warning',
                ] as [number, number, string, boolean],
            ),
          );
          editor.current?.setMarkers(marks);
        } catch {
          // A failed check leaves the markers as they were; running decides.
        }
      })();
    }, 500);
    return () => {
      clearTimeout(t);
      ctl.abort();
    };
  }, [sql, db]);

  const askConfirm = (s: string, kind: string) =>
    new Promise<boolean>((resolve) => setConfirm({ sql: s, kind, resolve }));

  const run = useCallback(
    async (all: boolean) => {
      if (!tab || !me || phase === 'streaming' || phase === 'checking') return;
      const sel = editor.current?.selection() ?? '';
      let stmts: Statement[];
      if (all) stmts = splitStatements(tab.sql);
      else if (sel.trim()) stmts = splitStatements(sel);
      else {
        const at = statementAt(tab.sql, editor.current?.offset() ?? 0);
        stmts = at ? [at] : [];
      }
      if (!stmts.length) return;
      setError(null);
      setExplain(null);
      setItems([]);
      stopAll.current = false;
      const caps = {
        maxRows: me.studio.max_result_rows,
        maxBytes: me.studio.max_result_mb * 1024 * 1024,
        timeoutMs: me.studio.statement_timeout_ms,
      };
      const started = performance.now();
      const collected: ResultItem[] = [];
      let ddl = false;
      for (const s of stmts) {
        if (stopAll.current) break;
        const cls = classify(s.text);
        if (needsConfirmation(cls, me.studio.confirm_writes)) {
          setPhase('checking');
          const ok = await askConfirm(s.text, cls);
          setConfirm(null);
          if (!ok) {
            setPhase('cancelled');
            return;
          }
        }
        setPhase('streaming');
        setLastSql(s.text);
        const h = runStatement(tab.db, s.text, caps, (its) => {
          setItems([...collected, ...its]);
          setVersion((v) => v + 1);
        });
        running.current = h;
        try {
          const r = await h.done;
          collected.push(...r.items);
          setItems([...collected]);
          if (r.outcome === 'timeout') {
            setError({ text: `Stopped at studio.statement_timeout_ms (${caps.timeoutMs} ms).` });
            setPhase('failed');
            return;
          }
          if (r.outcome === 'cancelled' && !r.items.some((i) => i.buffer?.truncated)) {
            setPhase('cancelled');
            return;
          }
          if (cls === 'ddl' || cls === 'account') ddl = true;
        } catch (e) {
          setError({ text: errorText(e), trace: isApiError(e) ? e.traceId : undefined });
          setPhase('failed');
          if (isApiError(e) && (e.code === 1146 || e.code === 1049)) invalidateCatalog(qc);
          return;
        } finally {
          running.current = null;
        }
      }
      if (ddl) invalidateCatalog(qc);
      setElapsed(Math.round(performance.now() - started));
      setPhase('done');
    },
    [tab, me, phase, qc],
  );

  const cancel = () => {
    stopAll.current = true;
    running.current?.cancel();
  };

  const doExplain = async () => {
    if (!tab) return;
    const sel = editor.current?.selection() ?? '';
    const s = sel.trim() || statementAt(tab.sql, editor.current?.offset() ?? 0)?.text || '';
    if (!s) return;
    setError(null);
    try {
      const r = await rowsOf(tab.db, `EXPLAIN FORMAT=JSON ${s}`);
      const text = r.rows[0]?.[0] ?? '';
      try {
        setExplain(JSON.stringify(JSON.parse(text), null, 2));
      } catch {
        setExplain(text);
      }
    } catch {
      try {
        const r = await rowsOf(tab.db, `EXPLAIN ${s}`);
        setExplain(
          [r.columns.map((c) => c.name).join('\t'), ...r.rows.map((row) => row.map((v) => v ?? 'NULL').join('\t'))].join('\n'),
        );
      } catch (e) {
        setError({ text: errorText(e), trace: isApiError(e) ? e.traceId : undefined });
      }
    }
  };

  if (!tab) return <p className="muted">…</p>;
  const base = `/t/${encodeURIComponent(tenant)}/q/`;
  const busy = phase === 'streaming' || phase === 'checking';
  return (
    <div className="stack" style={{ blockSize: '100%' }}>
      <div className="tabs" role="toolbar" aria-label="Editor tabs">
        {tabs.map((t) => (
          <span key={t.id} className="row" style={{ gap: 0 }}>
            <button aria-pressed={t.id === tab.id} onClick={() => void navigate(base + t.id)}>
              {t.title}
            </button>
            {tabs.length > 1 && (
              <button
                aria-label={`${intl.formatMessage({ id: 'query.closeTab' })} ${t.title}`}
                onClick={() => void navigate(base + closeTab(t.id))}
              >
                ×
              </button>
            )}
          </span>
        ))}
        <button aria-label={intl.formatMessage({ id: 'query.newTab' })} onClick={() => void navigate(base + addTab().id)}>
          +
        </button>
      </div>
      <div className="row">
        <label className="row">
          <FormattedMessage id="query.database" />
          <select
            value={tab.db ?? ''}
            onChange={(e) => updateTab(tab.id, { db: e.target.value || null })}
            data-testid="db-select"
          >
            <option value="">{intl.formatMessage({ id: 'query.noDatabase' })}</option>
            {(dbs.data ?? []).map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <button className="primary" onClick={() => void run(false)} disabled={busy} title="Ctrl/⌘ + Enter">
          <FormattedMessage id="query.run" />
        </button>
        <button onClick={() => void run(true)} disabled={busy}>
          <FormattedMessage id="query.runAll" />
        </button>
        <button onClick={() => void doExplain()} disabled={busy}>
          <FormattedMessage id="query.explain" />
        </button>
        <button onClick={cancel} disabled={!busy}>
          <FormattedMessage id="query.cancel" />
        </button>
        <span className="muted" role="status" data-testid="phase">
          {phase === 'streaming' ? 'Running…' : phase === 'cancelled' ? intl.formatMessage({ id: 'results.cancelled' }) : ''}
          {phase === 'done' && elapsed !== null ? intl.formatMessage({ id: 'results.elapsed' }, { ms: elapsed }) : ''}
        </span>
      </div>
      <Suspense fallback={<div className="editor-host muted">Loading editor…</div>}>
        <Monaco
          value={tab.sql}
          onChange={(v) => updateTab(tab.id, { sql: v })}
          onRun={() => void run(false)}
          completion={completion}
          label={`SQL editor, ${tab.title}`}
          handle={(h) => {
            editor.current = h;
          }}
        />
      </Suspense>
      {error && (
        <div className="error" role="alert" data-testid="query-error">
          {error.text}
          {error.trace && <span className="muted"> (trace {error.trace})</span>}
        </div>
      )}
      {explain && <pre aria-label="Explain">{explain}</pre>}
      <div className="stack" aria-live="polite">
        {items.length === 0 && phase === 'idle' && (
          <p className="muted">
            <FormattedMessage id="results.empty" />
          </p>
        )}
        {items.map((it, i) => (
          <ResultView key={i} item={it} index={i} db={tab.db} sql={lastSql} version={version} />
        ))}
      </div>
      <dialog ref={dialog} aria-labelledby="confirm-title" onCancel={() => confirm?.resolve(false)}>
        {confirm && (
          <div className="stack">
            <h2 id="confirm-title">
              <FormattedMessage id="query.confirmTitle" />
            </h2>
            <p>
              <FormattedMessage
                id="query.confirmBody"
                values={{ kind: confirm.kind, target: `${me?.principal.tenant ?? ''}${tab.db ? ` / ${tab.db}` : ''}` }}
              />
            </p>
            <pre>{confirm.sql}</pre>
            <div className="row">
              <button className="primary" onClick={() => confirm.resolve(true)}>
                <FormattedMessage id="query.confirm" />
              </button>
              <button autoFocus onClick={() => confirm.resolve(false)}>
                <FormattedMessage id="query.cancelConfirm" />
              </button>
            </div>
          </div>
        )}
      </dialog>
    </div>
  );
}
