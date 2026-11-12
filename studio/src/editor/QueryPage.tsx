import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { loadTabs, updateTab, useTabs, type Tab } from './tabs';
import { recordRun, saveQuery } from '../workspace/library';
import { graphsOf, invalidateCatalog, useDatabases, useSnapshot } from '../catalog/snapshot';
import { classify, needsConfirmation, splitStatements, statementAt, type Statement } from '../lib/sql';
import { runStatement, type RunHandle } from './runner';
import { ResultView, type ResultItem } from '../results/ResultView';
import { errorText, isApiError } from '../api/errors';
import { check, execute, rows as rowsOf, type QueryLanguage } from '../api/sql';
import type { CompletionSource, EditorLanguage, MonacoHandle } from './Monaco';
import { KEYWORDS } from './keywords';
import { classifyCypher, cypherAt, splitCypher, CYPHER_KEYWORDS } from '../graph/cypher';
import { mongoAt, MongoshError, splitMongo, toCommand } from '../docs/mongosh';
import { MONGO_WORDS } from '../docs/words';

const EDITOR_LANG: Record<QueryLanguage, EditorLanguage> = { sql: 'mysql', cypher: 'cypher', gql: 'cypher', mongo: 'javascript' };

/** The distinct names of two lists, sorted. */
export function union(a: readonly string[] | undefined, b: readonly string[] | undefined): string[] {
  return [...new Set([...(a ?? []), ...(b ?? [])])].sort();
}

/** Where a tab's statements run: Bolt's `db.graph` for graph languages. */
export function targetOf(t: Pick<Tab, 'db' | 'graph' | 'language'>): string | null {
  if (t.language === 'cypher' || t.language === 'gql') {
    if (t.graph) return t.db ? `${t.db}.${t.graph}` : t.graph;
  }
  return t.db;
}

/** The first command name's value: the collection of most commands. */
function collectionOf(cmd: Record<string, unknown>): string | undefined {
  const v = Object.values(cmd)[0];
  return typeof v === 'string' ? v : undefined;
}

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

  const lang: QueryLanguage = tab?.language ?? 'sql';
  const target = tab ? targetOf(tab) : null;
  // Labels, relationship types and property keys of the tab's graph.
  const graphMeta = useQuery({
    queryKey: ['graph-meta', target],
    enabled: (lang === 'cypher' || lang === 'gql') && !!tab?.graph,
    queryFn: async () => {
      const list = async (q: string) => {
        try {
          const r = await execute(target, q, { language: 'cypher', maxRows: 500 });
          return (r.results[0]?.rows ?? []).map((row) => row[0] ?? '').filter(Boolean);
        } catch {
          return [];
        }
      };
      const [labels, types, keys] = await Promise.all([
        list('CALL db.labels()'),
        list('CALL db.relationshipTypes()'),
        list('CALL db.propertyKeys()'),
      ]);
      return { labels, types, keys };
    },
    staleTime: 60_000,
  });
  // The tab's graph as the catalog snapshot lists it (labels and types the
  // user may read), which also fills the graph picker.
  const snapGraph = graphsOf(snap.data).find((g) => g.name === tab?.graph);
  const completion: CompletionSource = useMemo(
    () => ({
      keywords: lang === 'sql' ? KEYWORDS : lang === 'mongo' ? MONGO_WORDS : CYPHER_KEYWORDS,
      objects: () =>
        lang === 'cypher' || lang === 'gql'
          ? [
              ...union(graphMeta.data?.labels, snapGraph?.labels).map((l) => ({ name: l, detail: 'label' })),
              ...union(graphMeta.data?.types, snapGraph?.types).map((t) => ({ name: t, detail: 'relationship type' })),
            ]
          : lang === 'mongo'
            ? (snap.data?.objects ?? []).map((o) => ({ name: o.name, detail: o.kind }))
            : [
        ...(dbs.data ?? []).map((d) => ({ name: d, detail: 'database' })),
                ...(snap.data?.objects ?? []).map((o) => ({ name: o.name, detail: o.kind })),
              ],
      columns: () =>
        lang === 'cypher' || lang === 'gql'
          ? (graphMeta.data?.keys ?? []).map((k) => ({ name: k, detail: 'property key' }))
          : Object.entries(snap.data?.columns ?? {}).flatMap(([t, cols]) =>
              cols.map((c) => ({ name: c.name, detail: `${t}.${c.name} ${c.type}` })),
            ),
    }),
    [dbs.data, snap.data, lang, graphMeta.data, snapGraph],
  );

  // Check markers: Sql.Check 500 ms after typing stops; the gateway parses
  // each statement (positions) and binds reads and writes as the user.
  const sql = tab?.sql ?? '';
  const db = tab?.db ?? null;
  useEffect(() => {
    if (!sql.trim() || lang === 'cypher' || lang === 'gql') {
      editor.current?.setMarkers([]);
      return;
    }
    if (lang === 'mongo') {
      // MongoDB statements are checked here: parsed, never evaluated.
      const t = setTimeout(() => {
        const marks: [number, number, string, boolean][] = [];
        for (const st of splitMongo(sql)) {
          try {
            toCommand(st.text);
          } catch (e) {
            if (e instanceof MongoshError) marks.push([st.start + e.start, st.start + Math.max(e.end, e.start + 1), e.message, false]);
          }
        }
        editor.current?.setMarkers(marks);
      }, 300);
      return () => clearTimeout(t);
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
  }, [sql, db, lang]);

  const askConfirm = (s: string, kind: string) =>
    new Promise<boolean>((resolve) => setConfirm({ sql: s, kind, resolve }));

  const run = useCallback(
    async (all: boolean) => {
      if (!tab || !me || phase === 'streaming' || phase === 'checking') return;
      const sel = editor.current?.selection() ?? '';
      const tl = tab.language ?? 'sql';
      const split = tl === 'sql' ? splitStatements : tl === 'mongo' ? splitMongo : splitCypher;
      const atFn = tl === 'sql' ? statementAt : tl === 'mongo' ? mongoAt : cypherAt;
      let stmts: Statement[];
      if (all) stmts = split(tab.sql);
      else if (sel.trim()) stmts = split(sel);
      else {
        const at = atFn(tab.sql, editor.current?.offset() ?? 0);
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
      const log = (ok: boolean, err?: string) =>
        recordRun({
          sql: stmts.map((x) => x.text.trim()).join(';\n'),
          db: tab.db,
          language: tl,
          ms: Math.round(performance.now() - started),
          ok,
          error: err,
        });
      const collected: ResultItem[] = [];
      let ddl = false;
      for (const s of stmts) {
        if (stopAll.current) break;
        let cls = classify(s.text);
        let send = s.text;
        let where = targetOf(tab);
        let collection: string | undefined;
        if (tl === 'cypher' || tl === 'gql') cls = classifyCypher(s.text);
        if (tl === 'mongo') {
          try {
            const c = toCommand(s.text);
            cls = c.cls;
            send = JSON.stringify(c.command);
            where = c.db ?? tab.db;
            collection = collectionOf(c.command);
          } catch (e) {
            if (e instanceof MongoshError) {
              const full = all || !sel.trim() ? s.start : 0;
              editor.current?.setMarkers([[full + e.start, full + Math.max(e.end, e.start + 1), e.message, false]]);
              setError({ text: `${e.message} (Studio parses MongoDB commands and never runs code)` });
              setPhase('failed');
              return;
            }
            throw e;
          }
        }
        if (needsConfirmation(cls, me.studio.confirm_writes)) {
          setPhase('checking');
          const ok = await askConfirm(tl === 'mongo' ? `${s.text}\n\n${JSON.stringify(JSON.parse(send), null, 2)}` : s.text, cls);
          setConfirm(null);
          if (!ok) {
            setPhase('cancelled');
            return;
          }
        }
        setPhase('streaming');
        setLastSql(send);
        const h = runStatement(where, send, { ...caps, language: tl, collection }, (its) => {
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
            log(false, 'timeout');
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
          log(false, errorText(e));
          if (isApiError(e) && (e.code === 1146 || e.code === 1049)) invalidateCatalog(qc);
          return;
        } finally {
          running.current = null;
        }
      }
      if (ddl) invalidateCatalog(qc);
      setElapsed(Math.round(performance.now() - started));
      setPhase('done');
      log(true);
    },
    [tab, me, phase, qc],
  );

  const [saving, setSaving] = useState<string | null>(null);
  const [savedNote, setSavedNote] = useState(false);
  const title = tab?.title;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key === 's') {
        e.preventDefault();
        setSaving((v) => v ?? title ?? '');
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [title]);

  const cancel = () => {
    stopAll.current = true;
    running.current?.cancel();
  };

  const doExplain = async () => {
    if (!tab) return;
    const sel = editor.current?.selection() ?? '';
    const tl = tab.language ?? 'sql';
    const atFn = tl === 'sql' ? statementAt : tl === 'mongo' ? mongoAt : cypherAt;
    const s = sel.trim() || atFn(tab.sql, editor.current?.offset() ?? 0)?.text || '';
    if (!s) return;
    setError(null);
    if (tl !== 'sql') {
      try {
        let text = `EXPLAIN ${s}`;
        let where = targetOf(tab);
        if (tl === 'mongo') {
          const c = toCommand(s);
          text = JSON.stringify(c.command.explain ? c.command : { explain: c.command, verbosity: 'queryPlanner' });
          where = c.db ?? tab.db;
        }
        const r = await execute(where, text, { language: tl, maxRows: 1000 });
        const res = r.results[0];
        const rowsText = (res?.rows ?? []).map((row) =>
          row
            .map((v) => {
              try {
                return v && (v.startsWith('{') || v.startsWith('[')) ? JSON.stringify(JSON.parse(v), null, 2) : (v ?? 'NULL');
              } catch {
                return v ?? 'NULL';
              }
            })
            .join('\t'),
        );
        setExplain([(res?.columns ?? []).map((c) => c.name).join('\t'), ...rowsText, res?.info ?? ''].filter(Boolean).join('\n'));
      } catch (e) {
        setError({ text: errorText(e), trace: isApiError(e) ? e.traceId : undefined });
      }
      return;
    }
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
  const busy = phase === 'streaming' || phase === 'checking';
  return (
    <div className="stack" style={{ blockSize: '100%' }}>
      <div className="row">
        <label className="row">
          <FormattedMessage id="query.language" />
          <select
            value={lang}
            onChange={(e) => updateTab(tab.id, { language: e.target.value as QueryLanguage })}
            data-testid="lang-select"
          >
            <option value="sql">SQL</option>
            <option value="cypher">Cypher</option>
            <option value="gql">GQL</option>
            <option value="mongo">MongoDB</option>
          </select>
        </label>
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
        {(lang === 'cypher' || lang === 'gql') && (
          <label className="row">
            <FormattedMessage id="query.graph" />
            <input
              value={tab.graph ?? ''}
              onChange={(e) => updateTab(tab.id, { graph: e.target.value.trim() || undefined })}
              placeholder="graph"
              size={14}
              list={`graphs-${tab.id}`}
              data-testid="graph-input"
            />
            <datalist id={`graphs-${tab.id}`} data-testid="graph-list">
              {graphsOf(snap.data).map((g) => (
                <option key={g.name} value={g.name} />
              ))}
            </datalist>
          </label>
        )}
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
        <span className="grow" />
        {saving === null ? (
          <button onClick={() => setSaving(tab.title)} title="Ctrl/⌘ + S" data-testid="save-query">
            <FormattedMessage id="library.save" />
          </button>
        ) : (
          <form
            className="row"
            onSubmit={(e) => {
              e.preventDefault();
              const name = saving.trim();
              if (!name) return;
              saveQuery({ name, sql: tab.sql, db: tab.db, language: tab.language ?? 'sql' });
              updateTab(tab.id, { title: name });
              setSaving(null);
              setSavedNote(true);
              setTimeout(() => setSavedNote(false), 1500);
            }}
          >
            <input
              autoFocus
              value={saving}
              onChange={(e) => setSaving(e.target.value)}
              aria-label={intl.formatMessage({ id: 'library.saveAs' })}
              data-testid="save-name"
              onKeyDown={(e) => {
                if (e.key === 'Escape') setSaving(null);
              }}
            />
            <button type="submit" className="primary">
              <FormattedMessage id="library.save" />
            </button>
          </form>
        )}
        {savedNote && <span className="muted">{intl.formatMessage({ id: 'library.saved' })}</span>}
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
          language={EDITOR_LANG[lang]}
          label={`${lang === 'sql' ? 'SQL' : lang === 'mongo' ? 'MongoDB' : lang === 'gql' ? 'GQL' : 'Cypher'} editor, ${tab.title}`}
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
          <ResultView key={i} item={it} index={i} db={lang === 'sql' || lang === 'mongo' ? tab.db : targetOf(tab)} sql={lastSql} version={version} />
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
