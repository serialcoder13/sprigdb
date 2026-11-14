import { lazy, Suspense, useEffect, useRef, useState } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';
import { Grid } from './Grid';
import type { ResultBuffer } from './buffer';
import { exportAll, type ExportFormat } from './exports';
import { errorText } from '../api/errors';
import { Pager, track } from './cursor';
import type { QueryLanguage } from '../api/sql';
import { hasGraph } from '../graph/elements';

const Chart = lazy(() => import('./Chart').then((m) => ({ default: m.Chart })));
const GraphResult = lazy(() => import('../graph/GraphResult'));
const DocTree = lazy(() => import('../docs/DocTree'));

export interface ResultItem {
  buffer: ResultBuffer | null;
  affected?: string;
  info?: string;
  warnings?: number;
  /** The statement's language: graphs get a graph view,
   * documents a JSON tree. */
  language?: QueryLanguage;
  /** The collection a MongoDB result came from (for editing documents). */
  collection?: string;
}

/** One result: rows (grid or chart) with the cap notice and export, or
 * the affected-row count of a write. */
export function ResultView({
  item,
  index,
  db,
  sql,
  version,
}: {
  item: ResultItem;
  index: number;
  db: string | null;
  sql: string;
  version: number;
}) {
  const intl = useIntl();
  const lang = item.language ?? 'sql';
  const b0 = item.buffer;
  const graphable = !!b0 && (lang === 'cypher' || lang === 'gql') && hasGraph(b0.rowCount, b0.columns.length, (r, c) => b0.cell(r, c));
  const tree = !!b0 && lang === 'mongo';
  // The view the user picked, else the one the result suits. Worked out on
  // every render: rows stream in after the first one, so a graph found
  // later still opens as a graph.
  const [picked, setMode] = useState<'grid' | 'chart' | 'graph' | 'tree' | null>(null);
  const mode = picked ?? (graphable ? 'graph' : tree ? 'tree' : 'grid');
  const [exportError, setExportError] = useState<string | null>(null);
  const b = item.buffer;
  const pager = useRef<{ p: Pager; untrack: () => void } | null>(null);
  const [paging, setPaging] = useState<{ busy: boolean; done: boolean; reopenedAt: number | null; error: string | null }>({
    busy: false,
    done: false,
    reopenedAt: null,
    error: null,
  });
  const [, redraw] = useState(0);
  // The cursor closes when the result leaves the page or a new run
  // replaces it.
  useEffect(() => {
    return () => {
      pager.current?.untrack();
      pager.current = null;
    };
  }, [b, sql, version]);
  const fetchMore = async () => {
    if (!b) return;
    if (!pager.current) {
      const p = new Pager(db, sql, b, Math.min(b.maxRows, 1000));
      pager.current = { p, untrack: track(p) };
    }
    const { p } = pager.current;
    setPaging((s) => ({ ...s, busy: true, error: null }));
    try {
      const r = await p.more();
      setPaging({ busy: false, done: r.done, reopenedAt: p.reopenedAt, error: null });
      redraw((n) => n + 1);
    } catch (e) {
      setPaging((s) => ({ ...s, busy: false, error: errorText(e) }));
    }
  };
  const doExport = async (f: ExportFormat) => {
    setExportError(null);
    try {
      await exportAll(db, sql, f, 'result');
    } catch (e) {
      setExportError(errorText(e));
    }
  };
  if (!b) {
    return (
      <p role="status" data-testid={`result-${index}`}>
        <FormattedMessage id="results.affected" values={{ count: Number(item.affected ?? 0) }} />
        {item.info ? ` — ${item.info}` : ''}
        {item.warnings ? ` (${item.warnings} warnings)` : ''}
      </p>
    );
  }
  return (
    <section className="stack" aria-label={`Result ${index + 1}`} data-testid={`result-${index}`}>
      <div className="row">
        <span role="status" data-version={version}>
          <FormattedMessage id="results.rows" values={{ count: b.rowCount }} />
        </span>
        <div className="row" role="tablist" aria-label="View">
          <button role="tab" aria-selected={mode === 'grid'} onClick={() => setMode('grid')}>
            <FormattedMessage id="results.grid" />
          </button>
          {graphable && (
            <button role="tab" aria-selected={mode === 'graph'} onClick={() => setMode('graph')} data-testid="tab-graph">
              <FormattedMessage id="results.graph" />
            </button>
          )}
          {tree && (
            <button role="tab" aria-selected={mode === 'tree'} onClick={() => setMode('tree')} data-testid="tab-tree">
              <FormattedMessage id="results.tree" />
            </button>
          )}
          {lang === 'sql' && (
            <button role="tab" aria-selected={mode === 'chart'} onClick={() => setMode('chart')}>
              <FormattedMessage id="results.chart" />
            </button>
          )}
        </div>
        <span style={{ flex: 1 }} />
        {lang === 'sql' && (
          <>
            <button onClick={() => void doExport('csv')}>{intl.formatMessage({ id: 'results.export' })} CSV</button>
            <button onClick={() => void doExport('ndjson')}>{intl.formatMessage({ id: 'results.export' })} NDJSON</button>
          </>
        )}
      </div>
      {b.truncated && b.offset === 0 && !paging.done && (
        <p className="banner" role="status" data-testid="truncated">
          <FormattedMessage id="results.truncated" values={{ count: b.rowCount }} />
        </p>
      )}
      {b.truncated && lang === 'sql' && (
        <div className="row">
          {!paging.done && (
            <button onClick={() => void fetchMore()} disabled={paging.busy} data-testid="fetch-more">
              <FormattedMessage id="results.fetchMore" />
            </button>
          )}
          {b.offset > 0 && (
            <span className="muted" role="status" data-testid="window">
              <FormattedMessage
                id="results.window"
                values={{ from: b.offset + 1, to: b.offset + b.rowCount, cap: b.maxRows }}
              />
            </span>
          )}
          {paging.done && (
            <span className="muted" role="status" data-testid="all-fetched">
              <FormattedMessage id="results.allFetched" />
            </span>
          )}
        </div>
      )}
      {paging.reopenedAt !== null && (
        <p className="banner" role="status" data-testid="reopened">
          <FormattedMessage id="results.reopened" values={{ row: paging.reopenedAt + 1 }} />
        </p>
      )}
      {paging.error && <p className="error">{paging.error}</p>}
      {exportError && <p className="error">{exportError}</p>}
      {mode === 'graph' ? (
        <Suspense fallback={<p className="muted">Loading graph view…</p>}>
          <GraphResult buffer={b} rows={b.rowCount} db={db} label={`Result ${index + 1}`} language={lang === 'gql' ? 'gql' : 'cypher'} />
        </Suspense>
      ) : mode === 'tree' ? (
        <Suspense fallback={<p className="muted">Loading tree…</p>}>
          <DocTree buffer={b} rows={b.rowCount} label={`Result ${index + 1}`} db={db} collection={item.collection} />
        </Suspense>
      ) : mode === 'grid' ? (
        <Grid
          label={`Result ${index + 1}`}
          columns={b.columns.map((c) => c.name)}
          rowCount={b.rowCount}
          cell={(r, c) => b.cell(r, c)}
          numeric={(c) => b.numeric(c)}
          firstRow={b.offset}
        />
      ) : (
        <Suspense fallback={<p className="muted">Loading chart…</p>}>
          <Chart buffer={b} />
        </Suspense>
      )}
    </section>
  );
}
