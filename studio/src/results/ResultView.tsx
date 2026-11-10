import { lazy, Suspense, useEffect, useRef, useState } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';
import { Grid } from './Grid';
import type { ResultBuffer } from './buffer';
import { exportAll, type ExportFormat } from './exports';
import { errorText } from '../api/errors';
import { Pager, track } from './cursor';

const Chart = lazy(() => import('./Chart').then((m) => ({ default: m.Chart })));

export interface ResultItem {
  buffer: ResultBuffer | null;
  affected?: string;
  info?: string;
  warnings?: number;
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
  const [mode, setMode] = useState<'grid' | 'chart'>('grid');
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
          <button role="tab" aria-selected={mode === 'chart'} onClick={() => setMode('chart')}>
            <FormattedMessage id="results.chart" />
          </button>
        </div>
        <span style={{ flex: 1 }} />
        <button onClick={() => void doExport('csv')}>{intl.formatMessage({ id: 'results.export' })} CSV</button>
        <button onClick={() => void doExport('ndjson')}>{intl.formatMessage({ id: 'results.export' })} NDJSON</button>
      </div>
      {b.truncated && b.offset === 0 && !paging.done && (
        <p className="banner" role="status" data-testid="truncated">
          <FormattedMessage id="results.truncated" values={{ count: b.rowCount }} />
        </p>
      )}
      {b.truncated && (
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
      {mode === 'grid' ? (
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
