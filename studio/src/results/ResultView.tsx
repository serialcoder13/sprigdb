import { lazy, Suspense, useState } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';
import { Grid } from './Grid';
import type { ResultBuffer } from './buffer';
import { exportAll, type ExportFormat } from './exports';
import { errorText } from '../api/errors';

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
      {b.truncated && (
        <p className="banner" role="status" data-testid="truncated">
          <FormattedMessage id="results.truncated" values={{ count: b.rowCount }} />
        </p>
      )}
      {exportError && <p className="error">{exportError}</p>}
      {mode === 'grid' ? (
        <Grid
          label={`Result ${index + 1}`}
          columns={b.columns.map((c) => c.name)}
          rowCount={b.rowCount}
          cell={(r, c) => b.cell(r, c)}
          numeric={(c) => b.numeric(c)}
        />
      ) : (
        <Suspense fallback={<p className="muted">Loading chart…</p>}>
          <Chart buffer={b} />
        </Suspense>
      )}
    </section>
  );
}
