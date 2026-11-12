import { useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { addTab } from '../editor/tabs';
import { X } from '../icons';
import { clearHistory, deleteSaved, useLibrary, type HistoryItem, type SavedQuery } from './library';

function firstLine(sql: string): string {
  const line = sql.trim().split('\n')[0] ?? '';
  return line.length > 80 ? `${line.slice(0, 80)}…` : line;
}

function ago(at: number): string {
  const s = Math.max(0, Math.round((Date.now() - at) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 86400) return `${Math.round(s / 3600)} h ago`;
  return new Date(at).toLocaleDateString();
}

function useOpen() {
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  return (q: { sql: string; db: string | null; language: SavedQuery['language']; name?: string }) => {
    const t = addTab({ sql: q.sql, db: q.db, language: q.language, title: q.name ?? firstLine(q.sql).slice(0, 24) });
    void navigate(`/t/${encodeURIComponent(tenant)}/q/${t.id}`);
  };
}

/** Where the library lives, when it isn't the catalog, and the last failed write. */
function PersistNote() {
  const intl = useIntl();
  const where = useLibrary((s) => s.where);
  const error = useLibrary((s) => s.error);
  return (
    <>
      {(where === 'browser' || where === 'session') && (
        <p className="muted small" data-testid="library-where">
          {intl.formatMessage({ id: where === 'browser' ? 'library.browserOnly' : 'library.sessionOnly' })}
        </p>
      )}
      {error && (
        <p className="error small" role="alert">
          {intl.formatMessage({ id: 'library.writeFailed' }, { error })}
        </p>
      )}
    </>
  );
}

/** The ★ panel: saved queries, filtered by name or text; double-click or Enter opens one. */
export function SavedPanel() {
  const intl = useIntl();
  const saved = useLibrary((s) => s.saved);
  const open = useOpen();
  const [f, setF] = useState('');
  const shown = useMemo(() => {
    const q = f.toLowerCase();
    return q ? saved.filter((s) => s.name.toLowerCase().includes(q) || s.sql.toLowerCase().includes(q)) : saved;
  }, [saved, f]);
  return (
    <div className="stack library">
      <input
        type="search"
        placeholder={intl.formatMessage({ id: 'library.filter' })}
        aria-label={intl.formatMessage({ id: 'library.filter' })}
        value={f}
        onChange={(e) => setF(e.target.value)}
      />
      <PersistNote />
      {saved.length === 0 && <p className="muted">{intl.formatMessage({ id: 'library.noneSaved' })}</p>}
      <ul className="library-list" data-testid="saved-list">
        {shown.map((s) => (
          <li key={s.id}>
            <button className="library-item" onClick={() => open(s)} title={s.sql}>
              <span className="library-name">{s.name}</span>
              <span className="library-sub mono">{firstLine(s.sql)}</span>
            </button>
            <button
              className="icon-button"
              aria-label={`${intl.formatMessage({ id: 'library.delete' })} ${s.name}`}
              onClick={() => deleteSaved(s.id)}
            >
              <X size={14} />
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

/** The history panel: every run, newest first, with outcome and timing. */
export function HistoryPanel() {
  const intl = useIntl();
  const history = useLibrary((s) => s.history);
  const open = useOpen();
  const [f, setF] = useState('');
  const shown = useMemo(() => {
    const q = f.toLowerCase();
    return q ? history.filter((h) => h.sql.toLowerCase().includes(q) || (h.db ?? '').toLowerCase().includes(q)) : history;
  }, [history, f]);
  const meta = (h: HistoryItem) =>
    [h.db ?? '—', `${h.ms} ms`, ago(h.at), ...(h.client === 'shell' ? ['sprig'] : [])].join(' · ');
  return (
    <div className="stack library">
      <div className="row">
        <input
          type="search"
          style={{ flex: 1 }}
          placeholder={intl.formatMessage({ id: 'library.filterHistory' })}
          aria-label={intl.formatMessage({ id: 'library.filterHistory' })}
          value={f}
          onChange={(e) => setF(e.target.value)}
        />
        <button className="ghost" disabled={!history.length} onClick={clearHistory}>
          {intl.formatMessage({ id: 'library.clear' })}
        </button>
      </div>
      <PersistNote />
      {history.length === 0 && <p className="muted">{intl.formatMessage({ id: 'library.noHistory' })}</p>}
      <ul className="library-list" data-testid="history-list">
        {shown.map((h) => (
          <li key={h.id}>
            <button className="library-item" onClick={() => open(h)} title={h.error ?? h.sql}>
              <span className={`library-name mono${h.ok ? '' : ' error'}`}>{firstLine(h.sql)}</span>
              <span className="library-sub">{meta(h)}</span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
