import { useState } from 'react';
import { useIntl } from 'react-intl';
import { Plus, X } from '../icons';
import { filterSql, NO_VALUE, OPS, type Condition, type Op } from './filter';

/** Conditions on columns, or a raw WHERE clause (`</> WHERE`). Calls
 * `onApply` with the WHERE expression, or null to show every row. */
export function FilterBar({ columns, onApply }: { columns: string[]; onApply: (where: string | null) => void }) {
  const intl = useIntl();
  const [raw, setRaw] = useState(false);
  const [text, setText] = useState('');
  const [conds, setConds] = useState<Condition[]>([{ col: columns[0] ?? '', op: '=', value: '' }]);
  const t = (id: string) => intl.formatMessage({ id });
  const set = (i: number, patch: Partial<Condition>) => setConds(conds.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const apply = () => onApply(raw ? text.trim() || null : filterSql(conds.filter((c) => NO_VALUE.has(c.op) || c.value !== '')));
  return (
    <form
      className="filterbar"
      role="search"
      aria-label={t('filter.label')}
      onSubmit={(e) => {
        e.preventDefault();
        apply();
      }}
    >
      {raw ? (
        <label className="filter-raw">
          <span className="mono muted">WHERE</span>
          <input
            className="mono"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="status = 'open' AND total > 100"
            aria-label={t('filter.where')}
            data-testid="filter-where"
          />
        </label>
      ) : (
        conds.map((c, i) => (
          <span className="filter-cond" key={i}>
            <select value={c.col} onChange={(e) => set(i, { col: e.target.value })} aria-label={t('filter.column')}>
              {columns.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
            <select value={c.op} onChange={(e) => set(i, { op: e.target.value as Op })} aria-label={t('filter.operator')}>
              {OPS.map((o) => (
                <option key={o} value={o}>
                  {o}
                </option>
              ))}
            </select>
            {!NO_VALUE.has(c.op) && (
              <input
                value={c.value}
                onChange={(e) => set(i, { value: e.target.value })}
                aria-label={t('filter.value')}
                placeholder={c.op === 'IN' ? 'a, b, c' : c.op.includes('LIKE') ? '%text%' : ''}
                size={14}
              />
            )}
            {conds.length > 1 && (
              <button
                type="button"
                className="icon-button"
                aria-label={t('filter.remove')}
                onClick={() => setConds(conds.filter((_, j) => j !== i))}
              >
                <X size={14} />
              </button>
            )}
          </span>
        ))
      )}
      {!raw && (
        <button type="button" className="ghost" onClick={() => setConds([...conds, { col: columns[0] ?? '', op: '=', value: '' }])}>
          <Plus size={14} /> {t('filter.add')}
        </button>
      )}
      <button type="button" className="ghost mono" aria-pressed={raw} onClick={() => setRaw(!raw)} title={t('filter.rawTitle')}>
        {'</>'} WHERE
      </button>
      <span className="grow" />
      <button
        type="button"
        className="ghost"
        onClick={() => {
          setText('');
          setConds([{ col: columns[0] ?? '', op: '=', value: '' }]);
          onApply(null);
        }}
      >
        {t('filter.clear')}
      </button>
      <button type="submit" data-testid="filter-apply">
        {t('filter.apply')}
      </button>
    </form>
  );
}
