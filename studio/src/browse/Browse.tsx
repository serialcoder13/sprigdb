import { useCallback, useEffect, useMemo, useState } from 'react';
import { FormattedMessage, useIntl } from 'react-intl';
import { useSession } from '../auth/session';
import { Grid } from '../results/Grid';
import { begin, commit, execute, rollback, rows as fetchRows } from '../api/sql';
import { errorText } from '../api/errors';
import type { Cell, Column } from '../api/types';
import { pageQuery, readOnlyColumns, rereadFor, statementFor, type Change, type TableRef } from './edit';
import type { ColumnInfo } from '../catalog/snapshot';
import { ImportDialog } from './ImportDialog';
import { FilterBar } from './FilterBar';
import { ChevronRight, Plus, X } from '../icons';

const PAGE = 100;

interface Conflict {
  row: number;
  original: Cell[];
  current: Cell[] | null;
  mine: Change;
}

/** Browse and edit a table (chapter 44 step 10): keyset pages on the key,
 * edits saved in one transaction with optimistic concurrency. */
export function Browse({ db, table, cols, editable }: { db: string; table: string; cols: ColumnInfo[]; editable: boolean }) {
  const intl = useIntl();
  const me = useSession((s) => s.me);
  const keyNames = useMemo(() => cols.filter((c) => c.key === 'PRI').map((c) => c.name), [cols]);
  const [columns, setColumns] = useState<Column[]>([]);
  const [rows, setRows] = useState<Cell[][]>([]);
  const [starts, setStarts] = useState<(Cell[] | null)[]>([null]);
  const [hasMore, setHasMore] = useState(false);
  const [edits, setEdits] = useState<Map<number, Map<number, Cell>>>(new Map());
  const [deleted, setDeleted] = useState<Set<number>>(new Set());
  const [inserted, setInserted] = useState<Cell[][]>([]);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<string[] | null>(null);
  const [conflicts, setConflicts] = useState<Conflict[]>([]);
  const [showImport, setShowImport] = useState(false);
  const [active, setActive] = useState(0);
  const [filter, setFilter] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const apply = useCallback((r: { columns: Column[]; rows: Cell[][] }) => {
    setError(null);
    setColumns(r.columns);
    setHasMore(r.rows.length > PAGE);
    setRows(r.rows.slice(0, PAGE));
    setEdits(new Map());
    setDeleted(new Set());
    setInserted([]);
    setConflicts([]);
  }, []);
  const load = useCallback(
    (after: Cell[] | null) =>
      fetchRows(db, pageQuery({ db, table, keyNames }, after, PAGE + 1, filter)).then(apply, (e: unknown) => setError(errorText(e))),
    [db, table, keyNames, apply, filter],
  );
  useEffect(() => {
    let live = true;
    fetchRows(db, pageQuery({ db, table, keyNames }, null, PAGE + 1, filter)).then(
      (r) => live && apply(r),
      (e: unknown) => live && setError(errorText(e)),
    );
    return () => {
      live = false;
    };
  }, [db, table, keyNames, apply, filter]);

  const ref: TableRef = useMemo(() => {
    const generated = new Set(cols.filter((c) => c.generated).map((c) => c.name));
    return {
      db,
      table,
      columns,
      key: keyNames.map((k) => columns.findIndex((c) => c.name === k)).filter((i) => i >= 0),
      readOnly: readOnlyColumns(columns, generated),
    };
  }, [db, table, columns, cols, keyNames]);
  const canEdit = editable && keyNames.length > 0;
  const total = rows.length + inserted.length;
  const cell = (r: number, c: number): Cell => {
    if (r >= rows.length) return inserted[r - rows.length]?.[c] ?? null;
    const e = edits.get(r);
    return e?.has(c) ? (e.get(c) ?? null) : (rows[r]?.[c] ?? null);
  };
  const changes = (): Change[] => {
    const out: Change[] = [];
    for (const [r, vals] of edits) if (!deleted.has(r)) out.push({ kind: 'update', row: r, original: rows[r], values: vals });
    for (const r of deleted) out.push({ kind: 'delete', row: r, original: rows[r] });
    inserted.forEach((row, i) =>
      out.push({ kind: 'insert', row: rows.length + i, values: new Map(row.map((v, c) => [c, v] as [number, Cell]).filter(([, v]) => v !== null)) }),
    );
    return out;
  };

  const save = async (list: Change[]) => {
    setPreview(null);
    setError(null);
    const t = await begin(db);
    const found: Conflict[] = [];
    try {
      for (const ch of list) {
        const r = await execute(db, statementFor(ref, ch), { transaction: t.name });
        const affected = Number(r.results[0]?.affected_rows ?? '0');
        if (ch.kind !== 'insert' && affected === 0) {
          const cur = await fetchRows(db, rereadFor(ref, ch.original ?? [])).catch(() => null);
          found.push({ row: ch.row, original: ch.original ?? [], current: cur?.rows[0] ?? null, mine: ch });
        }
      }
      if (found.length) {
        await rollback(t);
        setConflicts(found);
        return;
      }
      await commit(t);
      await load(starts[starts.length - 1] ?? null);
    } catch (e) {
      await rollback(t).catch(() => undefined);
      setError(errorText(e));
    }
  };

  const [pending, setPending] = useState<Change[] | null>(null);
  let editedRows = 0;
  for (const [r] of edits) if (!deleted.has(r)) editedRows++;
  const pendingCount = editedRows + deleted.size + inserted.length;

  return (
    <div className="stack">
      {!keyNames.length && <p className="banner">{intl.formatMessage({ id: 'browse.noKey' })}</p>}
      {columns.length > 0 && (
        <FilterBar
          columns={columns.map((c) => c.name)}
          onApply={(w) => {
            setStarts([null]);
            setFilter(w);
          }}
        />
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {preview && (
        <div className="card stack" role="region" aria-label="Statements to run">
          <strong>These statements run in one transaction:</strong>
          <pre data-testid="edit-preview">{preview.join(';\n')}</pre>
          <div className="row">
            <button className="primary" onClick={() => void save(pending ?? changes())}>
              Run them
            </button>
            <button onClick={() => setPreview(null)}>Cancel</button>
          </div>
        </div>
      )}
      {conflicts.length > 0 && (
        <div className="card stack" role="alert" data-testid="conflicts">
          <strong>
            <FormattedMessage id="browse.conflict" />
          </strong>
          {conflicts.map((c) => (
            <table key={c.row}>
              <thead>
                <tr>
                  <th scope="col">Column</th>
                  <th scope="col">Original</th>
                  <th scope="col">Current</th>
                  <th scope="col">Mine</th>
                </tr>
              </thead>
              <tbody>
                {columns.map((col, i) => (
                  <tr key={col.name}>
                    <th scope="row">{col.name}</th>
                    <td>{c.original[i] ?? 'NULL'}</td>
                    <td>{c.current ? (c.current[i] ?? 'NULL') : '(deleted)'}</td>
                    <td>{c.mine.values?.has(i) ? (c.mine.values.get(i) ?? 'NULL') : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ))}
          <div className="row">
            <button
              className="primary"
              onClick={() => {
                const again = conflicts.filter((c) => c.current).map((c) => ({ ...c.mine, original: c.current ?? [] }));
                setConflicts([]);
                setPending(again);
                setPreview(again.map((c) => statementFor(ref, c)));
              }}
            >
              Keep mine (save against current)
            </button>
            <button onClick={() => void load(starts[starts.length - 1] ?? null)}>Take current</button>
          </div>
        </div>
      )}
      <Grid
        label={`${table} rows`}
        columns={columns.map((c) => c.name)}
        rowCount={total}
        cell={cell}
        height={480}
        editable={canEdit ? (r, c) => !ref.readOnly.has(c) && !deleted.has(r) : undefined}
        edited={(r, c) => r < rows.length && (edits.get(r)?.has(c) ?? false)}
        rowClass={(r) => (deleted.has(r) ? 'deleted' : r >= rows.length ? 'added' : '')}
        onActive={setActive}
        onEdit={(r, c, v) => {
          setActive(r);
          if (r >= rows.length) {
            const copy = inserted.map((x) => [...x]);
            copy[r - rows.length]![c] = v;
            setInserted(copy);
          } else {
            const m = new Map(edits);
            const e = new Map(m.get(r) ?? []);
            e.set(c, v);
            m.set(r, e);
            setEdits(m);
          }
        }}
      />
      <div className="table-footer" role="group" aria-label="Rows">
        {canEdit && (
          <>
            <span className={pendingCount ? 'pending' : 'muted'} data-testid="pending-count">
              {intl.formatMessage({ id: 'browse.pending' }, { count: pendingCount })}
            </span>
            <button
              className="primary"
              disabled={!pendingCount}
              data-testid="browse-apply"
              onClick={() => {
                const list = changes();
                setPending(list);
                setPreview(list.map((c) => statementFor(ref, c)));
              }}
            >
              <FormattedMessage id="browse.save" />
            </button>
            <button disabled={!pendingCount} onClick={() => void load(starts[starts.length - 1] ?? null)}>
              <FormattedMessage id="browse.discard" />
            </button>
            <button
              className="ghost"
              disabled={!pendingCount}
              onClick={() => {
                const text = `${changes()
                  .map((c) => statementFor(ref, c))
                  .join(';\n')};\n`;
                void navigator.clipboard?.writeText(text).then(() => {
                  setCopied(true);
                  setTimeout(() => setCopied(false), 1500);
                });
              }}
            >
              {intl.formatMessage({ id: copied ? 'browse.copied' : 'browse.copySql' })}
            </button>
          </>
        )}
        <span className="grow" />
        <span className="muted">
          {intl.formatMessage({ id: 'results.rows' }, { count: rows.length })} · page {starts.length}
        </span>
        <button
          className="icon-button"
          disabled={starts.length <= 1}
          aria-label={intl.formatMessage({ id: 'browse.prev' })}
          title={intl.formatMessage({ id: 'browse.prev' })}
          onClick={() => {
            const s = starts.slice(0, -1);
            setStarts(s);
            void load(s[s.length - 1] ?? null);
          }}
        >
          <ChevronRight size={16} style={{ transform: 'scaleX(-1)' }} />
        </button>
        <button
          className="icon-button"
          disabled={!hasMore || !keyNames.length}
          aria-label={intl.formatMessage({ id: 'browse.next' })}
          title={intl.formatMessage({ id: 'browse.next' })}
          onClick={() => {
            const last = rows[rows.length - 1];
            if (!last) return;
            const after = ref.key.map((i) => last[i] ?? null);
            setStarts([...starts, after]);
            void load(after);
          }}
        >
          <ChevronRight size={16} />
        </button>
        {canEdit && (
          <>
            <button onClick={() => setInserted([...inserted, columns.map(() => null)])}>
              <Plus size={14} /> <FormattedMessage id="browse.addRow" />
            </button>
            <button
              onClick={() => {
                if (active < rows.length) setDeleted(new Set(deleted).add(active));
                else setInserted(inserted.filter((_, i) => i !== active - rows.length));
              }}
            >
              <X size={14} /> <FormattedMessage id="browse.deleteRow" />
            </button>
          </>
        )}
        {editable && (
          <button onClick={() => setShowImport(true)}>
            <FormattedMessage id="browse.import" />
          </button>
        )}
      </div>
      {showImport && me && (
        <ImportDialog
          db={db}
          table={table}
          batchRows={me.studio.import_batch_rows}
          maxMb={me.studio.import_max_mb}
          onClose={() => {
            setShowImport(false);
            void load(null);
          }}
        />
      )}
    </div>
  );
}
