import { useRef, useState, type KeyboardEvent } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { Cell } from '../api/types';

export interface GridProps {
  label: string;
  columns: string[];
  rowCount: number;
  cell(row: number, col: number): Cell;
  numeric?(col: number): boolean;
  /** Editing (browse): which cells may change, and how. */
  editable?(row: number, col: number): boolean;
  onEdit?(row: number, col: number, value: Cell): void;
  edited?(row: number, col: number): boolean;
  rowClass?(row: number): string;
  height?: number;
  /** The result's row number of row 0 (a cursor's sliding window). */
  firstRow?: number;
}

const ROW_H = 26;

/**
 * The results grid (chapter 44, "Renderers"): virtualized rows, the ARIA
 * grid pattern with aria-rowcount and aria-rowindex, arrow-key navigation,
 * values rendered as text only.
 */
export function Grid(p: GridProps) {
  const scroller = useRef<HTMLDivElement>(null);
  const [active, setActive] = useState<[number, number]>([0, 0]);
  const [editing, setEditing] = useState<[number, number] | null>(null);
  const [draft, setDraft] = useState('');
  const v = useVirtualizer({
    count: p.rowCount,
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_H,
    overscan: 12,
  });
  const cols = p.columns.length;
  const focusCell = (r: number, c: number) => {
    const rr = Math.max(0, Math.min(p.rowCount - 1, r));
    const cc = Math.max(0, Math.min(cols - 1, c));
    setActive([rr, cc]);
    v.scrollToIndex(rr);
    requestAnimationFrame(() => {
      scroller.current?.querySelector<HTMLElement>(`[data-cell="${rr}:${cc}"]`)?.focus();
    });
  };
  const startEdit = (r: number, c: number) => {
    if (!p.editable?.(r, c)) return;
    const val = p.cell(r, c);
    setDraft(val ?? '');
    setEditing([r, c]);
  };
  const commit = (value: Cell) => {
    if (editing) p.onEdit?.(editing[0], editing[1], value);
    const [r, c] = editing ?? active;
    setEditing(null);
    focusCell(r, c);
  };
  const onKey = (e: KeyboardEvent) => {
    if (editing) return;
    const [r, c] = active;
    const page = Math.max(1, Math.floor((p.height ?? 400) / ROW_H) - 1);
    const moves: Record<string, [number, number]> = {
      ArrowDown: [r + 1, c],
      ArrowUp: [r - 1, c],
      ArrowRight: [r, c + 1],
      ArrowLeft: [r, c - 1],
      PageDown: [r + page, c],
      PageUp: [r - page, c],
      Home: [r, 0],
      End: [r, cols - 1],
    };
    const m = moves[e.key];
    if (m) {
      e.preventDefault();
      focusCell(e.ctrlKey && e.key === 'Home' ? 0 : m[0], e.ctrlKey && e.key === 'End' ? cols - 1 : m[1]);
    } else if ((e.key === 'Enter' || e.key === 'F2') && p.editable) {
      e.preventDefault();
      startEdit(r, c);
    }
  };
  const height = p.height ?? 400;
  return (
    <div
      className="grid-wrap"
      ref={scroller}
      style={{ blockSize: Math.min(height, (p.rowCount + 1) * ROW_H + 4) }}
    >
      <div
        role="grid"
        aria-label={p.label}
        aria-rowcount={(p.firstRow ?? 0) + p.rowCount + 1}
        aria-colcount={cols + 1}
        className="grid"
        onKeyDown={onKey}
        style={{ inlineSize: 64 + cols * 160, position: 'relative' }}
      >
        <div role="row" aria-rowindex={1} style={{ position: 'sticky', insetBlockStart: 0, zIndex: 2 }}>
          <div role="columnheader" style={{ inlineSize: 64 }} aria-colindex={1}>
            #
          </div>
          {p.columns.map((name, c) => (
            <div role="columnheader" key={c} aria-colindex={c + 2} title={name}>
              {name}
            </div>
          ))}
        </div>
        <div style={{ blockSize: v.getTotalSize(), position: 'relative' }}>
          {v.getVirtualItems().map((vr) => {
            const r = vr.index;
            return (
              <div
                role="row"
                key={r}
                aria-rowindex={(p.firstRow ?? 0) + r + 2}
                className={p.rowClass?.(r)}
                style={{ position: 'absolute', insetBlockStart: vr.start, insetInlineStart: 0 }}
              >
                <div role="rowheader" aria-colindex={1}>
                  {(p.firstRow ?? 0) + r + 1}
                </div>
                {p.columns.map((_, c) => {
                  const val = p.cell(r, c);
                  const isActive = active[0] === r && active[1] === c;
                  const isEditing = editing && editing[0] === r && editing[1] === c;
                  const cls = [
                    val === null ? 'null' : '',
                    p.numeric?.(c) ? 'num' : '',
                    p.edited?.(r, c) ? 'edited' : '',
                  ].join(' ');
                  return (
                    <div
                      role="gridcell"
                      key={c}
                      aria-colindex={c + 2}
                      aria-selected={isActive}
                      aria-readonly={p.editable ? !p.editable(r, c) : undefined}
                      tabIndex={isActive ? 0 : -1}
                      data-cell={`${r}:${c}`}
                      className={cls}
                      title={val ?? 'NULL'}
                      onClick={() => setActive([r, c])}
                      onDoubleClick={() => startEdit(r, c)}
                    >
                      {isEditing ? (
                        <span className="row" style={{ gap: 2 }}>
                          <input
                            autoFocus
                            aria-label={`New value for ${p.columns[c]}`}
                            value={draft}
                            onChange={(e) => setDraft(e.target.value)}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') commit(draft);
                              if (e.key === 'Escape') {
                                setEditing(null);
                                focusCell(r, c);
                              }
                            }}
                            style={{ inlineSize: 110, minBlockSize: 22, padding: '0 4px' }}
                          />
                          <button
                            title="Set NULL"
                            aria-label="Set NULL"
                            onClick={() => commit(null)}
                            style={{ minBlockSize: 22, padding: '0 4px' }}
                          >
                            ∅
                          </button>
                        </span>
                      ) : val === null ? (
                        'NULL'
                      ) : (
                        val
                      )}
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
