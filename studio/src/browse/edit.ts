// Statements for browse edits, with optimistic concurrency. Each UPDATE or
// DELETE names the row by its key and requires every column read to still
// hold the value read (`<=>`, NULL-safe), so a row someone else changed
// meanwhile matches nothing: 0 rows affected is a conflict. Approximate
// numbers compare by their text form, which is what the API returned.

import type { Cell, Column } from '../api/types';
import { ident, qualified, sqlValue } from '../lib/sql';

export interface TableRef {
  db: string;
  table: string;
  columns: Column[];
  /** Key columns (primary key), by index into `columns`. */
  key: number[];
  /** Columns that can't be written (generated, `_sprig_*`, binary). */
  readOnly: Set<number>;
}

export interface Change {
  kind: 'update' | 'delete' | 'insert';
  /** The row as read (update, delete). */
  original?: Cell[];
  /** New values by column index (update, insert). */
  values?: Map<number, Cell>;
  /** Index of the row in the page, to report conflicts. */
  row: number;
}

const APPROX = /^(FLOAT|DOUBLE|REAL|DECIMAL)/i;

function cond(t: TableRef, original: Cell[]): string {
  const parts: string[] = [];
  t.columns.forEach((c, i) => {
    if (c.binary) {
      // Base64 in the API: compare the key only for binary columns.
      if (t.key.includes(i)) parts.push(`TO_BASE64(${ident(c.name)}) <=> ${sqlValue(original[i] ?? null)}`);
      return;
    }
    const col = APPROX.test(c.type) || c.mysql_type === 4 || c.mysql_type === 5
      ? `CAST(${ident(c.name)} AS CHAR)`
      : ident(c.name);
    parts.push(`${col} <=> ${sqlValue(original[i] ?? null)}`);
  });
  return parts.join(' AND ');
}

/** The statement for one change. */
export function statementFor(t: TableRef, ch: Change): string {
  const target = qualified(t.db, t.table);
  if (ch.kind === 'insert') {
    const entries = [...(ch.values ?? new Map<number, Cell>()).entries()].filter(([i]) => !t.readOnly.has(i));
    if (!entries.length) return `INSERT INTO ${target} () VALUES ()`;
    const cols = entries.map(([i]) => ident(t.columns[i]!.name)).join(', ');
    const vals = entries.map(([, v]) => sqlValue(v)).join(', ');
    return `INSERT INTO ${target} (${cols}) VALUES (${vals})`;
  }
  const where = cond(t, ch.original ?? []);
  if (ch.kind === 'delete') return `DELETE FROM ${target} WHERE ${where} LIMIT 1`;
  const sets = [...(ch.values ?? new Map<number, Cell>()).entries()]
    .filter(([i]) => !t.readOnly.has(i))
    .map(([i, v]) => `${ident(t.columns[i]!.name)} = ${sqlValue(v)}`)
    .join(', ');
  return `UPDATE ${target} SET ${sets} WHERE ${where} LIMIT 1`;
}

/** The statement re-reading a row by its key (for the three-way view). */
export function rereadFor(t: TableRef, original: Cell[]): string {
  const where = t.key
    .map((i) => `${ident(t.columns[i]!.name)} <=> ${sqlValue(original[i] ?? null)}`)
    .join(' AND ');
  return `SELECT * FROM ${qualified(t.db, t.table)} WHERE ${where}`;
}

/** Keyset paging: rows after `after` in key order,
 * within `filter` (a WHERE expression from the filter bar) when given. */
export function pageQuery(
  t: { db: string; table: string; keyNames: string[] },
  after: Cell[] | null,
  limit: number,
  filter: string | null = null,
): string {
  const target = qualified(t.db, t.table);
  const conds: string[] = filter ? [`(${filter})`] : [];
  if (!t.keyNames.length) {
    return `SELECT * FROM ${target}${conds.length ? ` WHERE ${conds.join(' AND ')}` : ''} LIMIT ${limit}`;
  }
  const order = t.keyNames.map(ident).join(', ');
  if (after) {
    const lhs = t.keyNames.length === 1 ? ident(t.keyNames[0]!) : `(${order})`;
    const rhs = t.keyNames.length === 1 ? sqlValue(after[0] ?? null) : `(${after.map(sqlValue).join(', ')})`;
    conds.push(`${lhs} > ${rhs}`);
  }
  const where = conds.length ? ` WHERE ${conds.join(' AND ')}` : '';
  return `SELECT * FROM ${target}${where} ORDER BY ${order} LIMIT ${limit}`;
}

/** Writable columns: not generated, not `_sprig_*`, not binary. */
export function readOnlyColumns(columns: Column[], generated: Set<string>): Set<number> {
  const out = new Set<number>();
  columns.forEach((c, i) => {
    if (c.binary || c.name.startsWith('_sprig_') || generated.has(c.name)) out.add(i);
  });
  return out;
}
