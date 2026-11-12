// The table view's filter bar (the Studio and TUI redesign):
// conditions on columns, or a raw WHERE clause. The clause is the user's own
// SQL, run with the user's own rights, like anything typed in a query tab.

import { ident, sqlValue } from '../lib/sql';

export const OPS = ['=', '!=', '<', '<=', '>', '>=', 'LIKE', 'NOT LIKE', 'IN', 'IS NULL', 'IS NOT NULL'] as const;
export type Op = (typeof OPS)[number];

export interface Condition {
  col: string;
  op: Op;
  value: string;
}

/** Operators that take no value. */
export const NO_VALUE: ReadonlySet<Op> = new Set<Op>(['IS NULL', 'IS NOT NULL']);

function one(c: Condition): string {
  const col = ident(c.col);
  if (NO_VALUE.has(c.op)) return `${col} ${c.op}`;
  if (c.op === 'IN') {
    const items = c.value
      .split(',')
      .map((v) => v.trim())
      .filter((v) => v.length > 0);
    return items.length ? `${col} IN (${items.map((v) => sqlValue(v)).join(', ')})` : 'FALSE';
  }
  const op = c.op === '!=' ? '<>' : c.op;
  return `${col} ${op} ${sqlValue(c.value)}`;
}

/** The WHERE expression for `conds` (AND), or null when there are none. */
export function filterSql(conds: Condition[]): string | null {
  const parts = conds.filter((c) => c.col).map(one);
  return parts.length ? parts.join(' AND ') : null;
}
