// Data inheritance views: the statements the Inheritance page runs, and how
// their rows are grouped for display. Every statement goes through the
// gateway as the signed-in user, so the user's privileges decide what is
// shown (descendant tenants are named only for tenant administrators).

const ident = (s: string) => `\`${s.replace(/`/g, '``')}\``;

/** `db.table` or `table`, each part quoted. */
export function tableName(s: string): string {
  const t = s.trim();
  if (!t) throw new Error('name the table');
  return t.split('.').map(ident).join('.');
}

/** Each inheritable table's origin, own columns, group and shares. */
export const SHOW_TABLES = 'SHOW INHERITABLE TABLES';

/** Shares this tenant grants and receives (one row per table a share covers). */
export const SHOW_SHARES = 'SHOW SHARES';

/** A share as created, with the foreign-key group it carries. */
export function showCreateShare(name: string): string {
  if (!name) throw new Error('name the share');
  return `SHOW CREATE SHARE ${ident(name)}`;
}

/** What refuses a delete of the rows `where` selects (all rows when empty). */
export function checkReferences(table: string, where: string): string {
  const w = where.trim();
  return `SPRIG CHECK REFERENCES ${tableName(table)}${w ? ` WHERE ${w}` : ''}`;
}

/** Ids held by more than one tenant of the table's tree. */
export function checkIds(table: string): string {
  return `SPRIG CHECK IDS ${tableName(table)}`;
}

export type Obj = Record<string, string | null>;

/** Where a table comes from, in words. */
export function origin(t: Obj): string {
  const n = Number(t.Levels_up ?? '0');
  if (!n) return 'declared here';
  return `inherited from ${t.Origin ?? 'an ancestor'} (${n} level${n === 1 ? '' : 's'} up)`;
}

/** Shares grouped by name: one share covers every table of its group. */
export function sharesByName(rows: Obj[]): { name: string; role: string; tables: string[]; from: string; to: string }[] {
  const out = new Map<string, { name: string; role: string; tables: string[]; from: string; to: string }>();
  for (const r of rows) {
    const key = `${r.Role ?? ''}:${r.Share ?? ''}`;
    const e = out.get(key) ?? { name: r.Share ?? '', role: r.Role ?? '', tables: [], from: r.From ?? '', to: r.To ?? '' };
    if (r.Object && !e.tables.includes(r.Object)) e.tables.push(r.Object);
    e.tables.sort();
    out.set(key, e);
  }
  return [...out.values()];
}
