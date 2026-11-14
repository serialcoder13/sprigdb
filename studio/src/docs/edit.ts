// Saving an edited document, with optimistic concurrency. The gateway sends
// relaxed Extended JSON, which doesn't say whether a small integer was
// stored as int32 or int64, so a save never rewrites fields the user didn't
// touch: it is an update by `_id` that `$set`s only the edited paths, with
// their types as the badges say, and also requires every top-level field as
// read, so a concurrent change matches nothing (n = 0) and becomes a
// conflict instead of being overwritten.

import { idOf, toEjson, type BVal } from './ejson';

export type Path = (string | number)[];

/** A dotted path for `$set`; null when a key can't be addressed that way. */
export function dotted(path: Path): string | null {
  const parts: string[] = [];
  for (const p of path) {
    if (typeof p === 'number') parts.push(String(p));
    else if (!p || p.includes('.') || p.startsWith('$')) return null;
    else parts.push(p);
  }
  return parts.length ? parts.join('.') : null;
}

/** The value at `path` in `root`. */
export function getAt(root: BVal, path: Path): BVal | undefined {
  let cur: BVal | undefined = root;
  for (const p of path) {
    if (!cur) return undefined;
    if (cur.t === 'object') cur = cur.v.find(([k]) => k === p)?.[1];
    else if (cur.t === 'array' && typeof p === 'number') cur = cur.v[p];
    else return undefined;
  }
  return cur;
}

/** The optimistic filter: `_id` and every top-level field as read. */
export function filterOf(read: BVal): Record<string, unknown> {
  const q: Record<string, unknown> = { _id: idOf(read) };
  if (read.t === 'object') {
    for (const [k, v] of read.v) {
      // Operator-like and dotted names can't be matched by equality.
      if (k === '_id' || k.startsWith('$') || k.includes('.')) continue;
      q[k] = v.t === 'object' || v.t === 'array' ? { $eq: toEjson(v) } : toEjson(v);
    }
  }
  return q;
}

/** The `update` command setting `edits` (paths of `mine`) if `read` is unchanged. */
export function saveCommand(collection: string, read: BVal, mine: BVal, edits: Path[]): Record<string, unknown> {
  const set: Record<string, unknown> = {};
  for (const p of edits) {
    const d = dotted(p);
    const v = getAt(mine, p);
    if (d && v) set[d] = toEjson(v);
  }
  return { update: collection, updates: [{ q: filterOf(read), u: { $set: set }, multi: false, upsert: false }] };
}
