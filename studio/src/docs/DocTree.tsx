// The JSON tree: each document of a MongoDB result with
// a type badge on every value, collapsible, editable with its type kept
// unless the user changes the badge, saved as a replaceOne by `_id` that
// also requires every top-level field read to be unchanged (optimistic, as
// the SQL grid does with `<=>`). A conflict shows original, current and
// mine. Export writes canonical Extended JSON.

import { useMemo, useState } from 'react';
import type { ResultBuffer } from '../results/buffer';
import { execute } from '../api/sql';
import { errorText } from '../api/errors';
import { idOf, invalidScalar, parseDoc, SCALAR_TYPES, setAt, toEjson, type BsonType, type BVal } from './ejson';
import { dotted, saveCommand, type Path } from './edit';

const BADGE: Record<BsonType, string> = {
  object: 'obj',
  array: 'arr',
  string: 'str',
  int32: 'int',
  int64: 'long',
  double: 'dbl',
  decimal: 'dec',
  bool: 'bool',
  null: 'null',
  objectId: 'oid',
  date: 'date',
  binary: 'bin',
  timestamp: 'ts',
  regex: 're',
  minKey: 'min',
  maxKey: 'max',
};

function Badge({ t }: Readonly<{ t: BsonType }>) {
  return (
    <span className="badge type-badge" title={t} data-type={t}>
      {BADGE[t]}
    </span>
  );
}

interface EditTarget {
  path: (string | number)[];
  t: BsonType;
  v: string;
}

function Node({
  k,
  b,
  path,
  depth,
  onEdit,
}: Readonly<{ k: string; b: BVal; path: (string | number)[]; depth: number; onEdit?: (e: EditTarget) => void }>) {
  const [open, setOpen] = useState(depth < 2);
  if (b.t === 'object' || b.t === 'array') {
    const kids: [string, BVal, string | number][] =
      b.t === 'object' ? b.v.map(([kk, vv]) => [kk, vv, kk]) : b.v.map((vv, i) => [String(i), vv, i]);
    return (
      <li role="treeitem" aria-expanded={open} aria-selected={false}>
        <button className="tree-toggle" onClick={() => setOpen(!open)} aria-label={`${open ? 'Collapse' : 'Expand'} ${k}`}>
          {open ? '▾' : '▸'}
        </button>
        <span className="tree-key">{k}</span> <Badge t={b.t} />{' '}
        <span className="muted">
          {b.t === 'object' ? `{${kids.length}}` : `[${kids.length}]`}
        </span>
        {open && (
          <ul role="group">
            {kids.map(([kk, vv, step]) => (
              <Node key={kk} k={kk} b={vv} path={[...path, step]} depth={depth + 1} onEdit={onEdit} />
            ))}
          </ul>
        )}
      </li>
    );
  }
  return (
    <li role="treeitem" aria-selected={false}>
      <span className="tree-key">{k}</span> <Badge t={b.t} />{' '}
      <span className={`mono tree-val tree-${b.t}`}>{b.t === 'string' ? JSON.stringify(b.v) : b.v}</span>
      {onEdit && k !== '_id' && (
        <button className="link" onClick={() => onEdit({ path, t: b.t, v: b.v })} aria-label={`Edit ${k}`}>
          edit
        </button>
      )}
    </li>
  );
}

interface Conflict {
  original: BVal;
  current: BVal | null;
  mine: BVal;
}

function DocCard({
  doc,
  index,
  db,
  collection,
}: Readonly<{ doc: BVal; index: number; db: string | null; collection?: string }>) {
  const [orig, setOrig] = useState(doc);
  const [mine, setMine] = useState(doc);
  const [edit, setEdit] = useState<EditTarget | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [conflict, setConflict] = useState<Conflict | null>(null);
  const [edits, setEdits] = useState<Path[]>([]);
  const dirty = mine !== orig;
  const editable = !!collection && idOf(doc) !== undefined;

  const applyEdit = () => {
    if (!edit) return;
    const bad = invalidScalar(edit.t, edit.v);
    if (bad) {
      setErr(bad);
      return;
    }
    if (!dotted(edit.path)) {
      setErr('this field name has a dot or starts with $, so it can only be changed with a command');
      return;
    }
    const v: BVal = edit.t === 'null' ? { t: 'null', v: 'null' } : ({ t: edit.t, v: edit.v } as BVal);
    setMine(setAt(mine, edit.path, v));
    const key = JSON.stringify(edit.path);
    setEdits((xs) => (xs.some((x) => JSON.stringify(x) === key) ? xs : [...xs, edit.path]));
    setEdit(null);
    setErr(null);
    setSaved(false);
  };

  const save = async (against: BVal) => {
    if (!collection) return;
    setErr(null);
    try {
      const cmd = saveCommand(collection, against, mine, edits);
      const r = await execute(db, JSON.stringify(cmd), { language: 'mongo' });
      const n = Number(r.results[0]?.affected_rows ?? '0');
      if (n === 0) {
        // Changed since it was read: fetch the current document.
        const cur = await execute(db, JSON.stringify({ find: collection, filter: { _id: idOf(against) }, limit: 1, singleBatch: true }), {
          language: 'mongo',
        });
        const cell = cur.results[0]?.rows?.[0]?.[0] ?? null;
        setConflict({ original: against, current: cell ? parseDoc(cell) : null, mine });
        return;
      }
      setOrig(mine);
      setEdits([]);
      setConflict(null);
      setSaved(true);
    } catch (e) {
      setErr(errorText(e));
    }
  };

  return (
    <li className="doc-card" data-testid={`doc-${index}`}>
      <ul role="tree" aria-label={`Document ${index + 1}`} className="tree">
        <Node k={`#${index + 1}`} b={mine} path={[]} depth={0} onEdit={editable ? setEdit : undefined} />
      </ul>
      {edit && (
        <form
          className="row"
          onSubmit={(e) => {
            e.preventDefault();
            applyEdit();
          }}
          aria-label="Edit value"
        >
          <span className="mono">{edit.path.join('.')}</span>
          <select aria-label="Type" value={edit.t} onChange={(e) => setEdit({ ...edit, t: e.target.value as BsonType })} data-testid="edit-type">
            {SCALAR_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
          {edit.t !== 'null' && (
            <input aria-label="Value" value={edit.v} onChange={(e) => setEdit({ ...edit, v: e.target.value })} data-testid="edit-value" autoFocus />
          )}
          <button type="submit">Apply</button>
          <button type="button" onClick={() => setEdit(null)}>
            Cancel
          </button>
        </form>
      )}
      {dirty && !conflict && (
        <div className="row">
          <button className="primary" onClick={() => void save(orig)} data-testid="doc-save">
            Save document
          </button>
          <button
            onClick={() => {
              setMine(orig);
              setEdits([]);
            }}
          >
            Discard
          </button>
          <span className="muted">Sets only the edited fields, by _id, if no field changed since it was read</span>
        </div>
      )}
      {saved && (
        <p role="status" className="muted" data-testid="doc-saved">
          Saved.
        </p>
      )}
      {conflict && (
        <div className="banner stack" role="alert" data-testid="doc-conflict">
          <strong>The document changed since it was read.</strong>
          <div className="conflict-grid">
            <div>
              <h4>Original</h4>
              <pre>{JSON.stringify(toEjson(conflict.original), null, 1)}</pre>
            </div>
            <div>
              <h4>Current</h4>
              <pre>{conflict.current ? JSON.stringify(toEjson(conflict.current), null, 1) : '(deleted)'}</pre>
            </div>
            <div>
              <h4>Mine</h4>
              <pre>{JSON.stringify(toEjson(conflict.mine), null, 1)}</pre>
            </div>
          </div>
          <div className="row">
            {conflict.current && (
              <button className="primary" onClick={() => void save(conflict.current!)} data-testid="doc-keep-mine">
                Keep mine
              </button>
            )}
            <button
              onClick={() => {
                if (conflict.current) {
                  setOrig(conflict.current);
                  setMine(conflict.current);
                  setEdits([]);
                }
                setConflict(null);
              }}
            >
              Take current
            </button>
          </div>
        </div>
      )}
      {err && (
        <p className="error" role="alert">
          {err}
        </p>
      )}
    </li>
  );
}

/** Documents of a result (its `document` column), or every cell as JSON. */
export default function DocTree({
  buffer,
  rows,
  label,
  db = null,
  collection,
}: Readonly<{ buffer: ResultBuffer; rows: number; label: string; db?: string | null; collection?: string }>) {
  const docs = useMemo(() => {
    const out: (BVal | { error: string; text: string })[] = [];
    const col = Math.max(0, buffer.columns.findIndex((c) => c.name === 'document'));
    for (let r = 0; r < rows; r++) {
      const text = buffer.cell(r, col) ?? 'null';
      try {
        out.push(parseDoc(text));
      } catch (e) {
        out.push({ error: e instanceof Error ? e.message : String(e), text });
      }
    }
    return out;
  }, [buffer, rows]);
  const exportEjson = () => {
    const lines = docs.map((d) => ('error' in d ? d.text : JSON.stringify(toEjson(d)))).join('\n');
    const url = URL.createObjectURL(new Blob([lines + '\n'], { type: 'application/x-ndjson' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = `${collection ?? 'documents'}.ejson.ndjson`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  return (
    <section aria-label={`${label} as documents`} className="stack" data-testid="doc-tree">
      <div className="row">
        <span className="muted">{docs.length} documents</span>
        <span className="muted">
          Types are exact (canonical Extended JSON); a save changes only the fields you edit.
        </span>
        <button onClick={exportEjson} data-testid="export-ejson">
          Export Extended JSON
        </button>
      </div>
      <ol className="doc-list">
        {docs.map((d, i) =>
          'error' in d ? (
            <li key={i} className="mono">
              {d.text}
            </li>
          ) : (
            <DocCard key={i} doc={d} index={i} db={db} collection={collection} />
          ),
        )}
      </ol>
    </section>
  );
}
