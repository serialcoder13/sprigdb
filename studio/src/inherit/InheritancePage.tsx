// Data inheritance (chapter 42; inheritance plan step 7): each inheritable
// table's origin level and group, the shares with the tables they carry,
// and, for a delete that was refused (1451), what still references the
// rows. Read-only: every view is a statement run as the signed-in user.

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { errorText } from '../api/errors';
import { objects } from '../api/sql';
import { useDatabases } from '../catalog/snapshot';
import { checkIds, checkReferences, origin, sharesByName, SHOW_SHARES, SHOW_TABLES, showCreateShare, type Obj } from './statements';

export function InheritancePage() {
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>Inheritance</h1>
      <Tables />
      <Shares />
      <Blockers />
    </div>
  );
}

function Failure({ e }: { e: unknown }) {
  return (
    <p role="alert" className="error">
      {errorText(e)}
    </p>
  );
}

function Tables() {
  const q = useQuery({ queryKey: ['inherit', 'tables'], queryFn: () => objects('-', SHOW_TABLES) });
  return (
    <section className="card stack" aria-labelledby="inh-tables-h">
      <h2 id="inh-tables-h">Inheritable tables</h2>
      {q.error ? <Failure e={q.error} /> : null}
      {q.data && q.data.length === 0 && <p className="muted">No inheritable tables.</p>}
      {q.data && q.data.length > 0 && (
        <table data-testid="inh-tables">
          <thead>
            <tr>
              <th>Table</th>
              <th>Key</th>
              <th>Origin</th>
              <th>Columns added here</th>
              <th>Group</th>
              <th>Shares</th>
            </tr>
          </thead>
          <tbody>
            {q.data.map((t) => (
              <tr key={t.Table ?? ''}>
                <td>{t.Table}</td>
                <td>{t.Key}</td>
                <td>{origin(t)}</td>
                <td>{t.Own_columns || '—'}</td>
                <td>{t.Group}</td>
                <td>{t.Shares || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

function Shares() {
  const q = useQuery({ queryKey: ['inherit', 'shares'], queryFn: () => objects('-', SHOW_SHARES) });
  const [open, setOpen] = useState<{ name: string; rows: Obj[] | null; err: unknown } | null>(null);
  const show = async (name: string) => {
    try {
      setOpen({ name, rows: await objects('-', showCreateShare(name)), err: null });
    } catch (e) {
      setOpen({ name, rows: null, err: e });
    }
  };
  const groups = q.data ? sharesByName(q.data) : [];
  return (
    <section className="card stack" aria-labelledby="inh-shares-h">
      <h2 id="inh-shares-h">Shares and their groups</h2>
      <p className="muted">Sharing a table shares every table linked to it by foreign keys.</p>
      {q.error ? <Failure e={q.error} /> : null}
      {q.data && groups.length === 0 && <p className="muted">No shares.</p>}
      {groups.length > 0 && (
        <table data-testid="inh-shares">
          <thead>
            <tr>
              <th>Share</th>
              <th>Role</th>
              <th>To</th>
              <th>Tables</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {groups.map((g) => (
              <tr key={`${g.role}:${g.name}`}>
                <td>{g.name}</td>
                <td>{g.role}</td>
                <td>{g.to}</td>
                <td>{g.tables.join(', ')}</td>
                <td>{g.role === 'granted' && <button onClick={() => void show(g.name)}>Definition</button>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {open && (
        <div className="stack" data-testid="inh-share-def">
          <h3 style={{ margin: 0 }}>{open.name}</h3>
          {open.err ? <Failure e={open.err} /> : null}
          {open.rows?.map((r) => (
            <div key={r.Share ?? ''} className="stack">
              <pre className="logic-stmt">{r['Create Share']}</pre>
              <p className="muted">
                From space {r.From}, {r.Mode}. Group: {r.Group}
              </p>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function Blockers() {
  const dbs = useDatabases();
  const [db, setDb] = useState('');
  const [table, setTable] = useState('');
  const [where, setWhere] = useState('');
  const [out, setOut] = useState<{ kind: 'refs' | 'ids'; rows: Obj[] | null; err: unknown } | null>(null);
  const run = async (kind: 'refs' | 'ids') => {
    try {
      const sql = kind === 'refs' ? checkReferences(table, where) : checkIds(table);
      setOut({ kind, rows: await objects(db || '-', sql), err: null });
    } catch (e) {
      setOut({ kind, rows: null, err: e });
    }
  };
  return (
    <section className="card stack" aria-labelledby="inh-block-h">
      <h2 id="inh-block-h">Why a delete is refused</h2>
      <p className="muted">
        A delete is refused (1451) while a row anywhere below still references it. Tenant administrators see which tenant and row.
      </p>
      <div className="row">
        <label className="row">
          Database{' '}
          <select value={db} onChange={(e) => setDb(e.target.value)} data-testid="inh-db">
            <option value="">—</option>
            {(dbs.data ?? []).map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </label>
        <label className="row">
          Table <input value={table} onChange={(e) => setTable(e.target.value)} data-testid="inh-table" />
        </label>
        <label className="row">
          Where <input value={where} onChange={(e) => setWhere(e.target.value)} placeholder="id = 5" data-testid="inh-where" />
        </label>
        <button onClick={() => void run('refs')} disabled={!table}>
          Find references
        </button>
        <button onClick={() => void run('ids')} disabled={!table}>
          Check ids
        </button>
      </div>
      {out?.err ? <Failure e={out.err} /> : null}
      {out?.rows && out.rows.length === 0 && (
        <p className="muted" data-testid="inh-none">
          {out.kind === 'refs' ? 'Nothing references these rows: a delete would pass.' : 'No id is held by more than one tenant.'}
        </p>
      )}
      {out?.rows && out.rows.length > 0 && (
        <table data-testid="inh-blockers">
          <thead>
            <tr>
              {Object.keys(out.rows[0] ?? {}).map((k) => (
                <th key={k}>{k}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {out.rows.map((r, i) => (
              <tr key={i}>
                {Object.entries(r).map(([k, v]) => (
                  <td key={k}>{v}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
