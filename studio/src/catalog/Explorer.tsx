import { useState } from 'react';
import { NavLink, useParams } from 'react-router';
import { useIntl, FormattedMessage } from 'react-intl';
import { useQueryClient } from '@tanstack/react-query';
import { invalidateCatalog, useDatabases, useSnapshot } from './snapshot';

function DbNode({ tenant, db, filter, open: initial }: { tenant: string; db: string; filter: string; open: boolean }) {
  const [open, setOpen] = useState(initial);
  const snap = useSnapshot(open ? db : null);
  const base = `/t/${encodeURIComponent(tenant)}/db/${encodeURIComponent(db)}`;
  const f = filter.toLowerCase();
  const items = (snap.data?.objects ?? []).filter((o) => !f || o.name.toLowerCase().includes(f));
  return (
    <li>
      <button
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        style={{ border: 'none', background: 'none', padding: '0 4px' }}
        aria-label={`${open ? 'Collapse' : 'Expand'} ${db}`}
      >
        {open ? '▾' : '▸'}
      </button>
      <NavLink to={base} end>
        {db}
      </NavLink>
      {open && (
        <ul>
          {snap.isLoading && <li className="muted">…</li>}
          {items.map((o) => (
            <li key={`${o.kind}:${o.name}`}>
              <NavLink to={`${base}/obj/${o.kind}/${encodeURIComponent(o.name)}`} title={o.kind}>
                <span aria-hidden="true">{o.kind === 'view' ? '◇ ' : o.kind === 'table' ? '▤ ' : o.kind === 'graph' ? '◎ ' : 'ƒ '}</span>
                {o.name}
              </NavLink>
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

/** The explorer tree (chapter 44 step 7): databases, then objects by kind. */
export function Explorer() {
  const intl = useIntl();
  const { tenant = '', db: current } = useParams();
  const dbs = useDatabases();
  const qc = useQueryClient();
  const [filter, setFilter] = useState('');
  return (
    <div className="stack">
      <div className="row">
        <h2 style={{ margin: 0, fontSize: 15, flex: 1 }}>
          <FormattedMessage id="explorer.title" />
        </h2>
        <button onClick={() => invalidateCatalog(qc)} aria-label={intl.formatMessage({ id: 'explorer.refresh' })}>
          ↻
        </button>
      </div>
      <input
        type="search"
        placeholder={intl.formatMessage({ id: 'explorer.filter' })}
        aria-label={intl.formatMessage({ id: 'explorer.filter' })}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
      />
      {dbs.error && <p className="error">{String(dbs.error.message)}</p>}
      {dbs.data && dbs.data.length === 0 && (
        <p className="muted">
          <FormattedMessage id="explorer.empty" />
        </p>
      )}
      <nav className="tree" aria-label="Databases">
        <ul>
          {(dbs.data ?? []).map((d) => (
            <DbNode key={d} tenant={tenant} db={d} filter={filter} open={d === current} />
          ))}
        </ul>
      </nav>
    </div>
  );
}
