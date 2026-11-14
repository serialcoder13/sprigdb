import { useState } from 'react';
import { NavLink, useParams } from 'react-router';
import { useIntl, FormattedMessage } from 'react-intl';
import { useQueryClient } from '@tanstack/react-query';
import { invalidateCatalog, useDatabases, useSnapshot, type ObjectInfo } from './snapshot';
import { ChevronDown, ChevronRight, Database, Eye, RefreshCw, Share2, SquareFunction, Table2 } from '../icons';

const KIND_ICON: Record<ObjectInfo['kind'], typeof Table2> = {
  table: Table2,
  view: Eye,
  graph: Share2,
  procedure: SquareFunction,
  function: SquareFunction,
};

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
        className="tree-toggle"
        aria-label={`${open ? 'Collapse' : 'Expand'} ${db}`}
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
      </button>
      <NavLink to={base} end className="tree-db">
        <Database size={14} />
        {db}
      </NavLink>
      {open && (
        <ul>
          {snap.isLoading && <li className="muted">…</li>}
          {items.map((o) => {
            const Icon = KIND_ICON[o.kind] ?? Table2;
            return (
              <li key={`${o.kind}:${o.name}`}>
                <NavLink to={`${base}/obj/${o.kind}/${encodeURIComponent(o.name)}`} title={o.kind} className="tree-obj">
                  <Icon size={14} />
                  {o.name}
                </NavLink>
              </li>
            );
          })}
        </ul>
      )}
    </li>
  );
}

/** The explorer tree: databases, then objects by kind. */
export function Explorer() {
  const intl = useIntl();
  const { tenant = '', db: current } = useParams();
  const dbs = useDatabases();
  const qc = useQueryClient();
  const [filter, setFilter] = useState('');
  return (
    <div className="stack">
      <div className="row">
        <h2 className="side-title" style={{ flex: 1 }}>
          <FormattedMessage id="explorer.title" />
        </h2>
        <button className="icon-button" onClick={() => invalidateCatalog(qc)} aria-label={intl.formatMessage({ id: 'explorer.refresh' })}>
          <RefreshCw size={14} />
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
