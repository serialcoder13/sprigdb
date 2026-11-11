import { useState } from 'react';
import { Link, useParams } from 'react-router';
import { FormattedMessage } from 'react-intl';
import { useQuery } from '@tanstack/react-query';
import { useSnapshot } from './snapshot';
import { objects, rows as fetchRows } from '../api/sql';
import { qualified } from '../lib/sql';
import { Browse } from '../browse/Browse';
import { addTab } from '../editor/tabs';
import { useNavigate } from 'react-router';

type Tab = 'columns' | 'indexes' | 'ddl' | 'data';

/** An object's detail (chapter 44 step 7): columns, indexes with their
 * origin, the definition, and data (step 10). */
export function ObjectPage() {
  const { tenant = '', db = '', kind = 'table', name = '' } = useParams();
  const snap = useSnapshot(db);
  const navigate = useNavigate();
  const [tab, setTab] = useState<Tab>(kind === 'table' || kind === 'view' ? 'data' : 'ddl');
  const cols = snap.data?.columns[name] ?? [];
  const isTable = kind === 'table' || kind === 'view';
  const isGraph = kind === 'graph';
  const indexes = useQuery({
    queryKey: ['obj', db, name, 'indexes'],
    queryFn: () => objects(db, `SHOW INDEX FROM ${qualified(db, name)}`),
    enabled: tab === 'indexes' && kind === 'table',
  });
  const ddl = useQuery({
    queryKey: ['obj', db, name, 'ddl', kind],
    queryFn: async () => {
      const what = kind === 'view' ? 'VIEW' : kind === 'procedure' ? 'PROCEDURE' : kind === 'function' ? 'FUNCTION' : 'TABLE';
      const r = await fetchRows(db, `SHOW CREATE ${what} ${qualified(db, name)}`);
      const i = r.columns.findIndex((c) => /^Create /i.test(c.name));
      return r.rows[0]?.[i >= 0 ? i : 1] ?? '';
    },
    enabled: tab === 'ddl' && !isGraph,
  });
  const graph = isGraph ? snap.data?.objects.find((o) => o.kind === 'graph' && o.name === name) : undefined;
  const tabs: Tab[] = isTable ? (kind === 'table' ? ['data', 'columns', 'indexes', 'ddl'] : ['data', 'columns', 'ddl']) : isGraph ? [] : ['ddl'];
  return (
    <div className="stack">
      <nav aria-label="Breadcrumb" className="muted">
        <Link to={`/t/${encodeURIComponent(tenant)}/db/${encodeURIComponent(db)}`}>{db}</Link> / {kind}
      </nav>
      <div className="row">
        <h1 style={{ margin: 0 }}>{name}</h1>
        <button
          onClick={() => {
            const t = isGraph
              ? addTab({ db, graph: name, language: 'cypher', sql: 'MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25;', title: name })
              : addTab({ db, sql: isTable ? `SELECT * FROM ${qualified(null, name)} LIMIT 100;` : '', title: name });
            void navigate(`/t/${encodeURIComponent(tenant)}/q/${t.id}`);
          }}
        >
          Query
        </button>
        {kind === 'table' && (
          <Link to={`/t/${encodeURIComponent(tenant)}/settings?scope=table&db=${encodeURIComponent(db)}&table=${encodeURIComponent(name)}`}>
            Settings
          </Link>
        )}
      </div>
      <div className="tabs" role="tablist">
        {tabs.map((t) => (
          <button key={t} role="tab" aria-selected={tab === t} onClick={() => setTab(t)}>
            <FormattedMessage id={`object.${t}`} />
          </button>
        ))}
      </div>
      {tab === 'columns' && (
        <table>
          <thead>
            <tr>
              <th scope="col">#</th>
              <th scope="col">Name</th>
              <th scope="col">Type</th>
              <th scope="col">Null</th>
              <th scope="col">Key</th>
              <th scope="col">Default</th>
              <th scope="col">Extra</th>
            </tr>
          </thead>
          <tbody>
            {cols.map((c) => (
              <tr key={c.name}>
                <td>{c.position}</td>
                <td>{c.name}</td>
                <td className="mono">{c.type}</td>
                <td>{c.nullable ? 'yes' : 'no'}</td>
                <td>{c.key}</td>
                <td className="mono">{c.default ?? 'NULL'}</td>
                <td>{c.extra}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {tab === 'indexes' && (
        <div className="stack">
          <p className="muted">
            Indexes you or your tools created. Automatic indexes are hidden from SHOW INDEX; see{' '}
            <Link to={`/t/${encodeURIComponent(tenant)}/indexing?db=${encodeURIComponent(db)}&table=${encodeURIComponent(name)}`}>
              automatic indexing
            </Link>
            .
          </p>
          {indexes.error && <p className="error">{indexes.error.message}</p>}
          <table>
            <thead>
              <tr>
                <th scope="col">Index</th>
                <th scope="col">Seq</th>
                <th scope="col">Column</th>
                <th scope="col">Unique</th>
                <th scope="col">Type</th>
              </tr>
            </thead>
            <tbody>
              {(indexes.data ?? []).map((r, i) => (
                <tr key={i}>
                  <td>{r.Key_name}</td>
                  <td>{r.Seq_in_index}</td>
                  <td>{r.Column_name}</td>
                  <td>{r.Non_unique === '0' ? 'yes' : 'no'}</td>
                  <td>{r.Index_type}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {isGraph && (
        <section className="stack" aria-label="Graph" data-testid="graph-object">
          {!graph && !snap.isLoading && <p className="muted">No such graph, or you can't read it.</p>}
          {graph && (
            <>
              <dl className="kv">
                <dt>Kind</dt>
                <dd>{graph.graph_kind ?? ''}</dd>
                <dt>Labels</dt>
                <dd className="mono" data-testid="graph-labels">
                  {(graph.labels ?? []).join(', ') || '—'}
                </dd>
                <dt>Relationship types</dt>
                <dd className="mono" data-testid="graph-types">
                  {(graph.types ?? []).join(', ') || '—'}
                </dd>
              </dl>
              {graph.partial && <p className="muted">You may read only some of this graph's labels and relationship types (grants on single elements).</p>}
            </>
          )}
        </section>
      )}
      {tab === 'ddl' && !isGraph && (ddl.error ? <p className="error">{ddl.error.message}</p> : <pre data-testid="ddl">{ddl.data ?? '…'}</pre>)}
      {tab === 'data' && isTable && cols.length > 0 && <Browse key={`${db}.${name}`} db={db} table={name} cols={cols} editable={kind === 'table'} />}
    </div>
  );
}
