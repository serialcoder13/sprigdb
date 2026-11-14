import { useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useSession } from '../auth/session';
import { execute, objects } from '../api/sql';
import { errorText } from '../api/errors';
import { useDatabases, useSnapshot } from '../catalog/snapshot';
import {
  boundsStatement,
  parseBounds,
  precheck,
  resetStatement,
  setHere,
  setStatement,
  showStatement,
  type Scope,
  type SettingRow,
} from './settings';

/** Settings at every scope: effective value, source
 * and bounds for each key, the same chain as `sprigctl config explain`.
 * Tenant administrators set tenant, database and object values (and a
 * descendant tenant's); ancestors and the operational scopes show
 * read-only as sources. */
export function SettingsPage() {
  const intl = useIntl();
  const { tenant = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const me = useSession((s) => s.me);
  const home = me?.principal.tenant === tenant;
  const kind = (home ? params.get('scope') : 'tenant') ?? 'tenant';
  const db = params.get('db') ?? '';
  const table = params.get('table') ?? '';
  const dbs = useDatabases();
  const snap = useSnapshot(kind === 'table' ? db : null);
  const qc = useQueryClient();
  const [filter, setFilter] = useState('');
  const [edit, setEdit] = useState<{ key: string; value: string; bounds: string } | null>(null);
  const [pending, setPending] = useState<string | null>(null);
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);

  const scope: Scope | null = useMemo(() => {
    if (kind === 'database') return db ? { kind: 'database', db } : null;
    if (kind === 'table') return db && table ? { kind: 'table', db, table } : null;
    return { kind: 'tenant', tenant: home ? null : tenant };
  }, [kind, db, table, home, tenant]);

  const rows = useQuery({
    queryKey: ['settings', tenant, kind, db, table],
    enabled: !!scope,
    queryFn: async (): Promise<SettingRow[]> => {
      const r = await objects(null, showStatement(scope!));
      return r.map((x) => ({
        scope: x.SETTING_SCOPE ?? '',
        key: x.SETTING_NAME ?? '',
        value: x.VALUE ?? '',
        source: x.SOURCE_SCOPE ?? '',
        bounds: x.BOUNDS ?? '',
        clamped: x.CLAMPED === 'YES',
      }));
    },
  });

  const run = async (sql: string) => {
    setPending(null);
    setMessage(null);
    try {
      const r = await execute(null, sql);
      const w = r.results[0]?.warnings ?? 0;
      setMessage({ ok: true, text: w ? `Done, with ${w} warning(s): narrower values were clamped.` : 'Done.' });
      setEdit(null);
      await qc.invalidateQueries({ queryKey: ['settings'] });
    } catch (e) {
      setMessage({ ok: false, text: errorText(e) });
    }
  };

  const shown = (rows.data ?? []).filter((r) => !filter || r.key.includes(filter));
  const check = edit ? precheck(edit.value, parseBounds(edit.bounds)) : null;
  const setScope = (k: string, extra: Record<string, string> = {}) => setParams({ scope: k, ...extra });
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>
        <FormattedMessage id="settings.title" />
      </h1>
      <div className="row">
        <label className="row">
          <FormattedMessage id="settings.scope" />
          <select
            value={kind}
            disabled={!home}
            onChange={(e) => setScope(e.target.value, db ? { db } : {})}
            data-testid="settings-scope"
          >
            <option value="tenant">tenant {tenant}</option>
            <option value="database">database</option>
            <option value="table">table</option>
          </select>
        </label>
        {(kind === 'database' || kind === 'table') && (
          <select aria-label="Database" value={db} onChange={(e) => setScope(kind, { db: e.target.value })}>
            <option value="">(choose)</option>
            {(dbs.data ?? []).map((d) => (
              <option key={d}>{d}</option>
            ))}
          </select>
        )}
        {kind === 'table' && (
          <select aria-label="Table" value={table} onChange={(e) => setScope(kind, { db, table: e.target.value })}>
            <option value="">(choose)</option>
            {(snap.data?.objects ?? [])
              .filter((o) => o.kind === 'table')
              .map((o) => (
                <option key={o.name}>{o.name}</option>
              ))}
          </select>
        )}
        <input
          type="search"
          placeholder={intl.formatMessage({ id: 'settings.filter' })}
          aria-label={intl.formatMessage({ id: 'settings.filter' })}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
      </div>
      {!home && <p className="banner">Settings of descendant tenant {tenant}, as its administrator.</p>}
      {message && (
        <p className={message.ok ? 'banner' : 'error'} role={message.ok ? 'status' : 'alert'} data-testid="settings-message">
          {message.text}
        </p>
      )}
      {pending && (
        <div className="card stack" role="region" aria-label="Statement to run">
          <pre data-testid="settings-statement">{pending}</pre>
          <div className="row">
            <button className="primary" onClick={() => void run(pending)}>
              Run it
            </button>
            <button onClick={() => setPending(null)}>Cancel</button>
          </div>
        </div>
      )}
      {rows.error && <p className="error">{errorText(rows.error)}</p>}
      <table data-testid="settings-table">
        <thead>
          <tr>
            <th scope="col">Key</th>
            <th scope="col">Effective value</th>
            <th scope="col">Source</th>
            <th scope="col">Bounds</th>
            <th scope="col">
              <span className="visually-hidden">Actions</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {shown.map((r) => {
            const editing = edit?.key === r.key;
            return (
              <tr key={r.key}>
                <td className="mono">{r.key}</td>
                <td className="mono">
                  {editing ? (
                    <span className="stack">
                      <input
                        aria-label={`New value for ${r.key}`}
                        value={edit.value}
                        onChange={(e) => setEdit({ ...edit, value: e.target.value })}
                        aria-invalid={!!check}
                      />
                      {check && <span className="error">{check}</span>}
                    </span>
                  ) : (
                    <>
                      {r.value}
                      {r.clamped && <span className="badge"> clamped</span>}
                    </>
                  )}
                </td>
                <td>{setHere(r, scope ?? { kind: 'tenant', tenant: null }) ? <strong>{r.source}</strong> : r.source}</td>
                <td className="mono">{r.bounds}</td>
                <td className="row">
                  {scope && editing ? (
                    <>
                      <button className="primary" disabled={!!check} onClick={() => setPending(setStatement(scope, r.key, edit.value))}>
                        <FormattedMessage id="settings.set" />
                      </button>
                      <button onClick={() => setEdit(null)}>Cancel</button>
                    </>
                  ) : (
                    scope && (
                      <>
                        <button onClick={() => setEdit({ key: r.key, value: r.value, bounds: r.bounds })} aria-label={`Set ${r.key}`}>
                          <FormattedMessage id="settings.set" />
                        </button>
                        {setHere(r, scope) && (
                          <button onClick={() => setPending(resetStatement(scope, r.key))} aria-label={`Reset ${r.key}`}>
                            <FormattedMessage id="settings.reset" />
                          </button>
                        )}
                        {me?.privileges.settings_admin && (
                          <button
                            aria-label={`Lock ${r.key} for lower scopes`}
                            onClick={() => setPending(boundsStatement(scope, r.key, { locked: true }))}
                          >
                            Lock
                          </button>
                        )}
                      </>
                    )
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
