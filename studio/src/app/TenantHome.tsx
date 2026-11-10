import { Link, useParams } from 'react-router';
import { useSession } from '../auth/session';
import { useDatabases } from '../catalog/snapshot';

/** The tenant's home: databases, who you are, the session. */
export function TenantHome() {
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const dbs = useDatabases();
  if (!me) return null;
  const home = me.principal.tenant === tenant;
  return (
    <div className="stack">
      <h1 style={{ margin: 0 }}>{tenant}</h1>
      {!home && (
        <p className="banner">
          You administer this tenant from {me.principal.tenant}. Its settings and accounts are open to you; data views use your home
          tenant.
        </p>
      )}
      <div className="row" style={{ alignItems: 'stretch' }}>
        <section className="card" style={{ flex: 1, minInlineSize: 260 }} aria-labelledby="dbs-h">
          <h2 id="dbs-h">Databases</h2>
          <ul>
            {(dbs.data ?? []).map((d) => (
              <li key={d}>
                <Link to={`/t/${encodeURIComponent(me.principal.tenant ?? '')}/db/${encodeURIComponent(d)}`}>{d}</Link>
              </li>
            ))}
          </ul>
        </section>
        <section className="card" style={{ flex: 1, minInlineSize: 260 }} aria-labelledby="me-h">
          <h2 id="me-h">You</h2>
          <dl>
            <dt>Signed in as</dt>
            <dd>
              {me.principal.login}@{me.principal.host}
            </dd>
            <dt>Session</dt>
            <dd>
              idle limit {me.session.idle_minutes} min, at most {me.session.max_hours} h
            </dd>
            <dt>Privileges</dt>
            <dd>
              {[
                me.privileges.tenant_admin && 'tenant admin',
                me.privileges.settings_admin && 'settings admin',
                me.privileges.create_user && 'create user',
              ]
                .filter(Boolean)
                .join(', ') || 'ordinary user'}
            </dd>
            {me.gateway && (
              <>
                <dt>Gateway</dt>
                <dd>
                  {me.gateway.id} in {me.gateway.dc}
                </dd>
              </>
            )}
          </dl>
        </section>
      </div>
    </div>
  );
}
