import { Link, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { Mascot } from './Mascot';
import { useSession } from '../auth/session';
import { useDatabases } from '../catalog/snapshot';

/** The tenant's home: databases, who you are, the session. */
export function TenantHome() {
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const dbs = useDatabases();
  const intl = useIntl();
  if (!me) return null;
  const home = me.principal.tenant === tenant;
  return (
    <div className="stack">
      <div className="welcome-hero">
        <Mascot pose="waving" size={150} />
        <div>
          <h1>{intl.formatMessage({ id: 'welcome.title' }, { user: me.principal.login })}</h1>
          <p className="muted" style={{ margin: 0 }}>
            {intl.formatMessage({ id: 'welcome.lead' })}
          </p>
          <p className="muted" style={{ margin: '4px 0 0' }}>
            {tenant}
          </p>
        </div>
      </div>
      {!home && (
        <p className="banner">
          You administer this tenant from {me.principal.tenant}. Its settings and accounts are open to you; data views use your home
          tenant.
        </p>
      )}
      <div className="row" style={{ alignItems: 'stretch' }}>
        <section className="card" style={{ flex: 1, minInlineSize: 260 }} aria-labelledby="dbs-h">
          <h2 id="dbs-h">Databases</h2>
          <ul className="linklist">
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
