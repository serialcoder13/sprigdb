import { useEffect } from 'react';
import { NavLink, Outlet, useParams } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { logout, touch, useSession } from '../auth/session';
import { IdleDialog } from '../auth/IdleDialog';
import { TenantSwitcher } from '../tenant/TenantSwitcher';
import { Explorer } from '../catalog/Explorer';
import { setTheme, currentTheme, type Theme } from './theme';
import { useTabs } from '../editor/tabs';

export function Shell() {
  const { tenant = '' } = useParams();
  const me = useSession((s) => s.me);
  const intl = useIntl();
  const firstTab = useTabs((s) => s.tabs[0]?.id ?? 'new');
  useEffect(() => {
    const on = () => touch();
    window.addEventListener('keydown', on);
    window.addEventListener('pointerdown', on);
    return () => {
      window.removeEventListener('keydown', on);
      window.removeEventListener('pointerdown', on);
    };
  }, []);
  const base = `/t/${encodeURIComponent(tenant)}`;
  return (
    <div className="shell">
      <a className="skip" href="#content">
        <FormattedMessage id="nav.skip" />
      </a>
      <header className="topbar">
        <span className="brand">SprigDB Studio</span>
        <nav aria-label="Main">
          <NavLink to={base} end>
            <FormattedMessage id="nav.explorer" />
          </NavLink>
          <NavLink to={`${base}/q/${firstTab}`}>
            <FormattedMessage id="nav.query" />
          </NavLink>
          <NavLink to={`${base}/indexing`}>
            <FormattedMessage id="nav.indexing" />
          </NavLink>
          <NavLink to={`${base}/settings`}>
            <FormattedMessage id="nav.settings" />
          </NavLink>
          <NavLink to={`${base}/access/users`}>
            <FormattedMessage id="nav.access" />
          </NavLink>
        </nav>
        <TenantSwitcher />
        <label className="row">
          <span className="visually-hidden">{intl.formatMessage({ id: 'theme.label' })}</span>
          <select
            aria-label={intl.formatMessage({ id: 'theme.label' })}
            defaultValue={currentTheme()}
            onChange={(e) => setTheme(e.target.value as Theme)}
          >
            <option value="system">{intl.formatMessage({ id: 'theme.system' })}</option>
            <option value="light">{intl.formatMessage({ id: 'theme.light' })}</option>
            <option value="dark">{intl.formatMessage({ id: 'theme.dark' })}</option>
            <option value="contrast">{intl.formatMessage({ id: 'theme.contrast' })}</option>
          </select>
        </label>
        <span className="muted" data-testid="whoami">
          {me?.principal.login}
        </span>
        <button onClick={() => void logout()}>
          <FormattedMessage id="session.signOut" />
        </button>
      </header>
      <div className="body">
        <aside className="sidebar" aria-label="Explorer">
          <Explorer />
        </aside>
        <main className="main" id="content" tabIndex={-1}>
          <Outlet />
        </main>
      </div>
      <IdleDialog />
    </div>
  );
}
