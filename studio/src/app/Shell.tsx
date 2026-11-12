import { useEffect, useState } from 'react';
import { NavLink, Outlet } from 'react-router';
import { FormattedMessage, useIntl } from 'react-intl';
import { logout, stopImpersonating, touch, useSession } from '../auth/session';
import { IdleDialog } from '../auth/IdleDialog';
import { TenantSwitcher } from '../tenant/TenantSwitcher';
import { Explorer } from '../catalog/Explorer';
import { setTheme, currentTheme, type Theme } from './theme';
import { useTabs } from '../editor/tabs';
import { useOpenCount } from '../approvals/InboxPage';
import { operatorSession } from '../ops/ops';
import { Mascot } from './Mascot';
import { Command, LogOut, Search } from '../icons';
import { SECTIONS, resolve, sectionOf, type Section } from '../workspace/sections';
import { useRel, WorkspaceTabs } from '../workspace/WorkspaceTabs';
import { StatusBar } from '../workspace/StatusBar';
import { CommandPalette } from '../workspace/CommandPalette';
import { HistoryPanel, SavedPanel } from '../workspace/LibraryPanels';
import { loadLibrary } from '../workspace/library';

/** Studio's workspace (the Studio and TUI redesign):
 * a title bar, an activity bar of sections, a side panel with the section's
 * pages and the database tree, tabs for everything open, and a status bar. */
export function Shell() {
  const me = useSession((s) => s.me);
  const imp = useSession((s) => s.impersonating);
  const intl = useIntl();
  const { base, rel } = useRel();
  const firstTab = useTabs((s) => s.tabs[0]?.id ?? 'new');
  const openCount = useOpenCount();
  const [palette, setPalette] = useState(false);
  // A panel-only section (saved queries, history) chosen in the activity bar,
  // for the page it was chosen on; navigating elsewhere goes back to the page's section.
  const [panelOn, setPanelOn] = useState<{ id: string; rel: string } | null>(null);
  useEffect(() => {
    if (me) void loadLibrary(me.principal.tenant ?? '-', me.principal.user, me.studio.persist_drafts);
  }, [me]);
  useEffect(() => {
    const on = () => touch();
    const keys = (e: KeyboardEvent) => {
      touch();
      if ((e.ctrlKey || e.metaKey) && !e.shiftKey && !e.altKey && (e.key === 'k' || e.key === 'p')) {
        e.preventDefault();
        setPalette((p) => !p);
      }
    };
    window.addEventListener('keydown', keys);
    window.addEventListener('pointerdown', on);
    return () => {
      window.removeEventListener('keydown', keys);
      window.removeEventListener('pointerdown', on);
    };
  }, []);
  const isOperator = !!(me?.platform_roles.includes('operator') || operatorSession());
  const panelSection = panelOn && panelOn.rel === rel ? SECTIONS.find((s) => s.id === panelOn.id) : undefined;
  const active = panelSection ?? sectionOf(rel);
  const target = (s: Section) => (s.id === 'query' ? `${base}/q/${firstTab}` : resolve(base, s.to));
  const activityItem = (s: Section) => {
    const label = intl.formatMessage({ id: s.label });
    if (s.panel) {
      const on = s.id === active.id;
      return (
        <button
          key={s.id}
          className={`activity-button${on ? ' on' : ''}`}
          aria-label={label}
          title={label}
          aria-pressed={on}
          onClick={() => setPanelOn(on ? null : { id: s.id, rel })}
        >
          <s.icon size={20} />
        </button>
      );
    }
    return (
      <NavLink
        key={s.id}
        to={target(s)}
        className={s.id === active.id && !s.to.startsWith('/') ? 'on' : undefined}
        onClick={() => setPanelOn(null)}
        aria-label={label}
        title={label}
        data-testid={s.testId}
        aria-current={s.id === active.id && !s.to.startsWith('/') ? 'page' : undefined}
      >
        <s.icon size={20} />
        {s.id === 'performance' && openCount > 0 && (
          <span className="activity-badge" data-testid="inbox-badge" aria-label={`${openCount} waiting`}>
            {openCount}
          </span>
        )}
      </NavLink>
    );
  };
  const visible = SECTIONS.filter((s) => !s.operatorOnly || isOperator);
  return (
    <div className="shell">
      <a className="skip" href="#content">
        <FormattedMessage id="nav.skip" />
      </a>
      {imp && (
        <div className="banner impersonation" role="status" data-testid="impersonation-banner">
          <FormattedMessage
            id="imp.banner"
            values={{ user: imp.user, by: imp.by, until: new Date(imp.until).toLocaleTimeString() }}
          />{' '}
          <button onClick={() => void stopImpersonating()}>
            <FormattedMessage id="imp.stop" />
          </button>
        </div>
      )}
      <header className="titlebar">
        <span className="brand">
          <Mascot pose="avatar" size={26} />
          <FormattedMessage id="app.name" />
        </span>
        <TenantSwitcher />
        <button className="search-button" onClick={() => setPalette(true)} aria-keyshortcuts="Control+K">
          <Search size={14} />
          <span className="search-text">{intl.formatMessage({ id: 'palette.open' })}</span>
          <kbd>
            <Command size={11} /> K
          </kbd>
        </button>
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
        <button onClick={() => void logout()} className="signout">
          <LogOut size={14} />
          <FormattedMessage id="session.signOut" />
        </button>
      </header>
      <div className="body">
        <nav className="activity" aria-label={intl.formatMessage({ id: 'nav.sections' })}>
          {visible.filter((s) => !s.bottom).map(activityItem)}
          <span className="grow" />
          {visible.filter((s) => s.bottom).map(activityItem)}
        </nav>
        <aside className="sidebar" aria-label={intl.formatMessage({ id: active.label })}>
          <h2 className="side-title">
            <FormattedMessage id={active.label} />
          </h2>
          {active.panel === 'saved' && <SavedPanel />}
          {active.panel === 'history' && <HistoryPanel />}
          {!active.panel && active.links.length > 0 && (
            <nav className="side-links" aria-label={intl.formatMessage({ id: active.label })}>
              {active.links.map((l) => (
                <NavLink key={l.to} to={resolve(base, l.to)} end={l.end}>
                  <FormattedMessage id={l.label} />
                  {l.badge && openCount > 0 && (
                    <span className="badge" aria-hidden="true">
                      {openCount}
                    </span>
                  )}
                </NavLink>
              ))}
            </nav>
          )}
          {!active.panel && <Explorer />}
        </aside>
        <section className="workspace">
          <WorkspaceTabs />
          <main className="main" id="content" tabIndex={-1}>
            <Outlet />
          </main>
        </section>
      </div>
      <StatusBar />
      <CommandPalette open={palette} onClose={() => setPalette(false)} />
      <IdleDialog />
    </div>
  );
}
