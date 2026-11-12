import { useEffect, type ComponentType, type KeyboardEvent } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import { useIntl } from 'react-intl';
import { addTab, closeTab, useTabs } from '../editor/tabs';
import {
  Database,
  Eye,
  GitFork,
  KeyRound,
  Plus,
  Send,
  Settings,
  Share2,
  Shield,
  SquareFunction,
  SquareTerminal,
  Table2,
  X,
  Zap,
  ShardIcon,
  SproutIcon,
} from '../icons';
import { closeDoc, describe, openDoc, syncQueryTabs, resetForTenant, useWorkspace, type DocKind } from './store';

export const DOC_ICON: Record<DocKind, ComponentType<{ size?: number }>> = {
  home: SproutIcon,
  query: SquareTerminal,
  database: Database,
  table: Table2,
  view: Eye,
  graph: Share2,
  routine: SquareFunction,
  logic: SquareFunction,
  messaging: Send,
  kv: KeyRound,
  indexing: Zap,
  approvals: Zap,
  shards: ShardIcon,
  inheritance: GitFork,
  access: Shield,
  security: Shield,
  settings: Settings,
};

/** The tenant-relative part of the current path. */
export function useRel(): { base: string; rel: string } {
  const { tenant = '' } = useParams();
  const loc = useLocation();
  const base = `/t/${encodeURIComponent(tenant)}`;
  const rel = loc.pathname.startsWith(base) ? loc.pathname.slice(base.length).replace(/^\/+|\/+$/g, '') : '';
  return { base, rel };
}

/** The workspace's tab strip: query tabs and every page opened, side by side. */
export function WorkspaceTabs() {
  const intl = useIntl();
  const navigate = useNavigate();
  const { tenant = '' } = useParams();
  const { base, rel } = useRel();
  const queryTabs = useTabs((s) => s.tabs);
  const docs = useWorkspace((s) => s.docs);

  useEffect(() => resetForTenant(tenant), [tenant]);
  // Query tabs follow the editor's store; other pages open as you visit them.
  useEffect(() => {
    syncQueryTabs(
      queryTabs.map((t) => t.id),
      Object.fromEntries(queryTabs.map((t) => [t.id, t.title])),
    );
  }, [queryTabs]);
  useEffect(() => {
    const d = describe(rel);
    if (d) openDoc(d);
  }, [rel]);

  const close = (docRel: string) => {
    let next: string | null;
    if (docRel.startsWith('q/')) {
      const id = docRel.slice(2);
      const replacement = closeTab(id);
      next = closeDoc(docRel);
      if (rel === docRel) next = `q/${replacement}`;
    } else {
      next = closeDoc(docRel);
    }
    if (rel === docRel) void navigate(next ? `${base}/${next}` : base);
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
    const i = docs.findIndex((d) => d.rel === rel);
    const j = e.key === 'ArrowRight' ? i + 1 : i - 1;
    const d = docs[(j + docs.length) % docs.length];
    if (d) {
      e.preventDefault();
      void navigate(`${base}/${d.rel}`);
    }
  };

  return (
    <div className="ws-tabs">
      <div className="ws-tablist" role="tablist" aria-label={intl.formatMessage({ id: 'workspace.tabs' })} onKeyDown={onKey}>
        {docs.map((d) => {
          const Icon = DOC_ICON[d.kind];
          const active = d.rel === rel;
          return (
            <div key={d.rel || 'home'} role="presentation" className={`ws-tab${active ? ' on' : ''}`}>
              <button
                role="tab"
                aria-selected={active}
                tabIndex={active ? 0 : -1}
                title={`${d.title} (Delete closes)`}
                aria-keyshortcuts="Delete"
                onClick={() => void navigate(d.rel ? `${base}/${d.rel}` : base)}
                onAuxClick={(e) => {
                  if (e.button === 1) close(d.rel);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Delete') {
                    e.preventDefault();
                    close(d.rel);
                  }
                }}
              >
                <Icon size={14} />
                <span className="ws-tab-title">{d.title}</span>
                <span
                  className="ws-tab-close"
                  aria-hidden="true"
                  title={`${intl.formatMessage({ id: 'workspace.closeTab' })} ${d.title}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    close(d.rel);
                  }}
                >
                  <X size={12} />
                </span>
              </button>
            </div>
          );
        })}
      </div>
      <button
        className="ws-tab-new"
        aria-label={intl.formatMessage({ id: 'workspace.newQuery' })}
        title={intl.formatMessage({ id: 'workspace.newQuery' })}
        onClick={() => void navigate(`${base}/q/${addTab().id}`)}
      >
        <Plus size={14} />
      </button>
    </div>
  );
}
