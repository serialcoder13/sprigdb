// Workspace tabs (decision R1): every page you open becomes a tab, beside the
// query tabs, so several things stay open side by side. Query tabs keep their
// own store (editor/tabs.ts, with drafts); this store keeps the other pages
// and the order of everything.

import { create } from 'zustand';

export type DocKind =
  | 'home'
  | 'query'
  | 'database'
  | 'table'
  | 'view'
  | 'graph'
  | 'routine'
  | 'logic'
  | 'messaging'
  | 'kv'
  | 'indexing'
  | 'approvals'
  | 'shards'
  | 'inheritance'
  | 'access'
  | 'security'
  | 'settings';

export interface Doc {
  /** The tab's route, relative to the tenant base (`q/<id>` for query tabs). */
  rel: string;
  kind: DocKind;
  /** A page title; query tabs take theirs from the editor's store. */
  title: string;
}

interface WorkspaceState {
  tenant: string | null;
  docs: Doc[];
}

export const useWorkspace = create<WorkspaceState>(() => ({ tenant: null, docs: [] }));

/** Starts a fresh workspace when the tenant changes (called from an effect). */
export function resetForTenant(tenant: string): void {
  if (useWorkspace.getState().tenant !== tenant) useWorkspace.setState({ tenant, docs: [] });
}

/** Opens (or keeps) the tab for `doc`. */
export function openDoc(doc: Doc): void {
  const { docs } = useWorkspace.getState();
  const i = docs.findIndex((d) => d.rel === doc.rel);
  if (i < 0) useWorkspace.setState({ docs: [...docs, doc] });
  else if (docs[i]!.title !== doc.title) {
    useWorkspace.setState({ docs: docs.map((d, j) => (j === i ? doc : d)) });
  }
}

/** Closes the tab at `rel`; returns the tab to show next, if any. */
export function closeDoc(rel: string): string | null {
  const { docs } = useWorkspace.getState();
  const i = docs.findIndex((d) => d.rel === rel);
  if (i < 0) return null;
  const left = docs.filter((d) => d.rel !== rel);
  useWorkspace.setState({ docs: left });
  return (left[Math.max(0, i - 1)] ?? left[0])?.rel ?? null;
}

/** Keeps the query tabs in the workspace in step with the editor's tabs. */
export function syncQueryTabs(ids: string[], titles: Record<string, string>): void {
  const { docs } = useWorkspace.getState();
  const want = new Set(ids.map((id) => `q/${id}`));
  let next = docs.filter((d) => d.kind !== 'query' || want.has(d.rel));
  for (const id of ids) {
    const rel = `q/${id}`;
    const title = titles[id] ?? id;
    const at = next.findIndex((d) => d.rel === rel);
    if (at < 0) next = [...next, { rel, kind: 'query', title }];
    else if (next[at]!.title !== title) next = next.map((d, j) => (j === at ? { ...d, title } : d));
  }
  if (next.length !== docs.length || next.some((d, i) => d !== docs[i])) useWorkspace.setState({ docs: next });
}

const TITLES: Record<string, [DocKind, string]> = {
  '': ['home', 'Welcome'],
  redis: ['kv', 'Redis'],
  messaging: ['messaging', 'Messaging'],
  logic: ['logic', 'Logic'],
  indexing: ['indexing', 'Automatic indexing'],
  inbox: ['approvals', 'Approvals'],
  'inbox/policy': ['approvals', 'Approval policy'],
  shards: ['shards', 'Shards'],
  inheritance: ['inheritance', 'Inheritance'],
  'access/users': ['access', 'Users and access'],
  'access/keys': ['access', 'API keys'],
  'access/grants': ['access', 'Grants'],
  policies: ['access', 'Roles and policies'],
  security: ['security', 'Security'],
  settings: ['settings', 'Settings'],
};

const OBJECT_KINDS: Record<string, DocKind> = {
  table: 'table',
  view: 'view',
  graph: 'graph',
  procedure: 'routine',
  function: 'routine',
};

/** The tab a tenant-relative path opens; null for query tabs (their own store). */
export function describe(rel: string): Doc | null {
  if (rel === 'q' || rel.startsWith('q/')) return null;
  const known = TITLES[rel];
  if (known) return { rel, kind: known[0], title: known[1] };
  const parts = rel.split('/').map(decodeURIComponent);
  if (parts[0] === 'db' && parts.length === 2) return { rel, kind: 'database', title: parts[1]! };
  if (parts[0] === 'db' && parts[2] === 'obj' && parts.length === 5) {
    return { rel, kind: OBJECT_KINDS[parts[3]!] ?? 'table', title: parts[4]! };
  }
  if (parts[0] === 'logic' && parts[2] === 'new') return { rel, kind: 'logic', title: `New ${parts[3] ?? 'module'}` };
  if (parts[0] === 'logic' && parts.length === 4) return { rel, kind: 'logic', title: parts[3]! };
  if (parts[0] === 'access') return { rel, kind: 'access', title: parts[1] ?? 'Access' };
  return { rel, kind: 'home', title: parts[parts.length - 1] || 'Page' };
}
