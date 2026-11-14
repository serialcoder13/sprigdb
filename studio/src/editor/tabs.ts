// Editor tabs: text, database and title per tab; kept
// in IndexedDB per tenant and user while studio.persist_drafts is on.

import { create } from 'zustand';
import { clearDrafts, loadDraft, saveDraft } from '../lib/idb';
import { localId } from '../lib/ids';

export interface Tab {
  id: string;
  title: string;
  sql: string;
  db: string | null;
  /** The tab's language; SQL when absent. */
  language?: 'sql' | 'cypher' | 'gql' | 'mongo';
  /** For Cypher and GQL: the graph, sent as Bolt's `db.graph`. */
  graph?: string;
}

interface TabsState {
  tabs: Tab[];
  key: string | null;
  persist: boolean;
}

export const useTabs = create<TabsState>(() => ({
  tabs: [{ id: 'new', title: 'Query 1', sql: '', db: null }],
  key: null,
  persist: false,
}));

let saveTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleSave(): void {
  const s = useTabs.getState();
  if (!s.persist || !s.key) return;
  if (saveTimer) clearTimeout(saveTimer);
  const key = s.key;
  saveTimer = setTimeout(() => void saveDraft(key, useTabs.getState().tabs), 400);
}

/** Loads the drafts of `tenant`/`user` once per sign-in. */
export async function loadTabs(tenant: string, user: string, persist: boolean): Promise<void> {
  const key = `${tenant}/${user}`;
  if (useTabs.getState().key === key) return;
  useTabs.setState({ key, persist });
  if (!persist) return;
  const saved = await loadDraft<Tab[]>(key);
  if (saved && saved.length) useTabs.setState({ tabs: saved });
}

export function addTab(init: Partial<Tab> = {}): Tab {
  const n = useTabs.getState().tabs.length + 1;
  const tab: Tab = { id: localId(), title: `Query ${n}`, sql: '', db: null, ...init };
  useTabs.setState((s) => ({ tabs: [...s.tabs, tab] }));
  scheduleSave();
  return tab;
}

export function updateTab(id: string, patch: Partial<Tab>): void {
  useTabs.setState((s) => ({ tabs: s.tabs.map((t) => (t.id === id ? { ...t, ...patch } : t)) }));
  scheduleSave();
}

export function closeTab(id: string): string {
  const s = useTabs.getState();
  const i = s.tabs.findIndex((t) => t.id === id);
  let tabs = s.tabs.filter((t) => t.id !== id);
  if (!tabs.length) tabs = [{ id: localId(), title: 'Query 1', sql: '', db: null }];
  useTabs.setState({ tabs });
  scheduleSave();
  return (tabs[Math.max(0, i - 1)] ?? tabs[0]!).id;
}

/** At logout: forget drafts in memory and in the browser. */
export async function resetTabs(): Promise<void> {
  useTabs.setState({ tabs: [{ id: 'new', title: 'Query 1', sql: '', db: null }], key: null });
  await clearDrafts();
}

if (typeof window !== 'undefined') {
  window.addEventListener('sprig-signed-out', () => void resetTabs());
}
