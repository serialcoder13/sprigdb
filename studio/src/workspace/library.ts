// Saved queries and history (the Studio and TUI redesign).
// Kept in the tenant's catalog per user (/v1/library), shared with `sprig`, so
// they follow the user between browsers. When the gateway can't keep them (an
// accounts-file login, or a gateway without the routes) they stay in the
// browser while studio.persist_drafts is on, or for the session otherwise.
// Queries saved in the browser before the catalog kept them are moved there
// the first time it answers.

import { create } from 'zustand';
import { json } from '../api/client';
import { loadDraft, saveDraft } from '../lib/idb';
import { localId } from '../lib/ids';

export type Lang = 'sql' | 'cypher' | 'gql' | 'mongo';

export interface SavedQuery {
  id: string;
  name: string;
  sql: string;
  db: string | null;
  language: Lang;
  saved_at: number;
}

export interface HistoryItem {
  id: string;
  sql: string;
  db: string | null;
  language: Lang;
  at: number;
  ms: number;
  ok: boolean;
  /** The error, for failed runs. */
  error?: string;
  /** `studio` or `shell`, for runs the catalog keeps. */
  client?: string;
}

/** Where the library lives: the tenant's catalog, the browser, or this session
 * only; `loading` until the catalog answers or fails. */
export type Where = 'loading' | 'catalog' | 'browser' | 'session';

const HISTORY_MAX = 500;

interface LibraryState {
  key: string | null;
  where: Where;
  historyMax: number;
  saved: SavedQuery[];
  history: HistoryItem[];
  /** The last failed write to the catalog, shown in the panels. */
  error: string | null;
}

export const useLibrary = create<LibraryState>(() => ({
  key: null,
  where: 'session',
  historyMax: HISTORY_MAX,
  saved: [],
  history: [],
  error: null,
}));

interface Reply {
  saved: { name: string; language: string; database: string | null; text: string; saved_at_ms: number }[];
  history: {
    id: string;
    language: string;
    database: string | null;
    text: string;
    ok: boolean;
    ms: number;
    error: string | null;
    client: string;
    at_ms: number;
  }[];
  history_max: number;
}

const LANGS: Lang[] = ['sql', 'cypher', 'gql', 'mongo'];
const lang = (l: string): Lang => (LANGS.includes(l as Lang) ? (l as Lang) : 'sql');

function storeLocal(): void {
  const s = useLibrary.getState();
  if (s.where !== 'browser' || !s.key) return;
  void saveDraft(`${s.key}/saved`, s.saved);
  void saveDraft(`${s.key}/history`, s.history);
}

/** Sends a write to the catalog; a failure is kept for the panels to show. */
function send(path: string, body: unknown): Promise<unknown> | null {
  if (useLibrary.getState().where !== 'catalog') return null;
  return json(path, { method: 'POST', body }).then(
    (r) => {
      useLibrary.setState({ error: null });
      return r;
    },
    (e: unknown) => {
      useLibrary.setState({ error: e instanceof Error ? e.message : String(e) });
      return null;
    },
  );
}

/** Loads the library of `tenant`/`user` once per sign-in. */
export async function loadLibrary(tenant: string, user: string, persist: boolean): Promise<void> {
  const key = `${tenant}/${user}`;
  if (useLibrary.getState().key === key) return;
  useLibrary.setState({ key, where: 'loading', saved: [], history: [], error: null });
  const reply = await json<Reply>(`/v1/library?history=${HISTORY_MAX}`).catch(() => null);
  if (useLibrary.getState().key !== key) return;
  const local = persist ? await loadDraft<SavedQuery[]>(`${key}/saved`) : null;
  // Saves and runs made while the library loaded (kept in memory meanwhile).
  const during = useLibrary.getState();
  if (reply) {
    const saved: SavedQuery[] = reply.saved.map((q) => ({
      id: `saved:${q.name}`,
      name: q.name,
      sql: q.text,
      db: q.database,
      language: lang(q.language),
      saved_at: q.saved_at_ms,
    }));
    const history: HistoryItem[] = reply.history.map((h) => ({
      id: `run:${h.id}`,
      sql: h.text,
      db: h.database,
      language: lang(h.language),
      at: h.at_ms,
      ms: h.ms,
      ok: h.ok,
      ...(h.error ? { error: h.error } : {}),
      client: h.client,
    }));
    useLibrary.setState({
      where: 'catalog',
      historyMax: reply.history_max,
      saved,
      history: [...during.history, ...history].slice(0, reply.history_max),
    });
    // Send what was run and saved while loading, and move queries saved in
    // this browser before the catalog kept them (the newest wins a name).
    for (const h of [...during.history].reverse()) sendRun(h);
    const moving = [...during.saved, ...(local ?? [])].filter(
      (q, i, all) => !saved.some((s) => s.name === q.name) && all.findIndex((x) => x.name === q.name) === i,
    );
    for (const q of moving.reverse()) saveQuery({ name: q.name, sql: q.sql, db: q.db, language: q.language });
    if (local?.length) void saveDraft(`${key}/saved`, []);
    return;
  }
  if (!persist) {
    useLibrary.setState({ where: 'session' });
    return;
  }
  const history = await loadDraft<HistoryItem[]>(`${key}/history`);
  useLibrary.setState({
    where: 'browser',
    saved: [...during.saved, ...(local ?? []).filter((q) => !during.saved.some((d) => d.name === q.name))],
    history: [...during.history, ...(history ?? [])].slice(0, HISTORY_MAX),
  });
  storeLocal();
}

/** Saves (or, with an existing name, replaces) a query. */
export function saveQuery(q: Omit<SavedQuery, 'id' | 'saved_at'>): SavedQuery {
  const s = useLibrary.getState();
  const old = s.saved.find((x) => x.name === q.name);
  const id = old?.id ?? (s.where === 'catalog' ? `saved:${q.name}` : localId());
  const item: SavedQuery = { ...q, id, saved_at: Date.now() };
  useLibrary.setState({ saved: [item, ...s.saved.filter((x) => x.id !== item.id)] });
  storeLocal();
  // The catalog keeps the text with its secrets masked; show what it kept.
  void send('/v1/library/saved', { name: q.name, language: q.language, database: q.db, text: q.sql })?.then((r) => {
    const text = (r as { text?: string } | null)?.text;
    if (text !== undefined && text !== item.sql)
      useLibrary.setState((st) => ({ saved: st.saved.map((x) => (x.id === id ? { ...x, sql: text } : x)) }));
  });
  return item;
}

export function deleteSaved(id: string): void {
  const q = useLibrary.getState().saved.find((x) => x.id === id);
  useLibrary.setState((s) => ({ saved: s.saved.filter((x) => x.id !== id) }));
  storeLocal();
  if (q) void send('/v1/library/saved:delete', { name: q.name });
}

export function recordRun(item: Omit<HistoryItem, 'id' | 'at'>): void {
  const s = useLibrary.getState();
  const max = s.where === 'catalog' ? s.historyMax : HISTORY_MAX;
  if (max === 0) return;
  const entry: HistoryItem = { ...item, id: localId(), at: Date.now(), client: 'studio' };
  useLibrary.setState({ history: [entry, ...s.history].slice(0, max) });
  storeLocal();
  sendRun(entry);
}

function sendRun(h: HistoryItem): void {
  void send('/v1/library/history', {
    language: h.language,
    database: h.db,
    text: h.sql,
    ok: h.ok,
    ms: h.ms,
    error: h.error ?? null,
    client: 'studio',
  });
}

export function clearHistory(): void {
  useLibrary.setState({ history: [] });
  storeLocal();
  void send('/v1/library/history:clear', {});
}

if (typeof window !== 'undefined') {
  window.addEventListener('sprig-signed-out', () =>
    useLibrary.setState({ key: null, where: 'session', saved: [], history: [], error: null }),
  );
}
