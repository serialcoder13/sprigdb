import { beforeEach, describe, expect, it, vi } from 'vitest';

const calls: { path: string; body: unknown }[] = [];
let reply: unknown = null;
let hold: Promise<void> | null = null;

vi.mock('../api/client', () => ({
  json: (path: string, opts?: { body?: unknown }) => {
    calls.push({ path, body: opts?.body });
    if (path.startsWith('/v1/library?'))
      return (hold ?? Promise.resolve()).then(() => (reply ? reply : Promise.reject(new Error('404'))));
    if (path === '/v1/library/saved') {
      const b = opts?.body as { text: string; name: string };
      return Promise.resolve({ ...b, text: b.text.replace(/'hunter2'/, "'***'") });
    }
    return Promise.resolve({});
  },
}));
vi.mock('../lib/idb', () => ({ loadDraft: () => Promise.resolve(null), saveDraft: () => Promise.resolve() }));

const { clearHistory, deleteSaved, loadLibrary, recordRun, saveQuery, useLibrary } = await import('./library');

const reset = () => {
  calls.length = 0;
  useLibrary.setState({ key: null, where: 'session', historyMax: 500, saved: [], history: [], error: null });
};

describe('saved queries and history (decision R4)', () => {
  beforeEach(reset);

  it('saves, replaces by name, and deletes', () => {
    const a = saveQuery({ name: 'open orders', sql: 'SELECT 1', db: 'shop', language: 'sql' });
    saveQuery({ name: 'paid', sql: 'SELECT 2', db: 'shop', language: 'sql' });
    const a2 = saveQuery({ name: 'open orders', sql: 'SELECT 3', db: 'shop', language: 'sql' });
    expect(a2.id).toBe(a.id);
    expect(useLibrary.getState().saved.map((s) => [s.name, s.sql])).toEqual([
      ['open orders', 'SELECT 3'],
      ['paid', 'SELECT 2'],
    ]);
    deleteSaved(a.id);
    expect(useLibrary.getState().saved.map((s) => s.name)).toEqual(['paid']);
    expect(calls).toHaveLength(0);
  });

  it('keeps the newest 500 runs, newest first', () => {
    for (let i = 0; i < 505; i++) recordRun({ sql: `SELECT ${i}`, db: null, language: 'sql', ms: 1, ok: true });
    const h = useLibrary.getState().history;
    expect(h).toHaveLength(500);
    expect(h[0]!.sql).toBe('SELECT 504');
    clearHistory();
    expect(useLibrary.getState().history).toHaveLength(0);
  });
});

describe('the library in the tenant catalog', () => {
  beforeEach(() => {
    reset();
    reply = {
      saved: [{ name: 'paid', language: 'sql', database: 'shop', text: 'SELECT 2', saved_at_ms: 5 }],
      history: [
        {
          id: '9',
          language: 'sql',
          database: null,
          text: 'SELECT 9',
          ok: true,
          ms: 3,
          error: null,
          client: 'shell',
          at_ms: 7,
        },
      ],
      history_max: 2,
    };
  });

  it('loads from the catalog and writes every change back', async () => {
    await loadLibrary('acme', 'app', true);
    const s = useLibrary.getState();
    expect(s.where).toBe('catalog');
    expect(s.saved.map((q) => q.name)).toEqual(['paid']);
    expect(s.history[0]).toMatchObject({ sql: 'SELECT 9', client: 'shell' });

    saveQuery({ name: 'mk', sql: "CREATE USER u IDENTIFIED BY 'hunter2'", db: null, language: 'sql' });
    await Promise.resolve();
    await Promise.resolve();
    expect(useLibrary.getState().saved[0]!.sql).toBe("CREATE USER u IDENTIFIED BY '***'");

    recordRun({ sql: 'SELECT 1', db: 'shop', language: 'sql', ms: 4, ok: true });
    recordRun({ sql: 'SELECT 2', db: 'shop', language: 'sql', ms: 4, ok: true });
    expect(useLibrary.getState().history).toHaveLength(2);
    deleteSaved('saved:paid');
    clearHistory();
    expect(calls.map((c) => c.path)).toEqual([
      '/v1/library?history=500',
      '/v1/library/saved',
      '/v1/library/history',
      '/v1/library/history',
      '/v1/library/saved:delete',
      '/v1/library/history:clear',
    ]);
    expect(calls[2]!.body).toMatchObject({ client: 'studio', database: 'shop', text: 'SELECT 1' });
    expect(calls[4]!.body).toEqual({ name: 'paid' });
  });

  it('keeps what was saved and run while the library loaded', async () => {
    let release = () => {};
    hold = new Promise<void>((r) => (release = r));
    const run = loadLibrary('acme', 'early', false);
    expect(useLibrary.getState().where).toBe('loading');
    saveQuery({ name: 'early bird', sql: 'SELECT 5', db: 'shop', language: 'sql' });
    recordRun({ sql: 'SELECT 5', db: 'shop', language: 'sql', ms: 2, ok: true });
    release();
    await run;
    hold = null;
    const s = useLibrary.getState();
    expect(s.where).toBe('catalog');
    expect(s.saved.map((q) => q.name)).toEqual(['early bird', 'paid']);
    expect(s.history.map((h) => h.sql)).toEqual(['SELECT 5', 'SELECT 9']);
    expect(calls.map((c) => c.path)).toEqual(['/v1/library?history=500', '/v1/library/history', '/v1/library/saved']);
  });

  it('falls back to the session when the catalog can’t keep them', async () => {
    reply = null;
    await loadLibrary('acme', 'app', false);
    expect(useLibrary.getState().where).toBe('session');
    saveQuery({ name: 'x', sql: 'SELECT 1', db: null, language: 'sql' });
    expect(calls.map((c) => c.path)).toEqual(['/v1/library?history=500']);
  });
});
