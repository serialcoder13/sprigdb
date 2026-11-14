// Editor drafts in IndexedDB: per tenant and user, never
// results or tokens, cleared at logout; only when studio.persist_drafts.
// Any failure (private windows, blocked storage) leaves drafts in memory.

const DB = 'sprig-studio';
const STORE = 'drafts';

function open(): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    try {
      const req = indexedDB.open(DB, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(STORE);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

async function tx<T>(mode: IDBTransactionMode, f: (s: IDBObjectStore) => IDBRequest<T>): Promise<T | null> {
  const db = await open();
  if (!db) return null;
  return new Promise((resolve) => {
    try {
      const t = db.transaction(STORE, mode);
      const r = f(t.objectStore(STORE));
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

export function loadDraft<T>(key: string): Promise<T | null> {
  return tx<T>('readonly', (s) => s.get(key) as IDBRequest<T>);
}

export async function saveDraft(key: string, value: unknown): Promise<void> {
  await tx('readwrite', (s) => s.put(value, key));
}

export async function clearDrafts(): Promise<void> {
  await tx('readwrite', (s) => s.clear());
}
