// Per-browser preferences: theme, locale, column widths. Never tokens,
// results or drafts. Storage can be missing or throw (private windows,
// blocked site data); Studio works without it.

const PREFIX = 'sprig-studio.';

function store(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function getPref<T>(key: string, fallback: T): T {
  try {
    const raw = store()?.getItem(PREFIX + key);
    return raw == null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function setPref(key: string, value: unknown): void {
  try {
    store()?.setItem(PREFIX + key, JSON.stringify(value));
  } catch {
    // Preferences are a convenience.
  }
}
