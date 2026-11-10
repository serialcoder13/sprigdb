import { getPref, setPref } from '../lib/prefs';

export type Theme = 'system' | 'light' | 'dark' | 'contrast';

export function currentTheme(): Theme {
  return getPref<Theme>('theme', 'system');
}

export function applyTheme(t: Theme = currentTheme()): void {
  const el = document.documentElement;
  if (t === 'system') el.removeAttribute('data-theme');
  else el.setAttribute('data-theme', t);
}

export function setTheme(t: Theme): void {
  setPref('theme', t);
  applyTheme(t);
  window.dispatchEvent(new Event('sprig-theme'));
}

/** Whether the page is drawn dark (for editors and charts). */
export function isDark(): boolean {
  const t = currentTheme();
  if (t === 'dark' || t === 'contrast') return true;
  if (t === 'light') return false;
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? false;
}
