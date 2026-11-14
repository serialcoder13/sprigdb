// Settings statements.
// Studio generates the same statements a SQL client would and shows them
// before they run; the gateway checks types, bounds and privileges
// (39080-39084).

import { ident, literal, qualified } from '../lib/sql';

export type Scope =
  | { kind: 'tenant'; tenant: string | null }
  | { kind: 'database'; db: string }
  | { kind: 'table'; db: string; table: string };

export interface SettingRow {
  scope: string;
  key: string;
  value: string;
  source: string;
  bounds: string;
  clamped: boolean;
}

export interface Bounds {
  min?: number;
  max?: number;
  allowed?: string[];
  locked: boolean;
}

function target(s: Scope): string {
  switch (s.kind) {
    case 'tenant':
      return s.tenant ? `TENANT ${ident(s.tenant)}` : 'TENANT';
    case 'database':
      return `DATABASE ${ident(s.db)}`;
    case 'table':
      return `TABLE ${qualified(s.db, s.table)}`;
  }
}

/** A value as the settings grammar takes it: number, word or string. */
export function settingValue(v: string): string {
  const t = v.trim();
  if (/^-?\d+(\.\d+)?$/.test(t)) return t;
  if (/^(true|false|on|off)$/i.test(t)) return t.toLowerCase();
  return literal(t);
}

export function showStatement(s: Scope, like?: string): string {
  const l = like ? ` LIKE ${literal(like)}` : '';
  return `SHOW SPRIG SETTINGS${l} FOR ${target(s)}`;
}

export function setStatement(s: Scope, key: string, value: string): string {
  return `ALTER ${target(s)} SPRIG SET (${key} = ${settingValue(value)})`;
}

export function resetStatement(s: Scope, key: string): string {
  return `ALTER ${target(s)} SPRIG RESET (${key})`;
}

export function boundsStatement(s: Scope, key: string, b: { min?: string; max?: string; allowed?: string; locked?: boolean }): string {
  const parts: string[] = [];
  if (b.min?.trim()) parts.push(`MIN ${Number(b.min)}`);
  if (b.max?.trim()) parts.push(`MAX ${Number(b.max)}`);
  if (b.allowed?.trim())
    parts.push(
      `ALLOWED (${b.allowed
        .split(',')
        .map((x) => (/^-?\d+(\.\d+)?$/.test(x.trim()) ? x.trim() : literal(x.trim())))
        .join(', ')})`,
    );
  if (b.locked !== undefined) parts.push(b.locked ? 'LOCKED' : 'UNLOCKED');
  return `ALTER ${target(s)} SPRIG SET BOUNDS (${key} ${parts.join(' ')})`;
}

/** `min 1, max 6, allowed ('on', 'off'), locked` → Bounds. */
export function parseBounds(text: string): Bounds {
  const b: Bounds = { locked: /\blocked\b/.test(text) };
  const min = /\bmin (-?[\d.]+)/.exec(text);
  const max = /\bmax (-?[\d.]+)/.exec(text);
  const allowed = /allowed \(([^)]*)\)/.exec(text);
  if (min) b.min = Number(min[1]);
  if (max) b.max = Number(max[1]);
  if (allowed) b.allowed = allowed[1]!.split(',').map((x) => x.trim().replace(/^'|'$/g, ''));
  return b;
}

/** The browser's pre-check against the bounds in force (the server decides). */
export function precheck(value: string, b: Bounds): string | null {
  if (b.locked) return 'Locked by a higher scope.';
  const v = value.trim();
  if (b.allowed && !b.allowed.includes(v)) return `Allowed: ${b.allowed.join(', ')}.`;
  const n = Number(v);
  if (b.min !== undefined && !Number.isNaN(n) && n < b.min) return `At least ${b.min}.`;
  if (b.max !== undefined && !Number.isNaN(n) && n > b.max) return `At most ${b.max}.`;
  return null;
}

/** Whether a row's value comes from this scope (editable) or above. */
export function setHere(row: SettingRow, s: Scope): boolean {
  return row.source === row.scope || (s.kind === 'table' && row.source === 'object');
}
