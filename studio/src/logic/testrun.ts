// Logic test runs (chapter 41 "Test runs", chapter 44 step 26; decisions
// RC13–RC17): `POST /v1/databases/{db}/logic:testRun` runs a deployed
// version or the editor's draft against given inputs, in a transaction that
// is always rolled back, with `fetch` answered by mocks (never sent). The
// answer is the whole report: the event or the error, every statement, the
// writes, logs, outbound calls, the snapshot tables after the run, budgets
// and timing.

import { json } from '../api/client';

export interface FetchMock {
  method: string;
  url: string;
  status: number;
  headers?: Record<string, string>;
  body: unknown;
}

export interface TestRunRequest {
  kind?: string;
  name?: string;
  version?: number;
  source?: string;
  args?: unknown;
  row?: unknown;
  old?: unknown;
  op?: string;
  record?: unknown;
  message?: unknown;
  rows?: Record<string, unknown[]>;
  fetch?: FetchMock[];
}

export interface Stmt {
  sql: string;
  kind: string;
  rows?: number;
  affected?: number;
  ms: number;
  error?: { code?: number; name?: string; message?: string } | null;
}

export interface Fetched {
  method: string;
  url: string;
  body?: string;
  status?: number;
  error?: string;
}

export interface Report {
  database: string;
  kind: string;
  name: string;
  version: number | null;
  draft: boolean;
  runtime: string;
  deployedTier?: string;
  event: string | null;
  result: unknown;
  error: { code?: number; name?: string; message?: string } | null;
  statements: Stmt[];
  writes: Stmt[];
  logs: string[];
  fetches: Fetched[];
  tables: Record<string, Record<string, unknown>[]>;
  budget: { limits?: Record<string, number>; used?: Record<string, number> } | null;
  timing: { totalMs?: number; setupMs?: number; runMs?: number; dbMs?: number } | null;
  notes: string[];
}

export const testRun = (db: string, req: TestRunRequest) => json<Report>(`/v1/databases/${encodeURIComponent(db)}/logic:testRun`, { body: req });

export interface Param {
  name: string;
  type: string;
}

/** Splits `a BIGINT, n DECIMAL(10,2), s VARCHAR(40)` into names and types. */
export function parseParams(text: string): Param[] {
  const out: Param[] = [];
  let depth = 0;
  let cur = '';
  const push = () => {
    const t = cur.trim();
    cur = '';
    if (!t) return;
    const m = /^(?:(?:IN|OUT|INOUT)\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(.*)$/i.exec(t);
    if (m) out.push({ name: m[1]!, type: m[2]!.trim() });
  };
  for (const ch of text) {
    if (ch === '(') depth++;
    if (ch === ')') depth--;
    if (ch === ',' && depth === 0) push();
    else cur += ch;
  }
  push();
  return out;
}

/** A KIRun definition's parameters, typed as the test form shows them. */
export function kirunParams(definition: string): Param[] {
  try {
    const d = JSON.parse(definition) as { parameters?: Record<string, { schema?: { type?: string | string[] } }> };
    return Object.entries(d.parameters ?? {}).map(([name, p]) => {
      const t = p.schema?.type;
      return { name, type: (Array.isArray(t) ? t[0] : t) ?? 'ANY' };
    });
  } catch {
    return [];
  }
}

/** A typed argument from the text typed for it: numbers, booleans and JSON by
 * the parameter's type, text otherwise; blank is `null`. */
export function argValue(type: string, text: string): unknown {
  const t = text.trim();
  if (t === '' || /^null$/i.test(t)) return null;
  const ty = type.toLowerCase();
  if (/^(int|bigint|integer|smallint|tinyint|mediumint|long|float|double|decimal|numeric|real|number)/.test(ty)) {
    const n = Number(t);
    if (!Number.isFinite(n)) throw new Error(`'${t}' isn't a number`);
    return n;
  }
  if (/^bool/.test(ty)) {
    if (/^(true|1)$/i.test(t)) return true;
    if (/^(false|0)$/i.test(t)) return false;
    throw new Error(`'${t}' isn't true or false`);
  }
  if (/^(json|object|array|any)/.test(ty)) {
    try {
      return JSON.parse(t) as unknown;
    } catch {
      if (ty.startsWith('any')) return text;
      throw new Error(`'${t}' isn't JSON`);
    }
  }
  return text;
}

/** JSON typed in a box: blank is `undefined`, anything else must parse. */
export function jsonField(what: string, text: string): unknown {
  if (!text.trim()) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch (e) {
    throw new Error(`${what}: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
  }
}

/** The snapshot box: `{"table": [{...}, ...], ...}`. */
export function snapshotRows(text: string): Record<string, unknown[]> | undefined {
  const v = jsonField('Snapshot rows', text);
  if (v === undefined) return undefined;
  if (!v || typeof v !== 'object' || Array.isArray(v)) throw new Error('Snapshot rows: an object of table name → list of rows');
  for (const [t, rows] of Object.entries(v)) {
    if (!Array.isArray(rows)) throw new Error(`Snapshot rows: ${t} needs a list of rows`);
    if (rows.some((r) => !r || typeof r !== 'object' || Array.isArray(r))) throw new Error(`Snapshot rows: each row of ${t} is an object of column values`);
  }
  return v as Record<string, unknown[]>;
}

export interface MockForm {
  method: string;
  url: string;
  status: string;
  body: string;
}

/** The mock rows as the request's `fetch`: a body that parses as JSON is sent as JSON, text otherwise. */
export function mocks(rows: readonly MockForm[]): FetchMock[] {
  return rows
    .filter((m) => m.url.trim())
    .map((m) => {
      const status = m.status.trim() ? Number(m.status) : 200;
      if (!Number.isInteger(status) || status < 100 || status > 599) throw new Error(`mock ${m.url}: status ${m.status} isn't an HTTP status`);
      let body: unknown;
      try {
        body = m.body.trim() ? (JSON.parse(m.body) as unknown) : '';
      } catch {
        body = m.body;
      }
      return { method: m.method === 'any' ? '' : m.method, url: m.url.trim(), status, body };
    });
}

/** Budget lines: what was used of each limit. */
export interface BudgetLine {
  key: string;
  label: string;
  used: number | null;
  limit: number | null;
}

const BUDGETS: [string, string, string][] = [
  ['wallMs', 'wallMs', 'Wall time (ms)'],
  ['steps', 'steps', 'Steps'],
  ['dataOps', 'dataOps', 'Data operations'],
  ['rowsRead', 'rowsRead', 'Rows read'],
  ['peakBytes', 'memoryBytes', 'Peak memory (bytes)'],
  ['loopIterations', 'loopIterations', 'Loop iterations'],
];

export function budgetLines(b: Report['budget']): BudgetLine[] {
  if (!b) return [];
  const u = b.used ?? {};
  const l = b.limits ?? {};
  return BUDGETS.filter(([used, limit]) => u[used] !== undefined || l[limit] !== undefined).map(([used, limit, label]) => ({
    key: used,
    label,
    used: typeof u[used] === 'number' ? u[used] : null,
    limit: typeof l[limit] === 'number' ? l[limit] : null,
  }));
}
