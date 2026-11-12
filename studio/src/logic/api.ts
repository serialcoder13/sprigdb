// The logic editors' API (chapter 44 step 26; chapter 37 "As built for
// Studio", round 4): modules, versions, Logic.Compile (a dry run), logs and
// dead letters. Deploys, rollbacks and test runs are statements.

import { json } from '../api/client';

export interface ModuleRow {
  module_id: number;
  db_id: number;
  name: string;
  kind: string;
  runtime: 'kirun' | 'javascript' | 'lua' | 'sql';
  security: 'invoker' | 'definer';
  tier_pref: 'auto' | 'mariadb' | 'engine';
  active_version: number | null;
  state: string;
  created_by: number;
}

export interface Reason {
  code: string;
  at: string;
  detail: string;
}

export interface Placement {
  tier: string;
  reasons: Reason[];
  lints: Reason[];
  route: unknown;
  reads: string[];
  writes: string[];
  cost: number;
}

export interface Signature {
  params?: { name: string; type: string; sql?: string }[];
  events?: { name: string; fields: { name: string; type: string }[] }[];
  returns?: string;
  scalar?: boolean;
}

export interface VersionRow {
  module_id: number;
  version: number;
  signature: Signature | null;
  tier: string;
  placement: Placement | null;
  state: string;
  compiler_version: string;
}

export interface ModuleDetail {
  module: ModuleRow;
  versions: VersionRow[];
  binding: { binding_id: number; timing: string; events: string; state: string } | null;
  schedule: { cron: string; time_zone: string; next_fire_ms: number; last_outcome: string | null; paused: boolean } | null;
  routines: boolean;
  source?: string | null;
  plsql?: string | null;
  source_version?: number;
}

export interface Diagnostic {
  severity: 'error' | 'warning';
  code: number;
  name: string;
  message: string;
}

export interface CompileResult {
  ok: boolean;
  kind?: string;
  name?: string;
  runtime?: string;
  tier?: string;
  placement?: Placement;
  signature?: Signature | null;
  plsql?: string | null;
  plsql_hidden?: boolean;
  diagnostics: Diagnostic[];
}

export interface LogRow {
  log_id: number;
  module_id: number;
  version: number | null;
  at: string;
  level: string;
  source: string;
  message: string;
}

export interface DeadLetter {
  dl_id: number;
  binding_id: number;
  version: number;
  event_key: string;
  event: unknown;
  attempts: number;
  error_code: number;
  error_message: string;
  last_failed_at: string;
  state: string;
}

const base = (db: string) => `/v1/databases/${encodeURIComponent(db)}/logic`;
const one = (db: string, kind: string, name: string) => `${base(db)}/${encodeURIComponent(kind)}/${encodeURIComponent(name)}`;

export const listModules = (db: string) => json<{ modules: ModuleRow[]; routines: boolean }>(base(db));
export const getModule = (db: string, kind: string, name: string) => json<ModuleDetail>(one(db, kind, name));
export const getVersion = (db: string, kind: string, name: string, v: number) =>
  json<{ version: number; source: string; plsql: string | null }>(`${one(db, kind, name)}/versions/${v}`);
export const compileDry = (db: string, sql: string) => json<CompileResult>(`${base(db)}:compile`, { body: { sql } });
export const logs = (db: string, kind: string, name: string, after = 0) =>
  json<{ logs: LogRow[] }>(`${one(db, kind, name)}/logs?after=${after}&limit=200`);
export const deadLetters = (db: string, kind: string, name: string, state?: string) =>
  json<{ dead_letters: DeadLetter[]; binding: number | null }>(`${one(db, kind, name)}/deadLetters${state ? `?state=${state}` : ''}`);
export const settleDeadLetters = (db: string, kind: string, name: string, verb: 'replay' | 'discard', ids: number[] | 'all') =>
  json<{ replaying?: number; skipped?: number; discarded?: number }>(`${one(db, kind, name)}/deadLetters:${verb}`, {
    body: ids === 'all' ? { all: true } : { ids },
  });

// ---- Rollout (Studio round 6; decisions RC18–RC21) ----

export interface RolloutRow {
  module_id: number;
  active_version: number;
  candidate_version: number;
  pct: number;
  by: 'user' | 'tenant';
  started_at: string;
}

export interface VersionStats {
  version: number;
  calls: number;
  errors: number;
  errorRate: number;
  meanMs: number;
  p95Ms: number | null;
  /** Calls per bucket: bounds `buckets_ms`, then slower. */
  histogram: number[];
  since: string;
}

export interface RolloutView {
  module: number;
  active_version: number | null;
  rollout: RolloutRow | null;
  stats: VersionStats[];
  /** Why the gateway would roll the candidate back now, if it would. */
  verdict: string | null;
  min_calls: number;
  buckets_ms: number[];
}

export const getRollout = (db: string, kind: string, name: string) => json<RolloutView>(`${one(db, kind, name)}/rollout`);

/** The newest log line saying a rollout rolled back by itself (RC19), if any. */
export function autoRollback(rows: readonly LogRow[]): LogRow | null {
  let best: LogRow | null = null;
  for (const l of rows) {
    if (/rolled back by itself/i.test(l.message) && (!best || l.log_id > best.log_id)) best = l;
  }
  return best;
}
