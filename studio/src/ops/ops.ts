// Operator mode's data (chapter 44 step 17): the Admin API's replies
// through /v1/ops/ (decision ST35).

export interface OpsCluster {
  cluster_id: string;
  name: string;
  dc_id: string;
  topology: string;
  admin_state: string;
  nodes?: { node: string; addr: string }[];
  profile?: { server?: { version_text?: string } };
}

export interface Dc {
  dc_id: string;
  kind: string;
  state: string;
  dc_epoch: number;
  global_voter: boolean;
}

export interface Job {
  job_id: string;
  kind: string;
  state: string;
  owner: { worker_id?: string } | null;
  attempts: number;
  started_ms: number | null;
  error: string | null;
}

export interface OpsTenant {
  tenant_id: number;
  name: string;
  state: string;
  home_dc: string;
  child_count: number;
}

export interface Placement {
  mode: string;
  shards: { shard: string; cluster: string; size_bytes: number; busy_ms_per_sec: number; pinned: boolean }[];
  clusters: { cluster: string; state: string; full: number; bottleneck: string; holds_tenant: boolean }[];
}

export interface MoverJob {
  id: string;
  kind: string;
  tenant: number;
  state: string;
  source: string;
  target: string;
  progress?: { rows_copied?: number; lag_ms?: number; holds_ms?: number[]; tables?: number; tables_done?: number } | null;
  error?: string | null;
}

/** Catch-up lag as people read it. */
export function moveLag(j: MoverJob): string {
  const ms = j.progress?.lag_ms;
  if (ms === undefined || ms === null) return '';
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
}

export function fmtMs(ms: number | null | undefined): string {
  return ms ? new Date(ms).toISOString().replace('T', ' ').slice(0, 19) : '';
}

/**
 * A destructive or operator action: the operator types the target's name,
 * then a reason (chapter 44: "Destructive actions ask for the target's
 * name and a reason, which the audit records"). Null when abandoned.
 */
export function confirmTyped(target: string, action: string, ask: (q: string) => string | null = (q) => window.prompt(q)): string | null {
  const typed = ask(`${action}: type ${target} to confirm`);
  if (typed?.trim() !== target) return null;
  const reason = ask('Reason (recorded in the audit log)')?.trim();
  return reason ? reason : null;
}

// ---- Named operators (chapter 14, "Operators") ----
// Studio signs the operator in through /v1/ops:login and keeps the
// session token for this tab only (sessionStorage); every /v1/ops/ call
// carries it as x-sprig-operator, and the controller checks the person's
// role, DCs and scopes.

export const OPERATOR_HEADER = 'x-sprig-operator';
const KEY = 'sprig.operator';

export interface OperatorSession {
  token: string;
  name: string;
  expires_ms: number;
}

/** The login answer of /v1/ops:login and :enrol. */
export interface OperatorLogin {
  token?: string;
  expires_ms?: number;
  operator: { name: string; role: string; dcs: string[] };
  enrol_ticket?: string;
  otpauth_uri?: string;
  recovery_codes?: string[];
}

function store(): Storage | null {
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

/** The tab's operator session, if it is still valid at `now`. */
export function operatorSession(now: number = Date.now()): OperatorSession | null {
  try {
    const raw = store()?.getItem(KEY);
    if (!raw) return null;
    const s = JSON.parse(raw) as OperatorSession;
    return s.token && s.expires_ms > now ? s : null;
  } catch {
    return null;
  }
}

export function setOperatorSession(s: OperatorSession | null): void {
  try {
    if (s) store()?.setItem(KEY, JSON.stringify(s));
    else store()?.removeItem(KEY);
  } catch {
    // Private windows: the session lasts until the page reloads.
  }
}

/** The headers of an operator call. */
export function operatorHeaders(s: OperatorSession | null = operatorSession()): Record<string, string> {
  return s ? { [OPERATOR_HEADER]: s.token } : {};
}

/** The session a login answer opens, or null (enrolment or no token). */
export function sessionOf(r: OperatorLogin): OperatorSession | null {
  return r.token && r.expires_ms ? { token: r.token, name: r.operator.name, expires_ms: r.expires_ms } : null;
}

/** The body of an operator login: a 6-digit code goes as `code`, anything
 * else typed in the code field as `recovery_code` (`xxxxx-xxxxx`), as the
 * tenant sign-in's one field takes either. */
export function loginBody(name: string, password: string, code: string): Record<string, string> {
  const body: Record<string, string> = { name: name.trim(), password };
  const c = code.trim();
  if (/^\d{6}$/.test(c.replace(/\s/g, ''))) body.code = c.replace(/\s/g, '');
  else if (c) body.recovery_code = c;
  return body;
}

/** The base32 secret of an `otpauth://` link, for typing into an app. */
export function secretOf(uri: string): string {
  try {
    return new URL(uri).searchParams.get('secret') ?? '';
  } catch {
    return '';
  }
}
