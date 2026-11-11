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
