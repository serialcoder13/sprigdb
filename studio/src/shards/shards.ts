// The shards view's data: table groups from the
// tenant's catalog with each group's map from the controller.

export interface ShardRange {
  shard: number;
  /** First hash, hex. */
  lo: string;
  /** One past the last, hex; null is 2^64. */
  hi: string | null;
  cluster: string;
  state: string;
  epoch: number;
  job: string | null;
}

export interface ShardMember {
  db: string;
  table: string;
  routing: string;
  key_columns: string[] | null;
  parent: string | null;
  parent_fk: string | null;
  rekey_state: string;
  pinned: boolean;
}

export interface ShardGroup {
  group: number;
  kind: string;
  name: string;
  key_spec: { parts?: { class?: string; [k: string]: unknown }[] } | null;
  state: string;
  min_shards: number | null;
  origin: string;
  advisor_run: string | null;
  updated_at: string | null;
  members: ShardMember[];
  map?: { version: number | null; state: string | null; shards: ShardRange[] };
}

/** The group's key: the root member's key columns and the key's classes. */
export function keyText(g: ShardGroup): string {
  const root = g.members.find((m) => m.routing === 'columns' && m.key_columns?.length);
  const cols = root?.key_columns?.join(', ') ?? '';
  const classes = (g.key_spec?.parts ?? []).map((p) => p.class ?? '?').join(', ');
  if (!cols && !classes) return g.kind === 'sharded' ? '(not chosen yet)' : '—';
  return classes ? `${cols || '?'} (${classes})` : cols;
}

/** A range as `lo – hi` in hex, with 2^64 for an open end. */
export function rangeText(r: ShardRange): string {
  return `${r.lo} – ${r.hi ?? '2^64'}`;
}

/** Shards, distinct clusters (in order of first use) and held ranges. */
export function shardSummary(g: ShardGroup): { shards: number; clusters: string[]; held: number } {
  const shards = g.map?.shards ?? [];
  const clusters: string[] = [];
  for (const s of shards) if (!clusters.includes(s.cluster)) clusters.push(s.cluster);
  return { shards: shards.length, clusters, held: shards.filter((s) => s.state !== 'serving').length };
}
