// The catalog snapshot: names by kind and columns per logical database, from
// the gateway's Catalog service, which reads them with the user's own rights
// and answers 304 to If-None-Match while the tenant's catalog and auth
// versions haven't moved. Refetched after DDL, on a name error and every 60 s while
// visible.

import { useQuery, type QueryClient } from '@tanstack/react-query';
import { cachedJson } from '../api/client';

export interface ObjectInfo {
  name: string;
  kind: 'table' | 'view' | 'procedure' | 'function' | 'graph';
  rows: number | null;
  engine: string | null;
  comment: string;
  /** Graphs: `native` or `mapped`. */
  graph_kind?: string;
  /** Graphs: the labels and relationship types the user may read. */
  labels?: string[];
  types?: string[];
  /** Graphs: readable only through grants on single labels or types. */
  partial?: boolean;
}

/** The database's graphs in a snapshot. */
export function graphsOf(snap: Pick<DbSnapshot, 'objects'> | undefined): ObjectInfo[] {
  return (snap?.objects ?? []).filter((o) => o.kind === 'graph');
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  key: string;
  default: string | null;
  extra: string;
  generated: boolean;
  position: number;
}

export interface DbSnapshot {
  db: string;
  objects: ObjectInfo[];
  columns: Record<string, ColumnInfo[]>;
  fetchedAt: number;
}

export async function fetchDatabases(): Promise<string[]> {
  const r = await cachedJson<{ databases: string[] }>('/v1/catalog');
  return r.databases;
}

export async function fetchSnapshot(db: string): Promise<DbSnapshot> {
  const r = await cachedJson<Omit<DbSnapshot, 'fetchedAt'>>(`/v1/databases/${encodeURIComponent(db)}/catalog`);
  return { ...r, fetchedAt: Date.now() };
}

const REFRESH_MS = 60_000;

export function useDatabases() {
  return useQuery({
    queryKey: ['catalog', 'databases'],
    queryFn: fetchDatabases,
    refetchInterval: REFRESH_MS,
    refetchIntervalInBackground: false,
  });
}

export function useSnapshot(db: string | null | undefined) {
  return useQuery({
    queryKey: ['catalog', 'db', db],
    queryFn: () => fetchSnapshot(db ?? ''),
    enabled: !!db,
    refetchInterval: REFRESH_MS,
    refetchIntervalInBackground: false,
  });
}

/** After DDL or a name error: refetch every snapshot. */
export function invalidateCatalog(qc: QueryClient): void {
  void qc.invalidateQueries({ queryKey: ['catalog'] });
}

/** Primary-key columns of a table, in key order (by ordinal position). */
export function primaryKey(cols: ColumnInfo[] | undefined): string[] {
  return (cols ?? []).filter((c) => c.key === 'PRI').map((c) => c.name);
}
