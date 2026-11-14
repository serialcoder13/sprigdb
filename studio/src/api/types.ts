// Types of the data API's JSON: each SQL
// cell is the MySQL text form or null; binary columns are base64.

export interface Column {
  name: string;
  org_name?: string;
  table?: string;
  org_table?: string;
  database?: string;
  type: string;
  mysql_type: number;
  flags?: number;
  charset?: number;
  decimals?: number;
  length?: string;
  binary?: boolean;
}

export type Cell = string | null;
export type Row = Cell[];

export interface ResultSet {
  columns: Column[];
  rows: Row[];
  affected_rows?: string;
  last_insert_id?: string;
  warnings?: number;
  info?: string;
}

export interface ExecuteResponse {
  results: ResultSet[];
}

/** One Sql.Query NDJSON message. */
export type QueryPart =
  | { header: { index: number; columns: Column[] } }
  | { rows: { rows: Row[] } }
  | {
      end: {
        index: number;
        affected_rows: string;
        last_insert_id?: string;
        warnings?: number;
        info?: string;
        more_results?: boolean;
        /** The server's cap (max_rows, max_bytes) cut the result here. */
        truncated?: boolean;
      };
    }
  | { error: import('./errors').ErrorBody };

export interface Transaction {
  name: string;
  idle_deadline_ms: string;
  deadline_ms: string;
}

export interface TenantNode {
  name: string | null;
  id: string | null;
  path: string | null;
  relation: 'home' | 'admin' | 'foreign';
}

export interface StudioSettings {
  session_idle_minutes: number;
  session_max_hours: number;
  persist_drafts: boolean;
  history_max: number;
  max_result_rows: number;
  max_result_mb: number;
  statement_timeout_ms: number;
  confirm_writes: 'off' | 'ddl' | 'all';
  export_ticket_seconds: number;
  import_batch_rows: number;
  import_max_mb: number;
  feed_view_max_events: number;
  graph_max_elements: number;
  map_tile_url: string;
  view_as: string;
  operator_mode: string;
}

export interface Privileges {
  tenant_admin: boolean;
  settings_admin: boolean;
  create_user: boolean;
  grant_option: boolean;
  index_all: boolean;
  index_dbs: string[];
}

export interface WhoAmI {
  principal: { tenant: string | null; tenant_id: string | null; user: string; host: string; login: string };
  amr: string[];
  session: {
    id: string;
    client: string;
    created_at_ms: string;
    idle_minutes: number;
    max_hours: number;
    access_ttl_seconds: number;
  };
  tenants: TenantNode[];
  grants: string[];
  privileges: Privileges;
  platform_roles: string[];
  studio: StudioSettings;
  gateway?: { id: string; dc: string; studio_build: string };
  /** The administrator acting, in an impersonation session. */
  impersonator?: string | null;
}

import type { components } from './gen/v1';

/** Generated from the OpenAPI document (pnpm gen:api). */
export type Schemas = components['schemas'];
export type TokenReply = Schemas['TokenReply'];
export type ExportTicket = Schemas['ExportTicket'];
export type CreatedApiKey = Schemas['CreatedApiKey'];

// The hand-written WhoAmI must stay assignable to the generated one.
type Assignable<A, B> = A extends B ? true : false;
export const WHOAMI_MATCHES: Assignable<WhoAmI['principal'], Schemas['WhoAmI']['principal']> = true;
