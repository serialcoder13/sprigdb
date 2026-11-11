// SQL over the data API (chapter 37): Sql.Execute (buffered), Sql.Query
// (NDJSON, decoded in a worker), Transactions.

import { useSession, refresh } from '../auth/session';
import { CLIENT_HEADER, json } from './client';
import { ApiError, type ErrorBody } from './errors';
import { traceparent } from '../lib/ids';
import type { Column, ExecuteResponse, QueryPart, Row, Transaction } from './types';
import type { FromWorker, ToWorker } from './query.worker';

function dbPath(db: string | null | undefined): string {
  return encodeURIComponent(db && db.length ? db : '-');
}

/** Sql.Execute: every result, buffered (up to api.rest.max_buffered_bytes). */
export function execute(
  db: string | null | undefined,
  sql: string,
  opts: {
    transaction?: string;
    signal?: AbortSignal;
    multi?: boolean;
    language?: QueryLanguage;
    maxRows?: number;
    timeoutMs?: number;
  } = {},
): Promise<ExecuteResponse> {
  return json<ExecuteResponse>(`/v1/databases/${dbPath(db)}/sql:execute`, {
    body: {
      sql,
      transaction: opts.transaction,
      multi_statements: opts.multi ?? false,
      ...(opts.language && opts.language !== 'sql' ? { language: opts.language } : {}),
      ...(opts.maxRows ? { max_rows: opts.maxRows } : {}),
      ...(opts.timeoutMs ? { timeout_ms: opts.timeoutMs } : {}),
    },
    signal: opts.signal,
  });
}

/** The first result's rows, for small catalog queries. */
export async function rows(db: string | null | undefined, sql: string): Promise<{ columns: Column[]; rows: Row[] }> {
  const r = await execute(db, sql);
  const first = r.results.find((x) => x.columns?.length) ?? { columns: [], rows: [] };
  return { columns: first.columns ?? [], rows: first.rows ?? [] };
}

/** Rows as objects keyed by column name. */
export async function objects(db: string | null | undefined, sql: string): Promise<Record<string, string | null>[]> {
  const r = await rows(db, sql);
  return r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c.name, row[i] ?? null])));
}

export function begin(db: string | null | undefined): Promise<Transaction> {
  return json<Transaction>(`/v1/databases/${dbPath(db)}/transactions`, { body: {} });
}

export function commit(t: Transaction): Promise<unknown> {
  return json(`/v1/${t.name}:commit`, { body: {} });
}

export function rollback(t: Transaction): Promise<unknown> {
  return json(`/v1/${t.name}:rollback`, { body: {} });
}

export interface StreamHandlers {
  header(index: number, columns: Column[]): void;
  /** Rows of the current result; return false to stop the stream. */
  rows(rows: Row[]): boolean;
  end(end: Extract<QueryPart, { end: unknown }>['end']): void;
}

export interface Running {
  /** Resolves when the stream ends, is cancelled or fails. */
  done: Promise<'ended' | 'cancelled'>;
  cancel(): void;
}

/**
 * Sql.Query as NDJSON through a worker. Refreshes once on an expired token.
 * The returned `done` rejects with ApiError on failure.
 */
/** Caps sent with Sql.Query; the gateway clamps them to studio.* for Studio sessions. */
export interface QueryCaps {
  maxRows?: number;
  maxBytes?: number;
  timeoutMs?: number;
  /** `language` on Sql.Query (chapter 37, "Languages"): `sql` when absent. */
  language?: QueryLanguage;
}

/** The languages Sql.Query runs (chapter 37, bugfix4). */
export type QueryLanguage = 'sql' | 'cypher' | 'gql' | 'mongo';

export function query(db: string | null | undefined, sql: string, h: StreamHandlers, caps: QueryCaps = {}): Running {
  const worker = new Worker(new URL('./query.worker.ts', import.meta.url), { type: 'module' });
  let cancelled = false;
  let settle: { ok: (v: 'ended' | 'cancelled') => void; fail: (e: unknown) => void } | null = null;
  const done = new Promise<'ended' | 'cancelled'>((ok, fail) => {
    settle = { ok, fail };
  });
  const finish = (v: 'ended' | 'cancelled' | ApiError | Error) => {
    worker.terminate();
    if (v instanceof Error) settle?.fail(v);
    else settle?.ok(v);
    settle = null;
  };
  let retried = false;
  const start = () => {
    const token = useSession.getState().token;
    const tp = traceparent();
    const msg: ToWorker = {
      type: 'start',
      url: `/v1/databases/${dbPath(db)}/sql:query`,
      headers: {
        accept: 'application/x-ndjson',
        'content-type': 'application/json',
        'x-sprig-client': CLIENT_HEADER,
        traceparent: tp.header,
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({
        sql,
        ...(caps.maxRows ? { max_rows: caps.maxRows } : {}),
        ...(caps.maxBytes ? { max_bytes: caps.maxBytes } : {}),
        ...(caps.timeoutMs ? { timeout_ms: caps.timeoutMs } : {}),
        ...(caps.language && caps.language !== 'sql' ? { language: caps.language } : {}),
      }),
    };
    worker.onmessage = (ev: MessageEvent<FromWorker>) => {
      const m = ev.data;
      if (m.type === 'part') {
        for (const p of m.parts) {
          if (cancelled) return;
          if ('header' in p) h.header(p.header.index, p.header.columns);
          else if ('rows' in p) {
            if (!h.rows(p.rows.rows)) {
              cancelled = true;
              worker.postMessage({ type: 'cancel' } satisfies ToWorker);
              finish('cancelled');
              return;
            }
          } else if ('end' in p) h.end(p.end);
          else if ('error' in p) {
            finish(new ApiError(200, p.error, tp.traceId));
            return;
          }
        }
      } else if (m.type === 'http_error') {
        const body = (m.body ?? {}) as ErrorBody;
        if (m.status === 401 && body.code === 1045 && !retried) {
          retried = true;
          void refresh().then((t) => (t ? start() : finish(new ApiError(401, body, tp.traceId))));
          return;
        }
        finish(new ApiError(m.status, body, tp.traceId));
      } else if (m.type === 'failed') {
        finish(new Error(m.message));
      } else {
        finish(cancelled ? 'cancelled' : 'ended');
      }
    };
    worker.postMessage(msg);
  };
  start();
  return {
    done,
    cancel: () => {
      if (cancelled) return;
      cancelled = true;
      worker.postMessage({ type: 'cancel' } satisfies ToWorker);
      finish('cancelled');
    },
  };
}

/** One statement's verdict from Sql.Check (chapter 37). */
export interface CheckedStatement {
  start: number;
  end: number;
  class: 'read' | 'write' | 'ddl' | 'account' | 'transaction' | 'other';
  checked: 'bound' | 'parsed';
  diagnostics: {
    severity: 'error' | 'warning';
    source: 'server' | 'parser';
    code: number;
    name: string;
    message: string;
    start: number;
    end: number;
  }[];
}

/** Sql.Check: classes and problems with positions, nothing run. */
export async function check(
  db: string | null | undefined,
  sql: string,
  signal?: AbortSignal,
): Promise<CheckedStatement[]> {
  const r = await json<{ statements: CheckedStatement[] }>(`/v1/databases/${dbPath(db)}/sql:check`, {
    body: { sql },
    signal,
  });
  return r.statements;
}
