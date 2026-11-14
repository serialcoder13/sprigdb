// Fetching past the row cap. A cursor rides the gateway's stream of the
// statement with backpressure; it holds no backend session of its own, and
// the gateway closes it after studio.cursor_idle_seconds.
//
// The browser closes its cursor as soon as it isn't needed: a new run, the
// result leaving the page, the tab hidden or closed. "Fetch more" after
// that (or after the gateway's idle timeout, 39935) opens the statement
// again, skipping the rows already read, and says so: the rows after a
// reopen come from a new run of the statement.

import { json } from '../api/client';
import { isApiError } from '../api/errors';
import type { Column, Row } from '../api/types';
import type { ResultBuffer } from './buffer';

export const CURSOR_EXPIRED = 39935;
export const CURSOR_UNKNOWN = 39934;

export interface CursorPage {
  rows: Row[];
  /** Rows delivered or skipped so far (a 64-bit integer as text). */
  position: string;
  done: boolean;
}

export interface CursorOpened extends CursorPage {
  /** `cursors/<h>`; empty when the first page was the whole result. */
  name: string;
  columns: Column[];
  idle_seconds?: number;
}

function dbPath(db: string | null | undefined): string {
  return encodeURIComponent(db && db.length ? db : '-');
}

export function openCursor(
  db: string | null | undefined,
  sql: string,
  opts: { skip?: number; pageRows?: number } = {},
): Promise<CursorOpened> {
  return json<CursorOpened>(`/v1/databases/${dbPath(db)}/cursors`, {
    body: { sql, skip: opts.skip ?? 0, page_rows: opts.pageRows ?? 0 },
  });
}

export function fetchCursor(name: string, rows: number): Promise<CursorPage> {
  return json<CursorPage>(`/v1/${name}:fetch`, { body: { rows } });
}

export function closeCursor(name: string, keepalive = false): Promise<unknown> {
  return json(`/v1/${name}:close`, { body: {}, keepalive });
}

/** What one "fetch more" did. */
export interface MoreResult {
  added: number;
  done: boolean;
  /** The statement was run again from the rows already read. */
  reopened: boolean;
}

/**
 * Pages one result past its cap into `buffer`'s sliding window.
 * `position` is the result's row number after the last row read.
 */
export class Pager {
  private name = '';
  private position: number;
  private closed = false;
  private everOpened = false;
  done = false;
  /** The rows after `reopenedAt` came from a new run of the statement. */
  reopenedAt: number | null = null;

  constructor(
    readonly db: string | null,
    readonly sql: string,
    readonly buffer: ResultBuffer,
    readonly pageRows: number,
  ) {
    this.position = buffer.offset + buffer.rowCount;
  }

  /** Whether a cursor is open on the gateway now. */
  get open(): boolean {
    return this.name !== '';
  }

  /** The next page into the buffer. */
  async more(): Promise<MoreResult> {
    if (this.done) return { added: 0, done: true, reopened: false };
    this.closed = false;
    let reopened = false;
    let page: CursorPage;
    if (!this.name) {
      reopened = this.everOpened;
      const o = await openCursor(this.db, this.sql, { skip: this.position, pageRows: this.pageRows });
      this.everOpened = true;
      if (this.closed) {
        // Closed while opening (tab hidden): don't keep it.
        if (o.name) void closeCursor(o.name).catch(() => undefined);
      } else {
        this.name = o.name;
      }
      page = o;
    } else {
      try {
        page = await fetchCursor(this.name, this.pageRows);
      } catch (e) {
        if (isApiError(e) && (e.code === CURSOR_EXPIRED || e.code === CURSOR_UNKNOWN)) {
          this.name = '';
          return this.more().then((r) => ({ ...r, reopened: true }));
        }
        throw e;
      }
    }
    if (reopened && this.reopenedAt === null) this.reopenedAt = this.position;
    this.buffer.slide(page.rows);
    this.position = Number(page.position);
    if (page.done) {
      this.done = true;
      this.name = '';
    }
    return { added: page.rows.length, done: page.done, reopened };
  }

  /** Closes the gateway's cursor (the next `more` reopens it). */
  close(keepalive = false): void {
    this.closed = true;
    const n = this.name;
    this.name = '';
    if (n) void closeCursor(n, keepalive).catch(() => undefined);
  }
}

/** Every pager on the page, to close when the tab is hidden or closed. */
const live = new Set<Pager>();

export function track(p: Pager): () => void {
  live.add(p);
  return () => {
    live.delete(p);
    p.close();
  };
}

/** Closes every open cursor: idle tabs keep nothing open on the gateway. */
export function closeAll(keepalive = false): number {
  let n = 0;
  for (const p of live) {
    if (p.open) n++;
    p.close(keepalive);
  }
  return n;
}

let installed = false;

/** Closes cursors when the tab is hidden or the page goes away. */
export function installCursorHooks(): void {
  if (installed || typeof document === 'undefined') return;
  installed = true;
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') closeAll(true);
  });
  window.addEventListener('pagehide', () => closeAll(true));
}
