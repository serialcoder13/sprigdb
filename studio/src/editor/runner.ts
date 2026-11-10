// Running statements (chapter 44, "Results: streaming, caps, cursors,
// exports"): one stream per statement, results into capped buffers, cancel
// on a full buffer, on the user's Cancel, or at studio.statement_timeout_ms.
// The caps and the timeout also go to the gateway, which clamps them to the
// studio.* settings and stops the statement itself (decision ST8): a
// result it cut ends with `truncated`, a statement past its time with 3024.

import { query } from '../api/sql';
import { ResultBuffer } from '../results/buffer';
import type { ResultItem } from '../results/ResultView';

export interface Caps {
  maxRows: number;
  maxBytes: number;
  timeoutMs: number;
}

export interface RunHandle {
  done: Promise<{ items: ResultItem[]; outcome: 'ended' | 'cancelled' | 'timeout' }>;
  cancel(): void;
}

/** Runs one statement; `onUpdate` fires as rows arrive (for redraws). */
export function runStatement(db: string | null, sql: string, caps: Caps, onUpdate: (items: ResultItem[]) => void): RunHandle {
  const items: ResultItem[] = [];
  let current: ResultBuffer | null = null;
  let timedOut = false;
  let frame = 0;
  let finished = false;
  const redraw = () => {
    if (frame || finished) return;
    frame = requestAnimationFrame(() => {
      frame = 0;
      if (!finished) onUpdate([...items]);
    });
  };
  const running = query(db, sql, {
    header(_i, columns) {
      current = new ResultBuffer(columns, caps.maxRows, caps.maxBytes);
      items.push({ buffer: current });
      redraw();
    },
    rows(rows) {
      if (!current) return true;
      const more = current.push(rows);
      redraw();
      if (!more) current.truncated = true;
      return more;
    },
    end(e) {
      const last = items[items.length - 1];
      if (e.truncated && current) current.truncated = true;
      if (current && last && last.buffer === current) {
        last.warnings = e.warnings;
        last.info = e.info;
      } else {
        items.push({ buffer: null, affected: e.affected_rows, info: e.info, warnings: e.warnings });
      }
      current = null;
      redraw();
    },
  }, { maxRows: caps.maxRows, maxBytes: caps.maxBytes, timeoutMs: caps.timeoutMs });
  // The gateway stops the statement at the same time; the browser's timer
  // is the backstop for a gateway that doesn't answer.
  const timer = setTimeout(() => {
    timedOut = true;
    running.cancel();
  }, caps.timeoutMs + 2_000);
  const done = running.done
    .then((outcome) => {
      finished = true;
      if (frame) cancelAnimationFrame(frame);
      return { items, outcome: timedOut ? ('timeout' as const) : outcome };
    })
    .finally(() => {
      finished = true;
      clearTimeout(timer);
    });
  return { done, cancel: () => running.cancel() };
}
