// Decodes a Sql.Query NDJSON stream off the main thread (chapter 44, "API
// client and routes": streams are decoded in a Web Worker). The main thread
// sends {type: 'start', url, headers, body}; the worker posts header, rows,
// end, error and done messages, and stops on {type: 'cancel'} (the gateway
// then kills the backend query).

import { NdjsonDecoder } from './ndjson';
import type { QueryPart } from './types';

export type ToWorker =
  | { type: 'start'; url: string; headers: Record<string, string>; body: string }
  | { type: 'cancel' };

export type FromWorker =
  | { type: 'part'; parts: QueryPart[] }
  | { type: 'http_error'; status: number; body: unknown }
  | { type: 'failed'; message: string }
  | { type: 'done' };

let controller: AbortController | null = null;

function post(m: FromWorker): void {
  (self as unknown as Worker).postMessage(m);
}

async function start(m: Extract<ToWorker, { type: 'start' }>): Promise<void> {
  controller = new AbortController();
  try {
    const r = await fetch(m.url, {
      method: 'POST',
      headers: m.headers,
      body: m.body,
      signal: controller.signal,
      credentials: 'same-origin',
      cache: 'no-store',
    });
    if (!r.ok || !r.body) {
      let body: unknown = null;
      try {
        body = await r.json();
      } catch {
        body = { message: `${r.status} ${r.statusText}` };
      }
      post({ type: 'http_error', status: r.status, body });
      return;
    }
    const reader = r.body.getReader();
    const dec = new NdjsonDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      const parts = dec.push(value) as QueryPart[];
      if (parts.length) post({ type: 'part', parts });
    }
    const rest = dec.end() as QueryPart[];
    if (rest.length) post({ type: 'part', parts: rest });
    post({ type: 'done' });
  } catch (e) {
    if (controller.signal.aborted) {
      post({ type: 'done' });
    } else {
      post({ type: 'failed', message: e instanceof Error ? e.message : String(e) });
    }
  }
}

self.onmessage = (ev: MessageEvent<ToWorker>) => {
  const m = ev.data;
  if (m.type === 'start') void start(m);
  else controller?.abort();
};
