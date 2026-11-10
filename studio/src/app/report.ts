// Client errors go only to the gateway that served Studio (chapter 44:
// kind, message, stack, route; never data values).

let sent = 0;

export function reportError(kind: 'js' | 'chunk_load' | 'api', err: unknown, traceId?: string): void {
  if (sent++ > 20) return;
  const e = err instanceof Error ? err : new Error(String(err));
  const body = JSON.stringify({
    kind,
    message: e.message.slice(0, 500),
    stack: (e.stack ?? '').slice(0, 4000),
    route: window.location.pathname,
    trace_id: traceId,
  });
  try {
    void fetch('/studio/_/errors', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
      keepalive: true,
    });
  } catch {
    // Reporting is best effort.
  }
}
