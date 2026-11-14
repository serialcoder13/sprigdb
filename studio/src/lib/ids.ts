// Random ids: W3C trace context for each user action, and local ids.

function hex(bytes: number): string {
  const b = new Uint8Array(bytes);
  crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
}

/** A fresh `traceparent`: version 00, a new trace id and span id, sampled. */
export function traceparent(): { header: string; traceId: string } {
  const traceId = hex(16);
  return { header: `00-${traceId}-${hex(8)}-01`, traceId };
}

export function localId(): string {
  return hex(8);
}
