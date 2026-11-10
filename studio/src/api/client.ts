// The one call wrapper (chapter 44, "API client and routes"): bearer token,
// x-sprig-client, a fresh traceparent per call, one coordinated refresh and
// retry on an expired token, errors as ApiError with their trace id.

import { traceparent } from '../lib/ids';
import { ApiError, SESSION_ENDED_CODES, type ErrorBody } from './errors';

export const CLIENT_VERSION = '0.1.0';
export const CLIENT_HEADER = `studio/${CLIENT_VERSION}`;

export interface AuthHooks {
  /** The access token in memory, if any. */
  token(): string | null;
  /** One coordinated refresh; the new token, or null when the session ended. */
  refresh(): Promise<string | null>;
  /** The session ended (refresh refused). */
  ended(e: ApiError): void;
}

let hooks: AuthHooks | null = null;

export function setAuthHooks(h: AuthHooks | null): void {
  hooks = h;
}

export interface RequestOptions {
  method?: string;
  body?: unknown;
  accept?: string;
  signal?: AbortSignal;
  /** Send the bearer token (default true). */
  auth?: boolean;
  /** Extra headers. */
  headers?: Record<string, string>;
  /** Let the request outlive the page (closing a cursor on pagehide). */
  keepalive?: boolean;
}

async function errorFrom(r: Response, traceId: string): Promise<ApiError> {
  let body: ErrorBody;
  try {
    body = (await r.json()) as ErrorBody;
  } catch {
    body = { message: `${r.status} ${r.statusText}` };
  }
  return new ApiError(r.status, body, traceId);
}

/** Sends a request; resolves to the response when it is 2xx. */
export async function request(path: string, opts: RequestOptions = {}): Promise<Response> {
  const send = async (token: string | null) => {
    const tp = traceparent();
    const headers: Record<string, string> = {
      'x-sprig-client': CLIENT_HEADER,
      traceparent: tp.header,
      ...opts.headers,
    };
    if (opts.accept) headers.accept = opts.accept;
    const body = opts.body === undefined ? undefined : JSON.stringify(opts.body);
    if (body !== undefined) headers['content-type'] = 'application/json';
    if (token && opts.auth !== false) headers.authorization = `Bearer ${token}`;
    const r = await fetch(path, {
      method: opts.method ?? (body === undefined ? 'GET' : 'POST'),
      headers,
      body,
      signal: opts.signal,
      credentials: 'same-origin',
      cache: 'no-store',
      ...(opts.keepalive ? { keepalive: true } : {}),
    });
    return { r, traceId: tp.traceId };
  };
  let { r, traceId } = await send(hooks?.token() ?? null);
  if (r.status === 401 && opts.auth !== false && hooks) {
    const err = await errorFrom(r, traceId);
    // An expired access token: refresh once and retry. Refusals that end
    // the session (idle, max age) aren't retried.
    if (err.code === 1045) {
      const t = await hooks.refresh();
      if (t) {
        ({ r, traceId } = await send(t));
      } else {
        hooks.ended(err);
        throw err;
      }
    } else {
      if (SESSION_ENDED_CODES.has(err.code)) hooks.ended(err);
      throw err;
    }
  }
  if (!r.ok && r.status !== 304) {
    const err = await errorFrom(r, traceId);
    if (r.status === 401 && hooks && SESSION_ENDED_CODES.has(err.code)) hooks.ended(err);
    throw err;
  }
  return r;
}

/** A JSON call. */
export async function json<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const r = await request(path, opts);
  if (r.status === 204) return undefined as T;
  return (await r.json()) as T;
}

const etagged = new Map<string, { etag: string; body: unknown }>();

/**
 * A GET whose answer carries an ETag (the Catalog service, chapter 37):
 * sent with If-None-Match, and a 304 returns the copy kept from last time.
 */
export async function cachedJson<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const kept = etagged.get(path);
  const r = await request(path, {
    ...opts,
    headers: { ...opts.headers, ...(kept ? { 'if-none-match': kept.etag } : {}) },
  });
  if (r.status === 304 && kept) return kept.body as T;
  const body = (await r.json()) as T;
  const tag = r.headers.get('etag');
  if (tag) etagged.set(path, { etag: tag, body });
  else etagged.delete(path);
  return body;
}

/** Forgets every kept copy (sign-out, tenant switch). */
export function clearCachedJson(): void {
  etagged.clear();
}
