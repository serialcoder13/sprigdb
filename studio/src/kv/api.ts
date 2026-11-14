// The key-value API: Exec runs one Redis command on a namespace, Scan pages
// keys by prefix, Put and Delete take a version condition. Calls run in the
// caller's own session.

import { json } from '../api/client';
import type { Reply } from './commands';

export interface Entry {
  key: string;
  type: string;
  value?: string;
  text?: string;
  version: string;
  expires_at_ms: string;
}

const base = (db: string, ns: string) => `/v1/databases/${encodeURIComponent(db)}/namespaces/${encodeURIComponent(ns)}`;

export async function exec(db: string, ns: string, command: string[], signal?: AbortSignal): Promise<Reply> {
  const r = await json<{ reply: Reply }>(`${base(db, ns)}:exec`, { body: { command }, signal });
  return r.reply;
}

export function scan(db: string, ns: string, prefix: string, pageToken: string, limit = 100): Promise<{ entries: Entry[]; next_page_token: string }> {
  return json(`${base(db, ns)}:scan`, { body: { prefix, page_token: pageToken, limit } });
}

export function put(db: string, ns: string, key: string, text: string, ifVersion?: string): Promise<{ version: string }> {
  return json(`${base(db, ns)}:put`, { body: { key, text, ...(ifVersion ? { if_version: ifVersion } : {}) } });
}

export function del(db: string, ns: string, key: string, ifVersion?: string): Promise<{ deleted: boolean }> {
  return json(`${base(db, ns)}:delete`, { body: { key, ...(ifVersion ? { if_version: ifVersion } : {}) } });
}

/** The commands that show a key's value, by type. */
export function viewCommand(type: string, key: string, limit = 100): string[] {
  switch (type) {
    case 'hash':
      return ['HGETALL', key];
    case 'list':
      return ['LRANGE', key, '0', String(limit - 1)];
    case 'set':
      return ['SMEMBERS', key];
    case 'zset':
      return ['ZRANGE', key, '0', String(limit - 1), 'WITHSCORES'];
    case 'stream':
      return ['XRANGE', key, '-', '+', 'COUNT', String(limit)];
    default:
      return ['GET', key];
  }
}
