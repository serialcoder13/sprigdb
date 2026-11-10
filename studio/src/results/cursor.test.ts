import { afterEach, describe, expect, it, vi } from 'vitest';
import { ResultBuffer } from './buffer';
import { Pager, closeAll, track } from './cursor';

const cols = [{ name: 'n', type: 'LONG', mysql_type: 3 }];

interface Call {
  url: string;
  body: Record<string, unknown>;
  keepalive: boolean;
}

/** A gateway that serves rows 0..total-1 by cursor; `expire()` drops them. */
function fakeGateway(total: number) {
  const calls: Call[] = [];
  const open = new Map<string, number>();
  let n = 0;
  const reply = (status: number, body: unknown) =>
    new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
  const page = (from: number, rows: number) => {
    const to = Math.min(total, from + rows);
    return { rows: Array.from({ length: to - from }, (_, i) => [String(from + i)]), position: String(to), done: to >= total };
  };
  vi.stubGlobal('fetch', (url: string, init: RequestInit) => {
    const body = JSON.parse((init.body as string) ?? '{}') as Record<string, unknown>;
    calls.push({ url, body, keepalive: Boolean(init.keepalive) });
    if (url.endsWith('/cursors')) {
      const skip = Number(body.skip ?? 0);
      const p = page(skip, Number(body.page_rows ?? 10));
      const name = p.done ? '' : `cursors/c${++n}`;
      if (name) open.set(name, Number(p.position));
      return Promise.resolve(reply(200, { ...p, name, columns: cols }));
    }
    const m = /^\/v1\/(cursors\/c\d+):(fetch|close)$/.exec(url);
    const name = m?.[1] ?? '';
    if (m?.[2] === 'close') {
      const had = open.delete(name);
      return Promise.resolve(reply(200, { closed: had }));
    }
    const at = open.get(name);
    if (at === undefined) {
      return Promise.resolve(reply(409, { code: 39935, name: 'ER_SPRIG_STUDIO_CURSOR_EXPIRED', message: 'expired' }));
    }
    const p = page(at, Number(body.rows ?? 10));
    if (p.done) open.delete(name);
    else open.set(name, Number(p.position));
    return Promise.resolve(reply(200, p));
  });
  return { calls, open, expire: () => open.clear() };
}

function buffer(shown: number, cap: number): ResultBuffer {
  const b = new ResultBuffer(cols, cap, 1e9);
  b.push(Array.from({ length: shown }, (_, i) => [String(i)]));
  b.truncated = true;
  return b;
}

afterEach(() => vi.unstubAllGlobals());

describe('Pager', () => {
  it('opens after the rows shown, then fetches pages into a sliding window', async () => {
    const g = fakeGateway(35);
    const b = buffer(10, 10);
    const p = new Pager('shop', 'SELECT n FROM t', b, 10);
    expect((await p.more()).added).toBe(10);
    expect(g.calls[0]?.body).toMatchObject({ sql: 'SELECT n FROM t', skip: 10, page_rows: 10 });
    expect(b.offset).toBe(10);
    expect(b.cell(0, 0)).toBe('10');
    await p.more();
    const last = await p.more();
    expect(last).toMatchObject({ added: 5, done: true });
    expect(b.offset + b.rowCount).toBe(35);
    expect(b.cell(b.rowCount - 1, 0)).toBe('34');
    expect(p.open).toBe(false);
    expect(g.open.size).toBe(0);
  });

  it('reopens from where it stopped when the gateway expired the cursor', async () => {
    const g = fakeGateway(100);
    const b = buffer(10, 10);
    const p = new Pager(null, 'SELECT n', b, 10);
    await p.more();
    g.expire();
    const r = await p.more();
    expect(r.reopened).toBe(true);
    expect(b.cell(0, 0)).toBe('20');
    const reopen = g.calls.filter((c) => c.url.endsWith('/cursors'));
    expect(reopen.map((c) => c.body.skip)).toEqual([10, 20]);
    expect(p.reopenedAt).toBe(20);
  });

  it('closes every open cursor when the tab is hidden, and reopens on demand', async () => {
    const g = fakeGateway(1000);
    const a = new Pager(null, 'SELECT a', buffer(10, 10), 10);
    const b = new Pager(null, 'SELECT b', buffer(10, 10), 10);
    const untrackA = track(a);
    track(b);
    await a.more();
    await b.more();
    expect(g.open.size).toBe(2);
    expect(closeAll(true)).toBe(2);
    await Promise.resolve();
    expect(g.open.size).toBe(0);
    expect(g.calls.filter((c) => c.url.endsWith(':close')).every((c) => c.keepalive)).toBe(true);
    // Coming back: the next page opens a new cursor from row 20.
    await a.more();
    expect(g.calls.at(-1)?.body.skip).toBe(20);
    untrackA();
    await Promise.resolve();
    expect(g.open.size).toBe(0);
  });
});
